#!/usr/bin/env python3
"""
多 Slave 真正并发 RDMA 全量同步端到端测试。

拓扑：
    Master: --repl-transport tcp --repl-fullsync-transport rdma
            --repl-realtime-transport tcp   (走新的 async fullsync 路径)
    Slave A/B: 同时启动并向同一 Master 发起 REPLSYNC。

验收：
  1. 两台 Slave 都完成全量同步，抽样 Key 完全一致；
  2. Master INFO 的 rdma_fs_max_concurrency_seen >= 2，且 rdma_fs_total_completed >= 2
     （证明两个 job 在同一时间段内处于 RDMA_SENDING，并各自推进 WRITE）；
  3. 测试结束后 Master 仍存活，active_jobs 归零。

默认用 --rdma-chunk-size 1024 --rdma-send-slots 1 拉长单 job 发送窗口，
避免 soft-RoCE 上传输过快导致观测不到重叠。真实 NIC 上可调大。

运行前提：主机有可用 RDMA 设备（默认 siw0/rxe0），--host 必须是该设备可达的
本机地址（不能是 127.0.0.1；soft-RoCE 需要真实网卡地址）。
"""
import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def send_cmd(sock, *args):
    cmd = (f"*{len(args)}\r\n" + "".join(f"${len(a)}\r\n{a}\r\n" for a in args)).encode()
    sock.sendall(cmd)


def _read_item(sock, buf):
    while b"\r\n" not in buf[0]:
        chunk = sock.recv(65536)
        if not chunk:
            return buf[0], buf
        buf[0] += chunk
    line, rest = buf[0].split(b"\r\n", 1)
    line += b"\r\n"
    buf[0] = rest
    t = line[:1]
    if t == b"$":
        try:
            n = int(line[1:-2])
        except ValueError:
            return line, buf
        if n < 0:
            return line, buf
        while len(buf[0]) < n + 2:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf[0] += chunk
        payload, buf[0] = buf[0][:n + 2], buf[0][n + 2:]
        return line + payload, buf
    if t == b"*":
        try:
            n = int(line[1:-2])
        except ValueError:
            return line, buf
        out = [line]
        for _ in range(max(n, 0)):
            item, buf = _read_item(sock, buf)
            out.append(item)
        return b"".join(out), buf
    return line, buf


def read_resp(sock):
    buf = [b""]
    item, _ = _read_item(sock, buf)
    return item


def req(host, port, *args, timeout=5):
    s = socket.create_connection((host, port), timeout=timeout)
    try:
        send_cmd(s, *args)
        return read_resp(s)
    finally:
        s.close()


def parse_info(text):
    out = {}
    for line in text.decode(errors="ignore").splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def master_info(host, port):
    try:
        return parse_info(req(host, port, "INFO"))
    except Exception:
        return {}


def wait_master(host, port, tries=100):
    for _ in range(tries):
        try:
            if b"master" in req(host, port, "ROLE"):
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def wait_slave(host, port, tries=150):
    for _ in range(tries):
        try:
            if b"slave" in req(host, port, "ROLE"):
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def wait_key(host, port, key, expect, tries=300):
    for _ in range(tries):
        try:
            r = req(host, port, "HGET", key)
            if expect.encode() in r:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def preload(host, port, count, batch=1000):
    """持久连接 + 批量 pipeline，避免数万次短连接耗尽本地端口。"""
    s = socket.create_connection((host, port), timeout=30)
    try:
        done = 0
        while done < count:
            n = min(batch, count - done)
            payload = []
            for i in range(done, done + n):
                k, v = f"k:{i}", f"v{i}"
                payload.append(
                    f"*3\r\n$4\r\nHSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")
            s.sendall("".join(payload).encode())
            seen = 0
            buf = b""
            while seen < n:
                chunk = s.recv(65536)
                if not chunk:
                    raise RuntimeError("master closed during preload")
                buf += chunk
                seen = buf.count(b"+OK\r\n")
            done += n
    finally:
        s.close()


def stop(proc):
    if not proc:
        return
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default=os.environ.get("REPL_RDMA_SMOKE_HOST", "192.168.233.128"))
    ap.add_argument("--master-port", type=int, default=5260)
    ap.add_argument("--slave-a-port", type=int, default=5262)
    ap.add_argument("--slave-b-port", type=int, default=5264)
    ap.add_argument("--rdma-dev", default=os.environ.get("RDMA_DEV", "siw0"))
    ap.add_argument("--rdma-port", type=int, default=0,
                    help="RDMA CM 端口；0=master_port+10（同机测试避开 Slave 的 master_port+1 proxy 监听）")
    ap.add_argument("--preload", type=int, default=30000)
    ap.add_argument("--chunk-size", type=int, default=1024)
    ap.add_argument("--send-slots", type=int, default=1)
    ap.add_argument("--pause-us", type=int, default=2000)
    ap.add_argument("--wait-seconds", type=float, default=60.0)
    ap.add_argument("--work-dir", default="./artifacts/repl/multi-rdma-fullsync")
    args = ap.parse_args()

    if not Path(f"/sys/class/infiniband/{args.rdma_dev}").exists():
        print(f"MULTI_RDMA_FULLSYNC_SKIP: RDMA device {args.rdma_dev} not found")
        return 0

    if args.rdma_port <= 0:
        args.rdma_port = args.master_port + 10

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(bin_path).parent)
    wd = Path(args.work_dir).resolve()
    shutil.rmtree(wd, ignore_errors=True)
    wd.mkdir(parents=True, exist_ok=True)

    common = [
        bin_path,
        "--rdma-dev", args.rdma_dev,
        "--repl-transport", "tcp",
        "--repl-fullsync-transport", "rdma",
        "--repl-realtime-transport", "tcp",
        "--rdma-chunk-size", str(args.chunk_size),
        "--rdma-send-slots", str(args.send_slots),
        "--rdma-fullsync-pause-us", str(args.pause_us),
        "--rdma-port", str(args.rdma_port),
    ]

    def log(name):
        return open(wd / f"{name}.log", "wb")

    master = slave_a = slave_b = None
    master_log = slave_a_log = slave_b_log = None
    rc = 1
    try:
        master_log = log("master")
        master = subprocess.Popen(
            common + ["--port", str(args.master_port), "--role", "master",
                      "--master-host", args.host, "--master-port", str(args.master_port),
                      "--dump", str(wd / "master.dump"), "--aof", str(wd / "master.aof")],
            cwd=cwd, stdout=master_log, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        if not wait_master(args.host, args.master_port):
            print("MULTI_RDMA_FULLSYNC_FAIL: master not ready")
            return 1

        print(f"[1/4] preload {args.preload} keys")
        preload(args.host, args.master_port, args.preload)
        mid = args.preload // 2
        last = args.preload - 1

        print("[2/4] start two slaves")
        slave_a_log = log("slave-a")
        slave_b_log = log("slave-b")
        slave_a = subprocess.Popen(
            common + ["--port", str(args.slave_a_port), "--role", "slave",
                      "--master-host", args.host, "--master-port", str(args.master_port),
                      "--dump", str(wd / "a.dump"), "--aof", str(wd / "a.aof")],
            cwd=cwd, stdout=slave_a_log, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        slave_b = subprocess.Popen(
            common + ["--port", str(args.slave_b_port), "--role", "slave",
                      "--master-host", args.host, "--master-port", str(args.master_port),
                      "--dump", str(wd / "b.dump"), "--aof", str(wd / "b.aof")],
            cwd=cwd, stdout=slave_b_log, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        if not wait_slave(args.host, args.slave_a_port) or not wait_slave(args.host, args.slave_b_port):
            print("MULTI_RDMA_FULLSYNC_FAIL: slave not ready")
            return 1

        print("[3/4] wait fullsync completion")
        deadline = time.time() + args.wait_seconds
        info = {}
        while time.time() < deadline:
            info = master_info(args.host, args.master_port)
            try:
                completed = int(info.get("rdma_fs_total_completed", "0"))
            except ValueError:
                completed = 0
            if completed >= 2:
                break
            time.sleep(0.1)

        # 等待 manager 回收 job/快照（active_jobs 归零）再断言
        for _ in range(100):
            info = master_info(args.host, args.master_port)
            if info.get("rdma_fs_active_jobs") == "0":
                break
            time.sleep(0.1)

        ok_a = wait_key(args.host, args.slave_a_port, "k:0", "v0") and \
            wait_key(args.host, args.slave_a_port, f"k:{mid}", f"v{mid}") and \
            wait_key(args.host, args.slave_a_port, f"k:{last}", f"v{last}")
        ok_b = wait_key(args.host, args.slave_b_port, "k:0", "v0") and \
            wait_key(args.host, args.slave_b_port, f"k:{mid}", f"v{mid}") and \
            wait_key(args.host, args.slave_b_port, f"k:{last}", f"v{last}")

        try:
            completed = int(info.get("rdma_fs_total_completed", "0"))
            failed = int(info.get("rdma_fs_total_failed", "0"))
            max_conc = int(info.get("rdma_fs_max_concurrency_seen", "0"))
        except ValueError:
            completed = failed = max_conc = 0
        active = info.get("rdma_fs_active_jobs", "0")

        print("[4/4] result")
        print(f"  master_completed={completed} failed={failed} "
              f"max_concurrency_seen={max_conc} active_jobs={active}")
        print(f"  slave_a_converged={ok_a} slave_b_converged={ok_b}")
        print(f"  artifacts={wd}")

        if ok_a and ok_b and completed >= 2 and max_conc >= 2 and active == "0":
            print("MULTI_RDMA_FULLSYNC_PASS")
            rc = 0
        else:
            print("MULTI_RDMA_FULLSYNC_FAIL")
            rc = 1
        return rc
    finally:
        stop(slave_a)
        stop(slave_b)
        stop(master)
        for f in (master_log, slave_a_log, slave_b_log):
            if f:
                f.close()
        if rc != 0:
            for name in ("master", "slave-a", "slave-b"):
                p = wd / f"{name}.log"
                if p.exists():
                    print(f"--- {name}.log tail ---")
                    print("\n".join(p.read_text(errors="ignore").splitlines()[-30:]))


if __name__ == "__main__":
    sys.exit(main())
