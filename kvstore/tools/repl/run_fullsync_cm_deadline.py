#!/usr/bin/env python3
"""
RDMA-01 回归：CM 建连超时必须从 FULLRESYNC 完整写出后开始。

Master 在生成快照前通过测试钩子 `KVS_TEST_FULLSYNC_SNAPSHOT_DELAY_MS`
人为暂停 12s（> 10s CM_WAIT）。正确的实现下：
  - worker 在 header_sent 之前不计 CM 超时；
  - FULLRESYNC 发出后 Slave 正常建连并完成全量；
  - rdma_fs_total_failed == 0。

旧实现会在快照阶段就耗尽 CM 预算，header 发出时 job 已 FAILED。
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


def enc(*args):
    return (f"*{len(args)}\r\n" + "".join(f"${len(a)}\r\n{a}\r\n" for a in args)).encode()


def _read_item(sock, buf):
    while b"\r\n" not in buf[0]:
        chunk = sock.recv(65536)
        if not chunk:
            return buf[0], buf
        buf[0] += chunk
    line, rest = buf[0].split(b"\r\n", 1)
    line += b"\r\n"
    buf[0] = rest
    if line[:1] == b"$":
        n = int(line[1:-2])
        if n < 0:
            return line, buf
        while len(buf[0]) < n + 2:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf[0] += chunk
        payload, buf[0] = buf[0][:n + 2], buf[0][n + 2:]
        return line + payload, buf
    if line[:1] == b"*":
        n = int(line[1:-2])
        out = [line]
        for _ in range(max(n, 0)):
            item, buf = _read_item(sock, buf)
            out.append(item)
        return b"".join(out), buf
    return line, buf


def req(host, port, *args, timeout=5):
    s = socket.create_connection((host, port), timeout=timeout)
    try:
        s.sendall(enc(*args))
        return _read_item(s, [b""])[0]
    finally:
        s.close()


def info(host, port):
    try:
        text = req(host, port, "INFO").decode(errors="ignore")
        return dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
    except Exception:
        return {}


def wait_ready(host, port, tries=120):
    for _ in range(tries):
        try:
            if b"master" in req(host, port, "ROLE") or b"slave" in req(host, port, "ROLE"):
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def preload(host, port, count, batch=1000):
    s = socket.create_connection((host, port), timeout=30)
    done = 0
    while done < count:
        n = min(batch, count - done)
        out = []
        for i in range(done, done + n):
            k, v = f"k:{i}", f"v{i}"
            out.append(f"*3\r\n$4\r\nHSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")
        s.sendall("".join(out).encode())
        seen = 0
        buf = b""
        while seen < n:
            chunk = s.recv(65536)
            if not chunk:
                raise RuntimeError("master closed during preload")
            buf += chunk
            seen = buf.count(b"+OK\r\n")
        done += n
    s.close()


def verify_all(host, port, count, batch=1000):
    s = socket.create_connection((host, port), timeout=30)
    buf = [b""]
    bad = 0
    try:
        for start in range(0, count, batch):
            n = min(batch, count - start)
            out = []
            for i in range(start, start + n):
                k = f"k:{i}"
                out.append(f"*2\r\n$4\r\nHGET\r\n${len(k)}\r\n{k}\r\n")
            s.sendall("".join(out).encode())
            for i in range(start, start + n):
                item, _ = _read_item(s, buf)
                try:
                    head, rest = item.split(b"\r\n", 1)
                    ln = int(head[1:])
                except Exception:
                    bad += 1
                    continue
                val = rest[:-2] if ln >= 0 else None
                if val != f"v{i}".encode():
                    bad += 1
    finally:
        s.close()
    return bad


def stop(proc):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            proc.terminate()
    if proc:
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
    ap.add_argument("--host", default="192.168.233.128")
    ap.add_argument("--port", type=int, default=5490)
    ap.add_argument("--rdma-dev", default=os.environ.get("RDMA_DEV", "siw0"))
    ap.add_argument("--rdma-port", type=int, default=0)
    ap.add_argument("--preload", type=int, default=5000)
    ap.add_argument("--delay-ms", type=int, default=12000)
    ap.add_argument("--require-rdma", action="store_true")
    ap.add_argument("--work-dir", default="./artifacts/repl/fullsync-cm-deadline")
    args = ap.parse_args()

    if not Path(f"/sys/class/infiniband/{args.rdma_dev}").exists():
        if args.require_rdma:
            print("FULLSYNC_CM_DEADLINE_FAIL: required RDMA device missing")
            return 1
        print("FULLSYNC_CM_DEADLINE_SKIP: RDMA device missing")
        return 0
    if args.rdma_port <= 0:
        args.rdma_port = args.port + 10

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(bin_path).parent)
    wd = Path(args.work_dir).resolve()
    shutil.rmtree(wd, ignore_errors=True)
    wd.mkdir(parents=True, exist_ok=True)
    mlog = open(wd / "master.log", "wb")
    slog = open(wd / "slave.log", "wb")
    env = {**os.environ, "KVS_TEST_FULLSYNC_SNAPSHOT_DELAY_MS": str(args.delay_ms)}
    common = [bin_path, "--rdma-dev", args.rdma_dev,
              "--repl-transport", "tcp", "--repl-fullsync-transport", "rdma",
              "--repl-realtime-transport", "tcp",
              "--rdma-port", str(args.rdma_port),
              "--rdma-chunk-size", "1024", "--rdma-send-slots", "1",
              "--rdma-fullsync-pause-us", "200"]
    master = slave = None
    rc = 1
    try:
        master = subprocess.Popen(
            common + ["--port", str(args.port), "--role", "master",
                      "--master-host", args.host, "--master-port", str(args.port),
                      "--dump", str(wd / "master.dump"), "--aof", str(wd / "master.aof")],
            cwd=cwd, stdout=mlog, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
            env=env)
        if not wait_ready(args.host, args.port):
            print("FULLSYNC_CM_DEADLINE_FAIL: master not ready")
            return 1
        print(f"[1/4] preload {args.preload} keys")
        preload(args.host, args.port, args.preload)

        print(f"[2/4] start slave (master snapshot delay {args.delay_ms}ms)")
        slave = subprocess.Popen(
            common + ["--port", str(args.port + 2), "--role", "slave",
                      "--master-host", args.host, "--master-port", str(args.port),
                      "--dump", str(wd / "slave.dump"), "--aof", str(wd / "slave.aof")],
            cwd=cwd, stdout=slog, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        if not wait_ready(args.host, args.port + 2):
            print("FULLSYNC_CM_DEADLINE_FAIL: slave not ready")
            return 1

        # 确认测试钩子确实触发（header 尚未发送）
        saw_delay = False
        for _ in range(100):
            if "TEST fullsync snapshot delay" in (wd / "master.log").read_text(errors="ignore"):
                saw_delay = True
                break
            time.sleep(0.1)
        print("snapshot delay hook observed =", saw_delay)

        print("[3/4] wait fullsync completion")
        deadline = time.time() + args.delay_ms / 1000.0 + 60
        d = {}
        while time.time() < deadline:
            d = info(args.host, args.port)
            try:
                completed = int(d.get("rdma_fs_total_completed", "0"))
                failed = int(d.get("rdma_fs_total_failed", "0"))
            except ValueError:
                completed = failed = 0
            if completed >= 1 or failed >= 1:
                break
            time.sleep(0.1)
        for _ in range(100):
            d = info(args.host, args.port)
            if d.get("rdma_fs_active_jobs") == "0":
                break
            time.sleep(0.1)

        print("[4/4] verify all keys on slave")
        bad = verify_all(args.host, args.port + 2, args.preload)
        try:
            completed = int(d.get("rdma_fs_total_completed", "0"))
            failed = int(d.get("rdma_fs_total_failed", "0"))
        except ValueError:
            completed = failed = 0
        print(f"  completed={completed} failed={failed} active_jobs={d.get('rdma_fs_active_jobs')} "
              f"mismatches={bad}")
        if saw_delay and completed >= 1 and failed == 0 and bad == 0:
            print("FULLSYNC_CM_DEADLINE_PASS")
            rc = 0
        else:
            print("FULLSYNC_CM_DEADLINE_FAIL")
        return rc
    finally:
        stop(slave)
        stop(master)
        mlog.close()
        slog.close()
        if rc != 0:
            print("--- master.log tail ---")
            print("\n".join((wd / "master.log").read_text(errors="ignore").splitlines()[-40:]))


if __name__ == "__main__":
    sys.exit(main())
