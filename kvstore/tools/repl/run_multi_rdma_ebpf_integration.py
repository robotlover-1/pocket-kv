#!/usr/bin/env python3
"""
eBPF/Commit Gate + 多 Slave 并发 RDMA 全量同步综合测试（需要 root + eBPF）。

场景：
  1. Master: fullsync=rdma + realtime=ebpf+tcp + eBPF capture/proxy。
  2. Slave A/B 同时 FULLRESYNC；全量期间持续写 SET/MSET/DEL/同长度覆盖。
  3. 两个 job 都进入 RDMA_SENDING 且已 post WR 时 kill -9 B；
     A 必须完成全量、继续接收增量、最终 applied==master。
  4. 重启 B（读取本地 dump/AOF），等待 partial/full 追平后再次全量校验。
  5. 校验 repl_commit_gap/desync/timeout、session 有效性、资源归零、
     全部 preload + 写入 key 一致。

无 root、无 eBPF 产物或无 RDMA 设备时 SKIP（--require 时 FAIL）。
"""
import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2]
BUILD = SRC_ROOT / "build"


def enc(*args):
    """RESP 编码；同时接受 str 与 bytes（bytes 不能再被 repr 成 b'...'）。"""
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a.encode() if isinstance(a, str) else bytes(a)
        out.append(b"$%d\r\n" % len(b) + b + b"\r\n")
    return b"".join(out)


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
    if t == b"*":
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


def wait_ready(host, port, role, tries=200):
    for _ in range(tries):
        try:
            r = req(host, port, "ROLE")
            if role.encode() in r:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def preload(host, port, count, batch=200):
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


def get_values(host, port, keys, batch=1000):
    """批量 HGET，返回 {key: value|None}。"""
    out = {}
    s = socket.create_connection((host, port), timeout=30)
    buf = [b""]
    try:
        for start in range(0, len(keys), batch):
            part = keys[start:start + batch]
            payload = []
            for k in part:
                payload.append(f"*2\r\n$4\r\nHGET\r\n${len(k)}\r\n{k}\r\n")
            s.sendall("".join(payload).encode())
            for k in part:
                item, _ = _read_item(s, buf)
                try:
                    head, rest = item.split(b"\r\n", 1)
                    ln = int(head[1:])
                except Exception:
                    out[k] = b"<parse-error>"
                    continue
                out[k] = rest[:-2] if ln >= 0 else None
    finally:
        s.close()
    return out


def verify_map(host, port, expected):
    got = get_values(host, port, list(expected.keys()))
    bad = 0
    first = None
    for k, want in expected.items():
        if got.get(k) != want:
            bad += 1
            if first is None:
                first = (k, got.get(k), want)
    return bad, first


class Writer(threading.Thread):
    """持续写入 Master，模拟全量期间的增量流量。"""

    def __init__(self, host, port, duration, sleep_ms=5):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.duration = duration
        self.sleep_ms = sleep_ms
        self.stop_evt = threading.Event()
        self.expected = {}
        self.commands = 0
        self.error = None
        self._sock = None

    def _send(self, cmds):
        payload = b"".join(enc(*c) for c in cmds)
        self._sock.sendall(payload)
        buf = self._buf
        for _ in cmds:
            _read_item(self._sock, buf)

    def run(self):
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=30)
            self._buf = [b""]
            deadline = time.time() + self.duration
            n = 0
            long_val = b"L" * 1000
            while not self.stop_evt.is_set() and time.time() < deadline:
                cmds = []
                batch_expected = {}
                # 同长度覆盖
                v64 = (f"over:{n:06d}".encode() * 4)[:64]
                cmds.append(("HSET", "k:over", v64))
                batch_expected["k:over"] = v64
                # 边界长度
                cmds.append(("HSET", "k:long", long_val))
                batch_expected["k:long"] = long_val
                cmds.append(("HSET", "k:tiny", b"x"))
                batch_expected["k:tiny"] = b"x"
                # MSET
                a = f"a{n}".encode()
                b = f"b{n}".encode()
                cmds.append(("HMSET", "k:ms:a", a, "k:ms:b", b))
                batch_expected["k:ms:a"] = a
                batch_expected["k:ms:b"] = b
                # 非幂等 DEL：先 SET 再 DEL
                dk = f"k:del:{n}"
                cmds.append(("HSET", dk, b"to-delete"))
                cmds.append(("HDEL", dk))
                batch_expected[dk] = None
                # 唯一 key
                for j in range(5):
                    wk = f"k:w:{n}:{j}"
                    wv = f"w{n}-{j}".encode()
                    cmds.append(("HSET", wk, wv))
                    batch_expected[wk] = wv
                self._send(cmds)
                # 只有全部响应返回后才记录期望值：出错/停止时不会把未确认的写当已应用
                self.expected.update(batch_expected)
                self.commands += len(cmds)
                n += 1
                if self.sleep_ms > 0:
                    time.sleep(self.sleep_ms / 1000.0)
        except Exception as e:  # noqa: BLE001
            if not self.stop_evt.is_set():
                self.error = repr(e)

    def stop(self):
        self.stop_evt.set()

    def close(self):
        """测试结束/校验完成后再关闭客户端 socket，避免 FORGET 丢弃尾部提交。"""
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


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


def wait_applied(host, master_port, slave_port, timeout=90):
    last = {}
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = info(host, master_port)
        s = info(host, slave_port)
        try:
            mo = int(m.get("master_repl_offset", "-1"))
            ap = int(s.get("slave_repl_applied_offset", "-1"))
            loading = int(s.get("slave_fullsync_loading", "1"))
        except ValueError:
            mo = ap = -1
            loading = 1
        last = {"master": mo, "applied": ap, "loading": loading}
        if mo >= 0 and ap == mo and loading == 0:
            return True, last
        time.sleep(0.1)
    return False, last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default=os.environ.get("REPL_RDMA_SMOKE_HOST", "192.168.233.128"))
    ap.add_argument("--master-port", type=int, default=5570)
    ap.add_argument("--slave-a-port", type=int, default=5572)
    ap.add_argument("--slave-b-port", type=int, default=5574)
    ap.add_argument("--proxy-a-port", type=int, default=5672)
    ap.add_argument("--proxy-b-port", type=int, default=5674)
    ap.add_argument("--rdma-dev", default=os.environ.get("RDMA_DEV", "siw0"))
    ap.add_argument("--rdma-port", type=int, default=0)
    ap.add_argument("--preload", type=int, default=30000)
    ap.add_argument("--write-seconds", type=float, default=6.0)
    ap.add_argument("--writer-sleep-ms", type=float, default=5.0)
    ap.add_argument("--wait-seconds", type=float, default=120.0)
    ap.add_argument("--require", action="store_true")
    ap.add_argument("--work-dir", default="./artifacts/repl/multi-rdma-ebpf")
    args = ap.parse_args()

    if os.geteuid() != 0:
        print("MULTI_RDMA_EBPF_SKIP: requires root (eBPF)")
        return 1 if args.require else 0
    if not Path(f"/sys/class/infiniband/{args.rdma_dev}").exists():
        print("MULTI_RDMA_EBPF_SKIP: RDMA device missing")
        return 1 if args.require else 0
    proxy_bin = BUILD / "ebpf_proxy"
    capture_obj = BUILD / "replication/bpf/repl_client_capture.bpf.o"
    sockmap_obj = BUILD / "replication/bpf/repl_sockmap.bpf.o"
    if not proxy_bin.exists() or not capture_obj.exists() or not sockmap_obj.exists():
        print("MULTI_RDMA_EBPF_SKIP: missing eBPF build artifacts")
        return 1 if args.require else 0
    if args.rdma_port <= 0:
        args.rdma_port = args.master_port + 20

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(bin_path).parent)
    wd = Path(args.work_dir).resolve()
    shutil.rmtree(wd, ignore_errors=True)
    wd.mkdir(parents=True, exist_ok=True)
    pin = "/sys/fs/bpf/kvs-multi-rdma-ebpf"
    shutil.rmtree(pin, ignore_errors=True)
    os.makedirs(pin, exist_ok=True)

    common = [bin_path, "--rdma-dev", args.rdma_dev,
              "--repl-transport", "tcp", "--repl-fullsync-transport", "rdma",
              "--repl-realtime-transport", "ebpf+tcp",
              "--rdma-port", str(args.rdma_port),
              "--rdma-chunk-size", "1024", "--rdma-send-slots", "1",
              "--rdma-fullsync-pause-us", "2000",
              "--rdma-fullsync-max-connected", "4",
              "--net", "reactor"]
    master_args = common + ["--port", str(args.master_port), "--role", "master",
                            "--master-host", args.host, "--master-port", str(args.master_port),
                            "--dump", str(wd / "master.dump"),
                            "--aof", str(wd / "master.aof"),
                            "--ebpf-pin-path", pin,
                            "--ebpf-proxy-bin", str(proxy_bin),
                            "--ebpf-client-capture-obj", str(capture_obj)]

    def slave_args(name, port, proxy_port):
        return (common + ["--port", str(port), "--role", "slave",
                          "--master-host", args.host, "--master-port", str(args.master_port),
                          "--dump", str(wd / f"{name}.dump"),
                          "--aof", str(wd / f"{name}.aof"),
                          "--repl-proxy-port", str(proxy_port)])

    def log(name):
        return open(wd / f"{name}.log", "wb")

    master = slave_a = slave_b = None
    mlog = alog = blog = None
    writer = None
    rc = 1
    try:
        mlog = log("master")
        master = subprocess.Popen(master_args, cwd=cwd, stdout=mlog,
                                  stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        if not wait_ready(args.host, args.master_port, "master"):
            print("MULTI_RDMA_EBPF_FAIL: master not ready")
            return 1
        # 等 ebpf-proxy 把 client_ctl/捕获 map pin 好，避免 REPLSYNC slot 注册失败
        for _ in range(200):
            if Path(pin, "client_ctl").exists():
                break
            time.sleep(0.1)
        if not Path(pin, "client_ctl").exists():
            print("MULTI_RDMA_EBPF_FAIL: ebpf-proxy pin maps not ready")
            return 1
        print(f"[1/7] preload {args.preload} keys")
        preload(args.host, args.master_port, args.preload)

        alog = log("slave-a")
        blog = log("slave-b")
        print("[2/7] start slaves A/B (fullsync=rdma, realtime=ebpf+tcp)")
        slave_a = subprocess.Popen(slave_args("a", args.slave_a_port, args.proxy_a_port),
                                   cwd=cwd, stdout=alog, stderr=subprocess.STDOUT,
                                   preexec_fn=os.setsid)
        slave_b = subprocess.Popen(slave_args("b", args.slave_b_port, args.proxy_b_port),
                                   cwd=cwd, stdout=blog, stderr=subprocess.STDOUT,
                                   preexec_fn=os.setsid)
        if not wait_ready(args.host, args.slave_a_port, "slave") or \
           not wait_ready(args.host, args.slave_b_port, "slave"):
            print("MULTI_RDMA_EBPF_FAIL: slave not ready")
            return 1

        print("[3/7] start concurrent writer during fullsync")
        if args.write_seconds > 0:
            writer = Writer(args.host, args.master_port, args.write_seconds,
                            args.writer_sleep_ms)
            writer.start()
        else:
            print("  writer disabled (write_seconds=0)")

        print("[4/7] wait both jobs posted WR, then kill -9 B")
        killed = False
        deadline = time.time() + args.wait_seconds
        jobs_snapshot = ""
        while time.time() < deadline:
            d = info(args.host, args.master_port)
            try:
                posted = int(d.get("rdma_fs_sending_posted_jobs", "0") or 0)
            except ValueError:
                posted = 0
            if d.get("rdma_fs_jobs"):
                jobs_snapshot = d.get("rdma_fs_jobs")
            if posted >= 2:
                print("  in-flight jobs:", jobs_snapshot)
                slave_b.send_signal(signal.SIGKILL)
                killed = True
                print("  killed Slave B pid", slave_b.pid)
                break
            time.sleep(0.02)
        if not killed:
            print("MULTI_RDMA_EBPF_FAIL: could not observe two jobs with posted WR")
            return 1

        print("[5/7] A continues fullsync + incremental")
        ok_a, last_a = wait_applied(args.host, args.master_port, args.slave_a_port,
                                    timeout=args.wait_seconds)
        if writer:
            writer.stop()
            writer.join(timeout=10)
            if writer.error:
                print("  writer error:", writer.error)
        if not ok_a:
            print("MULTI_RDMA_EBPF_FAIL: A did not converge:", last_a)

        # 保证 writer 的增量在 A 上追平
        ok_a, last_a = wait_applied(args.host, args.master_port, args.slave_a_port,
                                    timeout=args.wait_seconds)
        print(f"  A applied={last_a} writer_cmds={writer.commands if writer else 0}")

        m = info(args.host, args.master_port)
        session_ok = m.get("repl_session_valid") == "1"
        gap = m.get("repl_commit_gap", "?")
        desync = m.get("repl_commit_desync", "?")
        timeout = m.get("repl_commit_timeout", "?")

        expected_pre = {f"k:{i}": f"v{i}".encode() for i in range(args.preload)}
        expected = dict(expected_pre)
        if writer:
            expected.update(writer.expected)
        bad_a, first_a = verify_map(args.host, args.slave_a_port, expected)
        print(f"  A full verify: bad={bad_a} first={first_a}")

        print("[6/7] restart B and wait catch-up")
        stop(slave_b)
        blog.close()
        blog = log("slave-b-restart")
        slave_b = subprocess.Popen(slave_args("b", args.slave_b_port, args.proxy_b_port),
                                   cwd=cwd, stdout=blog, stderr=subprocess.STDOUT,
                                   preexec_fn=os.setsid)
        if not wait_ready(args.host, args.slave_b_port, "slave"):
            print("MULTI_RDMA_EBPF_FAIL: restarted B not ready")
            return 1
        ok_b, last_b = wait_applied(args.host, args.master_port, args.slave_b_port,
                                    timeout=args.wait_seconds)
        bad_b, first_b = verify_map(args.host, args.slave_b_port, expected)
        print(f"  B applied={last_b} bad={bad_b} first={first_b}")

        print("[7/7] final checks")
        d = {}
        for _ in range(100):
            d = info(args.host, args.master_port)
            if d.get("rdma_fs_active_jobs") == "0":
                break
            time.sleep(0.1)
        leftovers = sorted(p.name for p in wd.glob("*fullsync.tmp*"))
        m = info(args.host, args.master_port)
        print(f"  master: completed={m.get('rdma_fs_total_completed')} "
              f"failed={m.get('rdma_fs_total_failed')} active={m.get('rdma_fs_active_jobs')} "
              f"session_valid={m.get('repl_session_valid')} "
              f"gap={m.get('repl_commit_gap')} desync={m.get('repl_commit_desync')} "
              f"timeout={m.get('repl_commit_timeout')}")
        print(f"  A_converged={ok_a} B_converged={ok_b} bad_a={bad_a} bad_b={bad_b} "
              f"leftovers={leftovers}")

        ok = (killed and ok_a and ok_b and bad_a == 0 and bad_b == 0
              and session_ok and gap == "0" and desync == "0" and timeout == "0"
              and d.get("rdma_fs_active_jobs") == "0" and not leftovers
              and master.poll() is None)
        if ok:
            print("MULTI_RDMA_EBPF_PASS")
            rc = 0
        else:
            print("MULTI_RDMA_EBPF_FAIL")
        return rc
    finally:
        if writer:
            writer.stop()
            writer.close()
        stop(slave_a)
        stop(slave_b)
        stop(master)
        for f in (mlog, alog, blog):
            if f:
                f.close()
        if rc != 0:
            for name in ("master", "slave-a", "slave-b", "slave-b-restart"):
                p = wd / f"{name}.log"
                if p.exists():
                    print(f"--- {name}.log tail ---")
                    print("\n".join(p.read_text(errors="ignore").splitlines()[-40:]))


if __name__ == "__main__":
    sys.exit(main())
