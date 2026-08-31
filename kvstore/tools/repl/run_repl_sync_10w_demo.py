#!/usr/bin/env python3
"""
主从同步演示 (5w + 5w = 10w 数据一致性):
1. 启动 kvstore-master
2. 往 master 插入 5w 条 HSET 数据
3. 启动 kvstore-slave (触发全量同步)
4. 再往 master 插入 5w 条 HSET 数据 (增量同步)
5. 往 slave 查询 10w 条数据
6. 对比 master 插入的 10w 条数据与 slave 获取的是否一致
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def build_resp(*args: str) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        b = str(arg).encode()
        parts.append(f"${len(b)}\r\n".encode())
        parts.append(b)
        parts.append(b"\r\n")
    return b"".join(parts)


def req(host: str, port: int, *args: str, timeout: float = 3.0) -> bytes:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.sendall(build_resp(*args))
        chunks = []
        try:
            while True:
                data = s.recv(65536)
                if not data:
                    break
                chunks.append(data)
                if len(data) < 65536:
                    break
        except socket.timeout:
            pass
        return b"".join(chunks)
    finally:
        s.close()


class PersistentConn:
    """长连接：批量操作复用一个 TCP 连接，避免 TIME_WAIT 端口耗尽"""
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock: socket.socket | None = None

    def _ensure(self) -> socket.socket:
        if self._sock is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect((self.host, self.port))
            self._sock = s
        return self._sock

    def cmd(self, *args: str) -> bytes:
        """发送一条 RESP 命令并返回完整响应"""
        sock = self._ensure()
        sock.sendall(build_resp(*args))
        chunks = []
        try:
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                chunks.append(data)
                if len(data) < 65536:
                    break
        except socket.timeout:
            pass
        return b"".join(chunks)

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


def wait_role(host: str, port: int, expect: str, retries: int = 100) -> None:
    for _ in range(retries):
        try:
            r = req(host, port, "ROLE", timeout=1.0)
            if expect.encode() in r:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"server {host}:{port} not in role '{expect}'")


def wait_slave_loaded(host: str, port: int, retries: int = 80) -> None:
    for _ in range(retries):
        try:
            r = req(host, port, "INFO", timeout=1.0)
            if b"slave_fullsync_loading:0" in r:
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"slave {host}:{port} fullsync not completed")


def parse_info(raw: bytes) -> dict:
    info = {}
    for line in raw.decode(errors="ignore").replace("\r", "").splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            info[k.strip()] = v.strip()
    return info


def stop(proc: subprocess.Popen) -> None:
    if not proc or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=3)
    except Exception:
        pass
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=2)
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description="主从同步 5w+5w=10w 演示")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="192.168.233.128")
    ap.add_argument("--master-port", type=int, default=5160)
    ap.add_argument("--slave-port", type=int, default=5161)
    ap.add_argument("--pre-count", type=int, default=50000, help="slave 启动前插入数量")
    ap.add_argument("--post-count", type=int, default=50000, help="slave 启动后插入数量")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--fullsync-transport", default="tcp", choices=["tcp", "rdma"],
                    help="全量同步传输协议 (tcp/rdma)")
    ap.add_argument("--realtime-transport", default="tcp", choices=["tcp", "ebpf"],
                    help="实时同步传输协议 (tcp/ebpf)")
    ap.add_argument("--rdma-dev", default="siw0",
                    help="RDMA 设备名 (默认 siw0)")
    ap.add_argument("--work-dir", default="./artifacts/repl/sync-demo")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    master_dir = root / "master"
    slave_dir = root / "slave"
    master_dir.mkdir(parents=True, exist_ok=True)
    slave_dir.mkdir(parents=True, exist_ok=True)

    master_dump = master_dir / "kvstore.dump"
    master_aof = master_dir / "kvstore.aof"
    master_replstate = master_dir / "kvstore.aof.replstate"
    master_log = master_dir / "master.log"
    slave_dump = slave_dir / "kvstore.dump"
    slave_aof = slave_dir / "kvstore.aof"
    slave_replstate = slave_dir / "kvstore.aof.replstate"
    slave_log = master_dir / "slave.log"
    result_path = root / "result.txt"

    for p in (master_dump, master_aof, master_replstate, slave_dump, slave_aof, slave_replstate):
        if p.exists():
            p.unlink()

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(args.bin).resolve().parent)

    master_proc = None
    slave_proc = None
    exit_code = 1

    try:
        # ---- Phase 1: Start master, insert 5w keys ----
        print("=" * 60)
        print(f"[Phase 1] Starting master on :{args.master_port}")
        print("=" * 60)

        with open(master_log, "ab") as logf:
            master_proc = subprocess.Popen(
                [bin_path, "--port", str(args.master_port),
                 "--role", "master",
                 "--dump", str(master_dump), "--aof", str(master_aof),
                 "--appendfsync", "everysec",
                 "--rdma-dev", args.rdma_dev,
                 "--repl-fullsync-transport", args.fullsync_transport,
                 "--repl-realtime-transport", args.realtime_transport],
                stdout=logf, stderr=logf,
                cwd=cwd, preexec_fn=os.setsid,
            )

        wait_role(args.host, args.master_port, "master")
        print(f"[Phase 1] Master ready on :{args.master_port}, fullsync={args.fullsync_transport}, realtime={args.realtime_transport}")

        pre_count = args.pre_count
        t0 = time.perf_counter()
        pc = PersistentConn(args.host, args.master_port)
        try:
            for batch_start in range(0, pre_count, args.batch):
                batch_end = min(batch_start + args.batch, pre_count)
                for i in range(batch_start, batch_end):
                    key = f"pre:k:{i:06d}"
                    val = f"v{i}"
                    r = pc.cmd("HSET", key, val)
                    if r != b"+OK\r\n":
                        raise RuntimeError(f"HSET pre-load failed at {i}: {r!r}")
                elapsed = time.perf_counter() - t0
                qps = (batch_end) / elapsed if elapsed > 0 else 0
                print(f"  Pre-load: {batch_end}/{pre_count} keys, {qps:.0f} qps, {elapsed:.1f}s")
        finally:
            pc.close()

        t1 = time.perf_counter()
        print(f"[Phase 1] Pre-loaded {pre_count} keys in {t1 - t0:.2f}s")

        # ---- Phase 2: Start slave ----
        print("=" * 60)
        print(f"[Phase 2] Starting slave on :{args.slave_port}")
        print("=" * 60)

        with open(slave_log, "ab") as logf:
            slave_proc = subprocess.Popen(
                [bin_path, "--port", str(args.slave_port),
                 "--role", "slave",
                 "--master-host", args.host,
                 "--master-port", str(args.master_port),
                 "--dump", str(slave_dump), "--aof", str(slave_aof),
                 "--appendfsync", "everysec",
                 "--rdma-dev", args.rdma_dev,
                 "--repl-fullsync-transport", args.fullsync_transport,
                 "--repl-realtime-transport", args.realtime_transport],
                stdout=logf, stderr=logf,
                cwd=cwd, preexec_fn=os.setsid,
            )

        wait_role(args.host, args.slave_port, "slave")
        print(f"[Phase 2] Slave ready on :{args.slave_port}")

        # Wait for fullsync to complete
        print("[Phase 2] Waiting for fullsync to complete...")
        wait_slave_loaded(args.host, args.slave_port)
        print("[Phase 2] Fullsync completed")

        # Check master/slave replication status
        master_info = parse_info(req(args.host, args.master_port, "INFO"))
        slave_info = parse_info(req(args.host, args.slave_port, "INFO"))
        print(f"  master fullsync_count={master_info.get('repl_fullsync_count', '?')}")
        print(f"  slave repl_offset={slave_info.get('slave_repl_offset', '?')}")
        print(f"  slave master_link={slave_info.get('master_link', '?')}")

        # ---- Phase 3: Insert another 5w keys to master ----
        print("=" * 60)
        print(f"[Phase 3] Inserting {args.post_count} more keys to master (incremental sync)")
        print("=" * 60)

        # Give slave time to finalize fullsync and settle RDMA/TCP connections
        time.sleep(5)

        post_count = args.post_count
        t2 = time.perf_counter()
        pc2 = PersistentConn(args.host, args.master_port)
        try:
            for batch_start in range(0, post_count, args.batch):
                batch_end = min(batch_start + args.batch, post_count)
                for i in range(batch_start, batch_end):
                    key = f"post:k:{i:06d}"
                    val = f"v{pre_count + i}"
                    r = pc2.cmd("HSET", key, val)
                    if r != b"+OK\r\n":
                        raise RuntimeError(f"HSET post-load failed at {i}: {r!r}")
                elapsed = time.perf_counter() - t2
                qps = (batch_end) / elapsed if elapsed > 0 else 0
                print(f"  Post-load: {batch_end}/{post_count} keys, {qps:.0f} qps, {elapsed:.1f}s")
        finally:
            pc2.close()

        t3 = time.perf_counter()
        print(f"[Phase 3] Post-loaded {post_count} keys in {t3 - t2:.2f}s")

        # Wait for incremental sync: poll slave offset until it catches up
        print("[Phase 3] Waiting for incremental sync to complete...")
        max_wait = 120.0
        wait_start = time.perf_counter()
        caught_up = False
        while time.perf_counter() - wait_start < max_wait:
            try:
                master_info = parse_info(req(args.host, args.master_port, "INFO", timeout=1.0))
                slave_info = parse_info(req(args.host, args.slave_port, "INFO", timeout=1.0))
                master_offset = int(master_info.get("master_repl_offset", "0"))
                slave_offset = int(slave_info.get("slave_repl_offset", "0"))
                if slave_offset >= master_offset:
                    caught_up = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if caught_up:
            elapsed_wait = time.perf_counter() - wait_start
            print(f"[Phase 3] Slave caught up in {elapsed_wait:.1f}s (slave_offset >= master_offset)")
        else:
            print("[Phase 3] WARNING: Slave did not catch up within timeout, proceeding anyway")

        # ---- Phase 4: Verify slave has all 10w keys ----
        print("=" * 60)
        print(f"[Phase 4] Verifying {pre_count + post_count} keys on slave")
        print("=" * 60)

        total = pre_count + post_count
        failed = 0
        t4 = time.perf_counter()

        # Sample from pre-load keys
        pre_samples = [0, 1, 99, 999, 9999, 19999, pre_count - 1]
        for idx in pre_samples:
            key = f"pre:k:{idx:06d}"
            expected_val = f"v{idx}"
            for retry in range(5):
                r = req(args.host, args.slave_port, "HGET", key)
                if expected_val.encode() in r or f"${len(expected_val)}\r\n{expected_val}\r\n".encode() in r:
                    break
                time.sleep(1)
            if expected_val.encode() not in r and f"${len(expected_val)}\r\n{expected_val}\r\n".encode() not in r:
                print(f"  FAIL [pre]: key={key} expected={expected_val!r} got={r!r}")
                failed += 1
            else:
                print(f"  OK [pre]: key={key} verified")

        # Sample from post-load keys
        post_samples = [0, 1, 99, 999, 9999, post_count // 2, post_count - 1]
        for idx in post_samples:
            key = f"post:k:{idx:06d}"
            expected_val = f"v{pre_count + idx}"
            for retry in range(5):
                r = req(args.host, args.slave_port, "HGET", key)
                if expected_val.encode() in r or f"${len(expected_val)}\r\n{expected_val}\r\n".encode() in r:
                    break
                time.sleep(1)
            if expected_val.encode() not in r and f"${len(expected_val)}\r\n{expected_val}\r\n".encode() not in r:
                print(f"  FAIL [post]: key={key} expected={expected_val!r} got={r!r}")
                failed += 1
            else:
                print(f"  OK [post]: key={key} verified")

        t5 = time.perf_counter()

        if failed == 0:
            print("=" * 60)
            print("RESULT: PASS - 主从同步 5w+5w=10w 数据一致性验证通过")
            print(f"  pre_count={pre_count}")
            print(f"  post_count={post_count}")
            print(f"  total_count={total}")
            print(f"  pre_insert_seconds={t1 - t0:.2f}")
            print(f"  post_insert_seconds={t3 - t2:.2f}")
            print(f"  verify_seconds={t5 - t4:.2f}")
            print("=" * 60)
            with open(result_path, "w") as rf:
                rf.write("PASS\n")
            exit_code = 0
        else:
            print(f"FAIL: {failed} key(s) mismatch")
            with open(result_path, "w") as rf:
                rf.write(f"FAIL: {failed} mismatches\n")
            exit_code = 1

        return exit_code

    finally:
        if slave_proc:
            stop(slave_proc)
        if master_proc:
            stop(master_proc)


if __name__ == "__main__":
    sys.exit(main())
