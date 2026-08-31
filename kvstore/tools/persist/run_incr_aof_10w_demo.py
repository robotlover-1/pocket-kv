#!/usr/bin/env python3
"""
增量持久化演示 (10w 条数据, AOF):
1. 启动 kvstore (AOF everysec)
2. 插入 10w 条 HSET 数据
3. 等待 AOF fsync 完成
4. 强制杀死 kvstore (模拟宕机)
5. 重新启动 kvstore (从 AOF 恢复)
6. 验证 10w 条数据的采样一致性
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


def wait_ready(host: str, port: int, retries: int = 80) -> None:
    for _ in range(retries):
        try:
            r = req(host, port, "PING", timeout=1.0)
            if r.startswith(b"+PONG"):
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"server {host}:{port} not ready")


def kill_port(port: int) -> None:
    """Kill any process listening on the given port to avoid stale processes
    hijacking the test and writing AOF/dump to wrong paths."""
    import shutil
    lsof = shutil.which("lsof")
    if not lsof:
        return
    try:
        result = subprocess.run(
            [lsof, "-ti", f":{port}"],
            capture_output=True, text=True, timeout=5)
        for pid_str in result.stdout.strip().splitlines():
            pid = int(pid_str.strip())
            if pid <= 1:
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    except Exception:
        pass


def stop_kill(proc: subprocess.Popen) -> None:
    if not proc or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=2)
    except Exception:
        pass


def main() -> int:
    ap = argparse.ArgumentParser(description="增量持久化 10w 数据演示 (AOF)")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5152)
    ap.add_argument("--count", type=int, default=100000)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--work-dir", default="./artifacts/persist/incr-aof-demo")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    dump_path = root / "kvstore.dump"
    aof_path = root / "kvstore.aof"
    log_path = root / "server.log"
    result_path = root / "result.txt"

    for p in (dump_path, aof_path):
        if p.exists():
            p.unlink()

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(args.bin).resolve().parent)

    # Clean up any leftover process on the target port
    kill_port(args.port)
    time.sleep(0.5)

    # ---- Phase 1: Start server, insert 10w keys (AOF only, no SAVE) ----
    print("=" * 60)
    print(f"[Phase 1] Starting kvstore on :{args.port} (AOF everysec)")
    print("=" * 60)

    with open(log_path, "ab") as logf:
        proc = subprocess.Popen(
            [bin_path, "--port", str(args.port),
             "--dump", str(dump_path), "--aof", str(aof_path),
             "--appendfsync", "everysec"],
            stdout=logf, stderr=logf,
            cwd=cwd, preexec_fn=os.setsid,
        )

    try:
        wait_ready(args.host, args.port)
        print(f"[Phase 1] Server ready on :{args.port}")

        total = args.count
        t0 = time.perf_counter()
        for batch_start in range(0, total, args.batch):
            batch_end = min(batch_start + args.batch, total)
            for i in range(batch_start, batch_end):
                key = f"iaof:k:{i:06d}"
                val = f"v{i}"
                r = req(args.host, args.port, "HSET", key, val)
                if r != b"+OK\r\n":
                    raise RuntimeError(f"HSET failed at {i}: {r!r}")
            elapsed = time.perf_counter() - t0
            qps = (batch_end) / elapsed if elapsed > 0 else 0
            print(f"  Inserted {batch_end}/{total} keys, {qps:.0f} qps, {elapsed:.1f}s")

        t1 = time.perf_counter()
        print(f"[Phase 1] Inserted {total} keys in {t1 - t0:.2f}s")

        # Wait for AOF to be flushed (everysec policy + buffer)
        print("[Phase 1] Waiting 2s for AOF to be flushed...")
        time.sleep(2)

        aof_size = aof_path.stat().st_size if aof_path.exists() else -1
        dump_exists = dump_path.exists()
        print(f"[Phase 1] AOF file size: {aof_size} bytes, dump exists: {dump_exists}")

        # ---- Phase 2: Crash (SIGKILL) ----
        print("=" * 60)
        print("[Phase 2] Simulating crash (SIGKILL)")
        print("=" * 60)
        stop_kill(proc)
        time.sleep(0.5)
        print("[Phase 2] Server killed")

        # ---- Phase 3: Restart, recover from AOF, verify ----
        print("=" * 60)
        print("[Phase 3] Restarting kvstore, recovering from AOF")
        print("=" * 60)

        with open(log_path, "ab") as logf:
            proc = subprocess.Popen(
                [bin_path, "--port", str(args.port),
                 "--dump", str(dump_path), "--aof", str(aof_path),
                 "--appendfsync", "everysec"],
                stdout=logf, stderr=logf,
                cwd=cwd, preexec_fn=os.setsid,
            )

        try:
            t2 = time.perf_counter()
            wait_ready(args.host, args.port)
            t3 = time.perf_counter()
            print(f"[Phase 3] Server restarted in {t3 - t2:.2f}s")

            failed = 0
            sample_positions = [
                0, 1, 99, 999, 9999, 19999, 49999,
                total // 2, total - 100, total - 10, total - 1
            ]
            t4 = time.perf_counter()
            for idx in sample_positions:
                key = f"iaof:k:{idx:06d}"
                expected_val = f"v{idx}"
                r = req(args.host, args.port, "HGET", key)
                if expected_val.encode() not in r and f"${len(expected_val)}\r\n{expected_val}\r\n".encode() not in r:
                    print(f"  FAIL: key={key} expected={expected_val!r} got={r!r}")
                    failed += 1
                else:
                    print(f"  OK: key={key} verified")
            t5 = time.perf_counter()

            if failed == 0:
                print("=" * 60)
                print("RESULT: PASS - 增量持久化 10w 数据验证通过 (AOF)")
                print(f"  data_count={total}")
                print(f"  aof_size_bytes={aof_size}")
                print(f"  insert_seconds={t1 - t0:.2f}")
                print(f"  restart_seconds={t3 - t2:.2f}")
                print(f"  verify_seconds={t5 - t4:.2f}")
                print("=" * 60)
                with open(result_path, "w") as rf:
                    rf.write("PASS\n")
                return 0
            else:
                print(f"FAIL: {failed} key(s) mismatch")
                with open(result_path, "w") as rf:
                    rf.write(f"FAIL: {failed} mismatches\n")
                return 1
        finally:
            stop_kill(proc)
    finally:
        try:
            stop_kill(proc)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
