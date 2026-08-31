#!/usr/bin/env python3
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def build_resp(*args: str) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        b = str(arg).encode()
        out.extend([f"${len(b)}\r\n".encode(), b, b"\r\n"])
    return b"".join(out)


def req(host: str, port: int, *args: str) -> bytes:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3)
    s.connect((host, port))
    s.sendall(build_resp(*args))
    chunks = []
    try:
        while True:
            data = s.recv(4096)
            if not data:
                break
            chunks.append(data)
            if len(data) < 4096:
                break
    except socket.timeout:
        pass
    finally:
        s.close()
    return b"".join(chunks)


def wait_ready(host: str, port: int, retries: int = 60) -> None:
    for _ in range(retries):
        try:
            if req(host, port, "PING").startswith(b"+PONG"):
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server not ready")


def stop_proc(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=1)
        return
    except Exception:
        pass
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=1)
    except Exception:
        pass


def kill_port(port: int) -> None:
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


def parse_info(raw: bytes) -> dict:
    text = raw.decode(errors="ignore")
    out = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def engine_prefix(engine: str) -> str:
    return {
        "array": "",
        "hash": "H",
        "rbtree": "R",
        "skiptable": "X",
    }[engine]


def engine_set_cmd(engine: str) -> str:
    return f"{engine_prefix(engine)}SET"


def engine_get_cmd(engine: str) -> str:
    return f"{engine_prefix(engine)}GET"


def adjust_count(engine: str, count: int) -> int:
    if engine == "array" and count > 1024:
        return 1024
    return count


def main() -> int:
    ap = argparse.ArgumentParser(description="mmap recovery performance benchmark")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5142)
    ap.add_argument("--count", type=int, default=2000)
    ap.add_argument("--engine", choices=["array", "hash", "rbtree", "skiptable"], default="hash")
    ap.add_argument("--appendfsync", choices=["always", "everysec"], default="everysec")
    ap.add_argument("--work-dir", default="./artifacts/persist/mmap-recover")
    args = ap.parse_args()

    args.count = adjust_count(args.engine, args.count)
    set_cmd = engine_set_cmd(args.engine)
    get_cmd = engine_get_cmd(args.engine)

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    dump_path = root / "kvstore.dump"
    aof_path = root / "kvstore.aof"
    log_path = root / "server.log"

    for p in (dump_path, aof_path):
        if p.exists():
            p.unlink()

    kill_port(args.port)
    time.sleep(0.5)

    logf = open(log_path, "ab")
    proc = subprocess.Popen(
        [args.bin, "--port", str(args.port), "--dump", str(dump_path), "--aof", str(aof_path), "--appendfsync", args.appendfsync],
        stdout=logf,
        stderr=logf,
        cwd=str(Path(args.bin).resolve().parent),
        preexec_fn=os.setsid,
    )

    try:
        wait_ready(args.host, args.port)
        for i in range(args.count):
            if req(args.host, args.port, set_cmd, f"mmap:key:{i}", f"value:{i}") != b"+OK\r\n":
                raise RuntimeError(f"SET failed at {i} for engine={args.engine}")
        if req(args.host, args.port, "SAVE") != b"+OK\r\n":
            raise RuntimeError("SAVE failed")
    finally:
        stop_proc(proc)
        logf.close()

    logf = open(log_path, "ab")
    start = time.perf_counter()
    proc = subprocess.Popen(
        [args.bin, "--port", str(args.port), "--dump", str(dump_path), "--aof", str(aof_path), "--appendfsync", args.appendfsync],
        stdout=logf,
        stderr=logf,
        cwd=str(Path(args.bin).resolve().parent),
        preexec_fn=os.setsid,
    )
    try:
        wait_ready(args.host, args.port)
        wall = time.perf_counter() - start
        info = parse_info(req(args.host, args.port, "INFO"))
        probe = req(args.host, args.port, get_cmd, f"mmap:key:{args.count - 1}")
        if f"value:{args.count - 1}".encode() not in probe:
            raise RuntimeError(f"recovery probe failed: {probe!r}")

        print("MMAP_RECOVER_BENCH_PASS")
        print(f"engine={args.engine}")
        print(f"count={args.count}")
        print(f"appendfsync={args.appendfsync}")
        print(f"restart_wall_seconds={wall:.6f}")
        for key in (
            "recover_total_ms",
            "recover_dump_ms",
            "recover_aof_ms",
            "recover_mmap_attempts",
            "recover_mmap_success",
            "recover_mmap_fallbacks",
            "recover_mmap_bytes",
            "recover_fread_bytes",
            "recover_tail_bytes",
        ):
            print(f"{key}={info.get(key, 'missing')}")
        print(f"dump_size_bytes={dump_path.stat().st_size if dump_path.exists() else -1}")
        print(f"aof_size_bytes={aof_path.stat().st_size if aof_path.exists() else -1}")
        print(f"artifacts={root}")
        return 0
    finally:
        stop_proc(proc)
        logf.close()


if __name__ == "__main__":
    sys.exit(main())
