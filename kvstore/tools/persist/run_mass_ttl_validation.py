#!/usr/bin/env python3
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


def build_resp(*args: str) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        b = str(arg).encode()
        out.append(f"${len(b)}\r\n".encode())
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


def send_cmd(host: str, port: int, *args: str) -> bytes:
    payload = build_resp(*args)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    sock.connect((host, port))
    sock.sendall(payload)
    chunks = []
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                break
            chunks.append(data)
            if len(data) < 4096:
                break
    except socket.timeout:
        pass
    finally:
        sock.close()
    return b"".join(chunks)


def wait_ready(host: str, port: int, retries: int = 40) -> None:
    for _ in range(retries):
        try:
            resp = send_cmd(host, port, "PING")
            if resp.startswith(b"+PONG"):
                return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("server not ready")


def parse_integer(resp: bytes) -> int:
    if not resp.startswith(b":") or not resp.endswith(b"\r\n"):
        raise ValueError(f"unexpected integer resp: {resp!r}")
    return int(resp[1:-2])


def batched_range(total: int, batch: int) -> Iterable[range]:
    start = 0
    while start < total:
        end = min(start + batch, total)
        yield range(start, end)
        start = end


def main() -> int:
    ap = argparse.ArgumentParser(description="Massive TTL regression / stress validation")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--keys", type=int, default=10000)
    ap.add_argument("--ttl", type=int, default=2, help="TTL seconds for each key")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--sample", type=int, default=20, help="sample keys to validate before/after expire")
    ap.add_argument("--prefix", default="mass_ttl")
    ap.add_argument("--engine-prefix", default="H", choices=["", "R", "H", "X"], help="command engine prefix, default H for hash engine")
    ap.add_argument("--work-dir", default="./artifacts/persist/mass-ttl")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    dump_path = root / "kvstore.dump"
    aof_path = root / "kvstore.aof"
    log_path = root / "server.log"

    for p in (dump_path, aof_path):
        if p.exists():
            p.unlink()

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(args.bin).resolve().parent)

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

        set_cmd = f"{args.engine_prefix}SET"
        get_cmd = f"{args.engine_prefix}GET"
        expire_cmd = f"{args.engine_prefix}EXPIRE"
        ttl_cmd = f"{args.engine_prefix}TTL"

        print(f"[1] loading {args.keys} keys with ttl={args.ttl}s (SET+EXPIRE per key)")
        started = time.time()
        for batch_range in batched_range(args.keys, args.batch):
            for i in batch_range:
                key = f"{args.prefix}:{i:08d}"
                val = f"v{i}"
                r1 = send_cmd(args.host, args.port, set_cmd, key, val)
                if r1 != b"+OK\r\n":
                    print("SET failed", key, r1)
                    return 2
                r2 = send_cmd(args.host, args.port, expire_cmd, key, str(args.ttl))
                if r2 != b"+OK\r\n":
                    print("EXPIRE failed", key, r2)
                    return 3
        write_cost = time.time() - started
        print(f"    write+expire finished in {write_cost:.3f}s")

        print(f"[2] validating {args.sample} samples before expiration")
        stride = max(1, args.keys // max(1, args.sample))
        samples = [min(i * stride, args.keys - 1) for i in range(args.sample)]
        for idx in samples:
            key = f"{args.prefix}:{idx:08d}"
            ttl = parse_integer(send_cmd(args.host, args.port, ttl_cmd, key))
            if ttl <= 0:
                print("TTL invalid before expiration", key, ttl)
                return 4
            val = send_cmd(args.host, args.port, get_cmd, key)
            expect = f"v{idx}".encode()
            if expect not in val:
                print("GET mismatch before expiration", key, val)
                return 5

        wait_s = args.ttl + 2
        print(f"[3] waiting {wait_s}s for expiration wave")
        time.sleep(wait_s)

        print(f"[4] validating {args.sample} samples after expiration")
        for idx in samples:
            key = f"{args.prefix}:{idx:08d}"
            get_resp = send_cmd(args.host, args.port, get_cmd, key)
            ttl_resp = send_cmd(args.host, args.port, ttl_cmd, key)
            if get_resp != b"$-1\r\n":
                print("GET should be null after expiration", key, get_resp)
                return 6
            if ttl_resp != b":-2\r\n":
                print("TTL should be -2 after expiration", key, ttl_resp)
                return 7

        print("[5] checking server still responsive")
        if send_cmd(args.host, args.port, "PING") != b"+PONG\r\n":
            print("PING failed after expiration wave")
            return 8

        print("MASS_TTL_VALIDATION_PASS")
        print(f"SUMMARY keys={args.keys} ttl={args.ttl} batch={args.batch} sample={args.sample}")
        return 0
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=3)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
