#!/usr/bin/env python3
"""
1w-level bulk test suite based on `make check` patterns.
Tests: bulk HSET/HGET/HDEL/HEXIST, TTL, persist+recover, DOC
All use hash engine. Output: one line per test, PASS/FAIL only.
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

PASS = 0
FAIL = 0
HOST = "127.0.0.1"
BULK = 10000
BULK_PORT = 5097


def build_resp(*args: str) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for a in args:
        b = str(a).encode()
        parts.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
    return b"".join(parts)


def start_server(port: int, clean: bool = True):
    dump = Path(f"/tmp/kvstore_bulk_{port}.dump")
    aof = Path(f"/tmp/kvstore_bulk_{port}.aof")
    if clean:
        for p in (dump, aof):
            if p.exists():
                p.unlink()
    p = subprocess.Popen(
        ["./kvstore", "--port", str(port), "--dump", str(dump),
         "--aof", str(aof), "--appendfsync", "always"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid)
    time.sleep(1.5)
    return p


def stop_server(proc):
    if proc is None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=2)
    except Exception:
        pass


def wait_ready(port: int, retries: int = 80):
    for _ in range(retries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((HOST, port))
            s.sendall(build_resp("PING"))
            if s.recv(1024).startswith(b"+PONG"):
                s.close()
                return
            s.close()
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"server not ready on :{port}")


def req(port: int, *args: str, timeout: float = 5.0) -> bytes:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((HOST, port))
        s.sendall(build_resp(*args))
        chunks = []
        while True:
            data = s.recv(65536)
            if not data:
                break
            chunks.append(data)
            if len(data) < 65536:
                break
        return b"".join(chunks)
    except socket.timeout:
        return b""
    finally:
        s.close()


def check(name: str, ok: bool):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}")


def test_bulk_set_get():
    print(f"[bulk] HSET/HGET/HEXIST/HDEL ({BULK:,} keys)")
    t0 = time.perf_counter()
    for i in range(BULK // 1000):
        batch_start = i * 1000
        batch_end = min(batch_start + 1000, BULK)
        for j in range(batch_start, batch_end):
            r = req(BULK_PORT, "HSET", f"bulk:k:{j:06d}", f"v{j}")
            if r != b"+OK\r\n":
                check(f"HSET key {j}", False)
                return
    elapsed = time.perf_counter() - t0
    check(f"HSET {BULK:,} keys ({elapsed:.1f}s, {BULK/elapsed:.0f} qps)", True)

    samples = (0, BULK // 4, BULK // 2, BULK * 3 // 4, BULK - 1)
    ok = True
    for idx in samples:
        r = req(BULK_PORT, "HGET", f"bulk:k:{idx:06d}")
        if f"v{idx}".encode() not in r:
            ok = False
    check(f"HGET {len(samples)} samples", ok)

    r = req(BULK_PORT, "HEXIST", f"bulk:k:{BULK//2:06d}")
    check("HEXIST existing key", b":1" in r)
    r = req(BULK_PORT, "HEXIST", "bulk:k:nonexist")
    check("HEXIST nonexist key", b":0" in r)

    r = req(BULK_PORT, "HDEL", "bulk:k:000000")
    check("HDEL single key", r in (b"+OK\r\n", b"OK\r\n"))
    r = req(BULK_PORT, "HGET", "bulk:k:000000")
    check("HGET after HDEL (null)", b"$-1" in r or b"NO EXIST" in r)


def test_bulk_ttl():
    print(f"[bulk-ttl] HEXPIRE/HTTL/HPERSIST ({BULK:,} keys)")
    prefix = "bulk_ttl"
    ttl_sec = 4

    for i in range(BULK):
        r = req(BULK_PORT, "HSET", f"{prefix}:{i:06d}", f"v{i}")
        if r != b"+OK\r\n":
            check(f"HSET for ttl {i}", False)
            return
    check(f"HSET {BULK:,} ttl keys", True)

    for i in range(BULK):
        r = req(BULK_PORT, "HEXPIRE", f"{prefix}:{i:06d}", str(ttl_sec))
        if r != b"+OK\r\n":
            check(f"HEXPIRE {i}", False)
            return
    check(f"HEXPIRE {BULK:,} keys", True)

    for idx in (0, BULK // 2, BULK - 1):
        r = req(BULK_PORT, "HTTL", f"{prefix}:{idx:06d}")
        if b":" not in r:
            check(f"HTTL before expiry idx={idx}", False)
            return
    check("HTTL before expiry (sample)", True)

    time.sleep(ttl_sec + 2)
    for idx in (0, BULK // 2, BULK - 1):
        r = req(BULK_PORT, "HGET", f"{prefix}:{idx:06d}")
        if b"$-1" not in r and b"NO EXIST" not in r:
            check(f"HGET after expiry idx={idx}", False)
            return
    check("HGET after expiry (null)", True)

    req(BULK_PORT, "HSET", "persist:test", "v")
    req(BULK_PORT, "HEXPIRE", "persist:test", "60")
    r = req(BULK_PORT, "HPERSIST", "persist:test")
    check("HPERSIST", r == b"+OK\r\n")
    r = req(BULK_PORT, "HTTL", "persist:test")
    check("HTTL after HPERSIST (-1)", b":-1" in r)


def test_bulk_persist():
    pport = BULK_PORT + 1
    print(f"[bulk-persist] SAVE+recover ({BULK:,} keys)")
    proc = start_server(pport)
    try:
        wait_ready(pport)
        t0 = time.perf_counter()
        for i in range(BULK):
            if req(pport, "HSET", f"persist:k:{i:06d}", f"v{i}") != b"+OK\r\n":
                check(f"persist HSET {i}", False)
                return
        elapsed = time.perf_counter() - t0
        check(f"HSET {BULK:,} ({elapsed:.1f}s)", True)

        r = req(pport, "SAVE")
        check("SAVE", r == b"+OK\r\n")
        dump = Path(f"/tmp/kvstore_bulk_{pport}.dump")
        aof = Path(f"/tmp/kvstore_bulk_{pport}.aof")
        dump_size = dump.stat().st_size if dump.exists() else -1
        aof_size = aof.stat().st_size if aof.exists() else -1
        check(f"dump={dump_size:,}B aof={aof_size:,}B", dump_size > 0)
    finally:
        stop_server(proc)

    proc = start_server(pport, clean=False)
    try:
        wait_ready(pport)
        ok = True
        for idx in (0, BULK // 4, BULK // 2, BULK * 3 // 4, BULK - 1):
            r = req(pport, "HGET", f"persist:k:{idx:06d}")
            if f"v{idx}".encode() not in r:
                ok = False
        check(f"recover verify {5} samples", ok)
    finally:
        stop_server(proc)
        # clean up temp files
        for p in (Path(f"/tmp/kvstore_bulk_{pport}.dump"),
                  Path(f"/tmp/kvstore_bulk_{pport}.aof")):
            if p.exists():
                p.unlink()


def test_bulk_doc():
    n = max(100, min(BULK, 1000))
    print(f"[bulk-doc] DOCSET/DOCGET/DOCEXIST/DOCDROP ({n:,} docs)")
    t0 = time.perf_counter()
    for i in range(n):
        r = req(BULK_PORT, "DOCSET", f"doc:{i:06d}", "name", f"user{i}")
        if r != b"+OK\r\n":
            check(f"DOCSET {i}", False)
            return
    elapsed = time.perf_counter() - t0
    check(f"DOCSET {n:,} ({elapsed:.1f}s)", True)

    ok = True
    for idx in (0, n // 2, n - 1):
        r = req(BULK_PORT, "DOCGET", f"doc:{idx:06d}", "name")
        if f"user{idx}".encode() not in r:
            ok = False
    check(f"DOCGET {3} samples", ok)

    r = req(BULK_PORT, "DOCEXIST", f"doc:{n//2:06d}")
    check("DOCEXIST existing", b":1" in r)
    r = req(BULK_PORT, "DOCEXIST", "doc:nonexist")
    check("DOCEXIST nonexist", b":0" in r)

    r = req(BULK_PORT, "DOCDROP", "doc:000000")
    check("DOCDROP", r == b"+OK\r\n")
    r = req(BULK_PORT, "DOCEXIST", "doc:000000")
    check("DOCEXIST after DOCDROP", b":0" in r)


def main() -> int:
    global BULK
    ap = argparse.ArgumentParser(description="1w-level bulk test suite")
    ap.add_argument("--count", type=int, default=10000)
    args = ap.parse_args()
    BULK = args.count

    print("=" * 50)
    print(f"kvstore 1w-level bulk test suite ({BULK:,} keys)")
    print("=" * 50)

    proc = start_server(BULK_PORT)
    try:
        wait_ready(BULK_PORT)
        test_bulk_set_get()
        test_bulk_ttl()
        test_bulk_doc()
    finally:
        stop_server(proc)
        for p in (Path(f"/tmp/kvstore_bulk_{BULK_PORT}.dump"),
                  Path(f"/tmp/kvstore_bulk_{BULK_PORT}.aof")):
            if p.exists():
                p.unlink()

    test_bulk_persist()

    print("=" * 50)
    print(f"RESULT: {PASS} PASS, {FAIL} FAIL")
    print("=" * 50)
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
