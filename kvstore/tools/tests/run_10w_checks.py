#!/usr/bin/env python3
"""
1w+ 级别大容量测试 —— 覆盖 RESP / TTL / 持久化 / 文档 四个维度。
只输出 PASS/FAIL 摘要，不打印中间过程。
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


# ── helpers ──────────────────────────────────────────────────

def build_resp(*args: str) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for a in args:
        b = str(a).encode()
        parts.append(f"${len(b)}\r\n".encode())
        parts.append(b)
        parts.append(b"\r\n")
    return b"".join(parts)


class PC:
    """Persistent connection with pipelined batch support."""
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
        sock = self._ensure()
        sock.sendall(build_resp(*args))
        chunks: list[bytes] = []
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


def wait_ready(host: str, port: int, retries: int = 80) -> None:
    for _ in range(retries):
        try:
            s = socket.socket(); s.settimeout(2)
            s.connect((host, port))
            s.sendall(build_resp("PING"))
            if s.recv(1024).startswith(b"+PONG"):
                s.close(); return
            s.close()
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"server {host}:{port} not ready")


def kill_port(port: int) -> None:
    import shutil
    lsof = shutil.which("lsof")
    if not lsof:
        return
    try:
        r = subprocess.run([lsof, "-ti", f":{port}"], capture_output=True, text=True, timeout=5)
        for p in r.stdout.strip().splitlines():
            try:
                pid = int(p.strip())
                if pid > 1:
                    os.kill(pid, signal.SIGKILL)
            except (ValueError, ProcessLookupError):
                pass
    except Exception:
        pass


# ── test runners ─────────────────────────────────────────────

def test_resp_10w(host: str, port: int, count: int) -> bool:
    """批量 HSET / HGET / HDEL，验证正确性。"""
    pc = PC(host, port)
    try:
        # 批量写入
        for i in range(count):
            r = pc.cmd("HSET", f"r10w:k:{i:06d}", f"v{i}")
            if r != b"+OK\r\n":
                print(f"  FAIL: HSET[{i}] = {r!r}")
                return False
        # 抽样校验读取
        for idx in (0, count // 2, count - 1):
            r = pc.cmd("HGET", f"r10w:k:{idx:06d}")
            if f"v{idx}".encode() not in r:
                print(f"  FAIL: HGET[{idx}] = {r!r}")
                return False
        # 抽样校验删除
        for idx in (0, count // 2, count - 1):
            r = pc.cmd("HDEL", f"r10w:k:{idx:06d}")
            if r != b"+OK\r\n":
                print(f"  FAIL: HDEL[{idx}] = {r!r}")
                return False
            r = pc.cmd("HGET", f"r10w:k:{idx:06d}")
            if b"$-1" not in r and b"NO EXIST" not in r:
                print(f"  FAIL: key not deleted HGET[{idx}] = {r!r}")
                return False
        return True
    finally:
        pc.close()


def test_ttl_1w(host: str, port: int, count: int, ttl_s: int = 2) -> bool:
    """批量 HEXPIRE + HTTL，验证过期。"""
    pc = PC(host, port)
    try:
        for i in range(count):
            r = pc.cmd("HSET", f"t10w:k:{i:06d}", f"v{i}")
            if r != b"+OK\r\n":
                print(f"  FAIL: HSET[{i}] = {r!r}")
                return False
        # 使用较长 TTL 避免批量操作耗时导致前几个 key 提前过期
        long_ttl = max(ttl_s, 5)
        for i in range(count):
            r = pc.cmd("HEXPIRE", f"t10w:k:{i:06d}", str(long_ttl))
            if r != b"+OK\r\n":
                print(f"  FAIL: HEXPIRE[{i}] = {r!r}")
                return False
        # 校验最后几个 key 的 TTL（前面的可能因耗时已接近过期）
        for idx in (count - 3, count - 2, count - 1):
            r = pc.cmd("HTTL", f"t10w:k:{idx:06d}")
            if not r.startswith(b":"):
                print(f"  FAIL: HTTL[{idx}] = {r!r}")
                return False
            ttl = int(r[1:].split(b"\r")[0] if b"\r" in r else r[1:])
            if ttl <= 0 or ttl > long_ttl:
                print(f"  FAIL: HTTL[{idx}] value={ttl} (expected 1..{long_ttl})")
                return False
        # 等待过期
        time.sleep(long_ttl + 3)
        for idx in (0, count // 2, count - 1):
            r = pc.cmd("HGET", f"t10w:k:{idx:06d}")
            if not (b"$-1" in r or b"NO EXIST" in r):
                print(f"  FAIL: key not expired HGET[{idx}] = {r!r}")
                return False
        return True
    finally:
        pc.close()


def test_persist_1w(host: str, port: int, count: int) -> bool:
    """批量 HSET + SAVE，验证持久化。"""
    pc = PC(host, port)
    try:
        for i in range(count):
            r = pc.cmd("HSET", f"p10w:k:{i:06d}", f"v{i}")
            if r != b"+OK\r\n":
                print(f"  FAIL: HSET[{i}] = {r!r}")
                return False
        r = pc.cmd("SAVE")
        if r != b"+OK\r\n":
            print(f"  FAIL: SAVE = {r!r}")
            return False
        return True
    finally:
        pc.close()


def test_doc_1w(host: str, port: int, count: int) -> bool:
    """批量 DOCSET / DOCGET / DOCEXIST / DOCCOUNT。"""
    pc = PC(host, port)
    try:
        for i in range(count):
            r = pc.cmd("DOCSET", f"d10w:u:{i:06d}", "name", f"user{i}")
            if r != b"+OK\r\n":
                print(f"  FAIL: DOCSET[{i}] = {r!r}")
                return False
        # 抽样校验
        for idx in (0, count // 2, count - 1):
            r = pc.cmd("DOCGET", f"d10w:u:{idx:06d}", "name")
            if f"user{idx}".encode() not in r:
                print(f"  FAIL: DOCGET[{idx}] = {r!r}")
                return False
            r = pc.cmd("DOCEXIST", f"d10w:u:{idx:06d}")
            if r != b":1\r\n":
                print(f"  FAIL: DOCEXIST[{idx}] = {r!r}")
                return False
            r = pc.cmd("DOCCOUNT", f"d10w:u:{idx:06d}")
            if r != b":1\r\n":
                print(f"  FAIL: DOCCOUNT[{idx}] = {r!r}")
                return False
        return True
    finally:
        pc.close()


# ── main ─────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="1w+ 大容量功能测试")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5300)
    ap.add_argument("--count", type=int, default=10000, help="每项测试的数据量")
    args = ap.parse_args()

    total = 0
    passed = 0

    def run_one(label: str, fn) -> None:
        nonlocal total, passed
        total += 1
        try:
            ok = fn(args.host, args.port, args.count)
        except Exception as e:
            print(f"[FAIL] {label} — exception: {e}")
            return
        if ok:
            passed += 1
            print(f"[PASS] {label}")
        else:
            print(f"[FAIL] {label}")

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(args.bin).resolve().parent)

    kill_port(args.port)
    time.sleep(0.3)

    proc = subprocess.Popen(
        [bin_path, "--port", str(args.port),
         "--aof", f"/tmp/kvstore_10w_test_{args.port}.aof",
         "--dump", f"/tmp/kvstore_10w_test_{args.port}.dump",
         "--appendfsync", "always"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=cwd, preexec_fn=os.setsid,
    )

    try:
        wait_ready(args.host, args.port)

        t0 = time.perf_counter()
        run_one(f"RESP  (HSET+HGET+HDEL x{args.count})", test_resp_10w)
        run_one(f"TTL   (HEXPIRE+HTTL x{args.count})", test_ttl_1w)
        run_one(f"PERSIST (HSET+SAVE x{args.count})", test_persist_1w)
        run_one(f"DOC   (DOCSET+DOCGET x{args.count})", test_doc_1w)

        elapsed = time.perf_counter() - t0
        print(f"\n{'='*50}")
        print(f"SUMMARY: {passed}/{total} PASS, count={args.count}, elapsed={elapsed:.1f}s")
        print(f"{'='*50}")

        return 0 if passed == total else 1

    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=2)
        except Exception:
            pass
        for suffix in (".aof", ".dump", ".aof.replstate"):
            p = Path(f"/tmp/kvstore_10w_test_{args.port}{suffix}")
            if p.exists():
                p.unlink()


if __name__ == "__main__":
    sys.exit(main())
