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
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        b = str(arg).encode()
        parts.append(f"${len(b)}\r\n".encode())
        parts.append(b)
        parts.append(b"\r\n")
    return b"".join(parts)


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


def parse_info(raw: bytes):
    text = raw.decode(errors="ignore").replace("\r", "")
    out = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def wait_ready(host: str, port: int, retries: int = 60) -> None:
    for _ in range(retries):
        try:
            resp = req(host, port, "ROLE")
            if b"master" in resp or b"slave" in resp:
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server not ready")


def stop_proc(proc):
    if not proc:
        return
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


def main() -> int:
    ap = argparse.ArgumentParser(description="validate rdma transport unsupported behavior")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--master-port", type=int, default=5210)
    ap.add_argument("--slave-port", type=int, default=5211)
    ap.add_argument("--wait", type=float, default=2.0)
    ap.add_argument("--work-dir", default="./artifacts/repl/rdma-unsupported")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    master_log = root / "master.log"
    slave_log = root / "slave.log"
    master_dump = root / "master.dump"
    master_aof = root / "master.aof"
    slave_dump = root / "slave.dump"
    slave_aof = root / "slave.aof"

    master = None
    slave = None
    mlog = open(master_log, "ab")
    slog = open(slave_log, "ab")

    try:
        master = subprocess.Popen(
            [args.bin, "--port", str(args.master_port), "--role", "master", "--dump", str(master_dump), "--aof", str(master_aof), "--repl-transport", "tcp"],
            stdout=mlog,
            stderr=mlog,
            cwd=str(Path(args.bin).resolve().parent),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.master_port)

        slave = subprocess.Popen(
            [args.bin, "--port", str(args.slave_port), "--role", "slave", "--master-host", args.host, "--master-port", str(args.master_port), "--dump", str(slave_dump), "--aof", str(slave_aof), "--repl-transport", "rdma"],
            stdout=slog,
            stderr=slog,
            cwd=str(Path(args.bin).resolve().parent),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.slave_port)
        time.sleep(args.wait)

        info = parse_info(req(args.host, args.slave_port, "INFO"))
        log_text = slave_log.read_text(errors="ignore") if slave_log.exists() else ""

        if info.get("repl_transport") != "rdma":
            raise RuntimeError(f"unexpected repl_transport: {info.get('repl_transport')}")
        if info.get("master_link") != "down":
            raise RuntimeError(f"unexpected master_link: {info.get('master_link')}")
        if "not compiled in" not in log_text:
            raise RuntimeError("expected unsupported rdma log message not found")

        print("REPL_RDMA_UNSUPPORTED_PASS")
        print(f"slave_repl_transport={info.get('repl_transport', 'missing')}")
        print(f"slave_master_link={info.get('master_link', 'missing')}")
        print(f"slave_log={slave_log}")
        print(f"artifacts={root}")
        return 0
    finally:
        stop_proc(slave)
        stop_proc(master)
        mlog.close()
        slog.close()


if __name__ == "__main__":
    sys.exit(main())
