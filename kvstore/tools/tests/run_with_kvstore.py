#!/usr/bin/env python3
"""Wrapper: start kvstore, run a test command, stop kvstore."""
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
                data = s.recv(4096)
                if not data:
                    break
                chunks.append(data)
                if len(data) < 4096:
                    break
        except socket.timeout:
            pass
        return b"".join(chunks)
    finally:
        s.close()


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


def main() -> int:
    ap = argparse.ArgumentParser(description="Run a test with automatic kvstore lifecycle")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--appendfsync", default="always")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()

    if not args.cmd:
        print("ERROR: no test command specified")
        return 1

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(args.bin).resolve().parent)

    kill_port(args.port)
    time.sleep(0.3)

    proc = subprocess.Popen(
        [bin_path, "--port", str(args.port), "--appendfsync", args.appendfsync,
         "--aof", f"/tmp/kvstore_test_wrapper_{args.port}.aof",
         "--dump", f"/tmp/kvstore_test_wrapper_{args.port}.dump"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=cwd, preexec_fn=os.setsid,
    )

    try:
        for _ in range(80):
            try:
                if req(args.host, args.port, "PING").startswith(b"+PONG"):
                    break
            except Exception:
                pass
            time.sleep(0.2)
        else:
            print("ERROR: server not ready")
            return 1

        # Build the test command - replace {HOST} and {PORT} template vars
        # Skip leading '--' if present (argparse REMAINDER may include it)
        cmd_args = args.cmd
        if cmd_args and cmd_args[0] == "--":
            cmd_args = cmd_args[1:]
        test_cmd = []
        for part in cmd_args:
            test_cmd.append(part.replace("{HOST}", args.host).replace("{PORT}", str(args.port)))

        print(f"[wrapper] kvstore ready on :{args.port}, running: {' '.join(test_cmd)}")
        result = subprocess.run(test_cmd)
        return result.returncode

    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=2)
        except Exception:
            pass
        for suffix in (".aof", ".dump", ".aof.replstate"):
            p = Path(f"/tmp/kvstore_test_wrapper_{args.port}{suffix}")
            if p.exists():
                p.unlink()


if __name__ == "__main__":
    sys.exit(main())
