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


def wait_fullsync_done(host: str, port: int, retries: int = 50):
    last = {}
    for _ in range(retries):
        try:
            last = parse_info(req(host, port, "INFO"))
            if last.get("slave_fullsync_loading") == "0":
                return last
        except Exception:
            pass
        time.sleep(0.2)
    return last


def wait_value(host: str, port: int, key: str, expect: str, retries: int = 50):
    last = b""
    for _ in range(retries):
        try:
            last = req(host, port, "HGET", key)
            if expect in last.decode(errors="ignore"):
                return last
        except Exception:
            pass
        time.sleep(0.2)
    return last


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


def start_slave(args, cwd, slog, slave_dump, slave_aof):
    return subprocess.Popen(
        [args.bin, "--port", str(args.slave_port), "--role", "slave", "--master-host", args.host, "--master-port", str(args.master_port), "--dump", str(slave_dump), "--aof", str(slave_aof), "--repl-transport", "rdma", "--repl-fullsync-transport", "rdma", "--repl-realtime-transport", "tcp", "--rdma-dev", args.rdma_dev],
        stdout=slog,
        stderr=slog,
        cwd=str(cwd),
        preexec_fn=os.setsid,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="RDMA replication smoke helper for stage 9.3")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="192.168.233.128")
    ap.add_argument("--master-port", type=int, default=5220)
    ap.add_argument("--slave-port", type=int, default=5222)
    ap.add_argument("--wait", type=float, default=4.0)
    ap.add_argument("--rdma-dev", default="rxe0")
    ap.add_argument("--work-dir", default="./artifacts/repl/rdma-smoke")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    build_log = root / "build.log"
    master_log = root / "master.log"
    slave_log = root / "slave.log"
    master_dump = root / "master.dump"
    master_aof = root / "master.aof"
    slave_dump = root / "slave.dump"
    slave_aof = root / "slave.aof"

    cwd = Path(args.bin).resolve().parent
    build = subprocess.run(
        ["make", "ENABLE_RDMA=1"],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    build_log.write_text(build.stdout)
    if build.returncode != 0:
        print("REPL_RDMA_SMOKE_BUILD_FAIL")
        print(f"build_log={build_log}")
        return build.returncode

    master = None
    slave = None
    for p in (build_log, master_log, slave_log, master_dump, master_aof, slave_dump, slave_aof, slave_aof.with_suffix(slave_aof.suffix + ".replstate")):
        if p.exists():
            p.unlink()
    mlog = open(master_log, "ab")
    slog = open(slave_log, "ab")
    try:
        master = subprocess.Popen(
            [args.bin, "--port", str(args.master_port), "--role", "master", "--master-host", args.host, "--dump", str(master_dump), "--aof", str(master_aof), "--repl-transport", "rdma", "--repl-fullsync-transport", "rdma", "--repl-realtime-transport", "tcp", "--rdma-dev", args.rdma_dev],
            stdout=mlog,
            stderr=mlog,
            cwd=str(cwd),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.master_port)
        req(args.host, args.master_port, "HSET", "rdma:smoke:key", "rdma-smoke-value")

        slave = start_slave(args, cwd, slog, slave_dump, slave_aof)
        wait_ready(args.host, args.slave_port)
        time.sleep(args.wait)

        info = wait_fullsync_done(args.host, args.slave_port)
        slave_value = req(args.host, args.slave_port, "HGET", "rdma:smoke:key")
        slave_value_text = slave_value.decode(errors="ignore").replace("\r", "\\r").replace("\n", "\\n")
        fullsync_done = info.get("slave_fullsync_loading", "missing") == "0"
        replicated_ok = "rdma-smoke-value" in slave_value.decode(errors="ignore")

        req(args.host, args.master_port, "HSET", "rdma:postsync:key", "rdma-postsync-value")
        postsync_value = wait_value(args.host, args.slave_port, "rdma:postsync:key", "rdma-postsync-value")
        postsync_value_text = postsync_value.decode(errors="ignore").replace("\r", "\\r").replace("\n", "\\n")
        postsync_ok = "rdma-postsync-value" in postsync_value.decode(errors="ignore")

        stop_proc(slave)
        slave = None
        time.sleep(1.0)
        req(args.host, args.master_port, "HSET", "rdma:resume:key", "rdma-resume-value")

        slave = start_slave(args, cwd, slog, slave_dump, slave_aof)
        wait_ready(args.host, args.slave_port)
        time.sleep(args.wait)

        resumed_info = wait_fullsync_done(args.host, args.slave_port)
        resume_value = wait_value(args.host, args.slave_port, "rdma:resume:key", "rdma-resume-value")
        resume_value_text = resume_value.decode(errors="ignore").replace("\r", "\\r").replace("\n", "\\n")
        resume_ok = "rdma-resume-value" in resume_value.decode(errors="ignore")

        log_text = slave_log.read_text(errors="ignore") if slave_log.exists() else ""
        continue_seen = "slave_parse - CONTINUE" in log_text

        print("REPL_RDMA_SMOKE_RESULT")
        print(f"build_log={build_log}")
        print(f"master_log={master_log}")
        print(f"slave_repl_transport={info.get('repl_transport', 'missing')}")
        print(f"slave_master_link={info.get('master_link', 'missing')}")
        print(f"slave_fullsync_loading={info.get('slave_fullsync_loading', 'missing')}")
        print(f"slave_repl_offset={info.get('slave_repl_offset', 'missing')}")
        print(f"fullsync_done={'yes' if fullsync_done else 'no'}")
        print(f"replicated_value_ok={'yes' if replicated_ok else 'no'}")
        print(f"slave_get_resp={slave_value_text}")
        print(f"postsync_value_ok={'yes' if postsync_ok else 'no'}")
        print(f"slave_postsync_get_resp={postsync_value_text}")
        print(f"resume_value_ok={'yes' if resume_ok else 'no'}")
        print(f"slave_resume_get_resp={resume_value_text}")
        print(f"partial_resync_continue_seen={'yes' if continue_seen else 'no'}")
        print(f"resumed_slave_fullsync_loading={resumed_info.get('slave_fullsync_loading', 'missing')}")
        print(f"resumed_slave_repl_offset={resumed_info.get('slave_repl_offset', 'missing')}")
        print(f"slave_log={slave_log}")
        print(f"rdma_log_has_not_compiled_in={'yes' if 'not compiled in' in log_text else 'no'}")
        print(f"artifacts={root}")
        return 0
    finally:
        stop_proc(slave)
        stop_proc(master)
        mlog.close()
        slog.close()


if __name__ == "__main__":
    sys.exit(main())
