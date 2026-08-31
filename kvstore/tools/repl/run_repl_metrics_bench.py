#!/usr/bin/env python3
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional


def read_text(path: Path) -> str:
    try:
        return path.read_text().strip()
    except Exception:
        return "missing"


def read_proc_status(pid: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    text = read_text(Path(f"/proc/{pid}/status"))
    if text == "missing":
        return out
    for line in text.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def read_proc_stat(pid: int) -> Dict[str, str]:
    text = read_text(Path(f"/proc/{pid}/stat"))
    out: Dict[str, str] = {}
    if text == "missing":
        return out
    try:
        fields = text.split()
        clk = os.sysconf(os.sysconf_names.get("SC_CLK_TCK", "SC_CLK_TCK"))
        out["utime_ticks"] = fields[13]
        out["stime_ticks"] = fields[14]
        out["rss_pages"] = fields[23]
        out["cpu_seconds"] = f"{(int(fields[13]) + int(fields[14])) / float(clk):.6f}"
    except Exception:
        return {}
    return out


def ss_summary(port: int) -> str:
    try:
        proc = subprocess.run(
            ["ss", "-tinp", f"( sport = :{port} or dport = :{port} )"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        text = proc.stdout.strip()
        return text if text else "missing"
    except Exception:
        return "missing"


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


def wait_ready(host: str, port: int, retries: int = 80) -> None:
    for _ in range(retries):
        try:
            resp = req(host, port, "ROLE")
            if b"master" in resp or b"slave" in resp:
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"server {host}:{port} not ready")


def stop_proc(proc: Optional[subprocess.Popen]) -> None:
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


def parse_info(raw: bytes) -> Dict[str, str]:
    text = raw.decode(errors="ignore").replace("\r", "")
    out: Dict[str, str] = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
        elif "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def expect_ok(resp: bytes, label: str) -> None:
    if resp not in (b"+OK\r\n", b":1\r\n"):
        raise RuntimeError(f"{label} failed: {resp!r}")


def hset(host: str, port: int, key: str, value: str) -> None:
    expect_ok(req(host, port, "HSET", key, value), f"HSET {key}")


def get_info(host: str, port: int) -> Dict[str, str]:
    return parse_info(req(host, port, "INFO"))


def pick(info: Dict[str, str], key: str) -> str:
    return info.get(key, "missing")


def main() -> int:
    ap = argparse.ArgumentParser(description="replication metrics baseline helper for stage 9.2")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--master-port", type=int, default=5188)
    ap.add_argument("--slave-port", type=int, default=5189)
    ap.add_argument("--preload-count", type=int, default=5000)
    ap.add_argument("--tail-count", type=int, default=1000)
    ap.add_argument("--sync-wait", type=float, default=6.0)
    ap.add_argument("--restart-wait", type=float, default=3.0)
    ap.add_argument("--work-dir", default="./artifacts/repl/metrics")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    master_dir = root / "master"
    slave_dir = root / "slave"
    master_dir.mkdir(parents=True, exist_ok=True)
    slave_dir.mkdir(parents=True, exist_ok=True)

    master_dump = master_dir / "master.dump"
    master_aof = master_dir / "master.aof"
    master_log = master_dir / "master.log"
    slave_dump = slave_dir / "slave.dump"
    slave_aof = slave_dir / "slave.aof"
    slave_log = slave_dir / "slave.log"

    for p in (master_dump, master_aof, slave_dump, slave_aof, slave_aof.with_suffix(slave_aof.suffix + ".replstate")):
        if p.exists():
            p.unlink()

    master_logf = open(master_log, "ab")
    slave_logf = open(slave_log, "ab")
    master = None
    slave = None

    try:
        master = subprocess.Popen(
            [args.bin, "--port", str(args.master_port), "--role", "master", "--dump", str(master_dump), "--aof", str(master_aof), "--repl-transport", "tcp", "--repl-fullsync-transport", "tcp", "--repl-realtime-transport", "tcp"],
            stdout=master_logf,
            stderr=master_logf,
            cwd=str(Path(args.bin).resolve().parent),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.master_port)

        t0 = time.perf_counter()
        for i in range(args.preload_count):
            hset(args.host, args.master_port, f"bench:pre:{i:05d}", f"value:{i:05d}")
        t1 = time.perf_counter()

        slave = subprocess.Popen(
            [
                args.bin,
                "--port", str(args.slave_port),
                "--role", "slave",
                "--master-host", args.host,
                "--master-port", str(args.master_port),
                "--dump", str(slave_dump),
                "--aof", str(slave_aof),
                "--repl-transport", "tcp",
                "--repl-fullsync-transport", "tcp",
                "--repl-realtime-transport", "tcp",
            ],
            stdout=slave_logf,
            stderr=slave_logf,
            cwd=str(Path(args.bin).resolve().parent),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.slave_port)
        time.sleep(args.sync_wait)
        t2 = time.perf_counter()

        master_after_full = get_info(args.host, args.master_port)
        slave_after_full = get_info(args.host, args.slave_port)
        master_status_after_full = read_proc_status(master.pid)
        slave_status_after_full = read_proc_status(slave.pid)
        master_stat_after_full = read_proc_stat(master.pid)
        slave_stat_after_full = read_proc_stat(slave.pid)
        master_ss_after_full = ss_summary(args.master_port)
        slave_ss_after_full = ss_summary(args.slave_port)

        for i in range(args.tail_count):
            hset(args.host, args.master_port, f"bench:tail:{i:05d}", f"tail:{i:05d}")
        t3 = time.perf_counter()
        time.sleep(1.0)

        master_after_tail = get_info(args.host, args.master_port)
        slave_after_tail = get_info(args.host, args.slave_port)
        master_status_after_tail = read_proc_status(master.pid)
        slave_status_after_tail = read_proc_status(slave.pid)
        master_stat_after_tail = read_proc_stat(master.pid)
        slave_stat_after_tail = read_proc_stat(slave.pid)
        master_ss_after_tail = ss_summary(args.master_port)
        slave_ss_after_tail = ss_summary(args.slave_port)

        stop_proc(slave)
        slave = None
        time.sleep(0.5)

        hset(args.host, args.master_port, "bench:restart:00001", "restart:00001")
        hset(args.host, args.master_port, "bench:restart:00002", "restart:00002")

        slave = subprocess.Popen(
            [
                args.bin,
                "--port", str(args.slave_port),
                "--role", "slave",
                "--master-host", args.host,
                "--master-port", str(args.master_port),
                "--dump", str(slave_dump),
                "--aof", str(slave_aof),
                "--repl-transport", "tcp",
                "--repl-fullsync-transport", "tcp",
                "--repl-realtime-transport", "tcp",
            ],
            stdout=slave_logf,
            stderr=slave_logf,
            cwd=str(Path(args.bin).resolve().parent),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.slave_port)
        time.sleep(args.restart_wait)
        t4 = time.perf_counter()

        master_after_restart = get_info(args.host, args.master_port)
        slave_after_restart = get_info(args.host, args.slave_port)
        master_status_after_restart = read_proc_status(master.pid)
        slave_status_after_restart = read_proc_status(slave.pid)
        master_stat_after_restart = read_proc_stat(master.pid)
        slave_stat_after_restart = read_proc_stat(slave.pid)
        master_ss_after_restart = ss_summary(args.master_port)
        slave_ss_after_restart = ss_summary(args.slave_port)

        print("REPL_METRICS_BASELINE_PASS")
        print(f"preload_count={args.preload_count}")
        print(f"tail_count={args.tail_count}")
        print(f"preload_write_seconds={t1 - t0:.6f}")
        print(f"fullsync_wait_seconds={t2 - t1:.6f}")
        print(f"tail_write_seconds={t3 - t2:.6f}")
        print(f"restart_resync_seconds={t4 - t3:.6f}")
        for prefix, info in (
            ("master_after_full", master_after_full),
            ("slave_after_full", slave_after_full),
            ("master_after_tail", master_after_tail),
            ("slave_after_tail", slave_after_tail),
            ("master_after_restart", master_after_restart),
            ("slave_after_restart", slave_after_restart),
        ):
            for key in (
                "master_replid",
                "master_repl_offset",
                "connected_slaves",
                "repl_fullsync_count",
                "repl_partialsync_ok_count",
                "repl_partialsync_err_count",
                "repl_broadcast_bytes",
                "repl_snapshot_bytes",
                "repl_backlog_histlen",
                "repl_backlog_start_offset",
                "repl_backlog_end_offset",
                "slave_master_replid",
                "slave_repl_offset",
                "master_link",
                "repl_transport",
            ):
                print(f"{prefix}_{key}={pick(info, key)}")
        for prefix, status in (
            ("master_after_full", master_status_after_full),
            ("slave_after_full", slave_status_after_full),
            ("master_after_tail", master_status_after_tail),
            ("slave_after_tail", slave_status_after_tail),
            ("master_after_restart", master_status_after_restart),
            ("slave_after_restart", slave_status_after_restart),
        ):
            for key in ("VmRSS", "VmHWM", "Threads", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"):
                print(f"{prefix}_{key}={status.get(key, 'missing')}")
        for prefix, stat in (
            ("master_after_full", master_stat_after_full),
            ("slave_after_full", slave_stat_after_full),
            ("master_after_tail", master_stat_after_tail),
            ("slave_after_tail", slave_stat_after_tail),
            ("master_after_restart", master_stat_after_restart),
            ("slave_after_restart", slave_stat_after_restart),
        ):
            for key in ("cpu_seconds", "utime_ticks", "stime_ticks", "rss_pages"):
                print(f"{prefix}_{key}={stat.get(key, 'missing')}")
        print(f"master_after_full_ss={master_ss_after_full}")
        print(f"slave_after_full_ss={slave_ss_after_full}")
        print(f"master_after_tail_ss={master_ss_after_tail}")
        print(f"slave_after_tail_ss={slave_ss_after_tail}")
        print(f"master_after_restart_ss={master_ss_after_restart}")
        print(f"slave_after_restart_ss={slave_ss_after_restart}")
        replstate = slave_aof.with_suffix(slave_aof.suffix + ".replstate")
        print(f"slave_replstate_path={replstate}")
        if replstate.exists():
            print(f"slave_replstate={replstate.read_text().strip()}")
        print(f"artifacts={root}")
        return 0
    finally:
        stop_proc(slave)
        stop_proc(master)
        master_logf.close()
        slave_logf.close()


if __name__ == "__main__":
    sys.exit(main())
