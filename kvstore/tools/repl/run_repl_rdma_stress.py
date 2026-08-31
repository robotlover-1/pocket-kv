#!/usr/bin/env python3
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from repl_ebpf_session import EbpfSession


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
    last_resp = b""
    last_err = None
    for _ in range(retries):
        try:
            resp = req(host, port, "ROLE")
            last_resp = resp
            if b"master" in resp or b"slave" in resp:
                return
        except Exception as exc:
            last_err = exc
        time.sleep(0.2)
    detail = last_resp.decode(errors="ignore") if last_resp else repr(last_err)
    raise RuntimeError(f"server not ready: {detail}")


def wait_fullsync_done(host: str, port: int, retries: int = 80):
    last = {}
    for _ in range(retries):
        try:
            last = parse_info(req(host, port, "INFO"))
            if last.get("slave_fullsync_loading") == "0":
                return last
        except Exception:
            pass
        time.sleep(0.25)
    return last


def wait_value(host: str, port: int, key: str, expect: str, retries: int = 80):
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


def fetch_info(host: str, port: int):
    try:
        return parse_info(req(host, port, "INFO"))
    except Exception:
        return {}


def wait_link_up(host: str, port: int, retries: int = 40):
    last = {}
    for _ in range(retries):
        last = fetch_info(host, port)
        if last.get("master_link") == "up":
            return True, last
        time.sleep(0.25)
    return False, last


def wait_value_with_reconnect_grace(host: str, port: int, key: str, expect: str, fast_retries: int = 20, grace_seconds: float = 4.0, reconnect_retries: int = 80):
    ok, got = wait_value_with_grace(host, port, key, expect, fast_retries=fast_retries, grace_seconds=grace_seconds)
    if ok:
        return True, got
    info = fetch_info(host, port)
    if info.get("master_link") != "down":
        return False, got
    link_ok, _ = wait_link_up(host, port, retries=reconnect_retries)
    if not link_ok:
        return False, got
    return wait_value_with_grace(host, port, key, expect, fast_retries=fast_retries, grace_seconds=grace_seconds)


def wait_value_with_grace(host: str, port: int, key: str, expect: str, fast_retries: int = 20, grace_seconds: float = 3.0):
    first = wait_value(host, port, key, expect, retries=fast_retries)
    first_text = first.decode(errors="ignore")
    if expect in first_text:
        return True, first_text
    deadline = time.monotonic() + max(grace_seconds, 0.1)
    last_text = first_text
    while time.monotonic() < deadline:
        time.sleep(0.25)
        cur = req(host, port, "HGET", key).decode(errors="ignore")
        last_text = cur
        if expect in cur:
            return True, cur
    return False, last_text


def capture_failure_log_slice(log_path: Path, out_path: Path, window_bytes: int = 8192):
    if not log_path.exists():
        return 0, out_path
    raw = log_path.read_bytes()
    total_len = len(raw)
    start = max(total_len - window_bytes, 0)
    out_path.write_bytes(raw[start:])
    return total_len, out_path


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
    cmd = [
        args.bin,
        "--port",
        str(args.slave_port),
        "--role",
        "slave",
        "--master-host",
        args.host,
        "--master-port",
        str(args.master_port),
        "--dump",
        str(slave_dump),
        "--aof",
        str(slave_aof),
        "--repl-transport",
        "rdma",
        "--repl-fullsync-transport",
        "rdma",
        "--repl-realtime-transport",
        "ebpf",
    ]
    if args.rdma_recv_slots > 0:
        cmd.extend(["--rdma-recv-slots", str(args.rdma_recv_slots)])
    if args.rdma_chunk_size > 0:
        cmd.extend(["--rdma-chunk-size", str(args.rdma_chunk_size)])
    if args.rdma_qp_wr_depth > 0:
        cmd.extend(["--rdma-qp-wr-depth", str(args.rdma_qp_wr_depth)])
    return subprocess.Popen(
        cmd,
        stdout=slog,
        stderr=slog,
        cwd=str(cwd),
        preexec_fn=os.setsid,
    )


def set_value(host: str, port: int, key: str, value: str) -> bytes:
    return req(host, port, "HSET", key, value)


def get_value_text(host: str, port: int, key: str) -> str:
    return req(host, port, "HGET", key).decode(errors="ignore")


def get_effective_tunable(value: int, default: int) -> int:
    return value if value > 0 else default


def main() -> int:
    ap = argparse.ArgumentParser(description="RDMA replication stress helper")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--master-port", type=int, default=5230)
    ap.add_argument("--slave-port", type=int, default=5232)
    ap.add_argument("--wait", type=float, default=4.0)
    ap.add_argument("--work-dir", default="./artifacts/repl/rdma-stress")
    ap.add_argument("--preload", type=int, default=128)
    ap.add_argument("--tail-writes", type=int, default=64)
    ap.add_argument("--restart-rounds", type=int, default=3)
    ap.add_argument("--soak-seconds", type=int, default=0)
    ap.add_argument("--soak-reconnect-interval", type=int, default=10)
    ap.add_argument("--soak-write-interval-ms", type=int, default=200)
    ap.add_argument("--rdma-recv-slots", type=int, default=0)
    ap.add_argument("--rdma-chunk-size", type=int, default=0)
    ap.add_argument("--rdma-qp-wr-depth", type=int, default=0)
    ap.add_argument("--allow-soak-failure", action="store_true")
    ap.add_argument("--ebpf", action="store_true")
    ap.add_argument("--force-fallback", action="store_true")
    ap.add_argument("--rdma-dev", default="rxe0")
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
    rdma_ebpf_out = root / "rdma_ebpf.out"
    rdma_ebpf_err = root / "rdma_ebpf.err"
    rdma_ebpf_summary = root / "rdma_ebpf_summary.txt"

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
        print("REPL_RDMA_STRESS_BUILD_FAIL")
        print(f"build_log={build_log}")
        return build.returncode

    master = None
    slave = None
    ebpf = None
    ebpf_rc = 0
    for p in (
        build_log,
        master_log,
        slave_log,
        master_dump,
        master_aof,
        slave_dump,
        slave_aof,
        slave_aof.with_suffix(slave_aof.suffix + ".replstate"),
        rdma_ebpf_out,
        rdma_ebpf_err,
        rdma_ebpf_summary,
    ):
        if p.exists():
            p.unlink()

    mlog = open(master_log, "ab")
    slog = open(slave_log, "ab")

    final_key = "rdma:stress:final:key"
    final_value = "rdma-stress-final-value"
    rounds_ok = True
    resumed_continue_rounds = 0
    soak_ok = True
    soak_availability_ok = True
    soak_recovery_ok = True
    soak_writes = 0
    soak_reconnects = 0
    soak_fail_key = ""
    soak_fail_value = ""
    soak_fail_seen = ""
    soak_fail_slave_link = ""
    soak_fail_slave_offset = ""
    soak_fail_master_link = ""
    failure_master_log_len = 0
    failure_slave_log_len = 0
    failure_master_tail = root / "failure_master_tail.log"
    failure_slave_tail = root / "failure_slave_tail.log"
    fullsync_done = False
    postsync_ok = False
    final_resume_ok = False
    last_info = {}
    final_info = {}

    if args.ebpf:
        ebpf = EbpfSession(root, Path(args.bin).resolve(), prefix="rdma")
        if not ebpf.start():
            print("REPL_RDMA_STRESS_EBPF_FAIL")
            print(f"ebpf_start_error={ebpf.start_error}")
            print(f"rdma_ebpf_output={ebpf.out_path}")
            print(f"rdma_ebpf_error={ebpf.err_path}")
            print(f"rdma_ebpf_summary={ebpf.summary_path}")
            return 1

    try:
        master = subprocess.Popen(
            [
                args.bin,
                "--port",
                str(args.master_port),
                "--role",
                "master",
                "--master-host",
                args.host,
                "--dump",
                str(master_dump),
                "--aof",
                str(master_aof),
                "--repl-transport",
                "rdma",
                "--repl-fullsync-transport",
                "rdma",
                "--repl-realtime-transport",
                "ebpf",
                "--rdma-dev",
                args.rdma_dev,
            ]
            + (["--rdma-recv-slots", str(args.rdma_recv_slots)] if args.rdma_recv_slots > 0 else [])
            + (["--rdma-chunk-size", str(args.rdma_chunk_size)] if args.rdma_chunk_size > 0 else [])
            + (["--rdma-qp-wr-depth", str(args.rdma_qp_wr_depth)] if args.rdma_qp_wr_depth > 0 else []),
            stdout=mlog,
            stderr=mlog,
            cwd=str(cwd),
            preexec_fn=os.setsid,
        )
        wait_ready(args.host, args.master_port)

        for i in range(args.preload):
            set_value(args.host, args.master_port, f"rdma:stress:pre:{i}", f"value-{i}")

        slave = start_slave(args, cwd, slog, slave_dump, slave_aof)
        wait_ready(args.host, args.slave_port)
        time.sleep(args.wait)
        last_info = wait_fullsync_done(args.host, args.slave_port)
        if args.force_fallback:
            stop_proc(slave)
            slave = None
            time.sleep(1.0)
            slave = subprocess.Popen(
                [args.bin, "--port", str(args.slave_port), "--role", "slave", "--master-host", args.host, "--master-port", str(args.master_port), "--dump", str(slave_dump), "--aof", str(slave_aof), "--repl-transport", "tcp"],
                stdout=slog,
                stderr=slog,
                cwd=str(cwd),
                preexec_fn=os.setsid,
            )
            wait_ready(args.host, args.slave_port)
            time.sleep(args.wait)
            last_info = wait_fullsync_done(args.host, args.slave_port)
        fullsync_done = last_info.get("slave_fullsync_loading") == "0"

        for i in range(args.tail_writes):
            set_value(args.host, args.master_port, f"rdma:stress:tail:{i}", f"tail-value-{i}")
        tail_probe = wait_value(args.host, args.slave_port, f"rdma:stress:tail:{args.tail_writes - 1}", f"tail-value-{args.tail_writes - 1}")
        postsync_ok = f"tail-value-{args.tail_writes - 1}" in tail_probe.decode(errors="ignore")
        if not postsync_ok:
            last_info = fetch_info(args.host, args.slave_port)
            master_info = fetch_info(args.host, args.master_port)
            soak_fail_slave_link = last_info.get("master_link", "")
            soak_fail_slave_offset = last_info.get("slave_repl_offset", "")
            soak_fail_master_link = master_info.get("master_link", "")
            failure_master_log_len, _ = capture_failure_log_slice(master_log, failure_master_tail)
            failure_slave_log_len, _ = capture_failure_log_slice(slave_log, failure_slave_tail)

        prev_log_len = 0
        for round_idx in range(args.restart_rounds):
            stop_proc(slave)
            slave = None
            time.sleep(1.0)
            round_key = f"rdma:stress:resume:{round_idx}"
            round_val = f"resume-value-{round_idx}"
            set_value(args.host, args.master_port, round_key, round_val)

            slave = start_slave(args, cwd, slog, slave_dump, slave_aof)
            wait_ready(args.host, args.slave_port)
            time.sleep(args.wait)
            last_info = wait_fullsync_done(args.host, args.slave_port)
            got = wait_value(args.host, args.slave_port, round_key, round_val).decode(errors="ignore")
            if round_val not in got:
                rounds_ok = False
                last_info = fetch_info(args.host, args.slave_port)
                master_info = fetch_info(args.host, args.master_port)
                soak_fail_slave_link = last_info.get("master_link", "")
                soak_fail_slave_offset = last_info.get("slave_repl_offset", "")
                soak_fail_master_link = master_info.get("master_link", "")
                failure_master_log_len, _ = capture_failure_log_slice(master_log, failure_master_tail)
                failure_slave_log_len, _ = capture_failure_log_slice(slave_log, failure_slave_tail)
                break

            log_text = slave_log.read_text(errors="ignore") if slave_log.exists() else ""
            if "slave_parse - CONTINUE" in log_text[prev_log_len:]:
                resumed_continue_rounds += 1
            prev_log_len = len(log_text)

        if rounds_ok and args.soak_seconds > 0:
            soak_deadline = time.monotonic() + args.soak_seconds
            next_reconnect_at = time.monotonic() + max(args.soak_reconnect_interval, 1)
            write_interval = max(args.soak_write_interval_ms, 1) / 1000.0
            while time.monotonic() < soak_deadline:
                idx = soak_writes
                key = f"rdma:soak:key:{idx}"
                value = f"rdma-soak-value-{idx}"
                set_value(args.host, args.master_port, key, value)
                ok, got = wait_value_with_reconnect_grace(args.host, args.slave_port, key, value, fast_retries=20, grace_seconds=6.0, reconnect_retries=80)
                if not ok:
                    soak_ok = False
                    soak_availability_ok = False
                    soak_fail_key = key
                    soak_fail_value = value
                    soak_fail_seen = got
                    last_info = fetch_info(args.host, args.slave_port)
                    master_info = fetch_info(args.host, args.master_port)
                    soak_fail_slave_link = last_info.get("master_link", "")
                    soak_fail_slave_offset = last_info.get("slave_repl_offset", "")
                    soak_fail_master_link = master_info.get("master_link", "")
                    failure_master_log_len, _ = capture_failure_log_slice(master_log, failure_master_tail)
                    failure_slave_log_len, _ = capture_failure_log_slice(slave_log, failure_slave_tail)
                    break
                soak_writes += 1

                if time.monotonic() >= next_reconnect_at:
                    stop_proc(slave)
                    slave = None
                    time.sleep(1.0)
                    slave = start_slave(args, cwd, slog, slave_dump, slave_aof)
                    wait_ready(args.host, args.slave_port)
                    time.sleep(args.wait)
                    last_info = wait_fullsync_done(args.host, args.slave_port)
                    ok, probe = wait_value_with_reconnect_grace(args.host, args.slave_port, key, value, fast_retries=20, grace_seconds=6.0, reconnect_retries=80)
                    if not ok:
                        soak_ok = False
                        soak_availability_ok = False
                        soak_fail_key = key
                        soak_fail_value = value
                        soak_fail_seen = probe
                        last_info = fetch_info(args.host, args.slave_port)
                        master_info = fetch_info(args.host, args.master_port)
                        soak_fail_slave_link = last_info.get("master_link", "")
                        soak_fail_slave_offset = last_info.get("slave_repl_offset", "")
                        soak_fail_master_link = master_info.get("master_link", "")
                        failure_master_log_len, _ = capture_failure_log_slice(master_log, failure_master_tail)
                        failure_slave_log_len, _ = capture_failure_log_slice(slave_log, failure_slave_tail)
                        break
                    soak_reconnects += 1
                    log_text = slave_log.read_text(errors="ignore") if slave_log.exists() else ""
                    if "slave_parse - CONTINUE" in log_text[prev_log_len:]:
                        resumed_continue_rounds += 1
                    prev_log_len = len(log_text)
                    next_reconnect_at = time.monotonic() + max(args.soak_reconnect_interval, 1)

                time.sleep(write_interval)

        set_value(args.host, args.master_port, final_key, final_value)
        final_resp = wait_value(args.host, args.slave_port, final_key, final_value)
        final_resume_ok = final_value in final_resp.decode(errors="ignore")
        final_info = fetch_info(args.host, args.slave_port)
        if args.soak_seconds > 0 and soak_fail_key:
            recovered, recovered_text = wait_value_with_reconnect_grace(args.host, args.slave_port, soak_fail_key, soak_fail_value, fast_retries=20, grace_seconds=6.0, reconnect_retries=80)
            fail_offset_num = int(soak_fail_slave_offset) if soak_fail_slave_offset.isdigit() else -1
            final_offset_num = int(final_info.get("slave_repl_offset", "-1")) if final_info.get("slave_repl_offset", "").isdigit() else -1
            soak_recovery_ok = recovered and final_offset_num >= fail_offset_num and final_info.get("master_link") == "up"
            if recovered:
                soak_fail_seen = recovered_text
        soak_ok = soak_availability_ok and soak_recovery_ok
        if not final_resume_ok:
            last_info = final_info
            master_info = fetch_info(args.host, args.master_port)
            soak_fail_slave_link = final_info.get("master_link", "")
            soak_fail_slave_offset = final_info.get("slave_repl_offset", "")
            soak_fail_master_link = master_info.get("master_link", "")
            failure_master_log_len, _ = capture_failure_log_slice(master_log, failure_master_tail)
            failure_slave_log_len, _ = capture_failure_log_slice(slave_log, failure_slave_tail)

        log_text = slave_log.read_text(errors="ignore") if slave_log.exists() else ""
        master_log_text = master_log.read_text(errors="ignore") if master_log.exists() else ""
        slave_async_disconnect_seen = 'cm_event_async - RDMA_CM_EVENT_DISCONNECTED' in log_text
        master_async_disconnect_seen = 'cm_event_async - RDMA_CM_EVENT_DISCONNECTED' in master_log_text
        slave_async_disconnect_impactful = slave_async_disconnect_seen and (final_info.get('master_link') != 'up' or not final_resume_ok)
        master_async_disconnect_impactful = master_async_disconnect_seen and (not fullsync_done or not postsync_ok or not rounds_ok or not final_resume_ok or final_info.get('master_link') != 'up')
        print("REPL_RDMA_STRESS_RESULT")
        print(f"build_log={build_log}")
        print(f"master_log={master_log}")
        print(f"slave_log={slave_log}")
        print(f"fullsync_done={'yes' if fullsync_done else 'no'}")
        print(f"postsync_tail_ok={'yes' if postsync_ok else 'no'}")
        print(f"restart_rounds={args.restart_rounds}")
        print(f"rdma_recv_slots={get_effective_tunable(args.rdma_recv_slots, 32)}")
        print(f"rdma_chunk_size={get_effective_tunable(args.rdma_chunk_size, 16384)}")
        print(f"rdma_qp_wr_depth={get_effective_tunable(args.rdma_qp_wr_depth, 64)}")
        print(f"restart_rounds_ok={'yes' if rounds_ok else 'no'}")
        print(f"soak_seconds={args.soak_seconds}")
        print(f"soak_writes={soak_writes}")
        print(f"soak_reconnects={soak_reconnects}")
        print(f"soak_ok={'yes' if soak_ok else 'no'}")
        print(f"soak_availability_ok={'yes' if soak_availability_ok else 'no'}")
        print(f"soak_recovery_ok={'yes' if soak_recovery_ok else 'no'}")
        print(f"soak_fail_key={soak_fail_key}")
        print(f"soak_fail_value={soak_fail_value}")
        print(f"soak_fail_seen={soak_fail_seen}")
        print(f"soak_fail_slave_link={soak_fail_slave_link}")
        print(f"soak_fail_slave_offset={soak_fail_slave_offset}")
        print(f"soak_fail_master_link={soak_fail_master_link}")
        print(f"failure_master_log_len={failure_master_log_len}")
        print(f"failure_slave_log_len={failure_slave_log_len}")
        print(f"failure_master_tail={failure_master_tail}")
        print(f"failure_slave_tail={failure_slave_tail}")
        print(f"partial_resync_continue_rounds={resumed_continue_rounds}")
        print(f"force_fallback={'yes' if args.force_fallback else 'no'}")
        print(f"final_resume_ok={'yes' if final_resume_ok else 'no'}")
        print(f"failure_slave_repl_transport={last_info.get('repl_transport', 'missing')}")
        print(f"failure_slave_master_link={last_info.get('master_link', 'missing')}")
        print(f"failure_slave_repl_offset={last_info.get('slave_repl_offset', 'missing')}")
        print(f"final_slave_repl_transport={final_info.get('repl_transport', 'missing')}")
        print(f"final_slave_master_link={final_info.get('master_link', 'missing')}")
        print(f"final_slave_repl_offset={final_info.get('slave_repl_offset', 'missing')}")
        print(f"rdma_slave_async_disconnect_seen={'yes' if slave_async_disconnect_seen else 'no'}")
        print(f"rdma_slave_async_disconnect_impactful={'yes' if slave_async_disconnect_impactful else 'no'}")
        print(f"rdma_master_async_disconnect_seen={'yes' if master_async_disconnect_seen else 'no'}")
        print(f"rdma_master_async_disconnect_impactful={'yes' if master_async_disconnect_impactful else 'no'}")
        print(f"rdma_continue_seen={'yes' if 'slave_parse - CONTINUE' in log_text else 'no'}")
        print(f"soak_failure_allowed={'yes' if args.allow_soak_failure else 'no'}")
        print(f"rdma_ebpf_enabled={'yes' if args.ebpf else 'no'}")
        print(f"rdma_ebpf_output={rdma_ebpf_out}")
        print(f"rdma_ebpf_error={rdma_ebpf_err}")
        print(f"rdma_ebpf_summary={rdma_ebpf_summary}")
        print(f"rdma_ebpf_returncode={ebpf_rc}")
        print(f"rdma_ebpf_uprobes_requested={len(ebpf.uprobe_specs) if ebpf else 0}")
        print(f"artifacts={root}")
        success = fullsync_done and postsync_ok and rounds_ok and final_resume_ok and (soak_recovery_ok or args.allow_soak_failure)
        return 0 if success else 1
    finally:
        if ebpf:
            ebpf_rc = ebpf.stop()
        stop_proc(slave)
        stop_proc(master)
        mlog.close()
        slog.close()


if __name__ == "__main__":
    sys.exit(main())
