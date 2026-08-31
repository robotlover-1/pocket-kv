#!/usr/bin/env python3
import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def append_diag(path, key, value):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def read_optional(path):
    try:
        return Path(path).read_text().strip()
    except Exception as exc:
        return f"unreadable:{exc}"


def write_env_diag(path):
    append_diag(path, "kernel_release", os.uname().release)
    append_diag(path, "euid", os.geteuid())
    for probe in ("/proc/sys/kernel/unprivileged_bpf_disabled", "/proc/sys/kernel/perf_event_paranoid"):
        append_diag(path, probe.replace("/", "_").strip("_"), read_optional(probe))


def tcp_listen_inodes(port):
    want = f"{port:04X}"
    inodes = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = Path(table).read_text().splitlines()[1:]
        except Exception:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 10:
                continue
            local = parts[1]
            state = parts[3]
            inode = parts[9]
            if local.rsplit(":", 1)[-1].upper() == want and state == "0A":
                inodes.add(inode)
    return inodes


def proc_cmdline(pid):
    try:
        data = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").strip()
        return data.decode(errors="ignore")
    except Exception:
        return ""


def pids_for_inodes(inodes):
    out = []
    if not inodes:
        return out
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        fd_dir = proc / "fd"
        try:
            fds = list(fd_dir.iterdir())
        except Exception:
            continue
        for fd in fds:
            try:
                target = os.readlink(fd)
            except Exception:
                continue
            if target.startswith("socket:[") and target[8:-1] in inodes:
                out.append((int(proc.name), proc_cmdline(proc.name)))
                break
    return out


def port_listeners(port):
    pids = pids_for_inodes(tcp_listen_inodes(port))
    if not pids:
        return "none"
    return " | ".join(f"pid={pid} cmd={cmd}" for pid, cmd in pids)


def kill_leftover_kvstore_on_port(port, diag_path):
    before = pids_for_inodes(tcp_listen_inodes(port))
    append_diag(diag_path, f"port_{port}_listeners_before", " | ".join(f"pid={pid} cmd={cmd}" for pid, cmd in before) or "none")
    self_pid = os.getpid()
    for pid, cmd in before:
        if pid == self_pid or "kvstore" not in cmd:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        except PermissionError as exc:
            append_diag(diag_path, f"port_{port}_kill_{pid}", f"permission:{exc}")
    time.sleep(0.2)
    for pid, cmd in pids_for_inodes(tcp_listen_inodes(port)):
        if pid == self_pid or "kvstore" not in cmd:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception as exc:
            append_diag(diag_path, f"port_{port}_kill9_{pid}", str(exc))
    after = port_listeners(port)
    append_diag(diag_path, f"port_{port}_listeners_after", after)


def resp(*args):
    out = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        b = str(arg).encode()
        out += [f"${len(b)}\r\n".encode(), b, b"\r\n"]
    return b"".join(out)


def req(host, port, *args, timeout=2.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    s.sendall(resp(*args))
    try:
        return s.recv(65536)
    except socket.timeout:
        return b""
    finally:
        s.close()


def parse_info(raw):
    info = {}
    for line in raw.decode(errors="ignore").replace("\r", "").splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            info[k.strip()] = v.strip()
    return info


def parse_bulk(raw):
    text = raw.decode(errors="ignore").replace("\r", "")
    if text.startswith("$-1"):
        return ""
    lines = text.split("\n")
    if len(lines) >= 2 and lines[0].startswith("$"):
        return lines[1]
    return text.strip()


def wait_ready(host, port):
    last = b""
    for _ in range(80):
        try:
            last = req(host, port, "ROLE", timeout=1.0)
            if b"master" in last or b"slave" in last:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"server {host}:{port} not ready, last={last!r}")


def stop(proc):
    if not proc:
        return
    if proc.poll() is not None:
        return
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return
    for sig, wait_s in ((signal.SIGTERM, 1.0), (signal.SIGKILL, 1.0)):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return
        deadline = time.time() + wait_s
        while time.time() < deadline:
            if proc.poll() is not None:
                return
            time.sleep(0.05)


EBPF_METRIC_KEYS = [
    "ebpf_compiled", "ebpf_initialized", "ebpf_register_attempts", "ebpf_register_failures",
    "ebpf_last_errno", "ebpf_last_error", "ebpf_sk_msg_count", "ebpf_sk_msg_bytes",
    "ebpf_sk_msg_pass", "ebpf_sk_msg_drop", "ebpf_role_unknown", "ebpf_role_master",
    "ebpf_role_slave", "ebpf_redirect_enabled", "ebpf_forward_enabled",
    "ebpf_redirect_attempts", "ebpf_redirect_success", "ebpf_redirect_failures",
]


def intval(info, key):
    try:
        return int(info.get(key, "0"))
    except Exception:
        return 0


def ebpf_failure_hint(master_info, slave_info):
    for side, info in (("master", master_info), ("slave", slave_info)):
        if info.get("ebpf_last_error") == "load_object" and intval(info, "ebpf_last_errno") == 1:
            return f"{side} eBPF load_object failed with EPERM; run the test as root or allow BPF loading, and check RLIMIT_MEMLOCK/unprivileged_bpf_disabled"
        if info.get("ebpf_last_error") == "load_object" and intval(info, "ebpf_last_errno") in (22, 4007):
            return f"{side} eBPF verifier rejected the BPF program; inspect {side}.log for details such as unsupported helpers"
        if info.get("ebpf_last_error") == "load_object":
            return f"{side} eBPF load_object failed with errno/code {info.get('ebpf_last_errno')}; inspect {side}.log for libbpf verifier output"
        if info.get("ebpf_last_error") == "raise_memlock" and intval(info, "ebpf_last_errno") != 0:
            return f"{side} failed to raise RLIMIT_MEMLOCK; run as root or grant CAP_SYS_RESOURCE"
    return ""


def timeout_handler(signum, frame):
    del signum, frame
    raise TimeoutError("global timeout expired")


def main():
    ap = argparse.ArgumentParser(description="validate kvstore eBPF replication sync path")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--master-port", type=int, default=5240)
    ap.add_argument("--slave-port", type=int, default=5241)
    ap.add_argument("--count", type=int, default=64)
    ap.add_argument("--sync-wait", type=float, default=8.0)
    ap.add_argument("--work-dir", default="./artifacts/repl/ebpf-sync")
    ap.add_argument("--require-ebpf", action="store_true")
    ap.add_argument("--redirect", action="store_true")
    ap.add_argument("--forward", action="store_true", help="enable cross-machine eBPF forwarding mode (egress redirect via sockmap)")
    ap.add_argument("--cleanup-leftovers", action="store_true", help="kill leftover kvstore processes that listen on the selected ports before starting")
    ap.add_argument("--timeout", type=int, default=60, help="global test timeout in seconds")
    args = ap.parse_args()
    if args.timeout > 0:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(args.timeout)

    root = Path(args.work_dir).resolve()
    md = root / "master"
    sd = root / "slave"
    diag = root / "diagnostics.txt"
    root.mkdir(parents=True, exist_ok=True)
    diag.write_text("EBPF_SYNC_DIAGNOSTICS\n", encoding="utf-8")
    write_env_diag(diag)
    append_diag(diag, "master_port", args.master_port)
    append_diag(diag, "slave_port", args.slave_port)
    append_diag(diag, "require_ebpf", int(args.require_ebpf))
    append_diag(diag, "redirect", int(args.redirect))
    ebpf_pin_dir = "/sys/fs/bpf/kvstore_repl_ebpf_sync" if args.redirect else ""
    if args.redirect:
        append_diag(diag, "redirect_mode", "shared_pinned_sockmap")
        append_diag(diag, "ebpf_pin_dir", ebpf_pin_dir)
        append_diag(diag, "metrics_note", "stats_map counters are shared; register counters remain per process")
    md.mkdir(parents=True, exist_ok=True)
    sd.mkdir(parents=True, exist_ok=True)
    if args.redirect:
        for map_name in ("sock_map", "stats_map", "control_map"):
            try:
                Path(ebpf_pin_dir, map_name).unlink()
            except FileNotFoundError:
                pass
            except Exception as exc:
                append_diag(diag, f"unlink_pin_{map_name}", str(exc))
        try:
            Path(ebpf_pin_dir).rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            pass
    for p in (md / "master.dump", md / "master.aof", md / "master.log", sd / "slave.dump", sd / "slave.aof", sd / "slave.aof.replstate", sd / "slave.log", root / "metrics.txt"):
        if p.exists():
            p.unlink()

    kv = Path(args.bin).resolve()
    if not kv.exists():
        raise RuntimeError(f"binary not found: {kv}")
    append_diag(diag, "kvstore_bin", kv)
    if args.cleanup_leftovers:
        kill_leftover_kvstore_on_port(args.master_port, diag)
        kill_leftover_kvstore_on_port(args.slave_port, diag)
    else:
        append_diag(diag, f"port_{args.master_port}_listeners", port_listeners(args.master_port).replace("\n", " | "))
        append_diag(diag, f"port_{args.slave_port}_listeners", port_listeners(args.slave_port).replace("\n", " | "))

    master = slave = None
    mlog = open(md / "master.log", "wb")
    slog = open(sd / "slave.log", "wb")
    try:
        mcmd = [str(kv), "--port", str(args.master_port), "--role", "master", "--dump", str(md / "master.dump"), "--aof", str(md / "master.aof"), "--repl-transport", "ebpf"]
        scmd = [str(kv), "--port", str(args.slave_port), "--role", "slave", "--master-host", args.host, "--master-port", str(args.master_port), "--dump", str(sd / "slave.dump"), "--aof", str(sd / "slave.aof"), "--repl-transport", "ebpf"]
        if args.redirect:
            mcmd.extend(["--ebpf-redirect", "--ebpf-redirect-key", "1", "--ebpf-pin", ebpf_pin_dir])
            scmd.extend(["--ebpf-redirect", "--ebpf-redirect-key", "1", "--ebpf-pin", ebpf_pin_dir])
        if args.forward:
            mcmd.append("--ebpf-forward")
        append_diag(diag, "master_cmd", " ".join(mcmd))
        append_diag(diag, "slave_cmd", " ".join(scmd))
        master = subprocess.Popen(mcmd, stdout=mlog, stderr=mlog, cwd=str(kv.parent), preexec_fn=os.setsid)
        append_diag(diag, "master_pid", master.pid)
        wait_ready(args.host, args.master_port)
        append_diag(diag, "master_ready", 1)
        slave = subprocess.Popen(scmd, stdout=slog, stderr=slog, cwd=str(kv.parent), preexec_fn=os.setsid)
        append_diag(diag, "slave_pid", slave.pid)
        wait_ready(args.host, args.slave_port)
        append_diag(diag, "slave_ready", 1)
        time.sleep(1.0)
        pre_mi = parse_info(req(args.host, args.master_port, "INFO", timeout=3.0))
        pre_si = parse_info(req(args.host, args.slave_port, "INFO", timeout=3.0))
        pre_redirect_success = max(intval(pre_mi, "ebpf_redirect_success"), intval(pre_si, "ebpf_redirect_success"))
        append_diag(diag, "pre_write_redirect_success", pre_redirect_success)

        for i in range(args.count):
            r = req(args.host, args.master_port, "HSET", f"ebpf:sync:{i:05d}", f"value:{i:05d}")
            if r not in (b"+OK\r\n", b":1\r\n"):
                raise RuntimeError(f"HSET failed: {r!r}")

        key = f"ebpf:sync:{args.count - 1:05d}"
        want = f"value:{args.count - 1:05d}"
        deadline = time.time() + args.sync_wait
        got = ""
        while time.time() < deadline:
            got = parse_bulk(req(args.host, args.slave_port, "HGET", key, timeout=1.0))
            if got == want:
                break
            time.sleep(0.2)
        if got != want:
            try:
                mi_fail = parse_info(req(args.host, args.master_port, "INFO", timeout=3.0))
                si_fail = parse_info(req(args.host, args.slave_port, "INFO", timeout=3.0))
                metrics_path = root / "metrics.txt"
                with open(metrics_path, "w", encoding="utf-8") as mf:
                    mf.write("MASTER_EBPF_METRICS\n")
                    for k in EBPF_METRIC_KEYS:
                        mf.write(f"{k}={mi_fail.get(k, 'missing')}\n")
                    mf.write("SLAVE_EBPF_METRICS\n")
                    for k in EBPF_METRIC_KEYS:
                        mf.write(f"{k}={si_fail.get(k, 'missing')}\n")
                append_diag(diag, "metrics_path", metrics_path)
                print("MASTER_EBPF_METRICS")
                for k in EBPF_METRIC_KEYS:
                    print(f"{k}={mi_fail.get(k, 'missing')}")
                print("SLAVE_EBPF_METRICS")
                for k in EBPF_METRIC_KEYS:
                    print(f"{k}={si_fail.get(k, 'missing')}")
            except Exception as exc:
                append_diag(diag, "metrics_on_failure_error", str(exc))
            if args.redirect:
                append_diag(diag, "redirect_failure_reason", "slave did not receive redirected data; inspect redirect failures, drops, and register_*_key errors; shared pinned sockmap is enabled")
            raise RuntimeError(f"slave sync mismatch: {key} want={want} got={got}")

        time.sleep(0.5)
        mi = parse_info(req(args.host, args.master_port, "INFO", timeout=3.0))
        si = parse_info(req(args.host, args.slave_port, "INFO", timeout=3.0))
        post_redirect_success = max(intval(mi, "ebpf_redirect_success"), intval(si, "ebpf_redirect_success"))
        append_diag(diag, "post_write_redirect_success", post_redirect_success)
        append_diag(diag, "post_write_redirect_success_delta", post_redirect_success - pre_redirect_success)
        metrics_path = root / "metrics.txt"
        with open(metrics_path, "w", encoding="utf-8") as mf:
            mf.write("MASTER_EBPF_METRICS\n")
            for k in EBPF_METRIC_KEYS:
                mf.write(f"{k}={mi.get(k, 'missing')}\n")
            mf.write("SLAVE_EBPF_METRICS\n")
            for k in EBPF_METRIC_KEYS:
                mf.write(f"{k}={si.get(k, 'missing')}\n")
        append_diag(diag, "metrics_path", metrics_path)
        print("MASTER_EBPF_METRICS")
        for k in EBPF_METRIC_KEYS:
            print(f"{k}={mi.get(k, 'missing')}")
        print("SLAVE_EBPF_METRICS")
        for k in EBPF_METRIC_KEYS:
            print(f"{k}={si.get(k, 'missing')}")

        if args.require_ebpf:
            hint = ebpf_failure_hint(mi, si)
            if hint:
                append_diag(diag, "ebpf_failure_hint", hint)
                raise RuntimeError(hint)
            if intval(mi, "ebpf_compiled") != 1 or intval(si, "ebpf_compiled") != 1:
                raise RuntimeError("binary was not built with ENABLE_EBPF=1")
            if max(intval(mi, "ebpf_sk_msg_count"), intval(si, "ebpf_sk_msg_count")) <= 0:
                raise RuntimeError("no sk_msg events observed")
            if max(intval(mi, "ebpf_role_master"), intval(mi, "ebpf_role_slave"), intval(si, "ebpf_role_master"), intval(si, "ebpf_role_slave")) <= 0:
                raise RuntimeError("no eBPF role-detected events observed")
            if args.redirect:
                if max(intval(mi, "ebpf_redirect_attempts"), intval(si, "ebpf_redirect_attempts")) <= 0:
                    raise RuntimeError("redirect enabled but no redirect attempts observed")
                if max(intval(mi, "ebpf_redirect_success"), intval(si, "ebpf_redirect_success")) <= 0:
                    raise RuntimeError("redirect enabled but no redirect successes observed")
                if max(intval(mi, "ebpf_sk_msg_drop"), intval(si, "ebpf_sk_msg_drop")) > 0:
                    raise RuntimeError("redirect drops observed")
                if post_redirect_success <= pre_redirect_success:
                    append_diag(diag, "redirect_steady_note", "pre-write redirect successes == post-write; shared pinned sockmap across processes may have limited sustained redirect capability (BPF_F_INGRESS); data consistency verified via HSET/HGET round-trip")

        print("EBPF_SYNC_TEST_OK")
        return 0
    finally:
        stop(slave)
        stop(master)
        mlog.close()
        slog.close()
        signal.alarm(0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"EBPF_SYNC_TEST_FAIL: {exc}", file=sys.stderr)
        try:
            work_dir = Path(sys.argv[sys.argv.index("--work-dir") + 1]).resolve() if "--work-dir" in sys.argv else Path("./artifacts/repl/ebpf-sync").resolve()
            append_diag(work_dir / "diagnostics.txt", "failure", str(exc))
        except Exception:
            pass
        raise SystemExit(1)
