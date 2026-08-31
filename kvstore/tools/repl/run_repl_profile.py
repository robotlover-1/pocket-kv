#!/usr/bin/env python3
import argparse
import ctypes.util
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


def run_cmd(cmd, cwd: Path):
    return subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def run_local(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)


def symbol_exists(binary: Path, symbol: str) -> bool:
    if not binary.exists():
        return False
    for cmd in (["nm", "-D", str(binary)], ["nm", str(binary)]):
        if not shutil.which(cmd[0]):
            continue
        proc = run_local(cmd)
        if proc.returncode != 0:
            continue
        for line in proc.stdout.splitlines():
            if line.rstrip().endswith(f" {symbol}") or line.rstrip().endswith(f" {symbol}@plt"):
                return True
    return False


def resolve_library_path(lib_hint: str):
    candidate = ctypes.util.find_library(lib_hint)
    names = []
    if candidate:
        names.append(candidate)
    names.extend([f"lib{lib_hint}.so", f"lib{lib_hint}.so.1"])
    seen = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        if "/" in name:
            p = Path(name)
            if p.exists():
                return p.resolve()
        if shutil.which("ldconfig"):
            proc = run_local(["ldconfig", "-p"])
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    if name in line and "=>" in line:
                        p = Path(line.split("=>", 1)[1].strip())
                        if p.exists():
                            return p.resolve()
        for base in ("/lib", "/lib64", "/usr/lib", "/usr/lib64", "/usr/local/lib", "/lib/x86_64-linux-gnu", "/usr/lib/x86_64-linux-gnu"):
            p = Path(base) / name
            if p.exists():
                return p.resolve()
    return None


def build_uprobe_specs(kvstore_bin: Path):
    specs = []
    for sym in (
        "repl_transport_send",
        "repl_handle_replica_send_failure",
        "repl_backlog_send_continue",
        "parse_resp_stream",
        "repl_rdma_try_send",
        "repl_rdma_wait_cq_recv_completion",
    ):
        if symbol_exists(kvstore_bin, sym):
            specs.append((kvstore_bin, sym, f"kvstore:{sym}"))

    librdmacm = resolve_library_path("rdmacm")
    if librdmacm:
        for sym in ("rdma_connect", "rdma_accept", "rdma_get_cm_event", "rdma_resolve_addr", "rdma_resolve_route"):
            if symbol_exists(librdmacm, sym):
                specs.append((librdmacm, sym, f"rdmacm:{sym}"))

    libibverbs = resolve_library_path("ibverbs")
    if libibverbs:
        for sym in ("ibv_post_send", "ibv_post_recv", "ibv_poll_cq"):
            if symbol_exists(libibverbs, sym):
                specs.append((libibverbs, sym, f"ibverbs:{sym}"))
    return specs


def build_bpftrace_script(bench_dir: Path, uprobe_specs):
    lines = [
        "#!/usr/bin/env bpftrace",
        "BEGIN",
        "{",
        '    printf("REPL_EBPF_TRACE_BEGIN\\n");',
        "}",
        'tracepoint:syscalls:sys_enter_read /comm == "kvstore"/ { @syscalls["read"] = count(); }',
        'tracepoint:syscalls:sys_enter_write /comm == "kvstore"/ { @syscalls["write"] = count(); }',
        'tracepoint:syscalls:sys_enter_recvfrom /comm == "kvstore"/ { @syscalls["recvfrom"] = count(); }',
        'tracepoint:syscalls:sys_enter_sendto /comm == "kvstore"/ { @syscalls["sendto"] = count(); }',
        'tracepoint:syscalls:sys_enter_recvmsg /comm == "kvstore"/ { @syscalls["recvmsg"] = count(); }',
        'tracepoint:syscalls:sys_enter_sendmsg /comm == "kvstore"/ { @syscalls["sendmsg"] = count(); }',
        'tracepoint:syscalls:sys_enter_epoll_wait /comm == "kvstore"/ { @syscalls["epoll_wait"] = count(); }',
        'tracepoint:syscalls:sys_enter_poll /comm == "kvstore"/ { @syscalls["poll"] = count(); }',
        'tracepoint:syscalls:sys_enter_futex /comm == "kvstore"/ { @syscalls["futex"] = count(); }',
        'tracepoint:sched:sched_switch /args->prev_comm == "kvstore"/ { @sched["switch_out"] = count(); }',
        'tracepoint:sched:sched_switch /args->next_comm == "kvstore"/ { @sched["switch_in"] = count(); }',
    ]
    for binary, symbol, label in uprobe_specs:
        path = str(binary).replace('"', '\\"')
        tag = label.replace('"', '\\"')
        lines.append(f'uprobe:{path}:{symbol} {{ @uprobes["{tag}"] = count(); }}')
    lines.extend([
        "END",
        "{",
        '    printf("REPL_EBPF_TRACE_END\\n");',
        "    print(@syscalls);",
        "    print(@sched);",
        "    print(@uprobes);",
        "}",
    ])
    return "\n".join(lines) + "\n"


MAP_LINE = re.compile(r'^@(?P<group>[A-Za-z0-9_]+)\[(?P<key>.+)\]:\s+(?P<value>\d+)$')


def strip_bpftrace_key(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1]
    return text


def summarize_bpftrace_output(raw_path: Path, summary_path: Path, uprobe_specs):
    text = raw_path.read_text(errors="ignore") if raw_path.exists() else ""
    grouped = {"syscalls": {}, "sched": {}, "uprobes": {}}
    for line in text.splitlines():
        m = MAP_LINE.match(line.strip())
        if not m:
            continue
        group = m.group("group")
        if group in grouped:
            grouped[group][strip_bpftrace_key(m.group("key"))] = int(m.group("value"))
    lines = ["REPL_EBPF_SUMMARY", f"raw_output={raw_path}", f"uprobes_requested={len(uprobe_specs)}", f"uprobes_observed={len(grouped['uprobes'])}"]
    for group in ("syscalls", "sched", "uprobes"):
        entries = grouped[group]
        lines.append(f"{group}_count={len(entries)}")
        for key in sorted(entries):
            safe_key = key.replace(":", "_").replace("/", "_").replace(" ", "_")
            lines.append(f"{group}_{safe_key}={entries[key]}")
    summary_path.write_text("\n".join(lines) + "\n")


def start_bpftrace(root: Path, bench_dir: Path, kvstore_bin: Path):
    bpftrace_bin = shutil.which("bpftrace")
    if not bpftrace_bin:
        return None, None, None, None, [], "bpftrace missing"
    if os.geteuid() != 0:
        return None, None, None, None, [], "bpftrace requires root privileges"
    script_path = root / "repl-profile.bt"
    stdout_path = root / "bpftrace.out"
    stderr_path = root / "bpftrace.err"
    summary_path = root / "ebpf-summary.txt"
    uprobe_specs = build_uprobe_specs(kvstore_bin)
    script_path.write_text(build_bpftrace_script(bench_dir, uprobe_specs))
    out_f = open(stdout_path, "w")
    err_f = open(stderr_path, "w")
    proc = subprocess.Popen([bpftrace_bin, str(script_path)], stdout=out_f, stderr=err_f, text=True, preexec_fn=os.setsid)
    time.sleep(1.0)
    if proc.poll() is not None:
        out_f.close()
        err_f.close()
        err_preview = "bpftrace exited immediately"
        try:
            err_text = stderr_path.read_text(errors="ignore").strip()
            if err_text:
                err_preview = err_text.splitlines()[0]
        except Exception:
            pass
        return None, stdout_path, stderr_path, summary_path, uprobe_specs, err_preview
    return (proc, out_f, err_f), stdout_path, stderr_path, summary_path, uprobe_specs, ""


def stop_bpftrace(proc_bundle):
    if not proc_bundle:
        return 0
    proc, out_f, err_f = proc_bundle
    rc = 0
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        rc = proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            rc = proc.wait(timeout=2)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                rc = proc.wait(timeout=1)
            except Exception:
                rc = 124
    out_f.close()
    err_f.close()
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(description="replication profiling helper for stage 9.2")
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--master-port", type=int, default=5188)
    ap.add_argument("--slave-port", type=int, default=5189)
    ap.add_argument("--preload-count", type=int, default=5000)
    ap.add_argument("--tail-count", type=int, default=1000)
    ap.add_argument("--sync-wait", type=float, default=6.0)
    ap.add_argument("--restart-wait", type=float, default=3.0)
    ap.add_argument("--work-dir", default="./artifacts/repl/profile")
    ap.add_argument("--perf", action="store_true", help="also run perf stat around the metrics bench when available")
    ap.add_argument("--ebpf", action="store_true", help="also run bpftrace-based eBPF tracing around the metrics bench when available")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    metrics_out = root / "metrics.txt"
    perf_out = root / "perf-stat.txt"
    bench_root = root / "bench-artifacts"
    kvstore_bin = Path(args.bin).resolve()
    bpftrace_out = root / "bpftrace.out"
    bpftrace_err = root / "bpftrace.err"
    ebpf_summary = root / "ebpf-summary.txt"

    bench_cmd = [
        "python3", "./tools/repl/run_repl_metrics_bench.py",
        "--bin", args.bin,
        "--host", args.host,
        "--master-port", str(args.master_port),
        "--slave-port", str(args.slave_port),
        "--preload-count", str(args.preload_count),
        "--tail-count", str(args.tail_count),
        "--sync-wait", str(args.sync_wait),
        "--restart-wait", str(args.restart_wait),
        "--work-dir", str(bench_root),
    ]

    cwd = Path(__file__).resolve().parent.parent.parent
    ebpf_proc = None
    ebpf_start_error = ""
    uprobe_specs = []

    if args.ebpf:
        ebpf_proc, bpftrace_out, bpftrace_err, ebpf_summary, uprobe_specs, ebpf_start_error = start_bpftrace(root, bench_root, kvstore_bin)
        if not ebpf_proc:
            print("REPL_PROFILE_HELPER_FAIL")
            print("ebpf_enabled=requested")
            print(f"ebpf_start_error={ebpf_start_error}")
            print(f"ebpf_requires_root={'yes' if 'root' in ebpf_start_error else 'no'}")
            print(f"bpftrace_output={bpftrace_out}")
            print(f"bpftrace_error={bpftrace_err}")
            print(f"ebpf_summary={ebpf_summary}")
            return 1

    try:
        metrics = run_cmd(bench_cmd, cwd)
        metrics_out.write_text(metrics.stdout + ("\n[stderr]\n" + metrics.stderr if metrics.stderr else ""))
        if metrics.returncode != 0:
            sys.stdout.write(metrics.stdout)
            sys.stderr.write(metrics.stderr)
            return metrics.returncode
    finally:
        ebpf_rc = stop_bpftrace(ebpf_proc) if ebpf_proc else 0

    if args.ebpf:
        summarize_bpftrace_output(bpftrace_out, ebpf_summary, uprobe_specs)

    print("REPL_PROFILE_HELPER_PASS")
    print(f"metrics_output={metrics_out}")

    perf_bin = shutil.which("perf")
    if args.perf and perf_bin:
        perf_cmd = [
            perf_bin, "stat", "-x", ",", "-o", str(perf_out),
            "python3", "./tools/repl/run_repl_metrics_bench.py",
            "--bin", args.bin,
            "--host", args.host,
            "--master-port", str(args.master_port + 10),
            "--slave-port", str(args.slave_port + 10),
            "--preload-count", str(args.preload_count),
            "--tail-count", str(args.tail_count),
            "--sync-wait", str(args.sync_wait),
            "--restart-wait", str(args.restart_wait),
            "--work-dir", str(root / "perf-bench-artifacts"),
        ]
        perf = run_cmd(perf_cmd, cwd)
        print("perf_enabled=yes")
        print(f"perf_output={perf_out}")
        print(f"perf_returncode={perf.returncode}")
        if perf.stdout.strip():
            print("perf_stdout_begin")
            print(perf.stdout.rstrip())
            print("perf_stdout_end")
        if perf.stderr.strip():
            print("perf_stderr_begin")
            print(perf.stderr.rstrip())
            print("perf_stderr_end")
    else:
        print("perf_enabled=no")
        print(f"perf_requested={'yes' if args.perf else 'no'}")
        print(f"perf_available={'yes' if perf_bin else 'no'}")

    bpftrace_bin = shutil.which("bpftrace")
    if args.ebpf:
        print("ebpf_enabled=yes")
        print(f"ebpf_tool={bpftrace_bin if bpftrace_bin else 'missing'}")
        print(f"ebpf_output={bpftrace_out}")
        print(f"ebpf_error={bpftrace_err}")
        print(f"ebpf_summary={ebpf_summary}")
        print(f"ebpf_returncode={ebpf_rc}")
        print(f"ebpf_uprobes_requested={len(uprobe_specs)}")
    else:
        print("ebpf_enabled=no")
        print(f"ebpf_requested={'yes' if args.ebpf else 'no'}")
        print(f"ebpf_available={'yes' if bpftrace_bin else 'no'}")

    print(f"artifacts={root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
