#!/usr/bin/env python3
import ctypes.util
import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path


MAP_LINE = re.compile(r'^@(?P<group>[A-Za-z0-9_]+)\[(?P<key>.+)\]:\s+(?P<value>\d+)$')


def _run_local(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)


def _symbol_exists(binary: Path, symbol: str) -> bool:
    if not binary.exists():
        return False
    for cmd in (["nm", "-D", str(binary)], ["nm", str(binary)]):
        if not shutil.which(cmd[0]):
            continue
        proc = _run_local(cmd)
        if proc.returncode != 0:
            continue
        for line in proc.stdout.splitlines():
            if line.rstrip().endswith(f" {symbol}") or line.rstrip().endswith(f" {symbol}@plt"):
                return True
    return False


def _resolve_library_path(lib_hint: str):
    candidate = ctypes.util.find_library(lib_hint)
    names = [candidate] if candidate else []
    names.extend([f"lib{lib_hint}.so", f"lib{lib_hint}.so.1"])
    for name in names:
        if not name:
            continue
        if "/" in name:
            p = Path(name)
            if p.exists():
                return p.resolve()
        if shutil.which("ldconfig"):
            proc = _run_local(["ldconfig", "-p"])
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


def _build_uprobe_specs(kvstore_bin: Path):
    specs = []
    for sym in (
        "repl_transport_send",
        "repl_handle_replica_send_failure",
        "repl_backlog_send_continue",
        "parse_resp_stream",
        "repl_rdma_try_send",
        "repl_rdma_wait_cq_recv_completion",
    ):
        if _symbol_exists(kvstore_bin, sym):
            specs.append((str(kvstore_bin), sym, f"kvstore:{sym}"))
    librdmacm = _resolve_library_path("rdmacm")
    if librdmacm:
        for sym in ("rdma_connect", "rdma_accept", "rdma_get_cm_event", "rdma_resolve_addr", "rdma_resolve_route"):
            if _symbol_exists(librdmacm, sym):
                specs.append((str(librdmacm), sym, f"rdmacm:{sym}"))
    libibverbs = _resolve_library_path("ibverbs")
    if libibverbs:
        for sym in ("ibv_post_send", "ibv_post_recv", "ibv_poll_cq"):
            if _symbol_exists(libibverbs, sym):
                specs.append((str(libibverbs), sym, f"ibverbs:{sym}"))
    return specs


def _build_bpftrace_script(uprobe_specs):
    lines = [
        "#!/usr/bin/env bpftrace",
        "BEGIN",
        "{",
        '    printf("REPL_EBPF_BEGIN\\n");',
        "}",
        'tracepoint:syscalls:sys_enter_read /comm == "kvstore"/ { @syscalls["read"] = count(); }',
        'tracepoint:syscalls:sys_enter_write /comm == "kvstore"/ { @syscalls["write"] = count(); }',
        'tracepoint:syscalls:sys_enter_recvfrom /comm == "kvstore"/ { @syscalls["recvfrom"] = count(); }',
        'tracepoint:syscalls:sys_enter_sendto /comm == "kvstore"/ { @syscalls["sendto"] = count(); }',
        'tracepoint:syscalls:sys_enter_epoll_wait /comm == "kvstore"/ { @syscalls["epoll_wait"] = count(); }',
        'tracepoint:syscalls:sys_enter_poll /comm == "kvstore"/ { @syscalls["poll"] = count(); }',
        'tracepoint:sched:sched_switch /args->prev_comm == "kvstore"/ { @sched["switch_out"] = count(); }',
        'tracepoint:sched:sched_switch /args->next_comm == "kvstore"/ { @sched["switch_in"] = count(); }',
    ]
    for path, symbol, label in uprobe_specs:
        lines.append(f'uprobe:{path}:{symbol} {{ @uprobes["{label}"] = count(); }}')
    lines.extend([
        "END",
        "{",
        '    printf("REPL_EBPF_END\\n");',
        "    print(@syscalls);",
        "    print(@sched);",
        "    print(@uprobes);",
        "}",
    ])
    return "\n".join(lines) + "\n"


def _summarize(raw_path: Path, summary_path: Path, uprobe_specs):
    text = raw_path.read_text(errors="ignore") if raw_path.exists() else ""
    grouped = {"syscalls": {}, "sched": {}, "uprobes": {}}
    for line in text.splitlines():
        m = MAP_LINE.match(line.strip())
        if not m:
            continue
        group = m.group("group")
        if group in grouped:
            grouped[group][m.group("key").strip().strip('"')] = int(m.group("value"))
    lines = [
        "REPL_EBPF_SUMMARY",
        f"raw_output={raw_path}",
        f"uprobes_requested={len(uprobe_specs)}",
        f"uprobes_observed={len(grouped['uprobes'])}",
    ]
    for group in ("syscalls", "sched", "uprobes"):
        entries = grouped[group]
        lines.append(f"{group}_count={len(entries)}")
        for key in sorted(entries):
            safe = key.replace(":", "_").replace("/", "_").replace(" ", "_")
            lines.append(f"{group}_{safe}={entries[key]}")
    summary_path.write_text("\n".join(lines) + "\n")


class EbpfSession:
    def __init__(self, work_dir: Path, kvstore_bin: Path, prefix: str = "rdma"):
        self.work_dir = work_dir
        self.kvstore_bin = kvstore_bin
        self.prefix = prefix
        self.proc = None
        self.out_f = None
        self.err_f = None
        self.out_path = work_dir / f"{prefix}_ebpf.out"
        self.err_path = work_dir / f"{prefix}_ebpf.err"
        self.summary_path = work_dir / f"{prefix}_ebpf_summary.txt"
        self.script_path = work_dir / f"{prefix}-ebpf.bt"
        self.uprobe_specs = []
        self.returncode = 0
        self.start_error = ""

    def start(self) -> bool:
        bpftrace_bin = shutil.which("bpftrace")
        if not bpftrace_bin:
            self.start_error = "bpftrace missing"
            return False
        if os.geteuid() != 0:
            self.start_error = "bpftrace requires root privileges"
            return False
        self.uprobe_specs = _build_uprobe_specs(self.kvstore_bin)
        self.script_path.write_text(_build_bpftrace_script(self.uprobe_specs))
        self.out_f = open(self.out_path, "w")
        self.err_f = open(self.err_path, "w")
        self.proc = subprocess.Popen([bpftrace_bin, str(self.script_path)], stdout=self.out_f, stderr=self.err_f, text=True, preexec_fn=os.setsid)
        time.sleep(1.0)
        if self.proc.poll() is not None:
            self.out_f.close()
            self.err_f.close()
            self.proc = None
            try:
                text = self.err_path.read_text(errors="ignore").strip()
                self.start_error = text.splitlines()[0] if text else "bpftrace exited immediately"
            except Exception:
                self.start_error = "bpftrace exited immediately"
            return False
        return True

    def stop(self):
        if not self.proc:
            return 0
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            self.returncode = self.proc.wait(timeout=5)
        except Exception:
            self.returncode = 124
        self.out_f.close()
        self.err_f.close()
        _summarize(self.out_path, self.summary_path, self.uprobe_specs)
        return self.returncode
