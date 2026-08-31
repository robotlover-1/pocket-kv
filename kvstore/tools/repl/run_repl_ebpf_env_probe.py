#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
from pathlib import Path


def read_text(path: str) -> str:
    try:
        return Path(path).read_text().strip()
    except Exception:
        return "missing"


def run(cmd):
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except Exception as e:
        return 127, "", str(e)


def yesno(v: bool) -> str:
    return "yes" if v else "no"


def path_access_state(path: str) -> str:
    p = Path(path)
    try:
        if p.exists():
            return "yes"
        return "no"
    except PermissionError:
        return "permission-denied"
    except Exception:
        return "error"


def main() -> int:
    perf = shutil.which("perf")
    bpftrace = shutil.which("bpftrace")
    bpftool = shutil.which("bpftool")
    clang = shutil.which("clang")
    llvm_strip = shutil.which("llvm-strip")

    print("REPL_EBPF_ENV_PROBE")
    print(f"kernel_release={os.uname().release}")
    print(f"perf_available={yesno(bool(perf))}")
    print(f"bpftrace_available={yesno(bool(bpftrace))}")
    print(f"bpftool_available={yesno(bool(bpftool))}")
    print(f"clang_available={yesno(bool(clang))}")
    print(f"llvm_strip_available={yesno(bool(llvm_strip))}")
    print(f"tracefs_available={path_access_state('/sys/kernel/tracing')}")
    print(f"debugfs_tracing_available={path_access_state('/sys/kernel/debug/tracing')}")
    print(f"kallsyms_readable={yesno(Path('/proc/kallsyms').exists() and os.access('/proc/kallsyms', os.R_OK))}")
    print(f"perf_event_paranoid={read_text('/proc/sys/kernel/perf_event_paranoid')}")
    print(f"unprivileged_bpf_disabled={read_text('/proc/sys/kernel/unprivileged_bpf_disabled')}")

    if perf:
        rc, out, err = run([perf, "--version"])
        print(f"perf_version_rc={rc}")
        print(f"perf_version={(out or err or 'missing').splitlines()[0] if (out or err) else 'missing'}")
    else:
        print("perf_version_rc=missing")
        print("perf_version=missing")

    if bpftrace:
        rc, out, err = run([bpftrace, "--version"])
        print(f"bpftrace_version_rc={rc}")
        print(f"bpftrace_version={(out or err or 'missing').splitlines()[0] if (out or err) else 'missing'}")
    else:
        print("bpftrace_version_rc=missing")
        print("bpftrace_version=missing")

    if bpftool:
        rc, out, err = run([bpftool, "version"])
        text = out or err or "missing"
        first = text.splitlines()[0] if text else "missing"
        print(f"bpftool_version_rc={rc}")
        print(f"bpftool_version={first}")
    else:
        print("bpftool_version_rc=missing")
        print("bpftool_version=missing")

    if perf:
        rc, out, err = run([perf, "stat", "-e", "task-clock", "true"])
        print(f"perf_stat_true_rc={rc}")
        print(f"perf_stat_true_stderr={(err.splitlines()[0] if err else 'missing')}")
    else:
        print("perf_stat_true_rc=missing")
        print("perf_stat_true_stderr=missing")

    if bpftrace:
        print(f"bpftrace_root_required={'yes' if os.geteuid() != 0 else 'no'}")
        rc, out, err = run([bpftrace, "-l", "tracepoint:syscalls:sys_enter_*"])
        print(f"bpftrace_list_rc={rc}")
        if out:
            lines = out.splitlines()
            print(f"bpftrace_list_sample={lines[0]}")
            print(f"bpftrace_list_count_hint={len(lines)}")
        else:
            print(f"bpftrace_list_sample={(err.splitlines()[0] if err else 'missing')}")
            print("bpftrace_list_count_hint=0")
    else:
        print("bpftrace_list_rc=missing")
        print("bpftrace_list_sample=missing")
        print("bpftrace_list_count_hint=0")

    readiness = []
    if perf:
        readiness.append("perf")
    if bpftrace or bpftool:
        readiness.append("ebpf-basic")
    if bpftrace and clang:
        readiness.append("ebpf-advanced")
    print(f"profiling_readiness={','.join(readiness) if readiness else 'minimal'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
