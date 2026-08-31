#!/usr/bin/env python3
"""
Master test runner: executes all test targets from README test suite.
Reports PASS/FAIL per target, captures errors on failure.
Usage: python3 tools/tests/run_all_tests.py [--skip-rdma] [--skip-ebpf] [--skip-repl] [--skip-demo]
"""
import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

# All test targets in order, with metadata
ALL_TARGETS = [
    # ---- Basic ----
    ("check",          "基础功能全套 (resp+ttl+persist+doc)", None),
    ("check-bulk-1w",  "批量 1w 级全套回归",                 None),
    ("check-mass-ttl", "海量 TTL 压测",                      None),

    # ---- Persist ----
    ("check-uring-persist", "io_uring 持久化验证",        None),
    ("check-mmap-recover",  "mmap 恢复验证",              None),

    # ---- Replication ----
    ("check-repl",          "主从复制基本验证",            "repl"),
    ("check-repl-metrics",  "复制指标基线",                "repl"),
    ("check-repl-profile",  "复制 profiling",              "repl"),

    # ---- Demo (10w) ----
    ("check-demo-full-dump", "全量持久化演示 (10w)",       "demo"),
    ("check-demo-incr-aof",  "增量持久化演示 (10w)",       "demo"),
    ("check-demo-repl-sync", "主从同步演示 (5w+5w=10w)",   "demo"),

    # ---- RDMA ----
    ("check-repl-rdma-smoke",    "RDMA 冒烟测试",          "rdma"),
    ("check-repl-rdma-stress",   "RDMA 压力测试",          "rdma"),
    ("check-repl-rdma-soak",     "RDMA 长时浸泡",          "rdma"),

    # ---- eBPF ----
    ("check-repl-ebpf-env",      "eBPF 环境探测",          "ebpf"),

    # ---- Standalone probes ----
    ("check-rdma-standalone-probe", "RDMA 环境探测",       "rdma"),
    ("check-rdma-pingpong-smoke",   "RDMA pingpong 测试",  "rdma"),
]


def is_cmd_available(cmd: str) -> bool:
    from shutil import which
    return which(cmd) is not None


def has_rdma() -> bool:
    """Probe if RDMA environment is available."""
    # Check for soft-RoCE (rxe/siw)
    try:
        r = subprocess.run(["rdma", "link"], capture_output=True, text=True, timeout=5)
        if "rxe" in r.stdout.lower() or "siw" in r.stdout.lower() or "mlx" in r.stdout.lower():
            return True
    except Exception:
        pass
    # Check sysfs
    for d in Path("/sys/class/infiniband").iterdir():
        if d.is_dir():
            return True
    return False


def has_ebpf() -> bool:
    """Probe if eBPF environment is available."""
    if not is_cmd_available("bpftool"):
        return False
    try:
        r = subprocess.run(["bpftool", "prog"], capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def run_target(target: str, desc: str) -> tuple:
    """Run a make target, return (passed: bool, elapsed: float, output: str)."""
    t0 = time.perf_counter()
    try:
        r = subprocess.run(
            ["make", target],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=1800,  # 30 min max per target
        )
        elapsed = time.perf_counter() - t0
        passed = r.returncode == 0
        output = r.stdout + r.stderr
        return passed, elapsed, output
    except subprocess.TimeoutExpired:
        elapsed = time.perf_counter() - t0
        return False, elapsed, "TIMEOUT (>30min)"
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return False, elapsed, str(e)


def extract_failure(output: str) -> str:
    """Extract the relevant failure message from output."""
    lines = output.strip().splitlines()
    # Collect lines around FAIL / Error / Traceback
    fail_lines = []
    capture = False
    for i, line in enumerate(lines):
        upper = line.upper()
        if "FAIL" in upper or "ERROR" in upper or "TRACEBACK" in upper or "FATAL" in upper:
            capture = True
        if capture:
            # Include context: a few lines before and after
            start = max(0, i - 2)
            end = min(len(lines), i + 5)
            for j in range(start, end):
                if j >= len(lines):
                    break
                if j not in {x for x, _ in fail_lines}:
                    fail_lines.append((j, lines[j]))
    if not fail_lines:
        # No explicit FAIL found, show tail
        tail = lines[-15:] if len(lines) > 15 else lines
        return "\n".join(tail)
    fail_lines.sort(key=lambda x: x[0])
    return "\n".join(line for _, line in fail_lines[:30])


def main() -> int:
    ap = argparse.ArgumentParser(description="Master test runner for kvstore")
    ap.add_argument("--skip-rdma", action="store_true", help="Skip RDMA tests")
    ap.add_argument("--skip-ebpf", action="store_true", help="Skip eBPF tests")
    ap.add_argument("--skip-repl", action="store_true", help="Skip replication tests")
    ap.add_argument("--skip-demo", action="store_true", help="Skip 10w-level demo tests")
    ap.add_argument("--only", help="Comma-separated target names to run (e.g. check,check-bulk-1w)")
    args = ap.parse_args()

    # Probe environment
    rdma_ok = has_rdma()
    ebpf_ok = has_ebpf()

    # Determine which tests to run
    selected = []
    only_set = set(args.only.split(",")) if args.only else None

    for target, desc, category in ALL_TARGETS:
        if only_set and target not in only_set:
            continue
        if category == "rdma" and (args.skip_rdma or not rdma_ok):
            continue
        if category == "ebpf" and (args.skip_ebpf or not ebpf_ok):
            continue
        if category == "repl" and (args.skip_repl or (not rdma_ok and not ebpf_ok)):
            continue
        if category == "demo" and args.skip_demo:
            continue
        selected.append((target, desc, category))

    if not selected:
        print("No tests selected.")
        return 0

    print("=" * 70)
    print("kvstore Master Test Suite")
    print(f"  RDMA available: {'yes' if rdma_ok else 'no'}")
    print(f"  eBPF available: {'yes' if ebpf_ok else 'no'}")
    print(f"  Tests to run:   {len(selected)}")
    print("=" * 70)

    total_elapsed = 0.0
    passed = 0
    failed = 0
    skipped = 0
    failures = []

    overall_start = time.perf_counter()

    for idx, (target, desc, category) in enumerate(selected, 1):
        print(f"\n[{idx}/{len(selected)}] {target}")
        print(f"  {desc} ... ", end="", flush=True)

        ok, elapsed, output = run_target(target, desc)
        total_elapsed += elapsed

        if ok:
            passed += 1
            print(f"PASS ({elapsed:.1f}s)")
        else:
            failed += 1
            fail_info = extract_failure(output)
            print(f"FAIL ({elapsed:.1f}s)")
            failures.append((target, desc, fail_info, elapsed))
            # Print failure details immediately
            print(f"  --- FAILURE DETAILS ({target}) ---")
            for line in fail_info.splitlines()[:20]:
                print(f"  | {line}")
            print(f"  --- END FAILURE ---")

    overall_elapsed = time.perf_counter() - overall_start

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUITE SUMMARY")
    print("=" * 70)
    print(f"  Total:    {len(selected)}")
    print(f"  Passed:   {passed}")
    print(f"  Failed:   {failed}")
    print(f"  Skipped:  {skipped}")
    print(f"  Wall time: {overall_elapsed:.1f}s")
    print()

    if failures:
        print("FAILED TARGETS:")
        for target, desc, info, elapsed in failures:
            print(f"  [{target}] {desc} ({elapsed:.1f}s)")
        print()
        return 1

    print("ALL TESTS PASSED")
    return 0


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
