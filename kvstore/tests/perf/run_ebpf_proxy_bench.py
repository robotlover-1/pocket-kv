#!/usr/bin/env python3
"""eBPF Proxy QPS 对比测试"""
import subprocess, sys, os, time, csv

PASSWORD = "2983372202"
PAYLOADS = [64, 128, 256, 512, 1024, 2048, 4096]
COUNT = 20000
BIN = "./tests/perf/test_ebpf_proxy_qps"
CSV = "/tmp/ebpf_proxy_qps_results.csv"

def sudo(cmd):
    proc = subprocess.run(
        f"echo '{PASSWORD}' | sudo -S {cmd}",
        shell=True, capture_output=True, text=True, timeout=120)
    return proc.stdout + proc.stderr

def run_test(mode, payload):
    """Run a single test, return qps_result dict or None."""
    print(f"  Running: mode={mode} payload={payload}...", flush=True)
    if mode == "ebpf":
        sudo("rm -rf /sys/fs/bpf/kvstore_repl_qps_test")

    proc = subprocess.run(
        [BIN, "--mode", mode, "--payload", str(payload), "--count", str(COUNT)],
        capture_output=True, text=True, timeout=120)
    output = proc.stdout + proc.stderr

    for line in output.split("\n"):
        parts = line.strip().split()
        if len(parts) >= 8 and parts[0] == mode:
            return {
                "mode": mode, "payload": payload,
                "qps": float(parts[2]),
                "avg_us": float(parts[3].replace("μs", "").replace("ms", "000")),
                "p50_us": float(parts[4].replace("μs", "").replace("ms", "000")),
                "p99_us": float(parts[5].replace("μs", "").replace("ms", "000")),
            }
    print(f"  FAILED for {mode} payload={payload}", flush=True)
    return None

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    print("=== eBPF Proxy QPS Benchmark ===\n")
    results = []

    for payload in PAYLOADS:
        for mode in ["none", "sync", "ebpf"]:
            r = run_test(mode, payload)
            if r:
                results.append(r)
            time.sleep(1)

    # Write CSV
    with open(CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["mode","payload","qps","avg_us","p50_us","p99_us"])
        w.writeheader()
        w.writerows(results)

    # Summary
    print("\n=== Summary ===")
    none = {r["payload"]: r["qps"] for r in results if r["mode"] == "none"}
    sync = {r["payload"]: r["qps"] for r in results if r["mode"] == "sync"}

    print(f"{'mode':<8} {'size':<6} {'qps':>10} {'vs_none':>10} {'vs_sync':>10}")
    print("-" * 50)
    for r in results:
        vs_none = f"{(r['qps']-none[r['payload']])/none[r['payload']]*100:+.1f}%" if r["payload"] in none and r["mode"] != "none" else ""
        vs_sync = f"{(r['qps']-sync[r['payload']])/sync[r['payload']]*100:+.1f}%" if r["payload"] in sync and r["mode"] == "ebpf" else ""
        print(f"{r['mode']:<8} {r['payload']:<6} {r['qps']:10.0f} {vs_none:>10} {vs_sync:>10}")

if __name__ == "__main__":
    main()
