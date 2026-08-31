#!/bin/bash
# run_ebpf_proxy_bench.sh — eBPF proxy QPS 全面对比测试
# 用法: sudo bash run_ebpf_proxy_bench.sh
#
# 测试架构:
#   none:  echo only, baseline QPS
#   sync:  echo + sync TCP forward to slave (转发在主路径)
#   ebpf:  echo only + ebpf-proxy(独立进程) kprobe → ringbuf → forward to slave
#
# 前提: make ebpf-proxy client_capture_bpf test_ebpf_proxy_qps

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJ_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

cd "$PROJ_DIR"

echo "=== eBPF Proxy 独立进程架构 QPS 测试 ==="
echo "payload range: 64 128 256 512 1024 2048 4096"
echo ""

CSV_FILE="/tmp/ebpf_proxy_qps_results.csv"
echo "mode,payload,qps,avg_us,p50_us,p99_us" > "$CSV_FILE"

PAYLOADS=(64 128 256 512 1024 2048 4096)
COUNT=20000

for payload in "${PAYLOADS[@]}"; do
    for mode in none sync ebpf; do
        echo "--- Testing: mode=$mode payload=$payload count=$COUNT ---"

        if [ "$mode" = "ebpf" ]; then
            rm -rf /sys/fs/bpf/kvstore_repl_qps_test 2>/dev/null || true
        fi

        ./tests/perf/test_ebpf_proxy_qps \
            --mode "$mode" \
            --payload "$payload" \
            --count "$COUNT" 2>/dev/null | \
            grep "^$mode " >> "$CSV_FILE" || true

        echo "    done: $mode payload=$payload"

        # port release
        sleep 1
    done
done

echo ""
echo "=== Results ==="
printf "%-8s %-6s %10s %10s %10s\n" "mode" "size" "qps" "vs_none" "vs_sync"
echo "------------------------------------------------------------"

# Parse and display with comparison
python3 -c "
import csv, sys

rows = []
with open('$CSV_FILE') as f:
    for r in csv.DictReader(f):
        rows.append(r)

# Get none baseline
baseline = {}
for r in rows:
    if r['mode'] == 'none':
        baseline[r['payload']] = float(r['qps'])

# Get sync baseline
sync_baseline = {}
for r in rows:
    if r['mode'] == 'sync':
        sync_baseline[r['payload']] = float(r['qps'])

for r in rows:
    mode = r['mode']
    p = r['payload']
    qps = float(r['qps'])

    vs_none = ''
    vs_sync = ''

    if mode != 'none' and p in baseline and baseline[p] > 0:
        pct = (qps - baseline[p]) / baseline[p] * 100
        vs_none = f'{pct:+.1f}%'

    if mode == 'ebpf' and p in sync_baseline and sync_baseline[p] > 0:
        pct = (qps - sync_baseline[p]) / sync_baseline[p] * 100
        vs_sync = f'{pct:+.1f}%'

    print(f'{mode:8s} {p:6s} {qps:10.0f} {vs_none:>10s} {vs_sync:>10s}')

print()
print('CSV saved to: $CSV_FILE')
"
