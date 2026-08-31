#!/bin/bash
# 一键基准测试 - 内存后端 + value 大小组合
set -e
cd "$(dirname "$0")/../.."

CSV=benchmarks/data/bench_fresh.csv
rm -f "$CSV"

run_bench() {
    local label="$1" ops="$2" sz="$3" bport="$4"
    echo "=== $label ops=$ops val_sz=$sz ==="
    sudo python3 tools/bench/bench_mem_backend.py \
        --binary ./kvstore \
        --base-port "$bport" \
        --ops "$ops" \
        --value-size "$sz" \
        --backends libc,jemalloc,custom \
        --csv "$CSV" \
        --run-label "$label" \
        2>&1 | grep -E "^libc:|^jemalloc:|^custom:"
}

run_bench "50k_128B"  50000  128   6500
run_bench "50k_4KB"   50000  4096  6600
run_bench "100k_128B" 100000 128   6700

echo "=== DONE ==="
echo "--- Summary ---"
cat "$CSV"
