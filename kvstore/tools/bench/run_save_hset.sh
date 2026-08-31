#!/usr/bin/env bash
# SAVE + AOF always 性能测试 — 写入统一为 redis-benchmark -c 50（对齐 AOF 并发对比）
# 与 tools/bench/run_persist_bench.sh 的 aof_always 场景使用相同参数：
#   redis-benchmark -n $TOTAL -c 50 -P 1 -d 64 -r $TOTAL HSET key:__rand_int__ value
# 差异：本脚本在写入后额外执行 SAVE，测量 SAVE 延迟。
set -uo pipefail

PROJ_DIR=/home/pp/Desktop/ls_study/proj/9.1-kvstore
BIN=$PROJ_DIR/kvstore
OUTDIR=$PROJ_DIR/benchmarks/data/persist_bench
TMPDIR=/tmp/kvstore_persist_bench
mkdir -p "$TMPDIR" "$OUTDIR"

pkill -f "kvstore.*5190" 2>/dev/null || true
sleep 1

run_test() {
    local label batch aof
    label=$1
    batch=$2
    aof=$3
    local TOTAL=1000000
    local iter=$((TOTAL / batch))
    local BENCH_C=50
    local fsync_arg="--appendfsync always"
    [ "$aof" = "disable" ] && fsync_arg="--aof-disable"

    echo ""
    echo "============================================"
    echo " $label (aof=$aof, batch=$batch, iter=$iter, -c $BENCH_C)"
    echo "============================================"

    rm -f kvstore.dump kvstore.aof
    $BIN --port 5190 --role master --mem libc --net reactor $fsync_arg > $TMPDIR/kvstore.log 2>&1 &
    local spid=$!

    for i in $(seq 1 30); do
        redis-cli -p 5190 PING >/dev/null 2>&1 && break
        sleep 0.2
    done
    sleep 1

    # 写入：redis-benchmark（与 AOF 并发对比同参 -c 50 -P 1 -d 64；-n 与 -r 一起随
    # batch 变（数据量 = key 池，保持一致）。唯一跨表差异：SAVE 变 n/r，Pipeline 变 P）。
    redis-benchmark -p 5190 -n $batch -c $BENCH_C -P 1 -d 64 -r $batch \
        HSET key:__rand_int__ value > $TMPDIR/redisbench_${label}_${aof}.txt 2>&1 || true
    local qps=$(grep -E "requests per second" $TMPDIR/redisbench_${label}_${aof}.txt \
        | head -1 | awk '{print $1}')

    # SAVE × iter，测量平均 SAVE 延迟
    local save_ms=0
    for i in $(seq 1 $iter); do
        local ts0=$(date +%s%N)
        redis-cli -p 5190 SAVE > /dev/null 2>&1 || true
        local ts1=$(date +%s%N)
        save_ms=$((save_ms + (ts1 - ts0) / 1000000))
    done
    local avg_s=$(echo "scale=1; $save_ms / $iter" | bc)
    local dumpsz=$(stat -c %s kvstore.dump 2>/dev/null || echo 0)

    printf "%s_%s,%s,%d,%.1f,%d\n" "$label" "$aof" "$qps" "$iter" "$avg_s" "$dumpsz" \
        >> $OUTDIR/save_summary2.csv
    echo "  结果: 写入QPS=$qps SAVE×$iter 平均SAVE=${avg_s}ms dump=${dumpsz}B"

    kill $spid 2>/dev/null || true
    sleep 2
}

echo "scenario,write_qps,save_count,avg_save_ms,dump_bytes" > $OUTDIR/save_summary2.csv

for aof in disable always; do
    run_test "100w_hset_save_1" 1000000 "$aof"
    run_test "10w_hset_save_10" 100000 "$aof"
    run_test "1w_hset_save_100" 10000 "$aof"
    run_test "1k_hset_save_1000" 1000 "$aof"
done

echo ""
echo "=== 最终 SAVE 结果 (HSET, aof=disable/always, redis-benchmark -c 50) ==="
cat $OUTDIR/save_summary2.csv
pkill -f "kvstore.*5190" 2>/dev/null || true
