#!/usr/bin/env bash
# Pipeline 批量性能基准测试 — AOF always vs AOF disable，不同 pipeline 深度
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$PROJ_DIR/kvstore"
RBIN=/opt/redis-7.2.9/bin
OUTDIR="$PROJ_DIR/benchmarks/data/pipeline_bench"
mkdir -p "$OUTDIR"

KVSTORE_PORT=5190
REDIS_PORT=6390
TMPDIR="/tmp/kvstore_pipeline_bench"
mkdir -p "$TMPDIR"

# 统一基准参数
BENCH_N=1000000
BENCH_C=50
BENCH_D=64
BENCH_R=1000000

# Pipeline 深度: 1, 10, 20, 40, 80, 160（含 P=1 以对齐 AOF 并发对比）
PIPELINE_DEPTHS=(1 10 20 40 80 160)

FAIL_COUNT=0

echo "============================================"
echo " Pipeline 批量性能基准测试"
echo " 日期: $(date)"
echo " Host: $(hostname) CPU: $(nproc) cores"
echo " redis-benchmark: -n $BENCH_N -c $BENCH_C -d $BENCH_D -r $BENCH_R"
echo " Pipeline 深度: ${PIPELINE_DEPTHS[*]}"
echo "============================================"

# ==================== Phase 1: 环境检查 ====================
echo ""
echo "=== Phase 1: 环境检查 ==="

check_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: $cmd 不在 PATH 中"
        return 1
    fi
    echo "  $cmd: $(command -v "$cmd")"
}

MISSING=0
check_cmd redis-benchmark || { echo "  安装: apt install redis-tools"; MISSING=1; }
check_cmd redis-server     || { echo "  安装: apt install redis-server"; MISSING=1; }
check_cmd redis-cli        || { echo "  安装: apt install redis-tools"; MISSING=1; }

if [ ! -x "$BIN" ]; then
    echo "ERROR: kvstore 二进制文件不存在: $BIN"
    echo "  编译: cd $PROJ_DIR && make"
    MISSING=1
else
    echo "  kvstore: $BIN"
fi

if [ "$MISSING" -eq 1 ]; then
    echo "请安装/编译缺失工具后重新运行。"
    exit 1
fi

check_port_free() {
    local port="$1" name="$2"
    if ss -tlnp 2>/dev/null | grep -q ":$port " || netstat -tlnp 2>/dev/null | grep -q ":$port "; then
        echo "ERROR: 端口 $port ($name) 已被占用"
        return 1
    fi
    echo "  端口 $port ($name): 空闲"
}

PORT_OK=0
check_port_free "$KVSTORE_PORT" "kvstore" || PORT_OK=1
check_port_free "$REDIS_PORT" "redis"     || PORT_OK=1
[ "$PORT_OK" -eq 1 ] && { echo "请释放端口后重新运行。"; exit 1; }

# ==================== 辅助函数 ====================

cleanup_all() {
    pkill -f "kvstore.*--port $KVSTORE_PORT" 2>/dev/null || true
    pkill -f "redis-server.*$REDIS_PORT" 2>/dev/null || true
    for ((i=1; i<=30; i++)); do
        if ! ss -tlnp "sport = :$KVSTORE_PORT" 2>/dev/null | grep -q ":$KVSTORE_PORT" && \
           ! ss -tlnp "sport = :$REDIS_PORT" 2>/dev/null | grep -q ":$REDIS_PORT"; then
            break
        fi
        sleep 0.5
    done
}
trap cleanup_all EXIT

wait_port() {
    local port=$1
    for ((i=1; i<=60; i++)); do
        if $RBIN/redis-cli -p "$port" PING >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

start_kvstore() {
    local extra="$1"
    cleanup_all
    rm -f kvstore.dump kvstore.aof
    if ss -tlnp "sport = :$KVSTORE_PORT" 2>/dev/null | grep -q ":$KVSTORE_PORT"; then
        echo "  FAIL: 端口 $KVSTORE_PORT 仍被占用"
        exit 1
    fi
    $BIN --port $KVSTORE_PORT --role master --mem libc --net reactor $extra \
        > "$TMPDIR/kvstore.log" 2>&1 &
    if ! wait_port $KVSTORE_PORT; then
        echo "  FAIL: kvstore 启动超时 (>60s)"
        tail -20 "$TMPDIR/kvstore.log"
        return 1
    fi
    sleep 1
}

start_redis() {
    local extra="$1"
    cleanup_all
    rm -rf "$TMPDIR/appendonlydir" "$TMPDIR/dump.rdb" "$TMPDIR/appendonly.aof"
    $RBIN/redis-server --port $REDIS_PORT --dir "$TMPDIR" --save "" $extra \
        > "$TMPDIR/redis.log" 2>&1 &
    if ! wait_port $REDIS_PORT; then
        echo "  FAIL: redis 启动超时 (>60s)"
        tail -20 "$TMPDIR/redis.log"
        return 1
    fi
    sleep 1
}

run_pipe_bench() {
    local label="$1" port="$2" pipe_depth="$3"
    shift 3
    local outfile="$OUTDIR/pipeline_${label}_P${pipe_depth}.txt"

    echo "  [$label P=$pipe_depth] $RBIN/redis-benchmark -p $port -n $BENCH_N -c $BENCH_C -P $pipe_depth $*"
    $RBIN/redis-benchmark -p "$port" -n "$BENCH_N" -c "$BENCH_C" -P "$pipe_depth" -d "$BENCH_D" -r "$BENCH_R" "$@" > "$outfile" 2>&1 || true

    local qps
    qps=$(grep -oP '[\d.]+(?= requests per second)' "$outfile" | tail -1 || echo "")
    if [ -z "$qps" ] || [ "$qps" = "0" ] || [ "$qps" = "0.00" ]; then
        echo "  $label P=$pipe_depth QPS: FAIL (qps=$qps)"
        echo "$label,$pipe_depth,FAIL" >> "$OUTDIR/pipeline_summary.csv"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        return 1
    fi
    echo "  $label P=$pipe_depth QPS: $qps"
    echo "$label,$pipe_depth,$qps" >> "$OUTDIR/pipeline_summary.csv"
}

# ==================== Phase 2: Pipeline 性能测试 ====================
echo ""
echo "============================================"
echo " Phase 2: AOF always vs AOF disable 性能对比"
echo "============================================"
echo "label,pipeline_depth,qps" > "$OUTDIR/pipeline_summary.csv"

TOTAL=$(( ${#PIPELINE_DEPTHS[@]} * 6 ))
CURRENT=0

for P in "${PIPELINE_DEPTHS[@]}"; do
    echo ""
    echo "========== Pipeline 深度: $P =========="

    # --- kvstore ECHO ---
    CURRENT=$((CURRENT + 1))
    echo "--- $CURRENT/$TOTAL: kvstore_echo P=$P ---"
    start_kvstore "--aof-disable" || exit 1
    run_pipe_bench "kvstore_echo" $KVSTORE_PORT "$P" "echo" "__rand_int__"
    cleanup_all

    # --- redis ECHO ---
    CURRENT=$((CURRENT + 1))
    echo "--- $CURRENT/$TOTAL: redis_echo P=$P ---"
    start_redis "" || exit 1
    run_pipe_bench "redis_echo" $REDIS_PORT "$P" "echo" "__rand_int__"
    cleanup_all

    # --- kvstore AOF disable ---
    CURRENT=$((CURRENT + 1))
    echo "--- $CURRENT/$TOTAL: kvstore_aof_disable P=$P ---"
    start_kvstore "--aof-disable" || exit 1
    run_pipe_bench "kvstore_aof_disable" $KVSTORE_PORT "$P" HSET key:__rand_int__ value
    cleanup_all

    # --- kvstore AOF always ---
    CURRENT=$((CURRENT + 1))
    echo "--- $CURRENT/$TOTAL: kvstore_aof_always P=$P ---"
    start_kvstore "--appendfsync always" || exit 1
    run_pipe_bench "kvstore_aof_always" $KVSTORE_PORT "$P" HSET key:__rand_int__ value
    cleanup_all

    # --- redis no AOF (HSET 3-arg: key field value) ---
    CURRENT=$((CURRENT + 1))
    echo "--- $CURRENT/$TOTAL: redis_no_aof P=$P ---"
    start_redis "" || exit 1
    run_pipe_bench "redis_no_aof" $REDIS_PORT "$P" HSET key:__rand_int__ __rand_int__ value
    cleanup_all

    # --- redis AOF always (HSET 3-arg) ---
    CURRENT=$((CURRENT + 1))
    echo "--- $CURRENT/$TOTAL: redis_aof_always P=$P ---"
    start_redis "--appendonly yes --appendfsync always" || exit 1
    run_pipe_bench "redis_aof_always" $REDIS_PORT "$P" HSET key:__rand_int__ __rand_int__ value
    cleanup_all
done

# ==================== Phase 3: 汇总 ====================
echo ""
echo "============================================"
echo " Phase 3: Pipeline 测试结果汇总"
echo "============================================"
echo ""
echo "=== Pipeline 对比结果 ($OUTDIR/pipeline_summary.csv) ==="
echo ""

printf "%-28s %5s  %12s\n" "label" "P" "qps"
printf "%-28s %5s  %12s\n" "----------------------------" "-----" "------------"

while IFS=',' read -r label depth qps; do
    printf "%-28s %5s  %12s\n" "$label" "$depth" "$qps"
done < "$OUTDIR/pipeline_summary.csv"

echo ""
echo "============================================"
if [ "$FAIL_COUNT" -eq 0 ]; then
    echo " 完成 (全部通过)"
else
    echo " 完成 ($FAIL_COUNT 项失败)"
fi
echo " 结果保存在 $OUTDIR/"
echo "============================================"
