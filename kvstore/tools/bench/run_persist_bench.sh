#!/usr/bin/env bash
# 持久化性能基准测试 — 统一 redis-benchmark HSET 参数 v2
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$PROJ_DIR/kvstore"
OUTDIR="$PROJ_DIR/benchmarks/data/persist_bench"
mkdir -p "$OUTDIR"

KVSTORE_PORT=5190
REDIS_PORT=6390
TMPDIR="/tmp/kvstore_persist_bench"
mkdir -p "$TMPDIR"

# 统一 redis-benchmark 参数
BENCH_N=1000000
BENCH_C=50
BENCH_P=1
BENCH_D=64
BENCH_R=1000000
BENCH_ARGS="-n $BENCH_N -c $BENCH_C -P $BENCH_P -d $BENCH_D -r $BENCH_R"

FAIL_COUNT=0

echo "============================================"
echo " 持久化性能基准测试 v2"
echo " 日期: $(date)"
echo " Host: $(hostname) CPU: $(nproc) cores"
echo " redis-benchmark: -n $BENCH_N -c $BENCH_C -P $BENCH_P -d $BENCH_D -r $BENCH_R"
echo " kvstore: HSET key:__rand_int__ value | redis: HSET key:__rand_int__ __rand_int__ value"
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
check_cmd redis-benchmark || { echo "  安装: apt install redis-tools 或从源码编译"; MISSING=1; }
check_cmd redis-server     || { echo "  安装: apt install redis-server 或从源码编译"; MISSING=1; }
check_cmd redis-cli        || { echo "  安装: apt install redis-tools 或从源码编译"; MISSING=1; }

if [ ! -x "$BIN" ]; then
    echo "ERROR: kvstore 二进制文件不存在或不可执行: $BIN"
    echo "  编译: cd $PROJ_DIR && make"
    MISSING=1
else
    echo "  kvstore: $BIN"
fi

if [ "$MISSING" -eq 1 ]; then
    echo ""
    echo "请安装/编译缺失工具后重新运行。"
    exit 1
fi

# 检查端口占用
check_port_free() {
    local port="$1" name="$2"
    if ss -tlnp 2>/dev/null | grep -q ":$port " || netstat -tlnp 2>/dev/null | grep -q ":$port "; then
        echo "ERROR: 端口 $port ($name) 已被占用"
        echo "  释放: sudo fuser -k $port/tcp"
        return 1
    fi
    echo "  端口 $port ($name): 空闲"
}

PORT_OK=0
check_port_free "$KVSTORE_PORT" "kvstore" || PORT_OK=1
check_port_free "$REDIS_PORT" "redis"     || PORT_OK=1

if [ "$PORT_OK" -eq 1 ]; then
    echo ""
    echo "请释放端口后重新运行。"
    exit 1
fi

# ==================== 辅助函数 ====================

cleanup_all() {
    pkill -f "kvstore.*--port $KVSTORE_PORT" 2>/dev/null || true
    pkill -f "redis-server.*$REDIS_PORT" 2>/dev/null || true
    # 等待端口真正释放
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
        if redis-cli -p "$port" PING >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

start_kvstore() {
    local extra="$1"
    cleanup_all
    rm -f kvstore.dump kvstore.aof
    # 确保端口已释放，否则退出
    if ss -tlnp "sport = :$KVSTORE_PORT" 2>/dev/null | grep -q ":$KVSTORE_PORT"; then
        echo "  FAIL: 端口 $KVSTORE_PORT 仍被占用，cleanup_all 未能释放"
        echo "  占用进程: $(ss -tlnp "sport = :$KVSTORE_PORT" 2>/dev/null)"
        exit 1
    fi
    $BIN --port $KVSTORE_PORT --role master --mem libc --net reactor $extra \
        > "$TMPDIR/kvstore.log" 2>&1 &
    if ! wait_port $KVSTORE_PORT; then
        echo "  FAIL: kvstore 启动超时 (>60s)"
        echo "  === kvstore 日志尾部 ==="
        tail -20 "$TMPDIR/kvstore.log"
        return 1
    fi
    sleep 1
}

start_redis() {
    local extra="$1"
    cleanup_all
    rm -f "$TMPDIR/dump.rdb" "$TMPDIR/appendonly.aof"
    redis-server --port $REDIS_PORT --dir "$TMPDIR" --save "" $extra \
        > "$TMPDIR/redis.log" 2>&1 &
    if ! wait_port $REDIS_PORT; then
        echo "  FAIL: redis 启动超时 (>60s)"
        echo "  === redis 日志尾部 ==="
        tail -20 "$TMPDIR/redis.log"
        return 1
    fi
    sleep 1
}

run_bench() {
    local label="$1" port="$2"
    shift 2
    local outfile="$OUTDIR/aof_${label}.txt"

    echo "  [$label] redis-benchmark -p $port $BENCH_ARGS $*"
    redis-benchmark -p "$port" $BENCH_ARGS "$@" > "$outfile" 2>&1 || true

    local qps
    qps=$(grep -oP '[\d.]+(?= requests per second)' "$outfile" | tail -1 || echo "")
    if [ -z "$qps" ] || [ "$qps" = "0" ] || [ "$qps" = "0.00" ]; then
        echo "  $label QPS: FAIL (qps=$qps)"
        echo "$label,FAIL" >> "$OUTDIR/aof_summary.csv"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        return 1
    fi
    echo "  $label QPS: $qps"
    echo "$label,$qps" >> "$OUTDIR/aof_summary.csv"
}

# ==================== Phase 2: AOF + ECHO (6 配置) ====================
echo ""
echo "============================================"
echo " Phase 2: AOF + ECHO 性能对比 (6 配置)"
echo "============================================"
echo "label,qps" > "$OUTDIR/aof_summary.csv"

# 1. kvstore echo (AOF 关闭, 纯协议开销)
echo ""
echo "--- 1/6: kvstore_echo ---"
start_kvstore "--aof-disable" || exit 1
run_bench "kvstore_echo" $KVSTORE_PORT "echo" "__rand_int__"
cleanup_all

# 2. redis echo (RDB 关闭, 纯协议开销)
echo ""
echo "--- 2/6: redis_echo ---"
start_redis "" || exit 1
run_bench "redis_echo" $REDIS_PORT "echo" "__rand_int__"
cleanup_all

# 3. kvstore aof_disable (无持久化 HSET)
echo ""
echo "--- 3/6: kvstore_aof_disable ---"
start_kvstore "--aof-disable" || exit 1
run_bench "kvstore_aof_disable" $KVSTORE_PORT HSET key:__rand_int__ value
cleanup_all

# 4. kvstore aof_always
echo ""
echo "--- 4/6: kvstore_aof_always ---"
start_kvstore "--appendfsync always" || exit 1
run_bench "kvstore_aof_always" $KVSTORE_PORT HSET key:__rand_int__ value
cleanup_all

# 5. redis no_aof (RDB 关闭, 无 AOF) — HSET 3-arg (key field value)
echo ""
echo "--- 5/6: redis_no_aof ---"
start_redis "" || exit 1
run_bench "redis_no_aof" $REDIS_PORT HSET key:__rand_int__ __rand_int__ value
cleanup_all

# 6. redis aof_always — HSET 3-arg
echo ""
echo "--- 6/6: redis_aof_always ---"
start_redis "--appendonly yes --appendfsync always" || exit 1
run_bench "redis_aof_always" $REDIS_PORT HSET key:__rand_int__ __rand_int__ value
cleanup_all

# ==================== Phase 3: SAVE 性能测试 ====================
echo ""
echo "============================================"
echo " Phase 3: SAVE 性能测试 (4 数据规模)"
echo "============================================"
echo "scenario,total_keys,write_qps,write_time_sec,save_count,save_time_sec,avg_save_ms" > "$OUTDIR/save_summary.csv"

# --- Step 1: 纯写入基线 ---
echo ""
echo "--- SAVE 写入基线 (HSET 100w, no AOF, no SAVE) ---"
start_kvstore "--aof-disable" || exit 1
baseline_out="$OUTDIR/aof_kvstore_write_baseline.txt"
redis-benchmark -p "$KVSTORE_PORT" $BENCH_ARGS HSET key:__rand_int__ value > "$baseline_out" 2>&1 || true
BASELINE_QPS=$(grep -oP '[\d.]+(?= requests per second)' "$baseline_out" | tail -1 || echo "0")
echo "  基线 QPS: $BASELINE_QPS"
# 基线跑完必须杀 kvstore，否则 save_scenario 的 start_kvstore 可能连到旧进程
cleanup_all

save_scenario() {
    local scenario="$1" data_size="$2" save_count="$3"
    echo ""
    echo "--- $scenario: data_size=$data_size save_count=$save_count ---"

    # 重启 kvstore fresh (无持久化)
    start_kvstore "--aof-disable" || exit 1

    # 基线写入
    local outfile="$OUTDIR/save_${scenario}_fill.txt"
    echo "  写入 ${data_size} keys (redis-benchmark) ..."
    write_start=$(date +%s%N)
    redis-benchmark -p "$KVSTORE_PORT" -n "$data_size" -c 50 -P 1 -d 64 -r "$data_size" \
        HSET key:__rand_int__ value > "$outfile" 2>&1 || true
    write_end=$(date +%s%N)

    local write_ns=$((write_end - write_start))
    local write_sec
    write_sec=$(echo "scale=6; $write_ns / 1000000000" | bc)

    # 提取写入 QPS
    local write_qps
    write_qps=$(grep -oP '[\d.]+(?= requests per second)' "$outfile" | tail -1 || echo "N/A")
    echo "  写入完成: ${write_sec}s, QPS=$write_qps"

    # 多次 SAVE 计时（带 3 次重试）
    local total_save_ms=0
    local save_fail=0
    for ((i=1; i<=save_count; i++)); do
        local st0 st1 sd saved=0
        for attempt in 1 2 3; do
            st0=$(date +%s%N)
            if redis-cli -p $KVSTORE_PORT SAVE > /dev/null 2>&1; then
                st1=$(date +%s%N)
                sd=$(( (st1 - st0) / 1000000 ))
                total_save_ms=$((total_save_ms + sd))
                saved=1
                break
            fi
            sleep 1
        done
        if [ "$saved" -eq 0 ]; then
            echo "  FAIL: SAVE 失败 ($scenario, iter=$i, 3次重试均失败)"
            save_fail=1
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    done

    if [ "$save_fail" -eq 1 ]; then
        local save_time_sec
        save_time_sec=$(echo "scale=6; $total_save_ms / 1000" | bc)
        local avg_save_ms
        avg_save_ms=$(echo "scale=1; $total_save_ms / $save_count" | bc)
        echo "$scenario,$data_size,$write_qps,$write_sec,$save_count,$save_time_sec,FAIL" >> "$OUTDIR/save_summary.csv"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        return 1
    fi

    local save_time_sec
    save_time_sec=$(echo "scale=6; $total_save_ms / 1000" | bc)
    local avg_save_ms
    avg_save_ms=$(echo "scale=1; $total_save_ms / $save_count" | bc)

    echo "  结果: 写入${write_sec}s SAVE总计${save_time_sec}s 平均${avg_save_ms}ms"
    echo "$scenario,$data_size,$write_qps,$write_sec,$save_count,$save_time_sec,$avg_save_ms" \
        >> "$OUTDIR/save_summary.csv"
}

save_scenario "100w_save_1"   1000000 1
save_scenario "10w_save_10"   100000  10
save_scenario "1w_save_100"   10000   100
save_scenario "1k_save_1000"  1000    1000

# ==================== Phase 4: 汇总 ====================
echo ""
echo "============================================"
echo " Phase 4: 测试结果汇总"
echo "============================================"

echo ""
echo "=== AOF 对比结果 ($OUTDIR/aof_summary.csv) ==="
column -t -s ',' "$OUTDIR/aof_summary.csv"

echo ""
echo "=== SAVE 性能结果 ($OUTDIR/save_summary.csv) ==="
column -t -s ',' "$OUTDIR/save_summary.csv"

echo ""
echo "============================================"
if [ "$FAIL_COUNT" -gt 0 ]; then
    echo " 完成 (FAIL: $FAIL_COUNT)"
else
    echo " 完成 (全部通过)"
fi
echo " 结果保存在 $OUTDIR/"
echo "============================================"
