# 持久化性能基准重测 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重写 `tools/bench/run_persist_bench.sh`，统一使用 `redis-benchmark -n 1000000 -c 50 -P 1 -d 64 -r 1000000 HSET`，重跑 AOF 和 SAVE 性能基准，更新 README。

**Architecture:** 单 bash 脚本，4 个 phase：环境检查 → AOF+ECHO 测试（8 配置）→ SAVE 测试（写入基线 + 分数据量 SAVE 耗时）→ 汇总输出。所有 redis-benchmark 原始输出保存为 txt，汇总数据写入 CSV。

**Tech Stack:** bash, redis-benchmark, redis-cli, redis-server, kvstore

---

## 文件结构

| 文件 | 操作 | 职责 |
|------|------|------|
| `tools/bench/run_persist_bench.sh` | 重写 | 持久化性能基准测试脚本 |
| `benchmarks/data/persist_bench/aof_summary.csv` | 覆盖 | AOF 测试汇总 |
| `benchmarks/data/persist_bench/save_summary.csv` | 覆盖 | SAVE 测试汇总 |
| `benchmarks/data/persist_bench/aof_*.txt` | 覆盖 | redis-benchmark 原始输出 |
| `README.md` | 修改 | 用新数据更新性能基准段落 |

---

### Task 1: 重写 `run_persist_bench.sh` — 头部分

**Files:**
- Rewrite: `tools/bench/run_persist_bench.sh`

从头重写脚本。先写环境变量、辅助函数、Phase 1。

- [ ] **Step 1: 写脚本头和环境变量**

```bash
#!/usr/bin/env bash
# 持久化性能基准测试 (v2 — 统一 redis-benchmark 参数)
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$PROJ_DIR/kvstore"
OUTDIR="$PROJ_DIR/benchmarks/data/persist_bench"
mkdir -p "$OUTDIR"

KVSTORE_PORT=5190
REDIS_PORT=6390
TMPDIR="/tmp/kvstore_persist_bench"
mkdir -p "$TMPDIR"

# 统一参数
BENCH_N=1000000
BENCH_C=50
BENCH_P=1
BENCH_D=64
BENCH_R=1000000
BENCH_CMD="HSET key:__rand_int__ value"

BENCH_ARGS="-n $BENCH_N -c $BENCH_C -P $BENCH_P -d $BENCH_D -r $BENCH_R"

echo "============================================"
echo " 持久化性能基准测试 v2"
echo " 日期: $(date)"
echo " Host: $(hostname) CPU: $(nproc) cores"
echo " 参数: redis-benchmark -n $BENCH_N -c $BENCH_C -P $BENCH_P -d $BENCH_D -r $BENCH_R"
echo "============================================"
```

- [ ] **Step 2: 写辅助函数**

```bash
cleanup_all() {
    pkill -f "kvstore.*--port $KVSTORE_PORT" 2>/dev/null || true
    pkill -f "redis-server.*$REDIS_PORT" 2>/dev/null || true
    sleep 1
}
trap cleanup_all EXIT

wait_port() {
    local port=$1
    for i in $(seq 1 60); do
        if redis-cli -p "$port" PING >/dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    return 1
}

start_kvstore() {
    local extra="$1"
    cleanup_all
    rm -f "$PROJ_DIR/kvstore.dump" "$PROJ_DIR/kvstore.aof"
    $BIN --port $KVSTORE_PORT --role master --mem libc --net reactor $extra \
        > "$TMPDIR/kvstore.log" 2>&1 &
    wait_port $KVSTORE_PORT || {
        echo "FAIL: kvstore 启动超时"
        echo "--- 日志尾部 ---"
        tail -30 "$TMPDIR/kvstore.log"
        return 1
    }
    sleep 1
}

start_redis() {
    local extra="$1"
    cleanup_all
    rm -f "$TMPDIR/dump.rdb" "$TMPDIR/appendonly.aof"
    redis-server --port $REDIS_PORT --dir "$TMPDIR" --save "" $extra \
        > "$TMPDIR/redis.log" 2>&1 &
    wait_port $REDIS_PORT || {
        echo "FAIL: redis-server 启动超时"
        echo "--- 日志尾部 ---"
        tail -30 "$TMPDIR/redis.log"
        return 1
    }
    sleep 2
}

run_bench() {
    local label="$1" port="$2" cmd="$3"
    local outfile="$OUTDIR/aof_${label}.txt"
    echo "  [$label] redis-benchmark -p $port $BENCH_ARGS $cmd"
    redis-benchmark -p "$port" $BENCH_ARGS $cmd > "$outfile" 2>&1 || true
    local qps
    qps=$(grep -oP '[\d.]+(?= requests per second)' "$outfile" | tail -1 || echo "N/A")
    if [ -z "$qps" ]; then qps="N/A"; fi
    echo "  $label QPS: $qps"
    echo "$label,$qps" >> "$OUTDIR/aof_summary.csv"
}

fail_count=0
mark_fail() {
    echo "FAIL: $1" >&2
    fail_count=$((fail_count + 1))
}
```

- [ ] **Step 3: 写 Phase 1 环境检查**

```bash
# ==================== Phase 1: 环境检查 ====================
echo ""
echo "=== Phase 1: 环境检查 ==="

check_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "FAIL: $1 未安装"
        case "$1" in
            redis-benchmark) echo "  安装: sudo apt install redis-tools";;
            redis-server)    echo "  安装: sudo apt install redis-server";;
            redis-cli)       echo "  安装: sudo apt install redis-tools";;
            *)               echo "  请安装 $1";;
        esac
        exit 1
    fi
    echo "  ✓ $1 ($(which "$1"))"
}

check_bin redis-benchmark
check_bin redis-server
check_bin redis-cli

if [ ! -x "$BIN" ]; then
    echo "FAIL: kvstore 二进制不存在 ($BIN)"
    echo "  编译: cd $PROJ_DIR && make clean && make"
    exit 1
fi
echo "  ✓ kvstore ($BIN)"

# 检查端口是否被占用
check_port() {
    if ss -tlnp "sport = :$1" 2>/dev/null | grep -q ":$1"; then
        echo "FAIL: 端口 $1 被占用"
        echo "  释放: kill \$(ss -tlnp 'sport = :$1' | grep -oP 'pid=\K\d+')"
        exit 1
    fi
}
check_port $KVSTORE_PORT
check_port $REDIS_PORT
echo "  ✓ 端口 $KVSTORE_PORT / $REDIS_PORT 空闲"
```

- [ ] **Step 4: 提交**

```bash
git add tools/bench/run_persist_bench.sh
git commit -m "wip: v2 脚本头部和 Phase 1 环境检查"
```

---

### Task 2: Phase 2 — AOF + ECHO 测试

**Files:**
- Modify: `tools/bench/run_persist_bench.sh`

- [ ] **Step 1: 追加 AOF 测试逻辑**

```bash
# ==================== Phase 2: AOF + ECHO ====================
echo ""
echo "=== Phase 2: AOF + ECHO 性能对比 ==="
echo "label,qps" > "$OUTDIR/aof_summary.csv"

# --- ECHO 基线 ---
echo ""
echo "--- kvstore echo ---"
start_kvstore "--aof-disable" || exit 1
run_bench "kvstore_echo" $KVSTORE_PORT "echo"

echo ""
echo "--- redis echo ---"
start_redis "" || exit 1
run_bench "redis_echo" $REDIS_PORT "echo"

# --- kvstore HSET 各 AOF 策略 ---
echo ""
echo "--- kvstore HSET aof_disable ---"
start_kvstore "--aof-disable" || exit 1
run_bench "kvstore_aof_disable" $KVSTORE_PORT "$BENCH_CMD"

echo ""
echo "--- kvstore HSET aof_always ---"
start_kvstore "--appendfsync always" || exit 1
run_bench "kvstore_aof_always" $KVSTORE_PORT "$BENCH_CMD"

echo ""
echo "--- kvstore HSET aof_everysec ---"
start_kvstore "--appendfsync everysec" || exit 1
run_bench "kvstore_aof_everysec" $KVSTORE_PORT "$BENCH_CMD"

# --- Redis HSET 各 AOF 策略 ---
echo ""
echo "--- redis no AOF ---"
start_redis "" || exit 1
run_bench "redis_no_aof" $REDIS_PORT "$BENCH_CMD"

echo ""
echo "--- redis AOF always ---"
start_redis "--appendonly yes --appendfsync always" || exit 1
run_bench "redis_aof_always" $REDIS_PORT "$BENCH_CMD"

echo ""
echo "--- redis AOF everysec ---"
start_redis "--appendonly yes --appendfsync everysec" || exit 1
run_bench "redis_aof_everysec" $REDIS_PORT "$BENCH_CMD"
```

- [ ] **Step 2: 提交**

```bash
git add tools/bench/run_persist_bench.sh
git commit -m "wip: Phase 2 AOF+ECHO 测试 (8 配置, 统一 HSET)"
```

---

### Task 3: Phase 3 — SAVE 测试

**Files:**
- Modify: `tools/bench/run_persist_bench.sh`

- [ ] **Step 1: 追加 SAVE 测试逻辑**

```bash
# ==================== Phase 3: SAVE 测试 ====================
echo ""
echo "=== Phase 3: SAVE 性能测试 ==="
echo "scenario,total_keys,write_qps,write_time_sec,save_count,save_time_sec,avg_save_ms,effective_qps" \
    > "$OUTDIR/save_summary.csv"

# --- Step 1: 纯写入基线 ---
echo ""
echo "--- SAVE 写入基线 (HSET 100w, no AOF, no SAVE) ---"
start_kvstore "--aof-disable" || exit 1
baseline_out="$OUTDIR/aof_kvstore_write_baseline.txt"
redis-benchmark -p $KVSTORE_PORT $BENCH_ARGS $BENCH_CMD > "$baseline_out" 2>&1 || true
BASELINE_QPS=$(grep -oP '[\d.]+(?= requests per second)' "$baseline_out" | tail -1 || echo "0")
echo "  基线 QPS: $BASELINE_QPS"

# --- Step 2: 不同数据量下的 SAVE 耗时 ---
# 每个场景用对应数据量填充，然后对该数据集做 save_count 次 SAVE
# 参数: label, data_size (填充量), key_range (-r 值), save_count
run_save_scenario() {
    local label="$1" data_size="$2" key_range="$3" save_count="$4"

    echo ""
    echo "--- $label (data_size=$data_size, save_count=$save_count) ---"

    # 清理并重启
    cleanup_all
    rm -f "$PROJ_DIR/kvstore.dump" "$PROJ_DIR/kvstore.aof"
    $BIN --port $KVSTORE_PORT --role master --mem libc --net reactor --aof-disable \
        > "$TMPDIR/kvstore_save.log" 2>&1 &
    wait_port $KVSTORE_PORT || {
        mark_fail "kvstore 启动失败 ($label)"
        echo "$label,$data_size,FAIL,0,$save_count,0,0,FAIL" >> "$OUTDIR/save_summary.csv"
        return
    }
    sleep 1

    # 灌数据
    local write_t0 write_t1 write_ms write_qps
    write_t0=$(date +%s%N)
    redis-benchmark -p $KVSTORE_PORT -n "$data_size" -c 50 -P 1 -d 64 -r "$key_range" \
        HSET key:__rand_int__ value > "$TMPDIR/save_fill_${label}.txt" 2>&1 || true
    write_t1=$(date +%s%N)
    write_ms=$(( (write_t1 - write_t0) / 1000000 ))
    if [ "$write_ms" -le 0 ]; then write_ms=1; fi
    write_qps=$(awk "BEGIN {printf \"%.0f\", $data_size / ($write_ms / 1000)}")
    echo "  写入: ${write_ms}ms, ${write_qps} qps ($data_size keys)"

    # 多次 SAVE 计时（每次 SAVE 在同一数据集上）
    local total_save_ms=0
    for i in $(seq 1 $save_count); do
        local st0 st1 sd
        st0=$(date +%s%N)
        redis-cli -p $KVSTORE_PORT SAVE > /dev/null 2>&1 || {
            mark_fail "SAVE 失败 ($label, iter=$i)"
        }
        st1=$(date +%s%N)
        sd=$(( (st1 - st0) / 1000000 ))
        total_save_ms=$((total_save_ms + sd))
    done

    local avg_save_ms=$((total_save_ms / save_count))
    local save_time_sec
    save_time_sec=$(awk "BEGIN {printf \"%.2f\", $total_save_ms / 1000}")

    # 有效 QPS: 假设写满 100w 总 key 的过程中按场景频率做 SAVE
    # 总写入 = data_size (每次灌入的 key 数)
    # 有效QPS = data_size / (write_time + avg_save_time)
    local write_time_sec
    write_time_sec=$(awk "BEGIN {printf \"%.2f\", $write_ms / 1000}")
    local effective_qps
    effective_qps=$(awk "BEGIN {printf \"%.0f\", $data_size / ($write_time_sec + $save_time_sec / $save_count)}")

    echo "  SAVE: ${save_count}次, 总${save_time_sec}s, 平均${avg_save_ms}ms/次"
    echo "  单次SAVE有效 QPS: $effective_qps (${data_size} keys / (${write_time_sec}s写入 + $(awk "BEGIN {printf \"%.3f\", $save_time_sec/$save_count}")s SAVE))"
    echo "$label,$data_size,$write_qps,$write_time_sec,$save_count,$save_time_sec,$avg_save_ms,$effective_qps" \
        >> "$OUTDIR/save_summary.csv"
}

# 4 个场景: data_size 递减，模拟不同 SAVE 频率
# 100w 写 1 次 SAVE → 数据量=100w, SAVE=1
# 每 10w SAVE 一次 → 数据量=10w, SAVE=10 (重复测 10 次取平均)
# 每 1w SAVE 一次  → 数据量=1w,  SAVE=100
# 每 1k SAVE 一次  → 数据量=1k,  SAVE=1000
run_save_scenario "100w_save_1"    1000000 1000000 1
run_save_scenario "10w_save_10"    100000  100000  10
run_save_scenario "1w_save_100"    10000   10000   100
run_save_scenario "1k_save_1000"   1000    1000    1000
```

- [ ] **Step 2: 提交**

```bash
git add tools/bench/run_persist_bench.sh
git commit -m "wip: Phase 3 SAVE 测试 (redis-benchmark 写入 + SAVE 单独计时)"
```

---

### Task 4: Phase 4 — 汇总输出 + 脚本结尾

**Files:**
- Modify: `tools/bench/run_persist_bench.sh`

- [ ] **Step 1: 追加汇总输出**

```bash
# ==================== Phase 4: 汇总 ====================
echo ""
echo "============================================"
echo " 测试完成"
echo "============================================"

echo ""
echo "=== AOF 对比结果 ==="
cat "$OUTDIR/aof_summary.csv" | column -t -s ','

echo ""
echo "=== SAVE 性能结果 ==="
cat "$OUTDIR/save_summary.csv" | column -t -s ','

echo ""
echo "结果保存在: $OUTDIR/"
echo "原始输出: $OUTDIR/aof_*.txt"

if [ "$fail_count" -gt 0 ]; then
    echo ""
    echo "⚠️  警告: $fail_count 个测试失败"
fi
```

- [ ] **Step 2: 提交**

```bash
git add tools/bench/run_persist_bench.sh
git commit -m "wip: Phase 4 汇总输出"
```

---

### Task 5: 脚本自检

**Files:**
- Verify: `tools/bench/run_persist_bench.sh`

- [ ] **Step 1: bash 语法检查**

```bash
bash -n tools/bench/run_persist_bench.sh
```
Expected: 无输出（语法正确）

- [ ] **Step 2: 验证脚本可独立运行（dry-run — 不实际跑 benchmark）**

写一个最小验证：检查脚本能通过 Phase 1 环境检查。

```bash
bash -x tools/bench/run_persist_bench.sh 2>&1 | head -30
```
Expected: 脚本开始执行，打印头部信息和环境检查结果。

- [ ] **Step 3: 提交**

```bash
git add tools/bench/run_persist_bench.sh
git commit -m "feat: 重写 persist_bench.sh — 统一 redis-benchmark HSET 参数"
```

---

### Task 6: 运行测试并获取数据

**Files:**
- Create: `benchmarks/data/persist_bench/aof_*.txt` (原始输出)
- Create: `benchmarks/data/persist_bench/aof_summary.csv`
- Create: `benchmarks/data/persist_bench/save_summary.csv`

- [ ] **Step 1: 确保 kvstore 已编译**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore
```
Expected: 编译成功

- [ ] **Step 2: 运行完整测试**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && bash tools/bench/run_persist_bench.sh
```
Expected: 脚本顺序执行所有测试，输出 QPS 数据，无 FAIL。
注意：整个过程预计 10-15 分钟（每轮 redis-benchmark 100 万次约 5-8 秒，SAVE 场景含 1000 次 SAVE 循环）。

- [ ] **Step 3: 检查输出完整性**

```bash
ls -la benchmarks/data/persist_bench/aof_*.txt
wc -l benchmarks/data/persist_bench/aof_summary.csv
wc -l benchmarks/data/persist_bench/save_summary.csv
```
Expected:
- 10 个 `aof_*.txt` 文件（包括 write_baseline）
- `aof_summary.csv` 10 行（含 header）
- `save_summary.csv` 5 行（含 header）

- [ ] **Step 4: 提交原始测试数据**

```bash
git add benchmarks/data/persist_bench/aof_*.txt
git add benchmarks/data/persist_bench/aof_summary.csv
git add benchmarks/data/persist_bench/save_summary.csv
git commit -m "bench: 更新持久化性能基准测试数据 (v2 统一参数)"
```

---

### Task 7: 更新 README

**Files:**
- Modify: `README.md` (行 4292-4468 区域)

- [ ] **Step 1: 用新数据替换 AOF 性能对比表**

找到 README 中 `##### 测试结果` 下的旧表格（约行 4348），替换为新数据。

```markdown
##### 测试结果

ECHO QPS 与 AOF 策略无关（ECHO 不写引擎、不写 AOF），同一服务器只需测一次。

| 配置 | ECHO (QPS) | HSET (QPS) |
|---|---|---|
| **kvstore** (ECHO 基线) | **<kvstore_echo>** | — |
| ├─ AOF 关闭（`--aof-disable`） | — | **<kvstore_aof_disable>** |
| ├─ AOF always（每条命令 fsync） | — | <kvstore_aof_always> |
| └─ AOF everysec（每秒 fsync） | — | <kvstore_aof_everysec> |
| **Redis** (ECHO 基线) | **<redis_echo>** | — |
| ├─ 无 AOF | — | <redis_no_aof> |
| ├─ AOF everysec | — | <redis_aof_everysec> |
| └─ AOF always | — | <redis_aof_always> |
```

用 CSV 中的实际值替换占位符。更新测试参数说明，标注 `n=1000000, c=50, P=1, d=64, HSET`。

- [ ] **Step 2: 用新数据替换 SAVE 性能测试表**

找到 README 中 `##### 测试结果` 下的 SAVE 旧表格（约行 4457），替换为新数据和方法说明。

```markdown
##### 测试结果

> **测试方法**：使用 `redis-benchmark -c 50 -P 1 -d 64 HSET key:__rand_int__ value` 灌入指定数据量的随机 key（Hash 引擎）。
> 写入计时由脚本用 `date +%s%N` 测量，SAVE 耗时重复 save_count 次取平均值。

| 场景 | 数据量 | SAVE 次数 | 写入 QPS | 写入耗时 | SAVE 总耗时 | 平均每次 SAVE | 有效 QPS |
|---|---|---|---|---|---|---|---|
| **100w → SAVE × 1** | 100万 | 1 | <100w_qps> | <100w_wtime>s | <100w_stime>s | **<avg_100w>ms** | **<100w_eqps>** |
| 10w → SAVE × 10 | 10万 | 10 | ... | ... | ... | ... | ... |
| 1w → SAVE × 100 | 1万 | 100 | ... | ... | ... | ... | ... |
| 1k → SAVE × 1000 | 1千 | 1000 | ... | ... | ... | ... | ... |

> 有效 QPS = 数据量 / (写入时间 + 平均每次 SAVE 时间)
```

用 CSV 中的实际值替换占位符。

- [ ] **Step 3: 更新结果分析段落**

根据新数据更新 README 中结果分析的文字（约行 4359-4421 和 4466+），替换所有对旧数据的引用（如 127,064 → 新值）。

**关键改动**：
- 将 "SET" 改为 "HSET"
- 将旧的 QPS 数字（127,064, 137,174, 134,953, 135,318 等）替换为新数据
- ECHO 部分的分析保持逻辑，数字更新
- SAVE 部分的分析根据新数据重写结论

- [ ] **Step 4: 提交**

```bash
git add README.md
git commit -m "docs: 更新持久化性能基准数据 (v2 统一参数 HSET 100w)"
```

---

### Task 8: 最终验证

- [ ] **Step 1: 检查 README 中无旧数据残留**

```bash
grep -n "127,064\|137,174\|134,953\|135,318\|123,305\|133,690\|46,468\|117,233" README.md
```
Expected: 无匹配（所有旧 QPS 数字已被替换）

- [ ] **Step 2: 检查 CSV 和 README 表格数据一致**

```bash
echo "CSV kvstore_echo:"
grep "kvstore_echo" benchmarks/data/persist_bench/aof_summary.csv
echo "README kvstore_echo:"
grep "kvstore.*ECHO" README.md | head -1
```
手动确认两边数字一致。

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "chore: 清理 README 旧性能基准数据"
```
