# 持久化性能基准重测 — 设计文档

**日期**: 2026-06-11
**状态**: 已确认

## 背景

README 中现有两套持久化性能测试数据：

1. **AOF 性能对比** — 用 `redis-benchmark` 测 kvstore vs Redis 在不同 AOF 策略下的 QPS
2. **SAVE 性能测试** — 用 `redis-cli --pipe` + shell 循环测不同 SAVE 频率对写入吞吐的影响

两套测试方法不一致，导致数据不可比：
- AOF 测试: `redis-benchmark -n 100000 -c 50 -P 1`（50 并发，无 pipeline），测出 ~135k QPS
- SAVE 测试: `redis-cli --pipe` + bash 循环 `date +%s%N`（单连接 + 进程启停开销），测出最好 ~49k keys/s

**根因**: 测试工具、并发度、Pipeline 设置都不统一，SAVE 测试的 shell 循环开销远超 kvstore 本身耗时。

## 目标

1. 统一所有测试使用 `redis-benchmark`，统一参数 `-n 1000000 -c 50 -P 1 -d 64`
2. AOF 部分重测 8 种配置（kvstore × 4 + Redis × 4）
3. SAVE 部分改为：`redis-benchmark` 纯写入基线 + 单独计时 SAVE，合并计算有效吞吐
4. 用新数据更新 README 表格

## 统一测试参数

```
redis-benchmark -n 1000000 -c 50 -P 1 -d 64 -r 1000000 HSET key:__rand_int__ value
```

- `-n 1000000`: 100 万次请求
- `-c 50`: 50 并发连接
- `-P 1`: 无 pipeline（每条命令独立请求-响应）
- `-d 64`: 64 字节 value（redis-benchmark 替换最后一个参数）
- `-r 1000000`: 100 万随机 key 空间（`__rand_int__` 替换为 0~999999，key 名如 `key:000000004213`）
- 命令 `HSET`: Hash 引擎，无容量限制

全部测试（AOF、SAVE 写入基线）使用同一命令。

## AOF + ECHO 测试矩阵

全部使用统一命令 `redis-benchmark -n 1000000 -c 50 -P 1 -d 64 -r 1000000`。

| 服务器 | echo | HSET no-AOF | HSET always | HSET everysec |
|--------|------|-------------|-------------|--------------|
| kvstore | ✓ | ✓ (`--aof-disable`) | ✓ (`--appendfsync always`) | ✓ (`--appendfsync everysec`) |
| Redis | ✓ | ✓ (无 `--appendonly`) | ✓ (`--appendonly yes --appendfsync always`) | ✓ (`--appendonly yes --appendfsync everysec`) |

ECHO 命令：`redis-benchmark ... echo`（直接传命令名作为位置参数）。
HSET 命令：`redis-benchmark ... HSET key:__rand_int__ value`。

## SAVE 测试设计

SAVE 命令 `redis-benchmark` 无法直接发送，因此拆为两步。使用 HSET 命令（Hash 引擎，无容量限制）创建唯一 key。

### Step 1: 纯写入基线

```
kvstore --aof-disable
redis-benchmark -n 1000000 -c 50 -P 1 -d 64 -r 1000000 HSET key:__rand_int__ value
```

记录 QPS，作为无 SAVE 干扰的 HSET 写入速度基线。

### Step 2: 不同数据量下的 SAVE 耗时

| 场景 | 数据量 | SAVE 次数 | redis-benchmark 命令 |
|------|--------|----------|---------------------|
| 100w → SAVE×1 | 1,000,000 | 1 | `-n 1000000 -r 1000000` |
| 10w → SAVE×10 | 100,000 | 10 | `-n 100000 -r 100000` |
| 1w → SAVE×100 | 10,000 | 100 | `-n 10000 -r 10000` |
| 1k → SAVE×1000 | 1,000 | 1000 | `-n 1000 -r 1000` |

每种场景：
1. 灌入对应数据量：`redis-benchmark ... HSET key:__rand_int__ value`
2. 发 `SAVE` 命令，用 `time redis-cli SAVE` 记录耗时
3. 重复 5 次取平均值
4. 合并计算：`有效QPS = 总数据量 / (写入时间 + avg_save_time × SAVE次数)`

## 脚本流程

```
Phase 1: 环境检查
  - 检查 kvstore, redis-benchmark, redis-server, redis-cli
  - 创建输出目录 benchmarks/data/persist_bench/
  - 清理残留进程 + 旧 AOF/dump 文件

Phase 2: AOF + ECHO
  - 每个配置: 启动服务器 → wait_port → redis-benchmark → 记录QPS → 停止服务器 → 清理文件
  - 输出: aof_summary.csv + aof_<label>.txt (原始输出)

Phase 3: SAVE
  - Step 1: 纯写入基线
  - Step 2: 按 [1k, 1w, 10w, 100w] 测 SAVE 耗时 × 5
  - 输出: save_summary.csv

Phase 4: 汇总
  - 终端打印对比表
  - 打印 FAIL 清单（如有）
```

## 错误处理

| 场景 | 处理 |
|------|------|
| 端口占用 | 提示 kill 占用进程，退出 |
| 二进制缺失 | 提示编译命令，退出 |
| redis-benchmark 缺失 | 提示安装命令，退出 |
| 服务器启动超时 (>60s) | 打印日志尾部，退出 |
| QPS 为 0 或 N/A | 标记 FAIL，继续下一个 |
| SAVE 失败 | 重试 3 次，仍失败标记 FAIL |

## 输出文件

```
benchmarks/data/persist_bench/
├── aof_summary.csv        # label,qps
├── save_summary.csv       # scenario,total_keys,write_qps,write_time_sec,save_count,save_time_sec,effective_qps
├── aof_kvstore_echo.txt
├── aof_redis_echo.txt
├── aof_kvstore_aof_disable.txt
├── aof_kvstore_aof_always.txt
├── aof_kvstore_aof_everysec.txt
├── aof_redis_no_aof.txt
├── aof_redis_aof_always.txt
├── aof_redis_aof_everysec.txt
└── aof_kvstore_write_baseline.txt
```

## README 更新

测试完成后，用新数据替换 README 中「持久化性能基准」段落（行 4292-4468）的表格数据。

## 不变更范围

- `tools/bench/bench_mem_backend.py` — 内存后端基准，不在本次范围
- SAVE 测试的 `redis-cli --pipe` 旧数据 — 保留在 `benchmarks/data/` 中不删除，仅 CSV 被新数据覆盖
