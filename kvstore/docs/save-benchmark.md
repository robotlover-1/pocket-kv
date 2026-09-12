# SAVE 持久化性能基准 — 测试记录

> **⚠️ 历史基准**：本文档为 2026-06-11 的早期 SAVE 数据，已被 2026-08 数据（SAVE+AOF always 对比，见 [`save-aof-always-mode-comparison.md`](data_analysis/save-aof-always-mode-comparison.md)）与 README「性能基准」取代。

## 测试目的

评估 `SAVE`（同步全量 dump）命令在不同数据量下的耗时，以及对有效写入吞吐的影响。

## 测试环境

- CPU: Intel Core Ultra 7 155H (4 vCPU) / 7.7GiB RAM
- OS: Ubuntu 20.04.6 / Linux 5.15.0-139 / KVM 虚拟机
- 磁盘: ext4 on /dev/sda5, write-through 缓存
- 编译: gcc -O2

## 测试方法

1. 使用 Hash 引擎（HSET 命令），避免 Array 引擎 1024 条上限
2. 四种数据规模：100w / 10w / 1w / 1k
3. 统一参数：`redis-benchmark -n <N> -c 50 -P 1 -d 64 -r <N> HSET key:__rand_int__ value`
4. 每个配置前重启服务器，清理 AOF/dump 文件
5. 测试脚本：`tools/bench/run_persist_bench.sh` Phase 3

```bash
# 100w 写入 + SAVE×1
redis-benchmark -p 5190 -n 1000000 -c 50 -P 1 -d 64 -r 1000000 HSET key:__rand_int__ value
time redis-cli -p 5190 SAVE
```

## 测试结果

### 最新数据 (2026-06-11, 渐进式 rehash 之后)

| 场景 | 数据量 | SAVE 次数 | 写入 QPS | 写入耗时 | 平均 SAVE | 有效 QPS |
|------|--------|----------|----------|---------|----------|---------|
| 100w → SAVE×1 | 100万 | 1 | 129,685 | 7.77s | 3,964ms | 85,232 |
| 10w → SAVE×10 | 10万 | 10 | 127,226 | 0.80s | 397ms | 20,976 |
| 1w → SAVE×100 | 1万 | 100 | 129,870 | 0.08s | 42ms | 2,313 |
| 1k → SAVE×1000 | 1000 | 1000 | 125,000 | 0.01s | 7ms | 141 |

> **有效 QPS** = 数据量 / (写入时间 + SAVE 总耗时)，模拟写入后 SAVE 的实际吞吐。

### 历史对比

| 场景 | 优化前 (旧 hash) | 优化后 (渐进式 rehash) | 提升 |
|------|-----------------|----------------------|------|
| 100w 写入 QPS | ~21,000 | 129,685 | **6.2×** |
| 100w 有效 QPS | ~19,000 | 85,232 | **4.5×** |
| SAVE 100w 耗时 | ~3.8s | 3,964ms | ~same (纯 I/O) |

> SAVE 耗时不变（纯磁盘 I/O），写入 QPS 因 hash 引擎渐进式 rehash 大幅提升。

## SAVE 耗时分析

### 耗时与数据量线性关系

| 数据量 | 平均 SAVE 耗时 | dump 文件大小 | 每万条耗时 |
|--------|---------------|-------------|-----------|
| 1000 | 7ms | ~32KB | ~70ms |
| 1万 | 42ms | ~320KB | ~42ms |
| 10万 | 397ms | ~3.2MB | ~40ms |
| 100万 | 3,964ms | ~32MB | ~40ms |

### SAVE 内部流程

```
persist_save_dump()
  ├── open(path, O_WRONLY|O_CREAT|O_TRUNC)
  ├── write 8B aof_offset header
  ├── kvs_dump_to_fd(fd):
  │   ├── 遍历 array 引擎 (KVS_ARRAY_SIZE 次)
  │   ├── 遍历 hash 引擎 ht[0] + ht[1] (双表, rehash 安全)
  │   ├── 遍历 rbtree 引擎 (中序遍历)
  │   ├── 遍历 skiptable 引擎 (第 0 层链表)
  │   └── 遍历 doc 引擎 (两层哈希)
  └── persist_fsync_fd(fd)
```

每写入一条 key-value：
```
[1B engine_id][4B klen][key][4B vlen][value]
```

### dump 二进制格式

```
[8B aof_offset]                              ← AOF 恢复位置
[1B engine_id=3][4B klen][key][4B vlen][val] ← Hash 引擎
[1B engine_id=1][4B klen][key][4B vlen][val] ← Array 引擎
...
```

## BGSAVE（异步）

BGSAVE 通过 `fork()` 子进程执行 dump，父进程继续服务：

```
persist_bgsave_start()
  ├── 捕获 aof_offset (父进程当前 AOF 位置)
  ├── fork()
  ├── 子进程: persist_save_dump_to(tmp_path, aof_offset) → rename → _exit
  └── 父进程: 记录 g_bgsave_pid, 继续服务

persist_bgsave_poll()
  └── waitpid(WNOHANG) → 检查子进程退出状态
```

**关键特性：**
- fork 时 COW (Copy-on-Write) 保证数据一致性
- 子进程持有 fork 时刻的数据快照
- 父进程在 fork 后继续处理请求（AOF 保证增量）
- `aof_offset` 记录 dump 时刻的 AOF 位置，恢复时跳过

## 恢复流程

```
persist_recover()
  ├── replay_dump_file(dump_path)        ← mmap 零拷贝
  │   ├── 读取 aof_offset header (8B)
  │   └── 按 engine_id 分发到各引擎
  ├── replay_file(aof_path, aof_offset)  ← mmap 跳过已 dump 部分
  │   └── parse_resp_stream 重放增量 RESP 命令
  └── kvs_active_expire_cycle(1000000)   ← 清理过期 key
```

## 结论

1. **SAVE 耗时纯磁盘 I/O (~40ms/万条)**，与引擎性能无关
2. **BGSAVE 推荐用于生产环境**（fork 子进程，不阻塞主线程）
3. **自动快照**（`autosnap` 规则）周期性触发 BGSAVE
4. **dump + AOF 双重持久化**：dump 保证全量恢复速度，AOF 保证增量不丢失
