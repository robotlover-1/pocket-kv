# kvstore AOF always 四种刷盘模式分析报告

> **⚠️ 现状更新（2026-08-03）**：本报告的结论（§5/§6）建议"同步批量改默认"。后续实测发现异步波动的根源是**测量方法**（非架构），**当前默认已定为异步批量 group commit**（`aof_fsync_per_command=0`），见 [`optimization-history/aof-concurrent.md`](optimization-history/aof-concurrent.md) 与 [`save-aof-always-mode-comparison.md`](data_analysis/save-aof-always-mode-comparison.md)。本文档保留为四种刷盘模式的分析记录。
>
> **⚠️ 语义现状更新（2026-08-06）**：现默认异步批量采用**先回复 + 有界窗口**——回包不等 fsync，AOF 独立线程按磁盘最大 fsync 率定频刷盘，崩溃窗口稳态 ~1.3ms / 最坏 ~7ms（受 `MAX_OUTSTANDING=16` 背压约束），不再对齐 redis 的落盘才回包语义。
>
> **⚠️ 语义现状更新（2026-08-11）**：默认异步批量新增**攒批窗口 `aof_group_commit_window_us=2000`**（`--aof-group-commit-window-us` 可调，0=退化为每 epoll 周期 flush 的旧行为）。攒批窗口消除 fsync 吞吐瓶颈：AOF always 100w QPS 78.8k → **118.7k**（追平 AOF 关闭 124.3k，CoV 1.4%）。崩溃窗口随窗口变宽：稳态 ~2.5ms / 最坏 ~10ms（含罕见调度停顿，与旧行为的最坏值同档）。回包仍不等 fsync（先回复语义不变），每命令延迟不受影响。

**日期**：2026-08-01
**测试条件**：KVM + Soft-RoCE 虚拟机，/dev/sda5 (ext4)，`redis-benchmark -c 50 -P 1 -d 64 -r N HSET key:__rand_int__ value`，每场景重启 kvstore + 删 AOF，同环境串行采样。

## 1. 背景

kvstore 的 AOF always 原始实现是**异步批量（group commit）**：命令攒批，每个 epoll 周期一次 io_uring 异步 write+fsync。实测其 QPS 波动高达 **±12%**，而 Redis 的 AOF always 只有 **±4.6%**。本报告通过实现四种刷盘模式并实测，定位方差的根本来源。

## 2. 四种模式的实现方案

| 模式 | 配置开关 | 实现 |
|---|---|---|
| **异步批量**（group commit） | `--aof-fsync-group-commit`（`per_command=0, sync=0`） | 命令 append 到 `g_aof_buf`；每个 epoll 周期结束 `persist_flush_pending()` 提交 **linked write+fsync 到 io_uring**（异步，不等完成）；响应立即发送 |
| **异步逐条**（现默认） | 默认（`per_command=1, sync=0`） | `persist_append_prepare` 里每条命令 append 后立即调 `persist_flush_pending()`（异步 io_uring 提交，不等） |
| **同步批量** | `--aof-fsync-sync-batch`（`per_command=0, sync=1`） | `persist_flush_pending()` 里 `sync=1` 时改为**直接 `pwrite + fdatasync`（阻塞等落盘）**；批量攒满 epoll 周期一次同步落盘 |
| **同步逐条** | `--aof-fsync-sync`（`per_command=1, sync=1`） | `persist_append_prepare` 的逐条分支 `sync=1` 时调 `persist_sync_append_write()`：每条命令 **直接 `pwrite + fdatasync`（阻塞）** |

核心代码：

```c
/* persist_append_prepare：逐条分支 */
if (g_cfg.aof_fsync_per_command) {
    if (g_cfg.aof_fsync_sync) persist_sync_append_write();  /* 同步逐条 */
    else                      persist_flush_pending();      /* 异步逐条 */
    return KVS_PERSIST_OK;
}

/* persist_flush_pending：同步批量分支 */
if (g_cfg.aof_fsync_sync) { persist_sync_append_write(); return; }  /* 同步批量 */
/* ... 否则异步 io_uring 批量 ... */
```

## 3. 实测数据

### 3.1 QPS 与波动（c=50, n=1M，aof_always，5 次采样）

| 模式 | 5 次 QPS | 中位数 | 波动 |
|---|---|---|---|
| 异步批量 | 85,273 / 92,293 / 85,653 / 96,394 / 85,273 | ~89k | **±12%** |
| 异步逐条 | 56,856 / 60,415 / 65,634 / 62,316 / 60,092 | ~60k | **±12-15%** |
| 同步批量 | 70,096 / 70,417 / 71,515 / 71,808 / 71,362 | ~71k | **±2.4%** |
| 同步逐条 | 3,754 / 4,116 / 3,927 / 4,094 / 4,071（n=20k） | ~4k | ~9% |

对比 Redis：44k QPS，±4.6%。

### 3.2 磁盘 fdatasync 原始延迟

直接测量（pwrite+fdatasync 循环）：**min 285µs / avg 320µs / max 643µs**——延迟抖动近 **2 倍**。

### 3.3 吞吐量对比矩阵

```
                   异步                  同步
    ┌─────────────┬────────────────┬──────────────┐
批量 │ group commit │ 89k, ±12%      │ sync-batch    │ 71k, ±2.4% ✓
逐条 │ per-command  │ 60k, ±12-15%   │ sync-per-cmd   │ 4k, ~9%
    └─────────────┴────────────────┴──────────────┘
```

## 4. 方差根源分析

### 4.1 根因：磁盘 fdatasync 延迟不稳定

此磁盘 fdatasync 延迟在 **285-643µs** 间波动（2 倍），这是所有方差的底层来源。

### 4.2 异步架构放大方差

异步模式（批量/逐条）下，reactor 以 CPU 速度**跑在磁盘前面**：
- 命令异步提交 io_uring，响应立即发；
- 磁盘稍慢 → **io_uring ring（512 批）填满** → `persist_flush_pending` 的 `sq_retry` 循环空转停滞（[kvs_persist.c:736-740](src/persistence/kvs_persist.c#L736-L740)）；
- 磁盘赶上 → 补偿性突发。

这个 **"run-ahead → ring 填满 → 停滞 → 突发"** 的 stop-and-go 模式，把 fsync 抖动放大成吞吐突刺 → **±12%**。

### 4.3 同步模式消除放大

- **同步批量**：每批 fdatasync **阻塞** reactor，无 run-ahead、无 ring 填满 → fsync 抖动被批内平均 → **±2.4%**（甚至优于 redis 的 ±4.6%）。
- **同步逐条**：每条命令被自己的 fsync 门控 → **~9%**，但因无批处理摊薄，fsync 320µs/命令 → 吞吐暴跌到 4k。

### 4.4 为什么 Redis 稳

Redis AOF always 是**同步批量**模型：每个事件循环批量的 write+fdatasync **同步阻塞**，把 ~320µs 的 fsync 摊薄到一批命令，抖动被批内平均 → ±4.6%，且吞吐 44k。

## 5. 结论

1. **kvstore 方差的根源是异步架构**，不是逐条/批量本身。
2. **同步批量（`--aof-fsync-sync-batch`）是最优方案**：71k QPS（> redis 44k）+ ±2.4% 波动（< redis ±4.6%），同时满足快和稳。
3. 异步逐条（现默认）在速度（60k）和稳定性（±12-15%）上都劣于同步批量，**建议改为同步批量默认**。
4. 同步逐条因无 fsync 摊薄而不可用（4k），只作实验对照。

## 6. 后续可选优化

- **同步批量改默认**，重跑全套基准确认。
- 若需保留异步批量更高的峰值（89k），可做**平滑背压**：ring 满时渐进限速而非停滞，兼顾吞吐与稳定。
- 跨磁盘/真实硬件（NVMe）下 fdatasync 延迟更低更稳，同步批量优势会更大。

## 7. 复现方法

```bash
# 异步批量
./kvstore --appendfsync always --aof-fsync-group-commit
# 异步逐条（默认）
./kvstore --appendfsync always
# 同步批量
./kvstore --appendfsync always --aof-fsync-sync-batch
# 同步逐条
./kvstore --appendfsync always --aof-fsync-sync
redis-benchmark -p 5190 -n 1000000 -c 50 -P 1 -d 64 -r 1000000 HSET key:__rand_int__ value
```
