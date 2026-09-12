# kvstore vs Redis：SAVE + AOF always 对比分析报告

**日期**：2026-08-01
**测试条件**：KVM + Soft-RoCE 虚拟机，/dev/sda5 (ext4)，`redis-benchmark -c 50 -P 1 -d 64 -r N HSET key:__rand_int__ value`，每场景重启 + 删 AOF 从空库开始，同环境串行采样。Redis 为 5.0.7，HSET 用 3-arg（`key field value`）。
**方法**：每个（模式 × 场景）跑 3 次，取均值 + 标准差（CoV）看波动。

> **⚠️ 语义现状更新（2026-08-06）**：本报告写于默认仍为"同步批量"时期，文中异步批量模式下「不等落盘」已演进为现默认的**先回复 + 有界窗口**语义——回包不等 fsync，AOF 独立线程按磁盘最大 fsync 率定频刷盘，崩溃窗口稳态 ~1.3ms / 最坏 ~7ms（受 `MAX_OUTSTANDING=16` 背压约束），不再对齐 redis 的落盘才回包。下文各模式的实测数据（吞吐/波动）仍有效；语义描述以本文与 `aof-fsync-modes-analysis.md` 的现状更新为准。
>
> **⚠️ 吞吐现状更新（2026-08-11）**：现默认异步批量新增攒批窗口 `aof_group_commit_window_us=2000`，AOF always 100w QPS 从文中的 ~87.8k 提升到 **118.7k**（追平 AOF 关闭 124.3k，仅 −4.5%），fsync 吞吐瓶颈消除，崩溃窗口稳态 ~2.5ms / 最坏 ~10ms。文中的「异步批量 −19% 开销」已过时，新数据见 README「AOF 并发性能对比」。

## 1. 背景

上轮分析（[aof-fsync-modes-analysis.md](../aof-fsync-modes-analysis.md)）已确认：kvstore 的 AOF always 方差根源是**异步架构**（run-ahead → io_uring ring 满 → 停滞 → 突发），并建议**同步批量（`--aof-fsync-sync-batch`）改为默认**；该建议曾于 08-01 落地为默认，后经多次调整，现定为**异步批量 group commit 默认**（`src/main/kvstore.c` 现默认 `.aof_fsync_per_command=0, .aof_fsync_sync=0`）——攒批共享 fsync，吞吐/稳定性兼顾。

本轮按 README「SAVE + AOF always 对比」方法论：
- **kvstore 异步批量 group commit**（现默认，per_command=0, sync=0）
- **kvstore 异步逐条**（opt-in，`--aof-fsync-per-command`，per_command=1, sync=0）
- **kvstore 同步批量**（opt-in，`--aof-fsync-sync-batch`，per_command=0, sync=1）
- **Redis 7.2.9**（`--appendonly yes --appendfsync always`，作为基线）

并顺带回答一个实测中暴露的问题：**为什么 Redis 的 QPS 从 100w 到 1k 都稳定，而 kvstore 在 1k 场景大幅下跌？**

## 2. 测试配置

| 配置 | 启动方式 | 说明 |
|---|---|---|
| 同步批量 | `./kvstore --appendfsync always --aof-fsync-sync-batch` | 批量攒满 epoll 周期，一次同步 `pwrite+fdatasync`（阻塞），每批摊薄 fsync |
| 异步批量（现默认） | `./kvstore --appendfsync always` | 命令攒批，每个 epoll 周期一次 io_uring linked write+fsync（不等落盘） |
| 异步逐条 | `./kvstore --appendfsync always --aof-fsync-per-command` | 每条命令 append 后立即异步 io_uring 提交（不等落盘） |
| Redis 7.2.9 | `/opt/redis-7.2.9/bin/redis-server --appendonly yes --appendfsync always` | HSET 3-arg，per-iteration 同步 fsync |

> 注意：异步逐条/同步批量等模式为 opt-in（`--aof-fsync-per-command` / `--aof-fsync-sync-batch`）；现默认异步批量（per_command=0, sync=0）无需配置。

写入参数统一：`redis-benchmark -n N -c 50 -P 1 -d 64 -r N HSET key:__rand_int__ value`（Redis 为 `__rand_int__ value` 前补 field）。**独立批口径**：每个场景重复 `count=100w/N` 批，每批重启空库（删 AOF/dump）→ 写 N 条 → SAVE 一次，记一个 QPS 与一次 SAVE 耗时，取均值/中位数（1k 场景 = 1000 个样本）。

## 3. 完整数据（独立批口径）

### 3.1 写入 QPS 均值（含 AOF 关闭基线）

| 场景 | 批次数 | AOF 关闭 | 异步批量(现默认) | 同步批量 | 异步逐条 | Redis 7.2.9 |
|---|---|---|---|---|---|---|
| **100w** | 1 | **108,163** | 87,788 | 68,379 | 63,692 | 44,324 |
| **10w** | 10 | 103,309 | 84,591 | **86,218** | 57,826 | 45,699 |
| 1w | 100 | 93,681 | 70,007 | **72,955** | 50,631 | 41,752 |
| 1k | 1000 | 61,613 | **50,115** | 50,477 | 37,370 | 43,008 |

> 100w 场景为 **5 次均值**（原单次 n=1 偏低）。其余场景 QPS 每批独立（写 N→SAVE→删库重启）测量；中位数与均值偏差 <5%。AOF 关闭 = `--aof-disable`。

### 3.2 QPS 批间波动（CoV）

| 场景 | AOF 关闭 | 异步批量(现默认) | 同步批量 | 异步逐条 | Redis 7.2.9 |
|---|---|---|---|---|---|
| 10w | 1.9% | 11.5% | 8.5% | 16.3% | 1.7% |
| 1w | 9.0% | 19.9% | 15.1% | 27.4% | 2.6% |
| 1k | 9.7% | 21.3% | 22.4% | **34.2%**（min 9.3k / max 71k） | 8.3% |

### 3.3 平均 SAVE 延迟（ms）

| 场景 | AOF 关闭 | 异步批量(现默认) | 同步批量 | 异步逐条 | Redis 7.2.9 |
|---|---|---|---|---|---|
| 100w | 94.6 | 106.6 | 91.8 | 95.6 | **645** |
| 10w | 9.9 | 9.1 | 9.6 | 9.5 | 47.7 |
| 1w | 3.9 | 3.6 | 3.4 | 3.4 | 7.9 |
| 1k | 3.3 | 3.1 | 3.1 | 3.1 | 4.1 |

> SAVE 延迟几乎与 AOF 开关无关（SAVE 路径独立于 AOF 刷盘模式），100w ~90-107ms；Redis 慢 6×。AOF 关闭 100w QPS 108,163 与 README SAVE-only 表（108,163）一致，跨表可比。

## 4. 吞吐与稳定性对比（稳态 100w/10w）

| 指标 | 异步批量(现默认) | 同步批量 | 异步逐条 | Redis 7.2.9 | 异步批量 vs Redis |
|---|---|---|---|---|---|
| 写入 QPS（100w） | **87,788** | 68,379 | 63,692 | 44,324 | **+98%** |
| QPS 波动（CoV，10w/1w） | ±12-20% | ±9-15% | ±16-27% | ±1.7-7.8% | 略高 |
| 写入 QPS（10w） | 84,591 | **86,218** | 57,826 | 45,699 | +85% |
| SAVE 100w | 106.6ms | **91.8ms** | 95.6ms | 645ms | **6× 更快** |
| SAVE 10w | **9.1ms** | 9.6ms | 9.5ms | 47.7ms | **5.2× 更快** |

**结论：异步批量（现默认）100w 吞吐最高（+98% vs Redis），但批间波动（±12-20%）高于同步批量（±9-15%）——异步架构 run-ahead→ring 满→停滞→突发 的代价。同步批量更稳、吞吐略低。两者都远超异步逐条（±16-27%）与 Redis。**

**AOF always 开销**（相对 AOF 关闭基线 100w QPS 108,163）：异步批量（现默认）**−19%**（87.8k）、同步批量 −37%（68.4k）、异步逐条 −41%（63.7k）、Redis −62%（44k，其无 AOF 基线 118k）。异步批量攒批摊薄最省。

## 5. 为什么 Redis 的 QPS 从 100w 到 1k 都稳定，而 kvstore 在 1k 大幅下跌？

### 5.1 现象

| 模式 | 100w 均值 | 1k 均值 | 绝对跌幅 | 百分比 |
|---|---|---|---|---|
| Redis 7.2.9 | 44,324 | 43,008 | ~1.3k | **-3%** |
| kvstore AOF 关闭 | 108,163 | 61,613 | ~46.6k | **-43%** |
| kvstore 异步批量(现默认) | 87,788 | 50,115 | ~37.7k | **-43%** |
| kvstore 同步批量 | 68,379 | 50,477 | ~17.9k | **-26%** |

**连 AOF 关闭都跌 43%**——1k 下跌的主因不是 fsync，而是每批"空库重启 + 50 连接建立"的启动相位。1k 场景下 异步批量（50.1k）/同步批量（50.5k）> Redis（43.0k）；异步逐条（37.4k）因批间高波动最低。看似 Redis「稳」、kvstore「退化」，实际原因分两层。

### 5.2 根因一：Redis 的稳定是"被 fsync 锁死在低吞吐"的假象

实测 Redis 7.2.9 **无 AOF = 116,965 QPS**，**AOF always = 43,957 QPS**（并发对比表，3 次均值）。Redis AOF always 每事件循环迭代同步 fsync，RTT 恒定 ~1.1ms：

| 数据量 | Redis 7.2.9 QPS | 隐含 RTT（`-c 50 -P 1` 下 QPS ≈ 50/RTT） |
|---|---|---|
| 1k | 41,025 | 1.22ms |
| 100w | 44,385 | 1.13ms |

RTT 在 1k 和 100w 几乎一样 → 吞吐被压在 ~44k，**怎么测都"稳定"**。它不是稳，而是天花板本来就低。5.0.7 同口径测 45,104（AOF always）——**版本几乎不影响该结论**。

### 5.3 根因二：kvstore 的 1k 下跌是"启动相位"测量 artifact，不是服务端退化

对三种配置做 N 扫描（n=1000→100k，每种都重启空库）：

| config | n=1000 | n=5000 | n=20k | n=100k | 稳态 |
|---|---|---|---|---|---|
| sync_batch | 45,455 | 57,471 | 67,114 | 66,800 | ~67k |
| async | 52,632 | 32,680 | 47,619 | 56,754 | ~57k |
| **aof_off（零持久化）** | 62,500 | 98,039 | 104,712 | 110,865 | ~110k |

关键证据：**连 AOF 关闭的 kvstore 都从 62.5k 爬到 110k（+77%）**——1k 下跌的主要成分不是 fsync，而是「服务器刚启动 + 50 条 TCP 连接刚建立 + 冷 AOF 文件 + 客户端事件循环冷启动」的**启动相位**。该开销是固定 ~0.3ms/请求（redis-benchmark 7.2.9 重测，原 5.0.7 工具同结论）：

- aof_off：1k RTT 0.80ms → 稳态 0.45ms
- sync_batch：1k RTT 1.10ms → 稳态 0.75ms

### 5.4 为什么百分比跌幅差这么多（Redis 8% vs kvstore 20%+）

同一个固定启动开销 ~0.3ms，摊在不同高度的稳态 RTT 上：

| 模式 | 稳态 RTT | 1k RTT | RTT 抬升 | 观测跌幅 |
|---|---|---|---|---|
| Redis 7.2.9 | 1.13ms | 1.22ms | +8% | **-8%** |
| kvstore sync_batch | 0.75ms | 1.10ms | +47% | **-22%** |
| kvstore async | ~0.88ms | 波动大 | 高 | **-32%** |

**Redis 稳态 RTT 高，启动开销占比小 → 跌得少；kvstore 稳态 RTT 低，启动开销占比大 → 跌得多。** 但绝对值上 kvstore 1k 并不输 Redis：sync_batch 49.6k > Redis 41.0k；async 36.2k 因批间波动最低。

### 5.5 独立批口径下异步逐条 1k 跌幅反而更大

同步批量每 epoll 周期同步 fdatasync，小样本时 fsync 摊不薄。独立批口径下，**异步逐条 1k 跌幅（-32%）大于同步批量（-22%）**：每批独立重启暴露了异步架构 run-ahead→ring 满→停滞→突发 的批间方差（1k CoV 33.3%，min 9,346 / max 71,429），个别慢批把均值大幅拉低。旧 3 次稳态测量反映的是稳态批量下 ring 动态被摊薄后的情况，独立批测量对异步更苛刻。

### 5.6 结论

**Redis 平 = 被 fsync 锁在 ~44k 低吞吐，测什么都一样；kvstore 跌 = 1k 把 benchmark 压进了启动相位，而 kvstore 稳态吞吐高（RTT 低），启动开销占比大。** 1k/1w 的 QPS 是测量 artifact，不作稳态结论依据；对比真实能力以 100w/10w 为准。独立批口径下异步逐条因批间高波动（CoV 33.3%）在 1k 跌幅最大。

## 6. 最终结论

1. **异步批量 group commit（现默认）是吞吐最优方案**：100w ~88k QPS（> Redis 44.3k，+98%）、AOF 开销仅 -19%（攒批摊薄）、SAVE 6× 快于 Redis；代价是批间波动 ±12-20%（异步架构 run-ahead→ring 满→停滞→突发）。同步批量更稳（±9-15%）但吞吐略低（~68k）。**当前默认已定为异步批量**，如追求极致稳定可切 `--aof-fsync-sync-batch`。
2. **异步逐条批间波动最大**（1k 场景 CoV 34.2%），且不攒批、高 P 被 ring 卡住，仅 P=1 领先；其价值在于响应不阻塞在 fsync 上、单命令延迟更低。
3. **Redis 的"稳定"不代表更强**：它是被每迭代同步 fsync 锁在低吞吐上（100w 到 1k 都 ~44k；5.0.7→7.2.9 几乎不变）。kvstore 的批量模型对 fsync 的摊薄远优于 Redis。
4. **1k 批的 kvstore QPS 偏低**（异步批量 50.1k > Redis 43.0k）系每批含"空库启动 + 50 连接建立"固定开销所致，是测量相位 artifact，不代表大批量能力；真实能力以 100w/10w 为准。

## 7. 复现方法

```bash
# kvstore 异步批量 group commit（现默认，无需额外 flag）
./kvstore --port 5190 --role master --mem libc --net reactor --appendfsync always
# kvstore 异步逐条（opt-in）
./kvstore --port 5190 --role master --mem libc --net reactor \
    --appendfsync always --aof-fsync-per-command
# kvstore 同步批量（opt-in）
./kvstore --port 5190 --role master --mem libc --net reactor \
    --appendfsync always --aof-fsync-sync-batch
# Redis 7.2.9（/opt/redis-7.2.9/bin）
/opt/redis-7.2.9/bin/redis-server --port 6390 --dir /tmp --save "" --appendonly yes --appendfsync always

# 独立批循环：重启空库 → 写 N → SAVE，重复 count=100w/N 次
for i in $(seq 1 $count); do
    pkill -x kvstore; rm -f kvstore.dump kvstore.aof   # 删文件 + 重启 = 清空
    ./kvstore --port 5190 --role master --mem libc --net reactor --appendfsync always &   # AOF 关闭用 --aof-disable
    sleep 0.3
    redis-benchmark -p 5190 -n $N -c 50 -P 1 -d 64 -r $N HSET key:__rand_int__ value   # 记 QPS
    redis-cli -p 5190 SAVE                                                            # 记耗时
done
```

原始数据：`/tmp/kvstore_save_aof_cmp/result_v4_gc.csv`（独立批，5555 样本，含 aof_off/异步批量/异步逐条/同步批量/redis）。
脚本：`/tmp/run_save_v4.sh`（`run_save_v4.sh aof_off` 只跑 AOF 关闭）。

## 8. 相关文档

- [aof-fsync-modes-analysis.md](../aof-fsync-modes-analysis.md) — 四种 AOF 刷盘模式的方差根因分析（异步架构放大 fsync 抖动）
- [save-benchmark.md](../save-benchmark.md) — SAVE 性能基线
