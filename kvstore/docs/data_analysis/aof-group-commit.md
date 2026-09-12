# AOF 异步批量攒批窗口优化历程（2026-08-11）

**一句话**：AOF always 的吞吐瓶颈是 **fsync 率**（每批一次 fsync），不是 CPU。引入**时间窗攒批**（`aof_group_commit_window_us=2000` 默认）后，100w QPS 从 **78.8k → 118.7k**（追平 AOF 关闭 124.3k，仅 −4.5%），崩溃窗口稳态 ~2.5ms。

> 本文档记录完整优化历程：问题 → 逐步数据分析 → 优化方法 → 验证结果 → 遇到的问题（方法论坑）。测试方法论详见 [`benchmark-methodology-qa.md`](benchmark-methodology-qa.md)。

---

## 1. 背景：AOF always 的吞吐瓶颈

现默认 AOF always 为**异步批量 group commit + 先回复**（回包不等 fsync，AOF 独立线程 io_uring 刷盘）。先回复语义下，**单请求延迟**与 fsync 解耦——但**吞吐**仍被 fsync 率钉死：

```
吞吐 ≈ 批大小 × fsync 率
```

关键事实：**每 epoll 周期 flush 一次 = 每批一次 write+fsync**（reactor.c 循环末尾 `persist_flush_pending()`）。`-c 50 -P 1` 下批≈50 条，fsync ~0.66ms → 吞吐 ≈ 50/0.66ms ≈ **76k**，正好是实测值。AOF 关闭（纯 CPU）→ 125k。

**问题定义**：即使先回复，主循环仍受 `MAX_OUTSTANDING=16` 背压把生产节奏钉在 AOF 线程的 fsync 完成率上。批大小不变 → 吞吐卡死。

---

## 2. 逐步数据分析

### 第一步：定位瓶颈 = fsync 率（非 CPU）

| 实验 | 结果 | 结论 |
|---|---|---|
| fdatasync 微基准（KVM /dev/sda5 ext4，1KB 追加 ×100） | 中位 **362µs** / p99 1ms | fsync 延迟 ~0.4-1ms 量级 |
| AOF always `-c 50 → 100`（批大小翻倍） | 75.9k → **92.1k（+22%）** | 吞吐随批大小缩放 → **fsync 率受限** |
| AOF 关闭 `-c 50 → 100`（对照组） | ~125k → ~118k（不变） | 纯 CPU 上限，无 fsync 闸门 |

### 第二步：吞吐模型

```
吞吐 = 批大小 × fsync 率 = 50 × (1/0.66ms) ≈ 76k   ✓ 与实测吻合
```

`MAX_OUTSTANDING=16` 只是让主循环最多超前 16 批，稳态吞吐仍被「AOF 线程每秒完成多少批 fsync」钉死。

### 第三步：攒批窗口对照实验

改动 flush 节奏为「攒满 N µs 再 flush」，窗口 0 / 2000 / 4000 对照：

| 配置 | QPS 中位数 | 相对现状 | 批龄峰值（INFO aof_max_batch_age_us） |
|---|---|---|---|
| **window=0（现状，每周期 flush）** | **75,706** | — | 1963-9443 |
| **window=2000µs** | **115,781** | **+53%** | 4845-10607 |
| window=4000µs | 113,849 | +50% | 5323-9849 |
| aof_off（参照） | 113,498 | — | — |

**结论**：窗口 **2000µs 就足够**（115.8k ≥ aof_off 113.5k，fsync 瓶颈消除）；4000 不更高（顶到 CPU 上限）。批龄峰值与 window=0 的最坏值同档（~10ms，多为罕见调度停顿）——稳态上 window=0 为 ~0.7ms、window=2000 为 ~2.5ms，多 1.8ms 换 +53% 吞吐。

---

## 3. 优化方法：`aof_group_commit_window_us` 时间窗攒批

**核心思想**：把「每 epoll 周期 flush」改成「攒批窗口未到且槽未满 → 跳过 handoff，多攒几周期共享一次 fsync」。窗口=0 时行为与旧版完全一致。

### 实现

| 文件 | 改动 |
|---|---|
| `include/kvstore/kvstore.h` | `kv_config_t.aof_group_commit_window_us`；`kvs_now_us()`；`persist_aof_max_batch_age_us()` |
| `src/expire/kvs_expire.c` | 新增 `kvs_now_us()`（µs 级 gettimeofday） |
| `src/main/kvstore.c` | 默认 `=2000`；CLI `--aof-group-commit-window-us`；conf key；INFO `aof_max_batch_age_us` |
| `src/persistence/kvs_persist.c` | `persist_flush_pending()` 改窗口感知；新增 `persist_flush_pending_force()`（drain/槽满强制）；批龄跟踪；`persist_aof_pending_flush_ms()` |
| `src/core/reactor.c` | `epoll_wait` 超时 = min(100, AOF 窗口剩余)（稀疏流量下保证 ~窗口量级 flush，2026-08-12） |

关键逻辑（`kvs_persist.c`）：
- 异步追加时记录 `g_batch_start_us`（批龄起点）
- `persist_flush_pending()`（reactor/ntyco/proactor 每循环调）：窗口未到且槽未满 → 跳过 handoff，仅 reap 回收 completed 槽
- `persist_flush_pending_force()`：无视窗口立即 handoff，供 **drain（SAVE/关停）** 与 **槽满（AOF_SLOT_MAX 4MB 兜底）** 使用
- `persist_aof_pending_flush_ms()`：返回距窗口结束的剩余时间，reactor 据此缩短 epoll 超时（见 §4.3 稀疏修复）
- 语义不变：**先回复**（回包不等 fsync）、崩溃窗口有界（时间窗 + `MAX_OUTSTANDING=16` 双约束）

### 配置

```
# 命令行
./kvstore --appendfsync always --aof-group-commit-window-us 2000
# 配置文件（默认已启用）
aof_group_commit_window_us=2000    # 0=每 epoll 周期 flush（旧行为）
```

---

## 4. 验证结果

### 4.1 AOF 并发套件（新默认，10 轮中位数）

| 配置 | 旧默认 | **新默认** | 变化 |
|---|---|---|---|
| kvstore AOF always | 78,824 | **118,725** | **+51%** |
| kvstore AOF 关闭 | 125,157 | 124,271 | — |
| Redis AOF always | 43,330 | 43,522 | — |

AOF always 现在只比 AOF 关闭低 **4.5%**（118.7k vs 124.3k），比 Redis 快 **2.7×**，批间 CoV **1.4%**（旧逐条模式 16-34%）。

### 4.2 Pipeline 各 P（对 redis 的领先）

| P | kv AOF always | redis AOF always | kv/redis | kv AOF开销 | redis AOF开销 |
|---|---|---|---|---|---|
| 1 | 118,709 | 44,283 | **268%** | 97% | 38% |
| 10 | 615,006 | 258,598 | **238%** | 91% | 41% |
| 40 | 947,867 | 484,731 | **196%** | 93% | 56% |
| 160 | 1,102,536 | 648,508 | **170%** | 94% | 66% |

**AOF开销** = 各自 `AOF always ÷ AOF 关闭`（越高开销越小）。kv 恒定 ~91-97%（时间窗攒批、与 P 无关）；redis 从 38% 摊薄到 66%（同步 per-迭代 fsync 被 P 摊薄）。**比值下降是 redis 追自身天花板的比例效应，非 kv 变差**——kv 绝对领先仍从 74k 扩到 454k。

### 4.3 崩溃窗口代价

| 负载 | 旧默认（window=0） | **新默认（window=2000）** |
|---|---|---|
| 持续负载稳态 | ~0.7ms | **~2.5ms**（攒批窗口 + fsync） |
| 稀疏流量（孤立单条写） | ~1ms（同轮 flush） | **~5-7ms**（修复后，见下） |

> **稀疏流量修复（2026-08-12）**：初版 window=2000 下孤立命令要等 reactor 下次唤醒（epoll **100ms 超时**）才 flush，稀疏崩溃窗口膨胀到 ~100ms。已修复：`persist_aof_pending_flush_ms()` 返回距窗口结束的剩余时间，reactor 的 `epoll_wait` 超时在有未 flush 槽时压到 min(100, 剩余)，稀疏落盘降到 **~6.6ms**（≈ 窗口 + 唤醒 + fsync）。持续负载吞吐不受影响（epoll 有事件时本就立即返回）。

先回复语义下回包延迟不增，单命令延迟不受影响。对比 Redis everysec 接受 1s 窗口，这里余量大。

---

## 5. 遇到的问题（方法论坑）

数据分析中踩了三个方法论坑，均已在 [`benchmark-methodology-qa.md`](benchmark-methodology-qa.md) 第七节详述：

1. **redis-benchmark 单线程客户端上限**：kvstore ECHO 极快，所有 P 下被单线程客户端封顶（双客户端并发实测各 P +36-40%）。**判断客户端是否瓶颈要加客户端线程数，不是加 `-c` 连接数**（单线程加连接数线程更忙，QPS 不变）。
2. **ms 精度计时量化**：`RPS = n/elapsed_ms`，两个服务器落同一 ms 桶会报告**逐位相同**的 QPS（曾把 ECHO P=20 的 `2,083,333 == 2,083,333` 当真实相等）。跨配置「精确相等」必须回原始轮次核对。
3. **ECHO P=20 比值谷底**（~101%）是**测量巧合**：P=20 处单客户端上限（~2.05M）恰好等于 redis 服务端速率（~2.05M），kv 被客户端压在此处、redis 自身也到此。**不是服务端曲线交叉**。

4. **稀疏流量崩溃窗口膨胀（2026-08-12 发现并修复）**：window=2000 下孤立命令处理完 `persist_flush_pending` 看到批龄 < 窗口就跳过 handoff，随后 reactor 睡进 epoll_wait（100ms 超时），要等下次唤醒才 flush → **稀疏崩溃窗口膨胀到 ~100ms**（实测 window=0 同轮 flush ~1ms、window=2000 无修复时 ~105ms）。修复见 §4.3：reactor 超时压到窗口剩余时间，落盘降到 ~6.6ms。**教训：验证崩溃窗口不能只看持续负载，必须测稀疏单条写。**

> 反例对照：**HSET 的 kv/redis 比值随 P 下降是真实服务端机制**（HSET 两侧均经双客户端验证为服务端瓶颈，无客户端 artifact）——下降由 redis 的 AOF 开销随 P 摊薄驱动（见 §4.2）。

## 6. 实验：落盘才回包 + io_uring（负结果，2026-08-12）

**问题**：能否用 io_uring 实现「落盘才回包」且吞吐不被 fsync 锁死？

**方法**：临时实现 `--aof-fsync-io-sync`（io_uring 提交 write+fsync，主线程 handoff 后 fence 等本批 fsync 完成再回包），与默认先回复、同步批量对比：

| 模式 | P=1 | P=10 | 语义 |
|---|---|---|---|
| **async（默认，先回复）** | **113,889** | **600,442** | 回包不等落盘 |
| io_sync（落盘才回包+io_uring） | 36,126 | 261,075 | 回包等本批 fsync |
| sync_batch（落盘才回包+直写） | 41,149 | 282,349 | 回包等 fsync |

**结论**：**io_uring 在「等 fsync」场景无利可图**——一旦回包必须等落盘，主线程等在那里没有重叠，io_uring 的异步提交优势用不上，反而多付出 handoff→AOF 线程→submit→CQE→reap→fence 的整条 IPC 往返，**比直接 `pwrite+fdatasync`（同步批量）还慢**（36k vs 41k）。这也解释了 Redis `always` 用同步直写而非 io_uring：它本来就等 fsync，io_uring 只有开销。

**核心洞察**：io_uring 的价值**恰恰在「不等」**（先回复 + 异步重叠）。该模式已回退（负结果不入产品）。

---

## 相关

- [`aof-fsync-modes-analysis.md`](../aof-fsync-modes-analysis.md) — 四种 AOF 刷盘模式的方差根因分析（含崩溃窗口现状更新）
- [`save-aof-always-mode-comparison.md`](save-aof-always-mode-comparison.md) — SAVE + AOF always 对比分析报告（含吞吐现状更新）
- [`optimization-history/aof-concurrent.md`](../optimization-history/aof-concurrent.md) — AOF 并发性能对比 · 分析
- [`benchmark-methodology-qa.md`](benchmark-methodology-qa.md) — 基准测试方法论 QA（第七节：客户端测量边界）
- 原始数据：`benchmarks/data/bench_aof_2026-08-11.json` / `bench_pipeline_2026-08-11.json`
