# AOF 并发性能对比 · 分析

AOF always 并发场景（`redis-benchmark -c 50 -P 1`，HSET）下 kvstore 与 Redis 的差异分析。测试数据与方法见 `README.md`「持久化性能基准 → AOF 并发性能对比」。

## ① AOF always 现默认：异步批量 group commit

现默认 per_command=0/sync=0：命令攒批到 `g_aof_buf`，每个 epoll 周期结束一次 io_uring 异步提交（linked write+fsync）覆盖整批，响应不等落盘。其他模式（异步逐条 `--aof-fsync-per-command` / 同步批量 / 同步逐条）见 [aof-fsync-modes-analysis.md](../aof-fsync-modes-analysis.md)。

## ② kvstore AOF always vs Redis AOF always

现默认异步批量（65,091，2026-08-02 7 轮中位数，CoV 6.6%）达 Redis（45,199）的 **144%**。kvstore 使用 io_uring 提交 write+fsync，相比 Redis 同步 write()+fdatasync()，io_uring 将提交和完成解耦，事件循环在磁盘 I/O 期间继续处理其他连接。注：reactor 统一写优化（2026-08-02）使 ECHO/AOF 关闭提升（+23%/+15%），但 AOF always P=1 从 88k 降到 66k（统一写延迟响应 flush 与 AOF io_uring 批交互变差）——不过 Pipeline 各 P 下 AOF always 仍全面超 Redis（145%-204%），净收益为正。

> **2026-08-02 勘误**：此前的"66-88k 波动"部分来自**未绑 taskset** 的测量（客户端裸跑时吞吐虚高到 ~82k）。服务端与 redis-benchmark 客户端**都绑 taskset 核 2/3 后**，AOF 并发 P=1（65,091）与 SAVE 100w（64,177）一致（差 1.4%），CoV 2-7%——**AOF always 的波动主要是测量方法（绑核）不一致，非异步批量固有**。

## ③ AOF always 开销

kvstore AOF always（异步批量，现默认）相对 AOF 关闭（124,493）**−48%**（65,091）——异步批量攒批 fsync，但统一写优化下 AOF 路径与响应 flush 交互略增开销。Redis AOF always（45,199）vs 无 AOF（113,278）**−60%**，per-iteration fsync 开销无法摊销。

## ④ 先回复 + 有界窗口（reply-first）语义

现默认异步批量采用**先回复 + 有界窗口**：回包不等 fsync，AOF 独立线程按磁盘最大 fsync 率定频刷盘；回包只排队 ≤MAX_OUTSTANDING=16 批，崩溃窗口稳态 ~1.3ms / 最坏 ~7ms。如需严格 durable-before-reply（落盘后才回复），可用同步批量（`--aof-fsync-sync-batch`）。

## ④b 先回复（reply-first）改造后 P-sweep（2026-08-06/07）

AOF 独立线程 + write-in-place 槽 + 先回复改造（HEAD=98eeefc）后，完整 P-sweep 对比改造前基线（README 08-04 异步批量默认、durable-before-reply）。方法同口径：`redis-benchmark -n 1000000 -c 50 -P N -d 64 -r 1000000 HSET key:__rand_int__ value`，每 P 重启空库 + 预热取 1 轮。完整报告见 git 历史（原 docs/superpowers/bench/2026-08-06-aof-thread-reply-first.md，已随开发过程稿清理）。

| P | 新实现(reply-first) | 基线(README 08-04) | 变化 |
|---|---:|---:|---:|
| 1  | 76,476 | 42,717 | **+79%** |
| 10 | 419,463 | 373,692 | +12% |
| 20 | 694,927 | 660,066 | +5% |
| 40 | 796,178 | 891,266 | **−11%** |
| 80 | 1,026,694 | 1,014,199 | +1% |
| 160 | 1,054,852 | 1,024,590 | +3% |

- **P=1 +79% 是最大收益**：先回复让回包不卡 fsync（对应 Task 3 的 ≈88.7k，全扫波动 73–77k）。
- **高 P 持平/略升**（+1%~+12%），符合预期。
- **P=40 回退 −11%**（796k vs 891k），4 组复测 762–835k 均低于基线，非噪声；P=80/160 无此现象，根因未收敛（方向：先回复窗口背压 + 槽轮转与 io_uring 提交在 P=40 段交互）。

## 相关

- fsync 模式完整分析：[aof-fsync-modes-analysis.md](../aof-fsync-modes-analysis.md)
