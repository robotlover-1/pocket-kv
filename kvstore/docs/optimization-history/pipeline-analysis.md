# Pipeline 批量性能测试 · 结果分析

Pipeline 深度 P=1/10/20/40/80/160 下 kvstore 与 Redis 的对比分析。测试数据与方法见 `README.md`「Pipeline 批量性能测试」。

## ① ECHO：全 P 持平或反超 Redis

reactor 统一写优化（事件循环末尾批量 drain）后，kvstore ECHO 全 P ≥ Redis（P=1 **119%**、P=10 **108%**、P=20 100%、P=40 **108%**、P=80 **132%**、P=160 **150%**）。统一写一次 `send` 发整批响应，摊薄每请求固定开销，低 P（RTT 受限）也反超。注：ECHO 低-中 P 方差较大，取 3 次中位数。

## ② AOF disable：kvstore 全 P 持平或反超

HSET 无持久化场景下，kvstore 全 P 反超 Redis（105%/115%/128%/133%/147%/143%）。统一写优化提升低 P；高 P 下 2-arg HSET（hash 引擎 key→value SET）成本低于 Redis 3-arg 真 hash field，优势扩大。注意 kv/redis HSET 语义不等价（见 README「命令格式与引擎容量说明」）。

## ③ AOF always：现默认异步批量 group commit 全深度大幅超越 Redis

现默认 group commit 下，kvstore AOF always 全 P 超 Redis（**145-204%**，P=160 最高 204%，2026-08-02 各 P 7 轮中位数）。机制 =「免等 fsync」+「攒批摊薄」两个优势合一：

- **P=1**：瓶颈是**单请求 RTT**。kvstore 回包不等 fsync（异步提交，RTT ≈ 0.75ms → 65k）；Redis appendfsync always **必须等 fsync 落盘才回包**（RTT 含 ~320µs fsync ≈ 1.14ms → 45k）→ **145%**。
- **高 P**：瓶颈是**fsync 批处理效率**。group commit 每 epoll 周期把整批（P×50 条，P=160 时 8000 条）一次 io_uring linked write+fsync 覆盖，fsync 摊薄到整批、事件循环不阻塞 → 吞吐涨到 1.23M（Redis 604k）→ **204%**。

### 异步逐条 vs 批量（2026-08-02 `taskset` 隔离核 2/3 实测，每 P 3 次中位数）

对比：异步逐条（`--aof-fsync-per-command`）每条命令独立提交 write+fsync，**全 P 落后于批量，且中高 P 跌到 Redis 之下**（原始数据 `benchmarks/data/pipeline_bench/pipeline_aof_modes_taskset_2026-08-02.txt`）：

| P   | 逐条  | 批量   | Redis | 逐条/批量 | 逐条/Redis | 逐条 CoV  |
| --- | ----- | ------ | ----- | --------- | ---------- | --------- |
| 1   | 44.8k | 69.1k  | 42.7k | 65%       | 105%       | **21.5%** |
| 10  | 220k  | 414k   | 255k  | 53%       | **86%**    | 9.1%      |
| 20  | 268k  | 636k   | 350k  | 42%       | **77%**    | 8.4%      |
| 40  | 449k  | 1,053k | 479k  | 43%       | **94%**    | **26.4%** |
| 80  | 409k  | 1,182k | 564k  | 35%       | **72%**    | 13.5%     |
| 160 | 623k  | 1,312k | 608k  | 47%       | 102%       | **19.7%** |

高 P 在飞命令撑爆 **480 slot / 1024 SQE 环** → `persist_flush_pending` 走 backpressure + `sq_retry` 空转停滞（`src/persistence/kvs_persist.c:753-762`），吞吐被环容量卡住、CoV 8-26% 明显高于批量（4-9%）与 Redis（1.6-7.8%）——逐条只在 P=1（105%）、P=160（102%）勉强与 Redis 持平，P=10-80 落后（72-94%）。**这就是默认选 group commit 的原因**。

## 结论

| 场景                           | P=1                   | P=10                  | P≥80                      | 说明                                 |
| ------------------------------ | --------------------- | --------------------- | -------------------------- | ------------------------------------ |
| ECHO（真 echo）                | **kv > Redis** (119%) | **kv > Redis** (108%) | **kv > Redis** (132-150%)  | 统一写批量 drain，全 P 持平或反超    |
| AOF disable（无持久化）        | **kv > Redis** (105%) | **kv > Redis** (115%) | **kv > Redis** (133-147%)  | 全 P 反超；kv/redis HSET 语义不等价  |
| AOF always（异步批量，现默认） | **kv > Redis** (148%) | **kv > Redis** (165%) | **kv >> Redis** (187-196%) | 免等 fsync + 攒批摊薄，全 P 大幅超越 |

## 优化项清单（Phase 1-6，参考 InazumaPlasma pipeline 实现）

kvstore Pipeline（批量流水线）路径的 12 项优化清单及位置。

| #  | 优化                                            | 位置                            | 效果                                 |
| -- | ----------------------------------------------- | ------------------------------- | ------------------------------------ |
| 1  | `-march=native -funroll-loops` 编译优化         | Makefile                        | 启用 AVX2/SSE4.2 + 循环展开          |
| 2  | `likely/unlikely` 分支预测提示                  | kvstore.h, reactor.c, kvstore.c | 6 处热路径 CPU 流水线优化            |
| 3  | `fast_itoa/fast_uitoa` 手写整型转换             | kvstore.c                       | 替代 snprintf，消除 glibc 格式串解析 |
| 4  | ECHO/PING 合并 3×queue_bytes→1×              | kvstore.c                       | 减少函数调用 + memcpy 次数           |
| 5  | strtol/atoi→内联解析（栈缓冲，无 glibc）       | kvstore.c                       | 每 RESP 命令省 2+ 次 strtol 调用     |
| 6  | TCP_NODELAY                                     | reactor.c                       | 消除 Nagle 算法延迟                  |
| 7  | `kvs_ascii_upper` 去 `toupper` locale 查找      | kvstore.c                       | 每命令省 4+ 次函数调用               |
| 8  | 响应缓冲`kvs_malloc`→VLA 栈分配                | kvstore.c                       | 消除 per-cmd malloc/free + mutex     |
| 9  | `strlen(argv[1])`→`argl[1]`                    | kvstore.c                       | 消除重复字符串扫描                   |
| 10 | 解析器`no_scratch`：ECHO/PING 跳过 scratch 拷贝 | kvstore.c                       | 省 ~65 bytes memcpy/cmd              |
| 11 | `mod_events` 加 `epoll_events` 缓存             | reactor.c                       | 跳过不变更的 epoll_ctl syscall       |
| 12 | `queue_bytes` 延迟 EPOLLOUT 注册                | reactor.c                       | pipeline 期间消除 epoll_ctl 切换     |

## 相关

- AOF fsync 模式分析：[aof-fsync-modes-analysis.md](../aof-fsync-modes-analysis.md)
