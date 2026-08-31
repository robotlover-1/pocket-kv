# AOF always 性能与稳定性问题排查记录

日期：2026-06-26

---

## 1. Redis AOF always 基准数据无效

### 问题

benchmark 脚本中 Redis 使用 `HSET key:__rand_int__ value`（2-arg），但 Redis 5.0.7 的 HSET 需要 3 个参数（key + field + value）。Redis 对所有请求返回 `ERR wrong number of arguments`，不写 AOF，QPS 虚高。

### 数据对比

| | 原始脚本 | 修正后 |
|---|---|---|
| Redis AOF always QPS | 123,533（假） | 45,683（真） |
| redis-benchmark 命令 | `HSET key:__rand_int__ value` | `HSET key:__rand_int__ __rand_int__ value` |

### 修复

`tools/bench/run_persist_bench.sh`：Redis 改用 `HSET key:__rand_int__ __rand_int__ value`（3-arg）。

---

## 2. Deferred response 链表 use-after-free → segfault

### 问题

c>=5 的多 client benchmark 中 kvstore 偶发 segfault crash（`error 4` = instruction fetch from unmapped memory）。

### 根因

`close_conn()` 释放 `conn_t` 后，`g_deferred_head` 链表中仍保留指向该 `conn_t` 的 `deferred_resp_t` 节点。后续 `persist_flush_deferred()` 遍历链表时访问已释放内存，导致 double-free 或函数指针损坏。

```
close_conn(c) → kvs_free(c)
    ↓ c 已释放
persist_flush_deferred() → dr = g_deferred_head
    ↓
queue_bytes(dr->c, ...) → dr->c 是悬空指针 → segfault
```

### 修复

在 `close_conn()` 中新增 `persist_cancel_deferred(c)` 调用，遍历链表移除所有 `dr->c == c` 的节点。

改动文件：
- [src/core/reactor.c:68](src/core/reactor.c#L68) — 调用 `persist_cancel_deferred(c)`
- [src/persistence/kvs_persist.c:272](src/persistence/kvs_persist.c#L272) — 实现链表清理
- [include/kvstore/kvstore.h:523](include/kvstore/kvstore.h#L523) — 函数声明

验证：10 轮 c=50 压力测试全部通过，无崩溃。

---

## 3. persist_append_raw 返回值被忽略 → 静默丢数据

### 问题

全部 8 处调用点忽略 `persist_append_raw()` 返回值。io_uring write 或 fallback 失败时，client 仍收到 `+OK`，但数据未落盘。

### 修复

8 处调用点全部添加返回值检查，失败时返回 `-ERR AOF write failed`：

| 调用位置 | 命令类型 |
|----------|---------|
| LOCK / UNLOCK / RENEW（3处） | 分布式锁 |
| DOCSET / DOCDEL / DOCDROP（3处） | 文档操作 |
| 主写命令路径（1处） | SET / HSET / DEL 等 |
| Replication（1处） | 主从复制 |

改动文件：[src/main/kvstore.c](src/main/kvstore.c)

---

## 4. persist_close 未 flush AOF buffer → 关闭时丢数据

### 问题

`persist_close()` 只调用 `persist_flush_aof_fd()`（fsync），未先将 `g_aof_buf` 中的缓冲数据写入磁盘。正常关闭时可能丢失最多 64KB 的 AOF 数据。

### 修复

`persist_close()` 中在 fsync 前先调用 `persist_aof_flush_buffer()`。

改动文件：[src/persistence/kvs_persist.c:598](src/persistence/kvs_persist.c#L598)

---

## 5. io_uring CQE 乱序导致 42% fallback

### 问题

`persist_write_and_fsync_uring()` 假设 write CQE 先于 fsync CQE 到达，按序收集：

```c
// 假设第一个 CQE 是 write
rc = cqe->res;  // 当 CQE 实际是 fsync 时，res=0
if (rc <= 0) return -1;  // 误判 write 失败！
```

kernel 5.15 上 ~42% 的调用中 fsync CQE 先到，触发错误的 fallback（pwrite + direct fsync）。

### 分析

内核不保证 CQE 到达顺序。`IORING_OP_FSYNC` 走 `io_fsync()` → `io_queue_async_work()`，可能与 `IORING_OP_WRITE` 并行完成，完成顺序不确定。

### 修复

同时收集两个 CQE，根据 `cqe->res` 值分发：write 返回 >0（写入字节数），fsync 返回 0（成功）或 <0（失败）。

```c
// 收集两个 CQE
cqe1: r1 = cqe->res
cqe2: r2 = cqe->res

// 按值分发
if (r1 > 0)      → r1 是 write, r2 是 fsync
else if (r2 > 0) → r2 是 write, r1 是 fsync
else             → 两者都失败
```

改动文件：[src/persistence/kvs_persist.c:155](src/persistence/kvs_persist.c#L155)

---

## 6. io_uring fsync vs direct fdatasync 性能差异

### 问题

CQE 修复后 HSET AOF always QPS 从 ~80K 降到 ~50K。原因是 fallback 路径使用了更快的 direct `fdatasync()`，修复后统一走 io_uring fsync。

### 微基准数据

kernel 5.15 上，预打开 fd，同文件重复操作：

| 操作 | p50 延迟 | ops/s |
|------|---------|-------|
| pwrite + fdatasync | 95µs | 9,847 |
| io_uring write+fsync batch | 121µs | 7,932 |
| fdatasync only | 84µs | 11,136 |
| io_uring fsync only | 130µs | 7,183 |

io_uring fsync 比 direct fdatasync 慢 55%。原因：kernel 5.15 上 `IORING_OP_FSYNC` 即使提交后立即等待，也走 `io_queue_async_work()` 异步工作队列调度，多了上下文切换开销。kernel 6.1+ 优化了此路径。

### 影响

HSET AOF always QPS 受 fsync 延迟影响较大（batch 小时摊销效果差）。SET 场景 batch 更大所以影响更小，但 SET 因 ARRAY 引擎 1024 槽限制无法用于 benchmark（见 #8）。

---

## 7. Group commit batch 大小分析

### 问题

kvstore group commit 每轮 reactor 循环只处理 ~23 条命令（c=50 时），batch 不满 50。

### 根因

`persist_flush_deferred()` 中回复是逐个发送的：

```
sendto(fd_7, "+OK\r\n")  → client_1 收到回复 → t=250µs 发出下一条
sendto(fd_8, "+OK\r\n")  → client_2 收到回复 → t=255µs 发出下一条
...
sendto(fd_56, "+OK\r\n") → client_50 收到回复 → t=350µs 发出下一条

t=351µs epoll_wait()
  → client_1..23 的下一条已到达 ✓
  → client_24..50 的下一条还在路上 ✗
```

回复发送的"时间散布"导致 epoll_wait 时只有部分 client 的数据就绪。

### 稳态

迟到者会顺延到下一批，形成自稳定循环（稳态 ~23 条/batch）。不会饿死。

---

## 8. SET benchmark 被 ARRAY 引擎 1024 限制污染

### 问题

`KVS_ARRAY_SIZE = 1024`。benchmark 100K 随机 key 下存满 1024 个后，99% 的 SET 返回 `ERR operation failed`（不写 AOF），QPS 虚高。

### 数据

| 命令 | 引擎 | 初始容量 | 100K 请求中成功写入 AOF 的占比 |
|------|------|---------|----------------------------|
| SET | ARRAY | 1024（固定） | ~1% |
| HSET | HASH | 4096（+动态 rehash） | 100% |

### 结论

benchmark 只能用 HSET（HASH 引擎），SET 的 110K QPS 是错误响应速度。已从 README 移除 SET 对比。

---

## 汇总

| # | 问题 | 严重性 | 状态 |
|---|------|--------|------|
| 1 | Redis benchmark 命令无效 | 数据错误 | ✅ 已修复 |
| 2 | Deferred response use-after-free | crash | ✅ 已修复 |
| 3 | persist_append_raw 返回值忽略 | 静默丢数据 | ✅ 已修复 |
| 4 | persist_close 未 flush buffer | 关闭丢数据 | ✅ 已修复 |
| 5 | io_uring CQE 乱序 | 42% fallback | ✅ 已修复 |
| 6 | io_uring vs direct fsync 性能 | 吞吐降低 | 🔧 待优化 |
| 7 | Group commit batch 太小 | 吞吐受限 | 🔧 架构限制 |
| 8 | ARRAY 引擎 1024 限制 | 基准失真 | 📝 已记录 |
