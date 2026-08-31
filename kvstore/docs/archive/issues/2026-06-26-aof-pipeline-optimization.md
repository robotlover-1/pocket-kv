# AOF always Pipeline 优化分析与尝试

日期：2026-06-26

## 问题

kvstore AOF always pipeline 性能远差于 Redis：

| P | kvstore AOF always | Redis AOF always | kv/redis |
|---|-------------------|-----------------|----------|
| 1 | 51K | 43K | 119% |
| 10 | 11K | 249K | 4.5% |
| 160 | 182K | 567K | 32% |

P=10 时 pipeline 反而有害（11K < 51K P=1）。

## Redis 为什么这么快

Redis 每轮事件循环处理全部就绪 client → 写 AOF buffer（动态 sds 字符串，不限大小）→ **一次** `fdatasync` → 发送全部回复。

```
Redis P=160, c=50:
  → 8000 条命令 → 1 次 fdatasync(~86µs) → 8000 个回复(一次批量 send)
  → 567K QPS
```

核心差异：
1. **AOF buffer 无限大**（sds 动态扩展），一轮循环所有命令装得下
2. **回复批量发送**（`handleClientsWithPendingWrites` 一次 send）
3. **每命令处理更快**（1.5µs vs kvstore 5µs）

## kvstore 为什么慢

### per-command AOF 开销分解

```
no-AOF P=160:    0.86µs/命令 → 1,160K QPS
AOF always P=160: 5.5µs/命令 → 182K QPS
差距: 4.6µs/命令
```

4.6µs 的构成：

| 开销项 | 单次 | 次数/batch | 分摊 µs/命令 |
|--------|------|-----------|------------|
| `persist_defer_response` malloc(144B)+memcpy+链表 | 1.0µs | 8000 | 1.0 |
| `kvs_free`（flush 阶段） | 0.2µs | 8000 | 0.2 |
| `queue_bytes`（flush 阶段逐条拷贝） | 0.1µs | 8000 | 0.1 |
| `flush_conn_output` → `send()`（**优化前 8000 次**） | 0.5-1µs | **8000→50** | 0.5→0.01 |
| 内联 `fdatasync`（64KB buffer 满触发） | ~130µs | 5-6次 | ~0.1 |
| 其他 | — | — | ~2.7 |

**最大瓶颈：`persist_flush_deferred` 中逐条回复的 `send()` 和 `kvs_malloc`/`kvs_free`。**

## 优化尝试

### 1. epoll_ctl 延迟（✓ 已采纳，P=160 +45%）

`queue_bytes` 每次调 `mod_events`（epoll_ctl），pipeline 下 160 次只有 1 次需要注册 EPOLLOUT。改为仅在 ring 从空变非空时调用。

```
提交：52c3749
文件：src/core/reactor.c
```

### 2. resp buffer 64KB→4KB（✓ 已采纳，P=160 +77% 累计）

`handle_parsed_command` 每条命令 `kvs_malloc(64KB)` 存 "+OK\r\n"（5 字节）。改为 `kvs_malloc(4KB)`。

```
提交：52c3749
文件：src/main/kvstore.c
```

### 3. scratch buffer（✓ 已采纳，P=160 +99% 累计）

`parse_resp_stream` 每个参数 `kvs_malloc(blen+1)`。P=160 时 480 次/batch。改为栈上 4KB scratch buffer 复用。

```
提交：01eede7
文件：src/main/kvstore.c
```

### 4. 两阶段回复交付（✓ 已采纳，P=160 +4%，P=10 +11%）

`persist_flush_deferred` 对每条 deferred response 逐条 `queue_bytes` + `flush_conn_output`（→ `send()`）。8000 条 = 8000 次 send()。改为两阶段：

1. 全部 `queue_bytes` 入 ring（不发送）
2. 按连接去重，每连接一次 `flush_conn_output`（50 次 send 替 8000 次）

```
提交：b655885
文件：src/persistence/kvs_persist.c
```

### 5. 增大 AOF buffer 64KB→512KB（✗ 无效）

Buffer 再大也不影响瓶颈——瓶颈不在内联 flush 次数，在 per-command 开销。

### 6. 预 flush（>50% buffer）（✗ 无效）

在 `on_read` 前检查 buffer 使用率，超过一半就预 flush。无效——内联 flush 触发频率已很低。

### 7. 异步 fsync 线程（✗ 不可行）

主线程提交 buffer → fsync 线程（io_uring write+fsync）→ 主线程继续处理。但 reactor 架构是"处理全部事件→一次 flush"，flush 后无事可做直到下一次 epoll_wait。异步完成的通知无法唤醒 epoll_wait。eventfd 需要改 reactor。

## 优化后效果汇总

| P | 原始 | 优化后 | 提升 |
|---|------|--------|------|
| 1 | 47,610 | 51,322 | +8% |
| 10 | 11,140 | 12,410 | +11% |
| 20 | 22,236 | 23,482 | +6% |
| 40 | 44,407 | 45,475 | +2% |
| 80 | 88,739 | 90,457 | +2% |
| 160 | 175,932 | 181,653 | +3% |

> 注：P=1 的提升主要来自 epoll_ctl 延迟 + resp 4KB + scratch buffer。AOF always 专用优化（两阶段回复）对 P=10 效果最明显。

## 剩余差距

即使全部优化后，P=160 时 kvstore（182K）仍是 Redis（567K）的 32%。

**根本原因：per-command 处理速度差距。**

```
kvstore 每命令 AOF 路径: ~5.5µs
  persist_defer_response (malloc+memcpy+list): ~1.0µs  ← 最大单项
  kvs_free:                                     ~0.2µs
  queue_bytes (逐条拷贝):                        ~0.1µs
  其他开销 (RESP parse, hash engine, dispatch):    ~4.2µs

Redis 每命令 AOF 路径:  ~1.5µs
  零 malloc（内嵌 buffer / 共享对象）
  零 RSS parse copy（zero-copy 指针）
  指令表 hash 查找（非 strcmp 链）
  sds AOF buffer（动态，无 size check）
  批量回复（writev / 单次 send）
```

要追平差距需要：
1. **去掉 `persist_defer_response` 的 malloc**：预分配数组或内嵌到 conn_t
2. **命令 dispatch 用 hash 表替 strcmp 链**
3. **RESP 解析改 zero-copy**（注意不污染 inbuf 的 `\r`——`repl_broadcast` 需要原始数据）
