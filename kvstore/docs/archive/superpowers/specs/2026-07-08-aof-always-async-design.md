# AOF Always 异步化设计

## 目标

将 `persist_append_raw` 从同步阻塞改为异步非阻塞，事件循环不再等待磁盘 fsync 完成即可处理下一个请求。保持一条命令一次刷盘语义，不使用 group commit，不引入额外线程。

## 动机

当前 `persist_append_raw` 在 `KVS_AOF_FSYNC_ALWAYS` 模式下调用 `io_uring_submit_and_wait(2)` 阻塞等待 write + fdatasync 两个 CQE。每次阻塞 100-300us（磁盘 fsync 延迟），期间事件循环无法处理任何请求，单连接 QPS 上限约 3000-10000。

改为异步后，事件循环可流水线处理请求，QPS 预期提升 5-10x。

## 方案：单线程异步 io_uring

### 核心改动

**`persist_append_raw` 不再等待 CQE**：
- 用 `io_uring_submit()` 替换 `io_uring_submit_and_wait()`
- write 和 fsync SQE 通过 `IOSQE_IO_LINK` 链接，保证顺序执行
- 函数返回新状态码 `KVS_PERSIST_PENDING`（-2），表示已提交但未完成

**调用方不再立即发送响应**：
- `handle_parsed_command` 中写命令路径：构建 response 后不调用 `queue_bytes`
- 将 `(conn, resp_buf, resp_len)` 存入 pending 队列
- CQE 收割时取出对应的 pending 条目，调用 `queue_bytes` 发送响应
- response buffer 所有权转移给 pending 队列

**CQE 收割嵌入事件循环**：
- 创建 persist io_uring 时配置 eventfd，将 eventfd 注册到各后端的 epoll
- 事件循环检测到 eventfd 可读时收割 CQE
- 每次命令处理完成后主动收割（机会性，零 syscall）

### 数据结构

#### Pending 队列

```c
#define KVS_PERSIST_OK      0   // AOF 关闭，无需持久化
#define KVS_PERSIST_PENDING 1   // 已提交 io_uring，等待 CQE
#define KVS_PERSIST_ERR     -1  // 提交失败

typedef struct persist_pending_s {
    conn_t             *conn;
    unsigned char      *resp;
    size_t              resp_len;
    int                 cqe_seen;     // 已收到的 CQE 数（0 或 1）
    int                 cqe_ok;       // 成功的 CQE 数
    int                 last_error;   // 最后一个错误码
    TAILQ_ENTRY(persist_pending_s) link;
} persist_pending_t;

static TAILQ_HEAD(, persist_pending_s) g_persist_pending_head;
```

FIFO 链表，提交时 tail-insert，收割时 head-remove。与 io_uring CQE 顺序一致（同一 SQ 上的 SQE 按序完成）。每个 pending 对应 2 个 CQE（write + fsync），`cqe_seen` 计数到 2 时出队。

### CQE 收割逻辑

每个 pending 对应 2 个 CQE（write + fsync，通过 `IOSQE_IO_LINK` 链接）。即使 write 失败导致 fsync 被取消，被取消的 SQE 仍会产生 CQE（res = -ECANCELED），所以每次收割总是 2 个 CQE 对应 1 个 pending。

```c
void persist_reap_cqes(void) {
    struct io_uring *ring = &g_persist_uring;
    struct io_uring_cqe *cqe;

    while (io_uring_peek_cqe(ring, &cqe) == 0) {
        persist_pending_t *p = TAILQ_FIRST(&g_persist_pending_head);
        if (!p) {
            // 有 CQE 无 pending，异常
            kvs_log_error("persist: CQE without pending entry");
            io_uring_cqe_seen(ring, cqe);
            continue;
        }

        if (cqe->res > 0) {
            // write CQE 成功
            g_aof_write_offset += cqe->res;
            p->cqe_ok++;
        } else if (cqe->res == 0) {
            // fsync CQE 成功
            p->cqe_ok++;
        } else {
            // 错误
            p->last_error = cqe->res;
        }
        p->cqe_seen++;
        io_uring_cqe_seen(ring, cqe);

        if (p->cqe_seen == 2) {
            // 两个 CQE 都已收到，出队
            TAILQ_REMOVE(&g_persist_pending_head, p, link);
            if (p->cqe_ok == 2 && p->conn && p->conn->fd > 0) {
                queue_bytes(p->conn, p->resp, p->resp_len);
            } else if (p->last_error != 0 && p->conn) {
                // fsync 失败：关闭连接，全局标记拒绝后续写入
                kvs_log_error("persist: fsync error %d, closing conn fd=%d",
                              p->last_error, p->conn->fd);
                p->conn->fd = -1;
                g_persist_fatal_error = 1;
            }
            kvs_free(p->resp);
            kvs_free(p);
        }
    }
}
```

### 三个后端的适配

#### Reactor（epoll）

1. 初始化时注册 persist eventfd 到 epoll：
```c
struct epoll_event ev = {.events = EPOLLIN, .data.fd = persist_eventfd};
epoll_ctl(g_epfd, EPOLL_CTL_ADD, persist_eventfd, &ev);
```

2. epoll_wait 返回时处理：
```c
if (events[i].data.fd == g_persist_eventfd) {
    uint64_t val;
    read(g_persist_eventfd, &val, sizeof(val));  // 消费 eventfd
    persist_reap_cqes();
}
```

3. 每次 `on_read` / `on_write` 处理后主动收割（机会性）：
```c
persist_reap_cqes();
```

#### Proactor（io_uring）

1. 现有 proactor 用 `io_uring_wait_cqe_timeout` 等待网络 CQE。需要将 persist uring 的 eventfd 也纳入等待。简单方案：每次网络 CQE 处理后调用 `persist_reap_cqes()`。
2. 或在网络 uring 空闲时，定时检查 persist uring CQE（epoll 方式兼容）。

#### NtyCo

1. 在 scheduler epoll 中注册 persist eventfd。
2. `nty_schedule_epoll` 返回时处理 persist eventfd → `persist_reap_cqes()`。
3. 或在 `server_reader` 循环中每次 `recv` 后主动收割。

### `handle_parsed_command` 改动

涉及写命令路径（SET、DEL、INCR、LPUSH 等约 15-20 条命令）。每条写命令末尾的改动模式：

```c
// 改动前：
ret = persist_append_raw(raw, rawlen);
// resp 已通过 queue_bytes 发送
// out: kvs_free(resp)

// 改动后：
int pr = persist_append_raw(raw, rawlen);
if (pr == KVS_PERSIST_PENDING) {
    // 将 resp 存入 pending，所有权转移
    persist_pending_enqueue(c, resp, (size_t)n);
    resp = NULL;  // 标记已转移，不在此处 free
}
// out:
if (resp) kvs_free(resp);
```

上述逻辑可封装为宏或内联函数减少重复：
```c
#define PERSIST_AND_RESPOND(c, resp, n, raw, rawlen)  \
    do {                                               \
        int __pr = persist_append_raw(raw, rawlen);    \
        if (__pr == KVS_PERSIST_PENDING) {             \
            persist_pending_enqueue(c, resp, n);       \
            resp = NULL;                               \
        }                                              \
    } while (0)
```

### 连接断开时的清理

当 client 连接在 pending 期间断开（`conn_free` 被调用）：
- `conn_free` 将 `c->fd` 置为 -1 作为关闭标记
- CQE 收割时检查 `p->conn->fd > 0`，为 -1 则跳过 `queue_bytes`，直接释放 `resp` 和 `pending`
- 无需遍历 pending 队列——conn 标记已足够

### io_uring 配置

```c
#define PERSIST_URING_ENTRIES 256  // 足够容纳流水线中的命令数

// 初始化（不用 SQPOLL，避免占用 100% CPU 一个核）
io_uring_queue_init(PERSIST_URING_ENTRIES, &g_persist_uring, 0);

// 创建 eventfd 并注册到 io_uring，CQE 完成时 eventfd 被写入
g_persist_eventfd = eventfd(0, EFD_NONBLOCK);
io_uring_register_eventfd(&g_persist_uring, g_persist_eventfd);
```

### 错误处理

| 场景 | 处理 |
|------|------|
| `io_uring_get_sqe` 返回 NULL（SQ 满） | 同步等待收割后重试，或返回错误 |
| write CQE 失败（`cqe->res < 0`） | 日志记录，标记 pending entry 为错误状态 |
| fsync CQE 失败 | 关闭对应 client 连接，全局标记拒绝后续写入（磁盘不可靠时不应继续接受写请求） |
| 连接在 pending 期间断开 | 收割时检测 `c->fd == -1`，跳过发送，直接释放 |
| pending 队列为空但收到 CQE | 异常，日志警告 |
| `IOSQE_IO_LINK` 中 write 失败 | fsync SQE 被取消，但仍产生 CQE（res = -ECANCELED），pending 的 `cqe_seen` 仍到 2，`cqe_ok != 2`，走错误路径 |

### 重写缓冲区（bgrewrite）兼容

当前 bgrewrite 期间会在 `persist_append_raw` 末尾调用 `append_to_rewrite_buffer`。改为异步后：
- `append_to_rewrite_buffer` 仍在 `persist_append_raw` 中调用（提交 SQE 后立即执行）
- 不受异步化影响，因为改写缓冲区只是内存操作

### AOF 关闭/重开场景

`persist_close()` / `persist_init()` 调用前需确保 pending 队列已排空：
```c
void persist_drain_pending(void) {
    while (!TAILQ_EMPTY(&g_persist_pending_head)) {
        // 等待并收割 CQE
        io_uring_submit_and_wait(&g_persist_uring, 1);
        persist_reap_cqes();
    }
}
```

### 测试要点

- 单元测试：pending 队列的 enqueue/dequeue/cleanup
- 集成测试：命令发送后立即 kill 进程，验证 AOF 文件完整性
- 压力测试：高 QPS 下 pending 队列不会无限增长
- 错误注入：模拟磁盘满/io_uring 失败，验证错误处理路径
- 连接断开：pending 期间客户端断开，验证无泄漏、无 crash

## 不做的事

- 不引入 group commit（多条命令共享一次 fsync）
- 不引入额外线程
- 不改变 `everysec` 策略（当前不存在此策略）
- 不改变 AOF rewrite 机制
- 不改变 AOF 文件格式

## 预期效果

| 指标 | 改动前 | 改动后 |
|------|--------|--------|
| 单连接 QPS（SET 1B value） | ~5000 | ~50000 |
| P50 延迟 | fsync 延迟 (~150us) | ~fsync 延迟（流水线不增加延迟） |
| CPU sys 开销 | 2 syscall/req | ~0.01 syscall/req |
| 事件循环阻塞 | 每请求阻塞 150us | 0 |
