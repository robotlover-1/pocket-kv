# AOF Always 异步化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `persist_append_raw` 从阻塞 `io_uring_submit_and_wait` 改为异步 `io_uring_submit`，事件循环通过 eventfd 收割 CQE，QPS 提升 5-10x。

**Architecture:** `persist_append_raw` 提交 write+fdatasync SQE（IOSQE_IO_LINK 链接）后立即返回 KVS_PERSIST_PENDING。调用方将 response buffer 所有权转移给 pending FIFO 队列。CQE 收割函数 `persist_reap_cqes` 在两个时机调用：(1) 各后端 eventfd 触发时被动收割，(2) 每次命令处理后主动收割。每个 pending 对应 2 个 CQE，`cqe_seen==2` 时出队发送响应。

**Tech Stack:** C, io_uring (liburing), epoll, eventfd, TAILQ

## Global Constraints

- 不使用 group commit（一条命令一次 fsync）
- 不引入额外线程
- 不改变 AOF rewrite 机制
- 不改变 AOF 文件格式
- 保持 `KVS_AOF_FSYNC_OFF` 模式行为不变（直接返回 0）

---

### Task 1: `kvs_persist.c` — Pending 队列、异步 append、CQE 收割、eventfd、排空

**Files:**
- Modify: `src/persistence/kvs_persist.c:1-866`

**Interfaces:**
- Produces: `persist_pending_t`, `g_persist_pending_head`, `KVS_PERSIST_OK/PENDING/ERR`, `persist_append_raw()` (new async semantics), `persist_reap_cqes()`, `persist_pending_enqueue()`, `persist_drain_pending()`, `persist_uring_fd()`, `g_persist_eventfd`, `g_persist_fatal_error`
- Consumes: `g_aof_fd`, `g_aof_write_offset`, `g_cfg.aof_fsync`, `g_persist_uring`, `g_bgrewrite_pid`, `queue_bytes()` (from reactor.c header), `conn_t` (from kvstore.h)

- [ ] **Step 1: Add `#include <sys/eventfd.h>` and pending queue infrastructure**

在 `kvs_persist.c` 第 1 行 `#include <liburing.h>` 后添加 `#include <sys/eventfd.h>`。

在第 49 行（`g_rewrite_buf_lock` 声明后）插入 pending 队列数据结构：

```c
#include <sys/eventfd.h>

/* ---- async append pending queue ---- */
typedef struct persist_pending_s {
    conn_t             *conn;
    unsigned char      *resp;
    size_t              resp_len;
    int                 cqe_seen;
    int                 cqe_ok;
    int                 last_error;
    TAILQ_ENTRY(persist_pending_s) link;
} persist_pending_t;

static TAILQ_HEAD(, persist_pending_s) g_persist_pending_head = TAILQ_HEAD_INITIALIZER(g_persist_pending_head);
static int g_persist_eventfd = -1;
static int g_persist_fatal_error = 0;
/* ---- end async append ---- */
```

- [ ] **Step 2: 修改 `persist_uring_init_once` 增加 eventfd 注册**

替换 `persist_uring_init_once`（第 54-58 行）：

```c
static int persist_uring_init_once(void) {
    if (g_persist_uring_ready) return 0;
    struct io_uring_params p = {0};
    if (io_uring_queue_init(256, &g_persist_uring, 0) != 0) return -1;
    g_persist_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_persist_eventfd < 0) {
        io_uring_queue_exit(&g_persist_uring);
        return -1;
    }
    if (io_uring_register_eventfd(&g_persist_uring, g_persist_eventfd) != 0) {
        close(g_persist_eventfd);
        g_persist_eventfd = -1;
        io_uring_queue_exit(&g_persist_uring);
        return -1;
    }
    g_persist_uring_ready = 1;
    return 0;
}
```

- [ ] **Step 3: 修改 `persist_uring_close` 清理 eventfd**

替换 `persist_uring_close`（第 61-65 行）：

```c
static void persist_uring_close(void) {
    if (!g_persist_uring_ready) return;
    if (g_persist_eventfd >= 0) {
        io_uring_unregister_eventfd(&g_persist_uring);
        close(g_persist_eventfd);
        g_persist_eventfd = -1;
    }
    io_uring_queue_exit(&g_persist_uring);
    g_persist_uring_ready = 0;
}
```

- [ ] **Step 4: 添加 `persist_pending_enqueue` 函数（非 static，供 kvstore.c 调用）**

在 `persist_uring_close` 之后添加：

```c
void persist_pending_enqueue(conn_t *c, unsigned char *resp, size_t resp_len) {
    persist_pending_t *p = (persist_pending_t *)kvs_malloc(sizeof(*p));
    if (!p) {
        kvs_free(resp);
        return;
    }
    p->conn = c;
    p->resp = resp;
    p->resp_len = resp_len;
    p->cqe_seen = 0;
    p->cqe_ok = 0;
    p->last_error = 0;
    TAILQ_INSERT_TAIL(&g_persist_pending_head, p, link);
}
```

- [ ] **Step 5: 添加 `persist_reap_cqes` — CQE 收割函数**

在 `persist_pending_enqueue` 之后添加：

```c
void persist_reap_cqes(void) {
    if (!g_persist_uring_ready) return;

    struct io_uring_cqe *cqe;
    while (io_uring_peek_cqe(&g_persist_uring, &cqe) == 0) {
        persist_pending_t *p = TAILQ_FIRST(&g_persist_pending_head);
        if (!p) {
            fprintf(stderr, "persist: CQE without pending entry\n");
            io_uring_cqe_seen(&g_persist_uring, cqe);
            continue;
        }

        if (cqe->res > 0) {
            g_aof_write_offset += cqe->res;
            p->cqe_ok++;
        } else if (cqe->res == 0) {
            p->cqe_ok++;
        } else {
            p->last_error = cqe->res;
        }
        p->cqe_seen++;
        io_uring_cqe_seen(&g_persist_uring, cqe);

        if (p->cqe_seen == 2) {
            TAILQ_REMOVE(&g_persist_pending_head, p, link);
            if (p->cqe_ok == 2 && p->conn && p->conn->fd > 0) {
                queue_bytes(p->conn, p->resp, p->resp_len);
            } else if (p->last_error != 0 && p->conn) {
                fprintf(stderr, "persist: fsync err %d, closing conn fd=%d\n",
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

- [ ] **Step 6: 添加 `persist_uring_fd` — 暴露 eventfd 给各后端**

```c
int persist_uring_fd(void) {
    return g_persist_eventfd;
}
```

- [ ] **Step 7: 添加 `persist_drain_pending` — 排空 pending 队列**

```c
void persist_drain_pending(void) {
    while (!TAILQ_EMPTY(&g_persist_pending_head)) {
        io_uring_submit_and_wait(&g_persist_uring, 1);
        persist_reap_cqes();
    }
}
```

- [ ] **Step 8: 重写 `persist_append_raw` — 异步化**

替换整个 `persist_append_raw` 函数（第 501-556 行）：

```c
int persist_append_raw(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;

    if (g_aof_fd < 0) return g_aof_disabled ? KVS_PERSIST_OK : KVS_PERSIST_ERR;
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) return KVS_PERSIST_OK;

    if (g_persist_fatal_error) return KVS_PERSIST_ERR;

    if (persist_uring_init_once() != 0) return KVS_PERSIST_ERR;

    off_t off = (off_t)g_aof_write_offset;

    /* write SQE */
    sqe_w = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_w) {
        /* SQ full — drain some CQEs and retry once */
        persist_reap_cqes();
        sqe_w = io_uring_get_sqe(&g_persist_uring);
        if (!sqe_w) return KVS_PERSIST_ERR;
    }
    io_uring_prep_write(sqe_w, g_aof_fd, buf, len, off);
    sqe_w->flags |= IOSQE_IO_LINK;

    /* fsync SQE */
    sqe_f = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_f) {
        persist_reap_cqes();
        sqe_f = io_uring_get_sqe(&g_persist_uring);
        if (!sqe_f) return KVS_PERSIST_ERR;
    }
    io_uring_prep_fsync(sqe_f, g_aof_fd, IORING_FSYNC_DATASYNC);

    /* submit, do NOT wait */
    int rc = io_uring_submit(&g_persist_uring);
    if (rc < 0) return KVS_PERSIST_ERR;

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    return KVS_PERSIST_PENDING;
}
```

- [ ] **Step 9: 修改 `persist_force_aof_flush` — 先排空 pending**

替换 `persist_force_aof_flush`（第 495-499 行）：

```c
int persist_force_aof_flush(void) {
    if (g_aof_fd < 0) return -1;
    persist_drain_pending();
    if (persist_fsync_fd_best_effort(g_aof_fd) != 0) return -1;
    return 0;
}
```

- [ ] **Step 10: 修改 `persist_close` — 先排空再关闭**

替换 `persist_close`（第 472-479 行）：

```c
void persist_close(void) {
    persist_drain_pending();
    if (g_aof_fd >= 0) {
        persist_flush_aof_fd(g_aof_fd);
        close(g_aof_fd);
    }
    g_aof_fd = -1;
    persist_uring_close();
}
```

- [ ] **Step 11: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1
```

验证编译通过，无 warning。

- [ ] **Step 12: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(persist): async append — pending queue, CQE reaping, eventfd"
```

---

### Task 2: `kvstore.h` — 新接口声明

**Files:**
- Modify: `include/kvstore/kvstore.h:501`

**Interfaces:**
- Produces: `KVS_PERSIST_OK`, `KVS_PERSIST_PENDING`, `KVS_PERSIST_ERR`, `persist_reap_cqes()`, `persist_drain_pending()`, `persist_uring_fd()`

- [ ] **Step 1: 在 header 中添加新声明**

在 `include/kvstore/kvstore.h` 第 501 行（`int persist_append_raw(...)` 声明）之前插入：

```c
/* async append return codes */
#define KVS_PERSIST_OK      0
#define KVS_PERSIST_PENDING 1
#define KVS_PERSIST_ERR     -1

void persist_reap_cqes(void);
void persist_pending_enqueue(conn_t *c, unsigned char *resp, size_t resp_len);
void persist_drain_pending(void);
int persist_uring_fd(void);
```

`persist_append_raw` 声明不变，保持原有签名。

- [ ] **Step 2: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1
```

- [ ] **Step 3: Commit**

```bash
git add include/kvstore/kvstore.h
git commit -m "feat(persist): add async append API declarations to header"
```

---

### Task 3: `kvstore.c` — 所有写命令适配异步返回

**Files:**
- Modify: `src/main/kvstore.c`

**Interfaces:**
- Consumes: `KVS_PERSIST_OK`, `KVS_PERSIST_PENDING`, `KVS_PERSIST_ERR`, `persist_pending_enqueue()` (via kvs_persist.c)

涉及 8 处 `persist_append_raw` 调用点，每处按此模式改为异步：

**改动模式：**

```c
// 改动前（行 1460 为例）：
if (persist_append_raw(raw, rawlen) != 0) {
    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
    goto out;
}

// 改动后：
int pr = persist_append_raw(raw, rawlen);
if (pr == KVS_PERSIST_ERR) {
    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
    goto out;
}
if (pr == KVS_PERSIST_PENDING) {
    /* resp ownership transfers to pending queue; freed on CQE */
    persist_pending_enqueue(c, resp, (size_t)n);
    resp = NULL;
}
```

**注意**：`persist_pending_enqueue` 已在 `kvstore.h` 中声明（Task 2），无需额外 `extern`。`goto out` 后不再执行后续代码，resp 已转移。

- [ ] **Step 1: 行 1460 — LOCK 命令**

按上述模式改写 LOCK 命令中的 `persist_append_raw` 调用。

- [ ] **Step 2: 行 1482 — UNLOCK 命令**

按模式改写。

- [ ] **Step 3: 行 1504 — RENEW 命令**

按模式改写。

- [ ] **Step 4: 行 1586 — DOCSET 命令**

按模式改写。

- [ ] **Step 5: 行 1611 — DOCDEL 命令**

按模式改写。

- [ ] **Step 6: 行 1632 — DOCDROP 命令**

按模式改写。

- [ ] **Step 7: 行 1800 — 复制 slave 写路径**

行 1799-1802：
```c
persist_note_write();
if (persist_append_raw(raw, rawlen) == 0) {
    repl_slave_note_durable(rawlen);
}
```
改为：
```c
persist_note_write();
int pr = persist_append_raw(raw, rawlen);
if (pr == KVS_PERSIST_PENDING || pr == KVS_PERSIST_OK) {
    repl_slave_note_durable(rawlen);
}
```
（slave 路径无 client connection，resp 不需 delayed send）

- [ ] **Step 8: 行 1818 — 主写路径（SET/DEL/INCR 等通用）**

行 1816-1822：
```c
if (!from_replication && is_write_cmd(cmd)) {
    persist_note_write();
    if (persist_append_raw(raw, rawlen) != 0) {
        n = resp_error(resp, BUFFER_CAP, "AOF write failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
```
改为：
```c
if (!from_replication && is_write_cmd(cmd)) {
    persist_note_write();
    int pr = persist_append_raw(raw, rawlen);
    if (pr == KVS_PERSIST_ERR) {
        n = resp_error(resp, BUFFER_CAP, "AOF write failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (pr == KVS_PERSIST_PENDING && c) {
        /* resp transferred to pending queue */
        resp = NULL;
    }
    /* KVS_PERSIST_OK: aof disabled, resp sent later as normal */
```

并在 `out:` 标签处将 `kvs_free(resp)` 之前的 `queue_bytes` 逻辑调整为：PENDING 时 `resp` 已被转移（为 NULL），不执行后续 `queue_bytes`。

具体来说，行 1836（通用的 `if (c) queue_bytes(...)`）需改为：
```c
if (c && resp) queue_bytes(c, (unsigned char *)resp, (size_t)n);
```
确保 PENDING 场景（`resp == NULL`）不重复发送。

- [ ] **Step 9: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1
```

确认所有 call site 适配完成，无 warning。

- [ ] **Step 10: Commit**

```bash
git add src/main/kvstore.c
git commit -m "feat(kvstore): adapt all write commands to async persist_append_raw"
```

---

### Task 4: `reactor.c` — eventfd epoll 集成 + 机会性收割

**Files:**
- Modify: `src/core/reactor.c:183-294`

**Interfaces:**
- Consumes: `persist_uring_fd()`, `persist_reap_cqes()`

- [ ] **Step 1: 在 `reactor_start` 中注册 persist eventfd 到 epoll**

在 `reactor_start()` 函数中，`g_epfd = epoll_create1(0)` 之后（约第 212 行后），添加：

```c
/* register persist uring eventfd for async AOF CQE notification */
{
    int pefd = persist_uring_fd();
    if (pefd >= 0) {
        struct epoll_event pev;
        memset(&pev, 0, sizeof(pev));
        pev.events = EPOLLIN;
        pev.data.fd = pefd;
        epoll_ctl(g_epfd, EPOLL_CTL_ADD, pefd, &pev);
    }
}
```

- [ ] **Step 2: 在 epoll_wait 事件循环中处理 persist eventfd**

在事件循环 `for (int i = 0; i < n; ++i)` 的开头（第 257 行后），在 `int fd = events[i].data.fd;` 之前，添加：

```c
if (events[i].data.fd == g_persist_eventfd_static) {
    uint64_t val;
    read(events[i].data.fd, &val, sizeof(val));
    persist_reap_cqes();
    continue;
}
```

需要用一个 static 变量保存 persist eventfd。或者直接在 for 循环中比较：

```c
for (int i = 0; i < n; ++i) {
    int fd = events[i].data.fd;
    if (fd == persist_uring_fd()) {
        uint64_t val;
        read(fd, &val, sizeof(val));
        persist_reap_cqes();
        continue;
    }
    conn_t *c = fdmap[fd];
    ...
```

- [ ] **Step 3: 在 `on_read` 末尾添加机会性收割**

在 `on_read` 函数末尾（第 151 行 `mod_events` 之后）添加：

```c
persist_reap_cqes();
```

- [ ] **Step 4: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1
```

- [ ] **Step 5: Commit**

```bash
git add src/core/reactor.c
git commit -m "feat(reactor): integrate persist eventfd and opportunistic CQE reaping"
```

---

### Task 5: `proactor.c` — 机会性收割

**Files:**
- Modify: `src/core/proactor.c:147-248`

**Interfaces:**
- Consumes: `persist_reap_cqes()`

Proactor 用 `io_uring_wait_cqe_timeout` 等待，最简单方案是每次 CQE 处理后调用 `persist_reap_cqes()`。

- [ ] **Step 1: 在每个 CQE 处理路径后添加收割**

在 proactor 的 while 循环中，`io_uring_cqe_seen(&ring, cqe)` 之后、`io_uring_submit(&ring)` 之前（第 241 行附近），添加：

```c
persist_reap_cqes();
```

这覆盖了所有三个事件类型（ACCEPT/READ/WRITE），确保每次网络 CQE 处理完都检查 persist CQE。

- [ ] **Step 2: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1
```

- [ ] **Step 3: Commit**

```bash
git add src/core/proactor.c
git commit -m "feat(proactor): add opportunistic persist CQE reaping"
```

---

### Task 6: `ntyco.c` + `nty_schedule.c` — 机会性收割

**Files:**
- Modify: `src/core/ntyco.c:40-77`
- Modify: `NtyCo/core/nty_schedule.c:344`

**Interfaces:**
- Consumes: `persist_reap_cqes()`

NtyCo 有一个独立 cron 线程每秒 tick。对于 AOF CQE 收割，使用最简单的方案：在 `server_reader` 中每次 `recv` 后收割。同时也可在 `nty_schedule_epoll` 返回后收割。

- [ ] **Step 1: 在 `server_reader` 中每次处理完命令后收割**

在 `ntyco.c` 的 `server_reader` 函数中，`parse_resp_stream` 调用之后、`flush_output_blocking` 之前（第 68-71 行），添加：

```c
persist_reap_cqes();
```

位置：第 68 行 `parse_resp_stream(c, c->inbuf, &c->in_len, 0);` 之后。

- [ ] **Step 2: 在 `flush_output_blocking` 调用后收割**

在第 71-75 行的 `flush_output_blocking` 调用之后也添加收割。

完整改动：
```c
c->in_len += (size_t)n;
parse_resp_stream(c, c->inbuf, &c->in_len, 0);
persist_reap_cqes();  /* reap AOF CQEs after command processing */

if (c->out_ring_len > 0) {
    if (flush_output_blocking(c) != 0) {
        close_conn_nty(c);
        return;
    }
    persist_reap_cqes();  /* also try after flushing output */
}
```

- [ ] **Step 3: 在 scheduler epoll 中添加 persist eventfd 支持**

在 `nty_schedule.c` 的 `nty_schedule_epoll` 函数（第 283-313 行）中，`nty_epoller_wait` 返回后、函数返回前（第 313 行 return 0 之前），收割：

```c
extern void persist_reap_cqes(void);
persist_reap_cqes();
```

（不需要头文件包含，`extern` 声明即可）。

- [ ] **Step 4: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1
```

- [ ] **Step 5: Commit**

```bash
git add src/core/ntyco.c NtyCo/core/nty_schedule.c
git commit -m "feat(ntyco): add opportunistic persist CQE reaping"
```

---

### Task 7: 构建验证 + 现有测试回归

**Files:**
- 无新建文件

- [ ] **Step 1: 完整构建**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make kvstore 2>&1 | tail -20
```

预期：编译成功，无 warning。

- [ ] **Step 2: 运行现有 AOF 持久化测试**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make test_persist_aof_demo && ./test_persist_aof_demo 2>&1
```

预期：测试通过，验证 AOF 恢复功能正常。

- [ ] **Step 3: 运行 io_uring 持久化测试**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make test_uring_persist && ./test_uring_persist 2>&1
```

预期：通过。

- [ ] **Step 4: 运行完整测试套件**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make check-persist 2>&1
```

预期：所有 persist 相关测试通过。

- [ ] **Step 5: 手动冒烟测试（reactor 后端）**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore --port 9999 --appendfsync always --aof /tmp/test_async_aof.aof --net reactor &
sleep 1
# 发送几条写命令
echo -e "SET k1 v1\r\nSET k2 v2\r\n" | nc -w 1 localhost 9999
# 等待 AOF 完成
sleep 1
# 检查 AOF 文件内容
cat /tmp/test_async_aof.aof | xxd | head -20
kill %1 2>/dev/null
rm -f /tmp/test_async_aof.aof
```

验证 AOF 文件包含完整的写命令。

- [ ] **Step 6: 手动冒烟测试（proactor 后端）**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore --port 9998 --appendfsync always --aof /tmp/test_async_aof_proactor.aof --net proactor &
sleep 1
echo -e "SET k1 v1\r\nSET k2 v2\r\n" | nc -w 1 localhost 9998
sleep 1
cat /tmp/test_async_aof_proactor.aof | xxd | head -20
kill %1 2>/dev/null
rm -f /tmp/test_async_aof_proactor.aof
```

- [ ] **Step 7: Commit**

```bash
git commit --allow-empty -m "test: verify build and regression tests after async AOF changes"
```

---

### Task 8: 压力测试 + 边界场景验证

**Files:**
- 无新建文件

- [ ] **Step 1: 高 QPS 压力测试**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore --port 9997 --appendfsync always --aof /tmp/stress_async.aof --net reactor &
sleep 1

# 用 redis-benchmark 压测（如可用）
redis-benchmark -p 9997 -t set -n 10000 -q 2>&1 || true

# 或用简单脚本
time for i in $(seq 1 1000); do
  echo -e "SET k$i v$i\r\n" | nc -w 1 localhost 9997 > /dev/null
done

kill %1 2>/dev/null
rm -f /tmp/stress_async.aof
```

验证：1000 条 SET 无超时、无错误。

- [ ] **Step 2: AOF 关闭模式回归**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore --port 9996 --appendfsync off --net reactor &
sleep 1
echo -e "SET k1 v1\r\n" | nc -w 1 localhost 9996
kill %1 2>/dev/null
```

验证：AOF off 模式不崩溃，命令正常处理。

- [ ] **Step 3: 连接断开场景**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore --port 9995 --appendfsync always --aof /tmp/disconnect.aof --net reactor &
sleep 1
# 发送命令后立即断连
(echo -ne "SET k1 v1\r\n"; sleep 0.01) | nc -w 0 localhost 9995 > /dev/null 2>&1 || true
sleep 1
kill %1 2>/dev/null
rm -f /tmp/disconnect.aof
```

验证：不崩溃，无内存泄漏（valgrind 可选）。

- [ ] **Step 4: AOF 排空后关闭验证**

检查 `persist_close` 中 `persist_drain_pending` 逻辑：pending 队列为空时 `persist_drain_pending` 立即返回。验证 kill 信号能正常关闭进程。

- [ ] **Step 5: Commit**

```bash
git commit --allow-empty -m "test: stress tests and edge cases for async AOF"
```
