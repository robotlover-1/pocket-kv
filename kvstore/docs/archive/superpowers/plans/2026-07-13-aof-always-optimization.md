# AOF Always 优化实现方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 优化 AOF always 策略的 QPS（基线 39,040，目标 +20~40%），通过 offset 拆分、预分配 ring buffer、user_data 索引、流水线化 submit/reap、io_uring 配置调优

**Architecture:** 改动集中在 `src/persistence/kvs_persist.c`。offset 拆分为 submitted（SQE 填充时推进）和 confirmed（CQE reap 时推进），TAILQ+malloc 替换为固定大小 ring buffer，CQE 通过 user_data 索引 slot 而非顺序匹配

**Tech Stack:** C, liburing, eventfd, epoll

## Global Constraints

- 严格保持语义：一条命令 = 一次独立 write + fdatasync
- fsync 完成后才回复客户端
- 只改 persistence 层，API 签名不变
- 必须使用 io_uring 做落盘
- eventfd + epoll 模式不变

---

### Task 1: 替换数据结构 — ring buffer + offset 拆分

**Files:**
- Modify: `src/persistence/kvs_persist.c:15,43-67`

**Interfaces:**
- Produces: `g_aof_write_submitted`, `g_aof_write_confirmed`, `g_pending_slots[]`, `g_pending_head`, `g_pending_tail`, `persist_pending_slot_t`
- Removes: `g_aof_write_offset`, `persist_pending_t`, `g_persist_pending_head`, TAILQ types

- [ ] **Step 1: Replace offset 变量**

在 `src/persistence/kvs_persist.c` 第 15 行，将：

```c
static long long g_aof_write_offset = 0;
```

替换为：

```c
static long long g_aof_write_submitted = 0;
static long long g_aof_write_confirmed = 0;
```

- [ ] **Step 2: 替换 pending 类型和存储**

在第 53-66 行，删除整个 `/* ---- async append pending queue ---- */` 区块：

```c
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

替换为：

```c
/* ---- async append pending ring buffer ---- */
#define PERSIST_PENDING_RING_SIZE  512

typedef struct {
    conn_t        *conn;
    unsigned char *resp;
    size_t         resp_len;
    uint8_t        cqe_count;    /* 0→1→2, incremented per CQE */
    uint8_t        cqe_ok;       /* count of successful CQEs */
    int8_t         last_error;   /* CQE res on failure, 0 otherwise */
} persist_pending_slot_t;

static persist_pending_slot_t g_pending_slots[PERSIST_PENDING_RING_SIZE];
static uint32_t               g_pending_head = 0;  /* next enqueue index */
static uint32_t               g_pending_tail = 0;  /* next reap index   */

static int g_persist_eventfd = -1;
static int g_persist_fatal_error = 0;
/* ---- end async append ---- */
```

- [ ] **Step 3: 删除不再需要的头文件引用**

第 7 行删除 `<sys/queue.h>` 的 include：

```c
// 删除这一行：
#include <sys/queue.h>
```

- [ ] **Step 4: 添加 ring buffer 辅助函数**

两个 helper 的位置：

- **前向声明**：放在 ring buffer 定义后（原 `g_persist_fatal_error` 之后），让 `persist_reap_cqes`（第 114 行）可见它们：

```c
static int g_persist_eventfd = -1;
static int g_persist_fatal_error = 0;

/* forward declarations for ring buffer helpers */
static void pending_ring_advance_tail(void);
static uint32_t pending_ring_reserve(void);
/* ---- end async append ---- */
```

- **实现**：放在 `persist_reap_cqes` 函数体之后（原第 155 行位置），这样 `persist_reap_cqes` 可以调用它们，且后续的 `persist_append_prepare`（第 613 行）也能看到：

```c
static uint32_t pending_ring_reserve(void) {
    while (g_pending_head - g_pending_tail >= PERSIST_PENDING_RING_SIZE) {
        persist_reap_cqes();  /* try to free completed slots */
        if (g_pending_head - g_pending_tail < PERSIST_PENDING_RING_SIZE)
            break;
        /* still full: block until at least one CQE completes */
        io_uring_submit_and_wait(&g_persist_uring, 1);
        persist_reap_cqes();
    }
    uint32_t idx = g_pending_head++;
    memset(&g_pending_slots[idx % PERSIST_PENDING_RING_SIZE], 0,
           sizeof(persist_pending_slot_t));
    return idx;
}

static void pending_ring_advance_tail(void) {
    g_pending_tail++;
}
```

- [ ] **Step 5: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make 2>&1 | head -30
```

预期：编译报错（因为引用了已删除的 `g_aof_write_offset`、`persist_pending_t`、TAILQ 宏），确认所有报错都在后续 task 覆盖范围内。记下报错行号。

- [ ] **Step 6: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "refactor(persist): replace TAILQ with ring buffer, split offset into submitted/confirmed"
```

---

### Task 2: 更新 io_uring 初始化 — 新 flags + ring 1024

**Files:**
- Modify: `src/persistence/kvs_persist.c:72-88`

**Interfaces:**
- Consumes: `g_persist_uring`, `g_persist_uring_ready`, `g_persist_eventfd`
- Produces: Updated `persist_uring_init_once` with SINGLE_ISSUER, COOP_TASKRUN, ring=1024

- [ ] **Step 1: 重写 persist_uring_init_once**

将第 72-88 行：

```c
static int persist_uring_init_once(void) {
    if (g_persist_uring_ready) return 0;
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

替换为：

```c
static int persist_uring_init_once(void) {
    if (g_persist_uring_ready) return 0;

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;

    if (io_uring_queue_init_params(1024, &g_persist_uring, &params) != 0)
        return -1;

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

- [ ] **Step 2: 调整 sq_space_left 阈值（在 Task 3 重写 persist_append_prepare 时一起处理）**

此步骤留到 Task 3 完成，阈值从 `< 2` 改为 `< 64`。

- [ ] **Step 3: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make 2>&1 | tail -20
```

预期：编译成功（初始化部分完成），但链接可能报 persist_append_prepare 等未适配函数的错误，或者编译器报 warning/error 关于 g_aof_write_offset 的引用。

- [ ] **Step 4: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "perf(persist): io_uring SINGLE_ISSUER+COOP_TASKRUN, ring 256→1024"
```

---

### Task 3: 重写 persist_append_prepare — user_data 索引 + submitted offset

**Files:**
- Modify: `src/persistence/kvs_persist.c:613-651`

**Interfaces:**
- Consumes: `g_aof_write_submitted`, `g_pending_slots`, `g_pending_head`, `g_persist_uring`, `g_aof_fd`, `g_cfg.aof_fsync`, `g_persist_fatal_error`, `pending_ring_reserve()`
- Produces: Updated `persist_append_prepare` setting `user_data = slot_idx`
- Public API unchanged: `int persist_append_prepare(const unsigned char *buf, size_t len)`

- [ ] **Step 1: 重写 persist_append_prepare**

将第 613-651 行：

```c
int persist_append_prepare(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;

    if (g_aof_fd < 0) return g_aof_disabled ? KVS_PERSIST_OK : KVS_PERSIST_ERR;
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) return KVS_PERSIST_OK;

    if (g_persist_fatal_error) return KVS_PERSIST_ERR;

    if (persist_uring_init_once() != 0) return KVS_PERSIST_ERR;

    /* ensure at least 2 free SQEs — if ring is filling up with
       deferred submits, submit and reap to free slots */
    if (io_uring_sq_space_left(&g_persist_uring) < 2) {
        persist_submit_pending();
        persist_reap_cqes();
        if (io_uring_sq_space_left(&g_persist_uring) < 2)
            return KVS_PERSIST_ERR;
    }

    off_t off = (off_t)g_aof_write_offset;

    /* write SQE */
    sqe_w = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_w) return KVS_PERSIST_ERR;
    io_uring_prep_write(sqe_w, g_aof_fd, buf, len, off);
    sqe_w->flags |= IOSQE_IO_LINK;

    /* fsync SQE */
    sqe_f = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_f) return KVS_PERSIST_ERR;
    io_uring_prep_fsync(sqe_f, g_aof_fd, IORING_FSYNC_DATASYNC);

    /* NOTE: do NOT call io_uring_submit() — caller batches and
       calls persist_submit_pending() when ready */

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    return KVS_PERSIST_PENDING;
}
```

替换为：

```c
int persist_append_prepare(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;
    uint32_t slot_idx;

    if (g_aof_fd < 0) return g_aof_disabled ? KVS_PERSIST_OK : KVS_PERSIST_ERR;
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) return KVS_PERSIST_OK;

    if (g_persist_fatal_error) return KVS_PERSIST_ERR;

    if (persist_uring_init_once() != 0) return KVS_PERSIST_ERR;

    /* ensure 2 free SQEs before reserving slot */
    if (io_uring_sq_space_left(&g_persist_uring) < 64) {
        persist_submit_pending();
        persist_reap_cqes();
        if (io_uring_sq_space_left(&g_persist_uring) < 2)
            return KVS_PERSIST_ERR;
    }

    /* reserve ring slot (may reap/block if full) */
    slot_idx = pending_ring_reserve();

    off_t off = (off_t)g_aof_write_submitted;

    /* write SQE */
    sqe_w = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_w) {
        g_pending_head--; /* rollback slot reservation */
        return KVS_PERSIST_ERR;
    }
    io_uring_prep_write(sqe_w, g_aof_fd, buf, len, off);
    sqe_w->flags |= IOSQE_IO_LINK;
    sqe_w->user_data = slot_idx;

    /* fsync SQE */
    sqe_f = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_f) {
        g_pending_head--; /* rollback slot reservation */
        return KVS_PERSIST_ERR;
    }
    io_uring_prep_fsync(sqe_f, g_aof_fd, IORING_FSYNC_DATASYNC);
    sqe_f->user_data = slot_idx;

    g_aof_write_submitted += (long long)len;

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    return KVS_PERSIST_PENDING;
}
```

- [ ] **Step 2: 验证变更**

确认：
- `user_data` 两个 SQE 都设置为 `slot_idx`
- `g_aof_write_submitted` 在 SQE 填充后立即推进
- SQE 获取失败时 `g_pending_head--` 回滚
- `sq_space_left < 64` 时触发 flush（减少阈值处的竞争），但最终检查 `< 2` 作为兜底

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "perf(persist): rewrite persist_append_prepare with user_data indexing and submitted offset"
```

---

### Task 4: 重写 persist_reap_cqes — user_data 索引 + confirmed offset

**Files:**
- Modify: `src/persistence/kvs_persist.c:114-154`

**Interfaces:**
- Consumes: `g_aof_write_confirmed`, `g_pending_slots`, `g_pending_tail`, `g_persist_uring`, `g_persist_fatal_error`, `pending_ring_advance_tail()`
- Produces: Updated `persist_reap_cqes` (public API unchanged)

- [ ] **Step 1: 重写 persist_reap_cqes**

将第 114-154 行：

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
            if (p->cqe_ok == 2) {
                if (p->conn && p->conn->fd > 0)
                    queue_bytes(p->conn, p->resp, p->resp_len);
            } else {
                fprintf(stderr, "persist: CQE error cqe_ok=%d last_error=%d\n",
                        p->cqe_ok, p->last_error);
                if (p->conn) {
                    p->conn->fd = -1;
                }
                g_persist_fatal_error = 1;
            }
            kvs_free(p->resp);
            kvs_free(p);
        }
    }
}
```

替换为：

```c
void persist_reap_cqes(void) {
    if (!g_persist_uring_ready) return;

    struct io_uring_cqe *cqe;
    while (io_uring_peek_cqe(&g_persist_uring, &cqe) == 0) {
        uint32_t idx = (uint32_t)cqe->user_data;
        persist_pending_slot_t *slot;

        /* resolve slot by its absolute index */
        if (idx < g_pending_tail || idx >= g_pending_head) {
            /* stale or out-of-range user_data; should not happen */
            fprintf(stderr, "persist: CQE with invalid user_data %u (head=%u tail=%u)\n",
                    idx, g_pending_head, g_pending_tail);
            io_uring_cqe_seen(&g_persist_uring, cqe);
            continue;
        }
        slot = &g_pending_slots[idx % PERSIST_PENDING_RING_SIZE];

        if (cqe->res > 0) {
            /* write CQE: advance confirmed offset */
            if (slot->cqe_count == 0)  /* first CQE for this slot is write */
                g_aof_write_confirmed += (long long)cqe->res;
            slot->cqe_ok++;
        } else if (cqe->res == 0) {
            /* fsync with 0-byte file or similar */
            slot->cqe_ok++;
        } else {
            slot->last_error = (int8_t)cqe->res;
        }
        slot->cqe_count++;
        io_uring_cqe_seen(&g_persist_uring, cqe);

        if (slot->cqe_count == 2) {
            if (slot->cqe_ok == 2) {
                if (slot->conn && slot->conn->fd > 0)
                    queue_bytes(slot->conn, slot->resp, slot->resp_len);
            } else {
                fprintf(stderr, "persist: CQE error cqe_ok=%d last_error=%d\n",
                        (int)slot->cqe_ok, (int)slot->last_error);
                if (slot->conn) {
                    slot->conn->fd = -1;
                }
                g_persist_fatal_error = 1;
            }
            kvs_free(slot->resp);
            slot->resp = NULL;
            slot->conn = NULL;
            pending_ring_advance_tail();
        }
    }
}
```

- [ ] **Step 2: 确认已删除 TAILQ_REMOVE 引用**

`grep -n "TAILQ" src/persistence/kvs_persist.c` 应返回空。

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "perf(persist): rewrite persist_reap_cqes with user_data slot indexing and confirmed offset"
```

---

### Task 5: 更新 persist_pending_enqueue, persist_drain_pending, persist_submit_pending

**Files:**
- Modify: `src/persistence/kvs_persist.c:101-112, 160-166, 608-611`

**Interfaces:**
- Consumes: `g_pending_head`, `g_pending_slots`
- Produces: Simplified `persist_pending_enqueue`, updated `persist_drain_pending`, `persist_submit_pending` (cosmetic only)
- Public API unchanged

- [ ] **Step 1: 重写 persist_pending_enqueue**

将第 101-112 行：

```c
int persist_pending_enqueue(conn_t *c, unsigned char *resp, size_t resp_len) {
    persist_pending_t *p = (persist_pending_t *)kvs_malloc(sizeof(*p));
    if (!p) return -1;
    p->conn = c;
    p->resp = resp;
    p->resp_len = resp_len;
    p->cqe_seen = 0;
    p->cqe_ok = 0;
    p->last_error = 0;
    TAILQ_INSERT_TAIL(&g_persist_pending_head, p, link);
    return 0;
}
```

替换为：

```c
int persist_pending_enqueue(conn_t *c, unsigned char *resp, size_t resp_len) {
    /* The slot was already reserved by persist_append_prepare.
     * Fill the conn/resp of the most recently reserved slot. */
    uint32_t idx = g_pending_head - 1;
    persist_pending_slot_t *slot = &g_pending_slots[idx % PERSIST_PENDING_RING_SIZE];
    slot->conn = c;
    slot->resp = resp;
    slot->resp_len = resp_len;
    return 0;
}
```

- [ ] **Step 2: 重写 persist_drain_pending**

将第 160-166 行：

```c
void persist_drain_pending(void) {
    if (!g_persist_uring_ready) return;
    while (!TAILQ_EMPTY(&g_persist_pending_head)) {
        io_uring_submit_and_wait(&g_persist_uring, 1);
        persist_reap_cqes();
    }
}
```

替换为：

```c
void persist_drain_pending(void) {
    if (!g_persist_uring_ready) return;
    while (g_pending_head != g_pending_tail) {
        io_uring_submit_and_wait(&g_persist_uring, 1);
        persist_reap_cqes();
    }
}
```

- [ ] **Step 3: persist_submit_pending 无需改动**

```c
void persist_submit_pending(void) {
    if (!g_persist_uring_ready) return;
    io_uring_submit(&g_persist_uring);
}
```

确认此函数无变更。

- [ ] **Step 4: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "refactor(persist): simplify enqueue/drain for ring buffer"
```

---

### Task 6: 更新所有 g_aof_write_offset 引用处

**Files:**
- Modify: `src/persistence/kvs_persist.c:548-552, 566-569, 661-662, 714`

**Interfaces:**
- Consumes: `g_aof_write_submitted`, `g_aof_write_confirmed`
- Removes: all references to `g_aof_write_offset`

需要改 4 处：

- [ ] **Step 1: finalize_rewrite_parent offset 初始化（第 548-552 行）**

```c
// Before (line 548-552):
    if (g_aof_fd >= 0) close(g_aof_fd);
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_offset = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_offset < 0) g_aof_write_offset = 0;

// After:
    if (g_aof_fd >= 0) close(g_aof_fd);
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_confirmed = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_confirmed < 0) g_aof_write_confirmed = 0;
    g_aof_write_submitted = g_aof_write_confirmed;
```

- [ ] **Step 2: persist_init offset 初始化（第 566-569 行）**

```c
// Before (line 566-569):
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_offset = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_offset < 0) g_aof_write_offset = 0;

// After:
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_confirmed = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_confirmed < 0) g_aof_write_confirmed = 0;
    g_aof_write_submitted = g_aof_write_confirmed;
```

- [ ] **Step 3: persist_save_dump（第 661 行）**

```c
// Before (line 661):
    unsigned long long aof_off = (unsigned long long)g_aof_write_offset;

// After:
    unsigned long long aof_off = (unsigned long long)g_aof_write_confirmed;
```

- [ ] **Step 4: persist_bgsave_start（第 714 行）**

```c
// Before (line 714):
    unsigned long long aof_off = (unsigned long long)g_aof_write_offset;

// After:
    unsigned long long aof_off = (unsigned long long)g_aof_write_confirmed;
```

- [ ] **Step 5: 验证无遗漏引用**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && grep -rn "g_aof_write_offset" src/
```

预期：无任何匹配。

- [ ] **Step 6: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "fix(persist): replace all g_aof_write_offset references with submitted/confirmed"
```

---

### Task 7: 构建、测试、基准

**Files:**
- 无新增文件

- [ ] **Step 1: 构建**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make 2>&1
```

预期：编译 + 链接成功，无 warning（`-Wall -Wextra` 下），无 error。

- [ ] **Step 2: 运行已有测试套件**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && bash tests/run_all.sh 2>&1 | tail -40
```

如果 `tests/run_all.sh` 不存在，则逐个运行：

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && for t in tests/*/test_*.c; do echo "=== $t ==="; make test_target_$(basename $t .c) && ./test_target_$(basename $t .c); done
```

预期：全部通过。

- [ ] **Step 3: 运行持久化基准**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && bash tools/bench/run_persist_bench.sh 2>&1
```

预期：QPS ≥ 40,000（基线 39,040），目标范围 47,000-55,000（+20~40%）。

- [ ] **Step 4: 验证 slave 复制路径无 stderr 报错**

启动 slave（使用 eBPF proxy 转发的环境），执行写命令，检查 stderr：

```bash
# 在 slave 终端确认无 "CQE without pending entry" 或 "invalid user_data" 输出
grep -i "persist:" /path/to/slave/stderr.log
```

预期：无匹配或仅有正常日志。

- [ ] **Step 5: 验证 dump/snapshot/bgrewriteaof 功能**

```bash
# 依次验证
# 1. 手动触发 BGSAVE
redis-cli -p <port> BGSAVE
# 2. 触发 BGREWRITEAOF
redis-cli -p <port> BGREWRITEAOF
# 3. 检查结果
redis-cli -p <port> INFO PERSISTENCE
```

预期：bgsave=ok, aof_rewrite=ok。

- [ ] **Step 6: Commit（如果有测试调整）**

```bash
git status
# 如果有测试文件变更：
git add tests/
git commit -m "test: update persist tests for ring buffer changes"
```
