# Pipeline AOF always 批量 Submit 优化 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 pipeline 场景下每条命令独立的 `io_uring_submit()` syscall 合并为批量提交，消除 P=10/20 时的 QPS 倒退。

**Architecture:** 拆分 `persist_append_raw()` 为 `persist_append_prepare()`（准备 SQE，不 submit）+ `persist_submit_pending()`（统一提交）。`on_read()` 在处理完当前 buffer 所有命令后调用 `persist_submit_pending()`。SQ ring 自然反压处理超大 P 场景。

**Tech Stack:** C, io_uring, epoll (reactor)

## Global Constraints

- 保持 durable-before-reply 语义：+OK 必须在 fdatasync 完成后才发送
- 一条命令一次 fdatasync：不引入 group commit
- 不改非 reactor 路径
- 最小改动

---

### Task 1: 头文件新增声明

**Files:**
- Modify: `include/kvstore/kvstore.h:512`

**Interfaces:**
- Produces: `int persist_append_prepare(const unsigned char *buf, size_t len);` — 准备 write+fsync SQE pair，不调 io_uring_submit()
- Produces: `void persist_submit_pending(void);` — 提交所有积压的 SQE

- [ ] **Step 1: 添加声明**

在 `int persist_append_raw(...)` 声明之后插入两行新声明：

```c
int persist_append_raw(const unsigned char *buf, size_t len);
int persist_append_prepare(const unsigned char *buf, size_t len);  /* 新增 */
void persist_submit_pending(void);                                   /* 新增 */
int persist_write_raw_fd(int fd, const unsigned char *buf, size_t len, long long *offset_io);
```

- [ ] **Step 2: 编译验证头文件**

```bash
make clean && make
```

Expected: 无编译错误（新增声明未使用，可能有 warning，忽略）。

- [ ] **Step 3: Commit**

```bash
git add include/kvstore/kvstore.h
git commit -m "feat(persist): declare persist_append_prepare and persist_submit_pending"
```

---

### Task 2: persist_append_prepare + persist_submit_pending 实现

**Files:**
- Modify: `src/persistence/kvs_persist.c:608-646`

**Interfaces:**
- Produces: `int persist_append_prepare(const unsigned char *buf, size_t len)` — 返回 `KVS_PERSIST_PENDING`
- Produces: `void persist_submit_pending(void)` — 调用 `io_uring_submit()`
- Modifies: `int persist_append_raw(...)` — 改为调用 prepare + submit

- [ ] **Step 1: 新增 persist_submit_pending()**

在 `persist_append_raw()` 函数定义之前（第 608 行前）插入：

```c
void persist_submit_pending(void) {
    if (!g_persist_uring_ready) return;
    io_uring_submit(&g_persist_uring);
}
```

- [ ] **Step 2: 新增 persist_append_prepare()**

在 `persist_submit_pending()` 之后插入：

```c
int persist_append_prepare(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;

    if (g_aof_fd < 0) return g_aof_disabled ? KVS_PERSIST_OK : KVS_PERSIST_ERR;
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) return KVS_PERSIST_OK;

    if (g_persist_fatal_error) return KVS_PERSIST_ERR;

    if (persist_uring_init_once() != 0) return KVS_PERSIST_ERR;

    /* ensure at least 2 free SQEs — if ring is filling up, submit and reap */
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

- [ ] **Step 3: 重写 persist_append_raw()**

将现有的 `persist_append_raw()` 函数体替换为对 prepare + submit 的调用：

```c
int persist_append_raw(const unsigned char *buf, size_t len) {
    int rc = persist_append_prepare(buf, len);
    if (rc == KVS_PERSIST_PENDING)
        persist_submit_pending();
    return rc;
}
```

- [ ] **Step 4: 编译验证**

```bash
make clean && make
```

Expected: 编译通过，无错误无 warning。

- [ ] **Step 5: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(persist): add persist_append_prepare and persist_submit_pending"
```

---

### Task 3: 写命令路径改用 persist_append_prepare

**Files:**
- Modify: `src/main/kvstore.c:1460,1487,1514,1601,1631,1657,1830,1849`

**Interfaces:**
- Consumes: `int persist_append_prepare(const unsigned char *buf, size_t len)` from Task 2

- [ ] **Step 1: 批量替换**

将所有 8 处 `persist_append_raw(raw, rawlen)` 改为 `persist_append_prepare(raw, rawlen)`：

```bash
sed -i 's/persist_append_raw(raw, rawlen)/persist_append_prepare(raw, rawlen)/g' src/main/kvstore.c
```

无需手动逐行改。这 8 处全在 `handle_parsed_command()` 的写命令路径中，语义完全一致。

- [ ] **Step 2: 编译验证**

```bash
make clean && make
```

Expected: 编译通过，无错误。`persist_append_raw` 仍被 AOF 恢复路径（`replay_file`）等非 reactor 路径使用，不会被误删。

- [ ] **Step 3: Commit**

```bash
git add src/main/kvstore.c
git commit -m "feat(kvstore): use persist_append_prepare in write command paths"
```

---

### Task 4: reactor on_read() 批量提交

**Files:**
- Modify: `src/core/reactor.c:117-154`

**Interfaces:**
- Consumes: `void persist_submit_pending(void)` from Task 2

- [ ] **Step 1: 在 on_read() 插入 persist_submit_pending()**

在 `on_read()` 中，recv 循环结束之后、`persist_reap_cqes()` 之前插入 `persist_submit_pending()`：

当前代码（`reactor.c:117-153`）：

```c
static void on_read(conn_t *c) {
    while (1) {
        if (c->in_len >= sizeof(c->inbuf)) {
            close_conn(c);
            return;
        }

        ssize_t n = recv(c->fd, c->inbuf + c->in_len, sizeof(c->inbuf) - c->in_len, 0);
        if (n > 0) {
            c->in_len += (size_t)n;
            parse_resp_stream(c, c->inbuf, &c->in_len, 0);
            continue;
        }

        if (n == 0) {
            if (c->out_ring_len > 0) {
                mod_events(c, EPOLLOUT);
                return;
            }
            close_conn(c);
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        close_conn(c);
        return;
    }

    /* try immediate write after processing pipeline batch:
     * if parse_resp_stream queued multiple responses, send()
     * them now instead of waiting for next epoll_wait→EPOLLOUT */
    if (c->out_ring_len > 0) on_write(c);

    if (c->out_ring_len > 0) mod_events(c, EPOLLIN | EPOLLOUT);
    else mod_events(c, EPOLLIN);

    persist_reap_cqes();
}
```

改为（只在 recv 循环之后插入一行 `persist_submit_pending();`）：

```c
static void on_read(conn_t *c) {
    while (1) {
        if (c->in_len >= sizeof(c->inbuf)) {
            close_conn(c);
            return;
        }

        ssize_t n = recv(c->fd, c->inbuf + c->in_len, sizeof(c->inbuf) - c->in_len, 0);
        if (n > 0) {
            c->in_len += (size_t)n;
            parse_resp_stream(c, c->inbuf, &c->in_len, 0);
            continue;
        }

        if (n == 0) {
            if (c->out_ring_len > 0) {
                mod_events(c, EPOLLOUT);
                return;
            }
            close_conn(c);
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        close_conn(c);
        return;
    }

    /* batch-submit all pending AOF SQE pairs accumulated during
     * the parse_resp_stream loop above */
    persist_submit_pending();

    /* try immediate write after processing pipeline batch:
     * if parse_resp_stream queued multiple responses, send()
     * them now instead of waiting for next epoll_wait→EPOLLOUT */
    if (c->out_ring_len > 0) on_write(c);

    if (c->out_ring_len > 0) mod_events(c, EPOLLIN | EPOLLOUT);
    else mod_events(c, EPOLLIN);

    persist_reap_cqes();
}
```

- [ ] **Step 2: 编译验证**

```bash
make clean && make
```

Expected: 编译通过，无错误。

- [ ] **Step 3: Commit**

```bash
git add src/core/reactor.c
git commit -m "feat(reactor): batch-submit pending AOF SQEs in on_read"
```

---

### Task 5: 功能回归测试

**Files:** 无新建/修改（纯验证）

- [ ] **Step 1: 运行基础功能测试**

```bash
make check
```

Expected: 全部 PASS。验证 RESP 协议、TTL、基础持久化、文档引擎功能不受影响。

- [ ] **Step 2: 运行批量 1w 级回归**

```bash
make check-bulk-1w
```

Expected: PASS。验证 HSET/HGET/TTL/SAVE+恢复/DOC 批量场景。

- [ ] **Step 3: 运行持久化测试**

```bash
make check-persist
make check-demo-full-dump
make check-demo-incr-aof
```

Expected: 全部 PASS。验证 AOF always 语义不变，dump/AOF 恢复正确。

- [ ] **Step 4: Commit（如有测试修复）**

```bash
git add -u
git commit -m "test: verify AOF batch submit with existing tests"
```

---

### Task 6: Pipeline 性能基准复测

**Files:** 无新建/修改（纯验证）

- [ ] **Step 1: 运行 pipeline 基准测试**

```bash
bash tools/bench/run_pipeline_bench.sh
```

Expected: 测试完成，输出到 `benchmarks/data/pipeline_bench/pipeline_summary.csv`。

- [ ] **Step 2: 提取 AOF always 数据对比**

```bash
grep "aof_always" benchmarks/data/pipeline_bench/pipeline_summary.csv
```

对比预期：
- P=1: 应接近 39K（无变化）
- P=10: 应从 11.6K 显著提升（目标 150K+）
- P=20: 应从 22.7K 显著提升（目标 250K+）
- P=160: 应从 128K 提升到 480K+

- [ ] **Step 3: 更新 README 性能数据**

用新数据更新 README.md 中 Pipeline 批量性能测试章节。

- [ ] **Step 4: Commit**

```bash
git add benchmarks/data/pipeline_bench/ README.md
git commit -m "docs: update pipeline benchmark data with batch submit optimization"
```

---
