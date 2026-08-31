# AOF ALWAYS per-command io_uring SQPOLL 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `appendfsync always` 做到严格 per-command 落盘（每条写命令 write+fsync 完成才回复客户端），使用 io_uring SQPOLL 内核线程消除提交 syscall 开销。

**Architecture:** 双 io_uring ring 设计 — 现有 `g_persist_uring`（无 SQPOLL，EVERYSEC 批量用）+ 新增 `g_persist_uring_sqpoll`（SQPOLL，ALWAYS per-command 用），互斥懒初始化。新增 `persist_aof_per_command_flush()` 直接写盘绕过 buffer，`persist_append_raw()` ALWAYS 分支改为立即调用此函数。

**Tech Stack:** C + liburing (already linked via Makefile `-luring`)

**Source Spec:** `docs/superpowers/specs/2026-06-29-aof-always-io_uring-sqpoll-design.md`

## Global Constraints

- 仅修改 `src/persistence/kvs_persist.c`
- EVERYSEC 路径完全不变
- SQPOLL 不可用时自动降级到 pwrite+fdatasync（兼容旧内核）
- 不改变任何 public 接口（对外函数签名不变）
- 维持现有代码风格（C89/C99 混用，4 空格缩进，行内注释用 `/* */`）

---

## 文件结构

| 文件 | 职责 | 变更类型 |
|------|------|----------|
| `src/persistence/kvs_persist.c` | AOF 持久化全部逻辑 | 修改（+~80 行） |

接口：
- 所有新增函数均为 `static`，不对外暴露
- `persist_append_raw` 接口不变（调用者 `handle_parsed_command` 在 `kvstore.c` 不需要改动）
- `persist_set_aof_policy` 接口不变
- `persist_close` 接口不变

---

### Task 1: 添加 SQPOLL ring 全局变量和 init/close 函数

**Files:**
- Modify: `src/persistence/kvs_persist.c:60-68`

**Interfaces:**
- Consumes: 现有 `g_persist_uring_ready`, `g_persist_uring`
- Produces: `g_persist_uring_sqpoll_ready` (static int), `g_persist_uring_sqpoll` (static struct io_uring), `persist_uring_sqpoll_init_once()` (static, returns 0 or -1)

- [ ] **Step 1: 在 g_persist_uring 之后添加 SQPOLL ring 全局变量**

在 line 61 `static struct io_uring g_persist_uring;` 之后添加：

```c
static int g_persist_uring_sqpoll_ready = 0;
static struct io_uring g_persist_uring_sqpoll;
```

- [ ] **Step 2: 在 persist_uring_init_once 之后添加 SQPOLL init 函数**

在 line 68 `}` 之后（persist_uring_init_once 结束），添加：

```c
static int persist_uring_sqpoll_init_once(void) {
    if (g_persist_uring_sqpoll_ready) return 0;
    struct io_uring_params p = {0};
    p.flags = IORING_SETUP_SQPOLL;
    p.sq_thread_idle = 2000;  /* kernel thread sleeps after 2s idle (kernel 5.11+) */
    if (io_uring_queue_init_params(64, &g_persist_uring_sqpoll, &p) != 0)
        return -1;
    g_persist_uring_sqpoll_ready = 1;
    return 0;
}
```

- [ ] **Step 3: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make -j$(nproc) 2>&1 | tail -20
```

Expected: 编译成功，无错误。

- [ ] **Step 4: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(aof): add SQPOLL ring globals and init for ALWAYS per-command"
```

---

### Task 2: 添加 persist_aof_per_command_flush 函数

**Files:**
- Modify: `src/persistence/kvs_persist.c`（在 `persist_fsync_fd_best_effort` 之后，`persist_aof_flush_buffer` 之前，约 line 196 处）

**Interfaces:**
- Consumes: `g_persist_uring_sqpoll`, `g_persist_uring_sqpoll_ready`, `g_aof_fd`, `g_aof_write_offset`, `persist_write_fd_sync`
- Produces: `persist_aof_per_command_flush(buf, len)` — static, returns 0 on success, -1 on failure

- [ ] **Step 1: 在 persist_fsync_fd_best_effort 之后插入新函数**

在 line 196 `}` (persist_fsync_fd_best_effort 结束) 之后，line 197 空行之前，插入：

```c
/* Per-command write+fsync for ALWAYS mode using io_uring SQPOLL.
 * Falls back to pwrite+fdatasync when SQPOLL is unavailable (old kernel). */
static int persist_aof_per_command_flush(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;
    struct io_uring_cqe *cqe;
    int written;

    if (len == 0) return 0;

    /* Degrade: SQPOLL unavailable → direct pwrite+fdatasync */
    if (persist_uring_sqpoll_init_once() != 0) {
        off_t off = (off_t)g_aof_write_offset;
        if (persist_write_fd_sync(g_aof_fd, buf, len, &off) != 0) return -1;
        if (fdatasync(g_aof_fd) != 0) return -1;
        g_aof_write_offset = (long long)off;
        return 0;
    }

    sqe_w = io_uring_get_sqe(&g_persist_uring_sqpoll);
    sqe_f = io_uring_get_sqe(&g_persist_uring_sqpoll);
    if (!sqe_w || !sqe_f) return -1;

    io_uring_prep_write(sqe_w, g_aof_fd, buf, len, (off_t)g_aof_write_offset);
    io_uring_prep_fsync(sqe_f, g_aof_fd, IORING_FSYNC_DATASYNC);
    sqe_f->flags |= IOSQE_IO_LINK;  /* chain: write completes → fsync begins */

    /* SQPOLL kernel thread polls SQ; io_uring_submit wakes it from idle */
    io_uring_submit(&g_persist_uring_sqpoll);

    /* wait for write completion */
    if (io_uring_wait_cqe(&g_persist_uring_sqpoll, &cqe) < 0 || !cqe) return -1;
    written = cqe->res;
    io_uring_cqe_seen(&g_persist_uring_sqpoll, cqe);
    if (written <= 0) return -1;

    /* wait for fsync completion */
    if (io_uring_wait_cqe(&g_persist_uring_sqpoll, &cqe) < 0 || !cqe) return -1;
    if (cqe->res < 0) { io_uring_cqe_seen(&g_persist_uring_sqpoll, cqe); return -1; }
    io_uring_cqe_seen(&g_persist_uring_sqpoll, cqe);

    g_aof_write_offset += (long long)written;
    return 0;
}
```

- [ ] **Step 2: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make -j$(nproc) 2>&1 | tail -20
```

Expected: 编译成功，无错误，无未使用函数警告（函数将在 Task 3 中使用）。

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(aof): add persist_aof_per_command_flush with SQPOLL + degrade"
```

---

### Task 3: 修改 persist_append_raw — ALWAYS 分支改为 per-command flush

**Files:**
- Modify: `src/persistence/kvs_persist.c:578-615`

**Interfaces:**
- Consumes: `persist_aof_per_command_flush`
- Produces: 修改后的 `persist_append_raw`（接口不变）

- [ ] **Step 1: 替换 persist_append_raw 函数体**

将 lines 578-615 替换为：

```c
int persist_append_raw(const unsigned char *buf, size_t len) {
    if (g_aof_fd < 0) return g_aof_disabled ? 0 : -1;

    /* oversized command: flush buffer first, then write directly */
    if (len >= AOF_BUF_SIZE) {
        if (persist_aof_flush_buffer() != 0) return -1;
        if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
            if (persist_aof_per_command_flush(buf, len) != 0) return -1;
        } else {
            off_t off = (off_t)g_aof_write_offset;
            if (persist_write_and_fsync_uring(g_aof_fd, buf, len, &off) != 0) {
                if (persist_write_fd_sync(g_aof_fd, buf, len, &off) != 0) return -1;
            }
            g_aof_write_offset = (long long)off;
        }
        if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);
        return 0;
    }

    /* small command routing */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
        /* ALWAYS: per-command write+fsync — no buffering */
        if (g_aof_buf_len > 0) persist_aof_flush_buffer();
        if (persist_aof_per_command_flush(buf, len) != 0) return -1;
    } else {
        /* EVERYSEC: buffer the write, flush deferred to cron timer */
        if (g_aof_buf_len + len > AOF_BUF_SIZE) {
            if (persist_aof_flush_buffer() != 0) return -1;
        }
        memcpy(g_aof_buf + g_aof_buf_len, buf, len);
        g_aof_buf_len += len;
        if (g_aof_buffered_since_ms == 0) g_aof_buffered_since_ms = kvs_now_ms();
        g_aof_dirty = 1;
    }

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    /* EVERYSEC group-commit: flush on 2ms timeout ceiling */
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) {
        if (kvs_now_ms() - g_aof_buffered_since_ms >= 2) {
            persist_aof_flush_buffer();
            g_aof_buffered_since_ms = 0;
        }
    }
    return 0;
}
```

- [ ] **Step 2: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make -j$(nproc) 2>&1 | tail -20
```

Expected: 编译成功，无错误。

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(aof): per-command flush in persist_append_raw for ALWAYS via SQPOLL"
```

---

### Task 4: 修改 persist_set_aof_policy — 运行时 ring 切换

**Files:**
- Modify: `src/persistence/kvs_persist.c:558-562`

**Interfaces:**
- Consumes: `g_persist_uring_sqpoll_ready`, `g_persist_uring_sqpoll`, `persist_aof_flush_buffer`
- Produces: 修改后的 `persist_set_aof_policy`（接口不变）

- [ ] **Step 1: 替换 persist_set_aof_policy 函数体**

将 lines 558-562 替换为：

```c
int persist_set_aof_policy(kvs_aof_fsync_policy_t policy) {
    if (policy != KVS_AOF_FSYNC_ALWAYS && policy != KVS_AOF_FSYNC_EVERYSEC) return -1;

    /* EVERYSEC → ALWAYS: flush any remaining buffer, SQPOLL ring init lazy */
    if (policy == KVS_AOF_FSYNC_ALWAYS && g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) {
        persist_aof_flush_buffer();
        g_aof_buf_len = 0;
    }

    /* ALWAYS → EVERYSEC: tear down SQPOLL ring, free kernel thread */
    if (policy != KVS_AOF_FSYNC_ALWAYS && g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
        if (g_persist_uring_sqpoll_ready) {
            io_uring_queue_exit(&g_persist_uring_sqpoll);
            g_persist_uring_sqpoll_ready = 0;
        }
    }

    g_cfg.aof_fsync = policy;
    return 0;
}
```

- [ ] **Step 2: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make -j$(nproc) 2>&1 | tail -20
```

Expected: 编译成功，无错误。

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(aof): handle SQPOLL ring init/teardown on runtime policy switch"
```

---

### Task 5: 修改 persist_close — 清理 SQPOLL ring

**Files:**
- Modify: `src/persistence/kvs_persist.c:547-556`

**Interfaces:**
- Consumes: `g_persist_uring_sqpoll_ready`, `g_persist_uring_sqpoll`
- Produces: 修改后的 `persist_close`（接口不变）

- [ ] **Step 1: 替换 persist_close 函数体**

将 lines 547-556 替换为：

```c
void persist_close(void) {
    if (g_aof_fd >= 0) {
        /* flush any buffered AOF data before fsync+close */
        if (g_aof_buf_len > 0) persist_aof_flush_buffer();
        persist_flush_aof_fd(g_aof_fd);
        close(g_aof_fd);
    }
    g_aof_fd = -1;
    persist_uring_close();
    if (g_persist_uring_sqpoll_ready) {
        io_uring_queue_exit(&g_persist_uring_sqpoll);
        g_persist_uring_sqpoll_ready = 0;
    }
}
```

- [ ] **Step 2: 构建验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make -j$(nproc) 2>&1 | tail -20
```

Expected: 编译成功，无错误。

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat(aof): clean up SQPOLL ring on persist_close"
```

---

### Task 6: 更新 persist_aof_flush_buffer 注释

**Files:**
- Modify: `src/persistence/kvs_persist.c:208`

**Interfaces:**
- Consumes: none
- Produces: 更新后的注释（无行为变更）

- [ ] **Step 1: 更新注释反映现状**

将 line 208 注释从：

```c
        /* ALWAYS: per-command, direct syscall 比 io_uring submit_and_wait 快 */
```

替换为：

```c
        /* ALWAYS: flush residual buffer (mode-switch edge case); normal per-command path
         * uses persist_aof_per_command_flush with SQPOLL, never goes through buffer */
```

- [ ] **Step 2: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "docs(aof): update persist_aof_flush_buffer comment for new ALWAYS flow"
```

---

### Task 7: 功能验证 — AOF 持久化恢复测试

**Files:**
- Read: `tests/test_persist_aof_demo.c`

**Description:** 手动测试 per-command ALWAYS 模式的数据持久化正确性。

- [ ] **Step 1: 构建 kvstore**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make -j$(nproc) 2>&1 | tail -5
```

Expected: 编译成功。

- [ ] **Step 2: 检查是否当前内核支持 SQPOLL**

```bash
uname -r
```

Expected: 显示内核版本。如果 >= 5.11 则 `sq_thread_idle` 有效，否则 SQPOLL 可用但内核线程不自动休眠（自动降级不影响功能）。

- [ ] **Step 3: 启动 kvstore（ALWAYS 模式）**

启动 kvstore 实例：

```bash
# 终端 1
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore kvstore.conf --role master
# 确保 kvstore.conf 中 appendfsync=always
```

Expected: kvstore 启动，显示 "Server started"。

- [ ] **Step 4: 写入测试数据 + 验证恢复**

在另一终端运行：

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
make test_persist_aof_demo 2>&1 | tail -5
# 按 test_persist_aof_demo.c 注释中的步骤操作：
# 1. 运行 ./test_persist_aof_demo --config tests/test.conf
# 2. 停止 kvstore (Ctrl+C)
# 3. 重启 kvstore
# 4. 程序自动验证数据恢复
```

Expected: 数据从 AOF 恢复成功，写入的 key 全部可读。

- [ ] **Step 5: 检查 log 确认 SQPOLL 路径生效**

```bash
# 检查是否使用了 io_uring（而非降级到 pwrite）
dmesg | grep -i "uring\|sqpoll" | tail -5 || echo "无 dmesg 输出 — 正常"
# 可以临时加一行 fprintf(stderr) 到 persist_aof_per_command_flush 的 SQPOLL 路径确认
```

Expected: 程序无异常退出，AOF 文件存在且非空。

---

### Task 8: 降级路径验证 — 老内核或 SQPOLL 不可用时不崩溃

- [ ] **Step 1: 确认降级逻辑正确**

代码审查：当 `io_uring_queue_init_params` 失败时（例如 `IORING_SETUP_SQPOLL` 不被支持），`persist_uring_sqpoll_init_once` 返回 -1，`persist_aof_per_command_flush` 自动走 `pwrite+fdatasync` 降级路径。

- [ ] **Step 2: 如果环境支持，模拟降级**

修改 `persist_uring_sqpoll_init_once` 临时返回 -1：

```c
static int persist_uring_sqpoll_init_once(void) {
    return -1;  /* 强制降级 */
    // ... 原代码 ...
}
```

重新编译并运行 test_persist_aof_demo，验证降级路径也能正常工作。

```bash
make clean && make -j$(nproc) 2>&1 | tail -5
# 重新运行 Step 3-5 的测试
```

Expected: 降级到 pwrite+fdatasync，数据持久化正常。

- [ ] **Step 3: 恢复代码**

撤销上一步的强制降级修改。

- [ ] **Step 4: Commit（如果测试过程中有日志/调试改动）**

```bash
git diff  # 确认只有预期的改动
# 如有调试改动，撤销它们
git checkout -- src/persistence/kvs_persist.c
```

---

### Task 9: 运行时模式切换测试

- [ ] **Step 1: 启动 kvstore**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
./kvstore kvstore.conf --role master
```

- [ ] **Step 2: 通过 redis-cli 来回切换模式并写入数据**

```bash
# 另开终端
redis-cli -p 6380  # 假设 kvstore 监听 6380
> CONFIG SET appendfsync everysec
> SET k1 v1
> CONFIG SET appendfsync always
> SET k2 v2
> SET k3 v3
> CONFIG SET appendfsync everysec
> SET k4 v4
> CONFIG SET appendfsync always
> SET k5 v5
> SAVE
```

Expected: 所有 SET 命令返回 OK，无崩溃，无错误。

- [ ] **Step 3: 重启验证数据恢复**

重启 kvstore → redis-cli 验证所有 key 存在：

```bash
> GET k1  # 应返回 v1
> GET k2  # 应返回 v2
> GET k3  # 应返回 v3
> GET k4  # 应返回 v4
> GET k5  # 应返回 v5
```

Expected: 所有 key 完整恢复。

---

### Task 10: 性能基准测试

- [ ] **Step 1: 运行 AOF 持久化性能基准**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
bash tools/bench/run_persist_bench.sh
```

Expected: 基准测试运行完成，生成 `benchmarks/data/persist_bench/aof_summary.csv`。

- [ ] **Step 2: 对比新旧性能**

```bash
cat benchmarks/data/persist_bench/aof_summary.csv
```

重点关注 `kvstore_aof_always` 行 QPS：

| 模式 | 目标 |
|------|------|
| kvstore_aof_always (新) | > Redis aof always (40,660 QPS) |
| kvstore_aof_everysec (新) | ≈ 旧 everysec（几乎不变） |

- [ ] **Step 3: 如果 SQPOLL 性能不如预期**

如果 SQPOLL per-command 的性能甚至不如旧的 group commit（这是预期的 — group commit 用 batching 摊销 fsync），**不需要 revert**。per-command 语义与 group commit 语义不同，按需求做就行。如果 _真的_ 比 pwrite+fdatasync 还差，检查是否进入了降级路径，或 SQPOLL ring 参数是否需要调优。

- [ ] **Step 4: 记录基准结果并提交**

```bash
# 如果基准数据有更新
git add benchmarks/data/persist_bench/
git commit -m "bench(aof): update ALWAYS SQPOLL per-command benchmark results"
```

---

### Task 11: 最终检查与清理

- [ ] **Step 1: 检查 diff 完整性**

```bash
git diff HEAD~5..HEAD --stat   # 或查看所有相关 commit
git diff HEAD~5..HEAD -- src/persistence/kvs_persist.c | head -200
```

Expected: 只有 kvs_persist.c 有改动。

- [ ] **Step 2: 最终构建**

```bash
make clean && make -j$(nproc) 2>&1
```

Expected: 编译成功，零 warning。

- [ ] **Step 3: 检查无遗留问题**

- 无 stub/TODO 代码保留
- 无调试 printf/fprintf 残留
- `g_persist_uring_sqpoll` 在所有退出路径上正确销毁（persist_close、模式切换）
- EVERYSEC 路径行为不变

- [ ] **Step 4: Commit any final cleanup**

```bash
git diff  # 确认干净
```

---

## 变更汇总

| 函数 | 变更类型 | 预计行数 |
|------|----------|----------|
| 全局变量（SQPOLL ring） | 新增 3 行 | +3 |
| `persist_uring_sqpoll_init_once` | 新增 9 行 | +9 |
| `persist_aof_per_command_flush` | 新增 ~35 行 | +35 |
| `persist_append_raw` | 重写 ~40 行 | +25/-20 |
| `persist_set_aof_policy` | 重写 ~12 行 | +12/-4 |
| `persist_close` | 添加 5 行 | +5 |
| `persist_aof_flush_buffer` | 注释修改 | +1/-1 |
| **合计** | | **+90/-25** |
