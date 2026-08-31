# AOF ALWAYS 逐命令 io_uring 刷盘（去掉 EVERYSEC）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 AOF 策略简化为 OFF/ALWAYS 两项，ALWAYS 模式下每条写命令直接用 io_uring write+fsync 落盘后回复客户端，去掉缓冲区和 EVERYSEC。

**Architecture:** 删除 `g_aof_buf` 缓冲区及所有组提交逻辑。`persist_append_raw()` 在 ALWAYS 模式下直接提交 io_uring write SQE + fsync SQE，`submit_and_wait` 等两个 CQE 完成后再返回。复用已有 `g_persist_uring` ring（64 entries, no SQPOLL）。

**Tech Stack:** C, liburing, 复用已有 io_uring ring

## Global Constraints

- 最小改动，不改无关代码
- 保持现有代码风格（注释密度、命名、缩进）
- 不改 `tests/` 下的测试文件
- 默认值保持 `KVS_AOF_FSYNC_ALWAYS`

---

### Task 1: 简化枚举和头文件声明

**Files:**
- Modify: `include/kvstore/kvstore.h:78-81`
- Modify: `include/kvstore/kvstore.h:525` (remove `persist_flush_pending` declaration)

**Interfaces:**
- Produces: `KVS_AOF_FSYNC_OFF = 0`, `KVS_AOF_FSYNC_ALWAYS = 1`

- [ ] **Step 1: 修改枚举定义**

将 [kvstore.h:78-81](include/kvstore/kvstore.h#L78-L81) 的 `kvs_aof_fsync_policy_t` 改为：

```c
typedef enum {
    KVS_AOF_FSYNC_OFF    = 0,
    KVS_AOF_FSYNC_ALWAYS = 1,
} kvs_aof_fsync_policy_t;
```

- [ ] **Step 2: 删除 persist_flush_pending 声明**

删除 [kvstore.h:525](include/kvstore/kvstore.h#L525) 这一行：

```c
void persist_flush_pending(void);
```

- [ ] **Step 3: 编译验证**

```bash
make -j$(nproc) 2>&1 | head -50
```

预期：编译报错（其他文件还在引用 `KVS_AOF_FSYNC_EVERYSEC` 和 `persist_flush_pending`），确认错误来自预期的文件。

- [ ] **Step 4: 提交**

```bash
git add include/kvstore/kvstore.h
git commit -m "refactor: simplify aof fsync enum to OFF/ALWAYS, remove persist_flush_pending decl"
```

---

### Task 2: 重写 kvs_persist.c 核心逻辑

**Files:**
- Modify: `src/persistence/kvs_persist.c`

**Interfaces:**
- Consumes: `KVS_AOF_FSYNC_OFF`, `KVS_AOF_FSYNC_ALWAYS` (from Task 1)
- Produces: `persist_append_raw()` 新语义, `persist_set_aof_policy()` 只接受 OFF/ALWAYS, `persist_aof_policy_name()` 返回 "off"/"always", `persist_autosnap_cron()` 无 AOF flush, `persist_flush_pending()` 删除

- [ ] **Step 1: 删除缓冲区相关的全局变量**

删除 [kvs_persist.c:30-39](src/persistence/kvs_persist.c#L30-L39)：

```c
/* 删除以下 6 行: */
static int g_aof_dirty = 0;
static long long g_aof_last_flush_ms = 0;
/* ... 空行 ... */
/* AOF write buffer: batch small RESP commands into larger io_uring writes */
#define AOF_BUF_SIZE 65536
static unsigned char g_aof_buf[AOF_BUF_SIZE];
static size_t g_aof_buf_len = 0;
static long long g_aof_buffered_since_ms = 0;
```

- [ ] **Step 2: 删除 persist_aof_flush_buffer() 函数**

删除 [kvs_persist.c:198-229](src/persistence/kvs_persist.c#L198-L229) 整个 `persist_aof_flush_buffer()` 函数。

- [ ] **Step 3: 删除 persist_flush_pending() 函数**

删除 [kvs_persist.c:236-241](src/persistence/kvs_persist.c#L236-L241) 整个 `persist_flush_pending()` 函数。

- [ ] **Step 4: 删除 persist_write_and_fsync_uring() 函数**

删除 [kvs_persist.c:141-191](src/persistence/kvs_persist.c#L141-L191) 整个 `persist_write_and_fsync_uring()` 函数。

- [ ] **Step 5: 重写 persist_append_raw()**

将 [kvs_persist.c:608-645](src/persistence/kvs_persist.c#L608-L645) 替换为：

```c
int persist_append_raw(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;
    struct io_uring_cqe *cqe;
    off_t off;
    int rc, r1, r2;

    if (g_aof_fd < 0) return g_aof_disabled ? 0 : -1;
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) return 0;

    if (persist_uring_init_once() != 0) return -1;

    off = (off_t)g_aof_write_offset;

    /* submit write SQE */
    sqe_w = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_w) return -1;
    io_uring_prep_write(sqe_w, g_aof_fd, buf, len, off);

    /* submit fsync SQE */
    sqe_f = io_uring_get_sqe(&g_persist_uring);
    if (!sqe_f) return -1;
    io_uring_prep_fsync(sqe_f, g_aof_fd, IORING_FSYNC_DATASYNC);

    /* wait for both completions */
    rc = io_uring_submit_and_wait(&g_persist_uring, 2);
    if (rc < 0) return -1;

    /* collect first CQE */
    rc = io_uring_wait_cqe(&g_persist_uring, &cqe);
    if (rc < 0 || !cqe) return -1;
    r1 = cqe->res;
    io_uring_cqe_seen(&g_persist_uring, cqe);

    /* collect second CQE */
    rc = io_uring_wait_cqe(&g_persist_uring, &cqe);
    if (rc < 0 || !cqe) return -1;
    r2 = cqe->res;
    io_uring_cqe_seen(&g_persist_uring, cqe);

    /* dispatch by result: write returns >0 (bytes), fsync returns 0 on success */
    if (r1 > 0) {
        if (r2 < 0) return -1;
        off += r1;
    } else if (r2 > 0) {
        if (r1 < 0) return -1;
        off += r2;
    } else {
        return -1;
    }

    g_aof_write_offset = (long long)off;

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    return 0;
}
```

- [ ] **Step 6: 简化 persist_autosnap_cron() — 去掉 AOF flush 逻辑**

将 [kvs_persist.c:936-970](src/persistence/kvs_persist.c#L936-L970) 替换为：

```c
int persist_autosnap_cron(void) {
    persist_bgsave_poll();
    persist_bgrewriteaof_poll();

    if (g_cfg.role != ROLE_MASTER) return 0;
    if (g_bgsave_pid > 0) return 0;
    if (g_cfg.autosnap_rule_count <= 0) return 0;

    long long now = kvs_now_ms();
    long long last_ms = g_last_snapshot_ms > 0 ? g_last_snapshot_ms : g_bgsave_last_end_ms;
    if (last_ms <= 0) last_ms = now;

    for (int i = 0; i < g_cfg.autosnap_rule_count; ++i) {
        long long sec = g_cfg.autosnap_rules[i].seconds;
        long long changes = g_cfg.autosnap_rules[i].changes;
        if ((long long)g_dirty_counter >= changes && now - last_ms >= sec * 1000) {
            return persist_bgsave_start();
        }
    }
    return 0;
}
```

- [ ] **Step 7: 简化 persist_set_aof_policy()**

将 [kvs_persist.c:588-592](src/persistence/kvs_persist.c#L588-L592) 替换为：

```c
int persist_set_aof_policy(kvs_aof_fsync_policy_t policy) {
    if (policy != KVS_AOF_FSYNC_OFF && policy != KVS_AOF_FSYNC_ALWAYS) return -1;
    g_cfg.aof_fsync = policy;
    return 0;
}
```

- [ ] **Step 8: 简化 persist_aof_policy_name()**

将 [kvs_persist.c:598-600](src/persistence/kvs_persist.c#L598-L600) 替换为：

```c
const char *persist_aof_policy_name(void) {
    return g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS ? "always" : "off";
}
```

- [ ] **Step 9: 简化 persist_force_aof_flush()**

将 [kvs_persist.c:602-606](src/persistence/kvs_persist.c#L602-L606) 替换为：

```c
int persist_force_aof_flush(void) {
    if (g_aof_fd < 0) return -1;
    if (persist_fsync_fd_best_effort(g_aof_fd) != 0) return -1;
    return 0;
}
```

- [ ] **Step 10: 简化 persist_close()**

将 [kvs_persist.c:577-586](src/persistence/kvs_persist.c#L577-L586) 替换为：

```c
void persist_close(void) {
    if (g_aof_fd >= 0) {
        persist_flush_aof_fd(g_aof_fd);
        close(g_aof_fd);
    }
    g_aof_fd = -1;
    persist_uring_close();
}
```

- [ ] **Step 11: 简化 persist_init()**

将 [kvs_persist.c:564-575](src/persistence/kvs_persist.c#L564-L575) 替换为：

```c
int persist_init(void) {
    if (g_aof_disabled) {
        g_aof_fd = -1;
        return 0;
    }
    g_aof_fd = open(g_cfg.aof_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) return -1;
    g_aof_write_offset = lseek(g_aof_fd, 0, SEEK_END);
    if (g_aof_write_offset < 0) g_aof_write_offset = 0;
    return 0;
}
```

- [ ] **Step 12: 简化 persist_recover()**

将 [kvs_persist.c:688-689](src/persistence/kvs_persist.c#L688-L689) 两行：

```c
    g_aof_last_flush_ms = g_last_snapshot_ms;
    g_aof_dirty = 0;
```

替换为：删除这两行（不再需要）。

- [ ] **Step 13: 简化 finalize_rewrite_parent()**

将 [kvs_persist.c:559-560](src/persistence/kvs_persist.c#L559-L560) 两行：

```c
    g_aof_dirty = 0;
    g_aof_last_flush_ms = kvs_now_ms();
```

替换为：删除这两行（不再需要）。

- [ ] **Step 14: 编译验证**

```bash
make -j$(nproc) 2>&1 | head -50
```

预期：编译成功，无警告（`-Wall -Wextra`）。

- [ ] **Step 15: 提交**

```bash
git add src/persistence/kvs_persist.c
git commit -m "refactor: per-command io_uring write+fsync for ALWAYS, remove buffer and EVERYSEC"
```

---

### Task 3: 更新 kvstore.c CLI/配置/命令解析

**Files:**
- Modify: `src/main/kvstore.c`

**Interfaces:**
- Consumes: `KVS_AOF_FSYNC_OFF`, `KVS_AOF_FSYNC_ALWAYS` (from Task 1)

- [ ] **Step 1: 简化 parse_appendfsync_policy() — 去掉 everysec，增加 off**

将 [kvstore.c:150-161](src/main/kvstore.c#L150-L161) 替换为：

```c
static int parse_appendfsync_policy(const char *s, kvs_aof_fsync_policy_t *out) {
    if (!s || !out) return -1;
    if (!strcasecmp(s, "always")) {
        *out = KVS_AOF_FSYNC_ALWAYS;
        return 0;
    }
    if (!strcasecmp(s, "off")) {
        *out = KVS_AOF_FSYNC_OFF;
        return 0;
    }
    return -1;
}
```

- [ ] **Step 2: 更新 help 文本中的 appendfsync 说明**

将 [kvstore.c:2307](src/main/kvstore.c#L2307) 这一行：

```c
                "  --appendfsync always|everysec  AOF fsync 策略 (默认 always)\n"
```

替换为：

```c
                "  --appendfsync always|off  AOF fsync 策略 (默认 always)\n"
```

- [ ] **Step 3: 编译验证**

```bash
make -j$(nproc) 2>&1
```

预期：编译成功。

- [ ] **Step 4: 提交**

```bash
git add src/main/kvstore.c
git commit -m "refactor: simplify appendfsync parsing to always|off, remove everysec"
```

---

### Task 4: 更新 reactor.c — 去掉 persist_flush_pending 调用

**Files:**
- Modify: `src/core/reactor.c:292`

- [ ] **Step 1: 删除 persist_flush_pending() 调用**

删除 [reactor.c:291-292](src/core/reactor.c#L291-L292) 这两行：

```c
        /* flush pending AOF buffered data */
        persist_flush_pending();
```

- [ ] **Step 2: 编译验证**

```bash
make -j$(nproc) 2>&1
```

预期：编译成功。

- [ ] **Step 3: 提交**

```bash
git add src/core/reactor.c
git commit -m "refactor: remove persist_flush_pending call from reactor loop"
```

---

### Task 5: 更新 kvstore.conf 配置示例

**Files:**
- Modify: `kvstore.conf`

- [ ] **Step 1: 更新注释**

将 [kvstore.conf:25](kvstore.conf#L25) 这一行：

```
# AOF fsync 策略: always / everysec
```

替换为：

```
# AOF fsync 策略: always / off
```

- [ ] **Step 2: 提交**

```bash
git add kvstore.conf
git commit -m "docs: update kvstore.conf appendfsync comment to always|off"
```

---

## 验证步骤（全部 Task 完成后）

```bash
# 1. 完整构建
make -j$(nproc) clean && make -j$(nproc)

# 2. 启动 kvstore，strace 确认 fsync 行为
./kvstore --appendfsync always &
sleep 1
# 用 redis-cli 发一条 SET
redis-cli -p 5160 SET testkey testval
# 检查 strace 输出确认每次 SET 都有 fdatasync

# 3. 测试 AOF 关闭模式
./kvstore --appendfsync off &
sleep 1
redis-cli -p 5160 SET testkey testval
# 确认不写 AOF 文件，或 AOF 文件大小不变

# 4. 检查 AOF 文件完整性
cat kvstore.aof
```
