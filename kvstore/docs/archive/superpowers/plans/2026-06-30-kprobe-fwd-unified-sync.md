# Kprobe Fwd 统一增量同步 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将增量同步从双路并行（repl_broadcast + kprobe fwd）改为 kprobe fwd 为主、repl_broadcast 为降级保底的互斥模式，共用 `c->fd`，消除重复传输。

**Architecture:** 拆分 `repl_broadcast` 的三项职责（发送/offset/backlog），offset 和 backlog 维护移到 `handle_parsed_command`；kprobe fwd 直接写 `c->fd` 替代 `g_kprobe_fwd_fd`；健康检查改为 per-slave + master 侧自检 + 空闲边界恢复。

**Tech Stack:** C, libbpf, TCP sockets, pthread

## Global Constraints

- 内核 6.1.176（已验证 `bpf_probe_read_user` 可靠）
- `ENABLE_KPROBE_RDMA=1` 编译
- 保持现有代码风格、错误处理模式
- 不影响 TCP-only 模式（`repl_realtime_transport=tcp`）的行为
- 每步可编译、可独立验证

## File Structure

```
include/kvstore/kvstore.h              — conn_t 新增 fwd_healthy, fwd_last_active
include/kvstore/replication/repl_kprobe.h — 删除 3 个声明 + KVS_KPROBE_FWD_PORT_OFFSET
src/main/kvstore.c                     — 全局变量, repl_broadcast, handle_parsed_command, queue_snapshot
src/replication/kvs_repl_kprobe.c      — 健康检查, ringbuf callback, 删除 4 个函数
```

---

### Task 1: conn_t 新增字段 + g_last_write_ts 声明

**Files:**
- Modify: `include/kvstore/kvstore.h:269`
- Modify: `include/kvstore/kvstore.h` (near line 450, add extern)

**Interfaces:**
- Produces: `conn_t.fwd_healthy` (int), `conn_t.fwd_last_active` (time_t), `extern volatile time_t g_last_write_ts`

- [ ] **Step 1: 在 conn_t 尾部添加两个字段**

在 `include/kvstore/kvstore.h` 的 `conn_t` struct，`next_replica` 之前添加：

```c
    int fwd_healthy;                   /* kprobe fwd 对此 slave 健康 (1) 或降级 (0) */
    time_t fwd_last_active;            /* 最后成功转发时间戳 */
```

插入位置：`out_ring_len` (line 268) 之后、`next_replica` (line 269) 之前。

- [ ] **Step 2: 添加 g_last_write_ts extern 声明**

在 `include/kvstore/kvstore.h` 中，`repl_master_offset()` 声明附近（line 450）添加：

```c
extern volatile time_t g_last_write_ts;  /* 最后一次写命令时间戳 */
```

- [ ] **Step 3: 编译验证**

```bash
make kvstore 2>&1 | tail -5
```

Expected: 编译成功，无 error。可能有 unused field warning（正常，Task 2-3 开始使用）。

- [ ] **Step 4: 提交**

```bash
git add include/kvstore/kvstore.h
git commit -m "feat: add conn_t.fwd_healthy, fwd_last_active; declare g_last_write_ts"
```

---

### Task 2: 全局变量替换

**Files:**
- Modify: `src/main/kvstore.c:76-77`
- Modify: `src/replication/kvs_repl_kprobe.c:45-51`

**Interfaces:**
- Consumes: (none — just vars)
- Produces: `g_last_write_ts` (replaces `g_last_broadcast_time`), removes `g_repl_broadcast_suppressed`, `g_fwd_healthy`, `g_fwd_last_active`, `g_kprobe_fwd_fd`, `KVS_KPROBE_FWD_PORT_OFFSET`

- [ ] **Step 1: 替换 kvstore.c 的全局变量**

在 `src/main/kvstore.c` line 75-77，替换：

```c
/* ─旧代码─ (删除) ────────────────
int g_repl_capture_slave_fd = -1;
volatile int g_repl_broadcast_suppressed = 0;
volatile time_t g_last_broadcast_time = 0;
──────────────────────────────── */

/* ─新代码─ */
int g_repl_capture_slave_fd = -1;
volatile time_t g_last_write_ts = 0;   /* 最后一次写命令时间戳，健康检查用 */
```

- [ ] **Step 2: 删除 kvs_repl_kprobe.c 的全局变量**

在 `src/replication/kvs_repl_kprobe.c` line 45-51，删除：

```c
/* ─删除以下全部─ */
volatile time_t g_fwd_last_active = 0;
volatile int g_fwd_healthy = 0;
extern volatile int g_repl_broadcast_suppressed;
#define KVS_KPROBE_FWD_PORT_OFFSET  13
static int g_kprobe_fwd_fd = -1;
```

- [ ] **Step 3: 编译验证**

```bash
make kvstore 2>&1
```

Expected: 编译报错 — 因为 `g_repl_broadcast_suppressed`、`g_fwd_healthy`、`g_fwd_last_active`、`g_last_broadcast_time`、`g_kprobe_fwd_fd` 在其他地方被引用。记录报错位置，供后续任务修复。

- [ ] **Step 4: 提交**

```bash
git add src/main/kvstore.c src/replication/kvs_repl_kprobe.c
git commit -m "refactor: replace g_last_broadcast_time with g_last_write_ts; remove global fwd vars"
```

---

### Task 3: 解耦 offset/backlog — handle_parsed_command

**Files:**
- Modify: `src/main/kvstore.c:1791-1798`

**Interfaces:**
- Consumes: `g_last_write_ts` (from Task 2), `repl_backlog_feed`, `repl_note_broadcast` (existing in kvs_repl.c)
- Produces: offset/backlog 更新从 repl_broadcast 独立出来

- [ ] **Step 1: 修改写命令分支**

在 `src/main/kvstore.c` line 1791-1798，将：

```c
        if (!from_replication && is_write_cmd(cmd)) {
            persist_note_write();
            if (persist_append_raw(raw, rawlen) != 0) {
                n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                goto out;
            }
            if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
        }
```

改为：

```c
        if (!from_replication && is_write_cmd(cmd)) {
            persist_note_write();
            if (persist_append_raw(raw, rawlen) != 0) {
                n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                goto out;
            }
            if (g_cfg.role == ROLE_MASTER) {
                g_last_write_ts = time(NULL);
                repl_backlog_feed(raw, rawlen);
                repl_note_broadcast(rawlen);
                repl_broadcast(raw, rawlen);   /* 内部判断 per-slave fwd_healthy */
            }
        }
```

- [ ] **Step 2: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error|warning'
```

Expected: `repl_broadcast` 内部仍引用 `g_repl_broadcast_suppressed` 和 `g_last_broadcast_time` — 报 error。下一步修复。

- [ ] **Step 3: 提交**

```bash
git add src/main/kvstore.c
git commit -m "refactor: decouple offset/backlog from repl_broadcast in handle_parsed_command"
```

---

### Task 4: 修改 repl_broadcast — 只对 unhealthy slave 发送

**Files:**
- Modify: `src/main/kvstore.c:482-521`

**Interfaces:**
- Consumes: `conn_t.fwd_healthy` (from Task 1)
- Produces: `repl_broadcast` 跳过 `fwd_healthy=1` 的 slave

- [ ] **Step 1: 重写 repl_broadcast 函数**

在 `src/main/kvstore.c` line 482，将整个 `repl_broadcast` 函数体替换为：

```c
void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    repl_note_send_context("broadcast", rawlen, repl_master_offset(), raw);

    pthread_mutex_lock(&g_repl_lock);
    conn_t **pp = &g_replicas;
    while (*pp) {
        conn_t *c = *pp;
        if (c->repl_draining) {
            *pp = c->next_replica;
            c->next_replica = NULL;
            c->is_replica = 0;
            continue;
        }
        if (c->repl_fullsync_pending) {
            pp = &c->next_replica;
            continue;
        }
        if (g_repl_fullsync_in_progress) {
            pp = &c->next_replica;
            continue;
        }
        /* kprobe fwd 健康的 slave 跳过 — 数据已由 ringbuf callback 转发 */
        if (c->fwd_healthy) {
            pp = &c->next_replica;
            continue;
        }
        if (repl_realtime_send(c, raw, rawlen) != 0) {
            if (repl_handle_replica_send_failure(c, pp)) continue;
            pp = &c->next_replica;
            continue;
        }
        c->repl_offset_sent = repl_master_offset();
        c->repl_last_send_ms = kvs_now_ms();
        pp = &c->next_replica;
    }
    pthread_mutex_unlock(&g_repl_lock);
}
```

**核心变化：**
- 删除 `g_repl_broadcast_suppressed` 检查（line 484）
- 删除 `g_last_broadcast_time = time(NULL)` 赋值（line 486）
- 删除 `repl_backlog_feed(raw, rawlen)` 调用（line 489）— 已移到 handle_parsed_command
- 新增 `if (c->fwd_healthy) continue;` — 跳过健康 slave

- [ ] **Step 2: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error'
```

Expected: 编译成功。如果 `g_last_broadcast_time` 在其他文件还有引用，会在链接时报 undefined reference — 下一步修复。

- [ ] **Step 3: 提交**

```bash
git add src/main/kvstore.c
git commit -m "refactor: repl_broadcast only sends to unhealthy slaves"
```

---

### Task 5: 修改 queue_snapshot — 删除独立连接

**Files:**
- Modify: `src/main/kvstore.c:628-647` (queue_snapshot 的 kprobe fwd 初始化段)

**Interfaces:**
- Consumes: `conn_t.fwd_healthy`, `conn_t.fwd_last_active` (Task 1), `repl_client_capture_flush_to_slave` (existing)
- Produces: queue_snapshot 中不再建立 port+13 连接

- [ ] **Step 1: 替换 kprobe fwd 初始化逻辑**

在 `src/main/kvstore.c` line 628-647（`g_repl_fullsync_in_progress = 0` 之后），将：

```c
    /* 启用 kprobe 异步转发探索模式... */
    extern volatile int g_repl_broadcast_suppressed;
    extern volatile time_t g_fwd_last_active;
    extern volatile int g_fwd_healthy;
    int kprobe_fwd_fd = repl_kprobe_fwd_connect_from_replica(c, g_cfg.port);
    if (kprobe_fwd_fd < 0) {
        g_fwd_healthy = 0;
        fprintf(stderr, "kprobe fwd: connect failed, skipping probe mode\n");
    } else {
        g_repl_broadcast_suppressed = 0;
        g_fwd_last_active = time(NULL);
        g_fwd_healthy = 1;
        fprintf(stderr, "kprobe fwd: probe mode (dual-path until proven healthy)\n");
    }
```

改为：

```c
    /* 全量同步完成后，kprobe fwd 作为主路径（共享 c->fd，不再建立独立连接） */
    int fwd_ok = 0;
    int cache_flushed = repl_client_capture_flush_to_slave(c);
    if (cache_flushed >= 0) {
        c->fwd_healthy = 1;
        c->fwd_last_active = time(NULL);
        fwd_ok = 1;
    } else {
        c->fwd_healthy = 0;
        fprintf(stderr, "kprobe fwd: cache flush failed, falling back to repl_broadcast\n");
    }
```

> **注意:** `cache_flushed` 变量声明需要调整。当前代码中 `int cache_flushed` 在 line 656 声明，需要提前到此处；同时原来的 `extern volatile int g_repl_broadcast_suppressed` 等 extern 声明全部删除。

**同步修改 (line 656):** `int cache_flushed = repl_client_capture_flush_to_slave(c);` 改为 `/* cache_flushed already set above */`，因为 flush 已经在前面完成。

**同步修改 (line 639):** 删除 `extern volatile int g_fwd_healthy;` 等引用。

- [ ] **Step 2: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error'
```

Expected: 编译成功。

- [ ] **Step 3: 提交**

```bash
git add src/main/kvstore.c
git commit -m "refactor: queue_snapshot sets per-slave fwd_healthy after flush, removes port+13 connect"
```

---

### Task 6: 重写健康检查

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c:1137-1155`

**Interfaces:**
- Consumes: `conn_t.fwd_healthy`, `conn_t.fwd_last_active`, `g_last_write_ts`
- Produces: `repl_kprobe_fwd_health_check` — 新逻辑

- [ ] **Step 1: 重写健康检查函数**

将 `repl_kprobe_fwd_health_check` (line 1137-1155) 替换为：

```c
void repl_kprobe_fwd_health_check(void) {
    time_t now = time(NULL);

    /* 1. 全局检测: BPF client_capture 是否还在运行 */
    if (repl_client_capture_get_stats(NULL, NULL, NULL, NULL, NULL) == 0) {
        /* BPF 未激活或无数据 — 可能是未加载，不判故障 */
        /* 仅当所有 slave 都依赖 kprobe fwd 且 BPF 掉了才标记 */
        /* 注: 精确的 BPF detach 检测后续通过直接检查 kprobe fd 实现 */
    }

    /* 2. 无写流量 — 不判故障，检查恢复条件 */
    if (now - g_last_write_ts > KVS_KPROBE_FWD_HEALTH_TIMEOUT) {
        pthread_mutex_lock(&g_repl_lock);
        for (conn_t *c = g_replicas; c; c = c->next_replica) {
            if (!c->fwd_healthy && !c->repl_draining
                && !c->repl_fullsync_pending) {
                c->fwd_healthy = 1;
                c->fwd_last_active = now;
                fprintf(stderr, "kprobe fwd: recovered slave fd=%d "
                        "(idle window)\n", c->fd);
            }
        }
        pthread_mutex_unlock(&g_repl_lock);
        return;
    }

    /* 3. 有写流量 — 检查 per-slave 转发活跃度 */
    pthread_mutex_lock(&g_repl_lock);
    for (conn_t *c = g_replicas; c; c = c->next_replica) {
        if (!c->fwd_healthy) continue;
        if (c->repl_draining || c->repl_fullsync_pending) continue;
        if (now - c->fwd_last_active > KVS_KPROBE_FWD_HEALTH_TIMEOUT) {
            c->fwd_healthy = 0;
            fprintf(stderr, "kprobe fwd: slave fd=%d unhealthy "
                    "(fwd_last_active=%lds_ago), fallback to repl_broadcast\n",
                    c->fd, (long)(now - c->fwd_last_active));
        }
    }
    pthread_mutex_unlock(&g_repl_lock);
}
```

> **后续优化:** ringbuf RB_ERR 计数器检测 — `g_client_stats_fd`（已在同文件 line 1285 声明为 `static`）可直接读取 key=2 获取 RB_ERR 值，检测到增长即触发全局降级。当前 per-slave 转发活跃度检测已覆盖核心故障场景（有写流量时 kprobe 不转发），ringbuf 溢出会导致 `fwd_last_active` 不更新从而自动降级。

- [ ] **Step 2: 确保 g_repl_lock 可见**

检查 `kvs_repl_kprobe.c` 顶部是否有 `extern pthread_mutex_t g_repl_lock;`。如果没有，在 extern 声明区域添加：

```c
extern pthread_mutex_t g_repl_lock;  /* 定义在 kvstore.c */
```

- [ ] **Step 3: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error'
```

Expected: 编译成功。

- [ ] **Step 4: 提交**

```bash
git add src/replication/kvs_repl_kprobe.c
git commit -m "feat: rewrite health check — per-slave, master-side self-check, idle-boundary recovery"
```

---

### Task 7: 修改 ringbuf callback — 遍历 slave 发送

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c:1364-1422` (client_ringbuf_cb)

**Interfaces:**
- Consumes: `conn_t.fwd_healthy`, `conn_t.fd`
- Produces: ringbuf callback 直接写 `c->fd`，不依赖全局 `g_kprobe_fwd_fd`

- [ ] **Step 1: 提取 is_repl_control 内联函数**

在 `client_ringbuf_cb` 之前添加：

```c
/* 过滤复制控制命令（REPLSYNC/REPLACK），防止 slave→master 的数据被转发回 slave */
static inline int is_repl_control(const unsigned char *data, size_t len) {
    if (len < 7) return 0;
    size_t scan = len < 128 ? len : 128;
    for (size_t i = 0; i + 8 <= scan; i++) {
        if ((memcmp(data + i, "REPLSYNC", 8) == 0) ||
            (memcmp(data + i, "REPLACK", 7) == 0))
            return 1;
    }
    return 0;
}
```

- [ ] **Step 2: 修改 STATE_INCR 分支**

将 `client_ringbuf_cb` 中 STATE_INCR 分支（line 1417-1421）：

```c
    /* STATE_INCR: 增量同步 — kprobe 异步转发为主路径，repl_broadcast 为保底 */
    if (g_fwd_healthy) {
        if (forward_to_slave(payload, payload_len) == 0)
            g_fwd_last_active = time(NULL);  /* 更新心跳 */
    }
```

改为：

```c
    /* STATE_INCR: 增量同步 — kprobe fwd 遍历所有健康 slave 直接写 c->fd */
    if (!is_repl_control(payload, payload_len)) {
        pthread_mutex_lock(&g_repl_lock);
        for (conn_t *c = g_replicas; c; c = c->next_replica) {
            if (c->repl_draining || c->repl_fullsync_pending) continue;
            if (!c->fwd_healthy) continue;

            size_t total_sent = 0;
            int err = 0;
            while (total_sent < payload_len) {
                ssize_t n = send(c->fd, payload + total_sent,
                                 payload_len - total_sent,
                                 MSG_NOSIGNAL);
                if (n <= 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        usleep(1000);
                        if (++err > 100) break;  /* 卡住则跳过，等健康检查降级 */
                        continue;
                    }
                    break;
                }
                total_sent += (size_t)n;
                err = 0;
            }
            if (total_sent == payload_len) {
                c->fwd_last_active = time(NULL);
            }
        }
        pthread_mutex_unlock(&g_repl_lock);
    }
```

- [ ] **Step 3: 确保 g_repl_lock 可见**

在 `kvs_repl_kprobe.c` 顶部 extern 声明区域添加（如不存在）：

```c
extern pthread_mutex_t g_repl_lock;  /* 定义在 kvstore.c */
```

- [ ] **Step 4: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error'
```

Expected: 编译成功。

- [ ] **Step 5: 提交**

```bash
git add src/replication/kvs_repl_kprobe.c
git commit -m "refactor: ringbuf callback sends to all healthy slaves via c->fd"
```

---

### Task 8: 删除 4 个废弃函数

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c`

**Interfaces:**
- Consumes: (none — just deletions)
- Produces: 移除 dead code

删除以下函数体（保留文件内其他代码不变）：

- [ ] **Step 1: 删除 forward_to_slave (line 1300-1361)**

删除 `forward_to_slave` 函数整个定义体，包括：
- `static int g_cache_l2_fd = -1;` 等静态变量（如果 `forward_to_slave` 是唯一使用者则删除，但注意 `g_cache_l2_fd` 等被 L2 缓存逻辑共用 — **只删 `forward_to_slave` 函数体，保留 L2 缓存变量**）

检查: `forward_to_slave` 使用的 `g_cache_l2_fd`, `g_cache_l2_path`, `g_cache_l2_bytes` 是 L2 缓存逻辑的一部分（被 `cache_spill_to_l2` 和 `client_ringbuf_cb` 使用），不能删除。

```c
/* 删除 forward_to_slave 函数 (line 1300-1361) — 逻辑已并入 client_ringbuf_cb */
```

- [ ] **Step 2: 删除 repl_kprobe_fwd_connect_from_replica (line 1165-1194)**

删除整个函数定义 + 其前的注释块 (line 1157-1194)。

- [ ] **Step 3: 删除 kprobe_fwd_slave_thread (line 1197-1243)**

删除整个函数定义（包括注释）。

- [ ] **Step 4: 删除 repl_kprobe_fwd_slave_init (line 1246-1270)**

删除整个函数定义（包括注释）。

- [ ] **Step 5: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error'
```

Expected: 编译成功。如果有 linker error 说 `repl_kprobe_fwd_connect_from_replica` undefined — 说明 `queue_snapshot` (已在 Task 5 移除调用) 或其他地方还有引用，排查后删除。

- [ ] **Step 6: 提交**

```bash
git add src/replication/kvs_repl_kprobe.c
git commit -m "refactor: remove forward_to_slave, repl_kprobe_fwd_connect_from_replica, kprobe_fwd_slave_thread, repl_kprobe_fwd_slave_init"
```

---

### Task 9: 清理 repl_kprobe.h

**Files:**
- Modify: `include/kvstore/replication/repl_kprobe.h:49-51, 60-62`

**Interfaces:**
- Consumes: (none)
- Produces: 移除不再需要的公开声明

- [ ] **Step 1: 删除 KVS_KPROBE_FWD_PORT_OFFSET**

在 `repl_kprobe.h` line 49-51，删除：

```c
/* 删除以下 3 行:
#define KVS_KPROBE_FWD_PORT_OFFSET  13
*/
```

- [ ] **Step 2: 删除 kprobe fwd 函数声明**

在 `repl_kprobe.h` line 60-62，删除：

```c
/* 删除以下 3 行:
int repl_kprobe_fwd_connect_from_replica(conn_t *c, int slave_port);
int repl_kprobe_fwd_slave_init(int base_port);
*/
```

保留 `void repl_kprobe_fwd_health_check(void);` — 仍在使用。

- [ ] **Step 3: 编译验证**

```bash
make kvstore 2>&1 | grep -E 'error'
```

Expected: 编译成功。

- [ ] **Step 4: 提交**

```bash
git add include/kvstore/replication/repl_kprobe.h
git commit -m "refactor: remove KVS_KPROBE_FWD_PORT_OFFSET and unused kprobe fwd declarations"
```

---

### Task 10: 全量构建 + 基本功能验证

**Files:**
- (无修改，纯验证)

- [ ] **Step 1: 全量 clean build**

```bash
make clean && make kvstore 2>&1 | tail -10
```

Expected: 编译零 error、零 warning（接受 pre-existing warnings）。

- [ ] **Step 2: 构建测试程序**

```bash
make test_repl_5w5w test_repl_basic test_repl_gap 2>&1 | tail -5
```

Expected: 全部编译成功。

- [ ] **Step 3: 启动 master（TCP 模式，不需 root）快速验证**

```bash
# 终端 1
./kvstore --port 9960 --role master \
    --repl-fullsync-transport tcp --repl-realtime-transport tcp &
MASTER_PID=$!
sleep 1

# 终端 2
./kvstore --port 9961 --role slave \
    --master-host 127.0.0.1 --master-port 9960 \
    --repl-fullsync-transport tcp --repl-realtime-transport tcp &
SLAVE_PID=$!
sleep 2

# 发送测试数据
echo -e '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc -w1 127.0.0.1 9960

# 验证 slave 数据
echo -e '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc -w1 127.0.0.1 9961

# 清理
kill $MASTER_PID $SLAVE_PID 2>/dev/null
wait 2>/dev/null
```

Expected: slave 返回 `$3\r\nbar\r\n`。

> **注意:** TCP 模式下 `fwd_healthy=0`（初始值），增量同步走 repl_broadcast 降级路径，行为应与改动前一致。

- [ ] **Step 4: 提交**

```bash
git commit -m "verify: full build passes; TCP mode basic replication works" --allow-empty
```

如果任何步骤失败，修复后重新验证再提交。

---

### Task 11: 清理过期注释

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c:1307-1309`

- [ ] **Step 1: 更新 bpf_probe_read_user 注释**

在 `kvs_repl_kprobe.c` line 1307-1309（`forward_to_slave` 删除后此处为空或成为 `is_repl_control` 附近），将：

```c
/* ── eBPF+tcp 新路径: INCR 模式直接转发到 slave ──
 * 当前禁用: bpf_probe_read_user 在内核 5.15 上可靠性不足，
 * 增量数据改由 repl_broadcast (TCP) 可靠发送。保留此函数供未来修复后启用。 */
```

改为：

```c
/* ── eBPF+tcp 增量路径: kprobe 捕获 → ringbuf → 直接转发到 slave ──
 * bpf_probe_read_user 在 kernel 6.1+ 已验证可靠 (见 tests/verify_bpf_read_user.c) */
```

- [ ] **Step 2: 编译验证 + 提交**

```bash
make kvstore 2>&1 | tail -3
git add src/replication/kvs_repl_kprobe.c
git commit -m "docs: update bpf_probe_read_user comment — verified reliable on 6.1"
```

---

## 测试回归清单

实现全部 task 后，需要验证：

1. **TCP 模式增量同步** — `./test_repl_basic` 通过
2. **eBPF+tcp 模式增量同步** — `sudo ./test_repl_5w5w` 通过（需双 VM/root）
3. **repl_broadcast 降级路径** — 删除 BPF object 后 slave 数据仍一致
4. **CONTINUE (partial resync)** — `./test_repl_gap` 通过
5. **AOF 持久化** — benchmark 数据前后一致
