# eBPF+tcp 增量同步 — kprobe 异步转发 实现计划

> **For agentic workers:** Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 增量同步期间 kprobe 截获 `tcp_recvmsg` 数据 → ringbuf → 异步线程 → TCP 转发到 slave，`repl_broadcast` 降级为保底路径。

**Architecture:** 双路径策略 — kprobe 转发（主路径）+ repl_broadcast（保底）。健康检查心跳机制 5s 超时自动切回 repl_broadcast，下一次 REPLDONE 重置。

**Tech Stack:** C, eBPF (libbpf), BPF ringbuf

## Global Constraints

- 不改变 kprobe BPF 程序（`repl_client_capture.bpf.c` 不变）
- 不改变全量同步期间 L1/L2 缓存逻辑
- 不牺牲数据可靠性（kprobe 异常时 repl_broadcast 接管）
- 编译条件：`ENABLE_EBPF=1 ENABLE_KPROBE_RDMA=1`

---

## 文件结构

| 文件 | 角色 | 改动类型 |
|------|------|---------|
| `src/replication/kvs_repl_kprobe.c` | kprobe 用户态：ringbuf 回调、forward、健康检查变量 | 修改 |
| `src/main/kvstore.c` | `repl_broadcast` 抑制逻辑、`queue_snapshot` 状态切换 | 修改 |
| `src/core/reactor.c` | reactor 100ms 定时器增加健康检查调用 | 修改 |

---

### Task 1: kprobe 健康检查和转发变量

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c:34-40`（常量区域后新增）

**Interfaces:**
- Produces: `g_fwd_last_active` (time_t), `g_fwd_healthy` (int)
- Produces: `extern void repl_kprobe_fwd_health_check(void)` — 供 reactor 定时器调用

- [ ] **Step 1: 新增全局变量和声明**

在 `src/replication/kvs_repl_kprobe.c` 的常量定义区域（`#define KVS_KPROBE_RINGBUF_POLL_MS 5` 之后）新增：

```c
/* ---- kprobe 转发健康检查 ---- */
#define KVS_KPROBE_FWD_HEALTH_TIMEOUT 5  /* 5秒无数据判定异常 */

volatile time_t g_fwd_last_active = 0;  /* 最后成功转发时间戳 */
volatile int g_fwd_healthy = 0;          /* 1=健康 0=异常 */
```

在 `extern volatile int g_repl_fullsync_in_progress;` 等 extern 声明区域新增：

```c
/* 供 reactor 定时器调用 */
void repl_kprobe_fwd_health_check(void);
```

在文件末尾（`repl_client_capture_note_repldone` 之后）新增健康检查函数：

```c
void repl_kprobe_fwd_health_check(void) {
    if (!g_fwd_healthy) return;
    if (time(NULL) - g_fwd_last_active > KVS_KPROBE_FWD_HEALTH_TIMEOUT) {
        g_fwd_healthy = 0;
        /* 切回 repl_broadcast */
        extern volatile int g_repl_broadcast_suppressed;
        g_repl_broadcast_suppressed = 0;
        fprintf(stderr, "kprobe fwd: health check FAILED, "
                "fallback to repl_broadcast (last_active=%lds ago)\n",
                (long)(time(NULL) - g_fwd_last_active));
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
make clean && make ENABLE_EBPF=1 ENABLE_RDMA=1 ENABLE_KPROBE_RDMA=1 -j4 2>&1 | tail -5
```

- [ ] **Step 3: Commit**

```bash
git add src/replication/kvs_repl_kprobe.c
git commit -m "feat(kprobe): add forward health check variables and timer callback"
```

---

### Task 2: forward_to_slave 启用 + ringbuf 回调增量转发

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c:1165`（去掉 `__attribute__((unused))`）
- Modify: `src/replication/kvs_repl_kprobe.c:1270-1276`（client_ringbuf_cb 增量分支）

**Interfaces:**
- Consumes: `g_fwd_healthy`, `g_fwd_last_active` (from Task 1)

- [ ] **Step 1: 去掉 forward_to_slave 的 unused 标记**

```c
// 改前:
__attribute__((unused))
static int forward_to_slave(const unsigned char *data, size_t len) {

// 改后:
static int forward_to_slave(const unsigned char *data, size_t len) {
```

- [ ] **Step 2: 增量阶段调用 forward_to_slave**

在 `client_ringbuf_cb` 中，将增量阶段的 `return 0;` 改为转发逻辑：

```c
    /* STATE_INCR: 增量同步 — kprobe 异步转发为主路径，repl_broadcast 为保底 */
    if (g_fwd_healthy) {
        if (forward_to_slave(payload, payload_len) == 0)
            g_fwd_last_active = time(NULL);  /* 更新心跳 */
    }
    return 0;
```

完整上下文（替换原有的 `return 0;` 行）：

```c
    if (g_repl_fullsync_in_progress) {
        /* ═══ STATE_FULL: 全量同步中 → 缓存 ═══ */
        // ... 不变 ...
    }
    /* STATE_INCR: 增量同步 — kprobe 异步转发为主路径，repl_broadcast 为保底 */
    if (g_fwd_healthy) {
        if (forward_to_slave(payload, payload_len) == 0)
            g_fwd_last_active = time(NULL);
    }
    return 0;
}
```

- [ ] **Step 3: 编译验证**

```bash
make clean && make ENABLE_EBPF=1 ENABLE_RDMA=1 ENABLE_KPROBE_RDMA=1 -j4 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
git add src/replication/kvs_repl_kprobe.c
git commit -m "feat(kprobe): enable forward_to_slave in incremental sync phase"
```

---

### Task 3: repl_broadcast 抑制 + queue_snapshot 状态切换

**Files:**
- Modify: `src/main/kvstore.c:70`（新增 `g_repl_broadcast_suppressed` 全局变量）
- Modify: `src/main/kvstore.c:477-478`（repl_broadcast 开头加抑制检查）
- Modify: `src/main/kvstore.c:631`（queue_snapshot 中 REPLDONE 后设置抑制 + 健康标志）

**Interfaces:**
- Consumes: `g_fwd_healthy`, `g_fwd_last_active` (from Task 1, 2)
- Produces: `volatile int g_repl_broadcast_suppressed`

- [ ] **Step 1: 新增全局变量**

在 `src/main/kvstore.c:73`（`g_repl_capture_slave_fd` 之后）新增：

```c
/* kprobe 转发接管增量同步时，压制 repl_broadcast */
volatile int g_repl_broadcast_suppressed = 0;
```

- [ ] **Step 2: repl_broadcast 增加抑制检查**

在 `repl_broadcast` 函数开头（`repl_note_send_context` 之前）新增：

```c
void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    /* kprobe 转发接管时跳过 repl_broadcast（保底路径静默） */
    if (g_repl_broadcast_suppressed) return;

    repl_note_send_context("broadcast", rawlen, repl_master_offset(), raw);
    // ... 原有逻辑 ...
```

- [ ] **Step 3: queue_snapshot 在 REPLDONE 后设置状态**

在 `queue_snapshot` 函数中，`repl_client_capture_set_fullsync(0)` 之后、cache flush 之前，新增 3 行：

找到这段代码（约 line 624-626）：
```c
    g_repl_fullsync_in_progress = 0;
    repl_client_capture_set_fullsync(0);
```

改为：
```c
    g_repl_fullsync_in_progress = 0;
    repl_client_capture_set_fullsync(0);

    /* 启用 kprobe 异步转发接管增量同步 */
    extern volatile int g_repl_broadcast_suppressed;
    extern volatile time_t g_fwd_last_active;
    extern volatile int g_fwd_healthy;
    g_repl_broadcast_suppressed = 1;
    g_fwd_last_active = time(NULL);
    g_fwd_healthy = 1;
    fprintf(stderr, "kprobe fwd: enabled for incremental sync\n");
```

- [ ] **Step 4: 编译验证**

```bash
make clean && make ENABLE_EBPF=1 ENABLE_RDMA=1 ENABLE_KPROBE_RDMA=1 -j4 2>&1 | tail -5
```

- [ ] **Step 5: Commit**

```bash
git add src/main/kvstore.c
git commit -m "feat(kprobe): repl_broadcast suppression + state switch at REPLDONE"
```

---

### Task 4: reactor 定时器增加健康检查

**Files:**
- Modify: `src/core/reactor.c:243`（100ms 定时器区域新增一行）

- [ ] **Step 1: 在 reactor 100ms 定时器中增加健康检查调用**

在 `persist_autosnap_cron()` 之后新增 `repl_kprobe_fwd_health_check()`：

```c
    if (now - g_last_expire >= 100) {
        int budget = expire_cycle_budget();
        kvs_active_expire_cycle(budget);
        persist_autosnap_cron();
#if KVS_ENABLE_KPROBE_RDMA
        extern void repl_kprobe_fwd_health_check(void);
        repl_kprobe_fwd_health_check();
#endif
        g_last_expire = now;
    }
```

- [ ] **Step 2: 编译验证**

```bash
make clean && make ENABLE_EBPF=1 ENABLE_RDMA=1 ENABLE_KPROBE_RDMA=1 -j4 2>&1 | tail -5
```

- [ ] **Step 3: Commit**

```bash
git add src/core/reactor.c
git commit -m "feat(kprobe): add forward health check to reactor timer"
```

---

### Task 5: 端到端验证

- [ ] **Step 1: 编译完整项目**

```bash
make clean && make ENABLE_EBPF=1 ENABLE_RDMA=1 ENABLE_KPROBE_RDMA=1 -j4 2>&1 | tail -5
```

Expected: 无编译错误。

- [ ] **Step 2: 以 TCP+eBPF 模式启动 master（验证 kprobe 加载并转入增量转发）**

```bash
echo '2983372202' | sudo -S ./kvstore kvstore.conf --role master \
    --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
    --aof-disable 2>&1 | head -30
```

Expected: 看到 `client_capture: initialized (pid=...), capture active` 和后续的 `kprobe fwd: enabled for incremental sync`。

- [ ] **Step 3: 启动 slave 并运行 test_repl_5w5w**

```bash
# Terminal 2 (slave):
./kvstore kvstore.conf --role slave --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp

# Terminal 3 (test):
./tests/test_repl_5w5w --config tests/test.conf --poll 500
```

Expected: Phase 5 增量同步完成，Phase 6 数据一致性验证 PASS。注意 master stderr 中是否有 `kprobe fwd: enabled for incremental sync` 以及正常转发期间无 `health check FAILED`。

- [ ] **Step 4: 验证健康检查回退（手动触发）**

在增量同步运行期间，手动杀掉 slave 的连接：

```bash
# 在 slave 上:
sudo pkill -9 kvstore
```

观察 master stderr 输出，应在 ~5 秒内看到：
```
kprobe fwd: health check FAILED, fallback to repl_broadcast (last_active=5s ago)
```

重新启动 slave，验证数据仍然一致（repl_broadcast 保底接管了）。

- [ ] **Step 5: Commit test results**

```bash
git add -A
git commit -m "test(kprobe): verify eBPF+tcp incremental forwarding + health check fallback"
```
