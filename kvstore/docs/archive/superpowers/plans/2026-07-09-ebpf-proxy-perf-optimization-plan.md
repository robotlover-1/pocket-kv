# ebpf-proxy 性能优化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 ebpf-proxy 转发 QPS 与 sync 转发的差距从 ~18% 缩小到 ≤5%

**Architecture:** 对 BPF 捕获 → ringbuf 回调 → TCP 发送全链路做 5 项独立优化，每项都是小范围改动。正常转发路径优化为 `ringbuf event → send_full → return`，无 malloc/cache/日志/循环扫描。

**Tech Stack:** C (GCC), BPF C (Clang), libbpf

## Global Constraints

- 不改变 AOF always 语义（每条命令立即完整转发）
- 不改动缓存数据结构
- 不在正常路径引入 malloc/日志/扫描
- 保持现有代码风格

---

## 文件结构

| 文件 | 改动类型 | 职责 |
|------|----------|------|
| `src/replication/bpf/repl_client_capture.bpf.c` | Modify | BPF kprobe 捕获，ITER_IOVEC 按 retval 裁剪 |
| `src/ebpf_proxy/proxy_slave.c` | Modify | slave 连接，SO_SNDBUF/SO_RCVBUF |
| `src/ebpf_proxy/main.c` | Modify | ringbuf 回调优化：send_full、REPL 快速判断、poll timeout |

---

### Task 1: BPF ITER_IOVEC 按 retval 裁剪

**Files:**
- Modify: `src/replication/bpf/repl_client_capture.bpf.c:239-241`

**Interfaces:**
- Consumes: `retval` (long, already capped to `CLIENT_ENTRY_MAX_LEN` at line 213-214)
- Produces: `safe_len` (unsigned long long), now additionally capped by retval

- [ ] **Step 1: 在 ITER_IOVEC 分支加 retval 裁剪**

在 `src/replication/bpf/repl_client_capture.bpf.c` 的 `kprobe_client_recv_return()` 中，找到 ITER_IOVEC 分支（line 239-241），在 `vec.l` 赋值和 `CLIENT_ENTRY_MAX_LEN` 裁剪之间加两行：

当前代码：
```c
            if (!vec.b || vec.l == 0) return 0;
            unsigned long long safe_len = vec.l;
            if (safe_len > (unsigned long long)CLIENT_ENTRY_MAX_LEN)
                safe_len = (unsigned long long)CLIENT_ENTRY_MAX_LEN;
```

修改为：
```c
            if (!vec.b || vec.l == 0) return 0;
            unsigned long long safe_len = vec.l;
            if (safe_len > (unsigned long long)retval)
                safe_len = (unsigned long long)retval;
            if (safe_len > (unsigned long long)CLIENT_ENTRY_MAX_LEN)
                safe_len = (unsigned long long)CLIENT_ENTRY_MAX_LEN;
```

- [ ] **Step 2: 编译 BPF 对象文件**

```bash
make client_capture_bpf
```
预期：编译成功，无警告。

- [ ] **Step 3: Commit**

```bash
git add src/replication/bpf/repl_client_capture.bpf.c
git commit -m "fix(bpf): cap ITER_IOVEC safe_len by retval to avoid stale data forwarding"
```

---

### Task 2: proxy_slave 设置 SO_SNDBUF/SO_RCVBUF

**Files:**
- Modify: `src/ebpf_proxy/proxy_slave.c:38-41`

**Interfaces:**
- Consumes: `ctx->fd` (int, valid socket fd from `socket()`)
- Produces: 无新增接口，只影响 `ctx->fd` 的内核 socket buffer 大小

- [ ] **Step 1: 在 socket() 之后、connect() 之前加 buffer 设置**

在 `src/ebpf_proxy/proxy_slave.c` 的 `proxy_slave_connect()` 中，TCP_NODELAY 设置之前加：

当前代码（line 38-41）：
```c
    tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(ctx->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(ctx->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    { int one = 1; setsockopt(ctx->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }
```

修改为：
```c
    tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(ctx->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(ctx->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    {
        int snd = 1024 * 1024;
        int rcv = 1024 * 1024;
        setsockopt(ctx->fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
        setsockopt(ctx->fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
    }
    { int one = 1; setsockopt(ctx->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }
```

- [ ] **Step 2: 编译 ebpf-proxy**

```bash
make ebpf-proxy
```
预期：编译成功，无警告。

- [ ] **Step 3: Commit**

```bash
git add src/ebpf_proxy/proxy_slave.c
git commit -m "perf(ebpf-proxy): set SO_SNDBUF/SO_RCVBUF to 1MB on slave socket"
```

---

### Task 3: main.c — send_full、REPL 快速判断、poll timeout

**Files:**
- Modify: `src/ebpf_proxy/main.c:108-166`（ringbuf_callback）, `main.c:171`（poll timeout）, `main.c:208`（poll timeout in reconnect）

**Interfaces:**
- Consumes: `proxy_slave_fd()`, `proxy_slave_is_connected()`, `cache_append()` — 均为现有接口
- Produces:
  - `static int proxy_send_full(int fd, const unsigned char *buf, size_t len)` — 新函数，阻塞完整发送
  - `static int is_repl_control_payload(const unsigned char *payload, size_t plen)` — 新函数，快速判断控制命令

- [ ] **Step 1: 添加 `proxy_send_full()` 函数**

在 `signal_handler()` 之后（line 53 之后）、`open_pinned_map()` 之前（line 56 之前）插入：

```c
/* 完整发送，处理 EINTR 和 partial send。
 * 正常路径返回 0，失败返回 -1。 */
static int proxy_send_full(int fd, const unsigned char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(fd, buf + off, len - off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}
```

- [ ] **Step 2: 添加 `is_repl_control_payload()` 函数**

在 `proxy_send_full()` 之后插入：

```c
/* 快速判断 payload 是否为复制控制命令（REPLSYNC/REPLACK/REPLDONE）。
 * 正常 RESP 命令以 '*'/'$'/':'/'+'/'-' 开头，首字节快速拒绝。 */
static int is_repl_control_payload(const unsigned char *payload, size_t plen) {
    if (plen < 7 || payload[0] != 'R')
        return 0;

    if (plen >= 8 && memcmp(payload, "REPLSYNC", 8) == 0)
        return 1;
    if (memcmp(payload, "REPLACK", 7) == 0)
        return 1;
    if (plen >= 8 && memcmp(payload, "REPLDONE", 8) == 0)
        return 1;

    return 0;
}
```

- [ ] **Step 3: 替换 REPL 控制命令扫描循环**

在 `ringbuf_callback()` 中，删除 line 130-139 的 for 循环：

```c
    /* 过滤 slave→master 复制控制命令，防止回传 slave */
    if (plen >= 7) {
        size_t scan = plen < 128 ? plen : 128;
        for (size_t i = 0; i + 7 <= scan; i++) {
            if ((scan - i >= 8 && memcmp(payload + i, "REPLSYNC", 8) == 0) ||
                memcmp(payload + i, "REPLACK", 7) == 0 ||
                (scan - i >= 8 && memcmp(payload + i, "REPLDONE", 8) == 0)) {
                return 0;  /* 静默丢弃控制命令 */
            }
        }
    }
```

替换为：

```c
    /* 过滤 slave→master 复制控制命令，防止回传 slave */
    if (is_repl_control_payload(payload, plen)) {
        return 0;
    }
```

- [ ] **Step 4: 替换发送逻辑为 send_full**

在 `ringbuf_callback()` 中，删除 line 141-165：

```c
    if (g_state == STATE_FORWARDING) {
        if (proxy_slave_is_connected(&g_slave)) {
            ssize_t n = send(proxy_slave_fd(&g_slave), payload, plen,
                             MSG_NOSIGNAL);
            if (n != (ssize_t)plen) {
                if (n < 0) {
                    fprintf(stderr, "ebpf-proxy: send to slave fd=%d failed "
                            "(errno=%d: %s), buffering\n",
                            proxy_slave_fd(&g_slave), errno, strerror(errno));
                } else {
                    fprintf(stderr, "ebpf-proxy: partial send %zd/%zu, "
                            "buffering remainder\n", n, plen);
                }
                cache_append(&g_cache, payload, plen);
                /* 不切换状态 — 下次 ringbuf 事件继续尝试发送 */
            }
        } else {
            /* slave 未连接，缓存 */
            cache_append(&g_cache, payload, plen);
        }
    } else {
        /* BUFFERING 状态 */
        cache_append(&g_cache, payload, plen);
    }
```

替换为：

```c
    if (g_state == STATE_FORWARDING) {
        if (proxy_slave_is_connected(&g_slave)) {
            if (proxy_send_full(proxy_slave_fd(&g_slave), payload, plen) != 0) {
                cache_append(&g_cache, payload, plen);
            }
        } else {
            cache_append(&g_cache, payload, plen);
        }
    } else {
        cache_append(&g_cache, payload, plen);
    }
```

- [ ] **Step 5: 缩短 ring_buffer__poll timeout**

第一处 — `main_loop()` 主循环，line 171:

```c
// 前
int rc = ring_buffer__poll(g_rb, 100 /* ms */);
// 后
int rc = ring_buffer__poll(g_rb, 5 /* ms */);
```

第二处 — 重连退避期间的 poll，line 208:

```c
// 前
ring_buffer__poll(g_rb, 100);
// 后
ring_buffer__poll(g_rb, 5);
```

- [ ] **Step 6: 编译 ebpf-proxy**

```bash
make ebpf-proxy
```
预期：编译成功，无警告。

- [ ] **Step 7: Commit**

```bash
git add src/ebpf_proxy/main.c
git commit -m "perf(ebpf-proxy): send_full, fast REPL check, 5ms poll timeout"
```

---

### Task 4: 全量构建 + 基准测试验证

**Files:**
- Test: `tests/perf/test_ebpf_proxy_qps`（现有测试）

- [ ] **Step 1: 全量构建**

```bash
make client_capture_bpf
make ebpf-proxy
make test_ebpf_proxy_qps
```
预期：三个目标全部编译成功。

- [ ] **Step 2: 运行 QPS 对比测试**

```bash
./tests/perf/test_ebpf_proxy_qps
```
预期输出：
- PROXY mode QPS
- SYNC mode QPS
- 差距 ≤ 5%

- [ ] **Step 3: 验证全量同步流程**

手动确认以下场景：
1. 启动 master + slave
2. 启动 ebpf-proxy，确认连接成功日志
3. 触发全量同步，确认 `REPLSYNC detected, state=BUFFERING` 日志
4. 全量同步完成，确认 `REPLDONE detected, flushing cache...` 和 `state=FORWARDING` 日志
5. 写入几条命令，确认 slave 收到一致数据

- [ ] **Step 4: Commit（如测试结果已确认）**

```bash
# 不需要额外 commit — 改动已在 Task 1-3 中提交
git log --oneline -4
```

---

## 依赖关系

```
Task 1 (BPF retval)  ─┐
Task 2 (SO_SNDBUF)   ─┼── Task 4 (Build + Test)
Task 3 (main.c)      ─┘
```

Task 1-3 互不依赖，可以并行执行。Task 4 依赖全部完成。
