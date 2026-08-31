# ebpf-proxy 性能优化设计：缩小与 sync 转发的 QPS 差距

**日期**: 2026-07-09
**目标**: ebpf-proxy 转发 QPS 比 sync 转发低 18%，目标压缩到 5% 以内

---

## 背景

AOF always 场景下，ebpf-proxy 作为独立进程，通过 kprobe hook `tcp_recvmsg` 捕获 master 接收到的写命令，经 ringbuf 传到用户态再转发给 slave。

当前 eBPF proxy 路径比 sync 路径多 ~18% 开销。分析发现 5 个优化点分布在 BPF 捕获 → ringbuf 回调 → TCP 发送全链路。

---

## 设计方案：全链路瘦身

### 修改总览

| # | 文件 | 改动 | 预期收益 | 风险 |
|---|------|------|----------|------|
| 1 | `repl_client_capture.bpf.c` | ITER_IOVEC 按 retval 裁剪 | 中 | 低 |
| 2 | `main.c` | REPL 扫描改首字节快速判断 | 低-中 | 极低 |
| 3 | `main.c` | send() → send_full() + 修复 partial send 错误缓存 | 中 | 低 |
| 4 | `proxy_slave.c` | SO_SNDBUF/SO_RCVBUF = 1MB | 中-高 | 低 |
| 5 | `main.c` | ring_buffer__poll timeout 100ms → 5ms | 低 | 极低 |

---

### 1. BPF ITER_IOVEC 按 retval 裁剪

**文件**: `src/replication/bpf/repl_client_capture.bpf.c`，`kprobe_client_recv_return()`

**问题**: ITER_IOVEC 分支使用 `vec.l`（iovec 的 `iov_len`，即缓冲区容量）作为数据长度，未按 `tcp_recvmsg` 实际返回值 `retval` 裁剪。当 TCP 只读到几十字节时，BPF 往 ringbuf 写入包含了未初始化数据的更大 payload。

**修改**: 在 `vec.l` 赋值后，`CLIENT_ENTRY_MAX_LEN` 裁剪前，增加 `retval` 裁剪：

```c
unsigned long long safe_len = vec.l;
if (safe_len > (unsigned long long)retval)       // 新增
    safe_len = (unsigned long long)retval;        // 新增
if (safe_len > (unsigned long long)CLIENT_ENTRY_MAX_LEN)
    safe_len = (unsigned long long)CLIENT_ENTRY_MAX_LEN;
```

**语义安全**: `retval` 已在上文裁剪到 `CLIENT_ENTRY_MAX_LEN`。裁剪顺序为 `vec.l → retval → CLIENT_ENTRY_MAX_LEN`，每步只缩小不放大。

---

### 2. REPL 控制命令扫描改首字节判断

**文件**: `src/ebpf_proxy/main.c`，`ringbuf_callback()`

**问题**: 每条 payload 最多扫描 128 字节，每个 offset 做 memcmp。正常 RESP 命令以 `*`/`$`/`:`/`+`/`-` 开头，`R` 开头的只有 `RESP3` hello 握手（极罕见）和 `REPLSYNC`/`REPLACK`/`REPLDONE` 三个复制控制命令。

**修改**: 新增 `is_repl_control_payload()` 函数，首字节快速拒绝：

```c
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

替换原有 for 循环扫描：

```c
if (is_repl_control_payload(payload, plen)) {
    return 0;
}
```

**语义安全**: 控制命令始终在 TCP 流的 payload 开头出现，不需要全文扫描。非 `R` 开头的 payload 一条 `if` 就跳过，正常 RESP 命令 100% 走快速路径。

---

### 3. send() 改 send_full() + 修复 partial send 错误缓存

**文件**: `src/ebpf_proxy/main.c`，`ringbuf_callback()`

**问题**:
- `send()` 不保证一次写完所有数据，触发 `cache_append` 的概率高于必要
- partial send 时缓存整条 payload，已发送部分会重复发送

**修改**: 新增 `proxy_send_full()` 函数：

```c
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

替换发送逻辑为：

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

**语义安全**: AOF always 要求每条命令完整转发，`send_full()` 保证这一点。正常路径变为 `ringbuf event → send_full → return`，无 malloc、无 cache、无日志、无扫描。

---

### 4. proxy_slave 设置 SO_SNDBUF/SO_RCVBUF

**文件**: `src/ebpf_proxy/proxy_slave.c`，`proxy_slave_connect()`

**问题**: sync 测试在 slave fd 上设了 `SO_SNDBUF = 262144`（256KB），proxy 路径没有设。默认 SO_SNDBUF 通常为 16KB-85KB，高 QPS 下 TCP 发送缓冲区容易满，导致 send() 阻塞。

**修改**: 在 `socket()` 之后、`connect()` 之前增加：

```c
int snd = 1024 * 1024;  /* 1MB */
int rcv = 1024 * 1024;  /* 1MB */
setsockopt(ctx->fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
setsockopt(ctx->fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
```

**为什么是 1MB**:
- ringbuf 最大 4MB，1MB 能容纳更多突发命令
- 实际内核会 cap 到 `net.core.wmem_max`，不会无限放大
- 每个 proxy 进程只有一个 slave 连接，内存不是瓶颈

**语义安全**: 不改变 AOF always 语义，只优化 TCP 层缓冲。

---

### 5. ring_buffer__poll timeout 100ms → 5ms

**文件**: `src/ebpf_proxy/main.c`，`main_loop()` 两处

**问题**: 100ms timeout 在高 QPS 下不直接影响吞吐（有数据时 poll 立即返回），但在状态切换、重连完成、低负载时会引入最长 100ms 的响应延迟。

**修改**: 两处 `ring_buffer__poll(g_rb, 100)` 改为 `ring_buffer__poll(g_rb, 5)`。

**为什么是 5ms**: 足够低延迟，符合 AOF always 语义；比 1ms 节省无数据时的 CPU 唤醒成本。

---

## 正常路径

优化后，正常转发路径为：

```
BPF kprobe 捕获命令（精确 retval 长度）
→ ringbuf
→ ringbuf_callback
→ is_repl_control_payload() 首字节快速拒绝
→ proxy_send_full(fd, payload, plen) 完整发送
→ return 0
```

中间无 malloc、无 cache、无 fprintf、无循环扫描。

缓存路径只在 slave 未连接或 send 失败时触发。

---

## 验证计划

1. 编译 `repl_client_capture.bpf.o` 和 `ebpf-proxy`
2. 运行 `test_ebpf_proxy_qps` QPS 对比测试（proxy 路径 vs sync 路径）
3. 确认差距从 ~18% 缩小到 ≤5%
4. 回归验证：确认全量同步（REPLSYNC→BUFFERING→REPLDONE→flush→FORWARDING）流程正常

---

## 不做的事

- 不做批量发送（batch submit）——AOF always 语义要求每条命令立即转发
- 不做内核态转发（sockmap/sk_msg）——架构大改，不适合当前阶段
- 不改 cache 数据结构——cache 只作为异常路径，优化重点是让正常路径不进入 cache
