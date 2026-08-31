# Kprobe Fwd 统一增量同步 — 设计文档

**日期:** 2026-06-30
**状态:** 待实现

## 目标

移除 `repl_broadcast` 和 kprobe fwd 的双路并行增量同步，改为 **kprobe fwd 为主、repl_broadcast 为降级保底** 的互斥模式，两者共用同一个 TCP 连接（`c->fd`），消除重复传输。

**优先级:** 减少重复传输 > 端口简化 > 代码简化

## 背景

当前架构（commit `58613c8`）双路并行：

```
Client → Master
  ├─ repl_broadcast (TCP, c->fd)        ← 可靠保底，永不关闭
  └─ kprobe fwd (TCP, g_kprobe_fwd_fd)  ← BPF 拦截转发
                    ↓
        Slave 按 repl_offset 去重
```

- 两个路径发送相同数据，带宽浪费
- kprobe fwd 使用独立连接 `port+13`，额外端口和监听线程
- `bpf_probe_read_user` 已验证在 kernel 6.1.176 上完全可靠

## 验证结果

在 kernel 6.1.176 上运行 `tests/verify_bpf_read_user`：
- BPF hits: 200/200（全捕获）
- Ringbuf captures: 198/200（差 2 是 poll 时序）
- Data mismatch: 0
- READ_ERR: 0, RB_ERR: 0

结论：`bpf_probe_read_user` 在 6.1.176 上可靠。

## 目标架构

```
Master
  └─ BPF client_capture → ringbuf callback
       ├─ [c->fwd_healthy] send(c->fd) → Slave
       └─ [c->fwd_unhealthy] 标记
  └─ handle_parsed_command
       ├─ 更新 repl_offset + backlog（始终执行）
       ├─ [c->fwd_unhealthy] repl_broadcast → send(c->fd)
       └─ [c->fwd_healthy] 跳过（kprobe 已发）

Slave
  └─ 主连接 recv → parse_resp_stream(from_replication=1)
       （kprobe_fwd_slave_thread 删除）
```

**端口变化:** 去掉 port+13，只用主端口。总端口从 5 减到 4。

## 模块设计

### 1. repl_offset 和 backlog 解耦

**位置:** `src/main/kvstore.c` — `handle_parsed_command`

`repl_broadcast` 当前做三件事：发送、更新 `g_master_repl_offset`、维护 `repl_backlog`。拆分：offset 和 backlog 维护移到 `handle_parsed_command`，发送保留在 `repl_broadcast` 但仅对 unhealthy slave 执行。

```c
// handle_parsed_command, 写命令成功分支:
if (!from_replication && is_write_cmd(cmd)) {
    persist_note_write();
    if (persist_append_raw(raw, rawlen) != 0) { ... }
    if (g_cfg.role == ROLE_MASTER) {
        g_last_write_ts = time(NULL);          // 健康检查参照
        repl_backlog_feed(raw, rawlen);        // 维护 backlog
        repl_note_broadcast(rawlen);           // 更新 g_master_repl_offset
        repl_broadcast(raw, rawlen);           // 仅对 unhealthy slave 发送
    }
}
```

### 2. repl_broadcast 改动

**位置:** `src/main/kvstore.c` — `repl_broadcast`

核心变化：对健康的 slave（`c->fwd_healthy=1`）跳过发送。

```c
void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    repl_note_send_context("broadcast", rawlen, repl_master_offset(), raw);

    pthread_mutex_lock(&g_repl_lock);
    conn_t **pp = &g_replicas;
    while (*pp) {
        conn_t *c = *pp;
        // ... draining / fullsync_pending / fullsync_in_progress 检查不变 ...

        // 只对 kprobe fwd 不健康的 slave 发送
        if (c->fwd_healthy) {
            pp = &c->next_replica;
            continue;
        }
        if (repl_realtime_send(c, raw, rawlen) != 0) {
            if (repl_handle_replica_send_failure(c, pp)) continue;
        }
        c->repl_offset_sent = repl_master_offset();
        c->repl_last_send_ms = kvs_now_ms();
        pp = &c->next_replica;
    }
    pthread_mutex_unlock(&g_repl_lock);
}
```

### 3. 健康检查

**位置:** `src/replication/kvs_repl_kprobe.c` — `repl_kprobe_fwd_health_check`

**`conn_t` 新增字段** (`include/kvstore/kvstore.h`):

```c
int fwd_healthy;         // 1=kprobe fwd 对此 slave 健康（初始 0）
time_t fwd_last_active;  // 最后成功转发时间戳
```

`fwd_healthy` 初始值为 0 — slave 连接建立时由 `repl_add_slave` 分配 `conn_t`（`calloc` 归零），全量同步完成后 `queue_snapshot` 中设为 1。

**状态机:**

```
fwd_healthy:
  1 ──[故障检测]──▶ 0 ──[恢复检测]──▶ pending ──[空闲窗口]──▶ 1

故障检测（1→0）：   有写流量 + kprobe 对此 slave 无转发 > 5s
恢复检测（0→1）：   BPF 正常 + ringbuf 正常 + 当前空闲 > 5s
```

**恢复必须走空闲边界** — 只在空闲时切回，防止字节流交叉。恢复后 5s 内验证转发有效，否则再次标记 unhealthy。

**健康检查函数:**

```c
void repl_kprobe_fwd_health_check(void) {
    // 1. 全局检查：BPF 是否还 attach？ringbuf RB_ERR 是否增长？
    if (bpf_kprobe_detached() || ringbuf_rb_err_increased()) {
        // 全局故障 → 全部 slave 标记 unhealthy
        for (conn_t *c = g_replicas; c; c = c->next_replica)
            c->fwd_healthy = 0;
        return;
    }

    time_t now = time(NULL);

    // 2. 无写流量 → 不判故障，检查恢复条件
    if (now - g_last_write_ts > KVS_KPROBE_FWD_HEALTH_TIMEOUT) {
        // 空闲窗口 — 尝试恢复
        for (conn_t *c = g_replicas; c; c = c->next_replica) {
            if (!c->fwd_healthy) {
                c->fwd_healthy = 1;
                c->fwd_last_active = now;
            }
        }
        return;
    }

    // 3. 有写流量 → 检查 per-slave 转发活跃度
    for (conn_t *c = g_replicas; c; c = c->next_replica) {
        if (!c->fwd_healthy) continue;
        if (now - c->fwd_last_active > KVS_KPROBE_FWD_HEALTH_TIMEOUT) {
            c->fwd_healthy = 0;
            // 日志输出
        }
    }
}
```

### 4. ringbuf callback 改动

**位置:** `src/replication/kvs_repl_kprobe.c` — `client_ringbuf_cb`

从全局单 fd（`g_kprobe_fwd_fd`）改为遍历所有 slave 的 `c->fd`。REPLSYNC/REPLACK 过滤逻辑从 `forward_to_slave` 提取为 `is_repl_control` 内联函数（行为不变，只是代码位置变化）。

```c
// STATE_INCR 分支 (g_repl_fullsync_in_progress == 0):
pthread_mutex_lock(&g_repl_lock);
for (conn_t *c = g_replicas; c; c = c->next_replica) {
    if (c->repl_draining || c->repl_fullsync_pending) continue;
    if (!c->fwd_healthy) continue;
    if (is_repl_control(payload, payload_len)) continue;

    if (send(c->fd, payload, payload_len, MSG_NOSIGNAL) > 0) {
        c->fwd_last_active = time(NULL);
    }
}
pthread_mutex_unlock(&g_repl_lock);
```

### 5. 全量同步交互

`queue_snapshot` 中的改动：

- `repl_kprobe_fwd_connect_from_replica` 调用删除
- **`c->fwd_healthy = 1` 在 `repl_client_capture_flush_to_slave` 成功之后设置**（不在之前，避免 flush 失败时 kprobe fwd 已被标记为健康）
- `repl_client_capture_flush_to_slave` 不变（使用 `repl_send_chunked` TCP 可靠发送）
- 全量同步期间 `g_repl_fullsync_in_progress=1` 时，ringbuf callback 走缓存路径，健康检查跳过

### 6. Slave 端简化

**删除：**
- `kprobe_fwd_slave_thread` 函数
- `repl_kprobe_fwd_slave_init` 函数
- `KVS_KPROBE_FWD_PORT_OFFSET` 常量
- `g_kprobe_fwd_fd` 全局变量（master 侧）
- Slave 端 `g_kprobe_running` 用于 fwd 线程的逻辑

**无需修改：**
- `parse_resp_stream(from_replication=1)` 照常处理主连接数据
- `handle_parsed_command` 中 `from_replication` 分支不变

### 7. 全局变量变化

| 变量 | 改动 |
|------|------|
| `g_repl_broadcast_suppressed` | **删除** — 改用 per-slave `c->fwd_healthy` |
| `g_kprobe_fwd_fd` | **删除** — 改用 `c->fd` |
| `g_fwd_healthy` | **删除** — 改用 `c->fwd_healthy` |
| `g_fwd_last_active` | **删除** — 改用 `c->fwd_last_active` |
| `g_last_broadcast_time` | **改为 `g_last_write_ts`** — 语义更准确 |

### 8. 删除的函数列表

| 函数 | 文件 | 原因 |
|------|------|------|
| `repl_kprobe_fwd_connect_from_replica` | `kvs_repl_kprobe.c` | 不再需要 port+13 连接 |
| `kprobe_fwd_slave_thread` | `kvs_repl_kprobe.c` | Slave 不再监听 port+13 |
| `repl_kprobe_fwd_slave_init` | `kvs_repl_kprobe.c` | Slave 不再初始化 fwd 线程 |
| `forward_to_slave` | `kvs_repl_kprobe.c` | 逻辑并入 ringbuf callback |

## 改动文件清单

| 文件 | 改动类型 |
|------|----------|
| `include/kvstore/kvstore.h` | `conn_t` 新增 `fwd_healthy`、`fwd_last_active`；`g_last_write_ts` 声明 |
| `src/main/kvstore.c` | `repl_broadcast` 加 healthy 判断；`handle_parsed_command` 解耦 offset/backlog；`queue_snapshot` 去掉独立连接 |
| `src/replication/kvs_repl_kprobe.c` | 健康检查重写；ringbuf callback 遍历 slave；删除 4 个函数 |
| `include/kvstore/replication/repl_kprobe.h` | 删除 `repl_kprobe_fwd_connect_from_replica`、`repl_kprobe_fwd_slave_init` 声明；删除 `KVS_KPROBE_FWD_PORT_OFFSET` |

## 测试要点

1. **单 slave 正常增量同步** — kprobe fwd 健康，数据通过 ringbuf→c->fd 发送，slave 数据一致
2. **kprobe fwd 故障降级** — 模拟 BPF detach/ringbuf 溢出，自动切到 repl_broadcast，数据一致
3. **故障恢复** — 空闲后健康恢复，切回 kprobe fwd
4. **多 slave 独立健康** — 一个 slave unhealthy 不影响其他 slave
5. **全量同步** — 全量同步→flush→增量 流程不受影响
6. **repl_offset 正确性** — offset 解耦后 partial resync (CONTINUE) 仍然正常

## 实现过程中发现的关键问题

### BPF iovec 读取返回缓冲区大小而非实际数据大小

**症状：** slave 能收到增量数据但 RESP 解析器在第一个命令后卡住，recv buffer 开头全是 `\0` 字节。

**根因：** `repl_client_capture.bpf.c` 中 `read_recv_data()` 返回 `vec.l`（iovec 缓冲区长度，如 8192），而非 `tcp_recvmsg` 的实际返回值（如 40 字节）。ringbuf entry 的 `payload_len` 设为 8192，导致大量零填充字节被发送到 slave。

**修复：** 在 `client_ringbuf_cb` 发送前 trim 尾部 `\0` 字节：
```c
while (payload_len > 0 && payload[payload_len - 1] == 0)
    payload_len--;
```

### RDMA/TCP 全量同步后时序竞争

**症状：** 全量同步完成后无增量数据到达 slave。

**根因：** 全量同步完成时 master 立即激活 kprobe fwd（`c->fwd_healthy=1`），ringbuf callback 开始通过 TCP `send(c->fd)` 转发增量数据。但 RDMA REPLDONE 与 TCP 增量数据在不同传输层，无顺序保证，TCP 增量数据可能在 RDMA REPLDONE 之前到达 slave，此时 `g_slave_loading_fullsync=1`，数据被当作 KVSD 二进制拦截并丢弃。

**修复：** RDMA 全量完成后，额外通过 TCP 发送一份 REPLDONE（`send(c->fd, ...)`）。TCP 顺序保证后续同一连接上的所有增量数据一定在 TCP REPLDONE 之后到达。

### ringbuf callback 持锁 send() 导致写 QPS 暴跌

**症状：** 增量写入 QPS 从 2000+ 降至 160。

**根因：** ringbuf callback 在持有 `g_repl_lock` 期间调用阻塞 `send()`，当 TCP 缓冲区满时持锁长达 100ms，阻塞了 `repl_broadcast`（也需要 `g_repl_lock`）和所有写命令处理。

**修复：** 在锁内只收集目标 fd 列表，释放锁后再执行 `send()`。
