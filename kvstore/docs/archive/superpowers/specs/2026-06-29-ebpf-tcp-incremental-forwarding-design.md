# eBPF+tcp 增量同步 — kprobe 异步转发

## 背景

当前 eBPF+tcp 增量同步路径：全量同步期间 kprobe 缓存客户端写入 → REPLDONE 后 flush 缓存 → 增量阶段走 `repl_broadcast`（纯 TCP）。

kprobe 在增量阶段仍然挂钩 `tcp_recvmsg`，但捕获的数据被丢弃（`client_ringbuf_cb` 中 `FULLSYNC_IN_PROGRESS=0` 时直接 `return 0`）。

目标是让 kprobe 在增量阶段接管数据转发：截获 TCP 数据 → ringbuf → 异步线程 → TCP write(slave)，把 `write(slave)` 从主请求路径移走。

## 架构

```
增量同步期间:

Client → tcp_recvmsg ──→ [kprobe entry/return] ──→ ringbuf
       │                                                   │
       │                                          ringbuf 消费者线程
       │                                                   │
       │                                          forward_to_slave(TCP)
       │                                                   │
       │                                              ┌────┴────┐
       │                                              │ 健康检查 │
       │                                              └────┬────┘
       │                                                   │
       │                                         健康? → Slave (主路径)
       │                                         不健康? → 切回 repl_broadcast
       │
       ▼
handle_parsed_command ──→ repl_broadcast ──→ Slave (保底，默认静默)
```

双路径策略：
- **kprobe 转发（主路径）**：健康时承担增量数据转发，`repl_broadcast` 被压制
- **repl_broadcast（保底）**：kprobe 异常时自动接管，不丢数据

## 健康检查

```c
static volatile time_t  g_fwd_last_active;  // 最后成功转发时间戳
static volatile int     g_fwd_healthy;       // 1=健康 0=异常

// ringbuf 消费者线程：有数据时更新心跳
// reactor 100ms 定时器：检查超时

if (g_fwd_healthy && time(NULL) - g_fwd_last_active > 5) {
    g_fwd_healthy = 0;
    g_repl_broadcast_suppressed = 0;  // 切回 repl_broadcast
}
```

规则：
- 5 秒无数据判定异常（正常负载下应有数千条/秒）
- 切换到 repl_broadcast 后不自动切回（避免震荡）
- 只有下次 REPLDONE 才重置 `g_fwd_healthy`

## 数据路径

### client_ringbuf_cb 改动

```c
static int client_ringbuf_cb(void *ctx, void *data, size_t size) {
    // ... 解析 payload_len, 过滤控制命令 ...

    if (g_repl_fullsync_in_progress) {
        // 全量同步中：L1/L2 缓存（不变）
    } else if (g_fwd_healthy) {
        forward_to_slave(payload, payload_len);  // 新增：kprobe 异步转发
    }
    return 0;
}
```

### forward_to_slave

复用现有函数（当前标记 `__attribute__((unused))`），去掉 unused 标记：
- 已实现 RESP 控制命令过滤（REPLSYNC/REPLACK）
- 已实现 EAGAIN 重试 + MSG_NOSIGNAL
- 无需改逻辑

### repl_broadcast 改动

```c
void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    if (g_repl_broadcast_suppressed) return;  // kprobe 接管中
    // ... 原有逻辑不变 ...
}
```

## 生命周期

```
Master 启动
  ├── 加载 client_capture BPF
  │     ├── 成功 → kprobe_loaded=1
  │     └── 失败 → kprobe_loaded=0, 全程 repl_broadcast（不变）
  ▼
Slave 连接 → FULLRESYNC 开始
  ├── repl_client_capture_set_fullsync(1)
  │     └── client_ringbuf_cb: 全量同步中 → 缓存 L1/L2（不变）
  ▼
REPLDONE 发送 → queue_snapshot:
  ├── ① flush L1+L2 缓存到 slave（不变）
  ├── ② repl_client_capture_set_fullsync(0)
  ├── ③ g_repl_broadcast_suppressed = 1
  └── ④ g_fwd_healthy = 1
  ▼
增量同步运行
  ├── 正常: kprobe → ringbuf → forward_to_slave
  ├── 健康检查: 每 100ms
  │     └── 超时 5s → g_fwd_healthy=0, g_repl_broadcast_suppressed=0
  ▼
Slave 断开/重连
  └── 下次 REPLDONE 重置
```

## 涉及文件

| 文件 | 改动 |
|------|------|
| `src/replication/kvs_repl_kprobe.c` | `client_ringbuf_cb` 增加增量转发分支；启用 `forward_to_slave`；新增健康检查变量和逻辑 |
| `src/replication/kvs_repl.c` | `repl_broadcast` 增加 `g_repl_broadcast_suppressed` 检查；`queue_snapshot` 增加抑制标志设置 |
| `src/main/kvstore.c` | reactor 定时器中增加健康检查调用 |

## 不变项

- 全量同步期间的 L1/L2 缓存逻辑不变
- kprobe BPF 程序（`repl_client_capture.bpf.c`）不变
- TCP fallback 路径不变
- `test_repl_5w5w` 不变（增量同步通过 offset 或数据验证判断完成）
