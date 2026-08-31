# 主从复制机制 Q&A

> 从对话中提取的复制机制相关问题与回答，覆盖全量同步、增量同步、eBPF+TCP 转发、缓存机制等。

---

## 一、全量同步流程

### Q1: 全量同步的同步请求由谁发起，Master 还是 Slave？

**Slave 发起。** Slave 通过 `slave_thread()` 连接 Master，发送 `REPLSYNC <replid> <offset> <durable_offset>` 命令。Master 被动响应，根据 replid 和 offset 判断走全量还是增量。

流程：Slave 连接 Master → 发送 REPLSYNC → Master 判断 `can_continue` → false 则全量同步（`queue_snapshot`），true 则增量同步（`+CONTINUE`）。

### Q2: 同步完成的信号是由 Master 发送的吗？

**是。** Master 发完 KVSD 快照数据后发送 `REPLDONE` 命令。Slave 收到后调用 `repl_slave_finish_fullsync()` 完成加载。增量同步同样以 `REPLDONE` 收尾。

### Q3: RDMA 是在 Master 接收到 Slave 发送的 REPLSYNC 后才调用的吗？

**是。** 调用链：Slave 发送 REPLSYNC → Master `handle_parsed_command` 处理 → 判断需要全量同步 → `queue_snapshot(c)` → `repl_rdma_start_fullsync(c)` 按需启动 RDMA listener。

Slave 侧在发送 REPLSYNC 之前就后台并发启动 RDMA 连接线程，会自动重试直到 Master listener 就绪。

### Q4: 每次有新连接到来时，Slave 的 replid 为 "?"，连接后 Master 将该 Slave 的 replid 标记，对吗？

**不对。** replid 是 **Master 的身份标识**（全局唯一，启动时随机生成），不是 Slave 的。

- Slave 第一次连接：`g_slave_master_replid = "?"`，发送 `REPLSYNC ? 0 0`
- Master 发现 `"?" != g_master_replid` → 全量同步，回复 `+FULLRESYNC <master_replid> ...`
- Slave 收到后记住 Master 的 replid：`g_slave_master_replid = master_replid`
- 下次重连：发送 `REPLSYNC <记住的replid> <offset>` → Master 比较是否匹配

Master 不做任何 per-slave 记录，只比较 Slave 发来的 replid 和自己的 `g_master_replid`。

---

## 二、全量同步期间的缓存机制

### Q5: 是不是每次客户端向 Master 写数据，都要调用一次缓存发送的函数？

**不是。** 缓存是 BPF 自动触发的：

- 全量同步期间（`FULLSYNC_IN_PROGRESS=1`）：BPF `kretprobe/tcp_recvmsg` 自动截获数据 → ringbuf → `client_ringbuf_cb` → L1 内存链表（4MB）/ L2 磁盘缓存
- 增量同步期间（`FULLSYNC_IN_PROGRESS=0`）：BPF 截获数据 → ringbuf → `client_ringbuf_cb` → 直接 `send(c->fd)` 转发

用户态 `repl_broadcast()` 在全量同步期间跳过 slave（`g_repl_fullsync_in_progress=1`），所以客户端写入路径不额外调用缓存函数。

### Q6: 缓存的数据在全量同步结束后发送给 Slave，此时客户端新写入的数据怎么办？

`queue_snapshot()` 末尾的处理顺序确保不乱：

1. `g_repl_fullsync_in_progress = 0` — 后续写入走 `repl_broadcast()` 或 kprobe 直发
2. `repl_client_capture_set_fullsync(0)` — BPF 切换增量模式
3. `repl_client_capture_flush_to_slave(c)` — 先发送 L1+L2 缓存的旧数据
4. `c->fwd_healthy = 1` — 启用 kprobe 直发

因为 TCP 是字节流有序的，flush 先写、新数据后写，Slave 收到顺序正确。BPF 侧 `kprobe/tcp_sendmsg` 探测到 REPLDONE 时还会提前清零 `FULLSYNC_IN_PROGRESS`。

### Q7: 全量同步时，客户端写入的数据会被 kprobe 转发吗？

**会被捕获（capture），不会被转发（forward）。**

- `FULLSYNC_IN_PROGRESS=1` → `client_ringbuf_cb` 走缓存分支（L1/L2），**不发送到 Slave**
- `FULLSYNC_IN_PROGRESS=0` → `client_ringbuf_cb` 走直发分支（`send(c->fd)`），直接转发到 Slave

### Q8: `client_ringbuf_cb` 什么时候被调用？

由专用轮询线程 `client_poll_thread` 驱动，每 5ms 调用 `ring_buffer__poll()`。当 BPF 程序通过 `bpf_ringbuf_output` 写入数据后，libbpf 在下次 poll 时发现新数据，**立即同步调用** `client_ringbuf_cb`。最大延迟 5ms。

### Q9: `repl_fullsync_pending` 和 `FULLSYNC_IN_PROGRESS` 有什么区别？

两个不同层面的标志：

| 标志 | 位置 | 作用 |
|------|------|------|
| `repl_fullsync_pending` | 用户态 `conn_t` 字段 | per-slave，表示"这个 slave 正在等待全量同步"，`repl_broadcast()` 跳过它 |
| `FULLSYNC_IN_PROGRESS` | BPF map `client_ctl[3]` | 全局，控制 BPF 行为：=1 缓存，=0 直发 |

`FULLSYNC_IN_PROGRESS` 不是数据中的字段，而是用户态通过 `repl_client_capture_set_fullsync()` 写入 BPF map 的控制变量。

---

## 三、增量同步与 eBPF+TCP 转发

### Q10: 增量同步时，客户端写入 Master 的数据不会经过 Master 解析吗？直接通过 kprobe 转发？

**会经过解析。** Master 正常执行 `parse_resp_stream → handle_parsed_command`（写引擎 + 写 AOF + 写 backlog）。kprobe 是**额外**在内核态截获一份同样的数据用于转发，不是替代正常路径。

两条并行路径：
- **kprobe 路径**：BPF 截获 → ringbuf → `send(c->fd)` 直发 Slave
- **repl_broadcast 路径**：用户态 `repl_broadcast(raw, rawlen)` → TCP 发送

`fwd_healthy=1` 时 repl_broadcast 跳过该 slave（避免重复），`fwd_healthy=0` 时 repl_broadcast 作为保底。

### Q11: 增量同步转发的数据是 RESP 格式吗？

**是。** 增量的每条数据就是客户端的原始 RESP 命令字节。`repl_broadcast()` 的参数是 `raw` + `rawlen`（客户端原始 RESP），kprobe 截获的也是 `tcp_recvmsg` 返回的原始字节。Slave 收到后走同一套 `parse_resp_stream(from_replication=1)` 解析。

### Q12: eBPF 探测的是 tcp_recvmsg 的 entry/kretprobe 吗？还探测 tcp_sendmsg 发送的 REPLDONE？

**三个都探测。** `repl_client_capture.bpf.o` 挂了 3 个 hook：

| Hook | 函数 | 作用 |
|------|------|------|
| `kprobe/tcp_recvmsg` | entry | 保存 `msg` 指针到 per-CPU map |
| `kretprobe/tcp_recvmsg` | return | 读 iovec 数据 → `bpf_ringbuf_output` |
| `kprobe/tcp_sendmsg` | — | 探测 REPLDONE → 提前清零 `FULLSYNC_IN_PROGRESS` |

第三个 hook 在 REPLDONE 报文刚发出时就切换 BPF 到增量模式，比用户态 `repl_client_capture_set_fullsync(0)` 更早。

### Q13: 全量同步发送到 Slave 的是 KVSD 格式，Slave 收到数据做了哪些操作？

1. 解析 `+FULLRESYNC` 头 → 设置 `g_slave_loading_fullsync=1`，打开临时文件
2. KVSD 二进制数据拦截：**不经过 RESP 解析，直接字节流写入临时文件**
3. `REPLDONE` 或达到 target_bytes → `repl_slave_finish_fullsync()`：
   - `fsync` + `close` 临时文件
   - `replay_dump_file(tmp_path)`：mmap → 按 `[engine_id][klen][key][vlen][value]` 格式分发到各存储引擎
   - `rename(tmp → dump_path)` 原子替换
   - 发送 REPLACK 给 Master

---

## 四、eBPF+TCP 增量同步架构（当前实现）

### Q14: eBPF+TCP 增量的完整数据流是怎样的？

```
Client Write
    │
    ▼
Master 内核 tcp_recvmsg()
    │
    ├─ kprobe 路径 (内核态拦截):
    │     kprobe/kretprobe 拦截 → BPF ringbuf
    │       → client_poll_thread (5ms 间隔)
    │       → client_ringbuf_cb()
    │           ├─ FULLSYNC_IN_PROGRESS=1: 缓存到 L1 (4MB) / L2 (磁盘)
    │           └─ FULLSYNC_IN_PROGRESS=0:
    │                遍历 replicas，fwd_healthy=1 的 slave:
    │                  send(c->fd, payload, len)  ← 共用 TCP 连接
    │
    └─ repl_broadcast 路径 (保底):
         handle_parsed_command → repl_broadcast(raw, rawlen)
           → fwd_healthy=1: 跳过 (已被 kprobe 服务)
           → fwd_healthy=0: TCP send()
```

### Q15: 全量→增量切换的完整序列

```
① g_repl_fullsync_in_progress = 0        ← 用户态关标志
② repl_client_capture_set_fullsync(0)     ← 通知 BPF 切换
   (BPF 侧可能更早: tcp_sendmsg 探测到 REPLDONE 时主动清零)
③ repl_client_capture_flush_to_slave(c)   ← 刷 L1+L2 缓存
   └─ 成功 → c->fwd_healthy = 1
   └─ 失败 → c->fwd_healthy = 0 (回退 repl_broadcast)
④ 增量同步开始: kprobe ringbuf 直发 + fwd_healthy 互斥
```

### Q16: fwd_healthy 健康检查机制

由 reactor 定时调用 `repl_kprobe_fwd_health_check()`：

- `fwd_healthy=1` 且 `fwd_last_active` 超时（5s）且近期有写 → `fwd_healthy=0`（回退）
- `fwd_healthy=0` 且近期无写（5s 空闲窗口）→ `fwd_healthy=1`（恢复）

**关键设计**：
- **共用 TCP 连接**：kprobe 转发和 repl_broadcast 共用同一个 `c->fd`，不再有 port+13 独立连接
- **TCP 顺序保证**：REPLDONE 通过 TCP 发送（即使全量走 RDMA），确保 kprobe fwd 数据在 REPLDONE 之后到达
- **fwd_healthy 互斥**：kprobe 转发和 repl_broadcast 互斥，不重复发送

---

## 五、RDMA 相关

### Q17: 项目 RDMA 用的 siw 还是 rxe？

**默认配置是 siw（Soft-iWARP），代码层面两者都兼容。**

| 场景 | 用什么 | 原因 |
|------|--------|------|
| 默认配置 (`kvstore.conf`) | `siw0` | 基于 TCP，兼容性更好 |
| 吞吐量基准测试 | `rxe0` | README 明确"两机均配置 Soft-RoCE" |
| 本地 loopback | `rxe0` | 同机可达 45 Gbps |

两者都是纯软件实现，`rdma_cm` 自动适配，切换只需改 `rdma_dev` 配置。硬件 RoCE/IB 同理。

---

## 六、Slave 侧处理

### Q18: Slave 连接 Master 后立即发送 REPLSYNC 吗？

**是。** Slave 不区分"第一次连接"还是"重连"，永远发 REPLSYNC。要不要做全量同步是 **Master 决定的**——replid 不匹配或 offset 不在 backlog 范围则全量，否则增量。

```c
// Slave: TCP 连接 → 后台启动 RDMA 连接线程 → 立即 TCP 发送 REPLSYNC
int tcp_fd = repl_transport_tcp_connect_slave(host, port);
pthread_create(&rdma_tid, NULL, repl_rdma_bg_connect_thread, rdma_arg);
resp_build_cmd4("REPLSYNC", replid, offbuf, durablebuf);
send(tcp_fd, cmd, n, 0);
```

---

## 相关文件

| 文件 | 说明 |
|------|------|
| `src/main/kvstore.c` | `handle_parsed_command`（REPLSYNC 处理）、`queue_snapshot`、`repl_broadcast`、`parse_resp_stream` |
| `src/replication/kvs_repl.c` | `slave_thread`、`repl_slave_finish_fullsync`、replid 管理、backlog |
| `src/replication/kvs_repl_kprobe.c` | `client_ringbuf_cb`、`client_poll_thread`、L1/L2 缓存、`repl_client_capture_flush_to_slave`、健康检查 |
| `src/replication/bpf/repl_client_capture.bpf.c` | BPF: `kprobe/tcp_recvmsg` + `kretprobe` + `kprobe/tcp_sendmsg` |
| `include/kvstore/kvstore.h` | `conn_t`（含 `fwd_healthy`、`repl_fullsync_pending`） |
| `docs/kvstore-data-flow.md` | 完整数据流文档（含 eBPF+TCP 当前实现） |
