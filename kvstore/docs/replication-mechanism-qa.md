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

> 本节描述的是 **ebpf+tcp（默认）** 下的行为。`kprobe-rdma` 模式另有一套内核态转发路径，
> 见 Q16。历史上还有一套「进程内 client_capture」实现（L1 内存链表 4MB / L2 临时文件 spill、
> `client_ringbuf_cb`、`client_poll_thread`），代码中**已移除**，下面 Q5~Q9 描述的是替代它的
> **独立 ebpf-proxy 进程**方案。

### Q5: 全量同步期间，客户端写入 Master 的数据由谁缓存？

**由独立的 `ebpf-proxy` 进程的 `proxy_cache` 缓存，不占用 master 进程的内存。**

```text
Client → Master tcp_recvmsg（fexit 捕获）
           → BPF ringbuf（64MB）
           → ebpf-proxy 主线程
               ├─ STATE=FORWARDING → 转发队列 → 转发线程 writev → Slave
               └─ STATE=BUFFERING（全量同步中）→ proxy_cache
```

每个 cache 节点都带 `session_id`，只能在本 replication session 内使用。

`repl_broadcast()` 在全量期间跳过下送（`g_repl_fullsync_in_progress=1`），但**字节仍由写命令
路径统一喂入 `repl_backlog`** —— backlog 负责「重新 REPLSYNC 后按 offset 续传」，
proxy_cache 负责「本 session 暂时送不出去」，两者职责不同，不会互相替代。

### Q6: 缓存的数据在全量同步结束后发送给 Slave，此时客户端新写入的数据怎么办？

由 master 与 proxy 的握手顺序保证不乱：

```text
全量开始（queue_snapshot）:
  ① g_repl_fullsync_in_progress = 1
  ② client_ctl[3] = 1（通知 proxy 切 BUFFERING）
  ③ 等 proxy 置 client_ctl[12] = 1 确认（最多 500ms）
       proxy 在此刻：清空转发队列 + 丢弃上一个 session 的 proxy_cache
  ④ snap_base_offset = master_repl_offset   ← 确认后才取，边界干净
  ⑤ repl_backlog_reset(snap_base_offset)    ← backlog 以快照边界重建
  ⑥ 生成快照 → RDMA 传输 → Slave load dump → 回 REPLDONE

全量结束（master 收到 REPLDONE）:
  ⑦ g_repl_fullsync_in_progress = 0
  ⑧ client_ctl[3] = 0 → proxy 切 FORWARDING → flush 本 session 的 proxy_cache
  ⑨ ebpf+tcp 不做 backlog 回放（增量只有 proxy→slave 一条路，回放会双路重复）
```

第 ③ 步的等待是必须的：如果 proxy 还在 FORWARDING 就取快照，这段时间下送的增量
**既在快照里、又会被重放**（对 INCR/DEL 这类非幂等命令是致命的）。

第 ⑤ 步同样重要：快照已经覆盖 `snap_base_offset` 之前的全部状态，旧 backlog 历史留着
只会让 partial resync 误判或与 proxy_cache 双重回放，所以直接以快照边界重建。

### Q7: 全量同步时，客户端写入的数据会被捕获吗？会被转发吗？

**会被捕获（capture），不会被转发（forward）。**

- `STATE=BUFFERING` → 进 `proxy_cache`，**不发给 Slave**
- `STATE=FORWARDING` → 进转发队列，转发线程 writev 到 Slave

### Q8: 没有 Slave 的时候，eBPF 还会捕获客户端数据吗？

**不会。** 新增了 `client_ctl[7] = CAPTURE_ENABLE`：

```c
/* fexit_tcp_recvmsg 最前面 */
__u64 *ctl_cap = bpf_map_lookup_elem(&client_ctl, &cap_key);
if (!ctl_cap || !*ctl_cap)
    return 0;   /* 连 pid 过滤都不做 */
```

- 第一个 Slave 完成 REPLSYNC 握手、建立 session 时 → master 置 1
- 最后一个 Slave 的复制连接消失时 → master 置 0

这样子无 Slave 时既没有 `bpf_probe_read_user` / `bpf_ringbuf_output` 的开销，
ringbuf 和 proxy_cache 也不会无意义增长、进而通过背压反过来拖慢正常客户端请求。
可用 INFO 的 `ebpf_capture_enabled` / `ebpf_capture_off_count` 观察。

### Q9: `repl_fullsync_pending`、`g_repl_fullsync_in_progress`、`client_ctl[3]` 有什么区别？

三个不同层面的标志：

| 标志 | 位置 | 作用 |
|------|------|------|
| `repl_fullsync_pending` | 用户态 `conn_t` 字段 | per-slave，表示"这个 slave 正在等待全量同步"，`repl_broadcast()` 跳过它 |
| `g_repl_fullsync_in_progress` | 用户态全局 | 全局，全量期间抑制增量下送 |
| `client_ctl[3] FULLSYNC_STATE` | BPF map（proxy 轮询） | 通知 `ebpf-proxy` 切 BUFFERING / FORWARDING |

---

## 三、增量同步与 eBPF+TCP 转发

### Q10: 增量同步时，客户端写入 Master 的数据不会经过 Master 解析吗？直接通过 eBPF 转发？

**会经过解析。** Master 正常执行 `parse_resp_stream → handle_parsed_command`
（写引擎 + 写 AOF + 写 backlog + 更新 `master_repl_offset`）。
eBPF 是**额外**在内核态截获一份同样的数据用于转发，不是替代正常路径。

两条路径各司其职：

- **eBPF 路径**：`fexit/tcp_recvmsg` → ringbuf → ebpf-proxy → 自己的 TCP 连接 → Slave
- **repl_broadcast 路径**：用户态 `repl_broadcast(raw, rawlen)` → 跳过 EBPF_TCP 副本
  （避免重复），但**照常喂 backlog / 推进 offset**

### Q11: 增量同步转发的数据是 RESP 格式吗？

**是。** 增量的每条数据就是客户端的原始 RESP 命令字节。`repl_broadcast()` 的参数是
`raw` + `rawlen`（客户端原始 RESP），eBPF 截获的也是 `tcp_recvmsg` 返回的原始字节。
Slave 收到后走同一套 `parse_resp_stream(from_replication=1)` 解析。

### Q12: eBPF 探测的是 tcp_recvmsg 的 entry/kretprobe 吗？还探测 tcp_sendmsg 发送的 REPLDONE？

**都不是。只有一个 hook：`fexit/tcp_recvmsg`。**

| Hook | 作用 |
|------|------|
| `fexit/tcp_recvmsg` | 直接从 `ctx[5]` 取返回值（实际读字节数），从 msghdr 读 iovec 数据 → `bpf_ringbuf_output` |

（已实测 kernel 6.1.176：`ctx[5]` 就是 `tcp_recvmsg` 的返回值，因此不再需要 fentry
保存 `count_before`，也不再需要 `kprobe/tcp_sendmsg` 去探测 REPLDONE —— 全量边界的
切换改由 master 写 `client_ctl[3]` 通知 proxy。）

### Q13: 为什么不做 `session_id` 之外的 offset 对齐？proxy_cache 为什么不自己管 offset？

eBPF 抓的是 `tcp_recvmsg` 原始请求流，它**不能天然知道业务命令最终是否执行成功**，
也无法可靠地把每个 recv chunk 与 Master 业务层的 replication offset 一一对应。
所以职责划分是：

```text
proxy_cache  → session 级（只保证"同一个 session 内不丢顺序"）
repl_backlog → offset 级（Master 侧权威复制历史，供 partial resync）
```

---

## 四、eBPF+TCP 增量同步架构（当前实现）

### Q14: eBPF+TCP 增量的完整数据流是怎样的？

```text
Client Write
    │
    ▼
Master 内核 tcp_recvmsg()
    │
    ├─ fexit 路径（内核态）:
    │     fexit/tcp_recvmsg
    │       → client_ctl[7]=0 ? 直接 return : 读数据 → BPF ringbuf
    │       → ebpf-proxy 主线程（独立进程）
    │           ├─ STATE=FORWARDING 且 slave 连接正常 → 转发队列 → 转发线程 writev → Slave
    │           └─ STATE=BUFFERING → proxy_cache
    │
    └─ repl_broadcast 路径（本模式不转发，只记账）:
         handle_parsed_command → repl_backlog_feed + repl_note_broadcast
                                → repl_broadcast(raw, rawlen)
                                    → EBPF_TCP 副本：跳过（已由 proxy 转发）
```

### Q15: 全量→增量的切换序列

```text
全量开始:
  ① g_repl_fullsync_in_progress = 1
  ② client_ctl[3] = 1 → proxy 切 BUFFERING（清转发队列 + 丢弃旧 proxy_cache）
  ③ master 等 client_ctl[12]=1 确认
  ④ snap_base_offset = master_repl_offset
  ⑤ repl_backlog_reset(snap_base_offset)

全量结束（收到 slave 的 REPLDONE）:
  ⑥ client_ctl[3] = 0 → proxy 切 FORWARDING → flush proxy_cache（本 session）
  ⑦ ebpf+tcp 不回放 backlog
```

### Q16: `fwd_healthy` 健康检查机制在 ebpf+tcp 下还生效吗？

**不生效，它是 `kprobe-rdma` 模式专用的。**

`repl_kprobe_fwd_health_check()`（由 reactor 定时调用）对每个 replica 显式跳过
`KVS_REPL_TRANSPORT_EBPF_TCP`：

- `fwd_healthy=1` 且 `fwd_last_active` 超时（5s）且近期有写 → `fwd_healthy=0`（回退 `repl_broadcast`）
- `fwd_healthy=0` 且近期无写（5s 空闲窗口）→ `fwd_healthy=1`（恢复）

ebpf+tcp 下 `fwd_healthy` 恒为 0，增量完全由 ebpf-proxy 独立进程负责。

### Q17: proxy 的数据连接断了怎么办？和复制 session 断了有什么区别？

两者语义不同，恢复方式也不同：

| 场景 | 判定 | proxy 行为 | 恢复方式 |
|------|------|-----------|---------|
| **仅数据通道断开**（`ebpf-proxy → slave` 的 TCP 断了，控制连接还在） | `client_ctl[8] SESSION_VALID=1` | 捕获的数据进 `proxy_cache`；发送侧拿到 EPIPE 会摘掉 fd（`proxy_slave_mark_down`）并重连 | 重连成功后**同 session** 先 flush cache 再继续转发，**不使用 backlog 重放** |
| **复制 session 断开**（Slave 控制连接也断了） | master 置 `client_ctl[8]=0` + `client_ctl[10] CACHE_INVALID=1` | 立即丢弃整个 cache，**拒绝 flush** | Slave 重新 REPLSYNC。恢复来源只能是 backlog（要求历史连续）或 FULLRESYNC |

关键点：**旧 proxy_cache 绝不跨 replication session 使用**。否则会和 backlog 回放叠加，
同一段写被应用两次。

### Q18: proxy_cache 满了会怎样？会丢数据吗？

**不会静默丢数据了。** 分两档：

```text
proxy_cache < 192MB（高水位）
    正常缓存

达到 192MB 高水位
    → 向上游发 backpressure（client_ctl[11]），master 暂停读取客户端

达到 256MB 硬上限
    → cache_append 返回 CACHE_APPEND_FULL，整个 session cache 标记 invalid + 清空
    → proxy 置 client_ctl[10] = 1 上报 master
    → master 断开 replica 链路，Slave 重连后走 partial / full resync
```

旧实现是「超过 256MB 就丢掉最旧节点继续跑」——对复制流来说等价于**静默丢一段**，
等于 Slave 永久缺数据，且非幂等命令无法自愈。现在降级为重新同步，但不允许形成错误副本。

### Q19: 什么样的 Slave 重连会被判定为 partial resync？

`repl_backlog_can_continue()` 必须**全部**满足：

```text
replid 匹配
AND backlog_contiguous == true
AND backlog.end_offset >= master_repl_offset
AND offset 落在 [backlog.start_offset, backlog.end_offset]
```

否则 FULLRESYNC。其中前两条是改造新增的关键防线：

```text
Slave 最后 offset=1000，backlog=[500,1200]；Slave 全部断开
Master 又写到 1500 → backlog_end 停在 1200（feed 因无 Slave 跳过）
Slave 重连请求 offset=1000：仍落在 [500,1200] 区间内

  旧逻辑：只看区间 → 误判可续 → 1200~1500 的写永久丢失
  新逻辑：backlog_contiguous=0 且 end_offset(1200) < master_offset(1500) → FULLRESYNC
```

### Q20: backlog 是「一直记录」还是「全量同步期间才启用」？

**有 Slave 时一直记录**（10MB 环形缓冲）。两个改造点：

1. **喂入点唯一**：只在写命令成功后 `repl_backlog_feed()` + `repl_note_broadcast()`。
   原先 `repl_broadcast()` 全量期间还会再喂一次，导致同一条 raw 被喂两遍、
   `backlog_end_offset` 跑在 `master_repl_offset` 前面，与 offset 推进语义脱钩。
2. **无 Slave 时不分配**：省 10MB，同时把历史标记为不连续（见 Q19）。

---

## 五、RDMA 相关

### Q21: 项目 RDMA 用的 siw 还是 rxe？

**默认配置是 siw（Soft-iWARP），代码层面两者都兼容。**

| 场景 | 用什么 | 原因 |
|------|--------|------|
| 默认配置 (`kvstore.conf`) | `siw0` | 基于 TCP，兼容性更好 |
| 吞吐量基准测试 | `rxe0` | README 明确"两机均配置 Soft-RoCE" |
| 本地 loopback | `rxe0` | 同机可达 45 Gbps |

两者都是纯软件实现，`rdma_cm` 自动适配，切换只需改 `rdma_dev` 配置。硬件 RoCE/IB 同理。

---

## 六、Slave 侧处理

### Q22: Slave 连接 Master 后立即发送 REPLSYNC 吗？

**是。** Slave 不区分"第一次连接"还是"重连"，永远发 REPLSYNC。要不要做全量同步是 **Master 决定的**——
replid 不匹配、backlog 历史不连续、backlog 没覆盖到 master 当前 offset、或 offset 不在 backlog 范围内，
任一条件成立即全量，否则增量（完整判定见 Q19）。

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
| `src/ebpf_proxy/main.c` | `ringbuf_callback`、`proxy_fwd_*`、session 状态机、`publish_cache_stats` |
| `src/ebpf_proxy/proxy_cache.c` | `cache_append` / `cache_flush`（带 session_id）、高/硬水位 |
| `src/ebpf_proxy/proxy_slave.c` | slave 连接、`proxy_slave_writev`、`proxy_slave_mark_down` |
| `src/replication/bpf/repl_client_capture.bpf.c` | BPF: 单个 `fexit/tcp_recvmsg` + `CAPTURE_ENABLE` 早退 |
| `src/replication/kvs_repl_kprobe.c` | kprobe-rdma 转发、`fwd_healthy` 健康检查（跳过 EBPF_TCP 副本） |
| `include/kvstore/replication/client_ctl.h` | `client_ctl` 键位约定（master / proxy / BPF 三方共用） |
| `include/kvstore/kvstore.h` | `conn_t`（含 `fwd_healthy`、`repl_fullsync_pending`、`repl_transport_kind`） |
| `docs/kvstore-data-flow.md` | 完整数据流文档（含 eBPF+TCP 当前实现） |
