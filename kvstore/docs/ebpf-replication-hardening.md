# eBPF 增量复制加固（阶段 1–3）

对应方案：`PocketKV_eBPF保留转发_优化实施方案.md`。本文覆盖：
**阶段 1（确定性 Bug）**、**阶段 2（proxy 故障恢复）**、**阶段 3（Commit Gate）**，
以及 P1/P2 小项（zRPC busy、proxy cache 水位、SipHash/VSEARCH 指标）。
数据面保持 `tcp_recvmsg → ringbuf → ebpf-proxy → Slave`；阶段 3 之后只有
Master post-commit 的写命令会被转发（见第 10 节）。

---

## 1. 改动总览

| 模块 | 改动 |
|---|---|
| Hash | value 独立分配 + `vcap`，扩容/删除统一释放（修复泄漏） |
| 持久化/复制 | 记录 `recover frontier`，Slave 重启后 `resume = min(saved_durable, frontier)` |
| 网络后端 | Reactor / Proactor / NtyCo 统一 `KVS_EBPF_CAPTURE_MAX_RECV=32764` |
| BPF | `retval > 32764` 计 `ST_TRUNCATED` 并置 fault（不再截断输出）；capture gate |
| proxy | 心跳健康上报、capture gate 丢弃、pidfile、argv 配置、proxy_stats |
| master | proxy 生命周期/健康线程、会话失效、REPLSYNC 恢复时序、INFO 指标 |
| 既有缺陷 | backlog 断开期不 feed、kprobe 健康检查劫持 TCP 副本 |

---

## 2. capture gate（client_ctl）

`client_ctl` 为 pinned array（16 项），由 proxy 创建，master / BPF 共同读写：

| key | 名称 | 语义 |
|---|---|---|
| 1 | master_pid | BPF fexit pid 过滤 |
| 2 | master_port | 保留 |
| 3 | fullsync_state | 1=proxy 进入 BUFFERING（缓存增量），0=FORWARDING |
| 4 | ringbuf_backpressure | BPF 生产端高水位 (>32MB)，proxy 低水位 (<8MB) 清除 |
| 5 | heartbeat | proxy 心跳，100ms 递增；2s 不推进视为失活 |
| 6 | fwdq_backpressure | 转发队列背压 |
| 7 | capture_enable | **1=允许捕获**（默认 0；BASELINE 握手后置 1） |
| 8 | capture_fault | **1=捕获不可信（截断）**，fail-closed |
| 9 | proxy_state | **proxy 回执**：0=FORWARDING，1=BUFFERING |
| 10 | commit_fault | proxy → Master：commit gate 故障原因（1..5） |
| 11 | cache_backpressure | cache 高水位（>192MB 置位，<128MB 清除） |

BPF fexit 依次检查：pid → `capture_enable==1` → `capture_fault` →
socket cookie 在 allowlist 中 → retval（按 int 解释）→ 输出 record。
`retval > CLIENT_ENTRY_MAX_LEN` 时：

```text
ST_TRUNCATED++ → client_ctl[8]=1 → 丢弃该 record（不输出截断数据）
```

master 健康线程观察到 fault 后：

```text
capture_enable=0
→ repl_session_invalidate(CAPTURE_TRUNCATED)
→ shutdown 所有 eBPF 副本控制连接
→ slave 重连 REPLSYNC
```

---

## 3. proxy 健康与自动重启

- master 以 `posix_spawn` 启动 proxy，argv 传入
  `--master-pid/--master-port/--pid-file`，proxy 把自身 pid 写入
  `<dump_path>.ebpfproxy.pid`。
- 启动前用 200ms 心跳探测是否已有活 proxy，避免重复 attach。
- 健康线程（100ms）：

```text
heartbeat 推进            → healthy
2s 未推进                 → unhealthy
unhealthy:
  capture_disable=1
  repl_session_invalidate(PROXY_HEARTBEAT_STALE)
  冷却 5s 后 kill(pidfile/child) + respawn
```

- 重新 spawn 后 proxy 会 unlink/repin 自己的 maps；master 每次读 `client_ctl`
  都重新 `bpf_obj_get`，并重置背压缓存 fd，避免指向旧 map。
- 若 proxy 是外部手动启动的（master 未 spawn），同样以心跳判定，pidfile 存在时
  可被 master kill。

---

## 4. REPLSYNC 恢复时序（eBPF 副本）

core 不变量：**slave 必须先应用 backlog/快照，再应用 proxy 缓存的增量**。

### 4.1 部分重同步（can_continue）

```text
REPLSYNC(req_offset)
  ├─ proxy 不健康 → 拒绝，slave 退避重试（fail-closed）
  ├─ capture_disable=1
  ├─ client_ctl[3]=1（proxy → BUFFERING）
  ├─ repl_backlog_send_continue([req_offset, master_offset])   // 同步发完
  ├─ 等待 client_ctl[9]==1（proxy 确认 BUFFERING，最多 100ms）
  │    └─ 超时 → 会话失效
  ├─ recovery_mode=2, resume_offset=master_offset
  └─ capture_disable=0（此后新写进入 proxy cache）

REPLACK(applied >= resume_offset)
  ├─ recovery_mode=0
  └─ 所有副本都不在恢复期 → client_ctl[3]=0 → proxy flush cache
```

### 4.2 全量重同步

```text
queue_snapshot()
  ├─ client_ctl[3]=1（BUFFERING）
  ├─ snap_base = master_offset，发送 KVSD 快照（reactor 阻塞，无新写）
  └─ 返回后：等待 proxy BUFFERING → capture_disable=0
               （此后捕获的增量全部 > snap_base，进 cache）

REPLDONE
  ├─ recovery_mode=0
  └─ client_ctl[3]=0 → flush cache（快照 → 增量 顺序正确）
```

超时（部分 10s / 全量 60s）未完成 → `BARRIER_TIMEOUT` 会话失效，slave 重试。

---

## 5. Slave 重启恢复边界

`persist_recover()` 计算：

```text
recovered_frontier = dump 头 AOF offset + AOF 实际回放字节
```

`repl_slave_state_load(frontier)`：

```text
resume = min(saved_durable, recovered_frontier)
resume = min(resume, saved_applied)
applied = durable = resume   → 立即写回 .replstate
```

网络断线但进程未退出时不经过该函数，仍从 `applied` 续传。

> 注意：现有持久化假设 slave 本地 AOF 与 master 复制 offset 对齐（原设计如此）。
> 若 AOF 被外部工具改写/截断，frontier 只会更保守（回到 full resync）。

---

## 6. 修复的两个既有缺陷

### 6.1 backlog 在无副本时停止 feed

`repl_backlog_feed()` 原来在 `!g_replicas` 时直接返回。slave 断开期间 master
继续写入，这些字节既不在 backlog 也不在转发队列；重连时 partial sync 只能
补到旧的 `backlog_end`，`[backlog_end, master_offset)` 永久丢失。

修复：

- backlog 一旦分配，即使无副本也继续 feed（独立 master 仍不分配，省 10MB）；
- REPLSYNC 的 `can_continue` 额外要求 `backlog_end >= master_offset`，
  不满足强制 full resync。

### 6.2 kprobe 健康检查劫持 TCP 副本

`repl_kprobe_fwd_health_check()` 无条件运行；空闲 5s 后把所有非
`EBPF_TCP` 副本标记 `fwd_healthy=1`。`repl_broadcast` 看到该标记会跳过
正常 TCP 发送，而非 kprobe 传输下没有 ringbuf 转发 → 增量静默中断。

修复：该检查仅在 `repl_realtime_transport == kprobe-rdma` 时运行。

---

## 7. 可观测性（INFO 新增字段）

```text
repl_capture_active                capture 数据面是否开启
repl_proxy_healthy                 proxy 心跳是否新鲜
repl_proxy_restart_count           proxy 重启次数
repl_session_invalid_count         会话失效次数
repl_last_invalid_reason           NONE / PROXY_HEARTBEAT_STALE / CAPTURE_TRUNCATED
                                   / COMMIT_TIMEOUT / PROXY_CACHE_FULL / BARRIER_TIMEOUT
repl_capture_events                BPF ringbuf 成功记录数
repl_capture_bytes                 BPF 捕获字节数
repl_capture_truncated             BPF 截断次数（应恒为 0）
repl_forwarded_bytes               proxy 已发往 slave 字节数
repl_proxy_pending_bytes           proxy 转发队列积压
repl_proxy_cache_bytes             proxy fullsync/recovery 缓存
repl_proxy_dropped_bytes           capture gate 丢弃的字节数
repl_proxy_reconnects              proxy 重连 slave 次数
slave_recovered_frontier           本次启动恢复到的复制边界
slave_resume_offset                本次启动实际续传点
recover_frontier / recover_aof_bytes / recover_dump_frontier   (recover 文本块)
```

---

## 8. 测试

### 8.1 非 root 回归

```bash
make kvstore ebpf-proxy
make test_hash_ownership
make test_vsync_protocol 2>/dev/null || make test_fullsync_protocol
make check-kvstore
make check-slave-crash-recovery     # Slave kill -9 重启恢复
make test_commit_gate               # Commit Gate framing/状态机单元测试
```

`check-slave-crash-recovery` 覆盖：

1. TCP 主从增量复制正常；
2. `kill -9` slave 后 master 继续写入；
3. 重启后 `resume <= recovered_frontier`（且 frontier > 0）；
4. 追平后 `slave_repl_applied_offset == master_repl_offset`；
5. 断开期间写入的数据在 slave 上可读。

### 8.2 eBPF 真机（需 root）

```bash
make kvstore ebpf-proxy client_capture_bpf
sudo ./kvstore kvstore.conf --role master       # 自动 spawn build/ebpf_proxy
sudo ./kvstore kvstore.conf --role slave

# 故障注入
sudo kill -9 $(cat <dump_path>.ebpfproxy.pid)
# 继续写入，观察 master：
#   repl_session_invalid_count 增加
#   repl_last_invalid_reason = PROXY_HEARTBEAT_STALE
#   proxy 自动重启，slave 重连 REPLSYNC，最终数据一致
```

半包/多连接、大 payload、Proxy crash 等矩阵见方案 §28。

### 8.3 跨机实测记录（Master 6.1.176 / Slave 5.15.0）

拓扑：Master `192.168.233.128:5260`，Slave `192.168.233.129:5260`
（proxy 数据口 5261），`repl_fullsync_transport=rdma`（siw0）+
`repl_realtime_transport=ebpf+tcp`。

| 场景 | 结果 |
|---|---|
| `test_repl_5w5w`（全量 5w + 增量 5w） | **PASS 24 / FAIL 0**；Phase 5.5 确认 `rdma+ebpf-tcp`；10w 条数据逐条校验一致 |
| kill -9 Slave → 写 1w 条(458KB) → 重启 | partial 追平 `applied == master_offset`；10,001 条按序应用；`capture` 恢复后新增量继续；`truncated=0`、`session_invalid_count=0` |
| kill -9 ebpf-proxy | 心跳 2s 过期 → 会话失效 → proxy 自动重启 → Slave REPLSYNC 恢复 |
| Master 重启 | AOF 恢复数据；replid 变化 → Slave full resync；之后增量正常 |
| Slave 重启恢复边界 | `resume = min(saved_durable, recovered_frontier)`；AOF/durable 正常推进 |

跨机验证额外发现并修复：

1. **大 backlog partial sync**：`repl_backlog_send_continue` 走 reactor 的
   256KB 输出环，>256KB 必然失败。eBPF 副本改用转发队列（`..._queued`），
   +CONTINUE 头改为同步 `send` 后再入队，保证 header 先于 backlog。
2. **转发状态复用**：`repl_fwd_purge_conn` 只标 `dropped` 不移除链表，
   `conn_t` 地址复用后新连接会命中旧 state，数据永不发送；命中 stale state
   时重置。
3. **watermark 缺失**：同步发送 backlog 后未推进转发线程 watermark，
   REPLACK 追赶会重复补发；补 `repl_fwd_set_watermark`。
4. **backlog 惰性分配窗口**：副本连接后未写入即断开时，断开期写入不在
   backlog；REPLSYNC 时提前分配。
5. **proxy 重连阻塞控制面**：连不上 slave 时退避循环最长 5s 不读控制 map，
   导致 REPLSYNC 等待 BUFFERING 超时；退避循环中同样处理控制面。
6. **capture 使能顺序**：先置本地 recv clamp 再写 `capture_disable=0`
   （关闭时反之），消除大 recv 触发 `CAPTURE_TRUNCATED` 的竞态。
7. **io_uring 兼容性**：Slave 5.15 不支持 `COOP_TASKRUN/SINGLE_ISSUER`，
   原固定 flags 使 AOF 线程启动即退出；改为逐级降级到 plain 模式。

---

## 9. 已知限制与后续

- **单 slave 假设**：恢复期 capture gate 是全局的；多副本时一个副本重同步会
  让所有副本共用 BUFFERING 窗口（正确但吞吐下降）。
- **Commit Gate 已实现（阶段3）**：见第 10 节；主从复制只转发 Master post-commit
  的写命令，proxy 按 cookie 重组 RESP 并校验 seq/长度/CRC。
- **eBPF 截断 fail-closed** 依赖 `repl_capture_active` 的三后端 recv 限流；
  若绕过网络后端直接 recv（未来新路径），仍需自行限制到 32744。
- **partial sync 仍会尝试 RDMA**：Slave 混合分支在 REPLSYNC 后无条件后台连 RDMA，
  而 Master 对 partial 不会启动 RDMA listener，因此会打印连接失败/超时日志
  （不影响恢复，TCP 数据仍然正确）。后续可按 `+CONTINUE` 响应跳过 RDMA 尝试。


---

## 10. 阶段 3：Commit Gate

### 10.1 数据面

```text
Client → tcp_recvmsg(fexit) → 24B record{cookie, recv_seq, len, flags} → ringbuf
      → ebpf-proxy（按 cookie 重组 RESP）→ Master 的 WRITE/SKIP 描述符
      → global_seq 有序队列 → Slave
```

- BPF 只捕获 **cookie allowlist** 中的客户端连接（accept 时注册、变 replica/关闭时移除），
  避免副本控制/RDMA listener 等 socket 被捕获；返回码按 int 解释（EAGAIN 不误判）；
  `recv_seq` 只在实际输出 record 时分配（缺口可检测 capture 丢失）。
- Master 在每条命令 dispatch 前记录 `command_seq/raw_len/FNV-1a64`；写命令
  `repl_broadcast` 成功后发布 `WRITE{global_seq}`，其余（读/失败）发布 `SKIP`；
  批量 64 条经 Unix `SOCK_SEQPACKET` 发往 proxy。
- proxy 为每条连接维护 RESP framer（复刻 Master client 路径：RESP array、inline、
  `+` 控制行、空行、畸形头都按同一规则消费），命令 seq 从 BASELINE 开始；
  descriptor 的 seq/长度/CRC 全部匹配后才进入 `global_seq` 有序队列。
- 只有同时满足“完整命令 + 已提交 + global_seq 连续”才转发；任何缺口/错位/
  超时（2s）/通道断开 → `client_ctl[10]` → Master 会话失效 + REPLSYNC。

### 10.2 控制面协议（`<dump>.commit.sock`）

```text
HELLO{epoch}                 proxy 重置全部连接状态
BASELINE{cookie,next_seq}    (重新)捕获时的 seq 对齐基准
IGNORE/FORGET{cookie}        连接变 replica / 关闭
DESC_BATCH{WRITE|SKIP[]}     每条命令的提交结果
RESUME{marker}               恢复期结束：丢弃 global_seq<=marker，其余按序放出
```

### 10.3 恢复期对齐

- 正常 REPLSYNC/fullsync **不再关闭 capture**（否则 framer 会从半包重启）：
  BUFFERING 期间已提交写进入 recovery 缓冲；Master 在**立屏障时**
  （`repl_barrier_begin`，owner 线程阻塞等待 proxy 应答，期间不会提交新写）
  记录 `marker=repl_commit_global_seq()`；Slave ACK 追平 / REPLDONE 后由
  `repl_barrier_release()` 发送 `RESUME{marker}`：`<=marker` 的提交已由
  backlog/快照覆盖而丢弃，`>marker` 的按序转发。
- `RESUME` 幂等：重复或更旧的 marker 不会重放已释放记录，也不会丢弃 `>marker`
  的新写（单元测试 `test_commit_gate` 覆盖）。
- proxy 崩溃/重启后：Master 关闭 capture，等待所有 client 到达命令边界
  （5s 超时则关闭半包连接），发 BASELINE 后再打开 capture；旧 proxy 的
  framing 状态不参与新会话。

### 10.4 可观测性（INFO）

```text
repl_commit_active / repl_commit_channel / repl_commit_global_seq
repl_commit_descriptors / repl_commit_writes / repl_commit_skips
repl_commit_pending_bytes / repl_commit_timeout / repl_commit_desync /
repl_commit_gap / repl_commit_recovery_bytes / repl_commit_next_global_seq
repl_commit_ctxs / repl_commit_pending_cmds / repl_commit_oldest_pending_ms
repl_commit_mem_hard_hit
repl_commit_fault_reason
repl_session_invalid_count / repl_last_invalid_reason
repl_proxy_cache_over_high / repl_proxy_cache_hard_hit
hash_lookups/hits/misses/probe_steps/max_chain/slots/entries/rehashing
vsearch_queries/candidates/returned/total_us/max_us/topk_sum/dim_*
```

### 10.5 实测

| 场景 | 结果 |
|---|---|
| 混合读写 + inline + 半包 + 100 条 pipeline（本地 root eBPF） | HSET/SET/GET 全部正确；`writes=105`、`skips=5`、`global_seq=105`；`gap/desync/timeout/truncated=0`；Slave `applied == master_offset` |
| kill -9 ebpf-proxy | 1 次会话失效（`COMMIT_CHANNEL`）；proxy 自动重启；Slave REPLSYNC 追平，0 timeout/gap |
| 单元测试 `test_commit_gate` | RESP/inline/控制行/空行/畸形/半包；CRC/seq/recv_seq 缺口；全局序；BASELINE 重定基；RESUME 丢弃/放行；pending 超时 — 全部 PASS |

### 10.6 部署注意

- `kvstore`、`ebpf-proxy`、`repl_client_capture.bpf.o` 必须**成对升级**（record 格式
  与 client_ctl 语义已变），并先停掉旧 ebpf-proxy。
- `<dump_path>.commit.sock` 所在目录需 Master/proxy 可写；手动启动 proxy 时需传
  `--commit-sock` 与 Master 相同路径。
- Commit Gate 故障按 fail-closed 处理：当前复制会话失效 + Slave REPLSYNC；
  proxy 重启后的重新对齐阶段可能短暂关闭 capture（客户端保持连接即可）。
