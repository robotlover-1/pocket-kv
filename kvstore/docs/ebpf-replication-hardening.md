# eBPF 增量复制加固（阶段 1 + 2）

对应方案：`PocketKV_eBPF保留转发_优化实施方案.md` 的 **阶段 1（确定性 Bug）** 与
**阶段 2（proxy 故障恢复）**。本阶段保留
`tcp_recvmsg → ringbuf → ebpf-proxy → Slave` 数据面，只补齐正确性与可恢复性；
阶段 3（Commit Gate / socket_cookie / Unix commit channel）尚未实现。

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
| 7 | capture_disable | **1=停止捕获**（故障恢复/REPLSYNC 追赶期间） |
| 8 | capture_fault | **1=捕获不可信（截断）**，fail-closed |
| 9 | proxy_state | **proxy 回执**：0=FORWARDING，1=BUFFERING |

BPF fexit 依次检查：pid → `capture_disable` → `capture_fault` → retval。
`retval > CLIENT_ENTRY_MAX_LEN` 时：

```text
ST_TRUNCATED++ → client_ctl[8]=1 → 丢弃该 record（不输出截断数据）
```

master 健康线程观察到 fault 后：

```text
capture_disable=1
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
make check-slave-crash-recovery     # 新增：Slave kill -9 重启恢复
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

---

## 9. 已知限制与后续

- **单 slave 假设**：恢复期 capture gate 是全局的；多副本时一个副本重同步会
  让所有副本共用 BUFFERING 窗口（正确但吞吐下降）。
- **阶段 3 未做**：capture 仍按"recv 原始字节流"转发，没有 Commit Gate；
  读命令、失败命令仍会被 eBPF 捕获并转发（方案 §9/§25）。
- **proxy cache**：仍是单阈值 256MB 淘汰；方案 §20 的 192/128MB 高低水位未做。
- **eBPF 截断 fail-closed** 依赖 `repl_capture_active` 的三后端 recv 限流；
  若绕过网络后端直接 recv（未来新路径），仍需自行限制到 32764。
