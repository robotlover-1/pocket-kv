# 多 Slave 复制控制面设计（M0/M1/M2）

> 状态：**设计评审稿**（对应《PocketKV 剩余问题修复与多 Slave 扩展实施方案》Task 3）。
> 本文只定义目标、不变量与分阶段落地方式，**不包含 M1/M2 的代码实现**。
> 基准：`pocket-kv` main（Commit Gate 适配 session/barrier 之后）。
> 原则：保留单份 eBPF capture 与单份 post-commit Commit Gate；只把"发送/恢复/流控"
> 下沉为 per-replica 状态。

## 1. 结论

当前 Master 的**控制面已经部分按连接设计**（`g_replicas` 链表、每连接
`repl_offset_sent` / `repl_applied_offset_ack` / `repl_durable_offset_ack` /
`repl_recovery_mode` / `repl_transport_kind`），但 **proxy 数据面仍是单目标**：

- `src/ebpf_proxy/main.c` 只有一个 `g_slave`、一个转发队列、一份 `g_cache`；
- `client_ctl[3]`（FULLSYNC/BUFFERING）、`client_ctl[11]`（cache 背压）是全局位；
- Master 的 barrier 也是全局单例（`g_barrier_active` / `g_barrier_end_offset`）。

因此：

| 目标 | 结论 |
|---|---|
| 仅让 2 个 Slave 收到增量、不保证独立恢复 | 中等改动，**不建议作为最终方案**（共享状态串扰风险高） |
| 2–3 Slave 正确复制 + TCP fullsync + 独立恢复 | **推荐 M1/M2 目标**，改动中等偏大 |
| N Slave 高吞吐 + 共享 payload + 独立 RDMA fullsync | M3，暂缓 |

**近期（单 Slave 可靠性展示）优先完成故障注入与一致性门禁，不并发推动多 QP 重构。**

## 2. 现状全局状态盘点（M0 清单）

| 状态 | 位置 | 当前粒度 | 多 Slave 目标 |
|---|---|---|---|
| `g_replicas` 链表 | `src/main/kvstore.c` | 每连接 | 保留 |
| `conn_t.repl_*` 发送/ACK 游标 | `include/kvstore/kvstore.h` | 每连接 | 保留，补 `replica_id`/`epoch`/`phase` |
| `g_barrier_active` / `g_barrier_end_offset` | `src/main/kvstore.c` | 全局单例 | 改为 per-replica barrier 或按 replica 集合引用计数 |
| `g_repl_session_id` | `src/replication/kvs_repl.c` | 全局会话 | 保留（commit 全局序仍共享），每 Slice 记录自己加入时的 epoch |
| `g_repl_backlog` | `src/replication/kvs_repl.c` | 全局单份 | 保留单份；按 `offset` 定位，不按 Slave 复制 |
| `client_ctl[3]` BUFFERING | pinned map | 全局 | 拆分：全局 capture 健康位 + per-replica 恢复位（新 map 或 key 区间） |
| `client_ctl[11]` cache 背压 | pinned map | 全局 | 改为 per-replica 背压聚合（慢副本不应永久背压所有客户端） |
| proxy `g_slave` / `g_cache` / forward queue | `src/ebpf_proxy/main.c` | 单实例 | `replica_ctx[]` + 每副本队列/cache |
| Commit Gate `global_seq` / framer / pending | `src/ebpf_proxy/proxy_commit.c` | 全局单份 | **保持单份**，输出 fan-out 到各副本游标 |
| `repl_session_invalid_count` / `repl_last_invalid_reason` | `src/replication/kvs_repl.c` | 全局 | 保留全局数据面失效；另加 per-replica 断开原因 |

## 3. 不变量（M0 交付物）

任何实现都必须满足：

1. **单次提交**：每条成功写只在 Master 产生一个 `global_seq`；同一命令的所有副本
   看到相同的 `(command_seq, raw_len, crc, global_seq)`。
2. **顺序**：每个 Slave 按 `global_seq` 严格递增应用；允许不同 Slave 进度不同，
   不允许同一 Slave 内乱序。
3. **不重复**：`global_seq <= resume_marker` 的记录不得再次发给该 Slave；marker
   必须与快照/backlog replay 覆盖的边界一致。
4. **不静默丢**：任一 framing/CRC/seq/global 缺口、内存上限或通道断开都必须
   fail-closed：失效该副本（或全局 capture），重连后 partial/full 重建。
5. **故障隔离**：一个 Slave 的 fullsync/慢消费/断线不得中断其他 LIVE 副本的增量。
6. **epoch 隔离**：旧 epoch 的 ACK、cache、Commit 描述符不得注入新 session。
7. **有界资源**：每副本队列、pending、recovery 均有上限；超限断开该副本，而不是
   拖垮全部副本。

## 4. 目标数据结构

```c
/* 共享：不可变命令记录的引用（M1 允许先每副本拷贝，M3 再共享 refcount） */
typedef struct repl_record {
    uint64_t global_seq;
    uint64_t start_offset, end_offset;
    uint32_t len;
    const unsigned char *payload;   /* 只读；生命周期由 log/refcount 管理 */
} repl_record_t;

typedef enum {
    REPL_PHASE_LIVE,     /* 正常实时转发 */
    REPL_PHASE_FULL,     /* 全量快照传输中 */
    REPL_PHASE_CATCHUP,  /* backlog replay 中，等待 ACK 到 marker */
    REPL_PHASE_OFFLINE,  /* 断线/慢副本被摘除，等待重连 */
} repl_phase_t;

typedef struct repl_replica_ctx {
    uint64_t replica_id;
    uint64_t session_epoch;
    int control_fd;          /* Master 控制连接 */
    int data_fd;             /* proxy → Slave 数据通道（proxy 侧持有） */
    uint64_t next_offset;    /* 下一个要发送的复制 offset */
    uint64_t acked_offset;
    uint64_t durable_offset;
    uint64_t resume_marker;  /* 本次 partial/full 覆盖到的 global_seq */
    repl_phase_t phase;
    /* per-replica forward queue / cache / backpressure / last_ack_ms ... */
} repl_replica_ctx_t;
```

Commit Gate 仍只产出**一份**有序 `WRITE` 流；fan-out 层为每个 `LIVE` 副本维护独立
游标与队列，`FULL/CATCHUP` 副本进入自己的 recovery 缓冲，不阻塞 LIVE 副本。

## 5. 控制协议变更（Master ↔ proxy）

现有 proxy 只接受单组 `slave_addr/slave_port`。M1 需要给每条控制消息加
`replica_id` + `session_epoch`：

```text
REGISTER   {replica_id, epoch, slave_addr, slave_port}
UNREGISTER {replica_id, epoch}
BUFFERING  {replica_id, epoch, base_offset, resume_marker_placeholder}
RESUME     {replica_id, epoch, marker}
DISCONNECT {replica_id, epoch, reason}
```

- `REGISTER/UNREGISTER` 可继续走 `proxy_cfg` map（key 加 replica 前缀），
  也可以新增一条 Unix 控制通道；推荐复用现有 commit socket 的扩展消息类型，
  避免第三套控制面。
- 向后兼容：单 Slave 时 `replica_id=0`，行为与当前一致。
- `HELLO` 仍清空全部；`REGISTER` 只创建/重置对应副本。

## 6. per-replica 状态机

```text
                 REPLSYNC(partial)
   OFFLINE ─────────────────────────► CATCHUP ── ACK>=marker ──► LIVE
      ▲                                   │
      │ 断线/超时/慢副本                   │ 需要 fullsync
      │                                   ▼
      └──────────────────────────────  FULL ── REPLDONE ──► LIVE
```

关键规则：

- `LIVE`：直接消费共享 commit 流；自己的队列满/发送慢 → 断开，转 `OFFLINE`，
  不影响其他副本。
- `CATCHUP`：Master 只向该副本 replay backlog 到 `catchup_end`；proxy 对该副本
  的提交进入该副本 recovery 缓冲；ACK `applied >= marker` 后发 `RESUME(marker)`，
  再 flush 该副本缓存。
- `FULL`：快照只生成一次（或每副本独立生成，取决于实现），该副本数据通道暂停
  直发；`REPLDONE` 后 `RESUME(marker)`。
- 全局 `capture_fault/capture_enable` 只由**真正的全局数据面故障**（BPF 截断、
  commit 通道断、所有副本均不可用）触发；单副本故障不关闭全局 capture。

## 7. 关键场景推演

### 7.1 A LIVE，B 开始 fullsync

```text
A: LIVE ─────────────── 持续收到增量（不受影响）
B: LIVE → FULL(snapshot) → CATCHUP → LIVE
proxy: 全局 commit 流不变；B 单独进入 recovery 缓冲
client_ctl[3]: 不再作为全局 BUFFERING 使用
```

### 7.2 A 正常，B kill -9

A 不触发任何重同步；B 重连后按自己的 `next_offset` 判断 partial/full。
Master `g_replicas` 摘除 B 时**不得**结束全局 session（当前 `repl_unlink_replica_locked`
已按"最后一个副本"判断，需继续沿用）。

### 7.3 proxy kill -9

全局 capture 失效 → 所有 eBPF 副本进入 `OFFLINE`；proxy 重启后各自
`REGISTER + BASELINE + RESUME`，旧 epoch 状态全部丢弃。M1 验收要求 A/B 都恢复，
且不能把旧 cache/旧 ACK 注入新 session。

### 7.4 A/B 同时 REPLSYNC

每个副本独立 `catchup_end/marker/ACK`；B 的 ACK 不能解除 A 的屏障（当前全局
`g_barrier_*` 必须改为 per-replica 或引用计数）。

### 7.5 backlog 被覆盖

只有落后的那个副本脱离并 fullsync；另一个副本继续 partial。`repl_backlog_feed`
的"无任何 Slave"判断需区分：

- 没有任何副本：不 feed，标记历史不连续；
- 某副本离线：仍 feed（其他副本需要），离线的那个重连时按自己的 offset 判断；
- 全局 backlog 缺口：所有副本 partial 均拒绝。

## 8. 分阶段落地

### M0（无行为改动，本次已交付文档）

- [x] 不变量定义（本文 §3）
- [x] 全局状态清单（§2）
- [x] 状态机与场景推演（§6/§7）
- [ ] 2/3 Slave 本地测试环境（M1 同步实现）

### M1（2-Slave MVP，先正确）

1. proxy `g_slave` → `replica_ctx[]`；每副本独立 TCP 连接、forward queue、cache。
2. `proxy_cfg` 控制项加 `replica_id`；Master REPLSYNC 时为每个副本写独立地址。
3. commit 输出 fan-out：`LIVE` 副本按各自游标入队；`FULL/CATCHUP` 副本进各自
   recovery 缓冲。
4. partial/fullsync 先 **TCP-only**；落后副本直接断开重同步。
5. 保留全局 Commit Gate/framer；所有故障 fail-closed。

### M2（独立恢复/独立流控）

1. 移除全局 BUFFERING 语义，改为 per-replica phase + marker。
2. Master barrier 改为 per-replica（或按活跃副本集合引用计数）。
3. 慢副本限额/断连策略；INFO 增加 per-replica 指标
   （`replica_id`、`epoch`、`phase`、`sent/acked/durable`、`lag_bytes`、`queue_bytes`、
   `disconnect_reason`）。
4. 全局 capture 只因全局故障关闭。

### M3（性能/RDMA，可选）

- 不可变 shared record + refcount，减少多副本拷贝；
- 每副本独立 RDMA QP/MR 与 fullsync 生命周期；
- 批量 writev/sendmsg、CPU/NUMA 调优与对比基准。

## 9. 测试矩阵（M1/M2 验收）

| 场景 | 验收 |
|---|---|
| 2 Slave 正常，混合 SET/GET/pipeline/50KB value | A/B 与 Master 一致，offset 追平 |
| A LIVE，B fullsync | A 持续收到增量，不因 B BUFFERING 停止 |
| A 正常，B kill -9 | A 零额外重同步；B partial/full 后追平 |
| B 很慢/backlog 被覆盖 | 仅 B 脱离并 fullsync；A 正常 |
| proxy kill 后自动启动 | 旧 epoch 不重放；A/B 各自恢复 |
| A/B 同时 REPLSYNC | 各自 marker/ACK 独立，不互相解除屏障 |
| 3 Slave 长时间压测 | 资源有界、最终一致、无 RSS 泄漏 |

采集字段：`replica_id, epoch, phase, sent_offset, ack_offset, durable_offset,
lag_bytes, queue_bytes, fullsync_count, partial_count, disconnect_reason`。

## 10. 风险

- **顺序/重复**：最大风险在"快照/backlog replay"与"proxy 实时缓存"的边界；
  RESUME marker 必须来自同一 owner 线程的屏障时刻（当前实现已在
  `repl_barrier_begin` 记录）。
- **全局位语义迁移**：任何仍被多个副本共享的 `client_ctl` 位都要逐个确认。
- **资源生命周期**：快照文件、RDMA MR/QP、per-replica cache 必须独立回收。
- **不要**在没有 M0 不变量与 2-Slave 测试的情况下直接上 M1 大补丁。
