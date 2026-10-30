# 多 Slave 复制控制面设计（M0/M1/M2）

> 状态：**M1 + M2 主体已实现**（2026-10-28），实现细节见 §11；M3（共享 payload /
> 每副本 RDMA）未做。§2 的"现状"保留为改造前快照，便于对照。
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

### M0（无行为改动，已交付）

- [x] 不变量定义（本文 §3）
- [x] 全局状态清单（§2）
- [x] 状态机与场景推演（§6/§7）
- [x] 2/3 Slave 本地测试环境（`tools/tests/test_multi_slave.sh`）

### M1（2-Slave MVP，已实现）

- [x] proxy `g_slave` → `replica_ctx[]`；每副本独立 TCP 连接、发送 FIFO、发送线程。
- [x] `proxy_cfg` 按 slot 下发 `slave_addr.<slot>` / `slave_port.<slot>` /
      `replica_gen.<slot>`；Master 在 REPLSYNC 时写独立地址（gen 是提交点）。
- [x] commit 输出 fan-out：全局序只解析一次，`LIVE` 副本直接入各自 FIFO；
      处于 barrier/recovery 的副本进 Commit Gate 的 **per-replica** 恢复缓冲。
- [x] partial/fullsync 先 TCP-only；多副本配置成 rdma fullsync 时拒绝第二个副本。
- [x] 保留全局 Commit Gate/framer；全局故障仍 fail-closed。

### M2（独立恢复/独立流控，已实现主体）

- [x] 移除全局 BUFFERING：`client_ctl[16 + slot*4 + {BARRIER,ACK,FAULT}]` 是
      per-replica 控制位（BARRIER/ACK 用单调 seq 握手）；proxy 按 slot 进入/离开恢复态。
- [x] Master barrier 改为 per-connection（`conn_t.repl_barrier_*`），
      RESUME 携带 `replica_id`，A 的 REPLACK 不会解除 B 的屏障。
- [x] 慢副本限额/断连：proxy 单副本 FIFO 128MB、恢复缓冲 128MB；超限只隔离该 slot
      （写 `SLOT_FAULT=1`），Master 在 REPLACK 时只拒绝该副本。
- [ ] INFO 的 per-replica 明细（`phase`/`lag_bytes`/`disconnect_reason`）尚未逐字段
      暴露；当前通过 proxy stderr、`proxy_stats`/`client_stats` 聚合和 Master 的
      `connected_slaves`/offset 观察。
- [x] 全局 capture 只因全局故障（capture 截断 / commit 通道）关闭。

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

## 11. 实现说明（2026-10-28，M1/M2 主体）

### 11.1 控制面

| 通道 | 键/字段 | 语义 |
|---|---|---|
| `proxy_cfg` | `slave_addr.<slot>` / `slave_port.<slot>` | 该副本数据通道地址（slot 0 同时写旧键 `slave_addr/slave_port` 兼容） |
| `proxy_cfg` | `replica_gen.<slot>` | **提交点**：每次 REPLSYNC 递增；0=注销。proxy 只有在 gen 变化时重读地址，避免半更新状态被当成新副本 |
| `client_ctl` | `16 + slot*4 + 0` BARRIER | Master→proxy：**单调递增的 seq**（0=未立/放行）。seq 变化即新屏障，避免漏看边沿 |
| `client_ctl` | `16 + slot*4 + 1` ACK | proxy→Master：回显已进入的 BARRIER seq；0=已离开恢复态 |
| `client_ctl` | `16 + slot*4 + 2` FAULT | proxy→Master：该副本缓冲溢出/数据面失败，需要重同步 |
| commit socket | `repl_commit_msg_t.replica_id` | RESUME(marker) 的目标 slot；0 表示 slot 0/旧版单副本 |

slot 的 `< 8` 上限与 `KVS_MAX_REPLICA_SLOTS`、BPF `client_ctl/proxy_cfg` 的
`max_entries` 保持一致。slot 身份是 **(Slave 源 IP, proxy 数据端口)**：同一 Slave
重连复用原 slot，proxy 重同步其状态；不同 Slave 即使同机也按端口区分。

### 11.2 数据面

- Commit Gate 仍只做一次 RESP framing / 描述符匹配 / `global_seq` 排序。
- 每条已提交 WRITE 在 `proxy_commit.c` 里按 `g_replica_mask` 分发：
  - 副本处于 buffering → 追加到该副本独立的 recovery 链表（单副本 128MB 上限）；
  - 副本 LIVE → 调 `commit_emit(slot, seq, buf, len)`，进入该副本 FIFO。
- 每个副本一个发送线程：独立 `connect/writev`、指数退避、512KB 批量、partial-write
  续写、失败节点回队首。`in_flight` + `barrier_gen` 保证 barrier 应答前在途批次静默，
  barrier 后丢弃 barrier 前的残余（已被快照/replay 覆盖）。
- `RESUME(slot, marker)`：Commit Gate 只丢弃该 slot 的 `<=marker` 记录，其余按序
  交给该 slot 的 FIFO；其他副本完全不受影响。

### 11.3 配置与限制

- 多副本 fullsync 仅支持 `--repl-fullsync-transport tcp`；配置 `rdma` 时第二个副本的
  REPLSYNC 会被拒绝（RDMA QP/MR/快照仍是进程级单例，M3 再扩展）。
- Slave 的 proxy 数据端口：
  - `rdma+ebpf-tcp`：专用 listener（`--repl-proxy-port`，缺省 `master_port+1`）；
  - `tcp+ebpf-tcp`：直接使用 Slave 主端口（主端口写路径在 `!from_replication` 时
    推进 replicated offset），REPLSYNC 第 5 参数把该端口告知 Master。
- 测试：`sudo make check-multi-slave`（非 root 自动 SKIP）。脚本在同一主机上用
  7810/7811 两个主端口 + 7801/7802 两个 proxy 端口启动 A/B，覆盖双活、B kill、
  B 强制 fullsync、proxy kill 自动拉起。

### 11.4 实测记录（2026-10-28）

- **本机 2 Slave**（`make check-multi-slave`，6.1，root+eBPF）：A/B 同时 fullsync →
  round1 双端一致；kill -9 B 后 A 持续复制、r2 只在 master/A 校验通过；B 删除
  dump/aof/replstate 重启强制 fullsync 时继续写 r3，A 正常、B 追平；kill ebpf-proxy
  后自动拉起、A/B 各自恢复；末尾的 silent-replica 场景中，一个只发 partial
  REPLSYNC、不回 REPLACK 的假副本在 hold 超时后被**单独拒绝**，
  `repl_session_invalid_count` 不变，A/B 链路与数据不受影响。RESULT: PASS。
- **跨机 2 Slave**（Master `192.168.233.128` 6.1，Slave B `192.168.233.129` 5.15，
  Slave A 本机）：同机/跨机两个 slot 同时 fullsync；r1 三端逐字节一致；
  kill 远端 B 后 r2 只在 Master/A 校验通过；远端 B 重启走
  **partial resync** `[6640,13280)`，REPLACK 追平后 `RESUME slot=1 marker=80`，
  三端 offset=13280、r1/r2 全部一致。跨机 slot 注册
  `ip=192.168.233.129 proxy_port=7831` 由 REPLSYNC 第 5 参数得到，proxy 直连
  远端主端口。
- `check-commit-gate-faults`（5 轮 kill proxy/slave）在本次改动后仍 PASS，
  timeout/desync/gap=0、主从 offset 一致。
- 单副本 `check-barrier-timeout` 仍无法在本地构造 partial barrier：
  唯一 Slave 离线期间 `g_replicas == NULL`，backlog 不喂入并标记不连续，
  重启只能 fullsync（既有行为）。per-replica 超时由 `check-multi-slave` 的
  silent-replica 场景覆盖。

### 11.5 2026-10-29 评审整改

按《PocketKV_多Slave_feature分支源码评审与改进方案》逐项处理：

| 评审项 | 处理 |
|---|---|
| P0 数据通道断线后续发半条 RESP | `replica_send_batch()` 任何 `writev` 错误/peer EOF 都**不再回退 FIFO**：未发节点直接丢弃、`failed=1`、挂起 `fault_pending`；主线程 `poll_replica_faults()` 统一 `proxy_commit_set_replica(0)` + `SLOT_FAULT` + 断开。发送线程不直接改 Commit Gate 全局状态。Slave 必须重连 REPLSYNC。 |
| P0 普通客户端可写 Slave | 新增 `REPLDATA <token>` 数据入口握手：Slave 进程级 64-bit 随机 token 随 REPLSYNC 第 6 参数上报，Master 经 `replica_token.<slot>` 交给 proxy；proxy 连接后先握手，Slave 校验后才置 `c->is_replication_data=1`。主端口与 rdma+ebpf-tcp 的专用 proxy listener **都**要求该握手。ROLE_SLAVE 下所有普通客户端一律 READONLY；错误 token 直接断开。 |
| P1 复制 AOF/durable 路径不统一 | proxy 数据连接与 `from_replication` 共用同一段 apply → AOF prepare → applied → durable 逻辑；修正了“先 durable 后 applied 导致 durable 永久落后最后一条命令”的顺序问题。**durable 仍表示 AOF 写队列边界，不等于 fsync**；真实 fsync-durable offset 作为后续项。 |
| P0/P1 fullsync 失败仍 release | `queue_snapshot()` 失败改为 `repl_barrier_cancel(c)`（绝不发 RESUME）、拒绝副本、复位 `g_repl_fullsync_in_progress/repl_fullsync_pending`，RDMA 配置下调用 stop/transfer cleanup。 |
| P1 陈旧 RESUME 无 slot 代次 | `RESUME` 增加 `replica_gen` + `barrier_seq`；proxy 仅当二者与当前 slot 状态一致时才放行恢复缓冲，失配只记录并忽略（不得释放新缓冲）。单测新增“拒绝的 RESUME 不释放 recovery”。 |
| P1 B fullsync 重置全局 backlog | `queue_snapshot()` 只在 `repl_backlog_contiguous()==0`（确实有历史缺口）时 reset；有 LIVE 副本保持连续时保留历史。新增测试：冻结 A 使 applied 落后于 B 快照边界，B fullsync 后 A 仍走 partial。 |
| P2 故障通知依赖 REPLACK | owner 线程 `repl_barrier_tick()` 周期检查 `SLOT_FAULT` 并立即摘除该副本；REPLACK 检查保留作兜底。 |
| P2 配置部分写入 | `slave_addr/slave_port/replica_token` 三键全部成功后才发布 `replica_gen`；任一失败撤销并拒绝该副本。 |
| P2 测试加固 | 稳定 RNG（sha256 seed）、`WaitSynced` 要求 applied==master 且 durable==applied、普通客户端写 Slave 断言 READONLY、SET/DEL/MSET 交错 + 同长度覆盖、30KB value + kill B、B fullsync 后 A partial、silent replica 单独拒绝、精确 PID 清理。 |
| P2 per-socket lock / INFO per-replica 明细 / 多副本 RDMA / 真 fsync durable | **未在本次修改**：按评审建议先做锁等待/性能采样再动 per-socket lock；INFO 明细和每副本 RDMA 属于后续阶段。 |

回归（2026-10-29，本机 root+eBPF）：
- `check-multi-slave`（40 keys/轮）：全部场景 PASS，含 security、mixed、30KB、PR-6 partial、proxy kill、silent replica。
- `check-commit-gate-faults` 5 轮 PASS：mismatches=0、timeout/desync/gap=0、最终 offset=353936。
- 跨机（128 6.1 Master + 129 5.15 远端 Slave）再次 PASS：REPLDATA 握手、partial resync、gen/seq RESUME 正常。
- `check-slave-crash-recovery`、`test_commit_gate`、`test_hash_ownership`、`test_fullsync_protocol` PASS。

仍存限制（明确不宣称已解决）：
- `check-barrier-timeout` 的单副本 SIGSTOP 方案在本地仍只能 fullsync（无副本期间 backlog 不 feed），per-replica 超时由 multi-slave 的 silent-replica 场景覆盖。
- `durable` 语义为 AOF 写队列边界；`aof_fsync=always + async group commit` 下崩溃窗口仍由 AOF group window 决定。
- 多副本 RDMA、per-socket lock、50KB/1MB 单命令（>32KB capture 限制）、8 slot 压测未覆盖。
- 数据通道“半条 RESP + RST”目前通过 30KB value + kill B 概率性覆盖；缺少确定性的
  proxy 发送中途 RST 注入钩子（评审 PR-1 的专项验收仍建议补）。
- RESUME 的跨通道错代竞态已做协议绑定与单测，但尚未构造“旧 RESUME 延迟送达”
  的端到端故障注入。
