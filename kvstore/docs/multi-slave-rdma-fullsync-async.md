# 多 Slave 真正并发 RDMA 全量同步（async fullsync）

> 实现依据：`PocketKV_真正并发多Slave_RDMA全量同步_实施方案_2026-10-29.md`。
> 旧单实例实现（`g_repl_rdma_ctx`）保留用于纯 RDMA / kprobe-rdma 实时路径；
> 本文描述的是 **fullsync=rdma + realtime=ebpf/ebpf+tcp/tcp** 下的新并发路径。

## 1. 启用条件

```bash
# Master 与 Slave 配置相同：
--repl-fullsync-transport rdma
--repl-realtime-transport ebpf+tcp     # 或 ebpf / tcp
--repl-transport tcp                    # 不能是 rdma / kprobe-rdma

# 可选：
--rdma-fullsync-max-concurrent 2        # 同时处于 RDMA_SENDING 的 job 上限（默认 2）
--rdma-fullsync-pause-us 0              # 每个 WRITE chunk 间隔（µs），用于限速/慢副本测试
--rdma-fullsync-repldone-timeout-ms 60000  # FULLSYNCEND 后等待 REPLDONE 的任务级超时
--rdma-fullsync-max-connected 4        # 同时持有 QP/CQ/MR 的 job 数上限（资源预算）
--rdma-fullsync-max-registered-mb 64   # 所有活动 job 发送 buffer MR 的总预算

# 同机部署（Master/Slave 在同一主机）必须显式指定 RDMA 端口：
--rdma-port 5381
```

> **同机端口冲突**：Slave 的实时 proxy listener 会绑定 `master_port+1`；RDMA 默认
> 端口也是 `master_port+1`。同机测试若不显式指定 `--rdma-port`，CM listener
> 绑定失败会明确回退 TCP 全量（与旧实现行为一致）。跨机部署无此问题。
> 集成测试脚本已默认使用 `master_port+10`。

`rdma_fs_should_enable()` 在以下情况返回 0（继续使用旧单实例路径）：

- `repl-fullsync-transport != rdma`；
- `repl-transport=rdma` 或 `kprobe-rdma`（纯 RDMA 实时/Kprobe 实时）；
- `repl-realtime-transport=rdma|kprobe-rdma`。

## 2. 架构

```
Master owner（reactor）
  REPLSYNC A/B
    -> per-slot Barrier + catchup_end（复用现有 M1/M2）
    -> queue_snapshot_async():
         owner 生成不可变快照（各自唯一文件、各自 snapshot_offset）
         rdma_fs_begin() 创建 job
         rdma_fs_snapshot_ready() 移交 fd 给 job
         发送 +FULLRESYNC(... rdma-write-v2 <epoch>)
         立即返回事件循环
  reactor tick -> repl_rdma_fs_tick()
         rdma_fs_drain_events_owner() 取 WR_DONE/FAILED
         WR_DONE -> 在 owner 线程发 +FULLSYNCEND <tid>
         FAILED  -> 只 cancel 该副本 Barrier 并 reject 该连接

RDMA fullsync manager（进程级）
  CM listener（懒启动：首次需要 RDMA 全量时绑定 rdma_port；绑定失败→TCP 回退）
    解析 CM private_data hello(tid, epoch) -> 查找 job
    资源预算检查（max_connected / max_registered_bytes），超额明确拒绝
    为 job 创建独立 PD/CQ/comp_chan/QP/send slots，accept
  job worker（每 job 1 个线程）
    WAIT_HEADER：等 owner 完整写出 FULLRESYNC（CM 超时从此才起算）
    wait established + remote MR + snapshot
    acquire 并发令牌
    用本 job 的 QP/CQ/slots 循环 RDMA WRITE（可与其他 job 真正重叠）
    全部 CQE 成功 -> WR_DONE；任何 CM/CQ/超时/资源错误 -> FAILED
  CM 线程统一销毁 QP/CQ/PD/child id（worker join 之后）
```

### 2.1 资源归属

每个 job 独占：

- `rdma_cm_id`（child connection）、`ibv_pd`、`ibv_cq`、`ibv_comp_channel`、`ibv_qp`；
- `send slots`（默认 16 × chunk，配置来自 `--rdma-send-slots/--rdma-chunk-size`）；
- 远端 `addr/rkey/capacity`、`write offset`、`posted/completed` 统计；
- 不可变快照 `fd/path/size`。

worker 与 CM 线程不访问 `conn_t`；所有 TCP 控制写（FULLSYNCEND/FULLSYNCABORT）
由 owner 线程执行。REPLDONE 按 `transfer_id + slot + replica_gen` 校验。

## 3. 协议变化

| 阶段 | 旧 | 新（v2） |
|---|---|---|
| REPLSYNC 能力协商 | 无 | 最后一个参数 `rdma-fs-v2`；Master 据此决定 async RDMA 或 TCP 回退 |
| FULLRESYNC | `... <tid> rdma-write` | `... <tid> rdma-write-v2 <master_epoch>` |
| RDMA CM hello | 无 | 24 字节 v1：`version/flags/reserved/tid/epoch`（网络字节序） |
| FULLRESYNCWR | 不变 | `<tid> <addr> <rkey> <capacity>`，由 owner 解析并按 tid 路由 |
| FULLSYNCEND | `<tid>` | 不变；仅在全部 WRITE 成功完成后发送 |
| REPLDONE | `REPLDONE` | v2 为 `REPLDONE <tid>`；旧对端仍发无参形式 |

兼容性：

- 新 Master + 旧 Slave：旧 Slave 的 REPLSYNC 不带 `rdma-fs-v2` → Master 明确走
  TCP sendfile 回退（header 的 tid=0），不会让旧 Slave 连新的 CM listener。
- 旧 Master + 新 Slave：新 Slave 多带一个参数；旧 Master 忽略/按 0 处理，
  FULLRESYNC 不带 epoch → Slave 不发送 CM hello，继续旧单实例 RDMA。

### 3.1 连接级 fullsync mode 与身份校验（评审 P0 修复）

`conn_t.repl_fs_mode` 显式记录当前连接的全量模式，REPLDONE 不再通过“消息有没有
tid”反推协议：

| mode | 含义 | REPLDONE 规则 |
|---|---|---|
| `NONE` | 没有活动全量 | 一律忽略，不释放 Barrier |
| `V2_RDMA` | 新并发 RDMA | 必须带 tid == `c->repl_transfer_id`，且 job 处于 `WAIT_REPLDONE` |
| `TCP_FALLBACK` | 明确的 TCP sendfile 回退 | 只接受无参旧式 REPLDONE |
| `LEGACY_RDMA` | 旧单实例 RDMA | 只接受无参旧式 REPLDONE |

同一控制连接已有 `is_replica` / `repl_fullsync_pending` / 非 NONE 模式时，**重复
REPLSYNC 直接拒绝并断开**，不在旧 Barrier 上叠新 job；`rdma_fs_begin()` 也防御
同一 `(owner_conn, slot)` 的活动 job。

owner 处理 `WR_DONE/FAILED` 事件时校验
`tid + slot + replica_gen + barrier_seq + 连接当前 tid/mode`；旧 job 的事件只
cancel/丢弃，绝不会对新 Barrier 发 END/ABORT/reject。

### 3.2 FULLSYNCEND 与控制面闭环（评审 P1 修复）

- `repl_fullsync_send_end()` / `repl_fullsync_abort_master()` 返回 int：循环累计
  偏移，短写/EAGAIN 超时/断线返回 -1；失败时该副本 fail-closed（cancel Barrier +
  reject），不会静默停在 `WAIT_REPLDONE`。
- owner 仅在 END **完整写出**后调用 `rdma_fs_mark_fullsyncend_sent()`，job 才进入
  `WAIT_REPLDONE` 并设置 deadline。
- `WR_DONE`（待发 END）与 `WAIT_REPLDONE` 都有任务级 deadline
  （`--rdma-fullsync-repldone-timeout-ms`，默认 60s，最小 1s）；超时只失败该 job，
  下一轮 drain 生成 FAILED 事件，仅拒绝该副本。

### 3.3 CQ / 生命周期加固

- CQE 必须 `IBV_WC_SUCCESS` + `IBV_WC_RDMA_WRITE` + `wr_id` 对应仍 in-flight 的
  slot；重复/未知/非法完成立即 fail-closed；
- `ibv_poll_cq()` 负返回值立即按 CQ error 处理；
- 完成字节按 `slot_len[]` 统计，不依赖 `wc.byte_len`；
- `ibv_post_send` 使用返回值判定错误（EAGAIN 重试，其余失败）；
- CM `setup_verbs` 每步检查 `job->cancel`，`CM_SETUP` 状态与 worker 超时/取消
  收敛；worker 失败会置 cancel，避免 setup 继续占用资源；
- `rdma_fs_mgr_stop()` 现在会 cancel + join 全部 worker、逐 job teardown、
  销毁 child id/listener/pipe；reactor 循环退出时调用 `repl_rdma_fs_shutdown()`。

### 3.4 CM 计时起点与资源预算（评审 RDMA-01/02）

- job 状态新增 `WAIT_HEADER`：`rdma_fs_begin()` 创建 worker 后，worker **不计时**
  等待 `rdma_fs_header_sent()`；owner 只有把 `FULLRESYNC` 完整写出后才调用它。
  10s 的 CM 建连预算严格从 header 写出开始，大快照/慢磁盘不会被误判为 CM 超时。
  快照阶段使用独立的 5 分钟上限（`RDMA_FS_HEADER_WAIT_MS`），测试钩子
  `KVS_TEST_FULLSYNC_SNAPSHOT_DELAY_MS` 可注入慢快照。
- 资源预算与发送并发分离：
  - `--rdma-fullsync-max-concurrent`：同时处于 `RDMA_SENDING` 的 job 上限（默认 2）；
  - `--rdma-fullsync-max-connected`：同时持有 PD/CQ/QP/MR 的 job 上限（默认 4）；
  - `--rdma-fullsync-max-registered-mb`：所有活动 job 发送 buffer MR 总预算（默认 64MB）。
  CM 建连时在分配任何 verbs 资源之前检查；超额立即置该 job FAILED、拒绝连接，由
  owner fail-closed 该副本并触发重试。Slave 在连接被拒前拿不到 PD，因此也不会先
  注册大目标 MR。

### 3.5 崩溃一致性与全量提交（评审 RV-01/07）

Slave 收到 `FULLRESYNC` 时只记录快照基点 `g_slave_fullsync_base_offset`，
**不提前**把持久化的 `applied/durable` 推进到该 offset。只有快照真正加载并
完成本地基线切换后才推进。

全量结束的提交顺序（任一步失败都 **不发送 REPLDONE**）：

1. `fsync(target fd)`；
2. `replay_dump_file_checked()`：先做结构校验、再写入引擎（坏文件不污染状态）；
3. 写入 `.fullsync_commit_pending` 标记并 fsync 文件/目录；
4. `rename(tmp, dump)`；
5. 把 dump 头 `aof_offset` 归零并 fsync；
6. `persist_aof_rebase()`；
7. 推进 `applied/durable` 到快照基点并保存 replstate；
8. 删除 pending marker 并 fsync 目录。

失败时：保留 marker、清空 replid/offset、shutdown 控制连接 → Slave 重连后
重新全量；Master 不会收到成功 REPLDONE。

重启时 `persist_recover()` 若发现 marker，会**跳过本地 dump/AOF**、删除
replstate，强制以 `? + 0` 重新 FULLRESYNC；成功提交后 marker 才被移除。
测试钩子 `KVS_TEST_FULLSYNC_FAIL_STEP` / `KVS_TEST_FULLSYNC_CRASH_STEP`
用于逐点注入 fsync/replay/rename/patch/rebase/state 失败与崩溃。

### 3.6 资源/快照容量与观测（评审 RV-02/03/06）

- `rdma_fullsync_max_registered_mb` 仅表示 **Master 发送 buffer MR** 预算
  （INFO 同时输出 `rdma_fs_master_send_registered_bytes` 别名）；Slave 端
  file-backed target 由 `repl_rdma_write_buf_max_mb` 限制，Master 在发送
  FULLRESYNC 前若快照超过该值会提前拒绝并记录原因。
- 快照写盘前检查 dump 目录可用空间（预留 16MiB），不足则 fail-closed。
- 快照仍在 owner 线程同步生成（RV-02）：日志输出
  `fullsync snapshot generated in Xms, bytes=...`，per-job INFO 也有 `snap_ms`；
  先量化延迟，再评估后台/COW 快照，不在未做一致性设计时把引擎遍历移到后台。
- 高频逐条 `slave_apply/slave_durable/slave_apply_cmd` 日志默认关闭，
  `KVS_REPL_VERBOSE_APPLY=1` 打开。

## 4. 失败隔离

- 单个 job 的 CM disconnect、CQ error、MR/资源创建失败、MR 等待超时、
  发送 drain 超时：只把该 job 置 FAILED；
- owner 收到失败事件后：发 `FULLSYNCABORT(tid)`、`repl_barrier_cancel(c)`、
  `repl_reject_replica(c)`；其他 job 的 QP/CQ/发送进度不受影响；
- 并发上限之外的 job 在 `WAIT_TOKEN` 等待；其 per-replica recovery buffer
  仍受现有 Barrier 超时/上限约束，超限只会拒绝该副本；
- job 资源销毁顺序：停止发送 → worker join → QP → slot MR/buffer → CQ → PD →
  comp_chan → child CM id。**不允许** detached CQ 线程或先销毁 comp_chan。

## 5. 可观测性（INFO）

```text
rdma_fs_active_jobs
rdma_fs_sending_jobs
rdma_fs_waiting_jobs
rdma_fs_total_started
rdma_fs_total_completed
rdma_fs_total_failed
rdma_fs_cm_rejects
rdma_fs_cq_errors
rdma_fs_registered_bytes
rdma_fs_connected_jobs
rdma_fs_resource_rejects
rdma_fs_max_connected
rdma_fs_max_registered_bytes
rdma_fs_sending_posted_jobs
rdma_fs_max_posted_bytes
rdma_fs_max_concurrency_seen
rdma_fs_jobs            # 每 job: tid/slot/gen/state/posted/completed + 阶段耗时
```

`rdma_fs_sending_posted_jobs >= 2` 表示两个 job 不仅同时 SENDING，而且各自已有
posted WR；`rdma_fs_jobs` 提供 `snap_ms/header_ms/cm_ms/mr_ms/token_ms/send_ms/
end_ms/ack_ms` 等阶段耗时，便于定位慢阶段。

高频增量调试日志（`slave_apply/slave_durable` 每条打印）默认关闭，设置
`KVS_REPL_VERBOSE_APPLY=1` 才输出。

`rdma_fs_max_concurrency_seen >= 2` 表示至少两个 job 同时处于 `RDMA_SENDING`
（由 worker 在持有并发令牌时更新，不依赖轮询采样）。

## 6. 测试

```bash
# 无硬件单测：CM hello 编解码/版本/字节序/零值拒绝 + 关闭 stub 行为
make test_multi_rdma_fullsync

# 端到端：两个 Slave 同时全量同步
#   - 默认全量校验所有 preload key（不是抽样）
#   - 解析 master 日志的 SENDING start/done 区间，断言真实时间重叠
#   - active_jobs 归零
# 需要可用 RDMA 设备与可达 IP（不能是 127.0.0.1）
make check-multi-rdma-fullsync \
  REPL_RDMA_SMOKE_HOST=192.168.233.128 RDMA_DEV=siw0

# CI 门禁：设备缺失时必须是 FAIL，不能把 SKIP 当通过
make check-multi-rdma-fullsync REPL_MULTI_RDMA_REQUIRE=1 ...

# 故障隔离：两个 job 进入发送后 kill -9 Slave B，A 仍完整完成
make check-multi-rdma-fault-isolation \
  REPL_RDMA_SMOKE_HOST=192.168.233.128 RDMA_DEV=siw0

# 协议状态机：无 tid/错 tid REPLDONE + 同连接重复 REPLSYNC（假 Slave，无需 RDMA 对端）
make check-fullsync-state-guard REPL_MULTI_RDMA_REQUIRE=1

# RDMA-01：快照人为延迟 12s（>10s CM 预算）仍必须成功
make check-multi-rdma-cm-deadline REPL_RDMA_SMOKE_HOST=... RDMA_DEV=siw0

# RDMA-02：max-connected=1 时第二个连接被资源预算拒绝并重试后完成
make check-multi-rdma-resource-limit REPL_RDMA_SMOKE_HOST=... RDMA_DEV=siw0

# RDMA-03/07：eBPF+Commit Gate + 并发写 + kill B + 重启追平（需要 root）
sudo make check-multi-rdma-ebpf REPL_RDMA_SMOKE_HOST=... RDMA_DEV=siw0 REPL_MULTI_RDMA_REQUIRE=1

# RV-01：持久化失败/崩溃逐点注入，必须无 REPLDONE 且重启强制全量
make check-fullsync-persistence-faults

# 直接控制测试参数：
python3 tools/repl/run_multi_rdma_fullsync.py --bin ./kvstore \
  --host 192.168.233.128 --master-port 5280 \
  --slave-a-port 5282 --slave-b-port 5284 --rdma-dev siw0 \
  --preload 30000 --chunk-size 1024 --send-slots 1 --pause-us 2000
```

`--pause-us` 拉长单 job 的发送窗口，用于在 soft-RoCE 上稳定观察两个 job 的
`RDMA_SENDING` 重叠；真实 NIC 可以设为 0 并调大 chunk/slots。

`--kill-b` 现在必须观察到 `rdma_fs_sending_posted_jobs >= 2`（两个 job 都已
posted WR）才注入 SIGKILL；观察不到直接 FAIL，不会退化成“握手阶段崩溃”测试。

**CI 必须检查输出包含 `MULTI_RDMA_FULLSYNC_PASS`**，不能只看退出码
（脚本在无设备时默认打印 `..._SKIP` 并返回 0；`--require-rdma` 可改为 FAIL）。

## 7. 已知边界

- 第一版不做多 job 共享快照：每个 Slave 使用自己 `snapshot_offset` 的独立快照；
- 快照在 owner 线程串行生成（存储遍历仍与 Master 写互斥），RDMA 传输并发；
- `--rdma-fullsync-max-concurrent` 默认 2；其余 job 等待，不会无限扩张 recovery buffer；
- `repl-transport=rdma` / `kprobe-rdma` 的旧实时路径不经过本模块；
- **TCP sendfile 回退仍同步占用 owner/reactor**（RDMA manager 不可用或旧 Slave
  未协商 v2 时）。该路径是兼容/故障路径，大快照+慢 Slave 会阻塞事件循环；
  后续应将 RDMA 与 TCP 统一抽象成异步 FullsyncTask，或对回退大小设上限；
- 全量提交失败后的**当前进程内**引擎可能已有部分新快照数据；当前策略是
  断开连接并由下一次 FULLRESYNC 重新加载（marker 保证重启也会强制全量）。
  若需要同一进程内立即恢复一致状态，应增加“加载前清空引擎”或受控重启。
