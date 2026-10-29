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
    为 job 创建独立 PD/CQ/comp_chan/QP/send slots，accept
  job worker（每 job 1 个线程）
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
rdma_fs_max_concurrency_seen
```

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

# 直接控制测试参数：
python3 tools/repl/run_multi_rdma_fullsync.py --bin ./kvstore \
  --host 192.168.233.128 --master-port 5280 \
  --slave-a-port 5282 --slave-b-port 5284 --rdma-dev siw0 \
  --preload 30000 --chunk-size 1024 --send-slots 1 --pause-us 2000
```

`--pause-us` 拉长单 job 的发送窗口，用于在 soft-RoCE 上稳定观察两个 job 的
`RDMA_SENDING` 重叠；真实 NIC 可以设为 0 并调大 chunk/slots。

**CI 必须检查输出包含 `MULTI_RDMA_FULLSYNC_PASS`**，不能只看退出码
（脚本在无设备时默认打印 `..._SKIP` 并返回 0；`--require-rdma` 可改为 FAIL）。

## 7. 已知边界

- 第一版不做多 job 共享快照：每个 Slave 使用自己 `snapshot_offset` 的独立快照；
- 快照在 owner 线程串行生成（存储遍历仍与 Master 写互斥），RDMA 传输并发；
- `--rdma-fullsync-max-concurrent` 默认 2；其余 job 等待，不会无限扩张 recovery buffer；
- `repl-transport=rdma` / `kprobe-rdma` 的旧实时路径不经过本模块。
