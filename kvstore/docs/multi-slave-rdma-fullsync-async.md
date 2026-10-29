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

# 端到端：两个 Slave 同时全量同步，断言 WRITE 时间重叠、数据一致、资源归零
# 需要可用 RDMA 设备与可达 IP（不能是 127.0.0.1）
make check-multi-rdma-fullsync \
  REPL_RDMA_SMOKE_HOST=192.168.233.128 RDMA_DEV=siw0

# 直接控制测试参数：
python3 tools/repl/run_multi_rdma_fullsync.py --bin ./kvstore \
  --host 192.168.233.128 --master-port 5280 \
  --slave-a-port 5282 --slave-b-port 5284 --rdma-dev siw0 \
  --preload 30000 --chunk-size 1024 --send-slots 1 --pause-us 2000
```

`--pause-us` 拉长单 job 的发送窗口，用于在 soft-RoCE 上稳定观察两个 job 的
`RDMA_SENDING` 重叠；真实 NIC 可以设为 0 并调大 chunk/slots。

## 7. 已知边界

- 第一版不做多 job 共享快照：每个 Slave 使用自己 `snapshot_offset` 的独立快照；
- 快照在 owner 线程串行生成（存储遍历仍与 Master 写互斥），RDMA 传输并发；
- `--rdma-fullsync-max-concurrent` 默认 2；其余 job 等待，不会无限扩张 recovery buffer；
- `repl-transport=rdma` / `kprobe-rdma` 的旧实时路径不经过本模块。
