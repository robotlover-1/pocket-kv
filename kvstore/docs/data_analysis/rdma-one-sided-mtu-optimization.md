# 单边 RDMA 全量同步 MTU 优化数据报告

> **⚠️ 现状更新（2026-08-03）**：本报告结论建议"生产选 rxe0 + MTU 9000"，但后续发现 **rxe0+siw0 同绑 ens33 会破坏跨机 RDMA**（`rdma_accept` 失败回退 TCP），当前生产配置已改为**单设备 siw0**（`rdma_dev=siw0`，`setup-rdma.sh` 只建 siw0）。MTU 数据仍有效（siw0 跨机 0.37→2.09 Gbps @9000），但 **§5.2 与 §6 的 rxe0 生产建议已不适用**。跨机全量同步现状见 README「性能基准」。
> **🆕 现状更新（2026-08-13）**：跨机 siw0 全量同步效率已定位——**RPS + 并行 QP** 把单 QP ~1.9 Gbps 提到 N=2/4 ~5.2-5.5 Gbps（**~56-61% of sendfile**，明确超 50%）。85MB+ MR 需 root + `ulimit -l unlimited`。详见 §8。

日期：2026-08-02
环境：Master 192.168.233.128 (kernel 6.1) → Slave 192.168.233.129 (kernel 5.15)，VMware VMXNET3，Soft-RDMA（rxe0 / siw0）
工具：`test_rdma_throughput`（全 RDMA WRITE，无 SEND 预热）、`test_sendfile_throughput`、`iperf3`
负载：85,000,008 bytes KVSD dump（1M keys，`gen_fullsync_dump.py`），chunk=256KB
方法：每档 MTU 每项 3 次取中位数，RDMA 全部 CMP 字节一致

## 1. 结论先行

1. **加大 MTU 对跨机 RDMA 和 sendfile 都是有效优化**（见 §4 数据）。
2. **跨机 RDMA 最优组合是 rxe0 + MTU 9000：3.14 Gbps**，是默认 siw0+1500（0.37 Gbps）的 **8.5 倍**。
3. **恶化点在 MTU 9216**：129 的 vNIC / VMware vSwitch 封顶 9000，9216+ 链路直接断（PING_FAIL + DHCP 掉线）。
4. **chunk 大小无关结论不受 MTU 影响**（跨机各 MTU 下 chunk 扫描仍平坦）。
5. 即使 MTU 9000 优化后，跨机 RDMA（3.14 Gbps）**仍打不过 TCP sendfile（8.9 Gbps）**——sendfile 靠内核 GSO/TSO 卸载，Soft-RDMA 没有。若只求跨机全量最快，sendfile/TCP 仍是首选；若必须走 RDMA，rxe+9000 是最优。

> **⚠️ 重要更正（满血 rxe0 数据）**：§4.2 的"本地 RDMA rxe0"列已用**满血 rxe0** 数据替换。此前首测本地 rxe 只有 ~9.5 Gbps、与 README 的 30 Gbps 差 3 倍，排查后确认是 **boot 自动创建的 rxe0 处于"降级"状态**（详见 §5.5）。重建满血 rxe0 后本地回到 ~26-31 Gbps，与 README 吻合。**本报告的跨机数据不受影响**（跨机 rxe 只能测降级态，见 §5.5）。

## 2. 为什么要试 MTU

此前 chunk 扫描证明跨机 RDMA 吞吐与 chunk/MR 大小无关，瓶颈是 **per-packet 软件处理**（UDP/TCP 封装 + KVM vSwitch 逐个包处理）。线缆上的包数 = 数据量 / MTU。**提高 MTU → 包数减少 → 直接打击瓶颈**。本报告验证这一假设并测量 sendfile 的同步变化。

## 3. 测试方法

- 每档 MTU 把 128/129 的 `ens33` 同时设为目标值（必须一致），`ip link up` 后 ping 验证连通。
- RDMA：`test_rdma_throughput --file` 全 WRITE，服务端 file-backed 目标 MR=全文件，`cmp` 校验。
- sendfile：`test_sendfile_throughput --file --size 262144`。
- iperf：`iperf3 -t 3 -f m`（TCP，本地口径，仅列于 §4.2）。
- provider：siw0（SoftiWARP/iWARP over TCP）与 rxe0（Soft-RoCE/UDP）各测一遍。

## 4. 数据

### 4.1 跨机（128→129，chunk=256KB）

| MTU | verbs active_mtu | RDMA siw0 | RDMA rxe0 | sendfile |
|-----|-----------------|-----------|-----------|----------|
| 1500 | 1024 | 0.37 Gbps | 0.84 Gbps | 5.1 Gbps |
| 4000 | 2048 | 1.01 Gbps | 1.70 Gbps | 7.9 Gbps |
| 6000 | 4096 | 1.44 Gbps | 3.09 Gbps | 7.9 Gbps |
| 9000 | 4096 | **2.09 Gbps** | **3.14 Gbps** | **8.9 Gbps** |
| 9216+ | — | 链路断（vSwitch 上限 9000） | | |

提升倍数（1500→9000）：**siw RDMA 5.7x / rxe RDMA 3.7x / sendfile 1.7x**。

### 4.2 本地（128 上，ens33 hairpin，chunk=256KB）

| MTU | RDMA siw0 | RDMA rxe0 | sendfile | iperf |
|-----|-----------|-----------|----------|-------|
| 1500 | ~26 Gbps | 26.5 Gbps | 62.6 Gbps | 70 Gbps |
| 4000 | ~29 Gbps | 31.5 Gbps | 67.2 Gbps | 67 Gbps |
| 6000 | ~28 Gbps | 31.4 Gbps | 66.0 Gbps | 70 Gbps |
| 9000 | ~29 Gbps | 31.6 Gbps | 68.9 Gbps | 65 Gbps |

> 注：本地 rxe0 列为**满血 rxe0**（重建后）数据；降级 rxe0 本地只有 9.9→19.2 Gbps，见 §5.5。本地 siw0 噪声极大（±30%），无清晰 MTU 趋势；满血 rxe0 本地 ~26-31.6 Gbps，与 README 的 ~30 Gbps 吻合，MTU 下基本平坦（本地不是包数瓶颈）。对比降级 rxe0 有强 MTU 缩放——per-packet 处理是瓶颈时 MTU 收益才明显。本地绝对吞吐远高于跨机（本地 rxe ~31 vs 跨机 3.1 Gbps）。

## 5. 分析

### 5.1 MTU 为什么对跨机 RDMA 有效
- 跨机 RDMA 每包要经过 rxe/siw 软件封装 + vSwitch 软件转发。包数 = 85MB/MTU。
- **rxe0（UDP）**：MTU 1500→6000，rxe 的 verbs `active_mtu` 1024→2048→4096，UDP 包数减 4 倍，吞吐 0.84→3.09 Gbps（3.7x）。**6000 后平台期**：rxe 的 verbs MTU 封顶 4096，UDP 没有 GSO 合并，netdev 再大也不减包数。
- **siw0（TCP）**：同样受 verbs MTU 封顶 4096，但底层是 TCP，**netdev MTU 继续增大时 MSS/GSO 更大，TCP 段数继续减少**，所以 6000→9000 仍从 1.44→2.09 Gbps（+45%）。
- **MTU 收益集中在 per-packet 是瓶颈的地方**：降级 rxe0 本地（9.9→19.2 Gbps）和跨机（0.84→3.14）有强 MTU 缩放；**满血 rxe0 本地（~31 Gbps 平坦）MTU 收益几乎消失**——说明满血本地瓶颈已不是包处理（是 memcpy/页故障/短传输噪声），包数再减也提不上去。

### 5.2 rxe0 全面优于 siw0（跨机）
| MTU | siw0 | rxe0 | rxe/siw |
|-----|------|------|---------|
| 1500 | 0.37 | 0.84 | 2.3x |
| 4000 | 1.01 | 1.70 | 1.7x |
| 9000 | 2.09 | 3.14 | 1.5x |

rxe 直走 UDP 封装（省掉 iWARP 的 TCP 全栈处理），per-message 软件开销更低，**纯吞吐优于 siw0**。但 rxe0 因双设备破坏跨机连接，当前生产已放弃，只用 siw0 单设备（见文件头现状更新）。本地对比：满血 rxe0（~31 Gbps）与 siw0（~26-29 Gbps）同量级，rxe 略高且 MTU 趋势更干净。

### 5.3 sendfile（TCP，与 RDMA provider 无关）

- sendfile 跨机：5.1 → 8.9 Gbps（1500→9000，+74%），在 4000 处已接近平台（7.9），9000 再涨到 8.9。
- TCP 随 MTU 改善，但涨幅远小于 RDMA——TCP 在 1500 时就有 GSO/TSO，包数影响没有 Soft-RDMA 那么致命。
- 本地 sendfile/iperf 已达 ~65-70 Gbps（CPU/环回受限），MTU 无收益。

### 5.4 恶化点
- 128 网卡本地可设 MTU 到 16000，但 **129 的 vNIC / VMware vSwitch 封顶 9000**，9216+ 跨机 ping 直接失败，且反复 down/up 会掉 DHCP lease（需重启恢复）。
- **生产安全上限 = 9000**。

### 5.5 重要发现：boot 自动创建的 rxe0 是"降级"的（解释"9 vs 30 Gbps"）

README 记录本地 rxe 单边 WRITE ~30 Gbps，但本机首测只有 ~9.5 Gbps，差 3 倍。排查后确认 **README 的 30 Gbps 是真实的，问题出在 `setup-rdma.service` 创建 rxe0 的时机**。

**根因**：`/usr/local/bin/setup-rdma.sh` 的执行顺序是**先 `rdma link add rxe0` 再 `rdma link add siw0`**，而实测 rxe0 的性能**取决于创建时 siw0 是否已存在**（128/129 两端均可复现，本地 256KB 单边 WRITE）：

- boot 脚本创建（siw0 尚不存在）→ ~9.5 Gbps（**降级**）
- `rdma link delete rxe0` 后重建（siw0 在场）→ **~29 Gbps（满血）**
- 满血 rxe0 建立后删除 siw0 → 跌回 ~10 Gbps

机制未完全定位：GID 相同、CPU 频率满速（2995MHz）、机器空闲（load 0.18）、MTU 循环不触发降级——行为稳定可复现但与 siw0 是否在场强相关。

**对数据的影响**：

- **本地 rxe 数据必须用满血 rxe0**（本报告 §4.2 已用重建后的数据），否则会低估 3 倍。
- **跨机 rxe 数据无法测满血**：跨机 rxe 需要两端 `rxe0` 单设备（129 双设备时服务端 `rdma_create_qp: Invalid argument`；128 双设备时客户端 rdma_cm 会选 siw0），而 rxe0 单设备必然降级。因此 §4.1 的跨机 rxe（0.84→3.14 Gbps）是降级态下的有效测量，满血跨机会更高。
- README 的"chunk 无关、包数是决定因素"结论在满血/降级、siw/rxe、全部 MTU 下均成立。

**修复建议**：原建议把 `setup-rdma.sh` 改为"先建 siw0 再建 rxe0"。**已落地且进一步演进**：`setup-rdma.sh` 现为**只创建 siw0**（显式删除 rxe0），因为 rxe0+siw0 双设备同绑 ens33 会破坏跨机 RDMA 连接——rxe0 的本地满血性能不再被采用。

## 6. 生产建议（已按 2026-08-03 现状更新）

1. **跨机全量同步走 RDMA**：当前配置 `rdma_dev=siw0` + 两端 `ens33 mtu 9000`，跨机 siw0 从 0.37 → 2.09 Gbps（5.7x）。需在 netplan 持久化 MTU，并确认 vSwitch 长期支持 jumbo。**不要用 rxe0**——rxe0+siw0 双设备同绑 ens33 会破坏跨机 RDMA（`rdma_accept` 失败回退 TCP）。
2. **若追求绝对跨机速度**：用 TCP sendfile（8.9 Gbps @9000），仍是 Soft-RDMA 的 2.8x，且无需 RDMA 设备依赖。
3. **MTU 9000 改动影响整个 VM 网络**（TCP MSS、其他流量），落地前需评估副作用；且 9216+ 会掉线，务必封顶 9000。
4. **`setup-rdma.sh` 只建 siw0**（已落地）——避免双设备破坏跨机连接。128 的 rxe0 曾因 VM 快照成为僵尸设备（重启才修复），这也是放弃 rxe0 的原因之一。
5. chunk 大小 / MR 大小不是优化方向（验证无影响），MTU 和 provider 选择才是。

## 7. 未验证事项
- 硬件 RoCE/InfiniBand 下的 MTU 效果（预期同样有效且瓶颈消失）。
- MTU 9000 在 vSwitch 上的长期稳定性（本次为短时测试）。
- 生产 kvstore 全量同步在 rxe+9000 下的端到端（含 replay）验证。

## 8. 后续：RPS + 并行 QP 破单核上限（2026-08-13）

> 日期 2026-08-13，环境同 §3（128 kernel 6.1 → 129 kernel 5.15，MTU 9000，RPS=f，全单边 RDMA WRITE）。
> 负载 85MB KVSD dump（+1G 文件对照），工具 `test_rdma_throughput` / `test_sendfile_throughput` / `run_rdma_parallel_qp.sh`。

### 8.1 数据完整性（跨机字节精确）

85MB 随机文件跨机 RDMA WRITE 后，单 QP 与 N=4 并行 4 个目标文件的 **sha256 全部与源一致** —— 跨机 RDMA WRITE 字节精确。

### 8.2 RDMA vs sendfile 效率（3 次中位数）

| 配置 | 样本（Gbps） | 中位数 | 占 iperf3 上限 | 占 sendfile |
|------|-----:|-----:|-----------:|-----------:|
| iperf3（链路上限） | 8 次 7.06~9.84 | 7.9（最好 9.8） | 100% | ~111% |
| sendfile 单 TCP | 7.42 / 8.66 / 8.79 / 8.82 / 9.08（修正后 5 次） | 8.79 | ~90% | 100% |
| sendfile 多 TCP N=2 | 9.78 / 9.45 / 9.60 | 9.60 | ~98% | ~109% |
| sendfile 多 TCP N=4 | 9.45 / 9.13 / 10.05 | 9.45 | ~96% | ~108% |
| RDMA 单 QP | 1.88 | 1.88 | ~19% | ~21% |
| RDMA N=2 | 4.73 / 5.52 / 5.61 | 5.52 | ~56% | ~63% |
| RDMA N=4 | 5.01 / 5.15 / 5.76 | 5.15 | ~53% | ~59% |

- **单 QP 只有 ~19%**（单核 siw TX 瓶颈）；**RPS + 并行 QP 破单核后冲到 iperf3 上限的 ~53-56%，明确超 50%**。
- **N=2 ≈ N=4**（发送端 4 vCPU 在 N=2 已饱和），加 QP 无益。
- sendfile 波动 6.3~10.6（共享 VM 抢核 + e1000 单 IRQ 迁移）、RDMA 波动 4.7~5.8；两者同向波动部分抵消，比值相对稳定。
- 早期"44%"是单次低值（sendfile 9.64 定值 + 单次 RDMA 4.3），3 次中位数后修正为 iperf3 上限的 ~53-56%。
- **TCP 基线（iperf3）**：跨机单流最好 ~9.8 Gbps 是链路上限（8 次中位 ~7.9 被共享 VM 的 CPU/IRQ 抢占拉低，双峰分布 7.0~8.2 与 9.0~9.8）；sendfile 单 TCP ~8.8、**多 TCP（N=2/4）~9.5-9.6 贴上限**（比单 TCP 高 ~9%）。`iperf3 -P 4` 不升反降（~7.2-7.6）是 iperf3 用户态拷贝 CPU 竞争，**不代表多 TCP 无效**——sendfile 零拷贝低 CPU，多连接能铺满链路。
- **sendfile 计时 bug 已修**：`test_sendfile_throughput` 客户端之前 `t1` 在 `shutdown(SHUT_WR)` 后停表，最后一段 socket buffer 未上链路，吞吐虚高 ~3-5%（旧值 9.02 偏高、客户端恒高于服务端）；已改为等对端 `read` 返回 EOF 再停表，客户端/服务端数字现一致。

### 8.3 注册 / 传输大小无影响（复现 1G 实验）

| 测试（单 QP） | 吞吐 |
|------|-----:|
| 85MB → 85MB MR | 1.88 Gbps |
| 85MB → **1G MR** | 1.92 Gbps |
| **1G** → 1G MR（`--file-direct`） | 1.86 Gbps |

注册大小、传输大小都无影响——瓶颈是 per-packet ~100k pps（e1000 单队列 + 软件封装），不是 MR / chunk / 数据量。

### 8.4 踩坑：85MB+ MR 需 root + `ulimit -l unlimited`

`ibv_reg_mr` 85MB+ 报 `Cannot allocate memory` 的根因：代码里 `setrlimit(RLIMIT_MEMLOCK, RLIM_INFINITY)` 在非 root 下**静默失败**（无 CAP_IPC_LOCK），且这两台 VM **root 默认 memlock 也是 64MB**（`ulimit -l` 实测）。所以 server（及 `--file-direct` 的 client）必须 `sudo bash -c 'ulimit -l unlimited; ...'`。之前 README 的 85MB 数据应是 sudo 跑的。

### 8.5 持久化

MTU 9000 + RPS 已写进 `setup-rdma.sh`（`setup-rdma.service` 开机运行），两台机一致。重启后自动重放，无需手动 `ip link set` / sysfs 写入。
