# eBPF 主从转发性能优化 — fentry/fexit 替代 kprobe

## 背景

当前 kvstore 主从复制使用 kprobe/kretprobe 挂载 `tcp_recvmsg` 截获客户端写入数据，通过 BPF ringbuf → 异步线程 → TCP 转发到 Slave。

优化后的跨机 QPS 数据：

| Payload | none QPS | sync QPS | kprobe QPS | kprobe vs sync |
|---------|----------|----------|------------|----------------|
| 64B     | 31,803   | 28,247   | 23,790     | −15.8%         |
| 128B    | 28,120   | 37,111   | 24,944     | −32.8%         |
| 256B    | 27,724   | 40,500   | 24,927     | −38.5%         |
| 512B    | 27,456   | 26,121   | 30,468     | +16.6%         |
| 1024B   | 27,523   | 27,041   | 24,821     | −8.2%          |
| 2048B   | 27,430   | 30,163   | 24,064     | −20.2%         |
| 4096B   | 29,327   | 34,907   | 23,839     | −31.7%         |

kprobe 在大部分 payload 下落后 sync 8%-39%。瓶颈：
1. kprobe/kretprobe 每次 `tcp_recvmsg` 触发两次软中断级 hook，开销 ~3-5µs/次
2. `bpf_probe_read_user` 从用户态 buf 再拷贝一份到 ringbuf，造成缓存污染
3. 主路径（read → echo → write）被 kprobe 拖慢，异步转发的收益被抵消

## 问题定义

使用哪种 eBPF 技术能让主从转发 QPS 稳定超过 sync 模式，同时支持跨机？

## 三条方案

### 方案 1：fentry/fexit on tcp_recvmsg（推荐，优先实施）

**机制**：用 BPF trampoline 替代 kprobe，hook 点不变，数据路径不变。

**与 kprobe 的核心区别**：

```
kprobe:   每次 tcp_recvmsg → 软中断 → int3 断点 → 保存/恢复寄存器 → BPF 执行
                              ↑ 每调用 ~3-5µs 开销

fentry:   每次 tcp_recvmsg → trampoline 直跳 → BPF 执行
                              ↑ 每调用 ~0.3-0.5µs 开销（~90% 降低）
```

**现状**：BPF 程序已写在 `tests/perf/kprobe_capture.bpf.c` 中的 `fentry_recv()` / `fexit_recv()`，因内核 5.15 verifier 拒绝 BTF typed pointer 参数而被禁用。内核 6.1+ verifier 已修复此问题。

**预估效果**：kprobe QPS 23K-30K → fentry QPS 28K-35K，预计大部分 payload 超过 sync。

**风险**：低。代码已有，改动量小。主要风险是内核 6.1 verifier 的实际放行情况。

**内核要求**：≥ 6.1。用 Ubuntu mainline 内核包升级，不升级 Ubuntu 版本。

### 方案 2：sockmap sk_msg 内核态 tee（备选，长期方向）

**机制**：`BPF_PROG_TYPE_SK_MSG` 在 Master 的 client socket 上截获消息，通过 `bpf_msg_send_data` 克隆到 slave-facing socket，同时 `SK_PASS` 放行原数据给 Master 进程。

```
         Master 内核
         ═══════════════════════════════
client → TCP ingress → sk_msg BPF
                          ├── SK_PASS → master:read() → echo（主路径不受影响）
                          └── bpf_msg_send_data → slave_sock → TCP → Slave
                               ↑ 全内核态，零用户态参与
```

**优势**：转发完全在 Master 内核完成，主路径零开销。理论 QPS 接近 `none` 基准。

**风险**：中高。sk_msg "tee" 模式（`bpf_msg_send_data` + `SK_PASS` 同时用）社区落地案例少，内核 5.15 的 sk_msg 实现可能有已知 bug，RESP 流式协议在 sk_msg 层可能看到分片。

**内核要求**：5.15 即可用。

### 方案 3：fentry + sk_buff 直读（方案 1 的升级）

**机制**：在方案 1 基础上，用 fentry 的 BTF 类型化参数直接访问 `sock *sk` → `sk_receive_queue` → `sk_buff`，从 `skb->data` 读内核数据，消除 `bpf_probe_read_user` 的额外拷贝。

```
当前: 内核 skb → tcp_recvmsg 拷贝 → 用户 buf → bpf_probe_read_user 再拷贝 → ringbuf
方案3: 内核 skb → tcp_recvmsg 拷贝 → 用户 buf    （正常 recv）
             └→ BPF 直读 skb->data → ringbuf     （零用户态拷贝）
```

**预估**：在方案 1 基础上再提升 5-10%。

**风险**：中。sk_buff 结构跨内核版本变化，需要 BTF CO-RE 适配。tcp_recvmsg 返回后 skb 可能已被释放。

**内核要求**：≥ 6.1（同方案 1）。

## 决策：分阶段路径

```
内核升级 (6.1) → 方案 1 (fentry) 
                    ├── QPS > sync (≥ 5/7 payload) → ✅ 采纳
                    └── QPS ≈ sync 或更低 → 方案 3 (sk_buff 直读)
                                           ├── QPS > sync → ✅ 采纳
                                           └── 仍不行 → 方案 2 (sockmap, 5.15 也可做)

内核升级失败 → 方案 2 (sockmap, 5.15 可用)
```

## 实现计划

### 环境变更

- **Master VM** (192.168.233.128)：安装 Ubuntu mainline 6.1.x 内核
- **Slave VM** (192.168.233.129)：不升级（不加载 BPF，内核 5.15 不变）
- **不升级 Ubuntu 版本**（20.04 保持不变）

### 代码变更

| 文件 | 改动 |
|------|------|
| `tests/perf/kprobe_capture.bpf.c` | fentry/fexit 程序已存在，移除 autoload 禁用 |
| `tests/perf/test_kprobe_repl_qps.c` | `load_kprobe_bpf()` 新增 fentry 优先逻辑，失败回退 kprobe |
| `tests/perf/Makefile.perf` | 可能需要调整 BTF 相关编译选项 |

用户态不做架构性改动——ringbuf 消费者线程、slave 连接、QPS 计时全部复用。

### load_fentry_bpf() 逻辑

```
1. bpf_object__open(BPF_OBJ)
2. 禁用 kprobe/kretprobe/tp 程序的 autoload
3. bpf_object__load()
4. bpf_program__attach_trace() 挂 fentry_recv (target: tcp_recvmsg)
5. bpf_program__attach_trace() 挂 fexit_recv (target: tcp_recvmsg)
6. 设置 ctl map: CTL_ENABLED=1, CTL_PID=<master_pid>
7. 成功 → 返回 obj；任一失败 → 关闭 obj，返回 NULL
```

## 验证计划

### Step 1：内核升级验证

**操作**：Master VM 安装 mainline 6.1 → reboot

**成功标准**：`uname -r` 输出 `6.1.*`

**失败处理**：grub 选 5.15 内核回退，进入方案 2

### Step 2：BPF 加载验证

**操作**：编译 perf 测试，`sudo ./test_kprobe_repl_qps --mode fentry --payload 64 --count 1000`

**成功标准**：stderr 输出 `[fentry] loaded, attached fentry+fexit`

**失败处理**：
- verifier 拒绝 BTF 类型 → 修正 fentry_recv 参数类型再试
- load 失败（其他原因）→ 进入方案 3

### Step 3：数据完整性验证

**操作**：`--mode fentry --payload 64 --count 1000`，slave receiver 统计

**成功标准**：
- `slave msgs == count`（slave 收到全部消息）
- `fwd count >= count`（ringbuf 不丢）

### Step 4：QPS 对比验证

**操作**：
```bash
# Master VM (128):
cd tests/perf && make -f Makefile.perf
sudo ./test_kprobe_repl_qps --mode all --payload 64 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801

# Slave VM (129):
./test_slave_receiver 15801
```

逐一跑 64B, 128B, 256B, 512B, 1024B, 2048B, 4096B 七个 payload。

**成功标准**：

| 结果 | 条件 | 后续 |
|------|------|------|
| ✅ 全胜 | fentry > sync，7/7 | 采纳方案 1 |
| ✅ 通过 | fentry > sync，≥ 5/7 | 采纳方案 1 |
| ⚠️ 持平 | fentry ≈ sync，差距 < 5% | 进入方案 3 |
| ❌ 落后 | fentry < sync，差距 > 5% | 进入方案 3 |

### 改动范围约束

- 不改 `src/` 下的生产代码
- 只改 `tests/perf/` 下的测试/验证程序
- 验证通过后再决定是否将 fentry 方案合入生产复制路径 (`src/replication/`)
