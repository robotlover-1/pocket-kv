# Kprobe 主从转发性能优化

日期：2026-06-26

## 问题

`test_kprobe_repl_qps` 测试中，kprobe 模式 QPS 在所有 payload 下均大幅落后 sync 模式：

| Payload | none QPS | sync QPS | kprobe QPS | kprobe vs sync | kprobe fwd |
|---------|----------|----------|------------|----------------|------------|
| 64B     | 27,651   | 21,007   | 14,294     | −32.0%         | 11,032     |
| 128B    | 28,472   | 21,573   | 12,150     | −43.7%         | 10,496     |
| 256B    | 28,530   | 20,597   | 16,266     | −21.0%         | 10,518     |
| 512B    | 28,583   | 22,297   | 17,278     | −22.5%         | 10,523     |
| 1024B   | 30,500   | 18,184   | 14,274     | −21.5%         | 10,495     |
| 2048B   | 29,196   | 17,482   | 12,694     | −27.4%         | 5,628      |
| 4096B   | 27,392   | 17,072   | 12,377     | −27.5%         | 5,499      |

kprobe QPS 仅为 sync 的 **56%~79%**。同时 ringbuf 转发存在大量丢失（kprobe fwd 列，2048B+ 时丢超过 50%）。

测试程序：[tests/perf/test_kprobe_repl_qps.c](../../tests/perf/test_kprobe_repl_qps.c)
BPF 程序：[tests/perf/kprobe_capture.bpf.c](../../tests/perf/kprobe_capture.bpf.c)

## 根因分析

### 根因 1（最关键）：Kprobe 开销在主路径上，并非真正异步

**kprobe entry+return hook 在 `tcp_recvmsg()` 内部触发，而 `tcp_recvmsg()` 是 `read()` 系统调用的一部分——它在 echo 线程上下文中同步执行。** 唯一被异步掉的只有最终的 `write(slave_fd)`。

```
sync 模式主路径:
  read(client) → write_full(client) → write_full(slave)
  总计: ~47.6μs/req (21,007 QPS)

kprobe 模式主路径:
  read(client) [含 kprobe entry + kretprobe 同步处理] → write_full(client)
  总计: ~70.0μs/req (14,294 QPS)  ← kprobe hook 开销 ~22μs 在主路径上
  (异步) ringbuf consumer → write_full(slave)
```

kprobe 模式用一个**更昂贵的同步操作**（BPF hook + 数据捕获，~22μs）替代了 sync 模式中**更便宜的同步操作**（`write(slave_fd)`，~11μs）。

### 根因 2：bpf_probe_read_user 引入额外的数据拷贝

```
sync 转发路径:
  内核 socket buf →[tcp_recvmsg]→ 用户 buf →[write(slave)]→ 内核 socket buf

kprobe 捕获路径:
  内核 socket buf →[tcp_recvmsg]→ 用户 buf →[bpf_probe_read_user]→ BPF ringbuf
                   (正常拷贝)                (额外 CPU 拷贝！)
```

`bpf_probe_read_user` 从已拷贝到用户态的 buf 中再读一份——这是一次纯额外的 CPU 拷贝，且受 BPF verifier 保护有运行时边界检查。128B payload 时这个开销占比最大，所以 −43.7% 是所有 payload 中最差的。

### 根因 3：每次 recv 触发 5 次 `bpf_probe_read_kernel` helper 调用

`read_iov_data()` 为了从 `msghdr` 中获取 iovec 数据指针，需要：

1. `bpf_probe_read_kernel` → 读 `iov` 指针（msg + 40）
2. `bpf_probe_read_kernel` → 读 `nr_segs`（msg + 48）
3. `bpf_probe_read_kernel` → 读 `iov[0]`（内核内存）

每个 helper 调用都有 verifier 检查 + 内存屏障开销。可以合并为一次读取。

### 根因 4：BPF 内部两次搬运（tmpbuf → ringbuf）

```c
// kprobe_capture.bpf.c 当前流程
buf = bpf_map_lookup_elem(&tmpbuf, &key);    // 从 per-CPU map 取临时 buf
// ... 在 buf 中填充 [4B len][payload] ...
bpf_ringbuf_output(&ringbuf, buf, total, 0); // 再拷贝到 ringbuf
```

数据从用户态 buf 拷到 per-CPU tmpbuf，再通过 `bpf_ringbuf_output` 拷到 ringbuf——这是 BPF 内部的两次搬运。可以用 `bpf_ringbuf_reserve` 直接在 ringbuf 中构造数据，省掉中间转存。

### 根因 5：Ringbuf 溢出丢数据

kprobe fwd 列显示大 payload 时丢失率超过 50%：

| Payload | kprobe QPS | kprobe fwd | 丢失率 |
|---------|-----------|------------|--------|
| 64B     | 14,294    | 11,032     | 22.8%  |
| 2048B   | 12,694    | 5,628      | 55.7%  |
| 4096B   | 12,377    | 5,499      | 55.6%  |

原因：
- Ringbuf 只有 1MB，大 payload 时极易满
- 消费者 `ring_buffer__poll(rb, 100)` 100ms 超时太长
- 消费者写 slave socket 可能阻塞（SO_SNDBUF=64KB 限制）
- 生产者（kprobe hook 在内核上下文）无法被反压

## 优化方案

### 方案 1：切换到 fentry/fexit（P0）

**原理**：fentry/fexit 使用 BPF trampoline 机制，比 kprobe 的 int3 断点快得多。测试环境 Linux 5.15 完全支持。

**预期收益**：QPS 提升 20-40%

**改动**：
- `SEC("kprobe/tcp_recvmsg")` → `SEC("fentry/tcp_recvmsg")`
- `SEC("kretprobe/tcp_recvmsg")` → `SEC("fexit/tcp_recvmsg")`
- 函数参数用 BTF 类型化指针直接访问，不再需要 PT_REGS 宏

**数据流变化**：

```
改前 (kprobe):
  int3 断点 → 寄存器保存/恢复 → BPF 程序 → 返回
  每次 hook ~1-3μs，两次合计 ~2-6μs

改后 (fentry/fexit):
  BPF trampoline → 直接调用 BPF 程序 → 返回
  每次 hook ~0.5-1μs，两次合计 ~1-2μs
```

**风险**：低。需要 CONFIG_DEBUG_INFO_BTF=y（5.15 默认开启）。如果 BTF 不可用，运行时自动回退到 kprobe。

### 方案 2：增大 ringbuf + 减小 poll 超时 + 增大 SNDBUF（P0）

**原理**：缓解 ringbuf 溢出丢数据问题。

**预期收益**：减少丢数据，不直接提升 QPS（瓶颈在主路径），但整体转发完整率提升。

**改动**：

| 参数 | 当前 | 改后 |
|------|------|------|
| Ringbuf 大小 | 1MB (`1 << 20`) | 4MB (`1 << 22`) |
| Poll timeout | 100ms | 5ms |
| Slave SO_SNDBUF | 64KB | 256KB |

**风险**：极低。纯参数调整。

### 方案 3：用 `bpf_ringbuf_reserve` 替代 `bpf_ringbuf_output`（P1）

**原理**：`bpf_ringbuf_reserve` 直接在 ringbuf 中预留空间并返回可写指针，消除 tmpbuf 中间拷贝。但这要求配合方案 4（改 `read_iov_data` 签名），因为 `bpf_ringbuf_reserve` 返回内核态地址，不能直接做 `bpf_probe_read_user` 的目标。

**预期收益**：QPS 提升 5-10%

**改动**：

```c
// 改前: tmpbuf → bpf_ringbuf_output (两次搬运)
unsigned char *buf = bpf_map_lookup_elem(&tmpbuf, &key);
int data_len = read_iov_data(*msg_ptr, buf + 4, CAPTURE_MAX_DATA);
bpf_ringbuf_output(&ringbuf, buf, 4 + data_len, 0);

// 改后: bpf_ringbuf_reserve 直接写 ringbuf
void *entry = bpf_ringbuf_reserve(&ringbuf, 4 + ESTIMATED_LEN, 0);
if (entry) {
    int data_len = read_iov_data_direct(*msg_ptr, (unsigned char *)entry + 4, ESTIMATED_LEN);
    if (data_len > 0) {
        // 调整大小: reserve 多预留的不用，commit 时指定实际大小
        bpf_ringbuf_commit(entry, 0);
    } else {
        bpf_ringbuf_discard(entry, 0);
    }
}
```

**风险**：低。但 `bpf_ringbuf_reserve` 返回的可写空间需要一次确定大小，当前先 `bpf_probe_read_user` 再打包的方式不适用——需先确定数据大小再 reserve。可以考虑：
- 先用 kretprobe 返回值确定大小
- reserve 后只在 ringbuf 内做一次 memcpy

### 方案 4：合并 `bpf_probe_read_kernel` 调用（P1）

**原理**：`read_iov_data` 当前分 3 次读 `iov`、`nr_segs`、`vec`。可以用一次 `bpf_probe_read_kernel` 读一个包含相邻字段的结构体。

**预期收益**：QPS 提升 5-10%

**改动**：

```c
// 改前: 3 次 bpf_probe_read_kernel
bpf_probe_read_kernel(&iov, sizeof(iov), (void *)(msg_ptr + 40));
bpf_probe_read_kernel(&nr_segs, sizeof(nr_segs), (void *)(msg_ptr + 48));
bpf_probe_read_kernel(&vec, sizeof(vec), &iov[0]);

// 改后: 1 次 bpf_probe_read_kernel 读更大的结构体
struct { unsigned long iov_base; unsigned long iov_len; unsigned long nr_segs; } head;
bpf_probe_read_kernel(&head, sizeof(head), (void *)(msg_ptr + 40));
// head.iov_base, head.iov_len 即 vec 信息
```

**风险**：低。需验证 iov_iter 结构在 5.15 上的布局是否和假设一致。

### 方案 5（核心改造）：从 sk_buff 直接读数据，消除 `bpf_probe_read_user`（P2）

**原理**：fexit 触发时，数据还在 socket receive queue 的 sk_buff 中。直接从 `sk->sk_receive_queue` 遍历 sk_buff 链表，用 `bpf_probe_read_kernel` 从内核态读数据，完全避免 `bpf_probe_read_user` 的第二次拷贝。

**预期收益**：QPS 提升 30-50%

**数据流变化**：

```
改前:
  内核 socket buf →[tcp_recvmsg]→ 用户 buf →[bpf_probe_read_user]→ BPF ringbuf
                                      ↑ 额外 CPU 拷贝

改后:
  内核 socket buf →[tcp_recvmsg]→ 用户 buf    (正常 recv 路径, 不动)
  内核 sk_buff ────→[bpf_probe_read_kernel]───→ BPF ringbuf
                        ↑ 零额外拷贝，直接从内核态读
```

**实现要点**：

```c
// fexit/tcp_recvmsg — 从 sk_buff 直接读
SEC("fexit/tcp_recvmsg")
int fexit_recv(struct pt_regs *ctx) {
    struct sock *sk = (struct sock *)ctx->di;  // 第一个参数
    long retval = ctx->ax;

    // 走 sk_receive_queue 读最后一个 sk_buff 的数据
    struct sk_buff *skb;
    bpf_probe_read_kernel(&skb, sizeof(skb), &sk->sk_receive_queue.next);
    // ... 从 skb->data + skb->len - retval 读数据 ...
}
```

**风险**：中。内核结构体偏移在不同版本可能变化，需要用 BPF CO-RE（`bpf_core_read` + `__builtin_preserve_access_index`）做可移植访问。`sk_receive_queue` 的 skb 在 `tcp_recvmsg` 返回后可能已被消费（取决于内核实现），需实际验证。

### 方案 6（备选）：从 `tcp_sendmsg` 截获（仅生产路径适用）

**说明**：生产环境的 `repl_kprobe.bpf.c` 已经 hook 在 `tcp_sendmsg` 上（而非 `tcp_recvmsg`），且不需要 kretprobe——所有信息在 entry 就可获取：

```c
// repl_kprobe.bpf.c — 当前生产实现
SEC("kprobe/tcp_sendmsg")
int kprobe_kvs_repl_tcp_sendmsg(struct pt_regs *ctx) {
    __u32 size = (__u32)ctx->dx;                // 第三个参数 = 数据长度
    unsigned long msg_ptr = (unsigned long)ctx->si;  // 第二个参数 = msghdr
    // ... 直接从参数获取 size 和 msg，不需要 entry+return 协作了
}
```

这个设计比测试程序好——只用一个 kprobe（非 kretprobe），且参数 `size` 直接可用。但同样受 `bpf_probe_read_user` 拷贝和 kprobe 机制本身的限制。切换到 fentry 同样适用。

## 实施计划

### 阶段一（低风险快速收益）

1. 方案 2：调整 ringbuf/poll/SNDBUF 参数
2. 方案 4：合并 `bpf_probe_read_kernel` 调用
3. 重新跑 benchmark，验证增益

### 阶段二（中等改动）

4. 方案 1：切换到 fentry/fexit
5. 方案 3：用 `bpf_ringbuf_reserve` 消除 BPF 内部拷贝
6. 重新跑 benchmark，验证增益

### 阶段三（核心改造）

7. 方案 5：从 sk_buff 直接读数据
8. 跑完整 benchmark 对比，验证是否达到或超过 sync 模式

### 目标

| 阶段 | kprobe vs sync 目标 |
|------|-------------------|
| 当前 | 56%~79% |
| 阶段一完 | 65%~85% |
| 阶段二完 | 80%~95% |
| 阶段三完 | 95%~110%（可能超越 sync） |

## 验证方法

### 正确性验证

- 所有模式（none/sync/kprobe）的 echo 响应正确率 100%
- kprobe fwd count 接近 echo count（丢失率 <1%）
- `test_kprobe_repl_qps` 全 payload 通过

### 性能验证

- 阶段一/二/三完成后分别跑全量 benchmark：
  ```bash
  for payload in 64 128 256 512 1024 2048 4096; do
      sudo ./test_kprobe_repl_qps --mode all --payload $payload --count 10000
  done
  ```
- 对比 kprobe vs sync 比例是否达到目标
- 检查 kprobe fwd 丢失率

### 回退条件

- 任一步骤导致 kprobe QPS 下降 >5%：回退该步骤，标记"不适用"
- BPF verifier 拒绝新程序：保留旧版本，降低内核版本要求或使用条件编译

## 影响范围

- [tests/perf/kprobe_capture.bpf.c](../../tests/perf/kprobe_capture.bpf.c) — 主要改动，BPF 程序
- [tests/perf/test_kprobe_repl_qps.c](../../tests/perf/test_kprobe_repl_qps.c) — 可能需要改 ringbuf poll 参数
- [src/replication/bpf/repl_kprobe.bpf.c](../../src/replication/bpf/repl_kprobe.bpf.c) — 如果开关打开，同步改动生产 BPF 程序

## 已知限制

1. **fentry/fexit 需要 BTF**：内核需编译时开启 CONFIG_DEBUG_INFO_BTF，大部分发行版 5.10+ 默认开启
2. **从 sk_buff 读依赖内核实现**：`tcp_recvmsg` 返回后 sk_buff 可能已从队列移除（取决于 `tcp_read_sock` 路径）。需在 5.15 上实际验证，若不可行则跳过方案 5
3. **bpf_ringbuf_reserve 的预分配大小**：需在 reserve 前知道数据大小，可以先用 retval 确定上限，预留空间可能略大于实际（浪费少量 ringbuf 空间换省一次拷贝）
4. **只优化测试路径**：本文档主要针对 `kprobe_capture.bpf.c`（kprobe 模式 echo QPS 测试）。生产路径 `repl_kprobe.bpf.c` 的优化由实际 benchmark 数据驱动，不在本文档覆盖范围内
