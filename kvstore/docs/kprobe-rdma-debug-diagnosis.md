# kprobe+RDMA 增量同步实现问题排查与诊断

> **⚠️ Legacy 传输路径**：本文档记录的 kprobe+RDMA 增量同步是**旧实时传输路径**。当前 `repl_realtime_transport=ebpf+tcp`（独立 ebpf-proxy 进程 + fentry 捕获），kprobe-rdma 不再是默认路径。仅作历史参考。

> 本文档详细记录 kprobe+RDMA 增量同步实现过程中遇到的所有问题、根因分析、解决方
> 案以及调试方法，作为 `kprobe-rdma-incrsync-implementation.md` 的配套诊断手册。

---

## 目录

1. [问题概览](#1-问题概览)
2. [BPF 加载失败](#2-bpf-加载失败)
3. [BPF ringbuf 无数据](#3-bpf-ringbuf-无数据)
4. [RDMA WRITE 失败](#4-rdma-write-失败)
5. [CO-RE 重定位失败](#5-co-re-重定位失败)
6. [BPF 验证器拒绝](#6-bpf-验证器拒绝)
7. [Slave 收不到数据](#7-slave-收不到数据)
8. [误报 "kprobe-rdma failed"](#8-误报-kprobe-rdma-failed)
9. [附录：调试工具与命令](#9-附录调试工具与命令)

---

## 1. 问题概览

| 编号 | 问题 | 现象 | 根因 | 修复 |
|------|------|------|------|------|
| P1 | BPF 加载 `-4007` | `failed to load BPF object: -4007` | CO-RE 重定位失败 | 去除 `-g` 编译或修正 `struct pt_regs` |
| P2 | BPF 加载 `-2` | `failed to open BPF object: ...` | 无 BTF 时 libbpf 0.5 无法解析 maps | 恢复 `-g` 但修正 struct 字段名匹配 |
| P3 | ringbuf 无数据 | `ringbuf_cb size=4 payload_len=0` 重复 | BPF 写 4 字节 0（通知模式） | 补全 `bpf_probe_read_*` 读真实数据 |
| P4 | `bpf_probe_read_*` 读不到数据 | 同上但 size>4 | `msg` 是内核指针 → 需 `read_kernel` | 区分内核/用户空间使用不同 helper |
| P5 | msghdr 偏移猜错 | `payload_len=0`，probe read 全失败 | `iov_iter` 布局与假设不同 | BTF dump 确认真实偏移 |
| P6 | R2 min value is negative | 验证器拒绝 | `bpf_probe_read_user` 的 size 参数类型 | 常量边界检查 |
| P7 | invalid access to map value | 验证器拒绝 | BPF 栈超 512B | 改用 per-CPU array |
| P8 | CO-RE relocation failed | -4007 即使字段名正确 | 匿名 union 无法 CO-RE 解析 | 放弃 CO-RE，用固定偏移 |
| P9 | RDMA WRITE 使 QP 断开 | slave listener exiting, CQ error | MR 未就绪时 rkey=0 导致 WRITE 失败 | 回调中检查 `rkey != 0` |
| P10 | "kprobe-rdma failed" 误报 | transport.log 大量重复 | send 返回 -1 触发 fallback log | 移除误报日志 |
| P11 | slave poll 一直 empty | RDMA WRITE 未到达 | 数据在 MR 交换之前到达，被回调跳过 | MR 就绪后自动开始 |

---

## 2. BPF 加载失败

### 2.1 错误 `-4007`（LIBBPF_ERRNO__RELOC）

**现象**：
```
kprobe: failed to load BPF object: -4007
```

**根因 1：CO-RE 重定位失败**。`-g` 编译时生成 BTF，libbpf 尝试解析 CO-RE 重定位。
当手动定义的 `struct pt_regs` 字段名与内核 BTF 不一致时，重定位失败。

**内核 5.15 x86_64 的 `struct pt_regs`**：
```c
struct pt_regs {          // 内核 BTF 中的命名
    unsigned long r15;    unsigned long r14;
    unsigned long r13;    unsigned long r12;
    unsigned long bp;     unsigned long bx;    // 注意: bp, bx (无 r 前缀)
    unsigned long r11;    unsigned long r10;
    unsigned long r9;     unsigned long r8;
    unsigned long ax;     unsigned long cx;    // 注意: ax, cx (无 r 前缀)
    unsigned long dx;     unsigned long si;    // 注意: dx, si, di
    unsigned long di;     unsigned long orig_ax;
    unsigned long ip;     unsigned long cs;
    unsigned long flags;  unsigned long sp;
    unsigned long ss;
};
```

**`bpf_tracing.h` 中的 PT_REGS_PARM 宏（非 __KERNEL__ 模式）**：
```c
#define PT_REGS_PARM1(x) ((x)->rdi)  // 注意: rdi (带 r 前缀)
#define PT_REGS_PARM2(x) ((x)->rsi)  // rsi
#define PT_REGS_PARM3(x) ((x)->rdx)  // rdx
```

**核心冲突**：内核 BTF 用 `di/si/dx`，PT_REGS 宏用 `rdi/rsi/rdx`。
同时满足两者的方式是**不使用 PT_REGS 宏**，直接访问寄存器：

```c
// ✅ 正确做法：直接访问 ctx 字段
__u32 size = (__u32)ctx->dx;   // PT_REGS_PARM3 ← 不使用这个宏
void *msg = (void *)ctx->si;    // PT_REGS_PARM2 ← 不使用这个宏
```

并将 `struct pt_regs` 定义为内核 BTF 兼容的字段名（无 r 前缀）。

**解决方案**（最终采用）：
1. 不使用 `bpf_tracing.h`（避免 PT_REGS_PARM 宏）
2. 直接访问 `ctx->dx`, `ctx->si`, `ctx->di`
3. `struct pt_regs` 字段名匹配内核 BTF
4. 用 `-g` 编译（需要 BTF 解析 maps section）

### 2.2 错误 `-2`（无法打开 BPF 对象）

**现象**：
```
kprobe: failed to open BPF object: build/replication/bpf/repl_kprobe.bpf.o
```

**根因**：去掉 `-g` 后 BPF 对象文件无 `.BTF` section。libbpf 0.5 在解析
`.maps` section 时需要 BTF 信息了解 map 的 key/value 类型和大小。
无 BTF 时 `bpf_object__open_file` 返回错误。

**解决方案**：恢复 `-g` 编译，修正 `struct pt_regs` 字段名匹配问题。

---

## 3. BPF ringbuf 无数据

### 3.1 现象

```
kprobe rdma: [DBG] ringbuf_cb size=4 payload_len=0    ← 重复上万次
```

### 3.2 原因链

初始 BPF 程序只往 ringbuf 写 4 字节 0：

```c
/* 原始代码（有问题） */
__u32 zero = 0;
bpf_ringbuf_output(&repl_ringbuf, &zero, 4, 0);  // 只写了一个 0
```

用户态回调读取 `payload_len = 0` 后直接 `return 0`，静默丢弃。

### 3.3 完整修复过程

**第 1 步：读取 msghdr 的 iovec**（最初设想）

```c
// 从 tcp_sendmsg 的 msg 参数读取数据
const struct msghdr *msg = PT_REGS_PARM2(ctx);
// 读取 msg_iter.iov → msg_iter->iov_base → 拷贝数据
```

**卡点**：`tcp_sendmsg` 在内核栈中调用，`msg` 是内核指针。必须用
`bpf_probe_read_kernel` 读取 `msg` 及其字段。但 `msg->msg_iter.iov`
指向**用户空间** iovec（或在某些路径下指向内核栈），需要区分。

**第 2 步：确认 struct 偏移**（通过 BTF dump + 运行时探测）

```
bpftool btf dump id 1 | grep -A12 "iov_iter'"
```

发现 kernel 5.15 的 `iov_iter` 布局与常识不同：

```
想当然:  iter|pad(8) + iov(8) + nr_segs(8) + iov_offset(8) + count(8)
实际:    iter|nofault|data_source|pad(8) + iov_offset(8) + count(8) + iov(8) + nr_segs(8)
```

**`iov` 不在 offset 8，而在 offset 24！**

因此 `iov` 指针在 `msg + 16 + 24 = msg + 40`。

**第 3 步：正确使用 probe_read 变体**

| 数据 | 位置 | 适合的 helper |
|------|------|-------------|
| `msg` 参数 | 内核栈 | `bpf_probe_read_kernel` |
| `msg + 40` (iov 指针) | 内核栈 | `bpf_probe_read_kernel` |
| `iov[0]` (iovec) | 内核栈（send 系统调用创建） | `bpf_probe_read_kernel` |
| `iov->iov_base` (数据) | 用户空间 | `bpf_probe_read_user` |

### 3.4 最终代码

```c
static __always_inline int read_msg_data(unsigned long msg_ptr,
    unsigned char *buf, int max_len)
{
    /* 1. 读 iov 指针 — bpf_probe_read_kernel(msg+40) */
    bpf_probe_read_kernel(&iov, sizeof(iov), msg_ptr + 40);
    if (!iov) return 0;

    /* 2. 读 nr_segs — bpf_probe_read_kernel(msg+48) */
    bpf_probe_read_kernel(&nr_segs, sizeof(nr_segs), msg_ptr + 48);
    if (nr_segs == 0) return 0;

    /* 3. 读 iovec — bpf_probe_read_kernel(iov[0]) */
    bpf_probe_read_kernel(&vec, sizeof(vec), &iov[0]);
    if (!vec.b || vec.l == 0) return 0;

    /* 4. 读用户数据 — bpf_probe_read_user(iov_base) */
    bpf_probe_read_user(buf, safe_len, vec.b);
    return safe_len;
}
```

---

## 4. RDMA WRITE 失败

### 4.1 现象

```
Master 日志: 有 ringbuf_cb 但无 wr_submit_data/WRITE 日志
Slave 日志: slave poll empty (head=tail=0) 持续
            slave listener exiting           ← QP 断开了
```

### 4.2 根因

在 `kprobe_ringbuf_cb` 中，RDMA WRITE 之前未检查 MR 信息是否已就绪：

```c
// 有问题的流程
ringbuf_cb() {
    wr_slot_acquire()     ← ✅ 成功
    wr_submit_data()      ← 使用 g_slave_mr.rkey (此时 = 0!)
    wr_submit_head()      ← 同样使用 rkey=0
}
```

`g_slave_mr.rkey = 0` 导致 RDMA WRITE 使用无效 rkey，
IBV_WC_RETRY_EXC_ERR（status=5），QP 进入错误状态。

### 4.3 时序问题

建链过程中，事件顺序如下：

```
时间线:
  QP connected           ← kprobe-rdma QP 就绪
  forward thread start   ← 开始轮询 ringbuf
  ringbuf 收到 50000+ 事件 ← 全量同步产生的 TCP 数据
    回调调用 wr_slot_acquire → wr_submit_data → ❌ rkey=0
  sleep(200ms)           ← connect_mr 中的延时
  KPROBEMR sent          ← MR 请求发送
  +KPROBERDMA received   ← MR 信息到达
  g_slave_mr.rkey 已设置  ← 但 QP 已损坏！
```

### 4.4 解决方案

在回调中添加 MR 就绪检查：

```c
static int kprobe_ringbuf_cb(void *ctx, void *data, size_t size) {
    ...
    /* ★ MR 未就绪时跳过 RDMA WRITE */
    if (g_slave_mr.rkey == 0 || !g_rdma_kprobe.connected) {
        return 0;  // 仅统计，不尝试 WRITE
    }
    ...
}
```

---

## 5. CO-RE 重定位失败

### 5.1 背景

CO-RE（Compile Once - Run Everywhere）是 BPF 的一种机制，允许 BPF 程序在
不同内核版本上运行。编译器记录 struct 字段的访问偏移，libbpf 在加载时
根据目标内核的 BTF 信息调整偏移。

### 5.2 在 kprobe+RDMA 中遇到的 CO-RE 问题

| 问题 | 原因 | 解决 |
|------|------|------|
| `struct pt_regs` 字段名不匹配 | 手动定义与内核 BTF 命名不同 | 匹配内核 BTF 命名 |
| `struct sock_common` 偏移不对 | 硬编码 `__pad[0x1c2]` 与实际不符 | 去掉端口过滤，仅用 PID 过滤 |
| `struct iov_iter` 匿名 union | `iov` 在匿名 union 内，CO-RE 无法解析 | 放弃 CO-RE，用固定偏移 |
| `invalid func unknown#195896080` | `bpf_core_read` 生成的 relocation 未解析 | 改用 `bpf_probe_read_kernel` |

### 5.3 最终策略

**放弃 CO-RE**，使用固定偏移 + 直接 probe read：

```c
/* ❌ CO-RE 方式（导致 -4007） */
bpf_core_read(&iter, sizeof(iter), &msg->msg_iter);

/* ✅ 固定偏移方式（工作正常） */
bpf_probe_read_kernel(&iov, sizeof(iov), (void *)msg_ptr + 40);
```

固定偏移需要知道目标内核的确切 struct 布局。通过 `bpftool btf dump` 确认。

### 5.4 注意事项

如果更换内核版本，需要重新验证偏移。建议每次更换内核后运行：

```bash
# 检查 msghdr 中 msg_iter 的偏移
sudo bpftool btf dump id 1 | grep -A15 "msghdr'" | head -10

# 检查 iov_iter 中各字段的偏移
sudo bpftool btf dump id 1 | grep -A12 "iov_iter'" | head -10
```

---

## 6. BPF 验证器拒绝

### 6.1 错误 "R2 min value is negative"

**现象**：
```
R2 min value is negative, either use unsigned or 'var &= const'
```

**根因**：`bpf_probe_read_user(buf + total, chunk, ...)` 的第二个参数 `chunk`
类型为 `int`，BPF 验证器无法证明其非负。

**修复**：添加明确的常量边界检查：

```c
int chunk = (int)vec.iov_len;
if (chunk < 0) break;          // ← 非负检查
if (chunk > 500) chunk = 500;  // ← 常量上界
bpf_probe_read_user(buf, chunk, ...);  // 验证器接受
```

### 6.2 错误 "BPF stack limit of 512 bytes exceeded"

**现象**：
```
Looks like the BPF stack limit of 512 bytes is exceeded
```

**根因**：BPF 程序的栈限制为 512 字节。`entry[504]`（4B header + 500B data）
加上其他局部变量超出限制。

**修复**：使用 `BPF_MAP_TYPE_PERCPU_ARRAY` 作为临时缓冲区：

```c
/* per-CPU 数组，每个 CPU 一份，无竞争 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, unsigned char[504]);  // 4 + 500 字节
} kprobe_tmpbuf SEC(".maps");

// 使用时：
entry = bpf_map_lookup_elem(&kprobe_tmpbuf, &key);
```

### 6.3 错误 "invalid access to map value"

**现象**：
```
invalid access to map value, value_size=504 off=503 size=500
```

**根因**：循环中 `buf + total + chunk` 超过 per-CPU 数组边界。

**修复**：确保每次读操作前有边界检查，且 `chunk` 不超过 `remaining`：

```c
if (total >= 500) break;
unsigned int remaining = 500 - total;
unsigned int chunk = (unsigned int)vec.iov_len;
if (chunk > remaining) chunk = remaining;
if (chunk == 0) break;
```

### 6.4 错误 "invalid func unknown#195896080"

**现象**：
```
invalid func unknown#195896080
```

**根因**：`0xBADB10`（`__builtin_preserve_access_index`）生成的 CO-RE 重定位
未解析。libbpf 将未解析的 relocation 当作函数调用，无效。

**修复**：不使用 `bpf_core_read` 或 `__builtin_preserve_access_index`。

---

## 7. Slave 收不到数据

### 7.1 slave poll 一直 empty

**现象**：
```
kprobe rdma: [DBG] slave poll empty (head=tail=0)  ← 持续
```

| 可能原因 | 检查方法 | 解决 |
|---------|---------|------|
| MR 未就绪时回调跳过 | master 日志 `MR not ready, skip` | 等待 MR 就绪后自动开始 |
| RDMA WRITE 失败 | master 日志 `cq_poll error` | 检查 QP 状态和 rkey |
| Slave listener 已退出 | slave 日志 `slave listener exiting` | listener 退出导致 QP 断开 |
| 数据全部在 MR 交换之前到达 | 时间线分析 | 正常，MR 就绪后的数据才会 WRITE |

### 7.2 slave listener 提前退出

**现象**：
```
kprobe rdma: [OK] slave listener accepted, MR ready
kprobe rdma: slave listener exiting  ← 紧随其后
```

**根因**：Master 的 KPROBEMR 通过 TCP 发送，但 slave 的 kprobe-rdma listener
在接受连接后进入等待 DISCONNECTED 的循环。如果 Master 端 QP 因错误断开
（如 rkey=0 的 RDMA WRITE），slave 侧收到 DISCONNECTED 后退出 listener。

**修复**：确保 Master 不在 MR 就绪前做 RDMA WRITE（参见第 4 节）。

---

## 8. 误报 "kprobe-rdma failed"

### 8.1 现象

```
[2026-05-25 23:49:56] realtime using KPROBE+RDMA
[2026-05-25 23:49:56] kprobe-rdma failed, fallback to TCP  ← 重复数千次
```

### 8.2 根因

`repl_transport_kprobe_rdma_send` 设计上始终返回 -1（触发 TCP 保底），
`repl_realtime_send` 把非 0 返回值当作"失败"记录日志。

```c
int repl_realtime_send(conn_t *c, const unsigned char *buf, size_t len) {
    int rc = ops->send(c, buf, len);  // kprobe_rdma_send 返回 -1
    if (rc == 0) return 0;
    transport_log("%s failed, fallback to TCP", ops->name);  // ← 误报
    rc = repl_transport_tcp_send(c, buf, len);
    return rc;
}
```

### 8.3 修复

移除误报日志，TCP 保底发送仍然执行：

```c
int repl_realtime_send(conn_t *c, const unsigned char *buf, size_t len) {
    int rc = ops->send(c, buf, len);
    if (rc == 0) return 0;
    /* kprobe-rdma 返回 -1 是设计使然（TCP 保底 + kprobe 数据源） */
    rc = repl_transport_tcp_send(c, buf, len);
    return rc;
}
```

---

## 9. 附录：调试工具与命令

### 9.1 BPF 相关

```bash
# 查看内核 BTF 的 struct 定义
sudo bpftool btf dump id 1 | grep -A15 "msghdr'"
sudo bpftool btf dump id 1 | grep -A12 "iov_iter'"

# 直接加载 BPF 对象测试
sudo bpftool prog load build/replication/bpf/repl_kprobe.bpf.o /sys/fs/bpf/test

# 查看 BPF map 内容
sudo bpftool map lookup name kprobe_stats key 0 0 0 0

# 查看 verifier 日志（加载失败时）
sudo bpftool prog load /path/to/prog.o /sys/fs/bpf/test 2>&1 | tail -30

# 检查 helper 可用性
sudo bpftool feature probe | grep probe_read
```

### 9.2 struct 偏移探测

写一个探测 BPF 程序，从不同偏移读取值并输出到 ringbuf：

```c
/* 探测 msg 偏移的 BPF 代码片段 */
SEC("kprobe/tcp_sendmsg")
int probe(struct pt_regs *ctx) {
    unsigned long data[8] = {};
    data[0] = ctx->dx;  // size
    data[1] = ctx->si;  // msg ptr
    bpf_probe_read_kernel(&data[2], 8, (void*)(ctx->si+12));
    bpf_probe_read_kernel(&data[3], 8, (void*)(ctx->si+16));
    bpf_probe_read_kernel(&data[4], 8, (void*)(ctx->si+24));
    bpf_probe_read_kernel(&data[5], 8, (void*)(ctx->si+32));
    bpf_probe_read_kernel(&data[6], 8, (void*)(ctx->si+40));
    bpf_probe_read_kernel(&data[7], 8, (void*)(ctx->si+48));
    bpf_ringbuf_output(&rb, data, 64, 0);
    return 0;
}
```

编译后在测试工具中读取 ringbuf 输出，根据已知字段值（如 `iter_type=0`）
反推出正确的偏移。

### 9.3 RDMA 相关

```bash
# 查看 RDMA 设备状态
ibv_devinfo

# 查看 QP 状态
sudo rdma resource show qp

# 查看 CQ 事件（libibverbs 调试）
export IBV_DEBUG=1
```

### 9.4 日志关键字速查

| 日志内容 | 含义 | 处理 |
|---------|------|------|
| `BPF loaded and attached` | BPF 加载成功 | ✅ 正常 |
| `master init OK, PID=xxx` | kprobe 初始化完成 | ✅ 正常 |
| `QP connected` | kprobe-rdma QP 就绪 | ✅ 正常 |
| `ringbuf_cb size=4 payload_len=0` | BPF 通知模式（无数据） | ❌ BPF 读数失败 |
| `ringbuf_cb size=NNN payload_len=NNN` | BPF 读到实际数据 | ✅ 正常 |
| `MR not ready, skip` | MR 未就绪跳过 RDMA WRITE | ℹ️ 正常（MR 交换前） |
| `KPROBEMR sent` | MR 请求已发送 | ✅ 正常 |
| `+KPROBERDMA received` | MR 信息已获取 | ✅ 正常 |
| `WRITE data failed` | RDMA WRITE 提交失败 | ❌ QP 或 slot 问题 |
| `no available WRITE slot` | 8 个 slot 全 in_flight | ⚠️ CQ 回收可能卡住 |
| `slave poll data head=N diff=M` | Slave 收到 RDMA 数据 | ✅ 正常 |
| `slave poll empty` | Slave 环形缓冲区空 | ℹ️ 等待数据 |
| `slave listener exiting` | Slave listener 退出 | ❌ QP 断开 |
| `cq_poll error status=5` | RETRY_EXC_ERR | ❌ 远端 QP 无响应 |
| `kprobe-rdma failed, fallback` | 误报（已修复） | ✅ 可忽略 |
