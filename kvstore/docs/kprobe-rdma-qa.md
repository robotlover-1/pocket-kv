# kprobe + RDMA 增量同步 — 问答整理

> **⚠️ Legacy 传输路径**：本文档描述的是 kprobe→RDMA WRITE 增量同步（**旧实时传输路径**）。当前 `repl_realtime_transport=ebpf+tcp`（独立 ebpf-proxy 进程 + fentry 捕获），kprobe-rdma 不再是默认/生产路径。仅作历史参考。

> 本文档整理了关于 kvstore 中 kprobe+RDMA 增量同步实现的所有问答。
> 涵盖架构设计、BPF 原理、RDMA 操作、数据流、Slave/Master 职责等。

---

## 目录

1. [架构总览](#1-架构总览)
2. [全量 vs 增量：双边 vs 单边](#2-全量-vs-增量双边-vs-单边)
3. [BPF kprobe 相关问题](#3-bpf-kprobe-相关问题)
4. [RDMA 操作相关问题](#4-rdma-操作相关问题)
5. [MR 环形缓冲区](#5-mr-环形缓冲区)
6. [Slave 侧处理](#6-slave-侧处理)
7. [Master 侧处理](#7-master-侧处理)
8. [建链与 MR 交换](#8-建链与-mr-交换)
9. [TCP 保底与去重](#9-tcp-保底与去重)
10. [资源管理](#10-资源管理)

---

## 1. 架构总览

### Q: 现在的全量同步走的双边 RDMA，增量同步走的 kprobe+单边 RDMA 对吗？

**A:** 对，完全正确。

| 同步类型 | QP | 操作类型 | 端口 | 描述 |
|---------|-----|---------|------|------|
| **全量同步 (Fullsync)** | `g_repl_rdma_ctx` | **双边 RDMA SEND/RECV** | 5161 | Master `ibv_post_send` → Slave `ibv_poll_cq` 收数据 |
| **增量同步 (Incremental)** | `g_rdma_kprobe` | **单边 RDMA WRITE** | 5172 | BPF 嗅探 → 用户态转发 → `ibv_post_send(IBV_WR_RDMA_WRITE)` 直接写 Slave 预置内存 |

**全量同步** — 收发都经过 RDMA，两端 CPU 都参与：
```
Master ibv_post_send(RDMA SEND) → Slave ibv_poll_cq → recv_slot
```

**增量同步** — 仅 Master 主动写 Slave 预置内存，Slave CPU 零参与传输：
```
BPF kprobe → ringbuf → 用户态 → RDMA WRITE → Slave MR (DMA直达)
                                                      ↑ Slave 轮询消费
```

---

### Q: kprobe + RDMA 的整体数据流是怎样的？

**A:** 完整数据流如下：

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Master 虚拟机                                 │
├─────────────────────────────────────────────────────────────────────┤
│  [kvstore Master]                                                   │
│         │ (send 增量 RESP，触发 tcp_sendmsg)                          │
│         ↓                                                           │
│  [eBPF kprobe] ──→ [Ring Buffer] ──→ [用户态转发模块]              │
│   kprobe/tcp_sendmsg   内核→用户      ring_buffer__poll              │
│   读取 iov 数据          传递数据      组装 WRITE WR                  │
│                                        │                             │
│                                        ↓ (RDMA WRITE, 单边操作)      │
└──────────────────────────────────────────┼──────────────────────────┘
                                           │ RDMA 网络
                                           ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        Slave 虚拟机                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                    (DMA 直达，CPU 零参与接收)         │
│  [预置 MR 环形缓冲区] ←── [轮询模块] ──→ [kvstore Slave]            │
│      DMA 写入            消费数据         应用命令                    │
│      producer_head        tail 指针        parse_resp_stream         │
│      由 Master 更新       本地更新           → handle_parsed_command  │
└─────────────────────────────────────────────────────────────────────┘
```

**分步说明：**

1. `repl_broadcast()` 广播增量 RESP 命令
2. TCP `send()` 触发内核 `tcp_sendmsg`
3. **kprobe** 在 `tcp_sendmsg` 入口拦截，读取 `msg→iov→iov_base` 数据
4. 数据写入 **BPF ringbuf**（内核→用户态通道）
5. **用户态转发线程** `ring_buffer__poll()` 消费 ringbuf
6. 回调 `kprobe_ringbuf_cb()` 做两次 **RDMA WRITE**：
   - Step 1: `wr_submit_data()` — 写数据到 Slave MR slot
   - Step 2: `wr_submit_head()` — 更新 Slave `producer_head`
7. **Slave 轮询线程** 检查 `producer_head != consumer_tail`
8. 有数据 → 拷贝到 stream buffer → `parse_resp_stream()` → 写入引擎 + AOF

---

## 2. 全量 vs 增量：双边 vs 单边

### Q: 增量同步时，不也是需要先建立 QP 连接吗？

**A:** 是的，增量同步也需要建立独立的 QP。与全量 QP 不同的是建链时机：

| QP | 建立时机 | 端口 | 用途 | 谁发起 |
|----|---------|------|------|-------|
| **Fullsync QP** (`g_repl_rdma_ctx`) | Slave 连接时主动建立 | 5161 | 全量 RDMA SEND/RECV | Slave `slave_thread` 中后台线程 |
| **增量 QP** (`g_rdma_kprobe`) | **懒加载** — 首次 `repl_realtime_send()` 时 | 5172 | 增量 RDMA WRITE | Master `repl_transport_kprobe_rdma_send()` 首次调用 |

**为什么增量 QP 要懒加载？**

因为全量同步可能走 TCP（RDMA fullsync 失败时 fallback），此时增量同步应该也走 TCP，不需要建立 QP。等到确实有增量数据要发时再建 QP，避免无用连接。

```c
// kvs_repl.c — repl_transport_kprobe_rdma_send()
static volatile int mr_connect_started = 0;
if (!mr_connect_started) {
    mr_connect_started = 1;
    // ★ 首次调用时才建立 QP
    pthread_create(&tid, NULL, kprobe_mr_connect_thread, ...);
}
return -1;  // 数据仍走 TCP 保底
```

### Q: 增量同步时 Slave 做了什么？

**A:** Slave 的职责相对简单，主要有三块：

#### ① 初始化阶段 — 注册 MR

```c
// kvs_repl_kprobe.c — repl_kprobe_rdma_slave_accept()
g_slave_ringbuf = calloc(1, sizeof(kprobe_rdma_ringbuf_t));
g_slave_ringbuf_mr = ibv_reg_mr(pd, g_slave_ringbuf,
    KPROBE_RDMA_RINGBUF_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
// 将 rkey + addr 拼接进响应文本
snprintf(resp, resp_cap, "+KPROBERDMA %u %lu ...", rkey, addr);
```

#### ② 监听/建链阶段 — kprobe-rdma listener

独立线程监听 **port+12（5172）** 端口，接受 Master 的 RDMA 连接。

#### ③ 运行阶段 — 轮询消费 MR 环形缓冲区

```c
static void *kprobe_rdma_slave_poll(void *arg) {
    while (g_kprobe_running) {
        __sync_synchronize();           // 内存屏障
        head = rb->producer_head;       // Master 通过 RDMA WRITE 更新
        tail = rb->consumer_tail;       // Slave 本地维护
        
        if (tail == head) { usleep(100); continue; }
        
        while (tail != head) {
            idx = tail % 1024;
            slot_len = *(uint32_t*)(rb->slots + idx * 512);
            slot_data = rb->slots + idx * 512 + 4;
            memcpy(stream_buf, slot_data, slot_len);
            parse_resp_stream(NULL, stream_buf, &stream_len, 1);
            tail++;
        }
        rb->consumer_tail = tail;       // 更新消费位置（本地，无需 RDMA）
        __sync_synchronize();
    }
}
```

**总结：**

| 职责 | 说明 |
|------|------|
| MR 注册 | `ibv_reg_mr` with `IBV_ACCESS_REMOTE_WRITE`，暴露环形缓冲区给 Master 直接写入 |
| 接受 RDMA 连接 | 监听 5172 端口，被动接受 Master 的 QP 连接 |
| 轮询消费 | 死循环检查 `producer_head != consumer_tail`，取出 slot 数据送 `parse_resp_stream` 解析执行 |
| 更新消费指针 | `consumer_tail` 只在 Slave 本地修改（单边 RDMA 不需要告诉 Master） |

### Q: 增量同步走 kprobe+rdma 时，repl_realtime_send 和 repl_transport_kprobe_rdma_send 是不是其实只调用一次就行？

**A:** 不是只调一次，**每条命令都会调**，但它们的职责不同：

| 层次 | 触发频率 | 做什么 |
|------|---------|--------|
| `repl_realtime_send()` | **每条命令** | 选择 transport ops，然后 fallback 到 TCP send |
| `repl_transport_kprobe_rdma_send()` | **每次返回 -1** | 建 QP（仅一次）+ 触发 TCP fallback |
| BPF kprobe + forward_thread | **自动、透明** | TCP send 进内核 → 自动拦截 → ringbuf → 后台轮询 → RDMA WRITE |

`repl_transport_kprobe_rdma_send()` 返回 -1 是一个设计约定 — 它本身不发数据，只是确保首次建立 QP，然后让 TCP 路径做实际的发送 + 提供 kprobe 拦截的数据源。

```c
int repl_realtime_send(conn_t *c, const unsigned char *buf, size_t len) {
    // ops = kprobe-rdma ops
    const repl_transport_ops_t *ops = repl_transport_ops_for_context(KVS_REPL_SEND_REALTIME);
    
    // 调 repl_transport_kprobe_rdma_send() → 返回 -1
    int rc = ops->send(c, buf, len);    // rc = -1
    
    if (rc == 0) return 0;   // 不成立
    
    // TCP fallback — 真正发数据
    rc = repl_transport_tcp_send(c, buf, len);
    return rc;
}
```

---

## 3. BPF kprobe 相关问题

### Q: BPF 挂载到哪里的？

**A:** 挂载在 **`tcp_sendmsg` 内核函数的入口处**。

```c
// repl_kprobe.bpf.c
SEC("kprobe/tcp_sendmsg")
int kprobe_kvs_repl_tcp_sendmsg(struct pt_regs *ctx)

// kvs_repl_kprobe.c — 用户态 attach
link = bpf_program__attach_kprobe(prog, false, "tcp_sendmsg");
```

```
用户态: repl_broadcast("HSET key val")
            ↓ send(tcp_fd, buf, len)
            ↓ (系统调用进入内核)
        tcp_sendmsg(sk, msg, size)   ← ★ BPF kprobe 挂在这里
            ↓
            ├─→ BPF 程序执行:
            │    读取 msg→iov→iov_base 数据
            │    PID 过滤
            │    bpf_ringbuf_output → ringbuf
            │    return 0 (放行 TCP)
            ↓
            └─→ 正常 TCP 协议栈处理 → 网卡发出
```

### Q: 为什么读了 `bpf_probe_read_kernel(iov[0])` 还要读 `bpf_probe_read_user(iov_base)`？

**A:** 这是两个不同层级的指针读取，缺一不可。

因为 `tcp_sendmsg` 的参数 `struct msghdr *msg` 是内核空间的指针，但 `iov_base` 指向的是用户空间的应用程序缓冲区。

```
tcp_sendmsg(struct sock *sk, struct msghdr *msg, size_t size)
                                                    ↑ 内核指针
```

完整路径：

```
ctx->si (寄存器 rsi = msg 指针)
  │ 这是内核空间的地址
  │
  ▼
bpf_probe_read_kernel(&iov, msg+40)    ← 读 msg->msg_iter->iov
  │ iov 指针也是内核空间的（iovec 数组由 kernel 管理）
  │
  ▼
bpf_probe_read_kernel(&vec, &iov[0])   ← 读 iovec[0] 结构体
  │ vec.b = iov_base（用户空间地址）
  │ vec.l = iov_len
  │
  ▼
bpf_probe_read_user(buf, vec.b)        ← 读实际数据
    用户空间地址 → 真正的 RESP 命令内容
```

**三步缺一不可：**
1. `bpf_probe_read_kernel(msg+40)` → 读 iov 指针（内核读内核）
2. `bpf_probe_read_kernel(&iov[0])` → 读 iovec 结构体内容（内核读内核）
3. `bpf_probe_read_user(iov_base)` → 读实际数据（内核读用户）

### Q: `bpf_probe_read_kernel` 和 `bpf_probe_read_user` 就是将数据拷贝到 ringbuf 吗？

**A:** 不是。它们只是读取到**临时缓冲区（per-CPU buffer）**，真正写入 ringbuf 的是 `bpf_ringbuf_output`。

```
用户程序 buf (用户空间)
    │ bpf_probe_read_user()
    ▼
per-CPU 临时缓冲区 (BPF map, 504B)    ← 临时工作台
    │ bpf_ringbuf_output()
    ▼
BPF ringbuf (1MB, 内核 map)           ← 传递到用户态
    │ ring_buffer__poll() 消费
    ▼
用户态 → kprobe_ringbuf_cb() 回调
```

### Q: `bpf_ringbuf_output` 有了数据就会触发 ringbuf_cb 吗？

**A:** 不是。`bpf_ringbuf_output` 只是把数据写进内核的 ringbuf map，**不会主动触发用户态回调**。用户态回调是通过 **`ring_buffer__poll()` 轮询**来触发的。

```c
// forward_thread 每 5ms 轮询一次
while (g_kprobe_running && g_rdma_kprobe.connected) {
    int err = ring_buffer__poll(g_kprobe_ringbuf, 5 /* ms */);
    // 如果有数据，内部会逐个调 kprobe_ringbuf_cb()
}
```

### Q: BPF Maps 是什么？BPF target 下系统头文件为什么不能用？

**A:** BPF Maps 是内核 BPF 子系统提供的一组**键值对存储结构**，用于 BPF 程序↔用户态程序通信以及 BPF 程序内部状态保持。

本项目用了 4 种 Map：

| Map | 类型 | 用途 |
|-----|------|------|
| `kprobe_ctl` | `BPF_MAP_TYPE_ARRAY` | 控制开关、目标 PID — 用户态写入，BPF 读取 |
| `kprobe_stats` | `BPF_MAP_TYPE_ARRAY` | 统计计数 — BPF 写入，用户态读取 |
| `repl_ringbuf` | `BPF_MAP_TYPE_RINGBUF` | 数据传递 — BPF 写入，用户态 `ring_buffer__poll` 消费 |
| `kprobe_tmpbuf` | `BPF_MAP_TYPE_PERCPU_ARRAY` | 临时缓冲区 — 规避 BPF 栈 512B 限制 |

**为什么 BPF target 下不能用系统头文件？**

BPF 程序使用 `-target bpf`，生成的 ELF 目标格式是 **BPF 字节码**，不是 x86-64 原生代码：

1. **`/usr/include` 的头文件包含 glibc 内联代码、系统调用封装** — 在 BPF 虚拟机中无法执行
2. **BPF 只能调用 BPF helper functions**（`bpf_map_lookup_elem`、`bpf_probe_read_kernel` 等），不能调用真正的系统调用或 libc 函数
3. **标准 Linux 结构体在不同内核版本布局不同** — BPF CO-RE 需要 BTF 信息做重定位，系统头文件可能与内核 BTF 不匹配

### Q: 临时缓冲区 per-CPU 大小是多大？ringbuf 是多大？

**A:**

| 资源 | 大小 |
|------|------|
| per-CPU 缓冲区 (`kprobe_tmpbuf`) | **504 字节** (4B 头 + 500B 数据) |
| BPF ringbuf (`repl_ringbuf`) | **1MB** (`1 << 20`) |

### Q: BPF 栈有 512 字节，per-CPU 缓冲区只有 504 字节，为什么还要另开缓冲区？数据不是直接放到 ringbuf 中了吗？

**A:** 两个原因：

**原因 1：栈上还有其他变量，总和超过 512B**

```c
int kprobe_kvs_repl_tcp_sendmsg(struct pt_regs *ctx)
{
    // 栈变量:
    __u64 *enabled;         // 8B
    __u64 *target_pid;      // 8B
    __u64 *stat;            // 8B
    __u32 pid;              // 4B
    __u32 size;             // 4B
    __u32 map_key = 0;      // 4B
    
    // read_msg_data 内部:
    unsigned long long safe_len;  // 8B
    unsigned long nr_segs;        // 8B
    struct {...} *iov, vec;       // ~24B
    // 合计 ≈ 80B
    
    // 如果 entry[504] 放栈上: 504+80 = 584B > 512B ❌
    // per-CPU buffer 后: 仅 8B 指针 + 80B = 88B < 512B ✅
}
```

**原因 2：per-CPU buffer 是 staging buffer，不是直接写 ringbuf**

```
用户空间数据 (iov_base)
    │ bpf_probe_read_user()
    ▼
per-CPU tmpbuf (504B)     ← BPF map 空间，不受栈限制
    │ 组装好 [4B len][500B data]
    ▼ bpf_ringbuf_output()
BPF ringbuf (1MB)         ← 内核→用户态通信
```

先组装好格式（4B 头 + payload），然后一次性推入 ringbuf。

### Q: `kprobe_load_bpf()` 能详细解释一下吗？

**A:** 该函数完整加载 BPF 程序到内核并 attach：

```c
static int kprobe_load_bpf(void) {
    // ① 跳过已加载
    if (g_kprobe_obj) return 0;

    // ② 打开 .o 文件 — 解析 ELF 中的 map 定义
    g_kprobe_obj = bpf_object__open_file("repl_kprobe.bpf.o", NULL);

    // ③ 加载到内核 — BPF verifier 检查 + 创建 maps + JIT 编译
    rc = bpf_object__load(g_kprobe_obj);

    // ④ 获取 map 文件描述符（用户态操作 maps 的句柄）
    g_kprobe_ctl_fd = bpf_object__find_map_fd_by_name(..., "kprobe_ctl");
    g_kprobe_ringbuf_fd = bpf_object__find_map_fd_by_name(..., "repl_ringbuf");

    // ⑤ attach 到内核函数 tcp_sendmsg
    link = bpf_program__attach_kprobe(prog, false, "tcp_sendmsg");
    // 之后每次调用 tcp_sendmsg() 都会执行我们的 BPF 代码

    // ⑥ 创建用户态 ringbuf 消费端（与回调绑定）
    g_kprobe_ringbuf = ring_buffer__new(g_kprobe_ringbuf_fd,
        kprobe_ringbuf_cb, NULL, NULL);
    // forward_thread 调 ring_buffer__poll() 时自动触发回调

    return 0;
}
```

### Q: `kprobe_rdma_forward_thread` 能解释一下吗？

**A:** 这是 **Master 侧的核心转发线程**，职责就是：**轮询 BPF ringbuf，有数据就调回调 `kprobe_ringbuf_cb`**。

```c
static void *kprobe_rdma_forward_thread(void *arg) {
    int poll_count = 0;
    while (g_kprobe_running && g_rdma_kprobe.connected) {
        if (g_kprobe_ringbuf) {
            // ★ 核心: ring_buffer__poll() — 读取 BPF ringbuf
            // 有数据时内部自动调 kprobe_ringbuf_cb()
            int err = ring_buffer__poll(g_kprobe_ringbuf, 5 /* ms */);
            if (err > 0) poll_count += err;
            else if (err < 0 && err != -EAGAIN) usleep(1000);
        } else {
            usleep(10000);
        }
    }
    return NULL;
}
```

### Q: 增量同步的 kprobe 程序 `kprobe_kvs_repl_tcp_sendmsg` 中数据不是直接放在 ringbuf 中了吗？那 per-CPU 缓冲在哪里用到了？

**A:** 数据是两层：**先到 per-CPU 缓冲区组装，再到 ringbuf 传递**。

```c
// ① 获取 per-CPU 缓冲区指针
entry = bpf_map_lookup_elem(&kprobe_tmpbuf, &map_key);

// ② 读数据到 per-CPU 缓冲区（probe_read_user 需要目标地址）
read_msg_data(msg_ptr, (*entry) + 4, ...);
//                    ↑ 写到 per-CPU 缓冲区

// ③ 从 per-CPU 缓冲区拷贝到 ringbuf
bpf_ringbuf_output(&repl_ringbuf, *entry, entry_size, 0);
//                                ↑ 从 per-CPU 缓冲区读
```

per-CPU 缓冲区是 staging buffer（临时工作台），在 BPF map 空间中（不受 512B 栈限制）。先在这里组装好 `[4B len][payload]` 格式，再一次性推入 ringbuf。

---

## 4. RDMA 操作相关问题

### Q: 为什么需要两个 RDMA WRITE（submit_data + submit_head）？

**A:** 这是**生产者-消费者环形缓冲区的标准模式**：

```c
/* Step 1: 写数据到 slot */
wr_submit_data(slot, payload_len + 4);
//  → RDMA WRITE: 数据写入 Slave MR 的 slots[idx * 512]

/* Step 2: 更新生产者指针（通知 Slave） */
wr_submit_head(slot);
//  → RDMA WRITE: 更新 Slave MR 的 producer_head
```

| 操作 | 写入位置 | 长度 | 作用 |
|------|---------|------|------|
| `wr_submit_data` | `remote_data_base + idx*512` | 4+payload (最大 504B) | **写实际数据**到 slot |
| `wr_submit_head` | `remote_head_addr` (偏移 0) | 8B | **通知 Slave**：有新数据可消费 |

如果只写数据不更新 head，Slave 的 `producer_head == consumer_tail`，永远不知道有新数据。

### Q: `wr_submit_data` 和 `wr_submit_head` 里的 slot 和全量中的 send slot 有什么区别？

**A:** 两者用途和机制完全不同：

| 对比维度 | Fullsync SEND slot (`repl_rdma_send_slot_t`) | Kprobe-rdma WRITE slot (`rdma_write_slot_t`) |
|---------|---------------------------------------------|---------------------------------------------|
| **RDMA 操作类型** | **RDMA SEND**（双边） | **RDMA WRITE**（单边） |
| **数据目的地** | Slave 提前 post 的 RECV 缓冲区 | Slave 预置 MR 环形缓冲区 |
| **Slave 接收方式** | `ibv_poll_cq` 收 completion | 轮询 MR 环形缓冲区 |
| **一个 slot 一次操作** | **一次 SEND** 发完 | **两次 WRITE**：data + head |
| **数据量** | 全量快照，可达 MB 级 | 增量命令，最大 500B |
| **属主** | `g_repl_rdma_ctx.send_slots[]` | `g_wr_slots[]`（全局） |

**Fullsync SEND slot** — 就是**发送缓冲区**。数据拷贝进去，`ibv_post_send` 发出去，Slave 从对应的 recv_slot 收。

**Kprobe-rdma WRITE slot** — 是**暂存区**，数据先放到这里，然后通过 RDMA WRITE **写入 Slave 的环形缓冲区**。

### Q: PD、CQ、QP 是什么时候创建的？在 established 之后还是之前？

**A:** **在 established 之前创建。** 不管是 Master（主动连接）还是 Slave（被动监听），PD、CQ、QP 都在连接建立前就创建好了。

**Master 侧：**
```c
rdma_resolve_addr()
rdma_resolve_route()
ibv_alloc_pd()               // ① PD
ibv_create_cq()              // ② CQ
rdma_create_qp()             // ③ QP
ibv_reg_mr()                 // ④ 本地 MR
rdma_connect()               // 发出连接请求
rdma_get_cm_event(ESTABLISHED)  // 等 established
```

**Slave 侧：**
```c
rdma_get_cm_event(CONNECT_REQUEST)
ibv_alloc_pd()               // ① PD
ibv_create_cq()              // ② CQ
rdma_create_qp()             // ③ QP
repl_kprobe_rdma_slave_accept()  // ④ 注册 MR
rdma_accept()                // 接受连接
rdma_get_cm_event(ESTABLISHED)
```

**为什么？** QP 需要关联 PD 和 CQ，而 `rdma_connect()` / `rdma_accept()` 发送的 CONNECT 请求中已经包含了 QP 信息（QP Number），对端的 RNIC 需要知道 QP 号才能建立连接。

### Q: 那 MR 呢？

**A:** MR 也是在 **established 之前**创建的。

- **Master 侧 MR**（WRITE slot 的本地 MR）完全在本地使用，QP 创建后就可以注册，在 connect 之前完成。
- **Slave 侧 MR**（环形缓冲区）也要在 `rdma_accept()` 之前注册好，因为 MR 信息（rkey、addr）需要通过 TCP 控制通道发给 Master。

**总结：所有资源（PD、CQ、QP、MR）都在 established 之前创建。** established 之后直接开始数据传输。

### Q: 增量同步的 QP 什么时候才关闭？

**A:** 增量 QP 在 **`repl_kprobe_rdma_cleanup()`** 中关闭，触发路径有两条：

**路径 A：Slave 断开连接**

```c
// kvs_repl.c — repl_transport_kprobe_rdma_disconnect_slave()
static void repl_transport_kprobe_rdma_disconnect_slave(int fd) {
    (void)fd;
    repl_kprobe_rdma_cleanup();  // ← 关闭增量 QP
}
```

**路径 B：程序退出**

在 `kvstore.c` 的 main 函数退出前清理所有资源。

**清理函数：**
```c
void repl_kprobe_rdma_cleanup(void) {
    g_kprobe_running = 0;        // 停止所有线程循环
    bpf_map_update_elem(ctl, 0, 0);  // 关 kprobe
    kprobe_unload_bpf();             // unload BPF 程序
    rdma_disconnect(g_rdma_kprobe.id);  // 断开 QP
    ibv_destroy_cq(g_rdma_kprobe.cq);   // 销毁 CQ
    ibv_dealloc_pd(g_rdma_kprobe.pd);   // 销毁 PD
    rdma_destroy_id(g_rdma_kprobe.id);  // 销毁 CM ID
    memset(&g_slave_mr, 0, sizeof(g_slave_mr));  // 清 MR 信息
}
```

**结论：** 增量 QP 的生命周期与 slave 连接绑定——slave 重连或断开时关闭，下次首次 `repl_realtime_send()` 时重新懒加载建立。

---

## 5. MR 环形缓冲区

### Q: MR 环形缓冲区怎么设计的？

**A:** 定义在 `include/kvstore/replication/repl_kprobe.h`：

```c
typedef struct __attribute__((packed)) kprobe_rdma_ringbuf_s {
    volatile uint64_t producer_head;   /* 偏移 0  — Master RDMA WRITE 更新 */
    volatile uint64_t consumer_tail;   /* 偏移 8  — Slave 本地更新 */
    unsigned char slots[1024 * 512];   /* 偏移 16 — 1024 个 slot, 每个 512B */
} kprobe_rdma_ringbuf_t;
```

**布局示意：**

```
偏移 0:   producer_head (8B)  ← Master 通过 RDMA WRITE 更新
偏移 8:   consumer_tail (8B)  ← Slave 本地更新（无需 RDMA）
偏移 16:  slots[0]           ┐
           [4B len][508B payload]
          slots[1]           │ 1024 个 slot
           [4B len][508B payload]  每个 512B
          ...                │
          slots[1023]        ┘
总大小: 16 + 1024×512 = 524,304B ≈ 512KB
```

**每个 slot 格式：**
```
[4 bytes: payload_len (uint32_t)]
[payload_len bytes: RESP 协议数据]
```

**关键设计点：**

| 特性 | 说明 |
|------|------|
| **单生产者单消费者** | Master 唯一写 `producer_head`，Slave 唯一写 `consumer_tail` |
| **幂等 slot** | 每个 slot 写完后才更新 head，Master 从不回退 head |
| **天然覆盖安全** | 即使 slot 1023 后回到 slot 0，只要 tail 也前进了就不会覆盖未消费数据 |
| **无需原子操作** | `producer_head` 只被 Master 写，`consumer_tail` 只被 Slave 写 |
| **RDMA WRITE 单边** | Master 直接写 Slave 内存，Slave CPU 零参与写入过程 |

### Q: MR 信息交换是干嘛的？

**A:** MR 信息交换的目的是让 **Master 获得直接写入 Slave 内存的"钥匙"**。

RDMA WRITE（单边）的特点是：**Master 直接写 Slave 的内存，Slave CPU 完全不知情**。但 RNIC 硬件要求：

1. **Slave 必须先注册 MR** — 告诉网卡"这块内存允许远程写入"
2. **Slave 把 MR 的 `rkey` 和地址告诉 Master**
3. **Master 拿着 `rkey` 和地址才能发起 RDMA WRITE**

**交换了什么：**
```
Slave 注册 MR 后 → 告诉 Master 三个信息:
1. rkey   = 3245678        ← "访问令牌" — 网卡验证写入权限
2. addr   = 0x7f5ce453f000 ← Slave 环形缓冲区的虚拟地址
3. size   = 524304          ← 缓冲区总大小
```

**没有 MR 交换会怎样？**
```c
// kprobe_ringbuf_cb() 中的保护检查
if (g_slave_mr.rkey == 0 || !g_rdma_kprobe.connected) {
    return 0;  // ★ MR 未就绪，跳过 RDMA WRITE
}
```

如果 `rkey = 0` 就发 RDMA WRITE：
- RNIC 拒绝写入 → **QP 进入 ERROR 状态** → 整个 QP 报废
- 地址随机值 → 写错内存 → **Slave 进程 crash 或数据损坏**

### Q: MR 信息交换是谁发送的？是 Slave 发送还是 Master 主动请求？

**A:** **Slave 被动响应，Master 主动请求。** 完整流程如下：

```
Master                                      Slave
  │                                           │
  │── RDMA QP Connect (port 5172) ──────────→ │  ① Master 发起 RDMA 连接
  │                                           │── ibv_reg_mr(IBV_ACCESS_REMOTE_WRITE)
  │                                           │  ② Slave 注册 MR
  │                                           │
  │── TCP: "KPROBEMR\r\n" ──────────────────→ │  ③ Master 通过 TCP 发送请求
  │                                           │── repl_kprobe_rdma_get_mr_text()
  │                                           │  ④ Slave 构造响应
  │←─ TCP: "+KPROBERDMA rkey addr ..." ──────│  ⑤ Slave 回复 MR 信息
  │                                           │
  │── repl_kprobe_rdma_parse_mr_info_direct() │  ⑥ Master 保存 MR 信息
```

**为什么需要两步（RDMA 建连 + TCP 传 MR 信息）？** 因为 RDMA CM 的 private data 有大小限制（通常 56 字节），而 MR 信息放不下。所以先建 RDMA QP，再通过 TCP 控制通道完成 MR 交换。

### Q: KPROBEMR 为什么用 TCP 发送？

**A:** 因为 **KPROBEMR 是一个控制命令，不是数据传输**。原因有三：

1. **TCP 通道已经存在** — `tcp_fd` 从复制控制链路 `dup()` 而来，直接复用
2. **KPROBEMR 是请求-响应模式** — 需要 Slave 回复 `+KPROBERDMA`，RDMA 是单向的（WRITE 只能 Master→Slave），没法让 Slave 回复
3. **新 QP 没有 RECV 缓冲区** — 增量 QP 只做 RDMA WRITE，没有 post RECV，没法收数据

### Q: `repl_kprobe_rdma_slave_accept` 中的 `resp_buf` 写入的是 `+KPROBERDMA`，是什么时候发送给 Master 的？

**A:** 在 listener 线程中 `resp_buf` 是**局部变量，实际没有被使用**。真正的 MR 信息是通过另外的路径发送的：

```c
// Slave listener 线程中:
char resp_buf[256];
repl_kprobe_rdma_slave_accept(pd, resp_buf, sizeof(resp_buf));
// resp_buf 被写入 "+KPROBERDMA rkey addr..."
// 但这里 resp_buf 是局部变量，没有被使用！
rdma_accept(...);   // 不接受 private data
```

真正的 MR 信息交换在 **TCP 控制通道**上，由 reactor 线程处理：

```
Master (TCP):  KPROBEMR\r\n ──────────────────→ Slave reactor
                                                    │
                                        kvstore.c:1038
                                        repl_kprobe_rdma_get_mr_text()
                                        → 读取 g_slave_ringbuf_mr->rkey/addr
                                                    │
Slave (TCP):   ←── +KPROBERDMA rkey addr ... ────── Master reactor
```

`resp_buf` 是**无效代码**——`repl_kprobe_rdma_slave_accept()` 的 `resp` 参数负责构造文本，但 caller 没使用它。

---

## 6. Slave 侧处理

### Q: Slave 轮询时是不是需要 Slave 的 CPU 参与？

**A:** **需要，但只参与消费阶段，不参与写入阶段。**

```c
static void *kprobe_rdma_slave_poll(void *arg) {
    while (g_kprobe_running) {
        __sync_synchronize();           // ① CPU 参与: 内存屏障
        head = rb->producer_head;       // ② CPU 参与: 读变量
        tail = rb->consumer_tail;
        
        if (tail == head) { usleep(100); continue; }
        
        // ③ CPU 参与: 拷贝数据、解析 RESP
        memcpy(stream_buf, rb->slots + off + 4, slot_len);
        parse_resp_stream(NULL, stream_buf, &stream_len, 1);
        
        rb->consumer_tail = tail;       // ④ CPU 参与: 更新指针
    }
}
```

**关键区别在于 RDMA WRITE 本身的写入过程是否消耗 Slave CPU：**

| 阶段 | Slave CPU 参与？ |
|------|----------------|
| **RDMA WRITE 写入数据到 MR** | ❌ 不参与（单边 RDMA，RNIC 直写内存） |
| **RDMA WRITE 更新 producer_head** | ❌ 不参与（单边 RDMA） |
| **轮询检查 head != tail** | ✅ 参与（100us 一次） |
| **拷贝数据 + parse_resp_stream** | ✅ 参与 |

对比 TCP 路径，Slave CPU 全程参与 `recv()` + 拷贝 + 解析。RDMA WRITE 的优势就是**写入阶段 Slave CPU 零参与**。

### Q: Slave 接收到增量同步的数据后，调用了 `parse_resp_stream` 解析发送的内容，但是怎么将命令等保存到本地的 AOF 文件的？

**A:** 调用链如下：

```
parse_resp_stream(buf, from_replication=1)
  → handle_parsed_command(c, argc, argv, raw, rawlen, 1)
      │
      ├─ engine_set(key, val)             ← ① 写入内存 hashtable
      │
      └─ if (from_replication && 是写命令 && !全量同步中):
           ├─ persist_append_raw(raw, rawlen)  ← ② 原始 RESP 追加到 AOF
           │     └─ io_uring pwrite() → 磁盘
           │
           ├─ repl_slave_note_durable()   ← ③ 更新持久化偏移量 + 发 REPLACK
           │
           └─ repl_slave_note_applied()   ← ④ 更新应用偏移量
```

| 步骤 | 做了什么 | 位置 |
|------|---------|------|
| `engine_set()` | 写入内存 hashtable | `kvstore.c:1528` |
| `persist_append_raw(raw, rawlen)` | 原始 RESP 字节追加到 AOF fd | `kvstore.c:1629` |
| `repl_slave_note_applied()` | `g_slave_repl_applied_offset += rawlen` | `kvs_repl.c` |
| `repl_slave_note_durable()` | 更新 durable offset，发 REPLACK | `kvs_repl.c` |

**全量同步期间跳过 AOF** — 数据直接加载到内存后 dump 到文件。
**增量同步期间才写 AOF** — 每条 RESP 原始字节追加到 `g_aof_fd`。

### Q: 增量同步时，Slave 先监听 request 再监听 establish 事件吗？

**A:** Slave 的 listener 是**被动等待**的，事件顺序是：

```
Slave listener:
  bind → listen
      │
      ├── ① rdma_get_cm_event(CONNECT_REQUEST)  ← Master 发起了 rdma_connect()
      │
      ├── 创建 PD/CQ/QP/MR
      │
      ├── ② rdma_accept()                       ← 接受连接
      │
      └── ③ rdma_get_cm_event(ESTABLISHED)      ← 双方 QP 就绪
```

对应 Master 侧：
```
Master:  rdma_connect()  →  Slave 收到 CONNECT_REQUEST
                            →  Slave 创建资源 → rdma_accept()
                            →  Master/Slave 都收到 ESTABLISHED
```

所以 Slave 先等 `CONNECT_REQUEST`（连接请求），然后创建资源、接受连接，最后等 `ESTABLISHED`（连接确认）。

---

## 7. Master 侧处理

### Q: Master 做了什么？

**A:** Master 在增量同步中的职责**远比 Slave 复杂**，完整链路如下：

#### ① 加载 BPF 并 attach

```c
// repl_kprobe_rdma_master_init()
bpf_object__load(obj);
bpf_program__attach(prog, "tcp_sendmsg");
bpf_map_update_elem(ctl, &pid, &pid);  // 设置 PID 过滤
```

#### ② 建立独立 RDMA QP

```c
// kprobe_rdma_qp_connect()
rdma_create_ep(&g_rdma_kprobe.id, ...);  // 端口 5172
```

#### ③ 交换 MR 信息

```
Master ──[TCP: KPROBEMR]──→ Slave
Master ←─[TCP: +KPROBERDMA rkey addr]── Slave
Master 保存: g_slave_mr.rkey = X
```

#### ④ 启动 ringbuf 消费线程（核心转发）

```c
// forward_thread → ring_buffer__poll(5ms)
// 有数据 → kprobe_ringbuf_cb():
//   1. wr_slot_acquire()    — 获取空闲 WR slot
//   2. wr_submit_data()     — RDMA WRITE 数据到 Slave MR slot
//   3. wr_submit_head()     — RDMA WRITE 更新 producer_head
```

#### ⑤ TCP 保底永不关闭

```c
// repl_transport_kprobe_rdma_send() → 返回 -1
// repl_transport_tcp_send() → TCP 正常发送（kprobe 拦截源）
```

**Master 职责总结：**

| 阶段 | 职责 |
|------|------|
| **初始化** | 编译/加载 BPF 程序，attach kprobe/tcp_sendmsg |
| **建链** | 创建独立 QP，连接到 Slave 5172 端口，交换 MR 信息 |
| **运行时** | `ring_buffer__poll` 消费 ringbuf → RDMA WRITE 到 Slave MR |
| **并发控制** | 管理 8 个 WR slot，控制 in_flight 数量，轮询 CQ 回收完成 |
| **保底** | `kprobe_rdma_send()` 返回 -1，TCP 始终正常发送 |
| **统计** | 记录事件数、字节数、RDMA WRITE 次数和错误数 |

### Q: 怎么知道 RDMA WRITE 是否成功？到达的数据可能都是通过 TCP 进行传输的？

**A:** 这正是当前架构的一个**真实风险**。

因为 `repl_transport_kprobe_rdma_send()` 直接返回 -1，TCP **始终发送**。如果 RDMA WRITE 静默失败，Slave 完全不知道——因为 TCP 保底路径会把数据完整送达。

**现有检测手段：**

**① Master 侧 CQ 轮询（检测 WR 是否完成）**
```c
ibv_poll_cq(g_rdma_kprobe.cq, 1, &wc);
if (wc.status != IBV_WC_SUCCESS) {
    g_rdma_errors++;  // ★ RDMA WRITE 失败计数
}
```

**② 统计日志**
```c
g_rdma_writes     // RDMA WRITE 成功次数
g_rdma_errors     // RDMA WRITE 失败次数
```

**缺乏真正的端到端验证。** 目前没有任何机制能确认 "Slave MR 中某条数据确实是通过 RDMA WRITE 写入的"。

**改进思路：**

| 方案 | 描述 |
|------|------|
| **A: 路径标记** | BPF 在数据中嵌入 magic byte `0xRD`，Slave poll 可识别 RDMA 路径数据 |
| **B: 独立统计** | Slave 侧分别统计 TCP 和 RDMA 到达的命令数，对比差异 |
| **C: 序列号校验** | Master 在 RDMA WRITE 中带递增序列号，Slave 检查是否有空洞 |

---

## 8. 建链与 MR 交换

### Q: `repl_transport_kprobe_rdma_connect_slave` 这个函数是什么时候触发的？

**A:** 这个函数在当前代码中**不被 `slave_thread` 直接调用**。它作为 `g_repl_transport_kprobe_rdma_ops` 的 `.connect_slave` 回调存在，但实际 kprobe-rdma 的连接走的是**懒加载路径**。

```
repl_realtime_send(c, buf, len)
  → ops->send() = repl_transport_kprobe_rdma_send()
      │
      ├─ 首次调用：
      │    getpeername() → 创建后台线程
      │    → kprobe_mr_connect_thread()
      │        → repl_kprobe_rdma_connect_mr()
      │            → kprobe_rdma_qp_connect()    ① 连 RDMA QP (port+12)
      │            → 启动 forward_thread          ② 开始轮询 ringbuf
      │            → send("KPROBEMR\r\n")         ③ 通过 TCP 请求 MR 信息
      │
      └─ 返回 -1 → reactor 通过 TCP send() 保底
```

### Q: Kprobe+RDMA 的 Slave 初始化线程中，不需要解析路由和解析地址吗？

**A:** 不需要。Slave 是**被动监听方**，只有 Master（主动连接方）才需要解析地址和路由。

| 步骤 | Master | Slave |
|------|--------|-------|
| `rdma_resolve_addr()` | ✅ **需要** | ❌ 不需要 |
| `rdma_resolve_route()` | ✅ **需要** | ❌ 不需要 |
| `rdma_bind_addr()` | ❌ 不需要 | ✅ **需要**—绑定端口 |
| `rdma_listen()` | ❌ 不需要 | ✅ **需要**—开始监听 |
| `rdma_connect()` | ✅ **主动发起** | ❌ 不需要 |
| `rdma_accept()` | ❌ 不需要 | ✅ **被动接受** |

**原理：** 地址/路由解析解决的是"怎么找到对端"的问题。Master 需要知道 Slave 的 IP 和端口，所以需要解析。Slave 只是**贴个牌子说"我在这"**，等着别人来找它。

---

## 9. TCP 保底与去重

### Q: 怎么区分数据是通过 TCP 还是 RDMA WRITE 发送到 Slave 的？

**A:** **不区分。** 两条路径都调用同一个入口 `parse_resp_stream(NULL, buf, &len, 1)`，都写入引擎，通过 **`repl_offset` 去重**。

```
                     repl_broadcast(buf, len)
                           │
            ┌──────────────┼──────────────┐
            ▼              │              ▼
       TCP send()          │    kprobe/tcp_sendmsg
            │              │         │
            ▼              │         ▼
      ┌─────────┐         │   BPF ringbuf → kprobe_ringbuf_cb
      │ Slave   │         │   → RDMA WRITE
      │ recv()  │         │         │
      │    ↓    │         │         ▼
      │ parse_  │         │   ┌─────────────┐
      │ resp_   │         │   │ Slave MR    │
      │ stream  │         │   │ ring buffer │
      │ (buf,1) │         │   │    ↓        │
      └────┬────┘         │   │ slave_poll  │
           │              │   │ parse_resp  │
           │              │   │ _stream     │
           │              │   │ (buf,1)     │
           │              │   └──────┬──────┘
           │              │          │
           └──────────────┼──────────┘
                          ▼
                   engine_set(key, val)
                   (Slave 写入引擎)
```

**去重机制：** `parse_resp_stream` 内部通过 `repl_offset` 跟踪已应用的命令偏移量。同一条命令：

- TCP 先到 → offset 更新 → 已写入引擎
- RDMA WRITE 后到 → offset 已经 ≥ 该命令 → **跳过不执行**

### Q: MR 没就绪时，从内核拦截的数据放在哪里的？

**A:** **哪都没放，直接丢弃了。**

```c
if (g_slave_mr.rkey == 0 || !g_rdma_kprobe.connected) {
    return 0;  // ← 告诉 ringbuf: "消费完成，数据可以覆盖了"
}
```

`kprobe_ringbuf_cb` 返回 0 表示"我已读完，这个 entry 可以释放"。BPF ringbuf 是个**覆盖式环形缓冲区**，不消费就被新数据覆盖。

**那数据丢了怎么办？——TCP 路径已经发出去了。**

```
时间线:
repl_broadcast("SET key val")
  ├─→ send(tcp_fd, "SET key val")     ← TCP 已发出！（保底）
  │
  └─→ tcp_sendmsg() 触发 kprobe
       → ringbuf → kprobe_ringbuf_cb()
            ├─ MR 就绪? → RDMA WRITE ✅
            └─ MR 未就绪? → return 0 ❌ (丢弃)
```

MR 未就绪期间的数据全部走 TCP 到达 Slave，完全正确。RDMA WRITE 只是加速路径，丢了回 TCP 就好。

---

## 10. 资源管理

### Q: ringbuf 一次可以放两个 per-CPU 缓冲区的数据？

**A:** 1MB 的 ringbuf 可以放很多个。每个 entry 最大 504B：

```
ringbuf 容量: 1MB = 1,048,576 字节
每 entry 最大: 504B
≈ 可同时存放约 2000+ 条 entry
```

但注意，**不是"一个 ringbuf entry 放两个 per-CPU 缓冲区"**。per-CPU 缓冲区是临时工作台，数据先读到这里，然后 `bpf_ringbuf_output()` 把数据**拷贝**到 ringbuf。每个 ringbuf entry 最大就是 504B（4B 头 + 500B 数据），不会合并两个。

### Q: 数据的 slot 索引是怎么计算的？

**A:** Master 通过 `g_wr_producer_seq`（全局生产者序号）计算目标 slot：

```c
// wr_submit_data() 中
uint64_t slot_idx = (uint64_t)(g_wr_producer_seq % g_slave_mr.slot_count);
uint64_t remote_off = slot_idx * g_slave_mr.slot_capacity;
// remote_addr = remote_data_base + remote_off
```

Slave 轮询时通过 `consumer_tail` 计算要消费的 slot：

```c
size_t idx = tail % KPROBE_RDMA_SLOT_COUNT;
size_t off = idx * KPROBE_RDMA_SLOT_CAPACITY;
```

---

## 附录：关键文件索引

| 文件 | 内容 |
|------|------|
| `src/replication/bpf/repl_kprobe.bpf.c` | BPF kprobe 程序，hook `tcp_sendmsg` |
| `src/replication/kvs_repl_kprobe.c` | kprobe+RDMA 核心模块：BPF 加载、ringbuf 消费、RDMA WRITE、Slave 轮询 |
| `include/kvstore/replication/repl_kprobe.h` | 头文件：环形缓冲区结构体 `kprobe_rdma_ringbuf_t`、函数声明 |
| `src/replication/kvs_repl.c` | Transport ops `g_repl_transport_kprobe_rdma_ops`、Slave thread hybrid 路径 |
| `src/main/kvstore.c` | `KPROBEMR`/`+KPROBERDMA` 命令处理、`parse_resp_stream()` |
| `docs/kprobe-rdma-incrsync-implementation.md` | 完整实现文档 |
| `docs/rdma-fullsync-implementation.md` | 全量同步实现文档（对比参考） |
