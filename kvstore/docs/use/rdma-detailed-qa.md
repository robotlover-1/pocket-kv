# KVStore RDMA 详细问答文档

> **⚠️ Legacy 传输路径**：本文档的 eBPF sockmap（§7-10）与 kprobe+RDMA WRITE 增量（§12-13）描述的是**旧实时传输路径**。当前 `repl_realtime_transport=ebpf+tcp`（独立 ebpf-proxy 进程 + fentry 捕获），kprobe-rdma 不再是默认路径。RDMA 全量同步部分仍有效。当前架构见 [`replication-mechanism-qa.md`](replication-mechanism-qa.md) 与 [`ebpf-forwarding-optimization-journey.md`](ebpf-forwarding-optimization-journey.md)。

> 本文档覆盖从主函数入口到 RDMA 全量同步、Pipeline 发送、CQ 轮询、错误处理、降级机制、eBPF sockmap 等全部核心话题，基于源码分析和实测验证。

---

## 目录

1. [RDMA 整体架构与流程](#1-rdma-整体架构与流程)
2. [Pipeline 发送模式](#2-pipeline-发送模式)
3. [CQ 轮询线程详解](#3-cq-轮询线程详解)
4. [WR 与 WC 的关系](#4-wr-与-wc-的关系)
5. [全量同步失败处理与降级](#5-全量同步失败处理与降级)
6. [Slave 线程的三种路径与重复代码](#6-slave-线程的三种路径与重复代码)
7. [eBPF sockmap 数据重定向与零拷贝](#7-ebpf-sockmap-数据重定向与零拷贝)
8. [eBPF 程序加载与管理](#8-ebpf-程序加载与管理)
9. [Pinned Maps](#9-pinned-maps)
10. [Fd 注册函数详解](#10-fd-注册函数详解)
11. [实时发送派发机制](#11-实时发送派发机制)
12. [RDMA WRITE 增量同步](#12-rdma-write-增量同步)
13. [Kprobe 捕获](#13-kprobe-捕获)
14. [Q&A 汇总](#14-qa-汇总)

---

## 1. RDMA 整体架构与流程

### 1.1 从主函数到 RDMA 的完整链路

```
main() [kvstore.c]
  │
  ├─ 解析命令行参数 (--rdma-dev, --rdma-port, --rdma-recv-slots 等)
  │
  ├─ reactor_start() / proactor_start()  ← 事件循环
  │     │
  │     └─ on_read() → parse_resp_stream() → handle_parsed_command()
  │           │
  │           └─ "REPLSYNC" 命令处理:
  │                 ├─ repl_add_slave(c)           ← 加入从库链表
  │                 ├─ repl_capture_set_target_fd() ← 设置 kprobe 捕获 fd
  │                 └─ queue_snapshot(c)           ← 发送全量快照
  │                       │
  │                       └─ repl_send_chunked(c, buf, len)
  │                             │
  │                             └─ repl_fullsync_send(c, buf, chunk)
  │                                   │
  │                                   ├─ ops->send() → repl_transport_rdma_send()
  │                                   │     │
  │                                   │     └─ repl_rdma_try_send(buf, len)
  │                                   │           │
  │                                   │           ├─ repl_rdma_acquire_send_slot()
  │                                   │           ├─ memcpy → slot buffer
  │                                   │           └─ ibv_post_send(QP, WR, &bad_wr)
  │                                   │
  │                                   └─ 失败 → repl_transport_tcp_send() (TCP fallback)
  │
  └─ start_rdma_master_listener()
        │
        └─ rdma_master_listener_thread()  ← 独立线程
              │
              ├─ rdma_create_event_channel()
              ├─ rdma_create_id() → rdma_bind_addr() → rdma_listen()
              ├─ rdma_get_cm_event() → RDMA_CM_EVENT_CONNECT_REQUEST
              ├─ rdma_accept()
              ├─ repl_rdma_create_qp()
              ├─ repl_rdma_prepare_buffers()
              ├─ repl_rdma_post_initial_recv()
              └─ repl_rdma_start_cq_poll_thread()
```

### 1.2 Slave 端启动流程

```
slave_thread() [独立线程]
  │
  ├─ repl_transport_tcp_connect_slave(host, port)  ← TCP 连接到 Master
  │
  ├─ repl_rdma_bg_connect_thread()  ← 后台线程，RDMA 连接
  │     └─ repl_transport_rdma_connect_slave()
  │           ├─ repl_rdma_reset_ctx()
  │           ├─ rdma_create_event_channel()
  │           ├─ rdma_create_id() → rdma_resolve_addr() → rdma_resolve_route()
  │           ├─ ibv_create_comp_channel() → ibv_alloc_pd() → ibv_create_cq()
  │           ├─ repl_rdma_create_qp()
  │           ├─ repl_rdma_prepare_buffers()   ← 分配 send/recv buffer 并注册 MR
  │           ├─ repl_rdma_post_initial_recv() ← 挂载初始 recv WR
  │           ├─ rdma_connect() → wait ESTABLISHED
  │           └─ repl_rdma_start_cq_poll_thread()
  │
  ├─ 等待 RDMA 就绪（最多 5s），发送 REPLSYNC over TCP
  │
  └─ 主循环: recv TCP + 轮询 RDMA recv → parse_resp_stream()
```

### 1.3 全量同步数据传输

```
queue_snapshot(c):
  1. kvs_snapshot_to_fp(tmp_path) → 生成二进制快照文件
  2. 读取文件大小 → 构造 +FULLRESYNC 头部
  3. repl_send_chunked(c, hdr, 70B)     ← FULLRESYNC 头部
  4. while (fread → repl_send_chunked)   ← 快照数据 (2.2MB / 256KB per chunk)
  5. repl_send_chunked(c, REPLDONE, 11B) ← +REPLDONE\n 标记完成
```

每个 chunk 通过 `repl_send_chunked_ctx` 发送，RDMA 模式下 chunk_size = 256KB。

---

## 2. Pipeline 发送模式

### 2.1 基本概念

Pipeline 模式是为了利用 RDMA 的异步能力，在等待前一个发送完成前发出下一个 WR，最大化 QP 吞吐量。

```
常量:
  KVS_RDMA_PIPELINE_DEPTH = 4    ← 最大在途 WR 数量
  KVS_RDMA_BATCH_MAX = 8         ← 批量 send WR 上限（当前未使用 batched post）
  KVS_RDMA_CQ_BATCH = 8          ← CQ 批量 poll 大小
```

### 2.2 Send Slot 管理

```c
typedef struct repl_rdma_send_slot_s {
    struct ibv_mr *mr;          // 注册的 MR
    unsigned char *buf;         // 发送缓冲区
    size_t cap;                 // 容量 (256KB)
    int in_flight;              // 1 = 已 ibv_post_send 但未完成
    uint64_t wr_id;             // 对应的 wr_id
} repl_rdma_send_slot_t;
```

- **4 个 Slot**，每个有独立的 buffer 和 MR
- **`in_flight = 1`** 表示该 slot 已提交给 QP，等待 CQ 完成通知
- CQ 轮询线程处理 WC 后将 `in_flight` 置 0

### 2.3 Slot 获取流程

```c
static int repl_rdma_acquire_send_slot(int timeout_ms) {
    // 线性扫描取第一个空闲 slot
    for (int i = 0; i < send_pipeline_depth; ++i) {
        slot = (head + i) % depth;
        if (!send_slots[slot].in_flight) {
            head = (slot + 1) % depth;
            return slot;
        }
    }
    // 所有 slot 都在飞行中：
    if (cq_poll_thread_running) {
        // CQ 线程在后台，等它释放 slot
        if (timeout_ms <= 0 || 超时) break;
        usleep(500); continue;
    } else {
        // 没有 CQ 线程：自己 poll CQ 回收 completion
        ibv_poll_cq(cq, 1, &wc);
        // ...处理 pipeline send completion 释放 slot
    }
    return -1;  // 超时或无可用 slot
}
```

### 2.4 发送失败或获取 Slot 失败时

当 `repl_rdma_acquire_send_slot` 返回 -1（超时 5s 或所有 slot 繁忙）：

```c
// 在 repl_rdma_try_send 中：
slot = repl_rdma_acquire_send_slot(5000);
if (slot < 0) {
    repl_rdma_log("try_send", "no available send slot");
    pthread_mutex_unlock(&g_repl_rdma_send_lock);
    return -1;  // ← 向上返回 -1
}
```

返回 -1 后，调用链：

```
repl_rdma_try_send() → -1
  → repl_transport_rdma_send() → -1
    → repl_fullsync_send() → 检测 rc != 0
      → transport_log("RDMA failed, fallback to TCP")
      → repl_transport_trigger_fallback("fullsync_send_fail", 3600000)
      → repl_transport_tcp_send(c, buf, len)  ← TCP fallback
```

> **关键**：非阻塞发送 + Pipeline 模式中，`ibv_post_send` 返回 0 ≠ 数据已传输。WR 可能因 QP 进入 error 状态被 flush（status=5），此时发布者无法感知，后续发送才会失败。

---

## 3. CQ 轮询线程详解

### 3.1 函数签名

```c
static void *repl_rdma_cq_poll_thread(void *arg);
```

### 3.2 完整流程

```
repl_rdma_cq_poll_thread:
  while (cq_poll_thread_running && connected):
    │
    ├─ ibv_get_cq_event(comp_chan, &ev_cq, &ev_ctx)
    │    阻塞等待 completion channel 事件
    │    返回后 ibv_ack_cq_events(cq, 1)
    │
    ├─ 批量 poll:
    │     ibv_poll_cq(cq, KVS_RDMA_CQ_BATCH=8, wc_batch)
    │     for each wc in wc_batch:
    │       repl_rdma_cq_process_wc(&wc)
    │         ├─ wc.status != IBV_WC_SUCCESS → connected=0, return
    │         ├─ wc.opcode == IBV_WC_SEND:
    │         │     wr_id & PIPELINE_WR_ID_FLAG ? 释放 send slot
    │         └─ wc.opcode == IBV_WC_RECV:
    │               pending_recv_push(slot, byte_len)
    │
    ├─ re-arm:
    │     ibv_req_notify_cq(cq, 0)        ← 重新使能事件通知
    │     ibv_poll_cq(cq, ...)            ← drain: 确保没有漏掉 completion
    │     有数据 → 回 ibv_poll_cq 循环
    │     无数据 → 回 ibv_get_cq_event 阻塞等待
    │
    └─ 检查 connected (如果为 0 则退出)
```

### 3.3 关键设计模式

**标准 RDMA 编程模式**（re-arm → drain → wait）：

```c
ibv_req_notify_cq(cq, 0);                    // re-arm
int n = ibv_poll_cq(cq, KVS_RDMA_CQ_BATCH, wc_batch);  // drain
if (n > 0) { /* 处理 WC，继续 */ continue; }
/* 无 completion → 回到 ibv_get_cq_event 阻塞等待 */
```

如果先 poll（空）再 re-arm，中间到达的 completion 不会触发新事件 → 永久阻塞。

### 3.4 WC 处理函数

```c
static void repl_rdma_cq_process_wc(struct ibv_wc *wc, int *adapt_counter) {
    if (wc->status != IBV_WC_SUCCESS) {
        // 错误处理：status=5=IBV_WC_WR_FLUSH_ERR
        // QP 进入 error state → connected=0
        g_repl_rdma_ctx.connected = 0;
        return;
    }
    if (wc->opcode == IBV_WC_SEND) {
        if (wr_id & PIPELINE_WR_ID_FLAG) {
            int slot = wr_id & ~PIPELINE_WR_ID_FLAG;
            repl_rdma_release_send_slot(slot);  // in_flight=0
            // 每 16 次 completion 调整一次 pipeline depth
        }
    } else if (wc->opcode == IBV_WC_RECV) {
        int slot = wr_id - 1;
        recv_slots[slot].posted = 0;
        repl_rdma_pending_recv_push(slot, byte_len);
    }
}
```

### 3.5 线程退出条件

```c
while (g_repl_rdma_ctx.cq_poll_thread_running && g_repl_rdma_ctx.connected)
```

任一条件为 false 时退出。退出前会 drain 剩余 CQ 事件。

---

## 4. WR 与 WC 的关系

### 4.1 基本概念

| 概念 | 全称 | 说明 |
|------|------|------|
| **WR** | Work Request | 用户提交给 QP 的工作请求（发送/接收） |
| **WQE** | Work Queue Element | QP 内部的工作队列元素（硬件视角） |
| **CQE** | Completion Queue Entry | CQ 中的完成条目 |
| **WC** | Work Completion | 用户读取的完成信息（由 CQE 转换而来） |

### 4.2 关系

```
用户提交: ibv_post_send(QP, WR)  ←── 创建 WQE 到 QP 的发送队列
                │
        RDMA 硬件处理 WQE
                │
                ├─ 成功 → CQ 中写入 CQE
                └─ 失败 → CQ 中写入 CQE (status ≠ IBV_WC_SUCCESS)
                │
用户读取: ibv_poll_cq(CQ, max, wc_array)  ←── CQE → WC
```

**核心规律**：

- **一个 WR → 一个 WC**（无论成功或失败）
- **一个 CQE = 一个 WC**（一一对应）
- `ibv_post_send` 返回 0 = WR 已加入 QP 队列，≠ 传输完成
- WC 的 `status` 指示传输结果

### 4.3 示例

```
Master 发送 2.2MB 快照，分 9 个 256KB chunk + 1 个头部 + REPLDONE:
  ibv_post_send(WR#1)  →  11 个 WR 提交给 QP
  ibv_post_send(WR#2)      CQ 线程收到 11 个 WC
  ...                      send_slot 逐个释放
  ibv_post_send(WR#11)     pipeline_depth 动态调整
```

### 4.4 flush 场景

当 QP 进入 error state（如 siw 空闲超时），QP 中所有未完成的 WQE 被 flush：

```c
// CQ 线程收到:
wc.status = IBV_WC_WR_FLUSH_ERR  // status=5
wc.opcode = IBV_WC_RECV
// → connected = 0, 线程退出
```

---

## 5. 全量同步失败处理与降级

### 5.1 降级触发条件

```
repl_fullsync_send(c, buf, len):
  rc = ops->send(c, buf, len)   // RDMA 发送
  if (rc == 0) return 0;        // 成功

  // 失败：
  transport_log("%s failed, fallback to TCP", ops->name);
  repl_transport_trigger_fallback("fullsync_send_fail", 3600000);
  rc = repl_transport_tcp_send(c, buf, len);  // TCP 重试
  return rc;
```

### 5.2 fallback 冷却机制

```c
void repl_transport_trigger_fallback(const char *reason, int cooldown_ms) {
    g_repl_transport_fallback_count++;
    g_repl_transport_fallback_until_ms = kvs_now_ms() + cooldown_ms;
    // 全量失败冷却 1 小时 (3600000ms)
    // 这样后续不会反复尝试 RDMA
}
```

在 `repl_should_use_rdma_now()` 中检查：

```c
if (g_repl_transport_fallback_until_ms > kvs_now_ms()) return 0;  // 冷却中
```

### 5.3 降级后的数据路径

```
RDMA 降级后:
  repl_fullsync_send()
    └─ repl_transport_tcp_send(c, buf, len)
         └─ queue_bytes(c, buf, len)
              └─ reactor on_write → send(c->fd) → TCP → Slave
```

### 5.4 降级场景汇总

| 场景 | 检测点 | 处理 |
|------|--------|------|
| QP 断开 (connected=0) | `repl_rdma_try_send` 开头 | 返回 -1 → fallback |
| Send slot 超时 (5s) | `repl_rdma_acquire_send_slot` | 返回 -1 → fallback |
| ibv_post_send 失败 | `repl_rdma_try_send` | connected=0 → fallback |
| CQ 错误 (status≠0) | CQ poll 线程 | connected=0 → 下次发送 fallback |
| fallback 冷却中 | `repl_should_use_rdma_now` | 跳过 RDMA，直接 TCP |

---

## 6. Slave 线程的三种路径与重复代码

### 6.1 三条路径

slave_thread 中有三个分支：

| 路径 | 条件 | 传输方式 |
|------|------|----------|
| **Hybrid kprobe-rdma** | `rdma_fullsync + kprobe_realtime` | 全量 RDMA，增量 kprobe |
| **Hybrid ebpf** | `rdma_fullsync + ebpf_realtime` | 全量 RDMA，增量 eBPF sockmap |
| **Single** | 其他 | 单一传输 (TCP/eBPF/RDMA) |

### 6.2 重复代码分析

Hybrid kprobe-rdma 和 hybrid ebpf 两条路径的代码结构几乎相同：

```
两个路径的共同结构:
  ├─ tcp_fd = tcp_connect_slave()
  ├─ 后台启动 bg_connect_thread (RDMA 建链)
  ├─ 注册 fd (ebpf 或 kprobe)
  ├─ for(;;) 主循环:
  │    ├─ 检查 slave_should_reconnect
  │    ├─ 发送 REPLSYNC (延迟到 RDMA 就绪或 5s 超时)
  │    ├─ TCP recv → parse_resp_stream
  │    ├─ RDMA recv check (全量数据)
  │    └─ parse after new data
  ├─ 断开连接
  └─ sleep → continue
```

差异点：

| 差异 | kprobe-rdma 路径 | ebpf 路径 |
|------|------------------|-----------|
| transport kind | `KVS_REPL_TRANSPORT_KPROBE_RDMA` | `KVS_REPL_TRANSPORT_EBPF` |
| fd 注册 | 无（kprobe 透明拦截） | `repl_ebpf_register_fd(tcp_fd, 0)` |
| RDMA recv 处理 | 检查 `rdma_blen > 0` | 有 batch drain + keepalive 过滤 |
| 断开 slave | `repl_transport_tcp_disconnect_slave` | `repl_transport_ebpf_disconnect_slave` |

### 6.3 重构建议

可以将共同逻辑提取为辅助函数，减少约 150 行重复代码。

---

## 7. eBPF sockmap 数据重定向与零拷贝

### 7.1 sockmap 是什么

`BPF_MAP_TYPE_SOCKMAP` 是一个存储 socket fd 的 BPF map。将 socket 注册到 sockmap 后，BPF 程序可以通过 `bpf_msg_redirect_map()` 将数据从一个 socket 直接转发到另一个 socket。

### 7.2 零拷贝原理

```
传统 TCP 转发:
  send(fd1, buf, len) → 内核拷贝 buf 到 socket 1 的发送缓冲区
                      → TCP 协议栈 → 网卡 → 远端
  
eBPF sockmap 重定向:
  send(fd1, buf, len) → sk_msg BPF 程序触发
                      → bpf_msg_redirect_map(sock_map, key, 0)
                      → 数据从 fd1 的发送缓冲区直接关联到 fd2 的接收缓冲区
                      → 不需要拷贝到用户态
                      → 不需要经过 TCP 协议栈（同一主机）
```

**零拷贝体现在**：

1. 数据不经过用户态（`send()` 触发 BPF 程序后，数据在内核态被重定向）
2. 不需要 `recv()` 读取再 `send()` 写入
3. `bpf_msg_redirect_map()` 操作的是 socket 的内存描述符，而非数据本身

### 7.3 BPF 程序逻辑

```c
SEC("sk_msg")
int kvstore_repl_sk_msg(struct sk_msg_md *msg) {
    // 1. 检查角色 (master/slave)
    // 2. 检查是否启用重定向
    // 3. bpf_msg_redirect_map(&sock_map, redirect_key, BPF_F_INGRESS?)
    // 4. 统计成功/失败
}
```

### 7.4 跨机限制

sockmap 的 `bpf_msg_redirect_map` 只能重定向到**同一主机**上的 socket。因此：

- 单机测试：slave 和 master 在同一台机器时，sockmap 直接转发
- 双机部署：slave 和 master 在不同机器时，数据仍然走 TCP 协议栈（只是在内核态完成了路由决策）

---

## 8. eBPF 程序加载与管理

### 8.1 `repl_ebpf_load_object()` 函数

```c
static int repl_ebpf_load_object(void) {
    // 1. 打开 BPF 目标文件 (repl_sockmap.bpf.o)
    obj = bpf_object__open_file(obj_path, NULL);

    // 2. 加载 BPF 到内核
    bpf_object__load(obj);

    // 3. 查找 maps:
    sock_map_fd = bpf_object__find_map_fd_by_name(obj, "sock_map");
    stats_map_fd = bpf_object__find_map_fd_by_name(obj, "stats_map");
    control_map_fd = bpf_object__find_map_fd_by_name(obj, "control_map");

    // 4. 查找并附加 sk_msg 程序:
    sk_msg_prog = bpf_object__find_program_by_name(obj, "kvstore_repl_sk_msg");
    sk_msg_fd = bpf_program__fd(sk_msg_prog);

    // 5. 附加到 sockmap:
    // 方式 A: bpf_prog_attach(sk_msg_fd, sock_map_fd, BPF_SK_MSG_VERDICT)
    // 方式 B: bpf_msg_redirect_hash 需要先 attach
}
```

### 8.2 三种加载模式

```c
// 模式 1: 独立 eBPF 守护进程管理
if (g_cfg.ebpf_pin_path[0]) {
    // 连接到已存在的 pinned maps
    repl_ebpf_open_pinned_maps();
}

// 模式 2: kvstore 直接管理
else {
    // 直接加载 BPF 对象文件
    repl_ebpf_load_object();
}

// 模式 3: 不启用
if (!g_cfg.ebpf_enabled) {
    // 跳过 eBPF 初始化
}
```

---

## 9. Pinned Maps

### 9.1 概念

BPF maps 默认与加载它的进程生命周期绑定。进程退出后 map 被销毁。

**Pinned maps** 通过 BPF 文件系统 (`/sys/fs/bpf/`) 将 map 持久化：

```bash
/sys/fs/bpf/kvstore_repl_sockmap/
├── sock_map
├── stats_map
└── control_map
```

### 9.2 用途

独立 eBPF 守护进程加载 BPF 程序并 pin maps，kvstore 通过路径访问已存在的 maps：

```c
int repl_ebpf_open_pinned_maps(void) {
    sock_map_fd = bpf_obj_get("/sys/fs/bpf/kvstore_repl_sockmap/sock_map");
    stats_map_fd = bpf_obj_get("/sys/fs/bpf/kvstore_repl_sockmap/stats_map");
    control_map_fd = bpf_obj_get("/sys/fs/bpf/kvstore_repl_sockmap/control_map");
}
```

### 9.3 配置

```bash
--ebpf-pin-path /sys/fs/bpf/kvstore_repl_sockmap
```

`g_cfg.ebpf_pin_path` 控制：
- 空字符串 → kvstore 自己加载 BPF
- 非空 → 连接已 pin 的 maps（由独立守护进程管理）

---

## 10. Fd 注册函数详解

### 10.1 `repl_ebpf_register_fd(fd, is_master_side)`

```c
int repl_ebpf_register_fd(int fd, int is_master_side) {
    // 1. 将 fd 插入 sock_map
    int sock_key = is_master_side ? KVS_EBPF_SOCK_KEY_MASTER_SIDE : g_next_slave_key++;
    bpf_map_update_elem(sock_map_fd, &sock_key, &fd, BPF_ANY);

    // 2. 更新 role_map
    int role = is_master_side ? KVS_EBPF_ROLE_MASTER_SIDE : KVS_EBPF_ROLE_SLAVE_SIDE;
    bpf_map_update_elem(role_map_fd, &fd, &role, BPF_ANY);

    // 3. sk_msg BPF 程序现在可以将数据重定向到此 fd
}
```

### 10.2 调用时机

```c
// Master 端 (on REPLSYNC):
repl_ebpf_register_fd(c->fd, 1);   // is_master_side=1

// Slave 端 (on connect):
repl_ebpf_register_fd(tcp_fd, 0);  // is_master_side=0
```

### 10.3 在 BPF 程序中的使用

```c
SEC("sk_msg")
int kvstore_repl_sk_msg(struct sk_msg_md *msg) {
    // 检查是否是 master 侧的 socket
    int *role = bpf_map_lookup_elem(&role_map, &msg->sk);
    if (!role || *role != KVS_EBPF_ROLE_MASTER_SIDE) return SK_PASS;

    // 重定向到 slave 侧的 socket
    bpf_msg_redirect_map(&sock_map, KVS_EBPF_SOCK_KEY_SLAVE_SIDE, BPF_F_INGRESS);
}
```

---

## 11. 实时发送派发机制

### 11.1 `repl_realtime_send()` 函数

```c
int repl_realtime_send(conn_t *c, const unsigned char *buf, size_t len) {
    // 1. 根据配置选择传输 ops
    const repl_transport_ops_t *ops = repl_transport_ops_for_context(KVS_REPL_SEND_REALTIME);
    
    // 2. 调用对应的 send 函数
    int rc = ops->send(c, buf, len);
    if (rc == 0) return 0;
    
    // 3. 失败后 fallback 到 TCP
    transport_log("%s failed, fallback to TCP", ops->name);
    rc = repl_transport_tcp_send(c, buf, len);
    return rc;
}
```

### 11.2 ops 选择逻辑

```c
static const repl_transport_ops_t *repl_transport_ops_for_context(int send_ctx) {
    if (send_ctx == KVS_REPL_SEND_REALTIME) {
        const char *t = repl_realtime_transport_name();
        
        if (!strcasecmp(t, "kprobe-rdma") && kprobe_rdma_ops.supported && kprobe_enabled)
            return &g_repl_transport_kprobe_rdma_ops;
            // send = repl_transport_kprobe_rdma_send (返回 -1，让 kprobe 拦截)
        
        if ((!strcasecmp(t, "ebpf") || !strcasecmp(t, "sockmap")) && ebpf_ops.supported)
            return &g_repl_transport_ebpf_ops;
            // send = repl_transport_ebpf_send = queue_bytes → reactor → send()
            // 内核触发 sk_msg BPF → bpf_msg_redirect_map
        
        return &g_repl_transport_tcp_ops;
        // send = repl_transport_tcp_send = queue_bytes → reactor → send()
    }
}
```

### 11.3 `repl_transport_ebpf_send` 详解

```c
static int repl_transport_ebpf_send(conn_t *c, const unsigned char *buf, size_t len) {
    return queue_bytes(c, buf, len);
}
```

因为 fd 已通过 `repl_ebpf_register_fd()` 注册到 sockmap，`send()` 系统调用会触发 BPF 程序进行重定向。用户态不需要额外操作。

---

## 12. RDMA WRITE 增量同步

### 12.1 架构

```
Master                              Slave
  |                                   |
  | 1. TCP: RDMA_INCR (rkey+addr)  →  | 注册 4MB MR
  |                                   | 启动轮询线程 (1ms)
  | 2. RDMA WRITE (数据)  →→→→→→→→→  | 写入 buffer[seq % size]
  | 3. RDMA WRITE (doorbell)→→→→→→→→  | 更新 head 指针
  |                                   | 4. 轮询 head != tail?
  |                                   | 5. parse_resp_stream → apply
  |                                   | 6. 更新 tail (本地)
```

### 12.2 缓冲区结构

```c
typedef struct repl_rdma_incr_buf_s {
    uint64_t magic;              // 魔数 KVSINCR
    volatile uint64_t head;      // Master 通过 RDMA WRITE doorbell 更新
    volatile uint64_t tail;      // Slave 本地更新
    uint8_t data[];              // 循环数据区 (4MB - 16 字节头部)
} repl_rdma_incr_buf_t;
```

### 12.3 RDMA WRITE 实现

```c
static int repl_rdma_incr_write(const unsigned char *data, size_t len) {
    // WR 1: RDMA WRITE 数据到 slave 缓冲区
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.wr.rdma.remote_addr = slave_buf_addr + offset;  // 计算写入位置
    wr.wr.rdma.rkey = slave_rkey;

    // WR 2: doorbell — 更新 head 指针 (链在 WR 1 之后)
    db_wr.opcode = IBV_WR_RDMA_WRITE;
    db_wr.wr.rdma.remote_addr = slave_buf_addr + offsetof(head);
    db_wr.wr.rdma.rkey = slave_rkey;
    wr.next = &db_wr;  // WR 链

    ibv_post_send(qp, &wr, &bad_wr);
}
```

---

## 13. Kprobe 捕获

### 13.1 BPF 程序

```c
SEC("kprobe/__sys_sendto")
int kp_capture_sendto(struct pt_regs *ctx) {
    // 1. 检查 enabled + target_fd
    if (!enabled || !target_fd) return 0;
    
    // 2. 读取参数 (x86_64 pt_regs 偏移)
    fd_val = ctx + 112;   // rdi = fd
    buf_val = ctx + 104;  // rsi = buf
    len_val = ctx + 96;   // rdx = len
    
    // 3. 匹配 target_fd
    if (fd_val != target_fd) return 0;
    
    // 4. 拷贝数据到 ring buffer
    e = bpf_ringbuf_reserve(&ringbuf, sizeof(*e), 0);
    bpf_probe_read_user(e->data, copy_len, buf);
    bpf_ringbuf_submit(e, 0);
}
```

### 13.2 用户态消费

```c
// 消费者线程
static void *repl_capture_consumer_thread(void *arg) {
    while (running) {
        ring_buffer__poll(rb, 100);  // 100ms 超时
    }
}

// Ring buffer 回调
static int repl_capture_ringbuf_cb(void *ctx, void *data, size_t data_sz) {
    __u64 len = *(const __u64 *)data;
    const unsigned char *payload = data + 8;  // 跳过 len 字段
    
    g_repl_capture_count++;
    g_repl_capture_bytes += len;
    
    if (repl_rdma_is_connected()) {
        repl_rdma_send_from_ebpf(payload, len);  // 尝试 RDMA 发送
    }
}
```

### 13.3 Attachment 方式

通过 `kprobe_events + PERF_TYPE_TRACEPOINT + ioctl`：

```c
// 1. 创建 kprobe event
echo "p:kprobes/kvstore_cap __sys_sendto" > /sys/kernel/debug/tracing/kprobe_events

// 2. 读取 event id
cat /sys/kernel/debug/tracing/events/kprobes/kvstore_cap/id

// 3. perf_event_open(PERF_TYPE_TRACEPOINT, id)
// 4. ioctl(PERF_EVENT_IOC_SET_BPF, prog_fd)
// 5. ioctl(PERF_EVENT_IOC_ENABLE)
```

---

## 14. Q&A 汇总

### Q1: send_slot 和 recv_slot 是什么？Pipeline 模式下 slot buffer 就是 MR 吗？

**A**: 

- **send_slot**: 预分配的 4 个发送缓冲区槽位，每个包含 `buf`（数据缓冲区）+ `mr`（该 buf 注册的 MR）+ `in_flight` 标志
- **recv_slot**: 32-64 个接收缓冲区槽位，每个包含 `buf` + `mr` + `posted` 标志
- **slot buffer ≠ MR**: slot buffer 是内存区域，MR 是 ibv_reg_mr 返回的注册信息（包含 lkey/rkey/地址）。即 MR 描述 buffer，buffer 是数据本身
- **pipeline 模式**: 4 个 slot 轮流使用，一个在发送时另一个可以准备数据，提高吞吐

### Q2: RDMA 什么情况下会降级为 TCP？

**A**: 见第 5 节。触发条件包括 QP 断开、send slot 超时、ibv_post_send 失败、CQ 错误。降级后触发 fallback 冷却（全量 1h），冷却期间不尝试 RDMA。

### Q3: eBPF sockmap 为什么是零拷贝？

**A**: 见第 7 节。数据在 send() 触发 sk_msg BPF 后，`bpf_msg_redirect_map` 在内核态将数据描述符从源 socket 关联到目标 socket，不需拷贝到用户态，不需经过 TCP 协议栈处理（同主机时）。

### Q4: CQ 中很多 WC 吗？一个 CQE 就有一个 WC？

**A**: 一个 WR → 一个 WQE → 完成时一个 CQE → 读取时一个 WC。一一对应关系。CQ 中可以同时存在多个 CQE（Pipeline 模式下 4 个 WR 同时进行，就有 4 个 CQE 等待处理）。

### Q5: Non-blocking send 失败或获取 send slot 失败怎么办？

**A**: `repl_rdma_try_send` 返回 -1，调用链逐层向上返回，最终 `repl_fullsync_send` 或 `repl_realtime_send` 触发 TCP fallback（见第 2.4 节、第 5 节）。

### Q6: slave_thread 中的 hybrid 路径有什么重复代码？

**A**: kprobe-rdma 和 ebpf 两条 hybrid 路径共享约 80% 的代码结构（TCP 连接、RDMA bg connect、REPLSYNC 延迟发送、主循环结构），仅在 transport kind、fd 注册、断开处理上有差异（见第 6 节）。

### Q7: 为什么 siw 环境下 RDMA 连接总是断开？

**A**: soft-iWARP (siw) 在 QP 空闲约 100ms 后会自动断开底层 TCP 连接。这是因为 Master 挂了 recv WR（等待 Slave 发数据）但 Slave 从不通过 RDMA 发数据，导致 siw 认为连接空闲而断开。表现在 CQ poll 线程收到 `status=5 (IBV_WC_WR_FLUSH_ERR)`。

### Q8: keepalive 为什么没能防止断连？

**A**: 初始 keepalive 间隔 3s 远大于 siw 的 100ms 超时。即使缩短到 200ms，也因调度延迟和条件竞争无法可靠防止断连。

### Q9: RDMA WRITE 相比 RDMA SEND 有什么优势？

**A**: RDMA WRITE 是**单边操作**——Master 直接写入 Slave 的内存，Slave CPU 零参与接收。不需要 recv WR、不需要 CQ 事件、没有 pending_recv 队列溢出问题。Slave 只需轮询共享内存中的 head/tail 指针即可感知新数据。

### Q10: 测试全部通过但 RDMA 未加速，这是成功还是失败？

**A**: **功能成功**——全量同步自动降级 TCP、增量同步走 eBPF sockmap、数据一致性 24/24 PASS。RDMA 加速因 siw 底层限制未生效。换用真实 RDMA 网卡或 SoftRoCE (rxe) 可获得完整加速效果。
