# RDMA 按需启停 + eBPF 全量同步数据缓存 — 实现计划

> **状态**: Phase 1-3 已实现 ✅ | Phase 4 集成测试待完善 ⚠️
> 本文档描述原始设计计划，实际实现细节可能与计划有差异（标注在对应章节）。

> 本文档详细描述了将 RDMA 生命周期从"进程启动时永久开启"改造为"全量同步时按需启停"，并引入三个 eBPF 探测点实现全量同步期间客户端写入数据缓存转发的完整实现计划。

---

## 目录

1. [背景与目标](#1-背景与目标)
2. [整体架构](#2-整体架构)
3. [Phase 1: RDMA 按需启停](#3-phase-1-rdma-按需启停)
4. [Phase 2: eBPF 三个探测点实现](#4-phase-2-ebpf-三个探测点实现)
5. [Phase 3: eBPF+tcp 增量同步路径](#5-phase-3-ebfptcp-增量同步路径)
6. [Phase 4: 集成测试](#6-phase-4-集成测试)
7. [文件修改清单](#7-文件修改清单)
8. [风险与注意事项](#8-风险与注意事项)

---

## 1. 背景与目标

### 1.1 当前状态

- RDMA listener 在 `main()` 启动时通过 `start_rdma_master_listener()` 永久开启
- `rdma_master_listener_thread` 线程持续运行，等待 slave 的 RDMA 连接
- 全量同步数据通过 RDMA SEND/RECV 传输
- 增量同步通过 kprobe+RDMA WRITE 或 eBPF sockmap 传输

### 1.2 目标状态

```
时序图:

Master:  [预写5w] → [等待Slave连接] → [收到REPLSYNC]
         → [开启RDMA] → [RDMA全量同步] → [发REPLDONE] → [关闭RDMA]
         ↑                    ↑                            ↑
客户端:  [写数据] ─────────── [再写5w] ────────────────── [继续写...]
         ↓                    ↓                            ↓
  ┌─ eBPF tcp_recvmsg 捕获 ──────────────────────────────────┐
  │  STATE_FULL:  L1(内存4MB) → L2(磁盘) 缓存               │
  │  STATE_INCR:  直接 send() → Slave TCP (eBPF+tcp 新路径)  │
  │  tcp_sendmsg: 探测 REPLDONE → 触发 flush + 切换到 INCR  │
  └──────────────────────────────────────────────────────────┘
```

### 1.3 三个 eBPF 探测点

| # | 探测点 | Hook | 实现位置 | 目的 |
|---|--------|------|---------|------|
| **①** | 主机发送 REPLDONE 命令 | `kprobe/tcp_sendmsg`（`repl_client_capture.bpf.c`） | ✅ 已实现 | 探测全量同步完成 REPLDONE，自动清除 `client_ctl[3]` (FULLSYNC_IN_PROGRESS=0) |
| **②** | 客户发送给主机的数据（增量同步用） | `kprobe/tcp_recvmsg` + `kretprobe/tcp_recvmsg`（`repl_client_capture.bpf.c`） | ✅ 已实现 | 捕获客户端写入命令，用于增量同步转发（新路径） |
| **③** | 缓存全量同步时客户发送给主机的数据 | 与②共用 `tcp_recvmsg` hook | ✅ 已实现 | 全量同步期间缓存客户端数据(L1+L2)，完成后 flush |

### 1.4 增量同步传输层对比

| 方案 | 配置 | 转发机制 | 数据拷贝 | 硬件要求 |
|------|------|---------|---------|---------|
| **eBPF+tcp** (新路径) | `kprobe-rdma` 自动启用 | kprobe/tcp_recvmsg → ringbuf → **用户态 send() → Slave TCP** | **1 份** (仅 TCP) | 无 |
| **kprobe+RDMA** (旧路径) | `kprobe-rdma` 自动启用 | send(slave_fd) 触发 kprobe → ringbuf → **RDMA WRITE → Slave MR** | 2 份 (TCP + RDMA) | RDMA 硬件 |
| **eBPF sockmap** | `ebpf` | sk_msg BPF → **bpf_msg_redirect_map()** | 1 份 | 无 |
| **TCP fallback** | `tcp` | 应用层 send() 转发 | 1 份 | 无 |

---

## 2. 整体架构

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            Master 进程                                    │
│                                                                          │
│  ┌──────────┐   SET/GET    ┌──────────────────────┐                     │
│  │  Client  │─────────────▶│  handle_parsed_cmd   │                     │
│  │  TCP连接 │              │  写命令→repl_broadcast│                     │
│  └──────────┘              └──────────┬───────────┘                     │
│       │                               │                                 │
│       │ ② tcp_recvmsg hook           │                                 │
│       ▼                               │                                 │
│  ┌──────────────────┐                │                                 │
│  │ client_cache     │                │                                 │
│  │ BPF ringbuf (1MB)│                │                                 │
│  └───────┬──────────┘                │                                 │
│          │                           │                                 │
│          │  ③ STATE: FULLSYNC/INCR   │                                 │
│          │  =FULL: L1缓存+L2磁盘    │                                 │
│          │  =INCR: 直接转发到slave  │                                 │
│          └──────────┬────────────────┘                                 │
│                     │                                                   │
│        ┌────────────┴────────────┐                                      │
│        ▼                         ▼                                      │
│  ┌──────────┐            ┌──────────────┐                              │
│  │  L1 内存 │ 超出4MB    │  L2 磁盘文件 │  ← 全量同步缓存              │
│  │  队列    │──────────▶│  /tmp/...    │                              │
│  └────┬─────┘            └──────┬───────┘                              │
│       │                          │                                      │
│       └─────────┬────────────────┘                                      │
│                 ▼                                                        │
│         ┌──────────────┐          ┌──────────────┐                     │
│         │  repl_broadcast        │  forward_to  │  ← INCR 新路径       │
│         │ ┌────────────┐│         │  _slave()   │  send(slave_fd)      │
│         │ │backlog_feed││ ← 始终  └──────┬───────┘                     │
│         │ └────────────┘│                │                              │
│         │ │realtime_send││ ← eBPF+tcp   │                              │
│         │ │ (跳过)      │    激活时跳过  │                              │
│         └──────┬───────┘                │                              │
│                │                        │                              │
│                │ ① tcp_sendmsg hook     │                              │
│                ▼                        ▼                              │
│  ┌────────────────────────────────────────┐                            │
│  │          Slave TCP 连接                │                            │
│  │  → FULLRESYNC header (RDMA 全量)       │                            │
│  │  → snapshot data (RDMA)                │                            │
│  │  → REPLDONE (TCP，控制命令始终走 TCP)  │  ← BPF kprobe/tcp_sendmsg 探测 │
│  │  → flush L1+L2 缓存数据 (TCP)         │                            │
│  │  → backlog gap replay (TCP, 兜底)      │                            │
│  │  → 增量数据 (TCP, eBPF+tcp 新路径)    │                            │
│  └────────────────────────────────────────┘                            │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Phase 1: RDMA 按需启停

### 3.1 修改 `main()` 启动流程

**文件**: `src/main/kvstore.c`

**改动**: 移除 `main()` 中的 `start_rdma_master_listener()` 调用（约 line 2136-2145）。

```c
// 移除以下代码块:
#if KVS_ENABLE_RDMA
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") ||
        !strcasecmp(g_cfg.repl_transport_backend, "rdma")) {
        if (start_rdma_master_listener() != 0) {
            fprintf(stderr, "failed to start rdma master listener thread\n");
            return 1;
        }
    }
#endif
```

同时新增全局标志:

```c
/* 全量同步进行中标志 */
volatile int g_repl_fullsync_in_progress = 0;
```

### 3.2 新增 `repl_rdma_start_fullsync()` — 按需启动 RDMA

**文件**: `src/replication/kvs_repl.c`

**伪代码**:

```c
/**
 * 全量同步开始时按需启动 RDMA。
 * Master 侧: 创建 listener → 等待 slave RDMA 连接 → 建立 QP
 * Slave 侧:  连接 master RDMA listener → 等待 ESTABLISHED
 *
 * 返回: 0 成功, -1 失败（失败后调用方回退到 TCP）
 */
int repl_rdma_start_fullsync(conn_t *c) {
#if KVS_ENABLE_RDMA
    // 1. 如果已是 connected 状态，直接返回成功
    if (g_repl_rdma_ctx.connected) return 0;

    // 2. 重置 RDMA 上下文（清理残留状态）
    repl_rdma_reset_ctx();

    // 3. 创建 event channel（最多重试 5 次）
    for (int i = 0; i < 5; i++) {
        g_repl_rdma_ctx.ec = rdma_create_event_channel();
        if (g_repl_rdma_ctx.ec) break;
        usleep(200000);
    }
    if (!g_repl_rdma_ctx.ec) return -1;

    if (g_cfg.role == ROLE_MASTER) {
        // 4. Master 侧: bind + listen + 等待 slave 连接
        //    (复用现有 rdma_master_listener_thread 中的建连逻辑)
        
        // 4a. 创建 listen id
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)(g_cfg.rdma_port > 0 ? 
                            g_cfg.rdma_port : g_cfg.port + 1));
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        
        if (rdma_create_id(g_repl_rdma_ctx.ec, &g_repl_rdma_ctx.listen_id, 
                           NULL, RDMA_PS_TCP) != 0) return -1;
        if (rdma_bind_addr(g_repl_rdma_ctx.listen_id, 
                          (struct sockaddr *)&addr) != 0) return -1;
        if (rdma_listen(g_repl_rdma_ctx.listen_id, 4) != 0) return -1;

        // 4b. 等待 slave 的 CONNECT_REQUEST（超时 10s）
        //     复用 repl_rdma_wait_event 机制
        //     获取 accepted_id，创建 PD/CQ/QP/buffers，rdma_accept
        //     → 等待 ESTABLISHED
        
        // ... 与 rdma_master_listener_thread 中的 accept 逻辑相同 ...
        
    } else {
        // 5. Slave 侧: resolve_addr → resolve_route → create QP → connect
        //    复用 repl_transport_rdma_connect_slave 中的逻辑
        
        // ... 现有 slave 连接逻辑 ...
    }

    // 6. 启动 CQ 轮询线程（pipeline 模式）
    g_repl_rdma_ctx.connected = 1;
    repl_rdma_start_cq_poll_thread();

    repl_rdma_log("fullsync", "RDMA started successfully");
    return 0;
#else
    return -1;
#endif
}
```

### 3.3 新增 `repl_rdma_stop_fullsync()` — 全量同步完成后关闭 RDMA

**文件**: `src/replication/kvs_repl.c`

```c
/**
 * 全量同步完成后关闭并释放所有 RDMA 资源。
 * 包括: CQ 轮询线程、QP、CQ、PD、buffers、event channel、listener
 */
void repl_rdma_stop_fullsync(void) {
#if KVS_ENABLE_RDMA
    repl_rdma_log("fullsync", "stopping RDMA");
    
    // 1. 标记断开连接
    g_repl_rdma_ctx.connected = 0;
    
    // 2. 完全重置 RDMA 上下文（释放所有资源）
    //    repl_rdma_reset_ctx() 已包含:
    //    - 停止 CQ 轮询线程
    //    - 注销 MR、释放 buffers
    //    - 销毁 CQ、PD、QP、CM ID、event channel
    repl_rdma_reset_ctx();
    
    // 3. 重置 listener 启动标志
    g_rdma_master_listener_started = 0;
    
    repl_rdma_log("fullsync", "RDMA stopped");
#endif
}
```

### 3.4 改造 `queue_snapshot()` — 整合 RDMA start/stop

**文件**: `src/main/kvstore.c`，`queue_snapshot()` 函数（约 line 496-598）

```c
static int queue_snapshot(conn_t *c) {
    // ... 现有 dump snapshot 逻辑 ...
    
    // NEW: 进入全量同步，设置标志
    g_repl_fullsync_in_progress = 1;
    repl_client_capture_set_fullsync(1);  // 通知 eBPF 开始缓存客户数据
    
    // NEW: 启动 RDMA（如果配置为 rdma 传输）
    int rdma_ok = 0;
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma")) {
        rdma_ok = (repl_rdma_start_fullsync(c) == 0);
    }
    
    // 以下数据发送如果 RDMA 就绪走 RDMA，否则回退 TCP
    // repl_send_chunked → repl_fullsync_send → repl_transport_ops_for_context
    // 该函数已根据 repl_fullsync_transport_name() 选择 RDMA/TCP
    
    // ... FULLRESYNC header + snapshot data 发送 (RDMA 或 TCP) ...
    
    // NEW: REPLDONE 发送前先停 RDMA — 控制命令始终走 TCP
    //      这样 BPF kprobe/tcp_sendmsg 可以探测到 REPLDONE，
    //      自动清除 client_ctl[3] (FULLSYNC_IN_PROGRESS=0)
    if (rdma_ok) {
        repl_rdma_stop_fullsync();
        rdma_ok = 0;  // 标记 RDMA 已关闭
    }
    
    // REPLDONE 通过 TCP (queue_bytes) 发送
    {
        size_t done = resp_build_cmd1(buf, buf_size, "REPLDONE");
        if (queue_bytes(c, buf, done) != 0) goto out;
    }
    
    // NEW: 全量同步完成
    g_repl_fullsync_in_progress = 0;
    repl_client_capture_set_fullsync(0);  // 读取 BPF map 验证探测结果
    
    // NEW: Flush eBPF 缓存的客户端数据
    // 通过 TCP 发送到 slave
    repl_client_capture_flush_to_slave(c);
    
    // ... 现有 backlog gap replay ...
}
```

### 3.5 改造 `repl_broadcast()` — 全量同步期间跳过实时发送

**文件**: `src/main/kvstore.c`

```c
void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    repl_note_send_context("broadcast", rawlen, repl_master_offset(), raw);
    repl_backlog_feed(raw, rawlen);  // 始终进 backlog
    repl_note_broadcast(rawlen);
    
    pthread_mutex_lock(&g_repl_lock);
    conn_t **pp = &g_replicas;
    while (*pp) {
        conn_t *c = *pp;
        // ...  draining / fullsync_pending 检查 ...
        
        // NEW: 全量同步期间跳过实时发送
        if (g_repl_fullsync_in_progress) {
            pp = &c->next_replica;
            continue;
        }
        
        // ... 正常实时发送 ...
        if (repl_realtime_send(c, raw, rawlen) != 0) {
            // ... 错误处理 ...
        }
    }
    pthread_mutex_unlock(&g_repl_lock);
}
```

### 3.6 从机侧改造

**文件**: `src/replication/kvs_repl.c` — `slave_thread()`

**改动**: 当前 hybrid 模式中 slave 在连接时后台启动 RDMA QP 连接，改为:

```c
// Hybrid 模式 (RDMA fullsync + TCP/kprobe incremental):
// 1. Slave 先通过 TCP 连接 master
// 2. 发送 REPLSYNC 触发全量同步
// 3. Slave 检测到 master 发起的 RDMA 连接请求 → 接受并建立 RDMA QP
//    (或: slave 收到 FULLRESYNC header 后主动连接 master RDMA listener)
// 4. 接收全量数据
// 5. 收到 REPLDONE → 从机断开 RDMA 连接
// 6. 后续增量数据走 TCP/kprobe-rdma
```

### 3.7 `start_rdma_master_listener()` 保留但改造

不删除此函数，将其改造为可被 `repl_rdma_start_fullsync()` 内部调用，提取建连逻辑为共用函数。

---

## 4. Phase 2: eBPF 三个探测点实现

### 4.1 探测点 ① — `repl_client_capture.bpf.c` 中新增 `kprobe/tcp_sendmsg`（✅ 已实现）

**实际实现位置**: `src/replication/bpf/repl_client_capture.bpf.c`（而非原计划的 `repl_kprobe.bpf.c`）

这样设计的好处：client capture 的三个探测点共享同一个 BPF 对象和 map（`client_ctl`、`client_stats`、`client_tmpbuf`），避免了跨 BPF 对象通信的复杂性。

#### 4.1.1 BPF map 控制键 (client_ctl)

```c
/* client_ctl:
 * [0]: ENABLED              — 0=禁用 1=启用
 * [1]: PID                  — Master 进程 PID
 * [2]: LISTEN_PORT          — Master 监听端口
 * [3]: FULLSYNC_IN_PROGRESS — 1=全量同步进行中 0=正常
 */
```

#### 4.1.2 `kprobe/tcp_sendmsg` — REPLDONE 检测逻辑（实际实现）

```c
SEC("kprobe/tcp_sendmsg")
int kprobe_client_sendmsg(struct pt_regs *ctx)
{
    // 1. 检查 ENABLED + PID 过滤
    // 2. 只在 FULLSYNC_IN_PROGRESS=1 时探测
    // 3. 从 msg->msg_iter->iov 读取发送数据
    // 4. 前 64 字节中扫描 "REPLDONE" 子串 (#pragma unroll)
    // 5. 匹配 → client_ctl[3] = 0 (自动清除 FULLSYNC_IN_PROGRESS)
    // 6. stats[7]++ (REPLDONE_DETECT 计数)
}
```

**关键设计决策**：
- REPLDONE 通过 `queue_bytes()` 走 TCP 发送（`queue_snapshot()` 中先停 RDMA 再发 REPLDONE）
- BPF 在 `tcp_sendmsg` 入口即可读取数据（数据在 iovec 中已准备好），无需 kretprobe
- 扫描窗口 64 字节，使用 `#pragma unroll` 保证 verifier 接受
- BPF 直接操作 `client_ctl[3]`，无需额外 ringbuf 通知机制

#### 4.1.3 用户态验证 — `repl_client_capture_set_fullsync()`

```c
void repl_client_capture_set_fullsync(int in_progress) {
    // 清除标志前先读取 BPF map 当前值
    // 如果 prev==0 且 in_progress==0 → BPF 已先一步清除
    // 日志输出: "already_cleared_by_bpf, repldone_detect=%llu"
    // 同时读取 stats[7] (REPLDONE_DETECT) 验证探测是否成功
}
```

**时序**：
```
queue_snapshot()
  ├─ set_fullsync(1)           → client_ctl[3]=1
  ├─ 发送快照 (RDMA)
  ├─ 停 RDMA
  ├─ REPLDONE (queue_bytes → TCP)
  │    └─ tcp_sendmsg → BPF kprobe → client_ctl[3]=0 ★
  ├─ set_fullsync(0)           → 读到 prev=0 → "already_cleared_by_bpf"
  └─ flush L1+L2 缓存
```

### 4.2 探测点 ②+③ — 新增 BPF 程序探测客户端写入

#### 4.2.1 新增 BPF 程序文件

**文件**: `src/replication/bpf/repl_client_capture.bpf.c`

```c
// ============================================================
// repl_client_capture.bpf.c — kprobe BPF 程序
//
// Hook tcp_recvmsg，捕获 Master 接收的客户端写入数据。
//
// 全量同步期间 (FULLSYNC_IN_PROGRESS=1):
//   数据写入 client_cache_ringbuf 缓存
//
// 全量同步完成后 (FULLSYNC_IN_PROGRESS=0):
//   数据仍然写入 ringbuf，用户态直接转发到 slave
//
// x86_64 调用约定 (tcp_recvmsg):
//   struct sock *sk          = PT_REGS_PARM1(ctx) = di
//   struct msghdr *msg       = PT_REGS_PARM2(ctx) = si
//   size_t size              = PT_REGS_PARM3(ctx) = dx
// ============================================================

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>

#ifndef BPF_MAP_TYPE_RINGBUF
#define BPF_MAP_TYPE_RINGBUF 27
#endif

#define CLIENT_ENTRY_HDR_SZ    4
#define CLIENT_ENTRY_MAX_LEN   500

/* x86_64 pt_regs */
struct pt_regs {
    unsigned long r15;    unsigned long r14;
    unsigned long r13;    unsigned long r12;
    unsigned long bp;     unsigned long bx;
    unsigned long r11;    unsigned long r10;
    unsigned long r9;     unsigned long r8;
    unsigned long ax;     unsigned long cx;
    unsigned long dx;     unsigned long si;
    unsigned long di;     unsigned long orig_ax;
    unsigned long ip;     unsigned long cs;
    unsigned long flags;  unsigned long sp;
    unsigned long ss;
};

/* ---- BPF Maps ---- */

/* Control Map:
 * [0]: ENABLED       — 0=禁用 1=启用
 * [1]: PID           — Master 进程 PID
 * [2]: LISTEN_PORT   — Master 监听端口（用于过滤客户端连接）
 * [3]: FULLSYNC_IN_PROGRESS — 1=全量同步进行中 0=正常
 * [4]: DROP_NON_RESP — 1=只捕获以 * 开头的 RESP 命令
 */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u64);
} client_ctl SEC(".maps");

/* Stats Map:
 * [0]: HIT           — 总命中次数
 * [1]: SKIP_PID      — PID 不匹配跳过
 * [2]: RB_ERR        — ringbuf 写入错误
 * [3]: DATA_OVR      — 数据超过上限
 * [4]: READ_ERR      — probe_read 失败
 * [5]: CACHED        — 缓存条目数
 */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, __u64);
} client_stats SEC(".maps");

/* Ringbuf — 缓存客户端写入数据 */
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 20);  /* 1MB */
} client_cache_ringbuf SEC(".maps");

/* 临时缓冲区（per-CPU，避免 BPF 栈 512B 限制） */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, unsigned char[CLIENT_ENTRY_HDR_SZ + CLIENT_ENTRY_MAX_LEN]);
} client_tmpbuf SEC(".maps");

/* ---- 从 msghdr 读取 iovec 数据 ---- */
static __always_inline int read_client_msg_data(unsigned long msg_ptr,
    unsigned char *buf, int max_len)
{
    /* msghdr 布局 (x86_64 kernel):
     *   msg_name(8) + msg_namelen(4) + pad(4) = 16
     *   msg_iter(40) = 56, 其中 iov 在 iter+24, nr_segs 在 iter+32
     *   所以 iov 指针在 msg_ptr + 16 + 24 = msg_ptr + 40
     *   nr_segs 在 msg_ptr + 16 + 32 = msg_ptr + 48
     */
    const struct { unsigned long b; unsigned long l; } *iov = 0;
    if (bpf_probe_read_kernel(&iov, sizeof(iov),
            (const void *)(msg_ptr + 40)) != 0)
        return 0;
    if (!iov) return 0;

    unsigned long nr_segs = 0;
    if (bpf_probe_read_kernel(&nr_segs, sizeof(nr_segs),
            (const void *)(msg_ptr + 48)) != 0)
        return 0;
    if (nr_segs == 0) return 0;

    struct { unsigned long b; unsigned long l; } vec;
    if (bpf_probe_read_kernel(&vec, sizeof(vec), &iov[0]) != 0)
        return 0;
    if (!vec.b || vec.l == 0) return 0;

    unsigned long long safe_len = vec.l;
    if (safe_len > (unsigned long long)max_len)
        safe_len = (unsigned long long)max_len;
    if (safe_len == 0) return 0;

    if (bpf_probe_read_user(buf, (__u32)safe_len,
            (const void *)(unsigned long)vec.b) != 0) {
        __u64 *st = bpf_map_lookup_elem(&client_stats,
            &(__u32){4});  /* READ_ERR */
        if (st) __sync_fetch_and_add(st, 1);
        return 0;
    }
    return (int)safe_len;
}

SEC("kprobe/tcp_recvmsg")
int kprobe_client_recv_msg(struct pt_regs *ctx)
{
    __u64 *enabled, *target_pid, *stat;

    /* 1. 检查开关 */
    enabled = bpf_map_lookup_elem(&client_ctl, &(__u32){0}); /* ENABLED */
    if (!enabled || !*enabled)
        return 0;

    /* 2. PID 过滤 — 只捕获 Master 进程的数据接收 */
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    target_pid = bpf_map_lookup_elem(&client_ctl, &(__u32){1}); /* PID */
    if (!target_pid)
        return 0;
    if (pid != (__u32)(*target_pid)) {
        stat = bpf_map_lookup_elem(&client_stats, &(__u32){1}); /* SKIP_PID */
        if (stat) __sync_fetch_and_add(stat, 1);
        return 0;
    }

    /* 3. 获取数据长度 */
    __u32 size = (__u32)ctx->dx;
    if (size == 0) return 0;
    if (size > CLIENT_ENTRY_MAX_LEN) {
        stat = bpf_map_lookup_elem(&client_stats, &(__u32){3}); /* DATA_OVR */
        if (stat) __sync_fetch_and_add(stat, 1);
        size = CLIENT_ENTRY_MAX_LEN;
    }

    /* 4. 读取数据 */
    __u32 map_key = 0;
    unsigned char(*entry)[CLIENT_ENTRY_HDR_SZ + CLIENT_ENTRY_MAX_LEN];
    entry = bpf_map_lookup_elem(&client_tmpbuf, &map_key);
    if (!entry) return 0;

    unsigned long msg_ptr = (unsigned long)ctx->si;

    int data_len = read_client_msg_data(msg_ptr, (*entry) + 4, CLIENT_ENTRY_MAX_LEN);
    if (data_len <= 0) return 0;

    /* 5. 写入 payload_len */
    __u32 payload_len = (__u32)data_len;
    __builtin_memcpy(*entry, &payload_len, 4);

    /* 6. 写入 ringbuf */
    int entry_size = CLIENT_ENTRY_HDR_SZ + data_len;
    if (bpf_ringbuf_output(&client_cache_ringbuf, *entry, entry_size, 0) != 0) {
        stat = bpf_map_lookup_elem(&client_stats, &(__u32){2}); /* RB_ERR */
        if (stat) __sync_fetch_and_add(stat, 1);
        return 0;
    }

    /* 7. 更新统计 */
    stat = bpf_map_lookup_elem(&client_stats, &(__u32){0}); /* HIT */
    if (stat) __sync_fetch_and_add(stat, 1);

    __u64 *in_progress = bpf_map_lookup_elem(&client_ctl, &(__u32){3});
    if (in_progress && *in_progress) {
        stat = bpf_map_lookup_elem(&client_stats, &(__u32){5}); /* CACHED */
        if (stat) __sync_fetch_and_add(stat, 1);
    }

    return 0;
}

char _license[] SEC("license") = "GPL";
```

#### 4.2.2 用互 eBPF 加载与管理

**文件**: `src/replication/kvs_repl_kprobe.c` — 新增以下函数

```c
/* ---- Client Capture BPF 管理 ---- */

static struct bpf_object *g_client_obj = NULL;
static int g_client_ctl_fd = -1;
static int g_client_stats_fd = -1;
static int g_client_ringbuf_fd = -1;
static struct ring_buffer *g_client_ringbuf = NULL;
static pthread_t g_client_poll_tid;
static int g_client_poll_started = 0;
static volatile int g_client_running = 0;

/* 缓存队列 */
typedef struct fullsync_cache_node_s {
    unsigned char *data;
    size_t len;
    struct fullsync_cache_node_s *next;
} fullsync_cache_node_t;

static fullsync_cache_node_t *g_cache_head = NULL;
static fullsync_cache_node_t *g_cache_tail = NULL;
static pthread_mutex_t g_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile int g_cache_count = 0;
static volatile unsigned long long g_cache_bytes = 0;

/* ringbuf 回调 — 接收客户端数据 */
static int client_ringbuf_cb(void *ctx, void *data, size_t size) {
    (void)ctx;
    if (size < 4) return 0;

    __u32 payload_len;
    memcpy(&payload_len, data, 4);
    if (payload_len == 0 || payload_len + 4 > size) return 0;

    unsigned char *payload = (unsigned char *)data + 4;

    if (g_repl_fullsync_in_progress) {
        /* 全量同步中 → 缓存到队列 */
        fullsync_cache_node_t *node = (fullsync_cache_node_t *)
            kvs_malloc(sizeof(fullsync_cache_node_t));
        if (!node) return -1;

        node->data = (unsigned char *)kvs_malloc(payload_len);
        if (!node->data) { kvs_free(node); return -1; }
        memcpy(node->data, payload, payload_len);
        node->len = payload_len;
        node->next = NULL;

        pthread_mutex_lock(&g_cache_lock);
        if (g_cache_tail) {
            g_cache_tail->next = node;
        } else {
            g_cache_head = node;
        }
        g_cache_tail = node;
        g_cache_count++;
        g_cache_bytes += payload_len;
        pthread_mutex_unlock(&g_cache_lock);
    }
    // else: 正常模式，数据直接走 repl_broadcast → backlog → 转发
    // 不需要在此额外处理

    return 0;
}

/* 初始化 client capture */
int repl_client_capture_init(void) {
    // 1. 打开 BPF 对象文件
    // 2. 加载
    // 3. 获取 map FDs
    // 4. Attach kprobe 到 tcp_recvmsg
    // 5. 创建 ringbuf
    // 6. 设置 PID = getpid()
    // 7. 设置 ENABLED = 1
    // 8. 启动 ringbuf 轮询线程
}

/* 设置全量同步状态 */
void repl_client_capture_set_fullsync(int in_progress) {
    if (g_client_ctl_fd < 0) return;
    __u64 val = in_progress ? 1 : 0;
    bpf_map_update_elem(g_client_ctl_fd, &(__u32){3}, &val, 0);
}

/* flush 缓存的客户端数据到 slave */
int repl_client_capture_flush_to_slave(conn_t *c) {
    if (!c) return -1;

    pthread_mutex_lock(&g_cache_lock);
    fullsync_cache_node_t *node = g_cache_head;
    g_cache_head = NULL;
    g_cache_tail = NULL;
    int count = g_cache_count;
    unsigned long long bytes = g_cache_bytes;
    g_cache_count = 0;
    g_cache_bytes = 0;
    pthread_mutex_unlock(&g_cache_lock);

    if (!node) {
        repl_rdma_log("cache_flush", "no cached data to flush");
        return 0;
    }

    fprintf(stderr, "repl: flushing %d cached entries (%llu bytes) to slave\n",
        count, bytes);

    /* 通过 TCP 发送缓存数据 */
    fullsync_cache_node_t *cur = node;
    while (cur) {
        if (repl_send_chunked(c, cur->data, cur->len) != 0) {
            fprintf(stderr, "repl: cache flush send failed\n");
            break;
        }
        fullsync_cache_node_t *next = cur->next;
        kvs_free(cur->data);
        kvs_free(cur);
        cur = next;
    }

    fprintf(stderr, "repl: cache flush complete\n");
    return 0;
}

/* 清理 */
void repl_client_capture_cleanup(void) {
    g_client_running = 0;
    // 等待轮询线程退出
    // 释放 ringbuf
    // 卸载 BPF 对象
    // 清理残留缓存节点
}
```

#### 4.2.3 新增头文件声明

**文件**: `include/kvstore/replication/repl_kprobe.h`

```c
/* ---- Client capture 接口 ---- */
int repl_client_capture_init(void);
void repl_client_capture_set_fullsync(int in_progress);
int repl_client_capture_flush_to_slave(conn_t *c);
void repl_client_capture_cleanup(void);

/* ---- REPLDONE 探测接口 ---- */
void repl_kprobe_fullsync_done(void);
```

### 4.3 整合到 `main()` 初始化流程

**文件**: `src/main/kvstore.c` — `main()` 中新增

```c
// 在 kprobe+RDMA 增量同步初始化之后，新增:
if (g_cfg.kprobe_enabled && g_cfg.role == ROLE_MASTER) {
    if (repl_client_capture_init() != 0) {
        fprintf(stderr, "client capture init failed, continuing without cache\n");
    }
}
```

---

## 5. Phase 3: eBPF+tcp 增量同步路径

### 5.1 数据流

```
Client TCP SET key
      │
      ▼
┌─ 内核 ──────────────────────────────────────┐
│  kprobe/tcp_recvmsg (kretprobe)               │
│  → 读取 iovec 中的 RESP 数据                   │
│  → 写入 client_cache_ringbuf (1MB, mmap 零拷贝)│
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌─ 用户态 (client_poll_thread) ────────────────┐
│  client_ringbuf_cb()                          │
│       │                                       │
│   g_repl_fullsync_in_progress?                │
│       │                                       │
│  ┌────┴────┐                                  │
│  ▼         ▼                                  │
│ FULL     INCR                                 │
│  │         │                                  │
│  ▼         ▼                                  │
│ L1缓存    forward_to_slave()                  │
│ (链表)     → send(slave_fd) → Slave           │
│  │                                             │
│  ├─ L1 < 4MB → 追加到链表                      │
│  └─ L1 ≥ 4MB → cache_spill_to_l2()           │
│                 → L2 磁盘文件                  │
└──────────────────────────────────────────────┘
```

### 5.2 核心改动

#### 5.2.1 全局转发标志

新增 `g_repl_client_capture_active` 标志，在 `client_capture` BPF 加载成功后置位。

- `repl_broadcast()` 中检查该标志，激活时跳过 `repl_realtime_send()` **避免同一份数据被发送两次**
- `REPLSYNC` handler 中记录 slave fd → `g_repl_capture_slave_fd`

#### 5.2.2 INCR 模式直接转发

```c
static int forward_to_slave(const unsigned char *data, size_t len) {
    extern int g_repl_capture_slave_fd;
    ssize_t sent = send(g_repl_capture_slave_fd, data, len, MSG_NOSIGNAL);
    return (sent < 0 || (size_t)sent != len) ? -1 : 0;
}
```

**关键**: `forward_to_slave()` 在 `client_poll_thread` 上下文中调用，直接 send 到 slave 的 TCP 连接。BPF 捕获的是 **纯应用层 RESP 数据**（TCP 头部已被内核去除），所以可以直接发送。

#### 5.2.3 L2 磁盘缓存

当 L1 内存队列超过 `CACHE_L1_MAX_BYTES` (4MB) 时，后续数据 spill 到磁盘:

```c
#define CACHE_L1_MAX_BYTES  (4 * 1024 * 1024)   /* 4MB */
#define CACHE_L2_PATH       "/tmp/kvstore_fs_cache"

/* 写入格式: [4B len][payload] */
```

flush 时按 **L1 → L2 顺序** 发送，保证数据有序。L2 发送完后删除临时文件。

### 5.3 新旧路径对比

| 维度 | kprobe+RDMA (旧) | eBPF+tcp (新) |
|------|-----------------|--------------|
| Hook 点 | tcp_sendmsg (master→slave) 触发 | tcp_recvmsg (client→master) 捕获 |
| 数据拷贝 | TCP 一份 + RDMA 一份 = **2 份** | TCP 一份 = **1 份** |
| 触发方式 | 需要先 send(slave_fd) 再 kprobe 捕获 | 在应用处理前直接捕获 |
| RDMA 依赖 | ✅ 必须有 | ❌ 不需要 |
| 增量路径 | send(slave_fd) → TCP + kprobe → RDMA | forward_to_slave() → TCP |
| `repl_broadcast` 行为 | 正常发送 | 跳过 (g_repl_client_capture_active) |

### 5.4 输入路径决策

```
写命令到达 Master 内核 (tcp_recvmsg)
        │
        ▼
  client_capture 已加载?
        │
   ├── Yes (g_repl_client_capture_active=1)
   │       │
   │   g_repl_fullsync_in_progress?
   │       │
   │   ├── Yes → L1 缓存 → L2 磁盘 (STATE_FULL)
   │   │          repl_broadcast 跳过
   │   │
   │   └── No  → forward_to_slave() (STATE_INCR)
   │              repl_broadcast 跳过
   │
   └── No (无 BPF, 非 root)
           │
           ▼
      reactor recv() → handle → repl_broadcast()
           │
           ├── repl_backlog_feed() → backlog
           └── repl_realtime_send() → slave
```

---

## 6. Phase 4: 集成测试

### 5.1 测试场景

#### 场景 1: RDMA 按需启停

```
1. 启动 master (不启动 RDMA listener)
2. 预写 5w 条数据
3. 启动 slave
4. Slave 连接 → 发送 REPLSYNC
5. 验证: master 在此刻才启动 RDMA listener
6. 验证: RDMA 全量同步完成
7. 验证: master 在 REPLDONE 后关闭 RDMA
8. 验证: slave 数据一致性 (5w)
```

#### 场景 2: 全量同步期间 eBPF 缓存

```
1. 启动 master + slave
2. 触发全量同步
3. 全量同步期间，客户端持续写入 5w 数据
4. 全量同步完成后
5. 验证: eBPF 缓存的客户端数据被发送到 slave
6. 验证: slave 数据一致性 (5w预存 + 5w缓存) = 10w
```

#### 场景 3: 后续增量同步

```
1. 全量同步 + 缓存数据 flush 完成后
2. 客户端继续写入数据
3. 验证: 数据通过正常增量同步路径到达 slave
4. 验证: slave 数据一致性
```

### 6.2 测试工具

基于现有的 `test_repl_5w5w.c` 改造:

- 新增 `--rdma-on-demand` 选项测试按需启停
- 新增 `--ebpf-cache` 选项测试 eBPF 缓存
- 保持向后兼容

### 6.3 验证指标

| 指标 | 预期 |
|------|------|
| RDMA 启动时机 | 仅在全量同步开始后 |
| RDMA 关闭时机 | REPLDONE 发送后立即 |
| 缓存数据完整性 | 全量同步期间客户端写入 100% 到达 slave |
| 增量同步连续性 | 缓存数据发送后 → 后续增量无缝衔接 |
| eBPF+tcp 新路径 | INCR 模式直接转发，`repl_broadcast` 跳过 |
| L2 磁盘缓存 | L1 超 4MB 时自动 spill，flush 时按序发送 |
| 无 RDMA 回退 | 自动回退 TCP，数据不丢失 |

---

## 7. 文件修改清单

### 7.1 新增文件

| # | 文件 | 说明 |
|---|------|------|
| 1 | `src/replication/bpf/repl_client_capture.bpf.c` | 新增 BPF 程序 — hook `tcp_recvmsg` 捕获客户端写入 |

### 7.2 修改文件

| # | 文件 | 改动内容 |
|---|------|---------|
| 1 | `src/main/kvstore.c` | 移除 `main()` 中 `start_rdma_master_listener()`；`queue_snapshot()` 整合 RDMA start/stop + flush；`repl_broadcast()` 增加全量同步跳过逻辑 + eBPF+tcp 激活跳过；新增全局标志 `g_repl_fullsync_in_progress`、`g_repl_client_capture_active`、`g_repl_capture_slave_fd`；`main()` 中新增 `repl_client_capture_init()` |
| 2 | `src/replication/kvs_repl.c` | 新增 `repl_rdma_start_fullsync()` / `repl_rdma_stop_fullsync()`；从机 `slave_thread()` 改造建连流程 |
| 3 | `src/replication/kvs_repl_kprobe.c` | 新增 REPLDONE 回调处理；新增 client capture 加载/管理/缓存队列/L2磁盘/flush 转发逻辑；新增 `forward_to_slave()`、`cache_spill_to_l2()`、`cache_flush_l2_to_slave()` |
| 4 | `src/replication/bpf/repl_client_capture.bpf.c` | **探测点①②③ 均在此文件实现**。新增 `kprobe/tcp_sendmsg`（REPLDONE 检测 + 自动清除 FULLSYNC_IN_PROGRESS）；新增 stats[7] REPLDONE_DETECT 计数器；已有 `kprobe/tcp_recvmsg` + `kretprobe/tcp_recvmsg`（客户端数据捕获）
| 5 | `include/kvstore/kvstore.h` | 新增 `g_repl_fullsync_in_progress`、`g_repl_client_capture_active`、`g_repl_capture_slave_fd` 外部变量声明；新增函数声明 |
| 6 | `include/kvstore/replication/repl_kprobe.h` | 新增 client capture 和 REPLDONE 接口声明 |
| 7 | `Makefile` | 新增 `repl_client_capture.bpf.o` 编译目标 |

---

## 8. 风险与注意事项

### 7.1 RDMA 启动延迟

- RDMA bind/listen/accept 过程可能耗时 100ms-500ms
- 从机需要设置合适的超时等待（建议 10s）
- 若超时未建立 RDMA 连接，自动回退 TCP

### 7.2 Backlog 溢出

- 当前 backlog 大小 1MB，全量同步期间大量写入可能溢出
- eBPF ringbuf 缓存 (1MB) 作为弹性扩展
- 若仍不足，考虑动态增大 backlog 或分段 flush

### 7.3 eBPF 程序限制

- BPF 栈空间 512B，复杂逻辑使用 per-CPU map 作为临时缓冲区
- `tcp_recvmsg` 在不同内核版本参数布局可能不同，需 CO-RE 兼容
- 需 root 权限加载 BPF 程序

### 7.4 连接区分

- `tcp_recvmsg` hook 会捕获所有进入 master 的数据，包括 slave 的 REPLACK
- 方案: 通过端口过滤（客户端连接端口 ≠ slave 连接端口）
- 更精确: 维护 slave socket cookie BPF map，排除已知 slave 连接

### 7.5 兼容性

- 保留 `repl_fullsync_transport=tcp` 的纯 TCP 路径
- 保留 `KVS_ENABLE_RDMA=0` 编译配置
- 确保在不支持 eBPF 的内核上正常回退
