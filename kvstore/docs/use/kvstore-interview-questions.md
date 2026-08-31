# kvstore 项目面试问题集

> 本文档模拟面试官视角，基于 kvstore 项目设计的面试问题。
> 覆盖 C 语言基础、系统编程、网络、存储、RDMA、eBPF、BPF、设计等多维度。
> 每题标注考察维度、难度、和答题要点。

---

## 目录

1. [C 语言与系统编程基础](#1-c-语言与系统编程基础)
2. [网络模型与 I/O](#2-网络模型与-io)
3. [数据结构与存储引擎](#3-数据结构与存储引擎)
4. [持久化与文件 I/O](#4-持久化与文件-io)
5. [内存管理](#5-内存管理)
6. [主从复制](#6-主从复制)
7. [RDMA 原理与编程](#7-rdma-原理与编程)
8. [eBPF 与 BPF 编程](#8-ebpf-与-bpf-编程)
9. [kprobe 与内核追踪](#9-kprobe-与内核追踪)
10. [并发与多线程](#10-并发与多线程)
11. [设计决策与架构](#11-设计决策与架构)
12. [调试与问题排查](#12-调试与问题排查)
13. [进阶挑战题](#13-进阶挑战题)

---

## 1. C 语言与系统编程基础

### Q1.1: `volatile` 关键字在项目中的使用场景

**考察维度**：C 语言细节 / 并发编程  
**难度**：⭐⭐  
**相关代码**：`kprobe_rdma_ringbuf_t` 中的 `producer_head` / `consumer_tail`、`g_wr_slots[].in_flight`

**问题**：项目中的 `producer_head` 和 `consumer_tail` 为什么要用 `volatile` 修饰？不加会有什么问题？

**追问**：`volatile` 能保证原子性吗？如果不能，这个场景为什么不需要原子操作？

**答题要点**：
- `volatile` 防止编译器优化——每次读取都从内存读取而非寄存器缓存
- `producer_head` 被 Master 通过 RDMA WRITE 写入，Slave 轮询读取——编译器不知道远端 RNIC 会修改该内存
- 不加 `volatile`，编译器可能把 `while (head == tail)` 优化为死循环（只读一次寄存器值）
- `volatile` **不能**保证原子性，但这里单生产者单消费者模型，不存在竞争条件

---

### Q1.2: `__sync_synchronize()` 的作用

**考察维度**：内存模型 / CPU 架构  
**难度**：⭐⭐⭐  
**相关代码**：`kprobe_rdma_slave_poll()` 中的 `__sync_synchronize()`

**问题**：Slave 轮询线程在读取 `producer_head` 前调用 `__sync_synchronize()`，在更新 `consumer_tail` 后也调用一次。为什么需要这两个屏障？

**追问**：x86 架构下 `__sync_synchronize()` 对应什么指令？ARM 下呢？

**答题要点**：
- 第一个屏障：读取 head 前，保证看到 Master 通过 RDMA WRITE 写入的最新值（DMA 写入可能乱序到达 CPU 缓存）
- 第二个屏障：写入 tail 后，保证写入对其他线程（或下次轮询）可见
- x86: `mfence` 指令（或 `lock addl`）
- ARM: `dmb` (Data Memory Barrier)

---

### Q1.3: `__attribute__((packed))` 的作用

**考察维度**：C 语言 / 内存布局  
**难度**：⭐⭐  
**相关代码**：`kprobe_rdma_ringbuf_t`

**问题**：为什么环形缓冲区结构体要用 `__attribute__((packed))`？如果不加，会有什么问题？

**答题要点**：
- 结构体默认会对齐（成员按自然边界对齐，结构体按最大成员对齐），`packed` 禁止自动填充
- 不加 packed，Slave 侧计算 `offset 16` 时可能因为对齐填充导致偏移错误
- RDMA WRITE 按固定偏移写入，偏移必须与结构体布局精确一致

---

### Q1.4: 项目中用到了哪些编译选项？各自的用途是什么？

**考察维度**：构建系统  
**难度**：⭐⭐  
**相关代码**：`Makefile`

**问题**：`-O2 -g -target bpf` 分别有什么用？为什么 BPF 程序用 `-target bpf` 而非常规编译？

**答题要点**：
- `-O2`：优化级别，BPF 必须至少 `-O2`，否则 verifier 可能拒绝（未优化代码可能有不可达路径）
- `-g`：生成 BTF 调试信息，libbpf 需要 BTF 做 CO-RE 重定位
- `-target bpf`：生成 BPF 字节码（不是 x86 指令）
- 常规 C 程序用默认 target（x86-64），BPF 程序必须在 BPF 虚拟机中运行

---

## 2. 网络模型与 I/O

### Q2.1: Reactor vs Proactor vs 协程的本质区别

**考察维度**：I/O 模型理解  
**难度**：⭐⭐⭐  
**相关代码**：`src/core/reactor.c` / `proactor.c` / `ntyco.c`

**问题**：Reactor、Proactor、协程三种模型都是解决 I/O 效率问题的，它们各自的核心思想是什么？在 kvstore 中分别怎么实现的？

**答题要点**：

| 模型 | 核心思想 | kvstore 实现 |
|------|---------|-------------|
| Reactor | 事件驱动，"就绪通知" | epoll_wait → 可读/可写回调 |
| Proactor | "操作完成通知"，一步到位 | io_uring SQE 提交 → CQE 消费 |
| 协程 | 同步写法，异步执行 | NtyCo hook recv → yield → resume |

---

### Q2.2: epoll 的 LT 和 ET 模式有什么区别？

**考察维度**：网络编程  
**难度**：⭐⭐⭐

**问题**：项目中用 epoll 水平触发（LT），如果改为边沿触发（ET）需要改哪些地方？

**答题要点**：
- LT：只要缓冲区有数据，每次 `epoll_wait` 都通知；ET：只有状态变化时通知一次
- LT 不要求一次性读完，编程简单；ET 必须循环读写直到 EAGAIN
- 改为 ET 需改：`on_read()` 必须 `while(recv() > 0)` 循环读取，直到返回 EAGAIN
- ET 配合非阻塞 I/O，减少 epoll 事件触发次数

---

### Q2.3: `queue_bytes` 为什么用链表而不用环形缓冲区？

**考察维度**：设计决策  
**难度**：⭐⭐⭐  
**相关代码**：`queue_bytes()` 中的 `out_node_t` 链表

**问题**：发送缓冲区为什么用链表实现而非环形缓冲区？在什么场景下链表比环形缓冲区更合适？

**答题要点**：
- 链表可以动态增长——单个连接的发送数据突发时不会溢出
- 环形缓冲区有固定大小，溢出需要特殊处理（丢弃或阻塞）
- 链表的缺点是分配开销——每次 `queue_bytes` 都 malloc/free
- 可以用 slab 分配器优化 out_node 的分配（项目中 `kvs_malloc` 走 custom slab 时小对象开销低）

---

## 3. 数据结构与存储引擎

### Q3.1: 为什么同时实现红黑树和跳表？

**考察维度**：数据结构理解  
**难度**：⭐⭐  
**相关代码**：`src/storage/kvs_rbtree.c` / `kvs_skiptable.c`

**问题**：红黑树和跳表都是有序数据结构，时间复杂度都是 O(log n)，为什么要实现两个？它们在实现上的本质区别是什么？

**答题要点**：
- **红黑树**：通过旋转保持平衡，最坏情况保证 O(log n)，实现复杂
- **跳表**：通过概率（50% 概率提升层数）实现平衡，实现简单，最坏情况 O(n) 概率极低
- 跳表更适合并发（锁范围更小），红黑树旋转可能涉及多个节点
- 跳表在范围查询时更高效（同层链表可以顺序遍历）

---

### Q3.2: FNV-1a 哈希算法为什么适合这个场景？

**考察维度**：哈希算法  
**难度**：⭐⭐  
**相关代码**：`kvs_hash.c` 中的 `_hash()`

**问题**：为什么选 FNV-1a 而非其他哈希算法（如 MurmurHash、XXHash）？

**答题要点**：
- **简单**：核心代码仅 5 行，易于调试和理解
- **速度**：纯整数运算，无循环依赖
- **分布均匀**：虽然不是加密哈希，但对一般 key 分布足够
- MurmurHash/XXHash 性能更好但代码复杂，作为学习项目 FNV-1a 更合适

---

### Q3.3: Hash 引擎的冲突解决策略

**考察维度**：哈希表实现  
**难度**：⭐⭐

**问题**：Hash 引擎用链地址法解决冲突，为什么不用开放地址法（线性探测/二次探测/双重哈希）？

**答题要点**：
- 链地址法实现简单，每个桶是一个链表
- 删除操作简单（链表节点删除）
- 开放地址法在负载高时性能急剧下降，且删除需要"标记删除"而非真正移除
- 项目中 `MAX_TABLE_SIZE=1024` 是固定大小，链地址法能处理超出桶数量的 key

---

### Q3.4: Doc 引擎的设计动机

**考察维度**：需求分析 / 设计  
**难度**：⭐⭐⭐

**问题**：文档引擎为什么不做在 value 字段里（比如把 JSON 存成 value），而要独立实现一层？

**答题要点**：
- 独立一层后，**持久化**和**复制**链路可以独立对齐——引擎有自己的 dump/AOF 路径
- 字段级别操作无需读写整个 value——`DOCSET key field value` 只需修改一个字段
- 如果 JSON 字符串存 value，修改一个字段需要：GET → JSON 解析 → 修改 → JSON 序列化 → SET，效率低

---

## 4. 持久化与文件 I/O

### Q4.1: 为什么优先用 mmap 恢复 dump 文件？

**考察维度**：文件 I/O / 操作系统  
**难度**：⭐⭐⭐

**问题**：mmap 恢复 dump 文件和 fread 逐块读取相比，各有什么优劣？为什么项目中 mmap 失败了才回退到 fread？

**答题要点**：

| 对比 | mmap | fread |
|------|------|-------|
| 内核→用户拷贝 | 0 次（直接映射） | 1 次（page cache → 用户 buf） |
| 编码复杂度 | 低（指针操作） | 中（管理读取进度） |
| 小文件 | 有页对齐浪费 | 无额外开销 |
| 大文件 | 按需加载，惰性 | 批量读取 |
| 内存压力 | 占用虚拟地址空间 | 临时缓冲区，用完释放 |

- mmap 恢复时，解析逻辑直接 `mapped + pos`，不需要手动管理文件读缓冲
- 内存压力大时 mmap 可能失败（虚拟地址空间不足），回退到传统 fread

---

### Q4.2: io_uring 做 AOF 写入的优势

**考察维度**：异步 I/O  
**难度**：⭐⭐⭐⭐  
**相关代码**：`persist_write_fd_uring()`

**问题**：为什么用 io_uring 写 AOF 文件？相比于普通的 `pwrite()` 有什么优势？io_uring 的 SQE 和 CQE 是什么？

**答题要点**：
- `pwrite()` 是同步的——调用线程阻塞直到磁盘写入完成
- io_uring 支持**异步提交**：提交 SQE 后立即返回，不阻塞
- SQE (Submission Queue Entry)：描述 I/O 操作（写什么、写到哪）
- CQE (Completion Queue Entry)：描述操作结果（写了多少字节、是否成功）
- 在 `appendfsync always` 模式下，io_uring 也能异步 fsync

---

### Q4.3: 为什么 AOF 重写要用 fork 子进程？

**考察维度**：进程模型 / 持久化设计  
**难度**：⭐⭐⭐

**问题**：BGREWRITEAOF 为什么用 fork 而不是线程？fork 在重写过程中如何保证数据一致性？

**追问**：fork 子进程写 AOF 期间，主进程还在处理写命令，这些新命令怎么办？

**答题要点**：
- fork 子进程获得父进程内存快照的完整副本（写时拷贝）
- 子进程写的是"fork 时刻的快照"，父进程继续处理新请求
- 新命令通过 `g_rewrite_buf` 缓冲区转发——重写完成后，缓冲区内容追加到新 AOF
- 用 fork 而非线程：线程共享内存，无法获得一致的快照

---

### Q4.4: 海量 TTL 的持久化与恢复

**考察维度**：大数据量 / 过期策略  
**难度**：⭐⭐⭐⭐  
**相关代码**：`persist_snapshot_expire_to_fd()`

**问题**：当有 100 万个不同 TTL 的 key 时，BGREWRITEAOF 如何做到不丢失 TTL 信息？恢复时如何处理这些 TTL？

**追问**：如果重写过程耗时 10 秒，部分 TTL 在重写期间已经过期了怎么办？

**答题要点**：
- 重写时使用 `remaining_ms = expire_at_ms - now_ms()`，写入的是**剩余 TTL**值（如 `EXPIRE key 3600`）
- 恢复时重放这些 `EXPIRE` 命令，重新设置 TTL，到期时间从恢复时刻重新计算
- 重写期间已过期的 key：如果 `remaining_ms <= 0`，跳过不写入（已过期无需持久化）
- 对于即将过期的 key（`remaining_ms` 很小），恢复后短时间内过期——不影响正确性

---

## 5. 内存管理

### Q5.1: Custom 分配器为什么用 slab + mmap 组合？

**考察维度**：内存分配器设计  
**难度**：⭐⭐⭐⭐  
**相关代码**：`src/memory/kvs_mem.c`

**问题**：slab 分配器和 mmap 分别解决什么问题？为什么不全部用 slab，也不全部用 mmap？

**答题要点**：
- **slab**：适合小对象（≤1024B），固定大小，分配快（链表头节点弹出），无外部碎片
- **mmap**：适合大对象（>1024B），每个分配独立映射，可以 munmap 归还系统
- 全部用 slab：大对象浪费（大对象放到 1024B class 会切分，不是）
- 全部用 mmap：小对象频繁分配/释放导致大量系统调用（mmap 是系统调用），性能差

---

### Q5.2: 项目中三种内存分配器如何切换？

**考察维度**：编译/运行时多态  
**难度**：⭐⭐

**问题**：`kvs_malloc` 如何在 libc/jemalloc/custom 之间切换？`INFO` 命令如何知道当前使用哪种分配器？

**答题要点**：
- 通过配置 `--mem libc|jemalloc|custom` 选择
- jemalloc 通过 `LD_PRELOAD` 实现——不修改代码，覆盖 libc 的 malloc/free
- custom 分配器通过宏或函数指针替换 `kvs_malloc`
- `INFO` 输出 `mem_backend` 字段，来自 `g_cfg.mem_backend`

---

## 6. 主从复制

### Q6.1: 复制的三种模式（全量/部分/TCP保底）之间的关系

**考察维度**：复制协议设计  
**难度**：⭐⭐⭐⭐

**问题**：当 Slave 请求同步时，Master 如何决定是全量同步还是部分同步？TCP 保底路径在什么情况下生效？

**答题要点**：
1. Slave 发 REPLSYNC 带 replid + offset
2. Master 检查 `repl_backlog_can_continue(replid, offset)`：
   - replid 匹配且 offset 在 backlog 范围 → **部分同步** (CONTINUE + backlog 数据)
   - 否则 → **全量同步** (FULLRESYNC + 快照数据)
3. TCP 保底：任何时候 RDMA/eBPF/kprobe-rdma 路径失败，`repl_transport_trigger_fallback()` 自动降级到 TCP

---

### Q6.2: rsync 为什么不适合做复制？

**考察维度**：系统设计  
**难度**：⭐⭐⭐

**问题**: rsync 也能做文件同步，为什么 kvstore 不用 rsync 而自己实现复制协议？

**答题要点**：
- **实时性**：rsync 是定时/按需同步，不是实时推送
- **延迟**：rsync 需要扫描文件、计算差异，延迟高
- **粒度**：rsync 同步整个文件，kvstore 需要同步每条写命令
- **状态机**：kvstore 复制需要跟踪 offset、处理断线重传、支持部分同步，rsync 无法实现
- **传输层**：kvstore 支持 RDMA/eBPF 等多传输层，rsync 只有 TCP

---

### Q6.3: repl_offset 的去重原理

**考察维度**：分布式系统  
**难度**：⭐⭐⭐⭐

**问题**：数据同时走 TCP 和 RDMA WRITE 两条路径到 Slave，Slave 怎么保证同一个命令不会被执行两次？

**答题要点**：
- 每条 RESP 命令有确定的长度，Master 侧 `repl_note_broadcast(rawlen)` 递增 offset
- Slave 侧 `parse_resp_stream()` 内部跟踪 `g_slave_repl_applied_offset`
- 两条路径都调用同一个 `parse_resp_stream(NULL, buf, &len, 1)`
- 当 TCP 和 RDMA 都到达时：
  - TCP 先到 → offset 前进 → 该命令已应用
  - RDMA 后到 → offset 已经 ≥ 该命令 → 跳过（`parse_resp_stream` 会处理剩余数据，但命令本身已在 TCP 路径执行过）
  - 反之亦然，无论哪条先到，另一条会被 offset 机制跳过

---

### Q6.4: 如何验证 kprobe+RDMA WRITE 路径确实在工作？

**考察维度**：调试/验证  
**难度**：⭐⭐⭐⭐

**问题**：`test_repl_5w5w --pre 50000 --post 50000` 全部通过，能证明 RDMA WRITE 路径在工作吗？如果不能，还需要什么手段？

**答题要点**：
- **不能证明**——数据可能全部走 TCP 保底路径到达，RDMA WRITE 全程静默失败
- 需要的手段：
  1. 查看 Master 日志 `wr_submit_data` / `wr_submit_head` 日志条数
  2. 统计对比：`g_rdma_writes` vs `g_total_events`
  3. 关闭 TCP 路径观察是否还能同步（危险！仅测试环境）
  4. 在数据中嵌入路径标记（如 magic byte），Slave 侧区分统计

---

## 7. RDMA 原理与编程

### Q7.1: RDMA SEND/RECV 与 RDMA WRITE 的区别

**考察维度**：RDMA 理解  
**难度**：⭐⭐⭐⭐  
**相关代码**：全量同步 vs 增量同步

**问题**：全量同步用 RDMA SEND/RECV（双边），增量同步用 RDMA WRITE（单边）。为什么全量同步不用 RDMA WRITE？

**答题要点**：

| 维度 | RDMA SEND/RECV | RDMA WRITE |
|------|---------------|------------|
| 操作类型 | 双边 | 单边 |
| Slave CPU 参与 | 需 pre-post RECV + CQ 处理 | 零参与 |
| Slave 通知 | CQ completion 事件 | 轮询内存指针 |
| 需要的信息 | QP 连接 | QP 连接 + 远端 MR (rkey, addr) |
| 数据流向 | 双向 | 单向（只有写入方） |

- 全量同步不需要"消费方确认"，但数据量大，使用成熟的 SEND/RECV 模式更简单可靠
- 全量同步的 Slave 侧已经有 `recv_slots` + CQ 处理逻辑，SEND/RECV 是自然选择
- 增量同步追求低延迟，RDMA WRITE 让 Slave CPU 零参与，更优

---

### Q7.2: RDMA 建立连接的过程

**考察维度**：RDMA 编程模型  
**难度**：⭐⭐⭐⭐

**问题**：描述一下 RDMA 从创建 event channel 到 ESTABLISHED 的完整流程。Master 和 Slave 分别执行哪些步骤？

**答题要点**：

```
Master:                              Slave:
rdma_create_event_channel()          rdma_create_event_channel()
rdma_create_id()                     rdma_create_id()
rdma_resolve_addr()                  rdma_bind_addr()
rdma_resolve_route()                 rdma_listen()
ibv_alloc_pd()                       rdma_get_cm_event(CONNECT_REQUEST)
ibv_create_cq()                      ibv_alloc_pd()
rdma_create_qp()                     ibv_create_cq()
ibv_reg_mr()                         rdma_create_qp()
rdma_connect()                       ibv_reg_mr()
  → Slave 收到 CONNECT_REQUEST       rdma_accept()
  ← Slave 回复                       rdma_get_cm_event(ESTABLISHED)
rdma_get_cm_event(ESTABLISHED)
```

---

### Q7.3: PD、CQ、QP、MR 之间的关系

**考察维度**：RDMA 资源模型  
**难度**：⭐⭐⭐⭐

**问题**：PD（保护域）、CQ（完成队列）、QP（队列对）、MR（内存区域）四者之间的关系是什么？可以多个 QP 共享一个 PD 吗？

**答题要点**：
- **PD**：资源容器，QP 和 MR 都必须属于一个 PD
- **QP**：发送/接收队列对，连接的对端端点
- **CQ**：QP 完成的 WR 通知通道
- **MR**：注册的内存区域，包含地址、长度、rkey/lkey、访问权限
- 多个 QP 可以共享同一个 PD，同一个 PD 下的 MR 可以被所有 QP 使用
- 项目中全量 QP 和增量 QP 使用独立 PD（避免 MR 权限冲突）

---

### Q7.4: Pipeline 发送模式解决什么问题？

**考察维度**：性能优化  
**难度**：⭐⭐⭐⭐

**问题**：为什么需要 Pipeline 发送模式？单 send buffer 有什么问题？Pipeline 的 CQ 轮询为什么用事件驱动而非 busy poll？

**答题要点**：
- **单 send buffer 问题**：每次 `ibv_post_send()` 后必须等待 CQ completion → 流水线阻断
- **Pipeline 方案**：4 个 buffer 组成环形队列，post 后立即返回，CQ 线程后台回收
- **事件驱动 vs busy poll**：
  - busy poll 浪费 CPU（100% 单核）
  - `ibv_get_cq_event()` 阻塞等待，有 completion 时才唤醒
  - 配合 re-arm → drain 模式，确保不遗漏 completion

---

### Q7.5: CQ 轮询的 re-arm → drain 模式

**考察维度**：RDMA 编程陷阱  
**难度**：⭐⭐⭐⭐⭐

**问题**：项目文档中提到了 CQ notification 的"re-arm → drain"模式。为什么必须先 re-arm 再 poll？如果先 poll 再 re-arm 会有什么问题？

**答题要点**：
- 错误顺序：`poll()` 空 → `re-arm` → `wait()`，但 `poll()` 和 `re-arm` 之间可能有一个 completion 到达
- 这个 completion 不会触发第二次通知，导致 `wait()` 永远阻塞
- 正确顺序：`re-arm` → `poll()`（drain 确认 arm 和 poll 之间没有漏掉 completion）→ 如果 poll 到数据 → 继续循环
- 这是 RDMA 编程的常见陷阱

---

## 8. eBPF 与 BPF 编程

### Q8.1: BPF 程序的验证和执行流程

**考察维度**：BPF 原理  
**难度**：⭐⭐⭐⭐  
**相关代码**：`kprobe_kvs_repl_tcp_sendmsg()`

**问题**：BPF 程序从 C 代码到内核执行需要经过哪些步骤？BPF verifier 拒绝程序的原因通常有哪些？

**答题要点**：
```
C 源码 → clang -target bpf → BPF ELF 对象 (.bpf.o)
  → bpf_object__open() → 解析 ELF section
  → bpf_object__load() → verifier 检查 + JIT 编译
  → bpf_program__attach() → 挂载到 hook 点
```

- verifier 拒绝的常见原因：
  1. **栈超过 512B**——`entry[504]` 太大
  2. **未初始化变量**——所有路径必须保证所有变量已赋值
  3. **循环无法证明终止**——除非用 `#pragma unroll` 或 bounded loop
  4. **越界访问**——`bpf_map_lookup_elem` 返回值必须检查 NULL
  5. **R2 寄存器范围**——`bpf_probe_read_user(buf, size)` 的 size 必须有上界

---

### Q8.2: kprobe BPF 中为什么用 `bpf_probe_read_kernel` 又用 `bpf_probe_read_user`？

**考察维度**：内存空间 / BPF 编程  
**难度**：⭐⭐⭐⭐  
**相关代码**：`repl_kprobe.bpf.c` 中的 `read_msg_data()`

**问题**：BPF 程序中为什么既要读 kernel 空间又要读 user 空间？`tcp_sendmsg` 的参数 `msg` 在哪个地址空间？`msg->msg_iter->iov->iov_base` 呢？

**答题要点**：
- `msg` 指针本身是**内核空间**的（`tcp_sendmsg` 是内核函数，参数在内核栈上）
- `msg->msg_iter->iov`（iovec 数组指针）也是**内核空间**的
- `iov->iov_base` 指向的是**用户空间**的应用程序缓冲区
- 所以：`bpf_probe_read_kernel(msg+40)` 读 iov 指针，`bpf_probe_read_user(iov_base)` 读实际数据

---

### Q8.3: BPF MAP_TYPE_PERCPU_ARRAY 为什么能绕过 512B 栈限制？

**考察维度**：BPF 限制  
**难度**：⭐⭐⭐⭐

**问题**：BPF 栈只有 512 字节，如果每个 entry 需要 504 字节怎么办？PERCPU_ARRAY 是怎么工作的？

**答题要点**：
- BPF 栈空间在 BPF 虚拟机中固定为 512 字节——这是 verifier 强制限制
- `PERCPU_ARRAY` 是 BPF map 的一种，在 BPF map 空间中分配，不受栈限制
- 每个 CPU 核心有一份独立的 504B 副本，互不干扰
- BPF 程序通过 `bpf_map_lookup_elem(&kprobe_tmpbuf, &key)` 获取当前 CPU 的缓冲区指针
- 栈上只放一个 8 字节指针，而不是 504 字节数据

---

### Q8.4: eBPF sockmap 的 sk_msg hook 和 kprobe 有什么区别？

**考察维度**：BPF hook 点理解  
**难度**：⭐⭐⭐⭐

**问题**：eBPF sockmap 的 `sk_msg` hook 和 kprobe `tcp_sendmsg` hook 都是在数据发送路径上，它们的本质区别是什么？为什么需要两个？

**答题要点**：

| 维度 | sk_msg hook | kprobe/tcp_sendmsg |
|------|------------|-------------------|
| 钩子位置 | **BPF 专用 hook 点**，位于 sockmap 层 | **内核函数入口**，通用探针 |
| 权限 | 需要 `CAP_BPF` + `CAP_NET_ADMIN` | 需要 `CAP_BPF` + `CAP_SYS_ADMIN`（kprobe） |
| 对数据的影响 | **可以修改/重定向**（`bpf_msg_redirect_map()`） | **只读**（无法修改数据流） |
| 性能 | 低开销（专为此设计） | 略高（函数入口全量追踪） |
| 用途 | 数据面（修改数据流） | 观测面（监控、调试） |

- kvstore 中 eBPF sockmap 用于**实际数据转发**（改数据流）
- kprobe 用于**观测和监控**（统计命中率、字节数）

---

## 9. kprobe 与内核追踪

### Q9.1: kprobe 和 uprobe 的区别

**考察维度**：内核追踪  
**难度**：⭐⭐⭐

**问题**：BPF 程序也可以挂到 uprobe（用户态探针）上。为什么不拦截用户态的 `repl_broadcast()` 而选择拦截内核的 `tcp_sendmsg`？

**答题要点**：
- **uprobe 拦截 `repl_broadcast()`**：
  - 优点：直接获取广播数据，不需要从 iovec 中提取
  - 缺点：需要符号表、位置随编译变化、需要 `CAP_SYS_PTRACE`
- **kprobe 拦截 `tcp_sendmsg`**：
  - 优点：不依赖用户态函数符号，对所有 TCP 连接通用
  - 缺点：需要从 iovec 中读取数据，需要处理内核/用户指针空间
- 本项目选择 kprobe，是为了**透明拦截**——不需要修改 `repl_broadcast` 逻辑

---

### Q9.2: CO-RE 是什么？为什么需要手动定义 `struct pt_regs`？

**考察维度**：BPF 兼容性  
**难度**：⭐⭐⭐⭐

**问题**：项目中手动定义了 `struct pt_regs` 的字段名（`di/si/dx`）而不是系统头文件中的字段名（`rdi/rsi/rdx`）。为什么？CO-RE 如何解决不同内核版本的结构体布局差异？

**答题要点**：
- kernel 5.15 BTF 中的 `struct pt_regs` 字段名是 `di/si/dx`（短名），而非 `rdi/rsi/rdx`（x86 寄存器名）
- 如果 BPF 程序用 `rdi` 访问，CO-RE 重定位时会找不到 BTF 中对应的字段，返回 -4007 错误
- CO-RE (Compile Once, Run Everywhere) 原理：
  1. 编译时记录需要访问的字段信息（BTF relocation）
  2. 加载时根据目标内核的 BTF 重新计算字段偏移
  3. 支持不同内核版本间的兼容
- 本项目因 `iov_iter` 中 iov 在匿名 union 内，CO-RE 无法正确解析，最终使用了**固定偏移**

---

### Q9.3: `struct pt_regs` 为什么要自己定义而不用 `bpf_tracing.h`？

**考察维度**：BPF 编程实践  
**难度**：⭐⭐⭐

**问题**：为什么 BPF 程序不直接 `#include <bpf/bpf_tracing.h>` 用 `PT_REGS_PARM2(ctx)` 宏，而要自己定义 `struct pt_regs`？

**答题要点**：
- `bpf_tracing.h` 的 `PT_REGS_PARM2` 宏展开后会访问 `ctx->rsi`，但 kernel 5.15 BTF 中字段名叫 `si`
- CO-RE 加载时，BPF verifier 检查字段名是否匹配 BTF，不匹配导致加载失败（-4007）
- 手动定义 `struct pt_regs` 使用与 BTF 完全一致的字段名（`dx`, `si`），CO-RE 可以正确重定位
- 这是**内核 BTF 字段命名不一致**导致的兼容性问题，不是所有内核版本都有此问题

---

## 10. 并发与多线程

### Q10.1: `g_kprobe_running` 为什么用 `volatile` 修饰？

**考察维度**：线程间通信  
**难度**：⭐⭐

**问题**：多个线程（forward_thread、slave_poll_thread、cleanup）都要读写 `g_kprobe_running`，为什么只用 `volatile` 不用锁或原子操作？

**答题要点**：
- `volatile` 保证每次读/写都从内存操作，不会被编译器优化掉
- 该变量只有**一个写者**（cleanup 或主线程调用 `g_kprobe_running = 0`）
- 其他线程只读取——不存在"写-写"竞争
- 对于"停止标志"这种场景，`volatile` 足够，不需要原子操作
- 如果多个线程同时写，则需要原子操作或锁

---

### Q10.2: wr_slot_acquire 中的总线锁问题

**考察维度**：并发设计  
**难度**：⭐⭐⭐⭐  
**相关代码**：`wr_slot_acquire()` 中的 `ibv_poll_cq()`

**问题**：`wr_slot_acquire` 在空闲 slot 不足时，会对 CQ 加锁并调用 `ibv_poll_cq()`。如果此时 CQ 轮询线程也在运行，会发生什么？项目中怎么解决的？

**答题要点**：
- CQ 轮询线程运行时，`cq_poll_thread_running == 1`，其他线程**不直接轮询 CQ**
- 代码检查 `g_rdma_kprobe.cq` 不为空才 poll——但增量 QP 没有独立 CQ 轮询线程，所有 completion 由 `wr_slot_acquire` 同步处理
- 全量同步的 CQ 轮询线程和 `acquire_send_slot` 之间通过 `cq_poll_thread_running` 标志隔离
- 这是 RDMA 编程的常见陷阱——**不要多个线程轮询同一个 CQ**

---

## 11. 设计决策与架构

### Q11.1: 为什么不用 Redis 而要从零写一个 kvstore？

**考察维度**：学习动机 / 架构权衡  
**难度**：⭐⭐

**问题**：Redis 已经非常成熟，为什么还要从零实现 kvstore？从这个项目中学到了什么 Redis 源码中没有的东西？

**答题要点**：
- Redis 源码庞大（~10 万行），修改和理解困难
- 从零实现可以完整经历：设计 → 编码 → 调试 → 优化的全过程
- 这个项目特有的学习点：
  - RDMA 编程（SEND/RECV vs WRITE）
  - eBPF/kprobe BPF 编程（CO-RE 陷阱、verifier 限制）
  - 自研 slab 分配器（与 jemalloc 对比）
  - io_uring 异步文件 I/O

---

### Q11.2: kvstore 可以做生产使用吗？有什么风险？

**考察维度**：工程判断  
**难度**：⭐⭐⭐

**问题**：从你对该项目的了解，它能否用于生产环境？如果不行，最大的风险在哪？

**答题要点**：
- 不适合生产环境，主要风险：
  1. **存储引擎**：Hash 表固定 1024 桶，大数据量冲突严重
  2. **持久化**：没有 AOF 校验和、truncated 恢复容错
  3. **复制**：没有认证机制、加密传输、脑裂保护
  4. **内存安全**：`strdup` 分配不释放、`kvs_free` 和 `free` 混用
  5. **单机单线程**：没有多核心利用（Reactor 单线程处理所有连接）
  6. **测试覆盖**：单元测试基本缺失
- 项目定位是**学习研究**，非生产

---

### Q11.3: 为什么同时实现四种传输层？

**考察维度**：架构演进  
**难度**：⭐⭐⭐

**问题**：从 TCP → RDMA SEND/RECV → eBPF sockmap → kprobe+RDMA WRITE，这四种传输层实现的演进思路是什么？每增加一种带来了什么新能力？

**答题要点**：

| 阶段 | 新增能力 | 代价 |
|------|---------|------|
| TCP | 基础复制 | 延迟高、CPU 开销大 |
| RDMA SEND/RECV | 全量零拷贝 | 需要专用硬件 |
| eBPF sockmap | 内核态转发 | 依赖内核版本、BPF 特性 |
| kprobe+RDMA WRITE | 单边写入，Slave CPU 零参与 | 最高复杂度 |

- 四种传输层通过 `repl_transport_ops_t` 接口实现**策略模式**——新增传输层不需要修改上层逻辑

---

## 12. 调试与问题排查

### Q12.1: BPF 程序加载失败，如何调试？

**考察维度**：排错能力  
**难度**：⭐⭐⭐⭐

**问题**：`bpf_object__load()` 返回 -4007（CO-RE 错误），应该怎么定位问题？

**答题要点**：
1. 查看内核日志：`dmesg | tail` —— BPF verifier 会打印拒绝原因
2. 启用 libbpf 日志：`libbpf_set_print(printf)` —— 查看 CO-RE 重定位细节
3. 使用 `bpftool`：`bpftool prog list`、`bpftool map dump`
4. 检查 BTF：`bpftool btf dump id <btf_id>` —— 查看 struct 字段名
5. `-4007` 具体含义：`LIBBPF_ERRNO__RELOC`，CO-RE 重定位失败，通常是被访问的 struct 字段在 BTF 中不存在或名称不匹配

---

### Q12.2: kprobe 拦截到了数据但 Slave 收不到，怎么排查？

**考察维度**：分布式排错  
**难度**：⭐⭐⭐⭐⭐

**问题**：BPF kprobe 命中了（`kprobe_hits > 0`），ringbuf 也有数据，但 Slave 侧没有收到数据。可能的原因有哪些？怎么逐一排查？

**答题要点**：
```
排查路径：
1. 检查 Master 日志：是否有 "MR not ready, skip"？→ MR 未交换完成
2. 检查 g_slave_mr.rkey 是否为 0 → MR 信息没收到
3. 检查 KPROBEMR/+KPROBERDMA 是否正常交换 → TCP 控制通道问题
4. 检查 wr_submit_data rc=0 → RDMA WRITE 提交是否成功
5. 检查 Slave 日志：head/tail 变化情况
6. 检查 Slave MR 内存：直接读 MR 地址，验证数据是否被 DMA 写入
7. 检查 conntrack/iptables：是否有防火墙拦截
```

---

### Q12.3: `test_repl_5w5w` 通过但怀疑 RDMA 路径失效，如何验证？

**考察维度**：验证思维  
**难度**：⭐⭐⭐⭐

**问题**：假设你怀疑 kprobe+RDMA WRITE 路径实际并没有工作，数据全走 TCP 过来的，有什么手段能**实锤**？

**答题要点**：
1. **最直接**：临时封掉 TCP 端口（iptables DROP），看增量数据是否还能复制
2. **统计对比**：Master 侧 `g_rdma_writes` 是否为 0
3. **日志分析**：`grep wr_submit_data /tmp/master.log | wc -l`
4. **BPF 统计**：`bpftool map dump name kprobe_stats` 查看命中数
5. **代码注入**：在数据中嵌入路径标记（如特定前缀），Slave 侧区分统计
6. **perf 观测**：Master 侧 `perf top` 查看 kprobe 开销是否存在

---

## 13. 进阶挑战题

### Q13.1: 如何实现 RDMA WRITE 的可靠性确认？

**考察维度**：系统设计  
**难度**：⭐⭐⭐⭐⭐

**问题**：RDMA WRITE 是"发后不管"的单边操作，Master 无法知道 Slave 是否真的消费了数据。如果 Slave 死机后恢复，Master 怎么知道该从哪里重传？

**答题要点**：
- RDMA WRITE 没有 ACK 机制，需要应用层自己确认
- 方案一：Slave 定期通过 TCP 通道发送 `REPLACK <consumed_offset>` 给 Master
- 方案二：Master 定期 RDMA READ `consumer_tail`，确认 Slave 消费进度
- 方案三：混合方案——Slave TCP 通知为主，Master RDMA READ 作为保底
- 目前项目依赖 TCP 保底路径做可靠性保证，RDMA 路径不负责可靠送达

---

### Q13.2: 如果 ringbuf 满了怎么办？

**考察维度**：背压设计  
**难度**：⭐⭐⭐⭐

**问题**：BPF ringbuf（1MB）如果瞬间写满了（`bpf_ringbuf_output` 返回 -ENOSPC），当前代码会丢失数据。怎么设计一个背压机制来防止丢失？

**答题要点**：
- 当前行为：`bpf_ringbuf_output` 失败 → 更新 `rb_err` 统计 → 丢弃
- 改进方案：
  1. **增大 ringbuf**：`max_entries` 改为 2MB 或更大
  2. **BPF 侧降速**：ringbuf 满时在 BPF 中设置一个标志，用户态消费完后清除
  3. **用户态背压**：`kprobe_ringbuf_cb` 返回 -1 告诉 libbpf "暂停通知"
  4. **阻塞 TCP send**：如果 ringbuf 满，`repl_broadcast` 等待 ringbuf 有空闲再执行
  5. **选择性采样**：ringbuf 满时跳过非关键数据，保证关键数据不丢

---

### Q13.3: 如何实现连接池？

**考察维度**：性能优化  
**难度**：⭐⭐⭐⭐

**问题**：每个客户端连接都独立分配 `conn_t` 结构体（含 64KB inbuf），频繁的连接建立/释放会造成大量内存分配。怎么优化？

**答题要点**：
- 对象池：预分配 N 个 `conn_t`，空闲链表管理
- `conn_t` 中的 inbuf 可以复用：连接关闭时，inbuf 不清空归还池子
- epoll 的 event.data.ptr 存 conn_t 指针，避免 fd → conn_t 的映射查找

---

### Q13.4: 事件循环中执行 `kvs_active_expire_cycle` 对延迟的影响

**考察维度**：实时系统  
**难度**：⭐⭐⭐⭐

**问题**：事件循环中同时处理网络 I/O 和 TTL 过期扫描，如果过期扫描耗时过长会阻塞新的网络请求。怎么解决？

**答题要点**：
- 当前方案：`kvs_active_expire_cycle(budget=20)`——每次最多扫描 20 个 key
- `budget` 控制每次的 CPU 时间，避免单次扫描过长
- 如果 20 个 key 的扫描时间仍太长（比如删除操作涉及大内存释放）：
  1. 进一步减小 budget
  2. 延迟删除（标记为过期，由后台线程异步释放内存）
  3. 将过期扫描移到独立线程

---

### Q13.5: 怎么支持引擎级别的范围查询？

**考察维度**：功能扩展  
**难度**：⭐⭐⭐⭐

**问题**：RBTREE 和 Skiptable 引擎支持有序存储，但当前没有实现 `RANGE` / `RGET` 命令。如果要实现按范围查询（如 `RGET key1..key10`），需要注意什么？

**答题要点**：
- 遍历有序链表/树的节点，收集范围内的 key-value
- 返回格式：RESP 数组（`*n\r\n$len\r\nkey\r\n$len\r\nvalue\r\n...`）
- 注意：
  1. 遍历期间是否允许其他线程修改？需要读锁或快照
  2. 如果范围过大（100 万个 key），需要分页或限制
  3. 需要正序/逆序遍历支持

---

### Q13.6: Memcached vs Redis vs kvstore

**考察维度**：系统对比  
**难度**：⭐⭐⭐

**问题**：kvstore 和 Redis 的主要区别是什么？如果让你在 Redis 和 kvstore 之间选择部署，你的考虑因素是什么？

**答题要点**：

| 维度 | Redis | kvstore |
|------|-------|---------|
| 成熟度 | 生产级，10年+ | 学习项目 |
| 数据结构 | 丰富（list/set/zset/stream/hyperloglog） | 基础（hash/rbtree/skiptable/doc） |
| 持久化 | RDB + AOF（校验和、容错） | Dump + AOF（无校验） |
| 集群 | Cluster（分片+复制+故障转移） | Sentinel（主从切换） |
| 内存管理 | jemalloc（深度优化） | libc/jemalloc/custom |
| 传输层 | TCP + TLS | TCP + RDMA + eBPF + kprobe |

- 选型因素：kvstore 适合 RDMA 环境、需要定制内核转发（eBPF）的场景
- Redis 适合大多数需要稳定可靠的标准场景

---

### Q13.7: BPF 程序的一次 bug 修复

**考察维度**：调试能力  
**难度**：⭐⭐⭐⭐⭐

**问题**：项目中遇到了一个 BPF verifier 错误：`R2 min value is negative`，这个错误的含义是什么？怎么解决的？

**答题要点**：
- **含义**：`bpf_probe_read_user(buf, size, ptr)` 的第二个参数 `size` 可能为负数
- verifier 要求所有 BPF helper 的参数必须在调用点有确定的**非负值**
- 问题代码：`bpf_probe_read_user(buf, (__u32)safe_len, ptr)`，`safe_len` 来自 `vec.l`
- 修复方法：
  ```c
  if (safe_len > 500) safe_len = 500;  // 加显式上界
  if (safe_len < 0) return 0;          // 加下界检查
  // 此时 verifier 知道 safe_len ∈ [0, 500]，是合法值
  ```

---

### Q13.8: 如果让你加一个新功能，你会加什么？怎么实现？

**考察维度**：工程视野  
**难度**：⭐⭐⭐⭐

**问题**：如果你是 kvstore 的下一个开发者，你会优先实现什么功能？简述实现思路。

**答题要点**（开放题，示例答案）：
- **功能**：`RANGE` 命令（有序引擎的范围查询）
- 实现思路：
  1. 在 `handle_parsed_command` 中新增 `RGET key start end` 解析
  2. 在 skiptable 中实现范围遍历——找到 start 节点后沿 forward[0] 链表遍历
  3. 返回 RESP 数组格式
- **功能**：多核支持（分片）
- 实现思路：
  1. 将 Hash 表拆为 N 个分片
  2. 每个分片一个锁（或一个线程）
  3. key 按 hash(slot) % N 路由到分片
- **功能**：AOF 校验和
- 实现思路：每次 fsync 前，计算前 N 字节的 crc64，写入 AOF 末尾

---

## 附录：按知识点分类速查

| 知识点 | 相关题目 |
|--------|---------|
| C 语言 | Q1.1–Q1.4 |
| 网络 I/O | Q2.1–Q2.3 |
| 数据结构 | Q3.1–Q3.4 |
| 文件 I/O | Q4.1–Q4.4 |
| 内存分配 | Q5.1–Q5.2 |
| 分布式复制 | Q6.1–Q6.4 |
| RDMA | Q7.1–Q7.5 |
| eBPF/BPF | Q8.1–Q8.4, Q9.1–Q9.3 |
| 并发编程 | Q10.1–Q10.2 |
| 系统设计 | Q11.1–Q11.3 |
| 调试排查 | Q12.1–Q12.3 |
| 进阶 | Q13.1–Q13.8 |
