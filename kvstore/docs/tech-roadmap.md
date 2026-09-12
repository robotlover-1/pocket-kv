# kvstore 技术路线与实现详解

> **本文档目标**：从技术选型到代码实现，逐层拆解 kvstore 的每个核心模块，帮助开发者理解"为什么选这个技术"和"这个技术怎么用代码实现"。

---

## 目录

1. [总览：技术栈与架构](#1-总览技术栈与架构)
2. [网络模型层：三种 I/O 模型](#2-网络模型层三种-io-模型)
3. [存储引擎层：五种数据结构](#3-存储引擎层五种数据结构)
4. [内存管理层：三种分配器](#4-内存管理层三种分配器)
5. [持久化层：全量 + 增量](#5-持久化层全量--增量)
6. [复制层：TCP / RDMA / eBPF](#6-复制层tcp--rdma--ebpf)
7. [TTL 过期系统](#7-ttl-过期系统)
8. [配置与命令行系统](#8-配置与命令行系统)
9. [协议层：RESP 编解码](#9-协议层resp-编解码)
10. [测试体系](#10-测试体系)

---

## 1. 总览：技术栈与架构

### 1.1 技术栈一览

| 层次 | 技术选型 | 核心文件 |
|------|----------|----------|
| **语言** | C99 | 全项目 |
| **网络 I/O 模型** | epoll / io_uring / 协程 | `src/core/reactor.c` / `proactor.c` / `ntyco.c` |
| **传输协议** | TCP + RDMA RC + eBPF sockmap | `src/replication/kvs_repl.c` |
| **序列化协议** | RESP (Redis Serialization Protocol) | `src/main/kvstore.c` |
| **持久化** | KVSD 二进制 + AOF (RESP) | `src/persistence/kvs_persist.c` |
| **存储引擎** | 数组 / 哈希 / 红黑树 / 跳表 | `src/storage/` |
| **内存管理** | libc / jemalloc / 自研 slab+mmap | `src/memory/kvs_mem.c` |
| **协程库** | NtyCo (submodule) | `NtyCo/core/` |
| **异步 I/O** | liburing (submodule) | `liburing/` |
| **eBPF** | libbpf + clang-embedded BPF | `src/replication/bpf/repl_sockmap.bpf.c` |
| **RDMA** | librdmacm + libibverbs | `src/replication/kvs_repl.c` (条件编译) |
| **构建系统** | GNU Make | `Makefile` |
| **测试** | Python + bash + nc | `tools/` + `tests/` |

### 1.2 架构交互图

```mermaid
flowchart TB
    Client[客户端: redis-cli / nc / SDK]

    subgraph Network["网络模型层 (src/core/)"]
        Reactor["Reactor (epoll)\nreactor.c"]
        Proactor["Proactor (io_uring)\nproactor.c"]
        NtyCo["NtyCo 协程\nntyco.c"]
    end

    subgraph Protocol["协议层"]
        RESP["RESP 解析与编码\nkvstore.c 通用函数"]
    end

    subgraph Cmd["命令分发"]
        CMD["handle_parsed_command()\nkvstore.c"]
    end

    subgraph Storage["存储引擎层 (src/storage/)"]
        Array["kvs_array.c\n数组 O(n)"]
        Hash["kvs_hash.c\n哈希表 FNV-1a"]
        RBTree["kvs_rbtree.c\n红黑树 O(log n)"]
        SkipTable["kvs_skiptable.c\n跳表 O(log n)"]
        Doc["kvs_doc.c\n文档型"]
    end

    subgraph Memory["内存管理层 (src/memory/)"]
        Libc["libc\nmalloc/free"]
        Jemalloc["jemalloc\nLD_PRELOAD"]
        Custom["custom\nslab+mmap"]
    end

    subgraph Expire["过期系统 (src/expire/)"]
        ExpireEngine["kvs_expire.c\n哈希+最小堆"]
    end

    subgraph Persistence["持久化层 (src/persistence/)"]
        Dump["dump (KVSD)\nkvs_persist.c"]
        AOF["AOF (RESP)\nkvs_persist.c"]
    end

    subgraph Replication["复制层 (src/replication/)"]
        ReplCore["kvs_repl.c\n复制协议引擎"]
        ReplEBPF["kvs_repl_ebpf.c\neBPF sockmap"]
        BPFObj["bpf/repl_sockmap.bpf.c\nBPF 程序"]
        Sentinel["kvs_sentinel.c\n哨兵"]
    end

    Client --> Network
    Network --> RESP
    RESP --> CMD

    CMD --> Storage
    CMD --> Expire
    CMD --> Persistence
    CMD --> Replication

    Storage --> Memory

    ReplCore --> ReplEBPF
    ReplEBPF --> BPFObj
```

---

## 2. 网络模型层：三种 I/O 模型

### 2.1 技术选型分析

kvstore 实现了三种网络 I/O 模型，通过配置 `--net reactor|proactor|ntyco` 切换：

| 模型 | 底层机制 | 并发模型 | 适用场景 | 代码量 |
|------|----------|----------|----------|--------|
| **Reactor** | epoll (水平触发) | 单线程事件循环 + 非阻塞 I/O | 通用 I/O 密集型 | ~200 行 |
| **Proactor** | io_uring | 单线程异步提交 + 完成轮询 | 高并发、大量磁盘 I/O | ~300 行 |
| **NtyCo** | 协程 (hook 阻塞调用) | 协程内同步编程、底层 epoll | 海量连接、简化编程 | ~100 行 |

**为什么选三种？** 不是为了生产冗余，而是为了**对比学习**：同一套业务逻辑用三种不同的 I/O 模型实现，便于理解每种模型的本质差异。

### 2.2 Reactor 模型 (epoll)

**核心思想**：事件驱动，非阻塞 I/O。把所有 I/O 操作注册到 epoll 上，当 I/O 事件就绪时回调处理。

**实现流程**：

```mermaid
flowchart LR
    A["epoll_create()\n创建 epoll 实例"] --> B["epoll_ctl(ADD)\n注册监听 socket"]
    B --> C["epoll_wait()\n等待事件"]
    C --> D{事件类型}

    D -->|"EPOLLIN"| E["on_read()\nrecv → parse_resp_stream"]
    D -->|"EPOLLOUT"| F["send queued data"]
    D -->|"新连接"| G["on_accept()\naccept → 注册到 epoll"]

    E --> C
    F --> C
    G --> C
```

**核心代码** (`src/core/reactor.c`)：

```c
int reactor_start(void) {
    // Step 1: 创建监听 socket
    int lfd = create_listener(port);

    // Step 2: 创建 epoll 实例
    g_epfd = epoll_create(1024);

    // Step 3: 注册监听 socket
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = lfd;
    epoll_ctl(g_epfd, EPOLL_CTL_ADD, lfd, &ev);

    // Step 4: 事件循环
    struct epoll_event events[MAX_EVENTS];
    while (1) {
        int n = epoll_wait(g_epfd, events, MAX_EVENTS, 100);  // 100ms 超时

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            conn_t *c = NULL;

            if (fd == lfd) {
                conn_t lc;
                lc.fd = lfd;
                on_accept(&lc);          // 新连接 → accept
            } else if (events[i].events & EPOLLIN) {
                c = fdmap[fd];
                if (c) on_read(c);       // 可读 → recv + 解析
            } else if (events[i].events & EPOLLOUT) {
                c = fdmap[fd];
                if (c) on_write(c);      // 可写 → send 队列数据
            }
        }

        // 周期性任务：过期检测、自动快照、后台持久化
        kvs_active_expire_cycle(expire_cycle_budget());
        persist_autosnap_cron();
        persist_bgsave_poll();
        persist_bgrewriteaof_poll();
    }
}
```

**数据输出 (queue_bytes)**：写操作将数据放入 `out_node_t` 链表，然后通过 `epoll_ctl` 注册 `EPOLLOUT` 事件。当 epoll 通知可写时，`on_write` 逐节点发送。

```c
int queue_bytes(conn_t *c, const unsigned char *buf, size_t len) {
    out_node_t *n = kvs_malloc(sizeof(*n));
    n->data = kvs_malloc(len);
    memcpy(n->data, buf, len);
    n->len = len;
    n->sent = 0;
    n->next = NULL;

    // 追加到链表尾部
    if (c->out_tail) c->out_tail->next = n;
    else c->out_head = n;
    c->out_tail = n;

    // 注册 EPOLLOUT 事件
    mod_events(c, EPOLLIN | EPOLLOUT);
    return 0;
}
```

### 2.3 Proactor 模型 (io_uring)

**核心思想**：异步 I/O，提交 SQE (Submission Queue Entry) 后立即返回，完成时从 CQ (Completion Queue) 取结果。避免了 epoll 的**就绪通知 → 应用层阻塞读写**的两步开销。

**实现流程**：

```mermaid
flowchart TB
    subgraph SQ["SQ (Submission Queue)"]
        SQE1["SQE: accept"]
        SQE2["SQE: recv"]
        SQE3["SQE: send"]
    end

    subgraph Kernel["内核"]
        IO_URING["io_uring 引擎"]
    end

    subgraph CQ["CQ (Completion Queue)"]
        CQE1["CQE: accept done"]
        CQE2["CQE: recv done"]
        CQE3["CQE: send done"]
    end

    SQ -->|"io_uring_submit()"| IO_URING
    IO_URING -->|"完成事件"| CQ
    CQ -->|"io_uring_cqes()"| Process["处理完成事件"]
    Process -->|"提交新的 SQE"| SQ
```

**核心代码** (`src/core/proactor.c`)：

```c
int proactor_start(unsigned short port) {
    // Step 1: 创建监听 socket
    int lfd = create_listener(port);

    // Step 2: 初始化 io_uring
    struct io_uring ring;
    io_uring_queue_init(KVS_URING_ENTRIES, &ring, 0);  // 1024 个 SQE

    // Step 3: 提交初始 accept SQE
    submit_accept(&ring, lfd);

    // Step 4: 异步事件循环
    while (1) {
        // 提交所有待处理的 SQE
        io_uring_submit(&ring);

        // 等待 CQE (完成事件)
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);

        // 处理完成事件
        uring_req_t *req = (uring_req_t *)cqe->user_data;
        int res = cqe->res;  // 操作结果

        switch (req->event) {
        case KVS_EVENT_ACCEPT:          // accept 完成
            conn_t *c = create_conn(res);
            submit_read(&ring, c);       // 提交 recv SQE
            submit_accept(&ring, lfd);   // 再次提交 accept SQE
            break;
        case KVS_EVENT_READ:            // recv 完成
            req->conn->in_len += res;
            parse_resp_stream(req->conn, ...);
            if (有数据要发送) submit_write(&ring, c);
            else submit_read(&ring, c);  // 继续读取
            break;
        case KVS_EVENT_WRITE:           // send 完成
            // 更新已发送偏移量
            // 如果还有更多数据，继续提交 send SQE
            break;
        }

        io_uring_cqe_seen(&ring, cqe);  // 标记 CQE 已消费
    }
}
```

### 2.4 NtyCo 协程模型

**核心思想**：协程内用同步编程风格（`recv` 阻塞），底层 NtyCo 自动将阻塞操作 hook 为异步。使得代码逻辑是顺序的，但底层 I/O 是并发的。

**为什么选 NtyCo？** 它是一个轻量级 C 协程库，通过修改 `errno` 和 hook socket 系统调用实现"同步写法、异步执行"。相比于 libtask、libmill，NtyCo 的设计更简洁，适合学习。

**核心代码** (`src/core/ntyco.c`)：

```c
void ntyco_server(conn_t *c) {
    // 协程内的代码是同步的，但底层不会阻塞
    while (1) {
        // recv 在协程内看起来是阻塞的
        // 但 NtyCo 自动 hook 了 recv → epoll 注册 → 协程 yield
        // 当数据到达时 → 协程 resume → recv 返回
        ssize_t n = recv(c->fd, c->inbuf + c->in_len,
                         sizeof(c->inbuf) - c->in_len, 0);
        if (n <= 0) break;

        c->in_len += (size_t)n;

        // 解析并处理命令
        parse_resp_stream(c, c->inbuf, &c->in_len, 0);

        // 发送响应（同步 flush）
        flush_output_blocking(c);
    }
    close_conn_nty(c);
}
```

**NtyCo hook 机制**：`recv` → `ntco_recv` → `epoll_ctl(fd, EPOLLIN)` → `nty_coroutine_yield()` → 协程挂起 → epoll_wait → 数据到达 → `nty_coroutine_resume()` → 继续执行。

---

## 3. 存储引擎层：五种数据结构

### 3.1 技术选型分析

| 引擎 | 数据结构 | 时间复杂度 | 特性 | 命令前缀 |
|------|----------|------------|------|----------|
| **Array** | 动态数组 + 线性查找 | O(n) | 最简单，适合小数据量 (< 1024) | 无前缀 |
| **Hash** | 链地址哈希表 (FNV-1a) | O(1) avg | 通用场景，大量 key | `H` |
| **RBTREE** | 红黑树 | O(log n) | 有序存储，范围查询 | `R` |
| **Skiptable** | 跳表 (概率平衡) | O(log n) avg | 有序存储，实现简单 | `X` |
| **Doc** | 哈希表 + 字段哈希 | O(1) | 文档型 value (平铺字段) | `DOC*` |

**为什么同时实现红黑树和跳表？** 两者都是有序数据结构但实现思路不同。红黑树通过旋转保持平衡，跳表通过概率层数实现平衡。对比实现有助于理解数据结构的本质。

### 3.2 Hash 引擎 (`src/storage/kvs_hash.c`)

**核心数据结构**：

```c
typedef struct hashnode_s {
    char *key;            // key (动态分配)
    char *value;          // value (动态分配)
    struct hashnode_s *next;  // 链地址法解决冲突
} hashnode_t;

typedef struct hashtable_s {
    hashnode_t **nodes;   // 哈希桶数组 (长度 MAX_TABLE_SIZE=1024)
    int max_slots;
    int count;
} hashtable_t;
```

**哈希函数**：FNV-1a (Fowler–Noll–Vo) 非加密哈希，速度快、分布均匀。

```c
static int _hash(char *key, int size) {
    unsigned int sum = 2166136261u;  // FNV offset basis
    for (int i = 0; key[i] != 0; ++i) {
        sum ^= (unsigned char)key[i];
        sum *= 16777619u;            // FNV prime
    }
    return (int)(sum % (unsigned int)size);
}
```

**SET 操作**：哈希 → 查找冲突链表 → 头插法插入新节点。

```c
int kvs_hash_set(kvs_hash_t *hash, char *key, char *value) {
    int idx = _hash(key, hash->max_slots);

    // 检查是否已存在
    for (hashnode_t *node = hash->nodes[idx]; node; node = node->next) {
        if (strcmp(node->key, key) == 0) return 1;  // 已存在
    }

    // 创建新节点，头插法
    hashnode_t *new_node = _create_node(key, value);
    new_node->next = hash->nodes[idx];
    hash->nodes[idx] = new_node;
    hash->count++;
    return 0;
}
```

### 3.3 RBTREE 引擎 (`src/storage/kvs_rbtree.c`)

**核心数据结构**：

```c
typedef struct _rbtree_node {
    unsigned char color;    // RED=1 / BLACK=2
    struct _rbtree_node *right, *left, *parent;
    KEY_TYPE key;           // char* (ENABLE_KEY_CHAR=1)
    void *value;            // void* 泛型指针
} rbtree_node;
```

**插入与平衡**：标准的红黑树左旋/右旋 + 颜色翻转，哨兵 `nil` 节点简化边界处理。

```c
// 插入修正：检查父子节点颜色冲突，通过旋转+变色恢复红黑树性质
static void rbtree_insert_fixup(rbtree *T, rbtree_node *z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            rbtree_node *y = z->parent->parent->right;  // 叔节点
            if (y->color == RED) {
                // Case 1: 叔节点为红色 → 父+叔变黑，爷变红
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;  // 向上继续检查
            } else {
                if (z == z->parent->right) {
                    // Case 2: z 是右孩子 → 左旋父节点
                    z = z->parent;
                    left_rotate(T, z);
                }
                // Case 3: z 是左孩子 → 右旋爷节点
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                right_rotate(T, z->parent->parent);
            }
        } else {
            // 对称情况：父节点是右孩子
            // ... 同理，方向相反
        }
    }
    T->root->color = BLACK;  // 根节点必须为黑
}
```

### 3.4 Skiptable 引擎 (`src/storage/kvs_skiptable.c`)

**核心数据结构**：多层链表，每层是前一层的一个"快车道"。

```
Level 3:  head ──────────────────────────────→ tail
Level 2:  head ──────────→ [30] ─────────────→ tail
Level 1:  head ──→ [10] ──→ [30] ──→ [50] ──→ tail
Level 0:  head → [5] → [10] → [20] → [30] → [40] → [50] → tail
```

```c
#define KVS_SKIPLIST_MAX_LEVEL 12
#define KVS_SKIPLIST_P 0.5   // 层数递增概率

typedef struct kvs_skipnode_s {
    char *key;
    char *value;
    int level;                       // 该节点的层数
    struct kvs_skipnode_s **forward; // 各层的下一个节点指针
} kvs_skipnode_t;
```

**层数生成 (概率算法)**：每次插入时随机决定节点层数，期望层数 = 1/(1-P) = 2。

```c
static int random_level(void) {
    int level = 0;
    while ((((double)rand()) / (RAND_MAX + 1.0)) < KVS_SKIPLIST_P
           && level < KVS_SKIPLIST_MAX_LEVEL) {
        ++level;
    }
    return level;
}
```

**搜索 + 插入**：从最高层开始，每层向前直到 next->key >= 目标，然后降一层继续。记录每层的"前驱节点"。

```c
int kvs_skiptable_set(kvs_skiptable_t *inst, char *key, char *value) {
    kvs_skipnode_t *update[KVS_SKIPLIST_MAX_LEVEL + 1];
    kvs_skipnode_t *cur = inst->header;

    // Step 1: 从最高层向下搜索，记录每层的前驱
    for (int i = inst->level; i >= 0; --i) {
        while (cur->forward[i] && strcmp(cur->forward[i]->key, key) < 0)
            cur = cur->forward[i];
        update[i] = cur;
    }

    cur = cur->forward[0];
    if (cur && strcmp(cur->key, key) == 0) {
        // key 已存在 → 更新 value
        kvs_free(cur->value);
        cur->value = strdup(value);
        return 0;
    }

    // Step 2: 生成随机层数
    int level = random_level();
    if (level > inst->level) {
        for (int i = inst->level + 1; i <= level; ++i)
            update[i] = inst->header;
        inst->level = level;
    }

    // Step 3: 创建新节点，逐层插入
    kvs_skipnode_t *node = skipnode_create(level, key, value);
    for (int i = 0; i <= level; ++i) {
        node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = node;
    }
    inst->count++;
    return 0;
}
```

### 3.5 Doc 引擎 (`src/storage/kvs_doc.c`)

**设计意图**：不把文档型 value 做在单个 value 字段里，而是作为独立的一层存储，与 Key-Value 引擎**共存**。这样持久化、复制链路都可以独立对齐。

```c
typedef struct kvs_doc_field_s {
    char *name;                    // 字段名
    char *value;                   // 字段值
    struct kvs_doc_field_s *next;  // 链表（链地址法）
} kvs_doc_field_t;

typedef struct kvs_doc_s {
    char *key;                           // 文档 key
    kvs_doc_field_t **fields;            // 字段哈希桶
    int field_count;
    int bucket_count;                    // KVS_DOC_FIELD_BUCKETS=16
    struct kvs_doc_s *next;              // 文档哈希链表
} kvs_doc_t;

typedef struct kvs_doc_table_s {
    kvs_doc_t **buckets;   // 文档哈希桶 (KVS_DOC_BUCKETS=1024)
    int size;
    int count;
} kvs_doc_table_t;
```

**支持的命令**：`DOCSET key field value` / `DOCGET key field` / `DOCDEL key field` / `DOCDROP key` / `DOCEXIST key` / `DOCCOUNT key` / `DOCGETALL key`。

---

## 4. 内存管理层：三种分配器

### 4.1 技术选型分析

| 后端 | 实现方式 | 特性 | 适用场景 |
|------|----------|------|----------|
| **libc** | `malloc()` / `free()` | 通用，无额外依赖 | 开发调试 |
| **jemalloc** | `LD_PRELOAD` 动态加载 | 高性能，碎片少 | 生产级负载 |
| **custom** | slab 分配器 + mmap 大块 | 可观测，有统计信息 | 研究学习 |

### 4.2 Custom 分配器设计 (`src/memory/kvs_mem.c`)

**Slab 分类**：将 ≤1024 字节的请求分为 8 个类别：

```c
#define SMALL_CLASS_COUNT 8
#define SMALL_MAX_SIZE 1024

small_class_t classes[SMALL_CLASS_COUNT] = {
    {32, ...},  {64, ...},  {128, ...}, {256, ...},
    {384, ...}, {512, ...}, {768, ...}, {1024, ...},
};
```

**分配路径**：

```mermaid
flowchart TD
    Alloc["kvs_malloc(size)"] --> Class{"size ≤ 1024?"}

    Class -->|"Yes: small"| Slab["查找对应的 class"]
    Slab --> FreeList{"free_list 有空闲?"}
    FreeList -->|"Yes"| Pop["弹出 free_list 头节点 → 返回"]
    FreeList -->|"No"| NewPage["mmap 新页 (4MB)\n切分为该 class 大小的 chunk\n全部加入 free_list"]
    NewPage --> Pop

    Class -->|"No: large"| Large["mmap(size + header)\n记录 large_hdr_t → 返回"]
```

**元数据头**：每个分配块前有一个头部记录大小和校验，用于统计和调试：

```c
typedef struct large_hdr_s {
    uint32_t magic;         // 0xC0DEC0DF - 用于校验
    uint32_t reserved;
    size_t request_size;    // 用户请求大小
    size_t mapping_size;    // 实际 mmap 大小
} large_hdr_t;
```

**可观测性**：通过 `INFO` 命令暴露统计信息：

```c
fprintf(info, "mem_backend:custom\n"
    "custom_small_alloc_calls:%llu\n"
    "custom_large_alloc_calls:%llu\n"
    "custom_current_requested_bytes:%llu\n"
    "custom_current_allocated_bytes:%llu\n"
    "custom_peak_small_inuse:%llu\n"
    "custom_peak_large_inuse_bytes:%llu\n"
    ...);
```

---

## 5. 持久化层：全量 + 增量

### 5.1 持久化策略

```mermaid
flowchart LR
    subgraph Runtime["运行时"]
        CMD["写命令\nSET/DEL/EXPIRE/..."]
        AOF_WRITE["写入 AOF 文件\nio_uring 优先 / 回退 pwrite"]
        DUMP_SCHED["定时 / 手动触发 SAVE/BGSAVE"]
    end

    subgraph Files["磁盘文件"]
        AOF_FILE["kvstore.aof\nRESP 命令流"]
        DUMP_FILE["kvstore.dump\nKVSD 二进制格式"]
    end

    subgraph Recover["启动恢复"]
        DUMP_LOAD["load dump 文件\nmmap 优先 / 回退 fread"]
        AOF_REPLAY["replay AOF\nmmap 优先 / 回退 fread"]
    end

    CMD --> AOF_WRITE
    AOF_WRITE --> AOF_FILE

    DUMP_SCHED -->|"BGSAVE 子进程"| DUMP_FILE

    AOF_FILE --> AOF_REPLAY
    DUMP_FILE --> DUMP_LOAD

    AOF_REPLAY --> Recovery["内存数据恢复"]
    DUMP_LOAD --> Recovery
```

**恢复顺序**：先恢复 dump 文件（全量二进制），再重放 AOF（增量 RESP 命令）。

```c
// src/persistence/kvs_persist.c
int persist_recover(void) {
    // Step 1: 恢复 dump 文件 (uint32_t klen + key + uint32_t vlen + value 二进制格式)
    replay_dump_file(g_cfg.dump_path);

    // Step 2: 重放 AOF 文件 (RESP 命令格式)
    replay_file(g_cfg.aof_path);
}
```

### 5.2 Dump 格式 (KVSD)

**格式**：二进制长度前缀格式，`uint32_t klen (4字节) + key (klen 字节) + uint32_t vlen (4字节) + value (vlen 字节)`，每对 key-value 为一条记录。

```
[4B klen][key bytes][4B vlen][value bytes]
[4B klen][key bytes][4B vlen][value bytes]
...
```

**mmap 恢复**：优先使用 mmap，减少用户态拷贝，失败回退到 fread。

```c
static int replay_dump_file(const char *path) {
    fd = open(path, O_RDONLY);
    fstat(fd, &st);

    // Step 1: mmap 文件到内存
    mapped = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) { close(fd); return 0; }
    size = (size_t)st.st_size;

    // Step 2: 解析二进制格式 uint32_t klen + key + uint32_t vlen + value
    while (pos + 4 <= size) {
        uint32_t klen, vlen;
        // 读 key 长度
        memcpy(&klen, mapped + pos, sizeof(klen));
        pos += sizeof(klen);
        if (pos + klen > size) break;
        // 读 key
        key = (char *)kvs_malloc(klen + 1);
        if (klen > 0) memcpy(key, mapped + pos, klen);
        key[klen] = '\0';
        pos += klen;
        // 读 value 长度
        if (pos + 4 > size) { kvs_free(key); break; }
        memcpy(&vlen, mapped + pos, sizeof(vlen));
        pos += sizeof(vlen);
        // 读 value
        value = (char *)kvs_malloc(vlen + 1);
        if (vlen > 0) memcpy(value, mapped + pos, vlen);
        value[vlen] = '\0';
        pos += vlen;
        // 恢复数据
        kvs_hash_set(&global_hash, key, value);
        kvs_free(key); kvs_free(value);
    }
    munmap(mapped, size);
    close(fd);
}
```

> ⚠️ **注意**：恢复时只恢复到 Hash 引擎。`kvs_load_dump_from_fd()` 在头文件中有声明，但目前**未实现**。

**kvs_dump_to_fd() 实现**：遍历所有 5 个引擎（array/hash/rbtree/skiptable/doc），统一写入二进制长度前缀格式。文档引擎的 value 会平铺为 `name=value name=value` 格式。

**kvs_snapshot_to_fd()**：与 dump 不同，snapshot 使用 **RESP 命令格式**（`SET key value\r\n`、`HSET key value\r\n` 等），用于 AOF 重写（BGREWRITEAOF）和全量同步。

**SAVE / BGSAVE**：

```c
int persist_save_dump_to(const char *path) {
    // 遍历所有引擎 → 按行写入 key\\nvalue\\n
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    kvs_dump_to_fd(fd);   // 遍历 hash / array / rbtree / skiptable / doc
    persist_fsync_fd(fd); // io_uring fsync 优先
    close(fd);
}

void persist_bgsave(void) {
    pid_t pid = fork();
    if (pid == 0) {
        // 子进程写临时文件
        persist_save_dump_to(tmp_path);
        rename(tmp_path, dump_path);
        _exit(0);
    }
    g_bgsave_pid = pid;  // 父进程记录 pid
}
```

### 5.3 AOF 格式与写入

**格式**：与 RESP 协议相同，每条写命令直接追加到文件。

```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n
```

**io_uring 写入**：

```c
int persist_write_fd_uring(int fd, const unsigned char *buf,
                           size_t len, off_t *offset) {
    while (written < len) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_write(sqe, fd, buf + written, chunk, *offset);
        io_uring_submit(&ring);
        io_uring_wait_cqe(&ring, &cqe);  // 等待完成
        written += cqe->res;             // 实际写入字节数
        io_uring_cqe_seen(&ring, cqe);
    }
}
```

**fsync 策略**：

```c
// always: 每次写命令后 fsync
// everysec: 每秒 fsync
if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
    persist_fsync_fd_best_effort(g_aof_fd);  // io_uring fsync 优先
}
```

**AOF 重写 (BGREWRITEAOF)**：后台子进程将当前内存快照写入临时 AOF，然后替换主 AOF 文件。重写期间的新命令通过缓冲区转发。

```c
void persist_bgrewriteaof(void) {
    pid_t pid = fork();
    if (pid == 0) {
        // 子进程：将内存快照写入临时 AOF
        persist_write_aof_snapshot_to(tmp_path);
        _exit(0);
    }
    g_bgrewrite_pid = pid;
    // 父进程将重写期间的新写命令追加到缓冲区 g_rewrite_buf
}
```

---

## 6. 复制层：TCP / RDMA / eBPF

### 6.1 总体架构

```mermaid
flowchart TB
    subgraph Master["Master 节点"]
        MV["存储引擎\n所有数据"]
        BACKLOG["Replication Backlog\n1MB 环形缓冲区"]
        REPL_LIST["Slave 链表\nconn_t *g_replicas"]
        RDMA_LISTENER["RDMA Listener 线程\nrdma_master_listener_thread()"]
    end

    subgraph Transport["传输策略层"]
        TCP_TCP["TCP transport\nrepl_transport_tcp_send()"]
        RDMA_TRANS["RDMA transport\nrepl_rdma_try_send()"]
        EBPF_TRANS["eBPF transport\nrepl_transport_ebpf_send()"]
    end

    subgraph Slave["Slave 节点"]
        SLAVE_THREAD["slave_thread()\n后台线程"]
        SLAVE_STATE["复制状态\nreplid / offset / durable"]
    end

    MV -->|"queue_snapshot()"| RDMA_LISTENER
    MV -->|"写命令广播\nrepl_broadcast()"| BACKLOG
    BACKLOG --> TCP_TCP
    BACKLOG --> EBPF_TRANS
    RDMA_LISTENER --> RDMA_TRANS

    TCP_TCP -->|"TCP 连接"| SLAVE_THREAD
    EBPF_TRANS -->|"eBPF sockmap\n内核态转发"| SLAVE_THREAD
    RDMA_TRANS -->|"RDMA RC 连接"| SLAVE_THREAD

    SLAVE_THREAD --> SLAVE_STATE
    SLAVE_THREAD -->|"REPLACK\napplied/durable offset"| Master
```

### 6.2 复制协议 (RESP-based)

采用类似 Redis 的复制协议：

| 命令 | 方向 | 说明 |
|------|------|------|
| `REPLSYNC <replid> <offset> <durable>` | Slave → Master | 请求同步 |
| `+FULLRESYNC <replid> <offset> <bytes>` | Master → Slave | 全量同步头 |
| `+CONTINUE <replid> <offset>` | Master → Slave | 部分同步头 |
| `REPLACK <applied> <durable>` | Slave → Master | 确认位点 |
| `REPLDONE` | Master → Slave | 全量同步完成 |

### 6.3 混合传输模式 (Hybrid Transport)

```mermaid
flowchart TD
    subgraph Phase1["FULLRESYNC Phase\n批量已有数据"]
        H1["Master 生成快照\nqueue_snapshot()"]
        H2["通过 RDMA 发送\nrepl_rdma_try_send()"]
    end

    subgraph Phase2["Realtime Phase\n增量命令"]
        R1["客户端 SET/DEL/..."]
        R2["Master 广播\nrepl_broadcast()"]
        R3["通过 eBPF sockmap\n内核态转发"]
    end

    H1 --> H2
    R1 --> R2 --> R3
```

**配置方式**：

```conf
repl_fullsync_transport=rdma    # 全量同步走 RDMA
repl_realtime_transport=ebpf+tcp    # 增量同步走 eBPF+tcp（推荐）
```

### 6.4 TCP 复制实现

**Master 侧命令广播** (`src/main/kvstore.c`)：

```c
void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    repl_note_send_context("broadcast", rawlen, repl_master_offset(), raw);
    repl_backlog_feed(raw, rawlen);  // 也写入 backlog（供 partial resync）
    repl_note_broadcast(rawlen);
    pthread_mutex_lock(&g_repl_lock);

    for (conn_t **pp = &g_replicas; *pp; ) {
        conn_t *c = *pp;
        if (c->repl_draining) {
            *pp = c->next_replica;   // 清理正在排空的 replica
            c->next_replica = NULL;
            c->is_replica = 0;
            continue;
        }
        if (c->repl_fullsync_pending) {
            pp = &c->next_replica;   // 全量同步未完成的跳过
            continue;
        }
        // 优先尝试实时传输 (eBPF/sockmap)
        if (repl_realtime_send(c, raw, rawlen) != 0) {
            if (repl_handle_replica_send_failure(c, pp)) continue;
        }
        // 更新统计
        c->repl_offset_sent = repl_master_offset();
        c->repl_last_send_ms = kvs_now_ms();
        pp = &c->next_replica;
    }
    pthread_mutex_unlock(&g_repl_lock);
}
```

> ⚠️ **注意**：实际签名是 `void repl_broadcast(const unsigned char *raw, size_t rawlen)`，**没有** `skip` 参数。排空（`repl_draining`）和全量同步等待（`repl_fullsync_pending`）机制替代了早期的 skip 逻辑。

**Backlog (环形缓冲区)**：

```c
typedef struct repl_backlog_s {
    unsigned char *buf;       // 1MB 环形缓冲区
    size_t cap;               // 1024*1024
    size_t histlen;           // 当前已使用的历史长度
    size_t head;              // 写入位置
    unsigned long long start_offset;
    unsigned long long end_offset;
} repl_backlog_t;
```

**Slave 线程** (`src/replication/kvs_repl.c`)：

```c
static void *slave_thread(void *arg) {
    for (;;) {
        // 建立 TCP 连接
        int fd = repl_transport_tcp_connect_slave(host, port);
        if (fd < 0) { sleep(1); continue; }

        // 发送 REPLSYNC
        snprintf(offbuf, "%llu", g_slave_repl_offset);
        snprintf(durablebuf, "%llu", g_slave_repl_durable_offset);
        resp_build_cmd4(cmd, "REPLSYNC", replid, offbuf, durablebuf);
        send(fd, cmd, n, 0);

        // 接收循环
        unsigned char buf[BUFFER_CAP];
        size_t blen = 0;
        for (;;) {
            ssize_t r = recv(fd, buf + blen, sizeof(buf) - blen, 0);
            if (r > 0) {
                blen += (size_t)r;
                parse_resp_stream(NULL, buf, &blen, 1);
                repl_slave_ack_heartbeat();
            } else break;
        }
    }
}
```

### 6.5 RDMA 全量同步

**详细实现**参见 `docs/rdma-fullsync-implementation.md`。这里给出核心要点：

#### 6.5.1 基础架构

**Master RDMA Listener 线程** (`src/replication/kvs_repl.c`)：

```c
static void *rdma_master_listener_thread(void *arg) {
    for (;;) {
        // 创建 RDMA CM 事件通道
        g_repl_rdma_ctx.ec = rdma_create_event_channel();
        rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP);

        // Bind + Listen (端口 = TCP端口 + 1)
        addr.sin_port = htons(g_cfg.rdma_port ?: g_cfg.port + 1);
        rdma_bind_addr(listen_id, &addr);
        rdma_listen(listen_id, 4);

        // 等待连接请求
        rdma_get_cm_event(ec, &event);  // RDMA_CM_EVENT_CONNECT_REQUEST
        g_repl_rdma_ctx.id = event->id;

        // 创建 Verbs 资源
        ibv_alloc_pd(id->verbs);          // 保护域
        ibv_create_cq(id->verbs, ...);    // 完成队列
        repl_rdma_create_qp();             // 可靠连接 QP (IBV_QPT_RC)
        repl_rdma_prepare_buffers();       // 注册 MR + 分配 pipeline send slots
        repl_rdma_post_initial_recv();     // 预 post recv WR

        // Accept
        rdma_accept(id, &param);
        repl_rdma_wait_event(ESTABLISHED, 5000);
        g_repl_rdma_ctx.connected = 1;
        // pipeline 模式下启动 CQ 轮询线程
        if (g_repl_rdma_ctx.send_pipeline_enabled)
            repl_rdma_start_cq_poll_thread();

        // 接收循环（hybrid 模式 sleep，非 hybrid 模式 poll recv）
        for (;;) {
            if (!g_repl_rdma_ctx.connected) break;
            if (hybrid_mode) { sleep(1); continue; }
            repl_rdma_wait_cq_recv_completion(2000, &slot, &len);
            parse_resp_stream(&conn, stream_buf, &stream_len, 0);
        }
    }
}
```

#### 6.5.2 同步发送模式（原始实现）

Pipeline 之前的原始发送模式，每次 send 同步等待 CQ completion：

```c
static int repl_rdma_try_send(const unsigned char *buf, size_t len) {
    pthread_mutex_lock(&g_repl_rdma_send_lock);

    memcpy(g_repl_rdma_ctx.send_buf, buf, len);

    struct ibv_sge sge = {
        .addr = (uintptr_t)g_repl_rdma_ctx.send_buf,
        .length = (uint32_t)len,
        .lkey = g_repl_rdma_ctx.send_mr->lkey,
    };
    struct ibv_send_wr wr = {
        .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };

    ibv_post_send(g_repl_rdma_ctx.id->qp, &wr, &bad_wr);
    repl_rdma_wait_cq_send_completion(1000);  // ← 流水线阻断点

    pthread_mutex_unlock(&g_repl_rdma_send_lock);
}
```

**瓶颈**：单 send buffer，每次 post 后必须等待 completion 才能准备下一块数据。在 soft-iWARP 上每次软件 WR 延迟约 100-500μs。

#### 6.5.3 Pipeline 发送模式（当前实现）

**设计目标**：通过多 send buffer 环形队列 + 后台 CQ 轮询线程，将数据拷贝、WR 提交、DMA 传输三者重叠。

**新增数据结构**：

```c
#define KVS_RDMA_PIPELINE_DEPTH      4   /* 多发送缓冲区深度 */
#define KVS_RDMA_CQ_BATCH            8   /* CQ 批量 poll 大小 */
#define KVS_RDMA_PIPELINE_WR_ID_FLAG 0x80000000UL  /* wr_id 高位标记 */

/* Pipeline 发送缓冲区槽位 */
typedef struct repl_rdma_send_slot_s {
    struct ibv_mr *mr;          /* 注册的内存区域 */
    unsigned char *buf;         /* 缓冲区 */
    size_t cap;                 /* 容量 */
    int in_flight;              /* 1 = 已 post 但未完成 */
    uint64_t wr_id;             /* 对应的 wr_id */
} repl_rdma_send_slot_t;
```

**非阻塞发送流水线**：

```c
static int repl_rdma_try_send(const unsigned char *buf, size_t len) {
    pthread_mutex_lock(&g_repl_rdma_send_lock);

    // pipeline 模式：非阻塞发送
    int slot = repl_rdma_acquire_send_slot(5000);  // 获取空闲 slot
    if (slot < 0) { ... }

    memcpy(g_repl_rdma_ctx.send_slots[slot].buf, buf, len);

    struct ibv_sge sge = {
        .addr = (uintptr_t)g_repl_rdma_ctx.send_slots[slot].buf,
        .length = (uint32_t)len,
        .lkey = g_repl_rdma_ctx.send_slots[slot].mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)slot | KVS_RDMA_PIPELINE_WR_ID_FLAG,
        .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };

    ibv_post_send(qp, &wr, &bad_wr);
    g_repl_rdma_ctx.send_slots[slot].in_flight = 1;  // 标记飞行中
    g_repl_rdma_ctx.send_slots_in_flight++;

    pthread_mutex_unlock(&g_repl_rdma_send_lock);
    return 0;  // ← 立即返回，不等待 CQ
}
```

**后台 CQ 轮询线程**：使用 completion channel 事件驱动，避免 busy poll。

```c
static void *repl_rdma_cq_poll_thread(void *arg) {
    while (running && connected) {
        ibv_get_cq_event(comp_chan, &ev_cq, &ev_ctx);  // 阻塞等待事件
        ibv_ack_cq_events(cq, 1);

        // 批量 poll
        while ((n = ibv_poll_cq(cq, KVS_RDMA_CQ_BATCH, wc)) > 0) {
            for (int i = 0; i < n; i++) {
                if (wc[i].opcode == IBV_WC_SEND) {
                    // 释放 send slot
                    int slot = wc[i].wr_id & ~KVS_RDMA_PIPELINE_WR_ID_FLAG;
                    g_repl_rdma_ctx.send_slots[slot].in_flight = 0;
                    g_repl_rdma_ctx.send_slots_in_flight--;
                } else if (wc[i].opcode == IBV_WC_RECV) {
                    // 推入 pending recv 队列
                    int slot = (int)(wc[i].wr_id - 1);
                    repl_rdma_pending_recv_push(slot, wc[i].byte_len);
                }
            }
        }
        /* 标准 re-arm → drain 模式 */
        ibv_req_notify_cq(cq, 0);
        ibv_poll_cq(cq, KVS_RDMA_CQ_BATCH, wc);  // 确认无漏网 completion
    }
}
```

**自适应 Pipeline 深度**：根据 in_flight 数量动态调节 2~4：

```c
static void repl_rdma_adjust_pipeline_depth(void) {
    int in_flight = g_repl_rdma_ctx.send_slots_in_flight;
    int depth = g_repl_rdma_ctx.send_pipeline_depth;

    if (in_flight <= depth / 2 && depth < KVS_RDMA_PIPELINE_DEPTH)
        g_repl_rdma_ctx.send_pipeline_depth = depth + 1;  // 增加深度
    else if (in_flight >= depth && depth > KVS_RDMA_PIPELINE_DEPTH_MIN)
        g_repl_rdma_ctx.send_pipeline_depth = depth - 1;  // 减少深度
}
```

**Pipeline 架构数据流**：

```mermaid
flowchart LR
    subgraph MasterSend["Master 发送端"]
        ACQ["acquire_send_slot()\n获取空闲 slot"]
        CPY["memcpy → slot buffer"]
        POST["ibv_post_send()\n非阻塞，立即返回"]
    end

    subgraph CQThread["CQ 轮询线程（后台）"]
        EVT["ibv_get_cq_event()\n等待 completion"]
        POLL["ibv_poll_cq()\n批量取 WC"]
        SEND_CMP["SEND completion\n→ release_send_slot()"]
        RECV_CMP["RECV completion\n→ pending_recv_push()"]
    end

    subgraph SlaveRecv["Slave 接收端"]
        RECV_DEQ["wait_cq_recv_completion()\n从 pending 队列取"]
        REPOST["repost_recv()\n重新挂载 recv WR"]
        PARSE["parse_resp_stream()"]
    end

    ACQ --> CPY --> POST
    POST -->|"QP 硬件"| EVT
    EVT --> POLL
    POLL --> SEND_CMP
    POLL --> RECV_CMP
    SEND_CMP -.->|"释放 slot"| ACQ
    RECV_CMP --> RECV_DEQ
    RECV_DEQ --> REPOST
    RECV_DEQ --> PARSE
```

#### 6.5.4 CQ 编程的关键陷阱与修复

在 Pipeline 实现中遇到了两个典型的 RDMA 编程问题：

**陷阱一：Notification re-arm 窗口期死锁**

```
错误顺序:
  ibv_poll_cq() → 全空 → ibv_req_notify_cq() → ibv_get_cq_event()
                            ↑
                      completion 在此窗口到达 → 永远丢失 event → 线程死锁

正确顺序 (re-arm → drain):
  ibv_req_notify_cq() → ibv_poll_cq()
                          ├─ 有数据 → 处理 → 重试
                          └─ 无数据 → ibv_get_cq_event() 等待
```

**陷阱二：多线程同时 poll 同一 CQ**

当 CQ 轮询线程运行时，其他路径（`wait_cq_recv_completion`、`acquire_send_slot`）不得直接 `ibv_poll_cq()`，否则会导致 completion channel event 被错误消费。解决方案：CQ 线程运行时，所有其他路径只检查 pending 队列，不碰 CQ。

```c
/* 正确做法：CQ 线程运行时，只等 pending 队列 */
if (g_repl_rdma_ctx.cq_poll_thread_running) {
    while (kvs_now_ms() < deadline) {
        if (repl_rdma_pending_recv_pop(&slot, &len) == 0) return 0;
        usleep(1000);
    }
    return -1;
}
/* 无 CQ 线程：直接 poll CQ（向后兼容） */
ibv_poll_cq(...);
```

#### 6.5.5 Pipeline 性能对比

| 指标 | 同步模式（改前） | Pipeline 模式（改后） | 预期提升 |
|---|---|---|---|
| 每次 send 阻塞 | 等待 100-500μs | 立即返回 (0μs) | 2-4x 吞吐 |
| CQ 轮询方式 | busy poll (100% 单核) | 事件驱动 (ibv_get_cq_event) | CPU ↓80% |
| send buffer 数量 | 1（无法 prep 下一块） | 4（环形队列重叠拷贝与 DMA） | 有效带宽翻倍 |
| CQ 多线程竞争 | N/A | 通过 cq_poll_thread_running 隔离 | 消除竞争死锁 |

**当前状态**：✅ Pipeline 模式默认启用 (`send_pipeline_enabled=1`)，不影响 TCP/eBPF 等其他传输路径。`repl_rdma_batch_send()` 批量 WR 提交接口已预留但未接入调用方。
```

### 6.6 eBPF 实时同步

> **⚠️ 现状（2026-08-03）**：本节的 eBPF sockmap 是**早期方案**，当前实时传输路径已演进为 **ebpf+tcp**（`repl_realtime_transport=ebpf+tcp`）：独立 ebpf-proxy 进程（`src/ebpf_proxy/`）用 fentry/fexit 截获 master 的 `tcp_recvmsg` → ringbuf → 批量 writev → TCP 跨机转发到 slave。sockmap（`ebpf_enabled=0`）仅为 kprobe 不可用时的回退方案。详见 [`docs/ebpf-forwarding-optimization-journey.md`](ebpf-forwarding-optimization-journey.md) 与 [`docs/replication-mechanism-qa.md`](replication-mechanism-qa.md)。

**目标**：在内核态完成数据转发，避免数据在"内核 → 用户态 → 内核"的来回拷贝。

**BPF 程序** (`src/replication/bpf/repl_sockmap.bpf.c`)：

```c
SEC("sk_msg")
int kvstore_repl_sk_msg(struct sk_msg_md *msg) {
    // 检查角色（master 侧才执行转发）
    role = current_role(msg);
    if (role != KVS_EBPF_ROLE_MASTER_SIDE) return SK_PASS;

    // 从 control_map 获取转发目标 key
    redirect_key = control_value(KVS_EBPF_CTL_REDIRECT_KEY);

    // 执行 sockmap 重定向
    if (control_value(KVS_EBPF_CTL_REDIRECT_INGRESS)) {
        // ingress 重定向：数据到目标 socket 的接收队列（本地优化）
        bpf_msg_redirect_map(msg, &sock_map, redirect_key, BPF_F_INGRESS);
    } else {
        // egress 重定向：通过 sockmap TCP 跨机转发
        bpf_msg_redirect_map(msg, &sock_map, redirect_key, 0);
    }
}
```

**用户态管理** (`src/replication/kvs_repl_ebpf.c`)：

```c
int repl_ebpf_init(void) {
    // 1. 加载 BPF 对象文件
    bpf_object__open_file("build/replication/bpf/repl_sockmap.bpf.o", NULL);
    bpf_object__load(obj);

    // 2. 查找 BPF maps
    g_repl_ebpf_sock_map_fd = bpf_object__find_map_fd_by_name(obj, "sock_map");
    g_repl_ebpf_stats_map_fd = bpf_object__find_map_fd_by_name(obj, "stats_map");
    g_repl_ebpf_control_map_fd = bpf_object__find_map_fd_by_name(obj, "control_map");

    // 3. 设置控制参数
    int zero = 0;
    int redirect_enabled = 1;
    bpf_map_update_elem(g_repl_ebpf_control_map_fd, &KVS_EBPF_CTL_REDIRECT_ENABLED,
                        &redirect_enabled, BPF_ANY);
    bpf_map_update_elem(g_repl_ebpf_control_map_fd, &KVS_EBPF_CTL_MASTER_PORT,
                        &g_cfg.port, BPF_ANY);

    // 4. 挂载 SK_MSG 程序到 sock_map
    bpf_prog_attach(prog_fd, g_repl_ebpf_sock_map_fd, BPF_SK_MSG_VERDICT);
}

int repl_ebpf_register_fd(int fd, int is_master_side) {
    // 将 socket fd 加入 sock_map
    int key = is_master_side ? KVS_EBPF_SOCK_KEY_MASTER_SIDE : slave_key;
    bpf_map_update_elem(g_repl_ebpf_sock_map_fd, &key, &fd, BPF_ANY);
}
```

#### 6.6.1 eBPF 独立守护进程模式

实际部署时，eBPF 加载和管理可由独立进程 `tools/ebpf/repl_ebpf_daemon` 完成（通过 `ebpf_enabled=0` 配置），kvstore 自身不加载 BPF 对象。

```bash
# 独立守护进程启动
tools/ebpf/repl_ebpf_daemon -c kvstore-ebpf.conf
```

配置文件 `kvstore-ebpf.conf`：
```
ebpf_obj_path=build/replication/bpf/repl_sockmap.bpf.o
ebpf_pin_path=/sys/fs/bpf/kvstore
ebpf_redirect=1
ebpf_redirect_key=0
ebpf_forward=0
```

当 `ebpf_enabled=1` 时，kvstore 直接在进程内加载 BPF 对象（向后兼容模式）。守护进程模式下 kvstore 通过 pin 路径与 eBPF maps 共享。

#### 6.6.2 传输层降级机制 (Transport Fallback)

当配置的传输层（RDMA/eBPF）运行中不可用或失败时，系统自动降级到 TCP：

```c
// 降级逻辑位于 repl_realtime_send() / repl_fullsync_send() 内部
// 当 RDMA WR 失败或 eBPF 重定向失败时，自动标记降级
if (repl_rdma_try_send(...) != 0 && g_repl_rdma_ctx.state == REPL_RDMA_STATE_FALLBACK_TCP) {
    // → 自动切到 repl_transport_tcp_send()
}
```

降级状态和原因可通过 `INFO` 命令的以下字段查看：
- `repl_transport_active`：当前活跃的传输层
- `repl_transport_fallback_count`：降级次数
- `repl_transport_fallback_reason`：降级原因
- `repl_transport_fallback_until_ms`：降级持续到的时间戳

**数据流**：

```
用户态 Set 命令
    │
    ▼
Master 线程: repl_broadcast()
    │  repl_realtime_send() → 最终 write(socket)
    │
    ▼
BPF 程序: sk_msg 钩子拦截消息
    │
    ├─ bpf_msg_redirect_map(sock_map, slave_key)
    │
    ▼
Slave socket 接收队列（ingress）或 TCP 发送（egress）
    │
    ▼
Slave 线程: recv() → parse_resp_stream()
```

---

## 7. TTL 过期系统

### 7.1 设计

**数据结构**：哈希表 (O(1) 查找) + 最小堆 (O(1) 取最快要过期的 key)。

```c
typedef struct kvs_expire_item_s {
    char *key;                          // key 名称
    int engine;                         // 所属引擎
    long long expire_at_ms;             // 到期时间戳 (ms)
    size_t heap_index;                  // 在堆中的位置
    struct kvs_expire_item_s *next;     // 哈希链地址
} kvs_expire_item_t;

typedef struct kvs_expire_table_s {
    kvs_expire_item_t **buckets;        // 哈希桶 (8192 槽)
    size_t size, count;
    kvs_expire_item_t **heap;           // 最小堆 (数组实现)
    size_t heap_size, heap_cap;         // 初始容量 1024
} kvs_expire_table_t;
```

```mermaid
flowchart TB
    subgraph HashIndex["哈希索引 (O(1) 查找)"]
        H0["bucket[0]"] --> H0N1["item: key=A → next→"]
        H1["bucket[1]"] --> H1N1["item: key=B"]
        H2["bucket[2]"]
    end

    subgraph MinHeap["最小堆 (O(1) 取最早过期)"]
        Heap["
        heap[0]: expire_at=100 (最早)\n
        heap[1]: expire_at=200\n
        heap[2]: expire_at=300\n
        heap[3]: expire_at=400\n
        "]
    end

    H0N1 -.->|"heap_index=2"| Heap
    H1N1 -.->|"heap_index=0"| Heap
```

### 7.2 核心操作

**设置 TTL** (`kvs_expire_set -> EXPIRE`)：

```c
int kvs_expire_set(kvs_expire_table_t *tab, int engine,
                   const char *key, long long ttl_ms) {
    // Step 1: 计算到期时间
    long long expire_at = kvs_now_ms() + ttl_ms;

    // Step 2: 查找是否已有该 key 的过期记录
    node = expire_find(tab, engine, key);

    if (node) {
        // 已有 → 更新时间
        node->expire_at_ms = expire_at;
        heap_update(tab, node);  // 堆中重新调整位置
    } else {
        // 新建
        node = kvs_calloc(1, sizeof(*node));
        node->key = strdup(key);
        node->engine = engine;
        node->expire_at_ms = expire_at;

        // 插入哈希桶
        idx = expire_hash(key, engine, tab->size);
        node->next = tab->buckets[idx];
        tab->buckets[idx] = node;

        // 插入最小堆
        heap_push(tab, node);
    }
}
```

**过期扫描** (`kvs_active_expire_cycle`)：在每次事件循环中调用的周期性函数。

```c
void kvs_active_expire_cycle(int budget) {
    long long now = kvs_now_ms();

    // 从堆顶取最快要过期的 key，最多 scan budget 个
    while (budget-- > 0 && global_expire.heap_size > 0) {
        kvs_expire_item_t *node = global_expire.heap[0];
        if (node->expire_at_ms > now) break;  // 堆顶还未到期

        // 删除过期 key（从所有引擎 + 过期表中删除）
        engine_del(node->engine, node->key);
        expire_free_node(&global_expire, node);
    }
}
```

---

## 8. 配置与命令行系统

### 8.1 默认配置

```c
kv_config_t g_cfg = {
    .role = ROLE_MASTER,
    .port = 5160,
    .master_host = "192.168.233.128",
    .master_port = 5160,
    .dump_path = "kvstore.dump",
    .aof_path = "kvstore.aof",
    .mem_backend = "libc",
    .net_backend = "reactor",
    .repl_transport_backend = "tcp",
    .repl_fullsync_transport = "rdma",
    .repl_realtime_transport = "tcp",
    .ebpf_obj_path = "build/replication/bpf/repl_sockmap.bpf.o",
    .ebpf_pin_path = "/sys/fs/bpf/kvstore_repl_sockmap",
    .ebpf_enabled = 0,
    .ebpf_redirect = 0,
    .ebpf_redirect_key = 0,
    .ebpf_forward = 0,
    .rdma_dev = "siw0",
    .rdma_ib_port = 1,
    .rdma_gid_idx = 1,
    .rdma_port = 0,
    .rdma_recv_slots = 64,
    .rdma_chunk_size = BUFFER_CAP * 4,  // 262144
    .rdma_qp_wr_depth = 64,
    .aof_fsync = KVS_AOF_FSYNC_ALWAYS,
    .log_mode = "info",
    .is_sentinel = 0,
    .sentinel_master_name = "mymaster",
    .sentinel_monitor_host = "127.0.0.1",
    .sentinel_monitor_port = 5000,
    .sentinel_known_slaves = "",
    .sentinel_down_after_ms = 5000,
    .sentinel_failover_timeout_ms = 10000,
    .sentinel_quorum = 1,
    .kprobe_enabled = 1,
    .repl_kprobe_obj_path = "build/replication/bpf/repl_kprobe.bpf.o",
};
```

> ⚠️ **注意**：`rdma_chunk_size` 的实际默认值是 `BUFFER_CAP * 4` (262144)，**不是** `BUFFER_CAP / 4` (16384)。`ebpf_enabled=0` 表示由独立守护进程管理 eBPF。

### 8.2 配置加载链

```mermaid
flowchart LR
    CMD["命令行参数\n--port 6380\n--mem jemalloc"] --> CONFIG["kvstore.conf\n配置解析"]
    CONFIG --> DEFAULTS["默认值\ng_cfg 结构体初始化"]
```

1. 结构体初始化为默认值
2. `parse_args()` 中先查找配置文件路径，调用 `parse_config_file()` 逐行解析配置文件，覆盖默认值
3. 然后解析命令行参数，覆盖配置文件

```c
// 配置解析：key=value 格式
static int parse_config_file(const char *path) {
    FILE *fp = fopen(path, "r");
    while (fgets(line, sizeof(line), fp)) {
        trim_inplace(line);
        if (line[0] == '\0' || line[0] == '#') continue;
        char *eq = strchr(line, '=');
        if (!eq) return -1;
        *eq = '\0';
        char *key = line;
        char *value = eq + 1;
        trim_inplace(key); trim_inplace(value);
        apply_config_kv(key, value);  // 统一键值对分发函数
    }
}
```

> 实际函数名为 `parse_config_file` 而非 `parse_config`。通过 `apply_config_kv()` 统一处理键值对分发，支持所有配置项。

---

## 9. 协议层：RESP 编解码

### 9.1 编码函数

全部位于 `src/main/kvstore.c` 开头：

```c
// 简单字符串: +OK\\r\\n
int resp_simple_string(char *out, size_t cap, const char *s) {
    return snprintf(out, cap, "+%s\\r\\n", s);
}

// 错误: -ERR xxx\\r\\n
int resp_error(char *out, size_t cap, const char *s) {
    return snprintf(out, cap, "-ERR %s\\r\\n", s);
}

// 整数: :123\\r\\n
int resp_integer(char *out, size_t cap, long long v) {
    return snprintf(out, cap, ":%lld\\r\\n", v);
}

// 批量字符串: $5\\r\\nhello\\r\\n
int resp_bulk(char *out, size_t cap, const char *s, size_t len) {
    int n = snprintf(out, cap, "$%zu\\r\\n", len);
    memcpy(out + n, s, len);
    out[n + len] = '\\r'; out[n + len + 1] = '\\n';
    return n + (int)len + 2;
}
```

### 9.2 解析器 (`parse_resp_stream`)

```c
int parse_resp_stream(conn_t *c, unsigned char *buf, size_t *len,
                      int from_replication) {
    size_t pos = 0;
    while (pos < *len) {
        if (buf[pos] == '+') {
            // Simple String → 复制协议命令
            // +FULLRESYNC / +CONTINUE 等
            if (from_replication) {
                parse_line(buf + pos, line_len);
                // 处理 FULLRESYNC / CONTINUE
            }
        } else if (buf[pos] == '*') {
            // RESP 数组: *n\\r\\n$len\\r\\narg\\r\\n...
            int argc = atoi(nbuf);
            for (int i = 0; i < argc; ++i) {
                // 解析 $len\\r\\ncontent\\r\\n
                argv[i] = ...;  // 指向 buffer 中的位置
                argl[i] = len;
            }
            // 派发到命令处理
            handle_parsed_command(c, argc, argv, argl, ..., from_replication);
        }
        // ... 跳过已处理的数据
    }
    // 更新剩余未处理的数据长度
    *len -= pos;
    memmove(buf, buf + pos, *len);
}
```

### 9.3 命令分发 (`handle_parsed_command`)

收到解析后的命令数组，进行字符串匹配后分发给对应的处理逻辑。完整支持的写命令列表：

| 类别 | 命令 |
|------|------|
| **基本 KV** | `SET`, `MSET`, `MOD`, `DEL` |
| **引擎前缀 + KV** | `RSET`, `RMSET`, `RMOD`, `RDEL`, `HSET`, `HMSET`, `HMOD`, `HDEL`, `XSET`, `XMSET`, `XMOD`, `XDEL` |
| **TTL** | `EXPIRE`, `PERSIST`, `REXPIRE`, `RPERSIST`, `HEXPIRE`, `HPERSIST`, `XEXPIRE`, `XPERSIST` |
| **文档** | `DOCSET`, `DOCDEL`, `DOCDROP` |
| **分布式锁** | `LOCK`, `UNLOCK`, `RENEW` |

```c
if (!strcmp(cmd, "SET")) {
    // 存储引擎 + 过期 + 持久化 + 复制广播
    engine_set(engine, key, value);
    kvs_expire_set(&global_expire, engine, key, ttl_ms);
    persist_append_raw(raw, rawlen);
    repl_broadcast(raw, rawlen);
    resp_simple_string(resp, cap, "OK");
    queue_bytes(c, resp, n);
}
else if (!strcmp(cmd, "GET")) {
    char *v = engine_get(engine, key);
    if (v) resp_bulk(resp, cap, v, strlen(v));
    else resp_null_bulk(resp, cap);
    queue_bytes(c, resp, n);
}
else if (!strcmp(cmd, "REPLSYNC")) {
    // 复制同步请求
    int can_continue = repl_backlog_can_continue(req_replid, req_offset);
    repl_add_slave(c);
    if (can_continue) repl_backlog_send_continue(c, req_offset);
    else queue_snapshot(c);
}
else if (!strcmp(cmd, "LOCK") && argc == 4) {
    // 分布式锁：LOCK key owner seconds
    int lrc = lock_acquire(KVS_ENGINE_ARRAY, argv[1], argv[2], atoll(argv[3]));
    if (lrc == 0) {
        // 成功获取
        persist_append_raw(raw, rawlen);
        if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
    }
}
```

---

## 10. 测试体系

### 10.1 测试架构

```mermaid
flowchart TB
    subgraph Unit["单元测试 (tests/)"]
        FS["test_fullsync_protocol.c\n全量同步协议"]
        VS["test_vsearch.c\nVSEARCH 参数校验"]
    end

    subgraph Integration["集成测试 (tests/integration/)"]
        RESP["test_resp_nc_strict.sh\nRESP 协议验证"]
        TTL["test_resp_ttl_nc.sh\nTTL 验证"]
        DOC["test_doc_nc.sh\n文档型验证"]
        QUEUE["test_resp_queue_recovery.sh\n队列恢复验证"]
    end

> ⚠️ **注意**：`tests/unit/` 目录存在但仅含 `.gitkeep`。真正的单元测试是 `tests/test_fullsync_protocol.c` 与 `tests/test_vsearch.c`（`make test_fullsync_protocol` / `make test_vsearch`），其余为集成测试级别。

    subgraph PersistTest["持久化测试 (tools/persist/)"]
        PERSIST["test_resp_persist_nc.sh\n基本链路"]
        URING["run_uring_persist_bench.py\nio_uring 性能"]
        MMAP["run_mmap_recover_bench.py\nmmap 恢复"]
        MASS_TTL["run_mass_ttl_validation.py\n海量 TTL"]
    end

    subgraph ReplTest["复制测试 (tools/repl/)"]
        FULLSYNC["run_repl_fullsync_test.sh\nTCP 全量同步"]
        METRICS["run_repl_metrics_bench.py\n复制指标"]
        PROFILE["run_repl_profile.py\n性能分析"]
        EBPF["run_repl_ebpf_sync_test.py\neBPF 同步"]
        RDMA_SMOKE["run_repl_rdma_smoke.py\nRDMA 冒烟"]
        RDMA_STRESS["run_repl_rdma_stress.py\nRDMA 压力/浸泡"]
        EBPF_ENV["run_repl_ebpf_env_probe.py\neBPF 环境探测"]
    end

    subgraph Bench["性能基准 (tools/bench/)"]
        BENCH_SCRIPTS["基准脚本"]
    end

    subgraph Runner["统一运行器"]
        RUNNER["run_with_kvstore.py\n自动启动/停止 kvstore"]
    end

    Integration --> RUNNER
    PersistTest --> RUNNER
    ReplTest --> RUNNER
```

### 10.2 关键技术

**统一进程管理** (`tools/tests/run_with_kvstore.py`)：自动启动 kvstore 进程作为测试前置条件，测试结束后自动清理。

```bash
# Makefile 中的典型测试目标
check-resp:
    python3 ./tools/tests/run_with_kvstore.py --bin ./kvstore \
        --host $(TEST_HOST) --port $(TEST_PORT) -- \
        bash ./tests/integration/test_resp_nc_strict.sh {HOST} {PORT}
```

**RDMA 测试脚本参数化** (`tools/repl/run_repl_rdma_stress.py`)：

```bash
make check-repl-rdma-stress \
    REPL_RDMA_STRESS_PRELOAD=128 \
    REPL_RDMA_STRESS_TAIL_WRITES=32 \
    REPL_RDMA_STRESS_RESTART_ROUNDS=2 \
    REPL_RDMA_STRESS_PRELOAD=256
```

**eBPF 环境探测** (`tools/repl/run_repl_ebpf_env_probe.py`)：在执行 eBPF 测试前检查内核版本、BPF 特性、clang/libbpf 是否可用，避免在不可用环境上失败。

---

## 附录

### A. 编译选项速查

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `ENABLE_RDMA` | 1 | 是否启用 RDMA 支持 |
| `ENABLE_EBPF` | 1 | 是否启用 eBPF 支持 |
| `CC` | gcc | C 编译器 |
| `CLANG` | clang | BPF 编译器 |
| `CFLAGS` | -Wall -Wextra -O2 | 编译标志 |

### B. 运行时配置速查

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `--port` | 监听端口 | 5160 |
| `--net` | 网络模型: reactor/proactor/ntyco | reactor |
| `--mem` | 内存后端: libc/jemalloc/custom | libc |
| `--role` | 角色: master/slave | master |
| `--master-host` | 主节点地址 | 192.168.233.128 |
| `--master-port` | 主节点端口 | 5160 |
| `--repl-transport` | 复制传输（单模式）: tcp/rdma/ebpf | tcp |
| `--repl-fullsync-transport` | 全量同步传输: tcp/rdma | rdma |
| `--repl-realtime-transport` | 实时同步传输: tcp/ebpf+tcp/kprobe-rdma/ebpf/tcp | tcp |
| `--ebpf-obj` | eBPF 对象文件路径 | build/replication/bpf/... |
| `--ebpf-pin` / `--ebpf-pin-path` | eBPF pin 路径 | /sys/fs/bpf/kvstore_repl_sockmap |
| `--ebpf-redirect` | 启用 eBPF 重定向 | 0 |
| `--ebpf-redirect-key` | eBPF 重定向 key | 0 |
| `--ebpf-forward` | 启用 eBPF 转发 | 0 |
| `--rdma-dev` | RDMA 设备 | siw0 |
| `--rdma-ib-port` | RDMA IB 端口 | 1 |
| `--rdma-gid-idx` | RDMA GID 索引 | 1 |
| `--rdma-port` | RDMA 监听端口（0=自动: 主端口+1） | 0 |
| `--rdma-recv-slots` | RDMA 接收槽位数 | 64 |
| `--rdma-chunk-size` | RDMA 分块大小（默认 BUFFER_CAP*4=262144） | 262144 |
| `--rdma-qp-wr-depth` | RDMA QP WR 深度 | 64 |
| `--kprobe-enabled` | 启用 kprobe 捕获 | 1 |
| `--kprobe-obj` | kprobe BPF 对象文件路径 | build/replication/bpf/repl_kprobe.bpf.o |
| `--appendfsync` | AOF 同步: always/everysec | always |
| `--dump` | dump 文件路径 | kvstore.dump |
| `--aof` | AOF 文件路径 | kvstore.aof |
| `--log-mode` | 日志级别: debug/info/warn/error | info |
| `--autosnap` | 自动快照规则 (格式: sec:changes,...) | 空(禁用) |
| `--sentinel` | 启用哨兵模式 | 0 |
| `--sentinel-master-name` | 哨兵监控名称 | mymaster |
| `--sentinel-monitor-host` | 哨兵监控主机 | 127.0.0.1 |
| `--sentinel-monitor-port` | 哨兵监控端口 | 5000 |
| `--sentinel-known-slaves` | 已知从节点列表 | 空 |
| `--sentinel-down-after` | 判定下线毫秒数 | 5000 |
| `--sentinel-failover-timeout` | 故障转移超时毫秒数 | 10000 |
| `--sentinel-quorum` | 哨兵法定人数 | 1 |

### C. 关键文件索引

| 模块 | 文件 | 函数/功能 |
|------|------|-----------|
| 入口 | `src/main/kvstore.c` | `main()`, `handle_parsed_command()`, `queue_snapshot()`, `repl_broadcast(const unsigned char *raw, size_t rawlen)`, `parse_resp_stream()`, `kvs_dump_to_fd()`, `kvs_snapshot_to_fd()` |
| Reactor | `src/core/reactor.c` | `reactor_start()`, `on_read()`, `on_write()`, `on_accept()`, `queue_bytes()`, `close_conn()` |
| Proactor | `src/core/proactor.c` | `proactor_start()`, `submit_accept()`, `submit_read()`, `submit_write()` |
| NtyCo | `src/core/ntyco.c` | `ntyco_start()`, `ntyco_server()`, `server_reader()` |
| Hash 引擎 | `src/storage/kvs_hash.c` | `kvs_hash_set/get/del` + FNV-1a hash |
| RBTREE 引擎 | `src/storage/kvs_rbtree.c` | `rbtree_insert_fixup`, `left_rotate`, `right_rotate` |
| Skiptable | `src/storage/kvs_skiptable.c` | `kvs_skiptable_set/search` + 概率层数 |
| Doc | `src/storage/kvs_doc.c` | `kvs_doc_set/get/del` + 字段哈希 |
| 内存管理 | `src/memory/kvs_mem.c` | `kvs_malloc/free/calloc` + slab+mmap |
| 持久化 | `src/persistence/kvs_persist.c` | `persist_recover()`, `persist_bgsave()`, `persist_bgrewriteaof()`, `persist_write_fd_uring()` |
| TTL | `src/expire/kvs_expire.c` | `kvs_expire_set()`, `kvs_active_expire_cycle()`, 堆操作 |
| 复制核心 | `src/replication/kvs_repl.c` | `repl_broadcast()`, `slave_thread()`, `rdma_master_listener_thread()`, `repl_rdma_try_send()`, `repl_rdma_post_initial_recv()`, `repl_rdma_acquire_send_slot()`, `repl_rdma_cq_poll_thread()`, `repl_backlog_feed()`, `repl_backlog_write_range()` |
| eBPF | `src/replication/kvs_repl_ebpf.c` | `repl_ebpf_init()`, `repl_ebpf_register_fd()`, `repl_ebpf_get_stats()` |
| BPF 程序 | `src/replication/bpf/repl_sockmap.bpf.c` | `kvstore_repl_sk_msg()` |
| 哨兵 | `src/replication/kvs_sentinel.c` | `sentinel_start()` |
| eBPF 守护进程 | `tools/ebpf/repl_ebpf_daemon.c` | eBPF 独立管理进程（通过 `ebpf_enabled=0` 启用） |
| 哈希工具 | `src/utils/hash.c` | 旧版哈希表实现（基于字符数组，未在 kvstore 中使用） |
| 头文件 | `include/kvstore/kvstore.h` | `conn_t`, `kv_config_t`, `repl_rdma_ctx_t`, `kvs_repl_ebpf_stats_t`, `kvs_expire_table_t` 等全部类型定义 |
| 头文件 | `include/kvstore/server.h` | `conn` 结构体（`kvs_stream_t` 未定义，当前未使用的遗留代码） |

### 6.7 kprobe + RDMA 增量同步

**目标**：通过 kprobe 透明拦截 TCP send 路径，将增量 RESP 数据通过 BPF ringbuf
传递到用户态，再由 RDMA WRITE（单边操作）直接写入 Slave 预置 MR 环形缓冲区，
实现低延迟的增量数据加速。

**核心架构**：

```
Master                                Slave
  │                                     │
  ├─ TCP send (保底) ────────────────► ─┤
  │  (kprobe 在此拦截)                  │
  │    ↓                                │
  │  BPF ringbuf                        │
  │    ↓                                │
  │  kprobe_ringbuf_cb()                │
  │    ↓                                │
  │  RDMA WRITE ────────────────────► ──┤
  │    (数据 + producer_head)           │ → poll 线程消费
  │                                     │ → parse 写入引擎
```

**三层数据读取策略**（BPF 程序 `repl_kprobe.bpf.c`）：

| 步骤 | 操作 | helper |
|------|------|--------|
| 1 | 读 `msg + 40` 处的 iov 指针 | `bpf_probe_read_kernel` |
| 2 | 读 `iov[0]` iovec 结构体 | `bpf_probe_read_kernel` |
| 3 | 读 `iov->iov_base` 用户数据 | `bpf_probe_read_user` |

**关键发现**：kernel 5.15 的 `iov_iter` 布局与通常假设不同 ——
`iov` 在 offset 24（非 8），`iov_offset` 在 offset 8（非 24）。

**独立 QP 设计**：

| QP | 端口 | 用途 |
|----|------|------|
| `g_repl_rdma_ctx` | `base_port + 1` (5161) | RDMA SEND/RECV 全量同步 |
| `g_rdma_kprobe` | `base_port + 12` (5172) | RDMA WRITE 增量同步 |

**MR 交换协议**（通过 TCP 控制通道）：

```
Master → Slave: KPROBEMR\r\n
Slave  → Master: +KPROBERDMA <rkey> <addr> <total_size> <slot_count> <slot_cap>\r\n
```

**配置方式**：
```conf
repl_fullsync_transport=rdma      # 全量同步走 RDMA
repl_realtime_transport=kprobe-rdma  # 增量同步走 kprobe+RDMA
```

**启动命令**：
```bash
sudo ./kvstore --port 5160 --role master \
  --repl-fullsync-transport rdma \
  --repl-realtime-transport kprobe-rdma \
  --rdma-dev siw0 --kprobe-enabled
```

**详细文档**：
- [`docs/kprobe-rdma-incrsync-implementation.md`](./use/kprobe-rdma-incrsync-implementation.md)
- [`docs/kprobe-rdma-debug-diagnosis.md`](./kprobe-rdma-debug-diagnosis.md)

---

