# eBPF Proxy 独立进程 + REPLDONE 修正 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 eBPF client_capture 从 master 进程独立为 `ebpf-proxy` 进程，同时修正 REPLDONE 发送方从 master 改为 slave。

**Architecture:** ebpf-proxy 加载修改后的 `repl_client_capture.bpf.o`，通过 pinned BPF maps 与 master 双向交换配置，kprobe hook master 的 tcp_recvmsg 截获增量数据直发 slave。proxy 检测 slave 发来的 REPLSYNC/REPLDONE 管理 FORWARDING↔BUFFERING 状态机。

**Tech Stack:** C, libbpf, BPF kprobe/kretprobe, RESP protocol, TCP

## Global Constraints

- 代码风格：匹配现有 C 代码（K&R 变体，4 空格缩进）
- 构建：Makefile (`CC=gcc`, `CLANG?=clang`)
- 不引入新的第三方依赖
- 保持现有 slave/master 通信路径不变（TCP/RDMA 全量同步）
- 最小改动原则：不改无关代码

---

## 文件结构

| 文件 | 动作 | 职责 |
|------|------|------|
| `src/ebpf_proxy/main.c` | 创建 | 入口 + 命令行 + 启动流程 + 主循环 + 信号处理 |
| `src/ebpf_proxy/proxy_cache.h` | 创建 | 缓存链表结构体 + 接口声明 |
| `src/ebpf_proxy/proxy_cache.c` | 创建 | 缓存链表实现(append/flush/drop) |
| `src/ebpf_proxy/proxy_slave.h` | 创建 | slave 连接接口声明 |
| `src/ebpf_proxy/proxy_slave.c` | 创建 | slave TCP 连接 + 重连(指数退避) |
| `src/replication/bpf/repl_client_capture.bpf.c` | 修改 | 移除 sendmsg probe, 新增 REPLSYNC/REPLDONE 检测, 新增 proxy_cfg map |
| `src/main/kvstore.c` | 修改 | 删除旧 init, 新增 proxy_cfg 写入, REPLDONE master handler |
| `src/replication/kvs_repl.c` | 修改 | slave 侧发送 REPLDONE, 删除 master 发送 REPLDONE |
| `src/replication/kvs_repl_kprobe.c` | 修改 | 删除 master 侧 client_capture init 和 ringbuf poll 相关代码 |
| `Makefile` | 修改 | 新增 ebpf-proxy target |

---

### Task 1: 修改 BPF 程序 — 移除 sendmsg probe，新增 REPLSYNC/REPLDONE 检测

**Files:**
- Modify: `src/replication/bpf/repl_client_capture.bpf.c`

**Interfaces:**
- Consumes: (none — first task)
- Produces: `client_ctl[3]` kernel 态写 FULLSYNC_STATE；`proxy_cfg` map（kernel 不读写，userspace 使用）；ringbuf magic `0xFFFFFFFF` 通知 flush

- [ ] **Step 1: 删除 `kprobe/tcp_sendmsg` 函数（`kprobe_client_sendmsg`）**

删除整个 `SEC("kprobe/tcp_sendmsg")` 段及其函数体（约 78 行，从 `/* ──── kprobe: 探测 REPLDONE 发送...` 到 `return 0;` 结束）。

- [ ] **Step 2: 新增 `proxy_cfg` map 定义**

在现有 maps 定义区（`client_ctl` 之后）添加：

```c
/* proxy_cfg — master↔ebpf-proxy 共享配置通道
 * BPF 内核代码不读写此 map，仅用于随 BPF 加载自动创建并 pin */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 16);
    __type(key, char[32]);
    __type(value, __u64);
} proxy_cfg SEC(".maps");
```

- [ ] **Step 3: 新增 REPLSYNC/REPLDONE 检测常量**

在文件头部常量定义区添加：

```c
#define KVS_EBPF_CLIENT_CTL_FULLSYNC_STATE 3
#define KVS_EBPF_CLIENT_STAT_REPLSYNC_DETECT 8
```

- [ ] **Step 4: 在 kretprobe recvmsg (`kprobe_client_recv_return`) 中添加 REPLSYNC 检测**

在现有写 ringbuf 之后、更新统计之前，插入 REPLSYNC 检测。将现有的 `in_progress` 检查改为 REPLSYNC/REPLDONE 逻辑。

完整修改后的 `kprobe_client_recv_return` 函数体（替换现有实现）：

```c
SEC("kretprobe/tcp_recvmsg")
int kprobe_client_recv_return(struct pt_regs *ctx)
{
    /* CTL_PID=0 表示禁用 */
    __u64 *ctl_pid = bpf_map_lookup_elem(&client_ctl, &(__u32){1});
    if (!ctl_pid || !*ctl_pid)
        return 0;

    /* 获取返回值 = 实际接收字节数 */
    long retval = (long)ctx->ax;
    if (retval <= 0)
        return 0;

    /* 获取 entry 保存的 msg 指针 */
    __u32 map_key = 0;
    unsigned long *msg_ptr = bpf_map_lookup_elem(&client_entry_msg, &map_key);
    if (!msg_ptr || *msg_ptr == 0) {
        __u64 *miss = bpf_map_lookup_elem(&client_stats, &(__u32){6});
        if (miss) __sync_fetch_and_add(miss, 1);
        return 0;
    }

    /* 限制数据大小 */
    __u32 size = (__u32)retval;
    if (size > CLIENT_ENTRY_MAX_LEN) {
        __u64 *st = bpf_map_lookup_elem(&client_stats, &(__u32){3});
        if (st) __sync_fetch_and_add(st, 1);
        size = CLIENT_ENTRY_MAX_LEN;
    }

    /* 读取数据到 tmpbuf */
    unsigned char(*entry)[CLIENT_ENTRY_HDR_SZ + CLIENT_ENTRY_MAX_LEN];
    entry = bpf_map_lookup_elem(&client_tmpbuf, &map_key);
    if (!entry) return 0;

    __u32 payload_len = 0;
    __builtin_memcpy(*entry, &payload_len, 4);

    int data_len = read_recv_data(*msg_ptr, (*entry) + 4, CLIENT_ENTRY_MAX_LEN);
    if (data_len <= 0) return 0;

    payload_len = (__u32)data_len;
    __builtin_memcpy(*entry, &payload_len, 4);

    /* ── 检测 REPLSYNC（全量同步开始）── */
    {
        int found = 0;
#pragma unroll
        for (int i = 0; i < 56; i++) {  /* 56 = 64 - 8 */
            if (!found && i + 8 <= data_len &&
                (*entry)[4 + i] == 'R' && (*entry)[4 + i + 1] == 'E' &&
                (*entry)[4 + i + 2] == 'P' && (*entry)[4 + i + 3] == 'L' &&
                (*entry)[4 + i + 4] == 'S' && (*entry)[4 + i + 5] == 'Y' &&
                (*entry)[4 + i + 6] == 'N' && (*entry)[4 + i + 7] == 'C') {
                found = 1;
            }
        }
        if (found) {
            __u32 ctl_key = KVS_EBPF_CLIENT_CTL_FULLSYNC_STATE;
            __u64 one = 1;
            bpf_map_update_elem(&client_ctl, &ctl_key, &one, 0);
            __u64 *st = bpf_map_lookup_elem(&client_stats,
                &(__u32){KVS_EBPF_CLIENT_STAT_REPLSYNC_DETECT});
            if (st) __sync_fetch_and_add(st, 1);
        }
    }

    /* ── 检测 REPLDONE（全量同步完成）── */
    {
        int found = 0;
#pragma unroll
        for (int i = 0; i < 56; i++) {
            if (!found && i + 8 <= data_len &&
                (*entry)[4 + i] == 'R' && (*entry)[4 + i + 1] == 'E' &&
                (*entry)[4 + i + 2] == 'P' && (*entry)[4 + i + 3] == 'L' &&
                (*entry)[4 + i + 4] == 'D' && (*entry)[4 + i + 5] == 'O' &&
                (*entry)[4 + i + 6] == 'N' && (*entry)[4 + i + 7] == 'E') {
                found = 1;
            }
        }
        if (found) {
            /* 清除 fullsync 状态 */
            __u32 ctl_key = KVS_EBPF_CLIENT_CTL_FULLSYNC_STATE;
            __u64 zero = 0;
            bpf_map_update_elem(&client_ctl, &ctl_key, &zero, 0);

            /* 写 magic flush 通知到 ringbuf */
            __u32 magic = 0xFFFFFFFF;
            __builtin_memcpy(*entry, &magic, 4);
            bpf_ringbuf_output(&client_cache_ringbuf, *entry, 4, 0);

            __u64 *st = bpf_map_lookup_elem(&client_stats, &(__u32){7});
            if (st) __sync_fetch_and_add(st, 1);

            /* 不继续写正常 ringbuf entry（已写 magic） */
            return 0;
        }
    }

    /* 写入 ringbuf（正常数据路径） */
    int entry_size = CLIENT_ENTRY_HDR_SZ + data_len;
    if (bpf_ringbuf_output(&client_cache_ringbuf, *entry, entry_size, 0) != 0) {
        __u64 *st = bpf_map_lookup_elem(&client_stats, &(__u32){2});
        if (st) __sync_fetch_and_add(st, 1);
        return 0;
    }

    /* 更新统计 */
    __u64 *st = bpf_map_lookup_elem(&client_stats, &(__u32){0});
    if (st) __sync_fetch_and_add(st, 1);

    __u64 *in_progress = bpf_map_lookup_elem(&client_ctl,
        &(__u32){KVS_EBPF_CLIENT_CTL_FULLSYNC_STATE});
    if (in_progress && *in_progress) {
        st = bpf_map_lookup_elem(&client_stats, &(__u32){5});
        if (st) __sync_fetch_and_add(st, 1);
    }

    return 0;
}
```

- [ ] **Step 5: 编译 BPF 程序，验证无错误**

```bash
clang -O2 -g -target bpf -D__TARGET_ARCH_x86 -I./include \
  -c src/replication/bpf/repl_client_capture.bpf.c \
  -o build/replication/bpf/repl_client_capture.bpf.o
```

Expected: 编译成功，无 warnings。如果 verifier 报 `back-edge from` 错误，需要展开循环扫描逻辑。

- [ ] **Step 6: 提交**

```bash
git add src/replication/bpf/repl_client_capture.bpf.c build/replication/bpf/repl_client_capture.bpf.o
git commit -m "feat(bpf): 移除 sendmsg probe, 在 kretprobe recvmsg 中检测 REPLSYNC/REPLDONE, 新增 proxy_cfg map"
```

---

### Task 2: 创建 proxy_cache — 缓存链表模块

**Files:**
- Create: `src/ebpf_proxy/proxy_cache.h`
- Create: `src/ebpf_proxy/proxy_cache.c`

**Interfaces:**
- Consumes: (none)
- Produces: `cache_node_t`, `cache_ctx_t` types; `cache_init()`, `cache_append()`, `cache_flush()`, `cache_destroy()`, `cache_stats()` functions

- [ ] **Step 1: 创建 `src/ebpf_proxy/proxy_cache.h`**

```c
#ifndef PROXY_CACHE_H
#define PROXY_CACHE_H

#include <stddef.h>
#include <stdint.h>

#define PROXY_CACHE_MAX_BYTES (256UL * 1024 * 1024)  /* 256MB */

typedef struct cache_node_s {
    struct cache_node_s *next;
    size_t len;
    unsigned char data[];  /* flexible array */
} cache_node_t;

typedef struct {
    cache_node_t *head;
    cache_node_t *tail;
    size_t total_bytes;
    size_t node_count;
    unsigned long long dropped;
    size_t max_bytes;
} cache_ctx_t;

/* 初始化缓存上下文 */
void cache_init(cache_ctx_t *ctx);

/* 追加数据到链表尾部。超过 PROXY_CACHE_MAX_BYTES 时丢弃 head */
int cache_append(cache_ctx_t *ctx, const unsigned char *data, size_t len);

/* 从 head 逐条发送到 fd，成功后释放节点。返回发送条数，-1 表示发送失败 */
int cache_flush(cache_ctx_t *ctx, int fd);

/* 释放所有节点 */
void cache_destroy(cache_ctx_t *ctx);

/* 获取统计: dropped 计数和 max_bytes 峰值 */
void cache_stats(cache_ctx_t *ctx, unsigned long long *dropped_out,
                 size_t *max_bytes_out);

#endif /* PROXY_CACHE_H */
```

- [ ] **Step 2: 创建 `src/ebpf_proxy/proxy_cache.c`**

```c
#include "proxy_cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <stdio.h>

void cache_init(cache_ctx_t *ctx) {
    if (!ctx) return;
    memset(ctx, 0, sizeof(*ctx));
}

int cache_append(cache_ctx_t *ctx, const unsigned char *data, size_t len) {
    if (!ctx || !data || len == 0) return -1;

    cache_node_t *node = (cache_node_t *)malloc(sizeof(cache_node_t) + len);
    if (!node) return -1;
    node->next = NULL;
    node->len = len;
    memcpy(node->data, data, len);

    /* 上限检查：丢弃最旧节点 */
    while (ctx->total_bytes + len > PROXY_CACHE_MAX_BYTES && ctx->head) {
        cache_node_t *old = ctx->head;
        ctx->head = old->next;
        if (!ctx->head) ctx->tail = NULL;
        ctx->total_bytes -= old->len;
        ctx->node_count--;
        ctx->dropped++;
        free(old);
    }

    /* 追加 */
    if (!ctx->head) {
        ctx->head = node;
        ctx->tail = node;
    } else {
        ctx->tail->next = node;
        ctx->tail = node;
    }
    ctx->total_bytes += len;
    ctx->node_count++;
    if (ctx->total_bytes > ctx->max_bytes) ctx->max_bytes = ctx->total_bytes;
    return 0;
}

int cache_flush(cache_ctx_t *ctx, int fd) {
    if (!ctx || fd < 0) return -1;
    int sent = 0;
    cache_node_t *node = ctx->head;
    while (node) {
        cache_node_t *next = node->next;
        ssize_t n = send(fd, node->data, node->len, MSG_NOSIGNAL);
        if (n != (ssize_t)node->len) {
            fprintf(stderr, "ebpf-proxy: cache_flush send failed: %zd/%zu\n",
                    n, node->len);
            /* 发送失败时停止 flush，保留剩余节点 */
            ctx->head = node;
            return sent;
        }
        sent++;
        ctx->total_bytes -= node->len;
        ctx->node_count--;
        free(node);
        node = next;
    }
    ctx->head = NULL;
    ctx->tail = NULL;
    ctx->total_bytes = 0;
    ctx->node_count = 0;
    return sent;
}

void cache_destroy(cache_ctx_t *ctx) {
    if (!ctx) return;
    cache_node_t *node = ctx->head;
    while (node) {
        cache_node_t *next = node->next;
        free(node);
        node = next;
    }
    memset(ctx, 0, sizeof(*ctx));
}

void cache_stats(cache_ctx_t *ctx, unsigned long long *dropped_out,
                 size_t *max_bytes_out) {
    if (!ctx) return;
    if (dropped_out) *dropped_out = ctx->dropped;
    if (max_bytes_out) *max_bytes_out = ctx->max_bytes;
}
```

- [ ] **Step 3: 提交**

```bash
git add src/ebpf_proxy/proxy_cache.h src/ebpf_proxy/proxy_cache.c
git commit -m "feat(ebpf-proxy): 缓存链表模块 — append/flush/drop, 256MB 上限"
```

---

### Task 3: 创建 proxy_slave — slave 连接管理

**Files:**
- Create: `src/ebpf_proxy/proxy_slave.h`
- Create: `src/ebpf_proxy/proxy_slave.c`

**Interfaces:**
- Consumes: (none)
- Produces: `proxy_slave_connect()`, `proxy_slave_disconnect()`, `proxy_slave_is_connected()`, `proxy_slave_fd()`

- [ ] **Step 1: 创建 `src/ebpf_proxy/proxy_slave.h`**

```c
#ifndef PROXY_SLAVE_H
#define PROXY_SLAVE_H

#include <stdint.h>

#define PROXY_SLAVE_BACKOFF_INIT_MS  100
#define PROXY_SLAVE_BACKOFF_MAX_MS   5000

typedef struct {
    int fd;
    char host[64];
    int port;
    unsigned int backoff_ms;       /* 当前退避间隔 */
    unsigned int backoff_max_ms;   /* 最大退避间隔 5000ms */
} proxy_slave_ctx_t;

/* 初始化 slave 上下文 */
void proxy_slave_init(proxy_slave_ctx_t *ctx, const char *host, int port);

/* 连接 slave。返回 0 成功，-1 失败。调用方负责管理退避策略 */
int proxy_slave_connect(proxy_slave_ctx_t *ctx);

/* 断开连接 */
void proxy_slave_disconnect(proxy_slave_ctx_t *ctx);

/* 检查是否已连接 */
int proxy_slave_is_connected(proxy_slave_ctx_t *ctx);

/* 获取 fd（-1 表示未连接） */
int proxy_slave_fd(proxy_slave_ctx_t *ctx);

#endif /* PROXY_SLAVE_H */
```

- [ ] **Step 2: 创建 `src/ebpf_proxy/proxy_slave.c`**

```c
#include "proxy_slave.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

void proxy_slave_init(proxy_slave_ctx_t *ctx, const char *host, int port) {
    if (!ctx) return;
    memset(ctx, 0, sizeof(*ctx));
    ctx->fd = -1;
    ctx->backoff_ms = PROXY_SLAVE_BACKOFF_INIT_MS;
    ctx->backoff_max_ms = PROXY_SLAVE_BACKOFF_MAX_MS;
    if (host) snprintf(ctx->host, sizeof(ctx->host), "%s", host);
    ctx->port = port;
}

int proxy_slave_connect(proxy_slave_ctx_t *ctx) {
    struct sockaddr_in addr;
    struct timeval tv;

    if (!ctx || ctx->host[0] == '\0' || ctx->port <= 0) return -1;
    if (ctx->fd >= 0) {
        close(ctx->fd);
        ctx->fd = -1;
    }

    ctx->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->fd < 0) {
        perror("ebpf-proxy: slave socket");
        return -1;
    }

    tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(ctx->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(ctx->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)ctx->port);
    if (inet_pton(AF_INET, ctx->host, &addr.sin_addr) <= 0) {
        fprintf(stderr, "ebpf-proxy: invalid slave host %s\n", ctx->host);
        close(ctx->fd); ctx->fd = -1; return -1;
    }

    if (connect(ctx->fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "ebpf-proxy: slave connect failed %s:%d (errno=%d), "
                "backoff %ums\n", ctx->host, ctx->port, errno, ctx->backoff_ms);
        close(ctx->fd); ctx->fd = -1;
        return -1;
    }

    fprintf(stderr, "ebpf-proxy: connected to slave %s:%d fd=%d\n",
            ctx->host, ctx->port, ctx->fd);
    ctx->backoff_ms = PROXY_SLAVE_BACKOFF_INIT_MS;
    return 0;
}

void proxy_slave_disconnect(proxy_slave_ctx_t *ctx) {
    if (!ctx || ctx->fd < 0) return;
    close(ctx->fd);
    ctx->fd = -1;
}

int proxy_slave_is_connected(proxy_slave_ctx_t *ctx) {
    return ctx && ctx->fd >= 0;
}

int proxy_slave_fd(proxy_slave_ctx_t *ctx) {
    return ctx ? ctx->fd : -1;
}
```

- [ ] **Step 3: 提交**

```bash
git add src/ebpf_proxy/proxy_slave.h src/ebpf_proxy/proxy_slave.c
git commit -m "feat(ebpf-proxy): slave 连接管理 — TCP 连接 + 指数退避重连"
```

---

### Task 4: 创建 ebpf-proxy 主入口 main.c

**Files:**
- Create: `src/ebpf_proxy/main.c`

**Interfaces:**
- Consumes: `proxy_cache.h` (cache_init/append/flush/destroy/stats), `proxy_slave.h` (proxy_slave_init/connect/disconnect/is_connected/fd)
- Produces: `ebpf-proxy` 可执行文件（其余文件由 Task 5 Makefile 整合）

- [ ] **Step 1: 创建 `src/ebpf_proxy/main.c`**

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/resource.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include "proxy_cache.h"
#include "proxy_slave.h"

/* ---- 状态 ---- */
typedef enum {
    STATE_FORWARDING = 0,
    STATE_BUFFERING  = 1,
} proxy_state_t;

static volatile int g_shutdown = 0;
static proxy_state_t g_state = STATE_FORWARDING;
static proxy_slave_ctx_t g_slave;
static cache_ctx_t g_cache;

/* BPF map fds */
static int g_client_ctl_fd = -1;
static int g_proxy_cfg_fd = -1;
static int g_client_stats_fd = -1;

/* ringbuf */
static struct bpf_object *g_bpf_obj = NULL;
static struct ring_buffer *g_rb = NULL;

/* kprobe links */
static struct bpf_link *g_kprobe_link = NULL;
static struct bpf_link *g_kretprobe_link = NULL;

/* config */
static char g_pin_path[256] = "/sys/fs/bpf/kvstore_repl_sockmap";
static char g_obj_path[256] = "build/replication/bpf/repl_client_capture.bpf.o";
static int g_master_pid = 0;
static int g_master_port = 0;
static unsigned int g_slave_ip = 0;
static int g_slave_port = 0;

/* ---- 信号处理 ---- */
static void signal_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* ---- 辅助函数 ---- */
static int open_pinned_map(const char *name, int *fd_out) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", g_pin_path, name);
    *fd_out = bpf_obj_get(path);
    return *fd_out >= 0 ? 0 : -1;
}

static int read_client_ctl_u64(__u32 key, __u64 *val) {
    if (g_client_ctl_fd < 0) return -1;
    return bpf_map_lookup_elem(g_client_ctl_fd, &key, val);
}

/* 从 proxy_cfg hash map 读 u64 值 */
static int read_proxy_cfg_u64(const char *key_str, __u64 *val) {
    char key[32] = {0};
    if (g_proxy_cfg_fd < 0) return -1;
    snprintf(key, sizeof(key), "%s", key_str);
    return bpf_map_lookup_elem(g_proxy_cfg_fd, key, val);
}

/* 写 client_ctl */
static int write_client_ctl_u64(__u32 key, __u64 val) {
    if (g_client_ctl_fd < 0) return -1;
    return bpf_map_update_elem(g_client_ctl_fd, &key, &val, BPF_ANY);
}

/* 轮询等待 proxy_cfg["master_pid"] 非零 */
static int wait_for_master_pid(int timeout_ms) {
    __u64 val = 0;
    int waited = 0;
    while (waited < timeout_ms) {
        if (read_proxy_cfg_u64("master_pid", &val) == 0 && val != 0) {
            g_master_pid = (int)val;
            return 0;
        }
        usleep(500000); /* 500ms */
        waited += 500;
    }
    return -1;
}

/* 从 proxy_cfg 读取 slave 地址 */
static int read_slave_addr(void) {
    __u64 addr = 0, port = 0;
    if (read_proxy_cfg_u64("slave_addr", &addr) != 0) return -1;
    if (read_proxy_cfg_u64("slave_port", &port) != 0) return -1;
    g_slave_ip = (unsigned int)addr;
    g_slave_port = (int)port;
    return (g_slave_ip != 0 && g_slave_port > 0) ? 0 : -1;
}

/* ---- ringbuf 回调 ---- */
static int ringbuf_callback(void *ctx, void *data, size_t len) {
    (void)ctx;
    if (len < 4) return 0;

    __u32 payload_len;
    memcpy(&payload_len, data, 4);
    unsigned char *payload = (unsigned char *)data + 4;
    size_t plen = (size_t)payload_len;

    if (payload_len == 0xFFFFFFFF) {
        /* magic flush signal */
        fprintf(stderr, "ebpf-proxy: REPLDONE detected, flushing cache...\n");
        int sent = cache_flush(&g_cache, proxy_slave_fd(&g_slave));
        if (sent >= 0) {
            g_state = STATE_FORWARDING;
            fprintf(stderr, "ebpf-proxy: cache flushed (%d items), "
                    "state=FORWARDING\n", sent);
        }
        return 0;
    }

    if (g_state == STATE_FORWARDING) {
        if (proxy_slave_is_connected(&g_slave)) {
            ssize_t n = send(proxy_slave_fd(&g_slave), payload, plen,
                             MSG_NOSIGNAL);
            if (n < 0) {
                fprintf(stderr, "ebpf-proxy: send to slave failed, "
                        "buffering\n");
                cache_append(&g_cache, payload, plen);
                g_state = STATE_BUFFERING;
            }
        } else {
            /* slave 未连接，缓存 */
            cache_append(&g_cache, payload, plen);
        }
    } else {
        /* BUFFERING 状态 */
        cache_append(&g_cache, payload, plen);
    }
    return 0;
}

/* 主循环 */
static void main_loop(void) {
    while (!g_shutdown) {
        int rc = ring_buffer__poll(g_rb, 100 /* ms */);
        if (rc < 0 && !g_shutdown) {
            fprintf(stderr, "ebpf-proxy: ring_buffer__poll error: %d\n", rc);
            break;
        }

        /* 检查 fullsync 状态变化 */
        __u64 fs_val = 0;
        if (read_client_ctl_u64(3, &fs_val) == 0) {
            if (fs_val == 1 && g_state == STATE_FORWARDING) {
                g_state = STATE_BUFFERING;
                fprintf(stderr, "ebpf-proxy: REPLSYNC detected, "
                        "state=BUFFERING\n");
            }
        }

        /* 检查 slave 连接 */
        if (!proxy_slave_is_connected(&g_slave)) {
            if (read_slave_addr() == 0) {
                char host[64];
                snprintf(host, sizeof(host), "%u.%u.%u.%u",
                         (g_slave_ip >> 24) & 0xFF, (g_slave_ip >> 16) & 0xFF,
                         (g_slave_ip >> 8) & 0xFF, g_slave_ip & 0xFF);
                proxy_slave_init(&g_slave, host, g_slave_port);

                /* 指数退避 */
                for (int attempt = 1; !g_shutdown; attempt++) {
                    if (proxy_slave_connect(&g_slave) == 0) break;
                    unsigned int delay = g_slave.backoff_ms;
                    if (delay < PROXY_SLAVE_BACKOFF_INIT_MS)
                        delay = PROXY_SLAVE_BACKOFF_INIT_MS;
                    if (delay > 5000) delay = 5000;
                    fprintf(stderr, "ebpf-proxy: reconnect attempt %d, "
                            "sleeping %ums\n", attempt, delay);
                    usleep(delay * 1000);
                    g_slave.backoff_ms *= 2;
                    if (g_slave.backoff_ms > g_slave.backoff_max_ms)
                        g_slave.backoff_ms = g_slave.backoff_max_ms;
                }
            }
        }

        /* 如果 slave 在线且是 FORWARDING 且有缓存数据，尝试 flush */
        if (g_state == STATE_FORWARDING &&
            proxy_slave_is_connected(&g_slave) &&
            g_cache.head) {
            cache_flush(&g_cache, proxy_slave_fd(&g_slave));
        }
    }
}

/* 退出清理 */
static void cleanup(void) {
    fprintf(stderr, "ebpf-proxy: shutting down...\n");

    /* detach kprobes */
    if (g_kprobe_link) { bpf_link__destroy(g_kprobe_link); g_kprobe_link = NULL; }
    if (g_kretprobe_link) { bpf_link__destroy(g_kretprobe_link); g_kretprobe_link = NULL; }

    /* flush 剩余缓存（best effort） */
    if (g_cache.head && proxy_slave_is_connected(&g_slave)) {
        fprintf(stderr, "ebpf-proxy: flushing remaining cache...\n");
        cache_flush(&g_cache, proxy_slave_fd(&g_slave));
    }
    cache_destroy(&g_cache);

    /* 断开 slave */
    proxy_slave_disconnect(&g_slave);

    /* 释放 BPF 资源 */
    if (g_rb) { ring_buffer__free(g_rb); g_rb = NULL; }
    if (g_bpf_obj) { bpf_object__close(g_bpf_obj); g_bpf_obj = NULL; }
    if (g_client_ctl_fd >= 0) { close(g_client_ctl_fd); g_client_ctl_fd = -1; }
    if (g_proxy_cfg_fd >= 0) { close(g_proxy_cfg_fd); g_proxy_cfg_fd = -1; }
    if (g_client_stats_fd >= 0) { close(g_client_stats_fd); g_client_stats_fd = -1; }

    fprintf(stderr, "ebpf-proxy: cleanup complete\n");
}

/* 加载并 attach BPF */
static int load_and_attach_bpf(void) {
    struct bpf_program *prog;

    g_bpf_obj = bpf_object__open_file(g_obj_path, NULL);
    if (libbpf_get_error(g_bpf_obj)) {
        fprintf(stderr, "ebpf-proxy: bpf_object__open_file failed\n");
        return -1;
    }

    if (bpf_object__load(g_bpf_obj) != 0) {
        fprintf(stderr, "ebpf-proxy: bpf_object__load failed\n");
        return -1;
    }

    /* pin maps */
    struct bpf_map *map;
    bpf_object__for_each_map(map, g_bpf_obj) {
        const char *name = bpf_map__name(map);
        if (!name) continue;
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", g_pin_path, name);
        unlink(path);
        bpf_map__pin(map, path);
    }

    /* 打开自己的 pinned maps */
    open_pinned_map("client_ctl", &g_client_ctl_fd);
    open_pinned_map("proxy_cfg", &g_proxy_cfg_fd);
    open_pinned_map("client_stats", &g_client_stats_fd);

    /* attach kprobe entry */
    prog = bpf_object__find_program_by_name(g_bpf_obj, "kprobe_client_recv_entry");
    if (!prog) { fprintf(stderr, "ebpf-proxy: find kprobe entry failed\n"); return -1; }
    g_kprobe_link = bpf_program__attach(prog);
    if (libbpf_get_error(g_kprobe_link)) {
        fprintf(stderr, "ebpf-proxy: attach kprobe entry failed\n");
        return -1;
    }

    /* attach kretprobe return */
    prog = bpf_object__find_program_by_name(g_bpf_obj, "kprobe_client_recv_return");
    if (!prog) { fprintf(stderr, "ebpf-proxy: find kretprobe return failed\n"); return -1; }
    g_kretprobe_link = bpf_program__attach(prog);
    if (libbpf_get_error(g_kretprobe_link)) {
        fprintf(stderr, "ebpf-proxy: attach kretprobe return failed\n");
        return -1;
    }

    /* 创建 ringbuf reader */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(g_bpf_obj, "client_cache_ringbuf");
    if (!rb_map) { fprintf(stderr, "ebpf-proxy: find ringbuf map failed\n"); return -1; }
    g_rb = ring_buffer__new(bpf_map__fd(rb_map), ringbuf_callback, NULL, NULL);
    if (!g_rb) { fprintf(stderr, "ebpf-proxy: ring_buffer__new failed\n"); return -1; }

    fprintf(stderr, "ebpf-proxy: BPF loaded, kprobes attached\n");
    return 0;
}

/* 打印使用说明 */
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [options]\n"
        "  --pin-path PATH   BPF map pin path (default: /sys/fs/bpf/kvstore_repl_sockmap)\n"
        "  --obj-path PATH   BPF object path (default: build/replication/bpf/repl_client_capture.bpf.o)\n"
        "  --help            Show this help\n",
        prog);
}

/* 解析命令行 */
static int parse_args(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
            usage(argv[0]); return 1;
        } else if (!strcmp(argv[i], "--pin-path") && i + 1 < argc) {
            snprintf(g_pin_path, sizeof(g_pin_path), "%s", argv[++i]);
        } else if (!strcmp(argv[i], "--obj-path") && i + 1 < argc) {
            snprintf(g_obj_path, sizeof(g_obj_path), "%s", argv[++i]);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]); return 1;
        }
    }
    return 0;
}

int main(int argc, char **argv) {
    int rc = parse_args(argc, argv);
    if (rc != 0) return rc;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    /* 提高 memlock 限制 */
    struct rlimit rlim = { RLIM_INFINITY, RLIM_INFINITY };
    setrlimit(RLIMIT_MEMLOCK, &rlim);

    cache_init(&g_cache);

    /* 确保 pin 目录存在 */
    mkdir(g_pin_path, 0755);

    /* 加载 BPF 并 pin maps */
    if (load_and_attach_bpf() != 0) {
        fprintf(stderr, "ebpf-proxy: BPF init failed\n");
        return 1;
    }

    /* 等待 master 写配置 */
    fprintf(stderr, "ebpf-proxy: waiting for master config...\n");
    if (wait_for_master_pid(30000) != 0) {
        fprintf(stderr, "ebpf-proxy: timeout waiting for master pid\n");
        cleanup();
        return 1;
    }

    /* 读 master port */
    __u64 port_val = 0;
    if (read_proxy_cfg_u64("master_port", &port_val) == 0) {
        g_master_port = (int)port_val;
    }

    /* 写 client_ctl（BPF kprobe 需要） */
    write_client_ctl_u64(1, (__u64)g_master_pid);
    write_client_ctl_u64(2, (__u64)g_master_port);

    fprintf(stderr, "ebpf-proxy: master pid=%d port=%d\n",
            g_master_pid, g_master_port);

    /* 尝试连接 slave */
    if (read_slave_addr() == 0) {
        char host[64];
        snprintf(host, sizeof(host), "%u.%u.%u.%u",
                 (g_slave_ip >> 24) & 0xFF, (g_slave_ip >> 16) & 0xFF,
                 (g_slave_ip >> 8) & 0xFF, g_slave_ip & 0xFF);
        proxy_slave_init(&g_slave, host, g_slave_port);
        proxy_slave_connect(&g_slave);
    }

    fprintf(stderr, "ebpf-proxy: entering main loop, state=FORWARDING\n");
    main_loop();
    cleanup();
    return 0;
}
```

- [ ] **Step 2: 提交**

```bash
git add src/ebpf_proxy/main.c
git commit -m "feat(ebpf-proxy): 主入口 — 启动流程 + 主循环 + ringbuf 回调 + 信号退出"
```

---

### Task 5: Makefile — 新增 ebpf-proxy 构建目标

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: `src/ebpf_proxy/main.c`, `src/ebpf_proxy/proxy_cache.c`, `src/ebpf_proxy/proxy_slave.c`
- Produces: `make ebpf-proxy` 命令

- [ ] **Step 1: 在 Makefile 中添加 ebpf-proxy target**

在 Makefile 的 `kvstore` target 之后添加：

```makefile
# ---- ebpf-proxy ----
EBPF_PROXY_SRC = src/ebpf_proxy/main.c src/ebpf_proxy/proxy_cache.c src/ebpf_proxy/proxy_slave.c
EBPF_PROXY_BIN = build/ebpf_proxy
EBPF_PROXY_CFLAGS = -Wall -Wextra -O2 -I./include

$(EBPF_PROXY_BIN): $(EBPF_PROXY_SRC)
	@mkdir -p build
	$(CC) $(EBPF_PROXY_CFLAGS) -o $@ $(EBPF_PROXY_SRC) -lbpf -lelf -lz -lpthread

ebpf-proxy: $(EBPF_PROXY_BIN)

# 将 client_capture bpf object 加入构建
CLIENT_CAPTURE_BPF_SRC = src/replication/bpf/repl_client_capture.bpf.c
CLIENT_CAPTURE_BPF_OBJ = build/replication/bpf/repl_client_capture.bpf.o

$(CLIENT_CAPTURE_BPF_OBJ): $(CLIENT_CAPTURE_BPF_SRC)
	@mkdir -p build/replication/bpf
	$(CLANG) $(BPF_KPROBE_CFLAGS) -c $< -o $@

client_capture_bpf: $(CLIENT_CAPTURE_BPF_OBJ)
```

- [ ] **Step 2: 构建 ebpf-proxy，验证编译通过**

```bash
make ebpf-proxy
```

Expected: 编译成功，生成 `build/ebpf_proxy`。

- [ ] **Step 3: 提交**

```bash
git add Makefile
git commit -m "build: 新增 ebpf-proxy 和 client_capture_bpf 构建目标"
```

---

### Task 6: Master 侧改动 — 删除旧 init + 写 proxy_cfg + REPLDONE handler

**Files:**
- Modify: `src/main/kvstore.c`

**Interfaces:**
- Consumes: `bpf_obj_get()` (来自 `kvs_repl_ebpf.c` 已有模式)
- Produces: master 写 `proxy_cfg` map；REPLDONE master-side handler

- [ ] **Step 1: 删除旧的 eBPF/kprobe 初始化代码**

在 `main()` 函数中（行 2373-2400），删除以下代码块：
- `if (g_cfg.ebpf_enabled) { ... repl_ebpf_init() ... }` (行 2373-2381)
- `if (g_cfg.kprobe_enabled && !strcasecmp(...)) { ... }` (行 2383-2393)
- `if (g_cfg.kprobe_enabled && g_cfg.role == ROLE_MASTER) { repl_client_capture_init() ... }` (行 2396-2400)

替换为：

```c
    /* 向 ebpf-proxy 传递 master 配置（通过 pinned proxy_cfg map） */
    if (g_cfg.role == ROLE_MASTER) {
        int proxy_cfg_fd = -1;
        char cfg_path[512];
        snprintf(cfg_path, sizeof(cfg_path), "%s/proxy_cfg", g_cfg.ebpf_pin_path);
        /* 等待 proxy 启动并 pin maps（最多 30s） */
        for (int retry = 0; retry < 60; retry++) {
            proxy_cfg_fd = bpf_obj_get(cfg_path);
            if (proxy_cfg_fd >= 0) break;
            usleep(500000);
        }
        if (proxy_cfg_fd >= 0) {
            __u64 val;
            char key[32];
            val = (__u64)getpid();
            snprintf(key, sizeof(key), "master_pid");
            bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
            val = (__u64)g_cfg.port;
            snprintf(key, sizeof(key), "master_port");
            bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
            fprintf(stderr, "master: wrote config to ebpf-proxy (pid=%d port=%d)\n",
                    getpid(), g_cfg.port);
            close(proxy_cfg_fd);
        } else {
            fprintf(stderr, "master: ebpf-proxy proxy_cfg not available, "
                    "continuing without ebpf-proxy\n");
        }
    }
```

删除文件顶部的相关 include（如果不再需要）：
- `#include "kvstore/replication/repl_kprobe.h"` — 如果 kprobe 相关函数不再直接调用，可以删除

- [ ] **Step 2: 修改 REPLSYNC handler — 写 slave 地址到 proxy_cfg**

在 `handle_parsed_command()` 的 REPLSYNC handler 中（行 1080-1133），在 `repl_add_slave(c)` 之前添加写 proxy_cfg 的逻辑。

找到 `repl_add_slave(c);` 这一行（约行 1112），在前面添加：

```c
        /* 向 ebpf-proxy 写入 slave 地址 */
        {
            char cfg_path[512];
            snprintf(cfg_path, sizeof(cfg_path), "%s/proxy_cfg", g_cfg.ebpf_pin_path);
            int proxy_cfg_fd = bpf_obj_get(cfg_path);
            if (proxy_cfg_fd >= 0) {
                __u64 val;
                char key[32];
                struct sockaddr_in peer;
                socklen_t peer_len = sizeof(peer);
                if (getpeername(c->fd, (struct sockaddr *)&peer, &peer_len) == 0) {
                    val = (__u64)ntohl(peer.sin_addr.s_addr);
                    snprintf(key, sizeof(key), "slave_addr");
                    bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
                    val = (__u64)ntohs(peer.sin_port);
                    snprintf(key, sizeof(key), "slave_port");
                    bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
                    fprintf(stderr, "master: wrote slave addr to ebpf-proxy\n");
                }
                close(proxy_cfg_fd);
            }
        }
```

- [ ] **Step 3: 修改 REPLDONE handler — master 侧处理 slave 发来的 REPLDONE**

替换现有 REPLDONE handler（行 1142-1145）：

```c
    if (!strcmp(cmd, "REPLDONE")) {
        /* Master 侧：slave 发来 REPLDONE 表示全量同步在 slave 侧已完成 */
        if (g_cfg.role == ROLE_MASTER && c && c->is_replica) {
            c->repl_fullsync_pending = 0;
            repl_rdma_log("master_repldone - slave fullsync complete");
            return 0;
        }
        /* Slave 侧兼容（旧代码路径，保留以防万一） */
        if (g_cfg.role == ROLE_SLAVE) {
            repl_rdma_log("slave_parse - REPLDONE (legacy)");
            repl_slave_finish_fullsync();
            return 0;
        }
        return 0;
    }
```

- [ ] **Step 4: 删除 queue_snapshot 中的 REPLDONE 发送和 client_capture 调用**

在 `queue_snapshot()` 函数中删除以下行：
- 行 571-572: `g_repl_fullsync_in_progress = 1;` 和 `repl_client_capture_set_fullsync(1);`
- 行 617-622: REPLDONE 构建和发送（`done = resp_build_cmd1(buf, buf_size, "REPLDONE"); ... if (repl_send_chunked(...) != 0)`)
- 行 624: `repl_client_capture_note_repldone();`
- 行 628-629: `g_repl_fullsync_in_progress = 0;` 和 `repl_client_capture_set_fullsync(0);`
- 行 634-638: RDMA 后 TCP 发送 REPLDONE
- 行 640-649: `repl_client_capture_flush_to_slave` 相关代码
- 行 662-663: `cache_flushed > 0` 相关逻辑

保留 KVSD 生成、header 发送、chunk 发送、backlog gap 回放逻辑。确保函数返回 0 时正确释放资源。

- [ ] **Step 5: 编译验证**

```bash
make clean && make
```

Expected: 编译成功。

- [ ] **Step 6: 提交**

```bash
git add src/main/kvstore.c
git commit -m "feat(master): 删除 eBPF/kprobe 内联初始化, 写 proxy_cfg, REPLDONE master handler, 移除 queue_snapshot 中的 REPLDONE 发送"
```

---

### Task 7: Slave 侧改动 — 全量同步完成后发送 REPLDONE

**Files:**
- Modify: `src/replication/kvs_repl.c`

**Interfaces:**
- Consumes: `g_slave_fd` (slave 到 master 的 TCP fd), `resp_build_cmd1()`
- Produces: `repl_slave_send_repldone()` 函数

- [ ] **Step 1: 新增 `repl_slave_send_repldone()` 函数**

在 `repl_slave_send_ack()` 函数之后（约行 2061）添加：

```c
static int repl_slave_send_repldone(void) {
    unsigned char cmd[128];
    size_t n;

    if (g_slave_fd < 0) return -1;

    n = resp_build_cmd1(cmd, sizeof(cmd), "REPLDONE");

    ssize_t sent = send(g_slave_fd, cmd, n, 0);
    if (sent == (ssize_t)n) {
        fprintf(stderr, "repl: slave sent REPLDONE to master\n");
        return 0;
    }
    fprintf(stderr, "repl: slave REPLDONE send failed: %s\n", strerror(errno));
    return -1;
}
```

- [ ] **Step 2: 修改 `repl_slave_finish_fullsync()`**

将函数末尾的 `repl_slave_send_ack();` 替换为 `repl_slave_send_repldone();`：

```c
void repl_slave_finish_fullsync(void) {
    // ... 前面的代码不变（KVSD 文件处理 + replay_dump_file + state save）...
    // 行 1978: 将 repl_slave_send_ack(); 替换为:
    repl_slave_send_repldone();
}
```

- [ ] **Step 3: 编译验证**

```bash
make clean && make
```

Expected: 编译成功。

- [ ] **Step 4: 提交**

```bash
git add src/replication/kvs_repl.c
git commit -m "feat(slave): 全量同步完成后由 slave 发送 REPLDONE 给 master"
```

---

### Task 8: 清理 kvs_repl_kprobe.c — 删除 master 侧 client_capture 代码

**Files:**
- Modify: `src/replication/kvs_repl_kprobe.c`

**Interfaces:**
- Consumes: (none — 删除代码)
- Produces: 清理后的 `kvs_repl_kprobe.c`，仅保留 slave 侧 kprobe+RDMA 逻辑

- [ ] **Step 1: 删除 master 侧 client_capture 函数**

删除以下函数（这些逻辑现在在 ebpf-proxy 中）：
- `repl_client_capture_init()` — 行 1458 附近
- `repl_client_capture_set_fullsync()` — 行 1590 附近
- `repl_client_capture_flush_to_slave()` — 行 1600 附近
- `repl_client_capture_cleanup()` — 行 1660 附近
- `repl_client_capture_note_repldone()` — 行 1118 附近
- `repl_client_capture_get_stats()` — 行 1718 附近
- 以及 `#if !KVS_ENABLE_KPROBE_RDMA` 分支中的 stub 函数

保留 slave 侧 kprobe+RDMA 相关代码（kprobe ringbuf poll、RDMA WRITE 转发等）。

- [ ] **Step 2: 删除 kvstore.c 中引用的全局变量**

在 `src/main/kvstore.c` 中删除：
- `volatile int g_repl_client_capture_active = 0;` (行 70)
- `int g_repl_capture_slave_fd = -1;` (行 73)
- 以及 `handle_parsed_command` 中 `g_repl_client_capture_active` 的引用 (行 1117-1120)

- [ ] **Step 3: 编译验证**

```bash
make clean && make
```

Expected: 编译成功，无 undefined reference。

- [ ] **Step 4: 提交**

```bash
git add src/replication/kvs_repl_kprobe.c src/main/kvstore.c
git commit -m "refactor: 从 master 进程删除 client_capture 相关代码"
```

---

### Task 9: 集成测试

**Files:**
- Modify: 无（手动测试）

- [ ] **Step 1: 准备测试环境**

确保两台机器或两个 terminal 窗口分别运行 master 和 slave。

```bash
# 编译所有组件
make clean && make && make ebpf-proxy && make client_capture_bpf
```

- [ ] **Step 2: 测试基本增量转发**

```bash
# Terminal 1: 启动 master
./kvstore --port 5160 --role master --ebpf-pin /sys/fs/bpf/kvstore_repl_sockmap

# Terminal 2: 启动 ebpf-proxy（等 master 启动后）
./build/ebpf_proxy --pin-path /sys/fs/bpf/kvstore_repl_sockmap

# Terminal 3: 启动 slave
./kvstore --port 5161 --role slave --master-host 127.0.0.1 --master-port 5160
```

验证：
```bash
# 客户端写入
redis-cli -p 5160 SET foo bar
# slave 侧查询
redis-cli -p 5161 GET foo
```

Expected: slave 返回 "bar"。

- [ ] **Step 3: 测试全量同步 + 缓存**

```bash
# 向 master 写入 20 个 key
for i in $(seq 1 20); do redis-cli -p 5160 SET key$i value$i; done

# 重启 slave（触发全量同步）
# 在 slave 全量同步期间，继续向 master 写入
redis-cli -p 5160 SET during_sync "this was written during fullsync"

# 验证全量同步后 slave 有所有 key（含 during_sync）
redis-cli -p 5161 GET during_sync
redis-cli -p 5161 GET key1
```

Expected: 所有 key 完整，`during_sync` 的值正确。

- [ ] **Step 4: 测试 ebpf-proxy 退出清理**

```bash
# 杀掉 ebpf-proxy
kill -TERM $(pidof ebpf_proxy)

# 验证 kprobe 已 detach（检查 /sys/kernel/debug/tracing/kprobe_events）
cat /sys/kernel/debug/tracing/kprobe_events | grep tcp_recvmsg
```

Expected: 无 proxy 注册的 kprobe（如果有其他进程注册的，不相关）。

- [ ] **Step 5: 运行现有回归测试**

```bash
make test_repl_basic
make test_repl_gap
```

Expected: 测试通过（可能需要调整测试预期，因为 slave 发送 REPLDONE 改变了时序）。

- [ ] **Step 6: 提交测试结果**

```bash
git add -u
git commit -m "test: ebpf-proxy 集成测试通过 + 现有回归测试验证"
```

---

## 实现顺序

```
Task 1 (BPF 程序) 
  → Task 2 (proxy_cache)
    → Task 3 (proxy_slave)
      → Task 4 (main.c)
        → Task 5 (Makefile)
          → Task 6 (master kvstore.c)
            → Task 7 (slave kvs_repl.c)
              → Task 8 (清理 kvs_repl_kprobe.c)
                → Task 9 (集成测试)
```

Tasks 2-4 可以并行（都在 `src/ebpf_proxy/` 下，互相独立头文件）。
Tasks 6-8 可以并行（修改不同文件）。
