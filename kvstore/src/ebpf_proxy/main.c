#define _GNU_SOURCE   /* CPU_SET/CPU_ZERO 等 sched 宏 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <time.h>
#include <arpa/inet.h>
/* 前向声明: 系统 linux/bpf.h 版本过旧缺少这些类型 */
struct bpf_link_info;
enum bpf_link_type;

#include <bpf/bpf.h>
#include <bpf/libbpf.h>

#include "proxy_slave.h"
#include "proxy_commit.h"
#include "kvstore/replication/client_ctl.h"

/* Commit Gate 捕获记录头：cookie(8) + recv_seq(8) + payload_len(4) + flags(4) */
#define CAPTURE_RECORD_HDR_SZ 24

/* ---- 多副本 fan-out ----
 * Master 只产生一份有序已提交流（Commit Gate），proxy 为每个 Slave 维护：
 *   - 独立数据连接（proxy → slave 的 master_port+1）
 *   - 独立发送 FIFO 与发送线程
 *   - 独立 phase（LIVE / RECOVERY）与 barrier 应答
 * 一个副本慢/断/全量重同步不会阻塞其他副本；恢复缓冲由 Commit Gate 按副本
 * 分开管理（proxy_commit_set_buffering），发送 FIFO 只承载 LIVE 数据。
 * 设计依据：docs/multi-slave-design.md。 */
#define PROXY_MAX_REPLICAS        KVS_MAX_REPLICA_SLOTS
/* 单副本 LIVE FIFO 内存上限：超过即隔离该副本（写 slot FAULT），由 Master
 * 只拒绝它并重新同步；全局其他副本继续。 */
#define PROXY_REPL_QUEUE_MAX      (128u * 1024 * 1024)
#define PROXY_REPL_BATCH_MAX      256
#define PROXY_REPL_BATCH_BYTES_MAX (512u * 1024)
#define PROXY_REPL_LINGER_US      200
/* 所有副本积压合计的高/低水位：只对整体内存压力做上游背压，
 * 不让单个慢副本永久背压所有客户端（per-replica 有独立 hard limit）。 */
#define PROXY_TOTAL_HIGH_BYTES    (192u * 1024 * 1024)
#define PROXY_TOTAL_LOW_BYTES     (128u * 1024 * 1024)
/* 全局转发积压背压：阈值要比旧版单队列 4MB 高——per-replica FIFO 由各自线程
 * 消费，单个慢副本在 128MB hard limit 前不应长期拖慢其他副本；64MB 只用于
 * 全局限速（proxy 主线程/内存整体跟不上），慢副本仍由 per-replica 上限隔离。 */
#define PROXY_FWDQ_BP_HIGH        (64u * 1024 * 1024)
#define PROXY_FWDQ_BP_LOW         (16u * 1024 * 1024)
/* barrier 处理等待在途 writev 的封顶时间。超过说明该副本数据通道已经卡死：
 * 隔离该副本（写入 slot FAULT），避免拖住 Master 的屏障握手。等待发生在共享
 * 主线程上，因此设得较短（健康副本 512KB 批次远小于此值）。 */
#define PROXY_BARRIER_QUIESCE_MS  1000

/* ---- replication session 身份 ----
 * 全由 master 通过 pinned client_ctl map 下发：
 *   client_ctl[8]  SESSION_VALID  当前 session 是否有效
 *   client_ctl[9]  SESSION_ID     当前 session 身份
 */
static uint64_t g_session_id = 0;
static int g_session_valid = 0;

/* ---- Commit Gate / capture gate ---- */
#define CLIENT_CTL_KEY_CAPTURE_ENABLE  KVS_CTL_CAPTURE_ENABLE
#define CLIENT_CTL_KEY_CAPTURE_FAULT   KVS_CTL_CAPTURE_FAULT
#define CLIENT_CTL_KEY_COMMIT_FAULT    KVS_CTL_COMMIT_FAULT

/* proxy_stats 键（与 repl_client_capture.bpf.c 的 map 定义对应） */
#define PST_FORWARDED_BYTES  0
#define PST_ENQUEUED_BYTES   1
#define PST_FWDQ_BYTES       2
#define PST_CACHE_BYTES      3
#define PST_DROPPED_BYTES    4
#define PST_RECONNECTS       5
#define PST_CAPTURE_DROP     6
#define PST_COMMIT_DESC      7
#define PST_COMMIT_WRITES    8
#define PST_COMMIT_PENDING   9
#define PST_COMMIT_TIMEOUT   10
#define PST_COMMIT_DESYNC    11
#define PST_COMMIT_GAP       12
#define PST_COMMIT_RECOVERY  13
#define PST_COMMIT_NEXT_G    14
#define PST_COMMIT_CHANNEL   15
#define PST_CACHE_OVER_HIGH  16
#define PST_CACHE_HARD_HIT   17
#define PST_CONN_CTX         18
#define PST_PENDING_CMD      19
#define PST_OLDEST_PENDING   20
#define PST_MEM_HARD_HIT     21

/* client_stats 私有区间（与 BPF 的 0~15 分开）：聚合所有副本的恢复/FIFO 统计 */
#define CSTAT_CACHE_BYTES      16
#define CSTAT_CACHE_NODES      17
#define CSTAT_CACHE_DROPPED    18
#define CSTAT_CACHE_DROP_BYTES 19
#define CSTAT_CACHE_MAX_BYTES  20
#define CSTAT_CACHE_INVALID    21

/* capture gate：drop=1 时丢弃所有 ringbuf/队列/缓存中的增量数据，
 * 等 Master 完成 REPLSYNC 后重新打开，避免残缺命令进入 slave。 */
static volatile int g_shutdown = 0;
static volatile int g_capture_drop = 0;
static unsigned long long g_dropped_bytes = 0;
static unsigned long long g_commit_fault_code = 0;
static int g_commit_fault_handled = 0;

static void commit_fault_cb(int reason);
static void commit_replica_fault_cb(uint64_t replica_id, int reason);

/* ---- 跨线程计数（原子） ---- */
static unsigned long long g_enq_bytes = 0;
static unsigned long long g_sent_bytes = 0;
static unsigned long long g_slave_reconnects = 0;
static unsigned long long g_fwdq_bp_count = 0;
static unsigned long long g_rb_bp_count = 0;
static unsigned long long g_writev_calls = 0;
static unsigned long long g_eagain_count = 0;
static unsigned long long g_poll_count = 0;
static unsigned long long g_poll_timeout = 0;
static unsigned long long g_poll_out = 0;
static unsigned long long g_poll_err = 0;
static unsigned long long g_partial_write = 0;
static __u64 g_fwdq_bp_state = 0;
static __u64 g_cache_bp_state = 0;

/* ---- 每个副本的发送 FIFO ---- */
typedef struct proxy_repl_node_s {
    struct proxy_repl_node_s *next;
    size_t off;          /* 已部分写出的偏移（writev 失败回退时保留） */
    size_t len;
    unsigned char data[];
} proxy_repl_node_t;

enum { REPL_PHASE_LIVE = 0, REPL_PHASE_RECOVERY = 1 };

typedef struct {
    int id;
    int used;                        /* proxy_cfg 中 replica_gen != 0 */
    uint32_t addr;                   /* peer.sin_addr.s_addr（网络序原始 u32） */
    int port;
    unsigned long long gen;          /* proxy_cfg 下发的 generation */
    unsigned long long conn_gen;     /* 本地连接代次：地址变化/强制重连时 +1 */

    int phase;                       /* LIVE / RECOVERY（barrier 期只入 commit recovery） */
    int resumed;                     /* 收到该副本的 RESUME */
    int failed;                      /* 已隔离，等待 Master 重同步（slot FAULT=1） */
    int fault_pending;               /* 发送线程发现不可确定投递，待主线程落控制面 */
    int fault_reason;
    unsigned long long barrier_last; /* 上次观察到的 slot BARRIER seq（0=未立） */
    unsigned long long barrier_gen;  /* 每次 barrier up +1（在途批次据此判断是否过期） */
    unsigned long long barrier_seq;  /* 已 ack 的 barrier seq（RESUME 代次校验用） */
    unsigned long long token;        /* proxy 数据入口认证 token（0=旧协议/不认证） */

    proxy_slave_ctx_t slave;         /* 仅发送线程访问 */
    int connected;                   /* 仅发送线程写，持 lock 读 */
    int in_flight;                   /* 有 writev 批次在途（barrier 需等待静默） */

    proxy_repl_node_t *head, *tail;
    size_t bytes;                    /* FIFO 中尚未发送的字节（按 off 计） */
    unsigned long long nodes;

    unsigned int backoff_ms;
    int stop;
    pthread_t tid;
    pthread_mutex_t lock;
    pthread_cond_t cond;             /* 数据/状态变化 */
    pthread_cond_t idle;             /* in_flight 归零 */
    unsigned long long reconnects;
    unsigned long long overflow_count;
    unsigned long long dropped_bytes;
    unsigned long long enq_bytes;
    unsigned long long sent_bytes;
} proxy_replica_t;

static proxy_replica_t g_reps[PROXY_MAX_REPLICAS];
static size_t g_repl_bytes_peak = 0;

/* BPF map fds */
static int g_client_ctl_fd = -1;
static int g_proxy_cfg_fd = -1;
static int g_client_stats_fd = -1;
static int g_proxy_stats_fd = -1;

/* ringbuf */
static struct bpf_object *g_bpf_obj = NULL;
static struct ring_buffer *g_rb = NULL;
static struct bpf_link *g_fexit_link = NULL;
static void *g_rb_meta = MAP_FAILED;
#define RB_BACKPRESSURE_LOW  (8u * 1024 * 1024)

/* config */
static char g_pin_path[256] = "/sys/fs/bpf/kvstore_repl_sockmap";
static char g_obj_path[256] = "build/replication/bpf/repl_client_capture.bpf.o";
static int g_master_pid = 0;
static int g_master_port = 0;
static int g_master_pid_arg = 0;
static int g_master_port_arg = 0;
static char g_pid_file[256] = {0};
static char g_commit_sock_path[512] = {0};

/* ---- 基础工具 ---- */
static void signal_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

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

static int write_client_ctl_u64(__u32 key, __u64 val) {
    if (g_client_ctl_fd < 0) return -1;
    return bpf_map_update_elem(g_client_ctl_fd, &key, &val, BPF_ANY);
}

static int read_proxy_cfg_u64(const char *key_str, __u64 *val) {
    char key[32] = {0};
    if (g_proxy_cfg_fd < 0) return -1;
    snprintf(key, sizeof(key), "%s", key_str);
    return bpf_map_lookup_elem(g_proxy_cfg_fd, key, val);
}

static void write_proxy_stat_u64(__u32 key, __u64 val) {
    if (g_proxy_stats_fd < 0) return;
    bpf_map_update_elem(g_proxy_stats_fd, &key, &val, BPF_ANY);
}

static void rb_meta_init(int rb_fd) {
    long page = sysconf(_SC_PAGESIZE);
    g_rb_meta = mmap(NULL, (size_t)page * 2, PROT_READ, MAP_SHARED, rb_fd, 0);
    if (g_rb_meta == MAP_FAILED) {
        fprintf(stderr, "ebpf-proxy: mmap ringbuf meta failed: %s\n", strerror(errno));
        g_rb_meta = NULL;
    }
}

static size_t ringbuf_occupancy(void) {
    if (!g_rb_meta || g_rb_meta == MAP_FAILED) return 0;
    const __u64 *m = (const __u64 *)g_rb_meta;
    __u64 prod = m[0];
    __u64 cons = m[513];
    return prod > cons ? (size_t)(prod - cons) : 0;
}

/* ---- FIFO ---- */
static void replica_drop_fifo_locked(proxy_replica_t *r) {
    proxy_repl_node_t *n = r->head;
    r->head = r->tail = NULL;
    r->bytes = 0;
    r->nodes = 0;
    while (n) {
        proxy_repl_node_t *nx = n->next;
        free(n);
        n = nx;
    }
}

static size_t replica_remaining(const proxy_repl_node_t *n) {
    return n->len - n->off;
}

/* 需要持 lock。返回 0 成功；-1 超限/OOM（调用方隔离该副本）。 */
static int replica_enqueue_locked(proxy_replica_t *r, const unsigned char *buf, size_t len) {
    if (r->bytes + len > PROXY_REPL_QUEUE_MAX) return -1;
    proxy_repl_node_t *n = (proxy_repl_node_t *)malloc(sizeof(*n) + len);
    if (!n) return -1;
    n->next = NULL;
    n->off = 0;
    n->len = len;
    memcpy(n->data, buf, len);
    if (r->tail) r->tail->next = n; else r->head = n;
    r->tail = n;
    r->bytes += len;
    r->nodes++;
    return 0;
}

/* 需要持 lock：立即停止该 slot 的发送并标记故障。
 * 不在发送线程里触碰 Commit Gate 全局状态（非线程安全）；主线程
 * poll_replica_faults() 统一执行 proxy_commit_set_replica(0)/slot FAULT/断开。
 * Master 会在 REPLACK 或周期 fault 检查中拒绝该副本，Slave 重连后 gen 变化
 * 会重置本上下文。 */
static void replica_mark_fault_locked(proxy_replica_t *r, int reason) {
    if (r->failed) return;
    r->failed = 1;
    r->fault_pending = 1;
    r->fault_reason = reason;
    r->overflow_count++;
    replica_drop_fifo_locked(r);
    pthread_cond_broadcast(&r->cond);
}

/* 主线程：把 fault_pending 落到控制面。 */
static void replica_apply_fault(proxy_replica_t *r) {
    pthread_mutex_lock(&r->lock);
    int pending = r->fault_pending;
    int used = r->used;
    int reason = r->fault_reason;
    r->fault_pending = 0;
    pthread_mutex_unlock(&r->lock);
    if (!pending) return;
    proxy_commit_set_replica((uint64_t)r->id, 0);
    if (used) write_client_ctl_u64(KVS_CTL_SLOT_FAULT(r->id), 1);
    proxy_slave_mark_down(&r->slave, "replica fault");
    fprintf(stderr, "ebpf-proxy: replica slot %d isolated reason=%d "
                    "(other replicas continue)\n", r->id, reason);
}

static void poll_replica_faults(void) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        pthread_mutex_lock(&r->lock);
        int pending = r->fault_pending;
        pthread_mutex_unlock(&r->lock);
        if (pending) replica_apply_fault(r);
    }
}

static size_t replicas_total_bytes(void) {
    size_t total = 0;
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        if (pthread_mutex_trylock(&r->lock) == 0) {
            total += r->bytes;
            pthread_mutex_unlock(&r->lock);
        }
    }
    if (total > g_repl_bytes_peak) g_repl_bytes_peak = total;
    return total;
}

/* ---- Commit Gate 落地：按副本 fan-out ----
 * 返回 0 表示该副本已接收（入 LIVE FIFO 或由 Commit Gate 放入恢复缓冲）；
 * 副本级失败（超限/OOM）就地隔离并返回 0，绝不让一个副本拖垮全局流。
 * 只有 proxy 完全无法归属/捕获已关闭时才返回 -1（fail-closed）。 */
static int commit_emit(uint64_t replica_id, uint64_t global_seq,
                       const unsigned char *buf, size_t len) {
    (void)global_seq;
    if (replica_id >= PROXY_MAX_REPLICAS) return -1;
    if (g_capture_drop) { g_dropped_bytes += len; return -1; }

    proxy_replica_t *r = &g_reps[replica_id];
    pthread_mutex_lock(&r->lock);
    if (!r->used) {
        r->dropped_bytes += len;
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    if (r->failed) {
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    if (replica_enqueue_locked(r, buf, len) != 0) {
        replica_mark_fault_locked(r, 1);
        pthread_mutex_unlock(&r->lock);
        return 0;
    }
    r->enq_bytes += len;
    __atomic_fetch_add(&g_enq_bytes, len, __ATOMIC_RELAXED);
    pthread_cond_signal(&r->cond);
    pthread_mutex_unlock(&r->lock);
    return 0;
}

/* Commit Gate 的 per-replica 恢复缓冲溢出：与 main FIFO 超限同样处理。 */
static void commit_replica_fault_cb(uint64_t replica_id, int reason) {
    (void)reason;
    if (replica_id >= PROXY_MAX_REPLICAS) return;
    proxy_replica_t *r = &g_reps[replica_id];
    pthread_mutex_lock(&r->lock);
    replica_mark_fault_locked(r, 5);
    pthread_mutex_unlock(&r->lock);
}

static void commit_fault_cb(int reason) {
    if (g_shutdown) return;   /* 退出阶段通道 EOF 不算故障 */
    g_commit_fault_code = (unsigned long long)reason;
    g_capture_drop = 1;
    write_client_ctl_u64(CLIENT_CTL_KEY_COMMIT_FAULT, (__u64)reason);
    fprintf(stderr, "ebpf-proxy: commit gate fault reason=%d\n", reason);
}

/* RESUME：Commit Gate 已把该副本恢复缓冲中 <=marker 的记录丢弃，并把其余记录
 * 经 commit_emit 放进 FIFO。在交付前把 phase 置 LIVE，保证发送线程按序送出。 */
static int commit_resume_cb(uint64_t replica_id, uint64_t replica_gen,
                            uint64_t barrier_seq, uint64_t marker) {
    if (replica_id >= PROXY_MAX_REPLICAS) return 0;
    proxy_replica_t *r = &g_reps[replica_id];
    int ok;
    pthread_mutex_lock(&r->lock);
    /* 只接受当前 slot generation + 当前 barrier seq 的 RESUME。 */
    ok = r->used && !r->failed && r->gen == replica_gen &&
         r->barrier_seq == barrier_seq;
    if (ok) {
        r->phase = REPL_PHASE_LIVE;
        r->resumed = 1;
        pthread_cond_broadcast(&r->cond);
    }
    unsigned long long cur_gen = r->gen, cur_seq = r->barrier_seq;
    pthread_mutex_unlock(&r->lock);
    if (ok) {
        fprintf(stderr, "ebpf-proxy: replica slot %llu RESUME gen=%llu seq=%llu "
                        "marker=%llu -> LIVE\n", (unsigned long long)replica_id,
                (unsigned long long)replica_gen, (unsigned long long)barrier_seq,
                (unsigned long long)marker);
    } else {
        fprintf(stderr, "ebpf-proxy: replica slot %llu RESUME gen=%llu seq=%llu "
                        "mismatch (current gen=%llu seq=%llu) — ignored\n",
                (unsigned long long)replica_id, (unsigned long long)replica_gen,
                (unsigned long long)barrier_seq, cur_gen, cur_seq);
    }
    return ok;
}

/* ---- 副本生命周期 ---- */
static void replica_configure(proxy_replica_t *r, uint32_t addr, int port,
                              unsigned long long gen, unsigned long long token) {
    pthread_mutex_lock(&r->lock);
    replica_drop_fifo_locked(r);
    r->used = 1;
    r->addr = addr;
    r->port = port;
    r->gen = gen;
    r->token = token;
    r->conn_gen++;
    r->phase = REPL_PHASE_RECOVERY;
    r->resumed = 0;
    r->failed = 0;
    r->fault_pending = 0;
    r->fault_reason = 0;
    r->barrier_last = 0;
    r->barrier_seq = 0;
    r->backoff_ms = 0;
    proxy_commit_set_replica((uint64_t)r->id, 1);
    proxy_commit_set_buffering((uint64_t)r->id, 1);
    pthread_cond_broadcast(&r->cond);
    pthread_mutex_unlock(&r->lock);
    write_client_ctl_u64(KVS_CTL_SLOT_ACK(r->id), 0);
    fprintf(stderr, "ebpf-proxy: replica slot %d configured %u.%u.%u.%u:%d gen=%llu\n",
            r->id, addr & 0xFF, (addr >> 8) & 0xFF, (addr >> 16) & 0xFF,
            (addr >> 24) & 0xFF, port, gen);
}

static void replica_deactivate(proxy_replica_t *r) {
    pthread_mutex_lock(&r->lock);
    replica_drop_fifo_locked(r);
    r->used = 0;
    r->gen = 0;
    r->conn_gen++;
    r->phase = REPL_PHASE_RECOVERY;
    r->resumed = 0;
    r->failed = 0;
    r->fault_pending = 0;
    r->fault_reason = 0;
    r->barrier_last = 0;
    r->barrier_seq = 0;
    r->token = 0;
    pthread_cond_broadcast(&r->cond);
    pthread_mutex_unlock(&r->lock);
    proxy_commit_set_replica((uint64_t)r->id, 0);
    fprintf(stderr, "ebpf-proxy: replica slot %d deactivated\n", r->id);
}

/* 扫描 proxy_cfg 的 per-slot 注册：replica_gen.<slot> 是提交点（最后写），
 * gen=0/addr=0 表示注销。每次变化都重置该 slot 的本地状态。 */
static void poll_replicas(void) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        char key[32];
        __u64 gen = 0, addr = 0, port = 0, token = 0;

        snprintf(key, sizeof(key), "replica_gen.%d", i);
        if (read_proxy_cfg_u64(key, &gen) != 0) gen = 0;
        snprintf(key, sizeof(key), "slave_addr.%d", i);
        if (read_proxy_cfg_u64(key, &addr) != 0) addr = 0;
        snprintf(key, sizeof(key), "slave_port.%d", i);
        if (read_proxy_cfg_u64(key, &port) != 0) port = 0;
        snprintf(key, sizeof(key), "replica_token.%d", i);
        if (read_proxy_cfg_u64(key, &token) != 0) token = 0;

        if (gen == 0 || addr == 0 || port == 0) {
            /* 单副本兼容：老 Master 只写 slave_addr/slave_port。slot 0 回退读取。 */
            if (i == 0 && gen == 0) {
                __u64 laddr = 0, lport = 0;
                if (read_proxy_cfg_u64("slave_addr", &laddr) == 0) addr = laddr;
                if (read_proxy_cfg_u64("slave_port", &lport) == 0) port = lport;
            }
            if (addr != 0 && port != 0 && i == 0 && gen == 0) {
                /* 兼容模式：用地址变化作为隐式 generation。 */
                gen = ((unsigned long long)addr << 32) | (unsigned long long)port;
            } else if (r->used) {
                replica_deactivate(r);
                continue;
            } else {
                continue;
            }
        }

        if (r->used && r->gen == gen && r->addr == (uint32_t)addr &&
            r->port == (int)port && r->token == (unsigned long long)token)
            continue;
        if ((uint32_t)addr == 0 || port == 0) {
            if (r->used) replica_deactivate(r);
            continue;
        }
        replica_configure(r, (uint32_t)addr, (int)port, gen, (unsigned long long)token);
    }
}

/* ---- 发送线程：每副本独立连接/FIFO/退避 ---- */
/* 发送一个批次；batch_gen 为取批次时的 barrier_gen。 */
static void replica_send_batch(proxy_replica_t *r, proxy_repl_node_t **batch, int n,
                               unsigned long long batch_gen) {
    (void)batch_gen;   /* 不再跨连接续发：任何发送错误都直接隔离该 slot */
    struct iovec iov[PROXY_REPL_BATCH_MAX];
    size_t total = 0;
    for (int i = 0; i < n; i++) {
        iov[i].iov_base = batch[i]->data + batch[i]->off;
        iov[i].iov_len = replica_remaining(batch[i]);
        total += iov[i].iov_len;
    }

    size_t sent = 0;
    int idx = 0;
    int gave_up = 0;
    while (idx < n && sent < total) {
        ssize_t w = proxy_slave_writev(&r->slave, &iov[idx], n - idx);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                __atomic_fetch_add(&g_eagain_count, 1, __ATOMIC_RELAXED);
                for (;;) {
                    int sfd = proxy_slave_fd(&r->slave);
                    if (sfd < 0) { gave_up = 1; break; }
                    struct pollfd pfd = {.fd = sfd, .events = POLLOUT};
                    __atomic_fetch_add(&g_poll_count, 1, __ATOMIC_RELAXED);
                    int pr = poll(&pfd, 1, 200);
                    if (pr < 0) {
                        if (errno == EINTR) continue;
                        gave_up = 1; break;
                    }
                    if (pr == 0) { __atomic_fetch_add(&g_poll_timeout, 1, __ATOMIC_RELAXED); continue; }
                    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
                        __atomic_fetch_add(&g_poll_err, 1, __ATOMIC_RELAXED);
                        gave_up = 1; break;
                    }
                    __atomic_fetch_add(&g_poll_out, 1, __ATOMIC_RELAXED);
                    break;
                }
                if (!gave_up) continue;
            }
            gave_up = 1;
            break;
        }
        if (w == 0) { gave_up = 1; break; }
        __atomic_fetch_add(&g_writev_calls, 1, __ATOMIC_RELAXED);
        sent += (size_t)w;
        __atomic_fetch_add(&g_sent_bytes, (unsigned long long)w, __ATOMIC_RELAXED);
        ssize_t left = w;
        while (idx < n && left >= (ssize_t)iov[idx].iov_len) {
            left -= iov[idx].iov_len;
            idx++;
        }
        if (idx < n && left > 0) {
            iov[idx].iov_base = (char *)iov[idx].iov_base + left;
            iov[idx].iov_len -= (size_t)left;
            __atomic_fetch_add(&g_partial_write, 1, __ATOMIC_RELAXED);
        }
    }

    /* 把 "已发送字节" 折算成节点状态：整节点释放，部分节点前移 off。 */
    size_t consumed = sent;
    int keep_from = n;
    for (int i = 0; i < n; i++) {
        size_t rem = replica_remaining(batch[i]);
        if (consumed >= rem) {
            consumed -= rem;
            free(batch[i]);
        } else {
            batch[i]->off += consumed;
            consumed = 0;
            keep_from = i;
            break;
        }
    }

    /* 已发送字节折算后的节点：整节点已在上面 free；keep_from 起是未完整发送的
     * 尾部。TCP 连接一旦出错，无法证明 Slave 对“半条 RESP”执行到了哪里，
     * 因此**绝不能**把 off 跨新连接续发（新连接 parser 会从命令中间开始）。
     * 直接丢弃该 slot 的全部未发数据并 fail-closed，由 Slave 重连后按已确认
     * offset 走 REPLSYNC。 */
    pthread_mutex_lock(&r->lock);
    r->in_flight = 0;
    r->sent_bytes += sent;
    for (int i = keep_from; i < n; i++) free(batch[i]);
    if (gave_up) {
        r->dropped_bytes += (unsigned long long)(total - sent);
        replica_mark_fault_locked(r, 3);   /* 3 = 数据通道不可确定投递 */
    } else {
        int drained = proxy_slave_drain(&r->slave);
        if (drained != 0)
            replica_mark_fault_locked(r, 4); /* 4 = 对端关闭/数据通道 EOF */
    }
    r->connected = 0;
    pthread_cond_broadcast(&r->idle);
    pthread_mutex_unlock(&r->lock);

    if (gave_up) {
        proxy_slave_mark_down(&r->slave, "replica writev indeterminate");
        fprintf(stderr, "ebpf-proxy: replica slot %d send failed (sent=%zu/%zu), "
                        "slot isolated — slave must resync\n", r->id, sent, total);
    } else if (r->failed) {
        proxy_slave_mark_down(&r->slave, "replica peer closed");
        fprintf(stderr, "ebpf-proxy: replica slot %d data channel closed by peer, "
                        "slot isolated — slave must resync\n", r->id);
    }
}

static void free_batch_nodes(proxy_repl_node_t **batch, int n) {
    for (int i = 0; i < n; i++) free(batch[i]);
}

static void *replica_thread_main(void *arg) {
    proxy_replica_t *r = (proxy_replica_t *)arg;
    unsigned long long local_gen = 0;
    uint32_t local_addr = 0;
    int local_port = 0;
    int connected = 0;
    unsigned int backoff = PROXY_SLAVE_BACKOFF_INIT_MS;
    proxy_repl_node_t *batch[PROXY_REPL_BATCH_MAX];

    for (;;) {
        pthread_mutex_lock(&r->lock);
        /* 等待：需要连接/断开、或 LIVE 且有数据 */
        for (;;) {
            if (r->stop) break;
            if (!r->used || r->failed) {
                if (connected) break;                  /* 需要断开 */
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_nsec += 200L * 1000L * 1000L;
                if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
                pthread_cond_timedwait(&r->cond, &r->lock, &ts);
                continue;
            }
            if (r->conn_gen != local_gen || !connected) break;   /* 需要（重）连 */
            if (r->phase == REPL_PHASE_LIVE && r->head) break;   /* 有数据 */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += (long)PROXY_REPL_LINGER_US * 1000L;
            if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
            pthread_cond_timedwait(&r->cond, &r->lock, &ts);
        }
        int stop = r->stop;
        int used = r->used;
        int failed = r->failed;
        unsigned long long want_gen = r->conn_gen;
        uint32_t want_addr = r->addr;
        int want_port = r->port;
        pthread_mutex_unlock(&r->lock);

        if (stop) break;

        if (connected && (!used || failed || want_gen != local_gen)) {
            proxy_slave_disconnect(&r->slave);
            connected = 0;
            pthread_mutex_lock(&r->lock);
            r->connected = 0;
            pthread_mutex_unlock(&r->lock);
        }
        if (!used || failed) continue;
        if (want_gen != local_gen) {
            local_gen = want_gen;
            backoff = PROXY_SLAVE_BACKOFF_INIT_MS;
        }
        if (!connected) {
            char host[64];
            if (want_addr != local_addr || want_port != local_port) {
                snprintf(host, sizeof(host), "%u.%u.%u.%u",
                         want_addr & 0xFF, (want_addr >> 8) & 0xFF,
                         (want_addr >> 16) & 0xFF, (want_addr >> 24) & 0xFF);
                proxy_slave_init(&r->slave, host, want_port);
                local_addr = want_addr;
                local_port = want_port;
            }
            if (proxy_slave_connect(&r->slave) == 0) {
                /* 认证 proxy 数据入口：普通客户端无法得知 token。失败即该 slot
                 * fail-closed（Master 会拒绝并让 Slave 重新 REPLSYNC）。 */
                unsigned long long tok;
                pthread_mutex_lock(&r->lock);
                tok = r->token;
                pthread_mutex_unlock(&r->lock);
                if (tok != 0 && proxy_slave_handshake(&r->slave, tok) != 0) {
                    fprintf(stderr, "ebpf-proxy: replica slot %d REPLDATA handshake "
                                    "failed — slot isolated\n", r->id);
                    pthread_mutex_lock(&r->lock);
                    replica_mark_fault_locked(r, 6);
                    pthread_mutex_unlock(&r->lock);
                    proxy_slave_disconnect(&r->slave);
                    connected = 0;
                    continue;
                }
                connected = 1;
                backoff = PROXY_SLAVE_BACKOFF_INIT_MS;
                pthread_mutex_lock(&r->lock);
                r->connected = 1;
                r->reconnects++;
                pthread_cond_broadcast(&r->cond);
                pthread_mutex_unlock(&r->lock);
                __atomic_fetch_add(&g_slave_reconnects, 1, __ATOMIC_RELAXED);
            } else {
                /* 连接退避不能等 r->cond：入队会持续 signal，退避会被数据
                 * 唤醒变成热循环 connect。分片睡眠并在状态变化时提前退出。 */
                unsigned int delay = backoff;
                if (delay < PROXY_SLAVE_BACKOFF_INIT_MS) delay = PROXY_SLAVE_BACKOFF_INIT_MS;
                for (unsigned int slept = 0; slept < delay && !g_shutdown; slept += 50) {
                    usleep(50 * 1000);
                    pthread_mutex_lock(&r->lock);
                    int changed = (r->stop || !r->used || r->failed ||
                                   r->conn_gen != local_gen);
                    pthread_mutex_unlock(&r->lock);
                    if (changed) break;
                }
                if (backoff < PROXY_SLAVE_BACKOFF_MAX_MS) backoff *= 2;
                if (backoff > PROXY_SLAVE_BACKOFF_MAX_MS) backoff = PROXY_SLAVE_BACKOFF_MAX_MS;
                continue;
            }
        }

        /* 取一批数据（只在 LIVE 时发送） */
        pthread_mutex_lock(&r->lock);
        if (r->stop || !r->used || r->failed || r->conn_gen != local_gen ||
            r->phase != REPL_PHASE_LIVE || !r->head) {
            pthread_mutex_unlock(&r->lock);
            continue;
        }
        unsigned long long batch_gen = r->barrier_gen;
        int n = 0;
        size_t bytes = 0;
        while (n < PROXY_REPL_BATCH_MAX && r->head) {
            size_t rem = replica_remaining(r->head);
            if (n > 0 && bytes + rem > PROXY_REPL_BATCH_BYTES_MAX) break;
            proxy_repl_node_t *node = r->head;
            r->head = node->next;
            if (!r->head) r->tail = NULL;
            r->bytes -= rem;
            r->nodes--;
            node->next = NULL;
            batch[n++] = node;
            bytes += rem;
        }
        r->in_flight = 1;
        pthread_mutex_unlock(&r->lock);

        if (n > 0) replica_send_batch(r, batch, n, batch_gen);
        else free_batch_nodes(batch, n);
    }
    if (connected) proxy_slave_disconnect(&r->slave);
    return NULL;
}

/* ---- barrier 轮询 ----
 * Master 写 slot BARRIER=<单调 seq> 后等待 slot ACK==seq。proxy 必须先把该副本
 * 切到 RECOVERY（Commit Gate 开始按副本缓存）并等待在途 writev 结束，再回显
 * seq 应答，保证应答之后不会再有 barrier 前数据流到 Slave。BARRIER=0 表示放行。 */
static void replica_barrier_poll(proxy_replica_t *r) {
    if (!r->used || r->failed) return;
    __u64 v = 0;
    if (read_client_ctl_u64(KVS_CTL_SLOT_BARRIER(r->id), &v) != 0) return;
    int b = v ? 1 : 0;
    if (v == r->barrier_last) return;

    if (b) {
        pthread_mutex_lock(&r->lock);
        r->phase = REPL_PHASE_RECOVERY;
        r->resumed = 0;
        r->barrier_gen++;
        replica_drop_fifo_locked(r);
        proxy_commit_set_buffering((uint64_t)r->id, 1);
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += PROXY_BARRIER_QUIESCE_MS / 1000;
        deadline.tv_nsec += (long)(PROXY_BARRIER_QUIESCE_MS % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) { deadline.tv_sec++; deadline.tv_nsec -= 1000000000L; }
        int timed_out = 0;
        while (r->in_flight && !r->stop) {
            if (pthread_cond_timedwait(&r->idle, &r->lock, &deadline) == ETIMEDOUT) {
                timed_out = 1;
                break;
            }
        }
        r->barrier_last = v;
        r->barrier_seq = v;
        if (timed_out) replica_mark_fault_locked(r, 2);
        pthread_mutex_unlock(&r->lock);
        if (timed_out) replica_apply_fault(r);
        write_client_ctl_u64(KVS_CTL_SLOT_ACK(r->id), v);   /* ACK 回显 seq */
        fprintf(stderr, "ebpf-proxy: replica slot %d barrier up seq=%llu (RECOVERY%s)\n",
                r->id, (unsigned long long)v,
                timed_out ? ", isolated after quiesce timeout" : "");
    } else {
        pthread_mutex_lock(&r->lock);
        int was_resumed = r->resumed;
        if (r->resumed) {
            /* 正常路径：RESUME 已把恢复数据放行并把 phase 置 LIVE。 */
            proxy_commit_set_buffering((uint64_t)r->id, 0);
            r->phase = REPL_PHASE_LIVE;
        } else {
            /* barrier 在 RESUME 之前被清零（stale reset / 竞态）：**不能**释放
             * 恢复缓冲，也不能切 LIVE。否则随后到达的 RESUME 无处可放行，或者
             * 旧恢复记录会排在新 LIVE 数据之后（乱序）。保持恢复态，等 RESUME
             * 或下一次 barrier up（up 会重置恢复缓冲）来推进。 */
            replica_drop_fifo_locked(r);
        }
        r->resumed = 0;
        r->barrier_last = 0;
        pthread_cond_broadcast(&r->cond);
        pthread_mutex_unlock(&r->lock);
        write_client_ctl_u64(KVS_CTL_SLOT_ACK(r->id), 0);   /* 离开恢复态 */
        fprintf(stderr, "ebpf-proxy: replica slot %d barrier down (%s)\n",
                r->id, was_resumed ? "LIVE" : "still RECOVERY, awaiting RESUME");
    }
}

static void poll_replica_barriers(void) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++)
        replica_barrier_poll(&g_reps[i]);
}

/* ---- 副本集合级操作 ---- */
/* 丢弃所有副本的 LIVE FIFO、断开数据通道、切回恢复态。用于 capture gate 关闭、
 * session 失效/切换：旧 session 的数据绝不能跨 session 重放。 */
static void replicas_reset_all(int rearm_buffering) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        if (!r->used) continue;
        pthread_mutex_lock(&r->lock);
        replica_drop_fifo_locked(r);
        r->phase = REPL_PHASE_RECOVERY;
        r->resumed = 0;
        r->failed = 0;
        r->conn_gen++;          /* 强制发送线程断开重连 */
        r->barrier_last = 0;
        pthread_cond_broadcast(&r->cond);
        pthread_mutex_unlock(&r->lock);
        if (rearm_buffering) {
            proxy_commit_set_replica((uint64_t)r->id, 1);
            proxy_commit_set_buffering((uint64_t)r->id, 1);
        }
    }
}

/* ---- capture gate ---- */
static void proxy_capture_drop_pending(void) {
    g_capture_drop = 1;

    /* 排空 ringbuf 并丢弃（回调此时直接 return） */
    if (g_rb) {
        for (int i = 0; i < 128; ++i) {
            int n = ring_buffer__consume(g_rb);
            if (n <= 0) break;
        }
    }

    replicas_reset_all(0);
    /* Commit Gate：丢弃所有 per-cookie framing/pending/recovery 状态；
     * 重新打开时 Master 会先发 BASELINE 再 enable capture。 */
    proxy_commit_purge();
    /* purge 清了恢复缓冲与 buffering 位：为仍注册的副本重新武装，
     * 这样 barrier 建立前的提交也会进恢复缓冲而不是直接下送。 */
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        if (!r->used) continue;
        proxy_commit_set_replica((uint64_t)r->id, 1);
        proxy_commit_set_buffering((uint64_t)r->id, 1);
    }
    fprintf(stderr, "ebpf-proxy: capture gate closed, all replica FIFOs dropped\n");
}

static void proxy_capture_resume(void) {
    g_capture_drop = 0;
    g_commit_fault_code = 0;
    g_commit_fault_handled = 0;
    fprintf(stderr, "ebpf-proxy: capture gate reopened\n");
}

/* capture gate：Master 写 client_ctl[7]=1 才捕获；[13]=1 表示截断/不可信。 */
static void proxy_capture_gate_poll(void) {
    __u64 gate_en = 0, gate_fault = 0;
    read_client_ctl_u64(CLIENT_CTL_KEY_CAPTURE_ENABLE, &gate_en);
    read_client_ctl_u64(CLIENT_CTL_KEY_CAPTURE_FAULT, &gate_fault);
    int want_drop = (gate_en != 1 || gate_fault != 0);
    if (want_drop && !g_capture_drop)
        proxy_capture_drop_pending();
    else if (!want_drop && g_capture_drop)
        proxy_capture_resume();
}

/* ---- ringbuf 回调：解析 Commit Gate 记录头，交给 commit 模块 ---- */
static int ringbuf_callback(void *ctx, void *data, size_t len) {
    (void)ctx;
    if (g_capture_drop) {
        __u64 en = 0, ft = 0;
        int gate_on = (read_client_ctl_u64(CLIENT_CTL_KEY_CAPTURE_ENABLE, &en) == 0 && en == 1 &&
                       read_client_ctl_u64(CLIENT_CTL_KEY_CAPTURE_FAULT, &ft) == 0 && ft == 0);
        if (!gate_on || g_commit_fault_code) {
            g_dropped_bytes += len;
            return 0;
        }
        g_capture_drop = 0;
        fprintf(stderr, "ebpf-proxy: capture gate opened mid-consume — accepting record\n");
    }
    if (len < CAPTURE_RECORD_HDR_SZ) return 0;

    const unsigned char *p = (const unsigned char *)data;
    uint64_t cookie, recv_seq;
    uint32_t payload_len, flags;
    memcpy(&cookie, p, 8);
    memcpy(&recv_seq, p + 8, 8);
    memcpy(&payload_len, p + 16, 4);
    memcpy(&flags, p + 20, 4);
    (void)flags;
    if (payload_len == 0 || CAPTURE_RECORD_HDR_SZ + payload_len > len) {
        fprintf(stderr, "[commit] drop bad record len=%zu payload=%u\n", len, payload_len);
        return 0;
    }

    proxy_commit_on_record(cookie, recv_seq,
                           p + CAPTURE_RECORD_HDR_SZ, payload_len);
    return 0;
}

/* 轮询 master 下发的 replication session 状态（client_ctl[8]/[9]）。
 * session 变化 = 一次新的 REPLSYNC 建立（新 id）或旧 session 失效（valid=0）。
 * 两种情况都必须丢弃旧 FIFO：前者防跨 session 重放，后者是 session end 清理。 */
static void poll_session_state(void) {
    if (g_client_ctl_fd < 0) return;
    __u64 sid = 0, valid = 0;
    __u32 k;
    k = KVS_CTL_SESSION_ID;
    if (bpf_map_lookup_elem(g_client_ctl_fd, &k, &sid) != 0) return;
    k = KVS_CTL_SESSION_VALID;
    bpf_map_lookup_elem(g_client_ctl_fd, &k, &valid);

    int sid_changed = ((uint64_t)sid != g_session_id);
    int valid_changed = ((int)valid != g_session_valid);
    if (!sid_changed && !valid_changed) return;

    if (sid_changed) {
        fprintf(stderr, "ebpf-proxy: replication session change %llu -> %llu\n",
                (unsigned long long)g_session_id, (unsigned long long)sid);
        g_session_id = (uint64_t)sid;
        /* 新 session 只能由 REPLSYNC 驱动：每个 slot 会重新注册（gen 变化）并建立
         * 新 barrier，由 configure/barrier-up 清理旧 FIFO。这里**不能**清恢复缓冲：
         * barrier 已 ack、新 session 的第一批提交可能正被缓存，清掉就是静默丢数据。
         * 只有 session 失效（valid=0）才无条件丢弃。 */
        if (!valid) replicas_reset_all(1);
    }
    g_session_valid = (int)valid;
    if (!g_session_valid) {
        fprintf(stderr, "ebpf-proxy: session invalidated by master, data discarded\n");
        replicas_reset_all(1);
    }
}

/* ---- proxy_stats / client_stats ---- */
static void proxy_stats_update(void) {
    if (g_proxy_stats_fd < 0) return;
    write_proxy_stat_u64(PST_FORWARDED_BYTES,
        __atomic_load_n(&g_sent_bytes, __ATOMIC_RELAXED));
    write_proxy_stat_u64(PST_ENQUEUED_BYTES,
        __atomic_load_n(&g_enq_bytes, __ATOMIC_RELAXED));

    size_t total = replicas_total_bytes();
    unsigned long long nodes = 0, reconnects = 0, overflow = 0, dropped = 0;
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        if (r->used) {
            reconnects += r->reconnects;
            nodes += r->nodes;
            dropped += r->dropped_bytes;
        }
        overflow += r->overflow_count;
    }
    write_proxy_stat_u64(PST_FWDQ_BYTES, (__u64)total);
    write_proxy_stat_u64(PST_CACHE_BYTES, (__u64)total);
    write_proxy_stat_u64(PST_DROPPED_BYTES,
        (__u64)(g_dropped_bytes + dropped));
    write_proxy_stat_u64(PST_RECONNECTS,
        (__u64)(__atomic_load_n(&g_slave_reconnects, __ATOMIC_RELAXED)));
    write_proxy_stat_u64(PST_CAPTURE_DROP, (__u64)(g_capture_drop ? 1 : 0));
    write_proxy_stat_u64(PST_CACHE_OVER_HIGH,
        (__u64)((g_cache_bp_state == 1) ? 1 : 0));
    write_proxy_stat_u64(PST_CACHE_HARD_HIT, (__u64)overflow);

    proxy_commit_stats_t cs;
    proxy_commit_get_stats(&cs);
    write_proxy_stat_u64(PST_COMMIT_DESC, (__u64)cs.descriptors);
    write_proxy_stat_u64(PST_COMMIT_WRITES, (__u64)cs.writes);
    write_proxy_stat_u64(PST_COMMIT_PENDING, (__u64)cs.pending);
    write_proxy_stat_u64(PST_COMMIT_TIMEOUT, (__u64)cs.timeout);
    write_proxy_stat_u64(PST_COMMIT_DESYNC, (__u64)cs.desync);
    write_proxy_stat_u64(PST_COMMIT_GAP, (__u64)cs.gap);
    write_proxy_stat_u64(PST_COMMIT_RECOVERY, (__u64)cs.recovery_bytes);
    write_proxy_stat_u64(PST_COMMIT_NEXT_G, cs.next_global_seq);
    write_proxy_stat_u64(PST_COMMIT_CHANNEL, (__u64)(cs.channel_connected ? 1 : 0));
    write_proxy_stat_u64(PST_CONN_CTX, (__u64)cs.conn_ctx_count);
    write_proxy_stat_u64(PST_PENDING_CMD, (__u64)cs.pending_cmd_count);
    write_proxy_stat_u64(PST_OLDEST_PENDING, (__u64)cs.oldest_pending_ms);
    write_proxy_stat_u64(PST_MEM_HARD_HIT, (__u64)cs.memory_hard_limit_count);
    (void)nodes;
}

static void publish_cache_stats(void) {
    if (g_client_stats_fd < 0) return;
    size_t total = replicas_total_bytes();
    unsigned long long nodes = 0, dropped = 0, drop_bytes = 0, overflow = 0;
    int invalid = 0;
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        if (pthread_mutex_trylock(&r->lock) == 0) {
            nodes += r->nodes;
            dropped += r->dropped_bytes;
            if (r->failed) invalid = 1;
            pthread_mutex_unlock(&r->lock);
        }
        overflow += r->overflow_count;
    }
    drop_bytes = dropped;
    __u64 vals[6];
    vals[0] = (__u64)total;
    vals[1] = (__u64)nodes;
    vals[2] = dropped;
    vals[3] = drop_bytes;
    vals[4] = (__u64)g_repl_bytes_peak;
    vals[5] = (__u64)(invalid || overflow > 0 ? 1 : 0);
    for (__u32 i = 0; i < 6; i++) {
        __u32 key = CSTAT_CACHE_BYTES + i;
        bpf_map_update_elem(g_client_stats_fd, &key, &vals[i], BPF_ANY);
    }
}

/* 聚合背压：只按所有副本总量发上游背压；单个慢副本由 per-replica FIFO 上限
 * 隔离（slot FAULT → Master 只拒绝该副本）。 */
static void backpressure_poll(void) {
    size_t total = replicas_total_bytes();
    size_t pending = total;
    if (g_fwdq_bp_state == 0 && pending > PROXY_FWDQ_BP_HIGH) {
        g_fwdq_bp_state = 1;
        g_fwdq_bp_count++;
        write_client_ctl_u64(KVS_CTL_FWDQ_BACKPRESSURE, 1);
    } else if (g_fwdq_bp_state == 1 && pending < PROXY_FWDQ_BP_LOW) {
        g_fwdq_bp_state = 0;
        write_client_ctl_u64(KVS_CTL_FWDQ_BACKPRESSURE, 0);
    }
    if (g_cache_bp_state == 0 && total > PROXY_TOTAL_HIGH_BYTES) {
        g_cache_bp_state = 1;
        write_client_ctl_u64(KVS_CTL_CACHE_BACKPRESSURE, 1);
        fprintf(stderr, "ebpf-proxy: total replica backlog over high watermark, "
                        "signalling backpressure\n");
    } else if (g_cache_bp_state == 1 && total < PROXY_TOTAL_LOW_BYTES) {
        g_cache_bp_state = 0;
        write_client_ctl_u64(KVS_CTL_CACHE_BACKPRESSURE, 0);
    }
}

/* ---- 主循环 ---- */
static void main_loop(void) {
    unsigned int stats_tick = 0;
    unsigned int scan_tick = 0;
    while (!g_shutdown) {
        /* 1) 先发现 slot 注册，再处理该 slot 的 barrier（Master 等待 ACK）。 */
        if (++scan_tick >= 10) {
            scan_tick = 0;
            poll_replicas();
        }
        poll_replica_faults();
        proxy_capture_gate_poll();
        poll_session_state();

        /* 2) Commit Gate 控制面先于 ringbuf，且先于 barrier 边沿处理：
         * Master 的 barrier down（slot BARRIER=0）在 RESUME 之后才写，先处理
         * Commit Gate 才能保证 RESUME 已把恢复记录放行，随后 barrier down
         * 的 set_buffering(0) 不会把这些记录丢弃。 */
        proxy_commit_poll();
        if (g_commit_fault_code && !g_commit_fault_handled) {
            proxy_capture_drop_pending();
            g_commit_fault_handled = 1;
        }
        poll_replica_barriers();

        /* 3) capture BPF 用 BPF_RB_NO_WAKEUP：必须主动 consume。 */
        int rc = ring_buffer__poll(g_rb, 1 /* ms */);
        if (rc < 0 && !g_shutdown) {
            fprintf(stderr, "ebpf-proxy: ring_buffer__poll error: %d\n", rc);
            break;
        }
        if (ring_buffer__consume(g_rb) < 0 && !g_shutdown) {
            fprintf(stderr, "ebpf-proxy: ring_buffer__consume error\n");
            break;
        }

        /* ringbuf 背压清除（低水位，由消费端负责，避免自锁） */
        if (ringbuf_occupancy() < RB_BACKPRESSURE_LOW) {
            __u32 bp_key = KVS_CTL_RINGBUF_BACKPRESSURE;
            __u64 bp_now = 0;
            if (g_client_ctl_fd >= 0 &&
                bpf_map_lookup_elem(g_client_ctl_fd, &bp_key, &bp_now) == 0 &&
                bp_now == 1) {
                g_rb_bp_count++;
                write_client_ctl_u64(KVS_CTL_RINGBUF_BACKPRESSURE, 0);
            }
        }

        backpressure_poll();

        if (++stats_tick >= 500) {
            stats_tick = 0;
            publish_cache_stats();
        }
    }
}

/* ---- 心跳 / 诊断线程 ---- */
static pthread_t g_hb_thread = 0;
static void *heartbeat_thread_main(void *arg) {
    (void)arg;
    __u64 hb = 0;
    while (!g_shutdown) {
        hb++;
        write_client_ctl_u64(KVS_CTL_PROXY_HEARTBEAT, hb);
        proxy_stats_update();
        usleep(100000);
    }
    return NULL;
}

static void *diag_thread_main(void *arg) {
    (void)arg;
    __u64 last_enq = 0, last_sent = 0;
    unsigned long long last_us = 0;
    while (!g_shutdown) {
        usleep(1000000);
        struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
        unsigned long long now_us = (unsigned long long)ts.tv_sec * 1000000ULL
                                    + (unsigned long long)ts.tv_nsec / 1000ULL;
        __u64 enq = __atomic_load_n(&g_enq_bytes, __ATOMIC_RELAXED);
        __u64 sent = __atomic_load_n(&g_sent_bytes, __ATOMIC_RELAXED);
        double dt = last_us ? (double)(now_us - last_us) / 1e6 : 1.0;
        fprintf(stderr,
                "ebpf-proxy: [diag] replicas=%d fwdq=%zuMB ringbuf=%zuMB "
                "enq=%.1fMB/s sent=%.1fMB/s bp=%llu/%llu "
                "wv=%llu eagain=%llu poll=%llu(pout=%llu,timeout=%llu,err=%llu) partial=%llu\n",
                PROXY_MAX_REPLICAS,
                replicas_total_bytes() / (1024 * 1024),
                ringbuf_occupancy() / (1024 * 1024),
                (enq - last_enq) / dt / (1024 * 1024),
                (sent - last_sent) / dt / (1024 * 1024),
                g_fwdq_bp_count, g_rb_bp_count,
                __atomic_load_n(&g_writev_calls, __ATOMIC_RELAXED),
                __atomic_load_n(&g_eagain_count, __ATOMIC_RELAXED),
                __atomic_load_n(&g_poll_count, __ATOMIC_RELAXED),
                __atomic_load_n(&g_poll_out, __ATOMIC_RELAXED),
                __atomic_load_n(&g_poll_timeout, __ATOMIC_RELAXED),
                __atomic_load_n(&g_poll_err, __ATOMIC_RELAXED),
                __atomic_load_n(&g_partial_write, __ATOMIC_RELAXED));
        last_enq = enq; last_sent = sent; last_us = now_us;
    }
    return NULL;
}

static void diag_start(void) {
    if (getenv("EBPF_PROXY_DIAG") == NULL) return;
    pthread_t t;
    if (pthread_create(&t, NULL, diag_thread_main, NULL) != 0)
        fprintf(stderr, "ebpf-proxy: failed to start diag thread\n");
}

static void heartbeat_start(void) {
    if (pthread_create(&g_hb_thread, NULL, heartbeat_thread_main, NULL) != 0)
        fprintf(stderr, "ebpf-proxy: failed to start heartbeat thread\n");
}

/* ---- 副本线程启动/停止 ---- */
static void replicas_init(void) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        memset(r, 0, sizeof(*r));
        r->id = i;
        pthread_mutex_init(&r->lock, NULL);
        pthread_cond_init(&r->cond, NULL);
        pthread_cond_init(&r->idle, NULL);
        proxy_slave_init(&r->slave, NULL, 0);
        r->backoff_ms = PROXY_SLAVE_BACKOFF_INIT_MS;
    }
}

static void replicas_start(void) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        if (pthread_create(&g_reps[i].tid, NULL, replica_thread_main, &g_reps[i]) != 0)
            fprintf(stderr, "ebpf-proxy: failed to start replica thread %d\n", i);
    }
}

static void replicas_stop(void) {
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        pthread_mutex_lock(&r->lock);
        r->stop = 1;
        pthread_cond_broadcast(&r->cond);
        pthread_cond_broadcast(&r->idle);
        pthread_mutex_unlock(&r->lock);
    }
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++)
        pthread_join(g_reps[i].tid, NULL);
    for (int i = 0; i < PROXY_MAX_REPLICAS; i++) {
        proxy_replica_t *r = &g_reps[i];
        replica_drop_fifo_locked(r);
    }
}

/* ---- 退出清理 ---- */
static void cleanup(void) {
    fprintf(stderr, "ebpf-proxy: shutting down...\n");

    proxy_commit_cleanup();

    /* 0. 先 detach fexit — 停止新数据进入 ringbuf */
    if (g_fexit_link) { bpf_link__destroy(g_fexit_link); g_fexit_link = NULL; }

    /* 1. 排空 ringbuf 到发送 FIFO。副本线程此刻仍在运行，会把已提交数据
     *    继续发往各 Slave；若先停线程再 drain，入队数据会积在已停线程上。 */
    if (g_rb) {
        int drained = 0;
        for (int i = 0; i < 50; i++) {
            int n = ring_buffer__poll(g_rb, 100);
            if (n <= 0) break;
            drained += n;
        }
        if (drained > 0)
            fprintf(stderr, "ebpf-proxy: drained %d remaining ringbuf entries\n", drained);
    }

    /* 2. 停所有副本线程（各自尽力发完 FIFO） */
    replicas_stop();

    /* 3. 释放 BPF 资源 */
    if (g_rb) { ring_buffer__free(g_rb); g_rb = NULL; }
    if (g_bpf_obj) { bpf_object__close(g_bpf_obj); g_bpf_obj = NULL; }
    if (g_client_ctl_fd >= 0) { close(g_client_ctl_fd); g_client_ctl_fd = -1; }
    if (g_proxy_cfg_fd >= 0) { close(g_proxy_cfg_fd); g_proxy_cfg_fd = -1; }
    if (g_client_stats_fd >= 0) { close(g_client_stats_fd); g_client_stats_fd = -1; }
    if (g_proxy_stats_fd >= 0) { close(g_proxy_stats_fd); g_proxy_stats_fd = -1; }
    if (g_pid_file[0]) unlink(g_pid_file);

    fprintf(stderr, "ebpf-proxy: cleanup complete\n");
}

/* ---- 加载并 attach BPF ---- */
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

    struct bpf_map *map;
    bpf_object__for_each_map(map, g_bpf_obj) {
        const char *name = bpf_map__name(map);
        if (!name) continue;
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", g_pin_path, name);
        unlink(path);
        int rc = bpf_map__pin(map, path);
        if (rc != 0) {
            fprintf(stderr, "ebpf-proxy: pin map '%s' to %s failed: %s\n",
                    name, path, strerror(errno));
        } else {
            fprintf(stderr, "ebpf-proxy: pinned map '%s' to %s\n", name, path);
        }
    }

    if (open_pinned_map("client_ctl", &g_client_ctl_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open client_ctl failed\n");
    if (open_pinned_map("proxy_cfg", &g_proxy_cfg_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open proxy_cfg failed\n");
    if (open_pinned_map("client_stats", &g_client_stats_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open client_stats failed\n");
    if (open_pinned_map("proxy_stats", &g_proxy_stats_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open proxy_stats failed\n");

    prog = bpf_object__find_program_by_name(g_bpf_obj, "fexit_tcp_recvmsg");
    if (!prog) { fprintf(stderr, "ebpf-proxy: find fexit failed\n"); return -1; }
    g_fexit_link = bpf_program__attach(prog);
    if (libbpf_get_error(g_fexit_link)) {
        fprintf(stderr, "ebpf-proxy: attach fexit failed\n");
        return -1;
    }

    struct bpf_map *rb_map = bpf_object__find_map_by_name(g_bpf_obj, "client_cache_ringbuf");
    if (!rb_map) { fprintf(stderr, "ebpf-proxy: find ringbuf map failed\n"); return -1; }
    int rb_fd = bpf_map__fd(rb_map);
    rb_meta_init(rb_fd);
    g_rb = ring_buffer__new(rb_fd, ringbuf_callback, NULL, NULL);
    if (!g_rb) { fprintf(stderr, "ebpf-proxy: ring_buffer__new failed\n"); return -1; }

    fprintf(stderr, "ebpf-proxy: BPF loaded, fexit attached\n");
    return 0;
}

static int wait_for_master_pid(int timeout_ms) {
    __u64 val = 0;
    int waited = 0;
    while (waited < timeout_ms) {
        if (read_proxy_cfg_u64("master_pid", &val) == 0 && val != 0) {
            g_master_pid = (int)val;
            return 0;
        }
        usleep(500000);
        waited += 500;
    }
    return -1;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [options]\n"
        "  --pin-path PATH   BPF map pin path (default: /sys/fs/bpf/kvstore_repl_sockmap)\n"
        "  --obj-path PATH   BPF object path (default: build/replication/bpf/repl_client_capture.bpf.o)\n"
        "  --master-pid PID  Master pid (skip proxy_cfg polling)\n"
        "  --master-port P   Master port\n"
        "  --pid-file PATH   Write own pid (for Master restart handling)\n"
        "  --commit-sock P   Commit Gate Unix socket path (passed by Master spawn)\n"
        "  --help            Show this help\n",
        prog);
}

static int parse_args(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--help") || !strcmp(argv[i], "-h")) {
            usage(argv[0]); return 1;
        } else if (!strcmp(argv[i], "--pin-path") && i + 1 < argc) {
            snprintf(g_pin_path, sizeof(g_pin_path), "%s", argv[++i]);
        } else if (!strcmp(argv[i], "--obj-path") && i + 1 < argc) {
            snprintf(g_obj_path, sizeof(g_obj_path), "%s", argv[++i]);
        } else if (!strcmp(argv[i], "--master-pid") && i + 1 < argc) {
            g_master_pid_arg = (int)strtol(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--master-port") && i + 1 < argc) {
            g_master_port_arg = (int)strtol(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--pid-file") && i + 1 < argc) {
            snprintf(g_pid_file, sizeof(g_pid_file), "%s", argv[++i]);
        } else if (!strcmp(argv[i], "--commit-sock") && i + 1 < argc) {
            snprintf(g_commit_sock_path, sizeof(g_commit_sock_path), "%s", argv[++i]);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]); return 1;
        }
    }
    return 0;
}

static void write_pid_file(void) {
    if (!g_pid_file[0]) return;
    FILE *f = fopen(g_pid_file, "w");
    if (!f) {
        fprintf(stderr, "ebpf-proxy: write pid file %s failed: %s\n",
                g_pid_file, strerror(errno));
        return;
    }
    fprintf(f, "%d\n", (int)getpid());
    fclose(f);
}

int main(int argc, char **argv) {
    int rc = parse_args(argc, argv);
    if (rc != 0) return rc;

    write_pid_file();

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    struct rlimit rlim = { RLIM_INFINITY, RLIM_INFINITY };
    setrlimit(RLIMIT_MEMLOCK, &rlim);

    replicas_init();

    mkdir(g_pin_path, 0755);

    if (load_and_attach_bpf() != 0) {
        fprintf(stderr, "ebpf-proxy: BPF init failed\n");
        return 1;
    }

    /* Commit Gate listener 必须在 client_ctl 等 map pin 之后才 bind：
     * Master 一旦 connect 就会写 capture_enable=1；若 map 尚未 pin/随后被
     * unlink+repin，arm 会写进旧 map，BPF 永远收不到 enable。 */
    if (proxy_commit_init(g_commit_sock_path[0] ? g_commit_sock_path : "./kvstore.commit.sock",
                          commit_emit, commit_fault_cb, commit_replica_fault_cb,
                          commit_resume_cb) != 0) {
        fprintf(stderr, "ebpf-proxy: commit listener init failed (Commit Gate disabled)\n");
    }

    if (g_master_pid_arg > 0) {
        g_master_pid = g_master_pid_arg;
        g_master_port = g_master_port_arg;
        fprintf(stderr, "ebpf-proxy: master config from argv (pid=%d port=%d)\n",
                g_master_pid, g_master_port);
    } else {
        fprintf(stderr, "ebpf-proxy: waiting for master config...\n");
        if (wait_for_master_pid(300000) != 0) {
            fprintf(stderr, "ebpf-proxy: timeout waiting for master pid\n");
            cleanup();
            return 1;
        }
        __u64 port_val = 0;
        if (read_proxy_cfg_u64("master_port", &port_val) == 0) {
            g_master_port = (int)port_val;
        }
    }

    write_client_ctl_u64(1, (__u64)g_master_pid);
    write_client_ctl_u64(2, (__u64)g_master_port);

    fprintf(stderr, "ebpf-proxy: master pid=%d port=%d replicas_max=%d\n",
            g_master_pid, g_master_port, PROXY_MAX_REPLICAS);

    /* 立即扫一次已注册的 slot（Master 可能已经写完 REPLSYNC 配置）。 */
    poll_replicas();

    heartbeat_start();
    diag_start();
    replicas_start();

    fprintf(stderr, "ebpf-proxy: entering main loop\n");
    main_loop();
    cleanup();
    return 0;
}
