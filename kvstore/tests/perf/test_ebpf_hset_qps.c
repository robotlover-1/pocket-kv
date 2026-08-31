/*
 * test_ebpf_hset_qps.c — eBPF 转发 QPS 对比测试（HSET 场景）
 *
 * 使用 redis-benchmark --hset 打真实 RESP 流量。
 *
 * 三种模式:
 *   none:  只处理 HSET，回 :1/:0（基准 QPS）
 *   sync:  HSET + 同步写 slave_fd（转发在主请求路径上）
 *   ebpf:  HSET + eBPF proxy（独立进程异步转发）
 *
 * 用法:
 *   sudo ./test_ebpf_hset_qps --mode all --payload 64 --count 50000
 *   sudo ./test_ebpf_hset_qps --mode ebpf --payload 64 --count 50000
 *
 * Slave 端预先启动 tcpsink:
 *   ./tcpsink -p 15901
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <math.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

/* ---- FNV-1a 64-bit (与 tcpsink 一致) ---- */
static inline uint64_t fnv1a_64(const unsigned char *data, int len) {
    uint64_t h = 14695981039346656037ULL;
    for (int i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* ---- 配置 ---- */
#define MASTER_PORT     15900
#define SLAVE_PORT      15901
#define BPF_PIN_PATH    "/sys/fs/bpf/kvstore_hset_qps_test"
#define EBPF_PROXY_BIN  "./build/ebpf_proxy"
/* 核隔离（T4 多核方案）：master handler 固定 CPU2（外部 taskset -c 2），
 * 转发线程 / proxy / 客户端各自独立核，不与 master 核争抢。 */
#define MASTER_CPU     2
#define FWD_THREAD_CPU 0
#define PROXY_CPU      0
#define CLIENT_CPU     3
#define CLIENT_CAPTURE_OBJ "build/replication/bpf/repl_client_capture.bpf.o"
#define HT_SIZE         65536
#define CONN_BUF_SZ     262144  /* 256KB per-connection buffer */

/* ---- 计时 ---- */
static inline double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000000.0 + (double)ts.tv_nsec / 1000.0;
}

/* ---- TCP helpers ---- */
static int set_nodelay(int fd) {
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}


/* ---- 简单哈希表 ---- */
typedef struct ht_node {
    struct ht_node *next;
    unsigned char *key;  /* "key\0field" */
    int key_len;
} ht_node_t;

static ht_node_t *g_ht[HT_SIZE];
static pthread_mutex_t g_ht_lock = PTHREAD_MUTEX_INITIALIZER;

static unsigned int ht_hash(const unsigned char *data, int len) {
    unsigned int h = 5381;
    for (int i = 0; i < len; i++)
        h = ((h << 5) + h) + data[i];
    return h % HT_SIZE;
}

static int ht_hset(const unsigned char *key, int kl,
                    const unsigned char *field, int fl) {
    int cl = kl + 1 + fl;
    unsigned char *combo = malloc((size_t)cl);
    memcpy(combo, key, kl);
    combo[kl] = 0;
    memcpy(combo + kl + 1, field, fl);

    unsigned int idx = ht_hash(combo, cl);

    pthread_mutex_lock(&g_ht_lock);
    ht_node_t *node = g_ht[idx];
    while (node) {
        if (node->key_len == cl && memcmp(node->key, combo, cl) == 0) {
            pthread_mutex_unlock(&g_ht_lock);
            free(combo);
            return 0;
        }
        node = node->next;
    }
    node = calloc(1, sizeof(*node));
    node->key  = combo;
    node->key_len = cl;
    node->next = g_ht[idx];
    g_ht[idx] = node;
    pthread_mutex_unlock(&g_ht_lock);
    return 1;
}

/* ---- RESP 命令扫描器 ----
 * 状态机直接扫字节流，arg 指针指向 buf 内部，不额外分配。
 * 单遍扫描，无回溯。
 *
 * 状态转换:
 *   HDR ('*') → ARGLEN (读数字) → BSIZE ('$' + 数字) → BDATA (读 N 字节)
 *                                                          ↓
 *                                                    cur_arg < arg_count? → BSIZE
 *                                                                         ↓
 *                                                                  cmd_complete = 1
 */

enum { RS_HDR, RS_ARGLEN, RS_BSIZE, RS_BDATA };

typedef struct {
    int state;
    int arg_count;
    int cur_arg;
    int cur_num;      /* 正在读的数字（arg count 或 bulk size） */
    unsigned char *args[8];
    int arg_lens[8];
} resp_scanner_t;

static void rs_init(resp_scanner_t *s) {
    memset(s, 0, sizeof(*s));
    s->state = RS_HDR;
}

/*
 * 扫描 buf[0..len-1]。找到完整命令返回 1，否则返回 0。
 * 出错返回 -1。
 * consumed: 该命令占用的字节数（含末尾 \r\n）。
 */
static int rs_scan(resp_scanner_t *s, const unsigned char *buf, int len,
                   int *consumed) {
    int i = 0;
    *consumed = 0;

    while (i < len) {
        unsigned char c = buf[i];

        if (s->state == RS_HDR) {
            if (c == '*') { s->state = RS_ARGLEN; s->cur_num = 0; }
            i++;
        } else if (s->state == RS_ARGLEN) {
            if (c >= '0' && c <= '9') {
                s->cur_num = s->cur_num * 10 + (c - '0');
                i++;
            } else if (c == '\r') { i++; }
            else if (c == '\n') {
                s->arg_count = s->cur_num;
                s->cur_arg = 0;
                s->state = (s->arg_count > 0 && s->arg_count <= 8)
                                ? RS_BSIZE : RS_HDR;
                i++;
            } else { return -1; }
        } else if (s->state == RS_BSIZE) {
            if (c == '$') { s->cur_num = 0; i++; }
            else if (c == '\r') { i++; }
            else if (c == '\n') {
                if (s->cur_num < 0 || s->cur_num > 65536) return -1;
                s->state = RS_BDATA;
                i++;
            } else if (c >= '0' && c <= '9') {
                s->cur_num = s->cur_num * 10 + (c - '0');
                i++;
            } else { return -1; }
        } else { /* RS_BDATA */
            int need = s->cur_num;
            int have = len - i;
            if (have < need + 2) return 0; /* 数据 + \r\n 不够 */

            s->args[s->cur_arg] = (unsigned char *)buf + i;
            s->arg_lens[s->cur_arg] = need;
            s->cur_arg++;
            i += need;

            /* 跳过 \r\n */
            if (buf[i] == '\r') i++;
            if (i < len && buf[i] == '\n') i++;

            if (s->cur_arg == s->arg_count) {
                *consumed = i;
                return 1;
            }
            s->state = RS_BSIZE;
        }
    }
    return 0;
}

/* ---- 全局状态 ---- */
static volatile int g_shutdown = 0;
static int g_mode = 0;           /* 0=none, 1=sync, 2=ebpf */
static int g_master_port = MASTER_PORT;
static int g_slave_port = SLAVE_PORT;
static int g_payload_size = 64;
static int g_pipeline = 1;        /* redis-benchmark -P 流水线深度 */
static int g_keyrange = 0;        /* redis-benchmark -r 键范围（0=跟随 count） */
static int g_req_count = 50000;
static int g_rounds = 5;
static int g_redis_clients = 50;
static int g_cpu = -1;
static const char *g_slave_host = "127.0.0.1";
static const char *g_ebpf_proxy_bin = EBPF_PROXY_BIN;
static const char *g_client_capture_obj = CLIENT_CAPTURE_OBJ;
static const char *g_csv_file = NULL;
static const char *g_hash_log = NULL;  /* master hash log 路径 */

/* sync 模式共享 slave fd（仅转发线程写） */
static int g_slave_fd = -1;

/* ---- sync 转发环形缓冲（Task 7：消除每命令 malloc/free，保留锁+condvar） ---- */
#define FWD_RING_SIZE (64 * 1024 * 1024)      /* 64MB 连续环形缓冲 */
#define FWD_BATCH_BYTES 16384                 /* 转发线程每次 writev 目标字节数 */
#define FWD_EMPTY_WAIT_US 200                 /* 空队列等待上限（µs）：攒批 drain */
#define FWD_MAX_IOV 512                       /* writev iovec 上限（每槽最多 2 段：payload wrap） */

static unsigned char *g_fwd_ring = NULL;      /* 预分配，fwd_thread_start 分配 */
static size_t g_fwd_ring_head = 0;            /* 转发线程读位置 */
static size_t g_fwd_tail = 0;                 /* handler 写位置（持锁） */

/* ---- 内联非阻塞转发（InazumaPlasma 式，实验 FWD_INLINE=1）----
 * 不加队列/转发线程：handler 持锁追加到写缓冲 + 非阻塞 send(MSG_DONTWAIT)。
 * 与"独立转发线程"方案做方法对比（是否更接近 none）。 */
static int g_fwd_inline = -1;                 /* 启动时从 getenv 初始化 */
static char g_slave_wbuf[8 * 1024 * 1024];    /* slave 写缓冲（InazumaPlasma 8MB 同款） */
static size_t g_slave_wlen = 0;
static pthread_mutex_t g_slave_wlock = PTHREAD_MUTEX_INITIALIZER;
static int g_slave_dead = 0;
static int g_fwd_stop = 0;
static int g_fwd_dead = 0;                    /* slave 写失败后置 1，后续入队丢弃 */
static pthread_t g_fwd_thread;
/* 诊断：fwd_enqueue 平均耗时（定位 sync P=1 跨机写耦合） */
static unsigned long long g_enq_ns = 0;
static unsigned long long g_enq_cnt = 0;

/* hash 日志（内容校验用） */
static FILE *g_hash_fp = NULL;
static long long g_hash_seq = 0;

/* ---- 信号 ---- */
static void signal_handler(int sig) { (void)sig; g_shutdown = 1; }

/* ---- eBPF proxy 管理 ---- */
static pid_t g_proxy_pid = 0;

static int proxy_start(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s 2>/dev/null; mkdir -p %s",
             BPF_PIN_PATH, BPF_PIN_PATH);
    system(cmd);

    pid_t pid = fork();
    if (pid < 0) { perror("fork ebpf-proxy"); return -1; }
    if (pid == 0) {
        /* proxy pin 到独立核 PROXY_CPU，不与 master 核争抢 */
        cpu_set_t cs; CPU_ZERO(&cs); CPU_SET(PROXY_CPU, &cs);
        if (sched_setaffinity(0, sizeof(cs), &cs) != 0)
            fprintf(stderr, "[test] proxy sched_setaffinity(%d) failed: %s\n",
                    PROXY_CPU, strerror(errno));
        execl(g_ebpf_proxy_bin, g_ebpf_proxy_bin,
              "--pin-path", BPF_PIN_PATH,
              "--obj-path", g_client_capture_obj, NULL);
        perror("exec ebpf-proxy"); _exit(1);
    }
    g_proxy_pid = pid;
    fprintf(stderr, "[test] ebpf-proxy PID=%d\n", pid);
    usleep(500000);
    return 0;
}

static void proxy_stop(void) {
    if (g_proxy_pid <= 0) return;
    kill(g_proxy_pid, SIGTERM);
    for (int i = 0; i < 60; i++) {
        if (waitpid(g_proxy_pid, NULL, WNOHANG) > 0) break;
        usleep(50000);
    }
    if (waitpid(g_proxy_pid, NULL, WNOHANG) == 0) {
        kill(g_proxy_pid, SIGKILL);
        waitpid(g_proxy_pid, NULL, 0);
    }
    g_proxy_pid = 0;
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s 2>/dev/null", BPF_PIN_PATH);
    system(cmd);
}

static int write_cfg_map(int fd, const char *name, __u64 val) {
    char key[32] = {0};
    snprintf(key, sizeof(key), "%s", name);
    return bpf_map_update_elem(fd, key, &val, BPF_ANY);
}

static int proxy_write_config(int master_pid) {
    char path[512];
    snprintf(path, sizeof(path), "%s/proxy_cfg", BPF_PIN_PATH);
    int fd = -1;
    for (int i = 0; i < 20; i++) {
        fd = bpf_obj_get(path);
        if (fd >= 0) break;
        usleep(100000);
    }
    if (fd < 0) {
        fprintf(stderr, "[test] proxy_cfg not found\n");
        return -1;
    }

    __u64 slave_ip = (__u64)(unsigned int)inet_addr(g_slave_host);
    if (slave_ip == (__u64)(unsigned int)-1)
        slave_ip = (127U << 24) | 1U;

    write_cfg_map(fd, "master_pid",  (__u64)master_pid);
    write_cfg_map(fd, "master_port", (__u64)g_master_port);
    write_cfg_map(fd, "slave_addr",  slave_ip);
    write_cfg_map(fd, "slave_port",  (__u64)g_slave_port);

    close(fd);
    fprintf(stderr, "[test] proxy_cfg: pid=%d slave=%s:%d\n",
            master_pid, g_slave_host, g_slave_port);
    return 0;
}

static int proxy_wait_ready(int timeout_ms) {
    char path[512];
    snprintf(path, sizeof(path), "%s/proxy_cfg", BPF_PIN_PATH);
    int waited = 0;
    while (waited < timeout_ms) {
        int fd = bpf_obj_get(path);
        if (fd >= 0) { close(fd); return 0; }
        usleep(100000);
        waited += 100;
    }
    return -1;
}

static int ebpf_wait_ringbuf_drain(int timeout_ms) {
    /* 等待 proxy 消费完 ringbuf。注意：不能另开 ring_buffer reader（BPF ringbuf
     * 单 consumer，harness 开 reader 会与 proxy 竞争并偷走其待消费记录，回调丢弃
     * 即数据丢失）。改为 mmap ringbuf 头页读 producer_pos/consumer_pos，相等即空。 */
    char path[512];
    snprintf(path, sizeof(path), "%s/client_cache_ringbuf", BPF_PIN_PATH);
    int map_fd = bpf_obj_get(path);
    if (map_fd < 0) return -1;
    long page = sysconf(_SC_PAGESIZE);
    void *meta = mmap(NULL, (size_t)page, PROT_READ, MAP_SHARED, map_fd, 0);
    if (meta == MAP_FAILED) { close(map_fd); return -1; }
    const __u64 *prod = (const __u64 *)meta;        /* ringbuf 页 0: producer_pos @0 */
    const __u64 *cons = (const __u64 *)meta + 1;    /*                consumer_pos @8 */
    int drained = 0;
    for (int i = 0; i < timeout_ms; i++) {
        if (__atomic_load_n(prod, __ATOMIC_ACQUIRE) ==
            __atomic_load_n(cons, __ATOMIC_ACQUIRE)) {
            drained = 1;
            break;
        }
        usleep(1000);
    }
    munmap(meta, (size_t)page);
    close(map_fd);
    return drained ? 0 : -1;
}

/* ---- 客户端连接处理 ----
 * 每个 client handler 线程有一个环形缓冲区:
 *   - append: 从 fd 读入新数据
 *   - consume: 扫描完整命令 → 执行 → 转发(sync)
 *
 * 注意: ebpf proxy 按 PID 过滤 tcp_recvmsg，本进程中所有线程
 * 的 read() 都会被 fexit 捕获（包括 accept 线程）。accept 线程
 * 只有 accept() 无 read()，所以不会误捕获。只有 client handler
 * 线程的 read() 会触发 fexit。
 */

typedef struct {
    unsigned char data[CONN_BUF_SZ];
    int head;   /* 写指针 */
    int tail;   /* 读指针 */
} conn_buf_t;

static inline int cb_avail(conn_buf_t *b) { return b->head - b->tail; }
static inline int cb_space(conn_buf_t *b) { return CONN_BUF_SZ - b->head; }

static void cb_compact(conn_buf_t *b) {
    if (b->tail == 0) return;
    int avail = cb_avail(b);
    if (avail > 0) memmove(b->data, b->data + b->tail, (size_t)avail);
    b->head = avail;
    b->tail = 0;
}

static void cb_consume(conn_buf_t *b, int n) { b->tail += n; }

typedef struct {
    int fd;
    int thread_id;
    int msgs_processed;
} client_ctx_t;

#define RESP_OK     ":1\r\n"
#define RESP_OK_LEN 4

/* ---- 单线程 epoll reactor（A1：对齐生产单线程 reactor）---- */
typedef struct reactor_conn_s {
    int fd;
    conn_buf_t in;                 /* 读缓冲 */
    char out[CONN_BUF_SZ];         /* 回包缓冲（非阻塞写） */
    size_t out_len, out_off;       /* 待发 / 已发偏移 */
    int epollout_reg;              /* 已注册 EPOLLOUT */
} reactor_conn_t;
/* 诊断：回包写 EAGAIN 卡顿计数（reactor 是否被客户端读回包速度卡住） */
static long g_epollout_regs = 0;   /* flush 因 EAGAIN 注册 EPOLLOUT 的次数 */
static long g_epollout_evts = 0;   /* EPOLLOUT 事件触发次数 */

/* ---- 无损背压（与生产 reactor 的 repl_ebpf_backpressure 对应）----
 * ebpf 模式下 proxy 的转发队列近满时置 client_ctl[4]=1，reactor 在 read 前停手等待，
 * 避免 ringbuf 溢出让 fexit 捕获静默丢数据（高 P 丢数据的修复）。 */
static int g_backp_fd = -1;
static __u64 g_backp_last_hb = 0;
static double g_backp_last_hb_us = 0;
static double g_backp_check_us = 0;   /* 上次实际查询时间（µs） */
static int g_backp_cached = 0;        /* 1ms 缓存结果：背压以 ms 计变化，逐 read 查询浪费 syscall */
static unsigned long long g_backp_stalls = 0;   /* 背压停顿次数（诊断：master 是否真的被节流） */
static int backpressured(void) {
    if (g_mode != 2) return 0;   /* 仅 ebpf 模式由 proxy 转发 */
    /* 1ms 缓存：背压以 ms 计变化，逐 read 查 2 次 bpf syscall 浪费——P=1 每命令 2 次
     * bpf_map_lookup_elem（key4+key6），实测占满 master 单核把 ebpf P=1 压到 155k
     * （vs 恢复缓存 212k）。1ms 滞后对高 P 无损（跨机 P=160 送达 ~100% 已验证）。 */
    double nowu = now_us();
    if (nowu - g_backp_check_us < 1000.0)
        return g_backp_cached;
    g_backp_check_us = nowu;
    if (g_backp_fd < 0) {
        char path[512];
        snprintf(path, sizeof(path), "%s/client_ctl", BPF_PIN_PATH);
        g_backp_fd = bpf_obj_get(path);
        if (g_backp_fd < 0) { g_backp_cached = 0; return 0; }   /* proxy 未启动 */
    }
    /* 双信号背压：client_ctl[4]=ringbuf（BPF 置位/proxy 消费清除），
     * client_ctl[6]=转发队列（入队置位/出队清除）。任一置位即停 master。 */
    __u32 key4 = 4, key6 = 6;
    __u64 v4 = 0, v6 = 0;
    if (bpf_map_lookup_elem(g_backp_fd, &key4, &v4) != 0) { g_backp_cached = 0; return 0; }
    if (bpf_map_lookup_elem(g_backp_fd, &key6, &v6) != 0) { g_backp_cached = 0; return 0; }
    if (v4 == 0 && v6 == 0) { g_backp_cached = 0; return 0; }
    /* proxy 心跳新鲜度：2s 内未推进视为 proxy 退出，避免背压标志陈旧致挂起 */
    __u32 hb_key = 5;
    __u64 hb = 0;
    if (bpf_map_lookup_elem(g_backp_fd, &hb_key, &hb) != 0) { g_backp_cached = 0; return 0; }
    if (hb != g_backp_last_hb) {
        g_backp_last_hb = hb;
        g_backp_last_hb_us = nowu;
    }
    if (nowu - g_backp_last_hb_us > 2000000.0) { g_backp_cached = 0; return 0; }   /* 2s */
    g_backp_cached = 1;
    return 1;
}

/* ---- 每命令实测（2026-08-12，验证高 P 摊薄）：reactor 每次 read() 处理多少命令、
 * 每 read() 耗时、每命令平均耗时。直接测量，非反推。 ---- */
static unsigned long long g_reactor_cmds = 0;   /* 全部连接处理的总命令数 */
static unsigned long long g_reactor_reads = 0;  /* read() 调用次数 */
static unsigned long long g_reactor_bytes = 0;  /* read() 捕获总字节数（与 tcpsink bytes 对比验证无损） */
static unsigned long long g_reactor_ns = 0;     /* reactor_conn_read 总耗时(ns) */

static void reactor_conn_close(reactor_conn_t *c, int epfd) {
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    free(c);
}

/* 追加回包并尝试非阻塞 flush；EAGAIN → 注册 EPOLLOUT；发完 → 注销 EPOLLOUT */
static void reactor_conn_flush(reactor_conn_t *c, int epfd) {
    while (c->out_off < c->out_len) {
        ssize_t w = write(c->fd, c->out + c->out_off, c->out_len - c->out_off);
        if (w < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                g_epollout_regs++;
                if (!c->epollout_reg) {
                    struct epoll_event ev = {.events = EPOLLOUT, .data.u64 = (uint64_t)(uintptr_t)c};
                    epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
                    c->epollout_reg = 1;
                }
                return;
            }
            reactor_conn_close(c, epfd); return;
        }
        c->out_off += (size_t)w;
    }
    if (c->out_len > 0) c->out_len = c->out_off = 0;
    if (c->epollout_reg) {
        struct epoll_event ev = {.events = EPOLLIN | EPOLLRDHUP, .data.u64 = (uint64_t)(uintptr_t)c};
        epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
        c->epollout_reg = 0;
    }
}

/* 无锁 SPSC 入队（reactor 单生产者，转发线程单消费者）：
 * 写 header+payload 后一次性 release 发布 tail，消除跨核 mutex（sync P=1 瓶颈）。 */
static int fwd_enqueue(const unsigned char *buf, size_t len) {
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    size_t need = len + sizeof(uint32_t);
    size_t tail = g_fwd_tail;   /* 本线程独占，写前快照 */
    size_t head, used;
    for (int spins = 0; ; spins++) {
        head = __atomic_load_n(&g_fwd_ring_head, __ATOMIC_ACQUIRE);
        used = (tail >= head) ? tail - head : FWD_RING_SIZE - (head - tail);
        if (used + need <= FWD_RING_SIZE) break;
        if (g_fwd_stop || g_fwd_dead) { clock_gettime(CLOCK_MONOTONIC, &t1); g_enq_ns += (unsigned long long)((t1.tv_sec-t0.tv_sec)*1000000000LL + (t1.tv_nsec-t0.tv_nsec)); g_enq_cnt++; return -1; }
        if (spins < 100) __builtin_ia32_pause(); else sched_yield();
    }
    /* 写 4B 长度头（可能跨环尾） */
    uint32_t l = (uint32_t)len;
    size_t hf = FWD_RING_SIZE - tail;
    if (sizeof(l) <= hf) memcpy(g_fwd_ring + tail, &l, sizeof(l));
    else {
        memcpy(g_fwd_ring + tail, &l, hf);
        memcpy(g_fwd_ring, (const char *)&l + hf, sizeof(l) - hf);
    }
    size_t ptail = (tail + sizeof(l)) % FWD_RING_SIZE;
    /* 写 payload（可能跨环尾） */
    size_t pf = FWD_RING_SIZE - ptail;
    if (len <= pf) memcpy(g_fwd_ring + ptail, buf, len);
    else {
        memcpy(g_fwd_ring + ptail, buf, pf);
        memcpy(g_fwd_ring, buf + pf, len - pf);
    }
    /* 一次 release 发布 tail：consumer 原子看到完整槽 */
    __atomic_store_n(&g_fwd_tail, (ptail + len) % FWD_RING_SIZE, __ATOMIC_RELEASE);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    g_enq_ns += (unsigned long long)((t1.tv_sec - t0.tv_sec) * 1000000000LL + (t1.tv_nsec - t0.tv_nsec));
    g_enq_cnt++;
    return 0;
}

static void *fwd_thread_main(void *arg) {
    (void)arg;
    static int fwd_discard = -1;      /* 诊断：FWD_DISCARD=1 跳过 writev */
    static size_t max_qbytes = 0;
    if (fwd_discard < 0)
        fwd_discard = getenv("FWD_DISCARD") != NULL;
    struct iovec iov[FWD_MAX_IOV];
    while (1) {
        size_t tail = __atomic_load_n(&g_fwd_tail, __ATOMIC_ACQUIRE);
        size_t head = g_fwd_ring_head;   /* 本线程独占（只此消费者写） */
        size_t committed = (tail >= head) ? tail - head : FWD_RING_SIZE - (head - tail);
        if (committed == 0) {
            if (g_fwd_stop) break;
            usleep(FWD_EMPTY_WAIT_US);
            continue;
        }
        /* 无锁扫描 [head, tail) → 攒 iovec（payload 跨环尾拆两段） */
        int niov = 0;
        size_t batch_bytes = 0;
        size_t pos = head;
        /* 预留 1 段：else 分支（payload 跨环尾）一次写 2 段 iovec，
         * 若 niov=FWD_MAX_IOV-1 时进入再走 else 会写 iov[512] 越界（stack smashing）。 */
        while (niov + 1 < FWD_MAX_IOV && pos != tail) {
            uint32_t l;
            size_t hf = FWD_RING_SIZE - pos;
            if (hf >= sizeof(l)) memcpy(&l, g_fwd_ring + pos, sizeof(l));
            else {
                memcpy(&l, g_fwd_ring + pos, hf);
                memcpy((char *)&l + hf, g_fwd_ring, sizeof(l) - hf);
            }
            size_t lpos = (pos + sizeof(l)) % FWD_RING_SIZE;
            size_t pf = FWD_RING_SIZE - lpos;
            if (l <= pf) {
                iov[niov].iov_base = g_fwd_ring + lpos; iov[niov].iov_len = l; niov++;
            } else {
                iov[niov].iov_base = g_fwd_ring + lpos; iov[niov].iov_len = pf; niov++;
                iov[niov].iov_base = g_fwd_ring;        iov[niov].iov_len = l - pf; niov++;
            }
            pos = (lpos + l) % FWD_RING_SIZE;
            batch_bytes += l;
        }
        if (committed > max_qbytes) max_qbytes = committed;
        if (batch_bytes > 0 && !fwd_discard && g_slave_fd >= 0) {
            ssize_t w = writev(g_slave_fd, iov, niov);
            if (w != (ssize_t)batch_bytes) {
                fprintf(stderr, "[fwd] writev partial/failed: %zd/%zu %s\n",
                        w, batch_bytes, strerror(errno));
                g_fwd_dead = 1;
            }
        }
        /* 发布 head（release）：释放空间给生产者 */
        __atomic_store_n(&g_fwd_ring_head, pos, __ATOMIC_RELEASE);
    }
    fprintf(stderr, "[fwd] exit: max_queue_bytes=%zu (%s)\n",
            max_qbytes, fwd_discard ? "discard-mode" : "write-mode");
    return NULL;
}

/* handler 内联调：锁 + 追加写缓冲 + 非阻塞 send（MSG_DONTWAIT）。EAGAIN 剩余留缓冲。 */
static void fwd_inline_forward(const unsigned char *cmd, size_t len) {
    pthread_mutex_lock(&g_slave_wlock);
    if (g_slave_dead || g_slave_fd < 0) { pthread_mutex_unlock(&g_slave_wlock); return; }
    if (g_slave_wlen + len > sizeof(g_slave_wbuf)) {
        /* 缓冲满：非阻塞 send 腾空间；仍满则置 dead（测试场景 slave 健康不应发生） */
        size_t sent = 0;
        while (sent < g_slave_wlen) {
            ssize_t n = send(g_slave_fd, g_slave_wbuf + sent, g_slave_wlen - sent,
                             MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n <= 0) break;
            sent += (size_t)n;
        }
        if (sent > 0) { memmove(g_slave_wbuf, g_slave_wbuf + sent, g_slave_wlen - sent); g_slave_wlen -= sent; }
        if (g_slave_wlen + len > sizeof(g_slave_wbuf)) { g_slave_dead = 1; g_slave_wlen = 0; pthread_mutex_unlock(&g_slave_wlock); return; }
    }
    memcpy(g_slave_wbuf + g_slave_wlen, cmd, len);
    g_slave_wlen += len;
    /* 非阻塞 send：能发多少发多少，剩余留缓冲（slave 健康时一次发完，缓冲清空） */
    size_t sent = 0;
    while (sent < g_slave_wlen) {
        ssize_t n = send(g_slave_fd, g_slave_wbuf + sent, g_slave_wlen - sent,
                         MSG_DONTWAIT | MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            g_slave_dead = 1; g_slave_wlen = 0; break;
        }
        sent += (size_t)n;
    }
    if (sent > 0) { memmove(g_slave_wbuf, g_slave_wbuf + sent, g_slave_wlen - sent); g_slave_wlen -= sent; }
    pthread_mutex_unlock(&g_slave_wlock);
}

static void fwd_thread_start(void) {
    g_fwd_stop = 0;
    g_fwd_dead = 0;
    if (!g_fwd_ring) g_fwd_ring = (unsigned char *)malloc(FWD_RING_SIZE);
    pthread_create(&g_fwd_thread, NULL, fwd_thread_main, NULL);
    /* 转发线程 pin 到独立核 FWD_THREAD_CPU，不与 handler 抢 master 核（失败仅告警） */
    cpu_set_t cs; CPU_ZERO(&cs); CPU_SET(FWD_THREAD_CPU, &cs);
    if (pthread_setaffinity_np(g_fwd_thread, sizeof(cs), &cs) != 0)
        fprintf(stderr, "[fwd] pthread_setaffinity_np(%d) failed: %s\n",
                FWD_THREAD_CPU, strerror(errno));
}

static void fwd_thread_stop(void) {
    g_fwd_stop = 1;   /* 无锁：consumer 在空队列轮询时检查该标志退出 */
    pthread_join(g_fwd_thread, NULL);
    fprintf(stderr, "[fwd] enq avg=%llu ns cnt=%llu\n",
            g_enq_cnt ? g_enq_ns / g_enq_cnt : 0, g_enq_cnt);
    g_fwd_ring_head = 0;
    g_fwd_tail = 0;
    free(g_fwd_ring);
    g_fwd_ring = NULL;
}

/* ---- Master accept 线程 ---- */
static int g_listen_fd = -1;
static pthread_t g_master_tid;   /* reactor 线程 */

/* reactor_conn_read: 非阻塞读 + 解析 + ht_hset + 转发 + 缓冲回包 */
static void reactor_conn_read(reactor_conn_t *c, int epfd) {
    /* 无损背压：转发队列近满时停手，避免 ringbuf 溢出静默丢数据 */
    while (backpressured()) {
        g_backp_stalls++;
        usleep(100);
    }
    struct timespec r0, r1;
    clock_gettime(CLOCK_MONOTONIC, &r0);
    char io[65536];
    /* 单次 read 上限对齐捕获 BPF 的 CLIENT_ENTRY_MAX_LEN=32764：超过会被 BPF 截断
     * 丢尾部（P=160 跨机曾丢 64%）。限制读大小后 BPF 永远捕获完整 recv。 */
    ssize_t n = read(c->fd, io, sizeof(io) > 32764 ? 32764 : sizeof(io));
    if (n <= 0) { reactor_conn_close(c, epfd); return; }
    g_reactor_bytes += (unsigned long long)n;   /* 与 tcpsink bytes= 对比（字节级无损验证） */
    if (cb_space(&c->in) < (int)n) cb_compact(&c->in);
    if (cb_space(&c->in) < (int)n) { reactor_conn_close(c, epfd); return; }
    memcpy(c->in.data + c->in.head, io, (size_t)n);
    c->in.head += (int)n;

    resp_scanner_t rs;
    rs_init(&rs);
    int batch_start = -1;   /* 本 read 内首个完整 HSET 命令的缓冲偏移（批量入队：一次 read 整块入队） */
    while (1) {
        int consumed = 0;
        int avail = cb_avail(&c->in);
        int rc = rs_scan(&rs, c->in.data + c->in.tail, avail, &consumed);
        if (rc <= 0) break;
        /* 兼容 redis-benchmark（*4 $4 HSET，大写）与 memtier（*3 $4 hset，小写）：
         * 命令字不区分大小写、argc>=3 即视为 HSET 写入命令。 */
        if (rs.arg_count >= 3 && rs.arg_lens[0] == 4 &&
            ((rs.args[0][0] == 'h' || rs.args[0][0] == 'H') &&
             (rs.args[0][1] == 's' || rs.args[0][1] == 'S') &&
             (rs.args[0][2] == 'e' || rs.args[0][2] == 'E') &&
             (rs.args[0][3] == 't' || rs.args[0][3] == 'T'))) {
            (void)ht_hset(rs.args[1], rs.arg_lens[1], rs.args[2], rs.arg_lens[2]);
            /* 转发：默认批量入队（一次 read 的连续 HSET 整块入队，一次原子对+一次 memcpy，
             * 摊薄每命令开销）/ FWD_INLINE 内联（每命令非阻塞 send）/ FWD_NOENQ 跳过 */
            if (g_mode == 1 && g_slave_fd >= 0 && getenv("FWD_NOENQ") == NULL) {
                if (g_fwd_inline) {
                    fwd_inline_forward(c->in.data + c->in.tail, (size_t)consumed);
                } else if (batch_start < 0) {
                    batch_start = c->in.tail;
                }
            }
        } else if (batch_start >= 0) {
            /* 非 HSET 命令打断：flush 已攒的 HSET 批量 */
            if (g_mode == 1 && g_slave_fd >= 0 && getenv("FWD_NOENQ") == NULL && !g_fwd_inline)
                (void)fwd_enqueue(c->in.data + batch_start, (size_t)(c->in.tail - batch_start));
            batch_start = -1;
        }
        if (c->out_len + RESP_OK_LEN > sizeof(c->out)) { reactor_conn_close(c, epfd); return; }
        memcpy(c->out + c->out_len, RESP_OK, RESP_OK_LEN);
        c->out_len += RESP_OK_LEN;
        cb_consume(&c->in, consumed);
        rs_init(&rs);
        g_reactor_cmds++;
    }
    /* 循环结束：flush 剩余 HSET 批量 */
    if (batch_start >= 0) {
        if (g_mode == 1 && g_slave_fd >= 0 && getenv("FWD_NOENQ") == NULL && !g_fwd_inline)
            (void)fwd_enqueue(c->in.data + batch_start, (size_t)(c->in.tail - batch_start));
    }
    clock_gettime(CLOCK_MONOTONIC, &r1);
    g_reactor_reads++;
    g_reactor_ns += (unsigned long long)((r1.tv_sec - r0.tv_sec) * 1000000000LL
                                         + (r1.tv_nsec - r0.tv_nsec));
    reactor_conn_flush(c, epfd);
}

/* ---- 单线程 epoll reactor（A1：对齐生产单线程 reactor）---- */
static void *reactor_main(void *arg) {
    (void)arg;
    /* 自建监听 socket（原 accept_thread 职责；单线程 reactor 内 accept） */
    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_ANY),
                               .sin_port = htons(g_master_port)};
    if (bind(g_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[master] bind"); close(g_listen_fd); g_listen_fd = -1; return NULL;
    }
    listen(g_listen_fd, 256);
    fprintf(stderr, "[master] port %d (mode=%s, reactor)\n",
            g_master_port,
            g_mode == 0 ? "none" : g_mode == 1 ? "sync" : "ebpf");

    int epfd = epoll_create1(0);
    int lfl = fcntl(g_listen_fd, F_GETFL, 0);
    fcntl(g_listen_fd, F_SETFL, lfl | O_NONBLOCK);
    struct epoll_event ev = {.events = EPOLLIN, .data.u64 = 0};   /* 0 = listen */
    epoll_ctl(epfd, EPOLL_CTL_ADD, g_listen_fd, &ev);

    while (!g_shutdown) {
        struct epoll_event evs[256];
        int n = epoll_wait(epfd, evs, 256, 100);
        for (int i = 0; i < n; i++) {
            reactor_conn_t *c = (reactor_conn_t *)(uintptr_t)evs[i].data.u64;
            if (c == NULL) {
                /* listen：accept 所有待接连接 */
                while (1) {
                    int cfd = accept(g_listen_fd, NULL, NULL);
                    if (cfd < 0) break;
                    set_nodelay(cfd);
                    int fl = fcntl(cfd, F_GETFL, 0);
                    fcntl(cfd, F_SETFL, fl | O_NONBLOCK);
                    reactor_conn_t *nc = calloc(1, sizeof(*nc));
                    nc->fd = cfd;
                    struct epoll_event cev = {.events = EPOLLIN | EPOLLRDHUP,
                                              .data.u64 = (uint64_t)(uintptr_t)nc};
                    epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);
                }
            } else if (evs[i].events & (EPOLLIN | EPOLLRDHUP)) {
                reactor_conn_read(c, epfd);
            } else if (evs[i].events & EPOLLOUT) {
                g_epollout_evts++;
                reactor_conn_flush(c, epfd);
            } else {   /* EPOLLERR | EPOLLHUP */
                reactor_conn_close(c, epfd);
            }
        }
    }
    fprintf(stderr, "[reactor] exit: epollout_regs=%ld epollout_evts=%ld backp_stalls=%llu\n",
            g_epollout_regs, g_epollout_evts, g_backp_stalls);
    if (g_reactor_reads > 0 && g_reactor_cmds > 0)
        fprintf(stderr, "[reactor] per-cmd: cmds=%llu reads=%llu bytes=%llu cmds/read=%.2f "
                        "read_avg_ns=%.0f cmd_avg_ns=%.2f\n",
                g_reactor_cmds, g_reactor_reads, g_reactor_bytes,
                (double)g_reactor_cmds / (double)g_reactor_reads,
                (double)g_reactor_ns / (double)g_reactor_reads,
                (double)g_reactor_ns / (double)g_reactor_cmds);
    close(epfd);
    close(g_listen_fd);
    g_listen_fd = -1;
    return NULL;
}

static void master_start(void) {
    g_shutdown = 0;   /* 复位：--mode all 下前一轮 master_stop 已置 1，不复位则本轮 reactor 立即退出 */
    pthread_create(&g_master_tid, NULL, reactor_main, NULL);
    for (int i = 0; i < 50 && g_listen_fd <= 0; i++)
        usleep(20000);
}

static void master_stop(void) {
    g_shutdown = 1;
    pthread_join(g_master_tid, NULL);
}

/* ---- redis-benchmark 运行器 ---- */
static const char *g_bench_host = NULL;   /* NULL=本地, 否则 SSH 到远端 */
static const char *g_master_host = "127.0.0.1"; /* redis-benchmark 连 master 的 IP */

typedef struct {
    double qps;
    int completed;
} bench_result_t;

/* 从 redis-benchmark CSV 解析 QPS（一行一条，聚合到 *qps）。 */
static double parse_bench_qps(FILE *fp) {
    char line[256];
    double qps = 0;
    while (fgets(line, sizeof(line), fp)) {
        /* 7.2.9 CSV: "HSET","rps","avg_lat","min","p50","p95","p99","max"
         * 取第 2 个字段（rps），跳过命令名 "HSET" 后的 "," 再取引号内数字。 */
        char *p = strstr(line, "HSET");
        if (p) {
            char *comma = strchr(p, ',');
            if (comma) {
                char *q = strchr(comma + 1, '"');
                if (q) qps = strtod(q + 1, NULL);
            }
        }
        fprintf(stderr, "[bench] %s", line);
    }
    return qps;
}

/* 从 memtier_benchmark 输出解析 Ops/sec（Totals 行第 2 字段）。 */
static double parse_bench_memtier_qps(FILE *fp) {
    char line[256];
    double qps = 0;
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "Totals", 6) == 0) {
            char *p = line + 6;
            while (*p == ' ' || *p == '\t') p++;
            qps = strtod(p, NULL);
        }
        fprintf(stderr, "[bench] %s", line);
    }
    return qps;
}

static bench_result_t run_redis_benchmark(void) {
    bench_result_t r = {0, 0};

    char cmd[1024];
    const char *use_mem = getenv("USE_MEMTIER");
    if (use_mem) {
        /* memtier 单进程多线程客户端（替代 redis-benchmark）：-t 2 -c 25 = 单进程 50 连接 */
        const char *mt = getenv("MTBIN");
        if (!mt || !*mt) mt = "/usr/local/bin/memtier_benchmark";
        int mt_t = 2, mt_c = 50, mt_tt = 5;
        const char *mte = getenv("MT_THREADS");
        const char *mce = getenv("MT_CLIENTS");
        const char *tte = getenv("MT_TEST_TIME");
        if (mte && atoi(mte) > 0) mt_t = atoi(mte);
        if (mce && atoi(mce) > 0) mt_c = atoi(mce);
        if (tte && atoi(tte) > 0) mt_tt = atoi(tte);
        snprintf(cmd, sizeof(cmd),
                 "%s -s %s -p %d -t %d -c %d --test-time=%d --pipeline=%d -d %d "
                 "--command='hset __key__ __data__' --command-key-pattern=R "
                 "--key-maximum=1000000 2>/dev/null",
                 mt, g_master_host, g_master_port, mt_t, mt_c, mt_tt, g_pipeline, g_payload_size);
    } else if (g_bench_host) {
        /* 远端运行 redis-benchmark（对齐 7.2.9） */
        snprintf(cmd, sizeof(cmd),
                 "sshpass -p '%s' ssh -o StrictHostKeyChecking=no "
                 "pp@%s '/opt/redis-7.2.9/bin/redis-benchmark -h %s -p %d "
                 "-t hset -n %d -c %d -r %d -d %d -P %d --csv' 2>/dev/null",
                 "2983372202", g_bench_host, g_master_host, g_master_port,
                 g_req_count, g_redis_clients,
                 g_keyrange ? g_keyrange : g_req_count, g_payload_size, g_pipeline);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "/opt/redis-7.2.9/bin/redis-benchmark -h %s -p %d "
                 "-t hset -n %d -c %d -r %d -d %d -P %d --csv 2>/dev/null",
                 g_master_host, g_master_port,
                 g_req_count, g_redis_clients,
                 g_keyrange ? g_keyrange : g_req_count, g_payload_size, g_pipeline);
    }

    fprintf(stderr, "[bench] %s\n", cmd);

    /* 多客户端实例（RB_INSTANCES=N）：P=1 下单线程 redis-benchmark 是瓶颈（~100k），
     * 加实例数（分核）而非 -c 才能推高客户端吞吐，验证服务器真实上限。 */
    int instances = 1;
    const char *env = getenv("RB_INSTANCES");
    if (env && atoi(env) > 0) instances = atoi(env);
    if (instances > 8) instances = 8;
    if (use_mem) instances = 1;   /* memtier 单进程多线程，不再多实例 */

    /* 额外实例优先钉空闲核：instance0 用 CLIENT_CPU(3)，其余用 CPU1、CPU0（避免占转发核 CPU0 时仅用于 none）。 */
    int pfd[8][2];
    pid_t cpids[8];
    double total_qps = 0;
    int spawned = 0;

    for (int i = 0; i < instances; i++) {
        if (pipe(pfd[i]) < 0) { perror("pipe"); break; }
        pid_t cpid = fork();
        if (cpid < 0) { perror("fork"); close(pfd[i][0]); close(pfd[i][1]); break; }
        if (cpid == 0) {
            close(pfd[i][0]);
            dup2(pfd[i][1], STDOUT_FILENO);
            close(pfd[i][1]);
            cpu_set_t cs; CPU_ZERO(&cs);
            if (use_mem) {
                /* memtier 单进程：钉到 {CLIENT_CPU, CPU1} 供 2 线程分核 */
                CPU_SET(CLIENT_CPU, &cs); CPU_SET(1, &cs);
                if (sched_setaffinity(0, sizeof(cs), &cs) != 0)
                    fprintf(stderr, "[bench] memtier sched_setaffinity failed: %s\n", strerror(errno));
                fprintf(stderr, "[bench] client[%d] memtier pin cpu%d,%d\n", i, CLIENT_CPU, 1);
            } else {
                int core = (i == 0) ? CLIENT_CPU : (i == 1 ? 1 : 0); /* 3,1,0,0... */
                CPU_SET(core, &cs);
                if (sched_setaffinity(0, sizeof(cs), &cs) != 0)
                    fprintf(stderr, "[bench] sched_setaffinity(%d) failed: %s\n", core, strerror(errno));
                fprintf(stderr, "[bench] client[%d] pin cpu%d\n", i, core);
            }
            execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
            _exit(127);
        }
        cpids[i] = cpid;
        close(pfd[i][1]);
        spawned++;
    }

    for (int i = 0; i < spawned; i++) {
        FILE *fp = fdopen(pfd[i][0], "r");
        if (fp) {
            total_qps += use_mem ? parse_bench_memtier_qps(fp) : parse_bench_qps(fp);
            fclose(fp);
        }
        waitpid(cpids[i], NULL, 0);
    }

    if (spawned > 0) {
        r.qps = total_qps;               /* 聚合 QPS（各实例之和） */
        r.completed = g_req_count * spawned;
    }
    return r;
}

/* ---- 统计 ---- */
static double compute_mean(const double *vals, int n) {
    if (n <= 0) return 0;
    double sum = 0;
    for (int i = 0; i < n; i++) sum += vals[i];
    return sum / n;
}

static double compute_stddev(const double *vals, int n, double mean) {
    if (n <= 1) return 0;
    double sum = 0;
    for (int i = 0; i < n; i++) {
        double d = vals[i] - mean;
        sum += d * d;
    }
    return sqrt(sum / (n - 1));
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static double compute_median(double *vals, int n) {
    if (n <= 0) return 0;
    double *s = malloc((size_t)n * sizeof(double));
    memcpy(s, vals, (size_t)n * sizeof(double));
    qsort(s, (size_t)n, sizeof(double), cmp_double);
    double m = n % 2 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2.0;
    free(s);
    return m;
}

/* ---- 输出 ---- */
static void print_result(const char *mode, int payload, double mean,
                          double median, double stddev,
                          double min_val, double max_val, int rounds) {
    printf("%-8s  %-6d  %10.0f  %10.0f  %10.0f  %10.0f  %10.0f  %10.0f  n=%d\n",
           mode, payload, median, mean, median, stddev, min_val, max_val, rounds);
    fflush(stdout);
}

/* ---- 单模式运行 ---- */
static double *run_one_mode(const char *mode_str) {
    double *qps_vals = calloc((size_t)g_rounds, sizeof(double));
    if (!qps_vals) return NULL;

    /* 打开 hash 日志 */
    if (g_hash_log) {
        char fname[512];
        snprintf(fname, sizeof(fname), "%s.%s", g_hash_log, mode_str);
        g_hash_fp = fopen(fname, "w");
        g_hash_seq = 0;
    }

    /* sync 模式：连 slave */
    g_slave_fd = -1;
    if (g_mode == 1) {
        g_slave_fd = socket(AF_INET, SOCK_STREAM, 0);
        set_nodelay(g_slave_fd);
        struct sockaddr_in sa = {.sin_family = AF_INET,
                                 .sin_port = htons(g_slave_port)};
        if (inet_pton(AF_INET, g_slave_host, &sa.sin_addr) != 1)
            sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (connect(g_slave_fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
            perror("[sync] connect slave");
            close(g_slave_fd);
            g_slave_fd = -1;
        } else {
            int snd = 262144;
            setsockopt(g_slave_fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
            struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
            setsockopt(g_slave_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        }
    }

    /* sync 模式：连上 slave 后启动转发线程（FWD_INLINE 内联路径不需要线程） */
    if (g_mode == 1 && g_slave_fd >= 0 && !g_fwd_inline)
        fwd_thread_start();

    /* ebpf 模式：启 proxy */
    if (g_mode == 2) {
        if (proxy_start() != 0 ||
            proxy_write_config(getpid()) != 0) {
            fprintf(stderr, "[ebpf] proxy init failed\n");
            proxy_stop();
            free(qps_vals);
            return NULL;
        }
        proxy_wait_ready(5000);
        usleep(500000);
    }

    /* 启动 master */
    master_start();

    /* 多轮 */
    for (int r = 0; r < g_rounds; r++) {
        fprintf(stderr, "[%s] round %d/%d\n", mode_str, r + 1, g_rounds);

        double t_start = now_us();
        bench_result_t br = run_redis_benchmark();
        double t_end = now_us();

        if (g_mode == 2) {
            ebpf_wait_ringbuf_drain(1000);
            /* 收尾统计修正：ringbuf 空不代表数据已到 slave——可能仍在 proxy 转发队列、
             * TCP 发送缓冲、网络、slave 接收缓冲。等 ~1s 让整条链路冲刷完再统计，
             * 否则"送达率"被过早统计压低（修复后 P=160 送达率偏低的一部分原因）。 */
            usleep(1000000);
        }

        double elapsed = t_end - t_start;
        qps_vals[r] = br.qps > 0
                          ? br.qps
                          : (elapsed > 0 ? (double)br.completed / elapsed * 1e6 : 0);

        fprintf(stderr, "[%s] round %d: QPS=%.0f\n",
                mode_str, r + 1, qps_vals[r]);

        if (r < g_rounds - 1) usleep(200000);
    }

    /* 清理 */
    master_stop();
    usleep(100000);

    if (g_mode == 2) proxy_stop();
    if (g_mode == 1 && g_slave_fd >= 0 && !g_fwd_inline) fwd_thread_stop();
    if (g_slave_fd >= 0) { close(g_slave_fd); g_slave_fd = -1; }

    if (g_hash_fp) {
        fprintf(stderr, "[%s] master hash log: %lld commands\n",
                mode_str, g_hash_seq);
        fclose(g_hash_fp);
        g_hash_fp = NULL;
    }

    return qps_vals;
}

static void system_warmup(void) {
    fprintf(stderr, "[test] warmup...\n");
    int sm = g_mode, sc = g_req_count;
    g_mode = 0; g_req_count = 1000;
    double *v = run_one_mode("warmup");
    free(v);
    g_mode = sm; g_req_count = sc;
    sleep(1);
}

/* ---- 用法 ---- */
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: sudo %s [options]\n"
        "  --mode, -m     none | sync | ebpf | all (default: all)\n"
        "  --payload, -p  HSET value 大小 (default: 64)\n"
        "  --count, -c    每轮请求数 (default: 50000)\n"
        "  --clients N    并发连接数 (default: 50)\n"
        "  --rounds, -r   轮数 (default: 5, 第1轮预热)\n"
        "  --slave-host   转发目标 (default: 127.0.0.1)\n"
        "  --port         主端口 (default: 15900)\n"
        "  --slave-port   转发端口 (default: 15901)\n"
        "  --proxy-bin    ebpf-proxy 路径\n"
        "  --bpf-obj      BPF .o 路径\n"
        "  --csv FILE     输出 CSV\n"
        "  --cpu N        CPU 亲和性\n", prog);
}

/* ---- NIC IRQ 固定（测量方法学，第三号假象修正）----
 * 根因：irqbalance 会动态把从机出接口（默认路由 iface）的网卡 IRQ 放到客户端核，
 * 客户端进程被 NIC 中断抢占 → 测得 QPS 虚低（跨机 sync 曾被此假象压到 ~85% none，
 * 实测根因是把 ens33 IRQ 从 client 核移走即恢复 ≈none）。
 * 本函数在 root 时自动：把该 IRQ 钉到空闲核（避开 MASTER/FWD/PROXY/CLIENT），并
 * 暂停 irqbalance（否则其 10s 周期会把 IRQ 搬回，污染 >10s 的测量），退出时恢复。
 * 非 root 只告警（无权限写 /proc/irq，测量可能失真）。 */
static void irq_resume_irqbalance(void) {
    FILE *pf = popen("pgrep -x irqbalance", "r");
    if (!pf) return;
    char buf[32];
    if (fgets(buf, sizeof(buf), pf)) {
        pid_t ibp = (pid_t)atoi(buf);
        if (ibp > 0) kill(ibp, SIGCONT);
    }
    pclose(pf);
}

static void irq_pause_irqbalance(void) {
    FILE *pf = popen("pgrep -x irqbalance", "r");
    if (!pf) return;
    char buf[32];
    if (fgets(buf, sizeof(buf), pf)) {
        pid_t ibp = (pid_t)atoi(buf);
        if (ibp > 0) {
            kill(ibp, SIGSTOP);
            fprintf(stderr, "[test] irqbalance(%d) paused (resumed on exit)\n", (int)ibp);
        }
    }
    pclose(pf);
}

static void pin_nic_irq_off_client(void) {
    if (geteuid() != 0) {
        fprintf(stderr, "[test] WARN: not root — cannot pin NIC IRQ off client core; "
                "irqbalance may skew QPS (run with sudo for valid measurements)\n");
        return;
    }

    if (getenv("SKIP_IRQ_PIN")) {
        /* 同机 loopback 测试流量不走 ens33，钉 NIC IRQ 无收益；且 4 核 VM 上目标核(CPU1)
         * 与 memtier 第 2 线程冲突反而把客户端压到瓶颈（低 P 比值失真）。仅停 irqbalance。 */
        irq_pause_irqbalance();
        atexit(irq_resume_irqbalance);
        fprintf(stderr, "[test] SKIP_IRQ_PIN: irqbalance paused, IRQ pin skipped\n");
        return;
    }

    /* 1. 默认路由接口 = 从机出接口 */
    FILE *fp = fopen("/proc/net/route", "r");
    char iface[32] = "";
    if (fp) {
        char line[256];
        if (fgets(line, sizeof(line), fp)) { /* header */ }
        while (fgets(line, sizeof(line), fp)) {
            char ifn[32];
            unsigned int dest, gw, flags;
            if (sscanf(line, "%31s %x %x %x", ifn, &dest, &gw, &flags) == 4 && dest == 0) {
                snprintf(iface, sizeof(iface), "%s", ifn);
                break;
            }
        }
        fclose(fp);
    }
    if (iface[0] == 0) {
        fprintf(stderr, "[test] WARN: no default-route iface, skip NIC IRQ pin\n");
        return;
    }

    /* 2. 读该 NIC 的 IRQ 号 */
    char irqpath[256], buf[64];
    snprintf(irqpath, sizeof(irqpath), "/sys/class/net/%s/device/irq", iface);
    FILE *f = fopen(irqpath, "r");
    if (!f) { fprintf(stderr, "[test] WARN: no /sys irq for %s\n", iface); return; }
    if (!fgets(buf, sizeof(buf), f)) { fclose(f); return; }
    fclose(f);
    int irq = atoi(buf);
    if (irq <= 0) { fprintf(stderr, "[test] WARN: bad irq %d for %s\n", irq, iface); return; }

    /* 3. 选空闲核：避开 MASTER/FWD/PROXY/CLIENT（<32 核有效） */
    int busy[4] = {MASTER_CPU, FWD_THREAD_CPU, PROXY_CPU, CLIENT_CPU};
    int ncpu = (int)sysconf(_SC_NPROCESSORS_ONLN);
    int target = -1;
    for (int c = 0; c < ncpu && target < 0; c++) {
        int is_busy = 0;
        for (int i = 0; i < 4; i++) if (c == busy[i]) { is_busy = 1; break; }
        if (!is_busy) target = c;
    }
    if (target < 0) { fprintf(stderr, "[test] WARN: no idle core for IRQ pin\n"); return; }

    /* 4. 写 smp_affinity（hex，bit=target） */
    char aff[32], affpath[128];
    snprintf(aff, sizeof(aff), "%x", (unsigned)(1u << target));
    snprintf(affpath, sizeof(affpath), "/proc/irq/%d/smp_affinity", irq);
    FILE *af = fopen(affpath, "w");
    if (!af) { fprintf(stderr, "[test] WARN: cannot open %s\n", affpath); return; }
    fputs(aff, af);
    fclose(af);
    fprintf(stderr, "[test] NIC IRQ %d (%s) pinned to cpu%d (aff=%s)\n",
            irq, iface, target, aff);
    irq_pause_irqbalance();
    atexit(irq_resume_irqbalance);
}

int main(int argc, char **argv) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    const char *mode_str = "all";

    struct option long_opts[] = {
        {"mode",       required_argument, 0, 'm'},
        {"payload",    required_argument, 0, 'p'},
        {"count",      required_argument, 0, 'c'},
        {"clients",    required_argument, 0, 'C'},
        {"rounds",     required_argument, 0, 'r'},
        {"slave-host", required_argument, 0, 'H'},
        {"port",       required_argument, 0, 'P'},
        {"slave-port", required_argument, 0, 'S'},
        {"proxy-bin",  required_argument, 0, 1000},
        {"bpf-obj",    required_argument, 0, 1001},
        {"csv",        required_argument, 0, 1002},
        {"cpu",        required_argument, 0, 1003},
        {"hash-log",   required_argument, 0, 1004},
        {"bench-host", required_argument, 0, 1005},
        {"master-host",required_argument, 0, 1006},
        {"pipeline",  required_argument, 0, 1007},
        {"keyrange",  required_argument, 0, 1008},
        {"help",       no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "m:p:c:r:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'm':  mode_str = optarg;            break;
        case 'p':  g_payload_size = atoi(optarg); break;
        case 'c':  g_req_count = atoi(optarg);    break;
        case 'C':  g_redis_clients = atoi(optarg); break;
        case 'r':  g_rounds = atoi(optarg);       break;
        case 'H':  g_slave_host = optarg;         break;
        case 'P':  g_master_port = atoi(optarg);  break;
        case 'S':  g_slave_port = atoi(optarg);   break;
        case 1000: g_ebpf_proxy_bin = optarg;     break;
        case 1001: g_client_capture_obj = optarg; break;
        case 1002: g_csv_file = optarg;           break;
        case 1003: g_cpu = atoi(optarg);          break;
        case 1004: g_hash_log = optarg;            break;
        case 1005: g_bench_host = optarg;          break;
        case 1006: g_master_host = optarg;         break;
        case 1007: g_pipeline = atoi(optarg);      break;
        case 1008: g_keyrange = atoi(optarg);      break;
        case 'h':  usage(argv[0]); return 0;
        default:   usage(argv[0]); return 1;
        }
    }

    struct rlimit rlim = {RLIM_INFINITY, RLIM_INFINITY};
    setrlimit(RLIMIT_MEMLOCK, &rlim);

    if (g_cpu >= 0) {
        cpu_set_t cs; CPU_ZERO(&cs);
        CPU_SET((unsigned)g_cpu, &cs);
        sched_setaffinity(0, sizeof(cs), &cs);
    }

    g_fwd_inline = getenv("FWD_INLINE") != NULL;

    /* 第三号测量假象修正：NIC IRQ 钉到空闲核（root 时），否则 irqbalance 可能把
     * 从机出接口的 IRQ 放到客户端核，客户端被中断抢占 → QPS 虚低。 */
    pin_nic_irq_off_client();

    if (system("which redis-benchmark >/dev/null 2>&1") != 0) {
        fprintf(stderr, "ERROR: redis-benchmark not found\n");
        return 1;
    }

    printf("=== eBPF HSET QPS 对比测试 ===\n");
    printf("payload=%d count=%d clients=%d rounds=%d slave=%s:%d\n\n",
           g_payload_size, g_req_count, g_redis_clients, g_rounds,
           g_slave_host, g_slave_port);

    int do_all = (strcmp(mode_str, "all") == 0);
    if (do_all) system_warmup();

    FILE *csv = NULL;
    if (g_csv_file) {
        csv = fopen(g_csv_file, "w");
        if (csv) fprintf(csv, "mode,size,clients,rounds,mean,median,stddev,min,max\n");
    }

    printf("%-8s  %-6s  %10s  %10s  %10s  %10s  %10s  %10s  %s\n",
           "mode", "size", "qps_median", "mean", "median",
           "stddev", "min", "max", "");
    printf("%-8s  %-6s  %10s  %10s  %10s  %10s  %10s  %10s  %s\n",
           "--------", "------", "----------", "----------",
           "----------", "----------", "----------", "----------", "----");

    const char *modes[]   = {"none", "sync", "ebpf"};
    int         modevals[] = {0, 1, 2};

    for (int i = 0; i < 3; i++) {
        if (!do_all && strcmp(mode_str, modes[i]) != 0) continue;

        g_mode = modevals[i];
        fprintf(stderr, "\n[test] === %s ===\n", modes[i]);

        double *qps_vals = run_one_mode(modes[i]);
        if (!qps_vals) {
            fprintf(stderr, "[test] %s failed\n", modes[i]);
            continue;
        }

        int meas = g_rounds - 1;
        if (meas > 0) {
            double *mv = qps_vals + 1;
            double mean   = compute_mean(mv, meas);
            double median = compute_median(mv, meas);
            double stddev = compute_stddev(mv, meas, mean);

            double min_val = mv[0], max_val = mv[0];
            for (int j = 1; j < meas; j++) {
                if (mv[j] < min_val) min_val = mv[j];
                if (mv[j] > max_val) max_val = mv[j];
            }

            print_result(modes[i], g_payload_size, mean, median, stddev,
                        min_val, max_val, meas);

            fprintf(stderr, "[%s] mean=%.0f median=%.0f stddev=%.0f "
                    "min=%.0f max=%.0f (n=%d)\n",
                    modes[i], mean, median, stddev, min_val, max_val, meas);

            if (csv) {
                fprintf(csv, "%s,%d,%d,%d,%.0f,%.0f,%.0f,%.0f,%.0f\n",
                        modes[i], g_payload_size, g_redis_clients, meas,
                        mean, median, stddev, min_val, max_val);
            }
        }
        free(qps_vals);
        sleep(1);
    }

    if (csv) { fclose(csv); fprintf(stderr, "CSV: %s\n", g_csv_file); }
    printf("\n完成。\n");
    return 0;
}
