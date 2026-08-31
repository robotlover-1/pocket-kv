#define _GNU_SOURCE   /* sendfile(2) 需要 */
#include "kvstore/kvstore.h"
#include "kvstore/replication/repl_kprobe.h"
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/sendfile.h>

#if KVS_ENABLE_RDMA
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#endif

#define KVS_REPL_BACKLOG_SIZE (10 * 1024 * 1024)

/* ---- 复制转发队列（T：转发从 reactor 剥离，单转发线程 FIFO） ---- */
typedef struct repl_fwd_node_s {
    conn_t *c;
    unsigned char *buf;          /* 深拷贝（reactor 的 raw 在 conn inbuf，会复用） */
    size_t len;
    unsigned long long end_offset; /* master offset（这批数据终点，exclusive），用于 watermark 去重 */
    struct repl_fwd_node_s *next;
} repl_fwd_node_t;

/* 单 slave 发送缓冲上限：st->buf 超过即转 backlog/背压（防慢 slave 无限堆积）。 */
#define MAX_FWD_SEND_BUF_BYTES (4 * 1024 * 1024)

static pthread_mutex_t g_fwd_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_fwd_cond = PTHREAD_COND_INITIALIZER;
static repl_fwd_node_t *g_fwd_head = NULL, *g_fwd_tail = NULL;
static size_t g_fwd_queue_bytes = 0;
static int g_fwd_stop = 0;
static pthread_t g_fwd_thread;
#define MAX_FWD_QUEUE_BYTES (64 * 1024 * 1024)

/* reactor 调：深拷贝 raw 入队，队列满则等待（背压）。返回 0 成功 / -1 停止中。 */
int repl_fwd_enqueue(conn_t *c, const unsigned char *buf, size_t len, unsigned long long end_offset) {
    pthread_mutex_lock(&g_fwd_lock);
    while (g_fwd_queue_bytes + len > MAX_FWD_QUEUE_BYTES && !g_fwd_stop)
        pthread_cond_wait(&g_fwd_cond, &g_fwd_lock);
    if (g_fwd_stop) { pthread_mutex_unlock(&g_fwd_lock); return -1; }
    repl_fwd_node_t *n = (repl_fwd_node_t *)kvs_malloc(sizeof(*n) + len);
    if (!n) { pthread_mutex_unlock(&g_fwd_lock); return -1; }
    n->c = c;
    n->buf = (unsigned char *)(n + 1);
    n->len = len;
    n->end_offset = end_offset;
    memcpy(n->buf, buf, len);
    n->next = NULL;
    if (g_fwd_tail) g_fwd_tail->next = n; else g_fwd_head = n;
    g_fwd_tail = n;
    g_fwd_queue_bytes += len;
    pthread_cond_signal(&g_fwd_cond);
    pthread_mutex_unlock(&g_fwd_lock);
    return 0;
}

/* ---- 转发线程每 slave 发送状态（转发线程独占，独立于 conn 的 out_ring） ---- */
typedef struct repl_fwd_send_state_s {
    conn_t *c;
    unsigned char *buf;                     /* 累积发送缓冲 */
    size_t len, cap;
    unsigned long long watermark;           /* 已交给转发线程的最高 master offset（终点 exclusive）；
                                               st->buf 内字节对应 [.., watermark)；去重：<= watermark
                                               的节点不再追加（避免 REPLACK/REPLDONE 重发已在下游的字节） */
    int stalled;                            /* 缓冲达 MAX_FWD_SEND_BUF_BYTES：暂停追加，转 backlog/背压 */
    int armed_epollout;                     /* 该 fd 当前是否注册了 EPOLLOUT */
    int dropped;                            /* reactor close_conn 已 purge：转发线程下次见到即丢弃（防 UAF） */
    struct repl_fwd_send_state_s *next;
} repl_fwd_send_state_t;

static repl_fwd_send_state_t *g_fwd_states = NULL;   /* 转发线程独占 */
static int g_fwd_epfd = -1;                          /* 转发线程自有 epoll（EPOLLOUT 可写） */

static repl_fwd_send_state_t *repl_fwd_find_state(conn_t *c) {
    for (repl_fwd_send_state_t *st = g_fwd_states; st; st = st->next) {
        if (st->c == c) return st;
    }
    return NULL;
}

static repl_fwd_send_state_t *repl_fwd_get_or_create(conn_t *c) {
    repl_fwd_send_state_t *st = repl_fwd_find_state(c);
    if (st) return st;
    /* 惰性建立：从 g_replicas 找对应 conn（转发线程未见到前可能已建好） */
    st = (repl_fwd_send_state_t *)kvs_malloc(sizeof(*st));
    if (!st) return NULL;
    st->c = c;
    st->buf = NULL;
    st->len = st->cap = 0;
    st->watermark = 0;
    st->stalled = 0;
    st->armed_epollout = 0;
    st->dropped = 0;
    st->next = g_fwd_states;
    g_fwd_states = st;
    return st;
}

static void repl_fwd_disarm_epollout(repl_fwd_send_state_t *st) {
    if (!st || !st->armed_epollout) return;
    if (g_fwd_epfd >= 0) epoll_ctl(g_fwd_epfd, EPOLL_CTL_DEL, st->c->fd, NULL);
    st->armed_epollout = 0;
}

static void repl_fwd_arm_epollout(repl_fwd_send_state_t *st) {
    if (!st || g_fwd_epfd < 0) return;
    if (st->armed_epollout) return;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLOUT;
    ev.data.fd = st->c->fd;
    if (epoll_ctl(g_fwd_epfd, EPOLL_CTL_ADD, st->c->fd, &ev) == 0)
        st->armed_epollout = 1;
}

/* 非阻塞发送缓冲。发完 → 更新 offset/时间戳；EAGAIN → 注册 EPOLLOUT；硬错误 → 丢弃。
 * 仅由转发线程调用，且持 g_fwd_lock：与 reactor 的 repl_fwd_purge_conn 串行化，防止
 * close_conn 释放 conn_t 后此处仍解引用 st->c（UAF）。st->dropped 时丢弃残留缓冲。 */
static void repl_fwd_drain_send(repl_fwd_send_state_t *st) {
    if (!st || st->len == 0) return;
    if (g_repl_fullsync_in_progress) return;   /* 全量期间不发增量，避免与全量发送者并发写同一 fd */
    if (st->dropped) {
        if (st->buf) { kvs_free(st->buf); st->buf = NULL; st->cap = 0; }
        st->len = 0;
        st->armed_epollout = 0;   /* g_fwd_epfd 的注册已由 purge 拆除 */
        return;
    }
    conn_t *c = st->c;
    size_t off = 0;
    while (off < st->len) {
        ssize_t n = write(c->fd, st->buf + off, st->len - off);
        if (n > 0) { off += (size_t)n; continue; }
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                repl_fwd_arm_epollout(st);
            } else {
                /* 断连/其它错误：丢弃该 slave 待发数据，交给 close_conn 的 purge 清理 */
                st->len = 0;
                if (st->buf) { kvs_free(st->buf); st->buf = NULL; st->cap = 0; }
                repl_fwd_disarm_epollout(st);
            }
            break;
        }
        break;   /* n == 0 不应发生（len>0），防御性 break */
    }
    if (off > 0) {
        memmove(st->buf, st->buf + off, st->len - off);
        st->len -= off;
    }
    if (st->len == 0 && !st->dropped) {
        repl_fwd_disarm_epollout(st);
        c->repl_offset_sent = repl_master_offset();
        c->repl_last_send_ms = kvs_now_ms();
    }
}

int repl_backlog_copy_range(unsigned long long offset, unsigned char **out_buf, size_t *out_len);

/* 持 g_fwd_lock 调用：向 st->buf 追加 [start, end) 中超出 watermark 的未覆盖后缀。
 * 以 watermark 去重：end <= watermark 已在下游覆盖 → 不追加；start < watermark 时剔除前缀。
 * 超 MAX_FWD_SEND_BUF_BYTES → 标 stalled，不追加（字节已在 backlog，靠 REPLACK/重放追回）。
 * 返回是否成功追加（0 无/失败，1 追加）。 */
static int repl_fwd_append(repl_fwd_send_state_t *st, const unsigned char *buf,
                           unsigned long long start, size_t len, unsigned long long end) {
    if (!st || st->dropped || st->stalled || len == 0) return 0;
    if (end <= st->watermark) return 0;                  /* 全部已覆盖 */
    size_t covered = (start < st->watermark) ? (size_t)(st->watermark - start) : 0;
    if (covered >= len) return 0;
    const unsigned char *src = buf + covered;
    size_t n = len - covered;
    if (st->len + n > MAX_FWD_SEND_BUF_BYTES) {
        st->stalled = 1;                                 /* 慢 slave：暂停追加，背压 */
        return 0;
    }
    if (st->len + n > st->cap) {
        size_t nc = st->cap ? st->cap : 4096;
        while (nc < st->len + n) nc *= 2;
        unsigned char *nb = kvs_realloc(st->buf, nc);
        if (!nb) { st->stalled = 1; return 0; }
        st->buf = nb; st->cap = nc;
    }
    memcpy(st->buf + st->len, src, n);
    st->len += n;
    if (end > st->watermark) st->watermark = end;
    return 1;
}

/* 持 g_fwd_lock 调用：缓冲有空间且落后于 backlog 尽头时，从 backlog 深拷贝追赶字节补进缓冲，
 * 使慢 slave 追平（不依赖 REPLACK 触发）。调用方在 drain 后调用。
 * stalled 时也调用：缓冲排出空间即解除 stall 并续供。 */
static void repl_fwd_refill(repl_fwd_send_state_t *st) {
    if (!st || st->dropped) return;
    if (g_repl_fullsync_in_progress) return;   /* 全量期间不拉增量（自愈追赶延后，backlog 保底） */
    /* 缓冲已排出空间（< 半满）：解除 stall，允许继续供数 */
    if (st->stalled && st->len < MAX_FWD_SEND_BUF_BYTES / 2) st->stalled = 0;
    if (st->stalled) return;
    if (st->len >= MAX_FWD_SEND_BUF_BYTES) return;
    unsigned long long end = repl_backlog_end_offset();
    if (st->watermark >= end) return;
    unsigned char *cbuf = NULL;
    size_t clen = 0;
    if (repl_backlog_copy_range(st->watermark, &cbuf, &clen) != 0 || clen == 0) return;
    repl_fwd_append(st, cbuf, st->watermark, clen, st->watermark + clen);
    kvs_free(cbuf);
}

/* 持 g_fwd_lock 调用：追加（watermark 去重）+ 非阻塞发送。st 已 dropped（conn 已关闭）则丢弃该节点。 */
static void repl_fwd_process_one(repl_fwd_node_t *n) {
    if (!n) return;
    repl_fwd_send_state_t *st = repl_fwd_get_or_create(n->c);
    if (st && !st->dropped) {
        unsigned long long start = n->end_offset > n->len ? n->end_offset - n->len : 0;
        repl_fwd_append(st, n->buf, start, n->len, n->end_offset);
        repl_fwd_drain_send(st);   /* 非阻塞 send + EAGAIN → EPOLLOUT */
        repl_fwd_refill(st);       /* drain 后落追 → 从 backlog 自愈追赶 */
    }
    kvs_free(n);
}

/* reactor 调用：读取该 conn 已交给转发线程的最高 offset（watermark）。持 g_fwd_lock。
 * 用于 REPLACK/REPLDONE 追赶收窄范围，避免重发 st->buf 内未刷字节（非幂等命令双倍应用）。 */
unsigned long long repl_fwd_get_watermark(conn_t *c) {
    if (!c) return 0;
    unsigned long long w = 0;
    pthread_mutex_lock(&g_fwd_lock);
    repl_fwd_send_state_t *st = repl_fwd_find_state(c);
    if (st && !st->dropped) w = st->watermark;
    pthread_mutex_unlock(&g_fwd_lock);
    return w;
}

/* reactor 调用：该 conn 是否已 stalled（缓冲满、转发线程暂停追加）。持 g_fwd_lock。
 * repl_broadcast 据此跳过入队（字节已在 backlog），避免队列被慢 slave 塞满阻塞 reactor。 */
int repl_fwd_is_stalled(conn_t *c) {
    if (!c) return 0;
    int s = 0;
    pthread_mutex_lock(&g_fwd_lock);
    repl_fwd_send_state_t *st = repl_fwd_find_state(c);
    if (st && !st->dropped) s = st->stalled;
    pthread_mutex_unlock(&g_fwd_lock);
    return s;
}

/* 非阻塞扫自有 epoll，收集可写/事件的 slave fd。
 * 仅触碰转发线程独占的 g_fwd_epfd（不触碰任何 conn/state 数据），故无需持 g_fwd_lock：
 * 把 epoll_wait 这个系统调用的耗时移出转发线程对 g_fwd_lock 的持有，减少 reactor
 * repl_fwd_enqueue 的锁竞争（IMPORTANT 3）。返回就绪 fd 数（0 表示无）。 */
static int repl_fwd_epoll_collect(struct epoll_event *evs, int max) {
    if (g_fwd_epfd < 0) return 0;
    return epoll_wait(g_fwd_epfd, evs, max, 0);
}

/* 持 g_fwd_lock 调用：处理单个可写 slave fd（EAGAIN 恢复续发）。（drain 引用 st->c，
 * 必须持锁与 reactor 的 repl_fwd_purge_conn 串行化防止 conn UAF。） */
static void repl_fwd_drain_fd(int fd) {
    if (fd < 0) return;
    for (repl_fwd_send_state_t *st = g_fwd_states; st; st = st->next) {
        if (st->dropped) continue;
        if (st->c->fd == fd) {
            repl_fwd_drain_send(st);
            repl_fwd_refill(st);
            break;
        }
    }
}

static void *repl_fwd_thread_main(void *arg) {
    (void)arg;
    while (!g_fwd_stop) {
        /* 队列入队/退出与 conn 清理（repl_fwd_purge_conn）均经 g_fwd_lock 串行化；
         * 任何 conn_t 解引用（process_one / drain_fd 内）必须持锁，保证发生在
         * reactor close_conn 的 repl_fwd_purge_conn 之前并与之互斥（防 UAF）。
         * IMPORTANT 3：尽量缩短持锁区间——出队+process_one 一段、每个 epollout drain
         * 单独一段，epoll_wait 系统调用本身不持锁，且 stalled/水位线把队列打空后 reactor
         * 的 enqueue 不再因满队列而长等待，使慢 slave 不再串行化整个 master。 */
        pthread_mutex_lock(&g_fwd_lock);
        if (g_fwd_stop) { pthread_mutex_unlock(&g_fwd_lock); break; }
        repl_fwd_node_t *n = NULL;
        if (g_fwd_head) {
            n = g_fwd_head;
            g_fwd_head = n->next;
            if (!g_fwd_head) g_fwd_tail = NULL;
            g_fwd_queue_bytes -= n->len;
        }
        if (n) repl_fwd_process_one(n);
        if (!n && !g_fwd_stop) {
            /* 空队列：有界等待，避免忙轮询；超时返回继续扫 EPOLLOUT */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            long ns = ts.tv_nsec + 50L * 1000000L;
            ts.tv_sec += ns / 1000000000L;
            ts.tv_nsec = ns % 1000000000L;
            pthread_cond_timedwait(&g_fwd_cond, &g_fwd_lock, &ts);
        }
        pthread_mutex_unlock(&g_fwd_lock);

        /* 扫 EPOLLOUT（不持锁做 epoll_wait，降低锁竞争）；每个就绪 fd 单独短持锁 drain */
        struct epoll_event evs[16];
        int ne = repl_fwd_epoll_collect(evs, 16);
        for (int i = 0; i < ne; i++) {
            int fd = evs[i].data.fd;
            if ((evs[i].events & (EPOLLERR | EPOLLHUP)) && (evs[i].events & EPOLLOUT) == 0) continue;
            pthread_mutex_lock(&g_fwd_lock);
            if (!g_fwd_stop) repl_fwd_drain_fd(fd);
            pthread_mutex_unlock(&g_fwd_lock);
        }
    }
    /* 停止：清理发送状态缓冲（线程退出后仅本线程访问，无需持锁） */
    repl_fwd_send_state_t *st = g_fwd_states;
    g_fwd_states = NULL;
    while (st) {
        repl_fwd_send_state_t *nx = st->next;
        if (st->buf) kvs_free(st->buf);
        kvs_free(st);
        st = nx;
    }
    return NULL;
}

/* reactor 的 close_conn 在 free conn_t 之前调用：从转发线程队列 + 发送状态移除该 conn（防 UAF）。
 * 持 g_fwd_lock：与转发线程的并发解引用串行化（转发线程在处理/发送该项时也持这把锁）。
 * 发送状态不直接 free（转发线程独占），而是标 dropped，转发线程下次见到即丢弃残留缓冲；
 * 这样即便有已出队未处理的节点，也绝不会对已释放的 conn 重新建状态。 */
void repl_fwd_purge_conn(conn_t *c) {
    if (!c) return;
    pthread_mutex_lock(&g_fwd_lock);
    /* 队列中剔除该 conn 的项 */
    repl_fwd_node_t **pp = &g_fwd_head;
    while (*pp) {
        repl_fwd_node_t *n = *pp;
        if (n->c == c) {
            *pp = n->next;
            g_fwd_queue_bytes -= n->len;
            kvs_free(n);
        } else {
            pp = &n->next;
        }
    }
    /* 尾部被剔除时重建 tail（单链表：从 head 走到末节点），避免 enqueue 从 NULL tail 覆盖 head */
    g_fwd_tail = NULL;
    if (g_fwd_head) {
        repl_fwd_node_t *it = g_fwd_head;
        while (it->next) it = it->next;
        g_fwd_tail = it;
    }
    /* 发送状态：标 dropped + 拆除自有 epoll 注册（c->fd 尚未 close） */
    for (repl_fwd_send_state_t *st = g_fwd_states; st; st = st->next) {
        if (st->c == c && !st->dropped) {
            st->dropped = 1;
            st->armed_epollout = 0;
            if (g_fwd_epfd >= 0) epoll_ctl(g_fwd_epfd, EPOLL_CTL_DEL, c->fd, NULL);
        }
    }
    c->repl_draining = 1;   /* 广播侧后续按 draining 丢弃（repl_remove_slave 已将其清零） */
    pthread_mutex_unlock(&g_fwd_lock);
}

int repl_fwd_start(void) {
    pthread_mutex_lock(&g_fwd_lock);
    g_fwd_stop = 0;
    pthread_mutex_unlock(&g_fwd_lock);
    if (g_fwd_epfd < 0) g_fwd_epfd = epoll_create1(0);
    return pthread_create(&g_fwd_thread, NULL, repl_fwd_thread_main, NULL) == 0 ? 0 : -1;
}
void repl_fwd_stop(void) {
    pthread_mutex_lock(&g_fwd_lock);
    g_fwd_stop = 1;
    pthread_cond_broadcast(&g_fwd_cond);
    pthread_mutex_unlock(&g_fwd_lock);
    pthread_join(g_fwd_thread, NULL);
    /* 清理残余队列 */
    for (repl_fwd_node_t *n = g_fwd_head; n; ) {
        repl_fwd_node_t *nx = n->next; kvs_free(n); n = nx;
    }
    g_fwd_head = g_fwd_tail = NULL; g_fwd_queue_bytes = 0;
}

/* 传输层日志 */
static FILE *g_transport_log = NULL;
static pthread_mutex_t g_transport_log_lock = PTHREAD_MUTEX_INITIALIZER;

static void transport_log(const char *fmt, ...) {
    va_list ap;
    time_t now;
    struct tm *tm_info;
    char timestamp[64];

    pthread_mutex_lock(&g_transport_log_lock);
    if (!g_transport_log) {
        g_transport_log = fopen("kvstore_transport.log", "a");
        if (!g_transport_log) { pthread_mutex_unlock(&g_transport_log_lock); return; }
    }
    time(&now);
    tm_info = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);
    fprintf(g_transport_log, "[%s] ", timestamp);
    va_start(ap, fmt);
    vfprintf(g_transport_log, fmt, ap);
    va_end(ap);
    fprintf(g_transport_log, "\n");
    fflush(g_transport_log);
    pthread_mutex_unlock(&g_transport_log_lock);
}

typedef struct repl_backlog_s {
    unsigned char *buf;
    size_t cap;
    size_t histlen;
    size_t head;
    unsigned long long start_offset;
    unsigned long long end_offset;
} repl_backlog_t;

static pthread_mutex_t g_slave_conf_lock = PTHREAD_MUTEX_INITIALIZER;
/* backlog 专用锁：repl_backlog_feed 被转发线程与 reactor/fullsync 并发调用 */
static pthread_mutex_t g_backlog_lock = PTHREAD_MUTEX_INITIALIZER;
#if KVS_ENABLE_RDMA
static pthread_mutex_t g_repl_rdma_send_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_repl_rdma_cq_lock = PTHREAD_MUTEX_INITIALIZER;
#endif
static char g_slave_host[128] = "";
static int g_slave_port = 0;
static int g_slave_conf_gen = 0;
static int g_master_link_up = 0;
static int g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
/* 非 static，供 kvstore.c 中的 KPROBEMR handler 使用 */
int g_slave_fd = -1;
static long long g_master_last_io_ms = 0;
static int g_slave_thread_started = 0;
static int g_rdma_master_listener_started = 0;
static long long g_slave_last_ack_ms = 0;
static char g_master_replid[41] = {0};
static unsigned long long g_master_repl_offset = 0;
static unsigned long long g_repl_fullsync_count = 0;
static unsigned long long g_repl_partialsync_ok_count = 0;
static unsigned long long g_repl_partialsync_err_count = 0;
static unsigned long long g_repl_broadcast_bytes = 0;
static unsigned long long g_repl_snapshot_bytes = 0;
static unsigned long long g_rdma_disconnect_count = 0;
static unsigned long long g_rdma_reject_count = 0;
static unsigned long long g_rdma_send_cq_error_count = 0;
static unsigned long long g_rdma_recv_cq_error_count = 0;
static unsigned long long g_repl_transport_fallback_count = 0;
static char g_repl_transport_active[32] = "tcp";
static char g_repl_transport_fallback_reason[64] = "";
static long long g_repl_transport_fallback_until_ms = 0;
static repl_backlog_t g_repl_backlog = {0};
static char g_slave_master_replid[41] = "?";
static unsigned long long g_slave_repl_offset = 0;
static unsigned long long g_slave_repl_applied_offset = 0;
static unsigned long long g_slave_repl_durable_offset = 0;
int g_slave_loading_fullsync = 0;
unsigned long long g_slave_fullsync_target_bytes = 0;
unsigned long long g_slave_fullsync_loaded_bytes = 0;
int g_slave_fullsync_tmp_fd = -1;   /* temp file fd for receiving KVSD full sync data */
static char g_slave_state_path[512] = {0};
static conn_t g_rdma_master_replica_conn = {0};

#if KVS_ENABLE_RDMA
#define KVS_RDMA_RECV_SLOTS_MAX 128
#define KVS_RDMA_RECV_SLOTS_DEFAULT 64             /* P3: 32→64 */
#define KVS_RDMA_CHUNK_SIZE_DEFAULT (BUFFER_CAP * 4)
#define KVS_RDMA_QP_WR_DEPTH_DEFAULT 64

/* ---- Pipeline Constants ---- */
#define KVS_RDMA_SEND_SLOTS_MAX   64              /* P3: 最大发送管道深度 */
#define KVS_RDMA_SEND_SLOTS_DEFAULT 16            /* P3: 默认 16（原固定 4） */
#define KVS_RDMA_CQ_BATCH            8   /* CQ 批量 poll 大小 */
#define KVS_RDMA_BATCH_MAX           8   /* 批量 send WR 上限（wr.next 链） */
#define KVS_RDMA_SIGNAL_INTERVAL     8   /* P3: 每 N 个 WR 产生 1 个 CQE */
#define KVS_RDMA_MAX_BATCH_TRACKERS  64  /* P3.2: 最多追踪 64 个 batch（64×8=512 slot） */
#define KVS_RDMA_PIPELINE_WR_ID_FLAG 0x80000000UL  /* wr_id 高位标记 pipeline send */

/* P3.2: batch tracker — 追踪批量 post 中各 slot，选择性 signal 时用于批量回收 */
typedef struct {
    int slots[KVS_RDMA_BATCH_MAX];
    int count;
    int signaled_slot;  /* 该 batch 中被 signaled 的 slot 索引 */
    int active;         /* 1 = pending, 0 = free */
} rdma_batch_tracker_t;

typedef enum repl_rdma_state_e {
    REPL_RDMA_STATE_INIT = 0,
    REPL_RDMA_STATE_CONNECTING,
    REPL_RDMA_STATE_ESTABLISHED,
    REPL_RDMA_STATE_SYNCING,
    REPL_RDMA_STATE_STEADY,
    REPL_RDMA_STATE_BACKOFF,
    REPL_RDMA_STATE_FAILED,
    REPL_RDMA_STATE_FALLBACK_TCP,
} repl_rdma_state_t;

static void repl_rdma_reset_ctx(void);
static void repl_rdma_reset_conn_ctx(int preserve_listener);
static void repl_rdma_log(const char *stage, const char *detail);
static void repl_rdma_set_state(repl_rdma_state_t st, const char *reason);
static const char *repl_rdma_state_name(repl_rdma_state_t st);
static void repl_rdma_cq_process_wc(struct ibv_wc *wc);

typedef struct repl_rdma_recv_slot_s {
    struct ibv_mr *mr;
    unsigned char *buf;
    size_t cap;
    int posted;
} repl_rdma_recv_slot_t;

/* ---- Pipeline 发送缓冲区槽位 ---- */
typedef struct repl_rdma_send_slot_s {
    struct ibv_mr *mr;          /* 注册的内存区域 */
    unsigned char *buf;         /* 缓冲区 */
    size_t cap;                 /* 容量 */
    int in_flight;              /* 1 = 已 post 但未完成 */
    uint64_t wr_id;             /* 对应的 wr_id（含 PIPELINE_WR_ID_FLAG） */
} repl_rdma_send_slot_t;

typedef struct repl_rdma_ctx_s {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
    struct rdma_cm_id *listen_id;
    struct rdma_cm_id *accepted_id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_chan;
    /* ---- 旧单 send_buf 保留作 fallback, send_pipeline 启用后不再使用 ---- */
    struct ibv_mr *send_mr;
    unsigned char *send_buf;
    size_t send_buf_cap;
    repl_rdma_recv_slot_t recv_slots[KVS_RDMA_RECV_SLOTS_MAX];
    size_t recv_buf_cap;
    int pending_recv_slots[KVS_RDMA_RECV_SLOTS_MAX];
    size_t pending_recv_lens[KVS_RDMA_RECV_SLOTS_MAX];
    int pending_recv_head;
    int pending_recv_tail;
    int pending_recv_count;
    int active_recv_slots;
    int active_qp_wr_depth;
    size_t active_chunk_size;
    int addr_resolved;
    int route_resolved;
    int qp_ready;
    int connected;
    repl_rdma_state_t state;
    /* ---- Pipeline 发送缓冲区 ---- */
    repl_rdma_send_slot_t send_slots[KVS_RDMA_SEND_SLOTS_MAX];
    int send_pipeline_head;          /* 下一个可用的空闲 slot 索引 */
    int send_slots_in_flight;        /* 当前 outstanding send WR 数 */
    int send_pipeline_depth;         /* 当前生效的 pipeline 深度（≤ KVS_RDMA_SEND_SLOTS_MAX） */
    int send_pipeline_enabled;       /* 是否启用 pipeline 模式 */

    /* ---- CQ 轮询线程 ---- */
    pthread_t cq_poll_thread;        /* CQ 轮询线程 ID */
    int cq_poll_thread_running;      /* CQ 轮询线程是否运行 */

    /* P3.3: 条件变量 — CQ 线程释放 slot 后唤醒发送线程。
     * 使用 g_repl_rdma_send_lock 作为保护锁，pthread_cond_wait 会原子解锁等。
     */
    pthread_cond_t send_slot_cond;

    /* P3.1b: 批量 post — 积累 WR 后一次 ibv_post_send */
    int pending_batch_count;
    int pending_batch_slots[KVS_RDMA_BATCH_MAX];
    size_t pending_batch_lens[KVS_RDMA_BATCH_MAX];

    /* P3.2: batch tracker — 选择性 signal 时追踪 batch → 批量回收 */
    rdma_batch_tracker_t batch_trackers[KVS_RDMA_MAX_BATCH_TRACKERS];
    int batch_tracker_next;  /* 下一个可用 tracker 索引 */

    /* ---- One-Sided WRITE ---- */
    int use_write_mode;
    uint64_t remote_write_addr;
    uint32_t remote_write_rkey;
    size_t write_total_sent;
    size_t write_total_size;

    /* ---- Fullsync transfer 状态 ---- */
    uint64_t transfer_id;         /* 当前 transfer ID（master/slave 共用） */
    uint64_t remote_capacity;     /* 远端 MR 容量 */
    size_t write_offset;          /* 已 WRITE 的字节数 */
    int posted_wr;                /* 已 post 的 WR 数 */
    int completed_wr;             /* 已完成的 WR 数 */
    int final_imm_posted;         /* 末包 WRITE_WITH_IMM 是否已 post */
    int cq_failed;                /* CQ 是否已失败 */
    conn_t *transfer_owner;       /* 拥有该 transfer 的 master 连接 */
} repl_rdma_ctx_t;

static repl_rdma_ctx_t g_repl_rdma_ctx = {0};

/* One-Sided WRITE: Slave 目标（一次 transfer 的接收端资源）。
 * 由 repl_fullsync_target_t 统一持有，清理走 repl_rdma_slave_target_cleanup()。 */
static repl_fullsync_target_t g_slave_fullsync_target;

/* 已被 master 放弃的 transfer id（FULLSYNCABORT）。即使 abort 在 prepare_target
 * 之前到达（目标尚未创建），prepare_target 也会拒绝该 tid，避免为回退的 transfer
 * 创建空目标文件。 */
static uint64_t g_slave_fullsync_aborted_tid = 0;

/* One-Sided WRITE: 是否已收到最终 WRITE_WITH_IMM（= g_slave_fullsync_target.imm_received 别名） */
#endif

typedef struct repl_transport_ops_s {
    const char *name;
    int supported;
    int (*send)(conn_t *c, const unsigned char *buf, size_t len);
    int (*connect_slave)(const char *host, int port);
    void (*disconnect_slave)(int fd);
} repl_transport_ops_t;

static int repl_transport_tcp_send(conn_t *c, const unsigned char *buf, size_t len) {
    if (!c || !buf || len == 0) return 0;
    return queue_bytes(c, buf, len);
}

static int repl_transport_ebpf_send(conn_t *c, const unsigned char *buf, size_t len) {
    /* eBPF 增量传输：
     *
     * 数据路径: queue_bytes → reactor on_write → send(c->fd) 
     *          → 内核触发 sk_msg BPF (因 fd 已注册到 sockmap)
     *          → BPF 执行 bpf_msg_redirect_map() 
     *          → sock_map[redirect_key] 的 TCP → 远端 slave
     *
     * c->fd 已通过 register_fd() 注册到 sock_map 和 role_map，
     * 因此 send() 系统调用会被 BPF 程序拦截并重定向。
     * 传输层为 TCP（跨机器数据必须走网络协议），
     * 但数据路由由 eBPF 程序在内核态决策，而非直接走内核 TCP 栈。 */
    return queue_bytes(c, buf, len);
}

static int repl_transport_tcp_connect_slave(const char *host, int port) {
    int fd;
    struct timeval tv;
    struct sockaddr_in addr;
    if (!host || host[0] == '\0' || port <= 0) return -1;
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

#if KVS_ENABLE_RDMA
static void repl_rdma_log(const char *stage, const char *detail) {
    fprintf(stderr, "repl rdma: %s%s%s\n", stage ? stage : "?", detail ? " - " : "", detail ? detail : "");
}

static int repl_rdma_cfg_recv_slots(void) {
    int v = g_cfg.rdma_recv_slots;
    if (v <= 0) v = KVS_RDMA_RECV_SLOTS_DEFAULT;
    if (v > KVS_RDMA_RECV_SLOTS_MAX) v = KVS_RDMA_RECV_SLOTS_MAX;
    return v;
}

static int repl_rdma_cfg_send_slots(void) {
    int v = g_cfg.rdma_send_slots;
    if (v <= 0) v = KVS_RDMA_SEND_SLOTS_DEFAULT;
    if (v > KVS_RDMA_SEND_SLOTS_MAX) v = KVS_RDMA_SEND_SLOTS_MAX;
    return v;
}

static int repl_rdma_cfg_qp_wr_depth(void) {
    int v = g_cfg.rdma_qp_wr_depth;
    int min_depth = repl_rdma_cfg_recv_slots() * 2;
    if (v <= 0) v = KVS_RDMA_QP_WR_DEPTH_DEFAULT;
    if (v < min_depth) v = min_depth;
    return v;
}

static size_t repl_rdma_cfg_chunk_size(void) {
    size_t v = (g_cfg.rdma_chunk_size > 0) ? (size_t)g_cfg.rdma_chunk_size : (size_t)KVS_RDMA_CHUNK_SIZE_DEFAULT;
    if (v < 1024) v = 1024;
    if (v > BUFFER_CAP * 4) v = BUFFER_CAP * 4;
    return v;
}

static void repl_rdma_refresh_runtime_cfg(void) {
    g_repl_rdma_ctx.active_recv_slots = repl_rdma_cfg_recv_slots();
    g_repl_rdma_ctx.active_qp_wr_depth = repl_rdma_cfg_qp_wr_depth();
    g_repl_rdma_ctx.active_chunk_size = repl_rdma_cfg_chunk_size();
}

/* 前向声明 */
static int repl_rdma_pending_recv_push(int slot, size_t len);
static int repl_rdma_pending_recv_pop(int *slot_out, size_t *len_out);
static int repl_rdma_flush_batch_locked(void);
static void repl_rdma_stop_cq_poll_thread(void);
static volatile int g_cq_poll_thread_exited = 0;
static int repl_rdma_start_cq_poll_thread(void);

/* ---- Pipeline: 获取一个空闲的 send slot ----
 * 返回 slot 索引，-1 表示所有 slot 均在飞行中。
 * 如果 timeout_ms > 0，会忙等待直到有 slot 可用或超时。
 */
static int repl_rdma_acquire_send_slot(int timeout_ms) {
    int slot;
    long long deadline = timeout_ms > 0 ? kvs_now_ms() + timeout_ms : 0;
    for (;;) {
        /* 连接已断开，立即返回 */
        if (!g_repl_rdma_ctx.connected) break;
        /* 线性扫描取第一个空闲 slot */
        for (int i = 0; i < g_repl_rdma_ctx.send_pipeline_depth; ++i) {
            slot = (g_repl_rdma_ctx.send_pipeline_head + i) % g_repl_rdma_ctx.send_pipeline_depth;
            if (!g_repl_rdma_ctx.send_slots[slot].in_flight) {
                g_repl_rdma_ctx.send_pipeline_head = (slot + 1) % g_repl_rdma_ctx.send_pipeline_depth;
                return slot;
            }
        }
        /* 所有 slot 均在飞行中 */
        if (g_repl_rdma_ctx.cq_poll_thread_running) {
            /* P3.3: CQ 线程 release_send_slot 时 signal，pthread_cond_timedwait
             * 原子解锁 g_repl_rdma_send_lock → 等待 → 重新加锁。
             * 50ms 超时作回退（CQ 线程可能阻塞在 ibv_get_cq_event）。 */
            if (timeout_ms <= 0) break;
            {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                long long remaining_ms = deadline - kvs_now_ms();
                if (remaining_ms <= 0) break;
                if (remaining_ms > 50) remaining_ms = 50;
                ts.tv_sec += (time_t)(remaining_ms / 1000);
                ts.tv_nsec += (long)((remaining_ms % 1000) * 1000000);
                if (ts.tv_nsec >= 1000000000L) {
                    ts.tv_sec++; ts.tv_nsec -= 1000000000L;
                }
                /* cond_wait 会原子释放锁并在被唤醒时重新加锁，
                 * 这正是我们需要的——释放锁让 CQ 线程可以 signal */
                pthread_cond_timedwait(&g_repl_rdma_ctx.send_slot_cond,
                                       &g_repl_rdma_send_lock, &ts);
            }
            continue;
        }
        /* CQ 轮询线程未运行：直接 poll CQ 回收 completion */
        if (g_repl_rdma_ctx.connected && g_repl_rdma_ctx.cq) {
            struct ibv_wc wc;
            if (ibv_poll_cq(g_repl_rdma_ctx.cq, 1, &wc) > 0) {
                if (wc.status == IBV_WC_SUCCESS &&
                    (wc.opcode == IBV_WC_SEND) &&
                    (wc.wr_id & KVS_RDMA_PIPELINE_WR_ID_FLAG)) {
                    int done_slot = (int)(wc.wr_id & ~KVS_RDMA_PIPELINE_WR_ID_FLAG);
                    if (done_slot >= 0 && done_slot < g_repl_rdma_ctx.send_pipeline_depth) {
                        g_repl_rdma_ctx.send_slots[done_slot].in_flight = 0;
                        g_repl_rdma_ctx.send_slots[done_slot].wr_id = 0;
                        g_repl_rdma_ctx.send_slots_in_flight--;
                        continue; /* 重试 */
                    }
                } else if (wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_RECV) {
                    /* 顺便处理 recv completion */
                    int recv_slot = (wc.wr_id > 0 && wc.wr_id <= (uint64_t)g_repl_rdma_ctx.active_recv_slots)
                        ? (int)(wc.wr_id - 1) : -1;
                    if (recv_slot >= 0 && recv_slot < g_repl_rdma_ctx.active_recv_slots) {
                        g_repl_rdma_ctx.recv_slots[recv_slot].posted = 0;
                        repl_rdma_pending_recv_push(recv_slot, (size_t)wc.byte_len);
                    }
                    continue;
                } else {
                    fprintf(stderr, "repl rdma: acquire_send_slot unexpected wc status=%d opcode=%d\n",
                        wc.status, wc.opcode);
                }
            }
        }
        if (timeout_ms <= 0) break;
        if (kvs_now_ms() >= deadline) break;
        usleep(500);
    }
    return -1;
}

/* ---- Pipeline: 释放一个 send slot（由 CQ completion 回调调用）---- */
static void repl_rdma_release_send_slot(int slot) {
    if (slot >= 0 && slot < g_repl_rdma_ctx.send_pipeline_depth) {
        g_repl_rdma_ctx.send_slots[slot].in_flight = 0;
        g_repl_rdma_ctx.send_slots[slot].wr_id = 0;
        g_repl_rdma_ctx.send_slots_in_flight--;
        /* P3.3: 通知等待 slot 的发送线程 */
        pthread_mutex_lock(&g_repl_rdma_send_lock);
        pthread_cond_signal(&g_repl_rdma_ctx.send_slot_cond);
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
    }
}

static int repl_rdma_pending_recv_push(int slot, size_t len) {
    if (slot < 0 || slot >= KVS_RDMA_RECV_SLOTS_MAX) return -1;
    if (g_repl_rdma_ctx.pending_recv_count >= KVS_RDMA_RECV_SLOTS_MAX) return -1;
    g_repl_rdma_ctx.pending_recv_slots[g_repl_rdma_ctx.pending_recv_tail] = slot;
    g_repl_rdma_ctx.pending_recv_lens[g_repl_rdma_ctx.pending_recv_tail] = len;
    g_repl_rdma_ctx.pending_recv_tail = (g_repl_rdma_ctx.pending_recv_tail + 1) % KVS_RDMA_RECV_SLOTS_MAX;
    g_repl_rdma_ctx.pending_recv_count++;
    return 0;
}

static int repl_rdma_pending_recv_pop(int *slot_out, size_t *len_out) {
    int slot;
    size_t len;
    if (g_repl_rdma_ctx.pending_recv_count <= 0) return -1;
    slot = g_repl_rdma_ctx.pending_recv_slots[g_repl_rdma_ctx.pending_recv_head];
    len = g_repl_rdma_ctx.pending_recv_lens[g_repl_rdma_ctx.pending_recv_head];
    g_repl_rdma_ctx.pending_recv_head = (g_repl_rdma_ctx.pending_recv_head + 1) % KVS_RDMA_RECV_SLOTS_MAX;
    g_repl_rdma_ctx.pending_recv_count--;
    if (slot_out) *slot_out = slot;
    if (len_out) *len_out = len;
    return 0;
}

static void repl_rdma_reset_conn_ctx(int preserve_listener) {
    int i;
    /* 先停止 CQ 轮询线程（后续销毁 comp_chan/cq 时会唤醒线程） */
    g_repl_rdma_ctx.connected = 0;
    repl_rdma_stop_cq_poll_thread();
    /* 等待主线程消费完 pending_recv 队列中的残余 recv 数据 */
    g_repl_rdma_ctx.pending_recv_count = 0;
    g_repl_rdma_ctx.pending_recv_head = 0;
    g_repl_rdma_ctx.pending_recv_tail = 0;
    for (i = 0; i < KVS_RDMA_RECV_SLOTS_MAX; ++i) {
        if (g_repl_rdma_ctx.recv_slots[i].mr) {
            ibv_dereg_mr(g_repl_rdma_ctx.recv_slots[i].mr);
            g_repl_rdma_ctx.recv_slots[i].mr = NULL;
        }
        if (g_repl_rdma_ctx.recv_slots[i].buf) {
            kvs_free(g_repl_rdma_ctx.recv_slots[i].buf);
            g_repl_rdma_ctx.recv_slots[i].buf = NULL;
        }
        g_repl_rdma_ctx.recv_slots[i].cap = 0;
        g_repl_rdma_ctx.recv_slots[i].posted = 0;
    }
    /* 清理旧单 send_buf（兼容） */
    if (g_repl_rdma_ctx.send_mr) {
        ibv_dereg_mr(g_repl_rdma_ctx.send_mr);
        g_repl_rdma_ctx.send_mr = NULL;
    }
    if (g_repl_rdma_ctx.send_buf) {
        kvs_free(g_repl_rdma_ctx.send_buf);
        g_repl_rdma_ctx.send_buf = NULL;
    }
    g_repl_rdma_ctx.send_buf_cap = 0;
    /* 清理 pipeline 多发送缓冲区 */
    for (i = 0; i < KVS_RDMA_SEND_SLOTS_MAX; ++i) {
        if (g_repl_rdma_ctx.send_slots[i].mr) {
            ibv_dereg_mr(g_repl_rdma_ctx.send_slots[i].mr);
            g_repl_rdma_ctx.send_slots[i].mr = NULL;
        }
        if (g_repl_rdma_ctx.send_slots[i].buf) {
            kvs_free(g_repl_rdma_ctx.send_slots[i].buf);
            g_repl_rdma_ctx.send_slots[i].buf = NULL;
        }
        g_repl_rdma_ctx.send_slots[i].cap = 0;
        g_repl_rdma_ctx.send_slots[i].in_flight = 0;
        g_repl_rdma_ctx.send_slots[i].wr_id = 0;
    }
    g_repl_rdma_ctx.send_pipeline_head = 0;
    g_repl_rdma_ctx.send_slots_in_flight = 0;
    g_repl_rdma_ctx.send_pipeline_depth = KVS_RDMA_SEND_SLOTS_MAX;
    g_repl_rdma_ctx.send_pipeline_enabled = 0;
    g_repl_rdma_ctx.recv_buf_cap = 0;
    memset(g_repl_rdma_ctx.pending_recv_slots, 0, sizeof(g_repl_rdma_ctx.pending_recv_slots));
    memset(g_repl_rdma_ctx.pending_recv_lens, 0, sizeof(g_repl_rdma_ctx.pending_recv_lens));
    g_repl_rdma_ctx.pending_recv_head = 0;
    g_repl_rdma_ctx.pending_recv_tail = 0;
    g_repl_rdma_ctx.pending_recv_count = 0;
    g_repl_rdma_ctx.active_recv_slots = 0;
    g_repl_rdma_ctx.active_qp_wr_depth = 0;
    g_repl_rdma_ctx.active_chunk_size = 0;
    if (g_repl_rdma_ctx.cq) {
        ibv_destroy_cq(g_repl_rdma_ctx.cq);
        g_repl_rdma_ctx.cq = NULL;
    }
    if (g_repl_rdma_ctx.comp_chan) {
        ibv_destroy_comp_channel(g_repl_rdma_ctx.comp_chan);
        g_repl_rdma_ctx.comp_chan = NULL;
    }
    if (g_repl_rdma_ctx.pd) {
        ibv_dealloc_pd(g_repl_rdma_ctx.pd);
        g_repl_rdma_ctx.pd = NULL;
    }
    if (g_repl_rdma_ctx.id) {
        struct rdma_cm_id *id = g_repl_rdma_ctx.id;
        g_repl_rdma_ctx.id = NULL;
        if (g_repl_rdma_ctx.accepted_id == id) g_repl_rdma_ctx.accepted_id = NULL;
        if (g_repl_rdma_ctx.listen_id == id) g_repl_rdma_ctx.listen_id = NULL;
        rdma_destroy_id(id);
    }
    if (g_repl_rdma_ctx.accepted_id) {
        struct rdma_cm_id *accepted_id = g_repl_rdma_ctx.accepted_id;
        g_repl_rdma_ctx.accepted_id = NULL;
        if (g_repl_rdma_ctx.listen_id == accepted_id) g_repl_rdma_ctx.listen_id = NULL;
        rdma_destroy_id(accepted_id);
    }
    if (!preserve_listener) {
        if (g_repl_rdma_ctx.listen_id) {
            rdma_destroy_id(g_repl_rdma_ctx.listen_id);
            g_repl_rdma_ctx.listen_id = NULL;
        }
        if (g_repl_rdma_ctx.ec) {
            rdma_destroy_event_channel(g_repl_rdma_ctx.ec);
            g_repl_rdma_ctx.ec = NULL;
        }
    }
    g_repl_rdma_ctx.addr_resolved = 0;
    g_repl_rdma_ctx.route_resolved = 0;
    g_repl_rdma_ctx.qp_ready = 0;
    g_repl_rdma_ctx.connected = 0;
    /* One-Sided WRITE / transfer 状态复位 */
    g_repl_rdma_ctx.use_write_mode = 0;
    g_repl_rdma_ctx.remote_write_addr = 0;
    g_repl_rdma_ctx.remote_write_rkey = 0;
    g_repl_rdma_ctx.write_total_sent = 0;
    g_repl_rdma_ctx.write_total_size = 0;
    g_repl_rdma_ctx.transfer_id = 0;
    g_repl_rdma_ctx.remote_capacity = 0;
    g_repl_rdma_ctx.write_offset = 0;
    g_repl_rdma_ctx.posted_wr = 0;
    g_repl_rdma_ctx.completed_wr = 0;
    g_repl_rdma_ctx.final_imm_posted = 0;
    g_repl_rdma_ctx.cq_failed = 0;
    g_repl_rdma_ctx.transfer_owner = NULL;
    repl_rdma_set_state(preserve_listener ? REPL_RDMA_STATE_BACKOFF : REPL_RDMA_STATE_INIT, preserve_listener ? "reset_conn_ctx_preserve_listener" : "reset_ctx");
}

static void repl_rdma_reset_ctx(void) {
    char last_stage[64];
    char last_preview[160];
    unsigned long long last_len = 0;
    unsigned long long last_offset = 0;
    repl_get_last_send_context(last_stage, sizeof(last_stage), &last_len, &last_offset, last_preview, sizeof(last_preview));
    fprintf(stderr, "repl rdma: reset_ctx - last_send_stage=%s last_send_len=%llu last_send_offset=%llu last_send_preview=%s\n",
        last_stage, last_len, last_offset, last_preview);
    repl_rdma_reset_conn_ctx(0);
}

static int repl_rdma_wait_event(enum rdma_cm_event_type expect, int timeout_ms);
static void repl_rdma_drop_master_replica_from_list(const char *reason);

static int repl_rdma_wait_event(enum rdma_cm_event_type expect, int timeout_ms) {
    struct pollfd pfd;
    struct rdma_cm_event *event = NULL;
    if (!g_repl_rdma_ctx.ec) return -1;
    fprintf(stderr, "repl rdma: wait_event_begin - expect=%s timeout_ms=%d\n", rdma_event_str(expect), timeout_ms);
    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = g_repl_rdma_ctx.ec->fd;
    pfd.events = POLLIN;
    if (poll(&pfd, 1, timeout_ms) <= 0) {
        repl_rdma_log("wait_event", "poll timeout or error");
        return -1;
    }
    if (rdma_get_cm_event(g_repl_rdma_ctx.ec, &event) != 0) {
        repl_rdma_log("wait_event", "rdma_get_cm_event failed");
        return -1;
    }
    int ok = (event->event == expect) ? 0 : -1;
    if (ok == 0) repl_rdma_log("cm_event", rdma_event_str(event->event));
    else fprintf(stderr, "repl rdma: cm_event unexpected - got=%s expect=%s\n", rdma_event_str(event->event), rdma_event_str(expect));
    rdma_ack_cm_event(event);
    return ok;
}

static int repl_rdma_drain_cm_events_nonblock(void) {
    struct pollfd pfd;
    struct rdma_cm_event *event = NULL;
    int saw_disconnect = 0;
    if (!g_repl_rdma_ctx.ec) return 0;
    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = g_repl_rdma_ctx.ec->fd;
    pfd.events = POLLIN;
    while (poll(&pfd, 1, 0) > 0) {
        if (rdma_get_cm_event(g_repl_rdma_ctx.ec, &event) != 0) break;
        repl_rdma_log("cm_event_async", rdma_event_str(event->event));
        switch (event->event) {
            case RDMA_CM_EVENT_DISCONNECTED:
            case RDMA_CM_EVENT_REJECTED:
            case RDMA_CM_EVENT_ADDR_ERROR:
            case RDMA_CM_EVENT_ROUTE_ERROR:
            case RDMA_CM_EVENT_CONNECT_ERROR:
            case RDMA_CM_EVENT_UNREACHABLE:
            case RDMA_CM_EVENT_DEVICE_REMOVAL:
            case RDMA_CM_EVENT_TIMEWAIT_EXIT:
                if (event->event == RDMA_CM_EVENT_REJECTED) g_rdma_reject_count++;
                else g_rdma_disconnect_count++;
                saw_disconnect = 1;
                {
                    char last_stage[64];
                    char last_preview[160];
                    unsigned long long last_len = 0;
                    unsigned long long last_offset = 0;
                    repl_get_last_send_context(last_stage, sizeof(last_stage), &last_len, &last_offset, last_preview, sizeof(last_preview));
                    fprintf(stderr, "repl rdma: cm_event_async_context - last_send_stage=%s last_send_len=%llu last_send_offset=%llu last_send_preview=%s\n",
                        last_stage, last_len, last_offset, last_preview);
                }
                repl_rdma_log("cm_event_async", "marking transport disconnected");
                g_repl_rdma_ctx.connected = 0;
                repl_rdma_drop_master_replica_from_list("cm_event_async_disconnect");
                repl_rdma_set_state(REPL_RDMA_STATE_FAILED, rdma_event_str(event->event));
                break;
            default:
                break;
        }
        rdma_ack_cm_event(event);
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = g_repl_rdma_ctx.ec->fd;
        pfd.events = POLLIN;
    }
    return saw_disconnect ? -1 : 0;
}

static void repl_rdma_drop_master_replica_shallow(conn_t *c) {
    if (!c) return;
    c->next_replica = NULL;
    c->is_replica = 0;
    c->repl_draining = 0;
}

static void repl_rdma_drop_master_replica_from_list(const char *reason) {
    if (!g_rdma_master_replica_conn.is_replica && !g_rdma_master_replica_conn.next_replica && !g_rdma_master_replica_conn.repl_draining) return;
    if (reason && *reason) repl_rdma_log("replica_cleanup", reason);
    g_rdma_master_replica_conn.repl_draining = 1;
    repl_remove_slave(&g_rdma_master_replica_conn);
    repl_rdma_drop_master_replica_shallow(&g_rdma_master_replica_conn);
}

static int repl_rdma_pick_non_loopback_ipv4(struct in_addr *out) {
    struct ifaddrs *ifaddr = NULL;
    struct ifaddrs *ifa = NULL;
    int rc = -1;
    if (!out) return -1;
    if (getifaddrs(&ifaddr) != 0) return -1;
    for (ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
        struct sockaddr_in *sin;
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) continue;
        if ((ifa->ifa_flags & IFF_UP) == 0) continue;
        if (ifa->ifa_flags & IFF_LOOPBACK) continue;
        sin = (struct sockaddr_in *)ifa->ifa_addr;
        if (sin->sin_addr.s_addr == htonl(INADDR_LOOPBACK) || sin->sin_addr.s_addr == 0) continue;
        *out = sin->sin_addr;
        rc = 0;
        break;
    }
    freeifaddrs(ifaddr);
    return rc;
}

static int repl_rdma_prepare_addr(const char *host, int port, struct sockaddr_in *addr) {
    if (!host || !addr || port <= 0) return -1;
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons((uint16_t)port);
    if (!strcmp(host, "127.0.0.1") || !strcmp(host, "localhost")) {
        if (repl_rdma_pick_non_loopback_ipv4(&addr->sin_addr) == 0) {
            char ipbuf[INET_ADDRSTRLEN] = {0};
            inet_ntop(AF_INET, &addr->sin_addr, ipbuf, sizeof(ipbuf));
            fprintf(stderr, "repl rdma: prepare_addr - remapped loopback host %s to %s\n", host, ipbuf[0] ? ipbuf : "?");
            return 0;
        }
    }
    if (inet_pton(AF_INET, host, &addr->sin_addr) <= 0) return -1;
    return 0;
}

static int repl_rdma_create_qp(void) {
    struct ibv_qp_init_attr attr;
    repl_rdma_refresh_runtime_cfg();
    fprintf(stderr, "repl rdma: create_qp - id=%p pd=%p cq=%p verbs=%p wr_depth=%d\n",
            (void *)g_repl_rdma_ctx.id, (void *)g_repl_rdma_ctx.pd,
            (void *)g_repl_rdma_ctx.cq,
            g_repl_rdma_ctx.id ? (void *)g_repl_rdma_ctx.id->verbs : NULL,
            g_repl_rdma_ctx.active_qp_wr_depth);
    if (!g_repl_rdma_ctx.id || !g_repl_rdma_ctx.pd || !g_repl_rdma_ctx.cq) return -1;
    if (!g_repl_rdma_ctx.id->verbs) {
        repl_rdma_log("create_qp", "id->verbs is NULL");
        return -1;
    }
    memset(&attr, 0, sizeof(attr));
    attr.send_cq = g_repl_rdma_ctx.cq;
    attr.recv_cq = g_repl_rdma_ctx.cq;
    attr.qp_type = IBV_QPT_RC;
    attr.cap.max_send_wr = (uint32_t)g_repl_rdma_ctx.active_qp_wr_depth;
    attr.cap.max_recv_wr = (uint32_t)g_repl_rdma_ctx.active_qp_wr_depth;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    if (rdma_create_qp(g_repl_rdma_ctx.id, g_repl_rdma_ctx.pd, &attr) != 0) {
        repl_rdma_log("create_qp", "rdma_create_qp failed");
        return -1;
    }
    g_repl_rdma_ctx.qp_ready = 1;
    repl_rdma_log("create_qp", "ok");
    return 0;
}

static int repl_rdma_connect_handshake(void) {
    const int establish_timeout_ms = 12000;
    if (!g_repl_rdma_ctx.id || !g_repl_rdma_ctx.qp_ready) return -1;
    repl_rdma_log("connect", "issuing rdma_connect");
    repl_rdma_set_state(REPL_RDMA_STATE_CONNECTING, "rdma_connect");
    if (rdma_connect(g_repl_rdma_ctx.id, NULL) != 0) {
        repl_rdma_log("connect", "rdma_connect failed");
        return -1;
    }
    if (repl_rdma_wait_event(RDMA_CM_EVENT_ESTABLISHED, establish_timeout_ms) != 0) {
        repl_rdma_log("connect", "established wait timed out or failed");
        return -1;
    }
    g_repl_rdma_ctx.connected = 1;
    repl_rdma_set_state(REPL_RDMA_STATE_ESTABLISHED, "established");
    repl_rdma_log("connect", "established");
    return 0;
}

static int repl_rdma_prepare_buffers(void) {
    repl_rdma_refresh_runtime_cfg();
    /* P3.3: 初始化条件变量（若已初始化则跳过） */
    static int cond_inited = 0;
    if (!cond_inited) {
        pthread_cond_init(&g_repl_rdma_ctx.send_slot_cond, NULL);
        cond_inited = 1;
    }
    size_t cap = repl_rdma_cfg_chunk_size();
    if (cap < BUFFER_CAP) cap = BUFFER_CAP;
    int i;
    if (!g_repl_rdma_ctx.pd) return -1;
    /* 分配 pipeline 多发送缓冲区 */
    g_repl_rdma_ctx.send_slots_in_flight = 0;
    g_repl_rdma_ctx.send_pipeline_head = 0;
    g_repl_rdma_ctx.send_slots_in_flight = 0;
    g_repl_rdma_ctx.send_pipeline_depth = repl_rdma_cfg_send_slots(); /* P3: 从配置读取 */
    g_repl_rdma_ctx.send_pipeline_enabled = 1;
    for (i = 0; i < KVS_RDMA_SEND_SLOTS_MAX; ++i) {
        g_repl_rdma_ctx.send_slots[i].buf = (unsigned char *)kvs_malloc(cap);
        if (!g_repl_rdma_ctx.send_slots[i].buf) {
            repl_rdma_log("prepare_buffers", "pipeline send buffer alloc failed");
            return -1;
        }
        g_repl_rdma_ctx.send_slots[i].cap = cap;
        g_repl_rdma_ctx.send_slots[i].in_flight = 0;
        g_repl_rdma_ctx.send_slots[i].wr_id = 0;
        memset(g_repl_rdma_ctx.send_slots[i].buf, 0, cap);
        g_repl_rdma_ctx.send_slots[i].mr = ibv_reg_mr(g_repl_rdma_ctx.pd,
            g_repl_rdma_ctx.send_slots[i].buf, cap, IBV_ACCESS_LOCAL_WRITE);
        if (!g_repl_rdma_ctx.send_slots[i].mr) {
            repl_rdma_log("prepare_buffers", "pipeline send mr register failed");
            return -1;
        }
    }
    /* 旧单 send_buf 保留作兼容 */
    g_repl_rdma_ctx.send_buf = (unsigned char *)kvs_malloc(cap);
    if (!g_repl_rdma_ctx.send_buf) {
        repl_rdma_log("prepare_buffers", "legacy buffer alloc failed");
        return -1;
    }
    g_repl_rdma_ctx.send_buf_cap = cap;
    g_repl_rdma_ctx.recv_buf_cap = cap;
    memset(g_repl_rdma_ctx.send_buf, 0, cap);
    g_repl_rdma_ctx.send_mr = ibv_reg_mr(g_repl_rdma_ctx.pd, g_repl_rdma_ctx.send_buf, cap, IBV_ACCESS_LOCAL_WRITE);
    if (!g_repl_rdma_ctx.send_mr) {
        repl_rdma_log("prepare_buffers", "legacy send mr register failed");
        return -1;
    }
    for (i = 0; i < g_repl_rdma_ctx.active_recv_slots; ++i) {
        g_repl_rdma_ctx.recv_slots[i].buf = (unsigned char *)kvs_malloc(cap);
        if (!g_repl_rdma_ctx.recv_slots[i].buf) {
            repl_rdma_log("prepare_buffers", "recv buffer alloc failed");
            return -1;
        }
        g_repl_rdma_ctx.recv_slots[i].cap = cap;
        g_repl_rdma_ctx.recv_slots[i].posted = 0;
        memset(g_repl_rdma_ctx.recv_slots[i].buf, 0, cap);
        g_repl_rdma_ctx.recv_slots[i].mr = ibv_reg_mr(g_repl_rdma_ctx.pd, g_repl_rdma_ctx.recv_slots[i].buf, cap, IBV_ACCESS_LOCAL_WRITE);
        if (!g_repl_rdma_ctx.recv_slots[i].mr) {
            repl_rdma_log("prepare_buffers", "recv mr register failed");
            return -1;
        }
    }
    return 0;
}

static int repl_rdma_post_recv_slot(int slot) {
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;
    if (slot < 0 || slot >= g_repl_rdma_ctx.active_recv_slots) return -1;
    if (!g_repl_rdma_ctx.id || !g_repl_rdma_ctx.id->qp || !g_repl_rdma_ctx.recv_slots[slot].mr || !g_repl_rdma_ctx.recv_slots[slot].buf) return -1;
    memset(g_repl_rdma_ctx.recv_slots[slot].buf, 0, g_repl_rdma_ctx.recv_slots[slot].cap);
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)g_repl_rdma_ctx.recv_slots[slot].buf;
    sge.length = (uint32_t)g_repl_rdma_ctx.recv_slots[slot].cap;
    sge.lkey = g_repl_rdma_ctx.recv_slots[slot].mr->lkey;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)(slot + 1);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    if (ibv_post_recv(g_repl_rdma_ctx.id->qp, &wr, &bad_wr) != 0) {
        repl_rdma_log("post_recv", "ibv_post_recv failed");
        return -1;
    }
    g_repl_rdma_ctx.recv_slots[slot].posted = 1;
    return 0;
}

static int repl_rdma_post_initial_recv(void) {
    int slot;
    for (slot = 0; slot < g_repl_rdma_ctx.active_recv_slots; ++slot) {
        if (repl_rdma_post_recv_slot(slot) != 0) return -1;
    }
    return 0;
}

static int repl_rdma_repost_recv(int slot) {
    return repl_rdma_post_recv_slot(slot);
}

/* P3.5: 获取一个已注册的发送 slot buffer 指针，供 pread() 直接写入。
 * 返回 0 成功，-1 无空闲 slot。调用者写入后必须调用 repl_rdma_commit_write_slot()。 */
int repl_rdma_get_write_slot(unsigned char **ptr, size_t *cap) {
    int slot;
    pthread_mutex_lock(&g_repl_rdma_send_lock);
    if (repl_rdma_drain_cm_events_nonblock() != 0 ||
        !g_repl_rdma_ctx.connected || !g_repl_rdma_ctx.id || !g_repl_rdma_ctx.id->qp ||
        !g_repl_rdma_ctx.send_pipeline_enabled) {
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    slot = repl_rdma_acquire_send_slot(5000);
    if (slot < 0) {
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    g_repl_rdma_ctx.send_slots[slot].in_flight = 1;
    g_repl_rdma_ctx.send_slots[slot].wr_id = (uint64_t)slot | KVS_RDMA_PIPELINE_WR_ID_FLAG;
    g_repl_rdma_ctx.send_slots_in_flight++;
    *ptr = g_repl_rdma_ctx.send_slots[slot].buf;
    *cap = g_repl_rdma_ctx.send_slots[slot].cap;
    /* 返回 slot 索引（编码在 cap 高位，hack for batch flush） */
    g_repl_rdma_ctx.pending_batch_slots[g_repl_rdma_ctx.pending_batch_count] = slot;
    /* caller must set pending_batch_lens */
    pthread_mutex_unlock(&g_repl_rdma_send_lock);
    return 0;
}

/* P3.5: 提交已写入的 send slot。调用者通过 get_write_slot 获取 slot 后直接写入，
 * 然后调用本函数提交，由 batch flush 机制批量 post。
 * 必须在同一线程中调用，不重新获取锁（get_write_slot 已释放）。 */
int repl_rdma_commit_write_slot(size_t len) {
    pthread_mutex_lock(&g_repl_rdma_send_lock);
    if (!g_repl_rdma_ctx.connected || g_repl_rdma_ctx.pending_batch_count <= 0) {
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    int bi = g_repl_rdma_ctx.pending_batch_count - 1;
    g_repl_rdma_ctx.pending_batch_lens[bi] = len;
    g_repl_rdma_ctx.pending_batch_count = bi + 1;
    /* batch 满时 flush */
    if (g_repl_rdma_ctx.pending_batch_count >= KVS_RDMA_BATCH_MAX) {
        if (repl_rdma_flush_batch_locked() != 0) {
            pthread_mutex_unlock(&g_repl_rdma_send_lock);
            return -1;
        }
    }
    pthread_mutex_unlock(&g_repl_rdma_send_lock);
    return 0;
}

/* P3.4: 零复制 — 返回注册 MR buffer 的直接指针，不 malloc+memcpy。
 * 调用者使用完毕后必须调用 repl_rdma_repost_recv(slot) 归还。 */
static unsigned char *repl_rdma_recv_direct(int slot, size_t *len) {
    if (slot < 0 || slot >= g_repl_rdma_ctx.active_recv_slots) return NULL;
    if (!g_repl_rdma_ctx.recv_slots[slot].buf) return NULL;
    if (*len > g_repl_rdma_ctx.recv_slots[slot].cap)
        *len = g_repl_rdma_ctx.recv_slots[slot].cap;
    return g_repl_rdma_ctx.recv_slots[slot].buf;
}

static int repl_rdma_wait_cq_send_completion(int timeout_ms) {
    struct ibv_wc wc;
    long long deadline = kvs_now_ms() + timeout_ms;
    if (!g_repl_rdma_ctx.cq) return -1;
    for (;;) {
        int n;
        pthread_mutex_lock(&g_repl_rdma_cq_lock);
        n = ibv_poll_cq(g_repl_rdma_ctx.cq, 1, &wc);
        if (n > 0) {
            if (wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_SEND) {
                /* Pipeline send completion */
                if (wc.wr_id & KVS_RDMA_PIPELINE_WR_ID_FLAG) {
                    int slot = (int)(wc.wr_id & ~KVS_RDMA_PIPELINE_WR_ID_FLAG);
                    if (slot >= 0 && slot < g_repl_rdma_ctx.send_pipeline_depth) {
                        g_repl_rdma_ctx.send_slots[slot].in_flight = 0;
                        g_repl_rdma_ctx.send_slots[slot].wr_id = 0;
                        g_repl_rdma_ctx.send_slots_in_flight--;
                    }
                }
                pthread_mutex_unlock(&g_repl_rdma_cq_lock);
                return 0;
            }
            if (wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_RECV) {
                int slot = (wc.wr_id > 0 && wc.wr_id <= (uint64_t)g_repl_rdma_ctx.active_recv_slots) ? (int)(wc.wr_id - 1) : -1;
                if (slot >= 0 && slot < g_repl_rdma_ctx.active_recv_slots) g_repl_rdma_ctx.recv_slots[slot].posted = 0;
                if (slot >= 0 && repl_rdma_pending_recv_push(slot, (size_t)wc.byte_len) != 0) {
                    pthread_mutex_unlock(&g_repl_rdma_cq_lock);
                    repl_rdma_log("send_cq", "pending recv queue overflow");
                    g_repl_rdma_ctx.connected = 0;
                    return -1;
                }
                pthread_mutex_unlock(&g_repl_rdma_cq_lock);
                continue;
            }
            pthread_mutex_unlock(&g_repl_rdma_cq_lock);
            fprintf(stderr, "repl rdma: send_cq failed - status=%d opcode=%d\n", wc.status, wc.opcode);
            g_rdma_send_cq_error_count++;
            g_repl_rdma_ctx.connected = 0;
            return -1;
        }
        pthread_mutex_unlock(&g_repl_rdma_cq_lock);
        if (repl_rdma_drain_cm_events_nonblock() != 0 || !g_repl_rdma_ctx.connected) {
            repl_rdma_log("send_cq", "transport already disconnected");
            return -1;
        }
        if (kvs_now_ms() >= deadline) {
            repl_rdma_drain_cm_events_nonblock();
            g_repl_rdma_ctx.connected = 0;
            repl_rdma_log("send_cq", "poll timeout or error");
            return -1;
        }
        usleep(1000);
    }
}

static int repl_rdma_wait_cq_recv_completion(int timeout_ms, int *slot_out, size_t *recv_len) {
    struct ibv_wc wc;
    long long deadline = kvs_now_ms() + timeout_ms;
    if (slot_out) *slot_out = -1;
    if (recv_len) *recv_len = 0;

    /* 优先从 pending_recv 队列取（CQ 轮询线程异步填充） */
    for (;;) {
        pthread_mutex_lock(&g_repl_rdma_cq_lock);
        if (repl_rdma_pending_recv_pop(slot_out, recv_len) == 0) {
            pthread_mutex_unlock(&g_repl_rdma_cq_lock);
            return (slot_out && *slot_out >= 0) ? 0 : -1;
        }
        pthread_mutex_unlock(&g_repl_rdma_cq_lock);
        /* pending 队列为空 */
        break;
    }

    /* CQ 轮询线程运行时，不直接 poll CQ（避免竞争），只等 pending 队列 */
    if (g_repl_rdma_ctx.cq_poll_thread_running) {
        /* 忙等待 pending 队列直到超时 */
        while (kvs_now_ms() < deadline) {
            usleep(1000);
            pthread_mutex_lock(&g_repl_rdma_cq_lock);
            if (repl_rdma_pending_recv_pop(slot_out, recv_len) == 0) {
                pthread_mutex_unlock(&g_repl_rdma_cq_lock);
                return (slot_out && *slot_out >= 0) ? 0 : -1;
            }
            pthread_mutex_unlock(&g_repl_rdma_cq_lock);
            if (repl_rdma_drain_cm_events_nonblock() != 0 || !g_repl_rdma_ctx.connected) {
                repl_rdma_log("recv_cq", "transport already disconnected");
                return -1;
            }
        }
        return -1;
    }

    /* ---- CQ 轮询线程未运行：直接 poll CQ（向后兼容） ---- */
    if (!g_repl_rdma_ctx.cq) return -1;
    for (;;) {
        int n;
        pthread_mutex_lock(&g_repl_rdma_cq_lock);
        n = ibv_poll_cq(g_repl_rdma_ctx.cq, 1, &wc);
        if (n > 0) {
            if (wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_RECV) {
                int slot = (wc.wr_id > 0 && wc.wr_id <= (uint64_t)g_repl_rdma_ctx.active_recv_slots) ? (int)(wc.wr_id - 1) : -1;
                if (slot >= 0 && slot < g_repl_rdma_ctx.active_recv_slots) g_repl_rdma_ctx.recv_slots[slot].posted = 0;
                if (slot_out) *slot_out = slot;
                if (recv_len) *recv_len = (size_t)wc.byte_len;
                pthread_mutex_unlock(&g_repl_rdma_cq_lock);
                return (slot >= 0) ? 0 : -1;
            }
            if (wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_SEND) {
                /* Pipeline send completion — release slot */
                if (wc.wr_id & KVS_RDMA_PIPELINE_WR_ID_FLAG) {
                    int slot = (int)(wc.wr_id & ~KVS_RDMA_PIPELINE_WR_ID_FLAG);
                    if (slot >= 0 && slot < g_repl_rdma_ctx.send_pipeline_depth) {
                        g_repl_rdma_ctx.send_slots[slot].in_flight = 0;
                        g_repl_rdma_ctx.send_slots[slot].wr_id = 0;
                        g_repl_rdma_ctx.send_slots_in_flight--;
                    }
                }
                pthread_mutex_unlock(&g_repl_rdma_cq_lock);
                continue;
            }
            pthread_mutex_unlock(&g_repl_rdma_cq_lock);
            fprintf(stderr, "repl rdma: recv_cq failed - status=%d opcode=%d\n", wc.status, wc.opcode);
            g_rdma_recv_cq_error_count++;
            if (repl_rdma_drain_cm_events_nonblock() != 0 || !g_repl_rdma_ctx.connected) {
                repl_rdma_log("recv_cq", "transport already disconnected after recv failure");
            }
            return -1;
        }
        pthread_mutex_unlock(&g_repl_rdma_cq_lock);
        if (repl_rdma_drain_cm_events_nonblock() != 0 || !g_repl_rdma_ctx.connected) {
            repl_rdma_log("recv_cq", "transport already disconnected while waiting for recv");
            return -1;
        }
        if (kvs_now_ms() >= deadline) {
            repl_rdma_log("recv_cq", "poll timeout or error");
            return -1;
        }
        usleep(1000);
    }
}

/* P3.1b: 批量 flush — 将积累的 WR 链式提交，一次 ibv_post_send。
 * 调用者必须持有 g_repl_rdma_send_lock。失败时回退所有待发送 slot。 */
static int repl_rdma_flush_batch_locked(void) {
    int count = g_repl_rdma_ctx.pending_batch_count;
    if (count == 0) return 0;
    struct ibv_sge sge[KVS_RDMA_BATCH_MAX];
    struct ibv_send_wr wr[KVS_RDMA_BATCH_MAX];
    struct ibv_send_wr *bad_wr = NULL;

    /* P3.2: 注册 batch tracker — 用于选择性 signal 时批量回收 unsignaled slot */
    int tracker_idx = -1;
    {
        int next = g_repl_rdma_ctx.batch_tracker_next;
        for (int t = 0; t < KVS_RDMA_MAX_BATCH_TRACKERS; t++) {
            int idx = (next + t) % KVS_RDMA_MAX_BATCH_TRACKERS;
            if (!g_repl_rdma_ctx.batch_trackers[idx].active) {
                tracker_idx = idx;
                g_repl_rdma_ctx.batch_tracker_next = (idx + 1) % KVS_RDMA_MAX_BATCH_TRACKERS;
                break;
            }
        }
    }
    /* 若 tracker 耗尽（不应发生，512 slots 足够），退化为全 signal */
    int signal_every = (tracker_idx >= 0) ? KVS_RDMA_SIGNAL_INTERVAL : 1;
    int signaled_slot = -1;

    for (int i = 0; i < count; i++) {
        size_t chunk_len = g_repl_rdma_ctx.pending_batch_lens[i];
        int slot = g_repl_rdma_ctx.pending_batch_slots[i];
        memset(&sge[i], 0, sizeof(sge[i]));
        sge[i].addr = (uintptr_t)g_repl_rdma_ctx.send_slots[slot].buf;
        sge[i].length = (uint32_t)g_repl_rdma_ctx.pending_batch_lens[i];
        sge[i].lkey = g_repl_rdma_ctx.send_slots[slot].mr->lkey;
        memset(&wr[i], 0, sizeof(wr[i]));
        wr[i].wr_id = (uint64_t)slot | KVS_RDMA_PIPELINE_WR_ID_FLAG;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        if (g_repl_rdma_ctx.use_write_mode) {
            /* 直接单边 WRITE：去掉 SEND 预热首包（实测跨机 rxe 下全 WRITE 无需预热，
             * 且首包 SEND completion 反而报 status=2 opcode=1 错误）。
             * 末包用普通 WRITE（不用 WRITE_WITH_IMM）——跨机 rxe 下 IMM 投递
             * 报 REMOTE_ACCESS_ERR 且数据不落盘（实测），完成信号走 TCP FULLSYNCEND。 */
            wr[i].opcode = IBV_WR_RDMA_WRITE;
            wr[i].wr.rdma.remote_addr = g_repl_rdma_ctx.remote_write_addr
                                        + g_repl_rdma_ctx.write_total_sent;
            wr[i].wr.rdma.rkey = g_repl_rdma_ctx.remote_write_rkey;
            g_repl_rdma_ctx.write_total_sent += chunk_len;
        } else {
            /* SEND 模式（非 WRITE 传输，未收到 FULLRESYNCWR 握手） */
            wr[i].opcode = IBV_WR_SEND;
        }
        /* P3.2: 仅每 signal_every 个 WR 中最后一个 signaled */
        int do_signal = ((i + 1) % signal_every == 0) || (i == count - 1);
        wr[i].send_flags = do_signal ? IBV_SEND_SIGNALED : 0;
        if (do_signal) signaled_slot = slot;
        wr[i].next = (i < count - 1) ? &wr[i + 1] : NULL;
        /* 填充 tracker */
        if (tracker_idx >= 0 && i < KVS_RDMA_BATCH_MAX) {
            g_repl_rdma_ctx.batch_trackers[tracker_idx].slots[i] = slot;
        }
    }
    /* 注册 tracker */
    if (tracker_idx >= 0) {
        g_repl_rdma_ctx.batch_trackers[tracker_idx].count = count;
        g_repl_rdma_ctx.batch_trackers[tracker_idx].signaled_slot = signaled_slot;
        g_repl_rdma_ctx.batch_trackers[tracker_idx].active = 1;
    }

    if (ibv_post_send(g_repl_rdma_ctx.id->qp, &wr[0], &bad_wr) != 0) {
        int save_errno = errno;
        fprintf(stderr, "repl rdma: flush_batch - ibv_post_send failed: errno=%d(%s) opcode=%d use_write=%d\n",
            save_errno, strerror(save_errno), wr[0].opcode, g_repl_rdma_ctx.use_write_mode);
        /* EAGAIN: SQ 满, drain CQ 后重试一次。
         * 注意：cq_process_wc 可能 release_send_slot（内部持锁 signal），而本函数
         * 调用时已持有 g_repl_rdma_send_lock —— 必须先解锁再 drain，否则死锁。 */
        if (save_errno == EAGAIN || save_errno == EWOULDBLOCK) {
            struct ibv_wc wc[KVS_RDMA_CQ_BATCH];
            pthread_mutex_unlock(&g_repl_rdma_send_lock);
            int n = ibv_poll_cq(g_repl_rdma_ctx.cq, KVS_RDMA_CQ_BATCH, wc);
            for (int k = 0; k < n; k++) repl_rdma_cq_process_wc(&wc[k]);
            pthread_mutex_lock(&g_repl_rdma_send_lock);
            if (ibv_post_send(g_repl_rdma_ctx.id->qp, &wr[0], &bad_wr) == 0) {
                g_repl_rdma_ctx.pending_batch_count = 0;
                return 0;  /* 重试成功 */
            }
            save_errno = errno;
        }
        /* 回退 tracker */
        if (tracker_idx >= 0) g_repl_rdma_ctx.batch_trackers[tracker_idx].active = 0;
        /* 回退所有 slot 预占 */
        for (int i = 0; i < count; i++) {
            int slot = g_repl_rdma_ctx.pending_batch_slots[i];
            g_repl_rdma_ctx.send_slots[slot].in_flight = 0;
            g_repl_rdma_ctx.send_slots[slot].wr_id = 0;
            g_repl_rdma_ctx.send_slots_in_flight--;
        }
        g_repl_rdma_ctx.connected = 0;
        g_repl_rdma_ctx.pending_batch_count = 0;
        return -1;
    }
    g_repl_rdma_ctx.pending_batch_count = 0;
    return 0;
}

/* P3.1b: 供外部调用的 flush（在发送循环结束后 flush 尾部 batch） */
int repl_rdma_flush_batch(void) {
    int rc;
    pthread_mutex_lock(&g_repl_rdma_send_lock);
    rc = repl_rdma_flush_batch_locked();
    pthread_mutex_unlock(&g_repl_rdma_send_lock);
    return rc;
}

static int repl_rdma_try_send(const unsigned char *buf, size_t len) {
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    int slot;
    if (!buf || len == 0) return 0;
    pthread_mutex_lock(&g_repl_rdma_send_lock);
    if (repl_rdma_drain_cm_events_nonblock() != 0) {
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    if (!g_repl_rdma_ctx.connected || !g_repl_rdma_ctx.id || !g_repl_rdma_ctx.id->qp) {
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    if (g_repl_rdma_ctx.send_pipeline_enabled) {
        /* ---- Pipeline 模式：非阻塞发送 ---- */
        if (len > g_repl_rdma_ctx.send_slots[0].cap) {
            repl_rdma_log("try_send", "pipeline payload too large");
            pthread_mutex_unlock(&g_repl_rdma_send_lock);
            return -1;
        }
        /* 获取空闲 send slot（等待不超过 5s） */
        slot = repl_rdma_acquire_send_slot(5000);
        if (slot < 0) {
            repl_rdma_log("try_send", "no available send slot");
            pthread_mutex_unlock(&g_repl_rdma_send_lock);
            return -1;
        }
        /* 拷贝数据到 slot buffer，积累到 batch，满时批量 post */
        memcpy(g_repl_rdma_ctx.send_slots[slot].buf, buf, len);
        /* P2.1: post 前预占 slot */
        g_repl_rdma_ctx.send_slots[slot].in_flight = 1;
        g_repl_rdma_ctx.send_slots[slot].wr_id = (uint64_t)slot | KVS_RDMA_PIPELINE_WR_ID_FLAG;
        g_repl_rdma_ctx.send_slots_in_flight++;
        /* 加入 batch */
        int bi = g_repl_rdma_ctx.pending_batch_count;
        g_repl_rdma_ctx.pending_batch_slots[bi] = slot;
        g_repl_rdma_ctx.pending_batch_lens[bi] = len;
        g_repl_rdma_ctx.pending_batch_count++;
        /* batch 满或单次发送时立即 flush */
        if (g_repl_rdma_ctx.pending_batch_count >= KVS_RDMA_BATCH_MAX) {
            if (repl_rdma_flush_batch_locked() != 0) {
                pthread_mutex_unlock(&g_repl_rdma_send_lock);
                return -1;
            }
        }
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return 0;
    }
    /* ---- 兼容旧模式：同步发送 ---- */
    if (!g_repl_rdma_ctx.send_buf || !g_repl_rdma_ctx.send_mr) {
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    if (len > g_repl_rdma_ctx.send_buf_cap) {
        repl_rdma_log("try_send", "payload too large");
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    memcpy(g_repl_rdma_ctx.send_buf, buf, len);
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)g_repl_rdma_ctx.send_buf;
    sge.length = (uint32_t)len;
    sge.lkey = g_repl_rdma_ctx.send_mr->lkey;
    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    if (ibv_post_send(g_repl_rdma_ctx.id->qp, &wr, &bad_wr) != 0) {
        repl_rdma_log("try_send", "ibv_post_send failed");
        g_repl_rdma_ctx.connected = 0;
        pthread_mutex_unlock(&g_repl_rdma_send_lock);
        return -1;
    }
    /* 旧模式仍同步等待 completion */
    int rc = repl_rdma_wait_cq_send_completion(5000);
    pthread_mutex_unlock(&g_repl_rdma_send_lock);
    return rc;
}
#endif

/* ---- 自适应 Pipeline 深度调节 ----
 * P2.2: 已禁用。缩小 depth 时高位 slot 的 completion 会被跳过（slot >= depth
 * 检查拒绝释放），导致 in_flight 泄漏和 slot 永久占用。
 * 后续若需动态调节，应区分 slot_capacity 和 active_window:
 *   - slot_capacity 不变，始终按 KVS_RDMA_SEND_SLOTS_MAX 回收所有 completion
 *   - window 只限制 acquire 时不越过当前窗口
 */
#if 0  /* P2.2: 已禁用 */
#define KVS_RDMA_SEND_SLOTS_MAX_MIN 2

static void repl_rdma_adjust_pipeline_depth(void) {
    int in_flight = g_repl_rdma_ctx.send_slots_in_flight;
    int depth = g_repl_rdma_ctx.send_pipeline_depth;

    if (in_flight <= depth / 2 && depth < KVS_RDMA_SEND_SLOTS_MAX) {
        g_repl_rdma_ctx.send_pipeline_depth = depth + 1;
#if KVS_REPL_DEBUG
        fprintf(stderr, "repl rdma: pipeline depth %d -> %d (in_flight=%d)\n",
            depth, depth + 1, in_flight);
#endif
    } else if (in_flight >= depth && depth > KVS_RDMA_SEND_SLOTS_MAX_MIN) {
        g_repl_rdma_ctx.send_pipeline_depth = depth - 1;
#if KVS_REPL_DEBUG
        fprintf(stderr, "repl rdma: pipeline depth %d -> %d (in_flight=%d)\n",
            depth, depth - 1, in_flight);
#endif
    }
}
#endif /* P2.2 */

/* ---- CQ completion 处理函数（CQ 轮询线程和 fallback 路径共用）---- */
static void repl_rdma_cq_process_wc(struct ibv_wc *wc) {
    if (wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr, "repl rdma: cq_poll error status=%d opcode=%d wr_id=0x%lx\n",
            wc->status, wc->opcode, (unsigned long)wc->wr_id);
        if (wc->opcode == IBV_WC_SEND || wc->opcode == IBV_WC_RDMA_WRITE) g_rdma_send_cq_error_count++;
        else g_rdma_recv_cq_error_count++;
        g_repl_rdma_ctx.connected = 0;
        return;
    }
    if (wc->opcode == IBV_WC_SEND || wc->opcode == IBV_WC_RDMA_WRITE) {
        if (wc->wr_id & KVS_RDMA_PIPELINE_WR_ID_FLAG) {
            int signaled_slot = (int)(wc->wr_id & ~KVS_RDMA_PIPELINE_WR_ID_FLAG);
            /* P3.2: 查找匹配的 batch tracker，批量回收 unsignaled slot */
            int batch_found = 0;
            for (int t = 0; t < KVS_RDMA_MAX_BATCH_TRACKERS; t++) {
                rdma_batch_tracker_t *tr = &g_repl_rdma_ctx.batch_trackers[t];
                if (tr->active && tr->signaled_slot == signaled_slot) {
                    for (int s = 0; s < tr->count; s++) {
                        repl_rdma_release_send_slot(tr->slots[s]);
                    }
                    tr->active = 0;
                    batch_found = 1;
                    break;
                }
            }
            if (!batch_found) {
                /* 未找到 tracker（旧式全 signal 模式或错误恢复）*/
                repl_rdma_release_send_slot(signaled_slot);
            }
        }
    } else if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
        repl_rdma_log("cq_process", "WRITE_WITH_IMM received");
        g_slave_fullsync_target.imm_received = 1;
    } else if (wc->opcode == IBV_WC_RECV) {
        int slot = (wc->wr_id > 0 && wc->wr_id <= (uint64_t)g_repl_rdma_ctx.active_recv_slots)
            ? (int)(wc->wr_id - 1) : -1;
        if (slot >= 0 && slot < g_repl_rdma_ctx.active_recv_slots) {
            g_repl_rdma_ctx.recv_slots[slot].posted = 0;
            repl_rdma_pending_recv_push(slot, (size_t)wc->byte_len);
        }
    }
}

/* ---- CQ 轮询线程 ----
 * 后台独立 poll CQ，将完成事件分发到对应队列。
 * 在 pipeline 模式下，发送/接收 completion 均由此线程异步处理。
 */
static void *repl_rdma_cq_poll_thread(void *arg) {
    (void)arg;
    struct ibv_cq *cq = g_repl_rdma_ctx.cq;
    struct ibv_wc wc_batch[KVS_RDMA_CQ_BATCH];
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    /* P2.2: adapt_counter 已移除（暂停动态 pipeline 调节） */

    repl_rdma_log("cq_poll", "thread started");
    while (g_repl_rdma_ctx.cq_poll_thread_running && g_repl_rdma_ctx.connected) {
        /* completion channel 事件驱动，但用 poll+超时 + 直接 drain 兜底：
         * rxe 下事件可能丢失（实测 chunk1 事件到、后续 IMM 事件丢），
         * 超时未收到事件时也直接 poll CQ，避免 completion 卡住。 */
        if (!g_repl_rdma_ctx.comp_chan) break;
        int have_event = 0;
        {
            struct pollfd pfd;
            memset(&pfd, 0, sizeof(pfd));
            pfd.fd = g_repl_rdma_ctx.comp_chan->fd;
            pfd.events = POLLIN;
            int pr = poll(&pfd, 1, 5);   /* 5ms 超时 */
            if (pr > 0) {
                if (ibv_get_cq_event(g_repl_rdma_ctx.comp_chan, &ev_cq, &ev_ctx) != 0) {
                    if (errno == EINTR) continue;
                    repl_rdma_log("cq_poll", "ibv_get_cq_event failed");
                    usleep(10000);
                    continue;
                }
                ibv_ack_cq_events(cq, 1);
                have_event = 1;
            } else if (pr < 0) {
                if (errno == EINTR) continue;
                usleep(1000);
                continue;
            }
            /* pr == 0（超时）→ 无事件，仍直接 drain 兜底 */
        }
        (void)have_event;

        /* 批量 poll completions */
        for (;;) {
            int n = ibv_poll_cq(cq, KVS_RDMA_CQ_BATCH, wc_batch);
            if (n <= 0) break;
            for (int i = 0; i < n; i++) {
                repl_rdma_cq_process_wc(&wc_batch[i]);
                if (!g_repl_rdma_ctx.connected) break;
            }
            if (!g_repl_rdma_ctx.connected) break;
        }

        /* 标准 RDMA 编程模式: re-arm → poll(再确认) → wait.
         *
         * 必须先 re-arm 再 poll，因为:
         * 如果先 poll(全空) 再 re-arm，中间有 completion 到达时
         * 该 completion 不会触发 event，导致永久阻塞。
         */
        if (g_repl_rdma_ctx.cq && g_repl_rdma_ctx.comp_chan && g_repl_rdma_ctx.connected) {
            ibv_req_notify_cq(cq, 0);
            /*  Drain: poll 一次确认 poll→re-arm 之间没有漏掉 completion */
            int n = ibv_poll_cq(cq, KVS_RDMA_CQ_BATCH, wc_batch);
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    repl_rdma_cq_process_wc(&wc_batch[i]);
                    if (!g_repl_rdma_ctx.connected) break;
                }
                /* 有数据，不阻塞等待，立即继续 re-arm→drain 循环 */
                continue;
            }
        }
        /* 无更多 completion → 继续循环（poll 等待事件或超时兜底 drain） */
    }
    /* Drain remaining events on exit */
    if (g_repl_rdma_ctx.comp_chan) {
        struct ibv_cq *drain_cq;
        void *drain_ctx;
        while (ibv_get_cq_event(g_repl_rdma_ctx.comp_chan, &drain_cq, &drain_ctx) == 0) {
            ibv_ack_cq_events(cq, 1);
        }
    }
    g_cq_poll_thread_exited = 1;
    repl_rdma_log("cq_poll", "thread exiting");
    return NULL;
}

static int repl_rdma_start_cq_poll_thread(void) {
    if (g_repl_rdma_ctx.cq_poll_thread_running) return 0;
    if (!g_repl_rdma_ctx.comp_chan || !g_repl_rdma_ctx.cq) return -1;
    /* 先 arm CQ notification */
    if (ibv_req_notify_cq(g_repl_rdma_ctx.cq, 0) != 0) {
        repl_rdma_log("cq_poll", "ibv_req_notify_cq failed");
        return -1;
    }
    g_repl_rdma_ctx.cq_poll_thread_running = 1;
    if (pthread_create(&g_repl_rdma_ctx.cq_poll_thread, NULL,
                       repl_rdma_cq_poll_thread, NULL) != 0) {
        g_repl_rdma_ctx.cq_poll_thread_running = 0;
        repl_rdma_log("cq_poll", "pthread_create failed");
        return -1;
    }
    pthread_detach(g_repl_rdma_ctx.cq_poll_thread);
    repl_rdma_log("cq_poll", "thread started");
    return 0;
}


static void repl_rdma_stop_cq_poll_thread(void) {
    if (!g_repl_rdma_ctx.cq_poll_thread_running) return;
    g_repl_rdma_ctx.cq_poll_thread_running = 0;
    g_cq_poll_thread_exited = 0;
    /* 断开 completion channel 以唤醒线程 */
    if (g_repl_rdma_ctx.comp_chan) {
        ibv_destroy_comp_channel(g_repl_rdma_ctx.comp_chan);
        g_repl_rdma_ctx.comp_chan = NULL;
    }
    /* 等待线程退出（最多 3 秒）*/
    for (int i = 0; i < 300 && !g_cq_poll_thread_exited; i++) {
        usleep(10000);
    }
    repl_rdma_log("cq_poll", "thread stop signalled");
}

static void repl_transport_tcp_disconnect_slave(int fd) {
    if (fd >= 0) close(fd);
}

static int repl_transport_ebpf_connect_slave(const char *host, int port) {
    int fd = repl_transport_tcp_connect_slave(host, port);
    if (fd < 0) return -1;
    if (repl_ebpf_register_fd(fd, 0) != 0) {
        fprintf(stderr, "repl ebpf: fd registration failed on slave link, using tcp-compatible path\n");
    }
    return fd;
}

static void repl_transport_ebpf_disconnect_slave(int fd) {
    repl_ebpf_unregister_fd(fd);
    repl_transport_tcp_disconnect_slave(fd);
}

static int repl_transport_rdma_send(conn_t *c, const unsigned char *buf, size_t len) {
    transport_log("RDMA send %zu bytes", len);
    (void)c;
    (void)buf;
    (void)len;
#if KVS_ENABLE_RDMA
    if (g_repl_rdma_ctx.connected) {
        return repl_rdma_try_send(buf, len);
    }
#endif
    return -1;
}

static const repl_transport_ops_t *repl_transport_ops(void);
static const repl_transport_ops_t *repl_transport_ops_for_conn(conn_t *c);
static int repl_should_use_rdma_now(void);
static void repl_transport_mark_active(const char *name);
static void repl_transport_trigger_fallback(const char *reason, int cooldown_ms);
static int repl_realtime_should_use_ebpf(void);

static int repl_transport_rdma_connect_slave(const char *host, int port) {
    (void)host;
    (void)port;
#if KVS_ENABLE_RDMA
    struct sockaddr_in dst;
    int rdma_port = g_cfg.rdma_port > 0 ? g_cfg.rdma_port : port + 1;
    repl_rdma_log("connect_slave", "begin");
    if (repl_rdma_prepare_addr(host, rdma_port, &dst) != 0) {
        repl_rdma_log("prepare_addr", "failed");
        repl_transport_trigger_fallback("rdma_prepare_addr_failed", 5000);
        return -1;
    }
    repl_rdma_log("prepare_addr", "ok");
    repl_rdma_reset_ctx();
    /* Retry event channel creation: transient failures can occur under load */
    for (int ec_retry = 0; ec_retry < 3; ec_retry++) {
        g_repl_rdma_ctx.ec = rdma_create_event_channel();
        if (g_repl_rdma_ctx.ec) break;
        char ec_err[128];
        snprintf(ec_err, sizeof(ec_err), "create failed errno=%d retry=%d", errno, ec_retry);
        repl_rdma_log("event_channel", ec_err);
        if (ec_retry < 2) usleep(200000);
    }
    if (!g_repl_rdma_ctx.ec) {
        repl_rdma_log("event_channel", "create failed after retries");
        return -1;
    }
    repl_rdma_log("event_channel", "created");
    if (rdma_create_id(g_repl_rdma_ctx.ec, &g_repl_rdma_ctx.id, NULL, RDMA_PS_TCP) != 0) {
        repl_rdma_log("create_id", "failed");
        repl_rdma_reset_ctx();
        return -1;
    }
    repl_rdma_log("create_id", "ok");
    if (rdma_resolve_addr(g_repl_rdma_ctx.id, NULL, (struct sockaddr *)&dst, 1000) != 0) {
        repl_rdma_log("resolve_addr", "rdma_resolve_addr failed");
        repl_transport_trigger_fallback("rdma_resolve_addr_failed", 5000);
        repl_rdma_reset_ctx();
        return -1;
    }
    repl_rdma_log("resolve_addr", "issued");
    if (repl_rdma_wait_event(RDMA_CM_EVENT_ADDR_RESOLVED, 1500) != 0) {
        repl_rdma_reset_ctx();
        return -1;
    }
    g_repl_rdma_ctx.addr_resolved = 1;
    repl_rdma_log("resolve_addr", "resolved");
    if (rdma_resolve_route(g_repl_rdma_ctx.id, 1000) != 0) {
        repl_rdma_log("resolve_route", "rdma_resolve_route failed");
        repl_transport_trigger_fallback("rdma_resolve_route_failed", 5000);
        repl_rdma_reset_ctx();
        return -1;
    }
    repl_rdma_log("resolve_route", "issued");
    if (repl_rdma_wait_event(RDMA_CM_EVENT_ROUTE_RESOLVED, 1500) != 0) {
        repl_rdma_reset_ctx();
        return -1;
    }
    g_repl_rdma_ctx.route_resolved = 1;
    repl_rdma_log("resolve_route", "resolved");
    if (!g_repl_rdma_ctx.comp_chan && g_repl_rdma_ctx.id && g_repl_rdma_ctx.id->verbs) {
        g_repl_rdma_ctx.comp_chan = ibv_create_comp_channel(g_repl_rdma_ctx.id->verbs);
        if (!g_repl_rdma_ctx.comp_chan) {
            repl_rdma_log("comp_channel", "create failed");
            repl_rdma_reset_ctx();
            return -1;
        }
        repl_rdma_log("comp_channel", "created");
    }
    if (!g_repl_rdma_ctx.pd && g_repl_rdma_ctx.id && g_repl_rdma_ctx.id->verbs) {
        g_repl_rdma_ctx.pd = ibv_alloc_pd(g_repl_rdma_ctx.id->verbs);
        if (!g_repl_rdma_ctx.pd) {
            repl_rdma_log("alloc_pd", "failed");
            repl_rdma_reset_ctx();
            return -1;
        }
        repl_rdma_log("alloc_pd", "ok");
    }
    if (!g_repl_rdma_ctx.cq && g_repl_rdma_ctx.id && g_repl_rdma_ctx.id->verbs) {
        repl_rdma_refresh_runtime_cfg();
        g_repl_rdma_ctx.cq = ibv_create_cq(g_repl_rdma_ctx.id->verbs, g_repl_rdma_ctx.active_qp_wr_depth, NULL, g_repl_rdma_ctx.comp_chan, 0);
        if (!g_repl_rdma_ctx.cq) {
            repl_rdma_log("create_cq", "failed");
            repl_rdma_reset_ctx();
            return -1;
        }
        repl_rdma_log("create_cq", "ok");
    }
    if (repl_rdma_create_qp() != 0) {
        repl_rdma_reset_ctx();
        return -1;
    }
    if (repl_rdma_prepare_buffers() != 0) {
        repl_rdma_reset_ctx();
        return -1;
    }
    if (repl_rdma_post_initial_recv() != 0) {
        repl_rdma_reset_ctx();
        return -1;
    }
    if (repl_rdma_connect_handshake() != 0) {
        repl_transport_trigger_fallback("rdma_connect_handshake_failed", 5000);
        repl_rdma_reset_ctx();
        return -1;
    }
    /* 启动 CQ 轮询线程（pipeline 模式：异步处理 send/recv completion） */
    if (g_repl_rdma_ctx.send_pipeline_enabled) {
        repl_rdma_start_cq_poll_thread();
    }
    repl_transport_mark_active("rdma");
    repl_rdma_log("connect_slave", "path complete but transport still experimental");
    return 1;
#endif
    return -1;
}

static void repl_transport_rdma_disconnect_slave(int fd) {
    (void)fd;
#if KVS_ENABLE_RDMA
    repl_rdma_reset_ctx();
#endif
}

static const repl_transport_ops_t g_repl_transport_tcp_ops = {
    .name = "tcp",
    .supported = 1,
    .send = repl_transport_tcp_send,
    .connect_slave = repl_transport_tcp_connect_slave,
    .disconnect_slave = repl_transport_tcp_disconnect_slave,
};

static const repl_transport_ops_t g_repl_transport_ebpf_ops = {
    .name = "ebpf",
    .supported = 1,
    .send = repl_transport_ebpf_send,
    .connect_slave = repl_transport_ebpf_connect_slave,
    .disconnect_slave = repl_transport_ebpf_disconnect_slave,
};

static const repl_transport_ops_t g_repl_transport_rdma_ops = {
    .name = "rdma",
    .supported = KVS_ENABLE_RDMA,
    .send = repl_transport_rdma_send,
    .connect_slave = repl_transport_rdma_connect_slave,
    .disconnect_slave = repl_transport_rdma_disconnect_slave,
};

/* kprobe+RDMA WRITE transport ops — kprobe 透明拦截 TCP send，
 * 用户态转发模块通过 RDMA WRITE 发送。send() 不应被直接调用。 */
/* 后台连接线程参数 */
typedef struct kprobe_mr_connect_arg_s {
    char host[64];
    int port;
    int tcp_fd;
} kprobe_mr_connect_arg_t;

static void *kprobe_mr_connect_thread(void *arg) {
    kprobe_mr_connect_arg_t *a = (kprobe_mr_connect_arg_t *)arg;
    fprintf(stderr, "kprobe rdma: [DBG] MR connect thread started for %s:%d\n",
        a->host, a->port);
    int rc = repl_kprobe_rdma_connect_mr(a->host, a->port, a->tcp_fd);
    fprintf(stderr, "kprobe rdma: [DBG] MR connect thread DONE rc=%d (0=OK)\n", rc);
    free(a);
    return NULL;
}

/* kprobe+RDMA WRITE transport ops
 * send() 返回 -1 触发 TCP fallback，kprobe 在内核态透明拦截。
 * 首次调用时在后台线程连接 slave 的 kprobe-rdma listener。 */
static int repl_transport_kprobe_rdma_send(conn_t *c, const unsigned char *buf, size_t len) {
    (void)buf; (void)len;
    /* 只对有效的 TCP socket fd 发起连接，跳过 stdin/stdout/stderr 等非 socket fd */
    if (!c || c->fd <= 2 || !KVS_ENABLE_KPROBE_RDMA) return 0;
    static volatile int mr_connect_started = 0;
    if (!mr_connect_started) {
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        if (getpeername(c->fd, (struct sockaddr *)&peer, &peer_len) == 0) {
            mr_connect_started = 1;
            char host[64];
            inet_ntop(AF_INET, &peer.sin_addr, host, sizeof(host));
            kprobe_mr_connect_arg_t *a = (kprobe_mr_connect_arg_t *)malloc(sizeof(*a));
            if (a) {
                snprintf(a->host, sizeof(a->host), "%s", host);
                a->port = (int)g_cfg.port;
                a->tcp_fd = dup(c->fd);
                pthread_t tid;
                if (pthread_create(&tid, NULL, kprobe_mr_connect_thread, a) == 0)
                    pthread_detach(tid);
                else {
                    close(a->tcp_fd);
                    free(a);
                }
            }
        }
    }
    /* 返回 -1，让 reactor 通过 TCP 发送（BPF kprobe 在内核拦截后经 ringbuf
     * 触发回调做 RDMA WRITE，此处只做 TCP 保底） */
    return -1;
}

static int repl_transport_kprobe_rdma_connect_slave(const char *host, int port) {
#if KVS_ENABLE_KPROBE_RDMA
    return repl_kprobe_rdma_establish(host, port);
#else
    (void)host; (void)port;
    return -1;
#endif
}

static void repl_transport_kprobe_rdma_disconnect_slave(int fd) {
    (void)fd;
#if KVS_ENABLE_KPROBE_RDMA
    repl_kprobe_rdma_cleanup();
#endif
}

static const repl_transport_ops_t g_repl_transport_kprobe_rdma_ops = {
    .name = "kprobe-rdma",
    .supported = KVS_ENABLE_KPROBE_RDMA,
    .send = repl_transport_kprobe_rdma_send,
    .connect_slave = repl_transport_kprobe_rdma_connect_slave,
    .disconnect_slave = repl_transport_kprobe_rdma_disconnect_slave,
};

static const char *repl_rdma_state_name(repl_rdma_state_t st) {
    switch (st) {
        case REPL_RDMA_STATE_INIT: return "INIT";
        case REPL_RDMA_STATE_CONNECTING: return "CONNECTING";
        case REPL_RDMA_STATE_ESTABLISHED: return "ESTABLISHED";
        case REPL_RDMA_STATE_SYNCING: return "SYNCING";
        case REPL_RDMA_STATE_STEADY: return "STEADY";
        case REPL_RDMA_STATE_BACKOFF: return "BACKOFF";
        case REPL_RDMA_STATE_FAILED: return "FAILED";
        case REPL_RDMA_STATE_FALLBACK_TCP: return "FALLBACK_TCP";
        default: return "UNKNOWN";
    }
}

static void repl_rdma_set_state(repl_rdma_state_t st, const char *reason) {
#if KVS_ENABLE_RDMA
    if (g_repl_rdma_ctx.state != st) {
        fprintf(stderr, "repl rdma: state transition %s -> %s%s%s\n",
            repl_rdma_state_name(g_repl_rdma_ctx.state), repl_rdma_state_name(st), reason ? " reason=" : "", reason ? reason : "");
    }
#endif
    g_repl_rdma_ctx.state = st;
}

static void repl_transport_mark_active(const char *name) {
    snprintf(g_repl_transport_active, sizeof(g_repl_transport_active), "%s", (name && *name) ? name : "tcp");
}

static void repl_transport_trigger_fallback(const char *reason, int cooldown_ms) {
    g_repl_transport_fallback_count++;
    snprintf(g_repl_transport_fallback_reason, sizeof(g_repl_transport_fallback_reason), "%s", reason ? reason : "transport_failure");
    g_repl_transport_fallback_until_ms = kvs_now_ms() + (cooldown_ms > 0 ? cooldown_ms : 5000);
    repl_transport_mark_active("tcp");
#if KVS_ENABLE_RDMA
    repl_rdma_set_state(REPL_RDMA_STATE_FALLBACK_TCP, g_repl_transport_fallback_reason);
#endif
}

const char *repl_transport_configured_name(void) {
    int use_rdma_fullsync = !strcasecmp(repl_fullsync_transport_name(), "rdma");
    int use_kprobe_realtime = !strcasecmp(repl_realtime_transport_name(), "kprobe-rdma");
    int use_ebpf_realtime = repl_realtime_should_use_ebpf();
    int use_ebpf_tcp_realtime = !strcasecmp(repl_realtime_transport_name(), "ebpf+tcp")
                             || !strcasecmp(repl_realtime_transport_name(), "tcp");
    if (use_rdma_fullsync && use_kprobe_realtime) return "rdma+kprobe";
    if (use_rdma_fullsync && use_ebpf_realtime) return "rdma+ebpf";
    if (use_rdma_fullsync && use_ebpf_tcp_realtime) return "rdma+ebpf-tcp";
    if (use_ebpf_tcp_realtime) return "tcp+ebpf-tcp";
    if (!strcasecmp(g_cfg.repl_transport_backend, "rdma")) return "rdma";
    if (!strcasecmp(g_cfg.repl_transport_backend, "ebpf") || !strcasecmp(g_cfg.repl_transport_backend, "sockmap")) return "ebpf";
    if (!strcasecmp(g_cfg.repl_transport_backend, "kprobe-rdma")) return "kprobe-rdma";
    return "tcp";
}

const char *repl_transport_active_name(void) {
    int use_rdma_fullsync = !strcasecmp(repl_fullsync_transport_name(), "rdma");
    int use_kprobe_realtime = !strcasecmp(repl_realtime_transport_name(), "kprobe-rdma");
    int use_ebpf_realtime = repl_realtime_should_use_ebpf();
    int use_ebpf_tcp_realtime = !strcasecmp(repl_realtime_transport_name(), "ebpf+tcp")
                             || !strcasecmp(repl_realtime_transport_name(), "tcp");
    if (use_rdma_fullsync && use_kprobe_realtime) return "rdma+kprobe";
    if (use_rdma_fullsync && use_ebpf_realtime) return "rdma+ebpf";
    if (use_rdma_fullsync && use_ebpf_tcp_realtime) return "rdma+ebpf-tcp";
    if (use_ebpf_tcp_realtime) return "tcp+ebpf-tcp";
    return g_repl_transport_active[0] ? g_repl_transport_active : repl_transport_configured_name();
}

const char *repl_transport_fallback_reason(void) {
    return g_repl_transport_fallback_reason;
}

unsigned long long repl_transport_fallback_count(void) {
    return g_repl_transport_fallback_count;
}

long long repl_transport_fallback_until_ms(void) {
    return g_repl_transport_fallback_until_ms;
}

static const repl_transport_ops_t *repl_transport_ops_for_conn(conn_t *c) {
    if (!c) {
        if (g_slave_transport_kind == KVS_REPL_TRANSPORT_RDMA) return &g_repl_transport_rdma_ops;
        if (g_slave_transport_kind == KVS_REPL_TRANSPORT_EBPF) return &g_repl_transport_ebpf_ops;
        if (g_slave_transport_kind == KVS_REPL_TRANSPORT_KPROBE_RDMA) return &g_repl_transport_kprobe_rdma_ops;
        if (g_slave_transport_kind == KVS_REPL_TRANSPORT_EBPF_TCP) return &g_repl_transport_tcp_ops;
        return &g_repl_transport_tcp_ops;
    }
    if (c->repl_transport_kind == KVS_REPL_TRANSPORT_RDMA) return &g_repl_transport_rdma_ops;
    if (c->repl_transport_kind == KVS_REPL_TRANSPORT_EBPF) return &g_repl_transport_ebpf_ops;
    if (c->repl_transport_kind == KVS_REPL_TRANSPORT_KPROBE_RDMA) return &g_repl_transport_kprobe_rdma_ops;
    if (c->repl_transport_kind == KVS_REPL_TRANSPORT_EBPF_TCP) return &g_repl_transport_tcp_ops;
    return &g_repl_transport_tcp_ops;
}

static int repl_should_use_rdma_now(void) {
    if (strcasecmp(repl_fullsync_transport_name(), "rdma") != 0 && strcasecmp(g_cfg.repl_transport_backend, "rdma") != 0) return 0;
    if (!g_repl_transport_rdma_ops.supported) return 0;
    if (g_repl_transport_fallback_until_ms > kvs_now_ms()) return 0;
    return 1;
}

static int repl_should_use_ebpf_now(void) {
    const char *t = repl_realtime_transport_name();
    if (strcasecmp(t, "ebpf") != 0 && strcasecmp(t, "sockmap") != 0
        && strcasecmp(g_cfg.repl_transport_backend, "ebpf") != 0 && strcasecmp(g_cfg.repl_transport_backend, "sockmap") != 0) return 0;
    if (!g_repl_transport_ebpf_ops.supported || !repl_ebpf_supported()) return 0;
    if (g_repl_transport_fallback_until_ms > kvs_now_ms()) return 0;
    return 1;
}

static const repl_transport_ops_t *repl_transport_ops(void) {
    /* In hybrid mode, the main ops are for realtime transport */
    int use_rdma_fullsync = !strcasecmp(repl_fullsync_transport_name(), "rdma");
    int use_ebpf_realtime = repl_realtime_should_use_ebpf();
    if (use_rdma_fullsync && use_ebpf_realtime) {
        /* Hybrid: RDMA for fullsync, eBPF for realtime.
         * The main transport is eBPF (over TCP) for realtime data. */
        if (g_repl_transport_ebpf_ops.supported && repl_ebpf_supported())
            return &g_repl_transport_ebpf_ops;
        return &g_repl_transport_tcp_ops;
    }
    if (repl_should_use_rdma_now()) return &g_repl_transport_rdma_ops;
    if (repl_should_use_ebpf_now()) return &g_repl_transport_ebpf_ops;
    return &g_repl_transport_tcp_ops;
}

static int repl_transport_supported(void) {
    if (!strcasecmp(g_cfg.repl_transport_backend, "rdma")) return g_repl_transport_rdma_ops.supported;
    if (!strcasecmp(g_cfg.repl_transport_backend, "kprobe-rdma")) return g_repl_transport_kprobe_rdma_ops.supported;
    if (!strcasecmp(g_cfg.repl_transport_backend, "ebpf") || !strcasecmp(g_cfg.repl_transport_backend, "sockmap")) return g_repl_transport_ebpf_ops.supported && repl_ebpf_supported();
    return 1;
}

const char *repl_transport_name(void) {
    int use_rdma_fullsync = !strcasecmp(repl_fullsync_transport_name(), "rdma");
    int use_kprobe_realtime = !strcasecmp(repl_realtime_transport_name(), "kprobe-rdma");
    int use_ebpf_realtime = repl_realtime_should_use_ebpf();
    if (use_rdma_fullsync && use_kprobe_realtime) return "rdma+kprobe";
    if (use_rdma_fullsync && use_ebpf_realtime) return "rdma+ebpf";
    return repl_transport_active_name();
}

int repl_transport_send(conn_t *c, const unsigned char *buf, size_t len) {
    const repl_transport_ops_t *ops = repl_transport_ops_for_conn(c);
    int rc = ops->send(c, buf, len);
    if (rc == 0) {
        repl_transport_mark_active(ops->name);
        return 0;
    }
    if (ops == &g_repl_transport_rdma_ops) {
        repl_transport_trigger_fallback("rdma_send_failure", 5000);
    } else if (ops == &g_repl_transport_ebpf_ops) {
        repl_transport_trigger_fallback("ebpf_send_failure", 5000);
    } else if (ops == &g_repl_transport_kprobe_rdma_ops) {
        /* kprobe-rdma send 正常情况下不直接调用；若被调用，不做 fallback */
    }
    return rc;
}

int repl_transport_send_many(conn_t *c, const unsigned char *buf1, size_t len1, const unsigned char *buf2, size_t len2) {
    if (repl_transport_send(c, buf1, len1) != 0) return -1;
    if (repl_transport_send(c, buf2, len2) != 0) return -1;
    return 0;
}

/* ---- Dual-transport: fullsync vs realtime ---- */

const char *repl_fullsync_transport_name(void) {
    /* Use dedicated config if set, otherwise fall back to repl_transport_backend */
    if (g_cfg.repl_fullsync_transport[0]) return g_cfg.repl_fullsync_transport;
    if (!strcasecmp(g_cfg.repl_transport_backend, "rdma")) return "rdma";
    return "tcp";
}

const char *repl_realtime_transport_name(void) {
    if (g_cfg.repl_realtime_transport[0]) return g_cfg.repl_realtime_transport;
    if (!strcasecmp(g_cfg.repl_transport_backend, "ebpf") || !strcasecmp(g_cfg.repl_transport_backend, "sockmap")) return "ebpf";
    if (!strcasecmp(g_cfg.repl_transport_backend, "kprobe-rdma")) return "kprobe-rdma";
    return "tcp";
}

static const repl_transport_ops_t *repl_transport_ops_for_context(int send_ctx) {
    if (send_ctx == KVS_REPL_SEND_FULLSYNC) {
        const char *t = repl_fullsync_transport_name();
        /* 只记录真正的全量同步传输。全量完成后的 backlog 回放（REPLACK/REPLDONE
         * 追赶，ebpf+tcp 模式下数据由 ebpf-proxy 转发）不是全量数据，不记录
         * transport 日志，避免增量阶段产生 "fullsync using TCP" 噪音。 */
        int is_real_fullsync = g_repl_fullsync_in_progress;
        if (!strcasecmp(t, "rdma") && g_repl_transport_rdma_ops.supported
            && g_repl_rdma_ctx.connected && g_repl_transport_fallback_until_ms <= kvs_now_ms()) {
            if (is_real_fullsync) transport_log("fullsync using RDMA");
            return &g_repl_transport_rdma_ops;
        }
        if (is_real_fullsync) transport_log("fullsync using TCP");
        return &g_repl_transport_tcp_ops;
    }
    /* KVS_REPL_SEND_REALTIME */
    const char *t = repl_realtime_transport_name();
    if (!strcasecmp(t, "kprobe-rdma") && g_repl_transport_kprobe_rdma_ops.supported && g_cfg.kprobe_enabled) {
        transport_log("realtime using KPROBE+RDMA");
        return &g_repl_transport_kprobe_rdma_ops;
    }
    if ((!strcasecmp(t, "ebpf") || !strcasecmp(t, "sockmap")) && g_repl_transport_ebpf_ops.supported && repl_ebpf_supported()) {
        transport_log("realtime using EBPF");
        return &g_repl_transport_ebpf_ops;
    }
    if (!strcasecmp(t, "ebpf+tcp") || !strcasecmp(t, "tcp")) {
        transport_log("realtime using EBPF+TCP (forward via BPF→TCP)");
        return &g_repl_transport_tcp_ops;
    }
    transport_log("realtime using TCP");
    return &g_repl_transport_tcp_ops;
}

int repl_fullsync_send(conn_t *c, const unsigned char *buf, size_t len) {
    const repl_transport_ops_t *ops = repl_transport_ops_for_context(KVS_REPL_SEND_FULLSYNC);
    int rc = ops->send(c, buf, len);
    if (rc == 0) {
        return 0;
    }
    /* If already on TCP, don't "fallback" to the same transport */
    if (ops == &g_repl_transport_tcp_ops) return -1;
    /* Fallback: try TCP on failure, and disable RDMA retry for this session */
    transport_log("%s failed, fallback to TCP", ops->name);
    repl_transport_trigger_fallback("fullsync_send_fail", 3600000); /* 1h cooldown */
    rc = repl_transport_tcp_send(c, buf, len);
    if (rc == 0) return 0;
    return -1;
}

int repl_realtime_send(conn_t *c, const unsigned char *buf, size_t len) {
    const repl_transport_ops_t *ops = repl_transport_ops_for_context(KVS_REPL_SEND_REALTIME);
    int rc = ops->send(c, buf, len);
    if (rc == 0) {
        return 0;
    }
    /* Fallback: TCP send — 同时为 kprobe 提供抓取数据源 */
    rc = repl_transport_tcp_send(c, buf, len);
    return rc;
}

static int repl_realtime_should_use_ebpf(void) {
    const char *t = repl_realtime_transport_name();
    return !strcasecmp(t, "ebpf") || !strcasecmp(t, "sockmap");
}

static void build_slave_state_path(void) {
    if (g_slave_state_path[0]) return;
    snprintf(g_slave_state_path, sizeof(g_slave_state_path), "%s.replstate", g_cfg.aof_path);
}


void repl_note_fullsync(size_t snapshot_bytes) {
    g_repl_fullsync_count++;
    g_repl_snapshot_bytes += (unsigned long long)snapshot_bytes;
}

static int ensure_repl_backlog(void) {
    if (g_repl_backlog.buf) return 0;
    g_repl_backlog.buf = (unsigned char *)kvs_malloc(KVS_REPL_BACKLOG_SIZE);
    if (!g_repl_backlog.buf) return -1;
    g_repl_backlog.cap = KVS_REPL_BACKLOG_SIZE;
    g_repl_backlog.histlen = 0;
    g_repl_backlog.head = 0;
    g_repl_backlog.start_offset = g_master_repl_offset;
    g_repl_backlog.end_offset = g_master_repl_offset;
    return 0;
}

int repl_backlog_feed(const unsigned char *buf, size_t len) {
    pthread_mutex_lock(&g_backlog_lock);
    if (!buf || len == 0) { pthread_mutex_unlock(&g_backlog_lock); return 0; }
    /* 无 slave 连接时不分配 backlog，节省 10 MB */
    if (!g_replicas) { pthread_mutex_unlock(&g_backlog_lock); return 0; }
    if (ensure_repl_backlog() != 0) { pthread_mutex_unlock(&g_backlog_lock); return -1; }
    if (len >= g_repl_backlog.cap) {
        buf += len - g_repl_backlog.cap;
        len = g_repl_backlog.cap;
        memcpy(g_repl_backlog.buf, buf, len);
        g_repl_backlog.head = 0;
        g_repl_backlog.histlen = len;
        g_repl_backlog.end_offset += len;
        g_repl_backlog.start_offset = g_repl_backlog.end_offset - g_repl_backlog.histlen;
        pthread_mutex_unlock(&g_backlog_lock);
        return 0;
    }

    size_t tail = (g_repl_backlog.head + g_repl_backlog.histlen) % g_repl_backlog.cap;
    size_t first = g_repl_backlog.cap - tail;
    if (first > len) first = len;
    memcpy(g_repl_backlog.buf + tail, buf, first);
    if (len > first) memcpy(g_repl_backlog.buf, buf + first, len - first);

    if (g_repl_backlog.histlen + len <= g_repl_backlog.cap) {
        g_repl_backlog.histlen += len;
    } else {
        size_t overflow = g_repl_backlog.histlen + len - g_repl_backlog.cap;
        g_repl_backlog.head = (g_repl_backlog.head + overflow) % g_repl_backlog.cap;
        g_repl_backlog.histlen = g_repl_backlog.cap;
    }

    g_repl_backlog.end_offset += len;
    g_repl_backlog.start_offset = g_repl_backlog.end_offset - g_repl_backlog.histlen;
    pthread_mutex_unlock(&g_backlog_lock);
    return 0;
}

void repl_note_broadcast(size_t bytes) {
    g_repl_broadcast_bytes += (unsigned long long)bytes;
    g_master_repl_offset += (unsigned long long)bytes;
}

static void ensure_master_replid(void) {
    unsigned int a, b, c, d, e;
    if (g_master_replid[0]) return;
    a = (unsigned int)(kvs_now_ms() & 0xffffffffu);
    b = (unsigned int)getpid();
    c = (unsigned int)((uintptr_t)&g_master_replid & 0xffffffffu);
    d = (unsigned int)(time(NULL) & 0xffffffffu);
    e = (unsigned int)((uintptr_t)pthread_self() & 0xffffffffu);
    snprintf(g_master_replid, sizeof(g_master_replid), "%08x%08x%08x%08x%08x", a, b, c, d, e);
    g_master_replid[40] = '\0';
}

static void repl_set_link_state(int up) {
    int old;
    pthread_mutex_lock(&g_slave_conf_lock);
    old = g_master_link_up;
    g_master_link_up = up;
    if (up) g_master_last_io_ms = kvs_now_ms();
    pthread_mutex_unlock(&g_slave_conf_lock);
#if KVS_ENABLE_RDMA
    if (!strcasecmp(repl_transport_name(), "rdma") && old != up) {
        fprintf(stderr, "repl rdma: link_state transition %s -> %s\n", old ? "up" : "down", up ? "up" : "down");
    }
#endif
}

int repl_slaveof(const char *host, int port) {
    if (!host || port <= 0) return -1;
    pthread_mutex_lock(&g_slave_conf_lock);
    snprintf(g_slave_host, sizeof(g_slave_host), "%s", host);
    g_slave_port = port;
    g_cfg.role = ROLE_SLAVE;
    snprintf(g_cfg.master_host, sizeof(g_cfg.master_host), "%s", host);
    g_cfg.master_port = port;
    g_slave_conf_gen++;
    pthread_mutex_unlock(&g_slave_conf_lock);
    repl_set_link_state(0);
    repl_slave_state_save();
    return 0;
}

int repl_slaveof_noone(void) {
    pthread_mutex_lock(&g_slave_conf_lock);
    g_slave_host[0] = '\0';
    g_slave_port = 0;
    g_cfg.role = ROLE_MASTER;
    g_cfg.master_host[0] = '\0';
    g_cfg.master_port = 0;
    g_slave_conf_gen++;
    pthread_mutex_unlock(&g_slave_conf_lock);
    repl_set_link_state(0);
    repl_slave_state_save();
    return 0;
}

int repl_get_master_addr(char *host, size_t cap, int *port) {
    if (!host || cap == 0 || !port) return -1;
    pthread_mutex_lock(&g_slave_conf_lock);
    snprintf(host, cap, "%s", g_slave_host);
    *port = g_slave_port;
    pthread_mutex_unlock(&g_slave_conf_lock);
    return 0;
}

int repl_is_master_link_up(void) {
    int up;
    pthread_mutex_lock(&g_slave_conf_lock);
    up = g_master_link_up;
    pthread_mutex_unlock(&g_slave_conf_lock);
    return up;
}

const char *repl_master_link_state_name(void) {
    return repl_is_master_link_up() ? "up" : "down";
}

const char *repl_master_id(void) {
    ensure_master_replid();
    return g_master_replid;
}

unsigned long long repl_master_offset(void) {
    return g_master_repl_offset;
}

unsigned long long repl_connected_slaves(void) {
    unsigned long long n = 0;
    pthread_mutex_lock(&g_repl_lock);
    for (conn_t *c = g_replicas; c; c = c->next_replica) n++;
    pthread_mutex_unlock(&g_repl_lock);
    return n;
}

unsigned long long repl_fullsync_count(void) {
    return g_repl_fullsync_count;
}

unsigned long long repl_partialsync_ok_count(void) {
    return g_repl_partialsync_ok_count;
}

unsigned long long repl_partialsync_err_count(void) {
    return g_repl_partialsync_err_count;
}

unsigned long long repl_broadcast_bytes(void) {
    return g_repl_broadcast_bytes;
}

unsigned long long repl_snapshot_bytes(void) {
    return g_repl_snapshot_bytes;
}

unsigned long long repl_backlog_size(void) {
    return (unsigned long long)g_repl_backlog.cap;
}

unsigned long long repl_backlog_histlen(void) {
    return (unsigned long long)g_repl_backlog.histlen;
}

unsigned long long repl_backlog_start_offset(void) {
    return g_repl_backlog.start_offset;
}

unsigned long long repl_backlog_end_offset(void) {
    return g_repl_backlog.end_offset;
}

int repl_rdma_effective_recv_slots(void) {
#if KVS_ENABLE_RDMA
    return repl_rdma_cfg_recv_slots();
#else
    return 0;
#endif
}

int repl_rdma_effective_qp_wr_depth(void) {
#if KVS_ENABLE_RDMA
    return repl_rdma_cfg_qp_wr_depth();
#else
    return 0;
#endif
}

int repl_rdma_effective_chunk_size(void) {
#if KVS_ENABLE_RDMA
    return (int)repl_rdma_cfg_chunk_size();
#else
    return 0;
#endif
}

int repl_rdma_is_connected(void) {
#if KVS_ENABLE_RDMA
    return g_repl_rdma_ctx.connected;
#else
    return 0;
#endif
}

unsigned long long repl_rdma_disconnect_count(void) {
    return g_rdma_disconnect_count;
}

unsigned long long repl_rdma_reject_count(void) {
    return g_rdma_reject_count;
}

unsigned long long repl_rdma_send_cq_error_count(void) {
    return g_rdma_send_cq_error_count;
}

unsigned long long repl_rdma_recv_cq_error_count(void) {
    return g_rdma_recv_cq_error_count;
}

void repl_note_partialsync_result(int ok) {
    if (ok) g_repl_partialsync_ok_count++;
    else g_repl_partialsync_err_count++;
}

/* Master: receive FULLRESYNCWR and enable WRITE mode */
void repl_rdma_set_remote_write_mr(uint32_t rkey, uint64_t addr) {
    g_repl_rdma_ctx.remote_write_rkey = rkey;
    g_repl_rdma_ctx.remote_write_addr = addr;
    g_repl_rdma_ctx.use_write_mode = 1;;
    g_repl_rdma_ctx.write_total_sent = 0;
    repl_rdma_log("set_remote_write_mr", "WRITE mode enabled");
}

int repl_rdma_set_write_total_size(size_t sz) {
    g_repl_rdma_ctx.write_total_size = sz;
    return 0;
}

/* WRITE 模式: 发送最终空 WRITE_WITH_IMM 通知 slave 数据已完成 */
void repl_rdma_send_final_imm(void) {
    if (!g_repl_rdma_ctx.use_write_mode) return;
    if (!g_repl_rdma_ctx.connected) return;

    /* 始终发送 IMM 信号（覆盖 warm-up SEND 已消费全部数据的情况） */

    struct ibv_send_wr wr, *bad;
    struct ibv_sge sge;
    static unsigned char dummy = 0;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)&dummy;
    sge.length = 1;
    sge.lkey = 0;  /* 对 1 字节 inline 数据不严格要求 lkey */

    memset(&wr, 0, sizeof(wr));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.imm_data = htonl((uint32_t)g_repl_rdma_ctx.write_total_size);
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.wr.rdma.remote_addr = g_repl_rdma_ctx.remote_write_addr;
    wr.wr.rdma.rkey = g_repl_rdma_ctx.remote_write_rkey;

    pthread_mutex_lock(&g_repl_rdma_send_lock);
    if (ibv_post_send(g_repl_rdma_ctx.id->qp, &wr, &bad) != 0) {
        fprintf(stderr, "repl rdma: send_final_imm failed: errno=%d(%s)\n",
            errno, strerror(errno));
    } else {
        repl_rdma_log("send_final_imm", "WRITE_WITH_IMM sent");
    }
    pthread_mutex_unlock(&g_repl_rdma_send_lock);
}

/* Master: wait for FULLRESYNCWR response from slave */
int repl_rdma_wait_for_write_mr_response(int tcp_fd, int timeout_ms) {
    char buf[256];
    int waited = 0;
    while (waited < timeout_ms) {
        ssize_t r = recv(tcp_fd, buf, sizeof(buf) - 1, MSG_DONTWAIT);
        if (r > 0) {
            buf[r] = '\0';
            unsigned long rkey = 0, addr = 0;
            if (sscanf(buf, "+FULLRESYNCWR %lu %lu", &rkey, &addr) == 2) {
                repl_rdma_set_remote_write_mr((uint32_t)rkey, (uint64_t)addr);
                return 0;
            }
        } else if (r == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            return -1;
        }
        usleep(10000);
        waited += 10;
    }
    g_repl_rdma_ctx.use_write_mode = 0;
    return -1;
}

/* Slave: 准备 file-backed WRITE 目标 —— mmap 临时文件 → 注册 MR → 发 FULLRESYNCWR。
 * 数据由 master 的 RDMA WRITE 直接写入文件页（零拷贝，无 buffer→file 拷贝）。
 * 失败/超限时发布任何 MR，master 的 MR 等待超时后走 sendfile 回退。
 * 上限来自配置 repl_rdma_write_buf_max_mb（默认 256）。
 * transfer_id 由 master 在 FULLRESYNC header 中下发，用于唯一命名 + 完成绑定。 */
int repl_rdma_slave_prepare_target(int tcp_fd, uint64_t transfer_id,
                                   size_t total_bytes) {
    repl_fullsync_target_t *t = &g_slave_fullsync_target;
    struct ibv_mr *mr;
    char path[512];
    int fd;
    void *mapping;

    repl_rdma_raise_memlock();           /* 大 dump 目标 MR 注册需要 >64MB 锁定内存 */
    repl_rdma_slave_target_cleanup(0);   /* 幂等清理上次残留 */

    /* 配置上限：超限拒绝（不发 FULLRESYNCWR，master 超时 → sendfile） */
    if (total_bytes == 0 || transfer_id == 0 ||
        total_bytes > (size_t)g_cfg.repl_rdma_write_buf_max_mb * 1024 * 1024) {
        repl_rdma_log("prepare_target", "target too large or bad id, reject");
        return -EFBIG;
    }
    /* 该 transfer 已被 master 放弃（回退 sendfile）：拒绝创建空目标，
     * sendfile 数据落到 legacy tmp，finish 才会回放正确文件。 */
    if (g_slave_fullsync_aborted_tid == transfer_id) {
        repl_rdma_log("prepare_target", "transfer aborted by master, reject");
        return -EFBIG;
    }

    /* 唯一命名：dump_path.fullsync.recv.tmp.<pid>.<transfer_id> */
    t->transfer_id = transfer_id;
    snprintf(path, sizeof(path), "%s.fullsync.recv.tmp.%ld.%llu",
             g_cfg.dump_path, (long)getpid(), (unsigned long long)transfer_id);

    fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) return -errno;
    if (ftruncate(fd, (off_t)total_bytes) < 0) { close(fd); unlink(path); return -errno; }

    mapping = mmap(NULL, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) { close(fd); unlink(path); return -errno; }

    struct ibv_pd *pd = NULL;
    for (int retry = 0; retry < 50; retry++) {
        pd = g_repl_rdma_ctx.pd;
        if (pd) break;
        usleep(100000);  /* 100ms */
    }
    if (!pd) { munmap(mapping, total_bytes); close(fd); unlink(path); return -1; }

    mr = ibv_reg_mr(pd, mapping, total_bytes,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) { munmap(mapping, total_bytes); close(fd); unlink(path); return -1; }

    t->fd = fd;
    snprintf(t->path, sizeof(t->path), "%s", path);
    t->mapping = mapping;
    t->mapping_len = total_bytes;
    t->expected_bytes = total_bytes;
    t->mr = mr;

    /* WRITE 模式使用本目标文件，关闭并清理 set_sync_state 开的 legacy tmp fd */
    if (g_slave_fullsync_tmp_fd >= 0) {
        char legacy[512];
        snprintf(legacy, sizeof(legacy), "%s.fullsync.recv.tmp.%ld",
                 g_cfg.dump_path, (long)getpid());
        close(g_slave_fullsync_tmp_fd);
        unlink(legacy);
        g_slave_fullsync_tmp_fd = -1;
    }

    /* FULLRESYNCWR：<transfer_id> <addr> <rkey> <capacity> */
    {
        char resp[256];
        int n = snprintf(resp, sizeof(resp), "+FULLRESYNCWR %llu %lu %u %zu\r\n",
                         (unsigned long long)transfer_id,
                         (unsigned long)(uintptr_t)mapping,
                         mr->rkey, total_bytes);
        if (send(tcp_fd, resp, (size_t)n, 0) < 0) {
            repl_rdma_slave_target_cleanup(1);   /* 删除不完整目标文件 */
            return -1;
        }
    }

    repl_rdma_log("prepare_target", "file-backed WRITE target ready, FULLRESYNCWR sent");
    return 0;
}

/* Slave: 幂等清理全量同步目标。remove_incomplete=1 时删除未完成的临时文件。
 * 所有成功/失败/超时/断开路径最终都回到这里。 */
void repl_rdma_slave_target_cleanup(int remove_incomplete) {
    repl_fullsync_target_t *t = &g_slave_fullsync_target;
    if (t->mr) {
        ibv_dereg_mr((struct ibv_mr *)t->mr);
        t->mr = NULL;
    }
    if (t->mapping) {
        if (t->fd >= 0) {
            munmap(t->mapping, t->mapping_len);   /* file-backed（Task 3） */
        } else {
            kvs_free(t->mapping);                 /* heap 模式 */
        }
        t->mapping = NULL;
    }
    if (t->fd >= 0) {
        close(t->fd);
        t->fd = -1;
    }
    if (remove_incomplete && t->path[0]) {
        unlink(t->path);
    }
    repl_fullsync_target_reset(t);
}

/* Master: 幂等清理全量同步 transfer 状态。不关闭共享 RDMA 连接。
 * 在传输完成、失败、超时、owner 连接断开时调用。 */
void repl_rdma_master_transfer_cleanup(conn_t *c) {
    if (c) {
        c->repl_transfer_id = 0;
        c->repl_fullsync_state = KVS_FULLSYNC_ST_IDLE;
        c->repl_fullsync_expected_bytes = 0;
        c->repl_fullsync_mode = KVS_FULLSYNC_RDMA_WRITE;
        c->repl_fullsync_deadline_ms = 0;
    }
    g_repl_rdma_ctx.use_write_mode = 0;
    g_repl_rdma_ctx.remote_write_addr = 0;
    g_repl_rdma_ctx.remote_write_rkey = 0;
    g_repl_rdma_ctx.write_total_sent = 0;
    g_repl_rdma_ctx.write_total_size = 0;
    g_repl_rdma_ctx.transfer_id = 0;
    g_repl_rdma_ctx.remote_capacity = 0;
    g_repl_rdma_ctx.write_offset = 0;
    g_repl_rdma_ctx.posted_wr = 0;
    g_repl_rdma_ctx.completed_wr = 0;
    g_repl_rdma_ctx.final_imm_posted = 0;
    g_repl_rdma_ctx.cq_failed = 0;
    if (g_repl_rdma_ctx.transfer_owner == c) g_repl_rdma_ctx.transfer_owner = NULL;
}

/* 提高 RLIMIT_MEMLOCK：注册大 MR（全量 WRITE 目标，如 85MB dump）需要锁定内存，
 * 默认 soft/hard 限制仅 64MB，超限 ibv_reg_mr 会 ENOMEM。best-effort（root 或硬限可提才生效）。 */
void repl_rdma_raise_memlock(void) {
    struct rlimit rlim;
    rlim.rlim_cur = RLIM_INFINITY;
    rlim.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_MEMLOCK, &rlim) != 0) {
        fprintf(stderr, "repl rdma: raise_memlock failed errno=%d(%s) — 大 dump 全量可能 ENOMEM\n",
                errno, strerror(errno));
    }
}

/* Master: 生成单调递增的非零 transfer ID（每进程） */
uint64_t repl_fullsync_next_transfer_id(void) {
    static uint64_t tid = 0;
    return ++tid;
}

/* Master: 登记活跃 transfer ID（供 FULLRESYNCWR 校验绑定） */
void repl_rdma_set_transfer_id(uint64_t tid) {
    g_repl_rdma_ctx.transfer_id = tid;
}

/* Master: 收到 FULLRESYNCWR 后设置远端 MR 并启用 WRITE 模式。
 * 校验 transfer 匹配（陈旧响应不得激活 WRITE）与 addr/rkey/capacity 合法。 */
void repl_rdma_set_remote_write_mr_info(uint64_t tid, uint64_t addr,
                                        uint32_t rkey, uint64_t capacity) {
    /* 无活跃 transfer 或 tid 不匹配：迟到/陈旧响应不得激活 WRITE 模式 */
    if (g_repl_rdma_ctx.transfer_id == 0 || tid != g_repl_rdma_ctx.transfer_id) {
        fprintf(stderr, "repl rdma: FULLRESYNCWR tid=%llu but active transfer=%llu, ignored\n",
                (unsigned long long)tid, (unsigned long long)g_repl_rdma_ctx.transfer_id);
        return;
    }
    if (addr == 0 || rkey == 0 ||
        capacity < g_repl_rdma_ctx.write_total_size) {
        fprintf(stderr, "repl rdma: invalid FULLRESYNCWR addr=0x%llx rkey=%u cap=%llu total=%zu\n",
                (unsigned long long)addr, rkey, (unsigned long long)capacity,
                g_repl_rdma_ctx.write_total_size);
        return;
    }
    g_repl_rdma_ctx.remote_write_addr = addr;
    g_repl_rdma_ctx.remote_write_rkey = rkey;
    g_repl_rdma_ctx.remote_capacity = capacity;
    g_repl_rdma_ctx.use_write_mode = 1;
    repl_rdma_log("set_remote_write_mr", "WRITE mode enabled");
}

/* Master: 等待 slave 的 FULLRESYNCWR（有界超时）。成功返回 0，超时/断开返回 -1 → sendfile 回退。
 *
 * 关键：queue_snapshot 在 reactor 线程内同步执行，等待期间 reactor 被阻塞，
 * 无法通过 parse_resp_stream 处理 slave 的 FULLRESYNCWR。因此这里直接 MSG_PEEK
 * 读 c->fd：只消费一行 FULLRESYNCWR，其余字节留给正常解析器。 */
int repl_fullsync_wait_remote_mr(conn_t *c, int timeout_ms) {
    char peek[512];
    long long deadline = timeout_ms > 0 ? kvs_now_ms() + timeout_ms : 0;
    (void)c;
    while (!timeout_ms || kvs_now_ms() < deadline) {
        /* 已被 reactor 解析路径设置（竞态窗口内到达时） */
        if (g_repl_rdma_ctx.use_write_mode && g_repl_rdma_ctx.remote_write_addr) return 0;
        if (!g_repl_rdma_ctx.connected) return -1;

        ssize_t n = recv(c->fd, peek, sizeof(peek), MSG_PEEK | MSG_DONTWAIT);
        if (n <= 0) {
            if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                fprintf(stderr, "repl rdma: wait_remote_mr recv errno=%d(%s)\n", errno, strerror(errno));
                return -1;
            }
            usleep(10000);
            continue;
        }

        /* 在整个 peeked 缓冲里搜索 FULLRESYNCWR 行。reactor 被 queue_snapshot 阻塞，
         * 缓冲里可能先有 `*N` RESP 心跳/ACK 等控制消息挡路，不能只看第一行。 */
        int found_at = -1;
        int line_end = -1;
        for (ssize_t i = 0; i + 14 <= n; i++) {
            if (peek[i] == '+' && memcmp(peek + i, "+FULLRESYNCWR ", 14) == 0) {
                ssize_t le = i;
                while (le + 1 < n && !(peek[le] == '\r' && peek[le + 1] == '\n')) le++;
                if (le + 1 < n) {
                    found_at = (int)i;
                    line_end = (int)le + 2;
                    break;
                }
                break;   /* 行未完整，等更多数据 */
            }
        }
        if (found_at < 0) { usleep(10000); continue; }

        kvs_fullsync_mr_msg_t mr;
        if (kvs_fullsync_parse_remote_mr(peek + found_at, (size_t)(line_end - found_at), &mr) != 0) {
            usleep(10000);
            continue;
        }
        if (kvs_fullsync_validate_remote_mr(&mr, g_repl_rdma_ctx.transfer_id,
                                            g_repl_rdma_ctx.write_total_size) != 0) {
            fprintf(stderr, "repl rdma: wait_remote_mr invalid/stale FULLRESYNCWR tid=%llu\n",
                    (unsigned long long)mr.transfer_id);
            return -1;
        }
        /* 消费 [0..line_end)：FULLRESYNCWR 之前的字节（心跳/ACK 等控制消息）一并丢弃。
         * 它们本应由被阻塞的 reactor 处理，但全量同步期间丢失无害（slave 会重发心跳）。 */
        {
            size_t consumed = 0;
            while (consumed < (size_t)line_end) {
                char discard[256];
                size_t want = (size_t)(line_end - (int)consumed);
                if (want > sizeof(discard)) want = sizeof(discard);
                ssize_t w = recv(c->fd, discard, want, MSG_DONTWAIT);
                if (w <= 0) break;
                consumed += (size_t)w;
            }
        }
        g_repl_rdma_ctx.remote_write_addr = mr.addr;
        g_repl_rdma_ctx.remote_write_rkey = mr.rkey;
        g_repl_rdma_ctx.remote_capacity = mr.capacity;
        g_repl_rdma_ctx.use_write_mode = 1;
        return 0;
    }
    return -1;
}


/* Master: 零拷贝 TCP sendfile 回退 —— 把 dump 剩余字节从文件页直发 slave TCP。 */
int repl_fullsync_sendfile(int tcp_fd, int dump_fd, size_t total_bytes) {
    off_t offset = 0;
    size_t sent = 0;
    while (sent < total_bytes) {
        ssize_t n = sendfile(tcp_fd, dump_fd, &offset, total_bytes - sent);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
            fprintf(stderr, "repl: fullsync sendfile failed: %s\n", strerror(errno));
            return -1;
        }
        if (n == 0) {
            fprintf(stderr, "repl: fullsync sendfile EOF before total_bytes\n");
            return -1;
        }
        sent += (size_t)n;
    }
    repl_rdma_log("fullsync_sendfile", "sent via TCP sendfile");
    return 0;
}

/* Master: 等待全部 send WR（含 WRITE）完成（cq 线程释放 slot）。 */
int repl_rdma_wait_all_sends(int timeout_ms) {
    long long deadline = kvs_now_ms() + timeout_ms;
    while (g_repl_rdma_ctx.send_slots_in_flight > 0
           && kvs_now_ms() < deadline && g_repl_rdma_ctx.connected) {
        usleep(1000);
    }
    return g_repl_rdma_ctx.send_slots_in_flight > 0 ? -1 : 0;
}

/* Master: 向 slave 发送 FULLSYNCEND（TCP 完成信号，兜底 rxe IMM 事件丢失）。 */
void repl_fullsync_send_end(int tcp_fd, uint64_t tid) {
    char msg[128];
    int n = snprintf(msg, sizeof(msg), "+FULLSYNCEND %llu\r\n",
                     (unsigned long long)tid);
    for (int i = 0; i < 100 && n > 0; i++) {
        ssize_t w = send(tcp_fd, msg, (size_t)n, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (w > 0) break;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { usleep(1000); continue; }
        break;
    }
}

/* Master: 向 slave 发送 FULLSYNCABORT，丢弃该 transfer 的残留 WRITE 目标（best-effort）。 */
void repl_fullsync_abort_master(int tcp_fd, uint64_t tid) {
    char msg[128];
    int n = snprintf(msg, sizeof(msg), "+FULLSYNCABORT %llu\r\n",
                     (unsigned long long)tid);
    for (int i = 0; i < 100 && n > 0; i++) {
        ssize_t w = send(tcp_fd, msg, (size_t)n, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (w > 0) break;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { usleep(1000); continue; }
        break;
    }
}

/* Slave: 把全量 KVSD 数据写入 file-backed 目标映射（SEND 预热 chunk 走这里，
 * 与 master WRITE 的数据一起落在同一目标文件；无 file-backed 目标时返回 -1 走 legacy tmp）。
 * 由 parse_resp_stream 的 KVSD 拦截调用。 */
int repl_rdma_slave_target_write(const unsigned char *data, size_t len,
                                 unsigned long long loaded_offset) {
    repl_fullsync_target_t *t = &g_slave_fullsync_target;
    if (t->fd < 0 || !t->mapping) return -1;
    if (loaded_offset + len > t->expected_bytes) return -1;
    memcpy((unsigned char *)t->mapping + loaded_offset, data, len);
    return 0;
}

/* Slave: 处理 FULLSYNCABORT —— 记录被放弃的 transfer，丢弃匹配的残留目标（含未完成临时文件）。 */
void repl_rdma_slave_abort_target(uint64_t tid) {
    repl_fullsync_target_t *t = &g_slave_fullsync_target;
    if (t->transfer_id != 0 && t->transfer_id != tid) {
        fprintf(stderr, "repl rdma: FULLSYNCABORT tid=%llu != active %llu, ignored\n",
                (unsigned long long)tid, (unsigned long long)t->transfer_id);
        return;
    }
    /* 即使目标尚未创建也记录：后续 prepare_target 将拒绝该 tid */
    g_slave_fullsync_aborted_tid = tid;
    if (!t->mapping && !t->mr && t->fd < 0) {
        fprintf(stderr, "repl rdma: FULLSYNCABORT tid=%llu (no target yet)\n",
                (unsigned long long)tid);
        return;
    }
    fprintf(stderr, "repl rdma: FULLSYNCABORT, cleaning target tid=%llu\n",
            (unsigned long long)t->transfer_id);
    repl_rdma_slave_target_cleanup(1);
}

int repl_rdma_slave_has_write_buffer(void) {
    return (g_slave_fullsync_target.mapping != NULL) ? 1 : 0;
}

/* Slave: 收到 master 的 FULLSYNCEND 控制信号（TCP），标记目标可 finalize。
   rxe 下 RDMA IMM 事件可能丢失，用该信号兜底（master 已 drain 全部 WRITE）。 */
void repl_rdma_slave_finalize_signal(uint64_t tid) {
    repl_fullsync_target_t *t = &g_slave_fullsync_target;
    if (t->transfer_id != 0 && t->transfer_id != tid) {
        fprintf(stderr, "repl rdma: FULLSYNCEND tid=%llu != active %llu, ignored\n",
                (unsigned long long)tid, (unsigned long long)t->transfer_id);
        return;
    }
    if (t->mapping || t->mr || t->fd >= 0) {
        t->complete = 1;
        repl_rdma_log("finalize_signal", "target marked complete (TCP signal)");
    }
}

int repl_rdma_slave_check_write_complete(void) {
    repl_fullsync_target_t *t = &g_slave_fullsync_target;
    /* 兜底：rxe 下 cq 线程的事件可能丢失导致 IMM 未被处理，这里直接 poll 一次抓
     * RECV_RDMA_WITH_IMM。与 cq 线程的竞争无害（谁先 poll 到谁处理）。 */
    if (!t->imm_received && !t->complete && g_repl_rdma_ctx.cq) {
        struct ibv_wc wc;
        int np = ibv_poll_cq(g_repl_rdma_ctx.cq, 1, &wc);
        if (np > 0 && wc.status == IBV_WC_SUCCESS) {
            if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                repl_rdma_log("check_write_complete", "IMM via direct poll");
                t->imm_received = 1;
            } else {
                repl_rdma_cq_process_wc(&wc);   /* 其他 completion 走正常分发 */
            }
        }
    }
    if ((!t->imm_received && !t->complete) || !t->mapping) return 0;
    /* 防止与 reset_ctx 并发：检查连接状态 */
    if (!g_repl_rdma_ctx.connected) return 0;

    /* 数据已由 RDMA WRITE 直接写入映射文件页。持久化：msync + 释放 RDMA 资源。
     * fd/path 保留给 repl_slave_finish_fullsync（replay + rename）。 */
    if (msync(t->mapping, t->mapping_len, MS_SYNC) < 0) {
        fprintf(stderr, "repl rdma: slave msync failed: %s\n", strerror(errno));
        return -1;
    }
    if (t->mr) {
        ibv_dereg_mr((struct ibv_mr *)t->mr);
        t->mr = NULL;
    }
    if (t->mapping) {
        munmap(t->mapping, t->mapping_len);
        t->mapping = NULL;
        t->mapping_len = 0;
    }

    g_slave_fullsync_loaded_bytes = t->expected_bytes;
    repl_slave_finish_fullsync();
    return 1;
}

void repl_slave_set_sync_state(const char *replid, unsigned long long applied_offset, unsigned long long durable_offset, int fullsync_loading, unsigned long long fullsync_target_bytes) {
    if (replid && *replid) {
        snprintf(g_slave_master_replid, sizeof(g_slave_master_replid), "%s", replid);
    }
    g_slave_repl_applied_offset = applied_offset;
    g_slave_repl_durable_offset = durable_offset;
    g_slave_repl_offset = applied_offset;
    g_slave_loading_fullsync = fullsync_loading;
    g_slave_fullsync_target_bytes = fullsync_loading ? fullsync_target_bytes : 0;
    g_slave_fullsync_loaded_bytes = 0;
#if KVS_ENABLE_RDMA
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") || !strcasecmp(g_cfg.repl_transport_backend, "rdma"))
        fprintf(stderr, "repl rdma: slave_sync_state - replid=%s applied_offset=%llu durable_offset=%llu fullsync_loading=%d target_bytes=%llu\n",
            g_slave_master_replid, g_slave_repl_applied_offset, g_slave_repl_durable_offset, g_slave_loading_fullsync, g_slave_fullsync_target_bytes);
#endif
    /* Manage fullsync temp file: open when starting fullsync, close when not */
    if (fullsync_loading && g_slave_fullsync_tmp_fd < 0) {
        char tmp_path[512];
        snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.recv.tmp.%ld",
                 g_cfg.dump_path, (long)getpid());
        g_slave_fullsync_tmp_fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (g_slave_fullsync_tmp_fd < 0) {
            fprintf(stderr, "repl: slave fullsync tmp file open failed: %s\n", strerror(errno));
        }
    } else if (!fullsync_loading && g_slave_fullsync_tmp_fd >= 0) {
        /* sync not loading but tmp fd still open — close and cleanup */
        char tmp_path[512];
        snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.recv.tmp.%ld",
                 g_cfg.dump_path, (long)getpid());
        close(g_slave_fullsync_tmp_fd);
        unlink(tmp_path);
        g_slave_fullsync_tmp_fd = -1;
    }
    repl_slave_state_save();
}

static int repl_slave_send_repldone(void);

void repl_slave_finish_fullsync(void) {
    static int already_done = 0;
    if (already_done) return;
    already_done = 1;
    g_slave_loading_fullsync = 0;
    g_slave_fullsync_target_bytes = 0;
    g_slave_fullsync_loaded_bytes = 0;
    if (g_slave_repl_durable_offset < g_slave_repl_applied_offset) {
        g_slave_repl_durable_offset = g_slave_repl_applied_offset;
    }
#if KVS_ENABLE_RDMA
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") || !strcasecmp(g_cfg.repl_transport_backend, "rdma")) {
        fprintf(stderr, "repl rdma: slave_fullsync - finished applied_offset=%llu durable_offset=%llu\n", g_slave_repl_applied_offset, g_slave_repl_durable_offset);
        {
            extern kv_config_t g_cfg;
            extern kvs_hash_t global_hash;
            int cnt = 0;
            for (int t = 0; t < 2; t++) {
                if (!global_hash.ht[t].nodes) continue;
                for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
                    for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                        cnt++;
                    }
                }
            }
            fprintf(stderr, "repl rdma: slave_debug - total_hash_entries=%d\n", cnt);
            char *v = kvs_hash_get(&global_hash, "pre:k:000000");
            fprintf(stderr, "repl rdma: slave_debug - HGET pre:k:000000 = %s\n", v ? v : "(null)");
        }
    }
#endif
    /* 全量同步完成后，从临时 KVSD 文件加载数据到内存并持久化。
     * WRITE 模式：数据在 repl_rdma_slave_prepare_target 的 mmap 目标文件里；
     * SEND/fallback 模式：数据在 parse_resp_stream 拦截写入的 legacy tmp 文件里。 */
    {
        repl_fullsync_target_t *tg = &g_slave_fullsync_target;
        int fd = (tg->fd >= 0) ? tg->fd : g_slave_fullsync_tmp_fd;
        char tmp_path[512];
        if (tg->path[0]) {
            snprintf(tmp_path, sizeof(tmp_path), "%s", tg->path);
        } else {
            snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.recv.tmp.%ld",
                     g_cfg.dump_path, (long)getpid());
        }

        if (fd >= 0) {
            fsync(fd);
            close(fd);
        }
        g_slave_fullsync_tmp_fd = -1;
        if (tg->fd >= 0) tg->fd = -1;

        if (fd >= 0 || tmp_path[0]) {
            /* Load KVSD data into memory + TTL */
            unsigned long long aof_off = replay_dump_file(tmp_path);
            fprintf(stderr, "repl: slave fullsync loaded from %s, aof_offset=%llu\n",
                    tmp_path, aof_off);

            /* Move temp file to official dump path as the persistence base */
            if (rename(tmp_path, g_cfg.dump_path) != 0) {
                fprintf(stderr, "repl: slave fullsync rename to %s failed: %s\n",
                        g_cfg.dump_path, strerror(errno));
            } else {
                fprintf(stderr, "repl: slave fullsync dump saved to %s\n", g_cfg.dump_path);
            }
        } else {
            fprintf(stderr, "repl: slave fullsync finished but no temp file\n");
        }
        /* 目标文件已 rename 为正式 dump，清 path 元数据（不再 unlink） */
        tg->path[0] = '\0';
        tg->fd = -1;
    }
    repl_slave_state_save();
    repl_slave_send_repldone();
}

void repl_slave_note_applied(size_t rawlen) {
    if (!g_slave_loading_fullsync) {
        unsigned long long before = g_slave_repl_applied_offset;
        g_slave_repl_applied_offset += (unsigned long long)rawlen;
        g_slave_repl_offset = g_slave_repl_applied_offset;
#if KVS_ENABLE_RDMA
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") || !strcasecmp(g_cfg.repl_transport_backend, "rdma"))
        fprintf(stderr, "repl rdma: slave_apply - applied_before=%llu rawlen=%zu applied_after=%llu durable=%llu\n",
            before, rawlen, g_slave_repl_applied_offset, g_slave_repl_durable_offset);
#endif
        repl_slave_state_save();
    } else {
        g_slave_fullsync_loaded_bytes += (unsigned long long)rawlen;
#if KVS_ENABLE_RDMA
        if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") || !strcasecmp(g_cfg.repl_transport_backend, "rdma"))
            fprintf(stderr, "repl rdma: slave_apply - fullsync_chunk=%zu loaded=%llu target=%llu\n",
                rawlen, g_slave_fullsync_loaded_bytes, g_slave_fullsync_target_bytes);
#endif
        if (g_slave_fullsync_target_bytes > 0 && g_slave_fullsync_loaded_bytes >= g_slave_fullsync_target_bytes) {
            repl_slave_finish_fullsync();
        }
    }
}

void repl_slave_note_durable(size_t rawlen) {
    (void)rawlen;
    if (g_slave_loading_fullsync) {
        repl_slave_send_ack();
        return;
    }
    if (g_slave_repl_durable_offset < g_slave_repl_applied_offset) {
        g_slave_repl_durable_offset = g_slave_repl_applied_offset;
        repl_slave_state_save();
    }
#if KVS_ENABLE_RDMA
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") || !strcasecmp(g_cfg.repl_transport_backend, "rdma"))
        fprintf(stderr, "repl rdma: slave_durable - applied=%llu durable=%llu\n",
            g_slave_repl_applied_offset, g_slave_repl_durable_offset);
#endif
    repl_slave_send_ack();
}

static int repl_transport_send_on_slave_link(const unsigned char *buf, size_t len) {
    if (!buf || len == 0) return 0;
    if (g_slave_transport_kind == KVS_REPL_TRANSPORT_RDMA) {
        return g_repl_transport_rdma_ops.send(NULL, buf, len);
    }
    return -1;
}

int repl_slave_send_ack(void) {
    unsigned char cmd[256];
    char applied[32];
    char durable[32];
    size_t n;
    long long now;
    if (g_cfg.role != ROLE_SLAVE) return 0;
    if (!repl_is_master_link_up()) return 0;
    /* Rate-limit: at most one REPLACK per second */
    now = kvs_now_ms();
    if (now - g_slave_last_ack_ms < 1000) return 0;
    snprintf(applied, sizeof(applied), "%llu", g_slave_repl_applied_offset);
    snprintf(durable, sizeof(durable), "%llu", g_slave_repl_durable_offset);
    n = resp_build_cmd3(cmd, sizeof(cmd), "REPLACK", applied, durable);
    if (g_slave_transport_kind == KVS_REPL_TRANSPORT_RDMA) {
        if (repl_transport_send_on_slave_link(cmd, n) == 0) {
            g_slave_last_ack_ms = kvs_now_ms();
            return 0;
        }
        return -1;
    }
    /* TCP / eBPF transport: send REPLACK over the slave fd */
    if (g_slave_fd >= 0) {
        ssize_t sent = send(g_slave_fd, cmd, n, 0);
        if (sent == (ssize_t)n) {
            g_slave_last_ack_ms = kvs_now_ms();
            return 0;
        }
    }
    return -1;
}

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

void repl_replica_update_ack(conn_t *c, unsigned long long applied_offset, unsigned long long durable_offset) {
    if (!c) return;
    c->repl_applied_offset_ack = applied_offset;
    c->repl_durable_offset_ack = durable_offset;
    c->repl_last_ack_ms = kvs_now_ms();
}

const char *repl_slave_master_id(void) {
    return g_slave_master_replid;
}

unsigned long long repl_slave_offset(void) {
    return g_slave_repl_applied_offset;
}

unsigned long long repl_slave_applied_offset(void) {
    return g_slave_repl_applied_offset;
}

unsigned long long repl_slave_durable_offset(void) {
    return g_slave_repl_durable_offset;
}

int repl_slave_loading_fullsync(void) {
    return g_slave_loading_fullsync;
}

int repl_slave_state_load(void) {
    FILE *fp;
    char replid[41] = {0};
    unsigned long long applied_offset = 0;
    unsigned long long durable_offset = 0;
    build_slave_state_path();
    fp = fopen(g_slave_state_path, "r");
    if (!fp) return 0;
    if (fscanf(fp, "%40s %llu %llu", replid, &applied_offset, &durable_offset) == 3) {
        snprintf(g_slave_master_replid, sizeof(g_slave_master_replid), "%s", replid);
        g_slave_repl_applied_offset = applied_offset;
        g_slave_repl_durable_offset = durable_offset;
        g_slave_repl_offset = applied_offset;
    } else {
        rewind(fp);
        if (fscanf(fp, "%40s %llu", replid, &applied_offset) == 2) {
            snprintf(g_slave_master_replid, sizeof(g_slave_master_replid), "%s", replid);
            g_slave_repl_applied_offset = applied_offset;
            g_slave_repl_durable_offset = applied_offset;
            g_slave_repl_offset = applied_offset;
        }
    }
    fclose(fp);
    return 0;
}

int repl_slave_state_save(void) {
    FILE *fp;
    build_slave_state_path();
    fp = fopen(g_slave_state_path, "w");
    if (!fp) return -1;
    fprintf(fp, "%s %llu %llu\n",
        g_slave_master_replid[0] ? g_slave_master_replid : "?",
        g_slave_repl_applied_offset,
        g_slave_repl_durable_offset);
    fclose(fp);
    return 0;
}

int repl_backlog_can_continue(const char *replid, unsigned long long offset) {
    unsigned long long want_offset;
    ensure_master_replid();
    if (!replid || strcmp(replid, g_master_replid) != 0) return 0;
    if (!g_repl_backlog.buf) return 0;
    want_offset = offset;
    if (want_offset > g_repl_backlog.end_offset) return 0;
    return want_offset >= g_repl_backlog.start_offset;
}

int repl_backlog_write_range(conn_t *c, unsigned long long offset) {
    size_t delta, start_index, first;
    if (!c || !g_repl_backlog.buf) return -1;
    if (offset < g_repl_backlog.start_offset || offset > g_repl_backlog.end_offset) return -1;
    delta = (size_t)(offset - g_repl_backlog.start_offset);
    start_index = (g_repl_backlog.head + delta) % g_repl_backlog.cap;
    first = g_repl_backlog.histlen - delta;
    if (first == 0) return 0;
    if (start_index + first <= g_repl_backlog.cap) {
        if (repl_send_chunked(c, g_repl_backlog.buf + start_index, first) != 0) return -1;
    } else {
        size_t part1 = g_repl_backlog.cap - start_index;
        size_t part2 = first - part1;
        if (repl_send_chunked(c, g_repl_backlog.buf + start_index, part1) != 0) return -1;
        if (repl_send_chunked(c, g_repl_backlog.buf, part2) != 0) return -1;
    }
    c->repl_offset_sent = g_repl_backlog.end_offset;
    c->repl_last_send_ms = kvs_now_ms();
    return 0;
}

/* 深拷贝 backlog [offset, end_offset) 区间（reactor REPLACK/REPLDONE 追赶时调）。
 * 持 g_backlog_lock 一致读取：backlog 是环形缓冲，转发线程 repl_backlog_feed 会覆盖，故先深拷贝。
 * 返回 malloc 的 *out_buf + 长度；调用方经 repl_fwd_enqueue 入队后 kvs_free(*out_buf)。
 * 成功返 0；无数据返 0 且 *out_len=0；失败返 -1。 */
int repl_backlog_copy_range(unsigned long long offset, unsigned char **out_buf, size_t *out_len) {
    pthread_mutex_lock(&g_backlog_lock);
    int rc = -1;
    *out_buf = NULL;
    *out_len = 0;
    if (!g_repl_backlog.buf) goto out;
    if (offset < g_repl_backlog.start_offset || offset > g_repl_backlog.end_offset) goto out;
    size_t delta = (size_t)(offset - g_repl_backlog.start_offset);
    size_t start_index = (g_repl_backlog.head + delta) % g_repl_backlog.cap;
    size_t len = g_repl_backlog.histlen - delta;
    if (len == 0) { rc = 0; goto out; }
    unsigned char *buf = (unsigned char *)kvs_malloc(len);
    if (!buf) goto out;
    if (start_index + len <= g_repl_backlog.cap) {
        memcpy(buf, g_repl_backlog.buf + start_index, len);
    } else {
        size_t part1 = g_repl_backlog.cap - start_index;
        memcpy(buf, g_repl_backlog.buf + start_index, part1);
        memcpy(buf + part1, g_repl_backlog.buf, len - part1);
    }
    *out_buf = buf;
    *out_len = len;
    rc = 0;
out:
    pthread_mutex_unlock(&g_backlog_lock);
    return rc;
}

int repl_backlog_send_continue(conn_t *c, unsigned long long offset) {
    char hdr[128];
    int hn;
    unsigned long long continue_offset;
    if (!c || !g_repl_backlog.buf) return -1;
    if (offset < g_repl_backlog.start_offset || offset > g_repl_backlog.end_offset) return -1;
    continue_offset = g_repl_backlog.end_offset;
    hn = snprintf(hdr, sizeof(hdr), "+CONTINUE %s %llu\r\n", repl_master_id(), continue_offset);
    repl_note_send_context("continue-header", (size_t)hn, offset, (unsigned char *)hdr);
    if (repl_send_chunked(c, (unsigned char *)hdr, (size_t)hn) != 0) return -1;
    repl_note_send_context("continue-backlog", (size_t)(g_repl_backlog.end_offset - offset), offset, g_repl_backlog.buf);
    return repl_backlog_write_range(c, offset);
}

static int snapshot_slave_conf(char *host, size_t cap, int *port, int *gen, int *role) {
    if (!host || !port || !gen || !role) return -1;
    pthread_mutex_lock(&g_slave_conf_lock);
    snprintf(host, cap, "%s", g_slave_host);
    *port = g_slave_port;
    *gen = g_slave_conf_gen;
    *role = g_cfg.role;
    pthread_mutex_unlock(&g_slave_conf_lock);
    return 0;
}

static int slave_should_reconnect(int local_gen) {
    int changed = 0;
    pthread_mutex_lock(&g_slave_conf_lock);
    if (g_cfg.role != ROLE_SLAVE || local_gen != g_slave_conf_gen) changed = 1;
    pthread_mutex_unlock(&g_slave_conf_lock);
    return changed;
}

static void repl_slave_retry_pause(int rdma_fail_streak) {
    const char *tname = repl_transport_name();
    if (!strcasecmp(tname, "rdma") || !strcasecmp(tname, "rdma+ebpf")) {
        int delay_ms = 100 * (rdma_fail_streak > 0 ? rdma_fail_streak : 1);
        if (delay_ms < 100) delay_ms = 100;
        if (delay_ms > 1000) delay_ms = 1000;
        usleep((useconds_t)delay_ms * 1000);
    } else sleep(1);
}

static void repl_slave_ack_heartbeat(void) {
    long long now = kvs_now_ms();
    if (!repl_is_master_link_up()) return;
    if (now - g_slave_last_ack_ms < 1000) return;
    repl_slave_send_ack();
}

#if KVS_ENABLE_RDMA
typedef struct repl_rdma_bg_connect_arg_s {
    char host[128];
    int port;
} repl_rdma_bg_connect_arg_t;

static void *repl_rdma_bg_connect_thread(void *arg) {
    repl_rdma_bg_connect_arg_t *a = (repl_rdma_bg_connect_arg_t *)arg;
    repl_transport_rdma_connect_slave(a->host, a->port);
    kvs_free(a);
    return NULL;
}
#endif

static void *slave_thread(void *arg) {
    int rdma_fail_streak = 0;
    (void)arg;
    for (;;) {
        char host[128];
        int port = 0;
        int gen = 0;
        int role = ROLE_MASTER;
        snapshot_slave_conf(host, sizeof(host), &port, &gen, &role);

        if (role != ROLE_SLAVE || host[0] == '\0' || port <= 0) {
            if (!strcasecmp(repl_transport_name(), "rdma")) repl_rdma_log("slave_loop", "link down because slave config is inactive");
            repl_set_link_state(0);
            repl_slave_retry_pause(rdma_fail_streak);
            continue;
        }

        if (!repl_transport_supported()) {
            if (!strcasecmp(repl_transport_name(), "rdma")) repl_rdma_log("slave_loop", "link down because transport is unsupported");
            repl_set_link_state(0);
            fprintf(stderr, "repl transport '%s' requested but not compiled in; falling back to disconnected state\n", repl_transport_name());
            rdma_fail_streak++;
            repl_slave_retry_pause(rdma_fail_streak);
            continue;
        }

        /* ---- Hybrid dual-transport: RDMA for fullsync + kprobe-rdma for realtime ---- */
        int use_rdma_fullsync = !strcasecmp(repl_fullsync_transport_name(), "rdma") && KVS_ENABLE_RDMA;
        int use_kprobe_realtime = !strcasecmp(repl_realtime_transport_name(), "kprobe-rdma");

        if (use_rdma_fullsync && use_kprobe_realtime) {
            int tcp_fd = repl_transport_tcp_connect_slave(host, port);
            if (tcp_fd < 0) {
                repl_set_link_state(0);
                rdma_fail_streak++;
                repl_slave_retry_pause(rdma_fail_streak);
                continue;
            }

            /* 按需模式: REPLSYNC 立即通过 TCP 发送，不再等待 RDMA 就绪。
             * Master 收到 REPLSYNC 后会在 queue_snapshot() 中按需启动 RDMA listener。
             * Slave 在后台并发尝试 RDMA 连接（会自动重试直到 master listener 就绪）。 */
#if KVS_ENABLE_RDMA
            {
                repl_rdma_bg_connect_arg_t *rdma_arg = (repl_rdma_bg_connect_arg_t *)kvs_malloc(sizeof(repl_rdma_bg_connect_arg_t));
                if (rdma_arg) {
                    snprintf(rdma_arg->host, sizeof(rdma_arg->host), "%s", host);
                    rdma_arg->port = port;
                    pthread_t rdma_tid;
                    if (pthread_create(&rdma_tid, NULL, repl_rdma_bg_connect_thread, rdma_arg) != 0) {
                        kvs_free(rdma_arg);
                    } else {
                        pthread_detach(rdma_tid);
                    }
                }
            }
#endif
            g_slave_transport_kind = KVS_REPL_TRANSPORT_KPROBE_RDMA;
            repl_transport_mark_active("kprobe-rdma");
            rdma_fail_streak = 0;

            /* 立即发送 REPLSYNC（不等待 RDMA） */
            {
                unsigned char cmd[256];
                char offbuf[32], durablebuf[32];
                snprintf(offbuf, sizeof(offbuf), "%llu", g_slave_repl_offset);
                snprintf(durablebuf, sizeof(durablebuf), "%llu", g_slave_repl_durable_offset);
                size_t n = resp_build_cmd4(cmd, sizeof(cmd), "REPLSYNC",
                    g_slave_master_replid[0] ? g_slave_master_replid : "?", offbuf, durablebuf);
                if (send(tcp_fd, cmd, n, 0) < 0) { close(tcp_fd); continue; }
                fprintf(stderr, "kprobe rdma: REPLSYNC sent over TCP immediately\n");
            }

            g_slave_fd = tcp_fd;
            repl_set_link_state(1);
            unsigned char buf[BUFFER_CAP * 4 + 4096];
            size_t blen = 0;

            for (;;) {
                if (slave_should_reconnect(gen)) break;

                int had_new_data = 0;

                /* TCP recv 控制消息（全量数据通过 RDMA arrival 通知触达） */
                ssize_t r = recv(tcp_fd, buf + blen, sizeof(buf) - blen, MSG_DONTWAIT);
                if (r > 0) {
                    blen += (size_t)r;
                    had_new_data = 1;
                    parse_resp_stream(NULL, buf, &blen, 1);
                    repl_slave_ack_heartbeat();
                    repl_set_link_state(1);
                } else if (r == 0) {
                    break;
                } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    break;
                }

                if (blen > 0) {
                    parse_resp_stream(NULL, buf, &blen, 1);
                }

                /* 也轮询 RDMA 全量数据 */
#if KVS_ENABLE_RDMA
                if (g_repl_rdma_ctx.connected) {
                        /* One-Sided WRITE: check IMM。
                         * 全量完成后 RDMA 数据已全部落 MR：不 break（否则 slave 断开
                         * 重连 → 重复 fullsync），而是断开 RDMA、保持 TCP 继续收增量。 */
                    if (repl_rdma_slave_check_write_complete() == 1) {
                        g_repl_rdma_ctx.connected = 0;
                        continue;
                    }
                    int recv_slot = -1;
                    size_t rdma_blen = 0;
                    if (repl_rdma_wait_cq_recv_completion(100, &recv_slot, &rdma_blen) == 0
                        && recv_slot >= 0 && rdma_blen > 0) {
                        /* P3.4: 零复制 — 直接读注册 buffer，省去 malloc+memcpy */
                        {
                            size_t dlen = rdma_blen;
                            unsigned char *direct = repl_rdma_recv_direct(recv_slot, &dlen);
                            if (direct) {
                                if (blen + dlen <= sizeof(buf)) {
                                    memcpy(buf + blen, direct, dlen);
                                    blen += dlen;
                                    had_new_data = 1;
                                }
                                repl_rdma_repost_recv(recv_slot);
                            }
                        }
                    }
                }
#endif

                if (had_new_data && blen > 0) {
                    parse_resp_stream(NULL, buf, &blen, 1);
                    repl_slave_ack_heartbeat();
                    repl_set_link_state(1);
                } else if (had_new_data) {
                    repl_slave_ack_heartbeat();
                    repl_set_link_state(1);
                }
            }

            g_slave_fd = -1;
            repl_transport_tcp_disconnect_slave(tcp_fd);
            g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
            repl_set_link_state(0);
            sleep(1);
            continue;
        }

        /* ---- Hybrid dual-transport: RDMA for fullsync + eBPF+tcp for realtime ----
         * eBPF kprobe/tcp_recvmsg 捕获客户端写入 → ringbuf → TCP send() 转发。
         * 全量同步走 RDMA，增量同步走 TCP（eBPF 转发），两者独立。
         * 全量完成后 RDMA 断开，TCP 连接保持，继续收增量数据。 */
        {
            int use_rdma_fullsync2 = !strcasecmp(repl_fullsync_transport_name(), "rdma") && KVS_ENABLE_RDMA;
            int use_ebpf_tcp = !strcasecmp(repl_realtime_transport_name(), "ebpf+tcp")
                            || !strcasecmp(repl_realtime_transport_name(), "tcp");
            if (use_rdma_fullsync2 && use_ebpf_tcp) {
                int tcp_fd = repl_transport_tcp_connect_slave(host, port);
                if (tcp_fd < 0) {
                    repl_set_link_state(0);
                    rdma_fail_streak++;
                    repl_slave_retry_pause(rdma_fail_streak);
                    continue;
                }

                /* 先发送 REPLSYNC — master 收到后会启动 RDMA listener */
                g_slave_transport_kind = KVS_REPL_TRANSPORT_EBPF_TCP;
                repl_transport_mark_active("ebpf+tcp");
                rdma_fail_streak = 0;

                /* Register TCP fd with eBPF sockmap for realtime sync */
                if (repl_ebpf_register_fd(tcp_fd, 0) != 0) {
                    fprintf(stderr, "repl ebpf: fd registration failed on slave link, "
                            "using tcp-compatible path\n");
                }

                /* ── 创建 proxy 监听器 (port+1) ──
                 * ebpf-proxy 通过此端口直连 slave，发送捕获的客户端写入数据。
                 * 在发送 REPLSYNC 之前创建，确保 master 通知 proxy 时 listener 已就绪。 */
                int proxy_listen_fd = -1;
                int proxy_fd = -1;
                int proxy_port = g_cfg.master_port + 1;
                {
                    proxy_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
                    if (proxy_listen_fd >= 0) {
                        int yes = 1;
                        setsockopt(proxy_listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
                        {
                            int flags = fcntl(proxy_listen_fd, F_GETFL, 0);
                            if (flags >= 0) fcntl(proxy_listen_fd, F_SETFL, flags | O_NONBLOCK);
                        }
                        struct sockaddr_in paddr;
                        memset(&paddr, 0, sizeof(paddr));
                        paddr.sin_family = AF_INET;
                        paddr.sin_port = htons((uint16_t)proxy_port);
                        paddr.sin_addr.s_addr = htonl(INADDR_ANY);
                        if (bind(proxy_listen_fd, (struct sockaddr *)&paddr, sizeof(paddr)) < 0) {
                            fprintf(stderr, "repl ebpf-tcp: proxy listener bind port=%d failed: %s\n",
                                    proxy_port, strerror(errno));
                            close(proxy_listen_fd);
                            proxy_listen_fd = -1;
                        } else if (listen(proxy_listen_fd, 1) < 0) {
                            fprintf(stderr, "repl ebpf-tcp: proxy listener listen failed: %s\n",
                                    strerror(errno));
                            close(proxy_listen_fd);
                            proxy_listen_fd = -1;
                        } else {
                            fprintf(stderr, "repl ebpf-tcp: proxy listener on port %d\n", proxy_port);
                        }
                    }
                }

                {
                    unsigned char cmd[256];
                    char offbuf[32], durablebuf[32];
                    snprintf(offbuf, sizeof(offbuf), "%llu", g_slave_repl_offset);
                    snprintf(durablebuf, sizeof(durablebuf), "%llu", g_slave_repl_durable_offset);
                    size_t n = resp_build_cmd4(cmd, sizeof(cmd), "REPLSYNC",
                        g_slave_master_replid[0] ? g_slave_master_replid : "?", offbuf, durablebuf);
                    if (send(tcp_fd, cmd, n, 0) < 0) { close(tcp_fd); continue; }
                    fprintf(stderr, "repl ebpf-tcp: REPLSYNC sent over TCP\n");
                }

                /* REPLSYNC 已发，等 master 启动 RDMA listener 后再尝试 RDMA 连接 */
                usleep(500000);  /* 500ms — 给 master 时间启动 RDMA listener */

                /* 后台尝试 RDMA 连接 */
        #if KVS_ENABLE_RDMA
                {
                    repl_rdma_bg_connect_arg_t *rdma_arg = (repl_rdma_bg_connect_arg_t *)
                        kvs_malloc(sizeof(repl_rdma_bg_connect_arg_t));
                    if (rdma_arg) {
                        snprintf(rdma_arg->host, sizeof(rdma_arg->host), "%s", host);
                        rdma_arg->port = port;
                        pthread_t rdma_tid;
                        if (pthread_create(&rdma_tid, NULL, repl_rdma_bg_connect_thread, rdma_arg) != 0) {
                            kvs_free(rdma_arg);
                        } else {
                            pthread_detach(rdma_tid);
                        }
                    }
                }
        #endif

                g_slave_fd = tcp_fd;
                repl_set_link_state(1);
                unsigned char buf[BUFFER_CAP * 4 + 4096];
                size_t blen = 0;

                /* ═══ 混合接收循环 ═══
                 * 全量同步期间: RDMA 收全量 chunk + TCP 收控制命令
                 * 全量同步完成后: RDMA 断开，TCP 继续收增量数据
                 * 同时接收 proxy 连接并读取 ebpf-proxy 转发的增量数据。
                 * 循环不退出——TCP 连接保持，增量数据持续到达 */
                for (;;) {
                    if (slave_should_reconnect(gen)) break;

                    int had_new_data = 0;

                    /* ── Accept proxy 连接（非阻塞，如尚未连接）── */
                    if (proxy_fd < 0 && proxy_listen_fd >= 0) {
                        proxy_fd = accept(proxy_listen_fd, NULL, NULL);
                        if (proxy_fd >= 0) {
                            fprintf(stderr, "repl ebpf-tcp: proxy connected fd=%d\n", proxy_fd);
                        } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                            fprintf(stderr, "repl ebpf-tcp: proxy accept error: %s\n", strerror(errno));
                        }
                    }

                    /* ── Proxy 增量数据 ── */
                    if (proxy_fd >= 0) {
                        ssize_t pr = recv(proxy_fd, buf + blen, sizeof(buf) - blen, MSG_DONTWAIT);
                        if (pr > 0) {
                            blen += (size_t)pr;
                            had_new_data = 1;
                            parse_resp_stream(NULL, buf, &blen, 1);
                            repl_slave_ack_heartbeat();
                            repl_set_link_state(1);
                        } else if (pr == 0) {
                            fprintf(stderr, "repl ebpf-tcp: proxy disconnected\n");
                            close(proxy_fd);
                            proxy_fd = -1;
                        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            fprintf(stderr, "repl ebpf-tcp: proxy recv error: %s\n",
                                    strerror(errno));
                            close(proxy_fd);
                            proxy_fd = -1;
                        }
                    }

                    /* ── TCP 控制消息 + 增量数据 ──
                     * FULLRESYNC arrives via TCP first; must process BEFORE
                     * RDMA poll so g_slave_loading_fullsync is set before
                     * the first KVSD chunk arrives. */
                    ssize_t r = recv(tcp_fd, buf + blen, sizeof(buf) - blen, MSG_DONTWAIT);
                    if (r > 0) {
                        blen += (size_t)r;
                        had_new_data = 1;
                        parse_resp_stream(NULL, buf, &blen, 1);
                        repl_slave_ack_heartbeat();
                        repl_set_link_state(1);
                    } else if (r == 0) {
                        break;
                    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        break;
                    }

                    /* ── RDMA 全量数据（全量同步期间）── */
        #if KVS_ENABLE_RDMA
                    if (g_repl_rdma_ctx.connected) {
                            /* One-Sided WRITE: check IMM。
                             * 全量完成后 RDMA 数据已全部落 MR：不 break（否则 slave 断开
                             * 重连 → 重复 fullsync），而是断开 RDMA、保持 TCP 继续收增量。 */
                    if (repl_rdma_slave_check_write_complete() == 1) {
                        g_repl_rdma_ctx.connected = 0;
                        continue;
                    }
                    int recv_slot = -1;
                        size_t rdma_blen = 0;
                        if (repl_rdma_wait_cq_recv_completion(100, &recv_slot, &rdma_blen) == 0
                            && recv_slot >= 0 && rdma_blen > 0) {
                            /* P3.4: 零复制 */
                            {
                                size_t dlen = rdma_blen;
                                unsigned char *direct = repl_rdma_recv_direct(recv_slot, &dlen);
                                if (direct) {
                                    if (blen + dlen <= sizeof(buf)) {
                                        memcpy(buf + blen, direct, dlen);
                                        blen += dlen;
                                        had_new_data = 1;
                                    } else {
                                        fprintf(stderr, "repl ebpf-tcp: buffer overflow blen=%zu rdma=%zu\n",
                                            blen, dlen);
                                    }
                                    repl_rdma_repost_recv(recv_slot);
                                }
                            }
                        }
                    }
        #endif

                    if (had_new_data && blen > 0) {
                        parse_resp_stream(NULL, buf, &blen, 1);
                        repl_slave_ack_heartbeat();
                        repl_set_link_state(1);
                    }

                    if (blen > 0) {
                        parse_resp_stream(NULL, buf, &blen, 1);
                    }

                    /* 无数据时短暂休眠避免忙循环 */
                    if (!had_new_data)
                        usleep(1000);
                }

                g_slave_fd = -1;
                repl_transport_tcp_disconnect_slave(tcp_fd);
                if (proxy_fd >= 0) close(proxy_fd);
                if (proxy_listen_fd >= 0) close(proxy_listen_fd);
                proxy_fd = -1;
                proxy_listen_fd = -1;
                g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
                repl_set_link_state(0);
                sleep(1);
                continue;
            }
        }

        /* ---- Hybrid dual-transport: RDMA for fullsync + eBPF for realtime ---- */
        int use_ebpf_realtime = repl_realtime_should_use_ebpf();

        /* If using dual transport, always establish TCP as the primary link.
         * REPLSYNC 立即通过 TCP 发送，不再等待 RDMA 就绪。
         * Master 收到 REPLSYNC 后在 queue_snapshot() 中按需启动 RDMA listener。 */
        if (use_rdma_fullsync && use_ebpf_realtime) {
            int tcp_fd = repl_transport_tcp_connect_slave(host, port);
            if (tcp_fd < 0) {
                repl_set_link_state(0);
                rdma_fail_streak++;
                repl_slave_retry_pause(rdma_fail_streak);
                continue;
            }

            /* 后台并发尝试 RDMA 连接（会自动重试直到 master listener 就绪） */
#if KVS_ENABLE_RDMA
            {
                repl_rdma_bg_connect_arg_t *rdma_arg = (repl_rdma_bg_connect_arg_t *)kvs_malloc(sizeof(repl_rdma_bg_connect_arg_t));
                if (rdma_arg) {
                    snprintf(rdma_arg->host, sizeof(rdma_arg->host), "%s", host);
                    rdma_arg->port = port;
                    pthread_t rdma_tid;
                    if (pthread_create(&rdma_tid, NULL, repl_rdma_bg_connect_thread, rdma_arg) != 0) {
                        kvs_free(rdma_arg);
                    } else {
                        pthread_detach(rdma_tid);
                    }
                }
            }
#endif
            g_slave_transport_kind = KVS_REPL_TRANSPORT_EBPF;
            repl_transport_mark_active("ebpf");
            rdma_fail_streak = 0;

            /* Register TCP fd with eBPF sockmap for realtime sync */
            if (repl_ebpf_register_fd(tcp_fd, 0) != 0) {
                fprintf(stderr, "repl ebpf: fd registration failed on slave link, using tcp-compatible path\n");
            }

            /* 立即发送 REPLSYNC（不等待 RDMA） */
            {
                unsigned char cmd[256];
                char offbuf[32], durablebuf[32];
                snprintf(offbuf, sizeof(offbuf), "%llu", g_slave_repl_offset);
                snprintf(durablebuf, sizeof(durablebuf), "%llu", g_slave_repl_durable_offset);
                size_t n = resp_build_cmd4(cmd, sizeof(cmd), "REPLSYNC",
                    g_slave_master_replid[0] ? g_slave_master_replid : "?", offbuf, durablebuf);
                if (send(tcp_fd, cmd, n, 0) < 0) break;
                fprintf(stderr, "repl: REPLSYNC sent over TCP immediately\n");
            }

            g_slave_fd = tcp_fd;
            repl_set_link_state(1);
            unsigned char buf[BUFFER_CAP * 4 + 4096];
            size_t blen = 0;

            for (;;) {
                if (slave_should_reconnect(gen)) break;

                int had_new_data = 0;

                /* TCP recv is non-blocking; FULLRESYNC header arrives via RDMA
                 * alongside snapshot data, so we don't block waiting for TCP. */
                ssize_t r = recv(tcp_fd, buf + blen, sizeof(buf) - blen, MSG_DONTWAIT);
                if (r > 0) {
                    blen += (size_t)r;
                    had_new_data = 1;
                    /* Parse TCP data immediately to free buffer space for RDMA */
                    parse_resp_stream(NULL, buf, &blen, 1);
                    repl_slave_ack_heartbeat();
                    repl_set_link_state(1);
                } else if (r == 0) {
                    break;
                } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    break;
                }

                /* Parse any leftover from previous RDMA chunk to maximize
                 * buffer space before checking for new RDMA data */
                if (blen > 0) {
                    parse_resp_stream(NULL, buf, &blen, 1);
                }

                /* Also check RDMA for fullsync data */
#if KVS_ENABLE_RDMA
                if (g_repl_rdma_ctx.connected) {
                        /* One-Sided WRITE: check IMM。
                         * 全量完成后 RDMA 数据已全部落 MR：不 break（否则 slave 断开
                         * 重连 → 重复 fullsync），而是断开 RDMA、保持 TCP 继续收增量。 */
                    if (repl_rdma_slave_check_write_complete() == 1) {
                        g_repl_rdma_ctx.connected = 0;
                        continue;
                    }
                    int recv_slot = -1;
                    size_t rdma_blen = 0;
                    if (repl_rdma_wait_cq_recv_completion(100, &recv_slot, &rdma_blen) == 0
                        && recv_slot >= 0 && rdma_blen > 0) {
                        /* P3.4: 零复制 */
                        {
                            size_t dlen = rdma_blen;
                            unsigned char *direct = repl_rdma_recv_direct(recv_slot, &dlen);
                            if (direct) {
                                fprintf(stderr, "repl rdma: slave_debug_rdma - recv_slot=%d rdma_blen=%zu blen_before=%zu buf_size=%zu\n",
                                    recv_slot, dlen, blen, sizeof(buf));
                                if (blen + dlen <= sizeof(buf)) {
                                    memcpy(buf + blen, direct, dlen);
                                    blen += dlen;
                                    had_new_data = 1;
                                } else {
                                    fprintf(stderr, "repl rdma: slave_debug_rdma - BUFFER OVERFLOW! blen=%zu rdma_blen=%zu sizeof(buf)=%zu\n",
                                        blen, dlen, sizeof(buf));
                                }
                                repl_rdma_repost_recv(recv_slot);
                            }
                        }
                    }
                }
#endif

                /* Only parse when new data was added to avoid busy-loop
                 * on incomplete data. */
                if (had_new_data && blen > 0) {
                    size_t before = blen;
                    parse_resp_stream(NULL, buf, &blen, 1);
                    fprintf(stderr, "repl rdma: slave_debug_parse - before=%zu after=%zu consumed=%zu\n",
                        before, blen, before - blen);
                    repl_slave_ack_heartbeat();
                    repl_set_link_state(1);
                } else if (had_new_data) {
                    /* TCP had data but parse_resp_stream consumed it all */
                    repl_slave_ack_heartbeat();
                    repl_set_link_state(1);
                }
            }

            g_slave_fd = -1;
            repl_transport_ebpf_disconnect_slave(tcp_fd);
            g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
            repl_set_link_state(0);
            sleep(1);
            continue;
        }

        /* ---- Single transport path (backward compatible) ---- */
        int fd = repl_transport_ops()->connect_slave(host, port);
        if (fd < 0) {
            if (!strcasecmp(repl_transport_configured_name(), "rdma")) {
                repl_rdma_set_state(REPL_RDMA_STATE_BACKOFF, "connect_slave_failed");
            }
            if (!strcasecmp(repl_transport_name(), "rdma")) repl_rdma_log("slave_loop", "link down because connect_slave failed");
            repl_set_link_state(0);
            rdma_fail_streak++;
            repl_slave_retry_pause(rdma_fail_streak);
            continue;
        }

        if (!strcasecmp(repl_transport_name(), "rdma") ||
            !strcasecmp(repl_transport_name(), "rdma+kprobe")) {
            unsigned char cmd[256];
            unsigned char stream_buf[BUFFER_CAP * 4 + 4096];
            char offbuf[32];
            char durablebuf[32];
            size_t n;
            size_t blen;
            size_t stream_len = 0;
            /* One-Sided WRITE: check IMM */
            if (repl_rdma_slave_check_write_complete() == 1) break;
            int recv_slot;
            g_slave_transport_kind = KVS_REPL_TRANSPORT_RDMA;
            repl_rdma_set_state(REPL_RDMA_STATE_SYNCING, "slave_replsync_sent");
            snprintf(offbuf, sizeof(offbuf), "%llu", g_slave_repl_offset);
            snprintf(durablebuf, sizeof(durablebuf), "%llu", g_slave_repl_durable_offset);
            n = resp_build_cmd4(cmd, sizeof(cmd), "REPLSYNC", g_slave_master_replid[0] ? g_slave_master_replid : "?", offbuf, durablebuf);
            if (repl_transport_send_on_slave_link(cmd, n) != 0) {
                repl_rdma_log("slave_loop", "link down because initial REPLSYNC send failed");
                repl_transport_ops()->disconnect_slave(fd);
                repl_set_link_state(0);
                rdma_fail_streak++;
                repl_slave_retry_pause(rdma_fail_streak);
                continue;
            }
            rdma_fail_streak = 0;
            repl_transport_mark_active("rdma");
            repl_set_link_state(1);
            repl_rdma_log("slave_loop", "sent initial REPLSYNC over rdma");
            for (;;) {
                if (slave_should_reconnect(gen)) {
                    repl_rdma_log("slave_loop", "breaking for reconnect generation change");
                    break;
                }
                if (!g_repl_rdma_ctx.connected) {
                    repl_rdma_log("slave_loop", "breaking because rdma transport disconnected");
                    break;
                }
                recv_slot = -1;
                if (repl_rdma_wait_cq_recv_completion(2000, &recv_slot, &blen) == 0 && recv_slot >= 0 && blen > 0) {
                    /* P3.4: 零复制 */
                    {
                        size_t dlen = blen;
                        unsigned char *direct = repl_rdma_recv_direct(recv_slot, &dlen);
                        if (!direct) {
                            repl_rdma_log("slave_loop", "failed to get recv direct");
                            break;
                        }
                        if (stream_len + dlen > sizeof(stream_buf)) {
                            repl_rdma_repost_recv(recv_slot);
                            stream_len = 0;
                            repl_rdma_log("slave_loop", "stream buffer overflow while appending recv payload");
                            break;
                        }
#if KVS_ENABLE_RDMA
                        {
                            size_t preview_len = dlen < 96 ? dlen : 96;
                            fprintf(stderr, "repl rdma: slave_chunk - recv_len=%zu preview=%.*s\n", dlen, (int)preview_len, direct);
                        }
#endif
                        memcpy(stream_buf + stream_len, direct, dlen);
                        stream_len += dlen;
                        repl_rdma_repost_recv(recv_slot);
                    }
                    parse_resp_stream(NULL, stream_buf, &stream_len, 1);
                    repl_slave_ack_heartbeat();
                    repl_rdma_set_state(REPL_RDMA_STATE_STEADY, "processed_replication_chunk");
                    repl_rdma_log("slave_loop", "processed rdma response chunk");
                    repl_set_link_state(1);
                    continue;
                }
                if (!g_repl_rdma_ctx.connected) {
                    repl_rdma_log("slave_loop", "link down after recv wait because transport disconnected");
                    break;
                }
                repl_set_link_state(1);
            }
            /* 主动断开 RDMA 连接，触发重连 */
            g_repl_rdma_ctx.connected = 0;
            while (!slave_should_reconnect(gen) && g_repl_rdma_ctx.connected) {
                sleep(1);
                repl_set_link_state(1);
            }
            if (slave_should_reconnect(gen)) repl_rdma_log("slave_loop", "link down because reconnect was requested");
            else if (!g_repl_rdma_ctx.connected) repl_rdma_log("slave_loop", "link down because transport remained disconnected after recv loop");
            repl_transport_ops()->disconnect_slave(fd);
            g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
            repl_set_link_state(0);
            if (!g_repl_rdma_ctx.connected) rdma_fail_streak++;
            else rdma_fail_streak = 0;
            repl_slave_retry_pause(rdma_fail_streak);
            continue;
        }

        unsigned char cmd[256];
        char offbuf[32];
        char durablebuf[32];
        snprintf(offbuf, sizeof(offbuf), "%llu", g_slave_repl_offset);
        snprintf(durablebuf, sizeof(durablebuf), "%llu", g_slave_repl_durable_offset);
        size_t n = resp_build_cmd4(cmd, sizeof(cmd), "REPLSYNC", g_slave_master_replid[0] ? g_slave_master_replid : "?", offbuf, durablebuf);
        if (send(fd, cmd, n, 0) < 0) {
            repl_transport_mark_active("tcp");
            g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
            repl_transport_ops()->disconnect_slave(fd);
            repl_set_link_state(0);
            sleep(1);
            continue;
        }

        if (repl_should_use_ebpf_now()) {
            repl_transport_mark_active("ebpf");
            g_slave_transport_kind = KVS_REPL_TRANSPORT_EBPF;
        } else {
            repl_transport_mark_active("tcp");
            g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
        }
        g_slave_fd = fd;
        repl_set_link_state(1);
        unsigned char buf[BUFFER_CAP];
        size_t blen = 0;

        for (;;) {
            if (slave_should_reconnect(gen)) break;
            ssize_t r = recv(fd, buf + blen, sizeof(buf) - blen, 0);
            if (r > 0) {
                blen += (size_t)r;
                parse_resp_stream(NULL, buf, &blen, 1);
                repl_slave_ack_heartbeat();
                repl_set_link_state(1);
                continue;
            }
            if (r == 0) break;
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            break;
        }

        g_slave_fd = -1;
        repl_transport_ops()->disconnect_slave(fd);
        g_slave_transport_kind = KVS_REPL_TRANSPORT_TCP;
        repl_set_link_state(0);
        sleep(1);
    }
    return NULL;
}

int repl_handle_replica_send_failure(conn_t *c, conn_t **linkp) {
    if (!c || !linkp) return 0;
#if KVS_ENABLE_RDMA
    if (c == &g_rdma_master_replica_conn) {
        conn_t *next = c->next_replica;
        repl_remove_slave(c);
        repl_rdma_drop_master_replica_shallow(c);
        *linkp = next;
        {
            char last_stage[64];
            char last_preview[160];
            unsigned long long last_len = 0;
            unsigned long long last_offset = 0;
            repl_get_last_send_context(last_stage, sizeof(last_stage), &last_len, &last_offset, last_preview, sizeof(last_preview));
            fprintf(stderr, "repl rdma: broadcast_context - last_send_stage=%s last_send_len=%llu last_send_offset=%llu last_send_preview=%s\n",
                last_stage, last_len, last_offset, last_preview);
        }
        repl_rdma_log("broadcast", "dropping stale master rdma replica before reset");
        repl_rdma_reset_conn_ctx(1);
        repl_rdma_log("broadcast", "master rdma conn ctx reset complete");
        return 1;
    }
#endif
    return 0;
}

int start_slave_thread(void) {
    pthread_t tid;
    if (g_slave_thread_started) return 0;
    if (g_cfg.role == ROLE_SLAVE && g_cfg.master_host[0] && g_cfg.master_port > 0) {
        repl_slaveof(g_cfg.master_host, g_cfg.master_port);
    }
    if (pthread_create(&tid, NULL, slave_thread, NULL) != 0) return -1;
    pthread_detach(tid);
    g_slave_thread_started = 1;
    return 0;
}

#if KVS_ENABLE_RDMA
static void *rdma_master_listener_thread(void *arg) {
    (void)arg;
    struct sockaddr_in addr;
    struct rdma_cm_event *event = NULL;
int consecutive_failures = 0;
    for (;;) {
        long long accept_start_ms = 0;
        long long initial_recv_start_ms = 0;
        if (g_cfg.role != ROLE_MASTER || (!repl_should_use_rdma_now())) {
            repl_rdma_reset_ctx();
            consecutive_failures = 0;
            sleep(1);
            continue;
        }

        /* Helper: on any setup failure, increment counter and fallback after 10 */
        #define LISTENER_FAIL(step_name) do { \
            consecutive_failures++; \
            repl_rdma_log("listener", step_name " failed"); \
            if (consecutive_failures >= 10) { \
                fprintf(stderr, "repl rdma: listener - giving up after %d failures (%s), falling back to TCP for 10s\n", \
                    consecutive_failures, step_name); \
                repl_transport_trigger_fallback("rdma_listener_fail", 10000); \
                repl_rdma_reset_ctx(); \
                consecutive_failures = 0; \
                sleep(3); \
            } else { \
                repl_rdma_reset_conn_ctx(1); \
                usleep(500000); \
            } \
            continue; \
        } while(0)

        if (!g_repl_rdma_ctx.ec) {
            for (int ec_retry = 0; ec_retry < 5; ec_retry++) {
                g_repl_rdma_ctx.ec = rdma_create_event_channel();
                if (g_repl_rdma_ctx.ec) break;
                char ec_err[128];
                snprintf(ec_err, sizeof(ec_err), "create failed errno=%d retry=%d", errno, ec_retry);
                repl_rdma_log("listener", ec_err);
                if (ec_retry < 4) usleep(200000);
            }
            if (!g_repl_rdma_ctx.ec) {
                LISTENER_FAIL("event channel create");
            }
            consecutive_failures = 0;
        }
        if (!g_repl_rdma_ctx.listen_id) {
            repl_rdma_drop_master_replica_shallow(&g_rdma_master_replica_conn);
            if (rdma_create_id(g_repl_rdma_ctx.ec, &g_repl_rdma_ctx.listen_id, NULL, RDMA_PS_TCP) != 0) {
                LISTENER_FAIL("create listen id");
            }
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            {
                int rdma_port = g_cfg.rdma_port > 0 ? g_cfg.rdma_port : g_cfg.port + 1;
                addr.sin_port = htons((uint16_t)rdma_port);
            }
            if (inet_pton(AF_INET, g_cfg.master_host[0] ? g_cfg.master_host : "0.0.0.0", &addr.sin_addr) <= 0) addr.sin_addr.s_addr = htonl(INADDR_ANY);
            if (rdma_bind_addr(g_repl_rdma_ctx.listen_id, (struct sockaddr *)&addr) != 0) {
                LISTENER_FAIL("bind");
            }
            if (rdma_listen(g_repl_rdma_ctx.listen_id, 4) != 0) {
                /* Destroy failed listen_id before retry; preserve ec */
                rdma_destroy_id(g_repl_rdma_ctx.listen_id);
                g_repl_rdma_ctx.listen_id = NULL;
                LISTENER_FAIL("listen");
            }
            repl_rdma_log("listener", "listening");
            consecutive_failures = 0;
        }
        #undef LISTENER_FAIL

        if (rdma_get_cm_event(g_repl_rdma_ctx.ec, &event) != 0) {
            repl_rdma_log("listener", "get_cm_event failed");
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            fprintf(stderr, "repl rdma: listener unexpected event - got=%s expect=%s\n", rdma_event_str(event->event), rdma_event_str(RDMA_CM_EVENT_CONNECT_REQUEST));
            rdma_ack_cm_event(event);
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        consecutive_failures = 0;
        repl_rdma_log("listener", "connect request received");
        g_repl_rdma_ctx.accepted_id = event->id;
        rdma_ack_cm_event(event);
        g_repl_rdma_ctx.id = g_repl_rdma_ctx.accepted_id;
        g_repl_rdma_ctx.comp_chan = ibv_create_comp_channel(g_repl_rdma_ctx.id->verbs);
        if (!g_repl_rdma_ctx.comp_chan) {
            repl_rdma_log("listener", "comp channel create failed");
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        g_repl_rdma_ctx.pd = ibv_alloc_pd(g_repl_rdma_ctx.id->verbs);
        if (!g_repl_rdma_ctx.pd) {
            repl_rdma_log("listener", "alloc pd failed");
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        repl_rdma_refresh_runtime_cfg();
        g_repl_rdma_ctx.cq = ibv_create_cq(g_repl_rdma_ctx.id->verbs, g_repl_rdma_ctx.active_qp_wr_depth, NULL, g_repl_rdma_ctx.comp_chan, 0);
        if (!g_repl_rdma_ctx.cq) {
            repl_rdma_log("listener", "create cq failed");
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        if (repl_rdma_create_qp() != 0 || repl_rdma_prepare_buffers() != 0 || repl_rdma_post_initial_recv() != 0) {
            repl_rdma_log("listener", "resource prepare failed");
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        accept_start_ms = kvs_now_ms();
        if (rdma_accept(g_repl_rdma_ctx.id, NULL) != 0) {
            repl_rdma_log("listener", "rdma_accept failed");
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        repl_rdma_log("listener", "accept issued");
        if (repl_rdma_wait_event(RDMA_CM_EVENT_ESTABLISHED, 5000) != 0) {
            fprintf(stderr, "repl rdma: listener_established_wait_fail - elapsed_ms=%lld\n", kvs_now_ms() - accept_start_ms);
            repl_rdma_reset_ctx();
            sleep(1);
            continue;
        }
        fprintf(stderr, "repl rdma: listener_established_ok - elapsed_ms=%lld\n", kvs_now_ms() - accept_start_ms);
        g_repl_rdma_ctx.connected = 1;
        repl_rdma_log("listener", "established");
        /* 启动 CQ 轮询线程（pipeline 模式） */
        if (g_repl_rdma_ctx.send_pipeline_enabled) {
            repl_rdma_start_cq_poll_thread();
        }
        {
                /* One-Sided WRITE: check IMM */
                    if (repl_rdma_slave_check_write_complete() == 1) break;
                    int recv_slot = -1;
            size_t recv_len = 0;
            unsigned char stream_buf[BUFFER_CAP * 4 + 4096];
            size_t stream_len = 0;
            memset(&g_rdma_master_replica_conn, 0, sizeof(g_rdma_master_replica_conn));
            g_rdma_master_replica_conn.repl_transport_kind = KVS_REPL_TRANSPORT_RDMA;
            initial_recv_start_ms = kvs_now_ms();

            /* In hybrid mode (RDMA fullsync + eBPF realtime), the main thread
             * sends fullsync data over RDMA and polls the shared CQ for send
             * completions. The listener must NOT compete for CQ entries;
             * it just waits for the connection to close. */
            int hybrid_mode = !strcasecmp(repl_fullsync_transport_name(), "rdma")
                           && repl_realtime_should_use_ebpf();

            for (;;) {
                if (!g_repl_rdma_ctx.connected) break;

                if (hybrid_mode) {
                    /* Hybrid: let main thread own the CQ; just sleep and
                     * periodically check connection status. */
                    sleep(1);
                    continue;
                }

                recv_slot = -1;
                recv_len = 0;
                if (repl_rdma_wait_cq_recv_completion(2000, &recv_slot, &recv_len) != 0 || recv_slot < 0 || recv_len == 0) {
                    continue;
                }
                /* P3.4: 零复制 */
                {
                    size_t dlen = recv_len;
                    unsigned char *direct = repl_rdma_recv_direct(recv_slot, &dlen);
                    if (!direct) {
                        repl_rdma_log("listener", "failed to get recv direct");
                        break;
                    }
                    if (stream_len + dlen > sizeof(stream_buf)) {
                        repl_rdma_repost_recv(recv_slot);
                        stream_len = 0;
                        repl_rdma_log("listener", "stream buffer overflow while appending recv payload");
                        break;
                    }
                    memcpy(stream_buf + stream_len, direct, dlen);
                    stream_len += dlen;
                    repl_rdma_repost_recv(recv_slot);
                }
                parse_resp_stream(&g_rdma_master_replica_conn, stream_buf, &stream_len, 0);
                if (initial_recv_start_ms != 0) {
                    fprintf(stderr, "repl rdma: listener_initial_payload_ok - elapsed_ms=%lld recv_len=%zu\n", kvs_now_ms() - initial_recv_start_ms, recv_len);
                    repl_rdma_log("listener", "processed initial rdma payload");
                    initial_recv_start_ms = 0;
                }
            }
            if (initial_recv_start_ms != 0) {
                fprintf(stderr, "repl rdma: listener_initial_payload_fail - elapsed_ms=%lld recv_slot=%d recv_len=%zu\n", kvs_now_ms() - initial_recv_start_ms, recv_slot, recv_len);
                repl_rdma_log("listener", "no initial rdma payload received");
            }
        }
        repl_rdma_log("listener", g_repl_rdma_ctx.connected ? "listener loop exiting while still marked connected" : "listener loop exiting after disconnect");
        /* cleanup 由主线程的 failover 逻辑处理，listener 线程不直接重置
         * 避免线程间 RDMA 资源竞态导致的 segfault */
    }
    return NULL;
}
#endif

int start_rdma_master_listener(void) {
#if KVS_ENABLE_RDMA
    pthread_t tid;
    const char *fullsync_t = repl_fullsync_transport_name();
    if (g_cfg.role != ROLE_MASTER) return 0;
    if (strcasecmp(fullsync_t, "rdma") != 0 && strcasecmp(g_cfg.repl_transport_backend, "rdma") != 0) return 0;
    if (g_rdma_master_listener_started) return 0;
    if (pthread_create(&tid, NULL, rdma_master_listener_thread, NULL) != 0) return -1;
    pthread_detach(tid);
    g_rdma_master_listener_started = 1;
#endif
    return 0;
}

#if KVS_ENABLE_RDMA
/**
 * repl_rdma_start_fullsync — 全量同步开始时按需启动 RDMA
 *
 * Master 侧: 创建 event channel → bind/listen → 等待 slave RDMA 连接
 *           → accept → 建立 QP/buffers → 等待 ESTABLISHED
 *
 * 返回: 0 成功, -1 失败（调用方应回退到 TCP）
 *
 * 注意: 此函数同步阻塞直到 RDMA 连接建立或超时（10s）
 */
int repl_rdma_start_fullsync(conn_t *c) {
    struct sockaddr_in addr;
    struct rdma_cm_event *event = NULL;
long long deadline = kvs_now_ms() + 10000;  /* 10s 超时 */
    (void)c;
    repl_rdma_raise_memlock();

    /* 如果已连接，直接返回成功 */
    if (g_repl_rdma_ctx.connected) return 0;

    /* 重置残留状态 */
    repl_rdma_reset_ctx();

    repl_rdma_log("fullsync_start", "begin");

    /* 创建 event channel */
    g_repl_rdma_ctx.ec = rdma_create_event_channel();
    if (!g_repl_rdma_ctx.ec) {
        repl_rdma_log("fullsync_start", "event channel create failed");
        return -1;
    }

    /* Master 侧: 创建 listener */
    if (rdma_create_id(g_repl_rdma_ctx.ec, &g_repl_rdma_ctx.listen_id,
                       NULL, RDMA_PS_TCP) != 0) {
        repl_rdma_log("fullsync_start", "create listen id failed");
        repl_rdma_reset_ctx();
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    {
        int rdma_port = g_cfg.rdma_port > 0 ? g_cfg.rdma_port : g_cfg.port + 1;
        addr.sin_port = htons((uint16_t)rdma_port);
    }
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (rdma_bind_addr(g_repl_rdma_ctx.listen_id,
                       (struct sockaddr *)&addr) != 0) {
        fprintf(stderr, "repl rdma: fullsync_start rdma_bind failed errno=%d(%s) port=%d\n",
                errno, strerror(errno), ntohs(addr.sin_port));
        repl_rdma_reset_ctx();
        return -1;
    }

    if (rdma_listen(g_repl_rdma_ctx.listen_id, 4) != 0) {
        fprintf(stderr, "repl rdma: fullsync_start rdma_listen failed errno=%d(%s) port=%d\n",
                errno, strerror(errno), ntohs(addr.sin_port));
        repl_rdma_reset_ctx();
        return -1;
    }

    repl_rdma_log("fullsync_start", "listening for slave RDMA connect");
    fprintf(stderr, "repl rdma: fullsync_start poll - ec=%p ec_fd=%d listen_id=%p\n",
            (void *)g_repl_rdma_ctx.ec,
            g_repl_rdma_ctx.ec ? g_repl_rdma_ctx.ec->fd : -1,
            (void *)g_repl_rdma_ctx.listen_id);

    /* 等待 slave 的 CONNECT_REQUEST（超时 10s） */
    {
        struct pollfd pfd;
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = g_repl_rdma_ctx.ec->fd;
        pfd.events = POLLIN;

        while (kvs_now_ms() < deadline) {
            if (poll(&pfd, 1, 1000) <= 0) {
                if (kvs_now_ms() >= deadline) {
                    repl_rdma_log("fullsync_start", "timeout waiting for CONNECT_REQUEST");
                    repl_rdma_reset_ctx();
                    return -1;
                }
                continue;
            }
            if (rdma_get_cm_event(g_repl_rdma_ctx.ec, &event) != 0) {
                repl_rdma_log("fullsync_start", "get_cm_event failed");
                repl_rdma_reset_ctx();
                return -1;
            }
            break;
        }

        if (!event) {
            repl_rdma_log("fullsync_start", "no event received");
            repl_rdma_reset_ctx();
            return -1;
        }

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            repl_rdma_log("fullsync_start", "unexpected event");
            rdma_ack_cm_event(event);
            repl_rdma_reset_ctx();
            return -1;
        }
    }

    /* 接受连接 */
    repl_rdma_log("fullsync_start", "connect request received");
    g_repl_rdma_ctx.accepted_id = event->id;
    rdma_ack_cm_event(event);
    g_repl_rdma_ctx.id = g_repl_rdma_ctx.accepted_id;

    /* 创建 PD / CQ / QP / buffers */
    g_repl_rdma_ctx.comp_chan = ibv_create_comp_channel(
        g_repl_rdma_ctx.id->verbs);
    if (!g_repl_rdma_ctx.comp_chan) {
        repl_rdma_log("fullsync_start", "comp channel create failed");
        repl_rdma_reset_ctx();
        return -1;
    }

    g_repl_rdma_ctx.pd = ibv_alloc_pd(g_repl_rdma_ctx.id->verbs);
    if (!g_repl_rdma_ctx.pd) {
        repl_rdma_log("fullsync_start", "alloc pd failed");
        repl_rdma_reset_ctx();
        return -1;
    }

    repl_rdma_refresh_runtime_cfg();
    g_repl_rdma_ctx.cq = ibv_create_cq(g_repl_rdma_ctx.id->verbs,
        g_repl_rdma_ctx.active_qp_wr_depth, NULL,
        g_repl_rdma_ctx.comp_chan, 0);
    if (!g_repl_rdma_ctx.cq) {
        repl_rdma_log("fullsync_start", "create cq failed");
        repl_rdma_reset_ctx();
        return -1;
    }

    if (repl_rdma_create_qp() != 0 ||
        repl_rdma_prepare_buffers() != 0 ||
        repl_rdma_post_initial_recv() != 0) {
        repl_rdma_log("fullsync_start", "resource prepare failed");
        repl_rdma_reset_ctx();
        return -1;
    }

    /* rdma_accept → 等待 ESTABLISHED (NULL = 使用默认 conn_param) */
    if (rdma_accept(g_repl_rdma_ctx.id, NULL) != 0) {
        repl_rdma_log("fullsync_start", "rdma_accept failed");
        repl_rdma_reset_ctx();
        return -1;
    }

    repl_rdma_log("fullsync_start", "accept issued, waiting for ESTABLISHED");

    {
        struct pollfd pfd;
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = g_repl_rdma_ctx.ec->fd;
        pfd.events = POLLIN;

        while (kvs_now_ms() < deadline) {
            if (poll(&pfd, 1, 1000) <= 0) continue;
            if (rdma_get_cm_event(g_repl_rdma_ctx.ec, &event) != 0) break;
            if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
                rdma_ack_cm_event(event);
                g_repl_rdma_ctx.connected = 1;
                repl_rdma_set_state(REPL_RDMA_STATE_ESTABLISHED,
                                    "fullsync_established");
                repl_rdma_log("fullsync_start", "established OK");

                /* 启动 CQ 轮询线程 */
                if (g_repl_rdma_ctx.send_pipeline_enabled) {
                    repl_rdma_start_cq_poll_thread();
                }

                /* 设置 master replica conn */
                memset(&g_rdma_master_replica_conn, 0,
                       sizeof(g_rdma_master_replica_conn));
                g_rdma_master_replica_conn.repl_transport_kind =
                    KVS_REPL_TRANSPORT_RDMA;

                repl_rdma_log("fullsync_start", "RDMA started successfully");
                return 0;
            }
            rdma_ack_cm_event(event);
        }
    }

    repl_rdma_log("fullsync_start", "establish timed out");
    repl_rdma_reset_ctx();
    return -1;
}

/**
 * repl_rdma_stop_fullsync — 全量同步完成后关闭并释放所有 RDMA 资源
 */
void repl_rdma_stop_fullsync(void) {
    if (!g_repl_rdma_ctx.connected &&
        !g_repl_rdma_ctx.listen_id &&
        !g_repl_rdma_ctx.id) {
        return;  /* 没有活跃的 RDMA 连接 */
    }

    repl_rdma_log("fullsync_stop", "stopping RDMA");

    /* 标记断开但不销毁资源 — rdma_disconnect 会触发 WR_FLUSH_ERR
     * 导致 siw 驱动中正在传输的数据丢失。全量数据已通过 RDMA 发完，
     * slave 在收到 REPLDONE 后自行处理。资源在下次 fullsync 时复用
     * 或进程退出时释放。 */
    g_repl_rdma_ctx.connected = 0;

    /* 停止 CQ 轮询线程 */
    repl_rdma_stop_cq_poll_thread();

    repl_rdma_log("fullsync_stop", "RDMA stopped");
}
#endif
