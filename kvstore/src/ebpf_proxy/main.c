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

/* ---- 跨线程安全 ----
 * g_state 用 g_state_lock 保护（主线程 set_state 写、转发线程 proxy_fwd_send_one
 * 持锁校验 FORWARDING 并 writev，原子于 BUFFERING flip）。
 * g_cache 用 g_cache_lock 保护。slave 连接状态 fd 由 proxy_slave.c 内部锁
 * （g_slave_lock）成对保护：主线程 connect/disconnect 写、转发线程经
 * proxy_slave_writev 读取并 writev，故无未同步的跨线程 fd 读写。 */
static pthread_mutex_t g_state_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_cache_lock = PTHREAD_MUTEX_INITIALIZER;

/* 主线程发布路由状态（写 g_state 必须在锁内） */
static void set_state(proxy_state_t s) {
    pthread_mutex_lock(&g_state_lock);
    g_state = s;
    pthread_mutex_unlock(&g_state_lock);
}

/* 加锁的缓存入队/刷出 */
static int cache_enq_wrap(const void *data, size_t len) {
    pthread_mutex_lock(&g_cache_lock);
    int rc = cache_append(&g_cache, (const unsigned char *)data, len);
    pthread_mutex_unlock(&g_cache_lock);
    return rc;
}
static int cache_flush_wrap(int fd) {
    pthread_mutex_lock(&g_cache_lock);
    int rc = cache_flush(&g_cache, fd);
    pthread_mutex_unlock(&g_cache_lock);
    return rc;
}

/* ---- 转发队列 + 独立转发线程 ----
 * 主线程读 ringbuf → proxy_fwd_enqueue 入队；
 * 转发线程出队 → writev 到 slave（阻塞 OK，慢 slave 只挡转发线程）。
 * 按字节限长 64MB，满时主线程在 enqueue 处等待（背压）。 */
typedef struct proxy_fwd_node_s {
    unsigned char *buf; size_t len;
    struct proxy_fwd_node_s *next;
} proxy_fwd_node_t;

static pthread_mutex_t g_pfwd_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_pfwd_cond = PTHREAD_COND_INITIALIZER;
static proxy_fwd_node_t *g_pfwd_head = NULL, *g_pfwd_tail = NULL;
static size_t g_pfwd_bytes = 0;
static int g_pfwd_stop = 0;
/* cache flush 请求：主线程置位唤醒 fwd 线程刷 cache。fwd 线程是 slave fd 的唯一 writer，
 * 主线程不再直接写 slave fd（消除了 fwd writev 与主线程 cache_flush 的并发写竞态）。 */
static int g_cache_flush_pending = 0;
static pthread_t g_pfwd_thread = 0;
#define MAX_PFWD_QUEUE_BYTES (64 * 1024 * 1024)
/* 超大 payload 阈值：等价替换原 BATCH_BUF_SZ 限制，> 此值直接进 cache */
/* 单 recv 条目超此阈值走 heap cache 慢路径。原 8200 按 P=1 单命令（~124B）设计；
 * 高 P 下一条 recv 是批量（P=160 时 ~20KB）>8200 全被误判走慢 cache，转发跟不上。
 * 提到 32768 覆盖 capture 最大条目（CLIENT_ENTRY_MAX_LEN=32764+4），让高 P 走快队列。 */
#define PFWD_LARGE_SZ 32768
#define PFWD_BATCH_MAX 256        /* 转发线程每次 writev 最多节点数 */
#define PFWD_BATCH_BYTES_MAX (512u * 1024)   /* 单批最多 512KB（≤ slave 1MB 发送缓冲，防 EAGAIN） */
#define PFWD_LINGER_US 200        /* 空队列等待上限（µs）：单条命令有界延迟，免每命令 futex_wake */

/* ---- 双信号背压 ----
 * client_ctl[4] = ringbuf 背压：BPF 生产端高水位置位，proxy 消费端（ringbuf 排空后）清除。
 * client_ctl[6] = 转发队列背压：proxy 入队端高水位置位，出队端低水位清除。
 * 拆成两个 key 避免 BPF/入队/出队三方写同一值互相覆盖（ringbuf 恢复但队列仍高时
 * 不能清除，反之亦然）。master 侧读两者取或。 */
#define CLIENT_CTL_KEY_BACKPRESSURE     4   /* ringbuf 背压 */
#define CLIENT_CTL_KEY_FWDQ_BACKPRESSURE 6  /* 转发队列背压 */
#define CLIENT_CTL_KEY_HEARTBEAT         5
/* 转发队列高水位 4MB：master 生产远超跨机转发，队列快速填满会阻塞 proxy 主线程、
 * ringbuf 停止排空而溢出。低水位提前触发背压、留足余量（4MB→64MB 有 60MB 余量）。 */
#define PFWD_BACKPRESSURE_HIGH      (4u * 1024 * 1024)
#define PFWD_BACKPRESSURE_LOW       (1u * 1024 * 1024)
static __u64 g_fwdq_bp_state = 0;   /* 最近写入 client_ctl[6] 的值 */
static int write_client_ctl_u64(__u32 key, __u64 val);   /* 定义在本文件后部 */
/* ringbuf 占用（m[513] = consumer_pos，用于 proxy 消费端清除 client_ctl[4]） */
static void *g_rb_meta = MAP_FAILED;
#define RB_BACKPRESSURE_LOW  (8u * 1024 * 1024)   /* ringbuf 排空到 8MB 以下即清背压 */
/* 诊断计数（原子累加，由诊断线程每秒打印，不进热路径 stderr） */
static unsigned long long g_enq_bytes = 0;
static unsigned long long g_sent_bytes = 0;
static unsigned long long g_fwdq_bp_count = 0;
static unsigned long long g_rb_bp_count = 0;
static unsigned long long g_inflight_bytes = 0;   /* 已出队未发完的字节（原子，诊断/背压用） */
static unsigned long long g_writev_calls = 0;     /* writev 成功调用次数 */
static unsigned long long g_eagain_count = 0;     /* EAGAIN 次数 */
static unsigned long long g_poll_count = 0;       /* poll 调用次数 */
static unsigned long long g_poll_timeout = 0;     /* poll 超时次数 */
static unsigned long long g_poll_out = 0;         /* poll 返回 POLLOUT 次数 */
static unsigned long long g_poll_err = 0;         /* poll 返回 POLLERR/POLLHUP/POLLNVAL */
static unsigned long long g_partial_write = 0;    /* 部分写（0<n<请求）次数 */
/* mmap ringbuf 头页（2 页）。必须用 BPF 对象内 fd（BPF 实际写入的实例），且在
 * ring_buffer__new 之前调用（fd 被其消费后 mmap 可能失败）。 */
static void rb_meta_init(int rb_fd) {
    long page = sysconf(_SC_PAGESIZE);
    g_rb_meta = mmap(NULL, (size_t)page * 2, PROT_READ, MAP_SHARED, rb_fd, 0);
    if (g_rb_meta == MAP_FAILED) {
        fprintf(stderr, "ebpf-proxy: mmap ringbuf meta failed: %s\n", strerror(errno));
        g_rb_meta = NULL;
    }
}

/* ringbuf 当前占用（producer_pos - consumer_pos）。consumer_pos 在页1偏移8（m[513]）：
 * 实测（kernel 6.1）页0/页1 都有 producer_pos@0，consumer_pos 只在页1@8（libbpf 推进）。 */
static size_t ringbuf_occupancy(void) {
    if (!g_rb_meta || g_rb_meta == MAP_FAILED) return 0;
    const __u64 *m = (const __u64 *)g_rb_meta;
    __u64 prod = m[0];
    __u64 cons = m[513];
    return prod > cons ? (size_t)(prod - cons) : 0;
}

/* 压力指标 = 队列字节 + 在途字节（已出队未发完）。仅看 g_pfwd_bytes 会在批次出队时过早
 * 判定"已排空"而清背压，但数据仍在 writev 发送中——导致 P=160 反复"过早恢复生产→再填满"。
 * 须持 g_pfwd_lock（g_pfwd_bytes 由持锁者维护）；g_inflight_bytes 原子读。 */
static unsigned long long fwdq_pending(void) {
    return g_pfwd_bytes + __atomic_load_n(&g_inflight_bytes, __ATOMIC_RELAXED);
}
/* 转发队列背压：入队后超高水位置 client_ctl[6]（须持 g_pfwd_lock，状态变化才写 map）。
 * 必须在入队后立即置位，不能等 ring_buffer__consume 整轮返回——一次 consume 可让队列
 * 从 0 涨到几十 MB，若等 consume 完才置位，队列先到 64MB、enqueue 阻塞在回调内、
 * consume 不返回、背压永不置位 → ringbuf 溢出。 */
static void fwdq_set_backpressure_locked(void) {
    if (g_fwdq_bp_state == 0 && fwdq_pending() > PFWD_BACKPRESSURE_HIGH) {
        g_fwdq_bp_state = 1;
        g_fwdq_bp_count++;
        write_client_ctl_u64(CLIENT_CTL_KEY_FWDQ_BACKPRESSURE, 1);
    }
}
/* 转发队列背压：pending 低于低水位才清 client_ctl[6]（须持 g_pfwd_lock） */
static void fwdq_clear_backpressure_locked(void) {
    if (g_fwdq_bp_state == 1 && fwdq_pending() < PFWD_BACKPRESSURE_LOW) {
        g_fwdq_bp_state = 0;
        write_client_ctl_u64(CLIENT_CTL_KEY_FWDQ_BACKPRESSURE, 0);
    }
}

/* 请求 fwd 线程刷新缓存（唤醒 + 置位）。fwd 线程是唯一 writer，主线程不直接写 slave fd。 */
static void request_cache_flush(void) {
    pthread_mutex_lock(&g_pfwd_lock);
    g_cache_flush_pending = 1;
    pthread_cond_broadcast(&g_pfwd_cond);
    pthread_mutex_unlock(&g_pfwd_lock);
}

/* 拷贝 payload 入队（payload 指针只在 ringbuf 回调期间有效） */
static void proxy_fwd_enqueue(const unsigned char *buf, size_t len) {
    pthread_mutex_lock(&g_pfwd_lock);
    while (g_pfwd_bytes + len > MAX_PFWD_QUEUE_BYTES && !g_pfwd_stop)
        pthread_cond_wait(&g_pfwd_cond, &g_pfwd_lock);
    if (g_pfwd_stop) { pthread_mutex_unlock(&g_pfwd_lock); return; }
    proxy_fwd_node_t *n = (proxy_fwd_node_t *)malloc(sizeof(*n) + len);
    if (!n) { pthread_mutex_unlock(&g_pfwd_lock); return; }
    n->buf = (unsigned char *)(n + 1); n->len = len;
    memcpy(n->buf, buf, len); n->next = NULL;
    if (g_pfwd_tail) g_pfwd_tail->next = n; else g_pfwd_head = n;
    g_pfwd_tail = n; g_pfwd_bytes += len;
    __atomic_fetch_add(&g_enq_bytes, len, __ATOMIC_RELAXED);
    fwdq_set_backpressure_locked();   /* 入队后立即按队列长度置位 */
    /* 无 cond_signal：转发线程靠 PFWD_LINGER_US timed-wait 自醒（免每命令 futex_wake，
     * 单条命令有界延迟 ≤PFWD_LINGER_US）。 */
    pthread_mutex_unlock(&g_pfwd_lock);
}

/* 转发线程处理一批节点：持 g_state_lock 校验 FORWARDING 后批量 writev（原子于 BUFFERING flip）。
 * 批内节点要么整体在 FORWARDING 下送、要么整体进 cache，不穿插全量流。
 * 注意：writev 部分写（w>=0 且 <total）按既有语义视为已送（slave 健康+SO_SNDTIMEO 下全写），
 * 与原逐节点版本一致。 */
static void proxy_fwd_send_batch(proxy_fwd_node_t **batch, int nbatch) {
    struct iovec iov[PFWD_BATCH_MAX];
    size_t total = 0;
    for (int i = 0; i < nbatch; i++) {
        iov[i].iov_base = batch[i]->buf;
        iov[i].iov_len = batch[i]->len;
        total += batch[i]->len;
    }
    pthread_mutex_lock(&g_state_lock);
    if (g_state == STATE_FORWARDING) {
        __atomic_fetch_add(&g_inflight_bytes, total, __ATOMIC_RELAXED);
        /* 循环 writev 处理部分写：单批最多 ~5MB（P=160 时 256 条 × 20KB），阻塞 socket
         * 的一次 writev 很可能 0<w<total（既有代码把部分写当成功、直接 free 节点，未发送
         * 尾部永久丢失——P=160 稳定丢 ~30% 的主因）。循环重发直到全部发送或出错。
         * 逐次 writev(n>0) 立即累计 sent 字节（非整批后），并扣减 in_flight。 */
        int idx = 0;
        size_t sent = 0;
        int gave_up = 0;
        while (idx < nbatch && sent < total) {
            ssize_t w = proxy_slave_writev(&g_slave, &iov[idx], nbatch - idx);
            if (w < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    g_eagain_count++;
                    /* 发送缓冲满（slave 暂缓）：等 socket 可写再重试，保持流顺序。
                     * 区分 POLLOUT 与 POLLERR/POLLHUP/POLLNVAL——后者是 fd 异常，必须放弃
                     * 回退 cache，不能继续无意义重试。 */
                    for (;;) {
                        int sfd = proxy_slave_fd(&g_slave);
                        if (sfd < 0) { gave_up = 1; break; }
                        struct pollfd pfd = {.fd = sfd, .events = POLLOUT};
                        g_poll_count++;
                        int pr = poll(&pfd, 1, 1000);
                        if (pr < 0) {
                            if (errno == EINTR) continue;
                            gave_up = 1; break;
                        }
                        if (pr == 0) { g_poll_timeout++; continue; }
                        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
                            g_poll_err++; gave_up = 1; break;
                        }
                        g_poll_out++;
                        break;   /* 可写，重试 writev */
                    }
                    if (!gave_up) continue;
                    break;
                }
                gave_up = 1;
                break;   /* 真错误：未发送部分回退 cache */
            }
            if (w == 0) {
                /* 理论不应发生（阻塞 socket 写 0 字节视为对端关闭），按错误处理 */
                gave_up = 1;
                break;
            }
            g_writev_calls++;
            sent += (size_t)w;
            __atomic_fetch_add(&g_sent_bytes, (unsigned long long)w, __ATOMIC_RELAXED);
            __atomic_fetch_sub(&g_inflight_bytes, (unsigned long long)w, __ATOMIC_RELAXED);
            ssize_t left = w;
            while (idx < nbatch && left >= (ssize_t)iov[idx].iov_len) {
                left -= iov[idx].iov_len;
                idx++;
            }
            if (idx < nbatch && left > 0) {
                iov[idx].iov_base = (char *)iov[idx].iov_base + left;
                iov[idx].iov_len -= (size_t)left;
                g_partial_write++;
            }
        }
        pthread_mutex_unlock(&g_state_lock);
        __atomic_fetch_sub(&g_inflight_bytes, (unsigned long long)(total - sent), __ATOMIC_RELAXED);
        if (sent < total && gave_up) {
            /* 真失败（非瞬态 EAGAIN）才回退 cache：当前节点从未发送起点起，后续整块 */
            fprintf(stderr, "ebpf-proxy: writev fail sent=%zu total=%zu errno=%d, "
                    "requeue %d nodes to cache\n",
                    sent, total, errno, nbatch - idx);
            if (idx < nbatch)
                cache_enq_wrap(iov[idx].iov_base, iov[idx].iov_len);
            for (int i = idx + 1; i < nbatch; i++)
                cache_enq_wrap(batch[i]->buf, batch[i]->len);
            /* 回退的数据必须在下一次发队列新数据前刷出（保持顺序），请求 flush */
            request_cache_flush();
        }
    } else {
        pthread_mutex_unlock(&g_state_lock);
        for (int i = 0; i < nbatch; i++) cache_enq_wrap(batch[i]->buf, batch[i]->len);
    }
}

static void *proxy_fwd_thread_main(void *arg) {
    (void)arg;
    proxy_fwd_node_t *batch[PFWD_BATCH_MAX];
    for (;;) {   /* stop 后仍继续，直到队列排空 + cache 刷完才退出（防关闭丢数据） */
        int nbatch = 0;
        int flush_pending = 0;
        pthread_mutex_lock(&g_pfwd_lock);
        while (!g_pfwd_head && !g_cache_flush_pending && !g_pfwd_stop) {
            /* 有界等待：空队列时短暂休眠让 ringbuf 回调攒批（linger），
             * 免每命令 futex_wake；单条命令最多等 PFWD_LINGER_US 即下送。 */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += (long)PFWD_LINGER_US * 1000L;
            if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
            pthread_cond_timedwait(&g_pfwd_cond, &g_pfwd_lock, &ts);
        }
        flush_pending = g_cache_flush_pending;
        g_cache_flush_pending = 0;
        /* 攒批：同时按节点数(PFWD_BATCH_MAX)与字节数(PFWD_BATCH_BYTES_MAX)上限。
         * 单批不能超过 slave 发送缓冲（1MB）——P=160 时 256×20KB=5MB 的批次会把
         * 1MB 缓冲瞬间撑满触发 EAGAIN，转发线程 poll 等 2s 后回退 cache，吞吐被卡死。 */
        size_t batch_bytes = 0;
        while (nbatch < PFWD_BATCH_MAX && g_pfwd_head &&
               batch_bytes + g_pfwd_head->len <= PFWD_BATCH_BYTES_MAX) {
            proxy_fwd_node_t *n = g_pfwd_head;
            g_pfwd_head = n->next;
            if (!g_pfwd_head) g_pfwd_tail = NULL;
            g_pfwd_bytes -= n->len;
            batch_bytes += n->len;
            batch[nbatch++] = n;
        }
        /* 出队后唤醒被 64MB 满队列背压阻塞的入队者（原代码缺此唤醒，属修复） */
        if (nbatch > 0) {
            fwdq_clear_backpressure_locked();   /* 出队后按队列长度清背压 */
            pthread_cond_broadcast(&g_pfwd_cond);
        }
        pthread_mutex_unlock(&g_pfwd_lock);

        /* fwd 线程是 slave fd 的唯一 writer：有 cache 待刷（REPLDONE/全量结束/回退/重连）
         * 时先刷 cache 再发队列，保证 pre/回退数据先于队列 post。主线程不再直接写 slave fd，
         * 消除并发写竞态（原 cache_flush_wrap 在主线程 send_full，与 fwd writev 抢同一 socket）。 */
        if (flush_pending) {
            int forwarding = 0;
            pthread_mutex_lock(&g_state_lock);
            forwarding = (g_state == STATE_FORWARDING);
            pthread_mutex_unlock(&g_state_lock);
            if (forwarding && proxy_slave_is_connected(&g_slave)) {
                int fd = proxy_slave_fd(&g_slave);
                int sent = cache_flush_wrap(fd);
                if (sent > 0)
                    fprintf(stderr, "ebpf-proxy: cache flushed (%d items) [fwd]\n", sent);
                /* 刷不完（fd 故障中断）：非停止态时保留 pending 重试（重连后 main_loop 会再请求）；
                 * 停止态不再重试，剩余 cache 由 cleanup 的 cache_destroy 释放。 */
                pthread_mutex_lock(&g_cache_lock);
                int leftover = (g_cache.head != NULL);
                pthread_mutex_unlock(&g_cache_lock);
                if (leftover) {
                    pthread_mutex_lock(&g_pfwd_lock);
                    int stopping = g_pfwd_stop;
                    pthread_mutex_unlock(&g_pfwd_lock);
                    if (!stopping) request_cache_flush();
                }
            }
        }

        if (nbatch == 0 && !flush_pending && g_pfwd_stop) break;   /* stop 且无工作 */
        if (nbatch > 0) {
            proxy_fwd_send_batch(batch, nbatch);
            for (int i = 0; i < nbatch; i++) free(batch[i]);
        }
    }
    return NULL;
}

/* 全量同步开始（切 BUFFERING）时清空转发队列剩余节点（IMPORTANT 2）：
 * 这些是全量开始前捕获的增量，排在转发线程尚未 writev 的队列尾部；若不清理，
 * 转发线程会在 BUFFERING 期间把它们下送到 slave，穿插进全量同步流（流破坏）。
 * 全量快照已包含这些写，故直接丢弃（而非进 cache 后被全量后重复应用）。 */
static void proxy_fwd_drain_to_cache_or_discard(void) {
    pthread_mutex_lock(&g_pfwd_lock);
    proxy_fwd_node_t *n = g_pfwd_head;
    g_pfwd_head = g_pfwd_tail = NULL;
    g_pfwd_bytes = 0;
    pthread_mutex_unlock(&g_pfwd_lock);
    while (n) {
        proxy_fwd_node_t *nx = n->next;
        free(n);
        n = nx;
    }
}

/* 启动/停止转发线程 */
static void proxy_fwd_start(void) {
    g_pfwd_stop = 0;
    g_cache_flush_pending = 0;
    if (pthread_create(&g_pfwd_thread, NULL, proxy_fwd_thread_main, NULL) != 0)
        fprintf(stderr, "ebpf-proxy: failed to start forwarding thread\n");
    /* 转发线程不单独钉核：4 核 VM 上钉 CPU1 与 memtier 客户端线程争抢，转发速率波动
     * （0.3%~89%），反而更不稳定。与 proxy 主线程共享 CPU0，靠 writev 重试 + 背压
     * 保证无损，吞吐受限但稳定。 */
}
static void proxy_fwd_stop(void) {
    pthread_mutex_lock(&g_pfwd_lock);
    g_pfwd_stop = 1;
    pthread_cond_broadcast(&g_pfwd_cond);
    pthread_mutex_unlock(&g_pfwd_lock);
    if (g_pfwd_thread)
        pthread_join(g_pfwd_thread, NULL);
    g_cache_flush_pending = 0;
    /* 排空剩余节点 */
    proxy_fwd_node_t *n = g_pfwd_head;
    while (n) { proxy_fwd_node_t *nx = n->next; free(n); n = nx; }
    g_pfwd_head = g_pfwd_tail = NULL;
    g_pfwd_bytes = 0;
}

/* ---- 独立心跳线程 ----
 * master 在背压标志置位时校验 client_ctl[5] 心跳新鲜度（2s 内须推进）判断 proxy 存活。
 * 原心跳在 main_loop 更新——当转发队列满（64MB）时主线程阻塞于 proxy_fwd_enqueue，
 * 心跳停止推进，master 误判 proxy 死亡而忽略背压标志、继续生产，导致 ringbuf 溢出
 * 丢数据（实测跨机 P=160 丢 64%）。独立线程保证心跳与主循环阻塞解耦。 */
static pthread_t g_hb_thread = 0;
static int g_client_ctl_fd = -1;   /* 定义在本文件后部（BPF map fds） */
static void *heartbeat_thread_main(void *arg) {
    (void)arg;
    __u64 hb = 0;
    while (!g_shutdown) {
        hb++;
        write_client_ctl_u64(CLIENT_CTL_KEY_HEARTBEAT, hb);
        usleep(100000);   /* 100ms 一跳，远低于 master 的 2s 过期窗口 */
    }
    return NULL;
}

/* 独立诊断线程：每秒打印一次累计统计（不进热路径 stderr） */
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
        pthread_mutex_lock(&g_pfwd_lock);
        fprintf(stderr,
                "ebpf-proxy: [diag] fwdq=%zuMB inflight=%lluMB ringbuf=%zuMB "
                "enq=%.1fMB/s sent=%.1fMB/s bp=%llu/%llu "
                "wv=%llu eagain=%llu poll=%llu(pout=%llu,timeout=%llu,err=%llu) partial=%llu\n",
                g_pfwd_bytes / (1024 * 1024),
                __atomic_load_n(&g_inflight_bytes, __ATOMIC_RELAXED) / (1024 * 1024),
                ringbuf_occupancy() / (1024 * 1024),
                (enq - last_enq) / dt / (1024 * 1024),
                (sent - last_sent) / dt / (1024 * 1024),
                g_fwdq_bp_count, g_rb_bp_count,
                g_writev_calls, g_eagain_count,
                g_poll_count, g_poll_out, g_poll_timeout, g_poll_err,
                g_partial_write);
        pthread_mutex_unlock(&g_pfwd_lock);
        last_enq = enq; last_sent = sent; last_us = now_us;
    }
    return NULL;
}
static void diag_start(void) {
    if (getenv("EBPF_PROXY_DIAG") == NULL) return;   /* 默认不打印：每秒刷屏干扰日志；排查时 EBPF_PROXY_DIAG=1 */
    pthread_t t;
    if (pthread_create(&t, NULL, diag_thread_main, NULL) != 0)
        fprintf(stderr, "ebpf-proxy: failed to start diag thread\n");
}
static void heartbeat_start(void) {
    if (pthread_create(&g_hb_thread, NULL, heartbeat_thread_main, NULL) != 0)
        fprintf(stderr, "ebpf-proxy: failed to start heartbeat thread\n");
}


/* BPF map fds（g_client_ctl_fd 前向声明在上方，供心跳线程使用） */
static int g_proxy_cfg_fd = -1;
static int g_client_stats_fd = -1;

/* ringbuf */
static struct bpf_object *g_bpf_obj = NULL;
static struct ring_buffer *g_rb = NULL;

/* fexit link（fexit-only） */
static struct bpf_link *g_fexit_link = NULL;

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

/* 快速判断 payload 是否为复制控制命令（REPLSYNC/REPLACK/REPLDONE）。
 * 正常 RESP 命令以 '*'/'$'/':'/'+'/'-' 开头，首字节快速拒绝。 */
static int is_repl_control_payload(const unsigned char *payload, size_t plen) {
    if (plen < 7 || payload[0] != 'R')
        return 0;

    if (plen >= 8 && memcmp(payload, "REPLSYNC", 8) == 0)
        return 1;
    if (memcmp(payload, "REPLACK", 7) == 0)
        return 1;
    if (plen >= 8 && memcmp(payload, "REPLDONE", 8) == 0)
        return 1;

    return 0;
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
        /* magic flush signal: REPLDONE，切回 FORWARDING 并请求 fwd 线程刷 cache。
         * 主线程不直接写 slave fd（fwd 线程是唯一 writer，避免并发写竞态）。 */
        fprintf(stderr, "ebpf-proxy: REPLDONE detected, requesting cache flush...\n");
        set_state(STATE_FORWARDING);
        request_cache_flush();
        return 0;
    }

    /* 过滤 slave→master 复制控制命令，防止回传 slave */
    if (is_repl_control_payload(payload, plen)) {
        return 0;
    }

    /* payload 指针只在回调期间有效，此处拷贝到稳定存储：
     * FORWARDING → 转发队列（转发线程负责 writev）；
     * 超大 payload → heap cache 慢路径；BUFFERING → cache 等 fullsync 结束 flush。 */
    if (g_state == STATE_FORWARDING && proxy_slave_is_connected(&g_slave)) {
        if (plen > PFWD_LARGE_SZ) {
            cache_enq_wrap(payload, plen);   /* 超大 payload：走 heap cache 慢路径 */
        } else {
            proxy_fwd_enqueue(payload, plen);
        }
    } else {
        cache_enq_wrap(payload, plen);
    }
    return 0;
}

/* 主循环 */
static void main_loop(void) {
    while (!g_shutdown) {
        /* capture BPF 用 BPF_RB_NO_WAKEUP（省每 recv 的 self-IPI，占 master CPU2 25-27%）：
         * producer 不发 eventfd 信号，ring_buffer__poll 只当 sleep（超时返回），
         * 必须用 ring_buffer__consume 主动处理缓冲里已有的记录。
         * 默认 wakeup 下 consume 幂等（poll 已处理则无剩余），向后兼容。 */
        int rc = ring_buffer__poll(g_rb, 1 /* ms */);
        if (rc < 0 && !g_shutdown) {
            fprintf(stderr, "ebpf-proxy: ring_buffer__poll error: %d\n", rc);
            break;
        }
        if (ring_buffer__consume(g_rb) < 0 && !g_shutdown) {
            fprintf(stderr, "ebpf-proxy: ring_buffer__consume error\n");
            break;
        }

        /* ringbuf 背压清除：BPF 只负责高水位置位（client_ctl[4]），此处由消费端
         * 在 ringbuf 排空到低水位后清除——清除不依赖新 fexit（master 被停后无 fexit），
         * 避免背压自锁。转发队列背压（client_ctl[6]）由入队/出队即时管理。 */
        if (ringbuf_occupancy() < RB_BACKPRESSURE_LOW) {
            __u32 bp_key = CLIENT_CTL_KEY_BACKPRESSURE;
            __u64 bp_now = 0;
            bpf_map_lookup_elem(g_client_ctl_fd, &bp_key, &bp_now);
            if (bp_now == 1) {
                g_rb_bp_count++;
                write_client_ctl_u64(CLIENT_CTL_KEY_BACKPRESSURE, 0);
            }
        }

        /* 注意：不再调用 batch_flush()。FORWARDING 数据已入转发队列，
         * 由独立转发线程 writev 到 slave。*/

        /* 检查 fullsync 状态变化
         * client_ctl[3] 由 master 进程在 queue_snapshot/REPLDONE 时写入:
         *   1 = 全量同步开始 → 切 BUFFERING
         *   0 = 全量同步结束 → flush 缓存 → 切 FORWARDING */
        __u64 fs_val = 0;
        if (read_client_ctl_u64(3, &fs_val) == 0) {
            if (fs_val == 1 && g_state == STATE_FORWARDING) {
                set_state(STATE_BUFFERING);
                /* IMPORTANT 2: 清空转发队列剩余增量，防其穿插进全量同步流 */
                proxy_fwd_drain_to_cache_or_discard();
                fprintf(stderr, "ebpf-proxy: fullsync start (client_ctl[3]=1), "
                        "state=BUFFERING\n");
            } else if (fs_val == 0 && g_state == STATE_BUFFERING) {
                fprintf(stderr, "ebpf-proxy: fullsync end (client_ctl[3]=0), "
                        "requesting cache flush...\n");
                set_state(STATE_FORWARDING);
                request_cache_flush();   /* fwd 线程（唯一 writer）刷 cache */
            }
        }

        /* 检查 slave 连接 */
        if (!proxy_slave_is_connected(&g_slave)) {
            if (read_slave_addr() == 0) {
                char host[64];
                snprintf(host, sizeof(host), "%u.%u.%u.%u",
                         g_slave_ip & 0xFF, (g_slave_ip >> 8) & 0xFF,
                         (g_slave_ip >> 16) & 0xFF, (g_slave_ip >> 24) & 0xFF);
                proxy_slave_init(&g_slave, host, g_slave_port);

                /* 指数退避 */
                for (int attempt = 1; !g_shutdown; attempt++) {
                    if (proxy_slave_connect(&g_slave) == 0) {
                        request_cache_flush();   /* 重连成功：让 fwd 线程刷残留 cache */
                        break;
                    }
                    unsigned int delay = g_slave.backoff_ms;
                    if (delay < PROXY_SLAVE_BACKOFF_INIT_MS)
                        delay = PROXY_SLAVE_BACKOFF_INIT_MS;
                    if (delay > 5000) delay = 5000;
                    fprintf(stderr, "ebpf-proxy: reconnect attempt %d, "
                            "sleeping %ums\n", attempt, delay);
                    /* 在退避期间继续 poll ringbuf，防止 ringbuf 满丢数据 */
                    int poll_iters = (int)(delay / 5);
                    for (int i = 0; i < poll_iters && !g_shutdown; i++) {
                        ring_buffer__poll(g_rb, 5);
                    }
                    g_slave.backoff_ms *= 2;
                    if (g_slave.backoff_ms > g_slave.backoff_max_ms)
                        g_slave.backoff_ms = g_slave.backoff_max_ms;
                }
            }
        }

        /* 不再由主线程直接 flush cache：fwd 线程是 slave fd 的唯一 writer，
         * cache flush 全部由其处理（消除主线程 send_full 与 fwd writev 的并发写竞态）。 */
    }
}

/* 退出清理 */
static void cleanup(void) {
    fprintf(stderr, "ebpf-proxy: shutting down...\n");

    /* 0. 先 detach fexit — 停止新数据进入 ringbuf */
    if (g_fexit_link) { bpf_link__destroy(g_fexit_link); g_fexit_link = NULL; }

    /* 1. 排空 ringbuf 到转发队列/缓存。转发线程此刻仍在运行，回调入队的数据
     *    由 fwd_thread 继续转发到 slave；若在停线程之后再 drain，入队的数据会
     *    积在已停的队列上被丢弃。 */
    if (g_rb) {
        int drained = 0;
        for (int i = 0; i < 50; i++) {  /* 最多 5s (50 × 100ms) */
            int n = ring_buffer__poll(g_rb, 100);
            if (n <= 0) break;
            drained += n;
        }
        if (drained > 0)
            fprintf(stderr, "ebpf-proxy: drained %d remaining ringbuf entries\n", drained);
    }

    /* 2. 停转发线程：先请求最后一次 cache flush（fwd 线程是唯一 writer），
     *    再 stop——fwd 线程刷完残留 cache 后才退出，避免关闭丢数据。 */
    request_cache_flush();
    proxy_fwd_stop();

    /* 3. 剩余缓存由 cache_destroy 释放（fwd 线程已尽力刷出；fd 故障时残留在此清理） */
    pthread_mutex_lock(&g_cache_lock);
    cache_destroy(&g_cache);
    pthread_mutex_unlock(&g_cache_lock);

    /* 4. 断开 slave */
    proxy_slave_disconnect(&g_slave);

    /* 5. 释放 BPF 资源 */
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
        int rc = bpf_map__pin(map, path);
        if (rc != 0) {
            fprintf(stderr, "ebpf-proxy: pin map '%s' to %s failed: %s\n",
                    name, path, strerror(errno));
        } else {
            fprintf(stderr, "ebpf-proxy: pinned map '%s' to %s\n", name, path);
        }
    }

    /* 打开自己的 pinned maps */
    if (open_pinned_map("client_ctl", &g_client_ctl_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open client_ctl failed\n");
    if (open_pinned_map("proxy_cfg", &g_proxy_cfg_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open proxy_cfg failed\n");
    if (open_pinned_map("client_stats", &g_client_stats_fd) != 0)
        fprintf(stderr, "ebpf-proxy: open client_stats failed\n");

    /* attach fexit（fexit-only：ctx[5] 取返回值，无 fentry） */
    prog = bpf_object__find_program_by_name(g_bpf_obj, "fexit_tcp_recvmsg");
    if (!prog) { fprintf(stderr, "ebpf-proxy: find fexit failed\n"); return -1; }
    g_fexit_link = bpf_program__attach(prog);
    if (libbpf_get_error(g_fexit_link)) {
        fprintf(stderr, "ebpf-proxy: attach fexit failed\n");
        return -1;
    }

    /* 创建 ringbuf reader：先 mmap 头页（双信号背压用 in-object fd），再创建 reader */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(g_bpf_obj, "client_cache_ringbuf");
    if (!rb_map) { fprintf(stderr, "ebpf-proxy: find ringbuf map failed\n"); return -1; }
    int rb_fd = bpf_map__fd(rb_map);
    rb_meta_init(rb_fd);
    g_rb = ring_buffer__new(rb_fd, ringbuf_callback, NULL, NULL);
    if (!g_rb) { fprintf(stderr, "ebpf-proxy: ring_buffer__new failed\n"); return -1; }

    fprintf(stderr, "ebpf-proxy: BPF loaded, fexit attached\n");
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

    /* 等待 master 写配置（持续重试，master 可能后启动） */
    fprintf(stderr, "ebpf-proxy: waiting for master config...\n");
    if (wait_for_master_pid(300000) != 0) {
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
                 g_slave_ip & 0xFF, (g_slave_ip >> 8) & 0xFF,
                 (g_slave_ip >> 16) & 0xFF, (g_slave_ip >> 24) & 0xFF);
        proxy_slave_init(&g_slave, host, g_slave_port);
        proxy_slave_connect(&g_slave);
    }

    /* 启动独立心跳线程 + 诊断线程 + 转发线程 */
    heartbeat_start();
    diag_start();
    proxy_fwd_start();

    fprintf(stderr, "ebpf-proxy: entering main loop, state=FORWARDING\n");
    main_loop();
    cleanup();
    return 0;
}
