#define _GNU_SOURCE   /* accept4 (SOCK_NONBLOCK) */
#include "kvstore/kvstore.h"
#include <netinet/tcp.h>
#include <unistd.h>

/* 接管复制转发（Task 3）：close_conn 在 free conn_t 前移除转发线程对该 conn 的引用（防 UAF） */
extern void repl_fwd_purge_conn(conn_t *c);

#define MAX_READS_PER_EVENT 16
/* 单次 recv 上限：对齐捕获 BPF 的 CLIENT_ENTRY_MAX_LEN=32764。若一次 recv 超过 32KB，
 * eBPF 捕获会截断尾部静默丢数据（增量复制丢失）。限制读大小后 BPF 永远捕获完整 recv；
 * 大 value/大批量仍由多次 recv + parse_resp_stream 跨片拼接处理。 */
#define MAX_RECV_BYTES 32764

int g_epfd = -1;
static conn_t *fdmap[65536];
static long long g_last_expire = 0;

static int expire_cycle_budget(void) {
    size_t count = global_expire.count;
    if (count >= 1000000) return 4096;
    if (count >= 300000) return 2048;
    if (count >= 100000) return 1024;
    if (count >= 30000) return 512;
    if (count >= 10000) return 256;
    if (count >= 1000) return 128;
    return 32;
}

static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int mod_events(conn_t *c, uint32_t events) {
    if (!c) return -1;
    if (likely(c->epoll_events == events)) return 0;  /* skip redundant syscall */
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = c->fd;
    c->epoll_events = events;
    return epoll_ctl(g_epfd, EPOLL_CTL_MOD, c->fd, &ev);
}

int queue_bytes(conn_t *c, const unsigned char *buf, size_t len) {
    size_t tail, first_chunk;
    size_t old_len;

    if (!c || !buf || len == 0) return -1;
    if (len > OUT_RING_SIZE - c->out_ring_len) return -1;

    old_len = c->out_ring_len;
    tail = c->out_ring_tail;
    first_chunk = OUT_RING_SIZE - tail;
    if (len <= first_chunk) {
        memcpy(c->out_ring + tail, buf, len);
    } else {
        memcpy(c->out_ring + tail, buf, first_chunk);
        memcpy(c->out_ring, buf + first_chunk, len - first_chunk);
    }
    c->out_ring_tail = (tail + len) & (OUT_RING_SIZE - 1);
    c->out_ring_len += len;

    /* register EPOLLOUT only when transitioning from empty→non-empty.
     * Defer during on_read pipeline batch — on_write will drain immediately. */
    if (unlikely(old_len == 0 && !c->defer_epollout)) {
        mod_events(c, EPOLLIN | EPOLLOUT);
    }
    return 0;
}

void close_conn(conn_t *c) {
    if (!c) return;

#if KVS_ENABLE_RDMA
    /* 若该连接是某个 fullsync transfer 的 owner，清理 master 侧 transfer 状态 */
    repl_rdma_master_transfer_cleanup(c);
#endif

    if (c->is_replica) {
        repl_remove_slave(c);
    }

    /* 从转发线程队列 + 发送状态移除该 conn，避免 free 后转发线程仍解引用 conn_t（UAF）。
     * 必须在 free 之前调用。 */
    repl_fwd_purge_conn(c);

    repl_ebpf_unregister_fd(c->fd);
    epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);

    if (c->fd >= 0 && c->fd < (int)(sizeof(fdmap) / sizeof(fdmap[0]))) {
        fdmap[c->fd] = NULL;
    }

    kvs_free(c->inbuf);   /* inbuf/out_ring 为一次合并分配，只 free 一次 */
    kvs_free(c);
}

static void on_accept(conn_t *lc) {
    while (1) {
        struct sockaddr_in cli;
        socklen_t len = sizeof(cli);

        /* accept4 一次完成 accept+O_NONBLOCK（省 2 次 fcntl syscall/连接） */
        int cfd = accept4(lc->fd, (struct sockaddr *)&cli, &len,
                          SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return;
        }

        if (cfd >= (int)(sizeof(fdmap) / sizeof(fdmap[0]))) {
            close(cfd);
            continue;
        }

        /* Disable Nagle's algorithm for low-latency pipeline responses */
        { int one = 1; setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }

        conn_t *c = (conn_t *)kvs_calloc(1, sizeof(*c));
        if (!c) {
            close(cfd);
            continue;
        }
        /* I/O 缓冲一次 malloc（不清零）：calloc 清零 1.3MB/连接是冷启动 1k 基准
         * 的主要开销（50 连接 = 65MB 清零 + 1.6 万缺页）；合并 inbuf+out_ring 为
         * 一次分配，省 1 次 mmap/连接。缓冲只在写入后读取（len 从 0 开始），无需清零。 */
        c->inbuf = (unsigned char *)kvs_malloc(INBUF_CAP + OUT_RING_SIZE);
        if (!c->inbuf) {
            kvs_free(c);
            close(cfd);
            continue;
        }
        c->out_ring = c->inbuf + INBUF_CAP;

        c->fd = cfd;
        c->repl_transport_kind = KVS_REPL_TRANSPORT_TCP;
        fdmap[cfd] = c;

        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.fd = cfd;
        epoll_ctl(g_epfd, EPOLL_CTL_ADD, cfd, &ev);
        c->epoll_events = EPOLLIN;  /* track initial mask for mod_events skip */
    }
}

static void on_write(conn_t *c);

static void on_read(conn_t *c) {
    /* ebpf+tcp 转发无损背压：转发队列近满时（proxy 置 client_ctl[4]）停手等待，
     * 避免 ringbuf 溢出让 fexit 捕获静默丢数据。非 ebpf+tcp 模式此调用恒 0。
     * usleep 轮询 ~10k/s，仅在转发落后时进入。
     * 只拦客户端连接：replica 连接读的是 REPLACK 等控制命令（proxy 已过滤），
     * 不产生转发压力，阻塞会让复制控制面超时。 */
    if (!c->is_replica) {
        while (repl_ebpf_backpressure())
            usleep(100);
    }
    int reads = 0;
    while (1) {
        if (unlikely(c->in_len >= INBUF_CAP)) {
            close_conn(c);
            return;
        }

        size_t want = INBUF_CAP - c->in_len;
        if (want > MAX_RECV_BYTES) want = MAX_RECV_BYTES;   /* 防捕获 BPF 32KB 截断 */
        ssize_t n = recv(c->fd, c->inbuf + c->in_len, want, 0);
        if (likely(n > 0)) {
            c->in_len += (size_t)n;
            c->defer_epollout = 1;  /* queue_bytes won't reg EPOLLOUT; on_write drains after */
            parse_resp_stream(c, c->inbuf, &c->in_len, 0);
            c->defer_epollout = 0;
            if (++reads >= MAX_READS_PER_EVENT) break;
            /* 缓冲已清空：无更多完整命令，停止 recv EAGAIN 探测（对齐 Redis
             * readQueryFromClient 读一次即返回，省一次多余的 recvfrom syscall）。 */
            if (c->in_len == 0) break;
            continue;
        }

        if (n == 0) {
            if (c->out_ring_len > 0) {
                mod_events(c, EPOLLOUT);
                return;
            }
            close_conn(c);
            return;
        }

        if (likely(errno == EAGAIN || errno == EWOULDBLOCK)) break;
        close_conn(c);
        return;
    }

    /* 不在读路径立即写——改为事件循环末尾 flush_conn_output(NULL) 统一写
     * （对齐 Redis beforeSleep：读完所有就绪连接后统一 flush，多连接并发调度更优）。
     * 读路径不再预注册 EPOLLOUT：flush_conn_output(NULL) 每轮末尾无条件执行，
     * 写不完时 on_write 末尾已按剩量注册 EPOLLOUT 兜底（EAGAIN/慢读由事件续写）。
     * 省 2 次 epoll_ctl MOD/命令（perf 实测 epoll_ctl 占 ECHO 路径 ~13%、HSET ~7%）。
     * 只保证 EPOLLIN 在位，不撤销其他路径已注册的 EPOLLOUT。 */
    if (likely((c->epoll_events & EPOLLIN) == 0)) mod_events(c, EPOLLIN);

    persist_reap_completions();
}

static void on_write(conn_t *c) {
    while (c->out_ring_len > 0) {
        size_t head = c->out_ring_head;
        size_t chunk = OUT_RING_SIZE - head;
        ssize_t w;

        if (chunk > c->out_ring_len) chunk = c->out_ring_len;

        w = send(c->fd, c->out_ring + head, chunk, 0);
        if (w < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_conn(c);
            return;
        }
        if (w == 0) break;

        c->out_ring_head = (head + (size_t)w) & (OUT_RING_SIZE - 1);
        c->out_ring_len -= (size_t)w;
    }

    if (c->out_ring_len > 0) mod_events(c, EPOLLIN | EPOLLOUT);
    else mod_events(c, EPOLLIN);
}

/* attempt immediate non-blocking write of queued output.
 * c != NULL: flush 单个连接；c == NULL: 遍历 fdmap 统一 flush 所有有 pending 输出的连接
 * （事件循环末尾调用，对齐 Redis beforeSleep）。 */
void flush_conn_output(conn_t *c) {
    if (c) {
        if (c->out_ring_len > 0) on_write(c);
        return;
    }
    for (conn_t **p = fdmap; p < fdmap + (int)(sizeof(fdmap) / sizeof(fdmap[0])); ++p) {
        conn_t *cc = *p;
        if (!cc || cc->is_listener || cc->out_ring_len == 0) continue;
        on_write(cc);
        if (*p != cc) continue;  /* on_write 出错 close_conn 已置 fdmap[fd]=NULL */
    }
}

int reactor_start(void) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) return -1;

    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    set_nonblock(lfd);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)g_cfg.port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(lfd);
        return -1;
    }

    if (listen(lfd, LISTEN_BACKLOG) < 0) {
        close(lfd);
        return -1;
    }

    g_epfd = epoll_create1(0);
    if (g_epfd < 0) {
        close(lfd);
        return -1;
    }

    /* register persist complete eventfd（AOF 线程批完成 → 主线程 drain/release） */
    {
        int pefd = persist_uring_fd();
        if (pefd >= 0) {
            struct epoll_event pev;
            memset(&pev, 0, sizeof(pev));
            pev.events = EPOLLIN;
            pev.data.fd = pefd;
            epoll_ctl(g_epfd, EPOLL_CTL_ADD, pefd, &pev);
        }
    }

    conn_t *lc = (conn_t *)kvs_calloc(1, sizeof(*lc));
    if (!lc) {
        close(lfd);
        close(g_epfd);
        g_epfd = -1;
        return -1;
    }

    lc->fd = lfd;
    lc->is_listener = 1;
    fdmap[lfd] = lc;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.fd = lfd;
    epoll_ctl(g_epfd, EPOLL_CTL_ADD, lfd, &ev);

    g_last_expire = kvs_now_ms();

    if (g_cfg.role == ROLE_SLAVE) start_slave_thread();

    while (1) {
        struct epoll_event events[MAX_EVENTS];
        /* AOF 攒批窗口约束：有未 flush 的 AOF 槽时，epoll 超时压到距窗口结束的剩余时间，
         * 保证稀疏流量下孤立命令也在 ~window 量级内 flush（否则要等 100ms epoll 超时）。 */
        int timeout_ms = 100;
        int aof_ms = persist_aof_pending_flush_ms();
        if (aof_ms >= 0 && aof_ms < timeout_ms) timeout_ms = aof_ms;
        int n = epoll_wait(g_epfd, events, MAX_EVENTS, timeout_ms);

        long long now = kvs_now_ms();
        if (now - g_last_expire >= 100) {
            int budget = expire_cycle_budget();
            kvs_active_expire_cycle(budget);
            persist_autosnap_cron();
#if KVS_ENABLE_KPROBE_RDMA
            extern void repl_kprobe_fwd_health_check(void);
            repl_kprobe_fwd_health_check();
#endif
            g_last_expire = now;
        }

        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            /* persist complete eventfd（AOF 线程批完成信号）：回收 completed 槽并放行 conn。
             * T2 起 io_uring submit 在 AOF 线程，此处只 drain。 */
            if (fd == persist_uring_fd()) {
                uint64_t val;
                ssize_t nread = read(fd, &val, sizeof(val));
                (void)nread;
                persist_reap_completions();
                continue;
            }

            conn_t *c = fdmap[fd];
            if (!c) continue;

            if (c->is_listener) {
                on_accept(c);
                continue;
            }

            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                if (c->out_ring_len > 0) {
                    on_write(c);
                    if (fdmap[fd] == c && c->out_ring_len == 0) close_conn(c);
                } else {
                    close_conn(c);
                }
                continue;
            }

            if ((events[i].events & EPOLLOUT) && c->out_ring_len > 0) {
                on_write(c);
                if (fdmap[fd] != c) continue;
            }

            if ((events[i].events & EPOLLIN) && fdmap[fd] == c) {
                on_read(c);
                if (fdmap[fd] != c) continue;
            }

            if ((events[i].events & EPOLLOUT) && fdmap[fd] == c && c->out_ring_len > 0) {
                on_write(c);
            }
        }

        /* group-commit: flush + reap any completed batches */
        persist_flush_pending();
        persist_reap_completions();

        /* 统一写：事件循环处理完所有就绪 fd 后，flush 所有有 pending 输出的连接
         * （对齐 Redis beforeSleep）。EPOLLOUT 事件驱动 on_write 保留作 EAGAIN/慢读兜底。 */
        flush_conn_output(NULL);
    }

    return 0;
}
