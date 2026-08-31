#include "kvstore/kvstore.h"
#include "nty_coroutine.h"
#include <arpa/inet.h>

static pthread_t g_ntyco_cron_tid;
static int g_ntyco_cron_started = 0;

static void close_conn_nty(conn_t *c) {
    if (!c) return;

    if (c->is_replica) repl_remove_slave(c);

    repl_ebpf_unregister_fd(c->fd);
    close(c->fd);

    kvs_free(c->inbuf);   /* inbuf/out_ring 为一次合并分配，只 free 一次 */
    kvs_free(c);
}

static int flush_output_blocking(conn_t *c) {
    while (c && c->out_ring_len > 0) {
        size_t head = c->out_ring_head;
        size_t chunk = OUT_RING_SIZE - head;
        ssize_t w;

        if (chunk > c->out_ring_len) chunk = c->out_ring_len;

        w = send(c->fd, c->out_ring + head, chunk, 0);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) return -1;

        c->out_ring_head = (head + (size_t)w) & (OUT_RING_SIZE - 1);
        c->out_ring_len -= (size_t)w;
    }
    return 0;
}

static void server_reader(void *arg) {
    conn_t *c = (conn_t *)arg;
    if (!c) return;

    while (1) {
        if (c->in_len >= INBUF_CAP) {
            close_conn_nty(c);
            return;
        }

        ssize_t n = recv(c->fd, c->inbuf + c->in_len, INBUF_CAP - c->in_len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            close_conn_nty(c);
            return;
        }
        if (n == 0) {
            if (c->out_ring_len > 0) {
                if (flush_output_blocking(c) != 0) {
                    close_conn_nty(c);
                    return;
                }
            }
            close_conn_nty(c);
            return;
        }

        c->in_len += (size_t)n;
        parse_resp_stream(c, c->inbuf, &c->in_len, 0);
        persist_flush_pending();
        persist_reap_completions();  /* reap AOF CQEs after command processing */

        if (c->out_ring_len > 0) {
            if (flush_output_blocking(c) != 0) {
                close_conn_nty(c);
                return;
            }
            persist_flush_pending();
            persist_reap_completions();  /* also try after flushing output */
        }
    }
}

static void *cron_thread_main(void *arg) {
    (void)arg;
    for (;;) {
        kvs_active_expire_cycle(32);
        persist_autosnap_cron();
        persist_bgsave_poll();
        persist_bgrewriteaof_poll();
        usleep(100 * 1000);  // 100ms，和 reactor 的节奏接近
    }
    return NULL;
}

static int start_ntyco_cron_thread(void) {
    if (g_ntyco_cron_started) return 0;
    if (pthread_create(&g_ntyco_cron_tid, NULL, cron_thread_main, NULL) != 0) return -1;
    pthread_detach(g_ntyco_cron_tid);
    g_ntyco_cron_started = 1;
    return 0;
}

static void server_main(void *arg) {
    unsigned short port = *(unsigned short *)arg;
    kvs_free(arg);

    fprintf(stderr, "[ntyco] server_main start, port=%u\n", port);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("[ntyco] socket");
        return;
    }

    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in local;
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(lfd, (struct sockaddr *)&local, sizeof(local)) != 0) {
        perror("[ntyco] bind");
        close(lfd);
        return;
    }
    if (listen(lfd, LISTEN_BACKLOG) != 0) {
        perror("[ntyco] listen");
        close(lfd);
        return;
    }

    fprintf(stderr, "[ntyco] listening on %u\n", port);

    while (1) {
        struct sockaddr_in remote;
        socklen_t len = sizeof(remote);
        int cfd = accept(lfd, (struct sockaddr *)&remote, &len);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("[ntyco] accept");
            break;
        }

        fprintf(stderr, "[ntyco] accepted fd=%d\n", cfd);

        conn_t *c = (conn_t *)kvs_calloc(1, sizeof(*c));
        if (!c) {
            close(cfd);
            continue;
        }
        /* I/O 缓冲一次 malloc（不清零），避免每连接 1.3MB calloc 清零 */
        c->inbuf = (unsigned char *)kvs_malloc(INBUF_CAP + OUT_RING_SIZE);
        if (!c->inbuf) {
            close_conn_nty(c);
            continue;
        }
        c->out_ring = c->inbuf + INBUF_CAP;
        c->fd = cfd;
        c->repl_transport_kind = KVS_REPL_TRANSPORT_TCP;

        nty_coroutine *co = NULL;
        nty_coroutine_create(&co, server_reader, c);
    }

    close(lfd);
}

int ntyco_start(unsigned short port) {
    fprintf(stderr, "[ntyco] start, port=%u\n", port);

    if (g_cfg.role == ROLE_SLAVE) start_slave_thread();
    if (start_ntyco_cron_thread() != 0) {
        fprintf(stderr, "[ntyco] failed to start cron thread\n");
        return -1;
    }

    unsigned short *pp = (unsigned short *)kvs_malloc(sizeof(unsigned short));
    if (!pp) return -1;
    *pp = port;

    nty_coroutine *server_co = NULL;
    nty_coroutine_create(&server_co, server_main, pp);

    fprintf(stderr, "[ntyco] schedule_run begin\n");
    nty_schedule_run();
    return 0;
}
