#include "kvstore/kvstore.h"
#include <liburing.h>

#define KVS_URING_ENTRIES 1024
#define KVS_EVENT_ACCEPT  1
#define KVS_EVENT_READ    2
#define KVS_EVENT_WRITE   3

typedef struct uring_req_s {
    int event;
    conn_t *conn;
    int fd;
    struct sockaddr_in client_addr;
    socklen_t client_len;
} uring_req_t;

static long long g_last_cron_ms = 0;

static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void free_req(uring_req_t *req) {
    if (req) kvs_free(req);
}

static void close_conn_uring(conn_t *c) {
    if (!c) return;

    if (c->is_replica) repl_remove_slave(c);

    repl_ebpf_unregister_fd(c->fd);
    close(c->fd);

    kvs_free(c->inbuf);   /* inbuf/out_ring 为一次合并分配，只 free 一次 */
    kvs_free(c);
}

static void run_cron_once(void) {
    long long now = kvs_now_ms();
    if (now - g_last_cron_ms < 100) return;

    kvs_active_expire_cycle(32);
    persist_autosnap_cron();
    persist_bgsave_poll();
    persist_bgrewriteaof_poll();
    g_last_cron_ms = now;
}

static int submit_accept(struct io_uring *ring, int lfd) {
    uring_req_t *req = (uring_req_t *)kvs_calloc(1, sizeof(*req));
    if (!req) return -1;

    req->event = KVS_EVENT_ACCEPT;
    req->fd = lfd;
    req->client_len = sizeof(req->client_addr);

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        free_req(req);
        return -1;
    }

    io_uring_prep_accept(sqe, lfd, (struct sockaddr *)&req->client_addr, &req->client_len, 0);
    io_uring_sqe_set_data(sqe, req);
    return 0;
}

static int submit_read(struct io_uring *ring, conn_t *c) {
    if (!c) return -1;
    if (c->in_len >= INBUF_CAP) return -1;

    uring_req_t *req = (uring_req_t *)kvs_calloc(1, sizeof(*req));
    if (!req) return -1;

    req->event = KVS_EVENT_READ;
    req->conn = c;
    req->fd = c->fd;

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        free_req(req);
        return -1;
    }

    io_uring_prep_recv(sqe, c->fd, c->inbuf + c->in_len, INBUF_CAP - c->in_len, 0);
    io_uring_sqe_set_data(sqe, req);
    return 0;
}

static int submit_write(struct io_uring *ring, conn_t *c) {
    size_t head, chunk;

    if (!c || c->out_ring_len == 0) return -1;

    head = c->out_ring_head;
    chunk = OUT_RING_SIZE - head;
    if (chunk > c->out_ring_len) chunk = c->out_ring_len;

    uring_req_t *req = (uring_req_t *)kvs_calloc(1, sizeof(*req));
    if (!req) return -1;

    req->event = KVS_EVENT_WRITE;
    req->conn = c;
    req->fd = c->fd;

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        free_req(req);
        return -1;
    }

    io_uring_prep_send(sqe, c->fd, c->out_ring + head, chunk, 0);
    io_uring_sqe_set_data(sqe, req);
    return 0;
}

static int create_listener(unsigned short port) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) return -1;

    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if (set_nonblock(lfd) != 0) {
        close(lfd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(lfd);
        return -1;
    }
    if (listen(lfd, LISTEN_BACKLOG) != 0) {
        close(lfd);
        return -1;
    }
    return lfd;
}

int proactor_start(unsigned short port) {
    int lfd = create_listener(port);
    if (lfd < 0) return -1;

    if (g_cfg.role == ROLE_SLAVE) start_slave_thread();

    struct io_uring ring;
    if (io_uring_queue_init(KVS_URING_ENTRIES, &ring, 0) != 0) {
        close(lfd);
        return -1;
    }

    g_last_cron_ms = kvs_now_ms();

    if (submit_accept(&ring, lfd) != 0) {
        io_uring_queue_exit(&ring);
        close(lfd);
        return -1;
    }
    io_uring_submit(&ring);

    while (1) {
        struct io_uring_cqe *cqe = NULL;
        int rc = io_uring_wait_cqe_timeout(&ring, &cqe, NULL);
        run_cron_once();

        if (rc == -ETIME || rc == -EINTR) continue;
        if (rc < 0) break;
        if (!cqe) continue;

        uring_req_t *req = (uring_req_t *)io_uring_cqe_get_data(cqe);
        int res = cqe->res;

        if (!req) {
            io_uring_cqe_seen(&ring, cqe);
            continue;
        }

        if (req->event == KVS_EVENT_ACCEPT) {
            int cfd = res;
            submit_accept(&ring, lfd);

            if (cfd >= 0) {
                conn_t *c = (conn_t *)kvs_calloc(1, sizeof(*c));
                if (!c) {
                    close(cfd);
                } else {
                    /* I/O 缓冲一次 malloc（不清零），避免每连接 1.3MB calloc 清零 */
                    c->inbuf = (unsigned char *)kvs_malloc(INBUF_CAP + OUT_RING_SIZE);
                    c->fd = cfd;
                    c->repl_transport_kind = KVS_REPL_TRANSPORT_TCP;
                    set_nonblock(cfd);
                    if (!c->inbuf) {
                        close_conn_uring(c);
                    } else {
                        c->out_ring = c->inbuf + INBUF_CAP;
                        if (submit_read(&ring, c) != 0) {
                            close_conn_uring(c);
                        }
                    }
                }
            }
        } else if (req->event == KVS_EVENT_READ) {
            conn_t *c = req->conn;
            if (!c) {
                /* ignore */
            } else if (res <= 0) {
                if (c->out_ring_len > 0) {
                    if (submit_write(&ring, c) != 0) close_conn_uring(c);
                } else {
                    close_conn_uring(c);
                }
            } else {
                c->in_len += (size_t)res;
                parse_resp_stream(c, c->inbuf, &c->in_len, 0);

                if (c->out_ring_len > 0) {
                    if (submit_write(&ring, c) != 0) close_conn_uring(c);
                } else {
                    if (submit_read(&ring, c) != 0) close_conn_uring(c);
                }
            }
        } else if (req->event == KVS_EVENT_WRITE) {
            conn_t *c = req->conn;
            if (!c || c->out_ring_len == 0) {
                /* ignore */
            } else if (res <= 0) {
                close_conn_uring(c);
            } else {
                c->out_ring_head = (c->out_ring_head + (size_t)res) & (OUT_RING_SIZE - 1);
                c->out_ring_len -= (size_t)res;

                if (c->out_ring_len > 0) {
                    if (submit_write(&ring, c) != 0) close_conn_uring(c);
                } else {
                    if (submit_read(&ring, c) != 0) close_conn_uring(c);
                }
            }
        }

        free_req(req);
        io_uring_cqe_seen(&ring, cqe);
        persist_flush_pending();
        persist_reap_completions();
        io_uring_submit(&ring);
    }

    io_uring_queue_exit(&ring);
    close(lfd);
    return -1;
}