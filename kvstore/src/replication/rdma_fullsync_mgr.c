/*
 * 多 Slave 并发 RDMA 全量同步管理器。
 *
 * 职责：
 *   - 进程级唯一 CM listener（绑定 rdma_port），按 hello(tid, epoch) 把连接派发给 job；
 *   - job 生命周期：begin -> [snapshot_ready] -> CM established -> remote MR -> worker
 *     -> WR_DONE -> owner FULLSYNCEND -> REPLDONE -> 资源销毁；
 *   - 所有 conn_t / TCP 控制写由 owner 线程负责，worker/CM 线程只改 job 状态；
 *   - 失败隔离：单个 job 的 CM/CQ/超时失败只取消该 job。
 *
 * 单实例数据结构的旧路径（g_repl_rdma_ctx）保持不变；本模块只服务
 * "fullsync=rdma + realtime=ebpf/tcp" 的新并发路径。
 */

#include "rdma_fullsync_internal.h"

#include <endian.h>
#include <stdint.h>
#include <string.h>

/* CM hello 编解码不依赖 verbs/rdma，始终编译真实实现（单测可覆盖）。 */
void rdma_fs_cm_hello_encode(unsigned char out[RDMA_FS_CM_HELLO_LEN],
                             uint64_t tid, uint64_t epoch) {
    if (!out) return;
    memset(out, 0, RDMA_FS_CM_HELLO_LEN);
    uint16_t ver = htobe16(RDMA_FS_CM_HELLO_VERSION);
    uint64_t t = htobe64(tid);
    uint64_t e = htobe64(epoch);
    memcpy(out + 0, &ver, sizeof(ver));
    memcpy(out + 8, &t, sizeof(t));
    memcpy(out + 16, &e, sizeof(e));
}

int rdma_fs_cm_hello_decode(const void *data, size_t len,
                            uint64_t *tid, uint64_t *epoch) {
    if (!data || len < RDMA_FS_CM_HELLO_LEN || !tid || !epoch) return -1;
    const unsigned char *p = (const unsigned char *)data;
    uint16_t ver = 0;
    uint64_t t = 0, e = 0;
    memcpy(&ver, p + 0, sizeof(ver));
    memcpy(&t, p + 8, sizeof(t));
    memcpy(&e, p + 16, sizeof(e));
    if (be16toh(ver) != RDMA_FS_CM_HELLO_VERSION) return -1;
    *tid = be64toh(t);
    *epoch = be64toh(e);
    if (*tid == 0 || *epoch == 0) return -1;
    return 0;
}

const char *rdma_fs_reason_name(int reason) {
    switch (reason) {
    case RDMA_FS_REASON_NONE: return "none";
    case RDMA_FS_REASON_CANCELLED: return "cancelled";
    case RDMA_FS_REASON_TIMEOUT: return "timeout";
    case RDMA_FS_REASON_CM: return "cm_error";
    case RDMA_FS_REASON_CQ: return "cq_error";
    case RDMA_FS_REASON_RESOURCE: return "resource_error";
    case RDMA_FS_REASON_SNAPSHOT: return "snapshot_error";
    case RDMA_FS_REASON_REMOTE_MR: return "remote_mr_error";
    case RDMA_FS_REASON_OWNER_CLOSED: return "owner_closed";
    default: return "unknown";
    }
}

#if KVS_ENABLE_RDMA

#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* 供 rdma_fullsync_transfer.c 使用。 */
pthread_mutex_t g_rdma_fs_mu = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_rdma_fs_cv = PTHREAD_COND_INITIALIZER;
int g_rdma_fs_sending = 0;
int g_rdma_fs_max_concurrent = 2;
unsigned g_rdma_fs_max_seen = 0;

enum fs_cmd_op {
    FS_CMD_DESTROY = 1,
    FS_CMD_STOP = 2
};

typedef struct fs_cmd {
    int op;
    uint64_t tid;
} fs_cmd_t;

static struct {
    int started;
    int ready;
    int shutdown;
    struct rdma_event_channel *cm_ec;
    struct rdma_cm_id *listen_id;
    pthread_t cm_thread;
    int cmd_pipe[2];
    rdma_fs_job_t *jobs;
    uint64_t next_tid;
    uint64_t epoch;

    unsigned long long total_started;
    unsigned long long total_completed;
    unsigned long long total_failed;
    unsigned long long cm_rejects;
    unsigned long long cq_errors;
    unsigned long long registered_bytes;
} g_mgr = { .cmd_pipe = { -1, -1 } };

static const char *fs_fullsync_transport_name(void) {
    if (g_cfg.repl_fullsync_transport[0]) return g_cfg.repl_fullsync_transport;
    if (!strcasecmp(g_cfg.repl_transport_backend, "rdma")) return "rdma";
    return "tcp";
}

static const char *fs_realtime_transport_name(void) {
    if (g_cfg.repl_realtime_transport[0]) return g_cfg.repl_realtime_transport;
    if (!strcasecmp(g_cfg.repl_transport_backend, "ebpf")
        || !strcasecmp(g_cfg.repl_transport_backend, "sockmap")) return "ebpf";
    if (!strcasecmp(g_cfg.repl_transport_backend, "kprobe-rdma")) return "kprobe-rdma";
    return "tcp";
}

int rdma_fs_should_enable(void) {
    if (g_cfg.role != ROLE_MASTER) return 0;
    if (strcasecmp(fs_fullsync_transport_name(), "rdma") != 0) return 0;
    /* 纯 RDMA / kprobe-rdma 实时路径仍使用旧的单实例实现。 */
    if (!strcasecmp(g_cfg.repl_transport_backend, "rdma")) return 0;
    if (!strcasecmp(g_cfg.repl_transport_backend, "kprobe-rdma")) return 0;
    const char *rt = fs_realtime_transport_name();
    if (!strcasecmp(rt, "rdma") || !strcasecmp(rt, "kprobe-rdma")) return 0;
    return 1;
}

/* ---------------- job list helpers（调用方持 g_rdma_fs_mu） ---------------- */

static rdma_fs_job_t *fs_find_locked(uint64_t tid) {
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        if (j->tid == tid) return j;
    }
    return NULL;
}

static void fs_unlink_locked(rdma_fs_job_t *job) {
    if (job->prev) job->prev->next = job->next;
    else g_mgr.jobs = job->next;
    if (job->next) job->next->prev = job->prev;
    job->next = job->prev = NULL;
}

static void fs_free_job_locked(rdma_fs_job_t *job) {
    fs_unlink_locked(job);
    if (job->snap_fd >= 0) {
        close(job->snap_fd);
        job->snap_fd = -1;
    }
    if (job->snap_path[0]) {
        unlink(job->snap_path);
        job->snap_path[0] = '\0';
    }
    free(job);
}

static int fs_enqueue_cmd_locked(int op, uint64_t tid) {
    if (g_mgr.cmd_pipe[1] < 0) return -1;
    fs_cmd_t cmd = { .op = op, .tid = tid };
    ssize_t n = write(g_mgr.cmd_pipe[1], &cmd, sizeof(cmd));
    if (n != (ssize_t)sizeof(cmd)) {
        /* 管道满：保留 destroy_enqueued=0，下次 drain 重试。 */
        return (errno == EAGAIN || errno == EWOULDBLOCK) ? -1 : -2;
    }
    return 0;
}

/* ---------------- CM listener thread ---------------- */

static void fs_destroy_now(rdma_fs_job_t *job);

static int fs_handle_connect_request(struct rdma_cm_id *id, uint64_t tid, uint64_t epoch) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = (tid != 0) ? fs_find_locked(tid) : NULL;
    if (!job || job->epoch != epoch || job->cancel || job->owner_conn == NULL
        || job->id != NULL || job->state == RDMA_FS_ST_DONE
        || job->state == RDMA_FS_ST_FAILED) {
        g_mgr.cm_rejects++;
        pthread_mutex_unlock(&g_rdma_fs_mu);
        fprintf(stderr, "repl rdma-fs: reject CM connect (tid=%llu epoch=%llu) — "
                        "no matching waiting job\n",
                (unsigned long long)tid, (unsigned long long)epoch);
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        return -1;
    }
    job->id = id;
    id->context = job;
    job->needs_destroy = 1;
    job->resources_destroyed = 0;
    job->state = RDMA_FS_ST_WAIT_ESTABLISHED;
    g_mgr.registered_bytes += 0;
    fprintf(stderr, "repl rdma-fs: CM connect tid=%llu t=%lld (waiting_jobs=%d)\n",
            (unsigned long long)tid, kvs_now_ms(), g_rdma_fs_sending);
    pthread_mutex_unlock(&g_rdma_fs_mu);

    if (rdma_fs_job_setup_verbs(job) != 0) {
        fprintf(stderr, "repl rdma-fs: tid=%llu resource setup failed\n",
                (unsigned long long)tid);
        rdma_reject(id, NULL, 0);
        pthread_mutex_lock(&g_rdma_fs_mu);
        job->cm_disconnected = 1;
        job->cancel = 1;
        job->cancel_reason = RDMA_FS_REASON_RESOURCE;
        job->state = RDMA_FS_ST_FAILED;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }

    pthread_mutex_lock(&g_rdma_fs_mu);
    int aborted = job->cancel || job->owner_conn == NULL;
    pthread_mutex_unlock(&g_rdma_fs_mu);
    if (aborted) {
        rdma_reject(id, NULL, 0);
        pthread_mutex_lock(&g_rdma_fs_mu);
        job->cm_disconnected = 1;
        if (job->state != RDMA_FS_ST_DONE) job->state = RDMA_FS_ST_FAILED;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }

    if (rdma_accept(id, NULL) != 0) {
        fprintf(stderr, "repl rdma-fs: tid=%llu rdma_accept failed: %s\n",
                (unsigned long long)tid, strerror(errno));
        pthread_mutex_lock(&g_rdma_fs_mu);
        job->cm_disconnected = 1;
        job->cancel = 1;
        job->cancel_reason = RDMA_FS_REASON_CM;
        job->state = RDMA_FS_ST_FAILED;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    return 0;
}

static void fs_handle_established(struct rdma_cm_id *id) {
    rdma_fs_job_t *job = (rdma_fs_job_t *)id->context;
    if (!job) return;
    pthread_mutex_lock(&g_rdma_fs_mu);
    if (job->id == id && !job->cancel) {
        job->established = 1;
        job->t_established_ms = kvs_now_ms();
        if (job->state == RDMA_FS_ST_WAIT_ESTABLISHED)
            job->state = RDMA_FS_ST_WAIT_MR;
        pthread_cond_broadcast(&g_rdma_fs_cv);
    }
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

static void fs_handle_cm_gone(struct rdma_cm_id *id, const char *why) {
    rdma_fs_job_t *job = (rdma_fs_job_t *)id->context;
    if (!job) return;
    pthread_mutex_lock(&g_rdma_fs_mu);
    if (job->id == id) {
        job->cm_disconnected = 1;
        job->established = 0;
        if (!job->destroy_pending && job->state != RDMA_FS_ST_DONE) {
            job->cancel = 1;
            if (!job->cancel_reason) job->cancel_reason = RDMA_FS_REASON_CM;
            if (job->state != RDMA_FS_ST_FAILED)
                job->state = RDMA_FS_ST_FAILED;
            fprintf(stderr, "repl rdma-fs: tid=%llu CM %s\n",
                    (unsigned long long)job->tid, why);
        }
        pthread_cond_broadcast(&g_rdma_fs_cv);
    }
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

static void fs_handle_destroy_cmd(uint64_t tid) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (!job) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return;
    }
    job->destroy_pending = 1;
    job->destroy_deadline_ms = kvs_now_ms() + RDMA_FS_DESTROY_WAIT_MS;
    int need_disconnect = (job->id != NULL && !job->cm_disconnected);
    struct rdma_cm_id *id = job->id;
    pthread_mutex_unlock(&g_rdma_fs_mu);

    if (need_disconnect && id) {
        if (rdma_disconnect(id) != 0) {
            /* 连接已经不可用：直接进入强制销毁。 */
            pthread_mutex_lock(&g_rdma_fs_mu);
            if (job->id == id) job->cm_disconnected = 1;
            pthread_mutex_unlock(&g_rdma_fs_mu);
        }
    }
}

static void fs_check_destroy_timeouts(void) {
    for (;;) {
        rdma_fs_job_t *target = NULL;
        pthread_mutex_lock(&g_rdma_fs_mu);
        long long now = kvs_now_ms();
        for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
            if (j->destroy_pending && j->id &&
                (j->cm_disconnected || now >= j->destroy_deadline_ms)) {
                target = j;
                break;
            }
        }
        pthread_mutex_unlock(&g_rdma_fs_mu);
        if (!target) break;
        fs_destroy_now(target);
    }
}

static void *fs_cm_thread(void *arg) {
    (void)arg;
    const int cmd_fd = g_mgr.cmd_pipe[0];

    while (!__atomic_load_n(&g_mgr.shutdown, __ATOMIC_ACQUIRE)) {
        struct pollfd pfds[2];
        int nfds = 0;
        pfds[nfds].fd = cmd_fd;
        pfds[nfds].events = POLLIN;
        nfds++;
        if (g_mgr.cm_ec) {
            pfds[nfds].fd = g_mgr.cm_ec->fd;
            pfds[nfds].events = POLLIN;
            nfds++;
        }
        int pr = poll(pfds, (nfds_t)nfds, 100);
        if (pr < 0 && errno != EINTR) {
            usleep(10000);
            continue;
        }

        if (pr > 0 && (pfds[0].revents & POLLIN)) {
            fs_cmd_t cmds[16];
            ssize_t n = read(cmd_fd, cmds, sizeof(cmds));
            if (n > 0) {
                size_t count = (size_t)n / sizeof(cmds[0]);
                for (size_t i = 0; i < count; i++) {
                    if (cmds[i].op == FS_CMD_STOP) {
                        __atomic_store_n(&g_mgr.shutdown, 1, __ATOMIC_RELEASE);
                    } else if (cmds[i].op == FS_CMD_DESTROY) {
                        fs_handle_destroy_cmd(cmds[i].tid);
                    }
                }
            }
        }

        if (g_mgr.cm_ec && pr > 0) {
            struct pollfd *cm_pfd = (nfds == 2) ? &pfds[1] : NULL;
            if (cm_pfd && (cm_pfd->revents & POLLIN)) {
                struct rdma_cm_event *ev = NULL;
                if (rdma_get_cm_event(g_mgr.cm_ec, &ev) == 0 && ev) {
                    struct rdma_cm_id *id = ev->id;
                    if (ev->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
                        /* private_data 在 ack 前有效，先解码再 ack。 */
                        uint64_t tid = 0, epoch = 0;
                        int ok = rdma_fs_cm_hello_decode(ev->param.conn.private_data,
                                                         ev->param.conn.private_data_len,
                                                         &tid, &epoch) == 0;
                        rdma_ack_cm_event(ev);
                        if (!ok) {
                            /* 没有合法 v1 hello：旧客户端/未知连接，拒绝。 */
                            pthread_mutex_lock(&g_rdma_fs_mu);
                            g_mgr.cm_rejects++;
                            pthread_mutex_unlock(&g_rdma_fs_mu);
                            fprintf(stderr, "repl rdma-fs: reject CM connect "
                                            "(missing/invalid hello)\n");
                            rdma_reject(id, NULL, 0);
                            rdma_destroy_id(id);
                        } else {
                            fs_handle_connect_request(id, tid, epoch);
                        }
                    } else {
                        enum rdma_cm_event_type type = ev->event;
                        rdma_ack_cm_event(ev);
                        if (type == RDMA_CM_EVENT_ESTABLISHED) {
                            fs_handle_established(id);
                        } else if (type == RDMA_CM_EVENT_DISCONNECTED) {
                            fs_handle_cm_gone(id, "disconnected");
                        } else if (type == RDMA_CM_EVENT_DEVICE_REMOVAL) {
                            fs_handle_cm_gone(id, "device removal");
                        } else if (type == RDMA_CM_EVENT_REJECTED) {
                            fs_handle_cm_gone(id, "rejected");
                        } else if (type == RDMA_CM_EVENT_CONNECT_ERROR) {
                            fs_handle_cm_gone(id, "connect error");
                        } else if (type == RDMA_CM_EVENT_UNREACHABLE) {
                            fs_handle_cm_gone(id, "unreachable");
                        } else if (type == RDMA_CM_EVENT_ADDR_ERROR
                                   || type == RDMA_CM_EVENT_ROUTE_ERROR) {
                            fs_handle_cm_gone(id, "addr/route error");
                        } else if (type == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
                            fs_handle_cm_gone(id, "timewait exit");
                        }
                    }
                }
            }
        }

        fs_check_destroy_timeouts();
    }
    return NULL;
}

static void fs_destroy_now(rdma_fs_job_t *job) {
    struct rdma_cm_id *id = NULL;
    pthread_mutex_lock(&g_rdma_fs_mu);
    id = job->id;
    job->destroy_pending = 0;
    pthread_mutex_unlock(&g_rdma_fs_mu);
    if (!id) {
        pthread_mutex_lock(&g_rdma_fs_mu);
        job->resources_destroyed = 1;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return;
    }

    /* worker 已退出；QP -> MR -> CQ -> PD -> comp_chan 的顺序由 teardown 保证。
     * 先清 context：之后到达的 CM 事件（若有）拿到 NULL 直接忽略，避免 UAF。 */
    id->context = NULL;
    rdma_fs_job_teardown_verbs(job);
    rdma_destroy_id(id);

    pthread_mutex_lock(&g_rdma_fs_mu);
    if (job->id == id) job->id = NULL;
    job->qp = NULL;
    job->cq = NULL;
    job->pd = NULL;
    job->comp_chan = NULL;
    job->resources_destroyed = 1;
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

/* ---------------- public API ---------------- */

int rdma_fs_mgr_start(void) {
    if (g_mgr.ready) return 0;
    if (g_mgr.started) return -1;   /* 正在启动（仅 owner 线程调用，理论上不可达） */
    if (!rdma_fs_should_enable()) return -1;
    g_mgr.started = 1;

    if (pipe(g_mgr.cmd_pipe) != 0) {
        fprintf(stderr, "repl rdma-fs: pipe failed: %s\n", strerror(errno));
        g_mgr.started = 0;
        return -1;
    }
    for (int i = 0; i < 2; i++) {
        int fl = fcntl(g_mgr.cmd_pipe[i], F_GETFL, 0);
        if (fl >= 0) fcntl(g_mgr.cmd_pipe[i], F_SETFL, fl | O_NONBLOCK);
        fl = fcntl(g_mgr.cmd_pipe[i], F_GETFD, 0);
        if (fl >= 0) fcntl(g_mgr.cmd_pipe[i], F_SETFD, fl | FD_CLOEXEC);
    }

    g_mgr.cm_ec = rdma_create_event_channel();
    if (!g_mgr.cm_ec) {
        fprintf(stderr, "repl rdma-fs: create event channel failed: %s\n", strerror(errno));
        goto fail;
    }
    if (rdma_create_id(g_mgr.cm_ec, &g_mgr.listen_id, NULL, RDMA_PS_TCP) != 0) {
        fprintf(stderr, "repl rdma-fs: create listen id failed: %s\n", strerror(errno));
        goto fail;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    int port = g_cfg.rdma_port > 0 ? g_cfg.rdma_port : g_cfg.port + 1;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (rdma_bind_addr(g_mgr.listen_id, (struct sockaddr *)&addr) != 0) {
        fprintf(stderr, "repl rdma-fs: bind port %d failed: %s\n", port, strerror(errno));
        goto fail;
    }
    if (rdma_listen(g_mgr.listen_id, 16) != 0) {
        fprintf(stderr, "repl rdma-fs: listen failed: %s\n", strerror(errno));
        goto fail;
    }

    if (g_cfg.rdma_fullsync_max_concurrent > 0) {
        int v = g_cfg.rdma_fullsync_max_concurrent;
        if (v > RDMA_FS_MAX_JOBS) v = RDMA_FS_MAX_JOBS;
        g_rdma_fs_max_concurrent = v;
    }
    if (g_rdma_fs_max_concurrent < 1) g_rdma_fs_max_concurrent = 1;

    g_mgr.epoch = ((uint64_t)getpid() << 32) ^ (uint64_t)time(NULL) ^ (uint64_t)kvs_now_ms();
    if (g_mgr.epoch == 0) g_mgr.epoch = 1;
    g_mgr.next_tid = 0;
    g_rdma_fs_max_seen = 0;
    g_mgr.ready = 1;
    g_mgr.shutdown = 0;

    if (pthread_create(&g_mgr.cm_thread, NULL, fs_cm_thread, NULL) != 0) {
        fprintf(stderr, "repl rdma-fs: create CM thread failed: %s\n", strerror(errno));
        g_mgr.ready = 0;
        goto fail;
    }
    fprintf(stderr, "repl rdma-fs: manager listening on RDMA port %d (epoch=%llu, "
                    "max_concurrent=%d)\n", port,
            (unsigned long long)g_mgr.epoch, g_rdma_fs_max_concurrent);
    return 0;

fail:
    if (g_mgr.listen_id) { rdma_destroy_id(g_mgr.listen_id); g_mgr.listen_id = NULL; }
    if (g_mgr.cm_ec) { rdma_destroy_event_channel(g_mgr.cm_ec); g_mgr.cm_ec = NULL; }
    if (g_mgr.cmd_pipe[0] >= 0) { close(g_mgr.cmd_pipe[0]); g_mgr.cmd_pipe[0] = -1; }
    if (g_mgr.cmd_pipe[1] >= 0) { close(g_mgr.cmd_pipe[1]); g_mgr.cmd_pipe[1] = -1; }
    /* 允许下一次 fullsync 重试（例如同机端口暂时被 Slave 占用，换拓扑后可用）。 */
    g_mgr.started = 0;
    g_mgr.ready = 0;
    return -1;
}

void rdma_fs_mgr_stop(void) {
    if (!g_mgr.started) return;
    __atomic_store_n(&g_mgr.shutdown, 1, __ATOMIC_RELEASE);
    if (g_mgr.cmd_pipe[1] >= 0) {
        fs_cmd_t cmd = { .op = FS_CMD_STOP, .tid = 0 };
        ssize_t wn = write(g_mgr.cmd_pipe[1], &cmd, sizeof(cmd));
        (void)wn;
    }
    if (g_mgr.ready) pthread_join(g_mgr.cm_thread, NULL);
    g_mgr.ready = 0;
    if (g_mgr.listen_id) { rdma_destroy_id(g_mgr.listen_id); g_mgr.listen_id = NULL; }
    if (g_mgr.cm_ec) { rdma_destroy_event_channel(g_mgr.cm_ec); g_mgr.cm_ec = NULL; }
    if (g_mgr.cmd_pipe[0] >= 0) { close(g_mgr.cmd_pipe[0]); g_mgr.cmd_pipe[0] = -1; }
    if (g_mgr.cmd_pipe[1] >= 0) { close(g_mgr.cmd_pipe[1]); g_mgr.cmd_pipe[1] = -1; }
    g_mgr.started = 0;
}

int rdma_fs_mgr_ready(void) {
    return g_mgr.ready ? 1 : 0;
}

int rdma_fs_begin(void *owner_conn, uint64_t owner_conn_id, int slot,
                  uint64_t replica_gen, uint64_t barrier_seq,
                  uint64_t snapshot_offset, uint64_t barrier_global_seq,
                  uint64_t *tid_out, uint64_t *epoch_out) {
    if (!g_mgr.ready || !owner_conn || !tid_out || !epoch_out) return -1;

    rdma_fs_job_t *job = (rdma_fs_job_t *)calloc(1, sizeof(*job));
    if (!job) return -1;
    pthread_mutex_lock(&g_rdma_fs_mu);
    /* 有界 job 表：异常客户端反复 REPLSYNC 时拒绝新建 job（调用方回退 TCP），
     * 避免 manager 内存/线程无上限增长。 */
    unsigned live = 0;
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) live++;
    if (live >= RDMA_FS_MAX_JOBS) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        free(job);
        fprintf(stderr, "repl rdma-fs: job table full (%u), reject new fullsync\n", live);
        return -1;
    }
    job->tid = ++g_mgr.next_tid;
    job->epoch = g_mgr.epoch;
    job->owner_conn = owner_conn;
    job->owner_conn_id = owner_conn_id;
    job->slot = slot;
    job->replica_gen = replica_gen;
    job->barrier_seq = barrier_seq;
    job->snapshot_offset = snapshot_offset;
    job->barrier_global_seq = barrier_global_seq;
    job->state = RDMA_FS_ST_NEW;
    job->snap_fd = -1;
    job->resources_destroyed = 1;
    job->t_begin_ms = kvs_now_ms();

    job->next = g_mgr.jobs;
    if (g_mgr.jobs) g_mgr.jobs->prev = job;
    g_mgr.jobs = job;
    g_mgr.total_started++;

    if (pthread_create(&job->worker, NULL, rdma_fs_job_worker, job) != 0) {
        fs_free_job_locked(job);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    job->worker_started = 1;
    pthread_mutex_unlock(&g_rdma_fs_mu);

    *tid_out = job->tid;
    *epoch_out = job->epoch;
    return 0;
}

int rdma_fs_snapshot_ready(uint64_t tid, int fd, const char *path, uint64_t size) {
    if (fd < 0 || !path || size == 0) return -1;
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (!job || job->cancel || job->snap_ready) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    job->snap_fd = fd;
    snprintf(job->snap_path, sizeof(job->snap_path), "%s", path);
    job->snap_size = size;
    job->snap_ready = 1;
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return 0;
}

int rdma_fs_on_remote_mr(uint64_t tid, void *owner_conn, uint64_t addr,
                         uint32_t rkey, uint64_t capacity) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (!job || job->owner_conn != owner_conn) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 0;
    }
    if (job->cancel || job->state == RDMA_FS_ST_FAILED || job->state == RDMA_FS_ST_DONE) {
        fprintf(stderr, "repl rdma-fs: stale FULLRESYNCWR tid=%llu (state=%d), ignored\n",
                (unsigned long long)tid, job->state);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 1;   /* 命中但已失效：吞掉，不激活任何目标 */
    }
    if (addr == 0 || rkey == 0 || !job->snap_ready || capacity < job->snap_size) {
        fprintf(stderr, "repl rdma-fs: invalid FULLRESYNCWR tid=%llu addr=0x%llx "
                        "rkey=%u cap=%llu snap=%llu\n",
                (unsigned long long)tid, (unsigned long long)addr, rkey,
                (unsigned long long)capacity, (unsigned long long)job->snap_size);
        job->cancel = 1;
        job->cancel_reason = RDMA_FS_REASON_REMOTE_MR;
        job->state = RDMA_FS_ST_FAILED;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 1;
    }
    job->remote_addr = addr;
    job->remote_rkey = rkey;
    job->remote_capacity = capacity;
    job->mr_ready = 1;
    job->t_mr_ms = kvs_now_ms();
    fprintf(stderr, "repl rdma-fs: tid=%llu MR ready t=%lld cap=%llu\n",
            (unsigned long long)tid, job->t_mr_ms, (unsigned long long)capacity);
    if (job->state == RDMA_FS_ST_WAIT_MR) job->state = RDMA_FS_ST_WAIT_MR;
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return 1;
}

int rdma_fs_on_repldone(uint64_t tid, void *owner_conn, int slot,
                        uint64_t replica_gen) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (!job || job->owner_conn != owner_conn) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 0;
    }
    if (job->slot != slot || job->replica_gen != replica_gen) {
        fprintf(stderr, "repl rdma-fs: REPLDONE tid=%llu slot/gen mismatch "
                        "(job %d/%llu, msg %d/%llu), ignored\n",
                (unsigned long long)tid, job->slot,
                (unsigned long long)job->replica_gen, slot,
                (unsigned long long)replica_gen);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 0;
    }
    if (job->state == RDMA_FS_ST_DONE) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 1;   /* 重复 REPLDONE：幂等 */
    }
    if (job->state != RDMA_FS_ST_WAIT_REPLDONE) {
        fprintf(stderr, "repl rdma-fs: REPLDONE tid=%llu in state=%d, ignored\n",
                (unsigned long long)tid, job->state);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 0;
    }
    job->state = RDMA_FS_ST_DONE;
    job->notified = 1;
    job->cancel = 1;
    job->cancel_reason = RDMA_FS_REASON_CANCELLED;
    g_mgr.total_completed++;
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return 1;
}

void rdma_fs_cancel_owner(void *owner_conn) {
    if (!owner_conn) return;
    pthread_mutex_lock(&g_rdma_fs_mu);
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        if (j->owner_conn != owner_conn) continue;
        j->owner_conn = NULL;
        j->notified = 1;
        j->cancel = 1;
        if (!j->cancel_reason) j->cancel_reason = RDMA_FS_REASON_OWNER_CLOSED;
        if (j->state != RDMA_FS_ST_DONE && j->state != RDMA_FS_ST_FAILED)
            j->state = RDMA_FS_ST_FAILED;
    }
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

void rdma_fs_cancel(uint64_t tid, int reason) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (job) {
        job->cancel = 1;
        job->notified = 1;
        if (!job->cancel_reason) job->cancel_reason = reason;
        if (job->state != RDMA_FS_ST_DONE) {
            job->state = RDMA_FS_ST_FAILED;
        }
        pthread_cond_broadcast(&g_rdma_fs_cv);
    }
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

int rdma_fs_drain_events_owner(rdma_fs_event_t *out, int max) {
    if (!out || max <= 0) return 0;

    /* 1) 回收已退出 worker（join 不能持锁）。 */
    {
        rdma_fs_job_t *to_join[RDMA_FS_MAX_JOBS];
        int n_join = 0;
        pthread_mutex_lock(&g_rdma_fs_mu);
        for (rdma_fs_job_t *j = g_mgr.jobs; j && n_join < RDMA_FS_MAX_JOBS; j = j->next) {
            if (j->worker_started && !j->worker_joined && j->worker_exited)
                to_join[n_join++] = j;
        }
        pthread_mutex_unlock(&g_rdma_fs_mu);
        for (int i = 0; i < n_join; i++) {
            pthread_join(to_join[i]->worker, NULL);
            pthread_mutex_lock(&g_rdma_fs_mu);
            to_join[i]->worker_joined = 1;
            pthread_mutex_unlock(&g_rdma_fs_mu);
        }
    }

    int n = 0;
    pthread_mutex_lock(&g_rdma_fs_mu);

    /* 2) 为终止状态的 job 安排 CM 资源销毁（worker 已 join）。 */
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        int terminal = (j->state == RDMA_FS_ST_FAILED || j->state == RDMA_FS_ST_DONE
                        || j->owner_conn == NULL);
        int worker_done = (!j->worker_started || j->worker_joined);
        if (terminal && worker_done && !j->destroy_enqueued) {
            if (j->needs_destroy) {
                if (fs_enqueue_cmd_locked(FS_CMD_DESTROY, j->tid) == 0)
                    j->destroy_enqueued = 1;
            } else {
                j->destroy_enqueued = 1;
            }
        }
    }

    /* 3) 生成 owner 事件。 */
    for (rdma_fs_job_t *j = g_mgr.jobs; j && n < max; j = j->next) {
        if (j->state == RDMA_FS_ST_WR_DONE && !j->notified) {
            rdma_fs_event_t *e = &out[n++];
            memset(e, 0, sizeof(*e));
            e->type = RDMA_FS_EV_WR_DONE;
            e->slot = j->slot;
            e->tid = j->tid;
            e->owner_conn = j->owner_conn;
            e->bytes_total = j->snap_size;
            e->bytes_completed = j->posted_bytes;
            j->notified = 1;
            j->state = RDMA_FS_ST_WAIT_REPLDONE;
            j->t_done_ms = kvs_now_ms();
        } else if (j->state == RDMA_FS_ST_FAILED && !j->notified) {
            rdma_fs_event_t *e = &out[n++];
            memset(e, 0, sizeof(*e));
            e->type = RDMA_FS_EV_FAILED;
            e->reason = j->cancel_reason;
            e->slot = j->slot;
            e->tid = j->tid;
            e->owner_conn = j->owner_conn;
            e->bytes_total = j->snap_size;
            e->bytes_completed = j->posted_bytes;
            j->notified = 1;
            if (!j->failed_counted) { g_mgr.total_failed++; j->failed_counted = 1; }
            if (j->cq_error && !j->cq_counted) { g_mgr.cq_errors++; j->cq_counted = 1; }
        }
    }

    /* 4) 释放已彻底销毁的 job。 */
    {
        rdma_fs_job_t *j = g_mgr.jobs;
        while (j) {
            rdma_fs_job_t *next = j->next;
            int joined = !j->worker_started || j->worker_joined;
            int destroyed = !j->needs_destroy || j->resources_destroyed;
            int terminal = (j->state == RDMA_FS_ST_FAILED || j->state == RDMA_FS_ST_DONE
                            || j->owner_conn == NULL);
            if (joined && destroyed && terminal && j->notified && j->destroy_enqueued) {
                if (j->state == RDMA_FS_ST_FAILED && !j->failed_counted) {
                    g_mgr.total_failed++;
                    j->failed_counted = 1;
                }
                if (j->cq_error && !j->cq_counted) {
                    g_mgr.cq_errors++;
                    j->cq_counted = 1;
                }
                fs_free_job_locked(j);
            }
            j = next;
        }
    }

    unsigned active = 0, waiting = 0, sending = 0, conc = 0;
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        active++;
        if (j->state == RDMA_FS_ST_WAIT_TOKEN || j->state == RDMA_FS_ST_WAIT_MR
            || j->state == RDMA_FS_ST_NEW || j->state == RDMA_FS_ST_WAIT_ESTABLISHED)
            waiting++;
        if (j->state == RDMA_FS_ST_SENDING) { sending++; conc++; }
    }
    (void)conc;

    pthread_mutex_unlock(&g_rdma_fs_mu);
    return n;
}

void rdma_fs_get_stats(rdma_fs_stats_t *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
    pthread_mutex_lock(&g_rdma_fs_mu);
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        out->active_jobs++;
        if (j->state == RDMA_FS_ST_SENDING) out->sending_jobs++;
        else if (j->state == RDMA_FS_ST_NEW || j->state == RDMA_FS_ST_WAIT_ESTABLISHED
                 || j->state == RDMA_FS_ST_WAIT_MR || j->state == RDMA_FS_ST_WAIT_TOKEN)
            out->waiting_jobs++;
        out->registered_bytes += (unsigned long long)j->slot_count * j->chunk_size;
    }
    out->total_started = g_mgr.total_started;
    out->total_completed = g_mgr.total_completed;
    out->total_failed = g_mgr.total_failed;
    out->cm_rejects = g_mgr.cm_rejects;
    out->cq_errors = g_mgr.cq_errors;
    out->max_concurrency_seen = g_rdma_fs_max_seen;
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

#else /* !KVS_ENABLE_RDMA */

int rdma_fs_should_enable(void) { return 0; }
int rdma_fs_mgr_start(void) { return -1; }
void rdma_fs_mgr_stop(void) {}
int rdma_fs_mgr_ready(void) { return 0; }
int rdma_fs_begin(void *owner_conn, uint64_t owner_conn_id, int slot,
                  uint64_t replica_gen, uint64_t barrier_seq,
                  uint64_t snapshot_offset, uint64_t barrier_global_seq,
                  uint64_t *tid_out, uint64_t *epoch_out) {
    (void)owner_conn; (void)owner_conn_id; (void)slot; (void)replica_gen;
    (void)barrier_seq; (void)snapshot_offset; (void)barrier_global_seq;
    (void)tid_out; (void)epoch_out;
    return -1;
}
int rdma_fs_snapshot_ready(uint64_t tid, int fd, const char *path, uint64_t size) {
    (void)tid; (void)fd; (void)path; (void)size; return -1;
}
int rdma_fs_on_remote_mr(uint64_t tid, void *owner_conn, uint64_t addr,
                         uint32_t rkey, uint64_t capacity) {
    (void)tid; (void)owner_conn; (void)addr; (void)rkey; (void)capacity; return 0;
}
int rdma_fs_on_repldone(uint64_t tid, void *owner_conn, int slot,
                        uint64_t replica_gen) {
    (void)tid; (void)owner_conn; (void)slot; (void)replica_gen; return 0;
}
void rdma_fs_cancel_owner(void *owner_conn) { (void)owner_conn; }
void rdma_fs_cancel(uint64_t tid, int reason) { (void)tid; (void)reason; }
int rdma_fs_drain_events_owner(rdma_fs_event_t *out, int max) { (void)out; (void)max; return 0; }
void rdma_fs_get_stats(rdma_fs_stats_t *out) { if (out) memset(out, 0, sizeof(*out)); }

#endif /* KVS_ENABLE_RDMA */
