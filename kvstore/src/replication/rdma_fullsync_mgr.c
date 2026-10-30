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
    case RDMA_FS_REASON_CONTROL_WRITE: return "control_write_error";
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
static int g_rdma_fs_repldone_timeout_ms = 60000;
static int g_rdma_fs_max_connected = 4;
static unsigned long long g_rdma_fs_max_registered_bytes = 64ULL * 1024 * 1024;

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
    unsigned long long wc_retry_exc_err;
    unsigned long long wc_flush_err;
    unsigned long long wc_remote_access_err;
    unsigned long long wc_local_prot_err;
    unsigned long long wc_rnr_retry_exc_err;
    unsigned long long wc_other_errors;
    unsigned long long post_send_errors;
    unsigned long long poll_cq_failures;
    unsigned long long cm_disconnects;
    unsigned long long registered_bytes;
    unsigned long long resource_rejects;
} g_mgr = { .cmd_pipe = { -1, -1 } };

/* 最近 8 次 job 首发错误：job 销毁后仍可在 INFO 中定位第一现场。 */
typedef struct {
    int valid;
    uint64_t tid;
    int slot, source, wc_status, post_rc, inflight;
    uint32_t vendor, qp_num;
    uint64_t wr_id, posted, completed;
    long long ts_ms;
} fs_first_error_entry_t;
static fs_first_error_entry_t g_fe_ring[8];
static unsigned g_fe_next;
static pthread_mutex_t g_fe_mu = PTHREAD_MUTEX_INITIALIZER;

void rdma_fs_remember_first_error(uint64_t tid, int slot, int source, int wc_status,
                                  uint32_t vendor, uint32_t qp_num, uint64_t wr_id,
                                  int post_rc, uint64_t posted, uint64_t completed,
                                  int inflight, long long ts_ms) {
    pthread_mutex_lock(&g_fe_mu);
    fs_first_error_entry_t *e = &g_fe_ring[g_fe_next++ % 8];
    e->valid = 1;
    e->tid = tid; e->slot = slot; e->source = source; e->wc_status = wc_status;
    e->post_rc = post_rc; e->inflight = inflight; e->vendor = vendor;
    e->qp_num = qp_num; e->wr_id = wr_id; e->posted = posted; e->completed = completed;
    e->ts_ms = ts_ms;
    pthread_mutex_unlock(&g_fe_mu);
}

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
    /* owner 事件 drain 只接入 reactor 主循环；其他网络后端继续走旧路径。 */
    if (strcmp(g_cfg.net_backend, "reactor") != 0) return 0;
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
    /* 资源预算：连接数 + 注册发送 buffer 总量。超额在分配 PD/CQ/QP/MR 之前拒绝，
     * 并把该 job 置 FAILED，让 owner 尽快 fail-closed 该副本并触发重试。 */
    unsigned connected = 0;
    unsigned long long reg = 0;
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        if (j->cm_setup || (j->needs_destroy && !j->resources_destroyed)) {
            connected++;
            reg += (unsigned long long)j->slot_count * j->chunk_size;
        }
    }
    int est_slots = 0;
    uint64_t est_bytes = 0;
    rdma_fs_estimate_resources(&est_slots, &est_bytes);
    if (connected >= (unsigned)g_rdma_fs_max_connected
        || reg + est_bytes > g_rdma_fs_max_registered_bytes) {
        g_mgr.resource_rejects++;
        job->cancel = 1;
        job->cancel_reason = RDMA_FS_REASON_RESOURCE;
        job->state = RDMA_FS_ST_FAILED;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        fprintf(stderr, "repl rdma-fs: tid=%llu rejected by resource budget "
                        "(connected=%u/%d reg=%llu+%llu>%llu)\n",
                (unsigned long long)tid, connected, g_rdma_fs_max_connected,
                reg, (unsigned long long)est_bytes, g_rdma_fs_max_registered_bytes);
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        return -1;
    }

    job->id = id;
    id->context = job;
    job->needs_destroy = 1;
    job->resources_destroyed = 0;
    job->cm_setup = 1;
    job->state = RDMA_FS_ST_CM_SETUP;
    g_mgr.registered_bytes += 0;
    fprintf(stderr, "repl rdma-fs: CM connect tid=%llu t=%lld (waiting_jobs=%d)\n",
            (unsigned long long)tid, kvs_now_ms(), g_rdma_fs_sending);
    pthread_mutex_unlock(&g_rdma_fs_mu);

    if (rdma_fs_job_setup_verbs(job) != 0) {
        fprintf(stderr, "repl rdma-fs: tid=%llu resource setup failed\n",
                (unsigned long long)tid);
        rdma_reject(id, NULL, 0);
        pthread_mutex_lock(&g_rdma_fs_mu);
        job->cm_setup = 0;
        job->cm_disconnected = 1;
        job->cancel = 1;
        job->cancel_reason = RDMA_FS_REASON_RESOURCE;
        job->state = RDMA_FS_ST_FAILED;
        pthread_cond_broadcast(&g_rdma_fs_cv);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }

    pthread_mutex_lock(&g_rdma_fs_mu);
    job->cm_setup = 0;
    int aborted = job->cancel || job->owner_conn == NULL;
    if (!aborted && job->state == RDMA_FS_ST_CM_SETUP)
        job->state = RDMA_FS_ST_WAIT_ESTABLISHED;
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
        job->cm_setup = 0;
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
            rdma_fs_note_cm_disconnect();
            if (!job->fe_valid) {
                job->fe_valid = 1;
                job->fe_source = 4; /* CM */
                job->fe_wc_status = -1;
                job->fe_qp_num = job->qp ? job->qp->qp_num : 0;
                job->fe_posted_bytes = job->posted_bytes;
                job->fe_completed_bytes = job->completed_bytes;
                job->fe_inflight = job->inflight_wr;
                job->fe_ts_ms = kvs_now_ms();
                rdma_fs_remember_first_error(job->tid, job->slot, job->fe_source,
                    job->fe_wc_status, job->fe_vendor_err, job->fe_qp_num, job->fe_wr_id,
                    job->fe_post_rc, job->fe_posted_bytes, job->fe_completed_bytes,
                    job->fe_inflight, job->fe_ts_ms);
                fprintf(stderr, "repl rdma-fs: FIRST ERROR tid=%llu slot=%d source=CM "
                        "why=%s posted=%llu/%llu completed=%llu inflight=%d ts_ms=%lld\n",
                        (unsigned long long)job->tid, job->slot, why,
                        (unsigned long long)job->posted_bytes,
                        (unsigned long long)job->snap_size,
                        (unsigned long long)job->completed_bytes,
                        job->inflight_wr, job->fe_ts_ms);
            }
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
    g_rdma_fs_repldone_timeout_ms = g_cfg.rdma_fullsync_repldone_timeout_ms;
    if (g_rdma_fs_repldone_timeout_ms < 1000) g_rdma_fs_repldone_timeout_ms = 1000;
    if (g_rdma_fs_repldone_timeout_ms > 600000) g_rdma_fs_repldone_timeout_ms = 600000;
    g_rdma_fs_max_connected = g_cfg.rdma_fullsync_max_connected;
    if (g_rdma_fs_max_connected < 1) g_rdma_fs_max_connected = 1;
    if (g_rdma_fs_max_connected > RDMA_FS_MAX_JOBS) g_rdma_fs_max_connected = RDMA_FS_MAX_JOBS;
    int reg_mb = g_cfg.rdma_fullsync_max_registered_mb;
    if (reg_mb < 1) reg_mb = 1;
    if (reg_mb > 65536) reg_mb = 65536;
    g_rdma_fs_max_registered_bytes = (unsigned long long)reg_mb * 1024ULL * 1024ULL;

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
                    "max_concurrent=%d, max_connected=%d, max_registered=%lluMB)\n",
            port, (unsigned long long)g_mgr.epoch, g_rdma_fs_max_concurrent,
            g_rdma_fs_max_connected,
            (unsigned long long)(g_rdma_fs_max_registered_bytes >> 20));
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

    /* 1) 取消所有活动 job 并唤醒等待中的 worker。 */
    pthread_mutex_lock(&g_rdma_fs_mu);
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        j->cancel = 1;
        j->notified = 1;
        if (!j->cancel_reason) j->cancel_reason = RDMA_FS_REASON_CANCELLED;
        if (j->state != RDMA_FS_ST_DONE) j->state = RDMA_FS_ST_FAILED;
        j->wait_deadline_ms = 0;
    }
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);

    /* 2) 停止 CM 线程（之后由本线程独占所有 CM/verbs 资源）。 */
    if (g_mgr.cmd_pipe[1] >= 0) {
        fs_cmd_t cmd = { .op = FS_CMD_STOP, .tid = 0 };
        ssize_t wn = write(g_mgr.cmd_pipe[1], &cmd, sizeof(cmd));
        (void)wn;
    }
    if (g_mgr.ready) pthread_join(g_mgr.cm_thread, NULL);
    g_mgr.ready = 0;

    /* 3) join 所有 worker。 */
    for (;;) {
        rdma_fs_job_t *target = NULL;
        pthread_mutex_lock(&g_rdma_fs_mu);
        for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
            if (j->worker_started && !j->worker_joined) { target = j; break; }
        }
        pthread_mutex_unlock(&g_rdma_fs_mu);
        if (!target) break;
        pthread_join(target->worker, NULL);
        pthread_mutex_lock(&g_rdma_fs_mu);
        target->worker_joined = 1;
        pthread_mutex_unlock(&g_rdma_fs_mu);
    }

    /* 4) 逐 job teardown（QP -> MR -> CQ -> PD -> comp_chan -> child id）并释放快照。 */
    for (;;) {
        rdma_fs_job_t *job = NULL;
        pthread_mutex_lock(&g_rdma_fs_mu);
        job = g_mgr.jobs;
        if (job) fs_unlink_locked(job);
        pthread_mutex_unlock(&g_rdma_fs_mu);
        if (!job) break;
        if (job->id) {
            job->id->context = NULL;
            rdma_fs_job_teardown_verbs(job);
            rdma_destroy_id(job->id);
            job->id = NULL;
        }
        if (job->snap_fd >= 0) {
            close(job->snap_fd);
            job->snap_fd = -1;
        }
        if (job->snap_path[0]) unlink(job->snap_path);
        free(job);
    }

    /* 5) listener / event channel / command pipe。 */
    if (g_mgr.listen_id) { rdma_destroy_id(g_mgr.listen_id); g_mgr.listen_id = NULL; }
    if (g_mgr.cm_ec) { rdma_destroy_event_channel(g_mgr.cm_ec); g_mgr.cm_ec = NULL; }
    if (g_mgr.cmd_pipe[0] >= 0) { close(g_mgr.cmd_pipe[0]); g_mgr.cmd_pipe[0] = -1; }
    if (g_mgr.cmd_pipe[1] >= 0) { close(g_mgr.cmd_pipe[1]); g_mgr.cmd_pipe[1] = -1; }
    g_mgr.started = 0;
    g_mgr.shutdown = 0;
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
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        live++;
        /* 同一 replica 连接/slot 同时只允许一个活动 job：重复 REPLSYNC
         * 不得在旧 Barrier/旧 job 上再叠一个新任务。 */
        /* slot=-1 表示 proxy 未注册（如无 eBPF 的环境），不能按 slot 去重；
         * 此时只靠 owner_conn 保证一连接一个 job。 */
        if (((j->owner_conn == owner_conn)
             || (slot >= 0 && j->slot == slot && j->replica_gen == replica_gen))
            && j->state != RDMA_FS_ST_FAILED && j->state != RDMA_FS_ST_DONE) {
            pthread_mutex_unlock(&g_rdma_fs_mu);
            free(job);
            fprintf(stderr, "repl rdma-fs: duplicate job for slot=%d conn=%p, reject\n",
                    slot, owner_conn);
            return -1;
        }
    }
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
    job->state = RDMA_FS_ST_WAIT_HEADER;
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
    job->t_snap_ready_ms = kvs_now_ms();
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return 0;
}

int rdma_fs_header_sent(uint64_t tid) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (!job || job->cancel) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    if (job->header_sent) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 0;   /* 幂等 */
    }
    if (job->state != RDMA_FS_ST_WAIT_HEADER) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    job->header_sent = 1;
    job->t_header_sent_ms = kvs_now_ms();
    job->state = RDMA_FS_ST_WAIT_ESTABLISHED;
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
    job->t_repldone_ms = kvs_now_ms();
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

int rdma_fs_mark_fullsyncend_sent(uint64_t tid) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    rdma_fs_job_t *job = fs_find_locked(tid);
    if (!job) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    if (job->state == RDMA_FS_ST_WAIT_REPLDONE) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return 0;   /* 幂等 */
    }
    if (job->state != RDMA_FS_ST_WR_DONE) {
        pthread_mutex_unlock(&g_rdma_fs_mu);
        return -1;
    }
    job->state = RDMA_FS_ST_WAIT_REPLDONE;
    job->t_end_sent_ms = kvs_now_ms();
    job->wait_deadline_ms = job->t_end_sent_ms + g_rdma_fs_repldone_timeout_ms;
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return 0;
}

int rdma_fs_format_jobs(char *out, size_t cap) {
    if (!out || cap == 0) return 0;
    out[0] = '\0';
    size_t pos = 0;
    pthread_mutex_lock(&g_rdma_fs_mu);
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        long long snap_ms = j->t_snap_ready_ms ? j->t_snap_ready_ms - j->t_begin_ms : -1;
        long long header_ms = j->t_header_sent_ms ? j->t_header_sent_ms - j->t_begin_ms : -1;
        long long cm_ms = (j->t_established_ms && j->t_header_sent_ms)
            ? j->t_established_ms - j->t_header_sent_ms : -1;
        long long mr_ms = (j->t_mr_ms && j->t_established_ms)
            ? j->t_mr_ms - j->t_established_ms : -1;
        long long token_ms = (j->t_send_start_ms && j->t_mr_ms)
            ? j->t_send_start_ms - j->t_mr_ms : -1;
        long long send_ms = (j->t_done_ms && j->t_send_start_ms)
            ? j->t_done_ms - j->t_send_start_ms : -1;
        long long end_ms = (j->t_end_sent_ms && j->t_done_ms)
            ? j->t_end_sent_ms - j->t_done_ms : -1;
        long long ack_ms = (j->t_repldone_ms && j->t_end_sent_ms)
            ? j->t_repldone_ms - j->t_end_sent_ms : -1;
        int n = snprintf(out + pos, cap - pos,
                         "%stid=%llu slot=%d gen=%llu state=%d posted=%llu "
                         "completed=%llu snap_ms=%lld header_ms=%lld cm_ms=%lld "
                         "mr_ms=%lld token_ms=%lld send_ms=%lld end_ms=%lld ack_ms=%lld",
                         pos ? ";" : "",
                         (unsigned long long)j->tid, j->slot,
                         (unsigned long long)j->replica_gen, j->state,
                         (unsigned long long)j->posted_bytes,
                         (unsigned long long)j->completed_bytes,
                         snap_ms, header_ms, cm_ms, mr_ms, token_ms, send_ms,
                         end_ms, ack_ms);
        if (n < 0 || (size_t)n >= cap - pos) break;
        pos += (size_t)n;
    }
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return (int)pos;
}

int rdma_fs_format_first_errors(char *out, size_t cap) {
    if (!out || cap == 0) return 0;
    out[0] = '\0';
    size_t pos = 0;
    pthread_mutex_lock(&g_fe_mu);
    for (unsigned i = 0; i < 8; i++) {
        fs_first_error_entry_t *e = &g_fe_ring[i];
        if (!e->valid) continue;
        const char *status = (e->wc_status >= 0)
            ? ibv_wc_status_str((enum ibv_wc_status)e->wc_status) : "n/a";
        int n = snprintf(out + pos, cap - pos,
            "%stid=%llu slot=%d source=%d wc=%d(%s) vendor=%u qp=%u wr=%llu "
            "post_rc=%d posted=%llu completed=%llu inflight=%d ts_ms=%lld",
            pos ? ";" : "", (unsigned long long)e->tid, e->slot, e->source,
            e->wc_status, status, e->vendor, e->qp_num, (unsigned long long)e->wr_id,
            e->post_rc, (unsigned long long)e->posted,
            (unsigned long long)e->completed, e->inflight, e->ts_ms);
        if (n < 0 || (size_t)n >= cap - pos) break;
        pos += (size_t)n;
    }
    pthread_mutex_unlock(&g_fe_mu);
    return (int)pos;
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

    /* 2) 任务级完成超时：
     *    - WR_DONE 已通知 owner，但 FULLSYNCEND 迟迟未确认发送成功；
     *    - WAIT_REPLDONE 已成功发送 END，但 Slave 长时间不回报 REPLDONE。
     * 只把该 job 置 FAILED，下一轮生成 FAILED 事件由 owner 隔离该副本。 */
    long long now_ms = kvs_now_ms();
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        int in_wait = ((j->state == RDMA_FS_ST_WR_DONE && j->notified)
                       || j->state == RDMA_FS_ST_WAIT_REPLDONE);
        if (in_wait && j->wait_deadline_ms > 0 && now_ms >= j->wait_deadline_ms) {
            fprintf(stderr, "repl rdma-fs: tid=%llu completion timeout in state=%d "
                            "(posted=%llu/%llu), fail-closed\n",
                    (unsigned long long)j->tid, j->state,
                    (unsigned long long)j->posted_bytes,
                    (unsigned long long)j->snap_size);
            j->state = RDMA_FS_ST_FAILED;
            j->cancel = 1;
            j->cancel_reason = RDMA_FS_REASON_TIMEOUT;
            j->notified = 0;   /* 允许下面的 FAILED 事件通知 owner */
            j->wait_deadline_ms = 0;
            pthread_cond_broadcast(&g_rdma_fs_cv);
        }
    }

    /* 3) 为终止状态的 job 安排 CM 资源销毁（worker 已 join）。 */
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
            e->replica_gen = j->replica_gen;
            e->barrier_seq = j->barrier_seq;
            e->owner_conn = j->owner_conn;
            e->bytes_total = j->snap_size;
            e->bytes_completed = j->posted_bytes;
            j->notified = 1;
            /* 只有 owner 成功写完 FULLSYNCEND 后才进入 WAIT_REPLDONE
             * （rdma_fs_mark_fullsyncend_sent）；超时防止 owner 侧漏处理。 */
            j->wait_deadline_ms = now_ms + g_rdma_fs_repldone_timeout_ms;
            j->t_done_ms = kvs_now_ms();
        } else if (j->state == RDMA_FS_ST_FAILED && !j->notified) {
            rdma_fs_event_t *e = &out[n++];
            memset(e, 0, sizeof(*e));
            e->type = RDMA_FS_EV_FAILED;
            e->reason = j->cancel_reason;
            e->slot = j->slot;
            e->tid = j->tid;
            e->replica_gen = j->replica_gen;
            e->barrier_seq = j->barrier_seq;
            e->owner_conn = j->owner_conn;
            e->bytes_total = j->snap_size;
            e->bytes_completed = j->posted_bytes;
            j->notified = 1;
            j->wait_deadline_ms = 0;
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
            || j->state == RDMA_FS_ST_NEW || j->state == RDMA_FS_ST_WAIT_HEADER
            || j->state == RDMA_FS_ST_CM_SETUP
            || j->state == RDMA_FS_ST_WAIT_ESTABLISHED)
            waiting++;
        if (j->state == RDMA_FS_ST_SENDING) { sending++; conc++; }
    }
    (void)conc;

    pthread_mutex_unlock(&g_rdma_fs_mu);
    return n;
}

void rdma_fs_note_wc_error(int wc_status) {
    switch (wc_status) {
    case IBV_WC_RETRY_EXC_ERR:
        __atomic_add_fetch(&g_mgr.wc_retry_exc_err, 1, __ATOMIC_RELAXED); break;
    case IBV_WC_WR_FLUSH_ERR:
        __atomic_add_fetch(&g_mgr.wc_flush_err, 1, __ATOMIC_RELAXED); break;
    case IBV_WC_REM_ACCESS_ERR:
        __atomic_add_fetch(&g_mgr.wc_remote_access_err, 1, __ATOMIC_RELAXED); break;
    case IBV_WC_LOC_PROT_ERR:
    case IBV_WC_LOC_ACCESS_ERR:
        __atomic_add_fetch(&g_mgr.wc_local_prot_err, 1, __ATOMIC_RELAXED); break;
    case IBV_WC_RNR_RETRY_EXC_ERR:
        __atomic_add_fetch(&g_mgr.wc_rnr_retry_exc_err, 1, __ATOMIC_RELAXED); break;
    default:
        __atomic_add_fetch(&g_mgr.wc_other_errors, 1, __ATOMIC_RELAXED); break;
    }
}
void rdma_fs_note_post_send_error(void) {
    __atomic_add_fetch(&g_mgr.post_send_errors, 1, __ATOMIC_RELAXED);
}
void rdma_fs_note_poll_cq_failure(void) {
    __atomic_add_fetch(&g_mgr.poll_cq_failures, 1, __ATOMIC_RELAXED);
}
void rdma_fs_note_cm_disconnect(void) {
    __atomic_add_fetch(&g_mgr.cm_disconnects, 1, __ATOMIC_RELAXED);
}

void rdma_fs_get_stats(rdma_fs_stats_t *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
    pthread_mutex_lock(&g_rdma_fs_mu);
    unsigned connected = 0;
    unsigned long long reg = 0;
    for (rdma_fs_job_t *j = g_mgr.jobs; j; j = j->next) {
        out->active_jobs++;
        if (j->state == RDMA_FS_ST_SENDING) {
            out->sending_jobs++;
            if (j->posted_bytes > 0) out->sending_posted_jobs++;
        }
        if (j->posted_bytes > out->max_posted_bytes)
            out->max_posted_bytes = j->posted_bytes;
        else if (j->state == RDMA_FS_ST_NEW || j->state == RDMA_FS_ST_WAIT_HEADER
                 || j->state == RDMA_FS_ST_CM_SETUP
                 || j->state == RDMA_FS_ST_WAIT_ESTABLISHED
                 || j->state == RDMA_FS_ST_WAIT_MR || j->state == RDMA_FS_ST_WAIT_TOKEN)
            out->waiting_jobs++;
        if (j->cm_setup || (j->needs_destroy && !j->resources_destroyed)) {
            connected++;
            reg += (unsigned long long)j->slot_count * j->chunk_size;
        }
    }
    out->registered_bytes = reg;
    out->connected_jobs = connected;
    out->resource_rejects = g_mgr.resource_rejects;
    out->max_connected = (unsigned)g_rdma_fs_max_connected;
    out->max_registered_bytes = g_rdma_fs_max_registered_bytes;
    out->total_started = g_mgr.total_started;
    out->total_completed = g_mgr.total_completed;
    out->total_failed = g_mgr.total_failed;
    out->cm_rejects = g_mgr.cm_rejects;
    out->cq_errors = __atomic_load_n(&g_mgr.cq_errors, __ATOMIC_RELAXED);
    out->wc_retry_exc_err = __atomic_load_n(&g_mgr.wc_retry_exc_err, __ATOMIC_RELAXED);
    out->wc_flush_err = __atomic_load_n(&g_mgr.wc_flush_err, __ATOMIC_RELAXED);
    out->wc_remote_access_err = __atomic_load_n(&g_mgr.wc_remote_access_err, __ATOMIC_RELAXED);
    out->wc_local_prot_err = __atomic_load_n(&g_mgr.wc_local_prot_err, __ATOMIC_RELAXED);
    out->wc_rnr_retry_exc_err = __atomic_load_n(&g_mgr.wc_rnr_retry_exc_err, __ATOMIC_RELAXED);
    out->wc_other_errors = __atomic_load_n(&g_mgr.wc_other_errors, __ATOMIC_RELAXED);
    out->post_send_errors = __atomic_load_n(&g_mgr.post_send_errors, __ATOMIC_RELAXED);
    out->poll_cq_failures = __atomic_load_n(&g_mgr.poll_cq_failures, __ATOMIC_RELAXED);
    out->cm_disconnects = __atomic_load_n(&g_mgr.cm_disconnects, __ATOMIC_RELAXED);
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
int rdma_fs_header_sent(uint64_t tid) { (void)tid; return -1; }
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
int rdma_fs_mark_fullsyncend_sent(uint64_t tid) { (void)tid; return -1; }

int rdma_fs_format_jobs(char *out, size_t cap) {
    if (out && cap) out[0] = '\0';
    return 0;
}
int rdma_fs_format_first_errors(char *out, size_t cap) {
    if (out && cap) out[0] = '\0';
    return 0;
}
void rdma_fs_note_wc_error(int s) { (void)s; }
void rdma_fs_note_post_send_error(void) {}
void rdma_fs_note_poll_cq_failure(void) {}
void rdma_fs_note_cm_disconnect(void) {}
void rdma_fs_remember_first_error(uint64_t tid, int slot, int source, int wc_status,
                                  uint32_t vendor, uint32_t qp_num, uint64_t wr_id,
                                  int post_rc, uint64_t posted, uint64_t completed,
                                  int inflight, long long ts_ms) {
    (void)tid; (void)slot; (void)source; (void)wc_status; (void)vendor; (void)qp_num;
    (void)wr_id; (void)post_rc; (void)posted; (void)completed; (void)inflight; (void)ts_ms;
}
int rdma_fs_drain_events_owner(rdma_fs_event_t *out, int max) { (void)out; (void)max; return 0; }
void rdma_fs_get_stats(rdma_fs_stats_t *out) { if (out) memset(out, 0, sizeof(*out)); }

#endif /* KVS_ENABLE_RDMA */
