/*
 * per-transfer RDMA 全量发送实现。
 *
 * 每个 rdma_fs_job_t 独占：PD / CQ / comp_chan / QP / send slots / 远端 MR /
 * 发送进度。worker 只操作本 job 的资源，绝不触碰全局 g_repl_rdma_ctx 或 conn_t。
 *
 * 完成语义：
 *   - 全部 WRITE 使用 IBV_SEND_SIGNALED，逐个 CQE 回收 slot；
 *   - 只有 posted_bytes == snap_size 且全部在途 WR 成功完成时，worker 才把 job
 *     置 WR_DONE；任何 CQ error / pread 失败 / post 失败 / 超时都置 FAILED；
 *   - owner 收到 WR_DONE 后才发 FULLSYNCEND（见 kvstore.c），因此不会在数据
 *     未落定时通知 Slave finalize。
 */

#include "rdma_fullsync_internal.h"

#if KVS_ENABLE_RDMA

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* g_rdma_fs_mu/cv/sending/max_concurrent/max_seen 由 rdma_fullsync_mgr.c 定义。 */

#define RDMA_FS_DEFAULT_SEND_SLOTS 16
#define RDMA_FS_DEFAULT_CHUNK      (256u * 1024u)
#define RDMA_FS_DEFAULT_QP_DEPTH   64
#define RDMA_FS_MAX_CHUNK          (4u * 1024u * 1024u)

static long long fs_cfg_send_slots(void) {
    int v = g_cfg.rdma_send_slots;
    if (v <= 0) v = RDMA_FS_DEFAULT_SEND_SLOTS;
    if (v > RDMA_FS_MAX_SLOTS) v = RDMA_FS_MAX_SLOTS;
    return v;
}

static size_t fs_cfg_chunk(void) {
    size_t v = (g_cfg.rdma_chunk_size > 0) ? (size_t)g_cfg.rdma_chunk_size
                                           : (size_t)RDMA_FS_DEFAULT_CHUNK;
    if (v < 1024) v = 1024;
    if (v > RDMA_FS_MAX_CHUNK) v = RDMA_FS_MAX_CHUNK;
    return v;
}

static int fs_cfg_qp_depth(int slots) {
    int v = g_cfg.rdma_qp_wr_depth;
    if (v <= 0) v = RDMA_FS_DEFAULT_QP_DEPTH;
    if (v < slots + 2) v = slots + 2;
    return v;
}

/* CM 线程在 setup 阶段检查 worker/owner 是否已取消。 */
static int fs_job_cancel_requested(rdma_fs_job_t *job) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    int c = job->cancel;
    pthread_mutex_unlock(&g_rdma_fs_mu);
    return c;
}

/* 构造 CLOCK_REALTIME 绝对超时，供 pthread_cond_timedwait 使用。 */
static struct timespec fs_deadline_after_ms(long long ms) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += (time_t)(ms / 1000);
    ts.tv_nsec += (long)((ms % 1000) * 1000000LL);
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }
    return ts;
}

int rdma_fs_job_setup_verbs(rdma_fs_job_t *job) {
    if (!job || !job->id || !job->id->verbs) return -1;

    size_t chunk = fs_cfg_chunk();
    int slots = (int)fs_cfg_send_slots();
    int depth = fs_cfg_qp_depth(slots);
    pthread_mutex_lock(&g_rdma_fs_mu);
    job->chunk_size = chunk;
    job->slot_count = slots;
    job->qp_depth = depth;
    job->pause_us = g_cfg.rdma_fullsync_pause_us > 0 ? g_cfg.rdma_fullsync_pause_us : 0;
    /* 防止异常配置把 reactor drain（pthread_join）与发送线程长时间卡住。 */
    if (job->pause_us > 100000) job->pause_us = 100000;
    pthread_mutex_unlock(&g_rdma_fs_mu);

    if (fs_job_cancel_requested(job)) return -1;
    job->comp_chan = ibv_create_comp_channel(job->id->verbs);
    if (!job->comp_chan) {
        fprintf(stderr, "repl rdma-fs: tid=%llu create_comp_channel failed: %s\n",
                (unsigned long long)job->tid, strerror(errno));
        return -1;
    }

    if (fs_job_cancel_requested(job)) return -1;
    job->pd = ibv_alloc_pd(job->id->verbs);
    if (!job->pd) {
        fprintf(stderr, "repl rdma-fs: tid=%llu alloc_pd failed: %s\n",
                (unsigned long long)job->tid, strerror(errno));
        ibv_destroy_comp_channel(job->comp_chan);
        job->comp_chan = NULL;
        return -1;
    }

    if (fs_job_cancel_requested(job)) return -1;
    job->cq = ibv_create_cq(job->id->verbs, (int)job->qp_depth, NULL,
                            job->comp_chan, 0);
    if (!job->cq) {
        fprintf(stderr, "repl rdma-fs: tid=%llu create_cq failed: %s\n",
                (unsigned long long)job->tid, strerror(errno));
        return -1;
    }

    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.send_cq = job->cq;
    attr.recv_cq = job->cq;
    attr.qp_type = IBV_QPT_RC;
    attr.cap.max_send_wr = (uint32_t)job->qp_depth;
    attr.cap.max_recv_wr = 1;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    if (fs_job_cancel_requested(job)) return -1;
    if (rdma_create_qp(job->id, job->pd, &attr) != 0) {
        fprintf(stderr, "repl rdma-fs: tid=%llu create_qp failed: %s\n",
                (unsigned long long)job->tid, strerror(errno));
        return -1;
    }
    /* rdma_create_qp 把 QP 写到 id->qp；worker/teardown 使用 job->qp。 */
    job->qp = job->id->qp;
    if (!job->qp) return -1;

    for (int i = 0; i < job->slot_count; i++) {
        if (fs_job_cancel_requested(job)) return -1;
        rdma_fs_slot_t *s = &job->slots[i];
        s->buf = (unsigned char *)malloc(job->chunk_size);
        if (!s->buf) return -1;
        s->cap = job->chunk_size;
        s->mr = ibv_reg_mr(job->pd, s->buf, s->cap, IBV_ACCESS_LOCAL_WRITE);
        if (!s->mr) {
            fprintf(stderr, "repl rdma-fs: tid=%llu reg_mr slot=%d cap=%zu failed: %s\n",
                    (unsigned long long)job->tid, i, s->cap, strerror(errno));
            return -1;
        }
    }
    return 0;
}

void rdma_fs_job_teardown_verbs(rdma_fs_job_t *job) {
    if (!job) return;

    /* QP 必须先销毁：其 WR 引用 slot MR。只有 worker 已 join 后才会调用。 */
    if (job->id && job->qp) {
        rdma_destroy_qp(job->id);
        job->qp = NULL;
    }
    for (int i = 0; i < RDMA_FS_MAX_SLOTS; i++) {
        rdma_fs_slot_t *s = &job->slots[i];
        if (s->mr) {
            ibv_dereg_mr(s->mr);
            s->mr = NULL;
        }
        if (s->buf) {
            free(s->buf);
            s->buf = NULL;
        }
        s->cap = 0;
    }
    if (job->cq) {
        ibv_destroy_cq(job->cq);
        job->cq = NULL;
    }
    if (job->pd) {
        ibv_dealloc_pd(job->pd);
        job->pd = NULL;
    }
    if (job->comp_chan) {
        ibv_destroy_comp_channel(job->comp_chan);
        job->comp_chan = NULL;
    }
}

/* 从 CQ 回收完成。返回完成数；*cq_err 置 1 表示出现失败/非法 completion。
 * 每个 CQE 必须严格匹配 RDMA_WRITE + 仍 in-flight 的 slot；完成字节按
 * 该 slot 实际 post 的长度统计（不信任 drivers 的 wc.byte_len）。 */
static int fs_poll_cq(rdma_fs_job_t *job, int slot_inflight[], size_t slot_len[],
                      int *inflight, int *cq_err, int *wc_status,
                      int *inflight_wr_out) {
    struct ibv_wc wc[8];
    int total = 0;
    *cq_err = 0;

    for (;;) {
        int n = ibv_poll_cq(job->cq, 8, wc);
        if (n < 0) {
            *cq_err = 1;
            if (wc_status) *wc_status = -1;
            fprintf(stderr, "repl rdma-fs: tid=%llu ibv_poll_cq failed\n",
                    (unsigned long long)job->tid);
            break;
        }
        if (n == 0) break;
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                *cq_err = 1;
                if (wc_status) *wc_status = (int)wc[i].status;
                fprintf(stderr, "repl rdma-fs: tid=%llu CQE error status=%d opcode=%d "
                        "vendor=%u wr_id=%llu\n",
                        (unsigned long long)job->tid, wc[i].status, wc[i].opcode,
                        wc[i].vendor_err, (unsigned long long)wc[i].wr_id);
                continue;
            }
            int slot = (wc[i].wr_id > 0 && wc[i].wr_id <= (uint64_t)job->slot_count)
                ? (int)(wc[i].wr_id - 1) : -1;
            if (wc[i].opcode != IBV_WC_RDMA_WRITE || slot < 0 || !slot_inflight[slot]) {
                *cq_err = 1;
                if (wc_status) *wc_status = (int)wc[i].status;
                fprintf(stderr, "repl rdma-fs: tid=%llu unexpected CQE opcode=%d "
                        "wr_id=%llu (slot in_flight=%d)\n",
                        (unsigned long long)job->tid, wc[i].opcode,
                        (unsigned long long)wc[i].wr_id,
                        slot >= 0 ? slot_inflight[slot] : -1);
                continue;
            }
            slot_inflight[slot] = 0;
            job->completed_bytes += (uint64_t)slot_len[slot];
            slot_len[slot] = 0;
            if (*inflight > 0) (*inflight)--;
            total++;
        }
    }
    if (inflight_wr_out) *inflight_wr_out = *inflight;
    return total;
}

/* 等待至少一个 completion（事件 + 直接 poll 双保险，兼容 rxe 丢事件）。 */
static void fs_wait_completion(rdma_fs_job_t *job, int timeout_ms) {
    if (!job->comp_chan) {
        struct timespec ts = { 0, (long)timeout_ms * 1000000L };
        nanosleep(&ts, NULL);
        return;
    }
    struct pollfd pfd;
    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = job->comp_chan->fd;
    pfd.events = POLLIN;
    int pr = poll(&pfd, 1, timeout_ms);
    if (pr > 0) {
        struct ibv_cq *ev_cq = NULL;
        void *ev_ctx = NULL;
        if (ibv_get_cq_event(job->comp_chan, &ev_cq, &ev_ctx) == 0) {
            ibv_ack_cq_events(ev_cq, 1);
            ibv_req_notify_cq(job->cq, 0);
        }
    }
}

static void fs_note_progress(rdma_fs_job_t *job, uint64_t posted, int inflight) {
    pthread_mutex_lock(&g_rdma_fs_mu);
    job->posted_bytes = posted;
    job->inflight_wr = inflight;
    pthread_mutex_unlock(&g_rdma_fs_mu);
}

void *rdma_fs_job_worker(void *arg) {
    rdma_fs_job_t *job = (rdma_fs_job_t *)arg;
    int reason = RDMA_FS_REASON_NONE;
    int failed = 0;
    int slot_inflight[RDMA_FS_MAX_SLOTS];
    size_t slot_len[RDMA_FS_MAX_SLOTS];
    memset(slot_inflight, 0, sizeof(slot_inflight));
    memset(slot_len, 0, sizeof(slot_len));
    int inflight = 0;
    int cq_err = 0;
    int token_held = 0;
    int log_sending = 0;
    int log_state = 0, log_cq = 0, log_reason = 0;
    uint64_t log_posted = 0, log_total = 0;

    /* 1) 等 CM established。 */
    struct timespec dl = fs_deadline_after_ms(RDMA_FS_CM_WAIT_MS);
    pthread_mutex_lock(&g_rdma_fs_mu);
    while (!job->established && !job->cancel) {
        int rc = pthread_cond_timedwait(&g_rdma_fs_cv, &g_rdma_fs_mu, &dl);
        if (rc == ETIMEDOUT) break;
    }
    if (job->cancel) {
        reason = job->cancel_reason ? job->cancel_reason : RDMA_FS_REASON_CANCELLED;
        failed = 1;
    } else if (!job->established) {
        reason = RDMA_FS_REASON_TIMEOUT;
        failed = 1;
    }
    if (!failed) job->state = RDMA_FS_ST_WAIT_MR;
    pthread_mutex_unlock(&g_rdma_fs_mu);
    if (failed) goto finish;

    /* 2) 等不可变快照 + 远端 MR。 */
    dl = fs_deadline_after_ms(RDMA_FS_MR_WAIT_MS);
    pthread_mutex_lock(&g_rdma_fs_mu);
    while (!job->cancel && !(job->snap_ready && job->mr_ready)) {
        int rc = pthread_cond_timedwait(&g_rdma_fs_cv, &g_rdma_fs_mu, &dl);
        if (rc == ETIMEDOUT) break;
    }
    if (job->cancel) {
        reason = job->cancel_reason ? job->cancel_reason : RDMA_FS_REASON_CANCELLED;
        failed = 1;
    } else if (!job->snap_ready) {
        reason = RDMA_FS_REASON_SNAPSHOT;
        failed = 1;
    } else if (!job->mr_ready) {
        reason = RDMA_FS_REASON_TIMEOUT;
        failed = 1;
    }
    pthread_mutex_unlock(&g_rdma_fs_mu);
    if (failed) goto finish;

    /* 3) 并发令牌：默认只允许 N 个 job 同时处于 RDMA_SENDING。 */
    pthread_mutex_lock(&g_rdma_fs_mu);
    job->state = RDMA_FS_ST_WAIT_TOKEN;
    while (!job->cancel && g_rdma_fs_sending >= g_rdma_fs_max_concurrent) {
        dl = fs_deadline_after_ms(1000);
        pthread_cond_timedwait(&g_rdma_fs_cv, &g_rdma_fs_mu, &dl);
    }
    if (job->cancel) {
        reason = job->cancel_reason ? job->cancel_reason : RDMA_FS_REASON_CANCELLED;
        failed = 1;
    } else {
        g_rdma_fs_sending++;
        if ((unsigned)g_rdma_fs_sending > g_rdma_fs_max_seen)
            g_rdma_fs_max_seen = (unsigned)g_rdma_fs_sending;
        token_held = 1;
        job->state = RDMA_FS_ST_SENDING;
        log_sending = g_rdma_fs_sending;
    }
    pthread_mutex_unlock(&g_rdma_fs_mu);
    if (failed) goto finish;
    fprintf(stderr, "repl rdma-fs: tid=%llu SENDING start t=%lld offset=%llu "
                    "bytes=%llu sending_jobs=%d\n",
            (unsigned long long)job->tid, kvs_now_ms(),
            (unsigned long long)job->snapshot_offset,
            (unsigned long long)job->snap_size, log_sending);

    /* 4) 按自己的 QP/CQ/slots/remote MR 发送整个快照。 */
    {
        uint64_t off = 0;
        uint64_t total = job->snap_size;
        int next_slot = 0;
        struct ibv_send_wr *bad = NULL;
        long long post_deadline = kvs_now_ms() + 60000;

        /* 先 arm 一次 CQ 通知。 */
        ibv_req_notify_cq(job->cq, 0);

        while (off < total) {
            pthread_mutex_lock(&g_rdma_fs_mu);
            int cancel = job->cancel;
            pthread_mutex_unlock(&g_rdma_fs_mu);

            int wc_status = 0;
            int done = fs_poll_cq(job, slot_inflight, slot_len, &inflight, &cq_err,
                                  &wc_status, NULL);
            if (cq_err) {
                reason = RDMA_FS_REASON_CQ;
                failed = 1;
                job->wc_status = wc_status;
                break;
            }
            (void)done;

            if (cancel) {
                reason = RDMA_FS_REASON_CANCELLED;
                failed = 1;
                break;
            }
            if (kvs_now_ms() >= post_deadline) {
                fprintf(stderr, "repl rdma-fs: tid=%llu post timeout posted=%llu/%llu\n",
                        (unsigned long long)job->tid, (unsigned long long)off,
                        (unsigned long long)total);
                reason = RDMA_FS_REASON_TIMEOUT;
                failed = 1;
                break;
            }
            if (inflight >= job->slot_count) {
                fs_wait_completion(job, 5);
                continue;
            }

            int slot = -1;
            for (int k = 0; k < job->slot_count; k++) {
                int cand = (next_slot + k) % job->slot_count;
                if (!slot_inflight[cand]) { slot = cand; break; }
            }
            if (slot < 0) {
                fs_wait_completion(job, 5);
                continue;
            }

            size_t want = job->chunk_size;
            if ((uint64_t)want > total - off) want = (size_t)(total - off);
            ssize_t r = pread(job->snap_fd, job->slots[slot].buf, want, (off_t)off);
            if (r <= 0) {
                fprintf(stderr, "repl rdma-fs: tid=%llu pread failed at %llu: %s\n",
                        (unsigned long long)job->tid, (unsigned long long)off,
                        r < 0 ? strerror(errno) : "EOF");
                reason = RDMA_FS_REASON_SNAPSHOT;
                failed = 1;
                break;
            }
            if (off + (uint64_t)r > job->remote_capacity) {
                fprintf(stderr, "repl rdma-fs: tid=%llu remote capacity exceeded "
                        "off=%llu len=%zd cap=%llu\n",
                        (unsigned long long)job->tid, (unsigned long long)off, r,
                        (unsigned long long)job->remote_capacity);
                reason = RDMA_FS_REASON_REMOTE_MR;
                failed = 1;
                break;
            }

            struct ibv_sge sge;
            struct ibv_send_wr wr;
            memset(&sge, 0, sizeof(sge));
            sge.addr = (uintptr_t)job->slots[slot].buf;
            sge.length = (uint32_t)r;
            sge.lkey = job->slots[slot].mr->lkey;
            memset(&wr, 0, sizeof(wr));
            wr.wr_id = (uint64_t)(slot + 1);
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = job->remote_addr + off;
            wr.wr.rdma.rkey = job->remote_rkey;

            if (job->pause_us > 0) usleep((useconds_t)job->pause_us);
            int post_rc = ibv_post_send(job->qp, &wr, &bad);
            if (post_rc != 0 && (post_rc == EAGAIN || post_rc == EWOULDBLOCK)) {
                /* SQ 满：先回收 CQE 再重试一次。 */
                fs_wait_completion(job, 5);
                fs_poll_cq(job, slot_inflight, slot_len, &inflight, &cq_err, NULL, NULL);
                if (cq_err) {
                    reason = RDMA_FS_REASON_CQ;
                    failed = 1;
                    break;
                }
                continue;
            }
            if (post_rc != 0) {
                fprintf(stderr, "repl rdma-fs: tid=%llu ibv_post_send failed rc=%d (%s) "
                        "bad_wr=%s\n",
                        (unsigned long long)job->tid, post_rc, strerror(post_rc),
                        bad ? "set" : "null");
                reason = RDMA_FS_REASON_CQ;
                failed = 1;
                break;
            }

            slot_inflight[slot] = 1;
            slot_len[slot] = (size_t)r;
            inflight++;
            next_slot = (slot + 1) % job->slot_count;
            off += (uint64_t)r;
            job->posted_bytes = off;
        }

        if (!failed) {
            /* 5) 等待全部在途 WR 成功完成。 */
            long long deadline = kvs_now_ms() + RDMA_FS_SEND_DRAIN_MS;
            while (inflight > 0) {
                pthread_mutex_lock(&g_rdma_fs_mu);
                int cancel = job->cancel;
                pthread_mutex_unlock(&g_rdma_fs_mu);
                if (cancel) {
                    reason = RDMA_FS_REASON_CANCELLED;
                    failed = 1;
                    break;
                }
                int wc_status = 0;
                fs_poll_cq(job, slot_inflight, slot_len, &inflight, &cq_err, &wc_status, NULL);
                if (cq_err) {
                    reason = RDMA_FS_REASON_CQ;
                    failed = 1;
                    job->wc_status = wc_status;
                    break;
                }
                if (inflight == 0) break;
                if (kvs_now_ms() >= deadline) {
                    fprintf(stderr, "repl rdma-fs: tid=%llu drain timeout inflight=%d "
                            "posted=%llu/%llu\n", (unsigned long long)job->tid, inflight,
                            (unsigned long long)job->posted_bytes,
                            (unsigned long long)total);
                    reason = RDMA_FS_REASON_TIMEOUT;
                    failed = 1;
                    break;
                }
                fs_wait_completion(job, 20);
            }
            if (!failed && off != total) {
                reason = RDMA_FS_REASON_SNAPSHOT;
                failed = 1;
            }
        }
        fs_note_progress(job, off, inflight);
    }

finish:
    pthread_mutex_lock(&g_rdma_fs_mu);
    if (token_held) {
        g_rdma_fs_sending--;
        token_held = 0;
    }
    if (job->cancel && !failed) {
        failed = 1;
        reason = job->cancel_reason ? job->cancel_reason : RDMA_FS_REASON_CANCELLED;
    }
    if (failed) {
        job->state = RDMA_FS_ST_FAILED;
        job->cancel = 1;
        job->cancel_reason = reason;
        job->cq_error = (reason == RDMA_FS_REASON_CQ) ? 1 : job->cq_error;
    } else if (job->posted_bytes == job->snap_size && inflight == 0) {
        job->state = RDMA_FS_ST_WR_DONE;
    } else {
        job->state = RDMA_FS_ST_FAILED;
        job->cancel_reason = RDMA_FS_REASON_TIMEOUT;
    }
    job->t_done_ms = kvs_now_ms();
    job->worker_exited = 1;
    log_state = job->state;
    log_posted = job->posted_bytes;
    log_total = job->snap_size;
    log_cq = job->cq_error;
    log_reason = job->cancel_reason;
    pthread_cond_broadcast(&g_rdma_fs_cv);
    pthread_mutex_unlock(&g_rdma_fs_mu);
    fprintf(stderr, "repl rdma-fs: tid=%llu SENDING done t=%lld state=%d posted=%llu/%llu "
                    "cq_error=%d reason=%s\n",
            (unsigned long long)job->tid, kvs_now_ms(), log_state,
            (unsigned long long)log_posted, (unsigned long long)log_total,
            log_cq, rdma_fs_reason_name(log_reason));
    return NULL;
}

#endif /* KVS_ENABLE_RDMA */
