#ifndef KVSTORE_RDMA_FULLSYNC_INTERNAL_H
#define KVSTORE_RDMA_FULLSYNC_INTERNAL_H

/*
 * rdma_fullsync_mgr.c / rdma_fullsync_transfer.c 的内部共享定义。
 * 只在 KVS_ENABLE_RDMA=1 时编译真实实现；公共 stub 在 mgr.c 中。
 */

#include "kvstore/kvstore.h"
#include "kvstore/replication/rdma_fullsync_mgr.h"

#if KVS_ENABLE_RDMA

#include <pthread.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define RDMA_FS_MAX_SLOTS         64
#define RDMA_FS_MAX_JOBS          64
#define RDMA_FS_SNAP_PATH_MAX     512
#define RDMA_FS_HEADER_WAIT_MS    300000  /* 等 owner 发送 FULLRESYNC（含大快照生成时间） */
#define RDMA_FS_CM_WAIT_MS        10000   /* 等 CM established（从 header_sent 起算） */
#define RDMA_FS_MR_WAIT_MS        15000   /* 等 FULLRESYNCWR */
#define RDMA_FS_SEND_DRAIN_MS     20000   /* 等待在途 WR 完成 */
#define RDMA_FS_DESTROY_WAIT_MS   2000

enum rdma_fs_state {
    RDMA_FS_ST_NEW = 0,
    RDMA_FS_ST_WAIT_HEADER,   /* job 已创建，等待 owner 完整写出 FULLRESYNC */
    RDMA_FS_ST_CM_SETUP,
    RDMA_FS_ST_WAIT_ESTABLISHED,
    RDMA_FS_ST_WAIT_MR,
    RDMA_FS_ST_WAIT_TOKEN,
    RDMA_FS_ST_SENDING,
    RDMA_FS_ST_WR_DONE,       /* worker 成功，等待 owner 发 FULLSYNCEND */
    RDMA_FS_ST_WAIT_REPLDONE, /* FULLSYNCEND 已发，等待 REPLDONE */
    RDMA_FS_ST_DONE,
    RDMA_FS_ST_FAILED
};

typedef struct rdma_fs_slot {
    struct ibv_mr *mr;
    unsigned char *buf;
    size_t cap;
} rdma_fs_slot_t;

typedef struct rdma_fs_job {
    uint64_t tid;
    uint64_t epoch;
    void *owner_conn;          /* opaque conn_t*，仅 owner 线程解引用 */
    uint64_t owner_conn_id;
    int slot;
    uint64_t replica_gen;
    uint64_t barrier_seq;
    uint64_t snapshot_offset;
    uint64_t barrier_global_seq;

    int state;
    int cancel;
    int cancel_reason;
    int cm_setup;              /* CM 线程正在 setup_verbs（cancel 时需尽快退出） */
    int notified;              /* owner 已取走 WR_DONE/FAILED 事件 */
    int failed_counted;        /* 统计去重：失败只计一次 */
    int cq_counted;            /* 统计去重：CQ error 只计一次 */
    int cq_error;
    int wc_status;
    int cm_disconnected;
    /* 首发错误（只写第一次；后续 flush 错误不覆盖） */
    int fe_valid;
    int fe_source;             /* 1=poll_cq 2=wc 3=post_send 4=cm 5=timeout */
    int fe_wc_status;
    int fe_post_rc;
    uint32_t fe_vendor_err;
    uint32_t fe_qp_num;
    uint64_t fe_wr_id;
    uint64_t fe_posted_bytes;
    uint64_t fe_completed_bytes;
    int fe_inflight;
    long long fe_ts_ms;

    /* RDMA 资源（CM listener 线程分配；worker 只使用 qp/cq） */
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_chan;
    struct ibv_qp *qp;
    rdma_fs_slot_t slots[RDMA_FS_MAX_SLOTS];
    int slot_count;
    int qp_depth;
    int pause_us;
    size_t chunk_size;
    int header_sent;           /* owner 已完整写出 FULLRESYNC（CM 计时从此开始） */
    int established;
    int mr_ready;
    uint64_t remote_addr;
    uint32_t remote_rkey;
    uint64_t remote_capacity;

    /* 不可变快照（owner 生成，worker 用 pread 读取） */
    int snap_fd;
    char snap_path[RDMA_FS_SNAP_PATH_MAX];
    uint64_t snap_size;
    int snap_ready;

    /* 进度 */
    uint64_t posted_bytes;
    uint64_t completed_bytes;
    int inflight_wr;

    /* 线程生命周期 */
    pthread_t worker;
    int worker_started;
    int worker_exited;
    int worker_joined;

    /* 资源销毁 */
    int needs_destroy;         /* CM id 已绑定，必须由 CM 线程销毁 */
    int resources_destroyed;
    int destroy_enqueued;
    int destroy_pending;
    long long destroy_deadline_ms;

    long long t_begin_ms;
    long long t_snap_ready_ms;
    long long t_header_sent_ms;
    long long t_established_ms;
    long long t_mr_ms;
    long long t_send_start_ms;
    long long t_done_ms;
    long long t_end_sent_ms;
    long long t_repldone_ms;
    long long wait_deadline_ms;   /* WR_DONE 待发 END / WAIT_REPLDONE 的任务级 deadline */

    struct rdma_fs_job *next;
    struct rdma_fs_job *prev;
} rdma_fs_job_t;

/* 由 rdma_fullsync_mgr.c 定义，worker 在持锁时更新并发观测值。 */
extern pthread_mutex_t g_rdma_fs_mu;
extern pthread_cond_t g_rdma_fs_cv;
extern int g_rdma_fs_sending;
extern int g_rdma_fs_max_concurrent;
extern unsigned g_rdma_fs_max_seen;

/* 错误分类计数（mgr.c 实现；INFO/Exporter 使用）。 */
void rdma_fs_note_wc_error(int wc_status);
void rdma_fs_note_post_send_error(void);
void rdma_fs_note_poll_cq_failure(void);
void rdma_fs_note_cm_disconnect(void);

/* 保留最近若干次 job 首发错误（job 销毁后 INFO 仍可查看）。 */
void rdma_fs_remember_first_error(uint64_t tid, int slot, int source, int wc_status,
                                  uint32_t vendor, uint32_t qp_num, uint64_t wr_id,
                                  int post_rc, uint64_t posted, uint64_t completed,
                                  int inflight, long long ts_ms);

/* 供 transfer.c 使用：worker 入口（pthread 参数为 job*）。 */
void *rdma_fs_job_worker(void *arg);

/* 估算一个新 job 将占用的发送 buffer 资源（slot 数 × chunk）。 */
void rdma_fs_estimate_resources(int *slots, uint64_t *bytes);

/* 供 mgr.c 使用：建/销毁每 job 的 QP/CQ/PD/slots。仅在 CM 线程调用。 */
int rdma_fs_job_setup_verbs(rdma_fs_job_t *job);
void rdma_fs_job_teardown_verbs(rdma_fs_job_t *job);

#endif /* KVS_ENABLE_RDMA */

#endif /* KVSTORE_RDMA_FULLSYNC_INTERNAL_H */
