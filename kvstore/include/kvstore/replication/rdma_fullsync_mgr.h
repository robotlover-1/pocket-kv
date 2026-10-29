#ifndef KVSTORE_REPLICATION_RDMA_FULLSYNC_MGR_H
#define KVSTORE_REPLICATION_RDMA_FULLSYNC_MGR_H

/*
 * 真正并发的多 Slave RDMA 全量同步管理器（M3）。
 *
 * 设计要点（见 docs/PocketKV_真正并发多Slave_RDMA全量同步_实施方案）：
 *   - 进程级唯一 RDMA CM listener，按 CM private_data 中的 (transfer_id, epoch)
 *     把连接派发给独立的 transfer/job；
 *   - 每个 job 独占自己的 PD/CQ/comp_chan/QP/send slots/远端 MR/发送进度；
 *   - 每个 job 一个后台 worker，多个 job 的 RDMA WRITE 可以真正重叠；
 *   - 控制连接（conn_t/fd）只由 Master owner 线程读写：worker 只更新 job 状态，
 *     owner 通过 rdma_fs_drain_events_owner() 取事件、发 FULLSYNCEND/ABORT；
 *   - 任何 job 失败只取消该 job，不影响其他 job 的 QP/CQ/发送进度。
 *
 * 本模块不依赖 kvstore 的 TCP/RESP 实现，便于单测；无 RDMA 编译时提供 stub。
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* CM hello 版本化身份字段长度（见 rdma_fs_cm_hello_encode）。 */
#define RDMA_FS_CM_HELLO_LEN 24
#define RDMA_FS_CM_HELLO_VERSION 1

/* owner 事件类型。 */
enum {
    RDMA_FS_EV_WR_DONE = 1,   /* 全部 WRITE 成功完成，可以给 Slave 发 FULLSYNCEND */
    RDMA_FS_EV_FAILED  = 2    /* job 失败/取消，owner 应 fail-closed 并只拒绝该副本 */
};

typedef struct rdma_fs_event {
    int type;
    int reason;                 /* 失败原因（rdma_fs_reason_t） */
    int slot;                   /* replica slot */
    uint64_t tid;               /* transfer_id */
    void *owner_conn;           /* opaque conn_t*，仅 owner 线程解引用 */
    uint64_t bytes_total;
    uint64_t bytes_completed;
} rdma_fs_event_t;

enum {
    RDMA_FS_REASON_NONE = 0,
    RDMA_FS_REASON_CANCELLED,
    RDMA_FS_REASON_TIMEOUT,
    RDMA_FS_REASON_CM,
    RDMA_FS_REASON_CQ,
    RDMA_FS_REASON_RESOURCE,
    RDMA_FS_REASON_SNAPSHOT,
    RDMA_FS_REASON_REMOTE_MR,
    RDMA_FS_REASON_OWNER_CLOSED
};

const char *rdma_fs_reason_name(int reason);

/*
 * 配置判定：是否启用新的并发多 Slave 全量同步路径。
 * 仅在 fullsync=rdma 且实时传输不是纯 RDMA/kprobe-rdma（保留旧路径）时启用。
 * 无 RDMA 编译或非 Master 时返回 0。
 */
int rdma_fs_should_enable(void);

/* 启动/停止进程级 CM listener。可重复调用；成功返回 0。 */
int rdma_fs_mgr_start(void);
void rdma_fs_mgr_stop(void);

/* listener 是否成功启动。未启动时调用方应回退 TCP sendfile。 */
int rdma_fs_mgr_ready(void);

/*
 * Master owner 线程：创建一个 transfer/job 并启动后台 worker。
 * 只登记元数据，不建立 RDMA 连接、不阻塞。
 * 返回 0 并输出 tid/epoch；epoch 用于 FULLRESYNC 头，Slave 在 CM hello 中回显。
 */
int rdma_fs_begin(void *owner_conn, uint64_t owner_conn_id, int slot,
                  uint64_t replica_gen, uint64_t barrier_seq,
                  uint64_t snapshot_offset, uint64_t barrier_global_seq,
                  uint64_t *tid_out, uint64_t *epoch_out);

/*
 * owner 线程完成不可变快照后调用：把 fd 所有权交给 job（job 关闭 fd 并 unlink path）。
 * 必须在发送 FULLRESYNC 头之前调用，保证 worker 只发送完整快照。
 */
int rdma_fs_snapshot_ready(uint64_t tid, int fd, const char *path, uint64_t size);

/*
 * Master RESP 解析器收到 FULLRESYNCWR 后调用（owner 线程）。
 * 返回 1 表示命中新 job（已处理），0 表示无匹配（调用方可走旧路径）。
 */
int rdma_fs_on_remote_mr(uint64_t tid, void *owner_conn, uint64_t addr,
                         uint32_t rkey, uint64_t capacity);

/*
 * Master owner 收到 REPLDONE 后调用。校验 tid/slot/gen 后把 job 置 DONE 并释放资源。
 * 返回 1 表示命中并已处理；0 表示不是本 job（陈旧/旧协议路径）。
 */
int rdma_fs_on_repldone(uint64_t tid, void *owner_conn, int slot,
                        uint64_t replica_gen);

/* owner 连接关闭前调用：取消该连接的所有 job，之后不再返回其事件。 */
void rdma_fs_cancel_owner(void *owner_conn);

/* 按 tid 取消单个 job（失败隔离）；reason 为 RDMA_FS_REASON_*。 */
void rdma_fs_cancel(uint64_t tid, int reason);

/*
 * Master owner 线程周期调用（reactor loop）：回收已退出的 worker、返回状态事件。
 * 返回事件个数；最多 max 个。事件按 job 首次出现顺序返回（每个 job 每类只一次）。
 */
int rdma_fs_drain_events_owner(rdma_fs_event_t *out, int max);

/* CM hello 编解码（纯字节序处理，不依赖 RDMA 头，可在单测中使用）。 */
void rdma_fs_cm_hello_encode(unsigned char out[RDMA_FS_CM_HELLO_LEN],
                             uint64_t tid, uint64_t epoch);
int rdma_fs_cm_hello_decode(const void *data, size_t len,
                            uint64_t *tid, uint64_t *epoch);

/* 可观测计数器（INFO 使用）。 */
typedef struct rdma_fs_stats {
    unsigned active_jobs;
    unsigned waiting_jobs;
    unsigned sending_jobs;
    unsigned long long total_started;
    unsigned long long total_completed;
    unsigned long long total_failed;
    unsigned long long cm_rejects;
    unsigned long long cq_errors;
    unsigned long long registered_bytes;
    unsigned max_concurrency_seen;
} rdma_fs_stats_t;

void rdma_fs_get_stats(rdma_fs_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif /* KVSTORE_REPLICATION_RDMA_FULLSYNC_MGR_H */
