#ifndef KVS_CLIENT_CTL_H
#define KVS_CLIENT_CTL_H

/* pinned client_ctl map 的键位约定 —— BPF 程序、ebpf-proxy 进程、kvstore master
 * 三方共用同一张 map（ebpf-proxy 加载 BPF 后 pin 到 bpffs，master 通过 bpf_obj_get
 * 打开写入）。改键必须同步这三处，以及与 bpffs 上残留的旧 map 一起重启。
 *
 * BPF 程序侧的对应定义在 src/replication/bpf/repl_client_capture.bpf.c
 * （BPF 不能 include 用户态头，键位以宏字面量重复存在，改动时一起改）。 */
enum {
    KVS_CTL_RESERVED              = 0,
    KVS_CTL_MASTER_PID            = 1,
    KVS_CTL_MASTER_PORT           = 2,
    KVS_CTL_FULLSYNC_STATE        = 3,  /* 旧版全局屏障位；多副本起由 slot 区间替代 */
    KVS_CTL_RINGBUF_BACKPRESSURE  = 4,  /* BPF 置位 / proxy 低水位清除 */
    KVS_CTL_PROXY_HEARTBEAT       = 5,  /* proxy 心跳，master 校验新鲜度 */
    KVS_CTL_FWDQ_BACKPRESSURE     = 6,  /* proxy 转发队列高/低水位 */
    KVS_CTL_CAPTURE_ENABLE        = 7,  /* master 写：无 Slave 时为 0，BPF 最前早退 */
    KVS_CTL_SESSION_VALID         = 8,  /* master 写：当前 replication session 是否有效 */
    KVS_CTL_SESSION_ID            = 9,  /* master 写：replication session 身份 */
    KVS_CTL_CACHE_INVALID         = 10, /* 旧版全局 cache 作废位；多副本用 slot FAULT */
    KVS_CTL_CACHE_BACKPRESSURE    = 11, /* proxy 写：所有副本积压总量超高水位 */
    KVS_CTL_PROXY_STATE           = 12, /* 旧版全局 BUFFERING 应答；多副本用 slot ACK */
    KVS_CTL_CAPTURE_FAULT         = 13, /* BPF 写：1=捕获截断/不可信，需 fail-closed */
    KVS_CTL_COMMIT_FAULT          = 14, /* proxy 写：Commit Gate 故障原因（1=timeout 2=desync 3=gap 4=channel 5=cache_full） */
    /* 多副本（slot）控制区间：每个 slot 4 个键，16 起连续排布。
     * BARRIER  master→proxy：1=该副本进入恢复缓冲（不要在 replay/快照前直发）
     * ACK      proxy→master：1=已进入 / 0=已离开；master 用下降沿重置遗留状态
     * FAULT    proxy→master：副本侧缓冲溢出/需要重新同步（只影响该副本）
     * RESERVED 预留（可用于 per-slot epoch/marker 观测）
     * 这些键只有 master/proxy 用户态访问，BPF 不迭代，map 扩容即可。 */
    KVS_CTL_REPL_SLOT_BASE        = 16,
    KVS_CTL_KEY_MAX               = 64, /* 与 BPF client_ctl map max_entries 一致 */
};

#define KVS_MAX_REPLICA_SLOTS 8
#define KVS_CTL_SLOT_BARRIER(slot) \
    (KVS_CTL_REPL_SLOT_BASE + (slot) * 4 + 0)
#define KVS_CTL_SLOT_ACK(slot) \
    (KVS_CTL_REPL_SLOT_BASE + (slot) * 4 + 1)
#define KVS_CTL_SLOT_FAULT(slot) \
    (KVS_CTL_REPL_SLOT_BASE + (slot) * 4 + 2)
#define KVS_CTL_SLOT_RESERVED(slot) \
    (KVS_CTL_REPL_SLOT_BASE + (slot) * 4 + 3)

#endif /* KVS_CLIENT_CTL_H */
