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
    KVS_CTL_FULLSYNC_STATE        = 3,  /* master 写：1=进入全量同步(BUFFERING) */
    KVS_CTL_RINGBUF_BACKPRESSURE  = 4,  /* BPF 置位 / proxy 低水位清除 */
    KVS_CTL_PROXY_HEARTBEAT       = 5,  /* proxy 心跳，master 校验新鲜度 */
    KVS_CTL_FWDQ_BACKPRESSURE     = 6,  /* proxy 转发队列高/低水位 */
    KVS_CTL_CAPTURE_ENABLE        = 7,  /* master 写：无 Slave 时为 0，BPF 最前早退 */
    KVS_CTL_SESSION_VALID         = 8,  /* master 写：当前 replication session 是否有效 */
    KVS_CTL_SESSION_ID            = 9,  /* master 写：replication session 身份 */
    KVS_CTL_CACHE_INVALID         = 10, /* master 写 1 表示旧 cache 作废；proxy 溢出时也置位 */
    KVS_CTL_CACHE_BACKPRESSURE    = 11, /* proxy 写：proxy_cache 超高水位 */
    KVS_CTL_PROXY_STATE           = 12, /* proxy 写：1=BUFFERING（master 等待全量边界确认） */
    KVS_CTL_KEY_MAX               = 16, /* 与 BPF client_ctl map max_entries 一致 */
};

#endif /* KVS_CLIENT_CTL_H */
