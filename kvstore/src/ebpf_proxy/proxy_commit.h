#ifndef PROXY_COMMIT_H
#define PROXY_COMMIT_H

#include <stddef.h>
#include <stdint.h>

/* 多副本 Commit Gate：全局序只解析一次，再按 replica_id fan-out。
 * emit 收到的是"某个副本要发送的一条已提交命令"：
 *   返回 0 = 已交付（入该副本队列）；-1 = 该副本未能交付，调用方按副本隔离
 *   （不应把整个 session 判死，除非所有副本都无法接收）。 */
typedef int (*proxy_commit_emit_fn)(uint64_t replica_id, uint64_t global_seq,
                                    const unsigned char *buf, size_t len);

/* 全局故障回调：1=timeout 2=desync 3=gap 4=channel 5=cache_full */
typedef void (*proxy_commit_fault_fn)(int reason);

/* 单个副本故障回调（恢复缓冲超限/交付失败）：只影响该 replica_id，
 * main 负责断开它的数据通道并置 client_ctl slot FAULT，让 Master 只重同步该副本。 */
typedef void (*proxy_commit_replica_fault_fn)(uint64_t replica_id, int reason);

/* 收到某副本的 RESUME：在把恢复记录交付给 emit 之前调用。
 * main 校验 (replica_id, replica_gen, barrier_seq) 是否与当前 slot 状态一致：
 * 返回 1 = 接受，把 phase 切回 LIVE 并按序交出恢复记录；
 * 返回 0 = 陈旧/错代 RESUME，忽略且**不得**释放当前恢复缓冲。 */
typedef int (*proxy_commit_resume_fn)(uint64_t replica_id, uint64_t replica_gen,
                                      uint64_t barrier_seq, uint64_t marker);

#define PROXY_COMMIT_MAX_REPLICAS 64

typedef struct {
    unsigned long long descriptors;
    unsigned long long writes;
    unsigned long long skips;
    unsigned long long pending;
    unsigned long long timeout;
    unsigned long long desync;
    unsigned long long gap;
    unsigned long long channel_fault;
    unsigned long long recovery_bytes;      /* 所有副本恢复缓冲的合计 */
    unsigned long long emitted_bytes;       /* 按副本交付的字节合计 */
    unsigned long long conn_ctx_count;      /* per-cookie framing 状态数 */
    unsigned long long pending_cmd_count;   /* 已捕获待匹配命令数 */
    unsigned long long oldest_pending_ms;   /* 最老 pending 命令的等待时长 */
    unsigned long long memory_hard_limit_count;
    uint64_t next_global_seq;
    int channel_connected;
} proxy_commit_stats_t;

int  proxy_commit_init(const char *sock_path,
                       proxy_commit_emit_fn emit,
                       proxy_commit_fault_fn fault,
                       proxy_commit_replica_fault_fn replica_fault,
                       proxy_commit_resume_fn resume);
void proxy_commit_cleanup(void);

/* 主循环每轮调用：读控制通道、匹配描述符、按 global_seq 放行。
 * 每个副本的"是否处于恢复缓冲"由 proxy_commit_set_buffering() 控制。 */
void proxy_commit_poll(void);

/* ringbuf 捕获记录：24B 头已解析 */
void proxy_commit_on_record(uint64_t cookie, uint64_t recv_seq,
                            const unsigned char *payload, size_t len);

/* capture gate 关闭 / proxy 重启：清空所有连接状态与副本恢复缓冲 */
void proxy_commit_purge(void);

/* 副本注册：只有 active 的副本参与 fan-out；激活/注销都会丢弃该副本的恢复缓冲 */
void proxy_commit_set_replica(uint64_t replica_id, int active);

/* 该副本进入/离开恢复缓冲：进入时清空旧缓冲；离开时丢弃未 RESUME 的数据（fail-closed） */
void proxy_commit_set_buffering(uint64_t replica_id, int buffering);

void proxy_commit_get_stats(proxy_commit_stats_t *out);

#endif /* PROXY_COMMIT_H */
