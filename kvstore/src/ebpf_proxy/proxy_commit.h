#ifndef PROXY_COMMIT_H
#define PROXY_COMMIT_H

#include <stddef.h>
#include <stdint.h>

/* 已提交命令的落地回调：main.c 决定进转发队列/cache。
 * 返回 0 = 已交付；-1 = 未能交付（调用方必须 fail-closed）。 */
typedef int (*proxy_commit_emit_fn)(const unsigned char *buf, size_t len);

/* 故障回调：1=timeout 2=desync 3=gap 4=channel 5=cache_full */
typedef void (*proxy_commit_fault_fn)(int reason);

typedef struct {
    unsigned long long descriptors;
    unsigned long long writes;
    unsigned long long skips;
    unsigned long long pending;
    unsigned long long timeout;
    unsigned long long desync;
    unsigned long long gap;
    unsigned long long channel_fault;
    unsigned long long recovery_bytes;
    unsigned long long emitted_bytes;
    unsigned long long conn_ctx_count;      /* per-cookie framing 状态数 */
    unsigned long long pending_cmd_count;   /* 已捕获待匹配命令数 */
    unsigned long long oldest_pending_ms;   /* 最老 pending 命令的等待时长 */
    unsigned long long memory_hard_limit_count; /* pending+recovery 触达共享上限次数 */
    uint64_t next_global_seq;
    int channel_connected;
} proxy_commit_stats_t;

int  proxy_commit_init(const char *sock_path,
                       proxy_commit_emit_fn emit,
                       proxy_commit_fault_fn fault);
void proxy_commit_cleanup(void);

/* 主循环每轮调用（含 slave 重连退避循环）。
 * buffering: proxy 当前处于 BUFFERING（fullsync/恢复期），已提交写先缓存。 */
void proxy_commit_poll(int buffering);

/* ringbuf 捕获记录：24B 头已解析 */
void proxy_commit_on_record(uint64_t cookie, uint64_t recv_seq,
                            const unsigned char *payload, size_t len);

/* capture gate 关闭 / proxy 重启：清空所有连接状态 */
void proxy_commit_purge(void);

void proxy_commit_get_stats(proxy_commit_stats_t *out);

/* 恢复期仍有未 RESUME 的缓存提交：main 不应提前 flush fullsync cache */
int proxy_commit_recovery_pending(void);
/* 取走一次 RESUME 事件（main 在 FORWARDING 状态下请求 cache flush） */
int proxy_commit_take_resume_event(void);

#endif /* PROXY_COMMIT_H */
