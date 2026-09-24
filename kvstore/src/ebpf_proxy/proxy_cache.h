#ifndef PROXY_CACHE_H
#define PROXY_CACHE_H

#include <stddef.h>
#include <stdint.h>

/* proxy_cache 的定位（§3/§14）：只负责"同一个 replication session 内"的临时传输
 * 中断 —— FULLRESYNC(BUFFERING) 期间的增量、Slave 数据通道短暂断开期间的增量、
 * writev 失败回退的增量。跨 session 的恢复由 Master 侧的 backlog 负责。 */

/* 硬上限：达到即认为本 session 的增量已无法保证无损，标记 invalid（不再静默丢弃）。 */
#define PROXY_CACHE_MAX_BYTES  (256UL * 1024 * 1024)
/* 高水位：只是向上游发背压（让 master 停手等待），继续正常缓存。 */
#define PROXY_CACHE_HIGH_BYTES (192UL * 1024 * 1024)

/* cache_append 返回值 */
#define CACHE_APPEND_OK        0
#define CACHE_APPEND_FAIL     (-1)   /* 参数/内存错误 */
#define CACHE_APPEND_FULL     (-2)   /* 已达硬上限：本节点未被接收，ctx 已标记 invalid */

typedef struct cache_node_s {
    struct cache_node_s *next;
    uint64_t session_id;      /* 产生该节点的 replication session（§7） */
    size_t len;
    unsigned char data[];     /* flexible array */
} cache_node_t;

typedef struct {
    cache_node_t *head;
    cache_node_t *tail;
    size_t total_bytes;
    size_t node_count;
    unsigned long long dropped;        /* 丢弃的节点数（跨 session 作废 / 溢出作废） */
    unsigned long long drop_bytes;     /* 丢弃的字节数 */
    size_t max_bytes;                  /* 峰值占用 */
    int over_high;                     /* 当前是否超高水位（背压去抖用） */
    int invalid;                       /* 本 session cache 已作废（硬上限触发） */
} cache_ctx_t;

/* 初始化缓存上下文 */
void cache_init(cache_ctx_t *ctx);

/* 追加数据到链表尾部，并打上产生它的 session_id。
 * 超过硬上限时不再"丢最旧继续跑"，而是标记 ctx->invalid 并返回 CACHE_APPEND_FULL。 */
int cache_append(cache_ctx_t *ctx, const unsigned char *data, size_t len,
                 uint64_t session_id);

/* 从 head 逐条发送到 fd，只发 session_id 匹配的节点；不匹配的（上一个 session 遗留）
 * 直接释放并计入 dropped，永不跨 session 重放。返回发送条数，-1 表示参数错误。 */
int cache_flush(cache_ctx_t *ctx, int fd, uint64_t session_id);

/* 丢弃全部节点（session 结束 / 全量同步建立新边界时用） */
void cache_clear(cache_ctx_t *ctx);

/* 是否超高水位（用于决定是否向上游发背压） */
int cache_over_high(cache_ctx_t *ctx);

/* 本 session cache 是否已作废（硬上限触发） */
int cache_is_invalid(cache_ctx_t *ctx);

/* 释放所有节点 */
void cache_destroy(cache_ctx_t *ctx);

/* 获取统计: dropped 计数 / 丢弃字节 / max_bytes 峰值 */
void cache_stats(cache_ctx_t *ctx, unsigned long long *dropped_out,
                 unsigned long long *drop_bytes_out, size_t *max_bytes_out);

#endif /* PROXY_CACHE_H */
