#ifndef PROXY_CACHE_H
#define PROXY_CACHE_H

#include <stddef.h>
#include <stdint.h>

/* Commit Gate 下 cache 中的都是已提交写，淘汰=丢数据：
 * 高水位只做背压（不淘汰），到 hard 直接 fail-closed。 */
#define PROXY_CACHE_MAX_BYTES  (256UL * 1024 * 1024)  /* hard limit */
#define PROXY_CACHE_HIGH_BYTES (192UL * 1024 * 1024)  /* backpressure on */
#define PROXY_CACHE_LOW_BYTES  (128UL * 1024 * 1024)  /* backpressure off */

typedef struct cache_node_s {
    struct cache_node_s *next;
    size_t len;
    unsigned char data[];  /* flexible array */
} cache_node_t;

typedef struct {
    cache_node_t *head;
    cache_node_t *tail;
    size_t total_bytes;
    size_t node_count;
    unsigned long long dropped;
    unsigned long long hard_hit;
    size_t max_bytes;
    int over_high;   /* 高低水位迟滞状态（用于背压） */
} cache_ctx_t;

/* 初始化缓存上下文 */
void cache_init(cache_ctx_t *ctx);

/* 追加数据到链表尾部。返回 0 成功；-2 表示达到 hard limit（调用方必须 fail-closed，
 * 不能淘汰已提交数据）。 */
int cache_append(cache_ctx_t *ctx, const unsigned char *data, size_t len);

/* 从 head 逐条发送到 fd，成功后释放节点。返回发送条数，-1 表示发送失败 */
int cache_flush(cache_ctx_t *ctx, int fd);

/* 释放所有节点 */
void cache_destroy(cache_ctx_t *ctx);

/* 获取统计: dropped 计数和 max_bytes 峰值 */
void cache_stats(cache_ctx_t *ctx, unsigned long long *dropped_out,
                 size_t *max_bytes_out);

#endif /* PROXY_CACHE_H */
