#include "proxy_cache.h"
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

void cache_init(cache_ctx_t *ctx) {
    if (!ctx) return;
    memset(ctx, 0, sizeof(*ctx));
}

/* 丢弃 head 起连续 n 个节点（内部用，需已持锁） */
static void cache_drop_head(cache_ctx_t *ctx, size_t n) {
    while (n-- > 0 && ctx->head) {
        cache_node_t *old = ctx->head;
        ctx->head = old->next;
        if (!ctx->head) ctx->tail = NULL;
        ctx->total_bytes -= old->len;
        ctx->node_count--;
        ctx->dropped++;
        ctx->drop_bytes += old->len;
        free(old);
    }
}

int cache_append(cache_ctx_t *ctx, const unsigned char *data, size_t len,
                 uint64_t session_id) {
    if (!ctx || !data || len == 0) return CACHE_APPEND_FAIL;
    /* 已作废：本 session 的增量已不可信，拒绝继续缓存（由 master 触发重新同步） */
    if (ctx->invalid) return CACHE_APPEND_FULL;

    /* 硬上限：复制流里"静默丢一段"等于 Slave 永久缺数据（且非幂等命令无法自愈）。
     * 因此不丢最旧继续跑，而是把整个 session 标记作废 + 清空，让上层的
     * CACHE_INVALID 机制把它降级为重新同步（partial/full resync）。 */
    if (ctx->total_bytes + len > PROXY_CACHE_MAX_BYTES) {
        fprintf(stderr, "ebpf-proxy: proxy_cache hard limit reached "
                "(%zu + %zu > %lu), invalidating session %llu cache\n",
                ctx->total_bytes, len, PROXY_CACHE_MAX_BYTES,
                (unsigned long long)session_id);
        cache_clear(ctx);
        ctx->invalid = 1;
        return CACHE_APPEND_FULL;
    }

    cache_node_t *node = (cache_node_t *)malloc(sizeof(cache_node_t) + len);
    if (!node) return CACHE_APPEND_FAIL;
    node->next = NULL;
    node->session_id = session_id;
    node->len = len;
    memcpy(node->data, data, len);

    if (!ctx->head) {
        ctx->head = node;
        ctx->tail = node;
    } else {
        ctx->tail->next = node;
        ctx->tail = node;
    }
    ctx->total_bytes += len;
    ctx->node_count++;
    if (ctx->total_bytes > ctx->max_bytes) ctx->max_bytes = ctx->total_bytes;
    if (ctx->total_bytes > PROXY_CACHE_HIGH_BYTES) ctx->over_high = 1;
    return CACHE_APPEND_OK;
}

int cache_over_high(cache_ctx_t *ctx) {
    return ctx ? ctx->over_high : 0;
}

int cache_is_invalid(cache_ctx_t *ctx) {
    return ctx ? ctx->invalid : 0;
}

void cache_clear(cache_ctx_t *ctx) {
    if (!ctx) return;
    cache_drop_head(ctx, (size_t)-1);
    ctx->head = NULL;
    ctx->tail = NULL;
    ctx->total_bytes = 0;
    ctx->node_count = 0;
    ctx->over_high = 0;
    /* 故意不动 ctx->invalid：数据丢了不等于 session 恢复了 */
}

/* 新 session：清数据 + 解除 invalid。只在确认建立了新的 replication session 时调用，
 * 否则 invalid 一旦置位就再也没机会复位，之后所有 cache_append 都会被拒。 */
void cache_reset_for_new_session(cache_ctx_t *ctx) {
    if (!ctx) return;
    int was_invalid = ctx->invalid;
    cache_clear(ctx);
    ctx->invalid = 0;
    if (was_invalid)
        fprintf(stderr, "ebpf-proxy: proxy_cache invalid flag cleared for new session\n");
}

static int send_full(int fd, const void *buf, size_t len) {
    const unsigned char *p = (const unsigned char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0) { sent += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd pfd = {.fd = fd, .events = POLLOUT};
            poll(&pfd, 1, 5); /* 等 5ms，不阻塞主循环 */
            continue;
        }
        return -1;
    }
    return 0;
}

int cache_flush(cache_ctx_t *ctx, int fd, uint64_t session_id) {
    if (!ctx || fd < 0) return -1;
    int sent = 0;
    cache_node_t *prev = NULL;
    cache_node_t *node = ctx->head;
    while (node) {
        cache_node_t *next = node->next;
        /* 跨 session 的节点一律作废（§7）：session 101 已断裂 → 新 session 102 的
         * REPLSYNC 绝不能把 101 的 cache 重放出去，否则与 backlog 回放叠加重复应用。 */
        if (node->session_id != session_id) {
            if (prev) prev->next = next; else ctx->head = next;
            if (ctx->tail == node) ctx->tail = prev;
            ctx->total_bytes -= node->len;
            ctx->node_count--;
            ctx->dropped++;
            ctx->drop_bytes += node->len;
            free(node);
            node = next;
            continue;
        }
        if (send_full(fd, node->data, node->len) < 0) {
            fprintf(stderr, "ebpf-proxy: cache_flush send failed (len=%zu errno=%d)\n",
                    node->len, errno);
            break;      /* 剩余节点保持在链表里，等下次再刷（tail 仍是合法节点） */
        }
        sent++;
        if (prev) prev->next = next; else ctx->head = next;
        if (ctx->tail == node) ctx->tail = prev;
        ctx->total_bytes -= node->len;
        ctx->node_count--;
        free(node);
        node = next;
    }
    if (!ctx->head) {
        ctx->tail = NULL;
        ctx->total_bytes = 0;
        ctx->node_count = 0;
        ctx->over_high = 0;
    } else if (ctx->total_bytes <= PROXY_CACHE_HIGH_BYTES) {
        ctx->over_high = 0;
    }
    return sent;
}

void cache_destroy(cache_ctx_t *ctx) {
    if (!ctx) return;
    cache_node_t *node = ctx->head;
    while (node) { cache_node_t *next = node->next; free(node); node = next; }
    memset(ctx, 0, sizeof(*ctx));
}

void cache_stats(cache_ctx_t *ctx, unsigned long long *dropped_out,
                 unsigned long long *drop_bytes_out, size_t *max_bytes_out) {
    if (!ctx) return;
    if (dropped_out) *dropped_out = ctx->dropped;
    if (drop_bytes_out) *drop_bytes_out = ctx->drop_bytes;
    if (max_bytes_out) *max_bytes_out = ctx->max_bytes;
}
