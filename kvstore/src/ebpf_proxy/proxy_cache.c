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

int cache_append(cache_ctx_t *ctx, const unsigned char *data, size_t len) {
    if (!ctx || !data || len == 0) return -1;

    cache_node_t *node = (cache_node_t *)malloc(sizeof(cache_node_t) + len);
    if (!node) return -1;
    node->next = NULL;
    node->len = len;
    memcpy(node->data, data, len);

    while (ctx->total_bytes + len > PROXY_CACHE_MAX_BYTES && ctx->head) {
        cache_node_t *old = ctx->head;
        ctx->head = old->next;
        if (!ctx->head) ctx->tail = NULL;
        ctx->total_bytes -= old->len;
        ctx->node_count--;
        ctx->dropped++;
        free(old);
    }

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
    return 0;
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

int cache_flush(cache_ctx_t *ctx, int fd) {
    if (!ctx || fd < 0) return -1;
    int sent = 0;
    cache_node_t *node = ctx->head;
    while (node) {
        cache_node_t *next = node->next;
        if (send_full(fd, node->data, node->len) < 0) {
            fprintf(stderr, "ebpf-proxy: cache_flush send failed: %zd/%zu errno=%d\n",
                    (ssize_t)-1, node->len, errno);
            ctx->head = node;
            return sent;
        }
        sent++;
        ctx->total_bytes -= node->len;
        ctx->node_count--;
        free(node);
        node = next;
    }
    ctx->head = NULL;
    ctx->tail = NULL;
    ctx->total_bytes = 0;
    ctx->node_count = 0;
    return sent;
}

void cache_destroy(cache_ctx_t *ctx) {
    if (!ctx) return;
    cache_node_t *node = ctx->head;
    while (node) { cache_node_t *next = node->next; free(node); node = next; }
    memset(ctx, 0, sizeof(*ctx));
}

void cache_stats(cache_ctx_t *ctx, unsigned long long *dropped_out,
                 size_t *max_bytes_out) {
    if (!ctx) return;
    if (dropped_out) *dropped_out = ctx->dropped;
    if (max_bytes_out) *max_bytes_out = ctx->max_bytes;
}
