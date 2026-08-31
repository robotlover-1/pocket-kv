#include "proxy_slave.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/* 连接状态（ctx->fd）的跨线程锁：
 * 主线程 connect/disconnect 写 fd，转发线程经 proxy_slave_writev 读 fd。
 * 所有 fd 读写都经此锁；writev 在锁内完成“检查已连接 + writev”，
 * 保证断开重连不会在处理中途关闭/复用 fd。 */
static pthread_mutex_t g_slave_lock = PTHREAD_MUTEX_INITIALIZER;

/* 在锁内返回有效 fd（-1 表示未连接） */
static int slave_locked_fd(proxy_slave_ctx_t *ctx) {
    int fd;
    pthread_mutex_lock(&g_slave_lock);
    fd = (ctx && ctx->fd > 0) ? ctx->fd : -1;
    pthread_mutex_unlock(&g_slave_lock);
    return fd;
}

void proxy_slave_init(proxy_slave_ctx_t *ctx, const char *host, int port) {
    if (!ctx) return;
    /* 不复用 memset（会清零 lock 之外的内部状态）；逐字段初始化 */
    pthread_mutex_lock(&g_slave_lock);
    ctx->fd = -1;
    pthread_mutex_unlock(&g_slave_lock);
    ctx->backoff_ms = PROXY_SLAVE_BACKOFF_INIT_MS;
    ctx->backoff_max_ms = PROXY_SLAVE_BACKOFF_MAX_MS;
    if (host) snprintf(ctx->host, sizeof(ctx->host), "%s", host);
    ctx->port = port;
}

int proxy_slave_connect(proxy_slave_ctx_t *ctx) {
    struct sockaddr_in addr;
    struct timeval tv;
    int fd;

    if (!ctx || ctx->host[0] == '\0' || ctx->port <= 0) return -1;

    /* 重置旧连接（锁内），避免与转发线程读 fd 竞态 */
    pthread_mutex_lock(&g_slave_lock);
    if (ctx->fd > 0) { close(ctx->fd); ctx->fd = -1; }
    pthread_mutex_unlock(&g_slave_lock);

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("ebpf-proxy: slave socket");
        return -1;
    }

    tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    /* 不显式设 SO_RCVBUF/SO_SNDBUF：显式设置会禁用 Linux TCP 窗口 autotuning，
     * 把窗口锁死（实测 SO_RCVBUF=1MB → 接收窗口 56KB、rwnd_limited 100%），
     * 跨机单连接吞吐被 rwnd 压到 ~1.5G。保留内核 autotuning 才能把窗口推到 BDP
     * （跨机 sync P=160 释放后 1.65M→2.34M，+42%）。 */
    { int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)ctx->port);
    if (inet_pton(AF_INET, ctx->host, &addr.sin_addr) <= 0) {
        fprintf(stderr, "ebpf-proxy: invalid slave host %s\n", ctx->host);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "ebpf-proxy: slave connect failed %s:%d (errno=%d), "
                "backoff %ums\n", ctx->host, ctx->port, errno, ctx->backoff_ms);
        close(fd);
        return -1;
    }

    /* 提交新 fd（锁内发布，转发线程 writev 经同一锁读取） */
    pthread_mutex_lock(&g_slave_lock);
    ctx->fd = fd;
    pthread_mutex_unlock(&g_slave_lock);

    fprintf(stderr, "ebpf-proxy: connected to slave %s:%d fd=%d\n",
            ctx->host, ctx->port, fd);
    ctx->backoff_ms = PROXY_SLAVE_BACKOFF_INIT_MS;
    return 0;
}

void proxy_slave_disconnect(proxy_slave_ctx_t *ctx) {
    int fd;
    if (!ctx) return;
    pthread_mutex_lock(&g_slave_lock);
    fd = ctx->fd;
    ctx->fd = -1;
    pthread_mutex_unlock(&g_slave_lock);
    /* 锁释放后再 close：若转发线程正持锁 writev，则本次 wait 到其完成后
     * 才拿到锁，保证 close 落后于所有在途写，不会写到一个已关闭的 fd */
    if (fd > 0) close(fd);
}

int proxy_slave_is_connected(proxy_slave_ctx_t *ctx) {
    return slave_locked_fd(ctx) > 0;
}

int proxy_slave_fd(proxy_slave_ctx_t *ctx) {
    return slave_locked_fd(ctx);
}

ssize_t proxy_slave_writev(proxy_slave_ctx_t *ctx, struct iovec *iov, int iovcnt) {
    ssize_t w = -1;
    pthread_mutex_lock(&g_slave_lock);
    if (ctx && ctx->fd > 0) {
        w = writev(ctx->fd, iov, iovcnt);
    }
    pthread_mutex_unlock(&g_slave_lock);
    /* -1 = 未连接或 writev 失败，由调用方回退 cache */
    return w;
}
