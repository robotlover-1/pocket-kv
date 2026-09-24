#ifndef PROXY_SLAVE_H
#define PROXY_SLAVE_H

#include <stdint.h>
#include <sys/uio.h>

#define PROXY_SLAVE_BACKOFF_INIT_MS  100
#define PROXY_SLAVE_BACKOFF_MAX_MS   5000

typedef struct {
    int fd;
    char host[64];
    int port;
    unsigned int backoff_ms;       /* 当前退避间隔 */
    unsigned int backoff_max_ms;   /* 最大退避间隔 5000ms */
} proxy_slave_ctx_t;

/* 初始化 slave 上下文 */
void proxy_slave_init(proxy_slave_ctx_t *ctx, const char *host, int port);

/* 连接 slave。返回 0 成功，-1 失败。调用方负责管理退避策略 */
int proxy_slave_connect(proxy_slave_ctx_t *ctx);

/* 断开连接 */
void proxy_slave_disconnect(proxy_slave_ctx_t *ctx);

/* 检查是否已连接 */
int proxy_slave_is_connected(proxy_slave_ctx_t *ctx);

/* 发送失败后由转发线程调用：摘掉 fd 并复位退避，让主循环重新 connect。
 * fd 号仍 > 0 不代表链路可用，必须显式标记，否则永不重连。 */
void proxy_slave_mark_down(proxy_slave_ctx_t *ctx, const char *why);

/* 获取 fd（-1 表示未连接） */
int proxy_slave_fd(proxy_slave_ctx_t *ctx);

/* 在内部锁保护下向 slave writev。返回实际写入字节数；
 * 未连接或 writev 失败返回 -1（调用方回退 cache）。
 * 锁跨越“检查已连接 + writev”，保证 disconnect 不会在处理中途
 * 关闭/复用 fd —— 写要么在旧 fd 上完成，要么看到 fd<=0 返回 -1。 */
ssize_t proxy_slave_writev(proxy_slave_ctx_t *ctx, struct iovec *iov, int iovcnt);

#endif /* PROXY_SLAVE_H */
