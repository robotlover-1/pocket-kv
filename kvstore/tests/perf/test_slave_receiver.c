/*
 * test_slave_receiver.c — 独立 Slave 接收进程（双端口）
 *
 * 运行在 Slave 机器上:
 *   port        — sync 转发接收（简单计数）
 *   port+13     — kprobe 转发接收（计数 + parse_resp_stream）
 *
 * 用法:
 *   ./test_slave_receiver --port 15801
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define KPROBE_PORT_OFFSET 13

static volatile int g_running = 1;
static int g_delay_us = 0;

static void sig_handler(int sig) { (void)sig; g_running = 0; }

typedef struct {
    int port;
    const char *label;
    long long total_msgs;
    long long total_bytes;
    int connections;
} listener_ctx_t;

static void *listener_thread(void *arg) {
    listener_ctx_t *ctx = (listener_ctx_t *)arg;
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_ANY),
                               .sin_port = htons(ctx->port)};
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[%s] bind port %d: %s\n", ctx->label, ctx->port, strerror(errno));
        return NULL;
    }
    if (listen(listen_fd, 5) < 0) {
        perror("listen"); return NULL;
    }
    printf("[%s] listening on 0.0.0.0:%d\n", ctx->label, ctx->port);
    fflush(stdout);

    while (g_running) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) continue;
            break;
        }

        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        getpeername(fd, (struct sockaddr *)&peer, &peer_len);
        printf("[%s] connection #%d from %s:%d\n",
               ctx->label, ++ctx->connections,
               inet_ntoa(peer.sin_addr), ntohs(peer.sin_port));
        fflush(stdout);

        int one_i = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one_i, sizeof(one_i));

        int conn_msgs = 0;
        long long conn_bytes = 0;
        char buf[65536];

        while (1) {
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n <= 0) break;
            if (g_delay_us > 0) usleep(g_delay_us);
            conn_msgs++;
            conn_bytes += n;
        }

        close(fd);
        ctx->total_msgs += conn_msgs;
        ctx->total_bytes += conn_bytes;
        printf("[%s] connection #%d closed: %d msgs, %.2f MB\n",
               ctx->label, ctx->connections, conn_msgs,
               (double)conn_bytes / (1024.0 * 1024.0));
        fflush(stdout);
    }

    close(listen_fd);
    return NULL;
}

int main(int argc, char **argv) {
    int port = 15801;

    struct option long_opts[] = {
        {"port", required_argument, 0, 'p'},
        {"delay", required_argument, 0, 'd'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "p:d:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'p': port = atoi(optarg); break;
        case 'd': g_delay_us = atoi(optarg); break;
        case 'h':
            printf("用法: %s [--port PORT]\n", argv[0]);
            return 0;
        default: return 1;
        }
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    /* sync 端口 (port) */
    listener_ctx_t sync_ctx = {.port = port, .label = "sync"};
    pthread_t sync_tid;
    pthread_create(&sync_tid, NULL, listener_thread, &sync_ctx);

    /* kprobe 转发端口 (port+13) */
    listener_ctx_t kp_ctx = {.port = port + KPROBE_PORT_OFFSET, .label = "kprobe"};
    pthread_t kp_tid;
    pthread_create(&kp_tid, NULL, listener_thread, &kp_ctx);

    printf("[slave] dual-port mode: sync=%d, kprobe=%d, delay=%dus\n",
           port, port + KPROBE_PORT_OFFSET, g_delay_us);
    fflush(stdout);

    pthread_join(sync_tid, NULL);
    pthread_join(kp_tid, NULL);

    printf("[slave] exiting.\n"
           "  sync:   %lld msgs, %.2f MB, %d connections\n"
           "  kprobe: %lld msgs, %.2f MB, %d connections\n",
           sync_ctx.total_msgs, (double)sync_ctx.total_bytes / (1024.0 * 1024.0),
           sync_ctx.connections,
           kp_ctx.total_msgs, (double)kp_ctx.total_bytes / (1024.0 * 1024.0),
           kp_ctx.connections);

    return 0;
}
