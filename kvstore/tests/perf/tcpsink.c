/*
 * tcpsink.c — 纯字节流接收 sink（验证 eBPF 转发是否丢字节/乱序/重复）
 *
 * eBPF 增量转发复制的是 master 收到的"原始 TCP 字节流"；TCP 不保证 recv() 边界对齐
 * RESP 命令，半条命令拆包是正常情况。因此本 sink 不做 RESP 解析——直接 recv 原始字节，
 * 统计字节总数 + FNV-1a-64 流式 hash。
 *
 * 验证方式：master 侧记录的"捕获总字节数" 与 tcpsink 收到的字节数、流式 hash 对比。
 * 完全丢弃 RESP 解析器，避免"半条命令卡死解析器 → slave 停止 recv → forward EAGAIN
 * 永久卡住"的假性塌陷（P=160 曾被此压到 1k QPS）。
 *
 * 用法: ./tcpsink -p 15901
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <unistd.h>

/* FNV-1a 64-bit（与 master 侧一致，流式累计） */
static inline uint64_t fnv1a_64(const unsigned char *data, int len, uint64_t h) {
    for (int i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

#define MAX_CLI 32
#define RB_SZ (1 * 1024 * 1024)   /* 每连接 1MB 缓冲：纯字节流，容纳大 burst */

typedef struct {
    int fd;
    unsigned char *rb;   /* 接收缓冲（纯字节，不做解析） */
    int head;
    long long n_bytes;   /* 已接收总字节数 */
    uint64_t hash;       /* 流式 FNV-1a-64 */
} client_t;

static volatile sig_atomic_t g_running = 1;
static int g_no_hash = 0;   /* -n/--no-hash: 跳过逐字节 FNV hash，消除测量端 hash 消费瓶颈 */
static int g_no_rcvbuf = 0; /* -R/--no-rcvbuf: 不设 SO_RCVBUF，保留 TCP autotuning（避免锁死接收窗口） */
static void sig_handler(int s) { (void)s; g_running = 0; }

int main(int argc, char **argv) {
    int port = 15901;
    for (int opt; (opt = getopt(argc, argv, "p:nhR")) != -1;) {
        if (opt == 'p') port = atoi(optarg);
        else if (opt == 'n') g_no_hash = 1;
        else if (opt == 'R') g_no_rcvbuf = 1;
        else { fprintf(stderr, "Usage: %s [-p port] [-n|--no-hash] [-R|--no-rcvbuf]\n", argv[0]); return opt == 'h' ? 0 : 1; }
    }
    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler); signal(SIGPIPE, SIG_IGN);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    { int one = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)); }
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_ANY), .sin_port = htons(port)};
    if (bind(lfd, (struct sockaddr *)&a, sizeof(a)) < 0) { perror("bind"); return 1; }
    listen(lfd, 16);
    { int fl = fcntl(lfd, F_GETFL, 0); fcntl(lfd, F_SETFL, fl | O_NONBLOCK); }
    fprintf(stderr, "[tcpsink] port %d (byte-stream + FNV-1a-64 hash)\n", port);

    client_t cli[MAX_CLI];
    memset(cli, 0, sizeof(cli));
    int n_cli = 0;
    long long total_bytes = 0;
    uint64_t total_hash = 14695981039346656037ULL;

    while (g_running) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd >= 0 && n_cli < MAX_CLI) {
            { int one = 1; setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }
            int rcv = 1 * 1024 * 1024;
            if (!g_no_rcvbuf) setsockopt(cfd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
            { int fl = fcntl(cfd, F_GETFL, 0); fcntl(cfd, F_SETFL, fl | O_NONBLOCK); }
            client_t *c = &cli[n_cli++];
            c->fd = cfd; c->rb = malloc(RB_SZ);
            c->head = 0; c->n_bytes = 0; c->hash = 14695981039346656037ULL;
            if (!c->rb) { close(cfd); n_cli--; continue; }
            fprintf(stderr, "[tcpsink] client %d fd=%d\n", n_cli, cfd);
        }

        fd_set rfds; FD_ZERO(&rfds); int maxfd = 0;
        for (int i = 0; i < n_cli; i++) { FD_SET(cli[i].fd, &rfds); if (cli[i].fd > maxfd) maxfd = cli[i].fd; }
        struct timeval tv = {.tv_sec = 0, .tv_usec = 100000};
        if (select(maxfd + 1, &rfds, NULL, NULL, &tv) < 0) { if (errno == EINTR) continue; break; }

        for (int i = 0; i < n_cli;) {
            client_t *c = &cli[i];
            if (!FD_ISSET(c->fd, &rfds)) { i++; continue; }
            for (;;) {
                /* 缓冲满：说明单连接突发 > 1MB，先落盘 hash 后整段丢弃字节继续计数 */
                if (c->head == RB_SZ) {
                    if (!g_no_hash) c->hash = fnv1a_64(c->rb, c->head, c->hash);
                    c->head = 0;
                }
                ssize_t nr = recv(c->fd, c->rb + c->head, RB_SZ - c->head, 0);
                if (nr > 0) {
                    c->n_bytes += nr;
                    if (c->head + (int)nr > RB_SZ) {
                        /* 一次 recv 超过剩余空间（异常大），分两段 hash */
                        int part = RB_SZ - c->head;
                        if (!g_no_hash) {
                            c->hash = fnv1a_64(c->rb + c->head, part, c->hash);
                            c->hash = fnv1a_64(c->rb + c->head + part, (int)nr - part, c->hash);
                        }
                        c->head = 0;
                    } else {
                        c->head += (int)nr;
                    }
                    continue;
                }
                if (nr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
                /* EOF 或错误：冲刷残留字节 + 收尾 */
                if (c->head > 0) { if (!g_no_hash) c->hash = fnv1a_64(c->rb, c->head, c->hash); c->head = 0; }
                fprintf(stderr, "[tcpsink] client %d done (bytes=%lld hash=%" PRIx64 ")\n",
                        i + 1, c->n_bytes, c->hash);
                total_bytes += c->n_bytes;
                total_hash ^= c->hash;   /* 各连接流式 hash 异或（单连接测试下即该流 hash） */
                close(c->fd); free(c->rb);
                if (i < --n_cli) { cli[i] = cli[n_cli]; }
                break;   /* 原为 continue：会复用已关闭的 fd 再 recv(EBADF) → 重复 done/n_cli 越界 */
            }
            i++;
        }
    }

    close(lfd);
    fprintf(stderr, "\n[tcpsink] bytes=%lld hash=%" PRIx64 "\n",
            total_bytes, total_hash);
    return 0;
}
