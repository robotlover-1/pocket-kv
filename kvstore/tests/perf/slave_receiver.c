/*
 * slave_receiver.c — 从机 TCP 接收器（端到端 ACK 版本）
 *
 * 双端口设计：
 *   DATA port (15901): 接收转发数据，统计。sync 时 ACK 回同一 fd，ebpf 时 ACK 写到 ACK fd。
 *   ACK  port (15902): 供 master（ebpf 模式）连接，阻塞等待确认。
 *
 * 用法: ./slave_receiver                    (默认端口 15901/15902)
 *       ./slave_receiver <data_port> <ack_port>
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static volatile sig_atomic_t g_running = 1;

static void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

static int tcp_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_ANY),
                               .sin_port = htons((uint16_t)port)};
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(fd); return -1;
    }
    if (listen(fd, 5) < 0) { perror("listen"); close(fd); return -1; }
    return fd;
}

int main(int argc, char **argv) {
    int data_port = (argc >= 2) ? atoi(argv[1]) : 15901;
    int ack_port  = (argc >= 3) ? atoi(argv[2]) : 15902;

    int no_ack = 0;
    for (int i = 3; i < argc; i++)
        if (strcmp(argv[i], "--no-ack") == 0) no_ack = 1;

    signal(SIGTERM, sig_handler);
    signal(SIGINT, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    int data_listen = tcp_listen(data_port);
    int ack_listen  = tcp_listen(ack_port);
    if (data_listen < 0 || ack_listen < 0) return 1;

    fprintf(stderr, "[slave] DATA port=%d  ACK port=%d\n", data_port, ack_port);

    int data_fd = -1;
    int ack_fd  = -1;
    int msg_count = 0;
    long long total_bytes = 0;

    while (g_running) {
        int nfds = 0;
        struct pollfd fds[4];

        /* 总是监听新连接（接受多轮测试的连接/重连） */
        fds[nfds].fd = data_listen; fds[nfds].events = POLLIN; nfds++;
        fds[nfds].fd = ack_listen;  fds[nfds].events = POLLIN; nfds++;

        int di = -1, ai = -1;
        if (data_fd >= 0) {
            di = nfds;
            fds[nfds].fd = data_fd; fds[nfds].events = POLLIN; nfds++;
        }

        int rc = poll(fds, nfds, 100);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* 新 DATA 连接 */
        if (fds[0].revents & POLLIN) {
            data_fd = accept(data_listen, NULL, NULL);
            if (data_fd >= 0) {
                int one = 1;
                setsockopt(data_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                fprintf(stderr, "[slave] DATA connected fd=%d\n", data_fd);
            }
        }

        /* 新 ACK 连接 */
        if (fds[1].revents & POLLIN) {
            ack_fd = accept(ack_listen, NULL, NULL);
            if (ack_fd >= 0) {
                int one = 1;
                setsockopt(ack_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                fprintf(stderr, "[slave] ACK connected fd=%d\n", ack_fd);
            }
        }

        /* DATA fd 可读 */
        if (di >= 0 && fds[di].revents & POLLIN) {
            char buf[65536];
            ssize_t n = read(data_fd, buf, sizeof(buf));
            if (n <= 0) {
                fprintf(stderr, "[slave] DATA fd=%d closed\n", data_fd);
                close(data_fd);
                data_fd = -1;
                continue;
            }
            msg_count++;
            total_bytes += n;

            if (!no_ack) {
                /* 选择 ACK 目标: ebpf 模式用 ack_fd，sync 模式用 data_fd */
                int ack_target = (ack_fd >= 0) ? ack_fd : data_fd;
                const char *ack = "OK\n";
                ssize_t sent = send(ack_target, ack, 3, MSG_NOSIGNAL);
                if (sent != 3) {
                    fprintf(stderr, "[slave] ACK send failed fd=%d sent=%zd errno=%d\n",
                            ack_target, sent, errno);
                }
            }
        }

        /* ack_fd 可读（对端关闭，如 master 退出）*/
        if (ai >= 0 && fds[ai].revents & (POLLIN | POLLHUP | POLLERR)) {
            char dummy[64];
            ssize_t n = read(ack_fd, dummy, sizeof(dummy));
            if (n <= 0) {
                fprintf(stderr, "[slave] ACK fd=%d closed\n", ack_fd);
                close(ack_fd);
                ack_fd = -1;
            }
        }
    }

    if (data_fd >= 0) close(data_fd);
    if (ack_fd >= 0) close(ack_fd);
    close(data_listen);
    close(ack_listen);

    fprintf(stderr, "[slave] done: msgs=%d bytes=%lld\n", msg_count, total_bytes);
    printf("msgs=%d bytes=%lld\n", msg_count, total_bytes);
    fflush(stdout);
    return 0;
}
