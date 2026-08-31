/* tiny_sync_echo.c — echo + sync forward to slave (for cross-machine sync benchmark) */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int write_full(int fd, const void *b, size_t n) {
    size_t w = 0;
    while (w < n) {
        ssize_t r = write(fd, (const char *)b + w, n - w);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        w += r;
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <port> <slave_ip> <slave_port>\n", argv[0]);
        return 1;
    }
    int port = atoi(argv[1]);
    const char *slave_ip = argv[2];
    int slave_port = atoi(argv[3]);
    signal(SIGPIPE, SIG_IGN);

    /* Connect to slave once (reuse connection) */
    int slave_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(slave_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(slave_port)};
    inet_pton(AF_INET, slave_ip, &sa.sin_addr);
    if (connect(slave_fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("connect slave"); return 1;
    }
    fprintf(stderr, "[sync-echo] connected to slave %s:%d\n", slave_ip, slave_port);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_ANY), .sin_port = htons(port)};
    bind(fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(fd, 5);
    fprintf(stderr, "[sync-echo] listening on :%d\n", port);

    while (1) {
        int c = accept(fd, NULL, NULL);
        if (c < 0) continue;
        setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        char buf[65536];
        while (1) {
            ssize_t n = read(c, buf, sizeof(buf));
            if (n <= 0) break;
            /* echo to client */
            if (write_full(c, buf, n) < 0) break;
            /* sync forward to slave — 在主路径上，串行 */
            if (write_full(slave_fd, buf, n) < 0) break;
        }
        close(c);
    }
    return 0;
}
