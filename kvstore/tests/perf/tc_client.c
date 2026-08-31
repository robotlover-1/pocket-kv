/*
 * tc_client.c — Tiny echo client for TC benchmark (runs on Slave VM)
 *
 * Usage: ./tc_client <master_ip> <port> <payload_size> <count>
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static double now_us(void) {
    struct timeval tv; gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1e6 + (double)tv.tv_usec;
}

static int write_full(int fd, const void *b, size_t n) {
    size_t w = 0;
    while (w < n) {
        ssize_t r = write(fd, (const char *)b + w, n - w);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        w += r;
    }
    return 0;
}

static int read_full(int fd, void *b, size_t n) {
    size_t r = 0;
    while (r < n) {
        ssize_t x = read(fd, (char *)b + r, n - r);
        if (x < 0) { if (errno == EINTR) continue; return -1; }
        if (x == 0) break;
        r += x;
    }
    return (int)r;
}

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <ip> <port> <payload> <count>\n", argv[0]);
        return 1;
    }
    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int payload = atoi(argv[3]);
    int count = atoi(argv[4]);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port)};
    inet_pton(AF_INET, ip, &addr.sin_addr);

    for (int retry = 0; retry < 30; retry++) {
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) break;
        usleep(200000);
    }

    char *req = malloc(payload);
    char *rsp = malloc(payload);
    memset(req, 'A', payload);

    /* warmup */
    int warmup = count / 10;
    if (warmup < 100) warmup = 100;
    if (warmup > 500) warmup = 500;
    for (int i = 0; i < warmup; i++) {
        write_full(fd, req, payload);
        read_full(fd, rsp, payload);
    }

    /* test */
    double t0 = now_us();
    for (int i = 0; i < count; i++) {
        write_full(fd, req, payload);
        if (read_full(fd, rsp, payload) != payload) break;
    }
    double elapsed = now_us() - t0;
    double qps = (elapsed > 0) ? (double)count / elapsed * 1e6 : 0;

    printf("qps=%.0f count=%d\n", qps, count);
    fflush(stdout);

    FILE *rf = fopen("/tmp/perf_client_result.txt", "w");
    if (rf) { fprintf(rf, "qps=%.0f count=%d\n", qps, count); fclose(rf); }

    close(fd); free(req); free(rsp);
    return 0;
}
