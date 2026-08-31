#ifndef PERF_COMMON_H
#define PERF_COMMON_H

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/* 简单计时器 — CLOCK_MONOTONIC，不受 NTP 调整影响 */
static inline double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000000.0 + (double)ts.tv_nsec / 1000.0;
}

static inline double now_ms(void) {
    return now_us() / 1000.0;
}

/* 纳秒转可读字符串 */
static inline const char *latency_str(double us) {
    static char buf[32];
    if (us < 1.0)       snprintf(buf, sizeof(buf), "%.1fns", us * 1000.0);
    else if (us < 1000.0) snprintf(buf, sizeof(buf), "%.0fμs", us);
    else                snprintf(buf, sizeof(buf), "%.2fms", us / 1000.0);
    return buf;
}

static inline const char *throughput_str(double bps) {
    static char buf[32];
    if (bps >= 1e9)       snprintf(buf, sizeof(buf), "%.2f Gbps", bps / 1e9);
    else if (bps >= 1e6)  snprintf(buf, sizeof(buf), "%.2f Mbps", bps / 1e6);
    else if (bps >= 1e3)  snprintf(buf, sizeof(buf), "%.2f Kbps", bps / 1e3);
    else                  snprintf(buf, sizeof(buf), "%.0f bps", bps);
    return buf;
}

/* 比较函数（qsort 用） */
static inline int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

/* 设置 fd 为非阻塞 */
static inline int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* 设置 TCP_NODELAY */
static inline int set_nodelay(int fd) {
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

/* 安全的 full write — EPIPE 时返回已写字节数，不触发 SIGPIPE */
static inline ssize_t write_full(int fd, const void *buf, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, (const char *)buf + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EPIPE) break; /* 对端已关闭，不触发 SIGPIPE */
            return -1;
        }
        written += (size_t)n;
    }
    return (ssize_t)written;
}

/* 安全的 full read */
static inline ssize_t read_full(int fd, void *buf, size_t len) {
    size_t nread = 0;
    while (nread < len) {
        ssize_t n = read(fd, (char *)buf + nread, len - nread);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;
        nread += (size_t)n;
    }
    return (ssize_t)nread;
}

#endif /* PERF_COMMON_H */
