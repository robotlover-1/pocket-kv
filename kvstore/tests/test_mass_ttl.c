/*
 * test_mass_ttl.c — 大量数据到期测试
 *
 * 设置 10000 个 key 并设置 10 秒过期时间，然后进入交互模式，
 * 你可以在另一个终端手动查看 TTL。测试程序会轮询抽样 key 的 TTL 状态。
 *
 * 注意: 本测试使用 HSET/HEXPIRE（HASH 引擎），因为默认 ARRAY 引擎
 *       最多只能存 1024 个 key。HASH 引擎使用链地址法，无此限制。
 *
 * 编译:
 *   make test_mass_ttl
 *
 * 用法:
 *   # 终端 1: 启动 kvstore
 *   ./kvstore kvstore.conf --role master
 *
 *   # 终端 2: 运行本测试
 *   ./test_mass_ttl --config tests/test.conf
 *
 *   # 终端 3: 手动检查 TTL（在本测试等待期间）
 *   redis-cli -p 5160 HTTL expire:k:000000
 *   redis-cli -p 5160 HTTL expire:k:005000
 *   redis-cli -p 5160 HTTL expire:k:009999
 *   redis-cli -p 5160 HGET  expire:k:000000   # 10s 后应返回 (nil)
 *
 * 选项:
 *   --host HOST       kvstore 地址 (默认 192.168.233.128)
 *   --port PORT       kvstore 端口 (默认 5160)
 *   --count N         设置 key 数量 (默认 10000)
 *   --ttl SECONDS     过期时间秒 (默认 10)
 *   --batch N         每批写入量 (默认 1000)
 *   --config PATH     加载配置文件
 *   -h                显示帮助
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>

#define BUFFER_SIZE 65536
#define MAX_KEY_LEN 64

#define ANSI_GREEN   "\033[32m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_RESET   "\033[0m"
#define ANSI_CLEAR_LINE "\033[2K"
#define ANSI_CURSOR_UP "\033[A"

static struct {
    const char *host;
    int port;
    int count;
    int ttl;
    int batch;
    const char *dump_path;
    const char *aof_path;
} g_opt = {
    .host = "192.168.233.128",
    .port = 5160,
    .count = 10000,
    .ttl = 10,
    .batch = 1000,
    .dump_path = "kvstore.dump",
    .aof_path = "kvstore.aof",
};

/* ---- TCP / RESP 工具 ---- */

static int tcp_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        struct hostent *he = gethostbyname(host);
        if (!he) { close(fd); return -1; }
        memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);
    }
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc == 0) { fcntl(fd, F_SETFL, flags); return fd; }
    if (rc < 0 && errno != EINPROGRESS) { close(fd); return -1; }
    struct pollfd pfd = {.fd = fd, .events = POLLOUT};
    rc = poll(&pfd, 1, timeout_ms > 0 ? timeout_ms : 5000);
    if (rc <= 0) { close(fd); return -1; }
    int err = 0;
    socklen_t elen = sizeof(err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen) != 0 || err != 0) {
        close(fd); return -1;
    }
    fcntl(fd, F_SETFL, flags);
    return fd;
}

static unsigned char *build_resp(int argc, const char **argv, size_t *out_len) {
    size_t cap = 64;
    for (int i = 0; i < argc; i++) cap += strlen(argv[i]) + 32;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return NULL;
    size_t pos = 0;
    int n = snprintf((char *)buf + pos, cap - pos, "*%d\r\n", argc);
    if (n < 0) { free(buf); return NULL; }
    pos += (size_t)n;
    for (int i = 0; i < argc; i++) {
        size_t len = strlen(argv[i]);
        n = snprintf((char *)buf + pos, cap - pos, "$%zu\r\n", len);
        if (n < 0) { free(buf); return NULL; }
        pos += (size_t)n;
        if (pos + len + 2 > cap) { free(buf); return NULL; }
        memcpy(buf + pos, argv[i], len);
        pos += len;
        buf[pos++] = '\r';
        buf[pos++] = '\n';
    }
    *out_len = pos;
    return buf;
}

static int send_all(int fd, const unsigned char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n <= 0) return -1;
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

static char *cmd(int fd, const char *arg0, ...) {
    int argc = 1;
    const char *args[64];
    args[0] = arg0;
    va_list ap;
    va_start(ap, arg0);
    while (argc < 64) {
        const char *a = va_arg(ap, const char *);
        if (!a) break;
        args[argc++] = a;
    }
    va_end(ap);
    size_t wlen;
    unsigned char *wbuf = build_resp(argc, args, &wlen);
    if (!wbuf) return NULL;
    if (send_all(fd, wbuf, wlen) != 0) { free(wbuf); return NULL; }
    free(wbuf);
    /* 读取响应 */
    struct timeval tv = {5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    size_t cap = 4096;
    size_t len = 0;
    unsigned char *resp = (unsigned char *)malloc(cap);
    if (!resp) return NULL;
    ssize_t r = read(fd, resp, cap - 1);
    if (r <= 0) { free(resp); return NULL; }
    len = (size_t)r;
    resp[len] = '\0';
    return (char *)resp;
}

/* ---- 配置文件解析 ---- */

static int parse_config_file(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) return -1;
    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        char *p = line;
        while (*p && isspace((unsigned char)*p)) p++;
        if (*p == '#' || *p == '\0' || *p == '\n') continue;
        char *eq = strchr(p, '=');
        if (!eq) continue;
        *eq = '\0';
        char *key = p;
        char *val = eq + 1;
        while (val && (*val == ' ' || *val == '\t')) val++;
        char *end = val + strlen(val) - 1;
        while (end > val && isspace((unsigned char)*end)) *end-- = '\0';
        if (strcmp(key, "host") == 0) g_opt.host = strdup(val);
        else if (strcmp(key, "port") == 0) g_opt.port = atoi(val);
        else if (strcmp(key, "count") == 0) g_opt.count = atoi(val);
        else if (strcmp(key, "ttl") == 0) g_opt.ttl = atoi(val);
        else if (strcmp(key, "batch") == 0) g_opt.batch = atoi(val);
        else if (strcmp(key, "dump_path") == 0) g_opt.dump_path = strdup(val);
        else if (strcmp(key, "aof_path") == 0) g_opt.aof_path = strdup(val);
    }
    fclose(fp);
    return 0;
}

/* ---- 主流程 ---- */

static void print_usage(const char *prog) {
    printf("用法: %s [选项]\n", prog);
    printf("  设置大量 key 并赋予 TTL，进入交互模式供手动检查\n\n");
    printf("选项:\n");
    printf("  --config PATH  加载配置文件 (默认 tests/test.conf)\n");
    printf("  --host HOST       kvstore 地址 (默认 %s)\n", g_opt.host);
    printf("  --port PORT       kvstore 端口 (默认 %d)\n", g_opt.port);
    printf("  --count N         设置 key 数量 (默认 %d)\n", g_opt.count);
    printf("  --ttl SECONDS     过期时间秒 (默认 %d)\n", g_opt.ttl);
    printf("  --batch N         每批写入量 (默认 %d)\n", g_opt.batch);
    printf("  --dump-path PATH  dump 文件路径 (默认 %s)\n", g_opt.dump_path);
    printf("  --aof-path PATH    AOF 文件路径 (默认 %s)\n", g_opt.aof_path);
    printf("  -h                显示帮助\n");
}

int main(int argc, char **argv) {
    /* 默认加载 tests/test.conf */
    parse_config_file("tests/test.conf");
    parse_config_file("test.conf");

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc)
            parse_config_file(argv[++i]);
        else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc)
            g_opt.host = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            g_opt.port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--count") == 0 && i + 1 < argc)
            g_opt.count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--ttl") == 0 && i + 1 < argc)
            g_opt.ttl = atoi(argv[++i]);
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            g_opt.batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "--dump-path") == 0 && i + 1 < argc)
            g_opt.dump_path = argv[++i];
        else if (strcmp(argv[i], "--aof-path") == 0 && i + 1 < argc)
            g_opt.aof_path = argv[++i];
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    printf(ANSI_BOLD "大量数据到期测试 (Mass TTL)\n" ANSI_RESET);
    printf("  地址: %s:%d\n", g_opt.host, g_opt.port);
    printf("  Key:  %d 个\n", g_opt.count);
    printf("  TTL:  %d 秒\n\n", g_opt.ttl);

    int fd = tcp_connect(g_opt.host, g_opt.port, 5000);
    if (fd < 0) {
        fprintf(stderr, ANSI_RED "错误: 无法连接 %s:%d\n" ANSI_RESET, g_opt.host, g_opt.port);
        return 1;
    }

    /* ---- Phase 1: 写入数据 + 设置 TTL ---- */
    struct timeval t0;
    gettimeofday(&t0, NULL);
    char ttl_str[16];
    snprintf(ttl_str, sizeof(ttl_str), "%d", g_opt.ttl);

    printf("写入 %d 条 key (TTL=%ds)...\n", g_opt.count, g_opt.ttl);
    for (int b = 0; b < g_opt.count; b += g_opt.batch) {
        int end = b + g_opt.batch;
        if (end > g_opt.count) end = g_opt.count;
        for (int i = b; i < end; i++) {
            char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
            snprintf(key, sizeof(key), "expire:k:%06d", i);
            snprintf(val, sizeof(val), "value:%06d", i);
            char *r = cmd(fd, "HSET", key, val, NULL);
            if (!r || strcmp(r, "+OK\r\n") != 0) {
                fprintf(stderr, "HSET 失败 at %d: %s\n", i, r ? r : "(无响应)");
                free(r); close(fd); return 1;
            }
            free(r);
            r = cmd(fd, "HEXPIRE", key, ttl_str, NULL);
            if (!r || (strcmp(r, ":1\r\n") != 0 && strcmp(r, "+OK\r\n") != 0)) {
                fprintf(stderr, "HEXPIRE 失败 at %d: %s", i, r ? r : "(无响应)\n");
                free(r); close(fd); return 1;
            }
            free(r);
        }
        struct timeval now;
        gettimeofday(&now, NULL);
        double elapsed = (double)(now.tv_sec - t0.tv_sec)
                       + (double)(now.tv_usec - t0.tv_usec) / 1000000.0;
        printf(ANSI_CLEAR_LINE "  写入: %d/%d (%.0f keys/s, %.1fs)" ANSI_CURSOR_UP,
               end, g_opt.count, (double)end / (elapsed > 0 ? elapsed : 0.001), elapsed);
    }
    printf(ANSI_CLEAR_LINE);
    struct timeval t1;
    gettimeofday(&t1, NULL);
    double write_elapsed = (double)(t1.tv_sec - t0.tv_sec)
                         + (double)(t1.tv_usec - t0.tv_usec) / 1000000.0;
    printf(ANSI_GREEN "写入完成: %d keys in %.2fs\n\n" ANSI_RESET, g_opt.count, write_elapsed);

    /* ---- Phase 2: 抽样检查 TTL ---- */
    int samples[] = {0, 1, 99, 999, 4999, 9999, -1};
    printf("抽样 TTL:\n");
    for (int si = 0; samples[si] >= 0; si++) {
        if (samples[si] >= g_opt.count) continue;
        char key[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "expire:k:%06d", samples[si]);
        char *r = cmd(fd, "TTL", key, NULL);
        if (r) {
            printf("  %s => %s", key, r);
            free(r);
        }
    }

    /* ---- Phase 3: 交互提示 ---- */
    printf("\n" ANSI_BOLD ANSI_CYAN "================================================\n");
    printf("  交互检查\n");
    printf("================================================\n" ANSI_RESET);
    printf("\n");
    printf("  在另一个终端手动检查 TTL:\n\n");
    printf("    redis-cli -p %d HTTL expire:k:000000\n", g_opt.port);
    printf("    redis-cli -p %d HTTL expire:k:005000\n", g_opt.port);
    printf("    redis-cli -p %d HTTL expire:k:009999\n", g_opt.port);
    printf("    redis-cli -p %d HGET  expire:k:000000\n", g_opt.port);
    printf("\n");
    printf("  TTL 返回值:\n");
    printf("    :N   = 还有 N 秒\n");
    printf("    :-1  = 未设置过期\n");
    printf("    :-2  = key 已过期删除\n");
    printf("\n");
    printf(ANSI_BOLD "按 Ctrl+C 退出\n" ANSI_RESET);
    printf(ANSI_CYAN "================================================\n\n" ANSI_RESET);

    /* ---- Phase 4: 轮询 TTL ---- */
    int ttl_samples[] = {0, 100, 500, 1000, 5000, 9999, -1};
    int rounds = 0;
    int all_expired = 0;

    while (!all_expired) {
        struct timeval now;
        gettimeofday(&now, NULL);
        double elapsed = (double)(now.tv_sec - t1.tv_sec)
                       + (double)(now.tv_usec - t1.tv_usec) / 1000000.0;

        printf(ANSI_CLEAR_LINE "  [第 %d 轮] 已过 %.0f 秒\n", ++rounds, elapsed);

        all_expired = 1;
        for (int si = 0; ttl_samples[si] >= 0; si++) {
            if (ttl_samples[si] >= g_opt.count) continue;
            char key[MAX_KEY_LEN];
            snprintf(key, sizeof(key), "expire:k:%06d", ttl_samples[si]);
            char *r = cmd(fd, "HTTL", key, NULL);
            if (r && r[0] == ':') {
                int ttl_val = atoi(r + 1);
                if (ttl_val == -2)
                    printf("    %s => 已过期 (key 不存在)\n", key);
                else if (ttl_val == -1)
                    printf("    %s => 无过期 (持久 key)\n", key);
                else {
                    printf("    %s => TTL=%d\n", key, ttl_val);
                    all_expired = 0;
                }
            }
            free(r);
        }

        if (elapsed > (double)g_opt.ttl + 5) {
            printf(ANSI_CLEAR_LINE "  TTL+5s 已到，检查全部 key 是否存在...\n");
            int exists = 0;
            for (int i = 0; i < g_opt.count; i++) {
                char key[MAX_KEY_LEN];
                snprintf(key, sizeof(key), "expire:k:%06d", i);
                char *r = cmd(fd, "HEXIST", key, NULL);
                if (r && r[0] == ':' && atoi(r + 1) == 1) exists++;
                free(r);
            }
            if (exists == 0) {
                printf(ANSI_GREEN "  所有 %d 个 key 已按 TTL=%ds 正常过期\n" ANSI_RESET,
                       g_opt.count, g_opt.ttl);
                break;
            } else {
                printf(ANSI_YELLOW "  仍有 %d 个 key 未过期，继续等待...\n" ANSI_RESET, exists);
            }
        }

        if (elapsed > (double)g_opt.ttl * 4) {
            printf(ANSI_YELLOW "  超时退出\n" ANSI_RESET);
            break;
        }

        sleep(1);
    }

    close(fd);
    return 0;
}
