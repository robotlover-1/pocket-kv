/*
 * test_batch.c — 批量流水线压力测试
 *
 * 验证 kvstore 的 RESP 流水线处理能力：
 *   1. 通过单次 send 发送 N 条命令，不逐个等待响应
 *   2. 之后一次性读取所有响应并验证
 *   3. 测试纯写入流水线、纯读取流水线、混合流水线
 *
 * 编译:
 *   make test_batch
 *
 * 用法:
 *   # 终端 1: 启动 kvstore
 *   ./kvstore kvstore.conf --role master
 *
 *   # 终端 2: 运行本测试
 *   ./test_batch --config tests/test.conf
 *
 * 选项:
 *   --host HOST       kvstore 地址 (默认 192.168.233.128)
 *   --port PORT       kvstore 端口 (默认 5160)
 *   --count N         每条流水线的命令数 (默认 10000)
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
#include <ctype.h>

#define BUFFER_SIZE (1024 * 1024)
#define MAX_KEY_LEN 64
#define MAX_RESP_SIZE (1024 * 1024)

#define ANSI_GREEN   "\033[32m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_RESET   "\033[0m"

static struct {
    const char *host;
    int port;
    int count;
    const char *dump_path;
    const char *aof_path;
} g_opt = {
    .host = "192.168.233.128",
    .port = 5160,
    .count = 10000,
    .dump_path = "kvstore.dump",
    .aof_path = "kvstore.aof",
};

/* ==== TCP / RESP 工具 ==== */

static int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    struct hostent *he = gethostbyname(host);
    if (he) memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);
    else if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) { close(fd); return -1; }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) { close(fd); return -1; }
    return fd;
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

/* ==== 响应验证 ==== */

static int count_ok(const unsigned char *resp, size_t len) {
    int ok = 0;
    size_t pos = 0;
    while (pos < len) {
        if (resp[pos] == '+') {
            if (pos + 5 <= len && memcmp(resp + pos, "+OK\r\n", 5) == 0) ok++;
            pos++;
            while (pos < len && resp[pos] != '\n') pos++;
            if (pos < len) pos++;
        } else if (resp[pos] == '$') {
            char *end;
            long blen = strtol((const char *)resp + pos + 1, &end, 10);
            if (!end || *end != '\r') break;
            /* 从当前 pos 到 \r\n 的偏移 */
            size_t hdr = (size_t)(end - (const char *)(resp + pos)) + 2;
            size_t skip = hdr;
            if (blen >= 0) skip += (size_t)blen + 2;
            if (pos + skip > len) break;
            ok++;
            pos += skip;
        } else if (resp[pos] == ':') {
            ok++;
            pos++;
            while (pos < len && resp[pos] != '\n') pos++;
            if (pos < len) pos++;
        } else if (resp[pos] == '-') {
            pos++;
            while (pos < len && resp[pos] != '\n') pos++;
            if (pos < len) pos++;
        } else break;
    }
    return ok;
}

/* ==== 单条流水线测试 ==== */

/* 响应读取辅助：用 poll 读取，直到收够 need 条或超时 */
static int drain_responses(int fd, unsigned char *resp, size_t *rlen, size_t cap, int need) {
    while (count_ok(resp, *rlen) < need && *rlen < cap) {
        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        int prc = poll(&pfd, 1, 5000);
        if (prc <= 0) break;
        ssize_t r = read(fd, resp + *rlen, cap - *rlen - 1);
        if (r <= 0) break;
        *rlen += (size_t)r;
    }
    return count_ok(resp, *rlen);
}

/* 分块发送 HSET/HGET，每块后读响应，避免 TCP 缓冲区死锁 */
static int run_pipeline(int fd, const char *label,
    unsigned char *batch, size_t blen, int expected)
{
    (void)batch;
    (void)blen;
    struct timeval t0, t1;
    gettimeofday(&t0, NULL);

    int is_get = (strcmp(label, "HGET") == 0);
    size_t chunk = 500;
    int sent = 0;
    unsigned char resp[1024 * 1024];
    size_t rlen = 0;

    while (sent < expected) {
        size_t cur = (size_t)(expected - sent);
        if (cur > chunk) cur = chunk;
        size_t cap = cur * 128;
        unsigned char *buf = (unsigned char *)malloc(cap);
        if (!buf) return -1;
        size_t pos = 0;

        for (size_t i = 0; i < cur; i++) {
            int idx = sent + (int)i;
            char key[64], val[64];
            snprintf(key, sizeof(key), "batch:k:%06d", idx);
            if (is_get) {
                int n = snprintf((char *)buf + pos, cap - pos,
                    "*2\r\n$4\r\nHGET\r\n$%zu\r\n%s\r\n", strlen(key), key);
                if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
                pos += (size_t)n;
            } else {
                snprintf(val, sizeof(val), "batch:v:%06d", idx);
                int n = snprintf((char *)buf + pos, cap - pos,
                    "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                    strlen(key), key, strlen(val), val);
                if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
                pos += (size_t)n;
            }
        }

        if (send_all(fd, buf, pos) != 0) { free(buf); return -1; }
        free(buf);
        sent += (int)cur;
        drain_responses(fd, resp, &rlen, sizeof(resp), sent);
    }

    /* 读取剩余响应 */
    drain_responses(fd, resp, &rlen, sizeof(resp), expected);
    int total_ok = count_ok(resp, rlen);

    gettimeofday(&t1, NULL);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec)
                   + (double)(t1.tv_usec - t0.tv_usec) / 1000000.0;
    double qps = (double)expected / (elapsed > 0 ? elapsed : 0.001);

    if (total_ok == expected) {
        printf(ANSI_GREEN "  [PASS]" ANSI_RESET " %s: %d/%d, %.0f qps, %.3fs\n",
               label, total_ok, expected, qps, elapsed);
        return 0;
    } else {
        printf(ANSI_RED "  [FAIL]" ANSI_RESET " %s: %d/%d, %.0f qps, %.3fs\n",
               label, total_ok, expected, qps, elapsed);
        return -1;
    }
}

/* 混合流水线：每轮 HSET+HGET 两条命令 */
static int run_mixed_pipeline(int fd, int rounds) {
    struct timeval t0, t1;
    gettimeofday(&t0, NULL);

    size_t chunk = 500;
    int sent = 0, expected = rounds * 2;
    unsigned char resp[1024 * 1024];
    size_t rlen = 0;

    while (sent < rounds) {
        size_t cur = (size_t)(rounds - sent);
        if (cur > chunk) cur = chunk;
        size_t cap = cur * 256;
        unsigned char *buf = (unsigned char *)malloc(cap);
        if (!buf) return -1;
        size_t pos = 0;

        for (size_t i = 0; i < cur; i++) {
            int idx = sent + (int)i;
            char key[64], val[64];
            snprintf(key, sizeof(key), "batch:mix:%06d", idx);
            snprintf(val, sizeof(val), "mix:v:%06d", idx);
            int n = snprintf((char *)buf + pos, cap - pos,
                "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n"
                "*2\r\n$4\r\nHGET\r\n$%zu\r\n%s\r\n",
                strlen(key), key, strlen(val), val,
                strlen(key), key);
            if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
            pos += (size_t)n;
        }

        if (send_all(fd, buf, pos) != 0) { free(buf); return -1; }
        free(buf);
        sent += (int)cur;
        drain_responses(fd, resp, &rlen, sizeof(resp), sent * 2);
    }

    drain_responses(fd, resp, &rlen, sizeof(resp), expected);
    int total_ok = count_ok(resp, rlen);

    gettimeofday(&t1, NULL);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec)
                   + (double)(t1.tv_usec - t0.tv_usec) / 1000000.0;
    double qps = (double)expected / (elapsed > 0 ? elapsed : 0.001);

    if (total_ok == expected) {
        printf(ANSI_GREEN "  [PASS]" ANSI_RESET " HSET+HGET: %d/%d, %.0f qps, %.3fs\n",
               total_ok, expected, qps, elapsed);
        return 0;
    } else {
        printf(ANSI_RED "  [FAIL]" ANSI_RESET " HSET+HGET: %d/%d, %.0f qps, %.3fs\n",
               total_ok, expected, qps, elapsed);
        return -1;
    }
}

/* ==== 配置文件解析 ==== */

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
        else if (strcmp(key, "dump_path") == 0) g_opt.dump_path = strdup(val);
        else if (strcmp(key, "aof_path") == 0) g_opt.aof_path = strdup(val);
    }
    fclose(fp);
    return 0;
}

/* ==== 帮助 ==== */

static void print_usage(const char *prog) {
    printf("用法: %s [选项]\n", prog);
    printf("  批量流水线压力测试\n\n");
    printf("选项:\n");
    printf("  --config PATH 加载配置文件 (默认 tests/test.conf)\n");
    printf("  --host HOST   kvstore 地址 (默认 %s)\n", g_opt.host);
    printf("  --port PORT   kvstore 端口 (默认 %d)\n", g_opt.port);
    printf("  --count N     每条流水线的命令数 (默认 %d)\n", g_opt.count);
    printf("  --dump-path PATH  dump 文件路径 (默认 %s)\n", g_opt.dump_path);
    printf("  --aof-path PATH    AOF 文件路径 (默认 %s)\n", g_opt.aof_path);
    printf("  -h            显示帮助\n");
}

/* ==== 主函数 ==== */

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

    printf(ANSI_BOLD "批量流水线压力测试\n" ANSI_RESET);
    printf("  地址: %s:%d\n", g_opt.host, g_opt.port);
    printf("  每条流水线: %d 条命令\n\n", g_opt.count);

    int failed = 0;

    /* 测试 1: 写入流水线 */
    printf("--- 写入流水线 ---\n");
    int fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fprintf(stderr, "无法连接 %s:%d\n", g_opt.host, g_opt.port); return 1; }
    if (run_pipeline(fd, "HSET", NULL, 0, g_opt.count) != 0) failed++;
    close(fd);

    /* 测试 2: 读取流水线（新连接） */
    printf("--- 读取流水线 ---\n");
    fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fprintf(stderr, "无法连接 %s:%d\n", g_opt.host, g_opt.port); return 1; }
    if (run_pipeline(fd, "HGET", NULL, 0, g_opt.count) != 0) failed++;
    close(fd);

    /* 测试 3: 混合流水线（新连接） */
    printf("--- 混合流水线 ---\n");
    fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fprintf(stderr, "无法连接 %s:%d\n", g_opt.host, g_opt.port); return 1; }
    if (run_mixed_pipeline(fd, g_opt.count) != 0) failed++;
    close(fd);

    printf("\n");
    if (failed == 0)
        printf(ANSI_GREEN ANSI_BOLD "  全部流水线测试通过 ✓\n" ANSI_RESET);
    else
        printf(ANSI_RED ANSI_BOLD "  %d 项测试失败 ✗\n" ANSI_RESET, failed);

    return failed;
}
