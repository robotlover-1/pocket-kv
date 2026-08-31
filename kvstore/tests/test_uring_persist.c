/*
 * test_uring_persist.c — io_uring 持久化验证测试
 *
 * 测试 io_uring 写入路径的持久化正确性与性能。
 * 用户手动管理 kvstore 生命周期，程序负责写入、SAVE、验证。
 *
 * 编译:
 *   make test_uring_persist
 *
 * 流程:
 *   Step 1: 用户启动 kvstore（见下方用法说明）
 *   Step 2: 本程序连接 kvstore → 写入 N 条 HSET → 记录写入耗时
 *   Step 3: 本程序执行 SAVE → 记录 SAVE 耗时
 *   Step 4: 用户停止并重启 kvstore
 *   Step 5: 本程序自动验证数据恢复
 *
 * 用法:
 *   # 终端 1: 启动 kvstore
 *   ./kvstore kvstore.conf --role master
 *
 *   # 终端 2: 运行测试
 *   ./test_uring_persist --config tests/test.conf
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
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

static struct {
    const char *host;
    int port;
    int count;
    int batch;
} g_opt = {
    .host = "192.168.233.128",
    .port = 5160,
    .count = 10000,
    .batch = 1000,
};

static double g_time_write = 0;
static double g_time_save = 0;
static double g_time_verify = 0;

static double time_now(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static void banner(const char *title) {
    printf("\n" ANSI_BOLD ANSI_CYAN "%s" ANSI_RESET "\n", title);
    for (const char *p = title; *p; p++) printf("=");
    printf("\n\n");
}

static void info(const char *fmt, ...) {
    va_list ap;
    printf("  ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void pass(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_GREEN "  [PASS]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void fail(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_RED "  [FAIL]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void prompt(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_BOLD "\n  >>> " ANSI_RESET);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf(ANSI_BOLD " <<<" ANSI_RESET "\n\n");
}

static void stat_line(const char *label, const char *fmt, ...) {
    char val[128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(val, sizeof(val), fmt, ap);
    va_end(ap);
    printf("  %-24s %s\n", label, val);
}

/* ── TCP / RESP ── */

static int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    struct hostent *he = gethostbyname(host);
    if (he) {
        memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);
    } else {
        addr.sin_addr.s_addr = inet_addr(host);
        if (addr.sin_addr.s_addr == (in_addr_t)-1) {
            close(fd); return -1;
        }
    }
    struct timeval tv = {2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd); return -1;
    }
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
        buf += n; len -= (size_t)n;
    }
    return 0;
}

static unsigned char *recv_resp(int fd) {
    size_t cap = 4096;
    size_t len = 0;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return NULL;
    while (len < BUFFER_SIZE) {
        if (cap - len < 1024) {
            cap *= 2;
            unsigned char *tmp = (unsigned char *)realloc(buf, cap);
            if (!tmp) { free(buf); return NULL; }
            buf = tmp;
        }
        ssize_t r = read(fd, buf + len, cap - len - 1);
        if (r <= 0) break;
        len += (size_t)r;
        if (len >= 2 && buf[len - 1] == '\n' && buf[len - 2] == '\r')
            break;
    }
    buf[len] = '\0';
    return buf;
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
    int ok = send_all(fd, wbuf, wlen);
    free(wbuf);
    if (ok != 0) return NULL;
    return (char *)recv_resp(fd);
}

static int wait_kvstore_up(int retries, double interval) {
    for (int i = 0; i < retries; i++) {
        int fd = tcp_connect(g_opt.host, g_opt.port);
        if (fd >= 0) {
            char *r = cmd(fd, "PING", NULL);
            close(fd);
            if (r && strcmp(r, "+PONG\r\n") == 0) { free(r); return 0; }
            free(r);
        }
        usleep((useconds_t)(interval * 1000000));
    }
    return -1;
}

static void wait_kvstore_down(double interval) {
    info("等待 kvstore 断开...");
    fflush(stdout);
    while (1) {
        int fd = tcp_connect(g_opt.host, g_opt.port);
        if (fd < 0) { info("kvstore 已断开 ✓"); return; }
        close(fd);
        usleep((useconds_t)(interval * 1000000));
    }
}

/* ================================================================
 * 主流程
 * ================================================================ */

/* ── 配置文件解析（支持 --config test.conf） ── */

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
        else if (strcmp(key, "batch") == 0) g_opt.batch = atoi(val);
    }
    fclose(fp);
    return 0;
}

int main(int argc, char **argv) {
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
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            g_opt.batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("用法: %s [选项]\n", argv[0]);
            printf("\nio_uring 持久化验证测试:\n");
            printf("  1. 用户手动启动 kvstore\n");
            printf("  2. 本程序写入数据并执行 SAVE\n");
            printf("  3. 用户手动停止并重启 kvstore\n");
            printf("  4. 本程序验证数据恢复\n\n");
            printf("选项:\n");
            printf("  --host HOST     kvstore 地址 (默认 %s)\n", g_opt.host);
            printf("  --port PORT     kvstore 端口 (默认 %d)\n", g_opt.port);
            printf("  --count N       写入数据量 (默认 %d)\n", g_opt.count);
            printf("  --batch N       每批写入量 (默认 %d)\n", g_opt.batch);
            printf("  --config PATH   加载配置文件 (默认 tests/test.conf)\n");
            printf("  -h              显示此帮助\n");
            printf("\n示例:\n");
            printf("  # 终端 1: ./kvstore --port %d --role master --appendfsync always\n", g_opt.port);
            printf("  # 终端 2: %s --port %d --count %d\n", argv[0], g_opt.port, g_opt.count);
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            return 1;
        }
    }

    banner("io_uring 持久化验证测试");
    info("目标: %s:%d", g_opt.host, g_opt.port);
    info("数据量: %d", g_opt.count);
    info("");

    /* ── Step 1: 连接 ── */
    banner("Step 1: 连接 kvstore");
    prompt("请确保 kvstore 已在 %s:%d 上启动", g_opt.host, g_opt.port);
    int fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fail("无法连接 kvstore"); return 1; }
    info("已连接到 %s:%d ✓", g_opt.host, g_opt.port);

    /* ── Step 2: 写入数据 ── */
    banner("Step 2: 写入数据");
    double t0 = time_now();
    for (int i = 1; i <= g_opt.count; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "uring:key:%d", i);
        snprintf(val, sizeof(val), "value:%d", i);
        char *r = cmd(fd, "HSET", key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("HSET 失败 @ %d, resp=%s", i, r ? r : "(null)");
            free(r); close(fd); return 1;
        }
        free(r);
        if (i % g_opt.batch == 0)
            info("已写入 %d/%d", i, g_opt.count);
    }
    double t1 = time_now();
    g_time_write = t1 - t0;
    pass("写入 %d 条完成, 耗时 %.3f 秒", g_opt.count, g_time_write);

    /* ── Step 3: SAVE ── */
    banner("Step 3: 执行 SAVE");
    char *save_r = cmd(fd, "SAVE", NULL);
    if (!save_r || strcmp(save_r, "+OK\r\n") != 0) {
        fail("SAVE 失败, resp=%s", save_r ? save_r : "(null)");
        free(save_r); close(fd); return 1;
    }
    free(save_r);
    double t2 = time_now();
    g_time_save = t2 - t1;
    pass("SAVE 完成, 耗时 %.3f 秒", g_time_save);
    close(fd);

    /* ── Step 4: 重启 ── */
    banner("Step 4: 重启 kvstore");
    prompt("请停止 kvstore (Ctrl+C) 并重新启动 (相同参数)");
    wait_kvstore_down(0.3);
    prompt("请重新启动 kvstore (相同参数)");
    info("等待 kvstore 恢复...");
    if (wait_kvstore_up(60, 0.5) != 0) {
        fail("重启后 kvstore 未就绪");
        return 1;
    }
    double t3 = time_now();
    info("kvstore 已恢复 ✓");

    /* ── Step 5: 验证恢复 ── */
    banner("Step 5: 验证数据恢复");
    fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fail("无法连接 kvstore"); return 1; }

    int sample_idxs[] = {1, g_opt.count / 2, g_opt.count};
    int all_ok = 1;
    for (int s = 0; s < 3; s++) {
        int idx = sample_idxs[s];
        char key[MAX_KEY_LEN], expected[64];
        snprintf(key, sizeof(key), "uring:key:%d", idx);
        snprintf(expected, sizeof(expected), "value:%d", idx);
        char *r = cmd(fd, "HGET", key, NULL);
        if (!r || !strstr(r, expected)) {
            fail("数据恢复验证失败: key=%s, expected=%s, resp=%s",
                 key, expected, r ? r : "(null)");
            all_ok = 0;
        } else {
            pass("数据恢复验证通过: %s = %s", key, expected);
        }
        free(r);
    }
    double t4 = time_now();
    g_time_verify = t4 - t3;
    close(fd);

    /* ── 结果 ── */
    banner("结果汇总");
    if (!all_ok) { fail("IO_URING_PERSIST_TEST_FAILED"); return 1; }
    pass("IO_URING_PERSIST_TEST_PASS");
    stat_line("写入耗时",       "%.3f 秒 (%.0f ops/s)", g_time_write, g_opt.count / g_time_write);
    stat_line("SAVE 耗时",      "%.3f 秒", g_time_save);
    stat_line("恢复验证耗时",   "%.3f 秒", g_time_verify);
    return 0;
}
