/*
 * test_mmap_recover.c — mmap 恢复验证测试
 *
 * 验证 kvstore 启动时通过 mmap 恢复 dump 文件的正确性与性能。
 * 用户手动管理 kvstore 生命周期，程序负责写入、SAVE、验证。
 *
 * 编译:
 *   make test_mmap_recover
 *
 * 流程:
 *   Step 1: 用户启动 kvstore（见下方用法说明）
 *   Step 2: 本程序连接 kvstore → 按指定引擎写入 N 条数据
 *   Step 3: 本程序执行 SAVE
 *   Step 4: 用户停止并重启 kvstore
 *   Step 5: 本程序自动验证数据恢复，从 INFO 读取 mmap 恢复统计
 *
 * 用法:
 *   # 终端 1: 启动 kvstore
 *   ./kvstore kvstore.conf --role master
 *
 *   # 终端 2: 运行测试
 *   ./test_mmap_recover --config tests/test.conf --engine hash
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
#include <sys/wait.h>
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
    const char *engine;
} g_opt = {
    .host = "192.168.233.128",
    .port = 5160,
    .count = 10000,
    .batch = 1000,
    .engine = "hash",
};

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
    printf(ANSI_YELLOW "[INFO]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void pass(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_GREEN "[PASS]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void fail(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_RED "[FAIL]" ANSI_RESET " ");
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

/* 获取 INFO 中指定字段的值 */
static char *get_info_field(const char *info, const char *field) {
    if (!info || !field) return NULL;
    const char *p = strstr(info, field);
    if (!p) return NULL;
    p += strlen(field);
    if (*p != '=') return NULL;
    p++;
    const char *start = p;
    while (*p && *p != '\r' && *p != '\n') p++;
    size_t len = (size_t)(p - start);
    char *out = (char *)malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

/* ── 引擎辅助 ── */

static const char *engine_prefix(void) {
    if (!strcmp(g_opt.engine, "hash")) return "H";
    if (!strcmp(g_opt.engine, "rbtree")) return "R";
    if (!strcmp(g_opt.engine, "skiptable")) return "X";
    return ""; /* array */
}

static int engine_adjust_count(void) {
    if (!strcmp(g_opt.engine, "array") && g_opt.count > 1024)
        return 1024;
    return g_opt.count;
}

/* ── 主测试逻辑 ── */

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
        else if (strcmp(key, "engine") == 0) g_opt.engine = strdup(val);
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
        else if (strcmp(argv[i], "--engine") == 0 && i + 1 < argc)
            g_opt.engine = argv[++i];
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("用法: %s [选项]\n", argv[0]);
            printf("\nmmap 恢复验证测试:\n");
            printf("  1. 用户手动启动 kvstore\n");
            printf("  2. 本程序写入 N 条数据并 SAVE\n");
            printf("  3. 用户手动停止并重启 kvstore\n");
            printf("  4. 本程序验证数据恢复和 mmap 统计\n\n");
            printf("选项:\n");
            printf("  --host HOST     kvstore 地址 (默认 %s)\n", g_opt.host);
            printf("  --port PORT     kvstore 端口 (默认 %d)\n", g_opt.port);
            printf("  --count N       写入数据量 (默认 %d)\n", g_opt.count);
            printf("  --engine NAME   引擎: array/hash/rbtree/skiptable (默认 %s)\n", g_opt.engine);
            printf("  --batch N       每批写入量 (默认 %d)\n", g_opt.batch);
            printf("  --config PATH   加载配置文件 (默认 tests/test.conf)\n");
            printf("  -h              显示此帮助\n");
            printf("\n示例:\n");
            printf("  # 终端 1: ./kvstore kvstore.conf --role master\n");
            printf("  # 终端 2: %s --config tests/test.conf --engine hash\n", argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            return 1;
        }
    }

    g_opt.count = engine_adjust_count();
    const char *prefix = engine_prefix();
    char set_cmd[32], get_cmd[32];
    snprintf(set_cmd, sizeof(set_cmd), "%sSET", prefix);
    snprintf(get_cmd, sizeof(get_cmd), "%sGET", prefix);

    banner("mmap 恢复验证测试");
    info("引擎: %s (SET=%s, GET=%s)", g_opt.engine, set_cmd, get_cmd);
    info("目标: %s:%d", g_opt.host, g_opt.port);
    info("数据量: %d", g_opt.count);
    info("");

    /* ── Step 1: 连接 ── */
    banner("Step 1: 连接 kvstore");
    prompt("请确保 kvstore 已在 %s:%d 上启动", g_opt.host, g_opt.port);
    int fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fail("无法连接 kvstore"); return 1; }
    /* ── Step 2: 写入数据 ── */
    banner("Step 2: 写入数据并 SAVE");

    for (int i = 1; i <= g_opt.count; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "mmap:key:%d", i);
        snprintf(val, sizeof(val), "value:%d", i);
        char *r = cmd(fd, set_cmd, key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("SET 失败 @ %d (engine=%s), resp=%s", i, g_opt.engine, r ? r : "(null)");
            free(r); close(fd); return 1;
        }
        free(r);
        if (i % g_opt.batch == 0)
            info("已写入 %d/%d", i, g_opt.count);
    }
    pass("写入 %d 条完成 (engine=%s)", g_opt.count, g_opt.engine);

    char *save_r = cmd(fd, "SAVE", NULL);
    if (!save_r || strcmp(save_r, "+OK\r\n") != 0) {
        fail("SAVE 失败, resp=%s", save_r ? save_r : "(null)");
        free(save_r); close(fd); return 1;
    }
    free(save_r);
    pass("SAVE 完成");
    close(fd);

    /* ── Step 3: 重启 ── */
    banner("Step 3: 重启 kvstore");
    prompt("请停止 kvstore (Ctrl+C) 并重新启动 (相同参数)");
    wait_kvstore_down(0.3);
    prompt("请重新启动 kvstore (相同参数)");
    info("等待 kvstore 恢复...");
    double restart_start = time_now();
    if (wait_kvstore_up(60, 0.5) != 0) {
        fail("重启后 kvstore 未就绪");
        return 1;
    }
    double restart_wall = time_now() - restart_start;
    pass("重启并就绪耗时: %.3f 秒", restart_wall);

    /* ── Step 4: 获取 INFO + 验证数据 ── */
    banner("Step 4: 验证数据恢复与 mmap 统计");
    fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { fail("重启后无法连接"); return 1; }

    /* 读取 INFO 获取恢复统计 */
    char *info_r = cmd(fd, "INFO", NULL);
    if (info_r) {
        char *v;
        info("恢复统计:");
        if ((v = get_info_field(info_r, "recover_total_ms")))      { info("  recover_total_ms=%s", v); free(v); }
        if ((v = get_info_field(info_r, "recover_dump_ms")))       { info("  recover_dump_ms=%s", v); free(v); }
        if ((v = get_info_field(info_r, "recover_aof_ms")))        { info("  recover_aof_ms=%s", v); free(v); }
        if ((v = get_info_field(info_r, "recover_mmap_attempts"))) { info("  recover_mmap_attempts=%s", v); free(v); }
        if ((v = get_info_field(info_r, "recover_mmap_success")))  { info("  recover_mmap_success=%s", v); free(v); }
        if ((v = get_info_field(info_r, "recover_mmap_fallbacks"))){ info("  recover_mmap_fallbacks=%s", v); free(v); }
        free(info_r);
    }

    /* 验证数据恢复 */
    char last_key[MAX_KEY_LEN], expected[64];
    snprintf(last_key, sizeof(last_key), "mmap:key:%d", g_opt.count);
    snprintf(expected, sizeof(expected), "value:%d", g_opt.count);
    char *gr = cmd(fd, get_cmd, last_key, NULL);
    if (!gr || !strstr(gr, expected)) {
        fail("数据恢复验证失败: key=%s, expected=%s, resp=%s",
             last_key, expected, gr ? gr : "(null)");
        free(gr); close(fd); return 1;
    }
    pass("数据恢复验证通过: %s = %s", last_key, expected);
    free(gr);
    close(fd);

    /* ── 结果 ── */
    banner("结果汇总");
    pass("MMAP_RECOVER_TEST_PASS");
    stat_line("引擎",         "%s", g_opt.engine);
    stat_line("数据量",       "%d", g_opt.count);
    stat_line("重启恢复耗时", "%.3f 秒", restart_wall);
    return 0;
}
