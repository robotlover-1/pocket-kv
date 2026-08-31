/*
 * test_persist_dump_demo.c — 全量持久化演示 (dump)
 *
 * 流程:
 *   用户启动 kvstore → 本程序写入数据 → SAVE → 用户停止 kvstore
 *   → 用户重启 kvstore → 本程序验证数据从 dump 恢复
 *
 * 编译:
 *   make test_persist_dump_demo
 *
 * 用法:
 *   # 终端 1: 启动 kvstore
 *   ./kvstore kvstore.conf --role master
 *
 *   # 终端 2: 运行全量持久化演示
 *   ./test_persist_dump_demo --config tests/test.conf
 *
 *   程序写入数据后会提示你停止并重启 kvstore，然后自动验证数据恢复。
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

/* ---------- 配置 ---------- */
#define BUFFER_SIZE 65536
#define MAX_KEY_LEN 64

/* ---------- ANSI ---------- */
#define ANSI_GREEN   "\033[32m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_RESET   "\033[0m"

/* ---------- 选项 ---------- */
static struct {
    const char *host;
    int port;
    int count;
    int batch;
    const char *dump_path;
    const char *aof_path;
} g_opt = {
    .host = "192.168.233.128",
    .port = 5160,
    .count = 50000,
    .batch = 1000,
    .dump_path = "kvstore.dump",
    .aof_path = "kvstore.aof",
};

/* ---------- 辅助函数 ---------- */

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

static void prompt(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_BOLD "\n  >>> " ANSI_RESET);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf(ANSI_BOLD " <<<" ANSI_RESET "\n\n");
}

static void info(const char *fmt, ...) {
    va_list ap;
    printf("  ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void stat_line(const char *label, const char *value) {
    printf("  %-20s %s\n", label, value);
}

/* ---------- TCP / RESP ---------- */

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
            close(fd);
            return -1;
        }
    }
    struct timeval tv = {1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
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
        buf += n;
        len -= (size_t)n;
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

/* 等待 kvstore 就绪（连上为止） */
static int wait_kvstore_up(int retries, double interval) {
    for (int i = 0; i < retries; i++) {
        int fd = tcp_connect(g_opt.host, g_opt.port);
        if (fd >= 0) {
            char *r = cmd(fd, "PING", NULL);
            close(fd);
            if (r && strcmp(r, "+PONG\r\n") == 0) {
                free(r);
                return 0;
            }
            free(r);
        }
        usleep((useconds_t)(interval * 1000000));
    }
    return -1;
}

/* 等待 kvstore 断开 */
static void wait_kvstore_down(double poll_interval) {
    info("等待 kvstore 断开...");
    fflush(stdout);
    while (1) {
        int fd = tcp_connect(g_opt.host, g_opt.port);
        if (fd < 0) {
            info("kvstore 已断开 ✓");
            fflush(stdout);
            return;
        }
        close(fd);
        usleep((useconds_t)(poll_interval * 1000000));
    }
}

/* 获取文件大小（自动尝试当前目录和父目录） */
static long long file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) return (long long)st.st_size;
    /* 尝试父目录（兼容从 tests/ 子目录运行） */
    char parent[512];
    snprintf(parent, sizeof(parent), "../%s", path);
    if (stat(parent, &st) == 0) return (long long)st.st_size;
    return -1;
}

static const char *fmt_bytes(long long bytes) {
    static char buf[32];
    if (bytes < 0) return "(不存在)";
    if (bytes < 1024)
        snprintf(buf, sizeof(buf), "%lld B", bytes);
    else if (bytes < 1024 * 1024)
        snprintf(buf, sizeof(buf), "%.1f KB", (double)bytes / 1024);
    else
        snprintf(buf, sizeof(buf), "%.1f MB", (double)bytes / 1024 / 1024);
    return buf;
}

/* ================================================================
 * 主流程
 * ================================================================ */

int main(int argc, char **argv) {
    /* 默认加载 tests/test.conf */
    {
        FILE *fp = fopen("tests/test.conf", "r");
        if (!fp) fp = fopen("test.conf", "r");
        if (fp) {
            char line[256];
            while (fgets(line, sizeof(line), fp)) {
                char *p = line;
                while (*p && isspace((unsigned char)*p)) p++;
                if (*p == '#' || *p == '\0' || *p == '\n') continue;
                char *eq = strchr(p, '=');
                if (!eq) continue;
                *eq = '\0';
                char *key = p, *val = eq + 1;
                while (val && (*val == ' ' || *val == '\t')) val++;
                char *end = val + strlen(val) - 1;
                while (end > val && isspace((unsigned char)*end)) *end-- = '\0';
                if (strcmp(key, "host") == 0) g_opt.host = strdup(val);
                else if (strcmp(key, "port") == 0) g_opt.port = atoi(val);
                else if (strcmp(key, "count") == 0) g_opt.count = atoi(val);
                else if (strcmp(key, "batch") == 0) g_opt.batch = atoi(val);
                else if (strcmp(key, "dump_path") == 0) g_opt.dump_path = strdup(val);
                else if (strcmp(key, "aof_path") == 0) g_opt.aof_path = strdup(val);
            }
            fclose(fp);
        }
    }

    /* 解析参数 */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            /* --config 已由上面的默认加载处理，此处重新加载指定文件 */
            i++;
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc)
            g_opt.host = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            g_opt.port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--count") == 0 && i + 1 < argc)
            g_opt.count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            g_opt.batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "--dump-path") == 0 && i + 1 < argc)
            g_opt.dump_path = argv[++i];
        else if (strcmp(argv[i], "--aof-path") == 0 && i + 1 < argc)
            g_opt.aof_path = argv[++i];
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("用法: %s [选项]\n", argv[0]);
            printf("\n全量持久化演示 (dump):\n");
            printf("  1. 用户手动启动 kvstore\n");
            printf("  2. 本程序写入数据并执行 SAVE\n");
            printf("  3. 用户手动停止 kvstore\n");
            printf("  4. 用户手动重启 kvstore\n");
            printf("  5. 本程序验证数据从 dump 恢复\n\n");
            printf("选项:\n");
            printf("  --config PATH   加载配置文件 (默认 tests/test.conf)\n");
            printf("  --host HOST     kvstore 地址 (默认 %s)\n", g_opt.host);
            printf("  --port PORT     kvstore 端口 (默认 %d)\n", g_opt.port);
            printf("  --count N       写入数据量 (默认 %d)\n", g_opt.count);
            printf("  --batch N       每批写入量 (默认 %d)\n", g_opt.batch);
            printf("  --dump-path PATH  dump 文件路径 (默认 kvstore.dump)\n");
            printf("  --aof-path PATH    AOF 文件路径 (默认 kvstore.aof)\n");
            printf("  -h              显示此帮助\n");
            printf("\n示例:\n");
            printf("  # 终端 1: ./kvstore kvstore.conf --role master\n");
            printf("  # 终端 2: %s --config tests/test.conf\n", argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            return 1;
        }
    }

    printf(ANSI_BOLD "kvstore 全量持久化演示 (dump)" ANSI_RESET "\n\n");
    info("kvstore 地址: %s:%d", g_opt.host, g_opt.port);
    info("数据量: %d", g_opt.count);
    info("");

    int fd = -1;
    int exit_code = 1;

    /* ═══════════════════════════════════════
     * Step 1: 连接 kvstore
     * ═══════════════════════════════════════ */
    banner("Step 1: 连接 kvstore");

    prompt("请确保 kvstore 已在 %s:%d 上启动", g_opt.host, g_opt.port);
    info("等待 kvstore 就绪...");
    if (wait_kvstore_up(60, 0.5) != 0) {
        fprintf(stderr, ANSI_RED "  ERROR: kvstore 未能在 %s:%d 就绪\n" ANSI_RESET,
                g_opt.host, g_opt.port);
        return 1;
    }
    info(ANSI_GREEN "kvstore 已连接 ✓" ANSI_RESET);

    fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { perror("connect"); return 1; }

    /* ═══════════════════════════════════════
     * Step 2: 写入数据
     * ═══════════════════════════════════════ */
    banner("Step 2: 写入数据");

    int count = g_opt.count;
    double t0 = time_now();
    info("正在写入 %d 条 HSET 数据 (persist:dump:*) ...", count);

    for (int batch_start = 0; batch_start < count; batch_start += g_opt.batch) {
        int batch_end = batch_start + g_opt.batch;
        if (batch_end > count) batch_end = count;
        for (int i = batch_start; i < batch_end; i++) {
            char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
            snprintf(key, sizeof(key), "persist:dump:%06d", i);
            snprintf(val, sizeof(val), "v%d", i);
            char *r = cmd(fd, "HSET", key, val, NULL);
            if (!r || strcmp(r, "+OK\r\n") != 0) {
                fprintf(stderr, ANSI_RED "  ERROR: 写入失败 at %d: %s\n" ANSI_RESET,
                        i, r ? r : "(no response)");
                free(r);
                close(fd);
                return 1;
            }
            free(r);
        }
        double elapsed = time_now() - t0;
        printf("\033[2K\r  写入: %s%d/%d%s keys, %.0f qps, %.1fs",
               ANSI_YELLOW, batch_end, count, ANSI_RESET,
               batch_end / (elapsed > 0 ? elapsed : 0.001), elapsed);
        fflush(stdout);
    }
    printf("\n");
    double write_time = time_now() - t0;
    info(ANSI_GREEN "写入完成 ✓" ANSI_RESET "  %d keys, %.2fs, %.0f qps",
         count, write_time, count / write_time);

    /* ═══════════════════════════════════════
     * Step 3: 执行 SAVE
     * ═══════════════════════════════════════ */
    banner("Step 3: 执行 SAVE");

    info("发送 SAVE 命令 (全量持久化到 dump 文件)...");
    char *r = cmd(fd, "SAVE", NULL);
    if (!r || strcmp(r, "+OK\r\n") != 0) {
        fprintf(stderr, ANSI_RED "  ERROR: SAVE 失败: %s\n" ANSI_RESET, r ? r : "(no response)");
        free(r);
        close(fd);
        return 1;
    }
    free(r);
    info(ANSI_GREEN "SAVE 完成 ✓" ANSI_RESET);

    close(fd);
    fd = -1;

    /* 显示持久化文件信息 */
    long long dump_sz = file_size(g_opt.dump_path);
    long long aof_sz = file_size(g_opt.aof_path);
    stat_line("dump 文件", fmt_bytes(dump_sz));
    stat_line("aof  文件", fmt_bytes(aof_sz));
    info("");

    if (dump_sz <= 0) {
        fprintf(stderr, ANSI_RED "  ERROR: dump 文件不存在或为空 (%s)\n" ANSI_RESET, g_opt.dump_path);
        fprintf(stderr, "  提示: 可通过 --dump-path 指定正确的 dump 文件路径\n");
        return 1;
    }

    /* ═══════════════════════════════════════
     * Step 4: 用户停止 kvstore
     * ═══════════════════════════════════════ */
    banner("Step 4: 停止 kvstore");

    prompt("请在另一个终端按 Ctrl+C 停止 kvstore，或执行: kill $(lsof -ti:%d)", g_opt.port);

    wait_kvstore_down(0.5);

    /* ═══════════════════════════════════════
     * Step 5: 用户重启 kvstore
     * ═══════════════════════════════════════ */
    banner("Step 5: 重启 kvstore");

    prompt("请重新启动 kvstore");

    info("等待 kvstore 就绪...");
    if (wait_kvstore_up(120, 0.5) != 0) {
        fprintf(stderr, ANSI_RED "  ERROR: kvstore 重启后未就绪\n" ANSI_RESET);
        return 1;
    }
    info(ANSI_GREEN "kvstore 已重启 ✓" ANSI_RESET);

    /* ═══════════════════════════════════════
     * Step 6: 验证数据恢复
     * ═══════════════════════════════════════ */
    banner("Step 6: 验证数据恢复");

    fd = tcp_connect(g_opt.host, g_opt.port);
    if (fd < 0) { perror("connect"); return 1; }

    info("验证 %d 条数据是否从 dump 恢复...", count);
    int failed = 0;
    double v0 = time_now();

    for (int i = 0; i < count; i++) {
        char key[MAX_KEY_LEN], expected[64];
        snprintf(key, sizeof(key), "persist:dump:%06d", i);
        snprintf(expected, sizeof(expected), "v%d", i);

        char *r = cmd(fd, "HGET", key, NULL);
        if (!r) { failed++; free(r); if (failed <= 3) fprintf(stderr, "  [FAIL] %s 无响应\n", key); }
        else {
            char check[256];
            snprintf(check, sizeof(check), "$%zu\r\n%s\r\n", strlen(expected), expected);
            if (strcmp(r, check) != 0) {
                failed++;
                if (failed <= 3)
                    fprintf(stderr, "  [FAIL] %s 期望 %s 实际 %s\n", key, expected, r);
            }
            free(r);
        }
        if (failed > 100) {
            info("失败过多，终止验证");
            break;
        }
        if (i % 1000 == 999 || i == count - 1) {
            double elapsed = time_now() - v0;
            printf("\033[2K\r  验证: %s%d/%d%s keys, %.0f qps, %.1fs",
                   ANSI_YELLOW, i + 1, count, ANSI_RESET,
                   (i + 1) / (elapsed > 0 ? elapsed : 0.001), elapsed);
            fflush(stdout);
        }
    }
    printf("\n");
    close(fd);

    double verify_time = time_now() - v0;

    /* ═══════════════════════════════════════
     * 结果
     * ═══════════════════════════════════════ */
    banner("结果");

    if (failed == 0) {
        printf(ANSI_GREEN ANSI_BOLD "  ✓ 全部 %d 条数据从 dump 恢复成功!\n" ANSI_RESET, count);
        stat_line("写入", fmt_bytes(count));
        printf("  %-20s %d keys, %.2fs, %.0f qps\n", "写入性能", count, write_time, count / write_time);
        printf("  %-20s %d keys, %.2fs, %.0f qps\n", "验证性能", count, verify_time, count / verify_time);
        stat_line("dump 文件", fmt_bytes(dump_sz));
        printf("\n" ANSI_GREEN "  全量持久化演示通过 ✓" ANSI_RESET "\n");
        exit_code = 0;
    } else {
        printf(ANSI_RED ANSI_BOLD "  ✗ %d/%d 条数据未从 dump 恢复!\n" ANSI_RESET, failed, count);
        exit_code = 1;
    }

    return exit_code;
}
