/*
 * test_repl_basic.c — 主从复制基本验证测试
 *
 * 验证 kvstore 主从复制功能：全量同步、增量同步、数据一致性。
 * 用户手动管理 Master/Slave 进程，程序负责写入、监控、验证。
 *
 * 编译:
 *   make test_repl_basic
 *
 * ═══════════════════════════════════════════════════════════════
 * 启动顺序（重要!）:
 *   1. 先启动 Master
 *   2. 再运行本测试（预存数据、等待 slave 连接）
 *   3. 看到提示后，再启动 Slave
 * ═══════════════════════════════════════════════════════════════
 *
 * 流程:
 *   Step 1: 用户启动 Master
 *   Step 2: 本程序连接 Master → 跨引擎预存 N 条数据
 *   Step 3: 提示用户启动 Slave → 等待全量同步完成
 *   Step 4: 再写入增量数据 → 等待增量同步
 *   Step 5: 验证 Master/Slave 数据一致性（多引擎）
 *   Step 6: 可选: SLAVEOF NO ONE → 断开写入 → SLAVEOF 重连 → 验证
 *
 * 用法:
 *   # 终端 1: 启动 Master（先启动）
 *   ./kvstore kvstore.conf --role master
 *
 *   # 终端 2: 运行测试
 *   ./test_repl_basic --config tests/test.conf
 *
 *   # 终端 3: 看到提示后再启动 Slave
 *   ./kvstore kvstore.conf --role slave
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
#define MAX_KEY_LEN 128

#define ANSI_GREEN   "\033[32m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_RESET   "\033[0m"

static struct {
    const char *master_host;
    const char *slave_host;
    int master_port;
    int slave_port;
    int count;
    int batch;
    int poll_ms;
} g_opt = {
    .master_host = "192.168.233.128",
    .slave_host = "192.168.233.129",
    .master_port = 5160,
    .slave_port = 5160,
    .count = 5000,
    .batch = 500,
    .poll_ms = 500,
};

__attribute__((unused)) static double time_now(void) {
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

__attribute__((unused)) static void stat_line(const char *label, const char *fmt, ...) {
    char val[128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(val, sizeof(val), fmt, ap);
    va_end(ap);
    printf("  %-24s %s\n", label, val);
}
/* ── TCP / RESP ── */

/* 带超时（3 秒）的 TCP 连接 */
static int tcp_connect_timeout(const char *host, int port, int timeout_ms) {
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
    if (rc == 0) {
        fcntl(fd, F_SETFL, flags);
        struct timeval tv = {3, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        return fd;
    }
    if (rc < 0 && errno != EINPROGRESS) { close(fd); return -1; }

    struct pollfd pfd = {.fd = fd, .events = POLLOUT};
    rc = poll(&pfd, 1, timeout_ms > 0 ? timeout_ms : 3000);
    if (rc <= 0) { close(fd); return -1; }

    int err = 0;
    socklen_t elen = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
    if (err) { close(fd); return -1; }

    fcntl(fd, F_SETFL, flags);
    struct timeval tv = {3, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return fd;
}

#define tcp_connect(h, p) tcp_connect_timeout(h, p, 3000)

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

static char *cmd_at(int fd, const char *arg0, ...) {
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

/* 从指定端口发送命令并返回原始 RESP 响应 */
static char *cmd_resp(const char *host, int port, const char *arg0, ...) {
    int fd = tcp_connect(host, port);
    if (fd < 0) return NULL;
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
    if (!wbuf) { close(fd); return NULL; }
    send_all(fd, wbuf, wlen);
    free(wbuf);
    char *resp = (char *)recv_resp(fd);
    close(fd);
    return resp;
}

/* 从 RESP 响应中提取批量字符串的值 */
/* 输入: "$5\r\nhello\r\n" → 输出 "hello"（需 free）*/
/* 输入: "$-1\r\n" → 输出 NULL（空值）*/
static char *extract_bulk(const char *resp) {
    if (!resp || resp[0] != '$') return NULL;
    /* 解析 $len\r\n */
    long blen = atol(resp + 1);
    if (blen < 0) return NULL; /* $-1 = null bulk */
    /* 找到数据起始位置（跳过 $len\r\n） */
    const char *p = resp;
    while (*p && *p != '\r') p++;
    if (*p != '\r') return NULL;
    p += 2; /* 跳过 \r\n */
    /* 复制 blen 字节，截掉尾部 \r\n */
    char *out = (char *)malloc((size_t)blen + 1);
    if (!out) return NULL;
    memcpy(out, p, (size_t)blen);
    out[blen] = '\0';
    return out;
}

/* 对比 master 和 slave 上同一个 key 的值 */
static int compare_key(const char *get_cmd, const char *key) {
    char *mv = cmd_resp(g_opt.master_host, g_opt.master_port, get_cmd, key, NULL);
    char *sv = cmd_resp(g_opt.slave_host, g_opt.slave_port, get_cmd, key, NULL);
    int ret = -1;
    /* 提取批量字符串 payload */
    char *mval = mv ? extract_bulk(mv) : NULL;
    char *sval = sv ? extract_bulk(sv) : NULL;
    if (mval && sval && strcmp(mval, sval) == 0) {
        pass("数据一致: %s %s = %s", get_cmd, key, mval);
        ret = 0;
    } else {
        fail("数据不一致: %s %s, master=[%s], slave=[%s]",
             get_cmd, key, mval ? mval : "(null)", sval ? sval : "(null)");
    }
    free(mv); free(sv); free(mval); free(sval);
    return ret;
}

/* 发送 PING 并检查是否收到 +PONG，带 3 秒超时 */
static int ping_port(const char *host, int port) {
    int fd = tcp_connect_timeout(host, port, 3000);
    if (fd < 0) {
        fprintf(stderr, "  [dbg] ping_port %d: connect failed (errno=%d)\n", port, errno);
        return -1;
    }

    /* 发送 *1\r\n$4\r\nPING\r\n */
    unsigned char ping[] = "*1\r\n$4\r\nPING\r\n";
    if (send_all(fd, ping, sizeof(ping) - 1) != 0) {
        fprintf(stderr, "  [dbg] ping_port %d: send failed (errno=%d)\n", port, errno);
        close(fd); return -1;
    }

    /* 用 poll + 单次 read 检查响应，最多等 2 秒 */
    unsigned char buf[64];
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int pret = poll(&pfd, 1, 2000);
    if (pret <= 0) {
        fprintf(stderr, "  [dbg] ping_port %d: poll timeout/err (pret=%d, errno=%d)\n", port, pret, errno);
        close(fd); return -1;
    }
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) {
        fprintf(stderr, "  [dbg] ping_port %d: read failed (n=%zd, errno=%d)\n", port, n, errno);
        return -1;
    }
    buf[n] = '\0';
    if (strstr((char *)buf, "+PONG") == NULL) {
        fprintf(stderr, "  [dbg] ping_port %d: unexpected resp [%s]\n", port, (char *)buf);
        return -1;
    }
    return 0;
}

static int wait_port_ready(const char *host, int port, int retries, double interval) {
    for (int i = 0; i < retries; i++) {
        if (ping_port(host, port) == 0) return 0;
        usleep((useconds_t)(interval * 1000000));
    }
    return -1;
}

/* 查询 INFO 字段 */
static int check_info_field(const char *host, int port, const char *field, const char *value) {
    int fd = tcp_connect(host, port);
    if (fd < 0) return -1;
    char *info = cmd_at(fd, "INFO", NULL);
    close(fd);
    if (!info) return -1;
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "%s:%s", field, value);
    int found = strstr(info, pattern) != NULL ? 0 : -1;
    free(info);
    return found;
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
        if (strcmp(key, "master_host") == 0) g_opt.master_host = strdup(val);
        else if (strcmp(key, "slave_host") == 0) g_opt.slave_host = strdup(val);
        else if (strcmp(key, "master_port") == 0) g_opt.master_port = atoi(val);
        else if (strcmp(key, "slave_port") == 0) g_opt.slave_port = atoi(val);
        else if (strcmp(key, "count") == 0) g_opt.count = atoi(val);
        else if (strcmp(key, "batch") == 0) g_opt.batch = atoi(val);
        else if (strcmp(key, "poll_ms") == 0) g_opt.poll_ms = atoi(val);
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
        else if (strcmp(argv[i], "--master-host") == 0 && i + 1 < argc)
            g_opt.master_host = argv[++i];
        else if (strcmp(argv[i], "--slave-host") == 0 && i + 1 < argc)
            g_opt.slave_host = argv[++i];
        else if (strcmp(argv[i], "--master-port") == 0 && i + 1 < argc)
            g_opt.master_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--slave-port") == 0 && i + 1 < argc)
            g_opt.slave_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--count") == 0 && i + 1 < argc)
            g_opt.count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            g_opt.batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "--poll") == 0 && i + 1 < argc)
            g_opt.poll_ms = atoi(argv[++i]);
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("用法: %s [选项]\n", argv[0]);
            printf("\n主从复制基本验证测试:\n");
            printf("  ═══ 启动顺序: ① Master → ② 本脚本 → ③ Slave ═══\n");
            printf("  1. 用户手动启动 Master\n");
            printf("  2. 本程序连接 Master → 跨引擎预存数据\n");
            printf("  3. 提示用户启动 Slave\n");
            printf("  4. 等待全量同步 → 写入增量 → 验证一致性\n\n");
            printf("选项:\n");
            printf("  --master-host HOST   Master 地址 (默认 %s)\n", g_opt.master_host);
            printf("  --slave-host HOST    Slave 地址 (默认 %s)\n", g_opt.slave_host);
            printf("  --master-port PORT    Master 端口 (默认 %d)\n", g_opt.master_port);
            printf("  --slave-port PORT     Slave 端口 (默认 %d)\n", g_opt.slave_port);
            printf("  --count N             预存/增量数据量 (默认 %d)\n", g_opt.count);
            printf("  --batch N             每批写入量 (默认 %d)\n", g_opt.batch);
            printf("  --poll MS             轮询间隔毫秒 (默认 %d)\n", g_opt.poll_ms);
            printf("  --config PATH         加载配置文件 (默认 tests/test.conf)\n");
            printf("  -h                    显示此帮助\n");
            printf("\n示例:\n");
            printf("  # 终端 1 (先启动 Master):\n");
            printf("  ./kvstore kvstore.conf --role master\n");
            printf("  # 终端 2:\n");
            printf("  %s --config tests/test.conf\n", argv[0]);
            printf("  # 终端 3 (看到提示后再启动 Slave):\n");
            printf("  rm -f kvstore.dump kvstore.aof    # 清理旧数据\n");
            printf("  ./kvstore kvstore.conf --role slave\n");
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            return 1;
        }
    }

    banner("主从复制基本验证测试");
    info("Master: %s:%d, Slave: %s:%d", g_opt.master_host, g_opt.master_port, g_opt.slave_host, g_opt.slave_port);
    info("预存/增量数据量: %d", g_opt.count);
    info("");

    /* ── Step 1: 连接 Master ── */
    banner("Step 1: 连接 Master 并预存数据");
    prompt("请确保 Master 已在 %s:%d 上启动", g_opt.master_host, g_opt.master_port);

    int fd = tcp_connect(g_opt.master_host, g_opt.master_port);
    if (fd < 0) { fail("无法连接 Master"); return 1; }
    info("Master 已连接 ✓");

    /* 写入各引擎测试数据 */
    /* Hash 引擎 */
    info("写入 Hash 引擎数据...");
    for (int i = 1; i <= g_opt.count; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "h:pre:%d", i);
        snprintf(val, sizeof(val), "hv:%d", i);
        char *r = cmd_at(fd, "HSET", key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("HSET 失败 @ %d, resp=%s", i, r ? r : "(null)");
            free(r); close(fd); return 1;
        }
        free(r);
        if (i % g_opt.batch == 0) info("  Hash: %d/%d", i, g_opt.count);
    }

    /* Array 引擎 */
    info("写入 Array 引擎数据...");
    for (int i = 1; i <= g_opt.count && i <= 1024; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "a:pre:%d", i);
        snprintf(val, sizeof(val), "av:%d", i);
        char *r = cmd_at(fd, "SET", key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("SET 失败 @ %d, resp=%s", i, r ? r : "(null)");
            free(r); close(fd); return 1;
        }
        free(r);
    }

    /* RBTREE 引擎 */
    info("写入 RBTREE 引擎数据...");
    for (int i = 1; i <= g_opt.count && i <= 1000; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "r:pre:%d", i);
        snprintf(val, sizeof(val), "rv:%d", i);
        char *r = cmd_at(fd, "RSET", key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("RSET 失败 @ %d, resp=%s", i, r ? r : "(null)");
            free(r); close(fd); return 1;
        }
        free(r);
    }

    /* Skiptable 引擎 */
    info("写入 Skiptable 引擎数据...");
    for (int i = 1; i <= g_opt.count && i <= 1000; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "x:pre:%d", i);
        snprintf(val, sizeof(val), "xv:%d", i);
        char *r = cmd_at(fd, "XSET", key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("XSET 失败 @ %d, resp=%s", i, r ? r : "(null)");
            free(r); close(fd); return 1;
        }
        free(r);
    }

    close(fd);
    pass("预存数据完成 (4 引擎, %d 条)", g_opt.count);

    /* ── Step 2: 启动 Slave ── */
    banner("Step 2: 启动 Slave 并等待全量同步");
    prompt("请在另一个终端启动 Slave:\n  ./kvstore --port %d --role slave \\\n      --master-host %s --master-port %d \\\n      --repl-fullsync-transport tcp --repl-realtime-transport tcp",
           g_opt.slave_port, g_opt.master_host, g_opt.master_port);

    info("等待 Slave (%s:%d) 就绪...", g_opt.slave_host, g_opt.slave_port);
    info("（最多等待 120 秒，每 0.5 秒探测一次 PING 响应）");
    if (wait_port_ready(g_opt.slave_host, g_opt.slave_port, 240, 0.5) != 0) {
        fail("Slave 未就绪"); return 1;
    }
    info("Slave 已就绪 ✓");

    /* 等待全量同步完成 */
    info("监控全量同步进度...");
    int sync_ok = 0;
    for (int i = 0; i < 120; i++) {
        if (check_info_field(g_opt.slave_host, g_opt.slave_port, "slave_fullsync_loading", "0") == 0 &&
            check_info_field(g_opt.slave_host, g_opt.slave_port, "master_link", "up") == 0) {
            pass("全量同步完成");
            sync_ok = 1;
            break;
        }
        usleep(g_opt.poll_ms * 1000);
    }
    if (!sync_ok) { fail("全量同步超时"); return 1; }

    /* ── Step 3: 增量写入 ── */
    banner("Step 3: 增量写入并等待同步");

    int post_count = g_opt.count > 1000 ? 1000 : g_opt.count;
    fd = tcp_connect(g_opt.master_host, g_opt.master_port);
    if (fd < 0) { fail("无法连接 Master"); return 1; }

    for (int i = 1; i <= post_count; i++) {
        char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "h:post:%d", i);
        snprintf(val, sizeof(val), "hv_post:%d", i);
        char *r = cmd_at(fd, "HSET", key, val, NULL);
        if (!r || (strcmp(r, "+OK\r\n") != 0 && strcmp(r, ":1\r\n") != 0)) {
            fail("增量 HSET 失败 @ %d", i);
            free(r); close(fd); return 1;
        }
        free(r);
        if (i % 200 == 0) info("增量: %d/%d", i, post_count);
    }
    close(fd);
    info("增量写入完成，等待同步...");
    sleep(3);

    /* ── Step 4: 验证一致性 ── */
    banner("Step 4: 验证主从数据一致性");

    int ok = 1;
    /* Hash 引擎预存 */
    ok &= (compare_key("HGET", "h:pre:1") == 0) ? 1 : 0;
    ok &= (compare_key("HGET", "h:pre:100") == 0) ? 1 : 0;
    ok &= (compare_key("HGET", "h:pre:5000") == 0) ? 1 : 0;
    /* Array 引擎预存 */
    ok &= (compare_key("GET", "a:pre:1") == 0) ? 1 : 0;
    ok &= (compare_key("GET", "a:pre:512") == 0) ? 1 : 0;
    /* RBTREE 引擎预存 */
    ok &= (compare_key("RGET", "r:pre:1") == 0) ? 1 : 0;
    ok &= (compare_key("RGET", "r:pre:500") == 0) ? 1 : 0;
    /* Skiptable 引擎预存 */
    ok &= (compare_key("XGET", "x:pre:1") == 0) ? 1 : 0;
    ok &= (compare_key("XGET", "x:pre:999") == 0) ? 1 : 0;
    /* 增量数据 */
    ok &= (compare_key("HGET", "h:post:1") == 0) ? 1 : 0;
    ok &= (compare_key("HGET", "h:post:1000") == 0) ? 1 : 0;

    if (ok) pass("数据一致性验证通过 (4 引擎, 含增量)");
    else    fail("数据一致性验证失败");

    /* ── 结果 ── */
    banner("结果汇总");
    if (ok) { pass("REPL_BASIC_TEST_PASS"); return 0; }
    else    { fail("REPL_BASIC_TEST_FAILED"); return 1; }
}
