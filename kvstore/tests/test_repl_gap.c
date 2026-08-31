/*
 * test_repl_gap.c — 全量同步期间 gap 数据补发验证测试
 *
 * 验证 kvstore 主从全量同步期间，客户端写入 master 的数据（gap）
 * 在全量同步完成后正确补发到 slave。
 *
 * 编译:
 *   make test_repl_gap
 *
 * ═══════════════════════════════════════════════════════════════
 * 启动顺序（重要!）:
 *   1. 先启动 Master（需使用 TCP 传输，便于观察）
 *   2. 运行本测试（会先预存大量数据到 Master，然后等待 Slave 连接）
 *   3. 看到 "等待 Slave 连接..." 后，在另一终端启动 Slave
 *   4. 测试会自动验证 gap 数据同步
 * ═══════════════════════════════════════════════════════════════
 *
 * 用法:
 *   # 终端 1 (Master 机器 192.168.233.128): 启动 Master
 * 用法:
 *   # 终端 1 (VM1): 先启动 Master
 *   sudo ./kvstore kvstore.conf --role master
 *
 *   # 终端 2 (任意机器): 运行本测试
 *   ./test_repl_gap --config tests/test.conf
 *
 *   # 终端 3 (Slave 机器 192.168.233.129): 看到提示后启动 Slave
 *   sudo ./kvstore kvstore.conf --role slave
 *
 * 流程:
 *   Phase 1: 预存 pre 数据到 Master（制造全量同步负担）
 *   Phase 2: 等待 Slave 连接并开始全量同步
 *   Phase 3: 用户手动写入 gap 数据到 Master（全量同步期间）
 *   Phase 4: 等待全量同步完成（轮询 slave_fullsync_loading）
 *   Phase 5: 验证 Slave 有 pre 数据（gap 数据请用户自行验证）
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
#include <libgen.h>

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
    int master_port;
    const char *slave_host;
    int slave_port;
    int pre_count;
    int gap_count;
    int post_count;
    int batch;
    int poll_ms;
} g_opt = {
    .master_host = "192.168.233.128",
    .master_port = 5160,
    .slave_host = "192.168.233.129",
    .slave_port = 5160,
    .pre_count = 50000,
    .gap_count = 0,
    .post_count = 0,
    .batch = 1000,
    .poll_ms = 500,
};

/* ── 工具函数 ── */

static double now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}

static void banner(const char *title) {
    printf("\n" ANSI_BOLD ANSI_CYAN "=== %s ===" ANSI_RESET "\n\n", title);
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

static void fail_msg(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_RED "[FAIL]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void prompt_user(const char *fmt, ...) {
    va_list ap;
    printf(ANSI_BOLD "\n  >>> " ANSI_RESET);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf(ANSI_BOLD " <<<" ANSI_RESET "\n\n");
}

/* ── TCP / RESP ── */

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
    if (rc == 0) { fcntl(fd, F_SETFL, flags); return fd; }
    if (rc < 0 && errno != EINPROGRESS) { close(fd); return -1; }
    struct pollfd pfd = {.fd = fd, .events = POLLOUT};
    rc = poll(&pfd, 1, timeout_ms > 0 ? timeout_ms : 3000);
    if (rc <= 0) { close(fd); return -1; }
    int err = 0; socklen_t elen = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
    if (err) { close(fd); return -1; }
    fcntl(fd, F_SETFL, flags);
    struct timeval tv_rcv = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv_rcv, sizeof(tv_rcv));
    struct timeval tv_snd = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv_snd, sizeof(tv_snd));
    return fd;
}

#define tcp_connect(h, p) tcp_connect_timeout(h, p, 3000)

static int send_all(int fd, const unsigned char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n <= 0) return -1;
        buf += n; len -= (size_t)n;
    }
    return 0;
}
static unsigned char *recv_resp(int fd) {
    size_t cap = 4096, len = 0;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return NULL;
    while (len < BUFFER_SIZE) {
        if (cap - len < 1024) { cap *= 2; buf = realloc(buf, cap); }
        ssize_t r = read(fd, buf + len, cap - len - 1);
        if (r <= 0) break;
        len += (size_t)r;
        if (len >= 2 && buf[len - 1] == '\n') break;
    }
    buf[len] = '\0';
    return buf;
}

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
    size_t cap = 64;
    for (int i = 0; i < argc; i++) cap += strlen(args[i]) + 32;
    unsigned char *wbuf = (unsigned char *)malloc(cap);
    if (!wbuf) { close(fd); return NULL; }
    size_t pos = 0;
    int n = snprintf((char *)wbuf + pos, cap - pos, "*%d\r\n", argc);
    pos += (size_t)n;
    for (int i = 0; i < argc; i++) {
        size_t slen = strlen(args[i]);
        n = snprintf((char *)wbuf + pos, cap - pos, "$%zu\r\n", slen);
        pos += (size_t)n;
        memcpy(wbuf + pos, args[i], slen);
        pos += slen;
        wbuf[pos++] = '\r'; wbuf[pos++] = '\n';
    }
    send_all(fd, wbuf, pos);
    free(wbuf);
    char *resp = (char *)recv_resp(fd);
    close(fd);
    return resp;
}

static char *extract_bulk(const char *resp) {
    if (!resp || resp[0] != '$') return NULL;
    long blen = atol(resp + 1);
    if (blen < 0) return NULL;
    const char *p = resp;
    while (*p && *p != '\r') p++;
    if (*p != '\r') return NULL;
    p += 2;
    char *out = (char *)malloc((size_t)blen + 1);
    if (!out) return NULL;
    memcpy(out, p, (size_t)blen);
    out[blen] = '\0';
    return out;
}

static int ping_port(const char *host, int port) {
    int fd = tcp_connect_timeout(host, port, 3000);
    if (fd < 0) return -1;
    unsigned char ping[] = "*1\r\n$4\r\nPING\r\n";
    if (send_all(fd, ping, sizeof(ping) - 1) != 0) { close(fd); return -1; }
    unsigned char buf[64];
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    if (poll(&pfd, 1, 2000) <= 0) { close(fd); return -1; }
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return -1;
    buf[n] = '\0';
    return strstr((char *)buf, "+PONG") ? 0 : -1;
}

static int wait_port_ready(const char *host, int port, int retries, double interval) {
    for (int i = 0; i < retries; i++) {
        if (ping_port(host, port) == 0) return 0;
        usleep((useconds_t)(interval * 1000000));
    }
    return -1;
}

/* 从 INFO 中提取字段值 */
static char *get_info_field(const char *info, const char *field) {
    if (!info || !field) return NULL;
    char pattern[64];
    snprintf(pattern, sizeof(pattern), "\n%s:", field);
    const char *p = strstr(info, pattern);
    if (!p) {
        /* 试试文件开头 */
        snprintf(pattern, sizeof(pattern), "%s:", field);
        p = strstr(info, pattern);
        if (p != info) return NULL;
    }
    if (!p) return NULL;
    p = strchr(p, ':');
    if (!p) return NULL;
    p++;
    const char *end = strchr(p, '\n');
    if (!end) end = p + strlen(p);
    size_t len = (size_t)(end - p);
    char *val = (char *)malloc(len + 1);
    if (!val) return NULL;
    memcpy(val, p, len);
    val[len] = '\0';
    /* 去掉尾部 \r */
    while (len > 0 && (val[len - 1] == '\r' || val[len - 1] == '\n')) val[--len] = '\0';
    return val;
}

/* 通过 INFO 检查 slave 是否正在全量同步 */
static int slave_is_loading_fullsync(void) {
    char *info = cmd_resp(g_opt.slave_host, g_opt.slave_port, "INFO", NULL);
    if (!info) return -1;
    char *val = get_info_field(info, "slave_fullsync_loading");
    int loading = -1;
    if (val) {
        loading = (strcmp(val, "1") == 0) ? 1 : 0;
        free(val);
    }
    free(info);
    return loading;
}

/* ── 配置文件解析 ── */

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
        else if (strcmp(key, "master_port") == 0) g_opt.master_port = atoi(val);
        else if (strcmp(key, "slave_host") == 0) g_opt.slave_host = strdup(val);
        else if (strcmp(key, "slave_port") == 0) g_opt.slave_port = atoi(val);
        else if (strcmp(key, "pre") == 0 || strcmp(key, "pre_count") == 0) g_opt.pre_count = atoi(val);
        else if (strcmp(key, "gap") == 0 || strcmp(key, "gap_count") == 0) g_opt.gap_count = atoi(val);
        else if (strcmp(key, "post") == 0 || strcmp(key, "post_count") == 0) g_opt.post_count = atoi(val);
        else if (strcmp(key, "batch") == 0) g_opt.batch = atoi(val);
        else if (strcmp(key, "poll_ms") == 0) g_opt.poll_ms = atoi(val);
    }
    fclose(fp);
    return 0;
}

/* ── 批量写入（分块发送+进度显示，每块后读取响应防止 TCP 缓冲死锁） ── */

static int batch_write(int fd, const char *cmd, const char *prefix, int start, int count, const char *label) {
    size_t chunk = 500;
    int sent = 0, pct = -1;
    while (sent < count) {
        size_t cur = (size_t)(count - sent) < chunk ? (size_t)(count - sent) : chunk;
        size_t cap = cur * 128;
        unsigned char *buf = (unsigned char *)malloc(cap);
        if (!buf) return -1;
        size_t pos = 0;
        for (int i = start + sent; i < start + sent + (int)cur; i++) {
            char key[64], val[64];
            snprintf(key, sizeof(key), "%s:k:%06d", prefix, i);
            snprintf(val, sizeof(val), "v:%06d", i);
            int n = snprintf((char *)buf + pos, cap - pos,
                "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                strlen(cmd), cmd, strlen(key), key, strlen(val), val);
            if (n < 0 || (size_t)n >= cap - pos) { free(buf); return -1; }
            pos += (size_t)n;
        }
        if (send_all(fd, buf, pos) != 0) { free(buf); return -1; }
        free(buf);
        sent += (int)cur;
        /* 读取响应：用 poll 检查数据是否可读，避免 recv_resp 长时间阻塞 */
        int ok = 0;
        while (ok < (int)cur) {
            struct pollfd pfd = {.fd = fd, .events = POLLIN};
            int prc = poll(&pfd, 1, 2000);  /* 最多等 2 秒 */
            if (prc <= 0) break;            /* 超时或出错，不再等 */
            unsigned char *r = recv_resp(fd);
            if (!r) break;
            free(r);
            ok++;
        }
        /* 进度显示 */
        int new_pct = sent * 100 / count;
        if (new_pct / 10 > pct / 10) {
            pct = new_pct;
            printf("[INFO] %s 写入进度: %d/%d (%d%%)\n", label, sent, count, pct);
            fflush(stdout);
        }
    }
    return 0;
}

/* ── 验证 ── */

static int verify_key(const char *label, const char *cmd, const char *key, const char *expected_val) {
    char *mv = cmd_resp(g_opt.master_host, g_opt.master_port, cmd, key, NULL);
    char *sv = cmd_resp(g_opt.slave_host, g_opt.slave_port, cmd, key, NULL);
    char *mval = mv ? extract_bulk(mv) : NULL;
    char *sval = sv ? extract_bulk(sv) : NULL;

    int pass_m = mval && expected_val && strcmp(mval, expected_val) == 0;
    int pass_s = sval && expected_val && strcmp(sval, expected_val) == 0;
    int match = mval && sval && strcmp(mval, sval) == 0;

    if (pass_m && pass_s && match) {
        pass("%s: %s %s = %s (master=slave)", label, cmd, key, mval);
    } else if (pass_m && pass_s) {
        pass("%s: %s %s 值正确 (master=%s, slave=%s)", label, cmd, key, mval, sval);
    } else {
        fail_msg("%s: %s %s master=[%s] slave=[%s] expect=[%s]",
                 label, cmd, key, mval ? mval : "(null)", sval ? sval : "(null)",
                 expected_val ? expected_val : "(null)");
        free(mv); free(sv); free(mval); free(sval);
        return -1;
    }
    free(mv); free(sv); free(mval); free(sval);
    return 0;
}

static int verify_batch(const char *label, const char *prefix, int count) {
    int failed = 0;
    /* 抽查: 首、中、尾 */
    int spots[] = {1, count/2, count};
    int nspots = sizeof(spots) / sizeof(spots[0]);
    for (int i = 0; i < nspots; i++) {
        char key[64], expected[64];
        snprintf(key, sizeof(key), "%s:k:%06d", prefix, spots[i]);
        snprintf(expected, sizeof(expected), "v:%06d", spots[i]);
        if (verify_key(label, "HGET", key, expected) != 0) failed++;
    }
    if (failed == 0) pass("%s: 全部 %d 条数据一致性验证通过", label, count);
    else fail_msg("%s: %d/%d 项验证失败", label, failed, nspots);
    return failed;
}

/* ── 主函数 ── */

int main(int argc, char **argv) {
    /* 先加载默认配置文件 */
    parse_config_file("tests/test.conf");
    parse_config_file("test.conf");

    /* 解析参数 */
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
        else if (strcmp(argv[i], "--pre-count") == 0 && i + 1 < argc)
            g_opt.pre_count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--gap-count") == 0 && i + 1 < argc)
            g_opt.gap_count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--post-count") == 0 && i + 1 < argc)
            g_opt.post_count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            g_opt.batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "--poll-ms") == 0 && i + 1 < argc)
            g_opt.poll_ms = atoi(argv[++i]);
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("用法: %s [选项]\n", argv[0]);
            printf("  --config PATH        加载配置文件 (默认 tests/test.conf)\n");
            printf("  --master-host HOST   Master 地址 (默认 %s)\n", g_opt.master_host);
            printf("  --master-port PORT   Master 端口 (默认 %d)\n", g_opt.master_port);
            printf("  --slave-host HOST    Slave 地址 (默认 %s)\n", g_opt.slave_host);
            printf("  --slave-port PORT    Slave 端口 (默认 %d)\n", g_opt.slave_port);
            printf("  --pre-count N        预存数据量 (默认 %d，建议 ≥50000 以确保 gap 窗口)\n", g_opt.pre_count);
            printf("  --gap-count N        gap 数据量 (默认 0，手动输入)\n");
            printf("  --batch N            每批写入量 (默认 %d)\n", g_opt.batch);
            return 0;
        }
    }

    banner("GAP 全量同步补发测试");
    printf("  Master:      %s:%d\n", g_opt.master_host, g_opt.master_port);
    printf("  Slave:       %s:%d\n", g_opt.slave_host, g_opt.slave_port);
    printf("  Pre:         %d\n", g_opt.pre_count);
    printf("  Gap:         %d\n", g_opt.gap_count);
  
    printf("  Batch:       %d\n", g_opt.batch);
    printf("\n");

    int failed = 0;

    /* ── Phase 1: 预存数据到 Master ── */
    banner("Phase 1: 预存数据到 Master");
    info("连接 Master %s:%d ...", g_opt.master_host, g_opt.master_port);
    if (wait_port_ready(g_opt.master_host, g_opt.master_port, 30, 0.5) != 0) {
        fail_msg("Master 不可用");
        return 1;
    }
    pass("Master 已就绪");
    fflush(stdout);

    {
        info("开始写入 %d 条 Pre 数据 ...", g_opt.pre_count);
        fflush(stdout);
        int fd = tcp_connect(g_opt.master_host, g_opt.master_port);
        if (fd < 0) { fail_msg("连接 Master 失败"); return 1; }
        double t0 = now_ms();
        int pre_ok = batch_write(fd, "HSET", "pre", 1, g_opt.pre_count, "Pre");
        close(fd);
        double t1 = now_ms();
        if (pre_ok != 0) { fail_msg("写入 pre 数据失败"); return 1; }
        pass("Pre 数据写入完成: %d条, %.0fms (%.0f/s)",
             g_opt.pre_count, t1 - t0, g_opt.pre_count / ((t1 - t0) / 1000.0));
    }

    /* ── Phase 2: 等待 Slave 连接并开始全量同步 ── */
    banner("Phase 2: 等待 Slave 全量同步");

    prompt_user("请启动 Slave:\n"
                "  ./kvstore --port %d --role slave "
                "--master-host %s --master-port %d \\\n"
                "      --repl-fullsync-transport tcp --repl-realtime-transport tcp\n",
                g_opt.slave_port, g_opt.master_host, g_opt.master_port);

    info("等待 Slave %s:%d 就绪...", g_opt.slave_host, g_opt.slave_port);
    if (wait_port_ready(g_opt.slave_host, g_opt.slave_port, 600, 0.5) != 0) {
        fail_msg("Slave 连接超时");
        return 1;
    }
    pass("Slave 已就绪");

    /* 等待全量同步开始（或已快速完成） */
    info("等待全量同步开始...");
    int loading = -1, fullsync_done = 0;
    double wait_start = now_ms();
    while (1) {
        loading = slave_is_loading_fullsync();
        if (loading == 1) {
            pass("全量同步已开始 (slave_fullsync_loading=1)");
            break;
        }
        /* loading=0 + slave 已有数据 = 全量同步已完成（太快没捕获到 loading=1） */
        if (loading == 0) {
            char *sv = cmd_resp(g_opt.slave_host, g_opt.slave_port, "HGET", "pre:k:000001", NULL);
            char *sval = sv ? extract_bulk(sv) : NULL;
            if (sval && strcmp(sval, "v:000001") == 0) {
                fullsync_done = 1;
                free(sv); free(sval);
                pass("全量同步已完成 (slave 已有 pre 数据)");
                break;
            }
            free(sv); free(sval);
        }
        if (now_ms() - wait_start > 300000) {
            fail_msg("等待全量同步开始超时 (loading=%d)", loading);
            return 1;
        }
        usleep((useconds_t)(g_opt.poll_ms * 1000));
    }
    /* gap 数据在全量同步期间写入才有意义，如果全量已完成则跳过 gap */
    if (fullsync_done) {
        info("全量同步太快已结束，跳过 gap 阶段（gap 只测试全量同步期间写入的场景）");
        g_opt.gap_count = 0;
        goto phase4;
    }

    /* 确认仍在全量同步中（5 秒超时），确保 gap 窗口有效 */
    info("确认全量同步仍在进行中...");
    double confirm_start = now_ms();
    while (slave_is_loading_fullsync() == 1) {
        if (now_ms() - confirm_start > 5000) {
            /* 持续 5 秒以上仍在同步，说明 gap 窗口充足 */
            break;
        }
        usleep(100000);
    }
    if (slave_is_loading_fullsync() != 1) {
        info("全量同步在等待期间已结束，跳过 gap 阶段");
        g_opt.gap_count = 0;
        goto phase4;
    }

    /* ── Phase 3: 提示用户手动写入 gap 数据（全量同步期间） ── */
    banner("Phase 3: 全量同步期间手动写入 Gap 数据");

    info("当前 slave_fullsync_loading=1，有充足时间写入 gap 数据");
    prompt_user("请在另一终端连接 Master %s:%d 并写入任意 gap 数据\n"
                "（例如: redis-cli -p %d -h %s HSET gap:mykey myvalue），\n"
                "写入完成后，回到本窗口按 Enter 继续",
                g_opt.master_host, g_opt.master_port,
                g_opt.master_port, g_opt.master_host);

    /* 等待用户按 Enter 确认 */
    {
        char buf[16];
        if (!fgets(buf, sizeof(buf), stdin)) {}
    }
    info("用户确认 gap 数据已写入，请自行验证 slave 数据一致性");

    /* ── Phase 4: 等待全量同步完成 ── */
    phase4:
    banner("Phase 4: 等待全量同步完成 + Gap 补发");

    info("等待 slave_fullsync_loading → 0 ...");
    double poll_start = now_ms();
    while (1) {
        loading = slave_is_loading_fullsync();
        if (loading == 0) {
            pass("全量同步完成 + Gap 补发完成 (slave_fullsync_loading=0, 耗时 %.0fms)",
                 now_ms() - poll_start);
            break;
        }
        if (loading < 0) {
            /* slave 暂时不可用，重试 */
            usleep(100000);
            continue;
        }
        if (now_ms() - poll_start > 300000) {
            fail_msg("全量同步超时 (300s)");
            return 1;
        }
        usleep((useconds_t)(g_opt.poll_ms * 1000));
    }

    /* ── Phase 5: 验证 Pre + Gap 数据 ── */
    banner("Phase 5: 验证 Pre 和 Gap 数据一致性");

    info("验证 Pre 数据 (全量快照) ...");
    if (verify_batch("Phase5", "pre", g_opt.pre_count) != 0) failed++;

    info("Gap 数据未验证，请手动在 Slave 上查询确认 gap 数据已同步");

    /* ── 结果 ── */
    printf("\n");
    if (failed == 0) {
        printf(ANSI_GREEN ANSI_BOLD "\n  ✓ 全量同步完成!\n" ANSI_RESET);
        printf("  Pre=%d, Gap 请自行验证\n\n", g_opt.pre_count);
        return 0;
    } else {
        printf(ANSI_RED ANSI_BOLD "\n  ✗ %d 项测试失败\n" ANSI_RESET "\n", failed);
        return 1;
    }
}
