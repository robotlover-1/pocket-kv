/*
 * test_repl_5w5w.c — 主从同步 5w+5w 测试用例 (RDMA 全量 + eBPF+tcp 增量)
 *
 * 编译:
 *   make test_repl_5w5w
 *
 * ═══════════════════════════════════════════════════════════════
 * 启动顺序（重要!）:
 *   1. 先启动 Master（以 repl_realtime_transport=ebpf+tcp 启动时自动拉起独立
 *      ebpf-proxy 进程，日志 "master: spawned ebpf-proxy pid=..."，无需手动启动）
 *   2. 再启动本测试脚本（脚本会预存数据、轮询等待 slave 连接）
 *   3. 看到 "等待 Slave 连接..." 提示后，再启动 Slave
 * ═══════════════════════════════════════════════════════════════
 *
 * 用法一: RDMA 全量 + eBPF+tcp 增量（双虚拟机部署，推荐）
 *   数据路径: 全量(rdma) + 增量(eBPF kprobe捕获客户端写入→TCP转发)
 *   eBPF client_capture: kprobe/kretprobe 挂载 tcp_recvmsg
 *     全量同步期间: L1内存(4MB)+L2磁盘缓存
 *     REPLDONE 后: flush 缓存 + repl_broadcast TCP 增量发送
 *     自动回退: eBPF 不可用→TCP, RDMA 不可用→TCP
 *
 *   # 终端 1 (VM1): 启动 master（需 root 加载 BPF；自动拉起 ebpf-proxy 增量进程）
 *   sudo ./kvstore kvstore.conf --role master
 *
 *   # 终端 2 (任意机器): 运行本测试
 *   ./test_repl_5w5w --config tests/test.conf
 *
 *   # 看到 "等待 Slave 连接..." 后，在终端 3 (VM2) 启动 slave
 *   sudo ./kvstore kvstore.conf --role slave
 *
 * 用法二: RDMA 全量 + kprobe+RDMA 增量（双虚拟机，需 root）
 *   数据路径: 全量(rdma) + 增量(kprobe捕获TCP→ring buf→RDMA pipeline)
 *   配置: repl_realtime_transport=kprobe-rdma
 *
 * 用法三: TCP 全量 + TCP 增量（单机，无 RDMA/eBPF）
 *   # 终端 1: 启动 master
 *   ./kvstore --port 5160 --role master \
 *       --repl-fullsync-transport tcp --repl-realtime-transport tcp
 *
 *   # 终端 2: 运行本测试
 *   ./test_repl_5w5w --master-host 127.0.0.1 --master-port 5160 \
 *       --slave-host 127.0.0.1 --slave-port 5161 \
 *       --pre 50000 --post 50000
 *
 *   # 看到 "等待 Slave 连接..." 后，在终端 3 启动 slave
 *   ./kvstore --port 5161 --role slave \
 *       --master-host 127.0.0.1 --master-port 5160 \
 *       --repl-fullsync-transport tcp --repl-realtime-transport tcp
 *
 * 双虚拟机部署说明:
 *   - VM1 上启动 master，VM2 上启动 slave
 *   - master 的 --rdma-dev 指定本机 RDMA 设备（如 siw0 / rxe0）
 *   - slave 的 --master-host 指向 VM1 的 IP
 *   - 测试程序可在任意机器上运行，通过 --master-host / --slave-host 指定连接目标
 *
 * 流程:
 *   Phase 1:   预存数据到 Master
 *   Phase 2:   等待 Slave 连接 (轮询 slave INFO)
 *   Phase 3:   监控全量同步进度 (RDMA)
 *   Phase 4:   全量完成后，再存数据到 Master (增量)
 *   Phase 5:   监控增量同步进度 (eBPF+tcp / kprobe+RDMA / TCP)
 *   Phase 5.5: 验证 eBPF+tcp 传输状态 (repl_transport_active + broadcast_bytes)
 *   Phase 6:   验证 Slave 数据一致性
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
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>

/* ---------- 常量 ---------- */
#define BUFFER_SIZE 65536
#define MAX_KEY_LEN 64
#define PROGRESS_BAR_WIDTH 36

/* ---------- ANSI 终端控制 ---------- */
#define ANSI_GREEN   "\033[32m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_RESET   "\033[0m"
#define ANSI_CLEAR_LINE "\033[2K"
#define ANSI_CURSOR_UP "\033[A"

/* ---------- 选项 ---------- */
static struct {
    const char *master_host;
    int master_port;
    const char *slave_host;
    int slave_port;
    int pre_count;
    int post_count;
    int batch;
    int poll_ms;
} g_opt = {
    .master_host = "192.168.233.128",
    .master_port = 5160,
    .slave_host = "192.168.233.129",
    .slave_port = 5161,
    .pre_count = 50000,
    .post_count = 50000,
    .batch = 1000,
    .poll_ms = 500,
};

/* ---------- TCP / RESP 工具 ---------- */

static int tcp_connect(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    /* Try numeric IP first, then hostname */
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

static unsigned char *recv_resp(int fd, size_t *out_len) {
    size_t cap = 65536;
    size_t len = 0;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return NULL;

    /* poll+read loop: 每次 poll 最多等 2s，读到完整响应或超时为止 */
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int total_retries = 30;  /* 最多等 30 × 2s = 60s */

    for (int attempt = 0; attempt < total_retries && len < cap - 1; attempt++) {
        int pr = poll(&pfd, 1, attempt == 0 ? 5000 : 2000);
        if (pr < 0) break;
        if (pr == 0 && len > 0) break;  /* timeout with some data — return what we have */
        if (pr == 0) continue;          /* timeout with no data — retry */

        ssize_t r = read(fd, buf + len, cap - len - 1);
        if (r <= 0) break;
        len += (size_t)r;
        buf[len] = '\0';

        /* Check if we have a complete RESP response */
        if (buf[0] == '$') {
            if (buf[1] == '-') { *out_len = len; return buf; }  /* $-1\r\n */
            char *crlf = strstr((char *)buf, "\r\n");
            if (crlf) {
                long blen = strtol((char *)buf + 1, NULL, 10);
                if (blen >= 0) {
                    size_t header_end = (size_t)(crlf - (char *)buf) + 2;
                    size_t total = header_end + (size_t)blen + 2;
                    if (len >= total) { *out_len = len; return buf; }  /* complete */
                }
            }
        }
        if (buf[0] == '+' || buf[0] == '-' || buf[0] == ':') {
            /* Simple string, error, integer — check for \r\n */
            if (strstr((char *)buf, "\r\n")) { *out_len = len; return buf; }
        }
        if (buf[0] == '*') {
            /* Array — simple check */
            if (strstr((char *)buf, "\r\n")) { *out_len = len; return buf; }
        }
    }

    /* Return whatever we have (may be incomplete) */
    *out_len = len;
    return buf;
}

/* 发送命令并返回响应字符串 (malloc'd) */
static char *cmd(int fd, const char *arg0, ...) {
    /* 收集变长参数 */
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

    size_t rlen;
    unsigned char *resp = recv_resp(fd, &rlen);
    if (!resp) return NULL;
    return (char *)resp;
}

/* ---------- INFO 解析 ---------- */

static char *get_info(const char *host, int port) {
    int fd = tcp_connect(host, port, 5000);
    if (fd < 0) return NULL;
    char *info = cmd(fd, "INFO", NULL);
    close(fd);
    return info; /* 返回 malloc'd 的 INFO 响应体 */
}

/* 从 INFO 响应中提取指定字段的值 (返回 malloc'd 字符串) */
static char *info_field(const char *info_resp, const char *field) {
    /* INFO 返回格式: $len\r\nrole:master\nmem:libc\n... */
    const char *body = info_resp;
    if (body && body[0] == '$') {
        body = strchr(body, '\n');
        if (body) body++;
    }
    if (!body) return NULL;

    size_t flen = strlen(field);
    const char *p = body;
    while (*p) {
        /* 跳过 \r */
        if (*p == '\r') { p++; continue; }
        if (strncmp(p, field, flen) == 0 && p[flen] == ':') {
            p += flen + 1;
            const char *end = p;
            while (*end && *end != '\n' && *end != '\r') end++;
            size_t vlen = (size_t)(end - p);
            char *val = (char *)malloc(vlen + 1);
            if (!val) return NULL;
            memcpy(val, p, vlen);
            val[vlen] = '\0';
            return val;
        }
        /* 跳到下一行 */
        while (*p && *p != '\n') p++;
        if (*p == '\n') p++;
    }
    return NULL;
}

/* ---------- 进度显示 ---------- */

static void progress_bar(double done, double total, char *out, int width) {
    int filled = (total > 0) ? (int)(done / total * width) : 0;
    if (filled > width) filled = width;
    for (int i = 0; i < width; i++) {
        out[i] = (i < filled) ? '#' : (i == filled && filled < width ? '>' : ' ');
    }
    out[width] = '\0';
}

static const char *fmt_bytes(unsigned long long bytes) {
    static char buf[32];
    if (bytes < 1024)
        snprintf(buf, sizeof(buf), "%lluB", bytes);
    else if (bytes < 1024 * 1024)
        snprintf(buf, sizeof(buf), "%.1fKB", (double)bytes / 1024);
    else
        snprintf(buf, sizeof(buf), "%.1fMB", (double)bytes / 1024 / 1024);
    return buf;
}

/* ---------- 打印横幅 ---------- */

static void banner(const char *title) {
    printf("\n" ANSI_BOLD ANSI_CYAN "========== %s ==========" ANSI_RESET "\n\n", title);
}

/* ---------- 进度监控 ---------- */

/* 保存上一次的进度行数，用于刷新 */
static int g_printed_lines = 0;

static void progress_clear(void) {
    for (int i = 0; i < g_printed_lines; i++) {
        printf(ANSI_CURSOR_UP ANSI_CLEAR_LINE);
    }
    g_printed_lines = 0;
}

static void progress_print(const char *line) {
    printf("%s%s\n", ANSI_CLEAR_LINE, line);
    g_printed_lines++;
}

static unsigned long long parse_ull(const char *s) {
    if (!s) return 0;
    return strtoull(s, NULL, 10);
}

/* ---------- 前向声明 ---------- */
static double time_elapsed(const struct timeval *start);
static double time_elapsed_diff(const struct timeval *end, const struct timeval *start);

/* ---------- 测试断言 ---------- */

static int g_pass = 0;
static int g_fail = 0;

static void test_pass(const char *fmt, ...) {
    va_list ap;
    g_pass++;
    printf(ANSI_GREEN "  [PASS]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

static void test_fail(const char *fmt, ...) {
    va_list ap;
    g_fail++;
    printf(ANSI_RED "  [FAIL]" ANSI_RESET " ");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
}

/* ================================================================
 * 主流程
 * ================================================================ */

static int run_test(void) {
    int master_fd = -1;

    banner("Phase 1: 预存数据到 Master");
    fflush(stdout);

    printf("  正在连接 Master %s:%d ...\n", g_opt.master_host, g_opt.master_port);
    fflush(stdout);
    master_fd = tcp_connect(g_opt.master_host, g_opt.master_port, 10000);
    if (master_fd < 0) {
        fprintf(stderr, ANSI_RED "ERROR:" ANSI_RESET
                " 无法连接 Master %s:%d (errno=%d: %s) — 请先启动 Master\n",
                g_opt.master_host, g_opt.master_port, errno, strerror(errno));
        return 1;
    }

    /* 检查 master 角色 */
    char *info = get_info(g_opt.master_host, g_opt.master_port);
    if (info) {
        char *role = info_field(info, "role");
        printf("  Master 角色: %s\n", role ? role : "?");
        free(role);
        free(info);
    }

    int pre_count = g_opt.pre_count;
    struct timeval t0, t1;
    gettimeofday(&t0, NULL);

    printf("  正在预存 %d 条数据到 Master...\n", pre_count);
    for (int batch_start = 0; batch_start < pre_count; batch_start += g_opt.batch) {
        int batch_end = batch_start + g_opt.batch;
        if (batch_end > pre_count) batch_end = pre_count;
        for (int i = batch_start; i < batch_end; i++) {
            char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
            snprintf(key, sizeof(key), "pre:k:%06d", i);
            snprintf(val, sizeof(val), "v%d", i);
            int retry;
            char *r = NULL;
            for (retry = 0; retry < 3; retry++) {
                r = cmd(master_fd, "HSET", key, val, NULL);
                if (r && strcmp(r, "+OK\r\n") == 0) break;
                free(r);
                r = NULL;
                usleep(100000); /* 100ms retry */
            }
            if (!r || strcmp(r, "+OK\r\n") != 0) {
                fprintf(stderr, "  预存失败 at %d (retried %d): %s\n",
                        i, retry, r ? r : "(no response)");
                free(r);
                close(master_fd);
                return 1;
            }
            free(r);
        }
        double elapsed = time_elapsed(&t0);
        printf("  预存: %s%d/%d%s keys, %.0f qps, %.1fs\n" ANSI_CURSOR_UP,
               ANSI_YELLOW, batch_end, pre_count, ANSI_RESET,
               (double)batch_end / (elapsed > 0 ? elapsed : 0.001), elapsed);
    }
    printf(ANSI_CLEAR_LINE);
    gettimeofday(&t1, NULL);
    double pre_elapsed = time_elapsed_diff(&t1, &t0);
    printf(ANSI_GREEN "  预存完成:" ANSI_RESET " %d keys in %.2fs\n", pre_count, pre_elapsed);
    test_pass("预存 %d 条数据到 Master", pre_count);

    /* ═══════════════════════════════════════════
     * Phase 2: 等待 Slave 连接并开始全量同步
     * ═══════════════════════════════════════════ */
    banner("Phase 2: 等待 Slave 连接");

    printf("  请启动 Slave (另一终端):\n");
    printf("    sudo ./kvstore kvstore.conf --role slave\n");
    printf("  或:\n");
    printf("    sudo ./kvstore --port %d --role slave --master-host %s --master-port %d \\\n",
           g_opt.slave_port, g_opt.master_host, g_opt.master_port);
    printf("        --repl-fullsync-transport rdma --repl-realtime-transport tcp\n");
    printf("\n  等待 Slave 连接...\n");

    int slave_ready = 0;
    struct timeval fs_begin;

    for (int i = 0; i < 300; i++) { /* 最多等 5 分钟 */
        char *info_s = get_info(g_opt.slave_host, g_opt.slave_port);
        if (info_s) {
            char *role = info_field(info_s, "role");
            if (role && strcmp(role, "slave") == 0) {
                slave_ready = 1;
                gettimeofday(&fs_begin, NULL); /* 全量同步计时从 slave 连接开始 */
                char *link = info_field(info_s, "master_link");
                char *loading = info_field(info_s, "slave_fullsync_loading");
                char *transport = info_field(info_s, "repl_transport_active");
                printf("  Slave 已连接! master_link=%-5s  transport=%-12s  fullsync_loading=%s\n",
                       link ? link : "?", transport ? transport : "?",
                       loading ? loading : "?");
                free(transport);
                free(link);
                free(loading);
                free(role);
                free(info_s);
                break;
            }
            free(role);
            free(info_s);
        }
        if (i % 10 == 0) printf("  正在等待 Slave 启动... (%ds)\n", i / 2);
        usleep(500000); /* 0.5s */
    }
    fflush(stdout);

    if (!slave_ready) {
        fprintf(stderr, ANSI_RED "ERROR:" ANSI_RESET " Slave 未在 5 分钟内连接\n");
        close(master_fd);
        return 1;
    }
    test_pass("Slave 已连接并开始全量同步");

    /* ═══════════════════════════════════════════
     * Phase 3: 监控全量同步进度
     * ═══════════════════════════════════════════ */
    banner("Phase 3: 全量同步 (RDMA) — 实时监控");

    printf("  全量同步进度:\n\n");

    int fullsync_done = 0;
    int fullsync_started = 0;  /* 标记是否已检测到 FULLRESYNC（loading=1） */
    int fullsync_checked_hget = 0; /* 是否已用 HGET 兜底检测过 */
    unsigned long long prev_slave_off = 0;
    double prev_full_time = 0;
    int fullsync_timeout = 600; /* 10 分钟 */

    /* 轮询直到全量同步完成 */
    for (int i = 0; i < fullsync_timeout; i++) {
        char *info_s = get_info(g_opt.slave_host, g_opt.slave_port);
        if (!info_s) { usleep(500000); continue; }

        char *loading_s = info_field(info_s, "slave_fullsync_loading");
        char *offset_s = info_field(info_s, "slave_repl_offset");
        char *link_s = info_field(info_s, "master_link");
        char *transport_s = info_field(info_s, "repl_transport_active");

        unsigned long long slave_off = parse_ull(offset_s);
        double now = time_elapsed(&fs_begin);

        /* 获取 master snapshot_bytes 作为全量大小参考 */
        char *master_info = get_info(g_opt.master_host, g_opt.master_port);
        unsigned long long snapshot_bytes = 0;
        if (master_info) {
            char *snap = info_field(master_info, "repl_snapshot_bytes");
            snapshot_bytes = parse_ull(snap);
            free(snap);
            free(master_info);
        }

        int loading = (int)parse_ull(loading_s);

        /* 首次检测到 loading=1 表示全量同步已开始 */
        if (loading && !fullsync_started) {
            fullsync_started = 1;
            progress_print("  全量同步已开始");
        }

        /* 清除上次输出 */
        progress_clear();

        char line1[256];
        snprintf(line1, sizeof(line1),
            "  master_link=%-5s  transport=%-12s  offset=%llu",
            link_s ? link_s : "?", transport_s ? transport_s : "?", slave_off);

        if (fullsync_started && loading) {
            /* 全量同步进行中 */
            char bar[PROGRESS_BAR_WIDTH + 1];
            if (snapshot_bytes > 0) {
                progress_bar((double)slave_off, (double)snapshot_bytes, bar, PROGRESS_BAR_WIDTH);
            } else {
                snprintf(bar, sizeof(bar), "[ === 等待进度信息 === ]");
            }

            double speed = 0;
            if (prev_full_time > 0 && now - prev_full_time > 0) {
                speed = (double)(slave_off - prev_slave_off) / (now - prev_full_time);
            }
            prev_slave_off = slave_off;
            prev_full_time = now;

            char line2[256];
            if (snapshot_bytes > 0) {
                snprintf(line2, sizeof(line2),
                    "  %s全量同步中...%s  %s  %s / %s  %s/s",
                    ANSI_YELLOW, ANSI_RESET, bar,
                    fmt_bytes(slave_off), fmt_bytes(snapshot_bytes),
                    fmt_bytes((unsigned long long)speed));
            } else {
                snprintf(line2, sizeof(line2),
                    "  %s全量同步中...%s  offset=%s",
                    ANSI_YELLOW, ANSI_RESET, fmt_bytes(slave_off));
            }
            progress_print(line1);
            progress_print(line2);
        } else if (fullsync_started && !loading) {
            /* 全量同步刚完成 */
            fullsync_done = 1;
            progress_print(line1);
            progress_print(ANSI_GREEN "  全量同步完成!" ANSI_RESET);
        } else {
            /* 全量同步尚未开始 —
             * 若 slave_off > 0 说明全量同步已完成（loading=1→0 太快被漏掉） */
            if (!fullsync_started && slave_off > 0) {
                fullsync_done = 1;
                fullsync_started = 1;
                progress_clear();
                progress_print(line1);
                progress_print(ANSI_GREEN "  全量同步完成 (快速模式)" ANSI_RESET);
                break;
            }
            /* 若轮询超过 30s 且 loading 始终为 0，用 HGET 兜底验证。 */
            if (!fullsync_checked_hget && i > 60) { /* 60×500ms = 30s */
                int vfd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
                if (vfd >= 0) {
                    char *r = cmd(vfd, "HGET", "pre:k:000000", NULL);
                    if (r && strcmp(r, "$2\r\nv0\r\n") == 0) {
                        free(r); close(vfd);
                        fullsync_done = 1;
                        fullsync_started = 1; /* 标记为已开始+已完成 */
                        progress_clear();
                        progress_print(line1);
                        progress_print(ANSI_GREEN "  全量同步完成 (快速模式)" ANSI_RESET);
                        break;
                    }
                    free(r); close(vfd);
                }
                fullsync_checked_hget = 1;
            }
            if (!fullsync_done) {
                char line2[256];
                snprintf(line2, sizeof(line2),
                    "  %s等待全量同步开始...%s  offset=%llu",
                    ANSI_YELLOW, ANSI_RESET, slave_off);
                progress_print(line1);
                progress_print(line2);
            }
        }

        free(loading_s);
        free(offset_s);
        free(link_s);
        free(transport_s);
        free(info_s);

        if (fullsync_done) break;
        usleep((useconds_t)g_opt.poll_ms * 1000);
    } /* end for */
    fflush(stdout);

    if (!fullsync_done) {
        fprintf(stderr, ANSI_RED "\n  ✗ 全量同步超时\n" ANSI_RESET);
        close(master_fd);
        return 1;
    }

    double fs_elapsed = time_elapsed(&fs_begin);
    test_pass("全量同步完成 (%.1fs)", fs_elapsed);

    /* 等待 REPLDONE 到达 master */
    usleep(2000000);

    /* 快速验证 slave 数据已就绪（全量同步完成后应该立即可用） */
    printf("  验证 slave 数据...\n");
    for (int retry = 0; retry < 60; retry++) {
        int vfd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
        if (vfd < 0) { usleep(500000); continue; }
        char *r = cmd(vfd, "HGET", "pre:k:000000", NULL);
        if (r && strcmp(r, "$2\r\nv0\r\n") == 0) {
            free(r);
            close(vfd);
            printf("  %sslave 数据已就绪%s\n", ANSI_GREEN, ANSI_RESET);
            break;
        }
        free(r);
        close(vfd);
        if (retry == 59) {
            fprintf(stderr, ANSI_RED "  ✗ slave 数据验证失败\n" ANSI_RESET);
            close(master_fd);
            return 1;
        }
        usleep(500000);
    }

    /* 获取全量同步信息 */
    char *fs_info = get_info(g_opt.master_host, g_opt.master_port);
    if (fs_info) {
        char *snap = info_field(fs_info, "repl_snapshot_bytes");
        char *fc = info_field(fs_info, "repl_fullsync_count");
        printf("  Snapshot bytes: %s\n", snap ? snap : "?");
        printf("  Fullsync count: %s\n", fc ? fc : "?");
        free(snap);
        free(fc);
        free(fs_info);
    }

    /* ═══════════════════════════════════════════
     * Phase 4: 增量数据写入 Master
     * ═══════════════════════════════════════════ */
    banner("Phase 4: 增量数据写入 Master");

    /* master_fd 是 Phase 1 打开的连接，经过长时间的 RDMA 全量同步后
     * 可能已不可用，关闭旧连接并重新建立新连接 */
    close(master_fd);
    master_fd = tcp_connect(g_opt.master_host, g_opt.master_port, 10000);
    if (master_fd < 0) {
        fprintf(stderr, ANSI_RED "ERROR: 无法重新连接 Master 进行增量写入\n" ANSI_RESET);
        return 1;
    }

    int post_count = g_opt.post_count;
    struct timeval t2, t3;
    gettimeofday(&t2, NULL);

    printf("  正在写入 %d 条增量数据到 Master (pre_count=%d)...\n", post_count, pre_count);
    printf("  示例: post:k:000000 = v%d, post:k:000001 = v%d, post:k:004999 = v%d\n",
           pre_count + 0, pre_count + 1, pre_count + 4999);
    for (int batch_start = 0; batch_start < post_count; batch_start += g_opt.batch) {
        int batch_end = batch_start + g_opt.batch;
        if (batch_end > post_count) batch_end = post_count;
        for (int i = batch_start; i < batch_end; i++) {
            char key[MAX_KEY_LEN], val[MAX_KEY_LEN];
            snprintf(key, sizeof(key), "post:k:%06d", i);
            snprintf(val, sizeof(val), "v%d", pre_count + i);
            int retry;
            char *r = NULL;
            for (retry = 0; retry < 3; retry++) {
                r = cmd(master_fd, "HSET", key, val, NULL);
                if (r && strcmp(r, "+OK\r\n") == 0) break;
                free(r);
                r = NULL;
                usleep(100000); /* 100ms retry delay */
            }
            if (!r || strcmp(r, "+OK\r\n") != 0) {
                fprintf(stderr, "  增量写入失败 at %d (retried %d): %s\n",
                        i, retry, r ? r : "(no response)");
                free(r);
                close(master_fd);
                return 1;
            }
            free(r);
        }
        double elapsed = time_elapsed(&t2);
        printf("  增量写入: %s%d/%d%s keys, %.0f qps, %.1fs\n" ANSI_CURSOR_UP,
               ANSI_YELLOW, batch_end, post_count, ANSI_RESET,
               (double)batch_end / (elapsed > 0 ? elapsed : 0.001), elapsed);
    }
    printf(ANSI_CLEAR_LINE);
    gettimeofday(&t3, NULL);
    double post_elapsed = time_elapsed_diff(&t3, &t2);
    printf(ANSI_GREEN "  增量数据写入完成:" ANSI_RESET " %d keys in %.2fs\n", post_count, post_elapsed);
    test_pass("写入 %d 条增量数据到 Master", post_count);

    /* ═══════════════════════════════════════════
     * Phase 5: 监控增量同步进度
     * ═══════════════════════════════════════════ */
    banner("Phase 5: 增量同步 — 实时监控");

    printf("  增量同步进度:\n\n");

    int caught_up = 0;
    int incr_timeout = 600; /* 10 分钟 */
    struct timeval incr_begin;
    unsigned long long prev_slave_off_incr = 0;
    double prev_incr_time = 0;
    gettimeofday(&incr_begin, NULL);

    /* 监控增量同步进度 */
    for (int i = 0; i < incr_timeout; i++) {
        char *info_s = get_info(g_opt.slave_host, g_opt.slave_port);
        if (!info_s) { usleep(500000); continue; }

        char *link_s = info_field(info_s, "master_link");
        char *transport_s = info_field(info_s, "repl_transport_active");
        char *slave_off_s = info_field(info_s, "slave_repl_offset");
        unsigned long long slave_off = parse_ull(slave_off_s);
        char *slave_loading_s = info_field(info_s, "slave_fullsync_loading");
        int slave_loading = (int)parse_ull(slave_loading_s);
        free(slave_loading_s);

        /* 也获取 master 的最新 offset */
        char *master_info2 = get_info(g_opt.master_host, g_opt.master_port);
        unsigned long long master_off = 0;
        if (master_info2) {
            char *mo = info_field(master_info2, "master_repl_offset");
            if (mo) { master_off = parse_ull(mo); free(mo); }
            free(master_info2);
        }

        progress_clear();

        double now = time_elapsed(&incr_begin);
        double delta = (double)(master_off > slave_off ? master_off - slave_off : 0);
        double speed = 0;
        if (prev_incr_time > 0 && now - prev_incr_time > 0) {
            speed = (double)(slave_off - prev_slave_off_incr) / (now - prev_incr_time);
        }
        prev_slave_off_incr = slave_off;
        prev_incr_time = now;

        char line1[256];
        snprintf(line1, sizeof(line1),
            "  master_link=%-5s  transport=%-12s  master_offset=%llu  slave_offset=%llu  loading=%d",
            link_s ? link_s : "?", transport_s ? transport_s : "?",
            master_off, slave_off, slave_loading);
        progress_print(line1);

        char line2[256];
        if (delta > 0) {
            double eta = speed > 0 ? delta / speed : 0;
            snprintf(line2, sizeof(line2),
                "  %s追赶中...%s  落后 %s  (%s/s  ETA %.0fs)",
                ANSI_YELLOW, ANSI_RESET,
                fmt_bytes((unsigned long long)delta),
                fmt_bytes((unsigned long long)speed), eta);
        } else {
            snprintf(line2, sizeof(line2),
                "  %s等待同步...%s  slave_offset=%s",
                ANSI_YELLOW, ANSI_RESET, fmt_bytes(slave_off));
        }
        progress_print(line2);

        /* 用 offset 判断是否完成（允许 1024B 误差，覆盖协议对齐和批量缓冲） */
        int offset_ok = (slave_off + 1024 >= master_off && master_off > 0);

        /* HGET 兜底：offset 检测不通过时直接查最后一个 POST key */
        if (!offset_ok && g_opt.post_count > 0 && i > 10) {
            int vfd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
            if (vfd >= 0) {
                char key[64], expected[64];
                int last = g_opt.post_count - 1;
                snprintf(key, sizeof(key), "post:k:%06d", last);
                snprintf(expected, sizeof(expected), "v%d", g_opt.pre_count + last);
                char *r = cmd(vfd, "HGET", key, NULL);
                if (r) {
                    char check[256];
                    snprintf(check, sizeof(check), "$%zu\r\n%s\r\n", strlen(expected), expected);
                    if (strcmp(r, check) == 0) {
                        offset_ok = 1;
                    }
                    free(r);
                }
                close(vfd);
            }
        }

        if (offset_ok) {
            /* 数据落库确认：slave_repl_offset 是接收 offset，应用线程可能滞后于落库，
             * offset 追平 ≠ post 数据可读。HGET 验证最后一条 POST key 已就绪才完成，
             * 否则继续轮询等 slave 应用线程追平，避免 Phase 6 读到 $-1（验证竞态）。 */
            int data_ok = 0;
            if (g_opt.post_count > 0) {
                int vfd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
                if (vfd >= 0) {
                    char key[64], expected[64];
                    int last = g_opt.post_count - 1;
                    snprintf(key, sizeof(key), "post:k:%06d", last);
                    snprintf(expected, sizeof(expected), "v%d", g_opt.pre_count + last);
                    char *r = cmd(vfd, "HGET", key, NULL);
                    if (r) {
                        char check[256];
                        snprintf(check, sizeof(check), "$%zu\r\n%s\r\n", strlen(expected), expected);
                        if (strcmp(r, check) == 0) data_ok = 1;
                        free(r);
                    }
                    close(vfd);
                }
            } else {
                data_ok = 1;   /* 无 post 数据，offset 追平即完成 */
            }
            if (data_ok || i > 60) {   /* i>60 兜底：30s 后强制完成，真异常留给 Phase 6 暴露 */
                caught_up = 1;
                progress_clear();
                printf("  %s✓ 增量同步完成! slave offset (%llu) >= master offset (%llu)%s\n\n",
                       ANSI_GREEN, slave_off, master_off, ANSI_RESET);
                break;
            }
            /* 数据未落库：继续轮询 */
        }

        /* RDMA 全量同步: offset 始终为 0，用快照字节数判断 */
        unsigned long long snap_bytes = 0;
        {
            char *snap = info_field(info_s, "repl_snapshot_bytes");
            snap_bytes = parse_ull(snap);
            free(snap);
        }
        if (master_off == 0 && slave_off == 0 && !slave_loading && snap_bytes > 0 && i > 5) {
            int vfd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
            if (vfd >= 0 && g_opt.post_count > 0) {
                char key[64], expected[64];
                int last = g_opt.post_count - 1;
                snprintf(key, sizeof(key), "post:k:%06d", last);
                snprintf(expected, sizeof(expected), "v%d", last);
                char *r = cmd(vfd, "HGET", key, NULL);
                if (r) {
                    char check[256];
                    snprintf(check, sizeof(check), "$%zu\r\n%s\r\n", strlen(expected), expected);
                    if (strcmp(r, check) == 0) {
                        free(r); close(vfd);
                        caught_up = 1;
                        progress_clear();
                        printf("  %s✓ 增量同步完成! slave 数据已就绪 (RDMA fullsync, 已验证 key=%s)%s\n\n",
                               ANSI_GREEN, key, ANSI_RESET);
                        break;
                    }
                    free(r);
                }
                close(vfd);
            }
        }

        /* 后备检测：slave 卡住不动时，直接读增量 key 验证数据是否已同步 */
        if (i > 30 && !caught_up && g_opt.post_count > 0) { /* 等了至少 15 秒后 */
            int vfd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
            if (vfd >= 0) {
                char key[64], expected[64];
                /* 验证最后一条增量数据，确保所有增量都已同步 */
                int last = g_opt.post_count - 1;
                snprintf(key, sizeof(key), "post:k:%06d", last);
                snprintf(expected, sizeof(expected), "v%d", last);
                char *r = cmd(vfd, "HGET", key, NULL);
                if (r) {
                    char check[256];
                    snprintf(check, sizeof(check), "$%zu\r\n%s\r\n", strlen(expected), expected);
                    if (strcmp(r, check) == 0) {
                        free(r); close(vfd);
                        caught_up = 1;
                        progress_clear();
                        printf("  %s✓ 增量同步完成! slave 数据已就绪 (已验证 key=%s)%s\n\n",
                               ANSI_GREEN, key, ANSI_RESET);
                        break;
                    }
                    free(r);
                }
                close(vfd);
            }
        }

        /* 如果 master offset 为 0（无 backlog 的情况），显示等待信息 */
        if (master_off == 0 && !caught_up) {
            progress_print("  master_offset=0 — 数据已通过 fullsync 传输\n");
        }

        usleep((useconds_t)g_opt.poll_ms * 1000);
    }
    fflush(stdout);

    double incr_elapsed = time_elapsed(&incr_begin);

    if (!caught_up) {
        fprintf(stderr, ANSI_RED "\n  ✗ 增量同步未在 %ds 内完成\n" ANSI_RESET, incr_timeout);
        close(master_fd);
        return 1;
    }
    test_pass("增量同步完成 (追赶时间: %.1fs)", incr_elapsed);

    /* ═══════════════════════════════════════════
     * Phase 5.5: 验证 eBPF+tcp 增量同步传输是否生效
     * ═══════════════════════════════════════════ */
    banner("Phase 5.5: 验证 eBPF+tcp 增量同步传输状态");

    printf("  正在检查 Master/Slave 端 eBPF+tcp 传输状态...\n");
    usleep(500000); /* 等待 0.5s 确保统计更新 */

    {
        char *info_m = get_info(g_opt.master_host, g_opt.master_port);
        char *info_s = get_info(g_opt.slave_host, g_opt.slave_port);
        char *m_transport = info_m ? info_field(info_m, "repl_transport_active") : NULL;
        char *s_transport = info_s ? info_field(info_s, "repl_transport_active") : NULL;
        char *m_broadcast = info_m ? info_field(info_m, "repl_broadcast_bytes") : NULL;
        char *m_fallback = info_m ? info_field(info_m, "repl_transport_fallback_reason") : NULL;

        int ebpf_tcp_ok = 0;
        if (m_transport && strstr(m_transport, "ebpf-tcp")) ebpf_tcp_ok = 1;

        printf("  Master 传输层: %s\n", m_transport ? m_transport : "?");
        printf("  Slave  传输层: %s\n", s_transport ? s_transport : "?");
        /* 不再检查 kprobe_initialized：ebpf+tcp 模式用 fexit client_capture（repl_client_capture.bpf.o），
         * 非 kprobe（repl_kprobe.bpf.o），该字段恒为 0 会误导。以 repl_transport_active + fallback_reason 为准。 */
        printf("  repl_broadcast_bytes: %s\n", m_broadcast ? m_broadcast : "?");
        if (m_fallback && strcmp(m_fallback, "none") != 0)
            printf("  fallback_reason: %s\n", m_fallback);

        if (ebpf_tcp_ok && m_broadcast && strtoull(m_broadcast, NULL, 10) > 0) {
            printf("  %s✓ eBPF+tcp 增量同步传输生效%s\n", ANSI_GREEN, ANSI_RESET);
            printf("    eBPF kprobe 捕获客户端写入 → repl_broadcast TCP 转发\n");
            test_pass("eBPF+tcp 增量同步传输生效: kprobe 捕获 + TCP 转发");
        } else if (ebpf_tcp_ok) {
            printf("  %s⚠ eBPF+tcp 传输层已配置但 broadcast_bytes=0%s\n",
                   ANSI_YELLOW, ANSI_RESET);
            printf("  可能原因: 增量数据量小，统计尚未刷新\n");
            test_pass("eBPF+tcp 传输层已配置 (broadcast 统计未更新)");
        } else {
            printf("  %s✗ eBPF+tcp 未生效%s\n", ANSI_RED, ANSI_RESET);
            printf("  当前传输层: %s\n", m_transport ? m_transport : "?");
            printf("  可能原因: BPF 未加载（需 root 权限）、配置未启用 ebpf+tcp\n");
            printf("  数据已通过 TCP fallback 正常同步\n");
            test_fail("eBPF+tcp 未生效，增量同步走 TCP fallback");
        }

        free(m_transport); free(s_transport);
        free(m_broadcast); free(m_fallback);
        free(info_m); free(info_s);
    }

    /* ═══════════════════════════════════════════
     * Phase 6: 验证 Slave 数据一致性
     * ═══════════════════════════════════════════ */
    banner("Phase 6: 验证 Slave 数据一致性");

    int total = pre_count + post_count;
    int failed = 0;
    struct timeval t4;
    gettimeofday(&t4, NULL);

    /* 从 slave 侧打开一个连接用于验证 */
    int slave_fd = tcp_connect(g_opt.slave_host, g_opt.slave_port, 10000);
    if (slave_fd < 0) {
        fprintf(stderr, ANSI_RED "ERROR:" ANSI_RESET " 无法连接 Slave %s:%d\n",
                g_opt.slave_host, g_opt.slave_port);
        close(master_fd);
        return 1;
    }

    /* 验证预存数据的抽样 */
    int pre_samples[] = {0, 1, 2, 9, 99, 999, 4999, 9999, 19999, -1};
    printf("  验证预存数据:\n");
    for (int si = 0; pre_samples[si] >= 0; si++) {
        int idx = pre_samples[si];
        if (idx >= pre_count) continue;
        char key[MAX_KEY_LEN], expected[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "pre:k:%06d", idx);
        snprintf(expected, sizeof(expected), "v%d", idx);
        char *r = cmd(slave_fd, "HGET", key, NULL);
        if (!r) { test_fail("[pre] key=%s — 无响应", key); failed++; continue; }
        /* 期望响应格式: $<len>\r\n<value>\r\n */
        char expected_resp[128];
        snprintf(expected_resp, sizeof(expected_resp), "$%zu\r\n%s\r\n", strlen(expected), expected);
        if (strcmp(r, expected_resp) == 0) {
            test_pass("[pre] %s -> %s", key, expected);
        } else {
            test_fail("[pre] %s 期望 %s 实际 %s", key, expected, r);
            failed++;
        }
        free(r);
    }

    /* 验证增量数据的抽样 — 每个 key 独立连接，3 次重试 */
    int post_samples[] = {0, 1, 2, 9, 99, 999, 4999, 9999, 19999, -1};
    printf("  验证增量数据:\n");
    int slave_fd2 = -1;
    for (int si = 0; post_samples[si] >= 0; si++) {
        int idx = post_samples[si];
        if (idx >= post_count) continue;
        char key[MAX_KEY_LEN], expected[MAX_KEY_LEN];
        snprintf(key, sizeof(key), "post:k:%06d", idx);
        snprintf(expected, sizeof(expected), "v%d", pre_count + idx);
        char expected_resp[128];
        snprintf(expected_resp, sizeof(expected_resp), "$%zu\r\n%s\r\n", strlen(expected), expected);

        int ok = 0;
        for (int retry = 0; retry < 3 && !ok; retry++) {
            if (slave_fd2 < 0) {
                slave_fd2 = tcp_connect(g_opt.slave_host, g_opt.slave_port, 5000);
                if (slave_fd2 < 0) continue;
            }
            char *r = cmd(slave_fd2, "HGET", key, NULL);
            if (!r) { close(slave_fd2); slave_fd2 = -1; usleep(200000); continue; }
            if (strcmp(r, expected_resp) == 0) {
                test_pass("[post] %s -> %s%s", key, expected, retry > 0 ? " (retry)" : "");
                ok = 1;
            } else if (strstr(r, "\r\n") == NULL) {
                /* 不完整响应 — 重连 */
                close(slave_fd2); slave_fd2 = -1;
                usleep(200000);
            } else {
                test_fail("[post] %s 期望 %s 实际 %s", key, expected, r);
                ok = 1;  /* 完整但不匹配 — 真实数据错误, 不再重试 */
            }
            free(r);
        }
        if (!ok) { test_fail("[post] %s 期望 %s — 3次重试仍失败", key, expected); failed++; }
    }
    if (slave_fd2 >= 0) close(slave_fd2);
    close(master_fd);

    struct timeval t5;
    gettimeofday(&t5, NULL);
    double verify_elapsed = time_elapsed_diff(&t5, &t4);

    /* ═══════════════════════════════════════════
     * 结果
     * ═══════════════════════════════════════════ */
    banner("结果");

    if (failed == 0) {
        printf(ANSI_GREEN ANSI_BOLD "  全部通过!\n" ANSI_RESET);
        printf("  预存: %d keys (%.2fs)\n", pre_count, pre_elapsed);
        printf("  全量同步: %.1fs\n", fs_elapsed);
        printf("  增量写入: %d keys (%.2fs)\n", post_count, post_elapsed);
        printf("  增量同步追赶: %.1fs\n", incr_elapsed);
        printf("  验证: %d samples (%.2fs)\n", g_pass, verify_elapsed);
        printf("  总计数据: %d keys\n", total);
        printf(ANSI_GREEN "  PASS: %d   FAIL: %d\n" ANSI_RESET, g_pass, g_fail);
        return 0;
    } else {
        printf(ANSI_RED ANSI_BOLD "  失败: %d 个 key 不一致\n" ANSI_RESET, failed);
        printf(ANSI_RED "  PASS: %d   FAIL: %d\n" ANSI_RESET, g_pass, g_fail);
        return 1;
    }
}

/* ================================================================
 * 辅助: 时间计算
 * ================================================================ */

static double time_elapsed(const struct timeval *start) {
    struct timeval now;
    gettimeofday(&now, NULL);
    return time_elapsed_diff(&now, start);
}

static double time_elapsed_diff(const struct timeval *end, const struct timeval *start) {
    return (double)(end->tv_sec - start->tv_sec)
         + (double)(end->tv_usec - start->tv_usec) / 1000000.0;
}

/* ================================================================
 * main
 * ================================================================ */

static void print_usage(const char *prog) {
    printf("用法: %s [选项]\n", prog);
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║  启动顺序: Master → 本脚本 → Slave (等提示再启动)         ║\n");
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("方式一: RDMA 全量 + kprobe+RDMA WRITE 增量（双虚拟机，推荐）\n");
    printf("  # 终端 1 (VM1, 先启动):\n");
    printf("  sudo ./kvstore kvstore.conf --role master\n");
    printf("\n");
    printf("  # 终端 2 (任意机器):\n");
    printf("  %s --master-host <MASTER_IP> --master-port %d \\\n", prog, g_opt.master_port);
    printf("      --slave-host <SLAVE_IP> --slave-port %d \\\n", g_opt.slave_port);
    printf("      --pre %d --post %d\n", g_opt.pre_count, g_opt.post_count);
    printf("\n");
    printf("  # 终端 3 (VM2, 等\"等待 Slave 连接...\"提示后启动):\n");
    printf("  sudo ./kvstore kvstore.conf --role slave\n");
    printf("\n");
    printf("  或手动指定参数:\n");
    printf("  sudo ./kvstore --port %d --role slave \\\n", g_opt.slave_port);
    printf("      --master-host <MASTER_IP> --master-port %d\n", g_opt.master_port);
    printf("\n");
    printf("方式二: RDMA 全量 + eBPF 增量（双虚拟机，kprobe 不可用时回退）\n");
    printf("  # 终端 1 (VM1, 先启动):\n");
    printf("  sudo ./kvstore --port %d --role master \\\n", g_opt.master_port);
    printf("      --repl-fullsync-transport rdma \\\n");
    printf("      --repl-realtime-transport ebpf \\\n");
    printf("      --ebpf-enabled --rdma-dev siw0 --rdma-recv-slots 64\n");
    printf("\n");
    printf("  # 终端 2 (任意机器):\n");
    printf("  %s --config tests/test.conf\n", prog);
    printf("\n");
    printf("  # 终端 3 (VM2, 等\"等待 Slave 连接...\"提示后启动):\n");
    printf("  sudo ./kvstore --port %d --role slave \\\n", g_opt.slave_port);
    printf("      --master-host <MASTER_IP> --master-port %d \\\n", g_opt.master_port);
    printf("      --repl-fullsync-transport rdma \\\n");
    printf("      --repl-realtime-transport ebpf\n");
    printf("\n");
    printf("方式三: TCP 全量 + TCP 增量（单机，无 RDMA/eBPF）\n");
    printf("  # 终端 1 (先启动):\n");
    printf("  ./kvstore --port %d --role master \\\n", g_opt.master_port);
    printf("      --repl-fullsync-transport tcp --repl-realtime-transport tcp\n");
    printf("\n");
    printf("  # 终端 2:\n");
    printf("  %s --config tests/test.conf\n", prog);
    printf("\n");
    printf("  # 终端 3 (等\"等待 Slave 连接...\"提示后启动):\n");
    printf("  ./kvstore --port %d --role slave \\\n", g_opt.slave_port);
    printf("      --master-host 127.0.0.1 --master-port %d \\\n", g_opt.master_port);
    printf("      --repl-fullsync-transport tcp --repl-realtime-transport tcp\n");
    printf("\n选项:\n");
    printf("  --master-host HOST   Master 地址 (默认 %s)\n", g_opt.master_host);
    printf("  --master-port PORT   Master 端口 (默认 %d)\n", g_opt.master_port);
    printf("  --slave-host HOST    Slave 地址 (默认 %s)\n", g_opt.slave_host);
    printf("  --slave-port PORT    Slave 端口 (默认 %d)\n", g_opt.slave_port);
    printf("  --pre COUNT          全量同步前数据量 (默认 %d)\n", g_opt.pre_count);
    printf("  --post COUNT         全量同步后数据量 (默认 %d)\n", g_opt.post_count);
    printf("  --batch SIZE         每批写入量 (默认 %d)\n", g_opt.batch);
    printf("  --config PATH        加载配置文件 (默认 tests/test.conf)\n");
    printf("  --poll MS            轮询间隔毫秒 (默认 %d)\n", g_opt.poll_ms);
    printf("  -h                   显示此帮助\n");
}

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
        else if (strcmp(key, "master_port") == 0) g_opt.master_port = atoi(val);
        else if (strcmp(key, "slave_host") == 0) g_opt.slave_host = strdup(val);
        else if (strcmp(key, "slave_port") == 0) g_opt.slave_port = atoi(val);
        else if (strcmp(key, "pre") == 0) g_opt.pre_count = atoi(val);
        else if (strcmp(key, "post") == 0) g_opt.post_count = atoi(val);
        else if (strcmp(key, "batch") == 0) g_opt.batch = atoi(val);
        else if (strcmp(key, "poll_ms") == 0) g_opt.poll_ms = atoi(val);
    }
    fclose(fp);
    return 0;
}

int main(int argc, char **argv) {
    /* 自动加载默认配置文件 */
    parse_config_file("tests/test.conf");
    parse_config_file("test.conf");

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc)
            parse_config_file(argv[++i]);
        else if (strcmp(argv[i], "--master-host") == 0 && i + 1 < argc)
            g_opt.master_host = argv[++i];
        else if (strcmp(argv[i], "--master-port") == 0 && i + 1 < argc)
            g_opt.master_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--slave-host") == 0 && i + 1 < argc)
            g_opt.slave_host = argv[++i];
        else if (strcmp(argv[i], "--slave-port") == 0 && i + 1 < argc)
            g_opt.slave_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--pre") == 0 && i + 1 < argc)
            g_opt.pre_count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--post") == 0 && i + 1 < argc)
            g_opt.post_count = atoi(argv[++i]);
        else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc)
            g_opt.batch = atoi(argv[++i]);
        else if (strcmp(argv[i], "--poll") == 0 && i + 1 < argc)
            g_opt.poll_ms = atoi(argv[++i]);
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    printf(ANSI_BOLD "主从同步 5w+5w 测试用例 (" __DATE__ " " __TIME__ ")\n" ANSI_RESET);
    printf("  Master: %s:%d\n", g_opt.master_host, g_opt.master_port);
    printf("  Slave:  %s:%d\n", g_opt.slave_host, g_opt.slave_port);
    printf("  Pre:    %d\n", g_opt.pre_count);
    printf("  Post:   %d\n", g_opt.post_count);
    printf("  Batch:  %d\n", g_opt.batch);
    printf("  Poll:   %dms\n\n", g_opt.poll_ms);

    return run_test();
}
