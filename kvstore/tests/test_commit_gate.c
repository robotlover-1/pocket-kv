// ============================================================
// test_commit_gate.c — ebpf-proxy Commit Gate 状态机单元测试（非 root）
//
// 通过 Unix SOCK_SEQPACKET 模拟 Master 控制面，直接注入 ringbuf 记录，
// 覆盖：RESP/inline 重组、SKIP/WRITE、半包、控制行、畸形包、CRC/seq 校验、
// recv_seq 缺口、全局序、BASELINE 重定基、RESUME 丢弃/放行、pending 超时。
// ============================================================
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>

#include "proxy_commit.h"
#include "kvstore/replication/commit_gate.h"

static int g_fail = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL %s (line %d)\n", m, __LINE__); g_fail++; } } while (0)

/* ---- emit 收集 ---- */
static unsigned char g_out[65536];
static size_t g_out_len = 0;
static int g_out_count = 0;

static int g_fail_emit = 0;

static int emit_cb(const unsigned char *buf, size_t len) {
    if (g_fail_emit) return -1;
    if (g_out_len + len <= sizeof(g_out)) {
        memcpy(g_out + g_out_len, buf, len);
        g_out_len += len;
    }
    g_out_count++;
    return 0;
}

static int g_fault_reason = 0;
static void fault_cb(int reason) { g_fault_reason = reason; }

static uint64_t crc64(const unsigned char *b, size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; ++i) { h ^= (uint64_t)b[i]; h *= 1099511628211ull; }
    return h;
}

static void reset_out(void) { g_out_len = 0; g_out_count = 0; g_fault_reason = 0; }

/* ---- 控制面客户端 ---- */
static int g_sock = -1;

static int connect_master(const char *path) {
    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    strncpy(sa.sun_path, path, sizeof(sa.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static void send_hello(uint64_t epoch) {
    repl_commit_msg_t m;
    memset(&m, 0, sizeof(m));
    m.magic = REPL_COMMIT_MAGIC; m.version = REPL_COMMIT_VERSION;
    m.type = REPL_COMMIT_MSG_HELLO; m.epoch = epoch;
    CHECK(send(g_sock, &m, sizeof(m), 0) == (ssize_t)sizeof(m), "send hello");
}

/* 后续控制消息使用的 commit epoch；0 = 旧式/不校验，便于既有用例。 */
static uint64_t g_msg_epoch = 0;

static void send_baseline(uint64_t cookie, uint64_t next_seq) {
    repl_commit_msg_t m;
    repl_commit_kv_t kv = { cookie, next_seq };
    memset(&m, 0, sizeof(m));
    m.magic = REPL_COMMIT_MAGIC; m.version = REPL_COMMIT_VERSION;
    m.type = REPL_COMMIT_MSG_BASELINE; m.count = 1; m.epoch = g_msg_epoch;
    unsigned char buf[sizeof(m) + sizeof(kv)];
    memcpy(buf, &m, sizeof(m));
    memcpy(buf + sizeof(m), &kv, sizeof(kv));
    CHECK(send(g_sock, buf, sizeof(buf), 0) == (ssize_t)sizeof(buf), "send baseline");
}

static void send_desc(uint64_t cookie, uint64_t seq, uint64_t global,
                      const unsigned char *raw, size_t len, uint32_t flags,
                      uint64_t crc_override, int force_bad_crc) {
    repl_commit_msg_t m;
    repl_commit_desc_t d;
    memset(&m, 0, sizeof(m));
    memset(&d, 0, sizeof(d));
    m.magic = REPL_COMMIT_MAGIC; m.version = REPL_COMMIT_VERSION;
    m.type = REPL_COMMIT_MSG_BATCH; m.count = 1; m.epoch = g_msg_epoch;
    d.cookie = cookie; d.command_seq = seq; d.global_seq = global;
    d.raw_len = (uint32_t)len; d.flags = flags;
    d.crc = force_bad_crc ? crc_override : crc64(raw, len);
    unsigned char buf[sizeof(m) + sizeof(d)];
    memcpy(buf, &m, sizeof(m));
    memcpy(buf + sizeof(m), &d, sizeof(d));
    CHECK(send(g_sock, buf, sizeof(buf), 0) == (ssize_t)sizeof(buf), "send desc");
}

static void send_resume(uint64_t marker) {
    repl_commit_msg_t m;
    memset(&m, 0, sizeof(m));
    m.magic = REPL_COMMIT_MAGIC; m.version = REPL_COMMIT_VERSION;
    m.type = REPL_COMMIT_MSG_RESUME; m.global_seq = marker; m.epoch = g_msg_epoch;
    CHECK(send(g_sock, &m, sizeof(m), 0) == (ssize_t)sizeof(m), "send resume");
}

static void poll_once(int buffering) {
    proxy_commit_poll(buffering);
    usleep(2000);
}

/* 让 proxy 接受连接 */
static void wait_accept(void) { for (int i = 0; i < 50; ++i) { poll_once(0); usleep(2000); } }

static void feed(uint64_t cookie, uint64_t seq, const char *data) {
    proxy_commit_on_record(cookie, seq, (const unsigned char *)data, strlen(data));
}

int main(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/kvs-commit-test-%d.sock", (int)getpid());
    unlink(path);
    CHECK(proxy_commit_init(path, emit_cb, fault_cb) == 0, "init");

    g_sock = connect_master(path);
    CHECK(g_sock >= 0, "connect listener");
    wait_accept();

    /* 1. RESP array WRITE */
    reset_out();
    send_hello(1);
    send_baseline(1001, 1);
    poll_once(0);
    const char *set = "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n";
    feed(1001, 1, set);
    send_desc(1001, 1, 1, (const unsigned char *)set, strlen(set), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0, "array write no fault");
    CHECK(g_out_count == 1 && g_out_len == strlen(set) && memcmp(g_out, set, strlen(set)) == 0,
          "array write emitted");

    /* 2. inline SKIP（PING） */
    reset_out();
    proxy_commit_purge();
    send_baseline(1002, 1);
    poll_once(0);
    const char *ping = "PING\r\n";
    feed(1002, 1, ping);
    send_desc(1002, 1, 0, (const unsigned char *)ping, strlen(ping), REPL_COMMIT_FLAG_SKIP, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0 && g_out_count == 0, "inline skip not emitted");

    /* 3. 半包：分两次到达 */
    reset_out();
    proxy_commit_purge();
    send_baseline(1003, 1);
    poll_once(0);
    const char *cmd = "*3\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    proxy_commit_on_record(1003, 1, (const unsigned char *)cmd, 10);
    poll_once(0);
    CHECK(g_out_count == 0, "half packet not framed");
    proxy_commit_on_record(1003, 2, (const unsigned char *)cmd + 10, strlen(cmd) - 10);
    send_desc(1003, 1, 1, (const unsigned char *)cmd, strlen(cmd), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    if (!(g_fault_reason == 0 && g_out_count == 1 &&
          g_out_len == strlen(cmd) && memcmp(g_out, cmd, strlen(cmd)) == 0))
        fprintf(stderr, "  [half] fault=%d out_count=%d out_len=%zu want=%zu\n",
                g_fault_reason, g_out_count, g_out_len, strlen(cmd));
    CHECK(g_fault_reason == 0 && g_out_count == 1 &&
          g_out_len == strlen(cmd) && memcmp(g_out, cmd, strlen(cmd)) == 0, "half packet assembled");

    /* 4. 控制行与空行不产生命令 */
    reset_out();
    proxy_commit_purge();
    send_baseline(1004, 1);
    poll_once(0);
    feed(1004, 1, "+OK\r\n\r\n");
    poll_once(0);
    CHECK(g_out_count == 0 && g_fault_reason == 0, "control/empty line skipped");
    const char *ping2 = "PING\r\n";
    feed(1004, 2, ping2);
    send_desc(1004, 1, 0, (const unsigned char *)ping2, strlen(ping2), REPL_COMMIT_FLAG_SKIP, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0, "seq stays aligned after control lines");

    /* 5. 畸形 RESP 头被跳过不影响后续命令 */
    reset_out();
    proxy_commit_purge();
    send_baseline(1005, 1);
    poll_once(0);
    feed(1005, 1, "*-1\r\n");
    const char *ping3 = "PING\r\n";
    feed(1005, 2, ping3);
    send_desc(1005, 1, 0, (const unsigned char *)ping3, strlen(ping3), REPL_COMMIT_FLAG_SKIP, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0, "malformed header skipped, next seq aligned");

    /* 6. CRC 错误 → DESYNC */
    reset_out();
    proxy_commit_purge();
    send_baseline(1006, 1);
    poll_once(0);
    feed(1006, 1, ping);
    send_desc(1006, 1, 0, (const unsigned char *)ping, strlen(ping), REPL_COMMIT_FLAG_SKIP, 0xdeadbeef, 1);
    poll_once(0);
    CHECK(g_fault_reason == 2, "crc mismatch → desync");

    /* 7. recv_seq 缺口 → GAP */
    reset_out();
    proxy_commit_purge();
    send_baseline(1007, 1);
    poll_once(0);
    proxy_commit_on_record(1007, 1, (const unsigned char *)ping, strlen(ping));
    proxy_commit_on_record(1007, 3, (const unsigned char *)ping, strlen(ping));
    poll_once(0);
    CHECK(g_fault_reason == 3, "recv_seq gap → gap fault");

    /* 8. 全局序：先到 global=2，等到 global=1 后按序发出 */
    reset_out();
    proxy_commit_purge();
    send_baseline(2001, 1);
    send_baseline(2002, 1);
    poll_once(0);
    const char *a = "*2\r\n$3\r\nDEL\r\n$1\r\na\r\n";
    const char *b = "*2\r\n$3\r\nDEL\r\n$1\r\nb\r\n";
    /* 先建立 global 基线（第一个 WRITE 的 global 被接受为起点） */
    feed(2001, 1, a);
    send_desc(2001, 1, 1, (const unsigned char *)a, strlen(a), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0 && g_out_count == 1, "global baseline write emitted");
    /* global=3 先到，必须等 global=2 */
    feed(2002, 1, b);
    send_desc(2002, 1, 3, (const unsigned char *)b, strlen(b), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    CHECK(g_out_count == 1, "global 3 waits for 2");
    const char *c = "*2\r\n$3\r\nDEL\r\n$1\r\nc\r\n";
    feed(2002, 2, c);
    send_desc(2002, 2, 2, (const unsigned char *)c, strlen(c), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0 && g_out_count == 3 &&
          memcmp(g_out + strlen(a), c, strlen(c)) == 0 &&
          memcmp(g_out + strlen(a) + strlen(c), b, strlen(b)) == 0,
          "global order preserved");

    /* 9. RESUME：BUFFERING 期间缓存，marker 丢弃旧提交 */
    reset_out();
    proxy_commit_purge();
    send_baseline(3001, 1);
    poll_once(0);
    poll_once(1);   /* buffering */
    feed(3001, 1, a);
    send_desc(3001, 1, 10, (const unsigned char *)a, strlen(a), REPL_COMMIT_FLAG_WRITE, 0, 0);
    feed(3001, 2, b);
    send_desc(3001, 2, 11, (const unsigned char *)b, strlen(b), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(1);
    CHECK(g_out_count == 0, "buffering holds writes");
    send_resume(10);
    poll_once(1);
    CHECK(g_fault_reason == 0 && g_out_count == 1 &&
          g_out_len == strlen(b) && memcmp(g_out, b, strlen(b)) == 0,
          "resume drops <=marker, emits >marker");

    /* 9b. 重复 RESUME 幂等；旧 marker 不应丢弃 >marker 的新写 */
    send_resume(10);
    poll_once(1);
    CHECK(g_fault_reason == 0 && g_out_count == 1 && g_out_len == strlen(b),
          "duplicate RESUME is idempotent");
    feed(3001, 3, c);
    send_desc(3001, 3, 12, (const unsigned char *)c, strlen(c), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(1);
    send_resume(10);   /* 旧 marker */
    poll_once(1);
    CHECK(g_fault_reason == 0 && g_out_count == 2 &&
          memcmp(g_out + strlen(b), c, strlen(c)) == 0,
          "stale RESUME keeps newer writes");

    /* 9c. epoch 隔离：旧 epoch 的 BATCH/RESUME 不得影响当前 session */
    reset_out();
    proxy_commit_purge();
    g_msg_epoch = 0xE1;
    send_baseline(6001, 1);
    poll_once(0);
    poll_once(1);
    feed(6001, 1, a);
    g_msg_epoch = 0xE2;
    send_desc(6001, 1, 20, (const unsigned char *)a, strlen(a), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(1);
    CHECK(g_fault_reason == 0, "stale epoch BATCH ignored");
    g_msg_epoch = 0xE1;
    send_desc(6001, 1, 20, (const unsigned char *)a, strlen(a), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(1);
    CHECK(g_fault_reason == 0 && g_out_count == 0, "current epoch BATCH buffered");
    g_msg_epoch = 0xE2;
    send_resume(19);
    poll_once(1);
    CHECK(g_fault_reason == 0 && g_out_count == 0, "stale epoch RESUME ignored");
    g_msg_epoch = 0xE1;
    send_resume(19);
    poll_once(1);
    CHECK(g_fault_reason == 0 && g_out_count == 1 &&
          g_out_len == strlen(a) && memcmp(g_out, a, strlen(a)) == 0,
          "current epoch RESUME drains recovery");
    g_msg_epoch = 0;

    /* 9d. emit 失败必须 fail-closed，且不得计入 delivered */
    reset_out();
    proxy_commit_purge();
    send_baseline(7001, 1);
    poll_once(0);
    feed(7001, 1, a);
    g_fail_emit = 1;
    send_desc(7001, 1, 30, (const unsigned char *)a, strlen(a), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    g_fail_emit = 0;
    CHECK(g_fault_reason == 1 && g_out_count == 0,
          "emit failure → timeout/fail-closed, nothing delivered");

    /* 9e. BASELINE 之前到达的首批记录必须保留（大 value 首条 record 不能丢） */
    reset_out();
    proxy_commit_purge();
    feed(8001, 1, a);                 /* ringbuf 记录先于 BASELINE 到达 */
    send_baseline(8001, 1);
    poll_once(0);
    send_desc(8001, 1, 40, (const unsigned char *)a, strlen(a), REPL_COMMIT_FLAG_WRITE, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0 && g_out_count == 1 &&
          g_out_len == strlen(a) && memcmp(g_out, a, strlen(a)) == 0,
          "pre-baseline captured bytes preserved");

    /* 10. BASELINE 重定基：从 seq=5 开始 */
    reset_out();
    proxy_commit_purge();
    send_baseline(4001, 5);
    poll_once(0);
    feed(4001, 1, ping);
    send_desc(4001, 5, 1, (const unsigned char *)ping, strlen(ping), REPL_COMMIT_FLAG_SKIP, 0, 0);
    poll_once(0);
    CHECK(g_fault_reason == 0 && g_out_count == 0, "baseline next_seq=5 aligned");

    /* 11. pending 超时（2s） */
    reset_out();
    proxy_commit_purge();
    send_baseline(5001, 1);
    poll_once(0);
    feed(5001, 1, ping);
    for (int i = 0; i < 2200 && g_fault_reason == 0; ++i) poll_once(0);
    CHECK(g_fault_reason == 1, "pending timeout → timeout fault");

    proxy_commit_cleanup();
    if (g_sock >= 0) close(g_sock);

    if (g_fail) { printf("commit gate test FAILED (%d)\n", g_fail); return 1; }
    printf("commit gate test OK\n");
    return 0;
}
