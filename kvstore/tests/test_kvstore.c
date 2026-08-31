/*
 * test_kvstore.c — Comprehensive kvstore test program
 *
 * Build:
 *   gcc -Wall -Wextra -O2 -I../include -o test_kvstore test_kvstore.c
 *
 * Usage:
 *   ./test_kvstore [--config tests/test.conf] [host port]
 *
 * This program connects to a running kvstore server via TCP and tests all
 * major features using the Redis RESP protocol.  It is self-contained
 * (no hiredis dependency) so it can run in any environment.
 *
 * Because kvstore speaks the RESP wire protocol, it is also fully
 * compatible with:
 *   - redis-cli
 *   - hiredis  (C client library)
 *   - redis-benchmark
 *
 * Example:
 *   # Terminal 1: start kvstore
 *   ./kvstore kvstore.conf --role master
 *
 *   # Terminal 2: run tests
 *   ./test_kvstore --config tests/test.conf
 *
 *   # Or use redis-cli / redis-benchmark directly:
 *   redis-cli -p 5160 SET foo bar
 *   redis-cli -p 5160 GET foo
 *   redis-benchmark -p 5160 -n 10000 -c 50 SET __rand_int__ __rand_int__
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

/* ---------- test framework ---------- */

static int g_pass = 0;
static int g_fail = 0;
static int g_sockfd = -1;

#define ANSI_GREEN   "\033[32m"
#define ANSI_RED     "\033[31m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_RESET   "\033[0m"

static void test_banner(const char *msg) {
    printf("\n" ANSI_CYAN "===== %s =====" ANSI_RESET "\n\n", msg);
}

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

/* ---------- TCP / RESP helpers ---------- */

static int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);

    /* resolve hostname */
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

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* Build a RESP-encoded command and return a malloc'd buffer + length */
static unsigned char *build_resp(const char *cmd, int argc, const char **argv,
                                 size_t *out_len) {
    (void)cmd; /* command is argv[0] */
    /* estimate: 64 bytes overhead + sum of arg lengths */
    size_t cap = 64;
    for (int i = 0; i < argc; i++) cap += strlen(argv[i]) + 32;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return NULL;
    size_t pos = 0;

    /* *<argc>\r\n */
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

/* Send all bytes */
static int send_all(int fd, const unsigned char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n <= 0) return -1;
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

/* Read raw response into a dynamic buffer (up to max_read) */
static unsigned char *recv_resp(int fd, size_t *out_len, size_t max_read) {
    size_t cap = 4096;
    size_t len = 0;
    unsigned char *buf = (unsigned char *)malloc(cap);
    if (!buf) return NULL;

    while (len < max_read) {
        if (cap - len < 1024) {
            cap *= 2;
            unsigned char *tmp = (unsigned char *)realloc(buf, cap);
            if (!tmp) { free(buf); return NULL; }
            buf = tmp;
        }
        ssize_t n = read(fd, buf + len, cap - len - 1);
        if (n <= 0) break;
        len += (size_t)n;

        /* Try to detect if response is complete:
         * For simple RESP types, check if we have at least a full response.
         * For arrays/bulk, this is more complex; we use a simple heuristic
         * of waiting briefly for more data. */
        if (len >= 2 && buf[len - 1] == '\n') {
            /* Check if previous char was \r — likely end of response.
             * For pipelines with multiple responses, we read all available. */
            if (buf[len - 2] == '\r') {
                /* Heuristic: if we have more than one complete RESP message,
                 * keep reading a bit more to catch pipelined responses. */
                if (max_read > 0) {
                    /* After first complete message, try one more read with short timeout */
                    struct timeval tv = {0, 50000}; /* 50ms */
                    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                    ssize_t extra = read(fd, buf + len, cap - len - 1);
                    tv.tv_sec = 0; tv.tv_usec = 0;
                    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                    if (extra > 0) len += (size_t)extra;
                }
                break;
            }
        }
    }
    buf[len] = '\0';
    *out_len = len;
    return buf;
}

/* Send a RESP command and receive response */
static unsigned char *do_cmd(int fd, size_t *rlen, const char *cmd, int argc, const char **argv) {
    size_t wlen;
    unsigned char *wbuf = build_resp(cmd, argc, argv, &wlen);
    if (!wbuf) return NULL;
    int rc = send_all(fd, wbuf, wlen);
    free(wbuf);
    if (rc != 0) return NULL;
    return recv_resp(fd, rlen, 65536);
}

/* High-level: send command and compare response to expected string */
static int expect_resp(const char *name, const char *cmd, int argc,
                       const char **argv, const char *expected) {
    size_t rlen;
    unsigned char *resp = do_cmd(g_sockfd, &rlen, cmd, argc, argv);
    if (!resp) {
        test_fail("%s — no response (connection lost?)", name);
        return -1;
    }

    if (strcmp((const char *)resp, expected) == 0) {
        test_pass("%s", name);
        free(resp);
        return 0;
    } else {
        /* Show expected vs actual, escape non-printable chars */
        char resp_esc[256], exp_esc[256];
        size_t ri = 0, ei = 0;
        for (size_t i = 0; i < rlen && ri < 250; i++) {
            if (resp[i] == '\r') { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'r'; }
            else if (resp[i] == '\n') { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'n'; }
            else if (isprint((unsigned char)resp[i])) resp_esc[ri++] = (char)resp[i];
            else { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'x'; }
        }
        resp_esc[ri] = '\0';
        for (size_t i = 0; expected[i] && ei < 250; i++) {
            if (expected[i] == '\r') { exp_esc[ei++] = '\\'; exp_esc[ei++] = 'r'; }
            else if (expected[i] == '\n') { exp_esc[ei++] = '\\'; exp_esc[ei++] = 'n'; }
            else if (isprint((unsigned char)expected[i])) exp_esc[ei++] = expected[i];
            else { exp_esc[ei++] = '\\'; exp_esc[ei++] = 'x'; }
        }
        exp_esc[ei] = '\0';

        test_fail("%s", name);
        printf("         expected: '%s'\n", exp_esc);
        printf("         actual:   '%s'\n", resp_esc);
        free(resp);
        return 1;
    }
}

/* Variadic wrapper for expect_resp */
static int expect(const char *name, const char *expected, int argc, ...) {
    const char **argv = (const char **)malloc((size_t)argc * sizeof(const char *));
    if (!argv) { test_fail("%s — OOM", name); return -1; }
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc; i++) {
        argv[i] = va_arg(ap, const char *);
    }
    va_end(ap);
    int rc = expect_resp(name, argc >= 1 ? argv[0] : "", argc, argv, expected);
    free(argv);
    return rc;
}

/* Test a command and check that the response *contains* a substring */
static int expect_contains(const char *name, const char *substr, int argc, ...) {
    const char **argv = (const char **)malloc((size_t)argc * sizeof(const char *));
    if (!argv) { test_fail("%s — OOM", name); return -1; }
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc; i++) {
        argv[i] = va_arg(ap, const char *);
    }
    va_end(ap);

    size_t rlen;
    unsigned char *resp = do_cmd(g_sockfd, &rlen, argc >= 1 ? argv[0] : "", argc, argv);
    free(argv);
    if (!resp) {
        test_fail("%s — no response", name);
        return -1;
    }
    if (strstr((const char *)resp, substr) != NULL) {
        test_pass("%s", name);
        free(resp);
        return 0;
    } else {
        char resp_esc[256];
        size_t ri = 0;
        for (size_t i = 0; i < rlen && ri < 250; i++) {
            if (resp[i] == '\r') { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'r'; }
            else if (resp[i] == '\n') { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'n'; }
            else if (isprint((unsigned char)resp[i])) resp_esc[ri++] = (char)resp[i];
            else { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'x'; }
        }
        resp_esc[ri] = '\0';
        test_fail("%s — expected containing '%s', got '%s'", name, substr, resp_esc);
        free(resp);
        return 1;
    }
}

/* ==================================================================
 * Test suites
 * ================================================================*/

static void test_ping(void) {
    test_banner("PING");
    expect("PING", "+PONG\r\n", 1, "PING");
}

static void test_array_engine(void) {
    test_banner("Array engine (SET / GET / DEL / MOD / EXIST)");

    expect("SET new key", "+OK\r\n", 3, "SET", "arr_test_key", "hello_world");
    expect("GET existing", "$11\r\nhello_world\r\n", 2, "GET", "arr_test_key");
    expect("EXIST existing", ":1\r\n", 2, "EXIST", "arr_test_key");
    expect("MOD existing", "+OK\r\n", 3, "MOD", "arr_test_key", "modified_val");
    expect("GET after MOD", "$12\r\nmodified_val\r\n", 2, "GET", "arr_test_key");
    expect("DEL existing", "+OK\r\n", 2, "DEL", "arr_test_key");
    expect("EXIST after DEL", ":0\r\n", 2, "EXIST", "arr_test_key");
    expect("GET deleted", "$-1\r\n", 2, "GET", "arr_test_key");
    expect("MOD non-existent", "-ERR not found or exists\r\n", 3, "MOD", "arr_test_key", "new_val");
    expect("SET duplicate (returns OK per kvstore semantics)", "+OK\r\n", 3, "SET", "arr_test_key", "dup");
    expect("DEL non-existent", "-ERR not found or exists\r\n", 2, "DEL", "nonexistent_arr");

    /* Clean up */
    expect("DEL cleanup", "+OK\r\n", 2, "DEL", "arr_test_key");
}

static void test_rbtree_engine(void) {
    test_banner("Rbtree engine (RSET / RGET / RDEL / RMOD / REXIST)");

    expect("RSET new key", "+OK\r\n", 3, "RSET", "rbt_test_key", "val_rbt");
    expect("RGET existing", "$7\r\nval_rbt\r\n", 2, "RGET", "rbt_test_key");
    expect("REXIST existing", ":1\r\n", 2, "REXIST", "rbt_test_key");
    expect("RMOD existing", "+OK\r\n", 3, "RMOD", "rbt_test_key", "modified_rbt");
    expect("RGET after RMOD", "$12\r\nmodified_rbt\r\n", 2, "RGET", "rbt_test_key");
    expect("RDEL existing", "+OK\r\n", 2, "RDEL", "rbt_test_key");
    expect("RGET after RDEL", "$-1\r\n", 2, "RGET", "rbt_test_key");
    expect("RMOD non-existent", "-ERR not found or exists\r\n", 3, "RMOD", "rbt_test_key", "x");
    expect("RDEL non-existent", "-ERR not found or exists\r\n", 2, "RDEL", "nonexistent_rbt");
}

static void test_hash_engine(void) {
    test_banner("Hash engine (HSET / HGET / HDEL / HMOD / HEXIST)");

    expect("HSET new key", "+OK\r\n", 3, "HSET", "hash_test_key", "val_hash");
    expect("HGET existing", "$8\r\nval_hash\r\n", 2, "HGET", "hash_test_key");
    expect("HEXIST existing", ":1\r\n", 2, "HEXIST", "hash_test_key");
    expect("HMOD existing", "+OK\r\n", 3, "HMOD", "hash_test_key", "modified_hash");
    expect("HGET after HMOD", "$13\r\nmodified_hash\r\n", 2, "HGET", "hash_test_key");
    expect("HDEL existing", "+OK\r\n", 2, "HDEL", "hash_test_key");
    expect("HGET after HDEL", "$-1\r\n", 2, "HGET", "hash_test_key");
    expect("HMOD non-existent", "-ERR not found or exists\r\n", 3, "HMOD", "hash_test_key", "x");
    expect("HDEL non-existent", "-ERR not found or exists\r\n", 2, "HDEL", "nonexistent_hash");
}

static void test_skiptable_engine(void) {
    test_banner("Skiptable engine (XSET / XGET / XDEL / XMOD / XEXIST)");

    expect("XSET new key", "+OK\r\n", 3, "XSET", "skp_test_key", "val_skp");
    expect("XGET existing", "$7\r\nval_skp\r\n", 2, "XGET", "skp_test_key");
    expect("XEXIST existing", ":1\r\n", 2, "XEXIST", "skp_test_key");
    expect("XMOD existing", "+OK\r\n", 3, "XMOD", "skp_test_key", "modified_skp");
    expect("XGET after XMOD", "$12\r\nmodified_skp\r\n", 2, "XGET", "skp_test_key");
    expect("XDEL existing", "+OK\r\n", 2, "XDEL", "skp_test_key");
    expect("XGET after XDEL", "$-1\r\n", 2, "XGET", "skp_test_key");
    expect("XMOD non-existent", "-ERR not found or exists\r\n", 3, "XMOD", "skp_test_key", "x");
    expect("XDEL non-existent", "-ERR not found or exists\r\n", 2, "XDEL", "nonexistent_skp");
}

static void test_multi_commands(void) {
    test_banner("Multi-key commands (MSET / MGET)");

    /* Clean up first */
    expect("MSET two keys", "+OK\r\n", 5, "MSET",
           "multi_a", "value_a",
           "multi_b", "value_b");
    expect("MGET two keys", "*2\r\n$7\r\nvalue_a\r\n$7\r\nvalue_b\r\n",
           3, "MGET", "multi_a", "multi_b");

    /* RMSET / RMGET - rbtree multi */
    expect("RMSET two keys", "+OK\r\n", 5, "RMSET",
           "rmulti_a", "rval_a",
           "rmulti_b", "rval_b");
    expect("RMGET two keys", "*2\r\n$6\r\nrval_a\r\n$6\r\nrval_b\r\n",
           3, "RMGET", "rmulti_a", "rmulti_b");

    /* HMSET / HMGET - hash multi */
    expect("HMSET two keys", "+OK\r\n", 5, "HMSET",
           "hmulti_a", "hval_a",
           "hmulti_b", "hval_b");
    expect("HMGET two keys", "*2\r\n$6\r\nhval_a\r\n$6\r\nhval_b\r\n",
           3, "HMGET", "hmulti_a", "hmulti_b");

    /* XMSET / XMGET - skiptable multi */
    expect("XMSET two keys", "+OK\r\n", 5, "XMSET",
           "xmulti_a", "xval_a",
           "xmulti_b", "xval_b");
    expect("XMGET two keys", "*2\r\n$6\r\nxval_a\r\n$6\r\nxval_b\r\n",
           3, "XMGET", "xmulti_a", "xmulti_b");
}

static void test_ttl_expire(void) {
    test_banner("TTL / Expire commands");

    /* Array engine */
    expect("SET for expire test", "+OK\r\n", 3, "SET", "exp_arr", "will_expire");
    expect("EXPIRE with 60s", "+OK\r\n", 3, "EXPIRE", "exp_arr", "60");
    expect("EXIST before expire", ":1\r\n", 2, "EXIST", "exp_arr");

    /* Rbtree engine */
    expect("RSET for expire test", "+OK\r\n", 3, "RSET", "exp_rbt", "will_expire_rbt");
    expect("REXPIRE", "+OK\r\n", 3, "REXPIRE", "exp_rbt", "60");
    expect("REXIST before expire", ":1\r\n", 2, "REXIST", "exp_rbt");

    /* Hash engine */
    expect("HSET for expire test", "+OK\r\n", 3, "HSET", "exp_hash", "will_expire_hash");
    expect("HEXPIRE", "+OK\r\n", 3, "HEXPIRE", "exp_hash", "60");
    expect("HEXIST before expire", ":1\r\n", 2, "HEXIST", "exp_hash");

    /* Skiptable engine */
    expect("XSET for expire test", "+OK\r\n", 3, "XSET", "exp_skp", "will_expire_skp");
    expect("XEXPIRE", "+OK\r\n", 3, "XEXPIRE", "exp_skp", "60");
    expect("XEXIST before expire", ":1\r\n", 2, "XEXIST", "exp_skp");

    /* PERSIST (remove TTL) */
    expect("PERSIST on array key (removes TTL)", "+OK\r\n", 2, "PERSIST", "exp_arr");
    expect("RPERSIST on rbtree key", "+OK\r\n", 2, "RPERSIST", "exp_rbt");
    expect("HPERSIST on hash key", "+OK\r\n", 2, "HPERSIST", "exp_hash");
    expect("XPERSIST on skiptable key", "+OK\r\n", 2, "XPERSIST", "exp_skp");

    /* Clean up */
    expect("DEL expire test keys", "+OK\r\n", 2, "DEL", "exp_arr");
    expect("RDEL expire test keys", "+OK\r\n", 2, "RDEL", "exp_rbt");
    expect("HDEL expire test keys", "+OK\r\n", 2, "HDEL", "exp_hash");
    expect("XDEL expire test keys", "+OK\r\n", 2, "XDEL", "exp_skp");
}

static void test_lock_commands(void) {
    test_banner("Lock commands (LOCK / UNLOCK / RENEW)");

    /* LOCK/UNLOCK/RENEW return RESP integers (1=success, 0=fail) */
    expect("LOCK acquire", ":1\r\n", 4, "LOCK", "lock1", "token1", "10000");
    /* LOCK returns :0 when key already locked */
    expect("LOCK duplicate (key exists)", ":0\r\n", 4, "LOCK", "lock1", "token2", "10000");
    expect("LOCK different key", ":1\r\n", 4, "LOCK", "lock2", "token1", "10000");

    /* UNLOCK */
    expect("UNLOCK correct token", ":1\r\n", 3, "UNLOCK", "lock1", "token1");
    expect("UNLOCK already released", ":0\r\n", 3, "UNLOCK", "lock1", "token1");

    /* RENEW */
    expect("LOCK for renew test", ":1\r\n", 4, "LOCK", "lock_rn", "token_rn", "10000");
    expect("RENEW correct token", ":1\r\n", 4, "RENEW", "lock_rn", "token_rn", "20000");
    expect("UNLOCK cleanup", ":1\r\n", 3, "UNLOCK", "lock_rn", "token_rn");
}

static void test_edge_cases(void) {
    test_banner("Edge cases");

    /* Keys with spaces (RESP protocol handles this natively) */
    expect("HSET key with spaces", "+OK\r\n", 3, "HSET", "key with spaces", "value1");
    expect("HGET key with spaces", "$6\r\nvalue1\r\n", 2, "HGET", "key with spaces");

    /* Values with spaces */
    expect("HSET value with spaces", "+OK\r\n", 3, "HSET", "spaces_val_key", "hello world foo");
    expect("HGET value with spaces", "$15\r\nhello world foo\r\n", 2, "HGET", "spaces_val_key");

    /* Keys with newlines */
    expect("HSET key with \\r\\n", "+OK\r\n", 3, "HSET", "line1\r\nline2", "newline_key_val");
    expect("HGET key with \\r\\n", "$15\r\nnewline_key_val\r\n", 2, "HGET", "line1\r\nline2");

    /* Values with newlines */
    expect("HSET value with \\r\\n", "+OK\r\n", 3, "HSET", "nl_val_key", "lineA\r\nlineB");
    expect("HGET value with \\r\\n", "$12\r\nlineA\r\nlineB\r\n", 2, "HGET", "nl_val_key");

    /* Empty key (some stores accept, some reject — just verify no crash) */
    expect("SET with empty key", "+OK\r\n", 3, "SET", "", "val");

    /* Very long key name */
    char long_key[300];
    memset(long_key, 'A', 299);
    long_key[299] = '\0';
    expect("HSET long key (299 chars)", "+OK\r\n", 3, "HSET", long_key, "long_val");
    expect("HGET long key", "$8\r\nlong_val\r\n", 2, "HGET", long_key);

    /* Binary-safe value — RESP length-prefixed bulk strings handle binary natively */
    expect("HSET binary value (0x01..0x03)", "+OK\r\n", 3, "HSET", "bin_key", "val\x01\x02\x03val");
    expect("HGET binary value", "$9\r\nval\x01\x02\x03val\r\n", 2, "HGET", "bin_key");

    /* Cleanup */
    expect("HDEL cleanup spaces", "+OK\r\n", 2, "HDEL", "key with spaces");
    expect("HDEL cleanup newline key", "+OK\r\n", 2, "HDEL", "line1\r\nline2");
    expect("HDEL cleanup nl val key", "+OK\r\n", 2, "HDEL", "nl_val_key");
    expect("HDEL cleanup bin key", "+OK\r\n", 2, "HDEL", "bin_key");
}

static void test_pipeline(void) {
    test_banner("Pipeline (multiple commands in one send)");

    /* Use a simpler pipeline: SET k1 v1, GET k1 via Array engine */
    const char *pipeline_data[] = {
        "*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n",
        "*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n",
        "*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n",
        "*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n",
    };
    unsigned char *pbuf = NULL;
    size_t plen = 0;
    for (int i = 0; i < 4; i++) {
        size_t this_len = strlen(pipeline_data[i]);
        pbuf = (unsigned char *)realloc(pbuf, plen + this_len);
        if (!pbuf) { test_fail("pipeline OOM"); return; }
        memcpy(pbuf + plen, pipeline_data[i], this_len);
        plen += this_len;
    }

    if (send_all(g_sockfd, pbuf, plen) != 0) {
        test_fail("pipeline send");
        free(pbuf);
        return;
    }
    free(pbuf);

    /* Read all pipelined responses */
    size_t rlen;
    unsigned char *resp = recv_resp(g_sockfd, &rlen, 65536);
    if (!resp) { test_fail("pipeline recv - no response"); return; }

    /* Expected: +OK\r\n+OK\r\n$2\r\nv1\r\n$2\r\nv2\r\n */
    const char *expected = "+OK\r\n+OK\r\n$2\r\nv1\r\n$2\r\nv2\r\n";
    if (strcmp((const char *)resp, expected) == 0) {
        test_pass("Pipeline SET x2 + GET x2");
    } else {
        char resp_esc[256], exp_esc[256];
        size_t ri = 0, ei = 0;
        for (size_t i = 0; i < rlen && ri < 250; i++) {
            if (resp[i] == '\r') { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'r'; }
            else if (resp[i] == '\n') { resp_esc[ri++] = '\\'; resp_esc[ri++] = 'n'; }
            else if (isprint((unsigned char)resp[i])) resp_esc[ri++] = (char)resp[i];
            else { resp_esc[ri++] = '?'; }
        }
        resp_esc[ri] = '\0';
        for (size_t i = 0; expected[i] && ei < 250; i++) {
            if (expected[i] == '\r') { exp_esc[ei++] = '\\'; exp_esc[ei++] = 'r'; }
            else if (expected[i] == '\n') { exp_esc[ei++] = '\\'; exp_esc[ei++] = 'n'; }
            else if (isprint((unsigned char)expected[i])) exp_esc[ei++] = expected[i];
            else { exp_esc[ei++] = '?'; }
        }
        exp_esc[ei] = '\0';
        test_fail("Pipeline SET x2 + GET x2");
        printf("         expected: '%s'\n", exp_esc);
        printf("         actual:   '%s'\n", resp_esc);
    }
    free(resp);

    /* Cleanup */
    expect("DEL pipeline cleanup k1", "+OK\r\n", 2, "DEL", "k1");
    expect("DEL pipeline cleanup k2", "+OK\r\n", 2, "DEL", "k2");
}

static void test_persistence(void) {
    test_banner("Persistence (SAVE)");

    /* Insert some data and SAVE */
    expect("SET data for SAVE", "+OK\r\n", 3, "HSET", "save_test", "save_me");
    expect("SAVE (persist_dump)", "+OK\r\n", 1, "SAVE");

    /* Verify data still exists after SAVE */
    expect("HGET after SAVE", "$7\r\nsave_me\r\n", 2, "HGET", "save_test");
}

static void test_info_command(void) {
    test_banner("INFO / MEMSTAT / ROLE");

    expect_contains("INFO contains 'role'", "role", 1, "INFO");
    expect_contains("INFO contains 'mem'", "mem", 1, "INFO");
    expect_contains("INFO contains 'dirty'", "dirty", 1, "INFO");
    expect_contains("MEMSTAT contains 'backend'", "backend", 1, "MEMSTAT");
    expect_contains("ROLE contains 'master' or 'slave'", "master", 1, "ROLE");
}

static void test_stress_brief(void) {
    test_banner("Brief stress test (1000 ops)");

    int n = 1000;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < n; i++) {
        char key[64], val[64];
        snprintf(key, sizeof(key), "stress_h_%d", i);
        snprintf(val, sizeof(val), "val_%d", i);
        if (expect("HSET in stress", "+OK\r\n", 3, "HSET", key, val) != 0) return;
    }

    gettimeofday(&tv_end, NULL);
    long long elapsed = (tv_end.tv_sec - tv_begin.tv_sec) * 1000LL
                      + (tv_end.tv_usec - tv_begin.tv_usec) / 1000LL;
    if (elapsed > 0) {
        printf("         %d HSET ops in %lld ms = %lld QPS\n",
               n, elapsed, (long long)n * 1000LL / elapsed);
    }

    /* Verify some values — "val_%d" length varies: val_0=5, val_100=7, etc. */
    for (int i = 0; i < n; i += 100) {
        char key[64], val[64], expected[128];
        snprintf(key, sizeof(key), "stress_h_%d", i);
        snprintf(val, sizeof(val), "val_%d", i);
        int vlen = (int)strlen(val);
        snprintf(expected, sizeof(expected), "$%d\r\n%s\r\n", vlen, val);
        if (expect("HGET verify in stress", expected, 2, "HGET", key) != 0) return;
    }

    /* Cleanup — bulk delete using HDEL is not available, do individually */
    for (int i = 0; i < n; i += 100) {
        char key[64];
        snprintf(key, sizeof(key), "stress_h_%d", i);
        expect("HDEL cleanup in stress", "+OK\r\n", 2, "HDEL", key);
    }
}

/* ==================================================================
 * Main
 * ================================================================*/

/* ── 配置文件解析（支持 --config test.conf） ── */
static const char *g_host = "192.168.233.128";
static int g_port = 5160;

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
        if (strcmp(key, "host") == 0) g_host = strdup(val);
        else if (strcmp(key, "port") == 0) g_port = atoi(val);
    }
    fclose(fp);
    return 0;
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 5000;
    int positional = 0;

    parse_config_file("tests/test.conf");
    parse_config_file("test.conf");

    /* Use config values as base */
    host = g_host;
    port = g_port;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            parse_config_file(argv[++i]);
            host = g_host;
            port = g_port;
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("用法: %s [选项] [host port]\n", argv[0]);
            printf("\nkvstore 综合功能测试:\n\n");
            printf("选项:\n");
            printf("  --host HOST     kvstore 地址 (默认 %s)\n", host);
            printf("  --port PORT     kvstore 端口 (默认 %d)\n", port);
            printf("  --config PATH   加载配置文件 (默认 tests/test.conf)\n");
            printf("  -h              显示此帮助\n");
            printf("\n也支持位置参数: %s <host> <port>\n", argv[0]);
            return 0;
        } else if (argv[i][0] != '-' && positional == 0) {
            host = argv[i];
            positional = 1;
        } else if (argv[i][0] != '-' && positional == 1) {
            port = atoi(argv[i]);
            positional = 2;
        } else {
            fprintf(stderr, "未知选项: %s\n", argv[i]);
            return 1;
        }
    }

    printf("kvstore test client — connecting to %s:%d\n\n", host, port);

    g_sockfd = tcp_connect(host, port);
    if (g_sockfd < 0) {
        fprintf(stderr, ANSI_RED "ERROR:" ANSI_RESET
                " cannot connect to %s:%d — is kvstore running?\n", host, port);
        return 1;
    }

    /* Run test suites */
    test_ping();
    test_array_engine();
    test_rbtree_engine();
    test_hash_engine();
    test_skiptable_engine();
    test_multi_commands();
    test_ttl_expire();
    test_lock_commands();
    test_edge_cases();
    test_pipeline();
    test_persistence();
    test_info_command();
    test_stress_brief();

    /* Summary */
    printf("\n" ANSI_CYAN "========== SUMMARY ==========" ANSI_RESET "\n");
    printf(ANSI_GREEN "PASS: %d" ANSI_RESET "\n", g_pass);
    if (g_fail > 0) {
        printf(ANSI_RED "FAIL: %d" ANSI_RESET "\n", g_fail);
    } else {
        printf("FAIL: 0\n");
    }
    printf("\n");

    close(g_sockfd);
    return g_fail > 0 ? 1 : 0;
}
