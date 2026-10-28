// ============================================================
// kvs_commit.c — Commit Gate（阶段3）Master 侧
//
//  - 全局连接注册表（owner 线程访问）：proxy 重连后发 BASELINE、等待命令边界；
//  - Unix SOCK_SEQPACKET 控制面：HELLO/BASELINE/IGNORE/FORGET/RESUME/DESC_BATCH；
//  - post-commit：写命令执行成功后发布 WRITE；其余命令发布 SKIP；
//  - global_seq：Master 执行顺序，proxy 按此顺序转发到 Slave。
// ============================================================
#include "kvstore/kvstore.h"
#include "kvstore/replication/commit_gate.h"

#include <sys/un.h>
#include <poll.h>

/* ---------------- 全局连接注册表（owner 线程） ---------------- */
static conn_t *g_conn_head = NULL;

void kvs_conn_register(conn_t *c) {
    if (!c) return;
    c->next_all = g_conn_head;
    g_conn_head = c;
}

void kvs_conn_unregister(conn_t *c) {
    if (!c) return;
    conn_t **pp = &g_conn_head;
    while (*pp) {
        if (*pp == c) { *pp = c->next_all; break; }
        pp = &(*pp)->next_all;
    }
    c->next_all = NULL;
}

/* ---------------- Commit Gate Master 状态 ---------------- */
#define COMMIT_SEND_POLL_MS 2

static int g_commit_fd = -1;
static char g_commit_path[512];
static int g_commit_path_ready = 0;
static uint64_t g_epoch = 0;
static uint64_t g_global_seq = 0;
static repl_commit_desc_t g_batch[REPL_COMMIT_BATCH_MAX];
static int g_batch_n = 0;
static pthread_mutex_t g_commit_lock = PTHREAD_MUTEX_INITIALIZER;

static unsigned long long g_desc_count = 0;
static unsigned long long g_write_count = 0;
static unsigned long long g_skip_count = 0;
static unsigned long long g_desync_count = 0;
static unsigned long long g_timeout_count = 0;
static unsigned long long g_gap_count = 0;
static char g_fault_reason[32] = "NONE";

static volatile int g_rearm_pending = 0;
static long long g_rearm_start_ms = 0;

static int commit_active_mode(void) {
    /* Commit Gate 依赖 ebpf-proxy 的 client_capture 数据面 */
    return g_cfg.role == ROLE_MASTER &&
           strstr(g_cfg.repl_realtime_transport, "ebpf+tcp") != NULL;
}

int repl_commit_active_mode(void) {
    return commit_active_mode();
}

static int commit_eligible(conn_t *c) {
    return c && !c->is_listener && !c->is_replica &&
           c->repl_socket_cookie != 0 && commit_active_mode();
}

const char *repl_commit_socket_path(void) {
    if (!g_commit_path_ready) {
        snprintf(g_commit_path, sizeof(g_commit_path), "%s%s",
                 g_cfg.dump_path[0] ? g_cfg.dump_path : "kvstore",
                 REPL_COMMIT_SOCK_SUFFIX);
        g_commit_path_ready = 1;
    }
    return g_commit_path;
}

uint64_t repl_commit_global_seq(void) {
    return __atomic_load_n(&g_global_seq, __ATOMIC_RELAXED);
}
unsigned long long repl_commit_descriptor_count(void) { return g_desc_count; }
unsigned long long repl_commit_write_count(void) { return g_write_count; }
unsigned long long repl_commit_skip_count(void) { return g_skip_count; }
unsigned long long repl_commit_desync_count(void) { return g_desync_count; }
unsigned long long repl_commit_timeout_count(void) { return g_timeout_count; }
unsigned long long repl_commit_gap_count(void) { return g_gap_count; }
const char *repl_commit_fault_reason_name(void) { return g_fault_reason; }

static void commit_set_fault(const char *reason) {
    snprintf(g_fault_reason, sizeof(g_fault_reason), "%s", reason ? reason : "UNKNOWN");
    /* 停 capture 由 health tick 统一处理（它读 fault/通道状态）。 */
}

void repl_commit_note_proxy_fault(const char *reason) {
    commit_set_fault(reason);
}

/* 非阻塞发送；EAGAIN 最多等 2ms。失败则断开通道，health tick 会重连+REARM。 */
static int commit_send_locked(const void *buf, size_t len) {
    if (g_commit_fd < 0) return -1;
    for (int attempt = 0; attempt < 2; ++attempt) {
        ssize_t w = send(g_commit_fd, buf, len, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (w == (ssize_t)len) return 0;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd pfd = { .fd = g_commit_fd, .events = POLLOUT };
            if (poll(&pfd, 1, COMMIT_SEND_POLL_MS) > 0) continue;
        } else if (w < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    close(g_commit_fd);
    g_commit_fd = -1;
    commit_set_fault("CHANNEL");
    return -1;
}

static void commit_flush_locked(void) {
    if (g_batch_n == 0) return;
    unsigned char msg[REPL_COMMIT_MAX_MSG];
    repl_commit_msg_t *h = (repl_commit_msg_t *)msg;
    memset(h, 0, sizeof(*h));
    h->magic = REPL_COMMIT_MAGIC;
    h->version = REPL_COMMIT_VERSION;
    h->type = REPL_COMMIT_MSG_BATCH;
    h->count = (uint32_t)g_batch_n;
    h->epoch = g_epoch;
    memcpy(msg + sizeof(*h), g_batch, (size_t)g_batch_n * sizeof(repl_commit_desc_t));
    size_t len = sizeof(*h) + (size_t)g_batch_n * sizeof(repl_commit_desc_t);
    if (commit_send_locked(msg, len) == 0)
        g_desc_count += (unsigned long long)g_batch_n;
    g_batch_n = 0;
}

void repl_commit_flush(void) {
    pthread_mutex_lock(&g_commit_lock);
    commit_flush_locked();
    pthread_mutex_unlock(&g_commit_lock);
}

static void commit_batch_add(const repl_commit_desc_t *d) {
    g_batch[g_batch_n++] = *d;
    if (g_batch_n >= REPL_COMMIT_BATCH_MAX) commit_flush_locked();
}

void repl_commit_publish_write(conn_t *c) {
    if (!commit_eligible(c) || c->cur_cmd_committed || c->cur_cmd_seq == 0) return;
    c->cur_cmd_committed = 1;
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd < 0) {
        commit_set_fault("CHANNEL");
        pthread_mutex_unlock(&g_commit_lock);
        return;
    }
    repl_commit_desc_t d;
    memset(&d, 0, sizeof(d));
    d.cookie = c->repl_socket_cookie;
    d.command_seq = c->cur_cmd_seq;
    d.global_seq = __atomic_add_fetch(&g_global_seq, 1, __ATOMIC_RELAXED);
    d.crc = c->cur_cmd_crc;
    d.raw_len = c->cur_cmd_len;
    d.flags = REPL_COMMIT_FLAG_WRITE;
    g_write_count++;
    commit_batch_add(&d);
    pthread_mutex_unlock(&g_commit_lock);
}

void repl_commit_publish_skip(conn_t *c) {
    if (!commit_eligible(c) || c->cur_cmd_committed || c->cur_cmd_seq == 0) return;
    c->cur_cmd_committed = 1;
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd < 0) {
        pthread_mutex_unlock(&g_commit_lock);
        return;
    }
    repl_commit_desc_t d;
    memset(&d, 0, sizeof(d));
    d.cookie = c->repl_socket_cookie;
    d.command_seq = c->cur_cmd_seq;
    d.global_seq = 0;
    d.crc = c->cur_cmd_crc;
    d.raw_len = c->cur_cmd_len;
    d.flags = REPL_COMMIT_FLAG_SKIP;
    g_skip_count++;
    commit_batch_add(&d);
    pthread_mutex_unlock(&g_commit_lock);
}

static int commit_send_msg(uint32_t type, const repl_commit_kv_t *kv,
                           uint64_t global_seq) {
    repl_commit_msg_t m;
    memset(&m, 0, sizeof(m));
    m.magic = REPL_COMMIT_MAGIC;
    m.version = REPL_COMMIT_VERSION;
    m.type = type;
    m.count = kv ? 1 : 0;
    m.epoch = g_epoch;
    m.global_seq = global_seq;
    unsigned char buf[sizeof(m) + sizeof(repl_commit_kv_t)];
    memcpy(buf, &m, sizeof(m));
    if (kv) memcpy(buf + sizeof(m), kv, sizeof(*kv));
    return commit_send_locked(buf, sizeof(m) + (kv ? sizeof(*kv) : 0));
}

void repl_commit_baseline(conn_t *c) {
    if (!commit_eligible(c)) return;
    /* 只捕获注册过的客户端连接（排除副本控制/RDMA listener 等） */
    repl_ebpf_cookie_allow(c->repl_socket_cookie, 1);
    repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie,
                            .value = c->repl_parsed_seq + 1 };
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0) commit_send_msg(REPL_COMMIT_MSG_BASELINE, &kv, 0);
    pthread_mutex_unlock(&g_commit_lock);
}

void repl_commit_ignore(conn_t *c) {
    if (!c || c->repl_socket_cookie == 0) return;
    repl_ebpf_cookie_allow(c->repl_socket_cookie, 0);
    repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie, .value = 0 };
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0) commit_send_msg(REPL_COMMIT_MSG_IGNORE, &kv, 0);
    pthread_mutex_unlock(&g_commit_lock);
}

void repl_commit_forget(conn_t *c) {
    if (!c || c->repl_socket_cookie == 0) return;
    repl_ebpf_cookie_allow(c->repl_socket_cookie, 0);
    repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie, .value = 0 };
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0) commit_send_msg(REPL_COMMIT_MSG_FORGET, &kv, 0);
    pthread_mutex_unlock(&g_commit_lock);
}

void repl_commit_resume(uint64_t global_seq) {
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0) commit_send_msg(REPL_COMMIT_MSG_RESUME, NULL, global_seq);
    pthread_mutex_unlock(&g_commit_lock);
}

int repl_commit_connected(void) {
    pthread_mutex_lock(&g_commit_lock);
    int ok = g_commit_fd >= 0;
    pthread_mutex_unlock(&g_commit_lock);
    return ok;
}

int repl_commit_channel_ok(void) {
    return repl_commit_connected();
}

void repl_commit_master_init(void) {
    if (!commit_active_mode()) return;
    (void)repl_commit_socket_path();
    g_epoch = ((uint64_t)getpid() << 32) ^ (uint64_t)kvs_now_ms();
}

/* 健康线程：重连 commit socket（proxy 可能刚启动/重启）。 */
void repl_commit_health_tick(void) {
    if (!commit_active_mode()) return;
    (void)repl_commit_socket_path();

    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0) {
        struct pollfd pfd = { .fd = g_commit_fd, .events = POLLIN };
        if (poll(&pfd, 1, 0) > 0 && (pfd.revents & (POLLHUP | POLLERR | POLLNVAL))) {
            close(g_commit_fd);
            g_commit_fd = -1;
            commit_set_fault("CHANNEL");
        }
    }
    if (g_commit_fd < 0) {
        int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd >= 0) {
            struct sockaddr_un sa;
            memset(&sa, 0, sizeof(sa));
            sa.sun_family = AF_UNIX;
            size_t plen = strlen(g_commit_path);
            if (plen >= sizeof(sa.sun_path)) plen = sizeof(sa.sun_path) - 1;
            memcpy(sa.sun_path, g_commit_path, plen);
            sa.sun_path[plen] = '\0';
            if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0) {
                g_commit_fd = fd;
                g_epoch = ((uint64_t)getpid() << 32) ^ (uint64_t)kvs_now_ms();
                repl_commit_msg_t hello;
                memset(&hello, 0, sizeof(hello));
                hello.magic = REPL_COMMIT_MAGIC;
                hello.version = REPL_COMMIT_VERSION;
                hello.type = REPL_COMMIT_MSG_HELLO;
                hello.epoch = g_epoch;
                if (commit_send_locked(&hello, sizeof(hello)) == 0) {
                    g_rearm_pending = 1;
                    g_rearm_start_ms = kvs_now_ms();
                    snprintf(g_fault_reason, sizeof(g_fault_reason), "NONE");
                }
            } else {
                close(fd);
            }
        }
    }
    pthread_mutex_unlock(&g_commit_lock);
}

/* owner 线程：proxy 重连后，等所有 client 处于命令边界 → BASELINE → 打开 capture。 */
int repl_commit_rearm_tick(void) {
    if (!g_rearm_pending) return 0;
    if (!commit_active_mode()) { g_rearm_pending = 0; return 0; }
    if (!repl_commit_connected()) return 0;

    long long now = kvs_now_ms();
    int partial = 0;
    for (conn_t *c = g_conn_head; c; c = c->next_all) {
        if (c->is_listener || c->is_replica) continue;
        if (c->in_len == 0) continue;
        if (now - g_rearm_start_ms > 5000) {
            /* 半包迟迟不完整：关闭该客户端，保证基准点是命令边界 */
            shutdown(c->fd, SHUT_RDWR);
        } else {
            partial = 1;
        }
    }
    if (partial) return 0;

    pthread_mutex_lock(&g_commit_lock);
    for (conn_t *c = g_conn_head; c; c = c->next_all) {
        if (c->is_listener || c->is_replica) continue;
        if (c->repl_socket_cookie == 0) continue;
        repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie,
                                .value = c->repl_parsed_seq + 1 };
        if (g_commit_fd >= 0) commit_send_msg(REPL_COMMIT_MSG_BASELINE, &kv, 0);
    }
    pthread_mutex_unlock(&g_commit_lock);

    repl_ebpf_set_capture_enable(1);
    g_rearm_pending = 0;
    fprintf(stderr, "repl commit: armed (baselines sent, capture enabled)\n");
    return 1;
}
