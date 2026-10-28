// ============================================================
// kvs_commit.c — Commit Gate（阶段3）Master 侧 + ebpf-proxy 生命周期
//
// 与 main 分支的 replication session/barrier 机制配合：
//   - session 建立时先发 BASELINE，再开 capture；
//   - 已提交写带 session_id 发布，proxy 只接受当前 session 的命令；
//   - session 结束/CACHE_INVALID 时 proxy 丢弃旧 cache 与 commit 状态。
// ============================================================
#include "kvstore/kvstore.h"
#include "kvstore/replication/commit_gate.h"
#include "kvstore/replication/client_ctl.h"

#include <sys/un.h>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <signal.h>
#include <strings.h>

#if KVS_ENABLE_EBPF
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#endif

extern char **environ;

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

/* ---------------- 通用 helpers ---------------- */
int kvs_socket_cookie(int fd, uint64_t *out) {
    socklen_t len = sizeof(*out);
    if (!out) return -1;
    *out = 0;
#ifdef SO_COOKIE
    return getsockopt(fd, SOL_SOCKET, SO_COOKIE, out, &len);
#else
    (void)fd; (void)len;
    return -1;
#endif
}

uint64_t kvs_cmd_crc64(const unsigned char *buf, size_t len) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < len; ++i) {
        h ^= (uint64_t)buf[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static volatile int g_capture_active = 0;
static volatile int g_capture_active_init = 0;

int repl_ebpf_capture_active(void) {
    if (!g_capture_active_init) {
        g_capture_active = (g_cfg.role == ROLE_MASTER &&
            strstr(g_cfg.repl_realtime_transport, "ebpf") != NULL) ? 1 : 0;
        g_capture_active_init = 1;
    }
    return g_capture_active;
}
void repl_ebpf_set_capture_active(int on) {
    g_capture_active_init = 1;
    g_capture_active = on ? 1 : 0;
}

static int ctl_open(void) {
#if KVS_ENABLE_EBPF
    if (!g_cfg.ebpf_pin_path[0]) return -1;
    char path[512];
    snprintf(path, sizeof(path), "%s/client_ctl", g_cfg.ebpf_pin_path);
    return bpf_obj_get(path);
#else
    return -1;
#endif
}

static int ctl_read_key(unsigned int key, unsigned long long *out) {
#if KVS_ENABLE_EBPF
    int fd = ctl_open();
    if (fd < 0) return -1;
    __u64 v = 0;
    int rc = bpf_map_lookup_elem(fd, &key, &v);
    close(fd);
    if (rc == 0 && out) *out = v;
    return rc;
#else
    (void)key; (void)out;
    return -1;
#endif
}

static int ctl_write_key(unsigned int key, unsigned long long val) {
    int fd = ctl_open();
#if KVS_ENABLE_EBPF
    if (fd < 0) return -1;
    __u64 v = val;
    int rc = bpf_map_update_elem(fd, &key, &v, BPF_ANY);
    close(fd);
    return rc;
#else
    (void)key; (void)fd;
    return -1;
#endif
}

void repl_ebpf_set_capture_enable(int enable) {
    ctl_write_key(KVS_CTL_CAPTURE_ENABLE, enable ? 1 : 0);
    repl_ebpf_set_capture_active(enable);
}

void repl_ebpf_cookie_allow(uint64_t cookie, int allow) {
    if (cookie == 0) return;
#if KVS_ENABLE_EBPF
    if (!g_cfg.ebpf_pin_path[0]) return;
    char path[512];
    snprintf(path, sizeof(path), "%s/client_cookies", g_cfg.ebpf_pin_path);
    int fd = bpf_obj_get(path);
    if (fd < 0) return;
    if (allow) {
        uint8_t one = 1;
        bpf_map_update_elem(fd, &cookie, &one, BPF_ANY);
    } else {
        bpf_map_delete_elem(fd, &cookie);
    }
    close(fd);
#else
    (void)allow;
#endif
}

/* ---------------- Commit Gate 状态 ---------------- */
#define COMMIT_SEND_POLL_MS 2

static int g_commit_fd = -1;
static char g_commit_path[512];
static int g_commit_path_ready = 0;
static uint64_t g_epoch = 0;
static uint64_t g_global_seq = 0;
static uint64_t g_commit_session = 0;
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

/* proxy 生命周期 */
static pid_t g_proxy_pid = -1;
static volatile int g_commit_reconnected = 0;   /* 通道重连成功，待 owner 线程补 BASELINE */
static int g_commit_ever_connected = 0;
static long long g_proxy_last_spawn_ms = 0;
static unsigned long long g_proxy_restart_count = 0;
static unsigned long long g_proxy_hb_last = 0;
static long long g_proxy_hb_last_ms = 0;
static int g_proxy_healthy = 0;
static int g_proxy_ever_healthy = 0;
static volatile int g_proxy_failure_pending = 0;
static volatile int g_health_stop = 0;
#define PROXY_HB_STALE_MS 2000
#define PROXY_RESPAWN_COOLDOWN_MS 5000

static int commit_active_mode(void) {
    return g_cfg.role == ROLE_MASTER &&
           strstr(g_cfg.repl_realtime_transport, "ebpf+tcp") != NULL;
}
int repl_commit_active_mode(void) { return commit_active_mode(); }

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

uint64_t repl_commit_global_seq(void) { return __atomic_load_n(&g_global_seq, __ATOMIC_RELAXED); }
unsigned long long repl_commit_descriptor_count(void) { return g_desc_count; }
unsigned long long repl_commit_write_count(void) { return g_write_count; }
unsigned long long repl_commit_skip_count(void) { return g_skip_count; }
unsigned long long repl_commit_desync_count(void) { return g_desync_count; }
unsigned long long repl_commit_timeout_count(void) { return g_timeout_count; }
unsigned long long repl_commit_gap_count(void) { return g_gap_count; }
const char *repl_commit_fault_reason_name(void) { return g_fault_reason; }
unsigned long long repl_ebpf_proxy_restart_count(void) { return g_proxy_restart_count; }

static void commit_set_fault(const char *reason) {
    snprintf(g_fault_reason, sizeof(g_fault_reason), "%s", reason ? reason : "UNKNOWN");
}
void repl_commit_note_proxy_fault(const char *reason) { commit_set_fault(reason); }

static int commit_send_locked(const void *buf, size_t len) {
    if (g_commit_fd < 0) {
        fprintf(stderr, "repl commit: send failed: no channel\n");
        return -1;
    }
    for (int attempt = 0; attempt < 2; ++attempt) {
        errno = 0;
        ssize_t w = send(g_commit_fd, buf, len, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (w == (ssize_t)len) return 0;
        if (w != (ssize_t)len) {
            fprintf(stderr, "repl commit: send attempt=%d len=%zu w=%zd errno=%d(%s)\n",
                    attempt, len, w, errno, strerror(errno));
        }
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
    h->epoch = g_commit_session;   /* BATCH 的 epoch 即 session id */
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
    if (g_commit_session == 0) return;
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
    d.session_id = g_commit_session;
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
    if (g_commit_session == 0) return;
    c->cur_cmd_committed = 1;
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd < 0) {
        pthread_mutex_unlock(&g_commit_lock);
        return;
    }
    repl_commit_desc_t d;
    memset(&d, 0, sizeof(d));
    d.cookie = c->repl_socket_cookie;
    d.session_id = g_commit_session;
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
                           uint64_t global_seq, uint64_t epoch,
                           uint64_t replica_id) {
    repl_commit_msg_t m;
    memset(&m, 0, sizeof(m));
    m.magic = REPL_COMMIT_MAGIC;
    m.version = REPL_COMMIT_VERSION;
    m.type = type;
    m.count = kv ? 1 : 0;
    m.epoch = epoch;
    m.global_seq = global_seq;
    m.replica_id = replica_id;
    unsigned char buf[sizeof(m) + sizeof(repl_commit_kv_t)];
    memcpy(buf, &m, sizeof(m));
    if (kv) memcpy(buf + sizeof(m), kv, sizeof(*kv));
    return commit_send_locked(buf, sizeof(m) + (kv ? sizeof(*kv) : 0));
}

void repl_commit_baseline(conn_t *c) {
    if (!commit_eligible(c)) return;
    /* BPF 只捕获 allowlist 中的客户端连接（排除副本控制/RDMA listener 等）。 */
    repl_ebpf_cookie_allow(c->repl_socket_cookie, 1);
    repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie,
                            .value = c->repl_parsed_seq + 1 };
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0)
        commit_send_msg(REPL_COMMIT_MSG_BASELINE, &kv, 0, g_commit_session, 0);
    pthread_mutex_unlock(&g_commit_lock);
}

void repl_commit_ignore(conn_t *c) {
    if (!c || c->repl_socket_cookie == 0) return;
    repl_ebpf_cookie_allow(c->repl_socket_cookie, 0);
    repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie, .value = 0 };
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0)
        commit_send_msg(REPL_COMMIT_MSG_IGNORE, &kv, 0, g_commit_session, 0);
    pthread_mutex_unlock(&g_commit_lock);
}

void repl_commit_forget(conn_t *c) {
    if (!c || c->repl_socket_cookie == 0) return;
    repl_ebpf_cookie_allow(c->repl_socket_cookie, 0);
    repl_commit_kv_t kv = { .cookie = c->repl_socket_cookie, .value = 0 };
    pthread_mutex_lock(&g_commit_lock);
    if (g_commit_fd >= 0)
        commit_send_msg(REPL_COMMIT_MSG_FORGET, &kv, 0, 0, 0);  /* 0=wildcard */
    pthread_mutex_unlock(&g_commit_lock);
}

/* 屏障放行时发送 RESUME：proxy 丢弃 recovery 缓冲中 global_seq <= marker 的提交
 * （已被快照/backlog replay 覆盖），并按 global_seq 放行其余记录。
 * 返回 -1 表示控制通道不可用/发送失败；调用方必须 fail-closed，不能继续 flush。 */
int repl_commit_resume(uint64_t replica_id, uint64_t global_seq) {
    if (!commit_active_mode() || g_commit_session == 0) return 0;
    pthread_mutex_lock(&g_commit_lock);
    int rc = 0;
    if (g_commit_fd < 0) {
        fprintf(stderr, "repl commit: RESUME(slot=%llu marker=%llu) not sent: channel fd<0 "
                        "(session=%llu)\n",
                (unsigned long long)replica_id,
                (unsigned long long)global_seq,
                (unsigned long long)g_commit_session);
        rc = -1;
    } else if (commit_send_msg(REPL_COMMIT_MSG_RESUME, NULL, global_seq,
                               g_commit_session, replica_id) != 0) {
        commit_set_fault("CHANNEL");
        rc = -1;
    }
    pthread_mutex_unlock(&g_commit_lock);
    return rc;
}

int repl_commit_connected(void) {
    pthread_mutex_lock(&g_commit_lock);
    int ok = g_commit_fd >= 0;
    pthread_mutex_unlock(&g_commit_lock);
    return ok;
}
int repl_commit_channel_ok(void) { return repl_commit_connected(); }

/* ---------------- replication session 钩子（owner 线程） ---------------- */
static void commit_send_all_baselines(void) {
    for (conn_t *c = g_conn_head; c; c = c->next_all) {
        if (c->is_listener || c->is_replica) continue;
        if (c->in_len > 0) {
            shutdown(c->fd, SHUT_RDWR);   /* 半包无法干净 framing，关闭重连 */
            continue;
        }
        repl_commit_baseline(c);
    }
}

void repl_commit_session_begin(unsigned long long sid) {
    g_commit_session = sid;
    repl_ebpf_set_capture_active(1);
    if (!commit_active_mode()) return;
    commit_send_all_baselines();
}

void repl_commit_session_end(void) {
    g_commit_session = 0;
    repl_ebpf_set_capture_active(0);
    pthread_mutex_lock(&g_commit_lock);
    g_batch_n = 0;
    pthread_mutex_unlock(&g_commit_lock);
}

/* ---------------- ebpf-proxy 生命周期 ---------------- */
static int proxy_heartbeat_live(void) {
    unsigned long long h1 = 0, h2 = 0;
    if (ctl_read_key(KVS_CTL_PROXY_HEARTBEAT, &h1) != 0) return 0;
    usleep(200000);
    if (ctl_read_key(KVS_CTL_PROXY_HEARTBEAT, &h2) != 0) return 0;
    return h2 != h1;
}

static void proxy_pidfile_path(char *out, size_t cap) {
    const char *base = g_cfg.dump_path[0] ? g_cfg.dump_path : "/tmp/kvstore";
    snprintf(out, cap, "%s.ebpfproxy.pid", base);
}

static pid_t proxy_read_pidfile(void) {
    char path[512];
    FILE *fp;
    int pid = -1;
    proxy_pidfile_path(path, sizeof(path));
    fp = fopen(path, "r");
    if (!fp) return -1;
    if (fscanf(fp, "%d", &pid) != 1) pid = -1;
    fclose(fp);
    return (pid_t)pid;
}

static int proxy_child_alive(void) {
    int status = 0;
    pid_t r;
    if (g_proxy_pid <= 0) return 0;
    r = waitpid(g_proxy_pid, &status, WNOHANG);
    if (r == g_proxy_pid) { g_proxy_pid = -1; return 0; }
    if (r == 0) return 1;
    if (r < 0 && errno == ECHILD) return kill(g_proxy_pid, 0) == 0;
    return 0;
}

static void proxy_kill(void) {
    pid_t pid = g_proxy_pid;
    if (pid <= 0) pid = proxy_read_pidfile();
    if (pid <= 0) return;
    if (kill(pid, 0) != 0) {
        if (pid == g_proxy_pid) g_proxy_pid = -1;
        return;
    }
    kill(pid, SIGKILL);
    if (pid == g_proxy_pid) {
        waitpid(pid, NULL, 0);
        g_proxy_pid = -1;
    }
}

static void proxy_spawn(void) {
    pid_t pid = -1;
    long long now = kvs_now_ms();
    if (!g_cfg.ebpf_proxy_bin[0] || !g_cfg.ebpf_client_capture_obj[0]) return;
    if (proxy_child_alive()) return;
    if (proxy_heartbeat_live()) return;
    if (now - g_proxy_last_spawn_ms < PROXY_RESPAWN_COOLDOWN_MS) return;

    char pidbuf[32], portbuf[32], pidfile[512];
    snprintf(pidbuf, sizeof(pidbuf), "%d", (int)getpid());
    snprintf(portbuf, sizeof(portbuf), "%d", g_cfg.port);
    proxy_pidfile_path(pidfile, sizeof(pidfile));

    char *argv[] = {
        (char *)g_cfg.ebpf_proxy_bin,
        (char *)"--pin-path", (char *)g_cfg.ebpf_pin_path,
        (char *)"--obj-path", (char *)g_cfg.ebpf_client_capture_obj,
        (char *)"--master-pid", pidbuf,
        (char *)"--master-port", portbuf,
        (char *)"--pid-file", pidfile,
        (char *)"--commit-sock", (char *)repl_commit_socket_path(),
        NULL
    };
    int rc = posix_spawn(&pid, g_cfg.ebpf_proxy_bin, NULL, NULL, argv, environ);
    g_proxy_last_spawn_ms = now;
    if (rc != 0) {
        fprintf(stderr, "repl: spawn ebpf-proxy failed (%s)\n", strerror(rc));
        return;
    }
    g_proxy_pid = pid;
    g_proxy_restart_count++;
    g_proxy_healthy = 0;
    fprintf(stderr, "repl: spawned ebpf-proxy pid=%d (restarts=%llu)\n",
            (int)pid, g_proxy_restart_count);
}

static void commit_health_tick(void) {
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
            size_t pl = strlen(g_commit_path);
            if (pl >= sizeof(sa.sun_path)) pl = sizeof(sa.sun_path) - 1;
            memcpy(sa.sun_path, g_commit_path, pl);
            sa.sun_path[pl] = '\0';
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
                    commit_set_fault("NONE");
                    g_commit_ever_connected = 1;
                    g_commit_reconnected = 1;   /* owner 线程补发 BASELINE */
                    fprintf(stderr, "repl commit: channel connected\n");
                }
            } else {
                close(fd);
            }
        }
    }
    pthread_mutex_unlock(&g_commit_lock);
}

int repl_commit_rearm_tick(void) {
    int reason = g_proxy_failure_pending;
    if (reason) {
        g_proxy_failure_pending = 0;
        repl_session_invalidate((repl_invalid_reason_t)reason);
        return 1;
    }

    /* 控制通道重连（proxy 重启/短暂断开）：在命令边界补发 BASELINE，
     * proxy 从该 seq 重新 framing；capture 无需关闭（framing 不会从半包重启）。 */
    if (g_commit_reconnected && g_commit_session != 0 && repl_commit_connected()) {
        g_commit_reconnected = 0;
        commit_send_all_baselines();
        fprintf(stderr, "repl commit: re-armed baselines after channel reconnect "
                        "(session=%llu)\n", (unsigned long long)g_commit_session);
        return 1;
    }
    return 0;
}

static void *proxy_health_thread(void *arg) {
    (void)arg;
    while (!g_health_stop) {
        commit_health_tick();
        if (commit_active_mode()) {
            unsigned long long hb = 0;
            int ok = (ctl_read_key(KVS_CTL_PROXY_HEARTBEAT, &hb) == 0);
            long long now = kvs_now_ms();
            if (ok && hb != g_proxy_hb_last) {
                g_proxy_hb_last = hb;
                g_proxy_hb_last_ms = now;
            }
            int healthy = ok && g_proxy_hb_last_ms > 0 &&
                          (now - g_proxy_hb_last_ms <= PROXY_HB_STALE_MS);
            if (healthy) {
                g_proxy_healthy = 1;
                g_proxy_ever_healthy = 1;
                /* Commit Gate 失效信号（BPF 截断 / proxy 侧 desync/timeout/gap/通道断）：
                 * fail-closed，失效当前复制会话，由 Slave 重连后重建。 */
                unsigned long long fault = 0;
                if (ctl_read_key(KVS_CTL_CAPTURE_FAULT, &fault) == 0 && fault) {
                    ctl_write_key(KVS_CTL_CAPTURE_FAULT, 0);
                    if (g_commit_session != 0)
                        g_proxy_failure_pending = REPL_INVALID_CAPTURE_TRUNCATED;
                    fprintf(stderr, "repl: capture truncated → session invalidated\n");
                }
                fault = 0;
                if (ctl_read_key(KVS_CTL_COMMIT_FAULT, &fault) == 0 && fault) {
                    ctl_write_key(KVS_CTL_COMMIT_FAULT, 0);
                    if (g_commit_session != 0) {
                        repl_invalid_reason_t r = REPL_INVALID_COMMIT_TIMEOUT;
                        if (fault == 2) r = REPL_INVALID_COMMIT_DESYNC;
                        else if (fault == 3) r = REPL_INVALID_COMMIT_GAP;
                        else if (fault == 4) r = REPL_INVALID_COMMIT_CHANNEL;
                        else if (fault == 5) r = REPL_INVALID_PROXY_CACHE_FULL;
                        g_proxy_failure_pending = (int)r;
                    }
                    fprintf(stderr, "repl: commit gate fault code=%llu → session invalidated\n",
                            (unsigned long long)fault);
                }
                /* 控制通道断开：capture 仍在跑但已无法发布提交描述符，
                 * proxy 侧 PENDING 会超时；直接失效会话（fail-closed）。 */
                if (g_commit_ever_connected && !repl_commit_connected() &&
                    g_commit_session != 0)
                    g_proxy_failure_pending = REPL_INVALID_COMMIT_CHANNEL;
            } else {
                g_proxy_healthy = 0;
                if (g_proxy_ever_healthy && g_commit_session != 0)
                    g_proxy_failure_pending = REPL_INVALID_PROXY_HEARTBEAT_STALE;
                if (now - g_proxy_last_spawn_ms >= PROXY_RESPAWN_COOLDOWN_MS) {
                    proxy_kill();
                    proxy_spawn();
                }
            }
        }
        usleep(100000);
    }
    return NULL;
}

int repl_ebpf_proxy_healthy(void) {
    if (!commit_active_mode()) return 1;
    return g_proxy_healthy;
}

void repl_ebpf_proxy_start(void) {
    if (!commit_active_mode()) return;
    (void)repl_commit_socket_path();
    g_epoch = ((uint64_t)getpid() << 32) ^ (uint64_t)kvs_now_ms();
    proxy_spawn();
}

void repl_ebpf_health_start(void) {
    pthread_t tid;
    if (!commit_active_mode()) return;
    /* Master 启动时已由 spawn_ebpf_proxy() 拉起 proxy；给首次启动留出加载 BPF/
     * 心跳的窗口，避免健康线程把"尚未开始心跳"误判为死亡而重复 spawn。 */
    if (g_proxy_last_spawn_ms == 0) g_proxy_last_spawn_ms = kvs_now_ms();
    if (pthread_create(&tid, NULL, proxy_health_thread, NULL) != 0) {
        fprintf(stderr, "repl: failed to start ebpf-proxy health thread\n");
        return;
    }
    pthread_detach(tid);
}

/* master 启动已由 src/main/kvstore.c 的 spawn_ebpf_proxy() 负责（含陈旧 pin 清理）；
 * 这里只初始化 commit socket 路径与 epoch，避免重复 spawn 两个 proxy。 */
void repl_commit_master_init(void) {
    if (!commit_active_mode()) return;
    (void)repl_commit_socket_path();
    g_epoch = ((uint64_t)getpid() << 32) ^ (uint64_t)kvs_now_ms();
}
void repl_commit_health_tick(void) { commit_health_tick(); }

int repl_ebpf_proxy_wait_buffering(int timeout_ms) {
    unsigned long long st = 0;
    int waited = 0;
    if (timeout_ms < 0) timeout_ms = 0;
    while (waited <= timeout_ms) {
        if (ctl_read_key(KVS_CTL_PROXY_STATE, &st) == 0 && st == 1) return 1;
        usleep(2000);
        waited += 2;
    }
    return 0;
}
