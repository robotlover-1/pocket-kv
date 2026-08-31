#include "kvstore/kvstore.h"
#include <strings.h>

#if KVS_ENABLE_EBPF
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#endif

static int g_repl_ebpf_initialized = 0;
static unsigned long long g_repl_ebpf_register_attempts = 0;
static unsigned long long g_repl_ebpf_register_failures = 0;
static int g_repl_ebpf_last_errno = 0;
static char g_repl_ebpf_last_error[128] = "none";

static void repl_ebpf_set_error(const char *stage, int err) {
    g_repl_ebpf_last_errno = err;
    snprintf(g_repl_ebpf_last_error, sizeof(g_repl_ebpf_last_error), "%s", stage ? stage : "unknown");
}

static void repl_ebpf_clear_error(void) {
    repl_ebpf_set_error("none", 0);
}

#if KVS_ENABLE_EBPF
static struct bpf_object *g_repl_ebpf_obj = NULL;
static int g_repl_ebpf_sock_map_fd = -1;
static int g_repl_ebpf_role_map_fd = -1;
static int g_repl_ebpf_stats_map_fd = -1;
static int g_repl_ebpf_control_map_fd = -1;
static int g_repl_ebpf_uses_pinned_maps = 0;
static int g_repl_ebpf_print_initialized = 0;
static int g_repl_ebpf_backpressure_fd = -1;
static int g_repl_ebpf_backpressure_enabled = -1;   /* -1=未初始化, 0=off, 1=on */
static __u64 g_repl_ebpf_last_hb = 0;
static long long g_repl_ebpf_last_hb_ms = 0;
static long long g_repl_ebpf_bp_check_ms = 0;   /* 上次实际查询时间 */
static int g_repl_ebpf_bp_cached = 0;           /* 缓存结果：背压以 ms 计变化，逐 read 查询浪费 syscall */
#define BACKPRESSURE_STALE_MS 2000   /* proxy 心跳过期阈值：视为 proxy 退出，恢复读取 */

static int repl_ebpf_libbpf_print(enum libbpf_print_level level, const char *format, va_list args) {
    /* 只打印 WARN 和 ERROR 级别，关闭 DEBUG/INFO 噪声 */
    if (level == LIBBPF_DEBUG || level == LIBBPF_INFO)
        return 0;
    return vfprintf(stderr, format, args);
}

static void repl_ebpf_enable_libbpf_log(void) {
    if (!g_repl_ebpf_print_initialized) {
        libbpf_set_print(repl_ebpf_libbpf_print);
        g_repl_ebpf_print_initialized = 1;
    }
}

static void repl_ebpf_clear_fds(void) {
    if (g_repl_ebpf_uses_pinned_maps) {
        if (g_repl_ebpf_sock_map_fd >= 0) close(g_repl_ebpf_sock_map_fd);
        if (g_repl_ebpf_role_map_fd >= 0) close(g_repl_ebpf_role_map_fd);
        if (g_repl_ebpf_stats_map_fd >= 0) close(g_repl_ebpf_stats_map_fd);
        if (g_repl_ebpf_control_map_fd >= 0) close(g_repl_ebpf_control_map_fd);
    }
    g_repl_ebpf_sock_map_fd = -1;
    g_repl_ebpf_role_map_fd = -1;
    g_repl_ebpf_stats_map_fd = -1;
    g_repl_ebpf_control_map_fd = -1;
    g_repl_ebpf_uses_pinned_maps = 0;
}

static int repl_ebpf_socket_cookie(int fd, __u64 *cookie_out) {
    socklen_t len = sizeof(*cookie_out);
    if (!cookie_out) return -1;
    *cookie_out = 0;
#ifdef SO_COOKIE
    return getsockopt(fd, SOL_SOCKET, SO_COOKIE, cookie_out, &len);
#else
    (void)fd;
    (void)len;
    return -1;
#endif
}

static void repl_ebpf_raise_memlock(void) {
    struct rlimit rlim;
    rlim.rlim_cur = RLIM_INFINITY;
    rlim.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_MEMLOCK, &rlim) != 0) {
        repl_ebpf_set_error("raise_memlock", errno);
    }
}

static int repl_ebpf_pin_path(char *out, size_t out_cap, const char *name) {
    if (!out || out_cap == 0 || !name || !*name || !g_cfg.ebpf_pin_path[0]) return -1;
    snprintf(out, out_cap, "%s/%s", g_cfg.ebpf_pin_path, name);
    return 0;
}

static int repl_ebpf_pin_map_fd(int fd, const char *name) {
    char path[512];
    if (fd < 0 || repl_ebpf_pin_path(path, sizeof(path), name) != 0) return -1;
    unlink(path);
    if (bpf_obj_pin(fd, path) != 0) return -1;
    return 0;
}

static int repl_ebpf_open_pinned_map(const char *name) {
    char path[512];
    if (repl_ebpf_pin_path(path, sizeof(path), name) != 0) return -1;
    return bpf_obj_get(path);
}

static int repl_ebpf_open_pinned_maps(void) {
    int sock_fd;
    int stats_fd;
    int control_fd;
    if (!g_cfg.ebpf_pin_path[0]) return -1;
    sock_fd = repl_ebpf_open_pinned_map("sock_map");
    stats_fd = repl_ebpf_open_pinned_map("stats_map");
    control_fd = repl_ebpf_open_pinned_map("control_map");
    if (sock_fd < 0 || stats_fd < 0 || control_fd < 0) {
        if (sock_fd >= 0) close(sock_fd);
        if (stats_fd >= 0) close(stats_fd);
        if (control_fd >= 0) close(control_fd);
        return -1;
    }
    g_repl_ebpf_sock_map_fd = sock_fd;
    g_repl_ebpf_stats_map_fd = stats_fd;
    g_repl_ebpf_control_map_fd = control_fd;
    g_repl_ebpf_role_map_fd = -1;
    g_repl_ebpf_uses_pinned_maps = 1;
    return 0;
}

static int repl_ebpf_pin_loaded_maps(void) {
    if (!g_cfg.ebpf_pin_path[0]) return 0;
    if (mkdir(g_cfg.ebpf_pin_path, 0755) != 0 && errno != EEXIST) return -1;
    if (repl_ebpf_pin_map_fd(g_repl_ebpf_sock_map_fd, "sock_map") != 0) return -1;
    if (repl_ebpf_pin_map_fd(g_repl_ebpf_stats_map_fd, "stats_map") != 0) return -1;
    if (repl_ebpf_pin_map_fd(g_repl_ebpf_control_map_fd, "control_map") != 0) return -1;
    return 0;
}

static int repl_ebpf_update_control(void) {
    __u32 key = 0;
    __u32 value = g_cfg.ebpf_redirect ? 1u : 0u;
    if (bpf_map_update_elem(g_repl_ebpf_control_map_fd, &key, &value, BPF_ANY) != 0) return -1;
    key = 1;
    value = g_cfg.ebpf_redirect_key >= 0 ? (unsigned int)g_cfg.ebpf_redirect_key : (g_cfg.ebpf_redirect ? 1u : 0u);
    if (bpf_map_update_elem(g_repl_ebpf_control_map_fd, &key, &value, BPF_ANY) != 0) return -1;
    key = 2;
    value = g_cfg.role == ROLE_SLAVE ? (unsigned int)g_cfg.master_port : (unsigned int)g_cfg.port;
    if (bpf_map_update_elem(g_repl_ebpf_control_map_fd, &key, &value, BPF_ANY) != 0) return -1;
    key = 3;
    value = (unsigned int)g_cfg.port;
    if (bpf_map_update_elem(g_repl_ebpf_control_map_fd, &key, &value, BPF_ANY) != 0) return -1;
    key = 4;
    /* KVS_EBPF_CTL_REDIRECT_INGRESS:
     *   1 = BPF_F_INGRESS (local redirect to receive queue)
     *   0 = no BPF_F_INGRESS (egress redirect for cross-machine forwarding) */
    value = g_cfg.ebpf_forward ? 0u : (g_cfg.ebpf_redirect ? 1u : 0u);
    if (bpf_map_update_elem(g_repl_ebpf_control_map_fd, &key, &value, BPF_ANY) != 0) return -1;
    return 0;
}

static int repl_ebpf_load_object(void) {
    struct bpf_program *sk_msg_prog;
    int sk_msg_fd;
    int rc;
    if (g_repl_ebpf_obj) return 0;

    /* 优先尝试连接已存在的 pinned maps（由独立 eBPF 守护进程挂载）
     * 这不需要 ebpf_obj_path，因此放在 obj_path 检查之前 */
    if (g_cfg.ebpf_pin_path[0]) {
        int opened = 0;
        for (int i = 0; i < 50; ++i) {
            if (repl_ebpf_open_pinned_maps() == 0) {
                opened = 1;
                break;
            }
            usleep(100000);
        }
        if (opened) {
            if (repl_ebpf_update_control() != 0) {
                repl_ebpf_set_error("update_pinned_control", errno);
                repl_ebpf_clear_fds();
                return -1;
            }
            repl_ebpf_clear_error();
            return 0;
        }
        /* pinned maps 不可用，继续尝试直接加载 */
        repl_ebpf_set_error("open_pinned_maps", errno ? errno : ENOENT);
    }

    if (!g_cfg.ebpf_obj_path[0]) {
        repl_ebpf_set_error("missing_obj_path", EINVAL);
        return -1;
    }
    repl_ebpf_raise_memlock();
    repl_ebpf_enable_libbpf_log();
    g_repl_ebpf_obj = bpf_object__open_file(g_cfg.ebpf_obj_path, NULL);
    if (libbpf_get_error(g_repl_ebpf_obj)) {
        repl_ebpf_set_error("open_object", (int)-libbpf_get_error(g_repl_ebpf_obj));
        g_repl_ebpf_obj = NULL;
        return -1;
    }
    rc = bpf_object__load(g_repl_ebpf_obj);
    if (rc != 0) {
        repl_ebpf_set_error("load_object", rc < 0 ? -rc : errno);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        return -1;
    }
    g_repl_ebpf_sock_map_fd = bpf_object__find_map_fd_by_name(g_repl_ebpf_obj, "sock_map");
    g_repl_ebpf_role_map_fd = bpf_object__find_map_fd_by_name(g_repl_ebpf_obj, "role_map");
    g_repl_ebpf_stats_map_fd = bpf_object__find_map_fd_by_name(g_repl_ebpf_obj, "stats_map");
    g_repl_ebpf_control_map_fd = bpf_object__find_map_fd_by_name(g_repl_ebpf_obj, "control_map");
    if (g_repl_ebpf_sock_map_fd < 0 || g_repl_ebpf_stats_map_fd < 0 || g_repl_ebpf_control_map_fd < 0) {
        repl_ebpf_set_error("find_maps", ENOENT);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        repl_ebpf_clear_fds();
        return -1;
    }
    sk_msg_prog = bpf_object__find_program_by_name(g_repl_ebpf_obj, "kvstore_repl_sk_msg");
    if (!sk_msg_prog) {
        repl_ebpf_set_error("find_sk_msg_program", ENOENT);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        repl_ebpf_clear_fds();
        return -1;
    }
    sk_msg_fd = bpf_program__fd(sk_msg_prog);
    if (sk_msg_fd < 0) {
        repl_ebpf_set_error("sk_msg_program_fd", EBADF);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        repl_ebpf_clear_fds();
        return -1;
    }
    if (bpf_prog_attach(sk_msg_fd, g_repl_ebpf_sock_map_fd, BPF_SK_MSG_VERDICT, 0) != 0) {
        repl_ebpf_set_error("attach_sk_msg", errno);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        repl_ebpf_clear_fds();
        return -1;
    }
    if (repl_ebpf_pin_loaded_maps() != 0) {
        repl_ebpf_set_error("pin_maps", errno);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        repl_ebpf_clear_fds();
        return -1;
    }
    if (repl_ebpf_update_control() != 0) {
        repl_ebpf_set_error("update_control", errno);
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
        repl_ebpf_clear_fds();
        return -1;
    }
    repl_ebpf_clear_error();
    return 0;
}
#endif

int repl_ebpf_init(void) {
#if KVS_ENABLE_EBPF
    if (repl_ebpf_load_object() != 0) return -1;
#endif
    g_repl_ebpf_initialized = 1;
#if !KVS_ENABLE_EBPF
    repl_ebpf_clear_error();
#endif
    return 0;
}

void repl_ebpf_cleanup(void) {
#if KVS_ENABLE_EBPF
    if (g_repl_ebpf_obj) {
        bpf_object__close(g_repl_ebpf_obj);
        g_repl_ebpf_obj = NULL;
    }
    if (g_repl_ebpf_backpressure_fd >= 0) {
        close(g_repl_ebpf_backpressure_fd);
        g_repl_ebpf_backpressure_fd = -1;
    }
    repl_ebpf_clear_fds();
#endif
    g_repl_ebpf_initialized = 0;
}

int repl_ebpf_supported(void) {
#if KVS_ENABLE_EBPF
    return 1;
#else
    return 1;
#endif
}

int repl_ebpf_register_fd(int fd, int is_master_side) {
#if !KVS_ENABLE_EBPF
    (void)is_master_side;
#endif
    g_repl_ebpf_register_attempts++;
    if (fd < 0) {
        repl_ebpf_set_error("register_invalid_fd", EBADF);
        g_repl_ebpf_register_failures++;
        return -1;
    }
    if (!g_repl_ebpf_initialized && repl_ebpf_init() != 0) {
        g_repl_ebpf_register_failures++;
        return -1;
    }
#if KVS_ENABLE_EBPF
    {
        int role_key = is_master_side ? 0 : 1;
        __u64 cookie = 0;
        __u32 role = is_master_side ? 1u : 2u;
        if (g_repl_ebpf_sock_map_fd < 0 || bpf_map_update_elem(g_repl_ebpf_sock_map_fd, &role_key, &fd, BPF_ANY) != 0) {
            repl_ebpf_set_error(is_master_side ? "register_sock_map_master_key" : "register_sock_map_slave_key", errno ? errno : EBADF);
            g_repl_ebpf_register_failures++;
            return -1;
        }
        if (g_repl_ebpf_role_map_fd >= 0 && repl_ebpf_socket_cookie(fd, &cookie) == 0 && cookie != 0) {
            bpf_map_update_elem(g_repl_ebpf_role_map_fd, &cookie, &role, BPF_ANY);
        }
    }
#endif
    return 0;
}

int repl_ebpf_register_forward_fd(int fd) {
    /* Register the slave TCP connection fd at the redirect key in the sockmap,
     * so the BPF program can redirect replication data to this socket for
     * cross-machine transmission via TCP.
     *
     * The redirect key is read from config (ebpf_redirect_key), defaulting to 1.
     * After registration, the BPF sk_msg program will:
     *   - intercept the sendmsg on the master side
     *   - bpf_msg_redirect_map(msg, &sock_map, redirect_key, 0)
     *   - data goes to this socket's send queue and out over TCP to the remote slave
     */
#if !KVS_ENABLE_EBPF
    (void)fd;
    return -1;
#else
    int redirect_key;
    if (fd < 0) return -1;
    if (!g_repl_ebpf_initialized && repl_ebpf_init() != 0) return -1;
    if (g_repl_ebpf_sock_map_fd < 0) return -1;
    redirect_key = g_cfg.ebpf_redirect_key >= 0 ? g_cfg.ebpf_redirect_key : 1;
    if (bpf_map_update_elem(g_repl_ebpf_sock_map_fd, &redirect_key, &fd, BPF_ANY) != 0) {
        repl_ebpf_set_error("register_forward_key", errno ? errno : EBADF);
        return -1;
    }
    return 0;
#endif
}

int repl_ebpf_unregister_fd(int fd) {
    if (fd < 0) return 0;
#if KVS_ENABLE_EBPF
    if (g_repl_ebpf_sock_map_fd >= 0) {
        int key0 = 0;
        int key1 = 1;
        bpf_map_delete_elem(g_repl_ebpf_sock_map_fd, &fd);
        bpf_map_delete_elem(g_repl_ebpf_sock_map_fd, &key0);
        bpf_map_delete_elem(g_repl_ebpf_sock_map_fd, &key1);
    }
    if (g_repl_ebpf_role_map_fd >= 0) {
        __u64 cookie = 0;
        if (repl_ebpf_socket_cookie(fd, &cookie) == 0 && cookie != 0) bpf_map_delete_elem(g_repl_ebpf_role_map_fd, &cookie);
    }
#endif
    return 0;
}

/* 无损背压查询：ebpf+tcp（proxy 转发）模式下，proxy 的转发队列近满时会通过
 * pinned client_ctl map key=4 置位；master 在每次 recv 前查询，置位则停手等待，
 * 把 ringbuf 溢出导致的 fexit 静默丢数据转为 TCP 背压（无损）。
 * 返回 1=应停手，0=可继续。 */
int repl_ebpf_backpressure(void) {
#if KVS_ENABLE_EBPF
    if (g_repl_ebpf_backpressure_enabled < 0) {
        g_repl_ebpf_backpressure_enabled =
            (strcasecmp(repl_realtime_transport_name(), "ebpf+tcp") == 0) ? 1 : 0;
    }
    if (!g_repl_ebpf_backpressure_enabled) return 0;
    /* 2ms 缓存：背压以 ms 计变化，逐 read 查询浪费 syscall（高 P 下 read 率 ~10^5/s） */
    long long now_ms = kvs_now_ms();
    if (now_ms - g_repl_ebpf_bp_check_ms < 2)
        return g_repl_ebpf_bp_cached;
    g_repl_ebpf_bp_check_ms = now_ms;
    if (g_repl_ebpf_backpressure_fd < 0) {
        char path[512];
        if (!g_cfg.ebpf_pin_path[0]) { g_repl_ebpf_bp_cached = 0; return 0; }
        snprintf(path, sizeof(path), "%s/client_ctl", g_cfg.ebpf_pin_path);
        int fd = bpf_obj_get(path);
        if (fd < 0) { g_repl_ebpf_bp_cached = 0; return 0; }   /* ebpf-proxy 未启动：无捕获，无需背压 */
        g_repl_ebpf_backpressure_fd = fd;
    }
    __u32 key = 4;   /* KVS_EBPF_CLIENT_CTL_BACKPRESSURE */
    __u64 val = 0;
    if (bpf_map_lookup_elem(g_repl_ebpf_backpressure_fd, &key, &val) != 0) {
        g_repl_ebpf_bp_cached = 0;
        return 0;
    }
    if (val == 0) {
        g_repl_ebpf_bp_cached = 0;
        return 0;
    }
    /* 背压标志置位：校验 proxy 心跳（client_ctl[5]）新鲜度，防止 proxy 崩溃/重启后
     * 标志陈旧导致 master 永久挂起。心跳在 BACKPRESSURE_STALE_MS 内未推进则视为
     * proxy 已退出，恢复读取（数据将不转发，但不会死锁；由监控/重启兜底）。 */
    __u32 hb_key = 5;
    __u64 hb = 0;
    if (bpf_map_lookup_elem(g_repl_ebpf_backpressure_fd, &hb_key, &hb) != 0) {
        g_repl_ebpf_bp_cached = 0;
        return 0;
    }
    if (hb != g_repl_ebpf_last_hb) {
        g_repl_ebpf_last_hb = hb;
        g_repl_ebpf_last_hb_ms = now_ms;
    }
    if (now_ms - g_repl_ebpf_last_hb_ms > BACKPRESSURE_STALE_MS) {
        static int warned = 0;
        if (!warned) {
            warned = 1;
            fprintf(stderr, "repl: ebpf-proxy heartbeat stale %lldms — "
                    "backpressure cleared (proxy likely dead)\n",
                    now_ms - g_repl_ebpf_last_hb_ms);
        }
        g_repl_ebpf_bp_cached = 0;
        return 0;
    }
    g_repl_ebpf_bp_cached = 1;
    return 1;
#else
    return 0;
#endif
}

int repl_ebpf_get_stats(kvs_repl_ebpf_stats_t *stats) {
    if (!stats) return -1;
    memset(stats, 0, sizeof(*stats));
    stats->initialized = (unsigned long long)g_repl_ebpf_initialized;
    stats->compiled = (unsigned long long)KVS_ENABLE_EBPF;
    stats->register_attempts = g_repl_ebpf_register_attempts;
    stats->register_failures = g_repl_ebpf_register_failures;
    stats->last_errno = g_repl_ebpf_last_errno;
    snprintf(stats->last_error, sizeof(stats->last_error), "%s", g_repl_ebpf_last_error);
    stats->redirect_enabled = (unsigned long long)g_cfg.ebpf_redirect;
    stats->forward_enabled = (unsigned long long)g_cfg.ebpf_forward;
#if KVS_ENABLE_EBPF
    if (g_repl_ebpf_stats_map_fd >= 0) {
        __u32 key;
        __u64 value = 0;
        key = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->sk_msg_count = value;
        key = 1;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->sk_msg_bytes = value;
        key = 2;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->sk_msg_pass = value;
        key = 3;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->sk_msg_drop = value;
        key = 4;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->redirect_attempts = value;
        key = 5;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->redirect_success = value;
        key = 6;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->redirect_failures = value;
        key = 7;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->role_unknown = value;
        key = 8;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->role_master = value;
        key = 9;
        value = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_stats_map_fd, &key, &value) == 0) stats->role_slave = value;
    }
#endif
    return 0;
}
