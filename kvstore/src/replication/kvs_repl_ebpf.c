#include "kvstore/kvstore.h"
#include <strings.h>
#include <spawn.h>
#include <sys/wait.h>
#include <signal.h>

#if KVS_ENABLE_EBPF
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#endif

extern char **environ;

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

/* capture 数据面开关（进程内缓存）：master + ebpf transport 时默认开启；
 * proxy 故障/截断/重同步期间由健康检查/会话失效路径调用 setter 关掉。 */
static volatile int g_repl_ebpf_capture_active = 0;
static volatile int g_repl_ebpf_capture_active_init = 0;

int repl_ebpf_capture_active(void) {
    if (!g_repl_ebpf_capture_active_init) {
        g_repl_ebpf_capture_active =
            (g_cfg.role == ROLE_MASTER &&
             strstr(g_cfg.repl_realtime_transport, "ebpf") != NULL) ? 1 : 0;
        g_repl_ebpf_capture_active_init = 1;
    }
    return g_repl_ebpf_capture_active;
}

void repl_ebpf_set_capture_active(int on) {
    g_repl_ebpf_capture_active_init = 1;
    g_repl_ebpf_capture_active = on ? 1 : 0;
}

/* SO_COOKIE：Commit Gate 用 socket cookie 关联 Master 命令与 BPF 捕获记录。
 * 与 BPF 的 bpf_get_socket_cookie(sk) 返回同一个值。 */
int kvs_socket_cookie(int fd, uint64_t *out) {
    socklen_t len = sizeof(*out);
    if (!out) return -1;
    *out = 0;
#ifdef SO_COOKIE
    return getsockopt(fd, SOL_SOCKET, SO_COOKIE, out, &len);
#else
    (void)fd;
    (void)len;
    return -1;
#endif
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

uint64_t kvs_cmd_crc64(const unsigned char *buf, size_t len) {
    uint64_t h = 1469598103934665603ULL;   /* FNV-1a 64 offset basis */
    for (size_t i = 0; i < len; ++i) {
        h ^= (uint64_t)buf[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* 同步写 BPF capture gate（client_ctl[7]，1=开启）并更新进程内状态。
 * REPLSYNC/失效路径需要立即生效，不能等 100ms 健康线程。 */
void repl_ebpf_set_capture_enable(int enable) {
    unsigned int key = 7;   /* CTL_KEY_CAPTURE_ENABLE */
    unsigned long long val = enable ? 1 : 0;
    int fd = -1;
    /* 顺序必须保证 recv clamp 先于 BPF 打开捕获生效（关闭时晚于 BPF 停止）：
     * 否则 map 写 1 到本地 active=1 之间的窗口内，BPF 可能已捕获一次
     * >32744 的 recv，触发 CAPTURE_TRUNCATED fail-closed。 */
    if (enable) repl_ebpf_set_capture_active(1);
#if KVS_ENABLE_EBPF
    if (g_cfg.ebpf_pin_path[0]) {
        char path[512];
        snprintf(path, sizeof(path), "%s/client_ctl", g_cfg.ebpf_pin_path);
        fd = bpf_obj_get(path);
        if (fd >= 0) {
            bpf_map_update_elem(fd, &key, &val, BPF_ANY);
            close(fd);
        }
    }
#else
    (void)key; (void)val; (void)fd;
#endif
    if (!enable) repl_ebpf_set_capture_active(0);
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
    return kvs_socket_cookie(fd, (uint64_t *)cookie_out);
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
    /* 背压键：4=ringbuf、6=proxy 转发队列、11=cache 高水位（任一置位即停读） */
    static const __u32 bp_keys[] = { 4, 6, 11 };
    int any = 0;
    for (size_t bi = 0; bi < sizeof(bp_keys) / sizeof(bp_keys[0]); ++bi) {
        __u64 val = 0;
        if (bpf_map_lookup_elem(g_repl_ebpf_backpressure_fd, &bp_keys[bi], &val) == 0 && val) {
            any = 1;
            break;
        }
    }
    if (!any) {
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

/* ============================================================
 * ebpf-proxy 生命周期与健康检查（阶段2）
 *
 * client_ctl 键（与 repl_client_capture.bpf.c 一致）：
 *   1=master pid  2=master port  3=fullsync  4=ringbuf 背压  5=heartbeat
 *   6=转发队列背压  7=capture_disable  8=capture_fault
 * capture_disable=1 时 BPF 停止捕获；proxy 观察同一键后丢弃已捕获数据。
 * 心跳每 100ms 推进一次，2s 不推进即判定 proxy 失活。
 * ============================================================ */
#define EBPF_CTL_KEY_MASTER_PID        1
#define EBPF_CTL_KEY_MASTER_PORT       2
#define EBPF_CTL_KEY_HEARTBEAT         5
#define EBPF_CTL_KEY_CAPTURE_ENABLE    7   /* 1=允许捕获（默认 0） */
#define EBPF_CTL_KEY_CAPTURE_FAULT     8
#define EBPF_CTL_KEY_COMMIT_FAULT      10  /* proxy 上报 commit gate 故障原因 */
#define EBPF_CLIENT_STATS_EVENTS       6   /* ST_RB_OK */
#define EBPF_CLIENT_STATS_BYTES        9   /* ST_BYTES */
#define EBPF_CLIENT_STATS_TRUNCATED    8   /* ST_TRUNCATED */
#define PROXY_HEARTBEAT_STALE_MS       2000
#define PROXY_RESPAWN_COOLDOWN_MS      5000

static pid_t g_ebpf_proxy_pid = -1;
static long long g_ebpf_proxy_last_spawn_ms = 0;
static unsigned long long g_ebpf_proxy_restart_count = 0;
static unsigned long long g_ebpf_hb_last = 0;
static long long g_ebpf_hb_last_ms = 0;
static int g_ebpf_proxy_healthy = 0;
static int g_ebpf_proxy_ever_healthy = 0;
static int g_ebpf_failure_handled = 0;
static volatile int g_ebpf_health_stop = 0;

static int repl_ebpf_is_capture_transport(void) {
    return g_cfg.role == ROLE_MASTER &&
           strstr(g_cfg.repl_realtime_transport, "ebpf") != NULL;
}

static int ebpf_ctl_open(void) {
#if KVS_ENABLE_EBPF
    char path[512];
    if (!g_cfg.ebpf_pin_path[0]) return -1;
    snprintf(path, sizeof(path), "%s/client_ctl", g_cfg.ebpf_pin_path);
    return bpf_obj_get(path);
#else
    return -1;
#endif
}

static int ebpf_ctl_read(unsigned int key, unsigned long long *out) {
#if KVS_ENABLE_EBPF
    int fd = ebpf_ctl_open();
    if (fd < 0) return -1;
    int rc = bpf_map_lookup_elem(fd, &key, out);
    close(fd);
    return rc;
#else
    (void)key; (void)out;
    return -1;
#endif
}

static int ebpf_ctl_write(unsigned int key, unsigned long long val) {
#if KVS_ENABLE_EBPF
    int fd = ebpf_ctl_open();
    if (fd < 0) return -1;
    int rc = bpf_map_update_elem(fd, &key, &val, BPF_ANY);
    close(fd);
    return rc;
#else
    (void)key; (void)val;
    return -1;
#endif
}

static void repl_ebpf_reset_backpressure_cache(void) {
#if KVS_ENABLE_EBPF
    if (g_repl_ebpf_backpressure_fd >= 0) {
        close(g_repl_ebpf_backpressure_fd);
        g_repl_ebpf_backpressure_fd = -1;
    }
    g_repl_ebpf_backpressure_enabled = -1;
    g_repl_ebpf_last_hb = 0;
    g_repl_ebpf_last_hb_ms = 0;
    g_repl_ebpf_bp_check_ms = 0;
    g_repl_ebpf_bp_cached = 0;
#endif
}

static void ebpf_proxy_pidfile_path(char *out, size_t cap) {
    const char *base = g_cfg.dump_path[0] ? g_cfg.dump_path : "/tmp/kvstore";
    snprintf(out, cap, "%s.ebpfproxy.pid", base);
}

static pid_t ebpf_proxy_read_pidfile(void) {
    char path[512];
    FILE *fp;
    int pid = -1;
    ebpf_proxy_pidfile_path(path, sizeof(path));
    fp = fopen(path, "r");
    if (!fp) return -1;
    if (fscanf(fp, "%d", &pid) != 1) pid = -1;
    fclose(fp);
    return (pid_t)pid;
}

/* 回收自己 spawn 的子进程；返回 1 表示仍存活。 */
static int ebpf_proxy_child_alive(void) {
    int status = 0;
    pid_t r;
    if (g_ebpf_proxy_pid <= 0) return 0;
    r = waitpid(g_ebpf_proxy_pid, &status, WNOHANG);
    if (r == g_ebpf_proxy_pid) {
        g_ebpf_proxy_pid = -1;
        return 0;
    }
    if (r == 0) return 1;
    if (r < 0 && errno == ECHILD) return kill(g_ebpf_proxy_pid, 0) == 0;
    return 0;
}

/* 已有活 proxy 检测（手动启动/上一次 master 退出后残留）：心跳在 200ms 内推进。 */
static int ebpf_proxy_heartbeat_live(void) {
    unsigned long long h1 = 0, h2 = 0;
    if (ebpf_ctl_read(EBPF_CTL_KEY_HEARTBEAT, &h1) != 0) return 0;
    usleep(200000);
    if (ebpf_ctl_read(EBPF_CTL_KEY_HEARTBEAT, &h2) != 0) return 0;
    return h2 != h1;
}

static void ebpf_proxy_kill(void) {
    pid_t pid = g_ebpf_proxy_pid;
    if (pid <= 0) pid = ebpf_proxy_read_pidfile();
    if (pid <= 0) return;
    if (kill(pid, 0) != 0) {
        if (pid == g_ebpf_proxy_pid) g_ebpf_proxy_pid = -1;
        return;
    }
    fprintf(stderr, "repl: killing stale ebpf-proxy pid=%d\n", (int)pid);
    kill(pid, SIGKILL);
    if (pid == g_ebpf_proxy_pid) {
        waitpid(pid, NULL, 0);
        g_ebpf_proxy_pid = -1;
    } else {
        for (int i = 0; i < 50; ++i) {
            if (kill(pid, 0) != 0) break;
            usleep(20000);
        }
    }
}

static void ebpf_proxy_spawn(void) {
    char pidfile[512], portbuf[32], masterbuf[32];
    pid_t pid = -1;
    int rc;
    long long now;

    if (!g_cfg.ebpf_proxy_bin[0] || !g_cfg.ebpf_client_capture_obj[0]) return;
    if (ebpf_proxy_child_alive()) return;            /* 自己的子进程还活着 */
    if (ebpf_proxy_heartbeat_live()) {               /* 外部启动的 proxy 活着，避免双 attach */
        fprintf(stderr, "repl: ebpf-proxy already running (external), skip spawn\n");
        return;
    }
    now = kvs_now_ms();
    if (now - g_ebpf_proxy_last_spawn_ms < PROXY_RESPAWN_COOLDOWN_MS) return;

    ebpf_proxy_pidfile_path(pidfile, sizeof(pidfile));
    snprintf(masterbuf, sizeof(masterbuf), "%d", (int)getpid());
    snprintf(portbuf, sizeof(portbuf), "%d", g_cfg.port);

    char *argv[] = {
        (char *)g_cfg.ebpf_proxy_bin,
        (char *)"--pin-path", (char *)g_cfg.ebpf_pin_path,
        (char *)"--obj-path", (char *)g_cfg.ebpf_client_capture_obj,
        (char *)"--master-pid", masterbuf,
        (char *)"--master-port", portbuf,
        (char *)"--pid-file", pidfile,
        (char *)"--commit-sock", (char *)repl_commit_socket_path(),
        NULL
    };
    rc = posix_spawn(&pid, g_cfg.ebpf_proxy_bin, NULL, NULL, argv, environ);
    g_ebpf_proxy_last_spawn_ms = now;
    if (rc != 0) {
        fprintf(stderr, "repl: spawn ebpf-proxy failed (%s) — eBPF+tcp 增量不可用\n",
                strerror(rc));
        return;
    }
    g_ebpf_proxy_pid = pid;
    g_ebpf_proxy_restart_count++;
    g_ebpf_proxy_healthy = 0;
    g_ebpf_failure_handled = 0;
    repl_ebpf_reset_backpressure_cache();
    fprintf(stderr, "repl: spawned ebpf-proxy pid=%d (restarts=%llu)\n",
            (int)pid, g_ebpf_proxy_restart_count);
}

int repl_ebpf_proxy_healthy(void) {
    if (!repl_ebpf_is_capture_transport()) return 1;
    return g_ebpf_proxy_healthy;
}

unsigned long long repl_ebpf_proxy_restart_count(void) {
    return g_ebpf_proxy_restart_count;
}

/* REPLSYNC 在 backlog/snapshot 发送完成后调用：等 proxy 真正切到 BUFFERING，
 * 之后才允许重新打开 capture，保证新捕获的增量一定进入 cache 而不是直接下送。 */
int repl_ebpf_proxy_wait_buffering(int timeout_ms) {
    unsigned long long st = 0;
    int waited = 0;
    if (!repl_ebpf_is_capture_transport()) return 1;
    if (timeout_ms < 0) timeout_ms = 0;
    while (waited <= timeout_ms) {
        if (ebpf_ctl_read(9 /* CLIENT_CTL_KEY_PROXY_STATE */, &st) == 0 && st == 1)
            return 1;
        usleep(2000);
        waited += 2;
    }
    return 0;
}

static void repl_ebpf_health_tick(void) {
    long long now;
    unsigned long long hb = 0, fault = 0;
    int hb_ok;

    if (!repl_ebpf_is_capture_transport()) return;
    now = kvs_now_ms();

    /* 子进程回收 + 心跳新鲜度（外部 proxy 也以心跳为准） */
    (void)ebpf_proxy_child_alive();
    hb_ok = (ebpf_ctl_read(EBPF_CTL_KEY_HEARTBEAT, &hb) == 0);
    if (hb_ok) {
        if (hb != g_ebpf_hb_last) {
            g_ebpf_hb_last = hb;
            g_ebpf_hb_last_ms = now;
        }
        g_ebpf_proxy_healthy = (now - g_ebpf_hb_last_ms) <= PROXY_HEARTBEAT_STALE_MS;
    } else {
        g_ebpf_proxy_healthy = 0;
    }
    if (g_ebpf_proxy_healthy) {
        g_ebpf_proxy_ever_healthy = 1;
        /* 只有 commit 通道也恢复后才解除“已失效”标记，避免 proxy 刚死、
         * 心跳尚未过期期间每 100ms 重复触发 COMMIT_CHANNEL 失效。 */
        if (!repl_commit_active_mode() || repl_commit_connected())
            g_ebpf_failure_handled = 0;
    }

    /* 恢复超时（fullsync/追赶卡住）→ 再次失效 */
    repl_recovery_tick();

    /* Commit Gate 控制面维护（重连/HELLO；成功后由 owner 线程 REARM） */
    repl_commit_health_tick();

    /* BPF 截断 fail-closed */
    if (g_ebpf_proxy_healthy && ebpf_ctl_read(EBPF_CTL_KEY_CAPTURE_FAULT, &fault) == 0 && fault) {
        repl_ebpf_set_capture_enable(0);
        repl_session_invalidate(REPL_INVALID_CAPTURE_TRUNCATED);
        ebpf_ctl_write(EBPF_CTL_KEY_CAPTURE_FAULT, 0);
        g_ebpf_failure_handled = 1;
        fprintf(stderr, "repl: capture truncated → replication session invalidated\n");
    }

    /* Commit Gate 故障（proxy 写 client_ctl[10]） */
    {
        unsigned long long cf = 0;
        if (g_ebpf_proxy_healthy &&
            ebpf_ctl_read(EBPF_CTL_KEY_COMMIT_FAULT, &cf) == 0 && cf) {
            repl_ebpf_set_capture_enable(0);
            repl_invalid_reason_t reason = REPL_INVALID_COMMIT_TIMEOUT;
            if (cf == 2) reason = REPL_INVALID_COMMIT_DESYNC;
            else if (cf == 3) reason = REPL_INVALID_COMMIT_GAP;
            else if (cf == 4) reason = REPL_INVALID_COMMIT_CHANNEL;
            repl_session_invalidate(reason);
            ebpf_ctl_write(EBPF_CTL_KEY_COMMIT_FAULT, 0);
            g_ebpf_failure_handled = 1;
            fprintf(stderr, "repl: commit gate fault code=%llu → session invalidated\n", cf);
        }
    }

    if (!g_ebpf_proxy_healthy) {
        /* proxy 失活：关 capture → 失效副本会话 → 冷却后重启 proxy */
        repl_ebpf_set_capture_enable(0);
        if ((g_ebpf_proxy_ever_healthy || repl_capture_gate_has_replica()) &&
            !g_ebpf_failure_handled) {
            repl_session_invalidate(REPL_INVALID_PROXY_HEARTBEAT_STALE);
            g_ebpf_failure_handled = 1;
        }
        if (now - g_ebpf_proxy_last_spawn_ms >= PROXY_RESPAWN_COOLDOWN_MS) {
            ebpf_proxy_kill();
            ebpf_proxy_spawn();
        }
        return;
    }

    if (repl_commit_active_mode()) {
        /* Commit Gate：capture 只由 owner 线程 REARM（等命令边界→BASELINE）打开；
         * 通道断开时保持关闭，并失效一次当前复制会话。 */
        if (!repl_commit_connected()) {
            repl_ebpf_set_capture_enable(0);
            if (g_ebpf_proxy_ever_healthy && !g_ebpf_failure_handled) {
                repl_session_invalidate(REPL_INVALID_COMMIT_CHANNEL);
                g_ebpf_failure_handled = 1;
            }
        }
        return;
    }

    /* 旧模式（无 Commit Gate）：按副本存在与否开关 capture */
    if (!repl_capture_gate_blocked()) {
        int gate = repl_capture_gate_has_replica();
        repl_ebpf_set_capture_enable(gate);
        if (gate) ebpf_ctl_write(EBPF_CTL_KEY_CAPTURE_FAULT, 0);
    }
}

static void *repl_ebpf_health_thread(void *arg) {
    (void)arg;
    while (!g_ebpf_health_stop) {
        repl_ebpf_health_tick();
        usleep(100000);
    }
    return NULL;
}

void repl_ebpf_proxy_start(void) {
    if (!repl_ebpf_is_capture_transport()) return;
    ebpf_proxy_spawn();
    /* master 重启后 proxy 可能还活着：刷新它使用的 pid/port（BPF pid 过滤键）。 */
    ebpf_ctl_write(EBPF_CTL_KEY_MASTER_PID, (unsigned long long)getpid());
    ebpf_ctl_write(EBPF_CTL_KEY_MASTER_PORT, (unsigned long long)g_cfg.port);
}

void repl_ebpf_health_start(void) {
    pthread_t tid;
    if (!repl_ebpf_is_capture_transport()) return;
    if (pthread_create(&tid, NULL, repl_ebpf_health_thread, NULL) != 0) {
        fprintf(stderr, "repl: failed to start ebpf-proxy health thread\n");
        return;
    }
    pthread_detach(tid);
    fprintf(stderr, "repl: ebpf-proxy health monitor started\n");
}

/* INFO：读取 pinned client_stats / proxy_stats（INFO 低频，直接开/关 map）。 */
int repl_ebpf_proxy_get_stats(kvs_repl_proxy_stats_t *out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
#if KVS_ENABLE_EBPF
    if (g_cfg.ebpf_pin_path[0]) {
        char path[512];
        unsigned int k;
        unsigned long long v = 0;

        snprintf(path, sizeof(path), "%s/client_stats", g_cfg.ebpf_pin_path);
        int fd = bpf_obj_get(path);
        if (fd >= 0) {
            k = EBPF_CLIENT_STATS_EVENTS;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->capture_events = v;
            k = EBPF_CLIENT_STATS_BYTES;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->capture_bytes = v;
            k = EBPF_CLIENT_STATS_TRUNCATED;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->capture_truncated = v;
            close(fd);
        }
        snprintf(path, sizeof(path), "%s/proxy_stats", g_cfg.ebpf_pin_path);
        fd = bpf_obj_get(path);
        if (fd >= 0) {
            k = 0;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->forwarded_bytes = v;
            k = 1;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->enqueued_bytes = v;
            k = 2;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->pending_bytes = v;
            k = 3;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->cache_bytes = v;
            k = 4;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->dropped_bytes = v;
            k = 5;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->proxy_reconnects = v;
            k = 6;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->capture_drop = (int)v;
            k = 7;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_descriptors = v;
            k = 8;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_writes = v;
            k = 9;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_pending = v;
            k = 10;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_timeout = v;
            k = 11;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_desync = v;
            k = 12;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_gap = v;
            k = 13;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_recovery_bytes = v;
            k = 14;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_next_global = v;
            k = 15;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->commit_channel_connected = (int)v;
            k = 16;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->cache_over_high = (int)v;
            k = 17;
            if (bpf_map_lookup_elem(fd, &k, &v) == 0) out->cache_hard_hit = v;
            close(fd);
        }
    }
#endif
    return 0;
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
