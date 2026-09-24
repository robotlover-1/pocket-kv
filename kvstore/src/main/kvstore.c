
#include "kvstore/kvstore.h"
#include "kvstore/replication/repl_kprobe.h"
#include "kvstore/replication/client_ctl.h"
#include <bpf/bpf.h>
#include <signal.h>
#include <ctype.h>
#include <strings.h>
#include <spawn.h>

extern char **environ;

/* kvs_repl.c 中的 static 变量，KPROBEMR 响应需要 */
extern int g_slave_fd;

/* kvs_repl.c 中的复制转发队列（T2：repl_broadcast 改入队） */
extern int repl_fwd_enqueue(conn_t *c, const unsigned char *buf, size_t len, unsigned long long end_offset);
extern int repl_fwd_start(void);
extern void repl_fwd_stop(void);
extern void repl_fwd_purge_conn(conn_t *c);
extern unsigned long long repl_fwd_get_watermark(conn_t *c);
extern int repl_fwd_is_stalled(conn_t *c);
extern int repl_backlog_copy_range(unsigned long long offset, unsigned char **out_buf, size_t *out_len);

/* 定义在本文件后部：repl_add_slave/repl_remove_slave 需要在此通知 proxy session 边界 */
static void repl_notify_ebpf_proxy_session(int valid, unsigned long long sid);
/* 定义在本文件后部：session end 时要放行屏障，避免 proxy 永久停在 BUFFERING */
static void repl_barrier_release(void);

#define KVS_DEFAULT_CONFIG_PATH "kvstore.conf"

/* 仅当 RDMA 被实际配置为传输层时才打印 RDMA 调试日志 */
#if KVS_ENABLE_RDMA
#define repl_rdma_log(fmt, ...) do { \
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma") || \
        !strcasecmp(g_cfg.repl_transport_backend, "rdma")) { \
        fprintf(stderr, "repl rdma: " fmt "\n", ##__VA_ARGS__); \
    } \
} while(0)
#else
#define repl_rdma_log(fmt, ...) ((void)0)
#endif

kv_config_t g_cfg = {
    .role = ROLE_MASTER,
    .port = 5160,
    .master_host = "192.168.233.128",
    .master_port = 5160,
    .dump_path = "kvstore.dump",
    .aof_path = "kvstore.aof",
    .mem_backend = "libc",
    .net_backend = "reactor",
    .repl_transport_backend = "tcp",
    .repl_fullsync_transport = "rdma",
    .repl_realtime_transport = "tcp",
    .ebpf_obj_path = "build/replication/bpf/repl_sockmap.bpf.o",
    .ebpf_pin_path = "/sys/fs/bpf/kvstore_repl_sockmap",
    .ebpf_proxy_bin = "build/ebpf_proxy",
    .ebpf_client_capture_obj = "build/replication/bpf/repl_client_capture.bpf.o",
    .ebpf_enabled = 0,
    .ebpf_redirect = 0,
    .ebpf_redirect_key = 0,
    .ebpf_forward = 0,
    .rdma_dev = "siw0",
    .rdma_ib_port = 1,
    .rdma_gid_idx = 1,
    .rdma_port = 0,
    .rdma_send_slots = 0,           /* 0 = use KVS_RDMA_SEND_SLOTS_DEFAULT (16) */
    .rdma_recv_slots = 0,           /* 0 = use KVS_RDMA_RECV_SLOTS_DEFAULT (64) */
    .rdma_chunk_size = BUFFER_CAP * 4,
    .rdma_qp_wr_depth = 64,
    .repl_rdma_write_buf_max_mb = 256,
    .aof_fsync = KVS_AOF_FSYNC_ALWAYS,
    .aof_fsync_per_command = 0,   /* 0=批量；1=逐条 */
    .aof_fsync_sync = 0,          /* 默认异步批量 group commit（攒批共享一次 fsync，响应不等落盘） */
    .aof_group_commit_window_us = 2000,  /* 攒批窗口（µs）：2000 消除 fsync 吞吐瓶颈且崩溃窗口 ~2.5ms；0=每周期 flush */
    .log_mode = "info",
    .is_sentinel = 0,
    .sentinel_master_name = "mymaster",
    .sentinel_monitor_host = "127.0.0.1",
    .sentinel_monitor_port = 5000,
    .sentinel_known_slaves = "",
    .sentinel_down_after_ms = 5000,
    .sentinel_failover_timeout_ms = 10000,
    .sentinel_quorum = 1,
    .kprobe_enabled = 1,
    .repl_kprobe_obj_path = "build/replication/bpf/repl_kprobe.bpf.o",
    .requirepass = "",
};
conn_t *g_replicas = NULL;
pthread_mutex_t g_repl_lock = PTHREAD_MUTEX_INITIALIZER;

/* 全量同步进行中标志 — 同步期间跳过实时广播，启停 RDMA */
volatile int g_repl_fullsync_in_progress = 0;

volatile time_t g_last_write_ts = 0;   /* 最后一次写命令时间戳，健康检查用 */

static pthread_mutex_t g_repl_last_send_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_repl_last_send_stage[64] = "none";
static unsigned long long g_repl_last_send_len = 0;
static unsigned long long g_repl_last_send_offset = 0;
static char g_repl_last_send_preview[160] = "";

int resp_simple_string(char *out, size_t cap, const char *s) { return snprintf(out, cap, "+%s\r\n", s); }
int resp_error(char *out, size_t cap, const char *s) { return snprintf(out, cap, "-ERR %s\r\n", s); }
int resp_integer(char *out, size_t cap, long long v) { return snprintf(out, cap, ":%lld\r\n", v); }
int resp_bulk(char *out, size_t cap, const char *s, size_t len) { int n = snprintf(out, cap, "$%zu\r\n", len); if ((size_t)n + len + 2 > cap) return -1; memcpy(out + n, s, len); out[n + len] = '\r'; out[n + len + 1] = '\n'; return n + (int)len + 2; }
int resp_null_bulk(char *out, size_t cap) { return snprintf(out, cap, "$-1\r\n"); }

static size_t append_bulk(unsigned char *out, size_t pos, size_t cap, const char *s) {
    size_t len = strlen(s);
    int n = snprintf((char *)out + pos, cap - pos, "$%zu\r\n", len);
    pos += (size_t)n;
    memcpy(out + pos, s, len); pos += len;
    out[pos++] = '\r'; out[pos++] = '\n';
    return pos;
}
size_t resp_build_cmd4(unsigned char *out, size_t cap, const char *cmd, const char *a1, const char *a2, const char *a3) { size_t pos=0; pos += (size_t)snprintf((char*)out+pos, cap-pos, "*4\r\n"); pos=append_bulk(out,pos,cap,cmd); pos=append_bulk(out,pos,cap,a1); pos=append_bulk(out,pos,cap,a2); pos=append_bulk(out,pos,cap,a3); return pos; }
size_t resp_build_cmd3(unsigned char *out, size_t cap, const char *cmd, const char *a1, const char *a2) { size_t pos=0; pos += (size_t)snprintf((char*)out+pos, cap-pos, "*3\r\n"); pos=append_bulk(out,pos,cap,cmd); pos=append_bulk(out,pos,cap,a1); pos=append_bulk(out,pos,cap,a2); return pos; }
size_t resp_build_cmd2(unsigned char *out, size_t cap, const char *cmd, const char *a1) { size_t pos=0; pos += (size_t)snprintf((char*)out+pos, cap-pos, "*2\r\n"); pos=append_bulk(out,pos,cap,cmd); pos=append_bulk(out,pos,cap,a1); return pos; }
size_t resp_build_cmd1(unsigned char *out, size_t cap, const char *cmd) { size_t pos=0; pos += (size_t)snprintf((char*)out+pos, cap-pos, "*1\r\n"); pos=append_bulk(out,pos,cap,cmd); return pos; }

static void repl_format_send_preview(char *out, size_t out_cap, const unsigned char *buf, size_t len) {
    size_t in_cap;
    size_t i;
    size_t pos = 0;
    if (!out || out_cap == 0) return;
    out[0] = '\0';
    if (!buf || len == 0) return;
    in_cap = len < 64 ? len : 64;
    for (i = 0; i < in_cap && pos + 2 < out_cap; ++i) {
        unsigned char ch = buf[i];
        if (ch == '\r') {
            if (pos + 2 >= out_cap) break;
            out[pos++] = '\\';
            out[pos++] = 'r';
        } else if (ch == '\n') {
            if (pos + 2 >= out_cap) break;
            out[pos++] = '\\';
            out[pos++] = 'n';
        } else if (isprint(ch)) {
            out[pos++] = (char)ch;
        } else {
            out[pos++] = '.';
        }
    }
    if (in_cap < len && pos + 4 < out_cap) {
        out[pos++] = '.';
        out[pos++] = '.';
        out[pos++] = '.';
    }
    out[pos] = '\0';
}

void repl_note_send_context(const char *stage, size_t len, unsigned long long offset, const unsigned char *buf) {
    pthread_mutex_lock(&g_repl_last_send_lock);
    snprintf(g_repl_last_send_stage, sizeof(g_repl_last_send_stage), "%s", (stage && *stage) ? stage : "unknown");
    g_repl_last_send_len = (unsigned long long)len;
    g_repl_last_send_offset = offset;
    repl_format_send_preview(g_repl_last_send_preview, sizeof(g_repl_last_send_preview), buf, len);
    pthread_mutex_unlock(&g_repl_last_send_lock);
}

void repl_get_last_send_context(char *stage, size_t stage_cap, unsigned long long *len, unsigned long long *offset, char *preview, size_t preview_cap) {
    pthread_mutex_lock(&g_repl_last_send_lock);
    if (stage && stage_cap > 0) snprintf(stage, stage_cap, "%s", g_repl_last_send_stage);
    if (len) *len = g_repl_last_send_len;
    if (offset) *offset = g_repl_last_send_offset;
    if (preview && preview_cap > 0) snprintf(preview, preview_cap, "%s", g_repl_last_send_preview);
    pthread_mutex_unlock(&g_repl_last_send_lock);
}

static int parse_autosnap_rules(const char *spec);
static int parse_config_file(const char *path);
static void trim_inplace(char *s);
static int parse_appendfsync_policy(const char *s, kvs_aof_fsync_policy_t *out) {
    if (!s || !out) return -1;
    if (!strcasecmp(s, "always")) {
        *out = KVS_AOF_FSYNC_ALWAYS;
        return 0;
    }
    if (!strcasecmp(s, "off")) {
        *out = KVS_AOF_FSYNC_OFF;
        return 0;
    }
    return -1;
}

static int parse_repl_transport_backend(const char *s) {
    if (!s) return -1;
    if (!strcasecmp(s, "tcp")) return 0;
    if (!strcasecmp(s, "rdma")) return 0;
    if (!strcasecmp(s, "ebpf")) return 0;
    if (!strcasecmp(s, "sockmap")) return 0;
    if (!strcasecmp(s, "kprobe-rdma")) return 0;
    if (!strcasecmp(s, "ebpf+tcp")) return 0;
    return -1;
}

static int parse_args(int argc, char **argv) {
    const char *config_path = NULL;
    /* First pass: find config file path */
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--config") && i + 1 < argc) {
            config_path = argv[i + 1];
            break;
        }
        if (i == 1 && argv[i][0] != '-') {
            /* First positional argument: treat as config file */
            config_path = argv[i];
            break;
        }
    }

    if (!config_path) {
        FILE *fp = fopen(KVS_DEFAULT_CONFIG_PATH, "r");
        if (fp) {
            fclose(fp);
            config_path = KVS_DEFAULT_CONFIG_PATH;
        }
    }

    if (config_path && parse_config_file(config_path) != 0) return -1;

    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--config") && i + 1 < argc) {
            ++i;
        }
        else if (!strcmp(argv[i], "--port") && i + 1 < argc) g_cfg.port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--role") && i + 1 < argc) g_cfg.role = !strcmp(argv[++i], "slave") ? ROLE_SLAVE : ROLE_MASTER;
        else if (!strcmp(argv[i], "--master-host") && i + 1 < argc) snprintf(g_cfg.master_host, sizeof(g_cfg.master_host), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--master-port") && i + 1 < argc) g_cfg.master_port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--requirepass") && i + 1 < argc) snprintf(g_cfg.requirepass, sizeof(g_cfg.requirepass), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--dump") && i + 1 < argc) snprintf(g_cfg.dump_path, sizeof(g_cfg.dump_path), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--aof") && i + 1 < argc) snprintf(g_cfg.aof_path, sizeof(g_cfg.aof_path), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--aof-disable")) persist_aof_disable();
        else if (!strcmp(argv[i], "--mem") && i + 1 < argc) snprintf(g_cfg.mem_backend, sizeof(g_cfg.mem_backend), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--net") && i + 1 < argc) {
            snprintf(g_cfg.net_backend, sizeof(g_cfg.net_backend), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--repl-transport") && i + 1 < argc) {
            if (parse_repl_transport_backend(argv[i + 1]) != 0) return -1;
            snprintf(g_cfg.repl_transport_backend, sizeof(g_cfg.repl_transport_backend), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--repl-fullsync-transport") && i + 1 < argc) {
            if (parse_repl_transport_backend(argv[i + 1]) != 0) return -1;
            snprintf(g_cfg.repl_fullsync_transport, sizeof(g_cfg.repl_fullsync_transport), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--repl-realtime-transport") && i + 1 < argc) {
            if (parse_repl_transport_backend(argv[i + 1]) != 0) return -1;
            snprintf(g_cfg.repl_realtime_transport, sizeof(g_cfg.repl_realtime_transport), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-obj") && i + 1 < argc) {
            snprintf(g_cfg.ebpf_obj_path, sizeof(g_cfg.ebpf_obj_path), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-pin") && i + 1 < argc) {
            snprintf(g_cfg.ebpf_pin_path, sizeof(g_cfg.ebpf_pin_path), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-pin-path") && i + 1 < argc) {
            snprintf(g_cfg.ebpf_pin_path, sizeof(g_cfg.ebpf_pin_path), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-proxy-bin") && i + 1 < argc) {
            snprintf(g_cfg.ebpf_proxy_bin, sizeof(g_cfg.ebpf_proxy_bin), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-client-capture-obj") && i + 1 < argc) {
            snprintf(g_cfg.ebpf_client_capture_obj, sizeof(g_cfg.ebpf_client_capture_obj), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-redirect")) {
            g_cfg.ebpf_redirect = 1;
        }
        else if (!strcmp(argv[i], "--ebpf-redirect-key") && i + 1 < argc) {
            g_cfg.ebpf_redirect_key = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--ebpf-forward")) {
            g_cfg.ebpf_forward = 1;
        }
        else if (!strcmp(argv[i], "--rdma-dev") && i + 1 < argc) {
            snprintf(g_cfg.rdma_dev, sizeof(g_cfg.rdma_dev), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-ib-port") && i + 1 < argc) {
            g_cfg.rdma_ib_port = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-gid-idx") && i + 1 < argc) {
            g_cfg.rdma_gid_idx = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-port") && i + 1 < argc) {
            g_cfg.rdma_port = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-send-slots") && i + 1 < argc) {
            g_cfg.rdma_send_slots = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-recv-slots") && i + 1 < argc) {
            g_cfg.rdma_recv_slots = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-chunk-size") && i + 1 < argc) {
            g_cfg.rdma_chunk_size = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--rdma-qp-wr-depth") && i + 1 < argc) {
            g_cfg.rdma_qp_wr_depth = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--repl-rdma-write-buf-max-mb") && i + 1 < argc) {
            g_cfg.repl_rdma_write_buf_max_mb = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--appendfsync") && i + 1 < argc) {
            kvs_aof_fsync_policy_t policy;
            if (parse_appendfsync_policy(argv[++i], &policy) != 0) return -1;
            g_cfg.aof_fsync = policy;
        }
        else if (!strcmp(argv[i], "--aof-fsync-per-command")) {
            g_cfg.aof_fsync_per_command = 1;
        }
        else if (!strcmp(argv[i], "--aof-fsync-group-commit")) {
            g_cfg.aof_fsync_per_command = 0;
        }
        else if (!strcmp(argv[i], "--aof-fsync-sync")) {
            g_cfg.aof_fsync_sync = 1;
            g_cfg.aof_fsync_per_command = 1;
        }
        else if (!strcmp(argv[i], "--aof-fsync-sync-batch")) {
            g_cfg.aof_fsync_sync = 1;
            g_cfg.aof_fsync_per_command = 0;
        }
        else if (!strcmp(argv[i], "--aof-group-commit-window-us") && i + 1 < argc) {
            g_cfg.aof_group_commit_window_us = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--log-mode") && i + 1 < argc) {
            snprintf(g_cfg.log_mode, sizeof(g_cfg.log_mode), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--autosnap") && i + 1 < argc) {
            persist_clear_autosnap_rules();
            if (parse_autosnap_rules(argv[++i]) != 0) return -1;
        }
        else if (!strcmp(argv[i], "--sentinel")) {
            g_cfg.is_sentinel = 1;
        }
        else if (!strcmp(argv[i], "--sentinel-master-name") && i + 1 < argc) {
            snprintf(g_cfg.sentinel_master_name, sizeof(g_cfg.sentinel_master_name), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--sentinel-monitor-host") && i + 1 < argc) {
            snprintf(g_cfg.sentinel_monitor_host, sizeof(g_cfg.sentinel_monitor_host), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--sentinel-monitor-port") && i + 1 < argc) {
            g_cfg.sentinel_monitor_port = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--sentinel-known-slaves") && i + 1 < argc) {
            snprintf(g_cfg.sentinel_known_slaves, sizeof(g_cfg.sentinel_known_slaves), "%s", argv[++i]);
        }
        else if (!strcmp(argv[i], "--sentinel-down-after") && i + 1 < argc) {
            g_cfg.sentinel_down_after_ms = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--sentinel-failover-timeout") && i + 1 < argc) {
            g_cfg.sentinel_failover_timeout_ms = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--sentinel-quorum") && i + 1 < argc) {
            g_cfg.sentinel_quorum = atoi(argv[++i]);
        }
        else if (!strcmp(argv[i], "--kprobe-enabled")) {
            g_cfg.kprobe_enabled = 1;
        }
        else if (!strcmp(argv[i], "--repl-kprobe-obj-path") && i + 1 < argc) {
            snprintf(g_cfg.repl_kprobe_obj_path, sizeof(g_cfg.repl_kprobe_obj_path), "%s", argv[++i]);
        }
        else if (argv[i][0] != '-') {
            /* Bare argument: treat as config file path */
            if (parse_config_file(argv[i]) != 0) return -1;
        }
        else return -1;
    }
    return 0;
}

static int parse_autosnap_rules(const char *spec) {
    if (!spec || !*spec) return 0;
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", spec);
    char *saveptr = NULL;
    for (char *tok = strtok_r(tmp, ",", &saveptr); tok; tok = strtok_r(NULL, ",", &saveptr)) {
        char *sep = strchr(tok, ':');
        if (!sep) return -1;
        *sep = '\0';
        long long sec = atoll(tok);
        long long changes = atoll(sep + 1);
        if (persist_register_autosnap_rule(sec, changes) != 0) return -1;
    }
    return 0;
}

static void trim_inplace(char *s) {
    char *start;
    char *end;
    size_t len;
    if (!s) return;
    start = s;
    while (*start && isspace((unsigned char)*start)) start++;
    if (start != s) memmove(s, start, strlen(start) + 1);
    len = strlen(s);
    if (len == 0) return;
    end = s + len - 1;
    while (end >= s && isspace((unsigned char)*end)) {
        *end = '\0';
        --end;
    }
}

static int apply_config_kv(const char *key, const char *value) {
    kvs_aof_fsync_policy_t policy;
    if (!key || !value) return -1;
    if (!strcmp(key, "port")) g_cfg.port = atoi(value);
    else if (!strcmp(key, "role")) g_cfg.role = !strcasecmp(value, "slave") ? ROLE_SLAVE : ROLE_MASTER;
    else if (!strcmp(key, "master_host")) snprintf(g_cfg.master_host, sizeof(g_cfg.master_host), "%s", value);
    else if (!strcmp(key, "master_port")) g_cfg.master_port = atoi(value);
    else if (!strcmp(key, "dump_path")) snprintf(g_cfg.dump_path, sizeof(g_cfg.dump_path), "%s", value);
    else if (!strcmp(key, "aof_path")) snprintf(g_cfg.aof_path, sizeof(g_cfg.aof_path), "%s", value);
    else if (!strcmp(key, "mem_backend")) snprintf(g_cfg.mem_backend, sizeof(g_cfg.mem_backend), "%s", value);
    else if (!strcmp(key, "net_backend")) snprintf(g_cfg.net_backend, sizeof(g_cfg.net_backend), "%s", value);
    else if (!strcmp(key, "repl_transport_backend")) {
        if (parse_repl_transport_backend(value) != 0) return -1;
        snprintf(g_cfg.repl_transport_backend, sizeof(g_cfg.repl_transport_backend), "%s", value);
    }
    else if (!strcmp(key, "repl_fullsync_transport")) {
        if (parse_repl_transport_backend(value) != 0) return -1;
        snprintf(g_cfg.repl_fullsync_transport, sizeof(g_cfg.repl_fullsync_transport), "%s", value);
    }
    else if (!strcmp(key, "repl_realtime_transport")) {
        if (parse_repl_transport_backend(value) != 0) return -1;
        snprintf(g_cfg.repl_realtime_transport, sizeof(g_cfg.repl_realtime_transport), "%s", value);
    }
    else if (!strcmp(key, "ebpf_enabled")) g_cfg.ebpf_enabled = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
    else if (!strcmp(key, "ebpf_obj_path")) snprintf(g_cfg.ebpf_obj_path, sizeof(g_cfg.ebpf_obj_path), "%s", value);
    else if (!strcmp(key, "ebpf_pin_path")) snprintf(g_cfg.ebpf_pin_path, sizeof(g_cfg.ebpf_pin_path), "%s", value);
    else if (!strcmp(key, "ebpf_proxy_bin")) snprintf(g_cfg.ebpf_proxy_bin, sizeof(g_cfg.ebpf_proxy_bin), "%s", value);
    else if (!strcmp(key, "ebpf_client_capture_obj")) snprintf(g_cfg.ebpf_client_capture_obj, sizeof(g_cfg.ebpf_client_capture_obj), "%s", value);
    else if (!strcmp(key, "ebpf_redirect")) g_cfg.ebpf_redirect = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
    else if (!strcmp(key, "ebpf_redirect_key")) g_cfg.ebpf_redirect_key = atoi(value);
    else if (!strcmp(key, "ebpf_forward")) g_cfg.ebpf_forward = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
    else if (!strcmp(key, "rdma_dev")) snprintf(g_cfg.rdma_dev, sizeof(g_cfg.rdma_dev), "%s", value);
    else if (!strcmp(key, "rdma_ib_port")) g_cfg.rdma_ib_port = atoi(value);
    else if (!strcmp(key, "rdma_gid_idx")) g_cfg.rdma_gid_idx = atoi(value);
    else if (!strcmp(key, "rdma_port")) g_cfg.rdma_port = atoi(value);
    else if (!strcmp(key, "rdma_send_slots")) g_cfg.rdma_send_slots = atoi(value);
    else if (!strcmp(key, "rdma_recv_slots")) g_cfg.rdma_recv_slots = atoi(value);
    else if (!strcmp(key, "rdma_chunk_size")) g_cfg.rdma_chunk_size = atoi(value);
    else if (!strcmp(key, "rdma_qp_wr_depth")) g_cfg.rdma_qp_wr_depth = atoi(value);
    else if (!strcmp(key, "repl_rdma_write_buf_max_mb")) g_cfg.repl_rdma_write_buf_max_mb = atoi(value);
    else if (!strcmp(key, "appendfsync")) {
        if (parse_appendfsync_policy(value, &policy) != 0) return -1;
        g_cfg.aof_fsync = policy;
    }
    else if (!strcmp(key, "aof_fsync_per_command")) g_cfg.aof_fsync_per_command = atoi(value);
    else if (!strcmp(key, "aof_fsync_sync")) { g_cfg.aof_fsync_sync = atoi(value); if (g_cfg.aof_fsync_sync) g_cfg.aof_fsync_per_command = 1; }
    else if (!strcmp(key, "aof_group_commit_window_us")) g_cfg.aof_group_commit_window_us = atoi(value);
    else if (!strcmp(key, "log_mode")) snprintf(g_cfg.log_mode, sizeof(g_cfg.log_mode), "%s", value);
    else if (!strcmp(key, "autosnap")) {
        persist_clear_autosnap_rules();
        if (parse_autosnap_rules(value) != 0) return -1;
    }
    else if (!strcmp(key, "sentinel")) g_cfg.is_sentinel = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
    else if (!strcmp(key, "sentinel_master_name")) snprintf(g_cfg.sentinel_master_name, sizeof(g_cfg.sentinel_master_name), "%s", value);
    else if (!strcmp(key, "sentinel_monitor_host")) snprintf(g_cfg.sentinel_monitor_host, sizeof(g_cfg.sentinel_monitor_host), "%s", value);
    else if (!strcmp(key, "sentinel_monitor_port")) g_cfg.sentinel_monitor_port = atoi(value);
    else if (!strcmp(key, "sentinel_known_slaves")) snprintf(g_cfg.sentinel_known_slaves, sizeof(g_cfg.sentinel_known_slaves), "%s", value);
    else if (!strcmp(key, "sentinel_down_after_ms")) g_cfg.sentinel_down_after_ms = atoi(value);
    else if (!strcmp(key, "sentinel_failover_timeout_ms")) g_cfg.sentinel_failover_timeout_ms = atoi(value);
    else if (!strcmp(key, "sentinel_quorum")) g_cfg.sentinel_quorum = atoi(value);
    else if (!strcmp(key, "kprobe_enabled")) g_cfg.kprobe_enabled = (!strcasecmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "yes"));
    else if (!strcmp(key, "repl_kprobe_obj_path")) snprintf(g_cfg.repl_kprobe_obj_path, sizeof(g_cfg.repl_kprobe_obj_path), "%s", value);
    else if (!strcmp(key, "requirepass")) snprintf(g_cfg.requirepass, sizeof(g_cfg.requirepass), "%s", value);
    else return -1;
    return 0;
}

static int parse_config_file(const char *path) {
    FILE *fp;
    char line[1024];
    int lineno = 0;
    if (!path || !*path) return -1;
    fp = fopen(path, "r");
    if (!fp) return -1;
    while (fgets(line, sizeof(line), fp)) {
        char *eq;
        char *key;
        char *value;
        lineno++;
        trim_inplace(line);
        if (line[0] == '\0' || line[0] == '#') continue;
        eq = strchr(line, '=');
        if (!eq) {
            fclose(fp);
            return -1;
        }
        *eq = '\0';
        key = line;
        value = eq + 1;
        trim_inplace(key);
        trim_inplace(value);
        if (apply_config_kv(key, value) != 0) {
            fclose(fp);
            return -1;
        }
    }
    fclose(fp);
    return 0;
}

void repl_add_slave(conn_t *c) {
    if (!c) return;  /* safety: slave 端 parse_resp_stream(NULL, ...) 可能误触发 */
    int first = 0;
    pthread_mutex_lock(&g_repl_lock);
    first = (g_replicas == NULL);
    for (conn_t *it = g_replicas; it; it = it->next_replica) {
        if (it == c) {
            c->is_replica = 1;
            c->repl_draining = 0;
            c->repl_fullsync_pending = 0;
            c->fwd_healthy = 0;
            c->fwd_last_active = 0;
            pthread_mutex_unlock(&g_repl_lock);
            return;
        }
    }
    c->is_replica = 1;
    c->repl_draining = 0;
    c->repl_fullsync_pending = 0;
    c->fwd_healthy = 0;
    c->fwd_last_active = 0;
    c->next_replica = g_replicas;
    g_replicas = c;
    pthread_mutex_unlock(&g_repl_lock);

    /* NO_REPLICA → 有 Slave：开新 replication session，同时打开 eBPF 捕获。
     * 捕获必须早于快照生成，快照之后的增量才可能被 proxy_cache 接住（§4.1/§5）。 */
    if (first) {
        unsigned long long sid = repl_session_begin();
        repl_notify_ebpf_proxy_session(1, sid);
        fprintf(stderr, "master: replication session begin id=%llu "
                        "(capture enabled, slave=%d)\n",
                sid, c->fd);
    }
}

/* 从 g_replicas 摘掉 c 并清零其副本状态。**调用方必须已持 g_repl_lock**。
 * 返回 1 表示摘掉的正是最后一个副本（调用方需要在锁外做 session end）。
 * 所有"从链表里移除副本"的动作都必须走这里 —— 手工改 next_replica/is_replica
 * 会绕过 session end，导致最后一个 Slave 掉线后 CAPTURE_ENABLE 仍然是 1。 */
static int repl_unlink_replica_locked(conn_t *c) {
    conn_t **pp = &g_replicas;
    int found = 0;
    while (*pp) {
        if (*pp == c) { *pp = c->next_replica; found = 1; break; }
        pp = &(*pp)->next_replica;
    }
    c->next_replica = NULL;
    c->is_replica = 0;
    c->repl_draining = 0;
    c->repl_fullsync_pending = 0;
    c->fwd_healthy = 0;
    c->fwd_last_active = 0;
    /* 只有确实摘掉了最后一个才算 session 结束（RDMA 侧会对不在链表里的哨兵 conn
     * 调 repl_remove_slave，误判会凭空作废一个正在服务的 session）。 */
    return (found && g_replicas == NULL);
}

/* 拒绝这个 replica 并让 Slave 尽快重连：标记 draining（广播侧不再发送），
 * 同时 shutdown 写方向 —— Slave 的 recv 立刻返回 0 → 走它自己的重连逻辑重新 REPLSYNC。
 * 只标记 draining 是不够的：Slave 侧没有空闲超时，控制连接会一直挂着不重试。
 * 不能直接 close_conn：调用栈上层（parse_resp_stream）还会用到 c。 */
static void repl_reject_replica(conn_t *c) {
    if (!c) return;
    c->repl_draining = 1;
    c->repl_fullsync_pending = 0;
    if (c->fd >= 0) shutdown(c->fd, SHUT_WR);
}

/* 最后一个 Slave 离开：session 失效 → 关捕获、作废旧 proxy_cache（§4.1/§8.2/§9）。
 * 此后 Master 的写仍会推进 master_repl_offset 但不再进 backlog（feed 因无 Slave
 * 跳过并标记历史不连续），下次 REPLSYNC 只能 FULLRESYNC。
 * 内部会做 bpf map 写入，必须在**锁外**调用。 */
static void repl_end_session_notify(void) {
    fprintf(stderr, "master: replication session end "
                    "(capture disabled, proxy cache invalidated)\n");
    /* 最后一个 Slave 都没了：屏障不能继续立着，否则新 Slave 接入时 proxy 一直停在
     * BUFFERING（虽然 begin 会复用屏障，但语义上应当先释放再重新建立）。 */
    repl_barrier_release();
    repl_session_end();
    repl_notify_ebpf_proxy_session(0, 0);
}

void repl_remove_slave(conn_t *c) {
    int last;
    pthread_mutex_lock(&g_repl_lock);
    last = repl_unlink_replica_locked(c);
    pthread_mutex_unlock(&g_repl_lock);
    if (last) repl_end_session_notify();
}

void repl_broadcast(const unsigned char *raw, size_t rawlen) {
    if (likely(!g_replicas)) return;  /* 无副本：零开销，省 repl_note_send_context(mutex+snprintf)+锁（perf：单机写路径 ~1.5%） */
    repl_note_send_context("broadcast", rawlen, repl_master_offset(), raw);

    int session_ended = 0;
    pthread_mutex_lock(&g_repl_lock);
    conn_t **pp = &g_replicas;
    while (*pp) {
        conn_t *c = *pp;
        if (c->repl_draining) {
            /* 统一走摘链 helper。原来手工改 next_replica/is_replica 会绕过 session end：
             * 最后一个 Slave 因 draining 被摘掉后 g_replicas 变 NULL，但 CAPTURE_ENABLE
             * 仍是 1，而随后真正 close 时的 repl_remove_slave 又因 found=0 不做清理。 */
            if (repl_unlink_replica_locked(c)) session_ended = 1;
            continue;   /* *pp 已指向下一个节点 */
        }
        if (c->repl_fullsync_pending) {
            pp = &c->next_replica;
            continue;
        }
        /* 全量期间只记 backlog（由调用方统一喂入，见下），不实时下送。
         * IMPORTANT：此处不能再 repl_backlog_feed —— 写命令成功后的统一喂入点
         * （execute_command 中 repl_backlog_feed + repl_note_broadcast）已经喂过一次，
         * 全量期间每条 raw 会被喂两遍：backlog_end_offset 跑在 master_repl_offset 前面，
         * 与 offset 推进语义脱钩（§11）。 */
        if (g_repl_fullsync_in_progress) {
            pp = &c->next_replica;
            continue;
        }
        /* ebpf+tcp: proxy 独立进程全权转发, repl_broadcast 不发送 */
        if (c->repl_transport_kind == KVS_REPL_TRANSPORT_EBPF_TCP) {
            pp = &c->next_replica;
            continue;
        }
        /* kprobe fwd healthy slaves served by ringbuf callback */
        if (c->fwd_healthy) {
            pp = &c->next_replica;
            continue;
        }
        /* 慢 slave 缓冲已满（stalled）：跳过入队，字节已在 backlog（2107 先行 feed），
         * 靠 REPLACK/转发线程自愈追赶补回。避免塞满转发队列阻塞 reactor（IMPORTANT 3/4）。 */
        if (repl_fwd_is_stalled(c)) {
            pp = &c->next_replica;
            continue;
        }
        /* 转发剥离：入队由转发线程发送。end_offset = 本批字节终点（backlog feed 后 master offset）。 */
        if (repl_fwd_enqueue(c, raw, rawlen, repl_master_offset()) != 0) {
            /* 入队失败（停止/内存/队列满未让出）→ 什么都不做，slave 通过 REPLACK 追回。
             * 这里**不能**再 repl_backlog_feed：字节早在业务写成功时就已经进了 backlog，
             * 再喂一次会让 backlog_end_offset += 2*rawlen 而 master_repl_offset 只 += rawlen，
             * 两者脱节，且环形缓冲里会多出一份重复字节，破坏 offset↔字节 的映射。 */
            pp = &c->next_replica;
            continue;
        }
        pp = &c->next_replica;
    }
    pthread_mutex_unlock(&g_repl_lock);

    /* draining 副本可能是最后一个：session end 必须在锁外做（内部要写 bpf map） */
    if (session_ended) repl_end_session_notify();
}

int repl_send_chunked(conn_t *c, const unsigned char *buf, size_t len) {
    return repl_send_chunked_ctx(c, buf, len, KVS_REPL_SEND_FULLSYNC);
}

int repl_send_chunked_ctx(conn_t *c, const unsigned char *buf, size_t len, int send_ctx) {
    size_t off = 0;
    size_t chunk_cap = !strcasecmp(repl_fullsync_transport_name(), "rdma") ? (g_cfg.rdma_chunk_size > 0 ? (size_t)g_cfg.rdma_chunk_size : (BUFFER_CAP * 4)) : len;
    if (!buf || len == 0) return 0;
    repl_note_send_context("chunked", len, repl_master_offset(), buf);
    if (chunk_cap < 1024) chunk_cap = 1024;
    if (chunk_cap > BUFFER_CAP * 4) chunk_cap = BUFFER_CAP * 4;
    if (chunk_cap == 0) chunk_cap = len;
    while (off < len) {
        size_t chunk = len - off;
        if (chunk > chunk_cap) chunk = chunk_cap;
        int rc;
        if (send_ctx == KVS_REPL_SEND_REALTIME) {
            rc = repl_realtime_send(c, buf + off, chunk);
        } else {
            rc = repl_fullsync_send(c, buf + off, chunk);
        }
        if (rc != 0) {
            /* P2.3 修复: repl_fullsync_send 已尝试 RDMA → TCP 回退，
             * 此处不应再绕过传输层直接 send()。直接返回失败，不推进 off。
             * 传输层已通过 repl_transport_trigger_fallback 禁用后续 RDMA 重试。 */
            return -1;
        }
        off += chunk;
    }
    /* P3.1b: flush 尾部 batch（不足 KVS_RDMA_BATCH_MAX 的积累 WR） */
    if (!strcasecmp(repl_fullsync_transport_name(), "rdma")) {
        repl_rdma_flush_batch();
    }
    return 0;
}

/* 写 pinned client_ctl map 的单个键。client_ctl 属于 repl_client_capture.bpf.c，
 * 由 ebpf-proxy 进程 pin 到 bpffs；master 通过 bpf_obj_get() 打开并写入，
 * ebpf-proxy 在主循环中轮询检测。map 不存在（proxy 未启动）时静默跳过。 */
static void repl_ebpf_client_ctl_set(__u32 key, __u64 val) {
#if KVS_ENABLE_EBPF
    char path[512];
    int fd;
    if (!g_cfg.ebpf_pin_path[0]) return;
    snprintf(path, sizeof(path), "%s/client_ctl", g_cfg.ebpf_pin_path);
    fd = bpf_obj_get(path);
    if (fd < 0) {
        /* ebpf-proxy 可能未启动，静默跳过 */
        return;
    }
    bpf_map_update_elem(fd, &key, &val, BPF_ANY);
    close(fd);
#else
    (void)key; (void)val;
#endif
}

static __u64 repl_ebpf_client_ctl_get(__u32 key) {
#if KVS_ENABLE_EBPF
    char path[512];
    int fd;
    __u64 val = 0;
    if (!g_cfg.ebpf_pin_path[0]) return 0;
    snprintf(path, sizeof(path), "%s/client_ctl", g_cfg.ebpf_pin_path);
    fd = bpf_obj_get(path);
    if (fd < 0) return 0;
    if (bpf_map_lookup_elem(fd, &key, &val) != 0) val = 0;
    close(fd);
    return val;
#else
    (void)key;
    return 0;
#endif
}

/* client_ctl[3] 的读写已统一收敛到 repl_barrier_begin/repl_barrier_release
 * （全量同步与 partial resync 共用同一套屏障语义，见下方屏障实现）。 */

/* 通知 ebpf-proxy 复制 session 边界（§4.1/§7/§9）。
 *   新 session：CAPTURE_ENABLE=1（BPF 开始抓）、SESSION_VALID=1、新 SESSION_ID、
 *               CACHE_INVALID=0（允许本 session 的 cache 在 REPLDONE 后 flush）。
 *   session 结束：CAPTURE_ENABLE=0（无 Slave，BPF 零开销）、SESSION_VALID=0、
 *               SESSION_ID=0、CACHE_INVALID=1（proxy 丢弃旧 cache，禁止跨 session flush）。 */
static void repl_notify_ebpf_proxy_session(int valid, unsigned long long sid) {
    if (valid) {
        /* 先定身份再置有效，最后才开捕获：proxy 按这三次写入的先后顺序观察，
         * 任何中间态都只是"已缓存但暂不 flush"，不会把数据算到上个 session 账上。 */
        repl_ebpf_client_ctl_set(KVS_CTL_SESSION_ID, sid);
        repl_ebpf_client_ctl_set(KVS_CTL_CACHE_INVALID, 0);
        repl_ebpf_client_ctl_set(KVS_CTL_SESSION_VALID, 1);
        repl_ebpf_client_ctl_set(KVS_CTL_CAPTURE_ENABLE, 1);
    } else {
        /* 先停捕获，再失效 session，最后清身份并置 CACHE_INVALID（proxy 丢弃旧 cache） */
        repl_ebpf_client_ctl_set(KVS_CTL_CAPTURE_ENABLE, 0);
        repl_ebpf_client_ctl_set(KVS_CTL_SESSION_VALID, 0);
        repl_ebpf_client_ctl_set(KVS_CTL_SESSION_ID, 0);
        repl_ebpf_client_ctl_set(KVS_CTL_CACHE_INVALID, 1);
    }
}

/* ============ ebpf-proxy 屏障（barrier）============
 * 为什么必须有：补数据走 master→slave 的**控制连接**，而 eBPF 实时转发走 proxy 的
 * **另一条** TCP 连接（slave 的 port+1），两条连接之间没有任何顺序保证。不设屏障时：
 *
 *   Slave 缺 [1000,1200)，master 正在 replay 旧命令 INCR a；
 *   与此同时新客户端执行 DEL a，经 proxy 实时通道直达 Slave。
 *   Slave 完全可能先收到 DEL a 再收到 INCR a —— 最终状态错误（非幂等命令无法自愈）。
 *
 * 因此凡是"绕过 proxy、直接把数据补到 Slave"的动作（FULLRESYNC 快照、partial resync
 * 的 backlog replay）都必须：
 *   ① 先让 proxy 进入 BUFFERING（本函数握手，且必须早于 CAPTURE_ENABLE=1）
 *   ② 补数据（快照/replay）
 *   ③ 等 Slave 确认已经追上（REPLACK applied >= catchup_end）再放行 → proxy flush cache
 *
 * 注意 ① 与 CAPTURE_ENABLE 的先后：若先开捕获再立屏障，这段窗口里捕获到的写会被
 * proxy 实时转发出去，随后 replay 又会带上同一条命令 → 重复应用。
 */
static int g_barrier_active = 0;
/* 放行条件：0 = 由 REPLDONE 放行（全量同步）；1 = 由 Slave 的 REPLACK 放行（partial resync）。
 * 全量**绝不能**用"applied >= catchup_end"来放行：Slave 一收到 +FULLRESYNC 就把自己的
 * applied 设成快照基准 offset，此时它还在往临时文件里写快照。若据此提前放行，proxy 会
 * 立刻 flush cache，而 Slave 正处于 loading_fullsync 状态 —— 它会把收到的任何非控制行
 * 当成 KVSD 字节**写进快照文件**，直接损坏全量数据（实测 replay_dump_file 报
 * "invalid engine_id 64 at pos 8"）。只有 REPLDONE 才代表"快照已加载完成"。 */
static int g_barrier_ack_gated = 0;
static unsigned long long g_barrier_end_offset = 0;   /* catchup_end = 立屏障时的 master_offset */
static long long g_barrier_deadline_ms = 0;           /* 等 Slave ack 的兜底期限，0=不设 */
#define REPL_BARRIER_WAIT_MS      3000    /* 等 proxy 应答 BUFFERING */
#define REPL_BARRIER_HOLD_MAX_MS 15000    /* 等 Slave 追上（兜底，防永久 BUFFERING） */

static int repl_proxy_barrier_needed(void) {
    if (!g_cfg.ebpf_pin_path[0]) return 0;
    return strcasecmp(g_cfg.repl_realtime_transport, "ebpf+tcp") == 0 ||
           strcasecmp(g_cfg.repl_transport_backend, "ebpf+tcp") == 0;
}

/* 立屏障并返回 catchup_end（= 屏障时刻的 master_offset，即本 session 需要补齐的上界）。
 * 返回 0 成功；-1 表示 proxy 未确认（fail-closed，调用方必须放弃本次 resync）。 */
static int repl_barrier_begin(unsigned long long *catchup_end) {
    if (catchup_end) *catchup_end = repl_master_offset();
    if (!repl_proxy_barrier_needed()) return 0;      /* 非 ebpf+tcp：同一连接有序，无需屏障 */
    if (g_barrier_active) {
        /* 屏障已立（如全量进行中又来了一个 Slave）。不复用旧 catchup_end：快照 dump 的是
         * "现在"的引擎状态，边界必须取当前 offset，否则 Slave 会以为要补 [旧边界, 现在)
         * 而这部分早已在 dump 里 → 重复应用。 */
        if (catchup_end) *catchup_end = repl_master_offset();
        return 0;
    }

    /* 先清应答再拉请求：proxy 按 [3] 的**边沿**应答，清 [12] 保证不会把上一轮的
     * 陈旧应答误判成本轮成功。 */
    repl_ebpf_client_ctl_set(KVS_CTL_PROXY_STATE, 0);
    repl_ebpf_client_ctl_set(KVS_CTL_FULLSYNC_STATE, 1);

    long long deadline = kvs_now_ms() + REPL_BARRIER_WAIT_MS;
    int ok = 0;
    while (kvs_now_ms() < deadline) {
        if (repl_ebpf_client_ctl_get(KVS_CTL_PROXY_STATE) == 1) { ok = 1; break; }
        usleep(2000);
    }
    if (!ok) {
        repl_ebpf_client_ctl_set(KVS_CTL_FULLSYNC_STATE, 0);   /* 回滚，避免遗留 BUFFERING */
        fprintf(stderr, "master: ebpf-proxy barrier NOT confirmed within %dms — "
                        "fail-closed, refusing to resync (proxy dead/hung?)\n",
                REPL_BARRIER_WAIT_MS);
        return -1;
    }
    g_barrier_active = 1;
    g_barrier_ack_gated = 0;                 /* 默认全量语义：由 REPLDONE 放行 */
    g_barrier_end_offset = repl_master_offset();
    g_barrier_deadline_ms = 0;
    if (catchup_end) *catchup_end = g_barrier_end_offset;
    fprintf(stderr, "master: proxy barrier up (BUFFERING confirmed), catchup_end=%llu\n",
            g_barrier_end_offset);
    return 0;
}

/* 放行：proxy 切回 FORWARDING 并 flush 本 session 的 cache。 */
static void repl_barrier_release(void) {
    if (!g_barrier_active) return;
    g_barrier_active = 0;
    g_barrier_ack_gated = 0;
    g_barrier_deadline_ms = 0;
    if (repl_proxy_barrier_needed()) {
        fprintf(stderr, "master: proxy barrier down — proxy will flush its cache\n");
        repl_ebpf_client_ctl_set(KVS_CTL_FULLSYNC_STATE, 0);
    }
}

int repl_barrier_pending(void) { return g_barrier_active; }

/* partial resync 专用：改为"等 Slave 的 REPLACK 追上 catchup_end 再放行"，
 * 并启用超时兜底（避免 Slave 无写流量不发 REPLACK 时永久 BUFFERING）。 */
static void repl_barrier_gate_on_ack(void) {
    if (!g_barrier_active) return;
    g_barrier_ack_gated = 1;
    g_barrier_deadline_ms = kvs_now_ms() + REPL_BARRIER_HOLD_MAX_MS;
}

/* Slave 的 REPLACK 反馈：只有 applied 真正追上 catchup_end 才能放行 —— 这样代理
 * flush cache 一定发生在 replay 数据被 Slave 应用**之后**，跨连接的重排被彻底消除。 */
void repl_barrier_note_applied(unsigned long long applied_offset) {
    if (!g_barrier_active || !g_barrier_ack_gated) return;   /* 全量由 REPLDONE 放行 */
    if (applied_offset >= g_barrier_end_offset) {
        fprintf(stderr, "master: slave caught up (applied=%llu >= catchup_end=%llu), "
                        "releasing proxy barrier\n", applied_offset, g_barrier_end_offset);
        repl_barrier_release();
    }
}

/* 兜底：Slave 长时间不 ack（例如 replay 为空、或链路静默）时强制放行，
 * 避免 proxy 永久停在 BUFFERING 而增量不再转发。由 reactor 的 100ms 定时块调用。 */
void repl_barrier_tick(void) {
    if (!g_barrier_active || !g_barrier_ack_gated || g_barrier_deadline_ms == 0) return;
    if (kvs_now_ms() < g_barrier_deadline_ms) return;
    fprintf(stderr, "master: proxy barrier held >%dms without slave ack — "
                    "force releasing (slave may have nothing to ack)\n",
            REPL_BARRIER_HOLD_MAX_MS);
    repl_barrier_release();
}

/* 生成快照并启动全量传输。snap_base_offset 由调用方在**屏障建立后**取好传入
 * （见 REPLSYNC handler）：屏障必须早于 CAPTURE_ENABLE，边界才干净。 */
static int queue_snapshot(conn_t *c, unsigned long long snap_base_offset) {
    char hdr[128];
    char tmp_path[512];
    int fd;
    size_t total = 0;
    size_t total_bytes = 0;
    int ret = -1;
    int rdma_ok = 0;

    repl_rdma_log("queue_snapshot - begin replid=%s offset=%llu", repl_master_id(), repl_master_offset());

    /* 全量同步期间抑制增量广播，避免 KVSD 数据流被 repl_broadcast 写穿插 */
    g_repl_fullsync_in_progress = 1;

    /* backlog 以 snap_base_offset 为基点重建（§6/§10）：快照已经覆盖 base 之前的
     * 全部状态，旧历史留着只会让 partial resync 误判或与 proxy_cache 双重回放。 */
    repl_backlog_reset(snap_base_offset);
    fprintf(stderr, "master: fullsync boundary snap_base_offset=%llu "
                    "backlog reset (session=%llu)\n",
            snap_base_offset, repl_session_id());

    /* 尝试启动 RDMA */
    if (!strcasecmp(g_cfg.repl_fullsync_transport, "rdma")) {
        if (repl_rdma_start_fullsync(c) == 0) {
            rdma_ok = 1;
            repl_rdma_log("queue_snapshot - RDMA started for fullsync");
        } else {
            repl_rdma_log("queue_snapshot - RDMA start failed, falling back to TCP");
        }
    }

    /* Generate KVSD snapshot to temp file */
    snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.tmp.%ld", g_cfg.dump_path, (long)getpid());
    fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) goto out;
    if (kvs_dump_to_fd(fd, snap_base_offset) != 0) { close(fd); unlink(tmp_path); goto out; }
    total_bytes = (size_t)lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    uint64_t transfer_id = 0;
    if (rdma_ok) {
        extern uint64_t repl_fullsync_next_transfer_id(void);
        extern void repl_rdma_set_transfer_id(uint64_t tid);
        extern int repl_rdma_set_write_total_size(size_t sz);
        transfer_id = repl_fullsync_next_transfer_id();
        c->repl_transfer_id = transfer_id;
        repl_rdma_set_transfer_id(transfer_id);
        repl_rdma_set_write_total_size(total_bytes);
    }

    int n = snprintf(hdr, sizeof(hdr), "+FULLRESYNC %s %llu %zu %llu %s\r\n",
                     repl_master_id(), snap_base_offset, total_bytes,
                     (unsigned long long)transfer_id, "rdma-write");

    repl_note_send_context("fullsync-header", (size_t)n, snap_base_offset, (unsigned char *)hdr);

    /* 发送 FULLRESYNC header —— 直接写 TCP 控制面（c->fd）。
     * 不能走 RDMA SEND：slave 的 RDMA 可能尚未 ESTABLISHED（rxe 事件延迟），
     * SEND 会丢 header 导致 slave 从不处理 FULLRESYNC、也无 FULLRESYNCWR。
     * 也不能 queue_bytes：reactor 被 queue_snapshot 阻塞，ring buffer 不会 flush。 */
    {
        size_t hoff = 0;
        int hs = 0;
        while (hoff < (size_t)n && hs < 200) {
            ssize_t w = send(c->fd, hdr + hoff, (size_t)n - hoff, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (w > 0) { hoff += (size_t)w; continue; }
            if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { usleep(1000); hs++; continue; }
            break;
        }
        if (hoff < (size_t)n) {
            repl_rdma_log("queue_snapshot - header send failed");
            close(fd);
            unlink(tmp_path);
            goto out;
        }
    }

    /* 数据发送：优先单边 RDMA WRITE（等 FULLRESYNCWR 握手），否则 TCP sendfile 回退。
     * WRITE 路径复用 repl_send_chunked → flush_batch（首包 SEND 预热 rxe + 其余 WRITE，
     * 数据经 slot buffer memcpy 发出，非零拷贝——soft-RoCE 不支持文件映射 WRITE 源）。 */
    {
        int data_sent = 0;
        int wr_wait = -1;
        int wr_handshake_ok = 0;
        if (rdma_ok) {
            extern int repl_fullsync_wait_remote_mr(conn_t *c, int timeout_ms);
            wr_wait = repl_fullsync_wait_remote_mr(c, 8000);
            wr_handshake_ok = (wr_wait == 0);
            repl_rdma_log("queue_snapshot - wait_remote_mr=%d", wr_wait);
        }
        if (rdma_ok && wr_wait == 0) {
            /* WRITE 路径：read dump → repl_send_chunked（flush_batch 首包 SEND 预热 + 其余 WRITE） */
            size_t chunk_cap = repl_rdma_effective_chunk_size();
            if (chunk_cap < 1024) chunk_cap = 1024;
            unsigned char *buf = (unsigned char *)kvs_malloc(chunk_cap);
            if (!buf) goto out;
            size_t r;
            int send_ok = 1;
            lseek(fd, 0, SEEK_SET);
            while ((r = (size_t)read(fd, buf, chunk_cap)) > 0) {
                if (repl_send_chunked(c, buf, r) != 0) { send_ok = 0; break; }
            }
            kvs_free(buf);
            if (send_ok) {
                data_sent = 1;
                total = total_bytes;
                /* 等全部 WRITE 完成（slave 目标数据落定），再经 TCP 发完成信号，
                 * 兜底 rxe 下 RDMA IMM 事件可能丢失导致 slave 不 finalize。 */
                extern int repl_rdma_wait_all_sends(int timeout_ms);
                extern void repl_fullsync_send_end(int tcp_fd, uint64_t tid);
                repl_rdma_wait_all_sends(15000);
                if (transfer_id) repl_fullsync_send_end(c->fd, transfer_id);
            } else {
                repl_rdma_log("queue_snapshot - WRITE send failed, sendfile fallback");
            }
        }
        if (!data_sent) {
            extern void repl_fullsync_abort_master(int tcp_fd, uint64_t tid);
            extern int repl_fullsync_sendfile(int tcp_fd, int dump_fd, size_t total_bytes);
            extern void repl_rdma_master_transfer_cleanup(conn_t *c);
            /* 仅当 WRITE 握手已成功（slave 确实建了 file-backed 目标）才发 abort 清理；
             * 若握手从未成功（如 cap 拒绝/超时），slave 无目标，不发 abort 以免污染数据流。 */
            if (transfer_id && wr_handshake_ok)
                repl_fullsync_abort_master(c->fd, transfer_id);
            repl_rdma_master_transfer_cleanup(c);
            if (repl_fullsync_sendfile(c->fd, fd, total_bytes) == 0) {
                data_sent = 1;
                total = total_bytes;
            }
        }
        if (!data_sent) {
            repl_rdma_log("queue_snapshot - data send failed");
            close(fd);
            unlink(tmp_path);
            goto out;
        }
    }
    close(fd);
    unlink(tmp_path);


    repl_note_fullsync(total);
    c->repl_offset_sent = repl_master_offset();
    c->repl_last_send_ms = kvs_now_ms();

    /* 移除此处 gap 回放：gap 数据在 slave REPLDONE 后统一回放（避免与 REPLDONE handler 双发）。 */

    /* 清 WRITE transfer 状态（不关 RDMA 连接）：之后 REPLDONE handler 的 gap 回放
     * 走 SEND/TCP，绝不走 WRITE（slave 的 file-backed MR 已随 IMM 释放）。 */
    if (rdma_ok) {
        extern void repl_rdma_master_transfer_cleanup(conn_t *c);
        repl_rdma_master_transfer_cleanup(c);
    }

    /* RDMA 保持运行，不在此关闭。从机处理完 snapshot 后发送 REPLDONE，
     * 主机在 REPLDONE handler 中关闭 RDMA 并开启增量同步。 */
    (void)rdma_ok;
    ret = 0;
    repl_rdma_log("queue_snapshot - complete snapshot_bytes=%zu repl_offset=%llu", total, repl_master_offset());
out:
    return ret;
}

static int is_readonly_slave_blocked(const char *cmd) {
    return strcmp(cmd, "GET") && strcmp(cmd, "MGET") && strcmp(cmd, "TTL") && strcmp(cmd, "EXIST") && strcmp(cmd, "OWNER")
        && strcmp(cmd, "RGET") && strcmp(cmd, "RMGET") && strcmp(cmd, "RTTL") && strcmp(cmd, "REXIST")
        && strcmp(cmd, "HGET") && strcmp(cmd, "HMGET") && strcmp(cmd, "HTTL") && strcmp(cmd, "HEXIST")
        && strcmp(cmd, "XGET") && strcmp(cmd, "XMGET") && strcmp(cmd, "XTTL") && strcmp(cmd, "XEXIST")
        && strcmp(cmd, "INFO") && strcmp(cmd, "MEMSTAT") && strcmp(cmd, "MEMORY_PURGE")
        && strcmp(cmd, "PING") && strcmp(cmd, "ECHO") && strcmp(cmd, "QUIT")
        && strcmp(cmd, "SLAVEOF") && strcmp(cmd, "ROLE")
        && strcmp(cmd, "DOCGET") && strcmp(cmd, "DOCGETALL") && strcmp(cmd, "DOCEXIST") && strcmp(cmd, "DOCCOUNT");
}

static int is_write_cmd(const char *cmd) {
    const char *writes[] = {
        "SET","MSET","MOD","DEL","EXPIRE","PERSIST","SETEX",
        "RSET","RMSET","RMOD","RDEL","REXPIRE","RPERSIST","RSETEX",
        "HSET","HMSET","HMOD","HDEL","HEXPIRE","HPERSIST","HSETEX",
        "XSET","XMSET","XMOD","XDEL","XEXPIRE","XPERSIST","XSETEX",
        "LOCK","UNLOCK","RENEW",
        "DOCSET","DOCDEL","DOCDROP",
        NULL
    };
    for (int i = 0; writes[i]; ++i) if (!strcmp(cmd, writes[i])) return 1;
    return 0;
}

static int cmd_engine(const char *cmd) {
    if (cmd[0] == 'R') return KVS_ENGINE_RBTREE;
    if (cmd[0] == 'H') return KVS_ENGINE_HASH;
    if (cmd[0] == 'X') return KVS_ENGINE_SKIPTABLE;
    return KVS_ENGINE_ARRAY;
}

static const char *strip_prefix(const char *cmd) {
    if (cmd[0] == 'R' || cmd[0] == 'H' || cmd[0] == 'X')
        return cmd + 1;
    return cmd;
}

static int engine_exist(int engine, char *key) {
    switch (engine) {
        case KVS_ENGINE_ARRAY: return kvs_array_exist(&global_array, key);
        case KVS_ENGINE_RBTREE: return kvs_rbtree_exist(&global_rbtree, key);
        case KVS_ENGINE_HASH: return kvs_hash_exist(&global_hash, key);
        case KVS_ENGINE_SKIPTABLE: return kvs_skiptable_exist(&global_skiptable, key);
        default: return 1;
    }
}
static char *engine_get(int engine, char *key) {
    switch (engine) {
        case KVS_ENGINE_ARRAY: return kvs_array_get(&global_array, key);
        case KVS_ENGINE_RBTREE: return kvs_rbtree_get(&global_rbtree, key);
        case KVS_ENGINE_HASH: return kvs_hash_get(&global_hash, key);
        case KVS_ENGINE_SKIPTABLE: return kvs_skiptable_get(&global_skiptable, key);
        default: return NULL;
    }
}
static int engine_set(int engine, char *key, char *value) {
    switch (engine) {
        case KVS_ENGINE_ARRAY: return kvs_array_set(&global_array, key, value);
        case KVS_ENGINE_RBTREE: return kvs_rbtree_set(&global_rbtree, key, value);
        case KVS_ENGINE_HASH: return kvs_hash_set(&global_hash, key, value);
        case KVS_ENGINE_SKIPTABLE: return kvs_skiptable_set(&global_skiptable, key, value);
        default: return -1;
    }
}
static int engine_mod(int engine, char *key, char *value) {
    switch (engine) {
        case KVS_ENGINE_ARRAY: return kvs_array_mod(&global_array, key, value);
        case KVS_ENGINE_RBTREE: return kvs_rbtree_mod(&global_rbtree, key, value);
        case KVS_ENGINE_HASH: return kvs_hash_mod(&global_hash, key, value);
        case KVS_ENGINE_SKIPTABLE: return kvs_skiptable_mod(&global_skiptable, key, value);
        default: return -1;
    }
}
static int engine_del(int engine, char *key) {
    switch (engine) {
        case KVS_ENGINE_ARRAY: return kvs_array_del(&global_array, key);
        case KVS_ENGINE_RBTREE: return kvs_rbtree_del(&global_rbtree, key);
        case KVS_ENGINE_HASH: return kvs_hash_del(&global_hash, key);
        case KVS_ENGINE_SKIPTABLE: return kvs_skiptable_del(&global_skiptable, key);
        default: return -1;
    }
}
static int try_expire(int engine, char *key) {
    if (kvs_expire_is_expired(&global_expire, engine, key)) {
        engine_del(engine, key);
        kvs_expire_del(&global_expire, engine, key);
        return 1;
    }
    return 0;
}

static int resp_array_from_values(char *out, size_t cap, int count, char **values);

static void repl_log_applied_command(const char *cmd, int argc, char **argv, size_t rawlen) {
    const char *key = (argc >= 2 && argv && argv[1]) ? argv[1] : "";
    unsigned long long before = repl_slave_offset();
    unsigned long long after = before + (unsigned long long)rawlen;
    repl_rdma_log("slave_apply_cmd - cmd=%s key=%s rawlen=%zu offset_before=%llu offset_after=%llu",
        cmd ? cmd : "?", key, rawlen, before, after);
    (void)argc;
}

static int repl_is_soak_key(const char *key) {
    return key && strstr(key, "rdma:soak:key:") == key;
}

static int engine_upsert(int engine, char *key, char *value) {
    int exists;
    if (!key || !value) return -1;
    try_expire(engine, key);
    exists = engine_exist(engine, key) == 0;
    if (exists) {
        if (engine_mod(engine, key, value) != 0) return -1;
        kvs_expire_persist(&global_expire, engine, key);
        return 0;
    }
    return engine_set(engine, key, value);
}

static int handle_multi_set(int engine, int argc, char **argv) {
    if (argc < 3 || (argc % 2) == 0) return -1;
    for (int i = 1; i < argc; i += 2) {
        if (!argv[i] || !argv[i + 1]) return -1;
    }
    for (int i = 1; i < argc; i += 2) {
        if (engine_upsert(engine, argv[i], argv[i + 1]) != 0) return -1;
    }
    return 0;
}

static int handle_multi_get(int engine, int argc, char **argv, char *resp, size_t cap) {
    char *values[32] = {0};
    if (argc < 2 || argc > 32) return -1;
    for (int i = 1; i < argc; ++i) {
        try_expire(engine, argv[i]);
        values[i - 1] = engine_get(engine, argv[i]);
    }
    return resp_array_from_values(resp, cap, argc - 1, values);
}

static int lock_acquire(int engine, char *key, char *token, long long ttl_ms) {
    if (!key || !token || ttl_ms <= 0) return -1;
    try_expire(engine, key);
    if (engine_exist(engine, key) == 0) return 1;
    if (engine_set(engine, key, token) != 0) return -1;
    if (kvs_expire_set(&global_expire, engine, key, ttl_ms) != 0) {
        engine_del(engine, key);
        kvs_expire_del(&global_expire, engine, key);
        return -1;
    }
    return 0;
}

static int lock_release(int engine, char *key, char *token) {
    char *cur;
    if (!key || !token) return -1;
    try_expire(engine, key);
    cur = engine_get(engine, key);
    if (!cur) return 1;
    if (strcmp(cur, token) != 0) return 1;
    if (engine_del(engine, key) != 0) return -1;
    kvs_expire_del(&global_expire, engine, key);
    return 0;
}

static int lock_renew(int engine, char *key, char *token, long long ttl_ms) {
    char *cur;
    if (!key || !token || ttl_ms <= 0) return -1;
    try_expire(engine, key);
    cur = engine_get(engine, key);
    if (!cur) return 1;
    if (strcmp(cur, token) != 0) return 1;
    return kvs_expire_set(&global_expire, engine, key, ttl_ms);
}

static int build_memstat_text(char *buf, size_t cap) {
    kvs_mem_stats_t st;
    if (kvs_mem_get_stats(&st) != 0) return -1;

    int n = snprintf(
        buf, cap,
        "backend=%s\n"
        "backend_id=%d\n"
        "initialized=%d\n"
        "alloc_calls=%llu\n"
        "calloc_calls=%llu\n"
        "realloc_calls=%llu\n"
        "free_calls=%llu\n"
        "small_max_size=%zu\n"
        "small_alloc_calls=%llu\n"
        "small_free_calls=%llu\n"
        "current_small_inuse=%llu\n"
        "peak_small_inuse=%llu\n"
        "total_small_page_bytes=%llu\n"
        "large_alloc_calls=%llu\n"
        "large_free_calls=%llu\n"
        "fallback_alloc_calls=%llu\n"
        "fallback_free_calls=%llu\n"
        "current_large_inuse_bytes=%llu\n"
        "peak_large_inuse_bytes=%llu\n"
        "current_fallback_inuse_bytes=%llu\n"
        "peak_fallback_inuse_bytes=%llu\n"
        "total_large_map_bytes=%llu\n"
        "active_large_map_bytes=%llu\n"
        "peak_active_large_map_bytes=%llu\n"
        "current_requested_bytes=%llu\n"
        "current_allocated_bytes=%llu\n"
        "internal_fragment_bytes=%llu\n"
        "internal_fragment_rate=%.6f\n"
        "small_page_used_bytes=%llu\n"
        "page_utilization=%.6f\n",
        st.backend_name ? st.backend_name : "unknown",
        st.backend_id,
        st.initialized,
        st.alloc_calls,
        st.calloc_calls,
        st.realloc_calls,
        st.free_calls,
        st.small_max_size,
        st.small_alloc_calls,
        st.small_free_calls,
        st.current_small_inuse,
        st.peak_small_inuse,
        st.total_small_page_bytes,
        st.large_alloc_calls,
        st.large_free_calls,
        st.fallback_alloc_calls,
        st.fallback_free_calls,
        st.current_large_inuse_bytes,
        st.peak_large_inuse_bytes,
        st.current_fallback_inuse_bytes,
        st.peak_fallback_inuse_bytes,
        st.total_large_map_bytes,
        st.active_large_map_bytes,
        st.peak_active_large_map_bytes,
        st.current_requested_bytes,
        st.current_allocated_bytes,
        st.internal_fragment_bytes,
        st.internal_fragment_ppm / 1000000.0,
        st.small_page_used_bytes,
        st.page_utilization_ppm / 1000000.0
    );
    if (n < 0 || (size_t)n >= cap) return -1;

    size_t pos = (size_t)n;
    for (size_t i = 0; i < st.class_count && i < 16; ++i) {
        n = snprintf(
            buf + pos, cap - pos,
            "class_%zu_size=%zu\nclass_%zu_pages=%zu\nclass_%zu_total_chunks=%zu\nclass_%zu_free_chunks=%zu\nclass_%zu_page_bytes=%zu\n",
            i, st.class_sizes[i],
            i, st.class_page_count[i],
            i, st.class_total_chunks[i],
            i, st.class_free_chunks[i],
            i, st.class_bytes_in_pages[i]
        );
        if (n < 0 || (size_t)n >= cap - pos) return -1;
        pos += (size_t)n;
    }
    return (int)pos;
}
static void kvs_ascii_upper(char *s) {
    if (!s) return;
    for (; *s; ++s) if (*s >= 'a' && *s <= 'z') *s -= 32;
}

/* Fast integer-to-string conversion — avoids glibc snprintf overhead in hot paths.
 * Based on InazumaPlasma's fast_itoa. */
static inline int fast_itoa(long long val, char *buf) {
    if (val == 0) { buf[0] = '0'; return 1; }
    int i = 0, sign = 0;
    unsigned long long uval;
    if (val < 0) { sign = 1; buf[0] = '-'; uval = (unsigned long long)(-val); }
    else { uval = (unsigned long long)val; }
    char temp[32];
    while (uval > 0) { temp[i++] = (char)((uval % 10) + '0'); uval /= 10; }
    for (int j = 0; j < i; j++) buf[sign + j] = temp[i - 1 - j];
    return sign + i;
}

static inline int fast_uitoa(unsigned long long val, char *buf) {
    if (val == 0) { buf[0] = '0'; return 1; }
    int i = 0;
    char temp[32];
    while (val > 0) { temp[i++] = (char)((val % 10) + '0'); val /= 10; }
    for (int j = 0; j < i; j++) buf[j] = temp[i - 1 - j];
    return i;
}

static void repl_collect_replica_ack_stats(unsigned long long *max_applied, unsigned long long *max_durable, long long *min_ack_age_ms, int *replicas) {
    unsigned long long applied = 0;
    unsigned long long durable = 0;
    long long min_age = -1;
    int count = 0;
    pthread_mutex_lock(&g_repl_lock);
    for (conn_t *rc = g_replicas; rc; rc = rc->next_replica) {
        long long ack_age = (rc->repl_last_ack_ms > 0) ? (kvs_now_ms() - rc->repl_last_ack_ms) : -1;
        count++;
        if (rc->repl_applied_offset_ack > applied) applied = rc->repl_applied_offset_ack;
        if (rc->repl_durable_offset_ack > durable) durable = rc->repl_durable_offset_ack;
        if (ack_age >= 0 && (min_age < 0 || ack_age < min_age)) min_age = ack_age;
    }
    pthread_mutex_unlock(&g_repl_lock);
    if (max_applied) *max_applied = applied;
    if (max_durable) *max_durable = durable;
    if (min_ack_age_ms) *min_ack_age_ms = min_age;
    if (replicas) *replicas = count;
}

static int resp_array_header(char *out, size_t cap, int count) {
    return snprintf(out, cap, "*%d\r\n", count);
}

static int resp_empty_array(char *out, size_t cap) {
    return snprintf(out, cap, "*0\r\n");
}

static int resp_array_append_bulk_or_null(char *out, size_t cap, size_t *pos, const char *s) {
    int n;
    if (!out || !pos || *pos >= cap) return -1;
    if (s) n = resp_bulk(out + *pos, cap - *pos, s, strlen(s));
    else n = resp_null_bulk(out + *pos, cap - *pos);
    if (n < 0 || (size_t)n > cap - *pos) return -1;
    *pos += (size_t)n;
    return 0;
}

static int resp_array_from_values(char *out, size_t cap, int count, char **values) {
    size_t pos = 0;
    int n = resp_array_header(out + pos, cap - pos, count);
    if (n < 0 || (size_t)n >= cap - pos) return -1;
    pos += (size_t)n;
    for (int i = 0; i < count; ++i) {
        if (resp_array_append_bulk_or_null(out, cap, &pos, values[i]) != 0) return -1;
    }
    return (int)pos;
}

static int resp_array_two_bulk(char *out, size_t cap, const char *a, const char *b) {
    size_t pos = 0;
    int n = resp_array_header(out + pos, cap - pos, 2);
    if (n < 0 || (size_t)n >= cap - pos) return -1;
    pos += (size_t)n;
    n = resp_bulk(out + pos, cap - pos, a, strlen(a));
    if (n < 0 || (size_t)n >= cap - pos) return -1;
    pos += (size_t)n;
    n = resp_bulk(out + pos, cap - pos, b, strlen(b));
    if (n < 0 || (size_t)n >= cap - pos) return -1;
    pos += (size_t)n;
    return (int)pos;
}

static int split_inline_argv(char *line, char **argv, int maxargc) {
    int argc = 0;
    char *p = line;
    while (*p && argc < maxargc) {
        while (*p && isspace((unsigned char)*p)) ++p;
        if (!*p) break;
        argv[argc++] = p;
        while (*p && !isspace((unsigned char)*p)) ++p;
        if (!*p) break;
        *p++ = '\0';
    }
    return argc;
}

int handle_parsed_command(conn_t *c, int argc, char **argv, size_t *argl, const unsigned char *raw, size_t rawlen, int from_replication) {

    int rc_ret = 0;
    int n = 0;

    /* ── AUTH 门禁：配置 requirepass 后，未通过鉴权的普通客户端只放行 AUTH/QUIT ──
     * from_replication 的内部复制连接不受限（复制回放无需客户端鉴权） */
    if (c && !from_replication && g_cfg.requirepass[0] && !c->authed) {
        int is_auth = argc >= 1 && argl[0] == 4
            && ((argv[0][0]|32)=='a' && (argv[0][1]|32)=='u'
                && (argv[0][2]|32)=='t' && (argv[0][3]|32)=='h');
        int is_quit = argc >= 1 && argl[0] == 4
            && ((argv[0][0]|32)=='q' && (argv[0][1]|32)=='u'
                && (argv[0][2]|32)=='i' && (argv[0][3]|32)=='t');
        if (!is_auth && !is_quit) {
            queue_bytes(c, (unsigned char *)"-NOAUTH Authentication required.\r\n",
                        sizeof("-NOAUTH Authentication required.\r\n") - 1);
            return 0;
        }
    }

    /* ── Fast path: ECHO / PING ──────────────────────────
     * |32 trick → case-insensitive ASCII compare.
     * Uses argv/argl pointers (may point to buf, not scratch).
     * Builds the full RESP bulk string in one stack buffer,
     * then single queue_bytes call (3→1 call reduction) */
    if (likely(c) && argc >= 1 && argl[0] == 4) {
        const unsigned char *s = (const unsigned char *)argv[0];
        if ((s[0]|32)=='e' && (s[1]|32)=='c' && (s[2]|32)=='h' && (s[3]|32)=='o') {
            if (argc == 2 && argl[1] > 0) {
                /* stack buffer: '$' + 20digits + '\r\n' + payload + '\r\n' */
                size_t total = 1 + 20 + 2 + argl[1] + 2;
                unsigned char resp_stack[256];
                unsigned char *resp = resp_stack;
                if (unlikely(total > sizeof(resp_stack))) {
                    resp = (unsigned char *)kvs_malloc(total);
                    if (!resp) return -1;
                }
                int pos = 0;
                resp[pos++] = '$';
                pos += fast_uitoa((unsigned long long)argl[1], (char *)resp + pos);
                resp[pos++] = '\r'; resp[pos++] = '\n';
                memcpy(resp + pos, argv[1], argl[1]); pos += (int)argl[1];
                resp[pos++] = '\r'; resp[pos++] = '\n';
                queue_bytes(c, resp, (size_t)pos);
                if (unlikely(resp != resp_stack)) kvs_free(resp);
                return 0;
            }
        }
        if ((s[0]|32)=='p' && (s[1]|32)=='i' && (s[2]|32)=='n' && (s[3]|32)=='g') {
            if (argc >= 2 && argl[1] > 0) {
                size_t total = 1 + 20 + 2 + argl[1] + 2;
                unsigned char resp_stack[256];
                unsigned char *resp = resp_stack;
                if (unlikely(total > sizeof(resp_stack))) {
                    resp = (unsigned char *)kvs_malloc(total);
                    if (!resp) return -1;
                }
                int pos = 0;
                resp[pos++] = '$';
                pos += fast_uitoa((unsigned long long)argl[1], (char *)resp + pos);
                resp[pos++] = '\r'; resp[pos++] = '\n';
                memcpy(resp + pos, argv[1], argl[1]); pos += (int)argl[1];
                resp[pos++] = '\r'; resp[pos++] = '\n';
                queue_bytes(c, resp, (size_t)pos);
                if (unlikely(resp != resp_stack)) kvs_free(resp);
            } else {
                queue_bytes(c, (unsigned char *)"+PONG\r\n", 7);
            }
            return 0;
        }
    }

    size_t _resp_sz = BUFFER_CAP;  /* 响应缓冲对齐 BUFFER_CAP：resp_* 函数用 BUFFER_CAP 作 cap，4KB 栈缓冲会溢出（大 value 响应写穿栈） */
    char resp_buf[_resp_sz];
    char *resp = resp_buf;

    if (argc <= 0) {
        rc_ret = -1;
        goto out;
    }

    kvs_ascii_upper(argv[0]);
    const char *cmd = argv[0];

    /* ebpf+tcp: proxy 直连 slave 转发写命令，需放行 */
    if (g_cfg.role == ROLE_SLAVE && !from_replication
        && !(c && !strcasecmp(repl_realtime_transport_name(), "ebpf+tcp"))
        && is_readonly_slave_blocked(cmd)) {
        n = resp_error(resp, BUFFER_CAP, "read only slave");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }

    /* Fast path: 高频 4 字符 hash 命令 HSET/HGET/HDEL/HMOD 直达引擎分派，
     * 绕开 ~35 次 strcmp 控制命令链（HSET P=160 分派热点 handle_parsed_command 40% + strcmp 9%）。
     * 语义与正常落空到引擎区完全一致（这些命令本就不匹配任何控制命令），只省 strcmp。 */
    if (argl[0] == 4 && cmd[0] == 'H') {
        const unsigned char *sc = (const unsigned char *)cmd;
        if ((sc[1]=='S' && sc[2]=='E' && sc[3]=='T') ||
            (sc[1]=='G' && sc[2]=='E' && sc[3]=='T') ||
            (sc[1]=='D' && sc[2]=='E' && sc[3]=='L') ||
            (sc[1]=='M' && sc[2]=='O' && sc[3]=='D')) {
            goto engine_dispatch;
        }
    }

    if (!strcmp(cmd, "PING")) {
        if (argc >= 2) n = resp_bulk(resp, BUFFER_CAP, argv[1], argl[1]);
        else n = resp_simple_string(resp, BUFFER_CAP, "PONG");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "ECHO") && argc == 2) {
        n = resp_bulk(resp, BUFFER_CAP, argv[1], argl[1]);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "QUIT")) {
        n = resp_simple_string(resp, BUFFER_CAP, "OK");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "AUTH")) {
        if (argc != 2) {
            n = resp_error(resp, BUFFER_CAP, "wrong number of arguments");
        } else if (!g_cfg.requirepass[0]) {
            n = resp_error(resp, BUFFER_CAP, "Client sent AUTH, but no password is set");
        } else if ((size_t)argl[1] != strlen(g_cfg.requirepass)
                   || strncmp(argv[1], g_cfg.requirepass, (size_t)argl[1]) != 0) {
            n = resp_error(resp, BUFFER_CAP, "invalid password");
        } else {
            if (c) c->authed = 1;
            n = resp_simple_string(resp, BUFFER_CAP, "OK");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "COMMAND")) {
        n = resp_empty_array(resp, BUFFER_CAP);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "CLIENT")) {
        /* 子命令大小写不敏感：go-redis 等客户端会发小写 setinfo，大小写敏感会导致 RESP 回复错位 */
        if (argc >= 2 && !strcasecmp(argv[1], "SETINFO")) {
            n = resp_simple_string(resp, BUFFER_CAP, "OK");
        } else {
            n = resp_array_two_bulk(resp, BUFFER_CAP, "id", "1");
            if (n < 0) n = resp_error(resp, BUFFER_CAP, "client subcommand failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "HELLO")) {
        n = resp_error(resp, BUFFER_CAP, "NOPROTO unsupported RESP version");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "REPLSYNC")) {
        /* slave 端通过 parse_resp_stream(NULL, ..., 1) 处理复制数据时，
         * 不应处理 REPLSYNC（缓存回放可能包含被 BPF 误捕获的 REPLSYNC） */
        if (from_replication && !c) goto out;
        const char *req_replid = argc >= 2 ? argv[1] : "?";
        unsigned long long req_offset = argc >= 3 ? (unsigned long long)strtoull(argv[2], NULL, 10) : 0;
        unsigned long long req_durable = argc >= 4 ? (unsigned long long)strtoull(argv[3], NULL, 10) : req_offset;
        /* Use req_offset for backlog check (data the slave needs).
         * req_durable may lag behind req_offset due to lazy AOF fsync,
         * but that doesn't prevent partial resync since the master
         * can replay from req_offset onward. */
        int can_continue = (argc >= 3 && repl_backlog_can_continue(req_replid, req_offset));
        repl_rdma_log("master_replsync - req_replid=%s req_offset=%llu req_durable=%llu backlog_start=%llu backlog_end=%llu can_continue=%d",
            req_replid, req_offset, req_durable, repl_backlog_start_offset(), repl_backlog_end_offset(), can_continue ? 1 : 0);
        if (c && (!strcasecmp(g_cfg.repl_realtime_transport, "ebpf") || !strcasecmp(g_cfg.repl_realtime_transport, "sockmap")
                || !strcasecmp(g_cfg.repl_transport_backend, "ebpf") || !strcasecmp(g_cfg.repl_transport_backend, "sockmap"))) {
            c->repl_transport_kind = KVS_REPL_TRANSPORT_EBPF;
            if (repl_ebpf_register_fd(c->fd, 1) != 0) {
                fprintf(stderr, "repl ebpf: fd registration failed on master replica link, using tcp-compatible path\n");
            }
        }
        if (c && (!strcasecmp(g_cfg.repl_realtime_transport, "kprobe-rdma")
                || !strcasecmp(g_cfg.repl_transport_backend, "kprobe-rdma"))) {
            c->repl_transport_kind = KVS_REPL_TRANSPORT_KPROBE_RDMA;
            /* kprobe 透明拦截，fd 注册在主进程初始化时已完成 */
            fprintf(stderr, "repl: replica transport set to kprobe-rdma\n");
        }
        if (c && (!strcasecmp(g_cfg.repl_realtime_transport, "ebpf+tcp")
                || !strcasecmp(g_cfg.repl_realtime_transport, "tcp"))) {
            c->repl_transport_kind = KVS_REPL_TRANSPORT_EBPF_TCP;
            fprintf(stderr, "repl: replica transport set to ebpf+tcp\n");
        }
        /* 向 ebpf-proxy 写入 slave 地址 */
        {
            char cfg_path[512];
            snprintf(cfg_path, sizeof(cfg_path), "%s/proxy_cfg", g_cfg.ebpf_pin_path);
            int proxy_cfg_fd = bpf_obj_get(cfg_path);
            if (proxy_cfg_fd >= 0) {
                __u64 val;
                char key[32] = {0};
                struct sockaddr_in peer;
                socklen_t peer_len = sizeof(peer);
                if (getpeername(c->fd, (struct sockaddr *)&peer, &peer_len) == 0) {
                    val = (__u64)(peer.sin_addr.s_addr);
                    snprintf(key, sizeof(key), "slave_addr");
                    bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
                    val = (__u64)(g_cfg.port + 1);  /* proxy listener on slave port+1 (assumes same port) */
                    snprintf(key, sizeof(key), "slave_port");
                    bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
                    fprintf(stderr, "master: wrote slave addr to ebpf-proxy (proxy port=%llu)\n",
                            (unsigned long long)val);
                }
                close(proxy_cfg_fd);
            }
        }
        /* ① 先立屏障，**再**建立 session（开 CAPTURE_ENABLE）。顺序不能反：
         * 若先开捕获，proxy 会在"屏障生效前"把这段窗口里捕获的写实时转发给 Slave，
         * 随后快照/replay 又会带上同一条命令 → 重复应用。
         * 屏障失败一律 fail-closed：宁可让 Slave 重连重试，也不能在无屏障的情况下
         * 让"控制连接补数据"和"proxy 实时转发"两条连接并行。 */
        unsigned long long catchup_end = 0;
        if (repl_barrier_begin(&catchup_end) != 0) {
            fprintf(stderr, "master: REPLSYNC rejected — proxy barrier unavailable "
                            "(fail-closed), slave will reconnect and retry\n");
            repl_reject_replica(c);
            return 0;
        }

        /* ② 建立 session：此后捕获到的写只会进 proxy_cache（proxy 已在 BUFFERING） */
        repl_add_slave(c);
        repl_replica_update_ack(c, req_offset, req_durable);
        c->repl_fullsync_pending = can_continue ? 0 : 1;

        if (can_continue) {
            repl_note_partialsync_result(1);
            /* ③ 只 replay 到 catchup_end（屏障时刻的 offset）。这段时间的新写进 proxy_cache，
             * 与 replay 区间不重叠；replay 完成后**不立即放行**，等 Slave 的 REPLACK 确认
             * applied >= catchup_end 再放行（repl_barrier_note_applied），
             * 保证 proxy flush 一定发生在 replay 数据被应用之后。 */
            repl_backlog_send_continue_upto(c, req_offset, catchup_end);
            if (req_offset >= catchup_end) {
                repl_barrier_release();   /* 无缺口：直接放行 */
            } else {
                repl_barrier_gate_on_ack();   /* 有缺口：等 Slave REPLACK 追上再放行 */
                fprintf(stderr, "master: partial resync replayed [%llu,%llu) — "
                                "waiting slave REPLACK before releasing barrier\n",
                        req_offset, catchup_end);
            }
        } else {
            repl_note_partialsync_result(argc >= 3 ? 0 : 1);
            /* 全量：快照边界就用屏障时刻的 catchup_end（比在 queue_snapshot 里重新取更早、
             * 且此时 proxy 已确认 BUFFERING，边界干净）。放行由 REPLDONE 触发。 */
            if (queue_snapshot(c, catchup_end) != 0) {
                repl_rdma_log("master_replsync - queue_snapshot failed");
                repl_reject_replica(c);
                repl_barrier_release();
            }
        }
        return 0;
    }
    if (!strcmp(cmd, "REPLACK")) {
        /* slave 端通过 parse_resp_stream(NULL, ..., 1) 处理复制数据时不应处理 REPLACK */
        if (from_replication && !c) return 0;
        unsigned long long applied_offset = argc >= 2 ? (unsigned long long)strtoull(argv[1], NULL, 10) : 0;
        unsigned long long durable_offset = argc >= 3 ? (unsigned long long)strtoull(argv[2], NULL, 10) : applied_offset;
        repl_replica_update_ack(c, applied_offset, durable_offset);

        /* proxy_cache 溢出（client_ctl[10]=1）：proxy 已经无法保证同一 session 内无损，
         * 此时绝不能让它继续 flush（会形成永久缺数据的错误副本）。断开 replica 链路，
         * 让 Slave 重新 REPLSYNC —— backlog 已不连续/偏移对不上，会走 FULLRESYNC（§12）。 */
        if (c && c->repl_transport_kind == KVS_REPL_TRANSPORT_EBPF_TCP
            && repl_ebpf_client_ctl_get(KVS_CTL_CACHE_INVALID) == 1) {
            fprintf(stderr, "master: ebpf-proxy cache invalid (overflow) — "
                            "dropping replica link to force full resync\n");
            repl_ebpf_client_ctl_set(KVS_CTL_CACHE_INVALID, 0);   /* 重新武装，避免反复触发 */
            repl_reject_replica(c);
            return 0;
        }

        /* partial resync 屏障的放行条件：Slave 的 applied 真正追上 catchup_end。
         * 必须在这里放行（而不是 replay 完就放），否则 proxy 的 cache flush 可能先于
         * 控制连接上的 replay 数据被应用 —— 那正是两条连接重排的根源。 */
        repl_barrier_note_applied(applied_offset);

        /* 若 slave 落后且 backlog 有数据，推送追赶。经转发线程发送（回放数据深拷贝后入队），
         * 使转发线程成为 c->fd 的唯一写者，避免与转发线程并发写同一 fd（流穿插/破坏）。
         * IMPORTANT 1：追赶起点取 max(applied_offset, watermark)——watermark 是已交给
         * 转发线程（可能仍留在 st->buf 未刷）的最高 offset；slave 的 applied 常滞后于它，
         * 若仍从 applied 重发，会把 st->buf 里未刷的字节重复下送（非幂等命令双倍应用）。 */
        if (c && c->repl_transport_kind != KVS_REPL_TRANSPORT_EBPF_TCP
            && repl_master_offset() > applied_offset
            && applied_offset >= repl_backlog_start_offset()
            && applied_offset <= repl_backlog_end_offset()) {
            unsigned long long wm = repl_fwd_get_watermark(c);
            unsigned long long copy_from = (applied_offset > wm) ? applied_offset : wm;
            if (copy_from < repl_backlog_end_offset()) {
                unsigned char *cbuf = NULL;
                size_t clen = 0;
                if (repl_backlog_copy_range(copy_from, &cbuf, &clen) == 0 && clen > 0) {
                    /* end_offset = copy_from + clen，与拷贝区间自洽（重发去重按此推算起点）。
                     * 入队失败不再回填 backlog：cbuf 本来就是从 backlog 拷出来的，
                     * 回填 = 把同一段历史再追加一遍，backlog_end_offset 会凭空前进而与
                     * master_repl_offset 脱节。slave 下一次 REPLACK 自然会重新触发追赶。 */
                    (void)repl_fwd_enqueue(c, cbuf, clen, copy_from + clen);
                    kvs_free(cbuf);
                }
            }
        }
        return 0;
    }
    if (!strcmp(cmd, "REPLDONE")) {
        /* Master 侧：slave 发来 REPLDONE 表示全量同步在 slave 侧已完成 */
        if (g_cfg.role == ROLE_MASTER && c && c->is_replica) {
            c->repl_fullsync_pending = 0;
            g_repl_fullsync_in_progress = 0;

            /* Slave 确认快照已加载完成 → 放行屏障：proxy 切回 FORWARDING 并 flush
             * 全量期间攒下的 proxy_cache。放行必须由 Slave 的确认触发，这样 cache 里的
             * 增量一定排在快照之后被应用。 */
            repl_barrier_release();

            /* 回放全量同步期间积压的增量数据。
             * 用 backlog 自身的 start_offset，而非 c->repl_offset_sent
             * （后者在 queue_snapshot 中设置为全量开始时的值，但 backlog
             *  在第一条增量数据写入时才初始化，start_offset 可能更大） */
            fprintf(stderr, "master: REPLDONE replaying backlog histlen=%llu "
                    "master_off=%llu backlog_start=%llu\n",
                    repl_backlog_histlen(), repl_master_offset(),
                    repl_backlog_start_offset());
            /* ebpf+tcp：增量由 proxy 独立进程全权转发（含全量期间的缓存），
             * backlog 回放会经 tcp_fd 与 proxy 双路送达 slave（交错乱序），故跳过。 */
            if (c && c->repl_transport_kind != KVS_REPL_TRANSPORT_EBPF_TCP
                && repl_backlog_histlen() > 0
                && repl_master_offset() > repl_backlog_start_offset()) {
                /* 积压回放经转发线程发送（深拷贝后入队），避免与转发线程并发写 c->fd。
                 * IMPORTANT 1：起点取 max(backlog_start, watermark)，不重发已交给转发
                 * 线程（仍在其 st->buf/队列中未刷）的字节，防止非幂等命令双倍应用。 */
                unsigned long long wm = repl_fwd_get_watermark(c);
                unsigned long long replay_from = repl_backlog_start_offset();
                if (wm > replay_from) replay_from = wm;
                if (replay_from < repl_backlog_end_offset()) {
                    unsigned char *cbuf = NULL;
                    size_t clen = 0;
                    int rc = repl_backlog_copy_range(replay_from, &cbuf, &clen);
                    if (rc == 0 && clen > 0) {
                        fprintf(stderr, "master: backlog replay from=%llu len=%zu enqueue\n",
                                replay_from, clen);
                        /* end_offset = replay_from + clen，与拷贝区间自洽（重发去重按此推算起点）。
                         * 入队失败不回填 backlog（cbuf 本就来自 backlog，回填=重复追加历史）。 */
                        (void)repl_fwd_enqueue(c, cbuf, clen, replay_from + clen);
                        kvs_free(cbuf);
                    } else {
                        fprintf(stderr, "master: backlog replay from=%llu rc=%d len=%zu\n",
                                replay_from, rc, clen);
                    }
                }
            }

            /* 从机已确认全量同步完成 — 关闭 RDMA，后续增量走 realtime transport */
            repl_rdma_stop_fullsync();
            repl_rdma_log("master_repldone - slave fullsync complete, RDMA stopped");
            return 0;
        }
        /* Slave 侧兼容（旧代码路径，保留以防万一） */
        if (g_cfg.role == ROLE_SLAVE) {
            repl_rdma_log("slave_parse - REPLDONE (legacy)");
            repl_slave_finish_fullsync();
            return 0;
        }
        return 0;
    }
    if (!strcmp(cmd, "KPROBEMR")) {
        /* Slave 侧收到 KPROBEMR 请求：返回 MR 信息 */
        fprintf(stderr, "kprobe rdma: KPROBEMR received, sending MR info...\n");
        char resp[384];
        int rn = repl_kprobe_rdma_get_mr_text(resp, sizeof(resp));
        if (rn > 0) {
            if (c) {
                queue_bytes(c, (unsigned char *)resp, (size_t)rn);
            } else if (g_slave_fd >= 0) {
                send(g_slave_fd, resp, (size_t)rn, MSG_NOSIGNAL);
            }
            fprintf(stderr, "kprobe rdma: KPROBEMR response sent (%d bytes): %s",
                rn, resp);
        }
        return 0;
    }
    if (!strcmp(cmd, "INFO")) {
        char info[12288];
        char recover[1024] = {0};
        kvs_repl_ebpf_stats_t ebpf_stats;
        kvs_ebpf_proxy_stats_t proxy_stats;
        kvs_repl_kprobe_stats_t kprobe_stats;
        int recover_n = persist_build_recover_text(recover, sizeof(recover));
        repl_ebpf_get_stats(&ebpf_stats);
        repl_ebpf_proxy_get_stats(&proxy_stats);
        repl_kprobe_rdma_get_stats(&kprobe_stats);

        unsigned long long max_replica_applied = 0;
        unsigned long long max_replica_durable = 0;
        long long min_replica_ack_age_ms = -1;
        int replicas = 0;
        repl_collect_replica_ack_stats(&max_replica_applied, &max_replica_durable, &min_replica_ack_age_ms, &replicas);

        snprintf(info, sizeof(info),
            "role:%s\n"
            "mem:%s\n"
            "dirty:%llu\n"
            "last_snapshot_ms:%lld\n"
            "autosnap_rules:%d\n"
            "bgsave:%s\n"
            "bgsave_pid:%ld\n"
            "aof_fsync:%s\n"
            "aof_max_batch_age_us:%lld\n"
            "aof_rewrite:%s\n"
            "aof_rewrite_pid:%ld\n"
            "master_host:%s\n"
            "master_port:%d\n"
            "master_link:%s\n"
            "repl_transport:%s\n"
            "repl_transport_configured:%s\n"
            "repl_transport_active:%s\n"
            "repl_transport_fallback_reason:%s\n"
            "repl_transport_fallback_count:%llu\n"
            "repl_transport_fallback_until_ms:%lld\n"
            "replicas:%d\n"
            "master_replid:%s\n"
            "master_repl_offset:%llu\n"
            "connected_slaves:%llu\n"
            "repl_fullsync_count:%llu\n"
            "repl_partialsync_ok_count:%llu\n"
            "repl_partialsync_err_count:%llu\n"
            "repl_broadcast_bytes:%llu\n"
            "repl_snapshot_bytes:%llu\n"
            "repl_backlog_size:%llu\n"
            "repl_backlog_histlen:%llu\n"
            "repl_backlog_start_offset:%llu\n"
            "repl_backlog_end_offset:%llu\n"
            "repl_backlog_contiguous:%d\n"
            "repl_session_id:%llu\n"
            "repl_session_valid:%d\n"
            "repl_proxy_barrier:%d\n"
            "ebpf_capture_enabled:%llu\n"
            "ebpf_capture_off_count:%llu\n"
            "ebpf_proxy_cache_bytes:%llu\n"
            "ebpf_proxy_cache_nodes:%llu\n"
            "ebpf_proxy_cache_dropped:%llu\n"
            "ebpf_proxy_cache_drop_bytes:%llu\n"
            "ebpf_proxy_cache_max_bytes:%llu\n"
            "ebpf_proxy_cache_invalid:%llu\n"
            "slave_master_replid:%s\n"
            "slave_repl_offset:%llu\n"
            "slave_repl_applied_offset:%llu\n"
            "slave_repl_durable_offset:%llu\n"
            "slave_fullsync_loading:%d\n"
            "replica_max_applied_offset_ack:%llu\n"
            "replica_max_durable_offset_ack:%llu\n"
            "replica_min_ack_age_ms:%lld\n"
            "rdma_recv_slots:%d\n"
            "rdma_chunk_size:%d\n"
            "rdma_qp_wr_depth:%d\n"
            "rdma_connected:%d\n"
            "rdma_disconnect_count:%llu\n"
            "rdma_reject_count:%llu\n"
            "rdma_send_cq_error_count:%llu\n"
            "rdma_recv_cq_error_count:%llu\n"
            "ebpf_compiled:%llu\n"
            "ebpf_initialized:%llu\n"
            "ebpf_register_attempts:%llu\n"
            "ebpf_register_failures:%llu\n"
            "ebpf_last_errno:%d\n"
            "ebpf_last_error:%s\n"
            "ebpf_sk_msg_count:%llu\n"
            "ebpf_sk_msg_bytes:%llu\n"
            "ebpf_sk_msg_pass:%llu\n"
            "ebpf_sk_msg_drop:%llu\n"
            "ebpf_redirect_enabled:%llu\n"
            "ebpf_forward_enabled:%llu\n"
            "ebpf_redirect_attempts:%llu\n"
            "ebpf_redirect_success:%llu\n"
            "ebpf_redirect_failures:%llu\n"
            "ebpf_role_unknown:%llu\n"
            "ebpf_role_master:%llu\n"
            "ebpf_role_slave:%llu\n"
            "kprobe_initialized:%d\n"
            "kprobe_rdma_connected:%d\n"
            "kprobe_hits:%llu\n"
            "kprobe_bytes:%llu\n"
            "kprobe_ringbuf_events:%llu\n"
            "kprobe_ringbuf_bytes:%llu\n"
            "kprobe_rdma_writes:%llu\n"
            "kprobe_rdma_errors:%llu\n"
            "%s",
            g_cfg.role == ROLE_MASTER ? "master" : "slave",
            kvs_mem_backend_name(),
            (unsigned long long)persist_dirty_count(),
            persist_last_snapshot_ms(),
            g_cfg.autosnap_rule_count,
            persist_bgsave_state_name(),
            (long)g_bgsave_pid,
            persist_aof_policy_name(),
            persist_aof_max_batch_age_us(),
            persist_bgrewriteaof_state_name(),
            (long)(persist_bgrewriteaof_in_progress() ? 1 : -1),
            g_cfg.master_host[0] ? g_cfg.master_host : "",
            g_cfg.master_port,
            repl_master_link_state_name(),
            repl_transport_name(),
            repl_transport_configured_name(),
            repl_transport_active_name(),
            repl_transport_fallback_reason()[0] ? repl_transport_fallback_reason() : "none",
            repl_transport_fallback_count(),
            repl_transport_fallback_until_ms(),
            replicas,
            repl_master_id(),
            repl_master_offset(),
            repl_connected_slaves(),
            repl_fullsync_count(),
            repl_partialsync_ok_count(),
            repl_partialsync_err_count(),
            repl_broadcast_bytes(),
            repl_snapshot_bytes(),
            repl_backlog_size(),
            repl_backlog_histlen(),
            repl_backlog_start_offset(),
            repl_backlog_end_offset(),
            repl_backlog_contiguous(),
            repl_session_id(),
            repl_session_valid(),
            repl_barrier_pending(),
            proxy_stats.capture_enabled,
            proxy_stats.capture_off_count,
            proxy_stats.cache_bytes,
            proxy_stats.cache_nodes,
            proxy_stats.cache_dropped,
            proxy_stats.cache_drop_bytes,
            proxy_stats.cache_max_bytes,
            proxy_stats.cache_invalid,
            repl_slave_master_id(),
            repl_slave_offset(),
            repl_slave_applied_offset(),
            repl_slave_durable_offset(),
            repl_slave_loading_fullsync(),
            max_replica_applied,
            max_replica_durable,
            min_replica_ack_age_ms,
            repl_rdma_effective_recv_slots(),
            repl_rdma_effective_chunk_size(),
            repl_rdma_effective_qp_wr_depth(),
            repl_rdma_is_connected(),
            repl_rdma_disconnect_count(),
            repl_rdma_reject_count(),
            repl_rdma_send_cq_error_count(),
            repl_rdma_recv_cq_error_count(),
            ebpf_stats.compiled,
            ebpf_stats.initialized,
            ebpf_stats.register_attempts,
            ebpf_stats.register_failures,
            ebpf_stats.last_errno,
            ebpf_stats.last_error,
            ebpf_stats.sk_msg_count,
            ebpf_stats.sk_msg_bytes,
            ebpf_stats.sk_msg_pass,
            ebpf_stats.sk_msg_drop,
            ebpf_stats.redirect_enabled,
            ebpf_stats.forward_enabled,
            ebpf_stats.redirect_attempts,
            ebpf_stats.redirect_success,
            ebpf_stats.redirect_failures,
            ebpf_stats.role_unknown,
            ebpf_stats.role_master,
            ebpf_stats.role_slave,
            kprobe_stats.kprobe_initialized,
            kprobe_stats.rdma_connected,
            kprobe_stats.kprobe_hits,
            kprobe_stats.kprobe_bytes,
            kprobe_stats.total_events,
            kprobe_stats.total_bytes,
            kprobe_stats.rdma_writes,
            kprobe_stats.rdma_errors,
            recover_n >= 0 ? recover : "");

        n = resp_bulk(resp, BUFFER_CAP, info, strlen(info));
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "MEMSTAT")) {
        char *info = (char *)kvs_malloc(BUFFER_CAP);
        n = build_memstat_text(info, BUFFER_CAP);
        if (n < 0) n = resp_error(resp, BUFFER_CAP, "memstat build failed");
        else n = resp_bulk(resp, BUFFER_CAP, info, (size_t)n);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "MEMORY_PURGE")) {
        /* 显式归还空闲内存给内核（基准测试用）：libc/custom→malloc_trim, jemalloc→mallctl purge */
        kvs_mem_purge();
        n = resp_simple_string(resp, BUFFER_CAP, "OK");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "SAVE")) {
        n = (persist_save_dump() == 0) ? resp_simple_string(resp, BUFFER_CAP, "OK") : resp_error(resp, BUFFER_CAP, "save failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "BGSAVE")) {
        int brc = persist_bgsave_start();
        if (brc == 0) n = resp_simple_string(resp, BUFFER_CAP, "Background saving started");
        else if (brc == 1) n = resp_error(resp, BUFFER_CAP, "background saving already in progress");
        else n = resp_error(resp, BUFFER_CAP, "bgsave failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "BGREWRITEAOF")) {
        int rrc = persist_bgrewriteaof_start();
        if (rrc == 0) n = resp_simple_string(resp, BUFFER_CAP, "Background append only file rewriting started");
        else if (rrc == 1) n = resp_error(resp, BUFFER_CAP, "aof rewrite already in progress");
        else n = resp_error(resp, BUFFER_CAP, "bgrewriteaof failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "APPENDFSYNC") && argc == 2) {
        kvs_aof_fsync_policy_t policy;
        if (parse_appendfsync_policy(argv[1], &policy) != 0 || persist_set_aof_policy(policy) != 0)
            n = resp_error(resp, BUFFER_CAP, "invalid fsync policy");
        else
            n = resp_simple_string(resp, BUFFER_CAP, "OK");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "CONFIG") && argc == 3) {
        if (!strcasecmp(argv[1], "APPENDFSYNC")) {
            kvs_aof_fsync_policy_t policy;
            if (parse_appendfsync_policy(argv[2], &policy) != 0 || persist_set_aof_policy(policy) != 0)
                n = resp_error(resp, BUFFER_CAP, "invalid fsync policy");
            else
                n = resp_simple_string(resp, BUFFER_CAP, "OK");
        } else {
            n = resp_error(resp, BUFFER_CAP, "unsupported config option");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "SNAPRULE") && argc == 3) {
        int arc = persist_register_autosnap_rule(atoll(argv[1]), atoll(argv[2]));
        n = (arc == 0) ? resp_simple_string(resp, BUFFER_CAP, "OK") : resp_error(resp, BUFFER_CAP, "snaprule failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "SNAPRULES")) {
        char *info = (char *)kvs_malloc(BUFFER_CAP);
        n = persist_build_autosnap_text(info, BUFFER_CAP);
        if (n < 0) n = resp_error(resp, BUFFER_CAP, "snaprules build failed");
        else n = resp_bulk(resp, BUFFER_CAP, info, (size_t)n);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "SNAPRULECLEAR")) {
        persist_clear_autosnap_rules();
        n = resp_simple_string(resp, BUFFER_CAP, "OK");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }

    if (!strcmp(cmd, "LOCK") && argc == 4) {
        int lrc = lock_acquire(KVS_ENGINE_ARRAY, argv[1], argv[2], atoll(argv[3]));
        if (lrc == 0) {
            n = resp_integer(resp, BUFFER_CAP, 1);
            if (!from_replication) {
                persist_note_write();
                int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
                if (pr == KVS_PERSIST_ERR) {
                    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    goto out;
                }
                if (pr == KVS_PERSIST_PENDING) {
                    resp = NULL;
                }
                if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            }
        } else if (lrc == 1) {
            n = resp_integer(resp, BUFFER_CAP, 0);
        } else {
            n = resp_error(resp, BUFFER_CAP, "lock failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "UNLOCK") && argc == 3) {
        int lrc = lock_release(KVS_ENGINE_ARRAY, argv[1], argv[2]);
        if (lrc == 0) {
            n = resp_integer(resp, BUFFER_CAP, 1);
            if (!from_replication) {
                persist_note_write();
                int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
                if (pr == KVS_PERSIST_ERR) {
                    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    goto out;
                }
                if (pr == KVS_PERSIST_PENDING) {
                    resp = NULL;
                }
                if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            }
        } else if (lrc == 1) {
            n = resp_integer(resp, BUFFER_CAP, 0);
        } else {
            n = resp_error(resp, BUFFER_CAP, "unlock failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "RENEW") && argc == 4) {
        int lrc = lock_renew(KVS_ENGINE_ARRAY, argv[1], argv[2], atoll(argv[3]));
        if (lrc == 0) {
            n = resp_integer(resp, BUFFER_CAP, 1);
            if (!from_replication) {
                persist_note_write();
                int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
                if (pr == KVS_PERSIST_ERR) {
                    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    goto out;
                }
                if (pr == KVS_PERSIST_PENDING) {
                    resp = NULL;
                }
                if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            }
        } else if (lrc == 1) {
            n = resp_integer(resp, BUFFER_CAP, 0);
        } else {
            n = resp_error(resp, BUFFER_CAP, "renew failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "OWNER") && argc == 2) {
        try_expire(KVS_ENGINE_ARRAY, argv[1]);
        char *v = engine_get(KVS_ENGINE_ARRAY, argv[1]);
        n = v ? resp_bulk(resp, BUFFER_CAP, v, strlen(v)) : resp_null_bulk(resp, BUFFER_CAP);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "SLAVEOF")) {
        if (argc == 3 && !strcasecmp(argv[1], "NO") && !strcasecmp(argv[2], "ONE")) {
            if (repl_slaveof_noone() != 0)
                n = resp_error(resp, BUFFER_CAP, "slaveof no one failed");
            else
                n = resp_simple_string(resp, BUFFER_CAP, "OK");
            if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
            return 0;
        }

        if (argc == 3) {
            int port = atoi(argv[2]);
            if (port <= 0 || repl_slaveof(argv[1], port) != 0)
                n = resp_error(resp, BUFFER_CAP, "slaveof failed");
            else
                n = resp_simple_string(resp, BUFFER_CAP, "OK");
            if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
            return 0;
        }

        n = resp_error(resp, BUFFER_CAP, "wrong args for SLAVEOF");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }
    if (!strcmp(cmd, "ROLE")) {
        unsigned long long role_max_replica_applied = 0;
        unsigned long long role_max_replica_durable = 0;
        long long role_min_replica_ack_age_ms = -1;
        int role_replicas = 0;
        repl_collect_replica_ack_stats(&role_max_replica_applied, &role_max_replica_durable, &role_min_replica_ack_age_ms, &role_replicas);
        if (g_cfg.role == ROLE_MASTER) {
            char durable[32];
            snprintf(durable, sizeof(durable), "%llu", role_max_replica_durable);
            n = snprintf(resp, BUFFER_CAP,
                "*3\r\n$6\r\nmaster\r\n$1\r\n0\r\n$%zu\r\n%s\r\n",
                strlen(durable), durable);
        } else {
            char mp[32];
            snprintf(mp, sizeof(mp), "%d", g_cfg.master_port);
            n = snprintf(resp, BUFFER_CAP,
                "*3\r\n$5\r\nslave\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                strlen(g_cfg.master_host), g_cfg.master_host,
                strlen(mp), mp);
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }

    if (!strcmp(cmd, "DOCSET") && argc == 4) {
        int drc = kvs_doc_set(&global_doc, argv[1], argv[2], argv[3]);
        if (drc == 0) {
            n = resp_simple_string(resp, BUFFER_CAP, "OK");
            if (!from_replication) {
                persist_note_write();
                int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
                if (pr == KVS_PERSIST_ERR) {
                    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    goto out;
                }
                if (pr == KVS_PERSIST_PENDING) {
                    resp = NULL;
                }
                if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            }
        } else {
            n = resp_error(resp, BUFFER_CAP, "docset failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (!strcmp(cmd, "DOCGET") && argc == 3) {
        char *v = kvs_doc_get(&global_doc, argv[1], argv[2]);
        n = v ? resp_bulk(resp, BUFFER_CAP, v, strlen(v)) : resp_null_bulk(resp, BUFFER_CAP);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (!strcmp(cmd, "DOCDEL") && argc == 3) {
        int drc = kvs_doc_del_field(&global_doc, argv[1], argv[2]);
        if (drc == 0) {
            n = resp_simple_string(resp, BUFFER_CAP, "OK");
            if (!from_replication) {
                persist_note_write();
                int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
                if (pr == KVS_PERSIST_ERR) {
                    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    goto out;
                }
                if (pr == KVS_PERSIST_PENDING) {
                    resp = NULL;
                }
                if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            }
        } else if (drc == 1) {
            n = resp_error(resp, BUFFER_CAP, "doc or field not found");
        } else {
            n = resp_error(resp, BUFFER_CAP, "docdel failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (!strcmp(cmd, "DOCDROP") && argc == 2) {
        int drc = kvs_doc_del(&global_doc, argv[1]);
        if (drc == 0) {
            n = resp_simple_string(resp, BUFFER_CAP, "OK");
            if (!from_replication) {
                persist_note_write();
                int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
                if (pr == KVS_PERSIST_ERR) {
                    n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                    if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    goto out;
                }
                if (pr == KVS_PERSIST_PENDING) {
                    resp = NULL;
                }
                if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            }
        } else if (drc == 1) {
            n = resp_error(resp, BUFFER_CAP, "doc not found");
        } else {
            n = resp_error(resp, BUFFER_CAP, "docdrop failed");
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (!strcmp(cmd, "DOCEXIST") && argc == 2) {
        n = resp_integer(resp, BUFFER_CAP, kvs_doc_exist(&global_doc, argv[1]) == 0 ? 1 : 0);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (!strcmp(cmd, "DOCCOUNT") && argc == 2) {
        int cnt = kvs_doc_field_count(&global_doc, argv[1]);
        n = resp_integer(resp, BUFFER_CAP, cnt);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }
    if (!strcmp(cmd, "DOCGETALL") && argc == 2) {
        int cnt = kvs_doc_field_count(&global_doc, argv[1]);
        if (cnt <= 0) {
            n = resp_empty_array(resp, BUFFER_CAP);
            if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
            goto out;
        }
        size_t pos = 0;
        int hn = resp_array_header(resp, BUFFER_CAP, cnt * 2);
        if (hn < 0) { n = resp_error(resp, BUFFER_CAP, "docgetall failed"); if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n); goto out; }
        pos += (size_t)hn;
        kvs_doc_t *d = NULL;
        if (global_doc.buckets) {
            unsigned int didx;
            for (didx = 0; didx < (unsigned int)global_doc.size; ++didx) {
                for (kvs_doc_t *dd = global_doc.buckets[didx]; dd; dd = dd->next) {
                    if (strcmp(dd->key, argv[1]) == 0) { d = dd; break; }
                }
                if (d) break;
            }
        }
        if (d) {
            for (int bi = 0; bi < d->bucket_count; ++bi) {
                for (kvs_doc_field_t *f = d->fields[bi]; f; f = f->next) {
                    if (resp_array_append_bulk_or_null(resp, BUFFER_CAP, &pos, f->name) != 0) break;
                    if (resp_array_append_bulk_or_null(resp, BUFFER_CAP, &pos, f->value) != 0) break;
                }
            }
        }
        n = (int)pos;
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
    }

    int engine;
    const char *op;
engine_dispatch:
    engine = cmd_engine(cmd);
    op = strip_prefix(cmd);
    int rc = -1;
    int should_reply_missing = 0;

    if (!strcmp(op, "SET") && argc == 3) {
        try_expire(engine, argv[1]);
        /* hash 引擎用长度感知 set，支持含 '\0' 的二进制 value（argl 有真实长度） */
        if (engine == KVS_ENGINE_HASH)
            rc = kvs_hash_set_len(&global_hash, argv[1], argv[2], argl[2]);
        else
            rc = engine_set(engine, argv[1], argv[2]);
        should_reply_missing = 1;
    }
    else if (!strcmp(op, "MSET")) {
        try_expire(engine, argv[1]);
        rc = handle_multi_set(engine, argc, argv);
    }
    else if (!strcmp(op, "MOD") && argc == 3) {
        try_expire(engine, argv[1]);
        rc = engine_mod(engine, argv[1], argv[2]);
        should_reply_missing = 1;
    }
    else if (!strcmp(op, "DEL") && argc == 2) {
        try_expire(engine, argv[1]);
        rc = engine_del(engine, argv[1]);
        kvs_expire_del(&global_expire, engine, argv[1]);
        should_reply_missing = 1;
    }
    else if (!strcmp(op, "GET") && argc == 2) {
        try_expire(engine, argv[1]);
        size_t gvlen = 0;
        char *v;
        /* hash 引擎用长度感知 get，正确取得含 '\0' 的二进制 value 长度 */
        if (engine == KVS_ENGINE_HASH)
            v = kvs_hash_get_len(&global_hash, argv[1], &gvlen);
        else {
            v = engine_get(engine, argv[1]);
            if (v) gvlen = strlen(v);
        }
#if KVS_ENABLE_RDMA
        if (g_cfg.role == ROLE_SLAVE && !from_replication && repl_is_soak_key(argv[1])) {
            repl_rdma_log("slave_get_inline - key=%s hit=%d value=%s slave_offset=%llu master_link=%s",
                argv[1], v ? 1 : 0, v ? v : "(null)", repl_slave_offset(), repl_master_link_state_name());
        }
#endif
        if (v) {
            if (gvlen + 16 <= OUT_RING_SIZE) {
                /* 流式直写 out_ring（header + value + CRLF），绕过 64KB resp 栈缓冲：
                 * 支持 64KB~256KB 大 value 读回；慢客户端 EAGAIN 由 flush 兜底续写。 */
                n = snprintf(resp, BUFFER_CAP, "$%zu\r\n", gvlen);
                if (c) {
                    queue_bytes(c, (unsigned char *)resp, (size_t)n);
                    queue_bytes(c, (unsigned char *)v, gvlen);
                    queue_bytes(c, (unsigned char *)"\r\n", 2);
                }
            } else {
                n = resp_error(resp, BUFFER_CAP, "value too large (max 256KB)");
                if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
            }
        } else {
            n = resp_null_bulk(resp, BUFFER_CAP);
            if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        }
        goto out;
        return 0;
    } else if (!strcmp(op, "MGET")) {
        n = handle_multi_get(engine, argc, argv, resp, BUFFER_CAP);
#if KVS_ENABLE_RDMA
        if (g_cfg.role == ROLE_SLAVE && !from_replication) {
            for (int i = 1; i < argc; ++i) {
                if (repl_is_soak_key(argv[i])) {
                    char *mv = engine_get(engine, argv[i]);
                    repl_rdma_log("slave_mget_inline - key=%s hit=%d value=%s slave_offset=%llu master_link=%s",
                        argv[i], mv ? 1 : 0, mv ? mv : "(null)", repl_slave_offset(), repl_master_link_state_name());
                }
            }
        }
#endif
        if (n < 0) n = resp_error(resp, BUFFER_CAP, "mget failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    } else if (!strcmp(op, "VSEARCH") && (argc == 4 || argc == 5)) {
        int dim = atoi(argv[1]);
        float *query = (float *)argv[2];
        int topk = atoi(argv[3]);
        /* 可选第 4 参数：缺省 semcache: 向后兼容老三参 */
        const char *prefix = (argc == 5) ? argv[4] : KVS_VSEARCH_DEFAULT_PREFIX;
        size_t plen = (argc == 5) ? argl[4] : sizeof(KVS_VSEARCH_DEFAULT_PREFIX) - 1;
        if (dim <= 0 || topk <= 0 || argl[2] != (size_t)dim * sizeof(float)) {
            n = resp_error(resp, BUFFER_CAP, "vsearch bad args");
        } else {
            n = kvs_vector_search(dim, query, topk, prefix, (int)plen, resp, BUFFER_CAP);
        }
        if (n < 0) n = resp_error(resp, BUFFER_CAP, "vsearch failed");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    } else if (!strcmp(op, "EXIST") && argc == 2) {
        try_expire(engine, argv[1]);
        n = resp_integer(resp, BUFFER_CAP, engine_exist(engine, argv[1]) == 0 ? 1 : 0);
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    } else if (!strcmp(op, "EXPIRE") && argc == 3) {
        try_expire(engine, argv[1]);
        if (engine_exist(engine, argv[1]) != 0) rc = 1;
        else rc = kvs_expire_set(&global_expire, engine, argv[1], atoll(argv[2]) * 1000);
        should_reply_missing = 1;
    } else if (!strcmp(op, "SETEX") && argc == 4) {
        /* SETEX key seconds value：设值并带 TTL（兼容 Redis 语义） */
        try_expire(engine, argv[1]);
        if (engine == KVS_ENGINE_HASH)
            rc = kvs_hash_set_len(&global_hash, argv[1], argv[3], argl[3]);
        else
            rc = engine_set(engine, argv[1], argv[3]);
        if (rc == 0)
            rc = kvs_expire_set(&global_expire, engine, argv[1], atoll(argv[2]) * 1000);
        should_reply_missing = 1;
    } else if (!strcmp(op, "TTL") && argc == 2) {
        try_expire(engine, argv[1]);
        if (engine_exist(engine, argv[1]) != 0) n = resp_integer(resp, BUFFER_CAP, -2);
        else {
            long long ttl = kvs_expire_ttl(&global_expire, engine, argv[1]);
            n = resp_integer(resp, BUFFER_CAP, ttl);
        }
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    } else if (!strcmp(op, "PERSIST") && argc == 2) {
        try_expire(engine, argv[1]);
        if (engine_exist(engine, argv[1]) != 0) rc = 1;
        else rc = kvs_expire_persist(&global_expire, engine, argv[1]);
        should_reply_missing = 1;
    } else {
        int expected_argc = 0;
        if (!strcmp(op, "SET") || !strcmp(op, "MOD") || !strcmp(op, "EXPIRE")) expected_argc = 3;
        else if (!strcmp(op, "SETEX")) expected_argc = 4;
        else if (!strcmp(op, "GET") || !strcmp(op, "DEL") || !strcmp(op, "EXIST") || !strcmp(op, "TTL") || !strcmp(op, "PERSIST")) expected_argc = 2;
        if (expected_argc > 0 && argc != expected_argc)
            n = resp_error(resp, BUFFER_CAP, "wrong number of arguments");
        else
            n = resp_error(resp, BUFFER_CAP, "unknown command or wrong args");
        if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
        goto out;
        return 0;
    }

    if (rc == 0) {
        /* static literal: skip snprintf format-string parse on every write */
        resp[0] = '+'; resp[1] = 'O'; resp[2] = 'K'; resp[3] = '\r'; resp[4] = '\n';
        n = 5;
        if (!strcmp(op, "SET") || !strcmp(op, "MOD")) {
            kvs_expire_persist(&global_expire, engine, argv[1]);
        }
        if (from_replication && is_write_cmd(cmd)) {
            if (!persist_recover_in_progress()) {
                repl_log_applied_command(cmd, argc, argv, rawlen);
            }
            if (g_cfg.role == ROLE_SLAVE && !persist_recover_in_progress()
                && !repl_slave_loading_fullsync()) {
                /* 全量同步期间不写 AOF（数据保存在 dump 文件）
                 * 增量同步期间写入 AOF 用于持久化 */
                persist_note_write();
                int pr = persist_append_prepare(NULL, raw, rawlen, NULL, 0);
                if (pr == KVS_PERSIST_PENDING || pr == KVS_PERSIST_OK) {
                    repl_slave_note_durable(rawlen);
                }
                /* else: AOF write failed — durability not achieved,
                 * slave will need full resync on reconnect */
            }
            if (!persist_recover_in_progress()) {
                repl_slave_note_applied(rawlen);
            }
        }
        /* ebpf+tcp: proxy 转发的写命令也需更新 slave offset */
        if (!from_replication && is_write_cmd(cmd)
            && g_cfg.role == ROLE_SLAVE
            && !strcasecmp(repl_realtime_transport_name(), "ebpf+tcp")) {
            repl_slave_note_applied(rawlen);
        }
        if (!from_replication && is_write_cmd(cmd)) {
            persist_note_write();
            int pr = persist_append_prepare(c, raw, rawlen, (unsigned char *)resp, (size_t)n);
            if (pr == KVS_PERSIST_ERR) {
                n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                goto out;
            }
            if (pr == KVS_PERSIST_PENDING) {
                resp = NULL;
            }
            /* KVS_PERSIST_OK: aof disabled, resp sent later as normal */
            if (g_cfg.role == ROLE_MASTER) {
                g_last_write_ts = time(NULL);
                repl_backlog_feed(raw, rawlen);
                repl_note_broadcast(rawlen);   /* 无条件计数 — offset 反映所有写命令 */
                repl_broadcast(raw, rawlen);
            }
        }
    } else if (rc == 1 && should_reply_missing) {
        if (!strcmp(op, "SET")) {
            /* static literal: skip snprintf format-string parse */
            resp[0] = '+'; resp[1] = 'O'; resp[2] = 'K'; resp[3] = '\r'; resp[4] = '\n';
            n = 5;
        }
        else n = resp_error(resp, BUFFER_CAP, "not found or exists");
    } else {
        n = resp_error(resp, BUFFER_CAP, "operation failed");
    }
    if (c && resp) queue_bytes(c, (unsigned char *)resp, (size_t)n);
    goto out;
out:
    return rc_ret;
}

/* 判断 buf+pos 是否 `+` 开头的已知控制命令行（FULLSYNCABORT/REPLDONE/CONTINUE 等）。
 * 全量同步期间这些控制行不属于 KVSD 数据，KVSD 拦截必须跳过它们。 */
static int kvs_fullsync_control_line(const unsigned char *buf, size_t len, size_t pos) {
    static const char *cmds[] = {"FULLSYNCABORT", "FULLSYNCEND", "REPLDONE", "CONTINUE",
                                 "FULLRESYNC", "KPROBEMR", "KPROBERDMA"};
    if (pos >= len || buf[pos] != '+') return 0;
    for (size_t i = 0; i < sizeof(cmds) / sizeof(cmds[0]); i++) {
        size_t cl = strlen(cmds[i]);
        if (pos + 1 + cl > len) continue;
        if (memcmp(buf + pos + 1, cmds[i], cl) != 0) continue;
        if (pos + 1 + cl == len) return 1;                      /* 行可能未完整，先跳过 */
        unsigned char c = buf[pos + 1 + cl];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') return 1;
    }
    return 0;
}

int parse_resp_stream(conn_t *c, unsigned char *buf, size_t *len, int from_replication) {
    /* KVSD fullsync interception: during full sync, write raw KVSD bytes
     * to temp file instead of RESP parsing. This is the single choke point
     * for ALL receive paths (TCP, RDMA, kprobe). */
    extern int g_slave_fullsync_tmp_fd;
    extern int g_slave_loading_fullsync;
    extern unsigned long long g_slave_fullsync_target_bytes;
    extern unsigned long long g_slave_fullsync_loaded_bytes;

#define PARSE_SCRATCH 4096
    char scratch[PARSE_SCRATCH];
    size_t scratch_off = 0;
    size_t pos = 0;
    while (pos < *len) {

        /* KVSD fullsync interception: during full sync, write raw KVSD bytes
         * to temp file instead of RESP parsing.  Placed inside the while loop
         * so that after +FULLRESYNC sets fullsync_loading, the *same* call's
         * remaining buffer bytes (which are already KVSD) are intercepted. */
        if (from_replication && g_slave_loading_fullsync &&
            !kvs_fullsync_control_line(buf, *len, pos)) {
            size_t remaining = g_slave_fullsync_target_bytes - g_slave_fullsync_loaded_bytes;
            size_t data_left = *len - pos;
            size_t to_write = (data_left < remaining) ? data_left : remaining;

            if (to_write > 0) {
                extern int repl_rdma_slave_target_write(const unsigned char *data,
                                                        size_t len,
                                                        unsigned long long loaded_offset);
                /* 优先写入 file-backed WRITE 目标（SEND 预热 chunk 与 master WRITE 数据同落一文件）；
                 * 无目标（sendfile 回退路径）则写 legacy tmp。 */
                if (repl_rdma_slave_target_write(buf + pos, to_write,
                                                 g_slave_fullsync_loaded_bytes) != 0) {
                    if (g_slave_fullsync_tmp_fd >= 0) {
                        ssize_t wr = write(g_slave_fullsync_tmp_fd, buf + pos, to_write);
                        if (wr < 0) { *len = 0; return -1; }
                        if ((size_t)wr != to_write) {
                            /* partial write — advance by what was actually written */
                            to_write = (size_t)wr;
                        }
                    }
                }
            }
            g_slave_fullsync_loaded_bytes += to_write;
            pos += to_write;

            if (to_write < data_left) {
                /* trailing bytes (e.g. REPLDONE) — keep in buf */
                memmove(buf, buf + pos, *len - pos);
                *len -= pos; pos = 0;
                if (g_slave_fullsync_target_bytes > 0 &&
                    g_slave_fullsync_loaded_bytes >= g_slave_fullsync_target_bytes) {
                    repl_slave_finish_fullsync();
                }
                if (*len > 0 && !g_slave_loading_fullsync) continue;
                return 0;
            }

            *len = 0;
            if (g_slave_fullsync_target_bytes > 0 &&
                g_slave_fullsync_loaded_bytes >= g_slave_fullsync_target_bytes) {
                repl_slave_finish_fullsync();
            }
            return 0;
        }

        if (buf[pos] == '+') {
            size_t line_start = pos + 1;
            while (pos + 1 < *len && !(buf[pos] == '\r' && buf[pos + 1] == '\n')) pos++;
            if (pos + 1 >= *len) break;
            if (pos > line_start) {
                size_t line_len = pos - line_start;
                char *line = (char *)kvs_malloc(line_len + 1);
                if (line) {
                    memcpy(line, buf + line_start, line_len);
                    line[line_len] = '\0';
                    char *argv[8] = {0};
                    int argc = split_inline_argv(line, argv, 8);
                    /* +KPROBERDMA 在 master 侧也需要处理 */
                    if (argc >= 6 && !strcmp(argv[0], "KPROBERDMA")) {
                        unsigned long rkey = (unsigned long)strtoull(argv[1], NULL, 10);
                        unsigned long addr = (unsigned long)strtoull(argv[2], NULL, 10);
                        fprintf(stderr, "kprobe rdma: +KPROBERDMA received (role=%s) rkey=%lu addr=0x%lx\n",
                            g_cfg.role == ROLE_MASTER ? "master" : "slave", rkey, addr);
                        if (g_cfg.role == ROLE_MASTER) {
                            repl_kprobe_rdma_parse_mr_info_direct(rkey, addr,
                                (size_t)strtoull(argv[3], NULL, 10),
                                (size_t)strtoull(argv[4], NULL, 10),
                                (size_t)strtoull(argv[5], NULL, 10));
                        }
                    } else if (argc >= 5 && !strcmp(argv[0], "FULLRESYNCWR")) {
                        /* Master 侧: Slave 发来的 file-backed MR 信息
                         * 格式: +FULLRESYNCWR <transfer_id> <addr> <rkey> <capacity> */
                        unsigned long long tid = strtoull(argv[1], NULL, 10);
                        unsigned long long addr = strtoull(argv[2], NULL, 10);
                        unsigned long long rkey = strtoull(argv[3], NULL, 10);
                        unsigned long long cap = strtoull(argv[4], NULL, 10);
                        fprintf(stderr, "repl rdma: +FULLRESYNCWR tid=%llu addr=0x%llx rkey=%llu cap=%llu\n",
                                tid, addr, rkey, cap);
                        if (g_cfg.role == ROLE_MASTER) {
                            repl_rdma_set_remote_write_mr_info(tid, (uint64_t)addr,
                                                               (uint32_t)rkey, cap);
                        }
                    } else if (from_replication && argc >= 4 && !strcmp(argv[0], "FULLRESYNC")) {
                        unsigned long long fullsync_target = strtoull(argv[3], NULL, 10);
                        unsigned long long transfer_id = strtoull(argv[4], NULL, 10);
                        repl_slave_set_sync_state(argv[1], (unsigned long long)strtoull(argv[2], NULL, 10), (unsigned long long)strtoull(argv[2], NULL, 10), 1, fullsync_target);
                        repl_rdma_log("slave_parse - FULLRESYNC replid=%s offset=%s target=%s tid=%llu", argv[1], argv[2], argv[3], transfer_id);
                        /* 尝试 One-Sided WRITE: 准备 file-backed 目标 + 注册 MR + 发 FULLRESYNCWR */
                        if (fullsync_target > 0) {
                            extern int g_slave_fd;
                            if (repl_rdma_slave_prepare_target(g_slave_fd, transfer_id, (size_t)fullsync_target) != 0) {
                                repl_rdma_log("slave_parse - WRITE target setup failed, will fall back");
                            }
                        }
                    } else if (from_replication && argc >= 2 && !strcmp(argv[0], "FULLSYNCABORT")) {
                        /* Slave 侧: master 放弃 WRITE 目标，丢弃残留（含未完成临时文件） */
                        unsigned long long tid = strtoull(argv[1], NULL, 10);
                        repl_rdma_log("slave_parse - FULLSYNCABORT tid=%llu", tid);
                        repl_rdma_slave_abort_target(tid);
                    } else if (from_replication && argc >= 2 && !strcmp(argv[0], "FULLSYNCEND")) {
                        /* Slave 侧: master 已完成全部 WRITE（已 drain），TCP 信号兜底 finalize */
                        unsigned long long tid = strtoull(argv[1], NULL, 10);
                        repl_rdma_log("slave_parse - FULLSYNCEND tid=%llu", tid);
                        repl_rdma_slave_finalize_signal(tid);
                    } else if (from_replication && argc >= 3 && !strcmp(argv[0], "CONTINUE")) {
                        unsigned long long continue_end = (unsigned long long)strtoull(argv[2], NULL, 10);
                        unsigned long long continue_start = repl_slave_offset();
                        unsigned long long durable_start = repl_slave_durable_offset();
                        if (continue_end < continue_start) continue_end = continue_start;
                        repl_slave_set_sync_state(argv[1], continue_start, durable_start, 0, 0);
                        repl_slave_send_ack();
                        repl_rdma_log("slave_parse - CONTINUE replid=%s start_offset=%llu durable_offset=%llu end_offset=%llu", argv[1], continue_start, durable_start, continue_end);
                    }
                    kvs_free(line);
                }
            }
            pos += 2;
            continue;
        }

        if (buf[pos] != '*') {
            size_t line_end = pos;
            while (line_end < *len && buf[line_end] != '\n') line_end++;
            if (line_end >= *len) break;

            size_t line_len = line_end - pos;
            if (line_len > 0 && buf[pos + line_len - 1] == '\r') line_len--;
            if (line_len == 0) {
                pos = line_end + 1;
                continue;
            }

            char *line = (char *)kvs_malloc(line_len + 1);
            if (!line) {
                if (c) {
                    char r[64];
                    int n = resp_error(r, sizeof(r), "oom");
                    queue_bytes(c, (unsigned char *)r, (size_t)n);
                }
                pos = line_end + 1;
                continue;
            }
            memcpy(line, buf + pos, line_len);
            line[line_len] = '\0';

            char *argv[32] = {0};
            size_t argl[32] = {0};
            int argc = split_inline_argv(line, argv, 32);
            if (argc > 0) {
                for (int i = 0; i < argc; ++i) argl[i] = strlen(argv[i]);
                handle_parsed_command(c, argc, argv, argl, buf + pos, line_end + 1 - pos, from_replication);
            }
            kvs_free(line);
            pos = line_end + 1;
            continue;
        }

        size_t start = pos, p = pos + 1;
        int incomplete = 0;
        int malformed = 0;

        while (p + 1 < *len && !(buf[p] == '\r' && buf[p + 1] == '\n')) p++;
        if (p + 1 >= *len) break;
        if (p - (pos + 1) >= 32) { pos = p + 2; continue; }
        char nbuf[32];
        memcpy(nbuf, buf + pos + 1, p - (pos + 1));
        nbuf[p - (pos + 1)] = '\0';
        int argc = 0;
        /* Inline parse from stack-local buffer (no strtol overhead) */
        { const char *ap = nbuf;
          while (*ap >= '0' && *ap <= '9') { argc = argc * 10 + (*ap++ - '0'); }
          if (*ap != '\0' || argc <= 0 || argc > 32) {
              if (c) { char r[64]; int n = resp_error(r, sizeof(r), "invalid argc");
                       queue_bytes(c, (unsigned char *)r, (size_t)n); }
              pos = p + 2; continue; } }
        p += 2;
        char *argv[32] = {0};
        size_t argl[32] = {0};
        int no_scratch = 0;  /* hot cmd: skip scratch copy for i>=1 */
        for (int i = 0; i < argc; ++i) {
            if (p >= *len) {
                incomplete = 1;
                break;
            }
            if (buf[p] != '$') {
                malformed = 1;
                break;
            }
            size_t lp = p + 1;
            while (lp + 1 < *len && !(buf[lp] == '\r' && buf[lp + 1] == '\n')) lp++;
            if (lp + 1 >= *len) {
                incomplete = 1;
                break;
            }
            if (lp - (p + 1) >= 32) { malformed = 1; break; }
            char lbuf[32];
            memcpy(lbuf, buf + p + 1, lp - (p + 1));
            lbuf[lp - (p + 1)] = '\0';
            /* Inline parse from stack-local buffer (no strtol overhead) */
            long blen = 0;
            { const char *bp = lbuf;
              while (*bp >= '0' && *bp <= '9') { blen = blen * 10 + (*bp++ - '0'); }
              if (*bp != '\0') blen = -1; }
            if (blen < 0) {
                malformed = 1;
                if (c) {
                    char r[128];
                    int n = resp_error(r, sizeof(r), "invalid bulk length");
                    queue_bytes(c, (unsigned char *)r, (size_t)n);
                }
                break;
            }
            p = lp + 2;
            if (p + (size_t)blen + 2 > *len) {
                incomplete = 1;
                break;
            }
            /* ECHO/PING detection: after parsing 4-char first arg, set flag
             * to skip scratch copy for remaining args (they stay in buf).
             * handle_parsed_command fast path reads them via argv/argl. */
            if (i == 0 && c && blen == 4) {
                const unsigned char *_c = buf + p;
                if (((_c[0]|32)=='e' && (_c[1]|32)=='c' && (_c[2]|32)=='h' && (_c[3]|32)=='o') ||
                    ((_c[0]|32)=='p' && (_c[1]|32)=='i' && (_c[2]|32)=='n' && (_c[3]|32)=='g'))
                    no_scratch = 1;
            }
            /* always copy argv[0] for NUL-termination (normal path may need it);
             * for i>=1 with no_scratch: point to buf, skip copy */
            if (i == 0 || !no_scratch) {
                if (scratch_off + (size_t)blen + 1 <= PARSE_SCRATCH) {
                    argv[i] = scratch + scratch_off;
                    scratch_off += (size_t)blen + 1;
                } else {
                    argv[i] = (char *)kvs_malloc((size_t)blen + 1);
                    if (!argv[i]) { malformed = 1; break; }
                }
                memcpy(argv[i], buf + p, (size_t)blen);
                argv[i][blen] = 0;
            } else {
                argv[i] = (char *)(buf + p);  /* no copy, not NUL-terminated */
            }
            argl[i] = (size_t)blen;
            p += (size_t)blen;
            if (!(buf[p] == '\r' && buf[p + 1] == '\n')) {
                malformed = 1;
                break;
            }
            p += 2;
        }
        if (incomplete) {
            for (int i = 0; i < argc; ++i) {
                if (no_scratch && i >= 1) continue;  /* buf ptr, not allocated */
                if (argv[i] < scratch || argv[i] >= scratch + PARSE_SCRATCH) kvs_free(argv[i]);
            }
            break;
        }
        if (malformed) {
            for (int i = 0; i < argc; ++i) {
                if (no_scratch && i >= 1) continue;
                if (argv[i] < scratch || argv[i] >= scratch + PARSE_SCRATCH) kvs_free(argv[i]);
            }
            scratch_off = 0;
            if (p > start) pos = p;
            else break;
            continue;
        }
        handle_parsed_command(c, argc, argv, argl, buf + start, p - start, from_replication);
        for (int i = 0; i < argc; ++i) {
            if (no_scratch && i >= 1) continue;  /* buf ptr, not allocated */
            if (argv[i] < scratch || argv[i] >= scratch + PARSE_SCRATCH) kvs_free(argv[i]);
        }
        scratch_off = 0;
        pos = p;
    }
    if (pos > 0 && pos < *len) {
        memmove(buf, buf + pos, *len - pos);
        *len -= pos;
    } else if (pos >= *len) {
        *len = 0;
    }
    return 0;
}

typedef int (*snapshot_emit_fn)(void *ctx, const unsigned char *buf, size_t len);

typedef struct snapshot_sink_s {
    snapshot_emit_fn emit;
    void *ctx;
} snapshot_sink_t;

static int emit_cmd3_sink(snapshot_sink_t *sink, const char *cmd, const char *a1, const char *a2) {
    unsigned char buf[BUFFER_CAP];
    size_t n = resp_build_cmd3(buf, sizeof(buf), cmd, a1, a2);
    return sink->emit(sink->ctx, buf, n);
}

static int emit_cmd4_sink(snapshot_sink_t *sink, const char *cmd, const char *a1, const char *a2, const char *a3) {
    unsigned char buf[BUFFER_CAP];
    size_t n = resp_build_cmd4(buf, sizeof(buf), cmd, a1, a2, a3);
    return sink->emit(sink->ctx, buf, n);
}

static int snapshot_emit_fp(void *ctx, const unsigned char *buf, size_t len) {
    FILE *fp = (FILE *)ctx;
    return fwrite(buf, 1, len, fp) == len ? 0 : -1;
}

static int snapshot_emit_fd(void *ctx, const unsigned char *buf, size_t len) {
    long long *state = (long long *)ctx;
    int fd = (int)state[0];
    long long off = state[1];
    if (persist_write_raw_fd(fd, buf, len, &off) != 0) return -1;
    state[1] = off;
    return 0;
}

/* deprecated: unused after KVSD unification */
static int maybe_emit_expire_sink(snapshot_sink_t *sink, int engine, const char *key) {
    long long ttl = kvs_expire_ttl(&global_expire, engine, key);
    if (ttl < 0) return 0;
    char sec[32]; snprintf(sec, sizeof(sec), "%lld", ttl);
    const char *cmd =
        engine == KVS_ENGINE_ARRAY ? "EXPIRE" :
        engine == KVS_ENGINE_RBTREE ? "REXPIRE" :
        engine == KVS_ENGINE_HASH ? "HEXPIRE" : "XEXPIRE";
    return emit_cmd3_sink(sink, cmd, key, sec);
}

/* deprecated: unused after KVSD unification */
static int snapshot_array_sink(snapshot_sink_t *sink) {
    for (int i = 0; i < KVS_ARRAY_SIZE; ++i) {
        if (global_array.table && global_array.table[i].key) {
            if (emit_cmd3_sink(sink, "SET", global_array.table[i].key, global_array.table[i].value) != 0) return -1;
            if (maybe_emit_expire_sink(sink, KVS_ENGINE_ARRAY, global_array.table[i].key) != 0) return -1;
        }
    }
    return 0;
}

/* deprecated: unused after KVSD unification */
static int snapshot_hash_sink(snapshot_sink_t *sink) {
    for (int t = 0; t < 2; t++) {
        if (!global_hash.ht[t].nodes) continue;
        for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
            for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                if (emit_cmd3_sink(sink, "HSET", node->key, node->value) != 0) return -1;
                if (maybe_emit_expire_sink(sink, KVS_ENGINE_HASH, node->key) != 0) return -1;
            }
        }
    }
    return 0;
}

/* deprecated: unused after KVSD unification */
static int snapshot_rbtree_node_sink(snapshot_sink_t *sink, rbtree_node *node, rbtree_node *nil) {
    if (node == nil) return 0;
    if (snapshot_rbtree_node_sink(sink, node->left, nil) != 0) return -1;
    if (emit_cmd3_sink(sink, "RSET", node->key, (char *)node->value) != 0) return -1;
    if (maybe_emit_expire_sink(sink, KVS_ENGINE_RBTREE, node->key) != 0) return -1;
    if (snapshot_rbtree_node_sink(sink, node->right, nil) != 0) return -1;
    return 0;
}

/* deprecated: unused after KVSD unification */
static int snapshot_skiptable_cb_sink(const char *key, const char *value, void *arg) {
    snapshot_sink_t *sink = (snapshot_sink_t *)arg;
    if (emit_cmd3_sink(sink, "XSET", key, value) != 0) return -1;
    if (maybe_emit_expire_sink(sink, KVS_ENGINE_SKIPTABLE, key) != 0) return -1;
    return 0;
}

/* deprecated: unused after KVSD unification */
static int snapshot_skiptable_sink(snapshot_sink_t *sink) {
    return kvs_skiptable_foreach(&global_skiptable, snapshot_skiptable_cb_sink, sink);
}

/* deprecated: unused after KVSD unification */
static int snapshot_doc_field_cb_sink(const char *name, const char *value, void *arg) {
    void **ctx = (void **)arg;
    snapshot_sink_t *sink = (snapshot_sink_t *)ctx[0];
    const char *key = (const char *)ctx[1];
    return emit_cmd4_sink(sink, "DOCSET", key, name, value);
}

/* deprecated: unused after KVSD unification */
static int snapshot_doc_cb_sink(const char *key, kvs_doc_t *doc, void *arg) {
    snapshot_sink_t *sink = (snapshot_sink_t *)arg;
    (void)doc;
    void *ctx[2] = { sink, (void *)key };
    return kvs_doc_foreach_field(&global_doc, key, snapshot_doc_field_cb_sink, ctx);
}

/* deprecated: unused after KVSD unification */
static int snapshot_doc_sink(snapshot_sink_t *sink) {
    return kvs_doc_foreach(&global_doc, snapshot_doc_cb_sink, sink);
}

/* deprecated: unused after KVSD unification */
static int snapshot_all_sink(snapshot_sink_t *sink) {
    if (!sink || !sink->emit) return -1;
    if (snapshot_array_sink(sink) != 0) return -1;
    if (snapshot_rbtree_node_sink(sink, global_rbtree.root, global_rbtree.nil) != 0) return -1;
    if (snapshot_hash_sink(sink) != 0) return -1;
    if (snapshot_skiptable_sink(sink) != 0) return -1;
    if (snapshot_doc_sink(sink) != 0) return -1;
    return 0;
}

/* deprecated: unused after KVSD unification */
int kvs_snapshot_to_fp(FILE *fp) {
    snapshot_sink_t sink;
    if (!fp) return -1;
    sink.emit = snapshot_emit_fp;
    sink.ctx = fp;
    return snapshot_all_sink(&sink);
}

/* deprecated: unused after KVSD unification */
int kvs_snapshot_to_fd(int fd) {
    snapshot_sink_t sink;
    long long fd_state[2];
    if (fd < 0) return -1;
    fd_state[0] = fd;
    fd_state[1] = 0;
    sink.emit = snapshot_emit_fd;
    sink.ctx = fd_state;
    return snapshot_all_sink(&sink);
}

/* ── SAVE buffered write context ──────────────────────
 * Replaces per-field write() syscalls with 4MB user-space
 * buffer + memcpy.  Reduces syscall count from O(keys)
 * (~6-7 per key) to O(data_size / 4MB). */
#define DUMP_BUF_SIZE (4 * 1024 * 1024)

struct dump_ctx {
    int fd;
    unsigned char *buf;
    size_t pos;
};

static int dump_flush(struct dump_ctx *ctx) {
    size_t off = 0;
    while (off < ctx->pos) {
        ssize_t w = write(ctx->fd, ctx->buf + off, ctx->pos - off);
        if (w <= 0) return -1;
        off += (size_t)w;
    }
    ctx->pos = 0;
    return 0;
}

static int dump_write_buf(struct dump_ctx *ctx, const void *data, size_t len) {
    if (ctx->pos + len > DUMP_BUF_SIZE) {
        if (dump_flush(ctx) != 0) return -1;
        /* still too large for empty buffer: write-through */
        if (len > DUMP_BUF_SIZE) {
            size_t off = 0;
            while (off < len) {
                ssize_t w = write(ctx->fd, (const char *)data + off, len - off);
                if (w <= 0) return -1;
                off += (size_t)w;
            }
            return 0;
        }
    }
    memcpy(ctx->buf + ctx->pos, data, len);
    ctx->pos += len;
    return 0;
}

static int dump_skiptable_write_kv(const char *key, const char *value, void *arg) {
    struct dump_ctx *ctx = (struct dump_ctx *)arg;
    uint8_t eng = (uint8_t)KVS_ENGINE_SKIPTABLE;
    uint8_t flags = 0;
    uint32_t klen = (uint32_t)strlen(key);
    uint32_t vlen = (uint32_t)strlen(value);
    long long ttl_sec = kvs_expire_ttl(&global_expire, KVS_ENGINE_SKIPTABLE, key);
    uint64_t exp = 0;
    if (ttl_sec >= 0) { flags = KVSD_FLAG_HAS_EXPIRE; exp = (uint64_t)(kvs_now_ms() + ttl_sec * 1000); }
    if (dump_write_buf(ctx, &eng, 1) != 0) return -1;
    if (dump_write_buf(ctx, &flags, 1) != 0) return -1;
    if (dump_write_buf(ctx, &klen, 4) != 0) return -1;
    if (dump_write_buf(ctx, key, klen) != 0) return -1;
    if (dump_write_buf(ctx, &vlen, 4) != 0) return -1;
    if (dump_write_buf(ctx, value, vlen) != 0) return -1;
    if (flags & KVSD_FLAG_HAS_EXPIRE) {
        if (dump_write_buf(ctx, &exp, 8) != 0) return -1;
    }
    return 0;
}

int kvs_dump_to_fd(int fd, unsigned long long aof_offset) {
    if (fd < 0) return -1;

    unsigned char *dbuf = (unsigned char *)kvs_malloc(DUMP_BUF_SIZE);
    if (!dbuf) return -1;
    struct dump_ctx ctx = { .fd = fd, .buf = dbuf, .pos = 0 };

    /* header: AOF file size at dump creation time */
    if (dump_write_buf(&ctx, &aof_offset, sizeof(aof_offset)) != 0)
        { kvs_free(dbuf); return -1; }

    /* dump helper: serialize [1B eng][1B flags][4B klen][key][4B vlen][value][8B expire?] */
#define DUMP_WRITE_KV_EX(engine_id, key, value) do {                 \
    uint8_t  _eng = (uint8_t)(engine_id);                             \
    uint8_t  _flags = 0;                                              \
    uint32_t _klen = (uint32_t)strlen(key);                           \
    uint32_t _vlen = (uint32_t)strlen(value);                         \
    long long _ttl_sec = kvs_expire_ttl(&global_expire, _eng, key);   \
    uint64_t _exp = 0;                                                \
    if (_ttl_sec >= 0) { _flags = KVSD_FLAG_HAS_EXPIRE;               \
        _exp = (uint64_t)(kvs_now_ms() + _ttl_sec * 1000); }          \
    if (dump_write_buf(&ctx, &_eng, 1) != 0)    { kvs_free(dbuf); return -1; } \
    if (dump_write_buf(&ctx, &_flags, 1) != 0)  { kvs_free(dbuf); return -1; } \
    if (dump_write_buf(&ctx, &_klen, 4) != 0)   { kvs_free(dbuf); return -1; } \
    if (dump_write_buf(&ctx, key, _klen) != 0)  { kvs_free(dbuf); return -1; } \
    if (dump_write_buf(&ctx, &_vlen, 4) != 0)   { kvs_free(dbuf); return -1; } \
    if (dump_write_buf(&ctx, value, _vlen) != 0){ kvs_free(dbuf); return -1; } \
    if (_flags & KVSD_FLAG_HAS_EXPIRE)                                 \
        if (dump_write_buf(&ctx, &_exp, 8) != 0){ kvs_free(dbuf); return -1; } \
} while(0)

    /* iterate all hash entries */
    for (int t = 0; t < 2; t++) {
        if (!global_hash.ht[t].nodes) continue;
        for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
            for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                DUMP_WRITE_KV_EX(KVS_ENGINE_HASH, node->key, node->value);
            }
        }
    }

    /* iterate array entries */
    for (int i = 0; i < KVS_ARRAY_SIZE; ++i) {
        if (global_array.table && global_array.table[i].key) {
            DUMP_WRITE_KV_EX(KVS_ENGINE_ARRAY, global_array.table[i].key, global_array.table[i].value);
        }
    }

    /* iterate rbtree entries */
    {
        rbtree_node *nil = global_rbtree.nil;
        rbtree_node **stack = (rbtree_node **)kvs_malloc(sizeof(rbtree_node*) * 256);
        int top = 0;
        rbtree_node *cur = global_rbtree.root;
        if (stack) {
            while (cur != nil || top > 0) {
                while (cur != nil) {
                    stack[top++] = cur;
                    cur = cur->left;
                }
                cur = stack[--top];
                DUMP_WRITE_KV_EX(KVS_ENGINE_RBTREE, cur->key, (char*)cur->value);
                cur = cur->right;
            }
            kvs_free(stack);
        }
    }

    /* iterate skiptable entries */
    kvs_skiptable_foreach(&global_skiptable, dump_skiptable_write_kv, &ctx);

    /* iterate doc entries (squash newlines to spaces) */
    {
        char doc_buf[BUFFER_CAP];
        for (int i = 0; i < global_doc.size; ++i) {
            for (kvs_doc_t *d = global_doc.buckets[i]; d; d = d->next) {
                int doc_pos = 0;
                for (int j = 0; j < d->bucket_count && doc_pos < (int)sizeof(doc_buf) - 4; ++j) {
                    for (kvs_doc_field_t *f = d->fields[j]; f; f = f->next) {
                        int n = snprintf(doc_buf + doc_pos, sizeof(doc_buf) - (size_t)doc_pos,
                            "%s=%s ", f->name, f->value);
                        if (n > 0) doc_pos += n;
                    }
                }
                if (doc_pos > 0 && doc_buf[doc_pos-1] == ' ') doc_pos--;
                doc_buf[doc_pos] = '\0';
                DUMP_WRITE_KV_EX(KVS_ENGINE_DOC, d->key, doc_buf);
            }
        }
    }

#undef DUMP_WRITE_KV_EX

    /* final flush + free */
    if (dump_flush(&ctx) != 0) { kvs_free(dbuf); return -1; }
    kvs_free(dbuf);
    return 0;
}

/* master 启动时 spawn 独立的 ebpf-proxy 进程（仍单独进程，但无需手动启动）。
 * 若 proxy_cfg map 已 pin（proxy 已由外部手动启动），不重复 spawn。
 * ebpf-proxy 需 root 加载 BPF，由 master（sudo 启动）fork 出的子进程继承 root 权限。 */
static int spawn_ebpf_proxy(void) {
    if (!g_cfg.ebpf_proxy_bin[0] || !g_cfg.ebpf_client_capture_obj[0]) return 0;

    char cfg_path[512];
    snprintf(cfg_path, sizeof(cfg_path), "%s/proxy_cfg", g_cfg.ebpf_pin_path);
    int fd = bpf_obj_get(cfg_path);
    if (fd >= 0) { close(fd); return 0; }   /* ebpf-proxy 已在跑 */

    pid_t pid;
    char *argv[] = {
        (char *)g_cfg.ebpf_proxy_bin,
        "--pin-path", (char *)g_cfg.ebpf_pin_path,
        "--obj-path", (char *)g_cfg.ebpf_client_capture_obj,
        NULL,
    };
    int rc = posix_spawn(&pid, g_cfg.ebpf_proxy_bin, NULL, NULL, argv, environ);
    if (rc != 0) {
        fprintf(stderr, "master: spawn ebpf-proxy failed (%s) — eBPF+tcp 增量将不可用，"
                        "可手动启动 %s\n", strerror(rc), g_cfg.ebpf_proxy_bin);
        return -1;
    }
    fprintf(stderr, "master: spawned ebpf-proxy pid=%d (bin=%s pin=%s obj=%s)\n",
            (int)pid, g_cfg.ebpf_proxy_bin, g_cfg.ebpf_pin_path, g_cfg.ebpf_client_capture_obj);
    return 0;
}

static void *ebpf_proxy_init_bg(void *arg) {
    (void)arg;
    int proxy_cfg_fd = -1;
    char cfg_path[512];
    snprintf(cfg_path, sizeof(cfg_path), "%s/proxy_cfg", g_cfg.ebpf_pin_path);
    for (int retry = 0; retry < 300; retry++) {
        proxy_cfg_fd = bpf_obj_get(cfg_path);
        if (proxy_cfg_fd >= 0) break;
        usleep(100000);
    }
    if (proxy_cfg_fd >= 0) {
        __u64 val;
        char key[32] = {0};
        val = (__u64)getpid();
        snprintf(key, sizeof(key), "master_pid");
        bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
        val = (__u64)g_cfg.port;
        snprintf(key, sizeof(key), "master_port");
        bpf_map_update_elem(proxy_cfg_fd, key, &val, BPF_ANY);
        fprintf(stderr, "master: wrote config to ebpf-proxy (pid=%d port=%d)\n",
                getpid(), g_cfg.port);
        close(proxy_cfg_fd);
    } else {
        fprintf(stderr, "master: ebpf-proxy proxy_cfg not available (%s), "
                "continuing without ebpf-proxy\n", strerror(errno));
    }
    return NULL;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    if (parse_args(argc, argv) != 0) {
        fprintf(stderr, "Usage: %s [kvstore.conf] [--role master|slave] [options]\n"
                "  kvstore.conf 中的所有选项均可通过命令行覆盖:\n"
                "  --port PORT             监听端口 (默认 5160)\n"
                "  --role master|slave     角色 (默认 master)\n"
                "  --master-host HOST      主机地址 (默认 192.168.233.128)\n"
                "  --master-port PORT      主机端口 (默认 5160)\n"
                "  --dump PATH             dump 文件路径 (默认 kvstore.dump)\n"
                "  --aof PATH              AOF 文件路径 (默认 kvstore.aof)\n"
                "  --mem libc|jemalloc|custom  内存后端 (默认 libc)\n"
                "  --net reactor|proactor|ntyco  网络模型 (默认 reactor)\n"
                "  --repl-fullsync-transport tcp|rdma  全量同步传输 (默认 rdma)\n"
                "  --repl-realtime-transport tcp|kprobe-rdma|ebpf  增量同步传输 (默认 kprobe-rdma)\n"
                "  --kprobe-enabled        启用 kprobe+RDMA 增量同步\n"
                "  --rdma-dev DEV          RDMA 设备 (默认 siw0)\n"
                "  --rdma-port PORT        RDMA 监听端口 (默认 0 = main+1)\n"
                "  --rdma-ib-port PORT     RDMA IB 端口 (默认 1)\n"
                "  --rdma-gid-idx IDX      RDMA GID 索引 (默认 1)\n"
                "  --rdma-send-slots N     发送管道深度 (默认 16)\n"
                "  --rdma-recv-slots N     接收槽位数 (默认 64)\n"
                "  --rdma-chunk-size SIZE  分块大小 (默认 262144)\n"
                "  --rdma-qp-wr-depth N    QP 队列深度 (默认 64)\n"
                "  --appendfsync always|off  AOF fsync 策略 (默认 always)\n"
                "  --ebpf-enabled          启用 eBPF sockmap\n"
                "  --ebpf-obj PATH         eBPF 对象文件路径\n"
                "  --ebpf-pin PATH         eBPF pin 路径\n"
                "  --ebpf-redirect         启用 eBPF 重定向\n"
                "  --ebpf-redirect-key N   eBPF 重定向 key\n"
                "  --ebpf-forward          启用 eBPF 转发\n"
                "  --aof-disable           禁用 AOF 持久化\n"
                "  --autosnap RULES        自动快照规则 (例如 60:1000,300:10)\n"
                "  --sentinel              启用哨兵模式\n"
                "  --sentinel-master-name NAME  哨兵主节点名\n"
                "  --sentinel-monitor-host HOST  哨兵监控主机\n"
                "  --sentinel-monitor-port PORT  哨兵监控端口\n"
                "  --sentinel-known-slaves LIST  已知从机列表\n"
                "  --sentinel-down-after MS     判定下线毫秒数\n"
                "  --sentinel-failover-timeout MS  故障转移超时\n"
                "  --sentinel-quorum N     哨兵法定人数\n"
                "  --log-mode MODE         日志模式 (默认 info)\n"
                "  --config PATH           配置文件路径 (默认 ./kvstore.conf)\n", argv[0]);
        return 1;
    }
    if (!strcmp(g_cfg.mem_backend, "jemalloc")) {
        if (kvs_mem_prepare_process(g_cfg.mem_backend, argv[0], argv) != 0 && getenv("KVS_MEM_JEMALLOC_ACTIVE") == NULL) {
            fprintf(stderr, "failed to prepare jemalloc process image\n");
            return 1;
        }
    }
    if (kvs_mem_init(g_cfg.mem_backend) != 0) {
        fprintf(stderr, "failed to init memory backend: %s\n", g_cfg.mem_backend);
        return 1;
    }
    kvs_array_create(&global_array);
    kvs_rbtree_create(&global_rbtree);
    kvs_hash_create(&global_hash);
    kvs_skiptable_create(&global_skiptable);
    kvs_expire_create(&global_expire);
    kvs_doc_create(&global_doc);
   
    if (g_cfg.is_sentinel) {
        return sentinel_start();
    }
    if (persist_init() != 0) { perror("persist_init"); return 1; }
    persist_recover();
    if (g_cfg.role == ROLE_SLAVE) repl_slave_state_load();
    /* eBPF+tcp 增量：master 启动时自动拉起独立的 ebpf-proxy 进程（无需手动启动），
     * 随后后台线程向 proxy_cfg map 写入 master 配置。 */
    if (g_cfg.role == ROLE_MASTER) {
        if (strstr(g_cfg.repl_realtime_transport, "ebpf"))
            spawn_ebpf_proxy();
        pthread_t tid;
        pthread_create(&tid, NULL, ebpf_proxy_init_bg, NULL);
        pthread_detach(tid);
    }

    /* kprobe+RDMA 增量同步初始化 */
    if (g_cfg.kprobe_enabled &&
        !strcasecmp(g_cfg.repl_realtime_transport, "kprobe-rdma")) {
        if (g_cfg.role == ROLE_MASTER) {
            if (repl_kprobe_rdma_master_init() != 0) {
                fprintf(stderr, "kprobe rdma master init failed, disabling\n");
                g_cfg.kprobe_enabled = 0;
            }
        } else if (g_cfg.role == ROLE_SLAVE) {
            repl_kprobe_rdma_slave_init();
        }
    }

    if (!strcmp(g_cfg.net_backend, "reactor")) {
        /* 启动复制转发线程。注意：master 的 reactor_start 为无限循环，进程由信号终止
         * （SIGINT/SIGTERM 默认动作，无 teardown 路径），故 repl_fwd_stop 实际不会被调用；
         * 进程退出时由 OS 回收线程/epoll/队列，无需人工清理。repl_fwd_stop 保留供未来的
         * 优雅关闭路径使用（Minor 已记文档）。 */
        if (g_cfg.role == ROLE_MASTER) repl_fwd_start();
        return reactor_start();
    } else if (!strcmp(g_cfg.net_backend, "proactor")) {
        return proactor_start((unsigned short)g_cfg.port);
    } else if (!strcmp(g_cfg.net_backend, "ntyco")) {
        return ntyco_start((unsigned short)g_cfg.port);
    } else {
        fprintf(stderr, "unknown net backend: %s\n", g_cfg.net_backend);
        return 1;
    }
}
