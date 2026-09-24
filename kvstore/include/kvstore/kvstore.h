#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <pthread.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/resource.h>

#include "kvstore/replication/fullsync.h"

#ifndef KVS_ENABLE_RDMA
#define KVS_ENABLE_RDMA 0
#endif

#ifndef KVS_ENABLE_EBPF
#define KVS_ENABLE_EBPF 0
#endif

#ifndef KVS_ENABLE_KPROBE_RDMA
#define KVS_ENABLE_KPROBE_RDMA 0
#endif

/* Branch prediction hints for CPU pipeline optimization */
#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

#define BUFFER_CAP 65536
#define INBUF_CAP (1024 * 1024)  /* 接收缓冲（支持 1MB 级大 value 命令；独立于 BUFFER_CAP，避免栈数组爆栈） */
#define MAX_EVENTS 1024
#define LISTEN_BACKLOG 128

#define ROLE_MASTER 1
#define ROLE_SLAVE 2

#define KVS_REPL_TRANSPORT_TCP 1
#define KVS_REPL_TRANSPORT_RDMA 2
#define KVS_REPL_TRANSPORT_EBPF 3
#define KVS_REPL_TRANSPORT_KPROBE_RDMA 4
#define KVS_REPL_TRANSPORT_EBPF_TCP 5

/* KVSD format flags */
#define KVSD_FLAG_HAS_EXPIRE  0x01   /* record has 8-byte expire_ms after value */

/* Replication send context: which transport to use */
#define KVS_REPL_SEND_FULLSYNC  1   /* bulk existing data: RDMA */
#define KVS_REPL_SEND_REALTIME  2   /* incremental real-time: eBPF */

#define KVS_ENGINE_ARRAY      1
#define KVS_ENGINE_RBTREE     2
#define KVS_ENGINE_HASH       3
#define KVS_ENGINE_SKIPTABLE  4
#define KVS_ENGINE_DOC        5

#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1
#define ENABLE_HASH 1
#define ENABLE_SKIPTABLE 1

#define KVS_ARRAY_SIZE (1024 * 1024)  /* 数组引擎 key 槽位上限（1024 太小，大库易满） */
#define MAX_TABLE_SIZE 1024
#define ENABLE_KEY_POINTER 1
#define RED 1
#define BLACK 2
#define ENABLE_KEY_CHAR 1

typedef int (*msg_handler)(char *msg, int length, char *response);

typedef enum {
    KVS_AOF_FSYNC_OFF    = 0,
    KVS_AOF_FSYNC_ALWAYS = 1,
} kvs_aof_fsync_policy_t;

#if ENABLE_ARRAY
typedef struct kvs_array_item_s {
    char *key;
    char *value;
} kvs_array_item_t;

struct kvs_hash_s;  /* 前置声明：kvs_array_t 持有 key→slot 哈希索引指针 */

typedef struct kvs_array_s {
    kvs_array_item_t *table;
    int idx;
    int total;
    int next_slot;          /* 下一个从未使用的槽位（空闲槽由 free_list 复用） */
    int *free_list;         /* 空闲槽位栈（del 释放的槽） */
    int free_count;
    int free_cap;
    struct kvs_hash_s *index;  /* key → slot 哈希索引（O(1) 查找，替代 O(N) 线性扫描） */
} kvs_array_t;

extern kvs_array_t global_array;
int kvs_array_create(kvs_array_t *inst);
void kvs_array_destory(kvs_array_t *inst);
int kvs_array_set(kvs_array_t *inst, char *key, char *value);
char* kvs_array_get(kvs_array_t *inst, char *key);
int kvs_array_del(kvs_array_t *inst, char *key);
int kvs_array_mod(kvs_array_t *inst, char *key, char *value);
int kvs_array_exist(kvs_array_t *inst, char *key);
#endif

#if ENABLE_RBTREE
#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE;
#endif

typedef struct _rbtree_node {
    unsigned char color;
    struct _rbtree_node *right;
    struct _rbtree_node *left;
    struct _rbtree_node *parent;
    KEY_TYPE key;
    void *value;
} rbtree_node;

typedef struct _rbtree {
    rbtree_node *root;
    rbtree_node *nil;
} rbtree;

typedef struct _rbtree kvs_rbtree_t;
extern kvs_rbtree_t global_rbtree;
int kvs_rbtree_create(kvs_rbtree_t *inst);
void kvs_rbtree_destory(kvs_rbtree_t *inst);
int kvs_rbtree_set(kvs_rbtree_t *inst, char *key, char *value);
char* kvs_rbtree_get(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_del(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_mod(kvs_rbtree_t *inst, char *key, char *value);
int kvs_rbtree_exist(kvs_rbtree_t *inst, char *key);
#endif

#if ENABLE_HASH
typedef struct hashnode_s {
    uint32_t hv;              // cached FNV-1a hash (32-bit, not modulo-reduced)
#if ENABLE_KEY_POINTER
    char *key;
    char *value;
#else
    char key[128];
    char value[512];
#endif
    size_t vlen;              /* value 字节长度（支持含 '\0' 的二进制 value） */
    struct hashnode_s *next;
} hashnode_t;

typedef struct hashtable_s {
    hashnode_t **nodes;
    int max_slots;
    int count;
} hashtable_t;

typedef struct kvs_hash_s {
    hashtable_t ht[2];        // ht[0]: active, ht[1]: expansion target
    int rehash_idx;           // next bucket to migrate, -1 = no rehash in progress
} kvs_hash_t;

extern kvs_hash_t global_hash;
int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destory(kvs_hash_t *hash);
int kvs_hash_set(kvs_hash_t *hash, char *key, char *value);
char *kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_set_len(kvs_hash_t *hash, char *key, char *value, size_t vlen);
char *kvs_hash_get_len(kvs_hash_t *hash, char *key, size_t *vlen);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);
/* kvs_vector.c：语义向量检索（VSEARCH 命令实现） */
#define KVS_VSEARCH_DEFAULT_PREFIX "semcache:"
int kvs_vector_search(int dim, const float *query, int topk,
                      const char *prefix, int plen, char *resp, int cap);
#endif

#if ENABLE_SKIPTABLE
typedef struct kvs_skiptable_s kvs_skiptable_t;
typedef int (*kvs_skip_visit_cb)(const char *key, const char *value, void *arg);
extern kvs_skiptable_t global_skiptable;
int kvs_skiptable_create(kvs_skiptable_t *inst);
void kvs_skiptable_destory(kvs_skiptable_t *inst);
int kvs_skiptable_set(kvs_skiptable_t *inst, char *key, char *value);
char *kvs_skiptable_get(kvs_skiptable_t *inst, char *key);
int kvs_skiptable_mod(kvs_skiptable_t *inst, char *key, char *value);
int kvs_skiptable_del(kvs_skiptable_t *inst, char *key);
int kvs_skiptable_exist(kvs_skiptable_t *inst, char *key);
int kvs_skiptable_foreach(kvs_skiptable_t *inst, kvs_skip_visit_cb cb, void *arg);
#endif

#define ENABLE_DOC 1

#if ENABLE_DOC
#define KVS_DOC_BUCKETS 1024
#define KVS_DOC_FIELD_BUCKETS 16

typedef struct kvs_doc_field_s {
    char *name;
    char *value;
    struct kvs_doc_field_s *next;
} kvs_doc_field_t;

typedef struct kvs_doc_s {
    char *key;
    kvs_doc_field_t **fields;
    int field_count;
    int bucket_count;
    struct kvs_doc_s *next;
} kvs_doc_t;

typedef struct kvs_doc_table_s {
    kvs_doc_t **buckets;
    int size;
    int count;
} kvs_doc_table_t;

extern kvs_doc_table_t global_doc;
int kvs_doc_create(kvs_doc_table_t *tab);
void kvs_doc_destroy(kvs_doc_table_t *tab);
int kvs_doc_set(kvs_doc_table_t *tab, const char *key, const char *field, const char *value);
char *kvs_doc_get(kvs_doc_table_t *tab, const char *key, const char *field);
int kvs_doc_del_field(kvs_doc_table_t *tab, const char *key, const char *field);
int kvs_doc_del(kvs_doc_table_t *tab, const char *key);
int kvs_doc_exist(kvs_doc_table_t *tab, const char *key);
int kvs_doc_field_exist(kvs_doc_table_t *tab, const char *key, const char *field);
int kvs_doc_field_count(kvs_doc_table_t *tab, const char *key);

typedef int (*kvs_doc_field_visit_cb)(const char *name, const char *value, void *arg);
int kvs_doc_foreach_field(kvs_doc_table_t *tab, const char *key, kvs_doc_field_visit_cb cb, void *arg);

typedef int (*kvs_doc_visit_cb)(const char *key, kvs_doc_t *doc, void *arg);
int kvs_doc_foreach(kvs_doc_table_t *tab, kvs_doc_visit_cb cb, void *arg);
#endif

#define KVS_EXPIRE_BUCKETS 8192
#define KVS_EXPIRE_HEAP_INIT_CAP 1024
typedef struct kvs_expire_item_s {
    char *key;
    int engine;
    long long expire_at_ms;
    size_t heap_index;
    struct kvs_expire_item_s *next;
} kvs_expire_item_t;

typedef struct kvs_expire_table_s {
    kvs_expire_item_t **buckets;
    size_t size;
    size_t count;
    kvs_expire_item_t **heap;
    size_t heap_size;
    size_t heap_cap;
} kvs_expire_table_t;
extern kvs_expire_table_t global_expire;

#define OUT_RING_SIZE (256 * 1024)

typedef struct conn_s {
    int fd;
    int is_listener;
    int is_replica;
    int authed;                 /* 客户端是否已通过 AUTH（requirepass 启用时检查） */
    int repl_draining;
    int repl_fullsync_pending;
    int repl_transport_kind;
    unsigned long long repl_offset_sent;
    unsigned long long repl_applied_offset_ack;
    unsigned long long repl_durable_offset_ack;
    long long repl_last_ack_ms;
    long long repl_last_send_ms;
    unsigned char *inbuf;          /* 独立 malloc 的接收缓冲（accept 时分配，不清零） */
    size_t in_len;
    unsigned char *out_ring;       /* 独立 malloc 的输出 ring（accept 时分配，不清零） */
    size_t out_ring_head;                   /* read position */
    size_t out_ring_tail;                   /* write position */
    size_t out_ring_len;                    /* pending bytes */
    int fwd_healthy;                   /* kprobe fwd health: 1=healthy, 0=fallback */
    time_t fwd_last_active;            /* last successful kprobe fwd send timestamp */
    uint32_t epoll_events;             /* cached epoll event mask to skip redundant epoll_ctl */
    int defer_epollout;                /* defer EPOLLOUT reg during on_read pipeline batch */
    /* ---- Fullsync transfer 状态（每个 replication 连接一份） ---- */
    uint64_t repl_transfer_id;         /* 当前全量同步 transfer ID */
    int repl_fullsync_state;           /* kvs_fullsync_state_t */
    uint64_t repl_fullsync_expected_bytes; /* 期望的 KVSD 总字节数 */
    int repl_fullsync_mode;            /* kvs_fullsync_mode_t：WRITE / sendfile */
    long long repl_fullsync_deadline_ms;   /* MR 等待截止时间 */
    struct conn_s *next_replica;
} conn_t;

#define KVS_AUTOSNAP_RULES_MAX 8

typedef struct {
    long long seconds;
    long long changes;
} kvs_autosnap_rule_t;

typedef struct {
    int role;
    int port;
    char master_host[128];
    int master_port;
    char dump_path[256];
    char aof_path[256];
    char mem_backend[32];
    char net_backend[32];
    char repl_transport_backend[32];
    char repl_fullsync_transport[32];   /* transport for fullsync/snapshot (RDMA) */
    char repl_realtime_transport[32];   /* transport for realtime broadcast (eBPF) */
    int ebpf_enabled;               /* 0=禁用, 1=由独立进程管理eBPF */
    char ebpf_obj_path[256];
    char ebpf_pin_path[256];
    char ebpf_proxy_bin[256];               /* ebpf-proxy 独立进程可执行路径（master 启动时 spawn） */
    char ebpf_client_capture_obj[256];      /* client_capture BPF 对象路径（fentry 捕获 tcp_recvmsg） */
    int ebpf_redirect;
    int ebpf_redirect_key;
    int ebpf_forward;
    char rdma_dev[32];
    int rdma_ib_port;
    int rdma_gid_idx;
    int rdma_port;          /* RDMA listener port (0 = auto: main port + 1) */
    int rdma_send_slots;
    int rdma_recv_slots;
    int rdma_chunk_size;
    int rdma_qp_wr_depth;
    int repl_rdma_write_buf_max_mb;   /* slave WRITE 目标大小上限（MB），超限拒绝 → sendfile 回退 */
    kvs_aof_fsync_policy_t aof_fsync;
    int aof_fsync_per_command;   /* 逐条刷盘：每条命令立即 write+fsync（实验，对齐 redis 行为） */
    int aof_fsync_sync;          /* 同步：逐条且响应前等 fsync 完成（redis 同步逐条语义） */
    int aof_group_commit_window_us; /* 异步批量：攒批时间窗（µs），0=每 epoll 周期 flush（现状） */
    char log_mode[32];
    int autosnap_rule_count;
    kvs_autosnap_rule_t autosnap_rules[KVS_AUTOSNAP_RULES_MAX];

    int is_sentinel;
    char sentinel_master_name[64];
    char sentinel_monitor_host[128];
    int sentinel_monitor_port;
    char sentinel_known_slaves[512];
    int sentinel_down_after_ms;
    int sentinel_failover_timeout_ms;
    int sentinel_quorum;

    /* kprobe+RDMA 增量同步 */
    int kprobe_enabled;                 /* 0=禁用 1=启用 */
    char repl_kprobe_obj_path[256];     /* kprobe BPF 对象文件路径 */
    char requirepass[256];              /* 客户端访问密码；空 = 不启用鉴权 */
} kv_config_t;

typedef struct {
    const char *backend_name;
    int backend_id;
    int initialized;
    size_t small_max_size;
    size_t class_count;
    unsigned long long alloc_calls;
    unsigned long long free_calls;
    unsigned long long calloc_calls;
    unsigned long long realloc_calls;
    unsigned long long small_alloc_calls;
    unsigned long long small_free_calls;
    unsigned long long large_alloc_calls;
    unsigned long long large_free_calls;
    unsigned long long fallback_alloc_calls;
    unsigned long long fallback_free_calls;
    unsigned long long current_small_inuse;
    unsigned long long peak_small_inuse;
    unsigned long long current_large_inuse_bytes;
    unsigned long long peak_large_inuse_bytes;
    unsigned long long current_fallback_inuse_bytes;
    unsigned long long peak_fallback_inuse_bytes;
    unsigned long long total_small_page_bytes;
    unsigned long long total_large_map_bytes;
    unsigned long long active_large_map_bytes;
    unsigned long long peak_active_large_map_bytes;
    unsigned long long current_requested_bytes;
    unsigned long long current_allocated_bytes;
    unsigned long long internal_fragment_bytes;
    unsigned long long small_page_used_bytes;
    unsigned int internal_fragment_ppm;
    unsigned int page_utilization_ppm;
    size_t class_sizes[24];
    size_t class_total_chunks[24];
    size_t class_free_chunks[24];
    size_t class_page_count[24];
    size_t class_bytes_in_pages[24];
} kvs_mem_stats_t;

typedef struct {
    unsigned long long initialized;
    unsigned long long compiled;
    unsigned long long register_attempts;
    unsigned long long register_failures;
    int last_errno;
    char last_error[128];
    unsigned long long sk_msg_count;
    unsigned long long sk_msg_bytes;
    unsigned long long sk_msg_pass;
    unsigned long long sk_msg_drop;
    unsigned long long redirect_enabled;
    unsigned long long redirect_attempts;
    unsigned long long redirect_success;
    unsigned long long redirect_failures;
    unsigned long long forward_enabled;
    unsigned long long role_unknown;
    unsigned long long role_master;
    unsigned long long role_slave;
} kvs_repl_ebpf_stats_t;

extern kv_config_t g_cfg;
extern int g_epfd;
extern conn_t *g_replicas;
extern pthread_mutex_t g_repl_lock;
extern volatile int g_repl_fullsync_in_progress;
extern int g_aof_fd;

void *kvs_malloc(size_t size);
void *kvs_calloc(size_t n, size_t size);
void *kvs_realloc(void *ptr, size_t size);
void kvs_free(void *ptr);
long long kvs_now_ms(void);
long long kvs_now_us(void);
int kvs_mem_prepare_process(const char *backend_name, char *argv0, char **argv);
int kvs_mem_init(const char *backend_name);
int kvs_mem_purge(void);   /* 显式归还空闲内存：libc/custom→malloc_trim, jemalloc→mallctl purge */
const char *kvs_mem_backend_name(void);
int kvs_mem_get_stats(kvs_mem_stats_t *stats);

int kvs_expire_create(kvs_expire_table_t *tab);
void kvs_expire_destroy(kvs_expire_table_t *tab);
int kvs_expire_set(kvs_expire_table_t *tab, int engine, const char *key, long long ttl_ms);
int kvs_expire_del(kvs_expire_table_t *tab, int engine, const char *key);
int kvs_expire_persist(kvs_expire_table_t *tab, int engine, const char *key);
int kvs_expire_is_expired(kvs_expire_table_t *tab, int engine, const char *key);
long long kvs_expire_ttl(kvs_expire_table_t *tab, int engine, const char *key);
int kvs_active_expire_cycle(int budget);

int reactor_start(void);
int proactor_start(unsigned short port);
int ntyco_start(unsigned short port);

int queue_bytes(conn_t *c, const unsigned char *buf, size_t len);
void flush_conn_output(conn_t *c);
void close_conn(conn_t *c);
int parse_resp_stream(conn_t *c, unsigned char *buf, size_t *len, int from_replication);
int handle_parsed_command(conn_t *c, int argc, char **argv, size_t *argl, const unsigned char *raw, size_t rawlen, int from_replication);

const char *repl_transport_name(void);
const char *repl_transport_configured_name(void);
const char *repl_transport_active_name(void);
const char *repl_transport_fallback_reason(void);
unsigned long long repl_transport_fallback_count(void);
long long repl_transport_fallback_until_ms(void);
const char *repl_fullsync_transport_name(void);
const char *repl_realtime_transport_name(void);
int repl_transport_send(conn_t *c, const unsigned char *buf, size_t len);
int repl_transport_send_many(conn_t *c, const unsigned char *buf1, size_t len1, const unsigned char *buf2, size_t len2);
int repl_fullsync_send(conn_t *c, const unsigned char *buf, size_t len);
int repl_rdma_flush_batch(void);  /* P3.1b: flush RDMA send batch */
void repl_rdma_set_remote_write_mr(uint32_t rkey, uint64_t addr);
int repl_rdma_set_write_total_size(size_t sz);
void repl_rdma_send_final_imm(void);
void repl_rdma_set_remote_write_mr_info(uint64_t tid, uint64_t addr, uint32_t rkey, uint64_t capacity);
uint64_t repl_fullsync_next_transfer_id(void);
void repl_rdma_set_transfer_id(uint64_t tid);
void repl_rdma_raise_memlock(void);
int repl_fullsync_wait_remote_mr(conn_t *c, int timeout_ms);
int repl_fullsync_sendfile(int tcp_fd, int dump_fd, size_t total_bytes);
void repl_fullsync_abort_master(int tcp_fd, uint64_t tid);
void repl_fullsync_send_end(int tcp_fd, uint64_t tid);
int repl_rdma_wait_all_sends(int timeout_ms);
int repl_rdma_slave_has_write_buffer(void);
int repl_rdma_slave_check_write_complete(void);
int repl_rdma_slave_prepare_target(int tcp_fd, uint64_t transfer_id, size_t total_bytes);
void repl_rdma_slave_target_cleanup(int remove_incomplete);
void repl_rdma_slave_abort_target(uint64_t tid);
void repl_rdma_slave_finalize_signal(uint64_t tid);
void repl_rdma_master_transfer_cleanup(conn_t *c);
int repl_realtime_send(conn_t *c, const unsigned char *buf, size_t len);
int repl_send_chunked(conn_t *c, const unsigned char *buf, size_t len);
int repl_send_chunked_ctx(conn_t *c, const unsigned char *buf, size_t len, int send_ctx);

void repl_add_slave(conn_t *c);
void repl_remove_slave(conn_t *c);
int repl_handle_replica_send_failure(conn_t *c, conn_t **linkp);
void repl_broadcast(const unsigned char *raw, size_t rawlen);
/* ebpf-proxy 屏障（定义在 src/main/kvstore.c）：保证"绕过 proxy 直接补数据"的动作
 * （FULLRESYNC 快照 / partial resync replay）与 proxy 的实时转发不会跨两条 TCP 连接重排。
 *  - repl_barrier_pending(): 屏障是否仍立着
 *  - repl_barrier_tick():    周期兜底放行，由 reactor 的定时块调用
 *  - repl_barrier_note_applied(): Slave 的 REPLACK 反馈，追上 catchup_end 即放行 */
int repl_barrier_pending(void);
void repl_barrier_tick(void);
void repl_barrier_note_applied(unsigned long long applied_offset);
void repl_note_send_context(const char *stage, size_t len, unsigned long long offset, const unsigned char *buf);
void repl_get_last_send_context(char *stage, size_t stage_cap, unsigned long long *len, unsigned long long *offset, char *preview, size_t preview_cap);
int start_slave_thread(void);
int start_rdma_master_listener(void);
int repl_rdma_start_fullsync(conn_t *c);
void repl_rdma_stop_fullsync(void);
int repl_slaveof(const char *host, int port);
int repl_slaveof_noone(void);
const char *repl_master_link_state_name(void);
const char *repl_master_id(void);
extern volatile time_t g_last_write_ts;
unsigned long long repl_master_offset(void);
unsigned long long repl_connected_slaves(void);
unsigned long long repl_fullsync_count(void);
unsigned long long repl_partialsync_ok_count(void);
unsigned long long repl_partialsync_err_count(void);
unsigned long long repl_broadcast_bytes(void);
unsigned long long repl_snapshot_bytes(void);
unsigned long long repl_backlog_size(void);
unsigned long long repl_backlog_histlen(void);
unsigned long long repl_backlog_start_offset(void);
unsigned long long repl_backlog_end_offset(void);
int repl_rdma_effective_recv_slots(void);
int repl_rdma_effective_qp_wr_depth(void);
int repl_rdma_effective_chunk_size(void);
int repl_rdma_is_connected(void);
unsigned long long repl_rdma_disconnect_count(void);
unsigned long long repl_rdma_reject_count(void);
unsigned long long repl_rdma_send_cq_error_count(void);
unsigned long long repl_rdma_recv_cq_error_count(void);
void repl_note_fullsync(size_t snapshot_bytes);
void repl_note_broadcast(size_t bytes);
int repl_backlog_feed(const unsigned char *buf, size_t len);
void repl_backlog_reset(unsigned long long base);
/* partial resync 的有界 replay：只发 [offset, end)，end 取屏障时刻的 catchup_end */
int repl_backlog_write_range_upto(conn_t *c, unsigned long long offset, unsigned long long end);
int repl_backlog_send_continue_upto(conn_t *c, unsigned long long offset, unsigned long long end);
unsigned long long repl_session_begin(void);
void repl_session_end(void);
unsigned long long repl_session_id(void);
int repl_session_valid(void);
int repl_backlog_contiguous(void);
int repl_backlog_can_continue(const char *replid, unsigned long long offset);
int repl_backlog_write_range(conn_t *c, unsigned long long offset);
int repl_backlog_send_continue(conn_t *c, unsigned long long offset);
void repl_note_partialsync_result(int ok);
void repl_slave_set_sync_state(const char *replid, unsigned long long applied_offset, unsigned long long durable_offset, int fullsync_loading, unsigned long long fullsync_target_bytes);
void repl_slave_finish_fullsync(void);
void repl_slave_note_applied(size_t rawlen);
void repl_slave_note_durable(size_t rawlen);
int repl_slave_send_ack(void);
void repl_replica_update_ack(conn_t *c, unsigned long long applied_offset, unsigned long long durable_offset);
const char *repl_slave_master_id(void);
unsigned long long repl_slave_offset(void);
unsigned long long repl_slave_applied_offset(void);
unsigned long long repl_slave_durable_offset(void);
int repl_slave_loading_fullsync(void);
int repl_slave_state_load(void);
int repl_slave_state_save(void);

int repl_ebpf_init(void);
void repl_ebpf_cleanup(void);
int repl_ebpf_supported(void);
int repl_ebpf_register_fd(int fd, int is_master_side);
int repl_ebpf_register_forward_fd(int fd);
int repl_ebpf_unregister_fd(int fd);
int repl_ebpf_get_stats(kvs_repl_ebpf_stats_t *stats);
int repl_ebpf_backpressure(void);   /* ebpf+tcp 转发背压：返回 1 表示转发路径落后，应暂停读取 */

/* ebpf-proxy 侧 proxy_cache 的运行时统计（由 proxy 周期性发布到 client_stats map）。
 * 字段含义见 src/ebpf_proxy/main.c 的 publish_cache_stats()。 */
typedef struct {
    unsigned long long cache_bytes;      /* 当前缓存占用 */
    unsigned long long cache_nodes;      /* 当前节点数 */
    unsigned long long cache_dropped;    /* 丢弃节点数（跨 session 作废 / 溢出作废） */
    unsigned long long cache_drop_bytes; /* 丢弃字节数 */
    unsigned long long cache_max_bytes;  /* 峰值占用 */
    unsigned long long cache_invalid;    /* 本 session cache 是否已作废 */
    unsigned long long capture_enabled;  /* client_ctl[7]：eBPF 捕获是否开启 */
    unsigned long long capture_off_count;/* BPF 因 capture_enabled=0 直接返回的次数 */
} kvs_ebpf_proxy_stats_t;
int repl_ebpf_proxy_get_stats(kvs_ebpf_proxy_stats_t *stats);

int persist_init(void);
void persist_close(void);

/* async append return codes */
#define KVS_PERSIST_OK      0
#define KVS_PERSIST_PENDING 1
#define KVS_PERSIST_ERR     -1

void persist_reap_completions(void);
void persist_drain_pending(void);
int persist_uring_fd(void);

int persist_append_raw(const unsigned char *buf, size_t len);
int persist_append_prepare(conn_t *c, const unsigned char *buf, size_t len,
                           unsigned char *resp, size_t resp_len);
void persist_submit_sqes(void);
void persist_flush_pending(void);
int persist_write_raw_fd(int fd, const unsigned char *buf, size_t len, long long *offset_io);
int persist_fsync_fd(int fd);
int persist_save_dump(void);
int persist_recover(void);
int persist_recover_in_progress(void);
int kvs_snapshot_to_fp(FILE *fp);
int kvs_snapshot_to_fd(int fd);
int kvs_dump_to_fd(int fd, unsigned long long aof_offset);
unsigned long long replay_dump_file(const char *path);
int kvs_load_dump_from_fd(int fd);
int persist_bgsave_start(void);
int persist_bgsave_poll(void);
int persist_bgsave_in_progress(void);
const char *persist_bgsave_state_name(void);
int persist_bgrewriteaof_start(void);
int persist_bgrewriteaof_poll(void);
int persist_bgrewriteaof_in_progress(void);
const char *persist_bgrewriteaof_state_name(void);
int persist_aof_disable(void);
int persist_set_aof_policy(kvs_aof_fsync_policy_t policy);
kvs_aof_fsync_policy_t persist_get_aof_policy(void);
const char *persist_aof_policy_name(void);
int persist_force_aof_flush(void);
long long persist_aof_max_batch_age_us(void);   /* 观测：handoff 时最大批龄（崩溃窗口上界，实验用） */
int persist_aof_pending_flush_ms(void);          /* 距下次 AOF flush 的剩余时间（ms）；无待 flush 槽或无需窗口约束时返回 -1 */
                                           /* 供 reactor 缩短 epoll_wait 超时：稀疏流量下崩溃窗口压在攒批窗口量级 */

void persist_note_write(void);
unsigned long long persist_dirty_count(void);
long long persist_last_snapshot_ms(void);
int persist_register_autosnap_rule(long long seconds, long long changes);
void persist_clear_autosnap_rules(void);
int persist_build_autosnap_text(char *buf, size_t cap);
int persist_autosnap_cron(void);
int persist_build_recover_text(char *buf, size_t cap);
int sentinel_start(void);

extern pid_t g_bgsave_pid;
extern long long g_bgsave_last_start_ms;
extern long long g_bgsave_last_end_ms;
extern unsigned long long g_dirty_counter;

int resp_simple_string(char *out, size_t cap, const char *s);
int resp_error(char *out, size_t cap, const char *s);
int resp_integer(char *out, size_t cap, long long v);
int resp_bulk(char *out, size_t cap, const char *s, size_t len);
int resp_null_bulk(char *out, size_t cap);
size_t resp_build_cmd4(unsigned char *out, size_t cap, const char *cmd, const char *a1, const char *a2, const char *a3);
size_t resp_build_cmd3(unsigned char *out, size_t cap, const char *cmd, const char *a1, const char *a2);
size_t resp_build_cmd2(unsigned char *out, size_t cap, const char *cmd, const char *a1);
size_t resp_build_cmd1(unsigned char *out, size_t cap, const char *cmd);

/* ---- kprobe+RDMA 增量同步 ---- */
struct ibv_pd; /* 前向声明，避免非 RDMA 编译单元警告 */
typedef struct {
    unsigned long long total_events;
    unsigned long long total_bytes;
    unsigned long long rdma_writes;
    unsigned long long rdma_errors;
    unsigned long long kprobe_hits;
    unsigned long long kprobe_bytes;
    int kprobe_initialized;
    int rdma_connected;
} kvs_repl_kprobe_stats_t;

int repl_kprobe_rdma_master_init(void);
int repl_kprobe_rdma_slave_init(void);
int repl_kprobe_rdma_establish(const char *host, int port);
int repl_kprobe_rdma_connect_mr(const char *host, int port, int tcp_fd);
int repl_kprobe_rdma_slave_accept(struct ibv_pd *pd, char *resp, size_t resp_cap);
void repl_kprobe_rdma_cleanup(void);
int repl_kprobe_rdma_get_stats(kvs_repl_kprobe_stats_t *stats);
int repl_kprobe_rdma_set_pid(pid_t pid);
int repl_kprobe_rdma_parse_mr_info(const char *resp);
void repl_kprobe_fullsync_done(void);
int repl_kprobe_rdma_get_mr_text(char *buf, size_t cap);
int repl_kprobe_rdma_parse_mr_info_direct(uint32_t rkey, uint64_t addr,
    size_t total_size, size_t slot_count, size_t slot_capacity);
int repl_kprobe_rdma_enqueue(const unsigned char *data, size_t len);

const char *repl_master_link_state_name(void);

#endif