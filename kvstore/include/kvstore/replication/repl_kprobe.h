#ifndef REPL_KPROBE_H
#define REPL_KPROBE_H

#include "kvstore/kvstore.h"

/* ---- 常量 ---- */
#define KPROBE_RDMA_SLOT_COUNT      1024
#define KPROBE_RDMA_SLOT_CAPACITY   512   /* 每槽 512B，容纳增量 RESP 命令 */
#define KPROBE_RDMA_RINGBUF_SIZE    (16 + KPROBE_RDMA_SLOT_COUNT * KPROBE_RDMA_SLOT_CAPACITY)

/* ---- Slave MR 环形缓冲区布局 ---- */
typedef struct __attribute__((packed)) kprobe_rdma_ringbuf_s {
    volatile uint64_t producer_head;   /* 偏移 0  — RDMA WRITE 更新 */
    volatile uint64_t consumer_tail;   /* 偏移 8  — Slave 本地更新 */
    unsigned char slots[KPROBE_RDMA_SLOT_COUNT * KPROBE_RDMA_SLOT_CAPACITY];
} kprobe_rdma_ringbuf_t;

/* 每个 slot 格式:
 *   [4 bytes: payload_len (uint32_t)]
 *   [payload_len bytes: RESP 协议数据] */

/* ---- 函数声明 ---- */

/* Master 侧初始化: 加载 BPF + 分配 WRITE slots */
int repl_kprobe_rdma_master_init(void);

/* Slave 侧初始化 */
int repl_kprobe_rdma_slave_init(void);

/* Master 侧建立 RDMA QP + 交换 MR 信息 */
int repl_kprobe_rdma_establish(const char *host, int port);

/* Master 侧: 连接 kprobe-rdma QP 到 Slave + 发送 KPROBEMR + 启动转发线程 */
int repl_kprobe_rdma_connect_mr(const char *host, int port, int tcp_fd);

/* Slave 侧注册 MR + 返回握手响应 */
int repl_kprobe_rdma_slave_accept(struct ibv_pd *pd,
    char *resp, size_t resp_cap);

/* 获取 MR 信息文本（用于 KPROBEMR 响应） */
int repl_kprobe_rdma_get_mr_text(char *buf, size_t cap);

/* 直接设置 Slave MR 信息（从 reactor 线程调用） */
int repl_kprobe_rdma_parse_mr_info_direct(uint32_t rkey, uint64_t addr,
    size_t total_size, size_t slot_count, size_t slot_capacity);

/* 清理 */
void repl_kprobe_rdma_cleanup(void);

/* 获取统计 */
int repl_kprobe_rdma_get_stats(kvs_repl_kprobe_stats_t *stats);

/* 设置 kprobe PID 过滤 */
int repl_kprobe_rdma_set_pid(pid_t pid);

/* 设置 kprobe fd 过滤 */
int repl_kprobe_rdma_set_fd(int fd);

void repl_kprobe_fwd_health_check(void);

#endif /* REPL_KPROBE_H */
