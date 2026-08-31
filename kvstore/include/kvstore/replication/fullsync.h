#ifndef KVSTORE_REPLICATION_FULLSYNC_H
#define KVSTORE_REPLICATION_FULLSYNC_H

#include <stdint.h>
#include <stddef.h>
#include <limits.h>

/*
 * 全量同步传输无关协议模块。
 *
 * 只依赖标准整数/size/errno 定义，不依赖任何 verbs/rdma 头文件，
 * 因此可以在无 RDMA 硬件的环境下做单元测试。
 *
 * 每个全量同步尝试携带一个 transfer_id，将 TCP 控制消息、RDMA 连接、
 * 远端 MR、期望字节数、临时文件、最终完成绑定在一起。
 * 陈旧或不同 transfer 的响应绝不能激活 WRITE 模式。
 */

#ifdef __cplusplus
extern "C" {
#endif

/* ---- 全量同步传输模式 ---- */
typedef enum kvs_fullsync_mode {
    KVS_FULLSYNC_RDMA_WRITE = 0,   /* 首选：单边 RDMA WRITE（零拷贝文件 MR） */
    KVS_FULLSYNC_RDMA_SEND  = 1,   /* 保留：双边 SEND，仅作为后续 benchmark 基线，本次不作为生产回退 */
    KVS_FULLSYNC_TCP        = 2    /* 回退：零拷贝 TCP sendfile */
} kvs_fullsync_mode_t;

/* ---- 全量同步状态 ---- */
typedef enum kvs_fullsync_state {
    KVS_FULLSYNC_ST_IDLE        = 0,
    KVS_FULLSYNC_ST_PREPARING   = 1,
    KVS_FULLSYNC_ST_WAIT_MR     = 2,  /* Master 等待 FULLRESYNCWR */
    KVS_FULLSYNC_ST_WRITING     = 3,  /* Master 正在 RDMA WRITE */
    KVS_FULLSYNC_ST_RDMA_SEND   = 4,  /* 生产代码本次不进入（保留给 benchmark） */
    KVS_FULLSYNC_ST_TCP         = 5,  /* sendfile 回退 */
    KVS_FULLSYNC_ST_WAIT_REPLDONE = 6,
    KVS_FULLSYNC_ST_COMPLETE    = 7,
    KVS_FULLSYNC_ST_FAILED      = 8
} kvs_fullsync_state_t;

/* ---- Slave 发布的远端 MR 信息 ---- */
typedef struct kvs_fullsync_mr_msg {
    uint64_t transfer_id;
    uint64_t addr;      /* 远端映射基地址 */
    uint32_t rkey;
    uint64_t capacity;  /* 远端映射容量（字节） */
} kvs_fullsync_mr_msg_t;

const char *kvs_fullsync_mode_name(kvs_fullsync_mode_t mode);

/*
 * 解析 `+FULLRESYNCWR <transfer_id> <addr> <rkey> <capacity>\r\n`。
 * 接受可选的 `+` 前缀，字段数必须精确，CRLF 结束，拒绝负值/多余字段/溢出。
 * 返回 0 或负 errno 值（-EINVAL 格式错误）。
 */
int kvs_fullsync_parse_remote_mr(const char *line, size_t len,
                                 kvs_fullsync_mr_msg_t *out);

/*
 * 校验远端 MR：
 *  - transfer_id 不匹配            -> -ESTALE
 *  - addr == 0 或 rkey == 0        -> -EINVAL
 *  - capacity < expected_bytes     -> -EOVERFLOW
 */
int kvs_fullsync_validate_remote_mr(const kvs_fullsync_mr_msg_t *msg,
                                    uint64_t expected_id,
                                    uint64_t expected_bytes);

/*
 * 计算远端 WRITE 目标地址：out = base + offset。
 * 校验 offset/len 不越出 capacity，且 base+offset 不溢出。返回 0 或 -EOVERFLOW。
 */
int kvs_fullsync_remote_addr(uint64_t base, uint64_t capacity,
                             uint64_t offset, size_t len, uint64_t *out);

/*
 * 解析 `REPLDONE <transfer_id> <bytes>\r\n`（可选 `+` 前缀）。
 * 返回 0 或负 errno 值（-EINVAL 格式错误）。
 */
int kvs_fullsync_parse_repldone(const char *line, size_t len,
                                uint64_t *transfer_id, uint64_t *bytes);

/* ---- Slave 全量同步目标（一次 transfer 的接收端资源） ----
 * 协议层不可见 verbs 类型：mr 以 void* 持有，由 kvs_repl.c 转型为
 * struct ibv_mr* 使用。这样资源清理逻辑可以在无 RDMA 环境下单测。
 */
typedef struct repl_fullsync_target {
    uint64_t transfer_id;
    uint64_t expected_bytes;
    int fd;                       /* 目标临时文件 fd；heap 模式为 -1 */
    char path[PATH_MAX];          /* 目标临时文件路径；heap 模式为空 */
    void *mapping;                /* mmap/kvs_malloc 的数据区 */
    size_t mapping_len;
    void *mr;                     /* struct ibv_mr* */
    int imm_received;             /* 是否已收到最终 WRITE_WITH_IMM */
    int complete;                 /* 目标数据是否已完整接收 */
} repl_fullsync_target_t;

/*
 * 幂等重置：fd=-1、mapping=NULL、mr=NULL、mapping_len=0、
 * expected_bytes=0、transfer_id=0、path 清空、标志清零。
 * 可安全重复调用。所有失败/成功/断开路径最终都要回到这里。
 */
void repl_fullsync_target_reset(repl_fullsync_target_t *t);

#ifdef __cplusplus
}
#endif

#endif /* KVSTORE_REPLICATION_FULLSYNC_H */
