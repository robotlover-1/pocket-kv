#ifndef KVS_COMMIT_GATE_H
#define KVS_COMMIT_GATE_H

/* Commit Gate 控制面协议（Master <-> ebpf-proxy，Unix SOCK_SEQPACKET）。
 * 该头文件只依赖 stdint.h，供 kvstore 与 ebpf_proxy 共同包含。 */

#include <stdint.h>

#define REPL_COMMIT_MAGIC    0x4b564d43u   /* 'KVMC' */
#define REPL_COMMIT_VERSION  1u
#define REPL_COMMIT_SOCK_SUFFIX ".commit.sock"

#define REPL_COMMIT_BATCH_MAX 64

/* 消息类型 */
enum {
    REPL_COMMIT_MSG_HELLO    = 1,   /* epoch */
    REPL_COMMIT_MSG_BASELINE = 2,   /* {cookie, value=next_cmd_seq} */
    REPL_COMMIT_MSG_IGNORE   = 3,   /* {cookie, value=0} */
    REPL_COMMIT_MSG_FORGET   = 4,   /* {cookie, value=0} */
    REPL_COMMIT_MSG_RESUME   = 5,   /* global_seq=marker：丢弃 <=marker 的 recovery 提交 */
    REPL_COMMIT_MSG_BATCH    = 6,   /* desc[] */
};

/* 描述符 flags */
#define REPL_COMMIT_FLAG_WRITE 1u
#define REPL_COMMIT_FLAG_SKIP  2u

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t type;
    uint32_t count;       /* BATCH: 描述符条数 */
    uint64_t epoch;       /* HELLO: master 实例 epoch */
    uint64_t global_seq;  /* RESUME: marker */
} repl_commit_msg_t;

typedef struct {
    uint64_t cookie;
    uint64_t session_id;    /* 产生该命令的 replication session（跨 session 丢弃） */
    uint64_t command_seq;   /* 每连接从 1 开始 */
    uint64_t global_seq;    /* WRITE 全局序；SKIP=0 */
    uint64_t crc;           /* FNV-1a 64(命令原始字节) */
    uint32_t raw_len;
    uint32_t flags;         /* REPL_COMMIT_FLAG_* */
} repl_commit_desc_t;

typedef struct {
    uint64_t cookie;
    uint64_t value;         /* BASELINE: next_cmd_seq；其它: 0 */
} repl_commit_kv_t;

/* 一条 SEQPACKET 消息 = repl_commit_msg_t + 可选 payload */
#define REPL_COMMIT_MAX_MSG \
    (sizeof(repl_commit_msg_t) + REPL_COMMIT_BATCH_MAX * sizeof(repl_commit_desc_t))

#endif /* KVS_COMMIT_GATE_H */
