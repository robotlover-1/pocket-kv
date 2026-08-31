/*
 * 全量同步传输无关协议实现（解析/校验/地址计算）。
 *
 * 只依赖标准 C 库，不依赖 verbs/rdma。所有解析器对输入做严格校验：
 *   - 必须以 \r\n 结束；
 *   - 字段数精确（不多不少）；
 *   - 数字字段为纯十进制、非负、不溢出 uint64；
 *   - 命令名精确匹配（可选 '+' 前缀）。
 * 返回 0 或负 errno 风格值：-EINVAL（格式错误）、-ESTALE（transfer 不匹配）、
 * -EOVERFLOW（容量/地址越界或整数溢出）。
 */
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#include "kvstore/replication/fullsync.h"

const char *kvs_fullsync_mode_name(kvs_fullsync_mode_t mode) {
    switch (mode) {
    case KVS_FULLSYNC_RDMA_WRITE: return "rdma-write";
    case KVS_FULLSYNC_RDMA_SEND:  return "rdma-send";
    case KVS_FULLSYNC_TCP:        return "sendfile";
    default:                      return "unknown";
    }
}

/* 解析纯十进制 uint64 token：拒绝空、前导 '-', 非数字字符，检测溢出。 */
static int parse_u64_token(const char *tok, size_t tok_len, uint64_t *out) {
    uint64_t v = 0;
    if (tok_len == 0 || tok[0] == '-') return -EINVAL;
    for (size_t i = 0; i < tok_len; i++) {
        unsigned char c = (unsigned char)tok[i];
        if (c < '0' || c > '9') return -EINVAL;
        uint64_t digit = (uint64_t)(c - '0');
        if (v > (UINT64_MAX - digit) / 10) return -EINVAL;  /* 溢出 */
        v = v * 10 + digit;
    }
    *out = v;
    return 0;
}

/* 将 line（以 \r\n 结束）拆成空白分隔的 token。返回 token 数或负 errno。 */
static int split_tokens(const char *line, size_t len,
                        const char **toks, size_t *lens, int max_toks) {
    if (!line || len < 2 || line[len - 2] != '\r' || line[len - 1] != '\n')
        return -EINVAL;
    const char *p = line;
    const char *end = line + len - 2;
    int count = 0;
    while (p < end) {
        while (p < end && (*p == ' ' || *p == '\t')) p++;
        if (p >= end) break;
        if (count >= max_toks) return -EINVAL;  /* 字段过多 */
        const char *start = p;
        while (p < end && *p != ' ' && *p != '\t') p++;
        toks[count] = start;
        lens[count] = (size_t)(p - start);
        count++;
    }
    return count;
}

/* 命令名是否精确匹配（可选 '+' 前缀）。 */
static int cmd_matches(const char *tok, size_t tok_len, const char *name) {
    if (tok_len > 0 && tok[0] == '+') { tok++; tok_len--; }
    size_t nlen = strlen(name);
    return (tok_len == nlen && memcmp(tok, name, nlen) == 0);
}

int kvs_fullsync_parse_remote_mr(const char *line, size_t len,
                                 kvs_fullsync_mr_msg_t *out) {
    const char *toks[8];
    size_t lens[8];
    uint64_t rkey;
    if (!line || !out) return -EINVAL;
    int n = split_tokens(line, len, toks, lens, 8);
    if (n < 0) return n;
    if (n != 5) return -EINVAL;
    if (!cmd_matches(toks[0], lens[0], "FULLRESYNCWR")) return -EINVAL;
    if (parse_u64_token(toks[1], lens[1], &out->transfer_id) != 0) return -EINVAL;
    if (parse_u64_token(toks[2], lens[2], &out->addr) != 0) return -EINVAL;
    if (parse_u64_token(toks[3], lens[3], &rkey) != 0) return -EINVAL;
    if (rkey > UINT32_MAX) return -EINVAL;
    out->rkey = (uint32_t)rkey;
    if (parse_u64_token(toks[4], lens[4], &out->capacity) != 0) return -EINVAL;
    return 0;
}

int kvs_fullsync_validate_remote_mr(const kvs_fullsync_mr_msg_t *msg,
                                    uint64_t expected_id,
                                    uint64_t expected_bytes) {
    if (!msg) return -EINVAL;
    if (msg->transfer_id != expected_id) return -ESTALE;
    if (msg->addr == 0 || msg->rkey == 0) return -EINVAL;
    if (msg->capacity < expected_bytes) return -EOVERFLOW;
    return 0;
}

int kvs_fullsync_remote_addr(uint64_t base, uint64_t capacity,
                             uint64_t offset, size_t len, uint64_t *out) {
    if (!out) return -EINVAL;
    if (offset > capacity) return -EOVERFLOW;
    if ((uint64_t)len > capacity - offset) return -EOVERFLOW;
    if (offset > UINT64_MAX - base) return -EOVERFLOW;
    *out = base + offset;
    return 0;
}

int kvs_fullsync_parse_repldone(const char *line, size_t len,
                                uint64_t *transfer_id, uint64_t *bytes) {
    const char *toks[4];
    size_t lens[4];
    if (!line || !transfer_id || !bytes) return -EINVAL;
    int n = split_tokens(line, len, toks, lens, 4);
    if (n < 0) return n;
    if (n != 3) return -EINVAL;
    if (!cmd_matches(toks[0], lens[0], "REPLDONE")) return -EINVAL;
    if (parse_u64_token(toks[1], lens[1], transfer_id) != 0) return -EINVAL;
    if (parse_u64_token(toks[2], lens[2], bytes) != 0) return -EINVAL;
    return 0;
}

void repl_fullsync_target_reset(repl_fullsync_target_t *t) {
    if (!t) return;
    t->transfer_id = 0;
    t->expected_bytes = 0;
    t->fd = -1;
    t->path[0] = '\0';
    t->mapping = NULL;
    t->mapping_len = 0;
    t->mr = NULL;
    t->imm_received = 0;
    t->complete = 0;
}
