// ============================================================
// proxy_commit.c — Commit Gate（阶段3）ebpf-proxy 侧
//
//  - 接收 Master 的 Unix SOCK_SEQPACKET 控制面（HELLO/BASELINE/IGNORE/
//    FORGET/RESUME/DESC_BATCH）；
//  - 按 socket_cookie 重组 RESP（与 kvstore parse_resp_stream 的 client 路径
//    语义一致），每连接命令序号从 BASELINE 开始；
//  - 每条命令必须收到 WRITE/SKIP 描述符，长度/CRC/seq 全部匹配；
//  - WRITE 按 global_seq 顺序交给 main（转发队列/cache/丢弃）；
//  - 任何缺口/错位/超时 → fail-closed 回调。
// ============================================================
#define _GNU_SOURCE
#include "proxy_commit.h"
#include "kvstore/replication/commit_gate.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define PC_MAX_COOKIES       4096
#define PC_MAX_CTX           4096      /* per-cookie 状态上限，防止 cookie 抖动导致无界增长 */
#define PC_CTX_BUF_CAP       (2u * 1024 * 1024)
/* pending（已捕获待匹配）+ recovery（BUFFERING 期间已提交）共享同一份内存预算，
 * 避免两个 256MB 队列叠加导致总 RSS 失控。 */
#define PC_MEMORY_MAX        (256ull * 1024 * 1024)
#define PC_TIMEOUT_MS        2000

typedef struct pc_cmd_s {
    struct pc_cmd_s *next;
    uint64_t seq;
    uint64_t global_seq;
    uint64_t crc;
    uint32_t len;
    int state;                 /* 0=pending 1=write */
    long long created_ms;
    unsigned char data[];
} pc_cmd_t;

typedef struct pc_desc_s {
    struct pc_desc_s *next;
    uint64_t seq;
    uint64_t global_seq;
    uint64_t crc;
    uint32_t len;
    uint32_t flags;
    long long created_ms;
} pc_desc_t;

typedef struct pc_ctx_s {
    struct pc_ctx_s *next;
    uint64_t cookie;
    unsigned char *buf;
    size_t buf_len, buf_cap;
    int baseline_set;
    int ignored;
    uint64_t next_seq;
    uint64_t last_recv_seq;
    int recv_seq_valid;
    pc_cmd_t *cmd_head, *cmd_tail;
    pc_desc_t *desc_head, *desc_tail;
    long long last_activity_ms;
} pc_ctx_t;

typedef struct pc_rec_s {
    struct pc_rec_s *next;
    uint64_t global_seq;
    size_t len;
    unsigned char data[];
} pc_rec_t;

static int g_listen_fd = -1;
static int g_master_fd = -1;
static char g_sock_path[512];
static uint64_t g_epoch = 0;
/* 当前 replication session 的 commit epoch（来自最新 BASELINE）；0 = 未建立。
 * 旧/跨 session 的 BATCH/RESUME/IGNORE 必须丢弃，避免污染新会话。 */
static uint64_t g_session_epoch = 0;

static proxy_commit_emit_fn g_emit = NULL;
static proxy_commit_fault_fn g_fault = NULL;
static int g_faulted = 0;

static pc_ctx_t *g_buckets[PC_MAX_COOKIES];

static uint64_t g_next_global = 0;       /* 0 = 未初始化，接受第一个 global_seq */
static pc_cmd_t *g_ready_head = NULL;
static int g_buffering = 0;
static int g_wait_resume = 0;
static long long g_wait_resume_ms = 0;
static int g_resume_event = 0;

static pc_rec_t *g_rec_head = NULL, *g_rec_tail = NULL;
static size_t g_rec_bytes = 0;
static unsigned long long g_pending_bytes = 0;

static unsigned long long g_st_desc = 0, g_st_writes = 0, g_st_skips = 0;
static unsigned long long g_st_timeout = 0, g_st_desync = 0, g_st_gap = 0;
static unsigned long long g_st_channel = 0, g_st_emitted = 0;
static unsigned long long g_st_mem_hard = 0;
static int g_ctx_count = 0;
static long long g_pending_cmd_count = 0;

static long long pc_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static void pc_fault(int reason) {
    if (g_faulted) return;
    g_faulted = 1;
    if (reason == 1) g_st_timeout++;
    else if (reason == 2) g_st_desync++;
    else if (reason == 3) g_st_gap++;
    else if (reason == 4) g_st_channel++;
    if (reason == 1) {
        long long now = pc_now_ms();
        fprintf(stderr, "[commit] timeout detail: buffering=%d wait_resume=%d(age=%lldms) "
                        "rec_bytes=%zu pending_bytes=%llu pending_cmds=%lld ctxs=%d "
                        "ready_global=%llu next_global=%llu\n",
                g_buffering, g_wait_resume,
                g_wait_resume ? now - g_wait_resume_ms : 0,
                g_rec_bytes, g_pending_bytes, g_pending_cmd_count, g_ctx_count,
                g_ready_head ? (unsigned long long)g_ready_head->global_seq : 0,
                (unsigned long long)g_next_global);
        for (int i = 0; i < PC_MAX_COOKIES; ++i) {
            for (pc_ctx_t *c = g_buckets[i]; c; c = c->next) {
                if (!c->cmd_head && !c->desc_head) continue;
                fprintf(stderr, "[commit] ctx cookie=%llu baseline=%d next_seq=%llu "
                                "cmd=%llu(age=%lld) desc=%llu(age=%lld)\n",
                        (unsigned long long)c->cookie, c->baseline_set,
                        (unsigned long long)c->next_seq,
                        c->cmd_head ? (unsigned long long)c->cmd_head->seq : 0,
                        c->cmd_head ? now - c->cmd_head->created_ms : 0,
                        c->desc_head ? (unsigned long long)c->desc_head->seq : 0,
                        c->desc_head ? now - c->desc_head->created_ms : 0);
            }
        }
    }
    fprintf(stderr, "ebpf-proxy: commit gate fault reason=%d\n", reason);
    if (g_fault) g_fault(reason);
}

/* ---------------- ctx 表 ---------------- */
static unsigned int pc_hash(uint64_t cookie) {
    return (unsigned int)((cookie * 11400714819323198485ull) >> 52) & (PC_MAX_COOKIES - 1);
}

static pc_ctx_t *pc_get_ctx(uint64_t cookie, int create) {
    unsigned int b = pc_hash(cookie);
    for (pc_ctx_t *c = g_buckets[b]; c; c = c->next)
        if (c->cookie == cookie) return c;
    if (!create) return NULL;
    if (g_ctx_count >= PC_MAX_CTX) {
        g_st_mem_hard++;
        return NULL;
    }
    pc_ctx_t *c = (pc_ctx_t *)calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->cookie = cookie;
    c->next = g_buckets[b];
    g_buckets[b] = c;
    g_ctx_count++;
    return c;
}

static void pc_cmd_free(pc_cmd_t *cmd) {
    if (!cmd) return;
    if (g_pending_bytes >= cmd->len) g_pending_bytes -= cmd->len;
    if (g_pending_cmd_count > 0) g_pending_cmd_count--;
    free(cmd);
}

static void pc_ctx_reset(pc_ctx_t *c) {
    if (!c) return;
    pc_cmd_t *cm = c->cmd_head;
    while (cm) { pc_cmd_t *nx = cm->next; pc_cmd_free(cm); cm = nx; }
    pc_desc_t *d = c->desc_head;
    while (d) { pc_desc_t *nx = d->next; free(d); d = nx; }
    c->cmd_head = c->cmd_tail = NULL;
    c->desc_head = c->desc_tail = NULL;
    c->buf_len = 0;
    c->baseline_set = 0;
    c->ignored = 0;
    c->next_seq = 0;
    c->recv_seq_valid = 0;
    c->last_recv_seq = 0;
}

static void pc_remove_ctx(pc_ctx_t *c) {
    if (!c) return;
    unsigned int b = pc_hash(c->cookie);
    pc_ctx_t **pp = &g_buckets[b];
    while (*pp) {
        if (*pp == c) { *pp = c->next; break; }
        pp = &(*pp)->next;
    }
    pc_ctx_reset(c);
    free(c->buf);
    free(c);
    if (g_ctx_count > 0) g_ctx_count--;
}

static void pc_ready_free_all(void) {
    pc_cmd_t *cm = g_ready_head;
    while (cm) { pc_cmd_t *nx = cm->next; pc_cmd_free(cm); cm = nx; }
    g_ready_head = NULL;
}

static void pc_recovery_free_all(void) {
    pc_rec_t *r = g_rec_head;
    while (r) { pc_rec_t *nx = r->next; free(r); r = nx; }
    g_rec_head = g_rec_tail = NULL;
    g_rec_bytes = 0;
}

void proxy_commit_purge(void) {
    for (int i = 0; i < PC_MAX_COOKIES; ++i) {
        pc_ctx_t *c = g_buckets[i];
        while (c) { pc_ctx_t *nx = c->next; pc_ctx_reset(c); free(c->buf); free(c); c = nx; }
        g_buckets[i] = NULL;
    }
    pc_ready_free_all();
    pc_recovery_free_all();
    g_next_global = 0;
    g_wait_resume = 0;
    g_wait_resume_ms = 0;
    g_resume_event = 0;
    g_pending_bytes = 0;
    g_ctx_count = 0;
    g_pending_cmd_count = 0;
    g_faulted = 0;
}

/* ---------------- framer（复刻 Master client 解析路径） ---------------- */
static void pc_frame_one(pc_ctx_t *ctx, const unsigned char *buf, size_t len);
static void pc_try_match(pc_ctx_t *ctx);

static void pc_frame_one(pc_ctx_t *ctx, const unsigned char *buf, size_t len) {
    pc_cmd_t *cmd = (pc_cmd_t *)malloc(sizeof(*cmd) + len);
    if (!cmd) { pc_fault(1); return; }
    cmd->next = NULL;
    cmd->seq = ctx->next_seq++;
    cmd->global_seq = 0;
    cmd->crc = 0;
    cmd->len = (uint32_t)len;
    cmd->state = 0;
    cmd->created_ms = pc_now_ms();
    memcpy(cmd->data, buf, len);
    if (ctx->cmd_tail) ctx->cmd_tail->next = cmd; else ctx->cmd_head = cmd;
    ctx->cmd_tail = cmd;
    g_pending_bytes += len;
    g_pending_cmd_count++;
    if (g_pending_bytes + g_rec_bytes > PC_MEMORY_MAX) {
        g_st_mem_hard++;
        pc_fault(1);
        return;
    }
    pc_try_match(ctx);
}

static void pc_frame_ctx(pc_ctx_t *ctx) {
    if (!ctx || !ctx->baseline_set) return;
    size_t pos = 0, len = ctx->buf_len;
    const unsigned char *buf = ctx->buf;

    while (pos < len) {
        if (buf[pos] == '*') {
            size_t p = pos + 1;
            while (p + 1 < len && !(buf[p] == '\r' && buf[p + 1] == '\n')) p++;
            if (p + 1 >= len) break;                 /* incomplete header */
            if (p - (pos + 1) >= 32) { pos = p + 2; continue; }
            int argc = 0, ok = 1;
            for (size_t i = pos + 1; i < p; ++i) {
                if (buf[i] < '0' || buf[i] > '9') { ok = 0; break; }
                argc = argc * 10 + (buf[i] - '0');
                if (argc > 1000000) { ok = 0; break; }
            }
            if (!ok || argc <= 0 || argc > 32) { pos = p + 2; continue; }
            size_t q = p + 2;
            int incomplete = 0, malformed = 0;
            for (int i = 0; i < argc; ++i) {
                if (q >= len) { incomplete = 1; break; }
                if (buf[q] != '$') { malformed = 1; break; }
                size_t lp = q + 1;
                while (lp + 1 < len && !(buf[lp] == '\r' && buf[lp + 1] == '\n')) lp++;
                if (lp + 1 >= len) { incomplete = 1; break; }
                if (lp - (q + 1) >= 32) { malformed = 1; break; }
                long blen = 0;
                int bok = 1;
                for (size_t x = q + 1; x < lp; ++x) {
                    if (buf[x] < '0' || buf[x] > '9') { bok = 0; break; }
                    blen = blen * 10 + (buf[x] - '0');
                    if (blen > (long)PC_CTX_BUF_CAP) { bok = 0; break; }
                }
                if (!bok) { malformed = 1; break; }
                q = lp + 2;
                if (q + (size_t)blen + 2 > len) { incomplete = 1; break; }
                q += (size_t)blen;
                if (!(buf[q] == '\r' && buf[q + 1] == '\n')) { malformed = 1; break; }
                q += 2;
            }
            if (incomplete) break;
            if (malformed) {
                if (q > pos) { pos = q; continue; }
                break;
            }
            pc_frame_one(ctx, buf + pos, q - pos);
            pos = q;
        } else if (buf[pos] == '+') {
            size_t p = pos + 1;
            while (p + 1 < len && !(buf[p] == '\r' && buf[p + 1] == '\n')) p++;
            if (p + 1 >= len) break;
            pos = p + 2;                              /* control line: no command */
        } else {
            size_t e = pos;
            while (e < len && buf[e] != '\n') e++;
            if (e >= len) break;
            size_t line_len = e - pos;
            if (line_len > 0 && buf[pos + line_len - 1] == '\r') line_len--;
            size_t x = pos, line_end = pos + line_len;
            int argc = 0;
            while (x < line_end) {
                while (x < line_end && isspace(buf[x])) x++;
                if (x >= line_end) break;
                argc++;
                while (x < line_end && !isspace(buf[x])) x++;
            }
            if (argc > 0)
                pc_frame_one(ctx, buf + pos, e + 1 - pos);
            pos = e + 1;
        }
    }

    if (pos > 0) {
        memmove(ctx->buf, ctx->buf + pos, len - pos);
        ctx->buf_len = len - pos;
    }
}

/* ---------------- 全局序 ---------------- */
static void pc_ready_insert(pc_cmd_t *cmd) {
    pc_cmd_t **pp = &g_ready_head;
    while (*pp && (*pp)->global_seq < cmd->global_seq) pp = &(*pp)->next;
    cmd->next = *pp;
    *pp = cmd;
}

static void pc_recovery_append(pc_cmd_t *cmd) {
    pc_rec_t *r = (pc_rec_t *)malloc(sizeof(*r) + cmd->len);
    if (!r) { pc_fault(1); return; }
    r->next = NULL;
    r->global_seq = cmd->global_seq;
    r->len = cmd->len;
    memcpy(r->data, cmd->data, cmd->len);
    if (g_rec_tail) g_rec_tail->next = r; else g_rec_head = r;
    g_rec_tail = r;
    g_rec_bytes += cmd->len;
    if (g_pending_bytes + g_rec_bytes > PC_MEMORY_MAX) {
        g_st_mem_hard++;
        pc_fault(5);
        return;
    }
}

/* 返回 0 = 已入 recovery 缓冲或已成功交付；-1 = 交付失败（已置 fault）。 */
static int pc_emit_cmd(pc_cmd_t *cmd) {
    if (g_buffering || g_wait_resume || g_rec_bytes > 0) {
        pc_recovery_append(cmd);
        return g_faulted ? -1 : 0;
    }
    if (g_emit) {
        if (g_emit(cmd->data, cmd->len) != 0) {
            /* 未交付：不能计入 emitted，也不能继续放行后续记录。 */
            pc_fault(1);
            return -1;
        }
        g_st_emitted += cmd->len;
    }
    return 0;
}

static void pc_drain_ready(void) {
    while (g_ready_head) {
        pc_cmd_t *cmd = g_ready_head;
        if (g_next_global == 0) g_next_global = cmd->global_seq;
        if (cmd->global_seq < g_next_global) { pc_fault(2); return; }
        if (cmd->global_seq > g_next_global) break;   /* 等待更早的写 */
        g_ready_head = cmd->next;
        int rc = pc_emit_cmd(cmd);
        pc_cmd_free(cmd);
        if (rc != 0) return;
        g_next_global++;
    }
}

/* ---------------- 描述符匹配 ---------------- */
static void pc_try_match(pc_ctx_t *ctx) {
    long long now = pc_now_ms();
    while (ctx->cmd_head && ctx->desc_head) {
        pc_cmd_t *cmd = ctx->cmd_head;
        pc_desc_t *d = ctx->desc_head;
        if (cmd->seq != d->seq) { fprintf(stderr, "[commit] seq mismatch cmd=%llu desc=%llu\n", (unsigned long long)cmd->seq, (unsigned long long)d->seq); pc_fault(2); return; }
        if (cmd->len != d->len) {
            char head[49]; size_t hn = cmd->len < 16 ? cmd->len : 16;
            for (size_t hi = 0; hi < hn; ++hi)
                snprintf(head + hi * 3, sizeof(head) - hi * 3, "%02x ", cmd->data[hi]);
            head[hn * 3] = '\0';
            fprintf(stderr, "[commit] len mismatch cookie=%llu cmd=%u desc=%u seq=%llu "
                            "head=%s\n", (unsigned long long)ctx->cookie,
                    cmd->len, d->len, (unsigned long long)cmd->seq, head);
            pc_fault(2); return;
        }
        /* CRC 由 main 侧同算法计算（FNV-1a 64） */
        uint64_t h = 1469598103934665603ull;
        for (uint32_t i = 0; i < cmd->len; ++i) {
            h ^= (uint64_t)cmd->data[i];
            h *= 1099511628211ull;
        }
        if (h != d->crc) { fprintf(stderr, "[commit] crc mismatch cmd=%llu desc=%llu len=%u\n", (unsigned long long)h, (unsigned long long)d->crc, cmd->len); pc_fault(2); return; }

        ctx->cmd_head = cmd->next;
        if (!ctx->cmd_head) ctx->cmd_tail = NULL;
        ctx->desc_head = d->next;
        if (!ctx->desc_head) ctx->desc_tail = NULL;
        free(d);

        if (d->flags & REPL_COMMIT_FLAG_WRITE) {
            cmd->global_seq = d->global_seq;
            cmd->state = 1;
            g_st_writes++;
            pc_ready_insert(cmd);
            pc_drain_ready();
        } else {
            g_st_skips++;
            pc_cmd_free(cmd);
        }
    }
    if (ctx->cmd_head && ctx->desc_head == NULL &&
        now - ctx->cmd_head->created_ms > PC_TIMEOUT_MS) {
        pc_fault(1);
        return;
    }
    if (ctx->desc_head && ctx->cmd_head == NULL &&
        now - ctx->desc_head->created_ms > PC_TIMEOUT_MS) {
        pc_fault(1);
    }
}

static void pc_on_desc(const repl_commit_desc_t *d) {
    if (g_faulted) return;
    pc_ctx_t *ctx = pc_get_ctx(d->cookie, 1);
    if (!ctx) { pc_fault(1); return; }
    if (ctx->ignored) return;
    if (!ctx->baseline_set) {
        /* BASELINE 尚未到达：先缓存描述符，等 baseline 对齐 */
        pc_desc_t *nd = (pc_desc_t *)malloc(sizeof(*nd));
        if (!nd) { pc_fault(1); return; }
        nd->next = NULL;
        nd->seq = d->command_seq;
        nd->global_seq = d->global_seq;
        nd->crc = d->crc;
        nd->len = d->raw_len;
        nd->flags = d->flags;
        nd->created_ms = pc_now_ms();
        if (ctx->desc_tail) ctx->desc_tail->next = nd; else ctx->desc_head = nd;
        ctx->desc_tail = nd;
        return;
    }
    pc_desc_t *nd = (pc_desc_t *)malloc(sizeof(*nd));
    if (!nd) { pc_fault(1); return; }
    nd->next = NULL;
    nd->seq = d->command_seq;
    nd->global_seq = d->global_seq;
    nd->crc = d->crc;
    nd->len = d->raw_len;
    nd->flags = d->flags;
    nd->created_ms = pc_now_ms();
    if (ctx->desc_tail) ctx->desc_tail->next = nd; else ctx->desc_head = nd;
    ctx->desc_tail = nd;
    pc_try_match(ctx);
}

/* ---------------- 记录输入 ---------------- */
void proxy_commit_on_record(uint64_t cookie, uint64_t recv_seq,
                            const unsigned char *payload, size_t len) {
    if (g_faulted) return;
    pc_ctx_t *ctx = pc_get_ctx(cookie, 1);
    if (!ctx) { pc_fault(1); return; }
    if (ctx->ignored) return;
    if (ctx->recv_seq_valid) {
        if (recv_seq != ctx->last_recv_seq + 1) {
            fprintf(stderr, "[commit] gap cookie=%llu expected=%llu got=%llu baseline=%d\n",
                    (unsigned long long)cookie,
                    (unsigned long long)(ctx->last_recv_seq + 1),
                    (unsigned long long)recv_seq, ctx->baseline_set);
            pc_fault(3); return;
        }
    }
    ctx->last_recv_seq = recv_seq;
    ctx->recv_seq_valid = 1;
    ctx->last_activity_ms = pc_now_ms();

    if (ctx->buf_len + len > PC_CTX_BUF_CAP) { pc_fault(1); return; }
    if (ctx->buf_len + len > ctx->buf_cap) {
        size_t nc = ctx->buf_cap ? ctx->buf_cap : 4096;
        while (nc < ctx->buf_len + len) nc *= 2;
        unsigned char *nb = (unsigned char *)realloc(ctx->buf, nc);
        if (!nb) { pc_fault(1); return; }
        ctx->buf = nb;
        ctx->buf_cap = nc;
    }
    if (len) memcpy(ctx->buf + ctx->buf_len, payload, len);
    ctx->buf_len += len;
    if (len > 4096)
        fprintf(stderr, "[commit] record cookie=%llu seq=%llu len=%zu baseline=%d buf=%zu\n",
                (unsigned long long)cookie, (unsigned long long)recv_seq, len,
                ctx->baseline_set, ctx->buf_len);

    if (ctx->baseline_set) pc_frame_ctx(ctx);
    else pc_try_match(ctx);   /* 可能已有缓存的描述符 */
}

/* ---------------- 控制面 ---------------- */
static void pc_on_baseline(uint64_t cookie, uint64_t next_seq) {
    pc_ctx_t *ctx = pc_get_ctx(cookie, 1);
    if (!ctx) { pc_fault(1); return; }
    int had_baseline = ctx->baseline_set;
    /* 新连接的首个 BASELINE 可能晚于第一批 ringbuf 记录到达：此时 ctx->buf
     * 里保存的正是该连接从命令边界开始的原始字节，必须保留并在 baseline 后
     * framing；若清空会把大 value 的前若干条 record 丢掉（实测 50KB SET
     * 丢失前 32744 字节，尾部被误判为 inline 命令 → COMMIT_DESYNC）。
     * 重新 BASELINE（重连/re-arm）时才丢弃旧字节：其中可能包含 Master 已解析
     * 过的命令，保留会造成 seq 错位。 */
    size_t saved_len = ctx->buf_len;
    pc_cmd_t *cm = ctx->cmd_head;
    while (cm) { pc_cmd_t *nx = cm->next; pc_cmd_free(cm); cm = nx; }
    pc_desc_t *d = ctx->desc_head;
    while (d) { pc_desc_t *nx = d->next; free(d); d = nx; }
    ctx->cmd_head = ctx->cmd_tail = NULL;
    ctx->desc_head = ctx->desc_tail = NULL;
    ctx->next_seq = next_seq ? next_seq : 1;
    ctx->baseline_set = 1;
    ctx->ignored = 0;
    /* 重新 BASELINE 后捕获流可能已跨过被丢弃的窗口：序号重新对齐 */
    ctx->recv_seq_valid = 0;
    ctx->last_recv_seq = 0;
    if (had_baseline) {
        ctx->buf_len = 0;
    } else {
        ctx->buf_len = saved_len;   /* 保留 baseline 之前已缓存的首批数据 */
    }
    pc_frame_ctx(ctx);
    pc_try_match(ctx);
}

static void pc_on_resume(uint64_t marker) {
    /* 丢弃 <= marker（已被 backlog/快照覆盖），其余按序交出。
     * 交付失败必须 fail-closed：未交付的记录不能计入 emitted，也不能继续放行。 */
    unsigned long long dropped_bytes = 0, emitted_bytes = 0;
    int dropped = 0, emitted = 0, deliver_failed = 0;
    pc_rec_t *r = g_rec_head;
    while (r) {
        pc_rec_t *nx = r->next;
        if (!deliver_failed && r->global_seq <= marker) {
            dropped++;
            dropped_bytes += r->len;
        } else if (!deliver_failed) {
            if (g_emit && g_emit(r->data, r->len) != 0) {
                deliver_failed = 1;
            } else {
                emitted++;
                emitted_bytes += r->len;
                g_st_emitted += r->len;
            }
        }
        free(r);
        r = nx;
    }
    g_rec_head = g_rec_tail = NULL;
    g_rec_bytes = 0;
    g_wait_resume = 0;
    g_wait_resume_ms = 0;
    if (deliver_failed) {
        fprintf(stderr, "ebpf-proxy: RESUME marker=%llu: delivery failed after "
                        "%d/%llu bytes — fail-closed\n",
                (unsigned long long)marker, emitted, emitted_bytes);
        pc_fault(1);
        return;
    }
    g_resume_event = 1;
    fprintf(stderr, "ebpf-proxy: RESUME marker=%llu: emitted %d/%llu bytes, "
                    "dropped %d/%llu bytes (<=marker)\n",
            (unsigned long long)marker, emitted, emitted_bytes,
            dropped, dropped_bytes);
}

/* 旧 session/旧 epoch 的消息：epoch=0 视为通配（兼容手动测试/旧 Master），
 * 非 0 且与当前 BASELINE 不一致则丢弃。 */
static int pc_epoch_stale(uint64_t epoch) {
    return epoch != 0 && g_session_epoch != 0 && epoch != g_session_epoch;
}

static void pc_process_msg(const unsigned char *buf, size_t len) {
    if (len < sizeof(repl_commit_msg_t)) return;
    repl_commit_msg_t h;
    memcpy(&h, buf, sizeof(h));
    if (h.magic != REPL_COMMIT_MAGIC || h.version != REPL_COMMIT_VERSION) {
        pc_fault(4);
        return;
    }
    const unsigned char *payload = buf + sizeof(h);
    size_t plen = len - sizeof(h);

    switch (h.type) {
    case REPL_COMMIT_MSG_HELLO:
        g_epoch = h.epoch;
        g_session_epoch = 0;
        proxy_commit_purge();
        break;
    case REPL_COMMIT_MSG_BASELINE: {
        repl_commit_kv_t kv;
        if (plen < sizeof(kv)) { pc_fault(2); break; }
        memcpy(&kv, payload, sizeof(kv));
        /* BASELINE 定义新 session 的 epoch（0 表示旧式/测试消息，不启用校验）。 */
        g_session_epoch = h.epoch;
        pc_on_baseline(kv.cookie, kv.value);
        break;
    }
    case REPL_COMMIT_MSG_IGNORE: {
        repl_commit_kv_t kv;
        if (plen < sizeof(kv)) break;
        if (pc_epoch_stale(h.epoch)) {
            fprintf(stderr, "ebpf-proxy: drop stale IGNORE epoch=%llu (current=%llu)\n",
                    (unsigned long long)h.epoch, (unsigned long long)g_session_epoch);
            break;
        }
        memcpy(&kv, payload, sizeof(kv));
        pc_ctx_t *ctx = pc_get_ctx(kv.cookie, 1);
        if (ctx) { pc_ctx_reset(ctx); ctx->ignored = 1; }
        break;
    }
    case REPL_COMMIT_MSG_FORGET: {
        repl_commit_kv_t kv;
        if (plen < sizeof(kv)) break;
        if (pc_epoch_stale(h.epoch)) {
            fprintf(stderr, "ebpf-proxy: drop stale FORGET epoch=%llu (current=%llu)\n",
                    (unsigned long long)h.epoch, (unsigned long long)g_session_epoch);
            break;
        }
        memcpy(&kv, payload, sizeof(kv));
        pc_ctx_t *ctx = pc_get_ctx(kv.cookie, 0);
        if (ctx) pc_remove_ctx(ctx);
        break;
    }
    case REPL_COMMIT_MSG_RESUME:
        if (pc_epoch_stale(h.epoch)) {
            fprintf(stderr, "ebpf-proxy: drop stale RESUME epoch=%llu (current=%llu)\n",
                    (unsigned long long)h.epoch, (unsigned long long)g_session_epoch);
            break;
        }
        pc_on_resume(h.global_seq);
        break;
    case REPL_COMMIT_MSG_BATCH: {
        if (pc_epoch_stale(h.epoch)) {
            fprintf(stderr, "ebpf-proxy: drop stale BATCH epoch=%llu (current=%llu)\n",
                    (unsigned long long)h.epoch, (unsigned long long)g_session_epoch);
            break;
        }
        size_t need = (size_t)h.count * sizeof(repl_commit_desc_t);
        if (plen < need || h.count > REPL_COMMIT_BATCH_MAX) { pc_fault(2); break; }
        for (uint32_t i = 0; i < h.count; ++i) {
            repl_commit_desc_t d;
            memcpy(&d, payload + (size_t)i * sizeof(d), sizeof(d));
            g_st_desc++;
            pc_on_desc(&d);
            if (g_faulted) break;
        }
        break;
    }
    default:
        break;
    }
}

static void pc_accept_master(void) {
    int fd = accept(g_listen_fd, NULL, NULL);
    if (fd < 0) return;
    if (g_master_fd >= 0) close(g_master_fd);
    g_master_fd = fd;
    proxy_commit_purge();
    fprintf(stderr, "ebpf-proxy: commit channel connected\n");
}

static void pc_read_master(void) {
    unsigned char buf[REPL_COMMIT_MAX_MSG];
    for (;;) {
        ssize_t n = recv(g_master_fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (n > 0) {
            pc_process_msg(buf, (size_t)n);
            if (g_faulted) return;
            continue;
        }
        if (n == 0) {
            close(g_master_fd);
            g_master_fd = -1;
            pc_fault(4);
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        if (errno == EINTR) continue;
        close(g_master_fd);
        g_master_fd = -1;
        pc_fault(4);
        return;
    }
}

void proxy_commit_poll(int buffering) {
    if (g_listen_fd < 0) return;

    if (g_master_fd < 0) {
        pc_accept_master();
    } else {
        pc_read_master();
    }

    if (buffering != g_buffering) {
        g_buffering = buffering;
        if (!buffering && g_rec_bytes > 0 && !g_wait_resume) {
            g_wait_resume = 1;
            g_wait_resume_ms = pc_now_ms();
        }
    }
    if (g_wait_resume && pc_now_ms() - g_wait_resume_ms > PC_TIMEOUT_MS) {
        pc_fault(1);
        return;
    }

    /* 超时检查：已就绪写等待更早 global_seq / 待匹配命令 */
    long long now = pc_now_ms();
    if (g_ready_head && g_next_global != 0 &&
        g_ready_head->global_seq > g_next_global &&
        now - g_ready_head->created_ms > PC_TIMEOUT_MS) {
        pc_fault(1);
        return;
    }
    for (int i = 0; i < PC_MAX_COOKIES; ++i) {
        for (pc_ctx_t *c = g_buckets[i]; c; c = c->next) {
            if (c->ignored) continue;
            if (c->cmd_head && !c->desc_head &&
                now - c->cmd_head->created_ms > PC_TIMEOUT_MS) {
                pc_fault(1);
                return;
            }
            if (c->desc_head && !c->cmd_head &&
                now - c->desc_head->created_ms > PC_TIMEOUT_MS) {
                pc_fault(1);
                return;
            }
        }
    }
}

/* ---------------- 初始化/清理/统计 ---------------- */
int proxy_commit_init(const char *sock_path,
                      proxy_commit_emit_fn emit,
                      proxy_commit_fault_fn fault) {
    g_emit = emit;
    g_fault = fault;
    snprintf(g_sock_path, sizeof(g_sock_path), "%s",
             (sock_path && sock_path[0]) ? sock_path : "./kvstore.commit.sock");

    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        fprintf(stderr, "ebpf-proxy: commit socket failed: %s\n", strerror(errno));
        return -1;
    }
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    size_t plen = strlen(g_sock_path);
    if (plen >= sizeof(sa.sun_path)) plen = sizeof(sa.sun_path) - 1;
    memcpy(sa.sun_path, g_sock_path, plen);
    sa.sun_path[plen] = '\0';
    unlink(g_sock_path);
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
        fprintf(stderr, "ebpf-proxy: commit bind %s failed: %s\n",
                g_sock_path, strerror(errno));
        close(fd);
        return -1;
    }
    chmod(g_sock_path, 0600);
    if (listen(fd, 1) != 0) { close(fd); return -1; }
    g_listen_fd = fd;
    fprintf(stderr, "ebpf-proxy: commit listener on %s\n", g_sock_path);
    return 0;
}

void proxy_commit_cleanup(void) {
    proxy_commit_purge();
    if (g_master_fd >= 0) { close(g_master_fd); g_master_fd = -1; }
    if (g_listen_fd >= 0) { close(g_listen_fd); g_listen_fd = -1; }
    if (g_sock_path[0]) unlink(g_sock_path);
}

int proxy_commit_recovery_pending(void) {
    return g_wait_resume || g_rec_bytes > 0;
}

int proxy_commit_take_resume_event(void) {
    int v = g_resume_event;
    g_resume_event = 0;
    return v;
}

void proxy_commit_get_stats(proxy_commit_stats_t *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
    out->descriptors = g_st_desc;
    out->writes = g_st_writes;
    out->skips = g_st_skips;
    out->pending = (unsigned long long)g_pending_bytes;
    out->timeout = g_st_timeout;
    out->desync = g_st_desync;
    out->gap = g_st_gap;
    out->channel_fault = g_st_channel;
    out->recovery_bytes = (unsigned long long)g_rec_bytes;
    out->emitted_bytes = g_st_emitted;
    out->conn_ctx_count = (unsigned long long)g_ctx_count;
    out->pending_cmd_count = (unsigned long long)(g_pending_cmd_count > 0 ? g_pending_cmd_count : 0);
    out->memory_hard_limit_count = g_st_mem_hard;
    out->next_global_seq = g_next_global;
    out->channel_connected = (g_master_fd >= 0);

    /* oldest pending：ready 队列头 + 每个 ctx 的 cmd/desc 头中最早创建的时间。 */
    long long now = pc_now_ms();
    long long oldest = 0;
    if (g_ready_head) oldest = g_ready_head->created_ms;
    if (g_wait_resume_ms && (oldest == 0 || g_wait_resume_ms < oldest))
        oldest = g_wait_resume_ms;
    for (int i = 0; i < PC_MAX_COOKIES; ++i) {
        for (pc_ctx_t *c = g_buckets[i]; c; c = c->next) {
            long long t = 0;
            if (c->cmd_head) t = c->cmd_head->created_ms;
            else if (c->desc_head) t = c->desc_head->created_ms;
            if (t && (oldest == 0 || t < oldest)) oldest = t;
        }
    }
    out->oldest_pending_ms = oldest > 0 ? (unsigned long long)(now - oldest) : 0;
}
