// ============================================================
// repl_client_capture.bpf.c — fexit-only BPF 程序
//
// fexit: 从 ctx[5] 直接取 tcp_recvmsg 返回值（实际读字节数），
//        再从 msghdr（ctx[1]）读取已接收数据，写入 ringbuf。
//
// kernel 6.1.176 已实测：ctx[5] = tcp_recvmsg 返回值。因此不再需要
// fentry 保存 count_before，也无需 per-thread 上下文 map。
// ============================================================

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>

#ifndef BPF_MAP_TYPE_RINGBUF
#define BPF_MAP_TYPE_RINGBUF 27
#endif

/* Commit Gate 事件头：cookie(8) + recv_seq(8) + payload_len(4) + flags(4) */
#define CLIENT_ENTRY_HDR_SZ    24
/* 单 recv 最大捕获字节数。PERCPU_ARRAY 值上限 32768，减去 24B 头 = 32744。
 * 高 P 下一条 recv 含多条命令（P=160 ~20KB），必须覆盖以免截断丢数据。 */
#define CLIENT_ENTRY_MAX_LEN   32744
#define CAPTURE_FLAG_NONE      0u
/* 生产端水位背压（client_ctl[4]）：每次 fexit 用 bpf_ringbuf_query 查 ringbuf 可用数据，
 * 超高水位直接置位让 master 停（检测频率与 tcp_recvmsg 同步，不受用户态采样粒度限制）；
 * 低水位清除。这是对"用户态按转发队列背压感知不到 ringbuf 堵塞"的根治。 */
#define RB_CTL_BACKPRESSURE_KEY   4
#define RB_HIGH_WATERMARK         (32u * 1024 * 1024)   /* 置位：可用数据 ≥32MB */
#define RB_LOW_WATERMARK          (8u * 1024 * 1024)    /* 清除：可用数据 ≤8MB */
#define RB_WAKE_THRESHOLD         (8u * 1024 * 1024)    /* 自适应唤醒阈值 */
/* capture gate（client_ctl，键位与 include/kvstore/replication/client_ctl.h 对齐）：
 *   7  = capture_enable：1 表示允许捕获（默认 0；Master BASELINE 握手完成后置 1）
 *   13 = capture_fault： 1 表示检测到截断/不可信捕获，需要 fail-closed 重同步 */
#define CTL_KEY_CAPTURE_ENABLE    7
#define CTL_KEY_CAPTURE_FAULT     13
/* bpf_ringbuf_output/query 标志（linux/bpf.h 未导出这些宏） */
#ifndef BPF_RB_NO_WAKEUP
#define BPF_RB_NO_WAKEUP 1
#endif
#ifndef BPF_RB_FORCE_WAKEUP
#define BPF_RB_FORCE_WAKEUP 2
#endif
#ifndef BPF_RB_AVAIL_DATA
#define BPF_RB_AVAIL_DATA 0
#endif

/* 从 msg+32 读取 {_count, ptr, _nr} 的偏移 */
struct iov_head {
    unsigned long long _count;
    unsigned long ptr;
    unsigned long _nr;
};

/* ---- BPF Maps ---- */

/* debug 统计键 */
#define ST_HIT        0   /* fexit 命中 pid 过滤 */
#define ST_HEAD_FAIL  1   /* 读 iov_head (msg+32) 失败 */
#define ST_RETVAL_LE0 2   /* ctx[5] 返回值 <= 0 */
#define ST_IOVEC      3   /* 走 IOVEC 分支 */
#define ST_UBUF       4   /* 走 UBUF 分支 */
#define ST_USER_FAIL  5   /* bpf_probe_read_user 读数据失败 */
#define ST_RB_OK      6   /* ringbuf_output 成功 */
#define ST_RB_DROP    7   /* ringbuf_output 失败（ringbuf 满，应被主进程背压避免） */
#define ST_TRUNCATED  8   /* retval > CLIENT_ENTRY_MAX_LEN，记录被丢弃并置 fault */
#define ST_BYTES      9   /* 成功写入 ringbuf 的 payload 字节数 */
#define ST_NO_COOKIE  10  /* bpf_get_socket_cookie 失败/为 0 */
#define ST_SEQ_FAIL   11  /* cookie_seq map 更新失败 */
#define ST_CAP_OFF    12  /* CAPTURE_ENABLE=0 直接返回（无 Slave/未握手，零开销路径） */

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, __u64);
} client_ctl SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 16);
    __type(key, char[32]);
    __type(value, __u64);
} proxy_cfg SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    /* 0~15 由 BPF 自己累加；16~21 由 ebpf-proxy 发布 proxy_cache 统计
     * （见 src/ebpf_proxy/main.c 的 publish_cache_stats），Master INFO 读同一张 map。 */
    __uint(max_entries, 32);
    __type(key, __u32);
    __type(value, __u64);
} client_stats SEC(".maps");

static __always_inline void cstat_inc(__u32 k) {
    __u64 *v = bpf_map_lookup_elem(&client_stats, &k);
    if (v) __sync_fetch_and_add(v, 1);
}

static __always_inline void cstat_add(__u32 k, __u64 n) {
    __u64 *v = bpf_map_lookup_elem(&client_stats, &k);
    if (v) __sync_fetch_and_add(v, n);
}

/* ebpf-proxy 侧统计（用户态写、Master 读 INFO 用）。
 * 键定义与 src/ebpf_proxy/main.c 的 PST_* 保持一致。 */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 32);
    __type(key, __u32);
    __type(value, __u64);
} proxy_stats SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    /* 64MB：高 P 长跑时转发率 ~292MB/s（P=160），4MB 只容 ~13ms，proxy 调度抖动
     * 即溢出丢数据（实测 P=160 长跑丢 ~49%）。64MB 容 ~218ms，吸收瞬时抖动。 */
    __uint(max_entries, 1 << 26);
} client_cache_ringbuf SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, unsigned char[CLIENT_ENTRY_HDR_SZ + CLIENT_ENTRY_MAX_LEN]);
} client_tmpbuf SEC(".maps");

/* 每连接（socket cookie）捕获序号：proxy 据此检测 ringbuf 丢记录（缺口→fail-closed） */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 16384);
    __type(key, __u64);
    __type(value, __u64);
} cookie_seq SEC(".maps");

/* 只捕获 Master 注册的客户端连接（accept 时 allow，变 replica/close 时 disallow）。
 * 避免捕获副本控制/RDMA listener 等其它 socket 的 recv（可能 >32744 触发假截断）。 */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 16384);
    __type(key, __u64);
    __type(value, __u8);
} client_cookies SEC(".maps");

/* ──── fexit: 读取数据，写入 ringbuf ──── */
SEC("fexit/tcp_recvmsg")
int fexit_tcp_recvmsg(__u64 *ctx)
{
    __u64 *ctl_pid = bpf_map_lookup_elem(&client_ctl, &(__u32){1});
    if (!ctl_pid || !*ctl_pid)
        return 0;

    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != (__u32)(*ctl_pid))
        return 0;

    /* capture gate：只有 Master BASELINE 握手完成后（enable=1）才捕获；
     * 截断/通道故障时 Master 置 0，从可信边界重建。 */
    __u32 gate_key = CTL_KEY_CAPTURE_ENABLE;
    __u64 *gate = bpf_map_lookup_elem(&client_ctl, &gate_key);
    if (!gate || *gate != 1) {
        cstat_inc(ST_CAP_OFF);
        return 0;
    }
    __u32 fault_key = CTL_KEY_CAPTURE_FAULT;
    __u64 *fault = bpf_map_lookup_elem(&client_ctl, &fault_key);
    if (fault && *fault)
        return 0;

    cstat_inc(ST_HIT);

    /* Commit Gate：socket cookie + 每连接捕获序号 */
    __u64 cookie = bpf_get_socket_cookie((void *)(unsigned long)ctx[0]);
    if (cookie == 0) {
        cstat_inc(ST_NO_COOKIE);
        return 0;
    }
    __u8 *allowed = bpf_map_lookup_elem(&client_cookies, &cookie);
    if (!allowed || *allowed != 1)
        return 0;

    /* tcp_recvmsg 返回类型是 int；ctx[5] 在错误返回时可能是未符号扩展的
     * 32 位负数（如 EAGAIN=0xFFFFFFF5），直接当 long long 会变成巨大正数并
     * 误判截断。必须按 int 解释。 */
    int ret_i = (int)(__u32)ctx[5];
    if (ret_i <= 0) {
        cstat_inc(ST_RETVAL_LE0);
        return 0;
    }
    long long retval = (long long)ret_i;
    /* fail-closed：超过单 record 上限时不截断输出（截断会让 slave 收到残缺命令/丢字节），
     * 记录 stats 并置 fault，由 Master 使当前复制会话失效并重同步。 */
    if (retval > CLIENT_ENTRY_MAX_LEN) {
        cstat_inc(ST_TRUNCATED);
        __u64 one = 1;
        bpf_map_update_elem(&client_ctl, &fault_key, &one, BPF_ANY);
        return 0;
    }

    unsigned long msg_ptr = (unsigned long)ctx[1];
    if (!msg_ptr)
        return 0;

    struct iov_head head;
    if (bpf_probe_read_kernel(&head, sizeof(head),
            (const void *)(msg_ptr + 32)) != 0) {
        cstat_inc(ST_HEAD_FAIL);
        return 0;
    }

    int data_len;
    unsigned long user_ptr;
    if (head._nr > 0) {
        cstat_inc(ST_IOVEC);
        if (!head.ptr) return 0;
        struct { unsigned long b; unsigned long l; } vec;
        if (bpf_probe_read_kernel(&vec, sizeof(vec), (const void *)head.ptr) != 0)
            return 0;
        if (!vec.b || vec.l == 0) return 0;
        unsigned long long safe_len = vec.l;
        if (safe_len > (unsigned long long)retval)
            safe_len = (unsigned long long)retval;
        if (safe_len > CLIENT_ENTRY_MAX_LEN)
            safe_len = CLIENT_ENTRY_MAX_LEN;
        if (safe_len == 0) return 0;
        data_len = (int)safe_len;
        user_ptr = (unsigned long)vec.b;
    } else {
        cstat_inc(ST_UBUF);
        if (!head.ptr || head._count == 0) return 0;
        /* ITER_UBUF: head.ptr = ubuf 基址（不随拷贝推进），数据在 [ubuf, ubuf+retval)。 */
        data_len = (int)retval;
        user_ptr = head.ptr;
    }
    if (data_len <= 0 || user_ptr == 0) return 0;
    if (data_len > CLIENT_ENTRY_MAX_LEN) return 0;
    /* volatile 阻止编译器把已知范围优化掉 AND；给 verifier 一个明确的
     * 无符号上界（bpf_probe_read_user 的 size 参数要求 min>=0）。 */
    volatile unsigned int rd_tmp = (unsigned int)data_len;
    unsigned int rd_len = rd_tmp & 0x7fffu;
    if (rd_len > CLIENT_ENTRY_MAX_LEN) rd_len = CLIENT_ENTRY_MAX_LEN;

    /* 序号只在“确实要输出 record”时分配：前面任何 early-return（EAGAIN/
     * 非目标 socket/截断）都不能消耗序号，否则 proxy 会看到空洞。 */
    __u64 *seqp = bpf_map_lookup_elem(&cookie_seq, &cookie);
    __u64 seq = seqp ? (*seqp + 1) : 1;
    if (bpf_map_update_elem(&cookie_seq, &cookie, &seq, BPF_ANY) != 0) {
        cstat_inc(ST_SEQ_FAIL);
        return 0;
    }

    /* 用 client_tmpbuf 中转 + ringbuf_output。自适应唤醒：
     * 低水位 BPF_RB_NO_WAKEUP(1) 省每 recv 的 self-IPI（原 flags=0 占 master CPU2 25-27%）；
     * 高水位 BPF_RB_FORCE_WAKEUP 及时通知 proxy 排空，避免突发灌满 ringbuf。 */
    __u32 tmp_key = 0;
    unsigned char(*entry)[CLIENT_ENTRY_HDR_SZ + CLIENT_ENTRY_MAX_LEN];
    entry = bpf_map_lookup_elem(&client_tmpbuf, &tmp_key);
    if (!entry) return 0;

    __u32 payload_len = (__u32)data_len;
    __u32 cap_flags = CAPTURE_FLAG_NONE;
    __builtin_memcpy(*entry, &cookie, 8);
    __builtin_memcpy(*entry + 8, &seq, 8);
    __builtin_memcpy(*entry + 16, &payload_len, 4);
    __builtin_memcpy(*entry + 20, &cap_flags, 4);

    if (bpf_probe_read_user((*entry) + CLIENT_ENTRY_HDR_SZ, rd_len,
            (const void *)user_ptr) != 0) {
        cstat_inc(ST_USER_FAIL);
        return 0;
    }

    /* 生产端水位背压：检测频率与 tcp_recvmsg 同步。只负责高水位置位（仅在状态变化时
     * 更新 map，避免正常低水位下每次 fexit 冗余写）；低水位由 proxy 消费端（ringbuf
     * 排空后）清除——BPF 若在此清除，master 被停后无 fexit 会自锁（实测 QPS 崩到 8k）。 */
    __u64 avail = bpf_ringbuf_query(&client_cache_ringbuf, BPF_RB_AVAIL_DATA);
    __u32 bp_key = RB_CTL_BACKPRESSURE_KEY;
    __u64 *bp = bpf_map_lookup_elem(&client_ctl, &bp_key);
    if (avail >= RB_HIGH_WATERMARK && bp && *bp == 0) {
        __u64 one = 1;
        bpf_map_update_elem(&client_ctl, &bp_key, &one, BPF_ANY);
    }
    /* 自适应唤醒：只在"跨过唤醒阈值"这一次 FORCE_WAKEUP（高水位后逐记录强制唤醒
     * 会重新形成之前想避免的 IPI 风暴）；否则 NO_WAKEUP 让 proxy 定时 poll。 */
    __u64 next = avail + CLIENT_ENTRY_HDR_SZ + (__u64)rd_len;
    __u64 rb_flags = (avail < RB_WAKE_THRESHOLD && next >= RB_WAKE_THRESHOLD)
                     ? BPF_RB_FORCE_WAKEUP : BPF_RB_NO_WAKEUP;

    if (bpf_ringbuf_output(&client_cache_ringbuf, *entry,
                           CLIENT_ENTRY_HDR_SZ + rd_len, rb_flags) != 0) {
        /* ringbuf 满：应已被上面水位背压避免；此处计数作 canary。 */
        cstat_inc(ST_RB_DROP);
    } else {
        cstat_inc(ST_RB_OK);
        cstat_add(ST_BYTES, (__u64)data_len);
    }
    return 0;
}

char _license[] SEC("license") = "GPL";
