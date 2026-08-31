// ============================================================
// repl_kprobe.bpf.c — kprobe BPF 程序
//
// Hook tcp_sendmsg，拦截 replication 流量。
// 从 tcp_sendmsg(struct sock *sk, struct msghdr *msg, size_t size) 的
// msg 参数中读取实际数据，写入 BPF ringbuf。
//
// 用户态回调 kprobe_ringbuf_cb 收到数据后进行 RDMA WRITE。
//
// x86_64 调用约定:
//   PT_REGS_PARM1(ctx) = sk (struct sock*)
//   PT_REGS_PARM2(ctx) = msg (struct msghdr*)
//   PT_REGS_PARM3(ctx) = size (size_t)
//
// 限制: 单次最大 500 字节，当前增量数据量小可满足。
// ============================================================

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>

/* ---- 兼容性定义 ---- */
#ifndef BPF_MAP_TYPE_RINGBUF
#define BPF_MAP_TYPE_RINGBUF 27
#endif

/* ringbuf entry 格式: [4B payload_len][payload_len bytes payload]
 * payload_len == 0 表示仅通知（无实际数据） */
#define KVS_KPROBE_ENTRY_HDR_SZ   4
#define KVS_KPROBE_ENTRY_MAX_LEN  500  /* 单次最大数据长度 */

/* 手动定义 pt_regs — 字段名必须与 kernel 5.15 BTF 完全一致
 * 否则 BPF CO-RE 加载时重定位失败。
 * 不使用 bpf_tracing.h 的 PT_REGS_PARM 宏，直接访问寄存器。 */
struct pt_regs {
    unsigned long r15;    unsigned long r14;
    unsigned long r13;    unsigned long r12;
    unsigned long bp;     unsigned long bx;
    unsigned long r11;    unsigned long r10;
    unsigned long r9;     unsigned long r8;
    unsigned long ax;     unsigned long cx;
    unsigned long dx;     unsigned long si;
    unsigned long di;     unsigned long orig_ax;
    unsigned long ip;     unsigned long cs;
    unsigned long flags;  unsigned long sp;
    unsigned long ss;
};

/* ---- BPF Maps ---- */

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u64);
} kprobe_ctl SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, __u64);
} kprobe_stats SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 22); /* 4MB */
} repl_ringbuf SEC(".maps");

/* 临时缓冲区（per-CPU，避免 BPF 栈 512B 限制） */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, unsigned char[KVS_KPROBE_ENTRY_HDR_SZ + KVS_KPROBE_ENTRY_MAX_LEN]);
} kprobe_tmpbuf SEC(".maps");

/* ---- Control Keys ---- */
/* 合并优化: PID=0 表示禁用，无需独立 ENABLED 键 */
#define KVS_KPROBE_CTL_PID             1
#define KVS_KPROBE_CTL_FULLSYNC_DONE   2  /* 写1表示全量同步完成（BPF检测到REPLDONE）*/

/* ---- Stats Keys ---- */
#define KVS_KPROBE_STAT_HIT            0
#define KVS_KPROBE_STAT_SKIP_PID       1
#define KVS_KPROBE_STAT_RB_ERR         2
#define KVS_KPROBE_STAT_DATA_OVR       3  /* 数据超过单次上限 */
#define KVS_KPROBE_STAT_READ_ERR       4  /* probe_read 失败 */
#define KVS_KPROBE_STAT_REPLDONE       5  /* REPLDONE 探测计数 */

/* 从 userspace 的 iovec 数组中读取数据拼接到 buf
 * 返回总数据长度（<= max_len），0 表示失败。
 *
 * 布局确认（通过 BTF + 探测）:
 *   msghdr: msg_name(8)+msg_namelen(4)+pad(4)+msg_iter(16)
 *   iov_iter: iter_type(1)+nofault(1)+data_source(1)+pad(5)=8B
 *             iov_offset(8)+count(8)+iov(8)+nr_segs(8)=40B
 *   iov 在 msg+16+24=msg+40, nr_segs 在 msg+16+32=msg+48
 *   iov 和 nr_segs 相邻 16 字节，合并为一次 bpf_probe_read_kernel */
static __always_inline int read_msg_data(unsigned long msg_ptr,
    unsigned char *buf, int max_len)
{
    /* 1. 合并读取 iov 指针 + nr_segs（msg+40 处连续 16 字节） */
    struct { unsigned long iov_ptr; unsigned long _nr_segs; } head;
    if (bpf_probe_read_kernel(&head, sizeof(head),
            (const void *)(msg_ptr + 40)) != 0)
        return 0;
    if (!head.iov_ptr || head._nr_segs == 0) return 0;

    /* 2. 读取第一个 iovec */
    struct { unsigned long b; unsigned long l; } vec;
    if (bpf_probe_read_kernel(&vec, sizeof(vec),
            (const void *)head.iov_ptr) != 0)
        return 0;
    if (!vec.b || vec.l == 0)
        return 0;

    /* 3. 限制大小不超过 max_len */
    unsigned long long safe_len = vec.l;
    if (safe_len > (unsigned long long)max_len)
        safe_len = (unsigned long long)max_len;
    if (safe_len == 0) return 0;

    /* 4. 从用户空间读取实际数据 */
    if (bpf_probe_read_user(buf, (__u32)safe_len,
            (const void *)(unsigned long)vec.b) != 0) {
        __u64 *st = bpf_map_lookup_elem(&kprobe_stats,
            &(__u32){KVS_KPROBE_STAT_READ_ERR});
        if (st) __sync_fetch_and_add(st, 1);
        return 0;
    }
    return (int)safe_len;
}

SEC("kprobe/tcp_sendmsg")
int kprobe_kvs_repl_tcp_sendmsg(struct pt_regs *ctx)
{
    /* CTL_PID=0 表示禁用（PID+enabled 合并） */
    __u64 *ctl_pid, *stat;

    /* 1. 检查开关 + PID 过滤（合并为一次 lookup） */
    ctl_pid = bpf_map_lookup_elem(&kprobe_ctl, &(__u32){KVS_KPROBE_CTL_PID});
    if (!ctl_pid || !*ctl_pid)
        return 0;

    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != (__u32)(*ctl_pid)) {
        stat = bpf_map_lookup_elem(&kprobe_stats,
            &(__u32){KVS_KPROBE_STAT_SKIP_PID});
        if (stat) __sync_fetch_and_add(stat, 1);
        return 0;
    }

    /* 2. 获取数据长度：dx = tcp_sendmsg 的 size 参数 (x86_64) */
    __u32 size = (__u32)ctx->dx;
    if (size == 0) return 0;
    if (size > KVS_KPROBE_ENTRY_MAX_LEN) {
        stat = bpf_map_lookup_elem(&kprobe_stats,
            &(__u32){KVS_KPROBE_STAT_DATA_OVR});
        if (stat) __sync_fetch_and_add(stat, 1);
        size = KVS_KPROBE_ENTRY_MAX_LEN;
    }

    /* 3. 从 msg 参数中读取数据 */
    __u32 map_key = 0;
    unsigned char(*entry)[KVS_KPROBE_ENTRY_HDR_SZ + KVS_KPROBE_ENTRY_MAX_LEN];
    entry = bpf_map_lookup_elem(&kprobe_tmpbuf, &map_key);
    if (!entry) return 0;

    /* si = tcp_sendmsg 的 msg 参数 (x86_64) */
    unsigned long msg_ptr = (unsigned long)ctx->si;

    /* 先写 4 字节长度头（初始为 0，读取成功后再更新） */
    __u32 payload_len = 0;
    __builtin_memcpy(*entry, &payload_len, 4);

    int data_len = read_msg_data(msg_ptr, (*entry) + 4, KVS_KPROBE_ENTRY_MAX_LEN);
    if (data_len <= 0) {
        /* 读数据失败，退化为通知模式 */
        payload_len = 0;
        __builtin_memcpy(*entry, &payload_len, 4);
        if (bpf_ringbuf_output(&repl_ringbuf, *entry, 4, 0) != 0) {
            stat = bpf_map_lookup_elem(&kprobe_stats,
                &(__u32){KVS_KPROBE_STAT_RB_ERR});
            if (stat) __sync_fetch_and_add(stat, 1);
        }
        return 0;
    }

    /* 4. 更新 payload_len 并写入 ringbuf */
    payload_len = (__u32)data_len;
    __builtin_memcpy(*entry, &payload_len, 4);

    int entry_size = KVS_KPROBE_ENTRY_HDR_SZ + data_len;
    if (bpf_ringbuf_output(&repl_ringbuf, *entry, entry_size, 0) != 0) {
        stat = bpf_map_lookup_elem(&kprobe_stats,
            &(__u32){KVS_KPROBE_STAT_RB_ERR});
        if (stat) __sync_fetch_and_add(stat, 1);
        return 0;
    }

    /* 5. 检测 REPLDONE 命令 — 匹配 payload 开头 "REPLDONE\r\n" (10字节)
     * REPLDONE 以 RESP 格式 *1\r\n$8\r\nREPLDONE\r\n (15字节) 发送，
     * 在 TCP 路径中整个命令在一个 tcp_sendmsg 调用中发送，
     * 因此检测 payload 开头偏移 0 即可（避免变长偏移导致 verifier 拒绝）。 */
    if (data_len >= 10) {
        unsigned char *payload = (*entry) + 4;
        if (payload[0] == 'R' &&
            payload[1] == 'E' &&
            payload[2] == 'P' &&
            payload[3] == 'L' &&
            payload[4] == 'D' &&
            payload[5] == 'O' &&
            payload[6] == 'N' &&
            payload[7] == 'E' &&
            payload[8] == '\r' &&
            payload[9] == '\n') {

            /* 设置 fullsync_done 标志 */
            __u32 fs_key = KVS_KPROBE_CTL_FULLSYNC_DONE;
            __u64 fs_val = 1;
            bpf_map_update_elem(&kprobe_ctl, &fs_key, &fs_val, 0);

            /* 统计 */
            __u64 *repldone_st = bpf_map_lookup_elem(&kprobe_stats,
                &(__u32){KVS_KPROBE_STAT_REPLDONE});
            if (repldone_st) __sync_fetch_and_add(repldone_st, 1);

            /* 写入特殊通知到 ringbuf (payload_len=0xFFFFFFFF 魔法值)
             * 用户态回调检测到该值后触发缓存 flush */
            __u32 magic = 0xFFFFFFFF;
            __builtin_memcpy(*entry, &magic, 4);
            bpf_ringbuf_output(&repl_ringbuf, *entry, 4, 0);
        }
    }

    /* 6. 更新统计 */
    stat = bpf_map_lookup_elem(&kprobe_stats, &(__u32){KVS_KPROBE_STAT_HIT});
    if (stat) __sync_fetch_and_add(stat, 1);

    return 0;  /* 放行 TCP 正常发送 */
}

char LICENSE[] SEC("license") = "GPL";
