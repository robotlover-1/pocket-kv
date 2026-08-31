/*
 * kprobe_capture.bpf.c — kprobe on tcp_recvmsg 截获接收数据到 ringbuf
 *
 * 用法：用户态加载后，tcp_recvmsg 返回的数据自动进 ringbuf。
 * 通过 PID + 端口过滤，只截获 echo server 连接上接收的数据。
 *
 * x86_64 调用约定 (tcp_recvmsg):
 *   struct sock *sk   = di (PT_REGS_PARM1)
 *   struct msghdr *msg = si (PT_REGS_PARM2)
 *   size_t len         = dx (PT_REGS_PARM3)
 *   返回值 (ax)         = 实际接收字节数
 */
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

#ifndef BPF_MAP_TYPE_RINGBUF
#define BPF_MAP_TYPE_RINGBUF 27
#endif

/* x86_64 pt_regs — 替代 vmlinux.h，避免 CO-RE BTF 依赖 */
struct pt_regs {
    unsigned long r15, r14, r13, r12, bp, bx;
    unsigned long r11, r10, r9, r8;
    unsigned long ax, cx, dx, si, di;
    unsigned long orig_ax, ip, cs, flags, sp, ss;
};

/* tracepoint 原始参数结构 */
struct trace_event_raw_sys_enter {
    unsigned long long unused;
    long id;
    unsigned long args[6];
};
struct trace_event_raw_sys_exit {
    unsigned long long unused;
    long id;
    long ret;
};

#define CAPTURE_MAX_DATA 2048
#define STAT_HIT         0
#define STAT_SKIP_PORT   1
#define STAT_SKIP_PID    2
#define STAT_RB_ERR      3
#define STAT_BYTES       4
#define STAT_SKIP_FD     5   /* tp: fd 不匹配被过滤 */
#define STAT_MSG_NULL    6   /* kretprobe: msg 指针为空 */
#define STAT_STEP1       7   /* debug: read_iov_data step 1 fail (kernel read offset 40) */
#define STAT_STEP2       8   /* debug: step 2 fail (iov_ptr or nr_segs zero) */
#define STAT_STEP3       9   /* debug: step 3 fail (user read iovec) */
#define STAT_STEP4      10   /* debug: step 4 fail (base or len zero) */
#define STAT_STEP5      11   /* debug: step 5 fail (user read data) */
#define CTL_ENABLED      0
#define CTL_PID          1
#define CTL_PORT         2
#define CTL_MASTER_FD    3   /* tp 模式: 只捕获此 fd 的 read()，消除反馈循环 */

/* 控制 map */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u64);
} ctl SEC(".maps");

/* 统计 map */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, __u64);
} stats SEC(".maps");

/* ringbuf */
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 22); /* 4MB */
} ringbuf SEC(".maps");

/* 临时缓冲区 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 2);
    __type(key, __u32);
    __type(value, unsigned char[4 + CAPTURE_MAX_DATA]);
} tmpbuf SEC(".maps");

/* 用于 kprobe→kretprobe 传递 msg 指针 (key=0) 和 sk 指针 (key=1) */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 2);
    __type(key, __u32);
    __type(value, unsigned long);
} entry_msg SEC(".maps");

/* 用于 tp: sys_enter_read → sys_exit_read 传递 buf+fd
 * key=TID, value={buf_ptr, fd}
 * fd 在 exit 侧过滤（避免 enter 时机 CTL_MASTER_FD 未设值的问题） */
struct tp_enter_data {
    unsigned long buf;
    __u64 fd;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1024);
    __type(key, __u32);
    __type(value, struct tp_enter_data);
} enter_buf SEC(".maps");

static __always_inline __u64 ctl_get(__u32 key) {
    __u64 *v = bpf_map_lookup_elem(&ctl, &key);
    return v ? *v : 0;
}

static __always_inline void stat_inc(__u32 key) {
    __u64 *v = bpf_map_lookup_elem(&stats, &key);
    if (v) __sync_fetch_and_add(v, 1);
}

static __always_inline void stat_add(__u32 key, __u64 delta) {
    __u64 *v = bpf_map_lookup_elem(&stats, &key);
    if (v) __sync_fetch_and_add(v, delta);
}

/* 从 msghdr→iov_iter 读取数据（支持 ITER_IOVEC 和 ITER_UBUF）
 *
 * 内核 5.15: tcp_recvmsg 使用 ITER_IOVEC → iov 指向用户 iovec 数组，nr_segs>=1
 * 内核 6.1:  tcp_recvmsg 使用 ITER_UBUF  → ubuf 直接指向用户缓冲区，nr_segs=0, count=len
 *
 * 偏移（x86_64）:
 *   msghdr.msg_iter    @ offset 16
 *   iov_iter.iov/ubuf  @ msg_iter + 24 → msg + 40
 *   iov_iter.count     @ msg_iter + 16 → msg + 32
 *   iov_iter.nr_segs   @ msg_iter + 32 → msg + 48
 *
 * 两步探测：先读 iov+count+nr_segs(24B)，nr_segs>0→IOVEC，否则→UBUF */
static __always_inline int read_iov_data(unsigned long msg_ptr,
    unsigned char *buf, int max_len, int *out_step)
{
    /* 1. 合并读取 count, iov/ubuf, nr_segs（24 字节从 msg_ptr+32）
     *    layout: [8B count] [8B iov/ubuf] [8B nr_segs] */
    struct { unsigned long long _count; unsigned long ptr; unsigned long _nr; } head;
    if (bpf_probe_read_kernel(&head, sizeof(head),
            (const void *)(msg_ptr + 32)) != 0) {
        *out_step = 1;
        stat_inc(STAT_STEP1);
        return 0;
    }

    /* 2. 探查迭代器类型 */
    if (head._nr > 0) {
        /* ITER_IOVEC: iov 指向用户态 iovec 结构体 */
        if (!head.ptr) { *out_step = 2; stat_inc(STAT_STEP2); return 0; }
        struct { unsigned long base; unsigned long len; } vec;
        if (bpf_probe_read_user(&vec, sizeof(vec),
                (const void *)head.ptr) != 0) {
            *out_step = 3; stat_inc(STAT_STEP3); return 0;
        }
        if (!vec.base || vec.len == 0) {
            *out_step = 4; stat_inc(STAT_STEP4); return 0;
        }
        unsigned long long safe_len = vec.len;
        if (safe_len > (unsigned long long)max_len)
            safe_len = (unsigned long long)max_len;
        if (bpf_probe_read_user(buf, (__u32)safe_len,
                (const void *)(unsigned long)vec.base) != 0) {
            *out_step = 5; stat_inc(STAT_STEP5); return 0;
        }
        *out_step = 0;
        return (int)safe_len;
    }

    /* ITER_UBUF: ubuf 直接指向用户缓冲区，count=实际长度 */
    if (!head.ptr || head._count == 0) {
        *out_step = 2; stat_inc(STAT_STEP2); return 0;
    }
    unsigned long long safe_len = head._count;
    if (safe_len > (unsigned long long)max_len)
        safe_len = (unsigned long long)max_len;
    if (bpf_probe_read_user(buf, (__u32)safe_len,
            (const void *)(unsigned long)head.ptr) != 0) {
        *out_step = 5; stat_inc(STAT_STEP5); return 0;
    }
    *out_step = 0;
    return (int)safe_len;
}

/* ──── fentry/fexit/tp 禁用：需要 BTF，改用 kprobe ──── */
#if 0
SEC("fentry/tcp_recvmsg")
int fentry_recv(__u64 *ctx)
{
    /* ctx[0]=sk, ctx[1]=msg, ctx[2]=len (fentry: ctx = 函数参数) */
    /* CTL_PID=0 表示禁用（PID+enabled 合并） */
    __u64 target_pid = ctl_get(CTL_PID);
    if (!target_pid) return 0;

    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != (__u32)target_pid) {
        stat_inc(STAT_SKIP_PID);
        return 0;
    }

    /* 保存 msg 指针 (key=0) 供 fexit 使用 */
    unsigned long msg_val = (unsigned long)ctx[1];
    __u32 key0 = 0;
    bpf_map_update_elem(&entry_msg, &key0, &msg_val, 0);

    /* 同时保存 sk 指针 (key=1) 供 fexit 的 sk_buff 回退 */
    unsigned long sk_val = (unsigned long)ctx[0];
    __u32 key1 = 1;
    bpf_map_update_elem(&entry_msg, &key1, &sk_val, 0);

    return 0;
}

/* ──── fexit: 在 tcp_recvmsg 返回时读取数据写 ringbuf ────
 *
 * Kernel 6.1 fexit ctx 不包含 retval（与 5.15 不同），ctx 布局与 fentry 相同:
 *   ctx[0]=sk, ctx[1]=msg, ctx[2]=len
 * 因此不能依赖 ctx[0] 做 retval 检查，改用 PID 过滤避免非目标进程的开销 */
SEC("fexit/tcp_recvmsg")
int fexit_recv(__u64 *ctx)
{
    /* CTL_PID=0 表示禁用（PID+enabled 合并） */
    __u64 target_pid = ctl_get(CTL_PID);
    if (!target_pid) return 0;

    /* PID 过滤: 只处理目标进程的调用 */
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != (__u32)target_pid) return 0;

    /* 从 entry_msg 取 fentry 保存的 msg 指针 (key=0) */
    __u32 key0 = 0;
    unsigned long *msg_ptr = bpf_map_lookup_elem(&entry_msg, &key0);
    if (!msg_ptr || *msg_ptr == 0) {
        stat_inc(STAT_MSG_NULL);
        return 0;
    }

    /* 用 tmpbuf 做临时缓冲 */
    unsigned char *buf = bpf_map_lookup_elem(&tmpbuf, &key0);
    if (!buf) return 0;

    /* 头 4 字节 = payload 长度 */
    __u32 plen = 0;
    __builtin_memcpy(buf, &plen, 4);

    int step = 0;
    int data_len = read_iov_data(*msg_ptr, buf + 4, CAPTURE_MAX_DATA, &step);
    if (data_len <= 0) {
        stat_inc(STAT_RB_ERR);
        return 0;
    }

    plen = (__u32)data_len;
    __builtin_memcpy(buf, &plen, 4);

    void *entry = bpf_ringbuf_reserve(&ringbuf, 4 + CAPTURE_MAX_DATA, 0);
    if (!entry) {
        stat_inc(STAT_RB_ERR);
        return 0;
    }
    bpf_probe_read_kernel(entry, 4 + CAPTURE_MAX_DATA, buf);
    bpf_ringbuf_submit(entry, 0);

    stat_inc(STAT_HIT);
    stat_add(STAT_BYTES, (__u64)data_len);

    /* 清除保存的 msg 指针 */
    unsigned long zero = 0;
    bpf_map_update_elem(&entry_msg, &key0, &zero, 0);

    return 0;
}

/* ──── 旧 kprobe 版本保留作为 fallback ──── */
#endif /* fentry/fexit/tp disabled */
SEC("kprobe/tcp_recvmsg")
int kp_recv_entry(struct pt_regs *ctx)
{
    /* CTL_PID=0 表示禁用（PID+enabled 合并，与生产代码一致） */
    __u64 target_pid = ctl_get(CTL_PID);
    if (!target_pid) return 0;

    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != (__u32)target_pid) {
        stat_inc(STAT_SKIP_PID);
        return 0;
    }

    /* 保存 msg 指针 (key=0) */
    unsigned long msg_ptr = (unsigned long)ctx->si;
    __u32 key0 = 0;
    bpf_map_update_elem(&entry_msg, &key0, &msg_ptr, 0);

    /* 保存 sk 指针 (key=1)，供 kretprobe 尝试 sk_buff 直读 */
    unsigned long sk_ptr = (unsigned long)ctx->di;
    __u32 key1 = 1;
    bpf_map_update_elem(&entry_msg, &key1, &sk_ptr, 0);

    return 0;
}

/* ──── 旧 kretprobe 版本保留作为 fallback ──── */
SEC("kretprobe/tcp_recvmsg")
int kp_recv_return(struct pt_regs *ctx)
{
    /* CTL_PID=0 表示禁用（与 entry 一致，无需独立 ENABLED 键） */
    __u64 target_pid = ctl_get(CTL_PID);
    if (!target_pid) return 0;

    long retval = (long)ctx->ax;
    if (retval <= 0) return 0;

    __u32 key0 = 0;
    unsigned long *msg_ptr = bpf_map_lookup_elem(&entry_msg, &key0);
    if (!msg_ptr || *msg_ptr == 0) {
        stat_inc(STAT_MSG_NULL);
        return 0;
    }

    /* 用 tmpbuf 做临时缓冲（BPF verifier 要求 map 指针作为 probe_read_user dst） */
    unsigned char *buf = bpf_map_lookup_elem(&tmpbuf, &key0);
    if (!buf) return 0;

    /* 头 4 字节 = payload 长度 */
    __u32 plen = 0;
    __builtin_memcpy(buf, &plen, 4);

        int step = 0; int data_len = read_iov_data(*msg_ptr, buf + 4, CAPTURE_MAX_DATA, &step);

    if (data_len <= 0) {
        stat_inc(STAT_RB_ERR);  /* 复用: 表示 read_iov_data 失败 */
        return 0;
    }

    plen = (__u32)data_len;
    __builtin_memcpy(buf, &plen, 4);

    /* reserve → memcpy → submit（替代 bpf_ringbuf_output）
     * BPF verifier 要求 __builtin_memcpy 的 size 为常量，统一 reserve+copy 固定大小 */
    void *entry = bpf_ringbuf_reserve(&ringbuf, 4 + CAPTURE_MAX_DATA, 0);
    if (!entry) {
        stat_inc(STAT_RB_ERR);
        return 0;
    }
    bpf_probe_read_kernel(entry, 4 + CAPTURE_MAX_DATA, buf);
    bpf_ringbuf_submit(entry, 0);

    stat_inc(STAT_HIT);
    stat_add(STAT_BYTES, (__u64)data_len);

    /* 清除保存的 msg 指针 */
    unsigned long zero = 0;
    bpf_map_update_elem(&entry_msg, &key0, &zero, 0);

    return 0;
}
/* ──── tracepoint: sys_enter_read ────
 * 优化: CTL_PID=0 表示禁用; 只按 PID 过滤，无 fd 过滤（与 kprobe 一致） */
SEC("tp/syscalls/sys_enter_read")
int tp_sys_enter_read(struct trace_event_raw_sys_enter *ctx)
{
    /* 单次 lookup: CTL_PID 编码为 (master_fd << 32) | pid
     * pid=0 表示禁用; master_fd=0 表示不启用 fd 过滤 */
    __u64 pid_val = ctl_get(CTL_PID);
    __u32 pid = (__u32)(pid_val & 0xFFFFFFFFUL);
    if (!pid) return 0;

    if ((bpf_get_current_pid_tgid() >> 32) != pid) {
        stat_inc(STAT_SKIP_PID);
        return 0;
    }

    /* fd 过滤: master_fd 在 high 32 bits */
    __u32 cap_fd = (__u32)(pid_val >> 32);
    if (cap_fd && (__u32)ctx->args[0] != cap_fd) {
        stat_inc(STAT_SKIP_FD);
        return 0;
    }

    /* 保存 buf 指针 (args[1]) 到 per-CPU array */
    unsigned long buf = ctx->args[1];
    if (!buf) return 0;
    __u32 k0 = 0;
    bpf_map_update_elem(&entry_msg, &k0, &buf, 0);
    return 0;
}

/* ──── tracepoint: sys_exit_read ────
 * 优化: 不重复检查 PID（enter 已过滤），直接读 buf 写 ringbuf */
SEC("tp/syscalls/sys_exit_read")
int tp_sys_exit_read(struct trace_event_raw_sys_exit *ctx)
{
    long ret = ctx->ret;
    if (ret <= 0) return 0;

    /* 从 per-CPU array 取 buf（仅匹配 PID 的 read 会存） */
    __u32 k0 = 0;
    unsigned long *saved_buf = bpf_map_lookup_elem(&entry_msg, &k0);
    if (!saved_buf || *saved_buf == 0)
        return 0;

    /* 限制大小 */
    unsigned long long data_len = (unsigned long long)ret;
    if (data_len > (unsigned long long)CAPTURE_MAX_DATA)
        data_len = (unsigned long long)CAPTURE_MAX_DATA;

    /* tmpbuf 临时缓冲 */
    unsigned char *buf = bpf_map_lookup_elem(&tmpbuf, &k0);
    if (!buf) return 0;

    __u32 plen = (__u32)data_len;
    __builtin_memcpy(buf, &plen, 4);

    if (bpf_probe_read_user(buf + 4, (__u32)data_len,
            (const void *)*saved_buf) != 0)
        return 0;

    /* ringbuf: reserve → copy → submit */
    void *entry = bpf_ringbuf_reserve(&ringbuf, 4 + CAPTURE_MAX_DATA, 0);
    if (!entry) {
        stat_inc(STAT_RB_ERR);
        return 0;
    }
    bpf_probe_read_kernel(entry, 4 + CAPTURE_MAX_DATA, buf);
    bpf_ringbuf_submit(entry, 0);

    stat_inc(STAT_HIT);
    stat_add(STAT_BYTES, (__u64)data_len);
    return 0;
}
char _license[] SEC("license") = "GPL";
