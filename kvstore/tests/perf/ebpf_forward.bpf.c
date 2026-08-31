/*
 * ebpf_forward.bpf.c — 最小化 sockmap sk_msg 转发 BPF 程序
 *
 * 在两个 socket 之间做内核态数据转发：
 *   key 0 → redirect to key 1
 *   key 1 → redirect to key 0
 *
 * 编译: clang -O2 -g -target bpf -D__TARGET_ARCH_x86 -I/usr/include/x86_64-linux-gnu -c ebpf_forward.bpf.c -o ebpf_forward.bpf.o
 */
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>

/* sockmap: 最多 2 个 socket（key 0 和 key 1） */
struct {
    __uint(type, BPF_MAP_TYPE_SOCKMAP);
    __uint(max_entries, 2);
    __type(key, int);
    __type(value, int);
} sock_map SEC(".maps");

/* 统计计数器 */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u64);
} stats_map SEC(".maps");

#define STAT_COUNT     0
#define STAT_BYTES     1
#define STAT_REDIRECT  2
#define STAT_PASS      3
#define STAT_FAIL      4

static __always_inline int other_key(int key) {
    return key == 0 ? 1 : 0;
}

SEC("sk_msg")
int forward_sk_msg(struct sk_msg_md *msg) {
    __u32 zero = 0;
    __u64 *cnt, *bytes, *redirect;

    cnt = bpf_map_lookup_elem(&stats_map, &zero);
    if (cnt) {
        __u32 k_count = STAT_COUNT;
        __u32 k_bytes = STAT_BYTES;
        __u64 *v;

        v = bpf_map_lookup_elem(&stats_map, &k_count);
        if (v) __sync_fetch_and_add(v, 1);

        v = bpf_map_lookup_elem(&stats_map, &k_bytes);
        if (v) __sync_fetch_and_add(v, msg->size);
    }

    /* 仅重定向到 key 1 (backend)，从 key 0 来的数据 → backend egress
     * 返回路径由用户态 helper 线程直接写 client_fd 处理 */
    int rc;

    rc = bpf_msg_redirect_map(msg, &sock_map, 1, 0);
    if (rc == SK_PASS) {
        __u32 k = STAT_REDIRECT;
        __u64 *v = bpf_map_lookup_elem(&stats_map, &k);
        if (v) __sync_fetch_and_add(v, 1);
        return SK_PASS;
    }

    /* 重定向失败（可能是 key 1 尝试重定向到自己）则直接放行 */
    __u32 k = STAT_PASS;
    __u64 *v = bpf_map_lookup_elem(&stats_map, &k);
    if (v) __sync_fetch_and_add(v, 1);
    return SK_PASS;
}

char LICENSE[] SEC("license") = "GPL";
