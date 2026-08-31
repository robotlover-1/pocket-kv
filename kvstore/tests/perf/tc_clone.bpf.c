/*
 * tc_clone.bpf.c — TC ingress BPF: clone TCP packets to VXLAN tunnel
 *
 * Attach: tc filter add dev ens33 ingress bpf obj tc_clone.bpf.o sec tc
 *
 * When a TCP packet arrives on ens33 destined for CLIENT_PORT,
 * clone it and redirect to vxlan100 interface.
 * Original packet proceeds normally to TCP stack (zero overhead).
 *
 * Build:
 *   clang -O2 -g -target bpf -D__TARGET_ARCH_x86 \
 *     -I/usr/include/x86_64-linux-gnu \
 *     -c tc_clone.bpf.c -o tc_clone.bpf.o
 */
#include <stdbool.h>
#include <stddef.h>
#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/in.h>
#include <linux/pkt_cls.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

/* Configuration map: userspace sets target port and vxlan ifindex */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 4);
    __type(key, __u32);
    __type(value, __u32);
} cfg SEC(".maps");

#define CFG_ENABLED 0
#define CFG_PORT    1
#define CFG_VXLAN_IFINDEX 2

/* Counters */
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 4);
    __type(key, __u32);
    __type(value, __u64);
} cnt SEC(".maps");

#define CNT_TOTAL   0
#define CNT_CLONED  1
#define CNT_SKIP    2
#define CNT_ERR     3

static __always_inline int parse_ipv4_tcp(struct __sk_buff *skb, __u16 *dport)
{
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    /* Ethernet header */
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return -1;
    if (eth->h_proto != bpf_htons(ETH_P_IP)) return -1;

    /* IP header */
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return -1;
    if (ip->protocol != IPPROTO_TCP) return -1;

    /* TCP header */
    struct tcphdr *tcp = (void *)(ip + 1);
    if ((void *)(tcp + 1) > data_end) return -1;

    *dport = tcp->dest;
    return 0;
}

SEC("tc")
int tc_ingress_clone(struct __sk_buff *skb)
{
    __u32 zero = 0;
    __u64 *v;

    /* Check enabled */
    __u32 *enabled = bpf_map_lookup_elem(&cfg, &zero);
    if (!enabled || !*enabled) return TC_ACT_OK;

    v = bpf_map_lookup_elem(&cnt, &zero);
    if (v) __sync_fetch_and_add(v, 1); /* CNT_TOTAL */

    /* Parse TCP dst port */
    __u16 dport = 0;
    if (parse_ipv4_tcp(skb, &dport) != 0) {
        __u32 k = CNT_SKIP;
        v = bpf_map_lookup_elem(&cnt, &k);
        if (v) __sync_fetch_and_add(v, 1);
        return TC_ACT_OK;
    }

    /* Filter by port */
    __u32 k_port = CFG_PORT;
    __u32 *target_port = bpf_map_lookup_elem(&cfg, &k_port);
    if (!target_port || dport != bpf_htons((__u16)*target_port)) {
        __u32 k = CNT_SKIP;
        v = bpf_map_lookup_elem(&cnt, &k);
        if (v) __sync_fetch_and_add(v, 1);
        return TC_ACT_OK;
    }

    /* Clone and redirect to VXLAN tunnel */
    __u32 k_ifidx = CFG_VXLAN_IFINDEX;
    __u32 *vxlan_idx = bpf_map_lookup_elem(&cfg, &k_ifidx);
    if (!vxlan_idx || !*vxlan_idx) {
        __u32 k = CNT_ERR;
        v = bpf_map_lookup_elem(&cnt, &k);
        if (v) __sync_fetch_and_add(v, 1);
        return TC_ACT_OK;
    }

    long rc = bpf_clone_redirect(skb, *vxlan_idx, 0 /* egress */);
    if (rc == 0) {
        __u32 k = CNT_CLONED;
        v = bpf_map_lookup_elem(&cnt, &k);
        if (v) __sync_fetch_and_add(v, 1);
    } else {
        __u32 k = CNT_ERR;
        v = bpf_map_lookup_elem(&cnt, &k);
        if (v) __sync_fetch_and_add(v, 1);
    }

    /* Always pass original packet through */
    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";
