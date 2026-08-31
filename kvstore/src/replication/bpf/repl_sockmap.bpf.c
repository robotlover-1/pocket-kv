#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define KVS_EBPF_STAT_SK_MSG_COUNT 0
#define KVS_EBPF_STAT_SK_MSG_BYTES 1
#define KVS_EBPF_STAT_SK_MSG_PASS 2
#define KVS_EBPF_STAT_SK_MSG_DROP 3
#define KVS_EBPF_STAT_REDIRECT_ATTEMPTS 4
#define KVS_EBPF_STAT_REDIRECT_SUCCESS 5
#define KVS_EBPF_STAT_REDIRECT_FAILURES 6
#define KVS_EBPF_STAT_ROLE_UNKNOWN 7
#define KVS_EBPF_STAT_ROLE_MASTER 8
#define KVS_EBPF_STAT_ROLE_SLAVE 9

#define KVS_EBPF_CTL_REDIRECT_ENABLED 0
#define KVS_EBPF_CTL_REDIRECT_KEY 1
#define KVS_EBPF_CTL_MASTER_PORT 2
#define KVS_EBPF_CTL_LOCAL_PORT 3
#define KVS_EBPF_CTL_REDIRECT_INGRESS 4

#define KVS_EBPF_ROLE_MASTER_SIDE 1
#define KVS_EBPF_ROLE_SLAVE_SIDE 2
#define KVS_EBPF_SOCK_KEY_MASTER_SIDE 0

struct {
    __uint(type, BPF_MAP_TYPE_SOCKMAP);
    __uint(max_entries, 4096);
    __type(key, int);
    __type(value, int);
} sock_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 10);
    __type(key, __u32);
    __type(value, __u64);
} stats_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 5);
    __type(key, __u32);
    __type(value, __u32);
} control_map SEC(".maps");

static __always_inline void add_stat(__u32 key, __u64 delta) {
    __u64 *value = bpf_map_lookup_elem(&stats_map, &key);
    if (value) *value += delta;
}

static __always_inline __u32 control_value(__u32 key) {
    __u32 *value = bpf_map_lookup_elem(&control_map, &key);
    return value ? *value : 0;
}

static __always_inline int remote_port_matches(__u32 remote_port, __u32 port) {
    __u32 remote_host = bpf_ntohl(remote_port);
    if (remote_host == port) return 1;
    if ((remote_host >> 16) == port) return 1;
    if ((remote_port >> 16) == bpf_htons(port)) return 1;
    return 0;
}

static __always_inline __u32 current_role(struct sk_msg_md *msg) {
    __u32 master_port = control_value(KVS_EBPF_CTL_MASTER_PORT);
    if (!master_port) return 0;
    if (msg->local_port == master_port) return KVS_EBPF_ROLE_MASTER_SIDE;
    if (remote_port_matches(msg->remote_port, master_port)) return KVS_EBPF_ROLE_SLAVE_SIDE;
    return 0;
}

SEC("sk_msg")
int kvstore_repl_sk_msg(struct sk_msg_md *msg) {
    __u32 redirect_enabled;
    __u32 redirect_key;
    __u32 role;
    int rc;

    add_stat(KVS_EBPF_STAT_SK_MSG_COUNT, 1);
    add_stat(KVS_EBPF_STAT_SK_MSG_BYTES, msg->size);

    role = current_role(msg);
    if (role == KVS_EBPF_ROLE_MASTER_SIDE) add_stat(KVS_EBPF_STAT_ROLE_MASTER, 1);
    else if (role == KVS_EBPF_ROLE_SLAVE_SIDE) add_stat(KVS_EBPF_STAT_ROLE_SLAVE, 1);
    else add_stat(KVS_EBPF_STAT_ROLE_UNKNOWN, 1);

    redirect_enabled = control_value(KVS_EBPF_CTL_REDIRECT_ENABLED);
    if (!redirect_enabled || role != KVS_EBPF_ROLE_MASTER_SIDE) {
        add_stat(KVS_EBPF_STAT_SK_MSG_PASS, 1);
        return SK_PASS;
    }

    redirect_key = control_value(KVS_EBPF_CTL_REDIRECT_KEY);
    add_stat(KVS_EBPF_STAT_REDIRECT_ATTEMPTS, 1);

    if (control_value(KVS_EBPF_CTL_REDIRECT_INGRESS)) {
        /* Ingress redirect: data goes to target socket's receive queue (local only) */
        rc = bpf_msg_redirect_map(msg, &sock_map, redirect_key, BPF_F_INGRESS);
    } else {
        /* Egress redirect: data goes to target socket's send queue (cross-machine) */
        rc = bpf_msg_redirect_map(msg, &sock_map, redirect_key, 0);
    }

    if (rc == SK_PASS) {
        add_stat(KVS_EBPF_STAT_REDIRECT_SUCCESS, 1);
        return SK_PASS;
    }
    add_stat(KVS_EBPF_STAT_REDIRECT_FAILURES, 1);
    add_stat(KVS_EBPF_STAT_SK_MSG_PASS, 1);
    return SK_PASS;
}

char LICENSE[] SEC("license") = "GPL";
