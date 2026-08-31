/*
 * test_kprobe_echo_qps.c — kprobe capture vs 用户态 echo QPS 对比
 *
 * 用法:
 *   sudo ./test_kprobe_echo_qps --mode ebpf --payload 64 --count 10000
 *   sudo ./test_kprobe_echo_qps --mode userspace --payload 64 --count 10000
 *   sudo ./test_kprobe_echo_qps --mode both --payload 64 --count 10000
 *
 * eBPF 模式工作原理：
 *   1. 加载 kprobe (tcp_recvmsg)，过滤 echo server 的 PID
 *   2. accept 客户端连接，设置大 SO_RCVBUF
 *   3. 不调用 read()，数据由 kprobe 截获进 ringbuf
 *   4. ringbuf 消费者线程读数据 → 直接 write 回 socket 作为 echo
 *
 * 用户态模式：传统 accept → read → write
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common.h"

#define ECHO_PORT      15700
#define BPF_OBJ_FILE   "./kprobe_capture.bpf.o"

static const char *g_bpf_obj = BPF_OBJ_FILE;
static int g_echo_port   = ECHO_PORT;
static int g_payload_size = 64;
static int g_req_count    = 10000;

/* ========== ringbuf 消费者线程 ========== */
typedef struct {
    struct bpf_object *obj;
    int client_fd;
    volatile int running;
    volatile int ready;
    int echo_count;
    int rb_err;
} ringbuf_consumer_t;

static int handle_ringbuf_event(void *ctx, void *data, size_t len) {
    ringbuf_consumer_t *rc = (ringbuf_consumer_t *)ctx;
    (void)data;
    if (len < 4) return 0;
    /* 只计数，不写 socket（kprobe 是观察者，echo server 负责响应） */
    rc->echo_count++;
    return 0;
}

static void *ringbuf_consumer_thread(void *arg) {
    ringbuf_consumer_t *rc = (ringbuf_consumer_t *)arg;

    struct bpf_map *rb_map = bpf_object__find_map_by_name(rc->obj, "ringbuf");
    if (!rb_map) {
        fprintf(stderr, "[ringbuf] ringbuf map not found\n");
        return NULL;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(rb_map),
                                              handle_ringbuf_event, rc, NULL);
    if (!rb) {
        fprintf(stderr, "[ringbuf] ring_buffer__new failed\n");
        return NULL;
    }

    rc->ready = 1;

    while (rc->running) {
        int n = ring_buffer__poll(rb, 100); /* 100ms timeout */
        if (n < 0) break;
    }

    ring_buffer__free(rb);
    fprintf(stderr, "[ringbuf] exiting, echo=%d err=%d\n", rc->echo_count, rc->rb_err);
    return NULL;
}

/* ========== eBPF 模式 echo server ========== */
static void *run_ebpf_mode(void *arg) {
    (void)arg;
    struct rlimit rl = {RLIM_INFINITY, RLIM_INFINITY};
    setrlimit(RLIMIT_MEMLOCK, &rl);

    /* 加载 BPF */
    struct bpf_object *obj = bpf_object__open(g_bpf_obj);
    if (!obj) {
        fprintf(stderr, "ebpf: bpf_object__open failed\n");
        return NULL;
    }
    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "ebpf: bpf_object__load failed\n");
        bpf_object__close(obj);
        return NULL;
    }

    /* 设置 PID filter */
    struct bpf_map *ctl = bpf_object__find_map_by_name(obj, "ctl");
    if (!ctl) {
        fprintf(stderr, "ebpf: ctl map not found\n");
        bpf_object__close(obj);
        return NULL;
    }
    __u32 pid_key = 0, port_key = 2;
    __u64 val_one = 1;
    __u64 val_pid = (__u64)getpid();
    __u64 val_port = (__u64)g_echo_port;
    bpf_map_update_elem(bpf_map__fd(ctl), &pid_key, &val_one, BPF_ANY);   /* enabled=1 */
    pid_key = 1;
    bpf_map_update_elem(bpf_map__fd(ctl), &pid_key, &val_pid, BPF_ANY);   /* PID */
    bpf_map_update_elem(bpf_map__fd(ctl), &port_key, &val_port, BPF_ANY); /* port */

    /* Attach kprobes */
    struct bpf_program *prog;
    struct bpf_link *link_entry = NULL, *link_ret = NULL;

    prog = bpf_object__find_program_by_name(obj, "kp_recv_entry");
    if (prog) {
        link_entry = bpf_program__attach_kprobe(prog, false, "tcp_recvmsg");
        if (libbpf_get_error(link_entry)) {
            fprintf(stderr, "ebpf: attach kp_recv_entry failed: %ld\n",
                    libbpf_get_error(link_entry));
            link_entry = NULL;
        }
    }
    if (!link_entry)
        fprintf(stderr, "ebpf: kprobe entry NOT attached, BPF capture disabled\n");

    prog = bpf_object__find_program_by_name(obj, "kp_recv_return");
    if (prog) {
        link_ret = bpf_program__attach_kprobe(prog, true, "tcp_recvmsg");
        if (libbpf_get_error(link_ret)) {
            fprintf(stderr, "ebpf: attach kp_recv_return failed: %ld\n",
                    libbpf_get_error(link_ret));
            link_ret = NULL;
        }
    }
    if (!link_ret)
        fprintf(stderr, "ebpf: kretprobe return NOT attached, BPF capture disabled\n");

    /* 监听 */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                               .sin_port = htons(g_echo_port)};
    bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(listen_fd, 1);

    printf("[ebpf mode] BPF loaded, listening on %d, waiting for client...\n", g_echo_port);

    int client_fd = accept(listen_fd, NULL, NULL);
    set_nodelay(client_fd);

    /* 设置大接收缓冲区（避免 TCP window 关闭） */
    int rcvbuf = 4 * 1024 * 1024; /* 4MB */
    setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    /* 启动 ringbuf 消费者 */
    ringbuf_consumer_t rc = {.obj = obj, .client_fd = client_fd, .running = 1};
    pthread_t thr;
    pthread_create(&thr, NULL, ringbuf_consumer_thread, &rc);
    while (!__atomic_load_n(&rc.ready, __ATOMIC_ACQUIRE))
        usleep(1000);

    printf("[ebpf mode] ringbuf consumer ready, client connected\n");

    /* eBPF 模式仍做正常 read/write echo：
     * kprobe 截获发生在 read() 过程中，ringbuf 消费者只计数 */
    char buf[65536];
    int echo_svr_count = 0;
    while (1) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n <= 0) break;
        echo_svr_count++;
        if (write_full(client_fd, buf, (size_t)n) < 0) break;
    }
    fprintf(stderr, "[ebpf echo svr] processed %d msgs, ringbuf captured %d\n",
            echo_svr_count, rc.echo_count);

    /* cleanup */
    rc.running = 0;
    pthread_join(thr, NULL);

    close(client_fd);
    close(listen_fd);
    if (link_entry) bpf_link__destroy(link_entry);
    if (link_ret) bpf_link__destroy(link_ret);
    bpf_object__close(obj);

    return NULL;
}

/* ========== 用户态模式 echo server ========== */
static void *userspace_echo_thread_func(void *arg) {
    int listen_fd = (int)(intptr_t)arg;
    int client_fd = accept(listen_fd, NULL, NULL);
    if (client_fd < 0) {
        perror("userspace: accept");
        return NULL;
    }
    set_nodelay(client_fd);

    int count = 0;
    char buf[65536];
    while (1) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n <= 0) break;
        count++;
        if (write_full(client_fd, buf, (size_t)n) < 0) break;
    }
    fprintf(stderr, "[userspace echo] processed %d msgs\n", count);
    close(client_fd);
    return NULL;
}

/* ========== QPS 客户端 ========== */
typedef struct {
    double qps;
    double avg_lat_us;
    double p50_lat_us;
    double p99_lat_us;
    double min_lat_us;
    double max_lat_us;
} qps_result_t;

static qps_result_t run_qps_client(void) {
    qps_result_t result;
    memset(&result, 0, sizeof(result));

    double *latencies = calloc((size_t)g_req_count, sizeof(double));
    if (!latencies) return result;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    set_nodelay(fd);

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                               .sin_port = htons(g_echo_port)};

    for (int retry = 0; retry < 50; retry++) {
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) break;
        if (errno == ECONNREFUSED) { usleep(50000); continue; }
        perror("client: connect"); close(fd); free(latencies); return result;
    }

    char *req = calloc(1, (size_t)g_payload_size);
    char *rsp = calloc(1, (size_t)g_payload_size);
    if (!req || !rsp) { close(fd); free(latencies); free(req); free(rsp); return result; }
    memset(req, 'A', (size_t)g_payload_size);

    /* 预热 */
    int warmup = g_req_count / 10;
    if (warmup < 100) warmup = 100;
    if (warmup > 1000) warmup = 1000;
    for (int i = 0; i < warmup; i++) {
        if (write_full(fd, req, (size_t)g_payload_size) < 0) goto done;
        if (read_full(fd, rsp, (size_t)g_payload_size) != (ssize_t)g_payload_size) goto done;
    }

    /* 正式测试 */
    double elapsed = 1.0;
    double t0 = now_us();
    for (int i = 0; i < g_req_count; i++) {
        double lt0 = now_us();
        if (write_full(fd, req, (size_t)g_payload_size) < 0) {
            fprintf(stderr, "client: write failed at %d\n", i);
            g_req_count = i; goto calc;
        }
        if (read_full(fd, rsp, (size_t)g_payload_size) != (ssize_t)g_payload_size) {
            fprintf(stderr, "client: read failed/short at %d\n", i);
            g_req_count = i; goto calc;
        }
        latencies[i] = now_us() - lt0;
    }
    elapsed = now_us() - t0;
    if (elapsed <= 0.0) elapsed = 1.0;

calc:
    qsort(latencies, (size_t)g_req_count, sizeof(double), cmp_double);
    result.qps = (double)g_req_count / elapsed * 1000000.0;
    result.min_lat_us = latencies[0];
    result.max_lat_us = latencies[g_req_count - 1];
    result.p50_lat_us = latencies[g_req_count / 2];
    result.p99_lat_us = latencies[(int)(g_req_count * 0.99)];

    double sum = 0;
    for (int i = 0; i < g_req_count; i++) sum += latencies[i];
    result.avg_lat_us = sum / g_req_count;

done:
    close(fd);
    free(req); free(rsp); free(latencies);
    return result;
}

static void print_result(const char *mode, const qps_result_t *r) {
    printf("%-14s  %-6d  %10.0f  %8s  %8s  %8s  %8s  %8s\n",
           mode, g_payload_size, r->qps,
           latency_str(r->avg_lat_us), latency_str(r->p50_lat_us),
           latency_str(r->p99_lat_us), latency_str(r->min_lat_us),
           latency_str(r->max_lat_us));
}

static void print_header(void) {
    printf("%-14s  %-6s  %10s  %8s  %8s  %8s  %8s  %8s\n",
           "mode", "payload", "qps", "avg", "p50", "p99", "min", "max");
    printf("--------------  ------  ----------  --------  --------  --------  --------  --------\n");
}

/* ========== Main ========== */
typedef enum { MODE_EBPF, MODE_USERSPACE, MODE_BOTH } test_mode_t;

static void usage(const char *prog) {
    fprintf(stderr,
        "用法: sudo %s [options]\n"
        "  --mode, -m    ebpf | userspace | both (默认: both)\n"
        "  --payload, -p 负载大小 (默认: 64)\n"
        "  --count, -c   请求数 (默认: 10000)\n"
        "  --port         监听端口 (默认: 15700)\n"
        "  --bpf-obj, -b BPF 对象路径 (默认: ./kprobe_capture.bpf.o)\n"
        "  --help, -h    帮助\n", prog);
}

int main(int argc, char **argv) {
    test_mode_t mode = MODE_BOTH;

    struct option long_opts[] = {
        {"mode", required_argument, 0, 'm'},
        {"payload", required_argument, 0, 'p'},
        {"count", required_argument, 0, 'c'},
        {"port", required_argument, 0, 1000},
        {"bpf-obj", required_argument, 0, 'b'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "m:p:c:b:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'm':
            if (strcmp(optarg, "ebpf") == 0) mode = MODE_EBPF;
            else if (strcmp(optarg, "userspace") == 0) mode = MODE_USERSPACE;
            else if (strcmp(optarg, "both") == 0) mode = MODE_BOTH;
            else { fprintf(stderr, "无效 mode\n"); return 1; }
            break;
        case 'p': g_payload_size = atoi(optarg); break;
        case 'c': g_req_count = atoi(optarg); break;
        case 1000: g_echo_port = atoi(optarg); break;
        case 'b': g_bpf_obj = optarg; break;
        case 'h': usage(argv[0]); return 0;
        default: usage(argv[0]); return 1;
        }
    }

    printf("=== kprobe capture vs userspace echo QPS 对比 ===\n");
    printf("payload=%d bytes, count=%d\n\n", g_payload_size, g_req_count);

    print_header();

    /* --- 用户态模式 --- */
    if (mode == MODE_USERSPACE || mode == MODE_BOTH) {
        printf("[userspace] starting echo server...\n");

        /* 在子线程启动 echo server，主线程做客户端 */
        pthread_t echo_tid;
        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        struct sockaddr_in addr = {.sin_family = AF_INET,
                                   .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                                   .sin_port = htons(g_echo_port)};
        bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr));
        listen(listen_fd, 1);

        pthread_create(&echo_tid, NULL, userspace_echo_thread_func, (void *)(intptr_t)listen_fd);
        usleep(100000);

        qps_result_t r = run_qps_client();
        print_result("userspace", &r);

        shutdown(listen_fd, SHUT_RDWR);
        pthread_join(echo_tid, NULL);
        close(listen_fd);
        printf("\n");
    }

    /* --- eBPF 模式 --- */
    if (mode == MODE_EBPF || mode == MODE_BOTH) {
        printf("[ebpf] loading kprobe BPF...\n");

        /* 在子线程启动 eBPF echo server，主线程做客户端 */
        pthread_t ebpf_tid;
        pthread_create(&ebpf_tid, NULL, run_ebpf_mode, NULL);
        usleep(500000); /* 等 BPF 加载 + ringbuf 就绪 */

        qps_result_t r = run_qps_client();
        print_result("kprobe_capture", &r);

        pthread_join(ebpf_tid, NULL);
        printf("\n");
    }

    printf("完成。\n");
    return 0;
}
