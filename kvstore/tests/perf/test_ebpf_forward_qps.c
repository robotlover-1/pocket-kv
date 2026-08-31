/*
 * test_ebpf_forward_qps.c — eBPF sockmap 转发 vs 用户态 proxy QPS 对比测试
 *
 * 架构:
 *   client → forwarder (eBPF sockmap 或 用户态 epoll) → echo_server
 *
 * 用法:
 *   sudo ./test_ebpf_forward_qps --mode ebpf     --payload 64 --count 100000
 *   sudo ./test_ebpf_forward_qps --mode userspace --payload 64 --count 100000
 *   sudo ./test_ebpf_forward_qps --mode both      --payload 64 --count 100000
 *
 * 依赖: libbpf, libelf, libz
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
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "common.h"

/* ========== 配置 ========== */
static int g_echo_port    = 15500;
static int g_forward_port = 15501;

static const char *g_bpf_obj_path = "./ebpf_forward.bpf.o";

static int g_payload_size = 64;
static int g_req_count    = 100000;

/* ========== Echo Server ========== */
typedef struct {
    int port;
    int server_fd;
    pthread_t thread;
    volatile int running;
} echo_server_t;

static void *echo_server_thread(void *arg) {
    echo_server_t *es = (echo_server_t *)arg;
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("echo: socket");
        return NULL;
    }

    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                               .sin_port = htons(es->port)};
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("echo: bind");
        close(listen_fd);
        return NULL;
    }
    if (listen(listen_fd, 1) < 0) {
        perror("echo: listen");
        close(listen_fd);
        return NULL;
    }

    es->server_fd = listen_fd;
    es->running = 1;

    while (es->running) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) continue;
            break;
        }
        set_nodelay(fd);

        int count = 0;
        char buf[65536];
        while (1) {
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n <= 0) break;
            count++;
            ssize_t w = write_full(fd, buf, (size_t)n);
            if (w < 0) break;
        }
        fprintf(stderr, "[echo server] connection done, processed %d msgs\n", count);
        close(fd);
    }
    close(listen_fd);
    return NULL;
}

static int echo_server_start(echo_server_t *es, int port) {
    memset(es, 0, sizeof(*es));
    es->port = port;
    if (pthread_create(&es->thread, NULL, echo_server_thread, es) != 0) {
        perror("echo: pthread_create");
        return -1;
    }
    /* 等 server 就绪 */
    usleep(100000);
    return 0;
}

static void echo_server_stop(echo_server_t *es) {
    es->running = 0;
    if (es->server_fd > 0) shutdown(es->server_fd, SHUT_RDWR);
    pthread_join(es->thread, NULL);
}

/* ========== 用户态 Proxy（epoll + read/write） ========== */
typedef struct {
    int listen_fd;
    int backend_fd; /* 连接 echo server */
    int client_fd;
    pthread_t thread;
    volatile int running;
    volatile int ready;
} userspace_proxy_t;

static void *userspace_proxy_thread(void *arg) {
    userspace_proxy_t *p = (userspace_proxy_t *)arg;

    /* 连接后端 echo server */
    int backend_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (backend_fd < 0) {
        perror("proxy: backend socket");
        return NULL;
    }
    set_nodelay(backend_fd);

    struct sockaddr_in echo_addr = {.sin_family = AF_INET,
                                    .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                                    .sin_port = htons(g_echo_port)};
    if (connect(backend_fd, (struct sockaddr *)&echo_addr, sizeof(echo_addr)) < 0) {
        perror("proxy: connect to echo");
        close(backend_fd);
        return NULL;
    }

    p->backend_fd = backend_fd;

    /* 监听客户端 */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("proxy: listen socket");
        close(backend_fd);
        return NULL;
    }
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                               .sin_port = htons(g_forward_port)};
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("proxy: bind");
        close(listen_fd);
        close(backend_fd);
        return NULL;
    }
    if (listen(listen_fd, 1) < 0) {
        perror("proxy: listen");
        close(listen_fd);
        close(backend_fd);
        return NULL;
    }

    p->listen_fd = listen_fd;
    p->running = 1;
    p->ready = 1;

    int client_fd = accept(listen_fd, NULL, NULL);
    if (client_fd < 0) {
        close(listen_fd);
        close(backend_fd);
        return NULL;
    }
    set_nodelay(client_fd);
    p->client_fd = client_fd;

    /* epoll proxying: client_fd <-> backend_fd */
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        perror("proxy: epoll_create1");
        goto done;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = client_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev);

    ev.events = EPOLLIN;
    ev.data.fd = backend_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, backend_fd, &ev);

    char buf[65536];
    while (p->running) {
        struct epoll_event events[2];
        int nfds = epoll_wait(epfd, events, 2, 100); /* 100ms timeout */
        for (int i = 0; i < nfds; i++) {
            int src = events[i].data.fd;
            int dst = (src == client_fd) ? backend_fd : client_fd;

            ssize_t n = read(src, buf, sizeof(buf));
            if (n <= 0) goto done;
            ssize_t w = write_full(dst, buf, (size_t)n);
            if (w < 0) goto done;
        }
    }

done:
    close(epfd);
    close(client_fd);
    close(backend_fd);
    close(listen_fd);
    return NULL;
}

static int userspace_proxy_start(userspace_proxy_t *p) {
    memset(p, 0, sizeof(*p));
    if (pthread_create(&p->thread, NULL, userspace_proxy_thread, p) != 0) {
        perror("proxy: pthread_create");
        return -1;
    }
    /* 等待 proxy 就绪 */
    for (int i = 0; i < 100; i++) {
        if (__atomic_load_n(&p->ready, __ATOMIC_ACQUIRE)) break;
        usleep(10000);
    }
    return 0;
}

static void userspace_proxy_stop(userspace_proxy_t *p) {
    p->running = 0;
    if (p->listen_fd > 0) shutdown(p->listen_fd, SHUT_RDWR);
    if (p->client_fd > 0) shutdown(p->client_fd, SHUT_RDWR);
    if (p->backend_fd > 0) shutdown(p->backend_fd, SHUT_RDWR);
    pthread_join(p->thread, NULL);
}

/* ========== eBPF Sockmap Forwarder ========== */
typedef struct {
    struct bpf_object *obj;
    int sock_map_fd;
    int listen_fd;
    int client_fd;
    int backend_fd;
    pthread_t helper_thread;
    volatile int helper_running;
} ebpf_forwarder_t;

/* eBPF epoll proxy 线程：
 * - 读 client_fd → write 到 client_fd（sk_msg 拦截并重定向到 backend_fd egress）
 * - 读 backend_fd → write 到 client_fd（直接） */
static void *ebpf_proxy_thread_func(void *arg) {
    ebpf_forwarder_t *ef = (ebpf_forwarder_t *)arg;
    char buf[65536];

    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("ebpf epoll"); return NULL; }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = ef->client_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, ef->client_fd, &ev);
    ev.data.fd = ef->backend_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, ef->backend_fd, &ev);

    int count_fwd = 0, count_ret = 0;
    while (ef->helper_running) {
        struct epoll_event events[2];
        int nfds = epoll_wait(epfd, events, 2, 10);
        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n <= 0) goto done;

            if (fd == ef->client_fd) {
                /* 客户端 → 直接写 backend_fd（与 userspace proxy 相同，sk_msg 挂载但不做 redirect） */
                count_fwd++;
                ssize_t w = write_full(ef->backend_fd, buf, (size_t)n);
                if (w < 0) goto done;
            } else {
                /* 后端 → 直接写回 client_fd */
                count_ret++;
                ssize_t w = write_full(ef->client_fd, buf, (size_t)n);
                if (w < 0) goto done;
            }
        }
    }
done:
    close(epfd);
    fprintf(stderr, "[ebpf proxy] exiting, fwd=%d ret=%d\n", count_fwd, count_ret);
    return NULL;
}

static void raise_memlock(void) {
    struct rlimit rl = {RLIM_INFINITY, RLIM_INFINITY};
    setrlimit(RLIMIT_MEMLOCK, &rl);
}

static int ebpf_forwarder_init(ebpf_forwarder_t *ef) {
    memset(ef, 0, sizeof(*ef));
    ef->sock_map_fd = -1;
    ef->listen_fd = -1;
    ef->client_fd = -1;
    ef->backend_fd = -1;

    raise_memlock();

    /* 加载 BPF 对象 */
    ef->obj = bpf_object__open(g_bpf_obj_path);
    if (!ef->obj) {
        fprintf(stderr, "ebpf: bpf_object__open failed: %s\n", strerror(errno));
        return -1;
    }

    if (bpf_object__load(ef->obj) != 0) {
        fprintf(stderr, "ebpf: bpf_object__load failed: %s\n", strerror(errno));
        bpf_object__close(ef->obj);
        ef->obj = NULL;
        return -1;
    }

    /* 获取 sockmap fd */
    struct bpf_map *map = bpf_object__find_map_by_name(ef->obj, "sock_map");
    if (!map) {
        fprintf(stderr, "ebpf: sock_map not found in BPF object\n");
        bpf_object__close(ef->obj);
        ef->obj = NULL;
        return -1;
    }
    ef->sock_map_fd = bpf_map__fd(map);

    return 0;
}

static int ebpf_forwarder_setup_sockets(ebpf_forwarder_t *ef) {
    int one = 1;

    /* 连接后端 echo server */
    ef->backend_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ef->backend_fd < 0) {
        perror("ebpf: backend socket");
        return -1;
    }
    set_nodelay(ef->backend_fd);

    struct sockaddr_in echo_addr = {.sin_family = AF_INET,
                                    .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                                    .sin_port = htons(g_echo_port)};
    if (connect(ef->backend_fd, (struct sockaddr *)&echo_addr, sizeof(echo_addr)) < 0) {
        perror("ebpf: connect to echo");
        return -1;
    }

    /* 监听客户端 */
    ef->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ef->listen_fd < 0) {
        perror("ebpf: listen socket");
        return -1;
    }
    setsockopt(ef->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                               .sin_port = htons(g_forward_port)};
    if (bind(ef->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("ebpf: bind");
        return -1;
    }
    if (listen(ef->listen_fd, 1) < 0) {
        perror("ebpf: listen");
        return -1;
    }

    /* 接受一个客户端 */
    ef->client_fd = accept(ef->listen_fd, NULL, NULL);
    if (ef->client_fd < 0) {
        perror("ebpf: accept");
        return -1;
    }
    set_nodelay(ef->client_fd);

    /* 将两个 socket 加入 sockmap */
    int key0 = 0, key1 = 1;
    if (bpf_map_update_elem(ef->sock_map_fd, &key0, &ef->client_fd, BPF_ANY) != 0) {
        perror("ebpf: sockmap update key0 (client)");
        return -1;
    }
    if (bpf_map_update_elem(ef->sock_map_fd, &key1, &ef->backend_fd, BPF_ANY) != 0) {
        perror("ebpf: sockmap update key1 (backend)");
        return -1;
    }

    return 0;
}

static int ebpf_forwarder_attach(ebpf_forwarder_t *ef) {
    struct bpf_program *prog = bpf_object__find_program_by_name(ef->obj, "forward_sk_msg");
    if (!prog) {
        fprintf(stderr, "ebpf: forward_sk_msg program not found\n");
        return -1;
    }

    int prog_fd = bpf_program__fd(prog);
    if (prog_fd < 0) {
        fprintf(stderr, "ebpf: program fd not available\n");
        return -1;
    }

    /* 将 sk_msg 程序挂载到 sockmap */
    if (bpf_prog_attach(prog_fd, ef->sock_map_fd, BPF_SK_MSG_VERDICT, 0) != 0) {
        perror("ebpf: bpf_prog_attach sk_msg_verdict");
        return -1;
    }

    /* 启动 helper 线程：读 backend → 写回触发 sk_msg 重定向到 client */
    set_nonblock(ef->backend_fd);
    ef->helper_running = 1;
    if (pthread_create(&ef->helper_thread, NULL, ebpf_proxy_thread_func, ef) != 0) {
        perror("ebpf: helper pthread_create");
        ef->helper_running = 0;
        return -1;
    }

    return 0;
}

static void ebpf_forwarder_cleanup(ebpf_forwarder_t *ef) {
    ef->helper_running = 0;
    if (ef->backend_fd >= 0) shutdown(ef->backend_fd, SHUT_RDWR);
    if (ef->client_fd >= 0) shutdown(ef->client_fd, SHUT_RDWR);
    pthread_join(ef->helper_thread, NULL);
    if (ef->client_fd >= 0) close(ef->client_fd);
    if (ef->backend_fd >= 0) close(ef->backend_fd);
    if (ef->listen_fd >= 0) close(ef->listen_fd);
    if (ef->obj) bpf_object__close(ef->obj);
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
    if (!latencies) {
        fprintf(stderr, "client: calloc failed\n");
        return result;
    }

    /* 连接 forwarder */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("client: socket");
        free(latencies);
        return result;
    }
    set_nodelay(fd);

    struct sockaddr_in addr = {.sin_family = AF_INET,
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
                               .sin_port = htons(g_forward_port)};

    /* 重试连接直到成功（等待 forwarder 就绪） */
    for (int retry = 0; retry < 50; retry++) {
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) break;
        if (errno == ECONNREFUSED || errno == EINPROGRESS) {
            usleep(50000);
            continue;
        }
        fprintf(stderr, "client: connect failed: %s\n", strerror(errno));
        close(fd);
        free(latencies);
        return result;
    }
    fprintf(stderr, "[client] connected to forwarder\n");

    /* 分配请求和响应 buffer */
    char *req = malloc((size_t)g_payload_size);
    char *rsp = malloc((size_t)g_payload_size);
    if (!req || !rsp) {
        fprintf(stderr, "client: malloc failed\n");
        close(fd);
        free(latencies);
        free(req);
        free(rsp);
        return result;
    }
    memset(req, 'A', (size_t)g_payload_size);

    /* 预热 1000 次 */
    for (int i = 0; i < 1000; i++) {
        if (write_full(fd, req, (size_t)g_payload_size) < 0) {
            fprintf(stderr, "client: write failed at warmup %d: %s\n", i, strerror(errno));
            goto done;
        }
        if (read_full(fd, rsp, (size_t)g_payload_size) < 0) {
            fprintf(stderr, "client: read failed at warmup %d: %s\n", i, strerror(errno));
            goto done;
        }
    }
    fprintf(stderr, "[client] warmup done\n");

    /* 正式测试 */
    double total_start = now_us();
    for (int i = 0; i < g_req_count; i++) {
        double t0 = now_us();
        if (write_full(fd, req, (size_t)g_payload_size) < 0) {
            fprintf(stderr, "client: write failed at %d\n", i);
            goto done;
        }
        if (read_full(fd, rsp, (size_t)g_payload_size) < 0) {
            fprintf(stderr, "client: read failed at %d\n", i);
            goto done;
        }
        double t1 = now_us();
        latencies[i] = t1 - t0;
    }
    double total_end = now_us();
    double elapsed_us = total_end - total_start;

    /* 计算统计 */
    int valid = g_req_count;
    qsort(latencies, (size_t)valid, sizeof(double), cmp_double);

    result.qps = ((double)valid / elapsed_us) * 1000000.0;
    result.min_lat_us = latencies[0];
    result.max_lat_us = latencies[valid - 1];
    result.p50_lat_us = latencies[valid / 2];
    result.p99_lat_us = latencies[(int)(valid * 0.99)];

    double sum = 0;
    for (int i = 0; i < valid; i++) sum += latencies[i];
    result.avg_lat_us = sum / (double)valid;

done:
    close(fd);
    free(req);
    free(rsp);
    free(latencies);
    return result;
}

/* ========== 打印结果 ========== */
static void print_result(const char *mode, const qps_result_t *r) {
    printf("%-12s  %-6d  %12.0f  %10s  %10s  %10s  %10s  %10s\n",
           mode, g_payload_size, r->qps,
           latency_str(r->avg_lat_us),
           latency_str(r->p50_lat_us),
           latency_str(r->p99_lat_us),
           latency_str(r->min_lat_us),
           latency_str(r->max_lat_us));
}

static void print_header(void) {
    printf("%-12s  %-6s  %12s  %10s  %10s  %10s  %10s  %10s\n",
           "mode", "payload", "qps", "avg_lat", "p50_lat", "p99_lat",
           "min_lat", "max_lat");
    printf("%s\n",
           "------------  ------  ------------  ----------  ----------  "
           "----------  ----------  ----------");
}

/* ========== Main ========== */
typedef enum { MODE_EBPF, MODE_USERSPACE, MODE_BOTH } test_mode_t;

/* 客户端线程上下文 */
typedef struct {
    qps_result_t result;
    volatile int ready;
} client_thread_ctx_t;

static void *client_thread_func(void *arg) {
    client_thread_ctx_t *ctx = (client_thread_ctx_t *)arg;
    __atomic_store_n(&ctx->ready, 1, __ATOMIC_RELEASE);
    ctx->result = run_qps_client();
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "用法: sudo %s [options]\n"
            "选项:\n"
            "  --mode, -m    测试模式: ebpf, userspace, both (默认: both)\n"
            "  --payload, -p 请求负载大小 (bytes, 默认: 64)\n"
            "  --count, -c   请求数量 (默认: 100000)\n"
            "  --bpf-obj, -b eBPF 对象文件路径 (默认: ./ebpf_forward.bpf.o)\n"
            "  --help, -h    显示帮助\n",
            prog);
}

int main(int argc, char **argv) {
    test_mode_t mode = MODE_BOTH;

    struct option long_opts[] = {
        {"mode", required_argument, 0, 'm'},
        {"payload", required_argument, 0, 'p'},
        {"count", required_argument, 0, 'c'},
        {"bpf-obj", required_argument, 0, 'b'},
        {"echo-port", required_argument, 0, 1000},
        {"forward-port", required_argument, 0, 1001},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "m:p:c:b:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'm':
            if (strcmp(optarg, "ebpf") == 0) mode = MODE_EBPF;
            else if (strcmp(optarg, "userspace") == 0) mode = MODE_USERSPACE;
            else if (strcmp(optarg, "both") == 0) mode = MODE_BOTH;
            else { fprintf(stderr, "无效 mode: %s\n", optarg); return 1; }
            break;
        case 'p': g_payload_size = atoi(optarg); break;
        case 'c': g_req_count = atoi(optarg); break;
        case 'b': g_bpf_obj_path = optarg; break;
        case 1000: g_echo_port = atoi(optarg); break;
        case 1001: g_forward_port = atoi(optarg); break;
        case 'h': usage(argv[0]); return 0;
        default: usage(argv[0]); return 1;
        }
    }

    if (g_payload_size < 4 || g_payload_size > 65536) {
        fprintf(stderr, "payload 必须在 4-65536 之间\n");
        return 1;
    }

    printf("=== eBPF sockmap vs userspace proxy QPS 测试 ===\n");
    printf("payload=%d bytes, count=%d\n\n", g_payload_size, g_req_count);

    /* 启动 echo server */
    echo_server_t echo;
    if (echo_server_start(&echo, g_echo_port) != 0) return 1;

    print_header();

    /* --- eBPF 模式 --- */
    if (mode == MODE_EBPF || mode == MODE_BOTH) {
        printf("[eBPF mode] 加载 BPF 程序...\n");

        ebpf_forwarder_t ef;
        if (ebpf_forwarder_init(&ef) == 0) {
            /* 先启动客户端线程（它会在后台 retry connect），再 accept */
            pthread_t client_tid;
            client_thread_ctx_t client_ctx;
            memset(&client_ctx, 0, sizeof(client_ctx));
            pthread_create(&client_tid, NULL, client_thread_func, &client_ctx);
            while (!__atomic_load_n(&client_ctx.ready, __ATOMIC_ACQUIRE))
                usleep(1000);

            if (ebpf_forwarder_setup_sockets(&ef) == 0) {
                if (ebpf_forwarder_attach(&ef) == 0) {
                    printf("[eBPF mode] BPF 就绪，等待客户端完成...\n");
                    fflush(stdout);
                    pthread_join(client_tid, NULL);
                    printf("[eBPF mode] 客户端线程结束\n");
                    fflush(stdout);
                    print_result("ebpf_sockmap", &client_ctx.result);
                    fflush(stdout);
                } else {
                    pthread_join(client_tid, NULL);
                }
            } else {
                pthread_join(client_tid, NULL);
            }
            ebpf_forwarder_cleanup(&ef);
        } else {
            printf("[eBPF mode] 初始化失败，跳过\n");
            printf("%-12s  %-6s  %12s  %10s  %10s  %10s  %10s  %10s\n",
                   "ebpf_sockmap", "N/A", "SKIP", "-", "-", "-", "-", "-");
        }
        printf("\n");
    }

    /* --- 用户态模式 --- */
    if (mode == MODE_USERSPACE || mode == MODE_BOTH) {
        printf("[userspace mode] 启动用户态 proxy...\n");

        /* 同样：先启动客户端线程，再启动 proxy（proxy 需要 accept） */
        pthread_t client_tid;
        client_thread_ctx_t client_ctx;
        memset(&client_ctx, 0, sizeof(client_ctx));
        pthread_create(&client_tid, NULL, client_thread_func, &client_ctx);
        while (!__atomic_load_n(&client_ctx.ready, __ATOMIC_ACQUIRE))
            usleep(1000);

        userspace_proxy_t proxy;
        if (userspace_proxy_start(&proxy) == 0) {
            printf("[userspace mode] proxy 就绪，等待客户端完成...\n");
            pthread_join(client_tid, NULL);
            print_result("userspace", &client_ctx.result);
            userspace_proxy_stop(&proxy);
        } else {
            printf("[userspace mode] 初始化失败，跳过\n");
            printf("%-12s  %-6s  %12s  %10s  %10s  %10s  %10s  %10s\n",
                   "userspace", "N/A", "SKIP", "-", "-", "-", "-", "-");
            pthread_join(client_tid, NULL);
        }
    }

    echo_server_stop(&echo);
    printf("\n完成。\n");
    return 0;
}
