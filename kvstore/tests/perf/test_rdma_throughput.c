/*
 * test_rdma_throughput.c — RDMA write/send 吞吐量测试 (rdma_cm 版本)
 *
 * 使用 rdma_cm (RDMA Connection Manager) 建立连接，
 * 与项目 kvs_repl.c 全量同步的 RDMA 路径一致。
 *
 * v2: 对齐生产代码 (kvs_repl.c) 的核心 RDMA 模式：
 *   - completion channel + ibv_get_cq_event 事件驱动
 *   - pipeline 发送 (4 槽位，fire-and-forget)
 *   - 独立 CQ 轮询线程 + re-arm → drain → wait
 *   - QP depth = 64 (与生产一致)
 *
 * 用法:
 *   # 服务端（接收方）
 *   ./test_rdma_throughput --server --port 18516
 *
 *   # 客户端（发送方）
 *   ./test_rdma_throughput --host <server_ip> --port 18516 \
 *       --mode write --size 65536 --iters 10000
 *
 * 依赖: libibverbs, librdmacm, libpthread
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <ifaddrs.h>
#include <infiniband/verbs.h>
#include <net/if.h>
#include <netdb.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "common.h"

/* ========== 配置 ========== */
#define DEFAULT_PORT      18516
#define DEFAULT_SIZE      65536
#define DEFAULT_ITERS     5000
#define DEFAULT_MODE      "write"
#define DEFAULT_CHUNK     262144  /* --file 模式默认 chunk 大小 (256KB, 对齐生产 rdma_chunk_size) */

/* ---- 对齐 kvs_repl.c ---- */
#define QP_WR_DEPTH             1024 /* 对齐原测试 */
#define PIPELINE_DEPTH          4    /* max pipeline slots (原模式用1, --file 用4) */
#define PIPELINE_WR_ID_FLAG     0x80000000UL
#define MAX_RECV_POST           256

/* 通过 rdma_cm private_data 交换 MR 信息 */
typedef struct {
    uint64_t addr;
    uint32_t rkey;
} mr_info_t;

/* pipeline 发送槽位（对齐 repl_rdma_send_slot_t） */
typedef struct {
    struct ibv_mr *mr;
    unsigned char *buf;
    size_t cap;
    volatile int in_flight;
} send_slot_t;

/* ========== RDMA 资源（rdma_cm 方式） ========== */
typedef struct {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
    struct rdma_cm_id *listen_id;  /* server only */
    struct ibv_pd *pd;
    struct ibv_mr *mr;             /* recv buffer MR (server) / fallback MR (client) */
    struct ibv_cq *cq;

    void *buf;
    size_t buf_size;

    mr_info_t local_mr;
    mr_info_t remote_mr;

    volatile int connected;

    /* ---- pipeline 发送（对齐 kvs_repl.c）---- */
    send_slot_t send_slots[PIPELINE_DEPTH];
    int send_pipeline_depth;

    /* ---- 统计 ---- */
    volatile int send_completed;
    volatile int recv_completed;
    volatile size_t recv_bytes;

    /* write 模式: 目标是 file-backed mmap（对齐生产），清理走 munmap */
    int file_backed;
    int out_fd;

    /* --file-direct 模式: 注册整个文件 mmap 作为单个发送 MR（支持 1G 级大注册），
     * 直接从文件发 chunk，跳过 4×buf_size 的 slot 分配（否则 1G 注册需 5G pinned）。 */
    int file_direct;
    struct ibv_mr *file_mr;
} rdma_res_t;

/* ========== 等待 rdma_cm 事件 ========== */
static int wait_cm_event(struct rdma_event_channel *ec,
                         enum rdma_cm_event_type expected,
                         struct rdma_cm_event **out_event,
                         int timeout_ms) {
    struct rdma_cm_event *event = NULL;
    struct pollfd pfd;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = ec->fd;
    pfd.events = POLLIN;

    if (poll(&pfd, 1, timeout_ms) <= 0) {
        fprintf(stderr, "[cm] poll timeout waiting for %s\n",
                rdma_event_str(expected));
        return -1;
    }

    if (rdma_get_cm_event(ec, &event) != 0) return -1;
    if (event->event != expected) {
        fprintf(stderr, "[cm] unexpected event: got=%s expect=%s\n",
                rdma_event_str(event->event), rdma_event_str(expected));
        rdma_ack_cm_event(event);
        return -1;
    }
    *out_event = event;
    return 0;
}

/* ========== 释放 send slot（由 CQ poll thread 调用）========== */
static volatile int g_release_calls = 0;
static volatile int g_release_rejected = 0;
static void release_send_slot(rdma_res_t *r, int slot) {
    if (slot >= 0 && slot < PIPELINE_DEPTH) {
        r->send_slots[slot].in_flight = 0;
        __sync_fetch_and_add(&g_release_calls, 1);
    } else {
        __sync_fetch_and_add(&g_release_rejected, 1);
    }
}

/* ========== 获取空闲 send slot（内联 poll，对齐 kvs_repl.c fallback 路径）========== */
static int acquire_send_slot(rdma_res_t *r) {
    for (;;) {
        if (!r->connected) return -1;
        for (int i = 0; i < r->send_pipeline_depth; i++) {
            if (!r->send_slots[i].in_flight) {
                return i;
            }
        }
        /* 所有 slot 在飞 → 批量 poll CQ 回收 completion */
        struct ibv_wc wc[8];
        int n = ibv_poll_cq(r->cq, 8, wc);
        if (n > 0) {
            for (int j = 0; j < n; j++) {
                if (wc[j].status == IBV_WC_SUCCESS &&
                    (wc[j].opcode == IBV_WC_SEND || wc[j].opcode == IBV_WC_RDMA_WRITE)) {
                    if (wc[j].wr_id & PIPELINE_WR_ID_FLAG) {
                        release_send_slot(r, (int)(wc[j].wr_id & ~PIPELINE_WR_ID_FLAG));
                        r->send_completed++;
                    }
                }
            }
            continue;
        }
        /* 无 completion → 短暂等待 */
        usleep(10);
    }
}

/* ========== 服务端 ========== */
static int run_server(const char *host, int port, size_t buf_size, const char *out_path) {
    rdma_res_t r;
    struct rdma_cm_event *event = NULL;
    int ret = -1;
    int write_mode = 1;   /* 只测单边 WRITE（file-backed 目标） */
    int out_fd = -1;

    memset(&r, 0, sizeof(r));
    r.buf_size = buf_size;
    r.send_pipeline_depth = PIPELINE_DEPTH;

    /* 1. 创建 event channel */
    r.ec = rdma_create_event_channel();
    if (!r.ec) {
        perror("rdma_create_event_channel");
        return -1;
    }

    /* 2. 创建 listen id */
    if (rdma_create_id(r.ec, &r.listen_id, NULL, RDMA_PS_TCP) != 0) {
        perror("rdma_create_id (listen)");
        goto cleanup;
    }

    /* 3. bind + listen */
    struct sockaddr_in addr = { .sin_family = AF_INET,
                                .sin_addr.s_addr = host ? inet_addr(host) : htonl(INADDR_ANY),
                                .sin_port = htons((uint16_t)port) };
    if (rdma_bind_addr(r.listen_id, (struct sockaddr *)&addr) != 0) {
        perror("rdma_bind_addr");
        goto cleanup;
    }
    if (rdma_listen(r.listen_id, 1) != 0) {
        perror("rdma_listen");
        goto cleanup;
    }
    printf("[server] rdma_cm listening on port %d\n", port);
    fflush(stdout);

    /* 4. 等待连接请求 */
    if (wait_cm_event(r.ec, RDMA_CM_EVENT_CONNECT_REQUEST, &event, 30000) != 0) {
        fprintf(stderr, "[server] timeout waiting for CONNECT_REQUEST\n");
        goto cleanup;
    }
    r.id = event->id;
    rdma_ack_cm_event(event);
    event = NULL;
    printf("[server] connect request received\n");
    fflush(stdout);

    /* 5. 分配资源（服务端用内联 poll，无需 comp_chan） */
    r.pd = ibv_alloc_pd(r.id->verbs);
    if (!r.pd) { perror("ibv_alloc_pd"); goto cleanup; }

    r.cq = ibv_create_cq(r.id->verbs, QP_WR_DEPTH, NULL, NULL, 0);
    if (!r.cq) { perror("ibv_create_cq"); goto cleanup; }

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = r.cq,
        .recv_cq = r.cq,
        .qp_type = IBV_QPT_RC,
        .cap = { .max_send_wr = QP_WR_DEPTH,
                  .max_recv_wr = QP_WR_DEPTH,
                  .max_send_sge = 1,
                  .max_recv_sge = 1 },
    };
    if (rdma_create_qp(r.id, r.pd, &qp_attr) != 0) {
        perror("rdma_create_qp");
        goto cleanup;
    }

    /* 7. 注册 MR —— write 模式用 file-backed 目标（对齐生产 repl_rdma_slave_prepare_target）：
     *    创建目标文件 → ftruncate → mmap MAP_SHARED → 注册 REMOTE_WRITE MR。
     *    send 模式保持 heap buffer。 */
    if (write_mode) {
        if (!out_path) {
            fprintf(stderr, "[server] write 模式需要 --out 目标文件路径\n");
            goto cleanup;
        }
        out_fd = open(out_path, O_CREAT | O_TRUNC | O_RDWR, 0600);
        if (out_fd < 0) { perror("open --out"); goto cleanup; }
        if (ftruncate(out_fd, (off_t)buf_size) < 0) { perror("ftruncate --out"); goto cleanup; }
        r.buf = mmap(NULL, buf_size, PROT_READ | PROT_WRITE, MAP_SHARED, out_fd, 0);
        if (r.buf == MAP_FAILED) { perror("mmap --out"); r.buf = NULL; goto cleanup; }
        r.file_backed = 1;
    } else {
        r.buf = aligned_alloc(4096, buf_size);
        if (!r.buf) { perror("aligned_alloc"); goto cleanup; }
        memset(r.buf, 0, buf_size);
    }

    r.mr = ibv_reg_mr(r.pd, r.buf, buf_size,
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                          IBV_ACCESS_REMOTE_WRITE);
    if (!r.mr) { perror("ibv_reg_mr"); goto cleanup; }

    r.local_mr.addr = (uint64_t)(uintptr_t)r.buf;
    r.local_mr.rkey = r.mr->rkey;

    /* 8. rdma_accept（附带 MR 信息）。
     * initiator_depth/responder_resources 保持 0：WRITE-only 路径不需要 RDMA read 能力，
     * 且 Soft-RoCE (rxe0) 的 CM 握手在二者 >0 时会失败（rping 留 0 才通）。 */
    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.rnr_retry_count = 3;
    param.private_data = &r.local_mr;
    param.private_data_len = sizeof(r.local_mr);

    if (rdma_accept(r.id, &param) != 0) {
        perror("rdma_accept");
        goto cleanup;
    }

    /* 9. 等待 ESTABLISHED */
    if (wait_cm_event(r.ec, RDMA_CM_EVENT_ESTABLISHED, &event, 20000) != 0) {
        fprintf(stderr, "[server] timeout waiting for ESTABLISHED\n");
        goto cleanup;
    }

    if (event->param.conn.private_data_len >= sizeof(mr_info_t)) {
        memcpy(&r.remote_mr, event->param.conn.private_data, sizeof(mr_info_t));
    }
    rdma_ack_cm_event(event);
    event = NULL;
    r.connected = 1;

    printf("[server] RDMA 连接已建立\n");
    printf("[server] 远端 MR: addr=0x%lx rkey=%u\n",
           r.remote_mr.addr, r.remote_mr.rkey);
    fflush(stdout);

    /* 10. Pre-post recv WRs */
    {
        struct ibv_sge sge = { .addr = (uint64_t)(uintptr_t)r.buf,
                                .length = (uint32_t)buf_size,
                                .lkey = r.mr->lkey };
        struct ibv_recv_wr recv_wr = { .wr_id = 0,
                                        .sg_list = &sge,
                                        .num_sge = 1 };
        struct ibv_recv_wr *bad_wr = NULL;
        for (int i = 0; i < MAX_RECV_POST; i++) {
            if (ibv_post_recv(r.id->qp, &recv_wr, &bad_wr) != 0) break;
        }
    }

    /* 11. 内联 poll CQ（服务端纯 sink，只收不检查） */
    while (r.connected) {
        struct ibv_wc wc[16];
        int n = ibv_poll_cq(r.cq, 16, wc);
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status == IBV_WC_WR_FLUSH_ERR) {
                    r.connected = 0;
                    break;
                }
                continue;
            }
            if (wc[i].opcode == IBV_WC_RECV ||
                wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                r.recv_bytes += wc[i].byte_len;
                r.recv_completed++;
                /* 重新 post recv */
                struct ibv_sge sge = { .addr = (uint64_t)(uintptr_t)r.buf,
                                        .length = (uint32_t)buf_size,
                                        .lkey = r.mr->lkey };
                struct ibv_recv_wr recv_wr = { .wr_id = (uint64_t)r.recv_completed,
                                                .sg_list = &sge,
                                                .num_sge = 1 };
                struct ibv_recv_wr *bad_wr = NULL;
                ibv_post_recv(r.id->qp, &recv_wr, &bad_wr);
            }
        }
        if (n == 0) usleep(100);
    }

    /* write 模式: 数据经 RDMA WRITE 落入 file-backed 目标。持久化 + 提示 harness 做 cmp。 */
    if (write_mode && r.buf) {
        if (msync(r.buf, buf_size, MS_SYNC) < 0)
            perror("msync");
        printf("[server] write 目标已落盘: %s (%.2f MB)，请与源文件 cmp 验证\n",
               out_path, (double)buf_size / (1024.0 * 1024.0));
    }
    printf("[server] 接收完成: %d msgs (SEND 路径), %.2f MB\n",
           r.recv_completed, (double)r.recv_bytes / (1024.0 * 1024.0));
    ret = 0;

cleanup:
    if (event) rdma_ack_cm_event(event);
    if (r.id && r.id->qp) rdma_destroy_qp(r.id);
    if (r.cq) ibv_destroy_cq(r.cq);
    if (r.mr) ibv_dereg_mr(r.mr);
    if (r.buf) {
        if (r.file_backed) munmap(r.buf, buf_size);
        else free(r.buf);
    }
    if (out_fd >= 0) close(out_fd);
    if (r.pd) ibv_dealloc_pd(r.pd);
    if (r.id) rdma_destroy_id(r.id);
    if (r.listen_id) rdma_destroy_id(r.listen_id);
    if (r.ec) rdma_destroy_event_channel(r.ec);
    return ret;
}

/* ========== 客户端 ========== */
static int run_client(const char *host, size_t buf_size, int iters,
                      int port, const char *file_path, size_t chunk_size,
                      int file_direct) {
    rdma_res_t r;
    struct rdma_cm_event *event = NULL;
    int ret = -1;
    int file_mode = (file_path != NULL);
    unsigned char *file_map = NULL;
    size_t file_size = 0;
    int fd = -1;

    memset(&r, 0, sizeof(r));
    r.buf_size = buf_size;
    r.file_direct = file_direct;

    if (file_mode) {
        r.send_pipeline_depth = PIPELINE_DEPTH; /* 4 槽位 pipeline */
    } else {
        r.send_pipeline_depth = 1; /* 原模式: 单槽位 */
    }

    /* 1. 创建 event channel */
    r.ec = rdma_create_event_channel();
    if (!r.ec) {
        perror("rdma_create_event_channel");
        return -1;
    }

    /* 2. 创建 id */
    if (rdma_create_id(r.ec, &r.id, NULL, RDMA_PS_TCP) != 0) {
        perror("rdma_create_id");
        goto cleanup;
    }

    /* 3. 解析地址（remap loopback → 实际 IP，对齐 kvs_repl.c） */
    struct sockaddr_in addr = { .sin_family = AF_INET,
                                .sin_port = htons((uint16_t)port) };
    {
        int addr_ok = 0;

        /* Loopback remap: RDMA 不能走 lo，用实际网卡 IP */
        if (!strcmp(host, "127.0.0.1") || !strcmp(host, "localhost")) {
            struct ifaddrs *ifaddr = NULL;
            if (getifaddrs(&ifaddr) == 0) {
                for (struct ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
                    if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) continue;
                    if ((ifa->ifa_flags & IFF_UP) == 0) continue;
                    if (ifa->ifa_flags & IFF_LOOPBACK) continue;
                    struct sockaddr_in *sin = (struct sockaddr_in *)ifa->ifa_addr;
                    if (sin->sin_addr.s_addr == htonl(INADDR_LOOPBACK) || sin->sin_addr.s_addr == 0) continue;
                    addr.sin_addr = sin->sin_addr;
                    {
                        char ipbuf[INET_ADDRSTRLEN] = {0};
                        inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
                        fprintf(stderr, "[client] loopback remapped: %s → %s\n", host, ipbuf);
                    }
                    addr_ok = 1;
                    break;
                }
                freeifaddrs(ifaddr);
            }
            if (!addr_ok)
                fprintf(stderr, "[client] loopback remap failed, trying %s directly\n", host);
        }

        /* 直接解析（非 loopback 或 remap 失败时） */
        if (!addr_ok) {
            if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
                struct hostent *he = gethostbyname(host);
                if (!he) {
                    fprintf(stderr, "解析主机 %s 失败\n", host);
                    goto cleanup;
                }
                memcpy(&addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);
            }
        }
    }

    if (rdma_resolve_addr(r.id, NULL, (struct sockaddr *)&addr, 2000) != 0) {
        perror("rdma_resolve_addr");
        goto cleanup;
    }

    if (wait_cm_event(r.ec, RDMA_CM_EVENT_ADDR_RESOLVED, &event, 3000) != 0) {
        fprintf(stderr, "ADDR_RESOLVED 超时或失败\n");
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    /* 4. 解析路由 */
    if (rdma_resolve_route(r.id, 2000) != 0) {
        perror("rdma_resolve_route");
        goto cleanup;
    }

    if (wait_cm_event(r.ec, RDMA_CM_EVENT_ROUTE_RESOLVED, &event, 3000) != 0) {
        fprintf(stderr, "ROUTE_RESOLVED 超时或失败\n");
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    /* 5. 分配资源（客户端用内联 poll，无需 comp_chan） */
    r.pd = ibv_alloc_pd(r.id->verbs);
    if (!r.pd) { perror("ibv_alloc_pd"); goto cleanup; }

    r.cq = ibv_create_cq(r.id->verbs, QP_WR_DEPTH, NULL, NULL, 0);
    if (!r.cq) { perror("ibv_create_cq"); goto cleanup; }

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = r.cq,
        .recv_cq = r.cq,
        .qp_type = IBV_QPT_RC,
        .cap = { .max_send_wr = QP_WR_DEPTH,
                  .max_recv_wr = QP_WR_DEPTH,
                  .max_send_sge = 1,
                  .max_recv_sge = 1 },
    };
    if (rdma_create_qp(r.id, r.pd, &qp_attr) != 0) {
        perror("rdma_create_qp");
        goto cleanup;
    }

    /* 7. 分配 pipeline send buffers + 注册 MR（对齐生产 repl_rdma_prepare_buffers）。
     *    --file-direct 模式跳过 slot 分配（直接注册文件 mmap，见步骤 7.5）。 */
    if (!r.file_direct) {
        for (int i = 0; i < PIPELINE_DEPTH; i++) {
            r.send_slots[i].buf = (unsigned char *)aligned_alloc(4096, buf_size);
            if (!r.send_slots[i].buf) {
                perror("aligned_alloc (send slot)");
                goto cleanup;
            }
            memset(r.send_slots[i].buf, 'R', buf_size);
            r.send_slots[i].cap = buf_size;
            r.send_slots[i].in_flight = 0;

            r.send_slots[i].mr = ibv_reg_mr(r.pd, r.send_slots[i].buf, buf_size,
                                            IBV_ACCESS_LOCAL_WRITE);
            if (!r.send_slots[i].mr) {
                perror("ibv_reg_mr (send slot)");
                goto cleanup;
            }
        }
    }

    /* 8. 注册 MR（用于 rdma_cm private_data 交换）。
     *    --file-direct 模式只用来握手，r.buf 缩小到 4KB，避免 1G 级注册再占 1G 内存。 */
    size_t handshake_size = r.file_direct ? 4096 : buf_size;
    r.buf = aligned_alloc(4096, handshake_size);
    if (!r.buf) { perror("aligned_alloc"); goto cleanup; }
    memset(r.buf, 'R', handshake_size);

    r.mr = ibv_reg_mr(r.pd, r.buf, handshake_size,
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                          IBV_ACCESS_REMOTE_WRITE);
    if (!r.mr) { perror("ibv_reg_mr"); goto cleanup; }

    r.local_mr.addr = (uint64_t)(uintptr_t)r.buf;
    r.local_mr.rkey = r.mr->rkey;

    /* ---- file 模式：mmap 文件 ---- */
    if (file_mode) {
        fd = open(file_path, O_RDONLY);
        if (fd < 0) { perror("open --file"); goto cleanup; }
        struct stat st;
        if (fstat(fd, &st) < 0) { perror("fstat --file"); goto cleanup; }
        file_size = (size_t)st.st_size;
        file_map = (unsigned char *)mmap(NULL, file_size, PROT_READ,
                                          MAP_PRIVATE, fd, 0);
        if (file_map == MAP_FAILED) {
            perror("mmap --file");
            file_map = NULL;
            goto cleanup;
        }
        printf("[client] file: %s, size=%.2f MB, chunk=%zu bytes\n",
               file_path, (double)file_size / (1024.0 * 1024.0), chunk_size);
        fflush(stdout);

        /* --file-direct：把整个文件 mmap 注册为单个 MR（大小=file_size，即"注册大小"），
         * 后续 chunk WRITE 直接从文件页发出。
         * access=0：只读文件 mmap，仅作发送源（WRITE 读本地 buffer），带 LOCAL_WRITE
         * 会触发 get_user_pages(FOLL_WRITE) 在只读映射上 EFAULT。 */
        if (r.file_direct) {
            r.file_mr = ibv_reg_mr(r.pd, file_map, file_size, 0);
            if (!r.file_mr) {
                perror("ibv_reg_mr (file-direct)");
                goto cleanup;
            }
            printf("[client] file-direct: registered whole file as 1 MR (%.2f MB)\n",
                   (double)file_size / (1024.0 * 1024.0));
            fflush(stdout);
        }
    }

    /* 9. rdma_connect（附带 MR 信息）。
     * initiator_depth/responder_resources 保持 0：WRITE-only 路径不需要 RDMA read 能力，
     * 且 Soft-RoCE (rxe0) 的 CM 握手在二者 >0 时会失败（rping 留 0 才通）。 */
    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.retry_count = 7;
    param.rnr_retry_count = 7;
    param.private_data = &r.local_mr;
    param.private_data_len = sizeof(r.local_mr);

    if (rdma_connect(r.id, &param) != 0) {
        perror("rdma_connect");
        goto cleanup;
    }

    /* 10. 等待 ESTABLISHED */
    if (wait_cm_event(r.ec, RDMA_CM_EVENT_ESTABLISHED, &event, 20000) != 0) {
        fprintf(stderr, "ESTABLISHED 超时或失败\n");
        goto cleanup;
    }

    if (event->param.conn.private_data_len >= sizeof(mr_info_t)) {
        memcpy(&r.remote_mr, event->param.conn.private_data, sizeof(mr_info_t));
    }
    rdma_ack_cm_event(event);
    event = NULL;
    r.connected = 1;

    printf("[client] RDMA 连接已建立\n");
    printf("[client] 远端 MR: addr=0x%lx rkey=%u\n",
           r.remote_mr.addr, r.remote_mr.rkey);
    fflush(stdout);

    /* 11. 发送循环 */
    int actual_iters = 0;
    int inflight = 0;
    size_t total_posted = 0;

    double t0 = now_us();

    if (file_mode) {
        if (r.file_direct) {
            /* ---- file-direct 模式：整文件已注册为单个 MR，chunk 直接从文件 mmap 发出。
             * 无 slot 拷贝；函数级 inflight 维护在飞窗口，末尾共享 drain 回收。 */
            size_t remaining = file_size;
            size_t off = 0;

            while (off < file_size) {
                size_t this_chunk = remaining > chunk_size ? chunk_size : remaining;
                struct ibv_sge sge = {
                    .addr = (uint64_t)(uintptr_t)(file_map + off),
                    .length = (uint32_t)this_chunk,
                    .lkey = r.file_mr->lkey };
                struct ibv_send_wr send_wr;
                memset(&send_wr, 0, sizeof(send_wr));
                send_wr.wr_id = 0;
                send_wr.sg_list = &sge;
                send_wr.num_sge = 1;
                send_wr.send_flags = (uint32_t)(IBV_SEND_SIGNALED);
                send_wr.opcode = IBV_WR_RDMA_WRITE;
                send_wr.wr.rdma.remote_addr = r.remote_mr.addr + off;
                send_wr.wr.rdma.rkey = r.remote_mr.rkey;
                struct ibv_send_wr *bad_wr = NULL;

                if (ibv_post_send(r.id->qp, &send_wr, &bad_wr) != 0) {
                    fprintf(stderr, "[client] ibv_post_send failed off=%zu\n", off);
                    break;
                }
                inflight++;
                total_posted += this_chunk;
                off += this_chunk;
                remaining -= this_chunk;
                actual_iters++;

                /* 在飞超阈值则回收 completion（防止 QP send queue 打满） */
                if (inflight >= 64) {
                    struct ibv_wc wc[32];
                    int n = ibv_poll_cq(r.cq, 32, wc);
                    if (n > 0) {
                        r.send_completed += n;
                        inflight -= n;
                    }
                }
            }
        } else {
            /* ---- file 模式（默认）：slot+memcpy pipeline 分块发送 ---- */
            size_t remaining = file_size;
            size_t off = 0;
            int chunk_idx = 0;

        for (;;) {
            if (off >= file_size) break;
            size_t this_chunk = remaining > chunk_size ? chunk_size : remaining;

            /* 获取空闲 slot（内联 poll 等 completion） */
            for (;;) {
                if (!r.connected) break;
                int found = -1;
                for (int si = 0; si < r.send_pipeline_depth; si++) {
                    if (!r.send_slots[si].in_flight) {
                        found = si;
                        break;
                    }
                }
                if (found >= 0) {
                    int slot = found;
                    /* 拷贝 chunk 到 send slot（对齐生产 kvs_repl.c） */
                    memcpy(r.send_slots[slot].buf, file_map + off, this_chunk);

                    struct ibv_sge sge;
                    sge.addr = (uint64_t)(uintptr_t)r.send_slots[slot].buf;
                    sge.length = (uint32_t)this_chunk;
                    sge.lkey = r.send_slots[slot].mr->lkey;
                    struct ibv_send_wr send_wr;
                    memset(&send_wr, 0, sizeof(send_wr));
                    send_wr.wr_id = PIPELINE_WR_ID_FLAG | (uint64_t)slot;
                    send_wr.sg_list = &sge;
                    send_wr.num_sge = 1;
                    send_wr.send_flags = (uint32_t)(IBV_SEND_SIGNALED);

                    /* 对齐生产 flush_batch: 全 RDMA WRITE（无 SEND 预热首包——生产已实测
                     * 直接单边 WRITE 可行，且首包 SEND completion 在跨机 rxe 报 status=2 opcode=1）。
                     * WRITE 写入 remote_addr + off，与生产 write_total_sent 一致。
                     * 不用 WRITE_WITH_IMM（跨机 rxe 报 REMOTE_ACCESS_ERR），完成靠连接 teardown + 外部 cmp。 */
                    send_wr.opcode = IBV_WR_RDMA_WRITE;
                    send_wr.wr.rdma.remote_addr = r.remote_mr.addr + off;
                    send_wr.wr.rdma.rkey = r.remote_mr.rkey;
                    struct ibv_send_wr *bad_wr = NULL;

                    if (ibv_post_send(r.id->qp, &send_wr, &bad_wr) != 0) {
                        fprintf(stderr, "[client] ibv_post_send failed chunk=%d\n", chunk_idx);
                        break;
                    }
                    r.send_slots[slot].in_flight = 1;
                    inflight++;
                    total_posted += this_chunk;
                    off += this_chunk;
                    remaining -= this_chunk;
                    chunk_idx++;
                    actual_iters = chunk_idx;
                    break;
                }
                /* 所有 slot 在飞 → poll CQ */
                struct ibv_wc wc[8];
                int n = ibv_poll_cq(r.cq, 8, wc);
                if (n > 0) {
                    for (int j = 0; j < n; j++) {
                        if (wc[j].status == IBV_WC_SUCCESS &&
                            (wc[j].opcode == IBV_WC_SEND || wc[j].opcode == IBV_WC_RDMA_WRITE)) {
                            if (wc[j].wr_id & PIPELINE_WR_ID_FLAG) {
                                release_send_slot(&r, (int)(wc[j].wr_id & ~PIPELINE_WR_ID_FLAG));
                                r.send_completed++;
                            }
                        }
                    }
                    inflight -= n;
                    continue;
                }
                usleep(10);
            }
        }
        }
    } else {
        /* ---- 原模式：同一 buffer 发送 iters 次 ---- */
        for (int i = 0; i < iters; i++) {
            struct ibv_sge sge = {
                .addr = (uint64_t)(uintptr_t)r.send_slots[0].buf,
                .length = (uint32_t)buf_size,
                .lkey = r.send_slots[0].mr->lkey };
            struct ibv_send_wr send_wr = {
                .sg_list = &sge,
                .num_sge = 1,
                .send_flags = IBV_SEND_SIGNALED };
            struct ibv_send_wr *bad_wr = NULL;

            send_wr.opcode = IBV_WR_RDMA_WRITE;
            send_wr.wr.rdma.remote_addr = r.remote_mr.addr;
            send_wr.wr.rdma.rkey = r.remote_mr.rkey;

            if (ibv_post_send(r.id->qp, &send_wr, &bad_wr) != 0) {
                struct ibv_wc wc[32];
                int n = ibv_poll_cq(r.cq, 32, wc);
                if (n > 0) {
                    inflight -= n;
                    r.send_completed += n;
                    if (ibv_post_send(r.id->qp, &send_wr, &bad_wr) != 0) {
                        fprintf(stderr, "[client] ibv_post_send 失败 iter=%d (retry)\n", i);
                        break;
                    }
                    inflight++;
                    actual_iters++;
                } else {
                    fprintf(stderr, "[client] ibv_post_send 失败 iter=%d (qp full)\n", i);
                    break;
                }
            } else {
                inflight++;
                actual_iters++;
            }

            /* 周期性 poll */
            if (inflight >= 256 || (i % 16 == 0) || i == iters - 1) {
                struct ibv_wc wc[32];
                while (inflight > 512) {
                    int n = ibv_poll_cq(r.cq, 32, wc);
                    if (n <= 0) { usleep(10); continue; }
                    inflight -= n;
                    r.send_completed += n;
                }
                if (inflight > 512) {
                    int n = ibv_poll_cq(r.cq, 64, wc);
                    if (n > 0) { inflight -= n; r.send_completed += n; }
                }
            }
        }
    }

    /* 12. Drain 所有 inflight */
    {
        while (inflight > 0 && r.connected) {
            struct ibv_wc wc[64];
            int n = ibv_poll_cq(r.cq, 64, wc);
            if (n <= 0) { usleep(100); continue; }
            inflight -= n;
            r.send_completed += n;
        }
    }

    double t1 = now_us();
    double elapsed_s = (t1 - t0) / 1000000.0;
    size_t actual_bytes;
    if (file_mode) {
        actual_bytes = total_posted;
    } else {
        actual_bytes = (size_t)actual_iters * buf_size;
    }
    double throughput_bps = (double)(actual_bytes * 8) / elapsed_s;

    printf("\n=== RDMA 吞吐量结果 ===\n");
    printf("  模式:       %s\n",
           file_mode ? "RDMA_WRITE (file, 单边)" : "RDMA_WRITE");
    if (file_mode) {
        printf("  文件:       %s\n", file_path);
        printf("  文件大小:   %.2f MB\n", (double)file_size / (1024.0 * 1024.0));
        printf("  chunk:      %zu bytes\n", chunk_size);
        printf("  chunks:     %d\n", actual_iters);
    } else {
        printf("  payload:    %zu bytes\n", buf_size);
        printf("  iters:      %d (成功: %d)\n", iters, actual_iters);
    }
    printf("  CQ 完成:     send=%d\n", r.send_completed);
    printf("  release_calls: %d rejected=%d\n", g_release_calls, g_release_rejected);
    printf("  总数据量:   %.2f MB\n", (double)actual_bytes / (1024.0 * 1024.0));
    printf("  耗时:       %.3f s\n", elapsed_s);
    printf("  吞吐量:     %s\n", throughput_str(throughput_bps));

    /* 断开连接 */
    rdma_disconnect(r.id);

    ret = 0;

cleanup:
    if (file_map && file_map != MAP_FAILED) munmap(file_map, file_size);
    if (fd >= 0) close(fd);
    if (event) rdma_ack_cm_event(event);
    if (r.id && r.id->qp) rdma_destroy_qp(r.id);
    if (r.cq) ibv_destroy_cq(r.cq);
    /* 释放 pipeline send slots */
    for (int i = 0; i < PIPELINE_DEPTH; i++) {
        if (r.send_slots[i].mr) ibv_dereg_mr(r.send_slots[i].mr);
        if (r.send_slots[i].buf) free(r.send_slots[i].buf);
    }
    if (r.mr) ibv_dereg_mr(r.mr);
    if (r.file_mr) ibv_dereg_mr(r.file_mr);
    if (r.buf) free(r.buf);
    if (r.pd) ibv_dealloc_pd(r.pd);
    if (r.id) rdma_destroy_id(r.id);
    if (r.ec) rdma_destroy_event_channel(r.ec);
    return ret;
}

/* ========== Main ========== */
static void usage(const char *prog) {
    fprintf(stderr,
            "用法:\n"
            "  服务端: %s --server [--port PORT] [--size SIZE] --out FILE\n"
            "  客户端: %s --host <server_ip> [options]\n"
            "选项:\n"
            "  --host, -H     服务端 IP（客户端模式必需）\n"
            "  --server, -s   以服务端模式运行\n"
            "  --port, -p     rdma_cm 端口 (默认: 18516)\n"
            "  --size           传输 payload / chunk 大小 (默认: 65536)\n"
            "  --iters          传输次数 (默认: 5000, --file 模式忽略)\n"
            "  --file           发送整个文件（客户端）—— 单边 RDMA WRITE（全 WRITE，无 SEND 预热）\n"
            "  --chunk-size     file 模式分块大小 (默认: 262144)\n"
            "  --file-direct    注册整个文件 mmap 为单个 MR，chunk 直接从文件发（支持 1G 级大注册）\n"
            "  --out            服务端 file-backed 目标文件（对齐生产）\n"
            "  --help, -h      显示帮助\n",
            prog, prog);
}

int main(int argc, char **argv) {
    /* 注册大 MR（≥85MB）需要锁定内存，默认 RLIMIT_MEMLOCK(64MB) 不够 */
    {
        struct rlimit rlim = { RLIM_INFINITY, RLIM_INFINITY };
        setrlimit(RLIMIT_MEMLOCK, &rlim);
    }

    const char *host = NULL;
    const char *file_path = NULL;
    const char *out_path = NULL;
    int server_mode = 0;
    int port = DEFAULT_PORT;
    size_t buf_size = DEFAULT_SIZE;
    int iters = DEFAULT_ITERS;
    size_t chunk_size = DEFAULT_CHUNK;
    int file_direct = 0;

    struct option long_opts[] = {
        {"host", required_argument, 0, 'H'},
        {"server", no_argument, 0, 's'},
        {"port", required_argument, 0, 'p'},
        {"size", required_argument, 0, 1000},
        {"iters", required_argument, 0, 1001},
        {"file", required_argument, 0, 1006},
        {"chunk-size", required_argument, 0, 1007},
        {"out", required_argument, 0, 1008},
        {"file-direct", no_argument, 0, 1009},
        /* 以下参数为兼容旧命令行保留，rdma_cm 自动处理，接受但忽略 */
        {"ib-dev", required_argument, 0, 1003},
        {"ib-port", required_argument, 0, 1004},
        {"gid-idx", required_argument, 0, 1005},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "H:sp:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'H': host = optarg; break;
        case 's': server_mode = 1; break;
        case 'p': port = atoi(optarg); break;
        case 1000: buf_size = (size_t)atol(optarg); break;
        case 1001: iters = atoi(optarg); break;
        case 1003: /* --ib-dev: rdma_cm 自动选择，忽略 */ break;
        case 1004: /* --ib-port: rdma_cm 自动选择，忽略 */ break;
        case 1005: /* --gid-idx: rdma_cm 自动选择，忽略 */ break;
        case 1006: file_path = optarg; break;
        case 1007: chunk_size = (size_t)atol(optarg); break;
        case 1008: out_path = optarg; break;
        case 1009: file_direct = 1; break;
        case 'h': usage(argv[0]); return 0;
        default: usage(argv[0]); return 1;
        }
    }

    if (!server_mode && !host) {
        fprintf(stderr, "错误: 客户端模式需要 --host\n");
        usage(argv[0]);
        return 1;
    }

    if (server_mode && file_path) {
        fprintf(stderr, "错误: --file 仅用于客户端模式\n");
        return 1;
    }
    if (!server_mode && out_path) {
        fprintf(stderr, "错误: --out 仅用于服务端模式\n");
        return 1;
    }

    /* 本工具只测单边 RDMA WRITE（对齐生产全量同步）；双边 SEND 已移除。 */
    /* 服务端的 recv buffer 需要 ≥ chunk_size */
    if (file_path && buf_size == DEFAULT_SIZE) buf_size = chunk_size;

    return server_mode
        ? run_server(host, port, buf_size, out_path)
        : run_client(host, buf_size, iters, port, file_path, chunk_size, file_direct);
}
