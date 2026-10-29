/*
 * 多 Slave 并发 RDMA 全量同步：协议/生命周期无关部分单测。
 *
 * 覆盖（无需真实 RDMA 硬件）：
 *   - CM hello v1 编解码、网络字节序、长度/版本/零值校验；
 *   - 传输无关的 job 事件/reason 名称；
 *   - RDMA 关闭时的 stub 行为（should_enable=0、mgr_start 失败、无事件）。
 *
 * 编译时强制 -DKVS_ENABLE_RDMA=0，避免依赖 libibverbs 环境。
 */
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "kvstore/replication/rdma_fullsync_mgr.h"

static int g_fail = 0;

#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        g_fail++; \
    } \
} while (0)

static void test_hello_roundtrip(void) {
    const uint64_t tids[] = { 1, 2, 0x1122334455667788ULL, UINT64_MAX };
    const uint64_t epochs[] = { 1, 0xdeadbeefcafebabeULL, UINT64_MAX };

    for (size_t i = 0; i < sizeof(tids) / sizeof(tids[0]); i++) {
        unsigned char buf[RDMA_FS_CM_HELLO_LEN];
        uint64_t tid = 0, epoch = 0;
        rdma_fs_cm_hello_encode(buf, tids[i], epochs[i % (sizeof(epochs) / sizeof(epochs[0]))]);
        CHECK(rdma_fs_cm_hello_decode(buf, sizeof(buf), &tid, &epoch) == 0);
        CHECK(tid == tids[i]);
        CHECK(epoch == epochs[i % (sizeof(epochs) / sizeof(epochs[0]))]);
    }
}

static void test_hello_wire_format(void) {
    unsigned char buf[RDMA_FS_CM_HELLO_LEN];
    rdma_fs_cm_hello_encode(buf, 0x0102030405060708ULL, 0x1112131415161718ULL);

    /* version 网络字节序：0x0001 */
    CHECK(buf[0] == 0x00 && buf[1] == 0x01);
    /* tid: 08 07 06 05 04 03 02 01 */
    CHECK(buf[8] == 0x01 && buf[9] == 0x02 && buf[10] == 0x03 && buf[11] == 0x04);
    CHECK(buf[12] == 0x05 && buf[13] == 0x06 && buf[14] == 0x07 && buf[15] == 0x08);
    /* epoch: 18 17 16 15 14 13 12 11 */
    CHECK(buf[16] == 0x11 && buf[17] == 0x12 && buf[18] == 0x13 && buf[19] == 0x14);
    CHECK(buf[20] == 0x15 && buf[21] == 0x16 && buf[22] == 0x17 && buf[23] == 0x18);
}

static void test_hello_rejects(void) {
    unsigned char buf[RDMA_FS_CM_HELLO_LEN];
    uint64_t tid = 0, epoch = 0;

    rdma_fs_cm_hello_encode(buf, 7, 9);
    /* 长度不足 */
    CHECK(rdma_fs_cm_hello_decode(buf, RDMA_FS_CM_HELLO_LEN - 1, &tid, &epoch) != 0);
    /* NULL */
    CHECK(rdma_fs_cm_hello_decode(NULL, RDMA_FS_CM_HELLO_LEN, &tid, &epoch) != 0);
    /* 版本不匹配 */
    buf[0] = 0x00;
    buf[1] = 0x02;
    CHECK(rdma_fs_cm_hello_decode(buf, sizeof(buf), &tid, &epoch) != 0);
    /* 零 tid / 零 epoch */
    rdma_fs_cm_hello_encode(buf, 0, 9);
    CHECK(rdma_fs_cm_hello_decode(buf, sizeof(buf), &tid, &epoch) != 0);
    rdma_fs_cm_hello_encode(buf, 7, 0);
    CHECK(rdma_fs_cm_hello_decode(buf, sizeof(buf), &tid, &epoch) != 0);
}

static void test_reason_names(void) {
    CHECK(rdma_fs_reason_name(RDMA_FS_REASON_NONE) != NULL);
    CHECK(rdma_fs_reason_name(RDMA_FS_REASON_TIMEOUT) != NULL);
    CHECK(rdma_fs_reason_name(RDMA_FS_REASON_CQ) != NULL);
    CHECK(rdma_fs_reason_name(12345) != NULL);
    CHECK(strcmp(rdma_fs_reason_name(RDMA_FS_REASON_CANCELLED), "cancelled") == 0);
}

static void test_disabled_stubs(void) {
    /* 本测试以 KVS_ENABLE_RDMA=0 编译：新路径必须完全关闭。 */
    CHECK(rdma_fs_should_enable() == 0);
    CHECK(rdma_fs_mgr_start() != 0);
    CHECK(rdma_fs_mgr_ready() == 0);

    uint64_t tid = 0, epoch = 0;
    CHECK(rdma_fs_begin(NULL, 0, 0, 0, 0, 0, 0, &tid, &epoch) != 0);
    CHECK(rdma_fs_on_remote_mr(1, NULL, 1, 1, 1) == 0);
    CHECK(rdma_fs_on_repldone(1, NULL, 0, 0) == 0);
    CHECK(rdma_fs_mark_fullsyncend_sent(1) != 0);

    rdma_fs_event_t evs[4];
    CHECK(rdma_fs_drain_events_owner(evs, 4) == 0);

    rdma_fs_stats_t st;
    memset(&st, 0xff, sizeof(st));
    rdma_fs_get_stats(&st);
    CHECK(st.active_jobs == 0 && st.total_started == 0);
}

int main(void) {
    test_hello_roundtrip();
    test_hello_wire_format();
    test_hello_rejects();
    test_reason_names();
    test_disabled_stubs();

    if (g_fail) {
        fprintf(stderr, "test_multi_rdma_fullsync: %d failure(s)\n", g_fail);
        return 1;
    }
    printf("test_multi_rdma_fullsync: ALL PASS\n");
    return 0;
}
