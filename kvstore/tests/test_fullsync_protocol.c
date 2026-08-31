/*
 * 全量同步传输无关协议的单元测试。
 *
 * 仅链接 kvs_fullsync.c + 本文件，不依赖任何 RDMA 硬件/verbs 头文件。
 * 编码了生产代码使用的精确 wire 格式：
 *   +FULLRESYNCWR <transfer_id> <addr> <rkey> <capacity>\r\n
 *   REPLDONE <transfer_id> <bytes>\r\n
 */
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#include "kvstore/replication/fullsync.h"

static void test_remote_mr_message(void) {
    kvs_fullsync_mr_msg_t mr = {0};
    const char line[] = "+FULLRESYNCWR 41 4096 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(line, sizeof(line) - 1, &mr) == 0);
    assert(mr.transfer_id == 41);
    assert(mr.addr == 4096);
    assert(mr.rkey == 77);
    assert(mr.capacity == 85000008);
    assert(kvs_fullsync_validate_remote_mr(&mr, 41, 85000008) == 0);
    assert(kvs_fullsync_validate_remote_mr(&mr, 42, 85000008) == -ESTALE);
}

static void test_remote_mr_no_plus_prefix(void) {
    kvs_fullsync_mr_msg_t mr = {0};
    const char line[] = "FULLRESYNCWR 41 4096 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(line, sizeof(line) - 1, &mr) == 0);
    assert(mr.transfer_id == 41 && mr.rkey == 77);
}

static void test_remote_mr_rejects(void) {
    kvs_fullsync_mr_msg_t mr = {0};

    /* 零地址 / 零 rkey / 容量不足 */
    const char zero_addr[] = "+FULLRESYNCWR 41 0 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(zero_addr, sizeof(zero_addr) - 1, &mr) == 0);
    assert(kvs_fullsync_validate_remote_mr(&mr, 41, 85000008) == -EINVAL);

    const char zero_rkey[] = "+FULLRESYNCWR 41 4096 0 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(zero_rkey, sizeof(zero_rkey) - 1, &mr) == 0);
    assert(kvs_fullsync_validate_remote_mr(&mr, 41, 85000008) == -EINVAL);

    const char small_cap[] = "+FULLRESYNCWR 41 4096 77 1024\r\n";
    assert(kvs_fullsync_parse_remote_mr(small_cap, sizeof(small_cap) - 1, &mr) == 0);
    assert(kvs_fullsync_validate_remote_mr(&mr, 41, 85000008) == -EOVERFLOW);

    /* 多余字段 */
    const char extra[] = "+FULLRESYNCWR 41 4096 77 85000008 999\r\n";
    assert(kvs_fullsync_parse_remote_mr(extra, sizeof(extra) - 1, &mr) == -EINVAL);

    /* 字段缺失 */
    const char missing[] = "+FULLRESYNCWR 41 4096 77\r\n";
    assert(kvs_fullsync_parse_remote_mr(missing, sizeof(missing) - 1, &mr) == -EINVAL);

    /* 截断 CRLF（无 \r\n 结束） */
    const char no_crlf[] = "+FULLRESYNCWR 41 4096 77 85000008";
    assert(kvs_fullsync_parse_remote_mr(no_crlf, sizeof(no_crlf) - 1, &mr) == -EINVAL);

    /* 负值 */
    const char neg[] = "+FULLRESYNCWR -41 4096 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(neg, sizeof(neg) - 1, &mr) == -EINVAL);

    /* 整数溢出（strtoull 溢出标记） */
    const char ovf[] = "+FULLRESYNCWR 18446744073709551616 4096 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(ovf, sizeof(ovf) - 1, &mr) == -EINVAL);

    /* 未知命令 */
    const char wrong_cmd[] = "+FULLSYNCWR 41 4096 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(wrong_cmd, sizeof(wrong_cmd) - 1, &mr) == -EINVAL);
}

static void test_remote_range(void) {
    uint64_t remote = 0;
    assert(kvs_fullsync_remote_addr(4096, 1024, 256, 512, &remote) == 0);
    assert(remote == 4352);
    assert(kvs_fullsync_remote_addr(4096, 1024, 768, 257, &remote) == -EOVERFLOW);
    assert(kvs_fullsync_remote_addr(4096, 1024, 2048, 1, &remote) == -EOVERFLOW);
    assert(kvs_fullsync_remote_addr(UINT64_MAX - 7, 16, 8, 1, &remote) == -EOVERFLOW);
    assert(kvs_fullsync_remote_addr(UINT64_MAX - 7, 16, 0, 1, &remote) == 0);
    assert(remote == UINT64_MAX - 7);
}

static void test_repldone_message(void) {
    uint64_t id = 0, bytes = 0;
    const char line[] = "REPLDONE 41 85000008\r\n";
    assert(kvs_fullsync_parse_repldone(line, sizeof(line) - 1, &id, &bytes) == 0);
    assert(id == 41 && bytes == 85000008);

    const char plus_line[] = "+REPLDONE 7 100\r\n";
    assert(kvs_fullsync_parse_repldone(plus_line, sizeof(plus_line) - 1, &id, &bytes) == 0);
    assert(id == 7 && bytes == 100);

    const char missing_bytes[] = "REPLDONE 41\r\n";
    assert(kvs_fullsync_parse_repldone(missing_bytes, sizeof(missing_bytes) - 1, &id, &bytes) == -EINVAL);

    const char no_crlf[] = "REPLDONE 41 100";
    assert(kvs_fullsync_parse_repldone(no_crlf, sizeof(no_crlf) - 1, &id, &bytes) == -EINVAL);

    const char extra[] = "REPLDONE 41 100 200\r\n";
    assert(kvs_fullsync_parse_repldone(extra, sizeof(extra) - 1, &id, &bytes) == -EINVAL);

    const char neg[] = "REPLDONE -41 100\r\n";
    assert(kvs_fullsync_parse_repldone(neg, sizeof(neg) - 1, &id, &bytes) == -EINVAL);
}

static void test_mode_names(void) {
    assert(strcmp(kvs_fullsync_mode_name(KVS_FULLSYNC_RDMA_WRITE), "rdma-write") == 0);
    assert(strcmp(kvs_fullsync_mode_name(KVS_FULLSYNC_RDMA_SEND), "rdma-send") == 0);
    assert(strcmp(kvs_fullsync_mode_name(KVS_FULLSYNC_TCP), "sendfile") == 0);
    assert(strcmp(kvs_fullsync_mode_name((kvs_fullsync_mode_t)99), "unknown") == 0);
}

/* 目标清理的幂等性：reset 在三种部分初始化阶段后都能安全重复调用，
 * 第二次调用后所有资源字段回到默认值。 */
static void test_target_reset_idempotent(void) {
    repl_fullsync_target_t t;
    memset(&t, 0, sizeof(t));

    /* 阶段 1：只有 fd */
    t.fd = 3;
    repl_fullsync_target_reset(&t);
    assert(t.fd == -1 && t.mapping == NULL && t.mr == NULL);
    assert(t.mapping_len == 0 && t.expected_bytes == 0 && t.transfer_id == 0);
    assert(t.path[0] == '\0' && !t.imm_received && !t.complete);
    repl_fullsync_target_reset(&t);  /* 第二次调用 */
    assert(t.fd == -1 && t.mapping == NULL && t.mr == NULL);

    /* 阶段 2：fd + mapping */
    t.fd = 5;
    t.mapping = (void *)0x1234;
    t.mapping_len = 4096;
    t.expected_bytes = 4096;
    strncpy(t.path, "/tmp/fullsync.recv", sizeof(t.path) - 1);
    repl_fullsync_target_reset(&t);
    assert(t.fd == -1 && t.mapping == NULL && t.mapping_len == 0);
    assert(t.expected_bytes == 0 && t.path[0] == '\0');
    repl_fullsync_target_reset(&t);
    assert(t.fd == -1 && t.mapping == NULL);

    /* 阶段 3：fd + mapping + MR 标记 */
    t.fd = 7;
    t.mapping = (void *)0x2000;
    t.mapping_len = 8192;
    t.mr = (void *)0x3000;
    t.transfer_id = 41;
    t.imm_received = 1;
    t.complete = 1;
    repl_fullsync_target_reset(&t);
    assert(t.fd == -1 && t.mapping == NULL && t.mr == NULL);
    assert(t.mapping_len == 0 && t.transfer_id == 0);
    assert(!t.imm_received && !t.complete);
    repl_fullsync_target_reset(&t);
    assert(t.fd == -1 && t.mapping == NULL && t.mr == NULL);
}

int main(void) {
    test_remote_mr_message();
    test_remote_mr_no_plus_prefix();
    test_remote_mr_rejects();
    test_remote_range();
    test_repldone_message();
    test_mode_names();
    test_target_reset_idempotent();
    printf("test_fullsync_protocol: all assertions passed\n");
    return 0;
}
