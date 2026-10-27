// ============================================================
// test_hash_ownership.c — hash value ownership 回归测试
//
// 目标（方案 §15.4）：
//   1. 同一 key 的 value 反复扩容/缩小/覆盖/删除后内容正确；
//   2. 大批量 key 扩容 + 删除 + 覆盖后 destroy 不泄漏；
//   3. 用 kvs_mem 计数做确定性泄漏断言：destroy 后 alloc_calls == free_calls。
//
// 独立编译（libc 后端，不依赖服务进程）：
//   make test_hash_ownership
// ============================================================
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvstore/kvstore.h"

static int g_failures = 0;

#define CHECK(cond, msg)                                                   \
    do {                                                                   \
        if (!(cond)) {                                                     \
            fprintf(stderr, "FAIL line %d: %s\n", __LINE__, msg);          \
            g_failures++;                                                  \
        }                                                                  \
    } while (0)

static void fill_value(char *buf, size_t len, int seed) {
    for (size_t i = 0; i < len; ++i)
        buf[i] = (char)('a' + (int)((i + (size_t)seed) % 26));
    buf[len] = '\0';
}

int main(void) {
    if (kvs_mem_init("libc") != 0) {
        fprintf(stderr, "kvs_mem_init failed\n");
        return 2;
    }

    kvs_hash_t hash;
    CHECK(kvs_hash_create(&hash) == 0, "kvs_hash_create");

    const size_t grow_sizes[] = {
        10, 1024, 10 * 1024, 100 * 1024, 64, 4096, 100 * 1024, 1
    };
    const size_t grow_count = sizeof(grow_sizes) / sizeof(grow_sizes[0]);
    char *buf = (char *)malloc(100 * 1024 + 1);
    if (!buf) return 2;

    /* 1. 单 key 反复扩容/缩小/覆盖 */
    for (int round = 0; round < 3; ++round) {
        for (size_t i = 0; i < grow_count; ++i) {
            size_t len = grow_sizes[i];
            fill_value(buf, len, (int)(i + (size_t)round));
            CHECK(kvs_hash_set_len(&hash, (char *)"grow:key", buf, len) == 0,
                  "set_len growth");
            size_t got = 0;
            char *v = kvs_hash_get_len(&hash, (char *)"grow:key", &got);
            CHECK(v != NULL && got == len && memcmp(v, buf, len) == 0,
                  "get_len roundtrip after growth");
        }
    }
    CHECK(kvs_hash_del(&hash, (char *)"grow:key") == 0, "del grown key");
    CHECK(kvs_hash_get(&hash, (char *)"grow:key") == NULL, "get after del");

    /* 2. 批量 key：扩容 → 删除一半 → 覆盖另一半（触发 rehash/shrink） */
    char key[64];
    for (int i = 0; i < 500; ++i) {
        snprintf(key, sizeof(key), "bulk:%04d", i);
        size_t len = (size_t)(16 + (i % 64) * 1024);
        fill_value(buf, len, i);
        CHECK(kvs_hash_set_len(&hash, key, buf, len) == 0, "bulk set");
    }
    for (int i = 0; i < 500; i += 2) {
        snprintf(key, sizeof(key), "bulk:%04d", i);
        CHECK(kvs_hash_del(&hash, key) == 0, "bulk del");
    }
    for (int i = 1; i < 500; i += 2) {
        snprintf(key, sizeof(key), "bulk:%04d", i);
        size_t len = (size_t)(2048 + (i % 32) * 2048);
        fill_value(buf, len, i + 7);
        CHECK(kvs_hash_set_len(&hash, key, buf, len) == 0, "bulk overwrite");
        size_t got = 0;
        char *v = kvs_hash_get_len(&hash, key, &got);
        CHECK(v != NULL && got == len && memcmp(v, buf, len) == 0,
              "bulk verify after overwrite");
    }

    /* 3. destroy 后引用计数平衡 ⇒ 无泄漏 */
    kvs_hash_destory(&hash);

    kvs_mem_stats_t stats;
    CHECK(kvs_mem_get_stats(&stats) == 0, "kvs_mem_get_stats");
    if (stats.alloc_calls != stats.free_calls) {
        fprintf(stderr, "FAIL: alloc_calls=%llu free_calls=%llu (leak)\n",
                (unsigned long long)stats.alloc_calls,
                (unsigned long long)stats.free_calls);
        g_failures++;
    } else {
        printf("alloc/free balanced: %llu\n",
               (unsigned long long)stats.alloc_calls);
    }

    free(buf);
    if (g_failures) {
        printf("hash ownership test FAILED (%d)\n", g_failures);
        return 1;
    }
    printf("hash ownership test OK\n");
    return 0;
}
