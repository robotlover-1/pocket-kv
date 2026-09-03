/**
 * @file test_vsearch.c
 * @brief VSEARCH 可选前缀参数 + 白名单校验的单元/边界测试（Task 4）
 *
 * 直链引擎源文件（绕开网络 / 无需运行中的 server）：
 *   src/storage/kvs_vector.c  src/storage/kvs_hash.c  src/memory/kvs_mem.c
 * 记录以 [u32 dim][float vec[dim]] 小端布局写入 global_hash —— 与 kvs_vector.c
 * 的 parse_vec 完全一致，也即 semantic 后端（Task5）实际写入格式。
 *
 * 覆盖（对应 brief Step3）：
 *   - 老三参默认 semcache: 命中
 *   - 新四参 semd:e5s:v1: 只命中该前缀 → 前缀隔离
 *   - 显式空前缀 / 非法字符 / 非白名单前缀 / dim 不在集 / topk 越界 → 报错不崩
 *   - 256/384/1024 异维记录混存 → 各按自己的 dim 扫、互不串
 *
 * 运行：
 *   gcc -O2 -I./include -o tests/test_vsearch_runtime tests/test_vsearch.c \
 *       src/storage/kvs_vector.c src/storage/kvs_hash.c src/memory/kvs_mem.c -lm
 *   ./tests/test_vsearch_runtime
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "kvstore/kvstore.h"

/* ---- 与 kvstore.c 完全一致的 resp 实现（kvs_vector.c 链接需要） ---- */
int resp_error(char *out, size_t cap, const char *s) { return snprintf(out, cap, "-ERR %s\r\n", s); }
int resp_bulk(char *out, size_t cap, const char *s, size_t len) {
    int n = snprintf(out, cap, "$%zu\r\n", len);
    if ((size_t)n + len + 2 > cap) return -1;
    memcpy(out + n, s, len); out[n + len] = '\r'; out[n + len + 1] = '\n';
    return n + (int)len + 2;
}

static int g_fail = 0;
static void check(int cond, const char *msg) {
    if (cond) printf("  ok: %s\n", msg);
    else { g_fail = 1; printf("FAIL: %s\n", msg); }
}

/* 向 global_hash 写入 [u32 dim][float vec[dim]]（小端）记录 */
static void put_rec(const char *key, int dim, const float *vec) {
    size_t vlen = 4 + (size_t)dim * 4;
    char *val = (char *)malloc(vlen);
    uint32_t d32 = (uint32_t)dim;
    memcpy(val, &d32, 4);
    memcpy(val + 4, vec, (size_t)dim * 4);
    kvs_hash_set_len(&global_hash, (char *)key, val, vlen);
    free(val);
}

static float *axis_vec(int dim) {
    float *v = calloc((size_t)dim, sizeof(float));
    v[0] = 1.0f;               /* 能量集中在 e0，cosine 可预见 */
    return v;
}
/* e0 上能量更大/略小，构造与查询朝向相近的一对 */
static float *half_vec(int dim) {
    float *v = calloc((size_t)dim, sizeof(float));
    v[0] = 0.9f;
    return v;
}

static int has_key(const char *resp, const char *key) {
    char pat[300];
    snprintf(pat, sizeof(pat), "$%zu\r\n%s\r\n", strlen(key), key);
    return strstr(resp, pat) != NULL;
}
static int is_err(const char *resp) { return strncmp(resp, "-ERR ", 5) == 0; }

int main(void) {
    printf("== kvs_vector_search: 前缀参数 + 白名单校验 ==\n");
    kvs_hash_create(&global_hash);
    const char *P_SEM = KVS_VSEARCH_DEFAULT_PREFIX;      /* "semcache:" */
    const char *P_E5S = "semd:e5s:v1:";
    const int D384 = 384, D256 = 256, D1024 = 1024;

    float *q384 = axis_vec(D384), *q256 = axis_vec(D256);
    float *semA = half_vec(D384), *semB = half_vec(D256);

    put_rec("semcache:a",       D384,  semA);
    put_rec("semd:e5s:v1:z",    D384,  semA);            /* 同值异前缀 */
    put_rec("semcache:r256",    D256,  semB);
    put_rec("semd:e5s:v1:r256", D256,  semB);            /* 同值异前缀 */
    put_rec("semcache:r1024",   D1024, q256);            /* 1024 维记录 */

    char resp[4096];
    int rc;

    /* [1] 老三参无条件（缺第 4 参）：分发层填默认 semcache:，仅命中 semcache */
    rc = kvs_vector_search(D384, q384, 5, P_SEM, (int)strlen(P_SEM), resp, sizeof(resp));
    check(rc > 0 && !is_err(resp), "老三参默认 semcache: 返回无误");
    check(has_key(resp, "semcache:a"), "老三参命中 semcache:a");
    check(!has_key(resp, "semd:e5s:v1:z"), "老三参不含 semd:e5s:v1（前缀隔离）");

    /* [2] 新四参 semd:e5s:v1: 只命中该前缀 */
    rc = kvs_vector_search(D384, q384, 5, P_E5S, (int)strlen(P_E5S), resp, sizeof(resp));
    check(rc > 0 && !is_err(resp), "四参 semd:e5s:v1: 返回无误");
    check(has_key(resp, "semd:e5s:v1:z"), "四参命中 semd:e5s:v1:z");
    check(!has_key(resp, "semcache:a"), "四参不含 semcache（前缀隔离）");

    /* [3] 参数校验一律报错的分类用例 */
    const struct { const char *pfx; const char *why; } badPfx[] = {
        { "",                 "显式空前缀" },
        { "other:",           "非白名单(字符集内)" },
        { "sem a:",           "含非法字符(空格)" },
        { "semcache",         "缺冒号/非白名单" },
    };
    for (size_t i = 0; i < sizeof(badPfx)/sizeof(badPfx[0]); i++) {
        char R[128];
        rc = kvs_vector_search(D384, q384, 5, badPfx[i].pfx,
                               (int)strlen(badPfx[i].pfx), R, sizeof(R));
        check(rc > 0 && is_err(R), badPfx[i].why);
    }
    /* 超长(65）前缀 */
    char longpfx[66]; memset(longpfx, 'a', 65); longpfx[65] = 0;
    rc = kvs_vector_search(D384, q384, 5, longpfx, 65, resp, sizeof(resp));
    check(rc > 0 && is_err(resp), "超长(65)前缀被拒");

    /* 非法 dim ∈ {240,512,0,-1} */
    const int baddims[] = {240, 512, 0, -1};
    for (size_t i = 0; i < sizeof(baddims)/sizeof(baddims[0]); i++) {
        char R[128];
        rc = kvs_vector_search(baddims[i], q384, 5, P_SEM, (int)strlen(P_SEM), R, sizeof(R));
        check(rc > 0 && is_err(R), "dim 不在 {256,384,1024}");
    }
    /* topk 越界 ∈ {0,150} */
    const int badk[] = {0, 150};
    for (size_t i = 0; i < sizeof(badk)/sizeof(badk[0]); i++) {
        char R[128];
        rc = kvs_vector_search(D384, q384, badk[i], P_SEM, (int)strlen(P_SEM), R, sizeof(R));
        check(rc > 0 && is_err(R), "topk 越界坏参");
    }

    /* [4] 256/384/1024 异维混存 → 各自按 own dim 扫、互不串 */
    rc = kvs_vector_search(D256, q256, 5, P_SEM, (int)strlen(P_SEM), resp, sizeof(resp));
    check(has_key(resp, "semcache:r256"), "256 维搜到 semcache:r256");
    check(!has_key(resp, "semcache:r1024"), "256 维不含 1024 维记录（want_dim 拦截）");
    check(!has_key(resp, "semcache:a"), "256 维不含 384 维记录（异维跳过）");
    rc = kvs_vector_search(D256, q256, 5, P_E5S, (int)strlen(P_E5S), resp, sizeof(resp));
    check(has_key(resp, "semd:e5s:v1:r256"), "256 维在 semd 前缀下命中");

    /* ---- 256-维 search 不出异维 / 非法前缀交错也已覆盖 ---- */

    kvs_hash_destory(&global_hash);
    free(q384); free(q256); free(semA); free(semB);

    printf(g_fail ? "\n== RESULT: FAIL ==\n" : "\n== RESULT: PASS ==\n");
    return g_fail ? 1 : 0;
}
