/**
 * @file kvs_vector.c
 * @brief 语义向量检索：遍历 global_hash 中 semcache:* 条目，暴力余弦 top-k
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "kvstore/kvstore.h"

#define VSEARCH_PREFIX "semcache:"
#define VSEARCH_PREFIX_LEN (sizeof(VSEARCH_PREFIX) - 1)

/* 值布局: [u32 dim][float vec[dim]]（小端） */
static int parse_vec(const char *value, size_t vlen, int want_dim, const float **vec_out, int *dim_out) {
    if (!value || vlen < 4) return -1;
    uint32_t dim = 0;
    memcpy(&dim, value, 4);
    if ((int)dim != want_dim) return -1;
    if (vlen < 4 + (size_t)dim * 4) return -1;
    *vec_out = (const float *)(value + 4);
    *dim_out = (int)dim;
    return 0;
}

static float cosine(const float *a, const float *b, int dim) {
    float dot = 0.0f, na = 0.0f, nb = 0.0f;
    for (int i = 0; i < dim; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if (na < 1e-9f || nb < 1e-9f) return 0.0f;
    return dot / (sqrtf(na) * sqrtf(nb));
}

/* 候选：暴力 top-k（本地规模小，O(n*k) 足够） */
typedef struct { const char *key; float score; } cand_t;

int kvs_vector_search(int dim, const float *query, int topk, char *resp, int cap) {
    if (!query || topk <= 0) return resp_error(resp, cap, "vsearch bad args");
    if (cap < 16) return -1;

    cand_t *cands = (cand_t *)calloc((size_t)topk, sizeof(cand_t));
    if (!cands) return resp_error(resp, cap, "vsearch oom");
    int n = 0;

    /* 遍历 ht[0]（rehash 中再查 ht[1]；节点只在一个表） */
    for (int t = 0; t < 2; t++) {
        if (t == 1 && global_hash.rehash_idx < 0) break;
        hashtable_t *ht = &global_hash.ht[t];
        for (int i = 0; i < ht->max_slots && ht->nodes; i++) {
            for (hashnode_t *node = ht->nodes[i]; node; node = node->next) {
                if (strncmp(node->key, VSEARCH_PREFIX, VSEARCH_PREFIX_LEN) != 0) continue;
                const float *vec = NULL; int vdim = 0;
                if (parse_vec(node->value, node->vlen, dim, &vec, &vdim) != 0) continue;
                float s = cosine(query, vec, dim);
                if (n < topk) {
                    cands[n].key = node->key; cands[n].score = s; n++;
                    /* 简单上浮：新插入候选冒泡到正确位置 */
                    for (int j = n - 1; j > 0 && cands[j].score > cands[j-1].score; j--) {
                        cand_t tmp = cands[j]; cands[j] = cands[j-1]; cands[j-1] = tmp;
                    }
                } else if (s > cands[topk-1].score) {
                    cands[topk-1].key = node->key; cands[topk-1].score = s;
                    for (int j = topk - 1; j > 0 && cands[j].score > cands[j-1].score; j--) {
                        cand_t tmp = cands[j]; cands[j] = cands[j-1]; cands[j-1] = tmp;
                    }
                }
            }
        }
    }

    /* RESP: *[2*k]\r\n (key,score) 交替 bulk */
    int pos = snprintf(resp, cap, "*%d\r\n", n * 2);
    char ds[32];
    for (int i = 0; i < n; i++) {
        int klen = strlen(cands[i].key);
        if (pos + 2 + klen + 2 + 16 > cap) { free(cands); return -1; } /* 前置粗检防超界 */
        int rb = resp_bulk(resp + pos, cap - pos, cands[i].key, klen);
        if (rb < 0) { free(cands); return -1; }
        pos += rb;
        int dlen = snprintf(ds, sizeof(ds), "%.4f", cands[i].score);
        rb = resp_bulk(resp + pos, cap - pos, ds, dlen);
        if (rb < 0) { free(cands); return -1; }
        pos += rb;
    }
    free(cands);
    return pos;
}
