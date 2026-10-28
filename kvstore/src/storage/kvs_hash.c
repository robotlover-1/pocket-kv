#include "kvstore/kvstore.h"
#include <sys/random.h>

kvs_hash_t global_hash = {0};

#define HASH_INIT_SIZE 4096
#define HASH_LOAD_FACTOR 2
#define REHASH_STEP_BUCKETS 10

/* ---- SipHash-2-4（keyed；防 Hash Flood） ---- */
static inline uint64_t _rotl64(uint64_t x, int b) { return (x << b) | (x >> (64 - b)); }
#define SIPROUND do {                         \
    v0 += v1; v1 = _rotl64(v1, 13); v1 ^= v0; v0 = _rotl64(v0, 32); \
    v2 += v3; v3 = _rotl64(v3, 16); v3 ^= v2;                     \
    v0 += v3; v3 = _rotl64(v3, 21); v3 ^= v0;                     \
    v2 += v1; v1 = _rotl64(v1, 17); v1 ^= v2; v2 = _rotl64(v2, 32); \
} while (0)

static uint64_t _siphash24(const unsigned char *in, size_t len,
                           uint64_t k0, uint64_t k1) {
    uint64_t v0 = k0 ^ 0x736f6d6570736575ULL;
    uint64_t v1 = k1 ^ 0x646f72616e646f6dULL;
    uint64_t v2 = k0 ^ 0x6c7967656e657261ULL;
    uint64_t v3 = k1 ^ 0x7465646279746573ULL;
    size_t i = 0;
    for (; i + 8 <= len; i += 8) {
        uint64_t m = 0;
        for (int b = 0; b < 8; ++b)
            m |= (uint64_t)in[i + b] << (8 * b);
        v3 ^= m; SIPROUND; SIPROUND; v0 ^= m;
    }
    uint64_t b = ((uint64_t)len & 0xff) << 56;
    for (int shift = 0; i < len; ++i, shift += 8)
        b |= (uint64_t)in[i] << shift;
    v3 ^= b; SIPROUND; SIPROUND; v0 ^= b;
    v2 ^= 0xff; SIPROUND; SIPROUND; SIPROUND; SIPROUND;
    return v0 ^ v1 ^ v2 ^ v3;
}

static void _hash_seed(uint64_t *k0, uint64_t *k1) {
    if (*k0 || *k1) return;
    ssize_t r0 = getrandom(k0, sizeof(*k0), 0);
    ssize_t r1 = getrandom(k1, sizeof(*k1), 0);
    if (r0 != (ssize_t)sizeof(*k0) || r1 != (ssize_t)sizeof(*k1)) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        *k0 = ((uint64_t)ts.tv_sec << 32) ^ (uint64_t)ts.tv_nsec ^
              ((uint64_t)getpid() << 16) ^ 0x9e3779b97f4a7c15ULL;
        *k1 = (*k0 * 0xbf58476d1ce4e5b9ULL) ^ 0x94d049bb133111ebULL;
    }
}

/* 统计（热路径用普通累加；单 reactor 线程写、INFO 读） */
static unsigned long long g_hash_lookups = 0;
static unsigned long long g_hash_hits = 0;
static unsigned long long g_hash_misses = 0;
static unsigned long long g_hash_probe_steps = 0;
static unsigned long long g_hash_max_chain = 0;

static uint32_t _hash_raw(kvs_hash_t *hash, const char *key, size_t klen) {
    _hash_seed(&hash->hash_k0, &hash->hash_k1);
    return (uint32_t)_siphash24((const unsigned char *)key, klen,
                                hash->hash_k0, hash->hash_k1);
}

/* modulo-reduced for a given table size */
static int _hash_idx(uint32_t hv, int size) {
    return (int)(hv % (uint32_t)size);
}

/* node + key 单块分配；value 独立分配，便于扩容时替换且无混合 ownership。 */
static hashnode_t *_create_node_len(uint32_t hv, char *key, size_t klen,
                                    char *value, size_t vlen) {
    size_t total = sizeof(hashnode_t) + klen + 1;
    hashnode_t *node = (hashnode_t*)kvs_malloc(total);
    if (!node) return NULL;
    memset(node, 0, sizeof(*node));
    node->hv = hv;
    node->klen = (uint32_t)klen;
    node->key = (char *)(node + 1);
    memcpy(node->key, key, klen);
    node->key[klen] = '\0';
    node->value = (char *)kvs_malloc(vlen + 1);
    if (!node->value) {
        kvs_free(node);
        return NULL;
    }
    if (vlen > 0) memcpy(node->value, value, vlen);
    node->value[vlen] = '\0';
    node->vlen = vlen;
    node->vcap = vlen + 1;
    return node;
}

static hashnode_t *_create_node(uint32_t hv, char *key, char *value) {
    return _create_node_len(hv, key, strlen(key), value, strlen(value));
}

/* 覆盖节点 value（长度感知）：容量足够就地写；否则分配新块、拷贝后释放旧值。 */
static int _overwrite_value(hashnode_t *n, char *value, size_t vlen) {
    if (vlen + 1 <= n->vcap) {
        if (vlen > 0) memcpy(n->value, value, vlen);
        n->value[vlen] = '\0';
        n->vlen = vlen;
        return 0;
    }
    size_t new_cap = n->vcap ? n->vcap * 2 : 64;
    if (new_cap < vlen + 1) new_cap = vlen + 1;
    char *nv = (char *)kvs_malloc(new_cap);
    if (!nv) return -2;
    if (vlen > 0) memcpy(nv, value, vlen);
    nv[vlen] = '\0';
    kvs_free(n->value);
    n->value = nv;
    n->vlen = vlen;
    n->vcap = new_cap;
    return 0;
}

/* 释放节点：value 独立分配，key 内联在 node 之后，无需单独 free。 */
static void _free_node(hashnode_t *n) {
    if (!n) return;
    kvs_free(n->value);
    kvs_free(n);
}

int kvs_hash_create(kvs_hash_t *hash) {
    if (!hash) return -1;
    memset(hash, 0, sizeof(*hash));
    hash->rehash_idx = -1;
    _hash_seed(&hash->hash_k0, &hash->hash_k1);
    hash->ht[0].nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * HASH_INIT_SIZE);
    if (!hash->ht[0].nodes) return -1;
    memset(hash->ht[0].nodes, 0, sizeof(hashnode_t*) * HASH_INIT_SIZE);
    hash->ht[0].max_slots = HASH_INIT_SIZE;
    hash->ht[0].count = 0;
    return 0;
}

void kvs_hash_destory(kvs_hash_t *hash) {
    if (!hash) return;
    for (int t = 0; t < 2; t++) {
        if (!hash->ht[t].nodes) continue;
        for (int i = 0; i < hash->ht[t].max_slots; ++i) {
            hashnode_t *node = hash->ht[t].nodes[i];
            while (node) {
                hashnode_t *tmp = node;
                node = node->next;
                _free_node(tmp);
            }
        }
        kvs_free(hash->ht[t].nodes);
        hash->ht[t].nodes = NULL;
    }
    hash->rehash_idx = -1;
}

/* returns 1 if rehash completed, 0 if still in progress, -1 on alloc error */
static int _rehash_step(kvs_hash_t *hash, int buckets) {
    if (hash->rehash_idx < 0) return 0;

    hashtable_t *h0 = &hash->ht[0];
    hashtable_t *h1 = &hash->ht[1];
    int remaining = buckets;

    while (remaining > 0 && hash->rehash_idx < h0->max_slots) {
        hashnode_t *node = h0->nodes[hash->rehash_idx];
        if (node) {
            /* migrate entire bucket chain */
            hashnode_t *next;
            while (node) {
                next = node->next;
                int idx = _hash_idx(node->hv, h1->max_slots);
                node->next = h1->nodes[idx];
                h1->nodes[idx] = node;
                h1->count++;
                h0->count--;
                node = next;
            }
            h0->nodes[hash->rehash_idx] = NULL;
            remaining--;
        }
        hash->rehash_idx++;
    }

    /* check if done */
    if (hash->rehash_idx >= h0->max_slots) {
        kvs_free(h0->nodes);
        h0->nodes = h1->nodes;
        h0->max_slots = h1->max_slots;
        h0->count = h1->count;
        memset(h1, 0, sizeof(*h1));
        hash->rehash_idx = -1;
        return 1;
    }
    return 0;
}

/* trigger expansion if load factor exceeded */
static int _maybe_expand(kvs_hash_t *hash) {
    if (hash->rehash_idx >= 0) return 0;

    hashtable_t *h0 = &hash->ht[0];
    if (h0->count <= h0->max_slots * HASH_LOAD_FACTOR) return 0;

    int new_size = h0->max_slots * 2;
    hashtable_t *h1 = &hash->ht[1];
    h1->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * new_size);
    if (!h1->nodes) return -1;
    memset(h1->nodes, 0, sizeof(hashnode_t*) * new_size);
    h1->max_slots = new_size;
    h1->count = 0;
    hash->rehash_idx = 0;

    return 0;
}

/* trigger shrink if table is too sparse */
static int _maybe_shrink(kvs_hash_t *hash) {
    if (hash->rehash_idx >= 0) return 0;

    hashtable_t *h0 = &hash->ht[0];
    if (h0->max_slots <= HASH_INIT_SIZE) return 0;
    if (h0->count > h0->max_slots / 8) return 0;

    int new_size = h0->max_slots / 2;
    if (new_size < HASH_INIT_SIZE) new_size = HASH_INIT_SIZE;

    hashtable_t *h1 = &hash->ht[1];
    h1->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * new_size);
    if (!h1->nodes) return -1;
    memset(h1->nodes, 0, sizeof(hashnode_t*) * new_size);
    h1->max_slots = new_size;
    h1->count = 0;
    hash->rehash_idx = 0;

    return 0;
}

/* search both tables, returns node pointer or NULL */
static hashnode_t *_find_node(kvs_hash_t *hash, const char *key) {
    size_t klen = strlen(key);
    g_hash_lookups++;
    /* try ht[0] */
    if (hash->ht[0].nodes && hash->ht[0].count > 0) {
        uint32_t hv = _hash_raw(hash, key, klen);
        int idx = _hash_idx(hv, hash->ht[0].max_slots);
        for (hashnode_t *n = hash->ht[0].nodes[idx]; n; n = n->next) {
            g_hash_probe_steps++;
            if (strcmp(n->key, key) == 0) { g_hash_hits++; return n; }
        }
    }
    /* try ht[1] if rehashing */
    if (hash->rehash_idx >= 0 && hash->ht[1].nodes && hash->ht[1].count > 0) {
        uint32_t hv = _hash_raw(hash, key, klen);
        int idx = _hash_idx(hv, hash->ht[1].max_slots);
        for (hashnode_t *n = hash->ht[1].nodes[idx]; n; n = n->next) {
            g_hash_probe_steps++;
            if (strcmp(n->key, key) == 0) { g_hash_hits++; return n; }
        }
    }
    g_hash_misses++;
    return NULL;
}

int kvs_hash_exist(kvs_hash_t *hash, char *key) {
    if (!hash || !key) return -1;
    _rehash_step(hash, REHASH_STEP_BUCKETS);
    return _find_node(hash, key) ? 0 : 1;
}

char *kvs_hash_get(kvs_hash_t *hash, char *key) {
    if (!hash || !key) return NULL;
    _rehash_step(hash, REHASH_STEP_BUCKETS);
    hashnode_t *n = _find_node(hash, key);
    return n ? n->value : NULL;
}

int kvs_hash_set(kvs_hash_t *hash, char *key, char *value) {
    if (!hash || !key || !value) return -1;
    _rehash_step(hash, REHASH_STEP_BUCKETS);

    /* duplicate check across both tables: 已存在则覆盖（对齐 Redis SET/HSET 语义） */
    hashnode_t *exist = _find_node(hash, key);
    if (exist) return _overwrite_value(exist, value, strlen(value));

    /* compute hv once */
    uint32_t hv = _hash_raw(hash, key, strlen(key));

    /* if rehashing, insert into ht[1]; else ht[0] */
    hashtable_t *target = (hash->rehash_idx >= 0) ? &hash->ht[1] : &hash->ht[0];
    int idx = _hash_idx(hv, target->max_slots);

    unsigned long long chain = 0;
    for (hashnode_t *it = target->nodes[idx]; it; it = it->next) chain++;
    if (chain + 1 > g_hash_max_chain) g_hash_max_chain = chain + 1;
    hashnode_t *new_node = _create_node(hv, key, value);
    if (!new_node) return -2;
    new_node->next = target->nodes[idx];
    target->nodes[idx] = new_node;
    target->count++;

    /* check if we need to start expanding */
    _maybe_expand(hash);
    return 0;
}

/* 长度感知 set：value 可为含 '\0' 的任意二进制（HSET/HGET 用 argl 长度调用） */
int kvs_hash_set_len(kvs_hash_t *hash, char *key, char *value, size_t vlen) {
    if (!hash || !key || !value) return -1;
    _rehash_step(hash, REHASH_STEP_BUCKETS);

    hashnode_t *exist = _find_node(hash, key);
    if (exist) return _overwrite_value(exist, value, vlen);

    uint32_t hv = _hash_raw(hash, key, strlen(key));
    hashtable_t *target = (hash->rehash_idx >= 0) ? &hash->ht[1] : &hash->ht[0];
    int idx = _hash_idx(hv, target->max_slots);

    unsigned long long chain = 0;
    for (hashnode_t *it = target->nodes[idx]; it; it = it->next) chain++;
    if (chain + 1 > g_hash_max_chain) g_hash_max_chain = chain + 1;
    hashnode_t *new_node = _create_node_len(hv, key, strlen(key), value, vlen);
    if (!new_node) return -2;
    new_node->next = target->nodes[idx];
    target->nodes[idx] = new_node;
    target->count++;

    _maybe_expand(hash);
    return 0;
}

/* 长度感知 get：*vlen 返回 value 真实字节数（含 '\0' 的二进制也能正确读回） */
char *kvs_hash_get_len(kvs_hash_t *hash, char *key, size_t *vlen) {
    if (!hash || !key) return NULL;
    _rehash_step(hash, REHASH_STEP_BUCKETS);
    hashnode_t *n = _find_node(hash, key);
    if (!n) return NULL;
    if (vlen) *vlen = n->vlen;
    return n->value;
}

int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value) {
    if (!hash || !key || !value) return -1;
    _rehash_step(hash, REHASH_STEP_BUCKETS);
    hashnode_t *n = _find_node(hash, key);
    if (!n) return 1;
    return _overwrite_value(n, value, strlen(value));
}

int kvs_hash_del(kvs_hash_t *hash, char *key) {
    if (!hash || !key) return -1;
    _rehash_step(hash, REHASH_STEP_BUCKETS);

    uint32_t hv = _hash_raw(hash, key, strlen(key));

    /* search ht[0] */
    if (hash->ht[0].nodes && hash->ht[0].count > 0) {
        int idx = _hash_idx(hv, hash->ht[0].max_slots);
        hashnode_t *cur = hash->ht[0].nodes[idx], *prev = NULL;
        while (cur) {
            if (strcmp(cur->key, key) == 0) {
                if (prev) prev->next = cur->next;
                else hash->ht[0].nodes[idx] = cur->next;
                _free_node(cur);
                if (hash->ht[0].count > 0) hash->ht[0].count--;
                _maybe_shrink(hash);
                return 0;
            }
            prev = cur; cur = cur->next;
        }
    }

    /* search ht[1] if rehashing */
    if (hash->rehash_idx >= 0 && hash->ht[1].nodes && hash->ht[1].count > 0) {
        int idx = _hash_idx(hv, hash->ht[1].max_slots);
        hashnode_t *cur = hash->ht[1].nodes[idx], *prev = NULL;
        while (cur) {
            if (strcmp(cur->key, key) == 0) {
                if (prev) prev->next = cur->next;
                else hash->ht[1].nodes[idx] = cur->next;
                _free_node(cur);
                if (hash->ht[1].count > 0) hash->ht[1].count--;
                _maybe_shrink(hash);
                return 0;
            }
            prev = cur; cur = cur->next;
        }
    }

    return 1;
}


int kvs_hash_get_stats(kvs_hash_stats_t *out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    out->lookups = g_hash_lookups;
    out->hits = g_hash_hits;
    out->misses = g_hash_misses;
    out->probe_steps = g_hash_probe_steps;
    out->max_chain = g_hash_max_chain;
    out->slots = global_hash.ht[0].max_slots;
    out->entries = global_hash.ht[0].count + global_hash.ht[1].count;
    out->rehashing = global_hash.rehash_idx >= 0;
    return 0;
}
