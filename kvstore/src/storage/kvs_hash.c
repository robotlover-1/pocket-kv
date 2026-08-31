#include "kvstore/kvstore.h"

kvs_hash_t global_hash = {0};

#define HASH_INIT_SIZE 4096
#define HASH_LOAD_FACTOR 2
#define REHASH_STEP_BUCKETS 10

/* raw FNV-1a returning full 32-bit hash */
static uint32_t _hash_raw(const char *key) {
    uint32_t sum = 2166136261u;
    for (int i = 0; key[i] != 0; ++i) {
        sum ^= (unsigned char)key[i];
        sum *= 16777619u;
    }
    return sum;
}

/* modulo-reduced for a given table size */
static int _hash_idx(uint32_t hv, int size) {
    return (int)(hv % (uint32_t)size);
}

/* single-allocation: node + key + value in one block */
static hashnode_t *_create_node_len(uint32_t hv, char *key, size_t klen,
                                    char *value, size_t vlen) {
    size_t total = sizeof(hashnode_t) + klen + 1 + vlen + 1;
    hashnode_t *node = (hashnode_t*)kvs_malloc(total);
    if (!node) return NULL;
    memset(node, 0, sizeof(*node));
    node->hv = hv;
    node->key = (char *)(node + 1);
    node->value = node->key + klen + 1;
    memcpy(node->key, key, klen + 1);
    memcpy(node->value, value, vlen);
    node->value[vlen] = '\0';
    node->vlen = vlen;
    return node;
}

static hashnode_t *_create_node(uint32_t hv, char *key, char *value) {
    return _create_node_len(hv, key, strlen(key), value, strlen(value));
}

/* 覆盖节点 value（长度感知）：新 value 能就地装下则就地写，否则重分配
 * （旧 value 与 node 同块分配，无法 free，保留旧行为直接泄漏）。 */
static int _overwrite_value(hashnode_t *n, char *value, size_t vlen) {
    if (vlen <= n->vlen) {
        memcpy(n->value, value, vlen);
        n->value[vlen] = '\0';
        n->vlen = vlen;
        return 0;
    }
    char *nv = (char *)kvs_malloc(vlen + 1);
    if (!nv) return -2;
    memcpy(nv, value, vlen);
    nv[vlen] = '\0';
    n->value = nv;
    n->vlen = vlen;
    return 0;
}

int kvs_hash_create(kvs_hash_t *hash) {
    if (!hash) return -1;
    memset(hash, 0, sizeof(*hash));
    hash->rehash_idx = -1;
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
                kvs_free(tmp);   /* single alloc: node+key+value contiguous */
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
    /* try ht[0] */
    if (hash->ht[0].nodes && hash->ht[0].count > 0) {
        uint32_t hv = _hash_raw(key);
        int idx = _hash_idx(hv, hash->ht[0].max_slots);
        for (hashnode_t *n = hash->ht[0].nodes[idx]; n; n = n->next) {
            if (strcmp(n->key, key) == 0) return n;
        }
    }
    /* try ht[1] if rehashing */
    if (hash->rehash_idx >= 0 && hash->ht[1].nodes && hash->ht[1].count > 0) {
        uint32_t hv = _hash_raw(key);
        int idx = _hash_idx(hv, hash->ht[1].max_slots);
        for (hashnode_t *n = hash->ht[1].nodes[idx]; n; n = n->next) {
            if (strcmp(n->key, key) == 0) return n;
        }
    }
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
    uint32_t hv = _hash_raw(key);

    /* if rehashing, insert into ht[1]; else ht[0] */
    hashtable_t *target = (hash->rehash_idx >= 0) ? &hash->ht[1] : &hash->ht[0];
    int idx = _hash_idx(hv, target->max_slots);

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

    uint32_t hv = _hash_raw(key);
    hashtable_t *target = (hash->rehash_idx >= 0) ? &hash->ht[1] : &hash->ht[0];
    int idx = _hash_idx(hv, target->max_slots);

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

    uint32_t hv = _hash_raw(key);

    /* search ht[0] */
    if (hash->ht[0].nodes && hash->ht[0].count > 0) {
        int idx = _hash_idx(hv, hash->ht[0].max_slots);
        hashnode_t *cur = hash->ht[0].nodes[idx], *prev = NULL;
        while (cur) {
            if (strcmp(cur->key, key) == 0) {
                if (prev) prev->next = cur->next;
                else hash->ht[0].nodes[idx] = cur->next;
                kvs_free(cur);  /* single alloc */
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
                kvs_free(cur);  /* single alloc */
                if (hash->ht[1].count > 0) hash->ht[1].count--;
                _maybe_shrink(hash);
                return 0;
            }
            prev = cur; cur = cur->next;
        }
    }

    return 1;
}
