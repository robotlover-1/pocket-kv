#include "kvstore/kvstore.h"

kvs_expire_table_t global_expire = {0};

static unsigned int expire_hash(const char *key, int engine, size_t buckets) {
    unsigned int h = 2166136261u;
    while (*key) {
        h ^= (unsigned char)(*key++);
        h *= 16777619u;
    }
    h ^= (unsigned int)engine;
    h *= 16777619u;
    return h % buckets;
}

long long kvs_now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

long long kvs_now_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000000 + tv.tv_usec;
}

static void heap_swap(kvs_expire_table_t *tab, size_t i, size_t j) {
    kvs_expire_item_t *tmp;
    if (!tab || i == j) return;
    tmp = tab->heap[i];
    tab->heap[i] = tab->heap[j];
    tab->heap[j] = tmp;
    tab->heap[i]->heap_index = i;
    tab->heap[j]->heap_index = j;
}

static void heap_sift_up(kvs_expire_table_t *tab, size_t idx) {
    while (idx > 0) {
        size_t parent = (idx - 1) / 2;
        if (tab->heap[parent]->expire_at_ms <= tab->heap[idx]->expire_at_ms) break;
        heap_swap(tab, parent, idx);
        idx = parent;
    }
}

static void heap_sift_down(kvs_expire_table_t *tab, size_t idx) {
    for (;;) {
        size_t left = idx * 2 + 1;
        size_t right = left + 1;
        size_t smallest = idx;
        if (left < tab->heap_size && tab->heap[left]->expire_at_ms < tab->heap[smallest]->expire_at_ms)
            smallest = left;
        if (right < tab->heap_size && tab->heap[right]->expire_at_ms < tab->heap[smallest]->expire_at_ms)
            smallest = right;
        if (smallest == idx) break;
        heap_swap(tab, idx, smallest);
        idx = smallest;
    }
}

static int heap_ensure_cap(kvs_expire_table_t *tab) {
    size_t new_cap;
    kvs_expire_item_t **new_heap;
    if (!tab) return -1;
    if (tab->heap_size < tab->heap_cap) return 0;
    new_cap = tab->heap_cap ? tab->heap_cap * 2 : KVS_EXPIRE_HEAP_INIT_CAP;
    new_heap = (kvs_expire_item_t **)kvs_realloc(tab->heap, new_cap * sizeof(*new_heap));
    if (!new_heap) return -1;
    tab->heap = new_heap;
    tab->heap_cap = new_cap;
    return 0;
}

static int heap_push(kvs_expire_table_t *tab, kvs_expire_item_t *node) {
    if (!tab || !node) return -1;
    if (heap_ensure_cap(tab) != 0) return -1;
    tab->heap[tab->heap_size] = node;
    node->heap_index = tab->heap_size;
    tab->heap_size++;
    heap_sift_up(tab, node->heap_index);
    return 0;
}

static void heap_remove_at(kvs_expire_table_t *tab, size_t idx) {
    size_t last;
    if (!tab || idx >= tab->heap_size) return;
    last = tab->heap_size - 1;
    if (idx != last) {
        heap_swap(tab, idx, last);
    }
    tab->heap[last]->heap_index = (size_t)-1;
    tab->heap[last] = NULL;
    tab->heap_size--;
    if (idx < tab->heap_size) {
        heap_sift_down(tab, idx);
        heap_sift_up(tab, idx);
    }
}

static void heap_update(kvs_expire_table_t *tab, kvs_expire_item_t *node) {
    if (!tab || !node || node->heap_index >= tab->heap_size) return;
    heap_sift_down(tab, node->heap_index);
    heap_sift_up(tab, node->heap_index);
}

static kvs_expire_item_t *expire_find(kvs_expire_table_t *tab, int engine, const char *key) {
    unsigned int idx;
    kvs_expire_item_t *cur;
    if (!tab || !tab->buckets || !key) return NULL;
    idx = expire_hash(key, engine, tab->size);
    for (cur = tab->buckets[idx]; cur; cur = cur->next) {
        if (cur->engine == engine && strcmp(cur->key, key) == 0) return cur;
    }
    return NULL;
}

static void expire_unlink_bucket(kvs_expire_table_t *tab, kvs_expire_item_t *node) {
    unsigned int idx;
    kvs_expire_item_t *cur, *prev = NULL;
    if (!tab || !tab->buckets || !node) return;
    idx = expire_hash(node->key, node->engine, tab->size);
    cur = tab->buckets[idx];
    while (cur) {
        if (cur == node) {
            if (prev) prev->next = cur->next;
            else tab->buckets[idx] = cur->next;
            return;
        }
        prev = cur;
        cur = cur->next;
    }
}

static void expire_free_node(kvs_expire_table_t *tab, kvs_expire_item_t *node) {
    if (!tab || !node) return;
    if (node->heap_index < tab->heap_size) heap_remove_at(tab, node->heap_index);
    expire_unlink_bucket(tab, node);
    kvs_free(node->key);
    kvs_free(node);
    if (tab->count > 0) tab->count--;
}

int kvs_expire_create(kvs_expire_table_t *tab) {
    if (!tab) return -1;
    tab->buckets = (kvs_expire_item_t **)kvs_calloc(KVS_EXPIRE_BUCKETS, sizeof(kvs_expire_item_t *));
    if (!tab->buckets) return -1;
    tab->size = KVS_EXPIRE_BUCKETS;
    tab->count = 0;
    tab->heap = (kvs_expire_item_t **)kvs_calloc(KVS_EXPIRE_HEAP_INIT_CAP, sizeof(kvs_expire_item_t *));
    if (!tab->heap) {
        kvs_free(tab->buckets);
        tab->buckets = NULL;
        return -1;
    }
    tab->heap_cap = KVS_EXPIRE_HEAP_INIT_CAP;
    tab->heap_size = 0;
    return 0;
}

void kvs_expire_destroy(kvs_expire_table_t *tab) {
    size_t i;
    if (!tab || !tab->buckets) return;
    for (i = 0; i < tab->size; ++i) {
        kvs_expire_item_t *cur = tab->buckets[i];
        while (cur) {
            kvs_expire_item_t *next = cur->next;
            kvs_free(cur->key);
            kvs_free(cur);
            cur = next;
        }
    }
    kvs_free(tab->buckets);
    kvs_free(tab->heap);
    tab->buckets = NULL;
    tab->heap = NULL;
    tab->size = 0;
    tab->count = 0;
    tab->heap_size = 0;
    tab->heap_cap = 0;
}

int kvs_expire_set(kvs_expire_table_t *tab, int engine, const char *key, long long ttl_ms) {
    long long expire_at;
    kvs_expire_item_t *node;
    unsigned int idx;
    if (!tab || !tab->buckets || !key || ttl_ms <= 0) return -1;
    expire_at = kvs_now_ms() + ttl_ms;
    node = expire_find(tab, engine, key);
    if (node) {
        node->expire_at_ms = expire_at;
        heap_update(tab, node);
        return 0;
    }
    node = (kvs_expire_item_t *)kvs_calloc(1, sizeof(*node));
    if (!node) return -1;
    node->key = (char *)kvs_malloc(strlen(key) + 1);
    if (!node->key) {
        kvs_free(node);
        return -1;
    }
    strcpy(node->key, key);
    node->engine = engine;
    node->expire_at_ms = expire_at;
    node->heap_index = (size_t)-1;
    idx = expire_hash(key, engine, tab->size);
    node->next = tab->buckets[idx];
    tab->buckets[idx] = node;
    if (heap_push(tab, node) != 0) {
        tab->buckets[idx] = node->next;
        kvs_free(node->key);
        kvs_free(node);
        return -1;
    }
    tab->count++;
    return 0;
}

int kvs_expire_del(kvs_expire_table_t *tab, int engine, const char *key) {
    kvs_expire_item_t *node;
    if (!tab || !tab->buckets || !key) return -1;
    node = expire_find(tab, engine, key);
    if (!node) return 1;
    expire_free_node(tab, node);
    return 0;
}

int kvs_expire_persist(kvs_expire_table_t *tab, int engine, const char *key) {
    return kvs_expire_del(tab, engine, key);
}

int kvs_expire_is_expired(kvs_expire_table_t *tab, int engine, const char *key) {
    kvs_expire_item_t *node = expire_find(tab, engine, key);
    if (!node) return 0;
    return kvs_now_ms() >= node->expire_at_ms;
}

long long kvs_expire_ttl(kvs_expire_table_t *tab, int engine, const char *key) {
    kvs_expire_item_t *node = expire_find(tab, engine, key);
    long long now, remain_ms, s;
    if (!node) return -1;
    now = kvs_now_ms();
    if (now >= node->expire_at_ms) return -2;
    remain_ms = node->expire_at_ms - now;
    s = remain_ms / 1000;
    if (remain_ms % 1000) s += 1;
    return s;
}

static int engine_del(int engine, const char *key);

int kvs_active_expire_cycle(int budget) {
    int removed = 0;
    long long now;
    if (budget <= 0 || !global_expire.heap || global_expire.heap_size == 0) return 0;
    now = kvs_now_ms();
    while (removed < budget && global_expire.heap_size > 0) {
        kvs_expire_item_t *node = global_expire.heap[0];
        if (!node || node->expire_at_ms > now) break;
        engine_del(node->engine, node->key);
        expire_free_node(&global_expire, node);
        removed++;
    }
    return removed;
}

static int engine_del(int engine, const char *key) {
    switch (engine) {
        case KVS_ENGINE_ARRAY: return kvs_array_del(&global_array, (char *)key);
        case KVS_ENGINE_RBTREE: return kvs_rbtree_del(&global_rbtree, (char *)key);
        case KVS_ENGINE_HASH: return kvs_hash_del(&global_hash, (char *)key);
        case KVS_ENGINE_SKIPTABLE: return kvs_skiptable_del(&global_skiptable, (char *)key);
        default: return -1;
    }
}
