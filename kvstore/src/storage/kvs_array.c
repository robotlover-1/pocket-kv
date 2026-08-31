#include "kvstore/kvstore.h"

kvs_array_t global_array = {0};

int kvs_array_create(kvs_array_t *inst) {
    if (!inst) return -1;
    /* 惰性分配 table：首次 SET 时才分配 1M 槽 × 16B = 16MB，
     * 避免未使用 array 引擎（如纯 hash 负载）时启动即占 16MB 基线内存。 */
    inst->table = NULL;
    inst->idx = 0;
    inst->total = 0;
    inst->next_slot = 0;
    inst->free_list = NULL;
    inst->free_count = 0;
    inst->free_cap = 0;
    /* key→slot 哈希索引：O(1) 查找，替代 find_slot 的 O(N) 线性扫描 */
    inst->index = (kvs_hash_t *)kvs_malloc(sizeof(kvs_hash_t));
    if (!inst->index) return -1;
    return kvs_hash_create(inst->index);
}

void kvs_array_destory(kvs_array_t *inst) {
    if (!inst) return;
    if (inst->index) {
        kvs_hash_destory(inst->index);
        kvs_free(inst->index);
        inst->index = NULL;
    }
    if (inst->free_list) {
        kvs_free(inst->free_list);
        inst->free_list = NULL;
    }
    if (!inst->table) return;
    for (int i = 0; i < KVS_ARRAY_SIZE; ++i) {
        kvs_free(inst->table[i].key);
        kvs_free(inst->table[i].value);
    }
    kvs_free(inst->table);
    inst->table = NULL;
    inst->total = 0;
}

static int find_slot(kvs_array_t *inst, char *key) {
    if (!inst || !inst->index || !key) return -1;
    const char *v = kvs_hash_get(inst->index, key);
    if (!v) return -1;
    int slot = atoi(v);
    return (slot >= 0 && slot < KVS_ARRAY_SIZE) ? slot : -1;
}

int kvs_array_set(kvs_array_t *inst, char *key, char *value) {
    if (!inst || !key || !value) return -1;
    /* 惰性分配 table（见 kvs_array_create） */
    if (!inst->table) {
        inst->table = (kvs_array_item_t *)kvs_malloc(KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
        if (!inst->table) return -2;
        memset(inst->table, 0, KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
    }
    if (find_slot(inst, key) >= 0) return kvs_array_mod(inst, key, value);

    /* O(1) 槽位分配：优先复用空闲槽，否则取 next_slot（替代 O(N) 空槽扫描） */
    int slot;
    if (inst->free_count > 0) {
        slot = inst->free_list[--inst->free_count];
    } else {
        slot = inst->next_slot++;
        if (slot >= KVS_ARRAY_SIZE) return -1;  /* 数组满 */
    }

    size_t klen = strlen(key), vlen = strlen(value);
    inst->table[slot].key = (char *)kvs_malloc(klen + 1);
    inst->table[slot].value = (char *)kvs_malloc(vlen + 1);
    if (!inst->table[slot].key || !inst->table[slot].value) {
        kvs_free(inst->table[slot].key);
        kvs_free(inst->table[slot].value);
        inst->table[slot].key = NULL;
        inst->table[slot].value = NULL;
        if (inst->free_count < inst->free_cap)
            inst->free_list[inst->free_count++] = slot;
        return -2;
    }
    memcpy(inst->table[slot].key, key, klen + 1);
    memcpy(inst->table[slot].value, value, vlen + 1);
    inst->total++;

    char slotbuf[16];
    snprintf(slotbuf, sizeof(slotbuf), "%d", slot);
    if (kvs_hash_set(inst->index, key, slotbuf) != 0) {
        /* 索引写失败（OOM）：回滚 table 槽位，槽位退回空闲栈 */
        kvs_free(inst->table[slot].key);
        kvs_free(inst->table[slot].value);
        inst->table[slot].key = NULL;
        inst->table[slot].value = NULL;
        inst->total--;
        if (inst->free_count < inst->free_cap)
            inst->free_list[inst->free_count++] = slot;
        return -2;
    }
    return 0;
}

char *kvs_array_get(kvs_array_t *inst, char *key) {
    int idx = find_slot(inst, key);
    return idx >= 0 ? inst->table[idx].value : NULL;
}

int kvs_array_del(kvs_array_t *inst, char *key) {
    int idx = find_slot(inst, key);
    if (idx < 0) return 1;
    kvs_free(inst->table[idx].key);
    kvs_free(inst->table[idx].value);
    inst->table[idx].key = NULL;
    inst->table[idx].value = NULL;
    if (inst->total > 0) inst->total--;

    /* 槽位入空闲栈，供后续 set 复用（保持 O(1)） */
    if (inst->free_count == inst->free_cap) {
        int new_cap = inst->free_cap ? inst->free_cap * 2 : 16;
        int *nf = (int *)kvs_realloc(inst->free_list, (size_t)new_cap * sizeof(int));
        if (nf) {
            inst->free_list = nf;
            inst->free_cap = new_cap;
        }
    }
    if (inst->free_count < inst->free_cap)
        inst->free_list[inst->free_count++] = idx;

    kvs_hash_del(inst->index, key);
    return 0;
}

int kvs_array_mod(kvs_array_t *inst, char *key, char *value) {
    int idx = find_slot(inst, key);
    if (idx < 0) return 1;
    size_t vlen = strlen(value);
    char *nv = (char *)kvs_malloc(vlen + 1);
    if (!nv) return -2;
    memcpy(nv, value, vlen + 1);
    kvs_free(inst->table[idx].value);
    inst->table[idx].value = nv;
    return 0;
}

int kvs_array_exist(kvs_array_t *inst, char *key) {
    return find_slot(inst, key) >= 0 ? 0 : 1;
}
