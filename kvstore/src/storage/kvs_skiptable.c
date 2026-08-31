#include "kvstore/kvstore.h"

#define KVS_SKIPLIST_MAX_LEVEL 12
#define KVS_SKIPLIST_P 0.5

typedef struct kvs_skipnode_s {
    char *key;
    char *value;
    int level;
    struct kvs_skipnode_s **forward;
} kvs_skipnode_t;

struct kvs_skiptable_s {
    int level;
    int count;
    kvs_skipnode_t *header;
};

kvs_skiptable_t global_skiptable = {0};

static kvs_skipnode_t *skipnode_create(int level, const char *key, const char *value) {
    kvs_skipnode_t *node = (kvs_skipnode_t *)kvs_calloc(1, sizeof(*node));
    if (!node) return NULL;

    node->forward = (kvs_skipnode_t **)kvs_calloc((size_t)level + 1, sizeof(kvs_skipnode_t *));
    if (!node->forward) {
        kvs_free(node);
        return NULL;
    }

    node->level = level;

    if (key) {
        size_t klen = strlen(key);
        node->key = (char *)kvs_malloc(klen + 1);
        if (!node->key) {
            kvs_free(node->forward);
            kvs_free(node);
            return NULL;
        }
        memcpy(node->key, key, klen + 1);
    }

    if (value) {
        size_t vlen = strlen(value);
        node->value = (char *)kvs_malloc(vlen + 1);
        if (!node->value) {
            kvs_free(node->key);
            kvs_free(node->forward);
            kvs_free(node);
            return NULL;
        }
        memcpy(node->value, value, vlen + 1);
    }

    return node;
}

static void skipnode_destroy(kvs_skipnode_t *node) {
    if (!node) return;
    kvs_free(node->key);
    kvs_free(node->value);
    kvs_free(node->forward);
    kvs_free(node);
}

static int random_level(void) {
    int level = 0;
    while ((((double)rand()) / ((double)RAND_MAX + 1.0)) < KVS_SKIPLIST_P && level < KVS_SKIPLIST_MAX_LEVEL) {
        ++level;
    }
    return level;
}

static kvs_skipnode_t *skiptable_search_node(kvs_skiptable_t *inst, char *key, kvs_skipnode_t **update) {
    if (!inst || !inst->header || !key) return NULL;

    kvs_skipnode_t *cur = inst->header;
    for (int i = inst->level; i >= 0; --i) {
        while (cur->forward[i] && strcmp(cur->forward[i]->key, key) < 0) {
            cur = cur->forward[i];
        }
        if (update) update[i] = cur;
    }

    cur = cur->forward[0];
    if (cur && strcmp(cur->key, key) == 0) return cur;
    return NULL;
}

int kvs_skiptable_create(kvs_skiptable_t *inst) {
    if (!inst) return -1;
    memset(inst, 0, sizeof(*inst));
    inst->header = skipnode_create(KVS_SKIPLIST_MAX_LEVEL, NULL, NULL);
    if (!inst->header) return -1;
    inst->level = 0;
    inst->count = 0;
    srand((unsigned int)time(NULL));
    return 0;
}

void kvs_skiptable_destory(kvs_skiptable_t *inst) {
    if (!inst || !inst->header) return;
    kvs_skipnode_t *cur = inst->header->forward[0];
    while (cur) {
        kvs_skipnode_t *next = cur->forward[0];
        skipnode_destroy(cur);
        cur = next;
    }
    skipnode_destroy(inst->header);
    inst->header = NULL;
    inst->level = 0;
    inst->count = 0;
}

int kvs_skiptable_set(kvs_skiptable_t *inst, char *key, char *value) {
    if (!inst || !inst->header || !key || !value) return -1;

    kvs_skipnode_t *update[KVS_SKIPLIST_MAX_LEVEL + 1] = {0};
    kvs_skipnode_t *found = skiptable_search_node(inst, key, update);
    if (found) return kvs_skiptable_mod(inst, key, value);

    int level = random_level();
    if (level > inst->level) {
        for (int i = inst->level + 1; i <= level; ++i) update[i] = inst->header;
        inst->level = level;
    }

    kvs_skipnode_t *node = skipnode_create(level, key, value);
    if (!node) return -2;

    for (int i = 0; i <= level; ++i) {
        node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = node;
    }
    inst->count++;
    return 0;
}

char *kvs_skiptable_get(kvs_skiptable_t *inst, char *key) {
    kvs_skipnode_t *node = skiptable_search_node(inst, key, NULL);
    return node ? node->value : NULL;
}

int kvs_skiptable_mod(kvs_skiptable_t *inst, char *key, char *value) {
    if (!inst || !key || !value) return -1;
    kvs_skipnode_t *node = skiptable_search_node(inst, key, NULL);
    if (!node) return 1;

    size_t vlen = strlen(value);
    char *nv = (char *)kvs_malloc(vlen + 1);
    if (!nv) return -2;
    memcpy(nv, value, vlen + 1);

    kvs_free(node->value);
    node->value = nv;
    return 0;
}

int kvs_skiptable_del(kvs_skiptable_t *inst, char *key) {
    if (!inst || !inst->header || !key) return -1;

    kvs_skipnode_t *update[KVS_SKIPLIST_MAX_LEVEL + 1] = {0};
    kvs_skipnode_t *node = skiptable_search_node(inst, key, update);
    if (!node) return 1;

    for (int i = 0; i <= inst->level; ++i) {
        if (update[i]->forward[i] != node) break;
        update[i]->forward[i] = node->forward[i];
    }

    while (inst->level > 0 && inst->header->forward[inst->level] == NULL) inst->level--;
    inst->count--;
    skipnode_destroy(node);
    return 0;
}

int kvs_skiptable_exist(kvs_skiptable_t *inst, char *key) {
    return kvs_skiptable_get(inst, key) ? 0 : 1;
}

int kvs_skiptable_foreach(kvs_skiptable_t *inst, kvs_skip_visit_cb cb, void *arg) {
    if (!inst || !inst->header || !cb) return -1;
    for (kvs_skipnode_t *cur = inst->header->forward[0]; cur; cur = cur->forward[0]) {
        int rc = cb(cur->key, cur->value, arg);
        if (rc != 0) return rc;
    }
    return 0;
}
