#include "kvstore/kvstore.h"

#if ENABLE_DOC

kvs_doc_table_t global_doc = {0};

static unsigned int doc_hash(const char *key, int buckets) {
    unsigned int h = 2166136261u;
    while (*key) {
        h ^= (unsigned char)(*key++);
        h *= 16777619u;
    }
    return h % (unsigned int)buckets;
}

int kvs_doc_create(kvs_doc_table_t *tab) {
    if (!tab) return -1;
    tab->buckets = (kvs_doc_t **)kvs_calloc(KVS_DOC_BUCKETS, sizeof(kvs_doc_t *));
    if (!tab->buckets) return -1;
    tab->size = KVS_DOC_BUCKETS;
    tab->count = 0;
    return 0;
}

static kvs_doc_t *doc_find(kvs_doc_table_t *tab, const char *key) {
    if (!tab || !tab->buckets || !key) return NULL;
    unsigned int idx = doc_hash(key, tab->size);
    for (kvs_doc_t *d = tab->buckets[idx]; d; d = d->next) {
        if (strcmp(d->key, key) == 0) return d;
    }
    return NULL;
}

/* 顶层表扩容重链：固定 1024 桶在大量 key 时每桶链长线性增长（doc_find 退化 O(N)），
 * 负载因子 ≥1 时翻倍并重链，保持 amortized O(1)。 */
static int doc_rehash(kvs_doc_table_t *tab) {
    int new_size = tab->size * 2;
    if (new_size > (1 << 22)) return -1;  /* 上限 4M 桶 */
    kvs_doc_t **nb = (kvs_doc_t **)kvs_calloc(new_size, sizeof(kvs_doc_t *));
    if (!nb) return -1;
    for (int i = 0; i < tab->size; ++i) {
        kvs_doc_t *d = tab->buckets[i];
        while (d) {
            kvs_doc_t *next = d->next;
            unsigned int idx = doc_hash(d->key, new_size);
            d->next = nb[idx];
            nb[idx] = d;
            d = next;
        }
    }
    kvs_free(tab->buckets);
    tab->buckets = nb;
    tab->size = new_size;
    return 0;
}

static kvs_doc_t *doc_create_entry(kvs_doc_table_t *tab, const char *key) {
    /* 先扩容（用 rehash 后的 size 计算新 doc 的桶位） */
    if (tab->count >= tab->size && doc_rehash(tab) != 0) return NULL;
    kvs_doc_t *d = (kvs_doc_t *)kvs_calloc(1, sizeof(*d));
    if (!d) return NULL;
    d->key = (char *)kvs_malloc(strlen(key) + 1);
    if (!d->key) { kvs_free(d); return NULL; }
    strcpy(d->key, key);
    d->bucket_count = KVS_DOC_FIELD_BUCKETS;
    d->fields = (kvs_doc_field_t **)kvs_calloc(d->bucket_count, sizeof(kvs_doc_field_t *));
    if (!d->fields) { kvs_free(d->key); kvs_free(d); return NULL; }
    d->field_count = 0;

    unsigned int idx = doc_hash(key, tab->size);
    d->next = tab->buckets[idx];
    tab->buckets[idx] = d;
    tab->count++;
    return d;
}

static kvs_doc_field_t *field_find(kvs_doc_t *d, const char *name) {
    if (!d || !d->fields || !name) return NULL;
    unsigned int idx = doc_hash(name, d->bucket_count);
    for (kvs_doc_field_t *f = d->fields[idx]; f; f = f->next) {
        if (strcmp(f->name, name) == 0) return f;
    }
    return NULL;
}

int kvs_doc_set(kvs_doc_table_t *tab, const char *key, const char *field, const char *value) {
    if (!tab || !key || !field || !value) return -1;
    kvs_doc_t *d = doc_find(tab, key);
    if (!d) {
        d = doc_create_entry(tab, key);
        if (!d) return -1;
    }

    kvs_doc_field_t *f = field_find(d, field);
    if (f) {
        char *nv = (char *)kvs_malloc(strlen(value) + 1);
        if (!nv) return -1;
        strcpy(nv, value);
        kvs_free(f->value);
        f->value = nv;
        return 0;
    }

    f = (kvs_doc_field_t *)kvs_calloc(1, sizeof(*f));
    if (!f) return -1;
    f->name = (char *)kvs_malloc(strlen(field) + 1);
    f->value = (char *)kvs_malloc(strlen(value) + 1);
    if (!f->name || !f->value) {
        kvs_free(f->name);
        kvs_free(f->value);
        kvs_free(f);
        return -1;
    }
    strcpy(f->name, field);
    strcpy(f->value, value);

    unsigned int idx = doc_hash(field, d->bucket_count);
    f->next = d->fields[idx];
    d->fields[idx] = f;
    d->field_count++;
    return 0;
}

char *kvs_doc_get(kvs_doc_table_t *tab, const char *key, const char *field) {
    if (!tab || !key || !field) return NULL;
    kvs_doc_t *d = doc_find(tab, key);
    if (!d) return NULL;
    kvs_doc_field_t *f = field_find(d, field);
    return f ? f->value : NULL;
}

int kvs_doc_del_field(kvs_doc_table_t *tab, const char *key, const char *field) {
    if (!tab || !key || !field) return -1;
    kvs_doc_t *d = doc_find(tab, key);
    if (!d) return 1;
    unsigned int idx = doc_hash(field, d->bucket_count);
    kvs_doc_field_t *cur = d->fields[idx], *prev = NULL;
    while (cur) {
        if (strcmp(cur->name, field) == 0) {
            if (prev) prev->next = cur->next;
            else d->fields[idx] = cur->next;
            kvs_free(cur->name);
            kvs_free(cur->value);
            kvs_free(cur);
            d->field_count--;
            return 0;
        }
        prev = cur;
        cur = cur->next;
    }
    return 1;
}

static void doc_free_fields(kvs_doc_t *d) {
    if (!d || !d->fields) return;
    for (int i = 0; i < d->bucket_count; ++i) {
        kvs_doc_field_t *f = d->fields[i];
        while (f) {
            kvs_doc_field_t *next = f->next;
            kvs_free(f->name);
            kvs_free(f->value);
            kvs_free(f);
            f = next;
        }
    }
    kvs_free(d->fields);
    d->fields = NULL;
    d->field_count = 0;
}

int kvs_doc_del(kvs_doc_table_t *tab, const char *key) {
    if (!tab || !tab->buckets || !key) return -1;
    unsigned int idx = doc_hash(key, tab->size);
    kvs_doc_t *cur = tab->buckets[idx], *prev = NULL;
    while (cur) {
        if (strcmp(cur->key, key) == 0) {
            if (prev) prev->next = cur->next;
            else tab->buckets[idx] = cur->next;
            doc_free_fields(cur);
            kvs_free(cur->key);
            kvs_free(cur);
            tab->count--;
            return 0;
        }
        prev = cur;
        cur = cur->next;
    }
    return 1;
}

int kvs_doc_exist(kvs_doc_table_t *tab, const char *key) {
    return doc_find(tab, key) ? 0 : 1;
}

int kvs_doc_field_exist(kvs_doc_table_t *tab, const char *key, const char *field) {
    kvs_doc_t *d = doc_find(tab, key);
    if (!d) return 1;
    return field_find(d, field) ? 0 : 1;
}

int kvs_doc_field_count(kvs_doc_table_t *tab, const char *key) {
    kvs_doc_t *d = doc_find(tab, key);
    return d ? d->field_count : -1;
}

int kvs_doc_foreach_field(kvs_doc_table_t *tab, const char *key, kvs_doc_field_visit_cb cb, void *arg) {
    if (!tab || !key || !cb) return -1;
    kvs_doc_t *d = doc_find(tab, key);
    if (!d) return 1;
    for (int i = 0; i < d->bucket_count; ++i) {
        for (kvs_doc_field_t *f = d->fields[i]; f; f = f->next) {
            if (cb(f->name, f->value, arg) != 0) return -1;
        }
    }
    return 0;
}

int kvs_doc_foreach(kvs_doc_table_t *tab, kvs_doc_visit_cb cb, void *arg) {
    if (!tab || !tab->buckets || !cb) return -1;
    for (int i = 0; i < tab->size; ++i) {
        for (kvs_doc_t *d = tab->buckets[i]; d; d = d->next) {
            if (cb(d->key, d, arg) != 0) return -1;
        }
    }
    return 0;
}

void kvs_doc_destroy(kvs_doc_table_t *tab) {
    if (!tab || !tab->buckets) return;
    for (int i = 0; i < tab->size; ++i) {
        kvs_doc_t *d = tab->buckets[i];
        while (d) {
            kvs_doc_t *next = d->next;
            doc_free_fields(d);
            kvs_free(d->key);
            kvs_free(d);
            d = next;
        }
    }
    kvs_free(tab->buckets);
    tab->buckets = NULL;
    tab->size = 0;
    tab->count = 0;
}

#endif
