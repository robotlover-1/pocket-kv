# Hash 引擎渐进式 rehash — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现 Hash 引擎渐进式 rehash + 性能优化，使 100 万 HSET QPS 超过 Redis（>123k）。

**Architecture:** `kvs_hash_t` 改为包容器含 `ht[2]`，负载因子超过 2.0 时触发扩容（×2），每次操作迁移 10 桶。节点缓存 FNV-1a hash 值用于 rehash 免重算。

**Tech Stack:** C

---

### Task 1: 结构体重定义 (`kvstore.h`)

**Files:**
- Modify: `include/kvstore/kvstore.h:134-161`

- [ ] **Step 1: 修改 `hashnode_t` — 加 `hv` 字段**

在 `hashnode_t` 顶部添加 `uint32_t hv`：

```c
#if ENABLE_HASH
typedef struct hashnode_s {
    uint32_t hv;              // cached FNV-1a hash (32-bit, not modulo-reduced)
#if ENABLE_KEY_POINTER
    char *key;
    char *value;
#else
    char key[128];
    char value[512];
#endif
    struct hashnode_s *next;
} hashnode_t;

typedef struct hashtable_s {
    hashnode_t **nodes;
    int max_slots;
    int count;
} hashtable_t;

typedef struct kvs_hash_s {
    hashtable_t ht[2];        // ht[0]: active, ht[1]: expansion target
    int rehash_idx;           // next bucket to migrate, -1 = no rehash in progress
} kvs_hash_t;

extern kvs_hash_t global_hash;
int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destory(kvs_hash_t *hash);
int kvs_hash_set(kvs_hash_t *hash, char *key, char *value);
char *kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);
#endif
```

关键变化：
- `hashnode_t` 加 `uint32_t hv`
- `kvs_hash_t` 从 `typedef struct hashtable_s` 变成 `typedef struct kvs_hash_s`（含 `ht[2]` + `rehash_idx`）
- 所有函数签名从 `hashtable_t *` / `kvs_hash_t *` 统一为 `kvs_hash_t *`

- [ ] **Step 2: 提交**

```bash
git add include/kvstore/kvstore.h
git commit -m "wip: hash struct — hashnode_t + hv, kvs_hash_s + ht[2] + rehash_idx"
```

---

### Task 2: Hash 核心实现 (`kvs_hash.c`)

**Files:**
- Rewrite: `src/storage/kvs_hash.c`

整个文件重写。按函数分段：

- [ ] **Step 1: 创建/销毁 + hash 函数**

```c
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

static hashnode_t *_create_node(uint32_t hv, char *key, char *value) {
    hashnode_t *node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
    if (!node) return NULL;
    memset(node, 0, sizeof(*node));
    node->hv = hv;
    size_t klen = strlen(key), vlen = strlen(value);
    node->key = (char *)kvs_malloc(klen + 1);
    node->value = (char *)kvs_malloc(vlen + 1);
    if (!node->key || !node->value) {
        kvs_free(node->key); kvs_free(node->value); kvs_free(node); return NULL;
    }
    memcpy(node->key, key, klen + 1);
    memcpy(node->value, value, vlen + 1);
    return node;
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
                kvs_free(tmp->key);
                kvs_free(tmp->value);
                kvs_free(tmp);
            }
        }
        kvs_free(hash->ht[t].nodes);
        hash->ht[t].nodes = NULL;
    }
    hash->rehash_idx = -1;
}
```

- [ ] **Step 2: 提交**

```bash
git add src/storage/kvs_hash.c
git commit -m "wip: hash — create/destroy/helpers with hv + HASH_INIT_SIZE=4096"
```

- [ ] **Step 3: rehash_step 渐进迁移 + 扩容触发**

```c
/* returns 1 if rehash completed, 0 if still in progress, -1 on alloc error */
static int _rehash_step(kvs_hash_t *hash, int buckets) {
    if (hash->rehash_idx < 0) return 0;  /* not rehashing */

    hashtable_t *h0 = &hash->ht[0];
    hashtable_t *h1 = &hash->ht[1];
    int migrated = 0;

    while (buckets > 0 && hash->rehash_idx < h0->max_slots) {
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
            buckets--;
        }
        hash->rehash_idx++;
        migrated++;
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
    if (hash->rehash_idx >= 0) return 0;  /* already rehashing */

    hashtable_t *h0 = &hash->ht[0];
    if (h0->count <= h0->max_slots * HASH_LOAD_FACTOR) return 0;

    int new_size = h0->max_slots * 2;
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
```

- [ ] **Step 4: 提交**

```bash
git add src/storage/kvs_hash.c
git commit -m "wip: hash — rehash_step(10 buckets) + load-factor(2.0) trigger"
```

- [ ] **Step 5: 查找辅助 + set/get/del/mod/exist 双表实现**

```c
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

    /* duplicate check across both tables */
    if (_find_node(hash, key)) return 1;

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
    hash->ht[0].count = (hash->rehash_idx >= 0)
        ? hash->ht[0].count  /* count tracked by _rehash_step */
        : target->count;

    /* check if we need to start expanding */
    _maybe_expand(hash);
    return 0;
}

int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value) {
    if (!hash || !key || !value) return -1;
    _rehash_step(hash, REHASH_STEP_BUCKETS);
    hashnode_t *n = _find_node(hash, key);
    if (!n) return 1;
    size_t vlen = strlen(value);
    char *nv = (char *)kvs_malloc(vlen + 1);
    if (!nv) return -2;
    memcpy(nv, value, vlen + 1);
    kvs_free(n->value);
    n->value = nv;
    return 0;
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
                kvs_free(cur->key); kvs_free(cur->value); kvs_free(cur);
                if (hash->ht[0].count > 0) hash->ht[0].count--;
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
                kvs_free(cur->key); kvs_free(cur->value); kvs_free(cur);
                if (hash->ht[1].count > 0) hash->ht[1].count--;
                return 0;
            }
            prev = cur; cur = cur->next;
        }
    }

    return 1;
}
```

- [ ] **Step 6: 提交**

```bash
git add src/storage/kvs_hash.c
git commit -m "feat: hash — dual-table set/get/del/mod/exist + progressive rehash"
```

---

### Task 3: 外部遍历点适配

**Files:**
- Modify: `src/main/kvstore.c:1991-2000,2100-2107`
- Modify: `src/replication/kvs_repl.c:1916-1921`

- [ ] **Step 1: kvstore.c — `snapshot_hash_sink` 适配双表遍历**

```c
static int snapshot_hash_sink(snapshot_sink_t *sink) {
    for (int t = 0; t < 2; t++) {
        if (!global_hash.ht[t].nodes) continue;
        for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
            for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                if (emit_cmd3_sink(sink, "HSET", node->key, node->value) != 0) return -1;
                if (maybe_emit_expire_sink(sink, KVS_ENGINE_HASH, node->key) != 0) return -1;
            }
        }
    }
    return 0;
}
```

- [ ] **Step 2: kvstore.c — `kvs_dump_to_fd` hash 遍历适配**

```c
    /* iterate all hash entries (both tables — each key is in exactly one) */
    for (int t = 0; t < 2; t++) {
        if (!global_hash.ht[t].nodes) continue;
        for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
            for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                DUMP_WRITE_KV(KVS_ENGINE_HASH, node->key, node->value);
            }
        }
    }
```

- [ ] **Step 3: kvs_repl.c — 全量同步计数遍历适配**

```c
            int cnt = 0;
            for (int t = 0; t < 2; t++) {
                if (!global_hash.ht[t].nodes) continue;
                for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
                    for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                        cnt++;
                    }
                }
            }
```

- [ ] **Step 4: 提交**

```bash
git add src/main/kvstore.c src/replication/kvs_repl.c
git commit -m "fix: dump/fullsync — traverse both ht[0] and ht[1] for rehash awareness"
```

---

### Task 4: 构建验证

- [ ] **Step 1: 编译所有 4 组配置**

```bash
make clean && make
```
Expected: 编译成功，无 warning。

- [ ] **Step 2: 快速功能验证**

启动 kvstore 进行基本 HSET/HGET/HDEL 操作：

```bash
./kvstore kvstore.conf --role master --mem libc --net reactor &
sleep 2
redis-cli -p 5160 HSET test_key hello
redis-cli -p 5160 HGET test_key
redis-cli -p 5160 HDEL test_key
redis-cli -p 5160 HGET test_key
```
Expected:
```
(integer) 1
"hello"
(integer) 1
(nil)
```

- [ ] **Step 3: 提交**

```bash
git commit --allow-empty -m "verify: build passes + basic hash ops functional"
```

---

### Task 5: 性能验证

- [ ] **Step 1: 运行 AOF 基准测试**

```bash
bash tools/bench/run_persist_bench.sh
```

- [ ] **Step 2: 检查 kvstore HSET QPS**

```bash
grep "kvstore_aof_disable" benchmarks/data/persist_bench/aof_summary.csv
```

Expected: `kvstore_aof_disable,<QPS>` 其中 QPS > 123,000（超过 Redis 无 AOF 的 122,865）。

- [ ] **Step 3: 检查 SAVE 100w 写入 QPS**

```bash
grep "100w_save_1" benchmarks/data/persist_bench/save_summary.csv
```

Expected: write_qps > 123,000。

- [ ] **Step 4: 更新 README 性能基准数据**

用新数据更新 README 中 AOF 和 SAVE 的表格数字。与之前一样的流程。

- [ ] **Step 5: 提交**

```bash
git add benchmarks/data/persist_bench/ README.md
git commit -m "bench: hash rehash 后性能数据 — kvstore HSET > Redis"
```

---

### Task 6: 边界测试

- [ ] **Step 1: rehash 期间 BGSAVE/SAVE 正确性**

```bash
# 灌 100w HSET → BGSAVE → check dump file exists
redis-cli -p 5160 HSET k:1 v:1
# ... (use redis-benchmark 1M)
redis-cli -p 5160 BGSAVE
sleep 5
ls -la kvstore.dump
```
Expected: dump 文件存在且非空。

- [ ] **Step 2: rehash 期间 shutdown 不泄漏**

```bash
# 灌数据触发 rehash → kill -TERM → 检查无内存泄漏
./kvstore kvstore.conf --role master &
# 快速灌 500k HSET (触发至少 1 次扩容)
redis-benchmark -p 5160 -n 500000 -c 50 -P 1 -d 64 -r 500000 HSET key:__rand_int__ value
kill %1
# valgrind 检查（如已安装）
valgrind --leak-check=full ./kvstore kvstore.conf --role master 2>&1 | grep "definitely lost"
```
Expected: 无 leak。

- [ ] **Step 3: 提交**

```bash
git commit --allow-empty -m "verify: rehash edge cases — dump + shutdown no-leak"
```
