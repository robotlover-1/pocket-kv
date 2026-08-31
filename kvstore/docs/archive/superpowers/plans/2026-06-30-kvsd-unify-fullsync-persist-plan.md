# KVSD 统一全量同步与全量持久化 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 全量同步和全量持久化使用相同的 KVSD 二进制格式，扩展支持 TTL。

**Architecture:** 替换 `queue_snapshot()` 中的 `kvs_snapshot_to_fp()` (RESP) 为 `kvs_dump_to_fd()` (KVSD)；在 `parse_resp_stream()` 入口处拦截全量同步数据写入临时文件；扩展 `replay_dump_file()` 支持 flags+TTL 并作为 slave 端的唯一 KVSD 加载路径。

**Tech Stack:** C, mmap, POSIX fd

## Global Constraints

- 最小改动，不改无关代码
- 不删除废弃函数，加注释标记
- KVSD_FLAG_HAS_EXPIRE = 0x01，bits 1-7 保留
- TTL 存储为绝对毫秒时间戳（`kvs_now_ms() + kvs_expire_ttl() * 1000`）
- 旧格式 KVSD 文件不兼容，replay 时 engine_id > 5 报错

---

## 文件结构

| 文件 | 职责 | 操作 |
|---|---|---|
| `include/kvstore/kvstore.h` | 声明 replay_dump_file()，定义 KVSD_FLAG_HAS_EXPIRE | Modify |
| `src/main/kvstore.c` | kvs_dump_to_fd() 扩展、queue_snapshot() 替换、parse_resp_stream() 拦截、废弃标记 | Modify |
| `src/persistence/kvs_persist.c` | replay_dump_file() 去 static + flags/TTL 解析 | Modify |
| `src/replication/kvs_repl.c` | repl_slave_set_sync_state() 打开临时文件、repl_slave_finish_fullsync() 改用 replay_dump_file、全局变量 | Modify |

---

### Task 1: Header — 声明和常量

**Files:**
- Modify: `include/kvstore/kvstore.h`

**Interfaces:**
- Produces: `#define KVSD_FLAG_HAS_EXPIRE 0x01`, `unsigned long long replay_dump_file(const char *path);`

- [ ] **Step 1: 添加 KVSD 常量和 replay_dump_file 声明**

在 `include/kvstore/kvstore.h` 中，在 `kvs_dump_to_fd` 声明附近（line 505-506）添加：

```c
/* KVSD format flags */
#define KVSD_FLAG_HAS_EXPIRE  0x01   /* record has 8-byte expire_ms after value */

/* KVSD load (used by both persist recovery and replication fullsync) */
unsigned long long replay_dump_file(const char *path);
```

在 `kvs_dump_to_fd` 声明（line 505）之后：
```c
int kvs_dump_to_fd(int fd, unsigned long long aof_offset);
unsigned long long replay_dump_file(const char *path);   /* <-- 新增 */
int kvs_load_dump_from_fd(int fd);
```

- [ ] **Step 2: 构建验证**

```bash
make clean && make
```

验证：编译通过，无新增 warning。

- [ ] **Step 3: Commit**

```bash
git add include/kvstore/kvstore.h
git commit -m "feat: add KVSD_FLAG_HAS_EXPIRE and replay_dump_file declaration"
```

---

### Task 2: kvs_dump_to_fd() — 扩展 flags + TTL

**Files:**
- Modify: `src/main/kvstore.c:2146-2225`

**Interfaces:**
- Consumes: `KVSD_FLAG_HAS_EXPIRE` from kvstore.h, `kvs_expire_ttl()` (existing), `kvs_now_ms()` (existing)
- Produces: New KVSD format with flags byte in every record

- [ ] **Step 1: 替换 DUMP_WRITE_KV 宏并更新调用点**

在 `kvs_dump_to_fd()` 中（当前 line 2153），替换 `DUMP_WRITE_KV` 为 `DUMP_WRITE_KV_EX`，并更新所有 5 个引擎的 `DUMP_WRITE_KV` 调用为 `DUMP_WRITE_KV_EX`：

```c
int kvs_dump_to_fd(int fd, unsigned long long aof_offset) {
    if (fd < 0) return -1;

    /* header: AOF file size at dump creation time */
    if (write(fd, &aof_offset, sizeof(aof_offset)) != sizeof(aof_offset)) return -1;

#define DUMP_WRITE_KV_EX(engine_id, key, value) do {                 \
    uint8_t  _eng = (uint8_t)(engine_id);                             \
    uint8_t  _flags = 0;                                              \
    uint32_t _klen = (uint32_t)strlen(key);                           \
    uint32_t _vlen = (uint32_t)strlen(value);                         \
    long long _ttl_sec = kvs_expire_ttl(&global_expire, _eng, key);   \
    uint64_t _exp = 0;                                                \
    if (_ttl_sec >= 0) { _flags = KVSD_FLAG_HAS_EXPIRE;               \
        _exp = (uint64_t)(kvs_now_ms() + _ttl_sec * 1000); }          \
    if (write(fd, &_eng, sizeof(_eng)) != sizeof(_eng)) return -1;   \
    if (write(fd, &_flags, sizeof(_flags)) != sizeof(_flags)) return -1; \
    if (write(fd, &_klen, sizeof(_klen)) != sizeof(_klen)) return -1; \
    if (write(fd, key, _klen) != (ssize_t)_klen) return -1;           \
    if (write(fd, &_vlen, sizeof(_vlen)) != sizeof(_vlen)) return -1; \
    if (write(fd, value, _vlen) != (ssize_t)_vlen) return -1;         \
    if (_flags & KVSD_FLAG_HAS_EXPIRE)                                 \
        if (write(fd, &_exp, sizeof(_exp)) != sizeof(_exp)) return -1; \
} while(0)

    /* iterate all hash entries */
    for (int t = 0; t < 2; t++) {
        if (!global_hash.ht[t].nodes) continue;
        for (int i = 0; i < global_hash.ht[t].max_slots; ++i) {
            for (hashnode_t *node = global_hash.ht[t].nodes[i]; node; node = node->next) {
                DUMP_WRITE_KV_EX(KVS_ENGINE_HASH, node->key, node->value);
            }
        }
    }

    /* iterate array entries */
    for (int i = 0; i < KVS_ARRAY_SIZE; ++i) {
        if (global_array.table && global_array.table[i].key) {
            DUMP_WRITE_KV_EX(KVS_ENGINE_ARRAY, global_array.table[i].key, global_array.table[i].value);
        }
    }

    /* iterate rbtree entries */
    {
        rbtree_node *nil = global_rbtree.nil;
        rbtree_node **stack = (rbtree_node **)kvs_malloc(sizeof(rbtree_node*) * 256);
        int top = 0;
        rbtree_node *cur = global_rbtree.root;
        if (stack) {
            while (cur != nil || top > 0) {
                while (cur != nil) {
                    stack[top++] = cur;
                    cur = cur->left;
                }
                cur = stack[--top];
                DUMP_WRITE_KV_EX(KVS_ENGINE_RBTREE, cur->key, (char*)cur->value);
                cur = cur->right;
            }
            kvs_free(stack);
        }
    }

    /* iterate skiptable entries */
    /* 注意: dump_skiptable_write_kv 需要同步改为 DUMP_WRITE_KV_EX，或内联替换 */
    kvs_skiptable_foreach(&global_skiptable, dump_skiptable_write_kv, &fd);

    /* iterate doc entries */
    {
        char doc_buf[BUFFER_CAP];
        for (int i = 0; i < global_doc.size; ++i) {
            for (kvs_doc_t *d = global_doc.buckets[i]; d; d = d->next) {
                int doc_pos = 0;
                for (int j = 0; j < d->bucket_count && doc_pos < (int)sizeof(doc_buf) - 4; ++j) {
                    for (kvs_doc_field_t *f = d->fields[j]; f; f = f->next) {
                        int n = snprintf(doc_buf + doc_pos, sizeof(doc_buf) - (size_t)doc_pos,
                            "%s=%s ", f->name, f->value);
                        if (n > 0) doc_pos += n;
                    }
                }
                if (doc_pos > 0 && doc_buf[doc_pos-1] == ' ') doc_pos--;
                doc_buf[doc_pos] = '\0';
                DUMP_WRITE_KV_EX(KVS_ENGINE_DOC, d->key, doc_buf);
            }
        }
    }

#undef DUMP_WRITE_KV_EX
    return 0;
}
```

同时更新 `dump_skiptable_write_kv()`（当前 line 2133-2144），替换其内部写逻辑为直接调用新的记录格式，或者改为内联 `DUMP_WRITE_KV_EX` 调用。最简单的方式是让 `dump_skiptable_write_kv` 也使用相同的格式——将其内部逻辑更新为与 `DUMP_WRITE_KV_EX` 一致：

```c
static int dump_skiptable_write_kv(const char *key, const char *value, void *arg) {
    int fd = *(int *)arg;
    uint8_t eng = (uint8_t)KVS_ENGINE_SKIPTABLE;
    uint8_t flags = 0;
    uint32_t klen = (uint32_t)strlen(key);
    uint32_t vlen = (uint32_t)strlen(value);
    long long ttl_sec = kvs_expire_ttl(&global_expire, KVS_ENGINE_SKIPTABLE, key);
    uint64_t exp = 0;
    if (ttl_sec >= 0) { flags = KVSD_FLAG_HAS_EXPIRE; exp = (uint64_t)(kvs_now_ms() + ttl_sec * 1000); }
    if (write(fd, &eng, sizeof(eng)) != sizeof(eng)) return -1;
    if (write(fd, &flags, sizeof(flags)) != sizeof(flags)) return -1;
    if (write(fd, &klen, sizeof(klen)) != sizeof(klen)) return -1;
    if (write(fd, key, klen) != (ssize_t)klen) return -1;
    if (write(fd, &vlen, sizeof(vlen)) != sizeof(vlen)) return -1;
    if (write(fd, value, vlen) != (ssize_t)vlen) return -1;
    if (flags & KVSD_FLAG_HAS_EXPIRE) {
        if (write(fd, &exp, sizeof(exp)) != sizeof(exp)) return -1;
    }
    return 0;
}
```

- [ ] **Step 2: 构建验证**

```bash
make clean && make
```

预期：编译通过，无新增 warning。

- [ ] **Step 3: Commit**

```bash
git add src/main/kvstore.c
git commit -m "feat: extend kvs_dump_to_fd with flags+TTL in KVSD format"
```

---

### Task 3: replay_dump_file() — 去 static + flags/TTL 解析 + sanity check

**Files:**
- Modify: `src/persistence/kvs_persist.c:319-423`

**Interfaces:**
- Consumes: `KVSD_FLAG_HAS_EXPIRE` from kvstore.h, `kvs_expire_set()`, `kvs_now_ms()`
- Produces: `unsigned long long replay_dump_file(const char *path)` — loads KVSD file, restores engine data + TTL, returns aof_offset

- [ ] **Step 1: 修改 replay_dump_file 签名和记录解析**

在 `src/persistence/kvs_persist.c` 中：

1. 删除 `static` 关键字（line 319）
2. 在记录解析循环（line 342-416）中，在 `engine_id = mapped[pos++]` 之后插入 flags 读取
3. 在 value 读取之后，条件读取 expire_ms
4. 在 engine dispatch 之后，条件调用 `kvs_expire_set()`
5. 增加 engine_id sanity check

具体修改点：

**修改 1 — line 319:** `static unsigned long long` → `unsigned long long`

**修改 2 — line 343 之后插入 flags 读取：**

原代码 `engine_id = mapped[pos++];` 之后，`klen` 读取之前，插入：

```c
        engine_id = mapped[pos++];

        /* sanity check: engine_id must be 1-5 */
        if (engine_id < 1 || engine_id > 5) {
            fprintf(stderr, "replay_dump_file: invalid engine_id %u at pos %zu (old format KVSD file?)\n",
                (unsigned int)engine_id, pos - 1);
            break;
        }

        /* read flags (new in unified KVSD format) */
        if (pos + 1 > size) break;
        uint8_t flags = mapped[pos++];
```

**修改 3 — dispatch 之后（line 410 `default: break;` 之后），`kvs_free(value)` 之前，插入 TTL 恢复：**

```c
            default:
                break;
        }

        /* restore TTL if present */
        if (flags & KVSD_FLAG_HAS_EXPIRE) {
            uint64_t expire_at_ms;
            if (pos + sizeof(expire_at_ms) <= size) {
                memcpy(&expire_at_ms, mapped + pos, sizeof(expire_at_ms));
                pos += sizeof(expire_at_ms);
                long long ttl_ms = (long long)expire_at_ms - kvs_now_ms();
                if (ttl_ms > 0) {
                    kvs_expire_set(&global_expire, engine_id, key, ttl_ms);
                }
            } else {
                kvs_free(key);
                kvs_free(value);
                break;
            }
        }

        kvs_free(key);
```

- [ ] **Step 2: 构建验证**

```bash
make clean && make
```

预期：编译通过，`replay_dump_file` 符号可供外部链接。

- [ ] **Step 3: Commit**

```bash
git add src/persistence/kvs_persist.c
git commit -m "feat: extend replay_dump_file with flags/TTL parsing and sanity check"
```

---

### Task 4: parse_resp_stream() — 全量同步数据拦截

**Files:**
- Modify: `src/main/kvstore.c:1810` (parse_resp_stream 函数开头)
- Modify: `src/replication/kvs_repl.c` (添加全局变量 `g_slave_fullsync_tmp_fd`，添加辅助函数声明)

**Interfaces:**
- Consumes: `g_slave_loading_fullsync`, `g_slave_fullsync_target_bytes`, `g_slave_fullsync_loaded_bytes`, `g_slave_fullsync_tmp_fd` (new)
- Produces: `repl_slave_fullsync_tmp_fd` EXTERN for kvstore.c to use

**为什么在这里拦截：** `parse_resp_stream()` 是 slave 端所有复制数据（TCP/RDMA/kprobe 等 12+ 个调用点）的唯一解析入口。在这里拦截全量同步数据，只需改一处，覆盖所有接收路径。

- [ ] **Step 1: 在 kvs_repl.c 中添加临时文件 fd 全局变量**

在 `src/replication/kvs_repl.c` 中，在现有 `g_slave_loading_fullsync` 等变量附近（line 85-87）和 `g_slave_fd` 附近添加：

```c
int g_slave_fullsync_tmp_fd = -1;   /* temp file fd for receiving KVSD full sync data */
```

并在 `repl_slave_set_sync_state()` 中（line 1884）增加临时文件管理。在函数末尾（`repl_slave_state_save()` 之前）添加：

```c
    /* Manage fullsync temp file: open when starting fullsync, close when not */
    if (fullsync_loading && g_slave_fullsync_tmp_fd < 0) {
        char tmp_path[512];
        snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.recv.tmp.%ld",
                 g_cfg.dump_path, (long)getpid());
        g_slave_fullsync_tmp_fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (g_slave_fullsync_tmp_fd < 0) {
            fprintf(stderr, "repl: slave fullsync tmp file open failed: %s\n", strerror(errno));
        }
    } else if (!fullsync_loading && g_slave_fullsync_tmp_fd >= 0) {
        /* sync not loading but tmp fd still open — close and cleanup */
        close(g_slave_fullsync_tmp_fd);
        g_slave_fullsync_tmp_fd = -1;
    }
```

- [ ] **Step 2: 在 parse_resp_stream() 开头添加全量同步拦截**

在 `src/main/kvstore.c` 的 `parse_resp_stream()` 函数中，在函数体开头（line 1810 之后，`#define PARSE_SCRATCH` 之前）添加：

```c
int parse_resp_stream(conn_t *c, unsigned char *buf, size_t *len, int from_replication) {
    /* KVSD fullsync interception: during full sync, write raw KVSD bytes
     * to temp file instead of RESP parsing. This is the single choke point
     * for ALL receive paths (TCP, RDMA, kprobe). */
    extern int g_slave_fullsync_tmp_fd;
    extern int g_slave_loading_fullsync;
    extern unsigned long long g_slave_fullsync_target_bytes;
    extern unsigned long long g_slave_fullsync_loaded_bytes;

    if (from_replication && g_slave_loading_fullsync) {
        size_t remaining = g_slave_fullsync_target_bytes - g_slave_fullsync_loaded_bytes;
        size_t to_write = (*len < remaining) ? *len : remaining;

        if (g_slave_fullsync_tmp_fd >= 0 && to_write > 0) {
            ssize_t wr = write(g_slave_fullsync_tmp_fd, buf, to_write);
            if (wr < 0) {
                *len = 0;
                return -1;
            }
        }
        g_slave_fullsync_loaded_bytes += to_write;

        if (to_write < *len) {
            /* trailing bytes (e.g. REPLDONE) — keep in buf for normal parsing */
            memmove(buf, buf + to_write, *len - to_write);
            *len -= to_write;
            if (g_slave_fullsync_target_bytes > 0 &&
                g_slave_fullsync_loaded_bytes >= g_slave_fullsync_target_bytes) {
                repl_slave_finish_fullsync();
            }
            return 0;
        }

        *len = 0;
        if (g_slave_fullsync_target_bytes > 0 &&
            g_slave_fullsync_loaded_bytes >= g_slave_fullsync_target_bytes) {
            repl_slave_finish_fullsync();
        }
        return 0;
    }

#define PARSE_SCRATCH 4096
    /* ... rest of existing code unchanged ... */
```

注意：在 `parse_resp_stream` 中调用 `repl_slave_finish_fullsync()` 需要前向声明。可以：
- 在文件顶部已有 `repl_slave_finish_fullsync` 的声明（确认一下），或者
- 用 `extern void repl_slave_finish_fullsync(void);` 在函数内声明

- [ ] **Step 3: 构建验证**

```bash
make clean && make
```

预期：编译通过。可能需要在 kvstore.c 顶部或 parse_resp_stream 内部加 `extern void repl_slave_finish_fullsync(void);` 声明。

- [ ] **Step 4: Commit**

```bash
git add src/main/kvstore.c src/replication/kvs_repl.c
git commit -m "feat: intercept fullsync data in parse_resp_stream for KVSD temp file"
```

---

### Task 5: queue_snapshot() — FILE* → fd, kvs_snapshot_to_fp → kvs_dump_to_fd

**Files:**
- Modify: `src/main/kvstore.c:545-675`

**Interfaces:**
- Consumes: `kvs_dump_to_fd(fd, offset)` (updated in Task 2)
- Produces: KVSD binary data over network (same external protocol: `+FULLRESYNC` header + binary body + `REPLDONE`)

- [ ] **Step 1: 重写 queue_snapshot 的生成和发送段**

当前 `queue_snapshot()` 中 lines 580-612（临时文件打开 → kvs_snapshot_to_fp → 读回 → 发送）替换为：

```c
    /* Generate KVSD snapshot to temp file */
    snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.tmp.%ld", g_cfg.dump_path, (long)getpid());
    int fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) goto out;
    if (kvs_dump_to_fd(fd, snap_base_offset) != 0) { close(fd); unlink(tmp_path); goto out; }
    total_bytes = (size_t)lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    int n = snprintf(hdr, sizeof(hdr), "+FULLRESYNC %s %llu %zu\r\n",
                     repl_master_id(), snap_base_offset, total_bytes);

    repl_note_send_context("fullsync-header", (size_t)n, snap_base_offset, (unsigned char *)hdr);
    if (repl_send_chunked(c, (unsigned char *)hdr, (size_t)n) != 0) {
        repl_rdma_log("queue_snapshot - header send failed");
        close(fd);
        unlink(tmp_path);
        goto out;
    }
    while ((r = (size_t)read(fd, buf, buf_size)) > 0) {
        total += r;
        repl_note_send_context("fullsync-snapshot", r, repl_master_offset(), buf);
        if (repl_send_chunked(c, buf, r) != 0) {
            repl_rdma_log("queue_snapshot - snapshot chunk send failed total=%zu chunk=%zu", total, r);
            close(fd);
            unlink(tmp_path);
            goto out;
        }
    }
    close(fd);
    unlink(tmp_path);
```

同时删除不再需要的变量 `fp`（line 548：`FILE *fp;` → 删除），以及任何对 `fp` 的引用。还需要包含 `<fcntl.h>` 如果尚未包含——检查现有 include。

在函数开头的变量声明中：
- 删除 `FILE *fp;`（line 548）
- 添加 `int fd;`

在 cleanup label `out:` 处：
- 删除 `if (fp) fclose(fp);`（line 672）

- [ ] **Step 2: 构建验证**

```bash
make clean && make
```

预期：编译通过。确认 `FILE *fp` 的所有引用已删除，所有 `fp` → `fd` 替换正确。

- [ ] **Step 3: Commit**

```bash
git add src/main/kvstore.c
git commit -m "feat: queue_snapshot uses kvs_dump_to_fd (KVSD) instead of RESP snapshot"
```

---

### Task 6: repl_slave_finish_fullsync() — 用 replay_dump_file 替换 kvs_dump_to_fd

**Files:**
- Modify: `src/replication/kvs_repl.c:1902-1946`

**Interfaces:**
- Consumes: `replay_dump_file(path)` (from Task 3), `g_slave_fullsync_tmp_fd` (from Task 4)
- Produces: Loaded in-memory data from KVSD temp file, temp file cleaned up

- [ ] **Step 1: 重写 repl_slave_finish_fullsync 的持久化段**

在 `src/replication/kvs_repl.c` 的 `repl_slave_finish_fullsync()` 中，替换 lines 1930-1943（`kvs_dump_to_fd` 落盘段）为：

```c
    /* 全量同步完成后，从临时 KVSD 文件加载数据到内存并持久化
     * 临时文件在 parse_resp_stream 拦截中已写入完整 KVSD 数据 */
    if (g_slave_fullsync_tmp_fd >= 0) {
        char tmp_path[512];
        snprintf(tmp_path, sizeof(tmp_path), "%s.fullsync.recv.tmp.%ld",
                 g_cfg.dump_path, (long)getpid());

        /* fsync the temp file before loading */
        fsync(g_slave_fullsync_tmp_fd);
        close(g_slave_fullsync_tmp_fd);
        g_slave_fullsync_tmp_fd = -1;

        /* Load KVSD data into memory + TTL */
        unsigned long long aof_off = replay_dump_file(tmp_path);
        fprintf(stderr, "repl: slave fullsync loaded from %s, aof_offset=%llu\n",
                tmp_path, aof_off);

        /* Move temp file to official dump path as the persistence base */
        if (rename(tmp_path, g_cfg.dump_path) != 0) {
            fprintf(stderr, "repl: slave fullsync rename to %s failed: %s\n",
                    g_cfg.dump_path, strerror(errno));
        } else {
            fprintf(stderr, "repl: slave fullsync dump saved to %s\n", g_cfg.dump_path);
        }
    } else {
        fprintf(stderr, "repl: slave fullsync finished but no temp file (fd=%d)\n",
                g_slave_fullsync_tmp_fd);
    }
```

注意：需要导入 `<unistd.h>` 或确认 `fsync` 可用（通常已在 unistd.h 中）。

也要更新 `repl_slave_finish_fullsync` 开头的 `g_slave_fullsync_tmp_fd` 清理路径：如果由于某种原因 `g_slave_fullsync_tmp_fd >= 0` 但上面没有进入（不可能但防御），在函数中已处理。

- [ ] **Step 2: 构建验证**

```bash
make clean && make
```

预期：编译通过。确认 `replay_dump_file` 链接正确。

- [ ] **Step 3: Commit**

```bash
git add src/replication/kvs_repl.c
git commit -m "feat: repl_slave_finish_fullsync uses replay_dump_file for KVSD loading"
```

---

### Task 7: 废弃代码标记

**Files:**
- Modify: `src/main/kvstore.c`（多个 snapshot_*_sink 函数和 maybe_emit_expire_sink）

**Interfaces:**
- 无新增接口，仅在废弃函数前加注释

- [ ] **Step 1: 标记废弃函数**

在以下函数定义前添加 `/* deprecated: unused after KVSD unification */`：

1. `maybe_emit_expire_sink()` — line 2032
2. `snapshot_array_sink()` — line 2043
3. `snapshot_hash_sink()` — line 2053
4. `snapshot_rbtree_node_sink()` — line 2066
5. `snapshot_skiptable_cb_sink()` — line 2075
6. `snapshot_skiptable_sink()` — line 2082
7. `snapshot_doc_field_cb_sink()` — line 2086
8. `snapshot_doc_cb_sink()` — line 2093
9. `snapshot_doc_sink()` — line 2100
10. `snapshot_all_sink()` — line 2104
11. `kvs_snapshot_to_fp()` — line 2114
12. `kvs_snapshot_to_fd()` — line 2122

对于 `dump_skiptable_write_kv()`（line 2133）— 这个函数已在 Task 2 中更新为新格式，不属于废弃代码。

- [ ] **Step 2: 构建验证**

```bash
make clean && make
```

预期：编译通过，无新增 warning。被标记的函数不生成 "unused function" 警告（因为是 static 且编译器可见）。

- [ ] **Step 3: Commit**

```bash
git add src/main/kvstore.c
git commit -m "chore: mark RESP snapshot functions as deprecated after KVSD unification"
```

---

### Task 8: 构建和编译回归

**Files:**
- 无代码改动，验证构建

**验证内容:**
- 全项目编译
- 确认无 warning、无未定义符号

- [ ] **Step 1: 完全清理构建**

```bash
make clean && make 2>&1 | tee /tmp/build.log
```

预期：编译成功，无 error。如有 warning 需逐一检查。

- [ ] **Step 2: 检查符号**

```bash
nm kvstore | grep -E 'replay_dump_file|KVSD_FLAG|g_slave_fullsync_tmp_fd'
```

预期：`replay_dump_file` 和 `g_slave_fullsync_tmp_fd` 可见，`KVSD_FLAG_HAS_EXPIRE` 是宏不需要符号。

- [ ] **Step 3: Commit**

```bash
# 无文件改动则不 commit
echo "Build verified — all symbols OK"
```

---

### Task 9: 本地集成测试 — SAVE/BGSAVE + TTL 持久化

**Files:**
- 测试环境：本地单机

**验证 spec 要求:**
- SAVE 后 kill + restart，数据 + TTL 恢复
- 全量同步端到端（本地 master + slave）

- [ ] **Step 1: 测试 SAVE + TTL 恢复**

```bash
# 启动 master
./kvstore --port 5160 --role master --dump /tmp/test_kvsd.dump --aof /tmp/test_kvsd.aof &

sleep 1

# 写入带 TTL 的数据
redis-cli -p 5160 SET key1 val1
redis-cli -p 5160 EXPIRE key1 300
redis-cli -p 5160 HSET hk1 f1 hv1
redis-cli -p 5160 HEXPIRE hk1 600
redis-cli -p 5160 RSET rk1 rv1
redis-cli -p 5160 REXPIRE rk1 900

# SAVE
redis-cli -p 5160 SAVE

sleep 1

# 确认数据在内存中
redis-cli -p 5160 GET key1     # 预期: val1
redis-cli -p 5160 TTL key1     # 预期: > 0
redis-cli -p 5160 HGET hk1 f1  # 预期: hv1
redis-cli -p 5160 HTTL hk1     # 预期: > 0

# Kill 并重启
kill %1
sleep 1

./kvstore --port 5160 --role master --dump /tmp/test_kvsd.dump --aof /tmp/test_kvsd.aof &
sleep 1

# 验证恢复
redis-cli -p 5160 GET key1     # 预期: val1
redis-cli -p 5160 TTL key1     # 预期: > 0  (之前会丢失 TTL)
redis-cli -p 5160 HGET hk1 f1  # 预期: hv1
redis-cli -p 5160 HTTL hk1     # 预期: > 0
redis-cli -p 5160 RGET rk1     # 预期: rv1
redis-cli -p 5160 RTTL rk1     # 预期: > 0

kill %1
```

- [ ] **Step 2: 验证**

所有 GET 返回预期值，所有 TTL > 0。

- [ ] **Step 3: 测试本地全量同步**

```bash
# 启动 master
./kvstore --port 5160 --role master --dump /tmp/test_ms_dump --aof /tmp/test_ms.aof &

sleep 1

# 预存数据
redis-cli -p 5160 SET pre:key1 val1
redis-cli -p 5160 EXPIRE pre:key1 500

# 启动 slave (触发全量同步)
./kvstore --port 5161 --role slave --master-host 127.0.0.1 --master-port 5160 \
    --dump /tmp/test_sl_dump --aof /tmp/test_sl.aof &
sleep 3

# 验证 slave 数据
redis-cli -p 5161 GET pre:key1    # 预期: val1
redis-cli -p 5161 TTL pre:key1    # 预期: > 0

kill %1 %2
```

- [ ] **Step 4: Commit**

```bash
# 无代码改动，测试结果记录
echo "Local integration tests passed" >> /tmp/kvsd_test_log.txt
```

---

### Task 10: 跨机测试 — test_repl_5w5w

**Files:**
- 运行: `tests/test_repl_5w5w`

**环境:**
- Master: 192.168.233.128
- Slave: 192.168.233.129
- SSH 密码: 2983372202

- [ ] **Step 1: 编译测试程序**

```bash
make test_repl_5w5w
```

预期：编译成功，生成 `tests/test_repl_5w5w`。

- [ ] **Step 2: 部署到主机**

```bash
sshpass -p '2983372202' scp kvstore 192.168.233.128:~/kvstore/
sshpass -p '2983372202' scp kvstore 192.168.233.129:~/kvstore/
sshpass -p '2983372202' scp tests/test_repl_5w5w 192.168.233.128:~/kvstore/
sshpass -p '2983372202' scp tests/test.conf 192.168.233.128:~/kvstore/
sshpass -p '2983372202' scp tests/test.conf 192.168.233.129:~/kvstore/
```

- [ ] **Step 3: 启动 Master**

```bash
sshpass -p '2983372202' ssh 192.168.233.128 \
    'cd ~/kvstore && sudo ./kvstore kvstore.conf --role master &'
sleep 2
```

- [ ] **Step 4: 运行测试程序**

```bash
sshpass -p '2983372202' ssh 192.168.233.128 \
    'cd ~/kvstore && ./test_repl_5w5w --config tests/test.conf'
```

- [ ] **Step 5: 看到 "等待 Slave 连接..." 后，启动 Slave**

```bash
sshpass -p '2983372202' ssh 192.168.233.129 \
    'cd ~/kvstore && sudo ./kvstore kvstore.conf --role slave &'
```

- [ ] **Step 6: 等待测试完成，检查输出**

测试程序 Phase 6 验证数据一致性。预期输出全部 PASS。

- [ ] **Step 7: 清理**

```bash
sshpass -p '2983372202' ssh 192.168.233.128 'pkill kvstore; pkill test_repl_5w5w'
sshpass -p '2983372202' ssh 192.168.233.129 'pkill kvstore'
```

- [ ] **Step 8: Commit**

```bash
# 记录测试结果
echo "Cross-machine test passed: test_repl_5w5w all phases OK" >> /tmp/kvsd_test_log.txt
```
