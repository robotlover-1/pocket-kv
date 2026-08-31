# AOF Always Inline Response Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove deferred response mechanism so AOF always sends responses inline (like disable/everysec), eliminating multi-client serialization that causes ~20x pipeline throughput gap vs Redis.

**Architecture:** Delete `deferred_resp_t` list and 4 related functions from `kvs_persist.c`. Replace `persist_flush_deferred()` with `persist_flush_pending()` — same AOF flush, no response delivery. Remove deferred response gating in `kvstore.c` so responses fall through to the normal `queue_bytes` path. Update `reactor.c` and `kvstore.h` to match.

**Tech Stack:** C, no new dependencies.

**Spec:** `docs/superpowers/specs/2026-06-26-aof-always-inline-response.md`

## Global Constraints

- No new dependencies
- Keep existing code style (same naming, formatting, comment density)
- Must compile and pass `make` without warnings
- Must not affect AOF disable or everysec modes
- Must not affect replication paths

---

### Task 1: All code changes (4 files, atomic)

**Files:**
- Modify: `src/persistence/kvs_persist.c:41-52,240-253,256-289,293-295,298-317,684-689,991-996`
- Modify: `src/main/kvstore.c:1776-1780`
- Modify: `src/core/reactor.c:77,291`
- Modify: `include/kvstore/kvstore.h:520-523`

**Interfaces:**
- Produces: `void persist_flush_pending(void)` — call at end of reactor iteration
- Removes: `persist_defer_response()`, `persist_flush_deferred()`, `persist_aof_has_pending()`, `persist_cancel_deferred()`

All changes are interdependent (won't compile separately), done in one task.

---

- [ ] **Step 1: Remove deferred_resp_t struct and global variables** (`kvs_persist.c` L41-52)

Delete these 12 lines:
```c
/* deferred response queue for group commit (ALWAYS mode)
 * responses are small ("+OK\r\n", ":1\r\n", etc.) — inline to avoid malloc */
#define DEFERRED_DATA_MAX 128
typedef struct deferred_resp_s {
    conn_t *c;
    unsigned char data[DEFERRED_DATA_MAX];
    size_t len;
    struct deferred_resp_s *next;
} deferred_resp_t;

static deferred_resp_t *g_deferred_head = NULL;
static deferred_resp_t *g_deferred_tail = NULL;
```

- [ ] **Step 2: Remove `persist_defer_response()`** (`kvs_persist.c` L240-253)

Delete these 14 lines:
```c
/* queue a response for deferred delivery after next AOF group flush */
void persist_defer_response(conn_t *c, const unsigned char *data, size_t len) {
    deferred_resp_t *dr;
    if (!c || !data || len == 0) return;
    if (len > DEFERRED_DATA_MAX) return; /* oversized, shouldn't happen for write cmds */
    dr = (deferred_resp_t *)kvs_malloc(sizeof(*dr));
    if (!dr) return;
    memcpy(dr->data, data, len);
    dr->len = len;
    dr->c = c;
    dr->next = NULL;
    if (g_deferred_tail) g_deferred_tail->next = dr;
    else g_deferred_head = dr;
    g_deferred_tail = dr;
}
```

- [ ] **Step 3: Remove `persist_flush_deferred()`** (`kvs_persist.c` L256-289)

Delete the entire function:
```c
void persist_flush_deferred(void) {
    deferred_resp_t *dr, *next;
    int flushed[256] = {0};  /* track which connections we've flushed */

    /* only group-commit flush in ALWAYS mode; everysec has its own timer */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && g_aof_buf_len > 0) {
        persist_aof_flush_buffer();
    }

    /* phase 1: queue all responses into ring buffers without sending */
    dr = g_deferred_head;
    while (dr) {
        next = dr->next;
        if (dr->c) queue_bytes(dr->c, dr->data, dr->len);
        dr = next;
    }

    /* phase 2: flush each connection's ring once (one send() per connection) */
    dr = g_deferred_head;
    while (dr) {
        next = dr->next;
        if (dr->c) {
            int fd = dr->c->fd;
            if (fd >= 0 && fd < (int)(sizeof(flushed)/sizeof(flushed[0])) && !flushed[fd]) {
                flushed[fd] = 1;
                flush_conn_output(dr->c);
            }
        }
        kvs_free(dr);
        dr = next;
    }
    g_deferred_head = NULL;
    g_deferred_tail = NULL;
}
```

- [ ] **Step 4: Add `persist_flush_pending()`** — insert at the same location (after the deleted `persist_flush_deferred`)

```c
/* flush pending AOF buffered data to disk after each reactor iteration.
 * only active in ALWAYS mode; everysec has its own timer. */
void persist_flush_pending(void) {
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && g_aof_buf_len > 0) {
        persist_aof_flush_buffer();
    }
}
```

- [ ] **Step 5: Remove `persist_aof_has_pending()`** (`kvs_persist.c` L293-295)

Delete these 3 lines:
```c
/* returns 1 if there is buffered AOF data waiting for group flush
 * (only meaningful in ALWAYS mode; everysec flushes on its own timer) */
int persist_aof_has_pending(void) {
    return (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && g_aof_buf_len > 0) ? 1 : 0;
}
```

- [ ] **Step 6: Remove `persist_cancel_deferred()`** (`kvs_persist.c` L298-317)

Delete the entire function:
```c
/* remove all deferred responses belonging to a closed connection */
void persist_cancel_deferred(conn_t *c) {
    deferred_resp_t *dr, *next, *prev = NULL;

    if (!c) return;

    dr = g_deferred_head;
    while (dr) {
        next = dr->next;
        if (dr->c == c) {
            /* unlink from list */
            if (prev) prev->next = next;
            else g_deferred_head = next;
            if (dr == g_deferred_tail) g_deferred_tail = prev;
            kvs_free(dr);
        } else {
            prev = dr;
        }
        dr = next;
    }
}
```

- [ ] **Step 7: Update `persist_append_raw()` comment** (`kvs_persist.c` L682-689)

Replace:
```c
    /* ALWAYS mode: group commit — rely on reactor batch flush (persist_flush_deferred);
     * only inline-flush on time threshold as latency ceiling */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
        if (kvs_now_ms() - g_aof_buffered_since_ms >= 2) {
            persist_aof_flush_buffer();
            g_aof_buffered_since_ms = 0;
        }
        /* else: response deferred by caller, flush in persist_flush_deferred() */
    }
```

With:
```c
    /* ALWAYS mode: buffer accumulates; flushed at reactor iteration end
     * (persist_flush_pending) or on 2ms timeout as latency ceiling */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
        if (kvs_now_ms() - g_aof_buffered_since_ms >= 2) {
            persist_aof_flush_buffer();
            g_aof_buffered_since_ms = 0;
        }
    }
```

- [ ] **Step 8: Update `persist_autosnap_cron()` always branch** (`kvs_persist.c` L991-996)

Replace:
```c
    /* ALWAYS mode: flush pending buffered data if timeout exceeded,
     * ensures deferred responses are delivered even without new commands */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && g_aof_buf_len > 0) {
        if (kvs_now_ms() - g_aof_buffered_since_ms >= 2) {
            persist_flush_deferred();
        }
    }
```

With:
```c
    /* ALWAYS mode: flush pending buffered data if timeout exceeded */
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && g_aof_buf_len > 0) {
        if (kvs_now_ms() - g_aof_buffered_since_ms >= 2) {
            persist_aof_flush_buffer();
        }
    }
```

- [ ] **Step 9: Remove deferred response branch in `kvstore.c`** (`src/main/kvstore.c` L1776-1780)

Delete these 4 lines:
```c
            /* group commit: if data was buffered (not flushed), defer response */
            if (c && persist_aof_has_pending()) {
                persist_defer_response(c, (unsigned char *)resp, (size_t)n);
                goto out;
            }
```

The surrounding code at L1768-1781 should now read:
```c
        if (!from_replication && is_write_cmd(cmd)) {
            persist_note_write();
            if (persist_append_raw(raw, rawlen) != 0) {
                n = resp_error(resp, BUFFER_CAP, "AOF write failed");
                if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
                goto out;
            }
            if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
        }
```

Response now falls through to the existing `queue_bytes` at L1788.

- [ ] **Step 10: Remove `persist_cancel_deferred` call in `reactor.c`** (`src/core/reactor.c` L76-77)

Delete these 2 lines:
```c
    /* clear any deferred responses for this connection */
    persist_cancel_deferred(c);
```

- [ ] **Step 11: Update reactor event loop call** (`src/core/reactor.c` L290-291)

Replace:
```c
        /* flush any deferred AOF responses from group commit */
        persist_flush_deferred();
```

With:
```c
        /* flush pending AOF buffered data */
        persist_flush_pending();
```

- [ ] **Step 12: Update `kvstore.h` declarations** (`include/kvstore/kvstore.h` L520-523)

Replace these 4 lines:
```c
void persist_defer_response(conn_t *c, const unsigned char *data, size_t len);
void persist_flush_deferred(void);
int persist_aof_has_pending(void);
void persist_cancel_deferred(conn_t *c);
```

With this 1 line:
```c
void persist_flush_pending(void);
```

- [ ] **Step 13: Build**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make clean && make
```

Expected: Compiles with zero errors and zero warnings.

---

### Task 2: Verify correctness — data survives kill -9

- [ ] **Step 1: Start kvstore with AOF always**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
rm -f kvstore.dump kvstore.aof
./kvstore --port 5190 --role master --mem libc --net reactor --appendfsync always --aof-disable=false &
sleep 1
redis-cli -p 5190 PING
```

Expected: `PONG`

- [ ] **Step 2: Write 1000 keys, verify in-memory read**

```bash
for i in $(seq 1 1000); do
  redis-cli -p 5190 HSET "key_$i" "value_$i" > /dev/null
done
redis-cli -p 5190 HGET key_500
```

Expected: `value_500`

- [ ] **Step 3: kill -9, restart, verify recovery**

```bash
kill -9 $(pgrep -f "kvstore.*5190")
sleep 1
./kvstore --port 5190 --role master --mem libc --net reactor --appendfsync always &
sleep 1
redis-cli -p 5190 HGET key_500
redis-cli -p 5190 HGET key_1
redis-cli -p 5190 HGET key_1000
```

Expected: All three return their values. May miss last ~few keys due to relaxed durability semantic — acceptable.

---

### Task 3: Benchmark — persist_bench (P=1, 50 clients)

- [ ] **Step 1: Run persist_bench**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
bash tools/bench/run_persist_bench.sh
```

- [ ] **Step 2: Check AOF always QPS**

```bash
grep "kvstore_aof_always" benchmarks/data/persist_bench/aof_summary.csv
```

Expected: QPS ≥ 49,135 (current baseline). Target: 70-80K.

- [ ] **Step 3: Check AOF disable and everysec are not degraded**

```bash
grep -E "kvstore_aof_disable|kvstore_aof_everysec" benchmarks/data/persist_bench/aof_summary.csv
```

Expected: Within 5% of current baselines (disable ~135K, everysec ~130K).

---

### Task 4: Benchmark — pipeline_bench (P=10/20/40/80/160, 50 clients)

- [ ] **Step 1: Run pipeline_bench**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
bash tools/bench/run_pipeline_bench.sh
```

- [ ] **Step 2: Check AOF always pipeline performance**

```bash
grep "kvstore_aof_always" benchmarks/data/pipeline_bench/pipeline_summary.csv
```

Expected: P=10 significantly above 12K (target >200K). P=160 approaching 600K.

- [ ] **Step 3: Check pipeline disable/everysec are not degraded**

```bash
grep -E "kvstore_aof_disable|kvstore_aof_everysec" benchmarks/data/pipeline_bench/pipeline_summary.csv
```

Expected: Within 10% of current baselines at each pipeline depth.

---

### Task 5: Commit

- [ ] **Step 1: Commit all changes**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
git add src/persistence/kvs_persist.c src/main/kvstore.c src/core/reactor.c include/kvstore/kvstore.h
git add docs/superpowers/specs/2026-06-26-aof-always-inline-response.md
git add docs/superpowers/plans/2026-06-26-aof-always-inline-response.md
git commit -m "perf: remove deferred response, inline AOF always replies

Drop the deferred_resp_t linked list and 4 related functions.
AOF always responses now go through queue_bytes immediately
(before fsync), matching the disable/everysec path.

This eliminates multi-client serialization caused by holding
responses until after fsync, which made effective batch size
drop from ~23 to ~6 commands per reactor cycle.

Durability semantic relaxed to match Redis: +OK means data
is in OS buffer, fsync completes shortly after.

Net: -66 lines, 0 new dependencies."
```

---

### Task 6: Update README benchmark data (if new baselines established)

- [ ] **Step 1: If benchmark results changed significantly, update README tables**

Only do this if the benchmark data changes from Task 3/4 are substantial enough to warrant updating published numbers. Check `aof_summary.csv` and `pipeline_summary.csv` against the tables in README lines 4480-4489 and 4716-4723. If QPS changed by >5%, update those tables.
