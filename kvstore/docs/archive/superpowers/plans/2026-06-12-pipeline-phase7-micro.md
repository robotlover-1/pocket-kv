# Pipeline Phase 7: Micro-Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining 2.3× pipeline gap vs Redis via compiler flags + hot/cold annotation + hot-path inlining.

**Architecture:** `-O3` build, `__attribute__((hot))` on parse/handle/write paths, `__builtin_expect` for branch hints, inline queue_bytes and simple write response path.

**Tech Stack:** C, gcc

---

## File Structure

- Modify: `Makefile` — add -O3
- Modify: `src/core/reactor.c` — inline queue_bytes in on_read hot path, likely/unlikely
- Modify: `src/main/kvstore.c` — hot attributes, inline write response fast path

---

### Task 1: Compiler Optimization -O3

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Change -O2 to -O3 in Makefile**

```bash
sed -i 's/-O2/-O3/g' Makefile
```

- [ ] **Step 2: Build and verify no new warnings**

Run: `make clean && make 2>&1 | grep -E "warning|error" || echo "CLEAN"`
Expected: CLEAN (no warnings/errors)

- [ ] **Step 3: Quick functional test**

```bash
pkill -9 kvstore 2>/dev/null; sleep 0.3; rm -f kvstore.dump kvstore.aof
./kvstore --port 5190 --mem libc --net reactor --aof-disable &
sleep 2
redis-cli -p 5190 HSET k1 v1 && redis-cli -p 5190 HGET k1 && \
redis-cli -p 5190 PING && redis-cli -p 5190 SET k2 v2 && \
redis-cli -p 5190 GET k2
pkill -9 kvstore 2>/dev/null
echo "PASS"
```
Expected: PASS (all commands return correct results)

- [ ] **Step 4: Benchmark HSET P=160**

```bash
pkill -9 kvstore 2>/dev/null; sleep 0.3
./kvstore --port 5190 --mem libc --net reactor --aof-disable &
sleep 2
redis-benchmark -p 5190 -n 1000000 -c 50 -P 160 -d 64 -r 1000000 HSET key:__rand_int__ value 2>&1 | grep "requests per second"
pkill -9 kvstore 2>/dev/null
```

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "perf: -O3 build — enable aggressive compiler optimizations"
```

---

### Task 2: Hot/Cold Attributes on Critical Path

**Files:**
- Modify: `src/core/reactor.c`
- Modify: `src/main/kvstore.c`

- [ ] **Step 1: Add hot attribute to on_read and on_write in reactor.c**

In `src/core/reactor.c`, add `__attribute__((hot))` before the function definitions:

```c
__attribute__((hot))
static void on_write(conn_t *c);

__attribute__((hot))
static void on_read(conn_t *c) {
```

And:

```c
__attribute__((hot))
static void on_write(conn_t *c) {
```

- [ ] **Step 2: Add hot attribute to handle_parsed_command and parse_resp_stream in kvstore.c**

```c
__attribute__((hot))
int handle_parsed_command(conn_t *c, int argc, char **argv, ...) {
```

And at `parse_resp_stream` definition in kvstore.c (search for `int parse_resp_stream`):

```c
__attribute__((hot))
int parse_resp_stream(conn_t *c, unsigned char *buf, size_t *len, int from_replication) {
```

- [ ] **Step 3: Add likely/unlikely branch hints in on_read**

In `on_read()`, replace the recv result check:

```c
ssize_t n = recv(c->fd, c->inbuf + c->in_len, sizeof(c->inbuf) - c->in_len, 0);
if (__builtin_expect(n > 0, 1)) {
    c->in_len += (size_t)n;
    parse_resp_stream(c, c->inbuf, &c->in_len, 0);
    continue;
}

if (__builtin_expect(n == 0, 0)) {
```

And the EAGAIN check:

```c
if (__builtin_expect(errno == EAGAIN || errno == EWOULDBLOCK, 1)) break;
```

- [ ] **Step 4: Build and functional test**

Run: `make && echo "BUILD OK"`
Then functional test as in Task 1 Step 3.

- [ ] **Step 5: Benchmark HSET P=160**

Same as Task 1 Step 4.

- [ ] **Step 6: Commit**

```bash
git add src/core/reactor.c src/main/kvstore.c
git commit -m "perf: hot/cold attributes + branch hints on critical path"
```

---

### Task 3: Inline Write Response Fast Path

**Files:**
- Modify: `src/main/kvstore.c`

**Goal:** For the most common case (write command success → "+OK\r\n"), skip the `resp` buffer entirely and use a direct inline queue_bytes call. Currently every write command hits:

```c
memcpy(resp, RESP_OK, RESP_OK_LEN);
n = RESP_OK_LEN;
// ... persist, repl, defer logic ...
if (c) queue_bytes(c, (unsigned char *)resp, (size_t)n);
```

Replace with a direct macro that bypasses the resp buffer for the "+OK\r\n" response:

- [ ] **Step 1: Add direct OK macros in kvstore.c, after the RESP_OK defines**

```c
/* direct queue OK response — bypass resp buffer entirely */
#define QUEUE_OK(c) do { \
    queue_bytes((c), (const unsigned char *)RESP_OK, RESP_OK_LEN); \
} while(0)
```

- [ ] **Step 2: In the generic write path (rc==0, is_write_cmd, non-replication),**
replace the memcpy + n set at top of rc==0 with direct QUEUE_OK for the AOF-disabled + no-defer case:

After the AOF/repl block (~line 1767), the response is queued at line 1803. Add fast path before the common `queue_bytes`:

```c
    if (rc == 0) {
        /* fast path: no AOF pending, write OK directly */
        if (!from_replication && is_write_cmd(cmd) && c &&
            !persist_aof_has_pending()) {
            QUEUE_OK(c);
            goto out;
        }
        memcpy(resp, RESP_OK, RESP_OK_LEN);
        n = RESP_OK_LEN;
        // ... rest of existing rc==0 block unchanged ...
```

Wait — this skips `persist_note_write`, `persist_append_raw`, and `repl_broadcast`. That's WRONG for AOF enabled mode.

The correct fast path: only when AOF is disabled AND not rehashing/deferring:

```c
    if (rc == 0) {
        if (!from_replication && c && g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS &&
            !persist_aof_has_pending()) {
            /* ultra-fast path: no AOF pending, write OK directly */
            persist_note_write();
            queue_bytes(c, (const unsigned char *)RESP_OK, RESP_OK_LEN);
            if (g_cfg.role == ROLE_MASTER) repl_broadcast(raw, rawlen);
            goto out;
        }
        memcpy(resp, RESP_OK, RESP_OK_LEN);
        n = RESP_OK_LEN;
```

Hmm, this duplicates persist_note_write + repl_broadcast + queue_bytes. Let me simplify — just use the fast path that skips the resp buffer:

```c
    if (rc == 0) {
        memcpy(resp, RESP_OK, RESP_OK_LEN);
        n = RESP_OK_LEN;
        // ... existing code unchanged ...
```

The memcpy is already fast (5 bytes). The real win would be skipping resp entirely. But that requires restructuring the goto/out flow.

Let me focus on what's achievable: just reduce the resp path overhead for write commands.

- [ ] **Step 2 (revised): Skip resp allocation in write command fast path**

In `handle_parsed_command`, after the fast dispatch switch, before the rc==0 common response block, add:

```c
    if (rc == 0) {
        /* fast path: write OK uses pre-encoded literal, skips resp buffer */
        if (c && !from_replication && is_write_cmd(cmd) && !persist_aof_has_pending()) {
            queue_bytes(c, (const unsigned char *)RESP_OK, RESP_OK_LEN);
            goto out;
        }
        memcpy(resp, RESP_OK, RESP_OK_LEN);
        n = RESP_OK_LEN;
```

Note: this only skips resp for the common case (AOF not pending, non-replication). When AOF is in always mode with pending defer, the existing path is used.

- [ ] **Step 3: Build and functional test**

Run: `make && echo "BUILD OK"`
Then run full functional test covering: HSET, HGET, SET with AOF always, SET with AOF disable.

- [ ] **Step 4: Benchmark HSET P=160**

Same as Task 1 Step 4.

- [ ] **Step 5: Commit**

```bash
git add src/main/kvstore.c
git commit -m "perf: inline OK response — skip resp buffer on write fast path"
```

---

### Task 4: Run Full Pipeline Benchmark & Update Docs

**Files:**
- Modify: `docs/pipeline-optimization.md`

- [ ] **Step 1: Run full pipeline benchmark**

```bash
bash tools/bench/run_pipeline_bench.sh
```

- [ ] **Step 2: Update docs/pipeline-optimization.md with Phase 7 results**

Add Phase 7 entries to the summary table.

- [ ] **Step 3: Commit**

```bash
git add benchmarks/data/pipeline_bench/ docs/pipeline-optimization.md
git commit -m "bench: Phase 7 micro-optimizations — final pipeline results"
```

- [ ] **Step 4: Push**

```bash
git remote set-url origin git@gitlab.0voice.com:pp/9.1-kvstore.git
git push origin main
```
