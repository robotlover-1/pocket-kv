# Full Sync One-Sided RDMA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one-sided RDMA WRITE the preferred kvstore full-sync data path, with the Master writing directly from a mapped KVSD file and the Slave receiving into a file-backed remote MR. On WRITE unavailability or failure, fall back to zero-copy TCP `sendfile` (two-sided RDMA SEND is no longer a production data fallback). Enforce a configurable snapshot-size limit (`repl_rdma_write_buf_max_mb`) on the Slave MR. Validate correctness locally and across `192.168.233.128 -> 192.168.233.129`. The KVSD file benchmark redesign is deferred to a follow-up iteration.

**Architecture:** TCP remains the full-sync control plane and exchanges a per-attempt transfer ID plus the Slave's file-backed remote MR. The Master writes only the KVSD byte range with ordered RDMA WRITE operations, uses WRITE_WITH_IMM for the final payload, and waits for a matching `REPLDONE` before replaying the gap. Each fallback starts a fresh transfer and Slave target so a partial one-sided write can never be reused.

**Tech Stack:** C11, Linux sockets/files/mmap, libibverbs/librdmacm, RESP-like replication control messages, Python integration harnesses, Bash performance harnesses.

## Global Constraints

- Preferred full-sync path is `RDMA WRITE`; fallback order is exactly `RDMA WRITE -> sendfile` (zero-copy TCP). Two-sided RDMA SEND is **not** a production data fallback (it remains only as a deferred benchmark baseline).
- TCP remains the authoritative control plane.
- The remote MR is a `MAP_SHARED` mapping of a uniquely named, exactly sized Slave temporary file.
- The Master's WRITE source is the mapped KVSD dump file registered as a single MR; no snapshot bytes are copied into registered send buffers on the data path.
- The Slave rejects snapshots above `repl_rdma_write_buf_max_mb` MiB (config, default `256`) before publishing an MR; rejection drives the Master's MR wait to time out into the `sendfile` fallback.
- `WRITE_WITH_IMM` means the ordered writes reached the remote memory domain; only matching TCP `REPLDONE` means application-level completion.
- Full-sync gap/backlog bytes must never be written into the snapshot MR; replay them only after matching `REPLDONE`.
- Every fallback attempt gets a new transfer ID and a fresh temporary target; byte offset is never carried across modes.
- The KVSD file benchmark redesign (transfer-only and end-to-end throughput, `rdma-write`/`rdma-send`/`sendfile` modes) is **deferred to a follow-up iteration** and is not part of this plan.
- Do not write the supplied SSH or sudo password to source, scripts, artifacts, shell history, or this plan; use it only via ephemeral process input.
- Before cross-host execution, inspect existing processes, ports, dump paths, routes, and RDMA devices; do not terminate or overwrite unrelated resources.
- Do not stage or modify the pre-existing `kvstore.conf` change or untracked `NtyCo` entry.

## File Structure

- Create `include/kvstore/replication/fullsync.h`: transport-independent transfer IDs, control-message parsing, state names, and remote-range validation.
- Create `src/replication/kvs_fullsync.c`: pure protocol/state helpers that can be unit tested without RDMA hardware.
- Create `tests/test_fullsync_protocol.c`: unit tests for parsing, stale IDs, capacity, overflow, and transitions.
- Modify `include/kvstore/kvstore.h`: attach full-sync attempt state to each replication connection.
- Modify `src/replication/kvs_repl.c`: own verbs state, file-backed Slave targets, WRITE posting/completions, fallback restart, and cleanup.
- Modify `src/main/kvstore.c`: issue transfer-aware control messages, wait for MR before payload, defer gap replay until `REPLDONE`, and validate completion IDs.
- Modify `src/core/reactor.c`: invoke replication cleanup when an owning TCP connection closes.
- Modify the root `Makefile`: compile the protocol module and expose its unit test target.
- Modify `tools/repl/run_repl_rdma_smoke.py`: verify negotiated mode, transfer ID, snapshot correctness, replay, and incremental continuation.
- Modify `tools/repl/run_repl_rdma_stress.py`: inject MR timeout and WRITE failure; verify the `sendfile` fallback.
- Modify `src/main/kvstore.c`: parse the new `repl_rdma_write_buf_max_mb` config and pass it into the Slave target-size check.
- Modify `README.md`: document the actual full-sync negotiation (WRITE vs `sendfile`) after validation.
- (Deferred to follow-up: `tests/perf/test_rdma_throughput.c`, `tests/perf/run_throughput_compare.sh`, `tests/perf/Makefile.perf` benchmark redesign.)

---

### Task 1: Transfer-Aware Full-Sync Protocol Helpers

**Files:**
- Create: `include/kvstore/replication/fullsync.h`
- Create: `src/replication/kvs_fullsync.c`
- Create: `tests/test_fullsync_protocol.c`
- Modify: `Makefile`

**Interfaces:**
- Produces:
  - `kvs_fullsync_mode_t` with `KVS_FULLSYNC_RDMA_WRITE`, `KVS_FULLSYNC_RDMA_SEND`, and `KVS_FULLSYNC_TCP`. `KVS_FULLSYNC_RDMA_SEND` is reserved for the deferred benchmark baseline and is **never** selected as a production fallback in this iteration; the production fallback for WRITE is `KVS_FULLSYNC_TCP` (sendfile).
  - `kvs_fullsync_state_t` with `IDLE`, `PREPARING`, `WAIT_MR`, `WRITING`, `RDMA_SEND`, `TCP`, `WAIT_REPLDONE`, `COMPLETE`, and `FAILED` states. Production transitions use `WRITING` -> `WAIT_REPLDONE` and, on failure, restart through `TCP`; `RDMA_SEND` is not entered by production code this iteration.
  - `kvs_fullsync_mr_msg_t { uint64_t transfer_id; uint64_t addr; uint32_t rkey; uint64_t capacity; }`.
  - `const char *kvs_fullsync_mode_name(kvs_fullsync_mode_t mode)`.
  - `int kvs_fullsync_parse_remote_mr(const char *line, size_t len, kvs_fullsync_mr_msg_t *out)`.
  - `int kvs_fullsync_validate_remote_mr(const kvs_fullsync_mr_msg_t *msg, uint64_t expected_id, uint64_t expected_bytes)`.
  - `int kvs_fullsync_remote_addr(uint64_t base, uint64_t capacity, uint64_t offset, size_t len, uint64_t *out)`.
  - `int kvs_fullsync_parse_repldone(const char *line, size_t len, uint64_t *transfer_id, uint64_t *bytes)`.
- Consumes: only standard integer, size, and errno definitions; no verbs headers.

- [ ] **Step 1: Write protocol tests that encode the accepted wire formats**

Create table-driven tests using these exact messages:

```c
static void test_remote_mr_message(void) {
    kvs_fullsync_mr_msg_t mr = {0};
    const char line[] = "+FULLRESYNCWR 41 4096 77 85000008\r\n";
    assert(kvs_fullsync_parse_remote_mr(line, sizeof(line) - 1, &mr) == 0);
    assert(mr.transfer_id == 41);
    assert(mr.addr == 4096);
    assert(mr.rkey == 77);
    assert(mr.capacity == 85000008);
    assert(kvs_fullsync_validate_remote_mr(&mr, 41, 85000008) == 0);
    assert(kvs_fullsync_validate_remote_mr(&mr, 42, 85000008) == -ESTALE);
}

static void test_remote_range(void) {
    uint64_t remote = 0;
    assert(kvs_fullsync_remote_addr(4096, 1024, 256, 512, &remote) == 0);
    assert(remote == 4352);
    assert(kvs_fullsync_remote_addr(4096, 1024, 768, 257, &remote) == -EOVERFLOW);
    assert(kvs_fullsync_remote_addr(UINT64_MAX - 7, 16, 8, 1, &remote) == -EOVERFLOW);
}

static void test_repldone_message(void) {
    uint64_t id = 0, bytes = 0;
    const char line[] = "REPLDONE 41 85000008\r\n";
    assert(kvs_fullsync_parse_repldone(line, sizeof(line) - 1, &id, &bytes) == 0);
    assert(id == 41 && bytes == 85000008);
}
```

Also reject zero address, zero rkey, insufficient capacity, extra fields, truncated CRLF, negative text, and integer overflow.

- [ ] **Step 2: Add the test build target and verify RED**

Add `src/replication/kvs_fullsync.c` to the production source list and a `test_fullsync_protocol` target that links only the new source and test. Run:

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore test_fullsync_protocol
```

Expected: compilation or link failure because the header/functions do not exist yet.

- [ ] **Step 3: Implement strict parsers and overflow-safe validation**

Use bounded token parsing with `strtoull`, explicit end-pointer checks, exact field counts, and CRLF validation. Implement range validation without overflowing:

```c
if (offset > capacity || len > capacity - offset)
    return -EOVERFLOW;
if (offset > UINT64_MAX - base)
    return -EOVERFLOW;
*out = base + offset;
```

Return negative errno-style values: `-EINVAL` for malformed messages, `-ESTALE` for transfer mismatch, and `-EOVERFLOW` for capacity/address violations.

- [ ] **Step 4: Run the focused unit test and sanitizer build**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore test_fullsync_protocol
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/test_fullsync_protocol
cc -fsanitize=address,undefined -I/home/pp/Desktop/ls_study/proj/9.1-kvstore/include \
  /home/pp/Desktop/ls_study/proj/9.1-kvstore/src/replication/kvs_fullsync.c \
  /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/test_fullsync_protocol.c \
  -o /tmp/test_fullsync_protocol_san && /tmp/test_fullsync_protocol_san
```

Expected: all assertions pass and sanitizers print no diagnostics.

- [ ] **Step 5: Commit the protocol unit**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  include/kvstore/replication/fullsync.h src/replication/kvs_fullsync.c \
  tests/test_fullsync_protocol.c Makefile
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "feat: add transfer-aware fullsync protocol"
```

### Task 2: Per-Connection State and Idempotent Resource Cleanup

**Files:**
- Modify: `include/kvstore/kvstore.h:259-290`
- Modify: `src/replication/kvs_repl.c:151-235`
- Modify: `src/replication/kvs_repl.c:1998-2410`
- Modify: `src/core/reactor.c:65-110`
- Test: `tests/test_fullsync_protocol.c`

**Interfaces:**
- Consumes: enums and message types from Task 1.
- Produces:
  - Per-connection fields `repl_transfer_id`, `repl_fullsync_state`, `repl_fullsync_expected_bytes`, `repl_fullsync_mode`, and `repl_fullsync_deadline_ms`.
  - `void repl_rdma_master_transfer_cleanup(conn_t *c)`.
  - `void repl_rdma_slave_target_cleanup(int remove_incomplete)`.
  - Internal `repl_fullsync_target_t` owning transfer ID, fd, path, mapping, MR, expected length, and completion flags.

- [ ] **Step 1: Add a cleanup state-machine test**

Expose a test-only reset helper under `#ifdef KVS_FULLSYNC_TESTING` and assert that cleanup can be called twice after each partially initialized stage: fd only, fd plus mapping, and fd plus mapping plus synthetic MR marker. The second call must leave `fd == -1`, `mapping == NULL`, `mr == NULL`, `length == 0`, and an empty path.

- [ ] **Step 2: Run the cleanup test and verify RED**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean test_fullsync_protocol CPPFLAGS=-DKVS_FULLSYNC_TESTING
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/test_fullsync_protocol
```

Expected: build failure because the target object and cleanup helper are not defined.

- [ ] **Step 3: Add explicit owner structures and initialization**

Extend `conn_t` with transfer state. Replace the loose Slave globals `g_rdma_write_recv_*` with:

```c
typedef struct repl_fullsync_target {
    uint64_t transfer_id;
    uint64_t expected_bytes;
    int fd;
    char path[PATH_MAX];
    void *mapping;
    size_t mapping_len;
    struct ibv_mr *mr;
    int imm_received;
    int complete;
} repl_fullsync_target_t;
```

Extend `repl_rdma_ctx_t` with `transfer_id`, `remote_capacity`, `write_offset`, `posted_wr`, `completed_wr`, `final_imm_posted`, `cq_failed`, and `transfer_owner`. Initialize all fd values to `-1` and pointers to `NULL` before any operation that can fail.

- [ ] **Step 4: Implement one idempotent cleanup path per side**

Slave cleanup order must be: mark inactive, deregister MR, unmap, close fd, optionally unlink incomplete path, then zero metadata while restoring `fd = -1`. Master cleanup clears remote MR fields and WR accounting but does not close the shared RDMA connection. Call cleanup from replication removal, TCP owner disconnect, CQ error, timeout, replay failure, rename failure, and normal completion.

- [ ] **Step 5: Run unit and non-RDMA builds**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore test_fullsync_protocol
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/test_fullsync_protocol
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean all ENABLE_RDMA=0
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean all ENABLE_RDMA=1
```

Expected: cleanup tests pass and both feature configurations build.

- [ ] **Step 6: Commit state ownership and cleanup**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  include/kvstore/kvstore.h src/replication/kvs_repl.c src/core/reactor.c \
  tests/test_fullsync_protocol.c
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "refactor: own fullsync RDMA resources per transfer"
```

### Task 3: File-Backed Slave Remote MR and Completion Gate

**Files:**
- Modify: `src/replication/kvs_repl.c:2204-2410`
- Modify: `src/main/kvstore.c:2005-2110`
- Test: `tools/repl/run_repl_rdma_smoke.py`

**Interfaces:**
- Consumes: `repl_fullsync_target_t`, transfer-aware protocol from Tasks 1-2, and the new config `g_cfg.repl_rdma_write_buf_max_mb` (default `256`).
- Produces:
  - `int repl_rdma_slave_prepare_target(int tcp_fd, uint64_t transfer_id, uint64_t total_bytes, const char *tmp_path)`. Rejects `total_bytes` above `repl_rdma_write_buf_max_mb` MiB before creating any file; publishes no MR and sends no `+FULLRESYNCWR`, so the Master's MR wait times out into `sendfile`.
  - `int repl_rdma_slave_complete_target(uint64_t transfer_id, uint32_t imm_data)`.
  - Wire response `+FULLRESYNCWR <transfer_id> <addr> <rkey> <capacity>\r\n`.

- [ ] **Step 1: Extend the smoke harness with an IMM/replay gate assertion**

Add a mode that starts a full sync, pauses before final IMM using a test injection environment variable, and asserts that the temporary target exists but the formal dump is not renamed and sampled keys are not reported as synchronized. After releasing IMM, require successful replay and matching samples.

- [ ] **Step 2: Run the focused smoke scenario and verify RED**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean all ENABLE_RDMA=1
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_smoke.py \
  --expect-mode rdma-write --pause-before-final-imm
```

Expected: argument is unrecognized or the run observes that the existing implementation does not provide the required file-backed/IMM gate semantics.

- [ ] **Step 3: Replace the heap receive buffer with an exactly sized mapped file**

In `repl_rdma_slave_prepare_target`:

```c
uint64_t cap = (uint64_t)g_cfg.repl_rdma_write_buf_max_mb * 1024 * 1024;
if (total_bytes == 0 || total_bytes > cap) return -EFBIG;   /* no MR, no response -> Master times out to sendfile */
if (g_active_target.active) return -EBUSY;
fd = open(tmp_path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
if (fd < 0) return -errno;
if (ftruncate(fd, (off_t)total_bytes) < 0) goto fail;
mapping = mmap(NULL, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
if (mapping == MAP_FAILED) goto fail;
mr = ibv_reg_mr(pd, mapping, total_bytes,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
if (!mr) goto fail;
```

Reject `total_bytes == 0`, values that do not fit `off_t`/`size_t`, and a second active target. The size limit comes from the config `repl_rdma_write_buf_max_mb` (default `256`). Publish MR information only after all operations succeed.

- [ ] **Step 4: Bind immediate completion to the active transfer**

Encode a transfer cookie derived from the active transfer ID in immediate data and compare it on Slave completion. A receive completion with `IBV_WC_WITH_IMM` sets `imm_received` only when it belongs to the active QP and expected cookie. Do not replay from a sender CQ, ordinary SEND completion, stale QP, or mismatched cookie.

- [ ] **Step 5: Persist, replay, and acknowledge in the required order**

After valid IMM: `msync(MS_SYNC)`, deregister MR, unmap, `fsync(fd)`, close, call the existing KVSD replay path, rename the successful temporary file, and send `REPLDONE <transfer_id> <total_bytes>\r\n`. Any failure invokes cleanup with unlink enabled and sends no success acknowledgment.

- [ ] **Step 6: Run the smoke test and inspect lifecycle logs**

```bash
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_smoke.py \
  --expect-mode rdma-write --pause-before-final-imm --verify-kvsd
```

Expected: no replay before release; after release, source/destination digest, key count, and sampled values match, and exactly one matching `REPLDONE` appears.

- [ ] **Step 7: Commit the Slave target implementation**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  src/replication/kvs_repl.c src/main/kvstore.c tools/repl/run_repl_rdma_smoke.py
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "feat: expose file-backed fullsync remote MR"
```

### Task 4: Master MR Negotiation and Complete WRITE of the KVSD Range

**Files:**
- Modify: `src/main/kvstore.c:590-690`
- Modify: `src/main/kvstore.c:1182-1295`
- Modify: `src/replication/kvs_repl.c:1028-1110`
- Modify: `src/replication/kvs_repl.c:1848-1905`
- Modify: `src/replication/kvs_repl.c:2169-2230`
- Test: `tools/repl/run_repl_rdma_smoke.py`

**Interfaces:**
- Consumes: remote MR message and Slave target from Tasks 1-3.
- Produces:
  - `int repl_fullsync_wait_remote_mr(conn_t *c, uint64_t transfer_id, uint64_t total_bytes, int timeout_ms)`.
  - `int repl_rdma_begin_write(uint64_t transfer_id, const kvs_fullsync_mr_msg_t *mr, uint64_t total_bytes)`.
  - `int repl_rdma_map_source(int fd, size_t total_bytes, void **map_out, struct ibv_mr **mr_out)`: `mmap` the KVSD dump file read-only and `ibv_reg_mr` it as a single MR (`IBV_ACCESS_LOCAL_WRITE`). This is the zero-copy source; the data path never `memcpy`s snapshot bytes into registered send buffers.
  - `int repl_rdma_post_write(size_t len, uint64_t file_offset, int final)`: post a WRITE whose SGE references the mapped source MR at `src_map + file_offset`, to remote `remote_base + file_offset`.
  - `int repl_rdma_wait_write_completions(int timeout_ms)`.
  - `int repl_rdma_unmap_source(void)` and `int repl_rdma_sendfile_fallback(int dst_fd, int src_fd, uint64_t remaining)`.

- [ ] **Step 1: Add smoke assertions for offset zero and final WRITE_WITH_IMM**

Instrument the existing test-only RDMA counters so the harness requires: selected mode `rdma-write`, first remote offset `0`, total posted bytes exactly equal to `total_bytes`, ordinary WRITE count greater than zero, exactly one final WRITE_WITH_IMM, and no RDMA SEND payload WR in a successful WRITE attempt.

- [ ] **Step 2: Run the smoke test and verify RED**

```bash
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_smoke.py \
  --expect-mode rdma-write --assert-write-range
```

Expected: failure because the Master neither waits for the MR nor writes the complete range from offset zero.

- [ ] **Step 3: Send transfer-aware metadata and wait safely for the MR**

Change the header to:

```text
+FULLRESYNC <replid> <offset> <total_bytes> <transfer_id> rdma-write\r\n
```

Generate a monotonically increasing nonzero transfer ID per process and assign it to the connection before sending the header. `repl_fullsync_wait_remote_mr` must use a bounded deadline and `MSG_PEEK` to wait for a complete CRLF line. Consume only a syntactically recognized `FULLRESYNCWR`; preserve unrelated bytes for the normal parser. Validate transfer ID, active RDMA owner, nonzero addr/rkey, and capacity before setting WRITE mode.

- [ ] **Step 4: Map the source file, remove the first-packet SEND warmup, and post the entire file as WRITE**

Before the chunk loop, call `repl_rdma_map_source(fd, total_bytes, &src_map, &src_mr)` so every SGE references the mapped file (`lkey = src_mr->lkey`, `addr = src_map + file_offset`); there is no `memcpy` and no read-into-slot loop on the data path. Delete the branch that posts the first snapshot chunk as `IBV_WR_SEND`. For every chunk, call `kvs_fullsync_remote_addr(remote_base, remote_capacity, file_offset, len, &remote_addr)` before posting. Set:

```c
wr.sg_list->lkey  = src_mr->lkey;
wr.sg_list->addr  = (uintptr_t)(src_map + file_offset);
wr.opcode = final ? IBV_WR_RDMA_WRITE_WITH_IMM : IBV_WR_RDMA_WRITE;
wr.wr.rdma.remote_addr = remote_addr;
wr.wr.rdma.rkey = remote_rkey;
```

Only the final chunk carries immediate data. Use the production queue depth and signaling interval; increment `write_offset` by `len` only after a successful post. After the last completion, unregister/unmap/close in `repl_rdma_unmap_source`.

- [ ] **Step 5: Gate mode transition on all sender completions**

Track posted and completed signaled ranges, fail on any non-success WC, and require `write_offset == total_bytes` plus final signaled completion before entering `WAIT_REPLDONE`. Do not clear transfer state at local CQ completion.

- [ ] **Step 6: Run protocol, build, and smoke tests**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore test_fullsync_protocol
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/test_fullsync_protocol
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean all ENABLE_RDMA=1
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_smoke.py \
  --expect-mode rdma-write --assert-write-range --verify-kvsd
```

Expected: protocol tests pass; smoke counters prove offset zero through exact EOF, one final IMM, no SEND payload; KVSD and sampled data match.

- [ ] **Step 7: Commit Master WRITE support**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  src/main/kvstore.c src/replication/kvs_repl.c tools/repl/run_repl_rdma_smoke.py
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "feat: transfer full snapshots with one-sided RDMA"
```

### Task 5: REPLDONE Boundary, Gap Replay, and sendfile Fallback

**Files:**
- Modify: `src/main/kvstore.c:590-690`
- Modify: `src/main/kvstore.c:1241-1305`
- Modify: `src/replication/kvs_repl.c:1848-1905`
- Modify: `src/replication/kvs_repl.c:2311-2520`
- Modify: `tools/repl/run_repl_rdma_stress.py:226-430`
- Test: `tools/repl/run_repl_rdma_smoke.py`

**Interfaces:**
- Consumes: transfer-aware completion and cleanup from Tasks 1-4.
- Produces:
  - `int repl_fullsync_restart(conn_t *c, kvs_fullsync_mode_t next_mode, const char *reason)`; `next_mode` is `KVS_FULLSYNC_TCP` (sendfile) for every WRITE fallback this iteration.
  - `int repl_rdma_sendfile_fallback(int dst_fd, int src_fd, off_t offset, size_t len)`; loops `sendfile()` until the whole remaining KVSD range is sent.
  - Test injection controls `KVS_TEST_FULLSYNC_MR_TIMEOUT` and `KVS_TEST_FULLSYNC_WRITE_FAIL_AFTER`, compiled or honored only in test mode. No SEND-failure injection: RDMA SEND is not a production data path.

- [ ] **Step 1: Add failing completion-boundary and fallback scenarios**

Extend the harness with four assertions:

1. Sender CQ completion without `REPLDONE` leaves Master full sync pending and does not replay gap.
2. Matching `REPLDONE` replays the gap through backlog/realtime path and advances the Slave offset.
3. Injected WRITE failure removes the first target and succeeds with a new transfer ID over TCP `sendfile`.
4. MR timeout (or a no-RDMA run) succeeds directly over TCP `sendfile`.

- [ ] **Step 2: Run the scenarios and verify RED**

```bash
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --inject-mr-timeout
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --inject-write-failure
```

Expected: unsupported arguments or incorrect reuse/completion behavior.

- [ ] **Step 3: Move all gap replay behind validated REPLDONE**

Remove snapshot-adjacent gap transmission from `queue_snapshot`. Parse `REPLDONE <transfer_id> <bytes>` with Task 1 helpers. Accept it only when ID, byte count, owner connection, and state `WAIT_REPLDONE` match. Then replay from `snap_base_offset` through the existing backlog/realtime path, never through `repl_fullsync_send`, and finally clear full-sync state.

- [ ] **Step 4: Implement attempt restart with a fresh identity and target**

On MR timeout/rejection, start `KVS_FULLSYNC_TCP` (sendfile) with a new transfer ID. On WRITE post/CQ/connection failure, send an abort/restart control transition, make the Slave clean and unlink the partial mapped target, then start sendfile from byte zero. Never continue an old byte offset across modes, and never restart a WRITE on the same partially written target.

- [ ] **Step 5: Add explicit diagnostics and status fields**

For each attempt log transfer ID, requested mode, negotiated mode, byte count, state, failure phase, provider status/errno, and next mode. Expose enough state through existing INFO/debug output for the Python harness to distinguish WRITE, SEND, and TCP without parsing incidental log prose.

- [ ] **Step 6: Run all fallback and disconnect cases**

```bash
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --inject-mr-timeout --expect-path rdma-write,sendfile
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --inject-write-failure --expect-path rdma-write,sendfile
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --no-rdma --expect-path sendfile
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --disconnect-during-write
```

Expected: every successful fallback has a new transfer ID, no incomplete receive file remains, data checks pass, and disconnect cleanup reports no leaked active target. The harness must verify the negotiated path is `sendfile` (TCP), never `rdma-send`.

- [ ] **Step 7: Run the complete local replication regression set**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore check-repl-rdma-smoke
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore check-repl-rdma-stress
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore check-repl-rdma-fallback
```

Expected: all targets pass. Record any target missing from the existing Makefile and invoke its Python script directly rather than silently skipping it.

- [ ] **Step 8: Commit completion and fallback semantics**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  src/main/kvstore.c src/replication/kvs_repl.c \
  tools/repl/run_repl_rdma_smoke.py tools/repl/run_repl_rdma_stress.py
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "fix: restart fullsync safely across transport fallback"
```

### Task 6: One-Sided KVSD File Benchmark — **DEFERRED to follow-up iteration (do not execute)**

> Skipped this iteration per user decision ("测试后面再考虑"). Detailed steps below are preserved for the follow-up plan. The current iteration's benchmark scope is limited to: verifying the production full sync actually used WRITE (counters/logs), plus the existing SEND-based file numbers left labeled as two-sided.

**Files:**
- Modify: `tests/perf/test_rdma_throughput.c:52-90`
- Modify: `tests/perf/test_rdma_throughput.c:169-342`
- Modify: `tests/perf/test_rdma_throughput.c:346-724`
- Modify: `tests/perf/test_rdma_throughput.c:800-870`
- Modify: `tests/perf/Makefile.perf:87-130`

**Interfaces:**
- Consumes: the production semantics of file-backed remote MR, final WRITE_WITH_IMM, and receiver ACK.
- Produces:
  - CLI modes `--mode rdma-send` and `--mode rdma-write` for file tests.
  - Machine-readable result fields `setup_ms`, `transfer_ms`, `transfer_gbps`, `e2e_ms`, `e2e_gbps`, `bytes`, `digest`, `mode`, and `verified`.

- [ ] **Step 1: Add a small deterministic correctness target that requires WRITE**

Add `test-fullsync-file-small` to generate a deterministic KVSD file, start the receiver with a distinct destination path, run `--mode rdma-write`, and finish with `cmp source destination`. Ensure the command fails if output reports `rdma-send`.

- [ ] **Step 2: Run the small target and verify RED**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf \
  -f Makefile.perf test-fullsync-file-small
```

Expected: failure because file mode currently forces SEND and does not create/acknowledge a mapped destination file.

- [ ] **Step 3: Extend private/control metadata**

Replace the two-field MR metadata with network-byte-order fields for protocol version, transfer ID, address, rkey, and capacity. Reject wrong version, zero fields, and a capacity smaller than the source. Keep the SEND mode handshake separate and explicitly labeled.

- [ ] **Step 4: Implement file-backed receiver and one-sided sender**

For `rdma-write`, receiver performs `open(O_EXCL)`, `ftruncate`, `mmap(MAP_SHARED)`, and `ibv_reg_mr(... REMOTE_WRITE)`. Sender posts successive `IBV_WR_RDMA_WRITE` operations and one final `IBV_WR_RDMA_WRITE_WITH_IMM`; remove the `--file` mode override to SEND. Retain existing local registered slot buffers, queue depth, and batched signaling so the benchmark measures the provider path rather than an artificially synchronous WR loop.

- [ ] **Step 5: Add transfer-only and end-to-end clocks**

Record `transfer_start` immediately before the first payload post and `transfer_end` at final sender CQ. Receiver performs `msync`, `fsync`, and deterministic digest/full comparison, then sends an ACK containing transfer ID, byte count, and verification result over the control socket. Record `e2e_end` when the sender receives that ACK. Report setup time separately from both intervals.

- [ ] **Step 6: Make correctness failures fail the process**

Return nonzero for mismatched IMM, byte count, transfer ID, digest, capacity, CQ status, short ACK, or persistence error. Print `verified=true` only after receiver validation succeeds.

- [ ] **Step 7: Run small and full-size correctness tests**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf -f Makefile.perf clean all
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf -f Makefile.perf test-fullsync-file-small
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf -f Makefile.perf test-fullsync-file
cmp /tmp/fullsync_dump.bin /tmp/fullsync_dump.rdma-write.recv
```

Expected: both modes complete, full file size is 85,000,008 bytes, `cmp` succeeds, and output includes both timing intervals.

- [ ] **Step 8: Commit the one-sided benchmark**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  tests/perf/test_rdma_throughput.c tests/perf/Makefile.perf
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "perf: benchmark one-sided RDMA file transfer"
```

### Task 7: Fair Comparison Harness and Environment Metadata — **DEFERRED to follow-up iteration (do not execute)**

> Skipped this iteration; preserved for the follow-up plan alongside Task 6.

**Files:**
- Modify: `tests/perf/run_throughput_compare.sh:1-300`
- Modify: `README.md:2080-2130`

**Interfaces:**
- Consumes: benchmark result fields from Task 6.
- Produces: one CSV/JSON result row per `sendfile`, `rdma-send`, and `rdma-write` run, including topology and provider metadata.

- [ ] **Step 1: Add a harness self-check for three distinct modes**

Add `--dry-run` that prints commands without starting servers. Assert from shell that output contains exactly one labeled command for each transport and that `rdma-write` includes the destination file and verification flags.

- [ ] **Step 2: Run dry-run and verify RED**

```bash
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf/run_throughput_compare.sh --dry-run | tee /tmp/fullsync-bench-dry-run.txt
grep -c 'mode=sendfile' /tmp/fullsync-bench-dry-run.txt
grep -c 'mode=rdma-send' /tmp/fullsync-bench-dry-run.txt
grep -c 'mode=rdma-write' /tmp/fullsync-bench-dry-run.txt
```

Expected: the current harness does not emit three correctly distinguished modes.

- [ ] **Step 3: Run all modes with identical payload and explicit addresses**

Accept separate server/client addresses and a topology label. Run the same KVSD source size for all modes. Do not silently rewrite `127.0.0.1`; print both requested and effective addresses. For same-host comparison, support a non-loopback TCP/sendfile address so RXE and TCP can be compared through the same host interface in addition to the documented loopback result.

- [ ] **Step 4: Capture interpretation metadata**

Record timestamp supplied by the shell, hostname, kernel, CPU model/count, requested/effective addresses, interface, `rdma link`, `ibv_devices`, provider/device, chunk, slot count, queue depth, signal interval, bytes, setup time, transfer-only result, end-to-end result, digest, and verification status. If an optional command is unavailable, record `unavailable` rather than omitting the field.

- [ ] **Step 5: Update documentation labels without claiming unmeasured results**

Document that earlier file results measured RDMA SEND, define both timing intervals, explain the local loopback/non-loopback distinction, and provide commands for rerunning. Do not place new performance numbers in README until Task 9 has produced verified artifacts.

- [ ] **Step 6: Run dry-run and a local three-mode comparison**

```bash
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf/run_throughput_compare.sh --dry-run
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf/run_throughput_compare.sh \
  --topology same-host-nonloopback --server-address 192.168.233.128 \
  --client-address 192.168.233.128 --verify
```

Expected: three verified rows with distinct mode labels and complete metadata. If `192.168.233.128` is not assigned locally, use the effective non-loopback address discovered by the harness and record it.

- [ ] **Step 7: Commit the comparison harness**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  tests/perf/run_throughput_compare.sh README.md
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "perf: report fullsync transport timing consistently"
```

### Task 8: Local End-to-End Verification and Review

**Files:**
- Modify only if failures expose a confirmed root cause in files from Tasks 1-7.
- Review: all changed files.

**Interfaces:**
- Consumes: complete production and benchmark implementation.
- Produces: reproducible local evidence before touching remote hosts.

- [ ] **Step 1: Build every supported configuration from clean state**

```bash
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean all ENABLE_RDMA=0
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore clean all ENABLE_RDMA=1
make -C /home/pp/Desktop/ls_study/proj/9.1-kvstore test_fullsync_protocol
/home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/test_fullsync_protocol
```

> **Pre-existing failure (documented, not caused by this change):** `ENABLE_RDMA=0` build fails at HEAD too — `repl_rdma_cq_process_wc`/`repl_rdma_cq_poll_thread`/`repl_rdma_start_cq_poll_thread`/`repl_rdma_stop_cq_poll_thread` and several other functions sit outside `#if KVS_ENABLE_RDMA` yet use verbs types (`struct ibv_wc`, `g_repl_rdma_ctx`). Confirmed via `git stash` of this work + `make clean all ENABLE_RDMA=0` at HEAD: `error: dereferencing pointer to incomplete type 'struct ibv_wc'` at `kvs_repl.c`. A full fix requires re-guarding a dozen+ functions — out of scope for this iteration (per acceptance criterion #9, document rather than expand scope). This iteration validates with `ENABLE_RDMA=1`, which is the configuration full sync actually runs under.

Expected: `ENABLE_RDMA=1`, `test_fullsync_protocol`, and the sanitizer all exit zero; `ENABLE_RDMA=0` is documented above as pre-existing.

- [ ] **Step 2: Run normal and forced-path local full sync**

```bash
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_smoke.py --expect-mode rdma-write --verify-kvsd
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --inject-mr-timeout --expect-path rdma-write,sendfile
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --inject-write-failure --expect-path rdma-write,sendfile
python3 /home/pp/Desktop/ls_study/proj/9.1-kvstore/tools/repl/run_repl_rdma_stress.py --no-rdma --expect-path sendfile
```

Expected: all data and cleanup assertions pass; negotiated paths are `rdma-write` or `sendfile`, never `rdma-send`.

- [ ] **Step 3: Deferred — local benchmark correctness and timing**

> Skipped this iteration (follow-up). Local benchmark runs with `test-fullsync-file-small`, `test-fullsync-file`, and `run_throughput_compare.sh` belong to Tasks 6-7.

- [ ] **Step 4: Review the diff for correctness**

Invoke `superpowers:requesting-code-review`. Give the reviewer the approved spec, this plan, and the complete diff. Require explicit review of MR lifetime, overflow checks, parser byte preservation, CQ/IMM ownership, fallback target disposal, and the `REPLDONE`/gap boundary.

- [ ] **Step 5: Apply only confirmed review findings and rerun affected tests**

For each accepted finding, first add or tighten a failing test, make the smallest production change, then rerun the focused test and the Task 8 build/smoke suite. Do not bundle unrelated cleanup.

- [ ] **Step 6: Commit local verification fixes**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add \
  include src tests tools README.md Makefile
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "test: verify one-sided fullsync locally"
```

Before committing, confirm `git diff --cached -- kvstore.conf` is empty and `NtyCo` is not staged.

### Task 9: Authorized Cross-Host Correctness and Performance Validation

**Files:**
- Create runtime artifacts only under a new ignored or `/tmp` result directory; do not add credentials.
- Modify `README.md` only after results are verified and reproducible.

**Interfaces:**
- Consumes: locally verified binaries and scripts from Task 8.
- Produces: cross-host correctness logs, mode evidence, environment metadata, and three-mode timing results.

- [ ] **Step 1: Inspect both hosts before changing anything**

Using ephemeral authentication, collect on both `192.168.233.128` and `192.168.233.129`: hostname, kernel, current kvstore processes, listening ports, candidate dump paths, free disk, route between hosts, `rdma link`, `ibv_devices`, and relevant interface addresses/offloads. Stop if a planned port/path belongs to an unrelated process or existing data set.

- [ ] **Step 2: Build or stage the exact same revision on both hosts**

Record `git rev-parse HEAD` and source diff hash locally and remotely. Build with `ENABLE_RDMA=1`. Do not use `sudo` for compilation. Use a new temporary test directory and unoccupied ports discovered in Step 1.

- [ ] **Step 3: Deferred — small cross-host one-sided benchmark correctness check**

> Skipped this iteration (benchmark follow-up). The production Master/Slave full sync in Step 4 covers byte-for-byte correctness.

- [ ] **Step 4: Run real kvstore Master-to-Slave full sync**

Populate deterministic data on the Master, configure the Slave to follow it, and require status/debug output to report `rdma-write`. Verify source/destination KVSD digest, expected key count, sampled values, expiration semantics, and matching replication offset after `REPLDONE`.

- [ ] **Step 5: Verify incremental continuation after full sync**

Write a new deterministic set of keys after full-sync completion. Require those keys and deletes/expirations to appear on the Slave and confirm the replication offset advances without a second full sync. This proves gap/backlog data was not written beyond the snapshot MR.

- [ ] **Step 6: Exercise cross-host fallback without harming unrelated services**

Run isolated instances with MR-timeout injection and WRITE-failure injection. Confirm distinct transfer IDs, cleanup of old temporary files, and a correct TCP `sendfile` fallback from byte zero. Verify the negotiated fallback is `sendfile`, never `rdma-send`.

- [ ] **Step 7: Deferred — three-mode full KVSD comparison**

> Skipped this iteration (benchmark follow-up, Tasks 6-7).

- [ ] **Step 8: Deferred — bottleneck analysis**

> Skipped this iteration (depends on deferred benchmark results). The "RDMA ~394 Mbps" SEND numbers remain historical SEND results.

- [ ] **Step 9: Update README with the new negotiation semantics and verified correctness**

Document that full sync now negotiates `rdma-write` (one-sided, file-backed, zero-copy) with a `sendfile` fallback, plus the new `repl_rdma_write_buf_max_mb` config. Preserve the earlier SEND numbers as historical two-sided SEND results rather than relabeling them. Do not add new throughput numbers this iteration (benchmark follow-up).

- [ ] **Step 10: Run final verification before claiming completion**

Invoke `superpowers:verification-before-completion`, rerun the focused protocol test, clean RDMA build, local smoke, cross-host WRITE correctness check, and validate every reported artifact. State any skipped test or unresolved failure plainly.

- [ ] **Step 11: Commit documentation only if requested**

```bash
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore add README.md
git -C /home/pp/Desktop/ls_study/proj/9.1-kvstore commit -m "docs: record one-sided fullsync validation"
```

Do not commit benchmark artifacts unless the project already tracks that artifact class and the user explicitly asks.

## Plan Self-Review

- Spec coverage: MR negotiation, file-backed target, master-side mapped source (zero-copy), full-range WRITE, final IMM, `REPLDONE`, deferred gap, all cleanup paths, sendfile fallback, `repl_rdma_write_buf_max_mb` config, local validation, and cross-host validation each have an owning task.
- Placeholder scan: Tasks 6-7 and the benchmark steps in Tasks 8-9 are explicitly marked DEFERRED (not placeholders); every non-deferred step has concrete actions.
- Type consistency: transfer ID is `uint64_t`, capacities/byte counts are `uint64_t`, remote addresses are `uint64_t`, and each production boundary validates conversion to `size_t` or `off_t` before use.
- Scope: the benchmark redesign (Tasks 6-7) is deferred to a follow-up iteration per user decision; this plan executes production WRITE + sendfile fallback and correctness validation only.
