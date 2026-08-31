# Full Sync One-Sided RDMA Design

Date: 2026-07-31
Status: Approved design

## Goal

Change kvstore full synchronization so that the preferred data path is one-sided RDMA WRITE into a Slave-owned, file-backed memory region, with the Master reading the snapshot directly from a mapped source file (zero-copy, no memcpy into registered send buffers). When WRITE is unavailable or fails, the full-sync data falls back to zero-copy TCP `sendfile`; two-sided RDMA SEND is no longer a production data fallback. A configurable limit, `repl_rdma_write_buf_max_mb`, controls the maximum snapshot size accepted for the file-backed remote MR. Validate correctness locally and across `192.168.233.128 -> 192.168.233.129`.

The KVSD file-transfer benchmark redesign is **deferred to a follow-up iteration** (see "Deferred: Benchmark Redesign" below). This iteration covers the production full-sync path only.

## Current Problem

The production code contains `IBV_WR_RDMA_WRITE` and `IBV_WR_RDMA_WRITE_WITH_IMM`, but the Master synchronously generates and sends the snapshot while handling `REPLSYNC`. The Slave returns its remote MR through the same TCP control connection, and the Master does not process that response before snapshot transmission. As a result, the WRITE mode is not reliably activated and the snapshot body is likely sent through RDMA SEND/RECV.

The KVSD throughput benchmark explicitly forces file mode to RDMA SEND. Its reported RDMA number therefore does not measure one-sided RDMA and does not validate the intended production data path.

## Scope

This change includes:

- The Master/Slave full-sync MR negotiation and state transitions.
- One-sided WRITE of the complete KVSD dump, sourced directly from a mapped Master file (zero-copy).
- Completion notification with WRITE_WITH_IMM and application-level completion with `REPLDONE`.
- Fallback from WRITE to zero-copy TCP `sendfile`.
- A configurable `repl_rdma_write_buf_max_mb` limit for the Slave file-backed remote MR.
- Resource cleanup for every success, timeout, disconnect, and fallback path.
- Local and two-host correctness validation.

This change does not redesign incremental replication, replace the TCP control plane, convert the whole replication reactor into a general asynchronous state machine, or redesign the KVSD file benchmark (deferred).

## Architecture

### Control Plane

TCP remains the authoritative control plane. It carries:

- `REPLSYNC`
- `FULLRESYNC`
- The Slave remote-MR response
- Fallback/error state
- `REPLDONE`

Every full sync has a transfer identifier. The identifier binds the TCP control messages, RDMA connection, remote MR, expected byte count, temporary file, and final completion. Responses for an old or different transfer must not activate WRITE mode.

### Data Plane Priority

The full-sync data path is selected in this order:

1. One-sided RDMA WRITE (preferred).
2. Zero-copy TCP `sendfile` (fallback).

Two-sided RDMA SEND is **not** a production data fallback. It remains only for control/header transmission on the RDMA path and as a separate benchmark baseline in the deferred follow-up. If a full sync negotiates WRITE, every KVSD payload byte travels by RDMA WRITE; otherwise the entire payload travels by `sendfile` over the TCP control connection.

WRITE is selected only after the Master has validated the Slave MR response. A partially written target must never be reused by a fallback transfer. If WRITE fails after data transfer starts, the current Slave target is discarded and the fallback starts with a fresh target and a clean transfer state.

The Master's WRITE source is the mapped KVSD dump file itself: the Master registers the file mapping as a single MR and posts SGEs that reference the mapping directly. No snapshot bytes are copied into registered send buffers on the data path.

### Slave Target Region

After receiving full-sync metadata, the Slave:

1. Creates a uniquely named temporary receive file.
2. Resizes it to exactly `total_bytes` with `ftruncate`.
3. Maps the complete file writable with `mmap`.
4. Registers the mapping with local-write and remote-write access.
5. Returns the transfer ID, base address, rkey, and capacity to the Master over TCP.

The Slave rejects zero, oversized, stale, or conflicting transfers before publishing an MR. "Oversized" is defined by the new config option `repl_rdma_write_buf_max_mb` (default `256`): a snapshot whose `total_bytes` exceeds the limit is rejected, the Slave publishes no MR, and the Master's MR wait times out into the `sendfile` fallback. The mapping and MR remain valid until the transfer succeeds, fails, times out, or disconnects.

## Protocol and State Flow

### Master

The Master follows these states:

```text
IDLE
  -> PREPARING_SNAPSHOT
  -> WAITING_FOR_REMOTE_MR
  -> WRITING_RDMA
  -> WAITING_FOR_REPLDONE
  -> COMPLETE
```

Fallback transitions are:

```text
WAITING_FOR_REMOTE_MR -- timeout/rejection --> sendfile (TCP)
WRITING_RDMA -- failure --> restart with sendfile (TCP)
```

A fallback transfer starts a fresh transfer ID and a fresh Slave target; byte offset is never carried across modes. RDMA SEND is not used for payload in any fallback path.

The existing synchronous snapshot flow may pump only the narrowly defined full-sync control response while waiting for the remote MR. It must use a bounded timeout and must not consume unrelated commands as if they were MR responses. Any bytes read while waiting must remain correctly ordered for the normal parser.

The Master validates:

- Matching transfer ID.
- Matching live RDMA peer/session.
- Capacity greater than or equal to `total_bytes`.
- Nonzero address and rkey.
- Every remote address calculation for overflow and bounds.

Each WRITE destination is `remote_base + file_offset`. Work requests on the same QP preserve order. The last data operation uses `IBV_WR_RDMA_WRITE_WITH_IMM`; earlier operations use `IBV_WR_RDMA_WRITE`. Signaling and queue-depth batching should follow the production configuration rather than signal every work request.

A local send CQ completion proves that the local NIC or software provider completed the work request. It does not prove that the Slave has persisted or replayed the dump. The Master treats only the later TCP `REPLDONE` as application completion.

### Slave

The Slave follows these states:

```text
IDLE
  -> PREPARING_REMOTE_MR
  -> WAITING_FOR_WRITE_IMM
  -> VERIFYING_TARGET
  -> PERSISTING_TARGET
  -> REPLAYING_DUMP
  -> SENDING_REPLDONE
  -> COMPLETE
```

The Slave pre-posts the receive needed for immediate-data notification. On the final WRITE_WITH_IMM completion it verifies:

- The completion belongs to the active QP and transfer.
- The expected final offset/length is consistent with `total_bytes`.
- No prior RDMA or connection error invalidated the transfer.

Immediate data is only a notification that ordered RDMA writes reached the remote memory domain according to verbs semantics. It is not a persistence acknowledgment. The Slave then synchronizes the mapped file as required, closes or unmaps it in the correct order, replays the KVSD data, renames the successful temporary file, and finally sends `REPLDONE`.

## Failure Handling and Cleanup

All exits use one idempotent cleanup routine per side.

Master cleanup releases or resets:

- Active transfer ID and mode.
- Remote address, rkey, capacity, and write offset.
- Pending WR/CQ accounting.
- Cooldown/error state where applicable.

Slave cleanup releases in safe order:

1. Stop accepting completions for the transfer.
2. Deregister the MR.
3. Unmap the mapping.
4. Close the file descriptor.
5. Remove the incomplete temporary file.
6. Clear transfer metadata.

Timeout, stale response, disconnect, CQ error, bounds error, short snapshot read, replay failure, and persistence failure must all reach cleanup. Failure logs identify the transfer ID, selected path, phase, errno or verbs status, and chosen fallback without logging credentials or sensitive memory values beyond what existing diagnostics require.

## Deferred: Benchmark Redesign (follow-up iteration)

The KVSD file-transfer benchmark redesign is **out of scope for this iteration**. It will be picked up in a follow-up iteration once the production path is validated. Deferred content (preserved here for reuse):

- A real one-sided `rdma-write` file mode in `test_rdma_throughput.c` (file-backed receiver destination, sender posting WRITE from a mapped source, final WRITE_WITH_IMM, receiver ACK).
- Explicitly labeled modes `sendfile`, `rdma-send` (two-sided baseline), and `rdma-write`; no SEND result may be labeled as one-sided.
- Both transfer-only and end-to-end timing, with setup cost reported separately.
- Environment metadata per run (chunk, queue depth, signal interval, interface, provider/device, CPU, topology).

Until then, the current README SEND-based file numbers remain labeled as two-sided RDMA SEND results and are not claimed as one-sided measurements.

## Testing Strategy

### Focused Tests

Add or extend tests for:

- Valid MR response activates WRITE mode.
- Stale transfer ID is rejected.
- Too-small capacity and address overflow are rejected.
- MR timeout selects the `sendfile` fallback.
- WRITE failure discards the partial target before a fresh `sendfile` restart from byte zero.
- `sendfile` completes a correct full sync when no remote MR is available.
- Final immediate notification is required before replay.
- `REPLDONE` is required before the Master reports full-sync completion.
- Cleanup is safe when called more than once.
- Destination KVSD bytes exactly match the source.

### Local Validation

Run:

- Build and existing replication tests.
- A real local Master/Slave full sync, followed by key-count and sampled value checks.
- Forced MR timeout and WRITE error to exercise the sendfile fallback, plus a no-RDMA run to exercise sendfile directly.

Local results must explicitly note that TCP loopback and Soft-RoCE over a non-loopback interface are different paths. Where possible, add a TCP/sendfile run through the same non-loopback interface address.

### Cross-Host Validation

Use `192.168.233.128` as Master and `192.168.233.129` as Slave. Before running destructive or environment-changing setup, inspect current services, ports, files, RDMA devices, and repository state. Do not overwrite an existing dump or terminate an unrelated service.

Validate:

- Both hosts expose compatible RDMA devices and routes.
- The negotiated full-sync mode is `rdma-write` in logs/counters.
- A KVSD dump transfers byte-for-byte correctly.
- Slave replay produces the expected key count and sampled values.
- Incremental replication resumes at the correct offset after full sync.
- The `sendfile` fallback completes a correct full sync when the MR handshake is forced to time out.
- CPU and provider/interface metadata needed to interpret Soft-RoCE results.

(Three-mode benchmark timing is deferred; do not claim WRITE-vs-sendfile throughput numbers in this iteration.)

Credentials supplied for this session are used only interactively or through ephemeral process input. They are not written to source files, scripts, command logs, design documents, shell history, or benchmark artifacts.

## Acceptance Criteria

The work is complete when:

1. A normal full sync negotiates a Slave MR before payload transmission and uses one-sided RDMA WRITE for the KVSD body, sourced directly from the mapped file (no memcpy on the data path).
2. The final payload operation uses WRITE_WITH_IMM and the Slave does not replay before receiving its completion.
3. The Master does not report completion before `REPLDONE`.
4. WRITE and `sendfile` paths each complete a correct full sync, with the tested WRITE-to-sendfile fallback on MR timeout and on WRITE failure.
5. Partial WRITE targets are never reused by fallback transfers.
6. The `repl_rdma_write_buf_max_mb` config rejects oversized snapshots and drives them to the sendfile fallback.
7. Source and destination KVSD files match, and the cross-host Slave contains the expected data after replay.
8. Incremental replication resumes at the correct offset after full sync; gap/backlog bytes never land in the snapshot MR.
9. Existing replication tests still pass, or any pre-existing failure is documented with evidence.

The benchmark redesign is explicitly deferred and is not an acceptance gate for this iteration.
