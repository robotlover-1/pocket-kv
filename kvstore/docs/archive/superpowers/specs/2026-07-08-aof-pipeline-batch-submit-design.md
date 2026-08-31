# Pipeline AOF always 批量 Submit 优化

**日期**：2026-07-08
**状态**：设计审核中

---

## 背景

Pipeline AOF always 场景下，P=10/20 时 QPS 反而低于 P=1，P=160 仅 128K（Redis 的 22%）。

根因：`persist_append_raw()` 每条命令独立调用 `io_uring_submit()`（= `io_uring_enter` 系统调用），pipeline 批量 recv 后，IO subit、fdatasync、响应发送三个环节完全碎片化。

### 当前数据

| P 深度 | kvstore AOF always | Redis AOF always | kv/redis |
|--------|--------------------|------------------|----------|
| 1      | 39,040             | 42,434           | 92%      |
| 10     | 11,622             | 243,724          | 5%       |
| 20     | 22,722             | 361,141          | 6%       |
| 40     | 44,845             | 456,830          | 10%      |
| 80     | 88,386             | 524,659          | 17%      |
| 160    | 128,469            | 583,090          | 22%      |

### P=10/20 QPS 低于 P=1 的原因

三个放大效应叠加：

1. **io_uring_submit syscall 开销 × P**：P=10 一个 reactor 周期内 10 次 `io_uring_enter`，背靠背无摊销
2. **fdatasync 串行化 + 响应分片返回**：CQE 逐个到达 eventfd，每到达一个需要独立的 epoll_wait → reap → EPOLLOUT → on_write 周期
3. **客户端等待**：redis-benchmark -P 10 发完 10 条后等全部响应，碎片化发送导致客户端大量空等

## 约束

- **保持 durable-before-reply 语义**：+OK 必须在 fdatasync 完成后才发送
- **一条命令一次 fdatasync**：不引入 group commit（多个命令共享一次 fsync）
- 不改非 reactor 路径（proactor/ntyco/AOF 恢复）
- 最小改动

## 方案：拆分 persist_append_prepare + persist_submit_pending

### 核心思路

`persist_append_raw()` 拆为两步：
- **prepare**：获取 SQE，设 `IOSQE_IO_LINK`，不调 `io_uring_submit()`
- **submit**：在 `on_read()` 处理完当前 buffer 所有命令后统一提交

SQ ring（256 深）自然反压作为自动分流阈值：每命令 2 SQE，128 条触达上限，自动中途 subit + reap。

### 时序（P=160）

```
on_read:
  recv(160条)
  cmd1~128: persist_append_prepare → 256 SQE 就绪
    cmd129: ring 满 → persist_submit_pending()  ← syscall 1
            → persist_reap_cqes() → queue_bytes(已就绪响应)
            → ring 腾出空间
  cmd129~160: persist_append_prepare → 64 SQE
  persist_submit_pending()                        ← syscall 2
  persist_reap_cqes() → queue_bytes(批量响应)
  on_write() → send(批量)
```

对比当前：160 次 subit → 2 次。

### 改动清单

| 文件 | 改动 | 行数 |
|------|------|------|
| `src/persistence/kvs_persist.c` | 新增 `persist_append_prepare()`、`persist_submit_pending()`、`persist_append_raw()` 重构为 prepare + subit | ~25 |
| `src/main/kvstore.c` | 写命令路径 `persist_append_raw` → `persist_append_prepare`（8 处） | ~5 |
| `src/core/reactor.c` | `on_read()` 中插入 `persist_submit_pending()` | ~5 |
| `include/kvstore/kvstore.h` | 新增函数声明 | ~3 |

### persist_append_prepare() 逻辑

```c
int persist_append_prepare(const unsigned char *buf, size_t len) {
    // 1. 前置检查 (fd valid, fsync mode, fatal error)
    // 2. SQ ring 空间不足时：submit 已积压 SQE + reap CQE 腾空间
    // 3. 获取 write SQE + fsync SQE（IOSQE_IO_LINK）
    // 4. 不调 io_uring_submit()
    // 5. append_to_rewrite_buffer() 照常调用（纯内存拷贝）
    return KVS_PERSIST_PENDING;
}
```

### persist_append_raw() 保持兼容

```c
int persist_append_raw(const unsigned char *buf, size_t len) {
    int rc = persist_append_prepare(buf, len);
    if (rc == KVS_PERSIST_PENDING)
        persist_submit_pending();
    return rc;
}
```

### 不改的

- `IOSQE_IO_LINK` 语义：每条命令独立 write+fsync
- `persist_pending_enqueue` / `persist_reap_cqes`：响应发送逻辑不变
- `g_aof_write_offset` 更新时机：仍在 CQE 收割时
- BGREWRITEAOF 的 `append_to_rewrite_buffer()`：保持在 `persist_append_prepare()` 中（纯内存拷贝）

### 预期效果

| P 深度 | 当前 QPS | 预期 QPS | 预期 kv/redis |
|--------|----------|----------|---------------|
| 1      | 39K      | 39K      | 92%           |
| 10     | 11.6K    | 150-200K | 60-80%        |
| 20     | 22.7K    | 250-320K | 70-90%        |
| 40     | 44.8K    | 350-420K | 75-90%        |
| 80     | 88.4K    | 420-500K | 80-95%        |
| 160    | 128K     | 480-550K | 80-95%        |

### 崩溃窗口分析

| | 当前（逐条 submit） | 方案（批量 submit） |
|---|---|---|
| 崩溃窗口大小 | 1 条命令（~1μs） | 最多 128 条（~100μs） |
| 窗口内未确认数据 | 0-1 条 | 0-128 条 |
| durable-before-reply | ✓（客户端均未收到 +OK） | ✓（客户端均未收到 +OK） |
| AOF 恢复 | 可能恢复未确认数据（副作用） | 未确认数据不在 AOF 中 |

批量 submit 的崩溃窗口更大（SQ ring 共享内存中未提交 SQE 全部丢失），但 blast radius 内的命令全都没有语义承诺——从协议正确性角度无退化。未 confirm 的数据不在 AOF 中也更干净。

### 风险

1. **SQ ring 满时的 fallback 路径**：已在 `persist_append_prepare` 中处理——ring 空间不足时自动 subit + reap
2. **`append_to_rewrite_buffer` 保持原位**：纯内存拷贝，无 IO，仍在 `persist_append_prepare()` 中调用。BGREWRITEAOF 测试验证无回归
3. **eventfd 通知依然存在**：CQE 仍通过 eventfd → epoll 通知。响应路径碎片化问题部分缓解（reap 时批量收割），但未被完全消除。方案 B（全链路批量化）可在本方案验证后再评估

### 验证计划

1. `make check` — 基础功能全套
2. `make check-bulk-1w` — 批量 1w 级回归
3. `make check-persist` — 持久化测试（验证 AOF always 语义不变）
4. `make check-demo-full-dump` — 10w 全量持久化演示
5. `make check-demo-incr-aof` — 10w 增量持久化演示
6. `bash tools/bench/run_pipeline_bench.sh` — pipeline 性能基准
