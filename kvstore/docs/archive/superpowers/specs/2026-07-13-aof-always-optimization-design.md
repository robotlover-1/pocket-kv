# AOF Always 策略优化设计

## 目标

优化 AOF always 策略的 QPS，约束：

- 严格保持语义：一条命令 = 一次独立 write + fdatasync
- 必须使用 io_uring 做落盘
- fsync 完成后才回复客户端
- 只改 persistence 层，不动 eBPF proxy / replication 层

## 当前状态

QPS 基线约 39,040（`bash tools/bench/run_persist_bench.sh`），每命令开销：

```text
malloc(resp) → 获取 2 个 SQE → 填充 write+fsync → malloc(pending) → TAILQ 入队
→ 批量 io_uring_submit() → 内核写 eventfd → epoll 唤醒 → read eventfd
→ peek 2 个 CQE → queue_bytes 回复 → free(resp) + free(pending)
```

存在问题：

- `g_aof_write_offset` 单变量同时用于 SQE 填充和 CQE 确认，管道内多条命令共用同一 offset，隐式依赖 ring 深度容错
- 每条命令 `kvs_malloc` + `kvs_free` 各两次（pending + resp）
- TAILQ 顺序依赖：CQE 回收按 TAILQ 头取 slot，耦合 CQE 顺序与入队顺序

## 架构

```text
当前（串行）:
  填充 SQE → 暂存 → 批量 submit → 等待 CQE(epoll) → reap → 回复
  一批结束才能开始下一批

优化后（流水线）:
  Batch N SQE 填充 → submit → ┐
                              ├→ 同时进行
  Batch N+1 SQE 填充 → submit ┘
                              ┐
  Batch N CQE reap → 回复      ├→ 同时进行
                              ┘
  Batch N+1 CQE reap → 回复
```

## 变更详情

### 1. offset 拆分

```text
g_aof_write_submitted   — SQE 填充时立即 += len（预约位置）
g_aof_write_confirmed   — write CQE 成功后 += res（确认落盘）
```

- `persist_append_prepare` 用 `submitted` 做 offset，管道内多条命令各拿各的位置
- `dump/snapshot/rewrite` 用 `confirmed`
- 删除旧 `g_aof_write_offset`

### 2. pending ring buffer 替代 TAILQ + malloc

```c
#define PERSIST_PENDING_RING_SIZE  512

typedef struct {
    conn_t        *conn;
    unsigned char *resp;
    size_t         resp_len;
    uint8_t        cqe_count;    // 0→1→2
    uint8_t        cqe_ok;
    int8_t         last_error;
} persist_pending_slot_t;

static persist_pending_slot_t  g_pending_slots[512];
static uint32_t                g_pending_head = 0;
static uint32_t                g_pending_tail = 0;
```

- 删除 `TAILQ_HEAD`、`TAILQ_ENTRY`、`persist_pending_t` 的 malloc/free
- `persist_pending_enqueue` 只填充当前 slot 的 conn/resp（可选，slave 为 NULL）
- `cqe_seen` → `cqe_count`，语义相同

### 3. user_data 索引 slot

SQE 填充时写 `user_data = slot_idx`，CQE 回收时直接索引：

```c
sqe_w->user_data = slot_idx;
sqe_f->user_data = slot_idx;
```

`persist_reap_cqes` 不再从 TAILQ 头部取 pending 项，改为 `cqe->user_data` 直接定位 slot。消除 CQE 回收对入队顺序的依赖。

### 4. 流水线化算法

`persist_append_prepare`：

1. `pending_ring_reserve()` 预留 slot（ring 满则强制 reap + 必要时阻塞等待）
2. 填充 linked write+fsync SQE 对，user_data = slot_idx
3. `g_aof_write_submitted += len`

`persist_reap_cqes`：

1. `io_uring_peek_cqe` → 取 user_data → 索引 slot
2. write CQE 成功 → `g_aof_write_confirmed += res`
3. slot->cqe_count == 2 时：回复客户端（如有 conn），释放 resp，advance tail

### 5. io_uring 配置

```c
struct io_uring_params params = {0};
params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;
io_uring_queue_init_params(1024, &g_persist_uring, &params);
```

| 变更 | 原因 |
| --- | --- |
| ring 256→1024 | 更多 in-flight SQE，减少提前 flush |
| SINGLE_ISSUER | 只有主线程提交，跳过内核 SQ 锁 |
| COOP_TASKRUN | 减少 task-work IPI |
| 不用 DEFER_TASKRUN | 不兼容 eventfd，破坏现有事件模型 |

`sq_space_left` 阈值从 2 调整为 64。

### 6. 错误处理

| 错误 | 处理 |
| --- | --- |
| SQE 耗尽 | 强制 submit+reap 后重试，仍失败返回 ERR |
| write/fsync CQE 失败 | 标记 slot 错误 + `g_persist_fatal_error = 1` |
| ring buffer 满 | 强制 reap → 仍满则 `io_uring_submit_and_wait(1)` 阻塞等待 |

`g_persist_fatal_error` 传播逻辑不变：置位后所有 `persist_append_prepare` 返回 ERR，调用方向客户端返回 `"AOF write failed"`。

### 7. slave 复制路径

slave 的 from_replication 路径调用 `persist_append_prepare` 但不调 `persist_pending_enqueue`。新设计中 slot 的 conn/resp 为空，reap 时不发送回复，仅推进 confirmed offset 和释放 slot。不再产生 `"CQE without pending entry"` stderr 输出。

## 不变部分

- 一条命令 = 一对 linked write+fdatasync SQEs
- fsync 完成后才回复客户端（master 路径）
- 全局 `g_persist_uring` 单实例
- eventfd + epoll 异步通知模式
- `IOSQE_IO_LINK` 链接 write → fdatasync

## 验收标准

- QPS ≥ 当前基线（40,000），目标提升 20-40%
- `bash tools/bench/run_persist_bench.sh` 通过
- `tests/` 现有测试套件全量通过
- eBPF proxy slave 路径无 stderr 报错
- dump/snapshot/bgrewriteaof 功能正常
