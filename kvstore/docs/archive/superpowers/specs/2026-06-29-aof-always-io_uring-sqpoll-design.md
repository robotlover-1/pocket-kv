# AOF ALWAYS per-command io_uring SQPOLL 设计

## 目标

`appendfsync always` 做到严格 per-command 落盘（每条写命令执行后 write+fsync 完成才回复客户端），落盘使用 io_uring，性能目标高于 Redis 同配置。

## 背景

### 当前实现状态

经过 commits `da91b23` → `a3d6091` → `3cf6cd6` 三次迭代后，ALWAYS 模式当前行为：

- 命令缓冲到 64KB `g_aof_buf`
- reactor 循环结束时（`persist_flush_pending`）或 2ms 超时（`persist_autosnap_cron`）触发批量 flush
- flush 使用 `pwrite()` + `fdatasync()` 直接 syscall
- **不是严格 per-command**：使用 group commit，响应延迟发送

### Commit 历史教训

| Commit | 做了什么 | 结论 |
|--------|---------|------|
| `da91b23` | per-command flush via io_uring | io_uring submit_and_wait 对单次操作开销大 |
| `a3d6091` | per-command flush via pwrite+fdatasync | 直接 syscall 比 io_uring 单操作快 |
| `3cf6cd6` | Revert 回 group commit | group commit 吞吐更高（摊销 fsync） |

关键发现：io_uring `submit_and_wait` 的 ring buffer 往返开销 > 两次直接 syscall（pwrite + fdatasync）。要 io_uring 在 per-command 场景胜出，必须消除提交 syscall。

### 性能基准（来自 benchmarks/data/persist_bench/aof_summary.csv）

| 配置 | QPS |
|------|-----|
| kvstore always（group commit） | 56,838 |
| Redis always | 40,660 |
| kvstore everysec | 114,850 |
| Redis everysec | 120,380 |

当前 group commit 已比 Redis always 高 40%，但语义不同（非严格 per-command）。

## 方案选择

在以下三个方案中选择 **方案 1: SQPOLL**：

1. **SQPOLL mode**（选中）: `IORING_SETUP_SQPOLL` 内核线程轮询 SQ，消除 io_uring_enter 提交 syscall
2. 固定资源注册: `io_uring_register_files` + `io_uring_register_buffers` 减少内核 per-op 开销
3. SQPOLL + 固定资源组合: 复杂度更高，单 fd 场景固定资源收益有限

选择理由：per-command 场景下 io_uring 的主要劣势是 `submit_and_wait` syscall 开销，SQPOLL 精确解决此问题。方案 2 的单 fd/buffer 注册收益不大。

## 架构

### 双 Ring 设计

```
g_persist_uring              — 现有，64 entries, 无 SQPOLL — EVERYSEC 批量用
g_persist_uring_sqpoll       — 新增，64 entries, SQPOLL    — ALWAYS per-command 用
```

两个 ring 互斥使用（同一时刻只有一个模式 active），懒初始化。ALWAYS 下 SQPOLL 内核线程存在，切换到 EVERYSEC 时销毁。

### 数据流

```
ALWAYS:
  handle_parsed_command()
    → persist_append_raw(raw, rawlen)
        → persist_aof_per_command_flush(buf, len)    // 直接落盘，不走 g_aof_buf
            → io_uring_get_sqe ×2
            → prep_write + prep_fsync(IOSQE_IO_LINK)
            → io_uring_submit                         // 通知 SQPOLL 线程
            → io_uring_wait_cqe ×2                   // 等 write + fsync 完成
            → g_aof_write_offset += written
    → queue_bytes                                    // 落盘完成后立即回复
```

```
EVERYSEC（不变）:
  handle_parsed_command()
    → persist_append_raw(raw, rawlen)                // 缓冲到 g_aof_buf
    → queue_bytes                                    // 立即回复
  ... 每 1s ...
    persist_autosnap_cron()
      → persist_aof_flush_buffer()
          → persist_write_and_fsync_uring()          // g_persist_uring 批量
```

## 详细设计

### 新增函数

#### persist_uring_sqpoll_init_once()

```c
static int persist_uring_sqpoll_init_once(void) {
    if (g_persist_uring_sqpoll_ready) return 0;
    struct io_uring_params p = {0};
    p.flags = IORING_SETUP_SQPOLL;
    p.sq_thread_idle = 2000;  // 2s 无请求后内核线程休眠 (kernel 5.11+)
    if (io_uring_queue_init_params(64, &g_persist_uring_sqpoll, &p) != 0)
        return -1;
    g_persist_uring_sqpoll_ready = 1;
    return 0;
}
```

关键参数：

- `IORING_SETUP_SQPOLL`: 创建内核线程自动消费 SQ entries
- `sq_thread_idle = 2000`: 2 秒无请求后内核线程休眠，旧内核忽略此字段
- Ring 大小 64: per-command 场景每次只消费 2 个 SQE，64 足够

#### persist_aof_per_command_flush(buf, len)

```c
static int persist_aof_per_command_flush(const unsigned char *buf, size_t len) {
    struct io_uring_sqe *sqe_w, *sqe_f;
    struct io_uring_cqe *cqe;
    int written;

    if (len == 0) return 0;

    // SQPOLL 不可用时降级到 pwrite+fdatasync
    if (persist_uring_sqpoll_init_once() != 0) {
        off_t off = (off_t)g_aof_write_offset;
        if (persist_write_fd_sync(g_aof_fd, buf, len, &off) != 0) return -1;
        if (fdatasync(g_aof_fd) != 0) return -1;
        g_aof_write_offset = (long long)off;
        return 0;
    }

    sqe_w = io_uring_get_sqe(&g_persist_uring_sqpoll);
    sqe_f = io_uring_get_sqe(&g_persist_uring_sqpoll);
    if (!sqe_w || !sqe_f) return -1;

    io_uring_prep_write(sqe_w, g_aof_fd, buf, len, (off_t)g_aof_write_offset);
    io_uring_prep_fsync(sqe_f, g_aof_fd, IORING_FSYNC_DATASYNC);
    sqe_f->flags |= IOSQE_IO_LINK;  // write → fsync 链式执行

    // SQPOLL 内核线程自动取 SQE。io_uring_submit 写 SQ tail 通知/唤醒线程。
    io_uring_submit(&g_persist_uring_sqpoll);

    // 等 write CQE
    if (io_uring_wait_cqe(&g_persist_uring_sqpoll, &cqe) < 0 || !cqe) return -1;
    written = cqe->res;
    io_uring_cqe_seen(&g_persist_uring_sqpoll, cqe);
    if (written <= 0) return -1;

    // 等 fsync CQE
    if (io_uring_wait_cqe(&g_persist_uring_sqpoll, &cqe) < 0 || !cqe) return -1;
    if (cqe->res < 0) { io_uring_cqe_seen(&g_persist_uring_sqpoll, cqe); return -1; }
    io_uring_cqe_seen(&g_persist_uring_sqpoll, cqe);

    g_aof_write_offset += written;
    return 0;
}
```

关键设计决策：

1. **`io_uring_submit` + `io_uring_wait_cqe` 分开**：不等 `submit_and_wait`。SQPOLL 线程已有 SQE，`io_uring_submit` 仅写 SQ tail（唤醒 idle 线程），`io_uring_wait_cqe` 用 `IORING_ENTER_GETEVENTS` 等完成
2. **`IOSQE_IO_LINK`**：write 失败则 fsync 不执行，保证原子性
3. **降级路径**：SQPOLL 初始化失败 → 自动回退 pwrite+fdatasync（兼容旧内核）

### 修改函数

#### persist_append_raw()

ALWAYS 分支不再缓冲，直接调用 `persist_aof_per_command_flush`：

```c
int persist_append_raw(const unsigned char *buf, size_t len) {
    if (g_aof_fd < 0) return g_aof_disabled ? 0 : -1;

    if (len >= AOF_BUF_SIZE) {
        if (persist_aof_flush_buffer() != 0) return -1;
        if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
            if (persist_aof_per_command_flush(buf, len) != 0) return -1;
        } else {
            off_t off = (off_t)g_aof_write_offset;
            if (persist_write_and_fsync_uring(g_aof_fd, buf, len, &off) != 0) {
                if (persist_write_fd_sync(g_aof_fd, buf, len, &off) != 0) return -1;
            }
            g_aof_write_offset = (long long)off;
        }
    } else {
        if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
            if (g_aof_buf_len > 0) persist_aof_flush_buffer();
            if (persist_aof_per_command_flush(buf, len) != 0) return -1;
        } else {
            if (g_aof_buf_len + len > AOF_BUF_SIZE) {
                if (persist_aof_flush_buffer() != 0) return -1;
            }
            memcpy(g_aof_buf + g_aof_buf_len, buf, len);
            g_aof_buf_len += len;
        }
    }

    // group commit 时间追踪: 仅 EVERYSEC
    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) {
        if (g_aof_buffered_since_ms == 0) g_aof_buffered_since_ms = kvs_now_ms();
        g_aof_dirty = 1;
    }

    if (g_bgrewrite_pid > 0) append_to_rewrite_buffer(buf, len);

    if (g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) {
        if (kvs_now_ms() - g_aof_buffered_since_ms >= 2) {
            persist_aof_flush_buffer();
            g_aof_buffered_since_ms = 0;
        }
    }
    return 0;
}
```

#### persist_aof_flush_buffer()

ALWAYS 分支简化——正常情况下 per-command flush 不经过 buffer，但 `persist_flush_pending` 仍会调用此函数。保留 ALWAYS 分支（`g_aof_buf_len` 为 0 时快速返回），但可以考虑用 `assert(g_aof_buf_len > 0)` 或直接移除 ALWAYS 分支。

建议：保持现有逻辑不变，ALWAYS 路径仍使用 pwrite+fdatasync（用于 flush 其他模式切换时可能残留的 buffer 数据）。不需要改动。

#### persist_set_aof_policy()

运行时模式切换：

```c
int persist_set_aof_policy(int policy) {
    if (policy != KVS_AOF_FSYNC_ALWAYS && policy != KVS_AOF_FSYNC_EVERYSEC)
        return -1;

    if (policy == KVS_AOF_FSYNC_ALWAYS && g_cfg.aof_fsync != KVS_AOF_FSYNC_ALWAYS) {
        persist_aof_flush_buffer();
        g_aof_buf_len = 0;
    }

    if (policy == KVS_AOF_FSYNC_EVERYSEC && g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS) {
        if (g_persist_uring_sqpoll_ready) {
            io_uring_queue_exit(&g_persist_uring_sqpoll);
            g_persist_uring_sqpoll_ready = 0;
        }
    }

    g_cfg.aof_fsync = policy;
    return 0;
}
```

#### persist_close()

```c
void persist_close(void) {
    if (g_aof_buf_len > 0) persist_aof_flush_buffer();
    if (g_aof_fd >= 0) {
        persist_fsync_fd_best_effort(g_aof_fd);
        close(g_aof_fd);
        g_aof_fd = -1;
    }
    persist_uring_close();
    if (g_persist_uring_sqpoll_ready) {
        io_uring_queue_exit(&g_persist_uring_sqpoll);
        g_persist_uring_sqpoll_ready = 0;
    }
}
```

### 新增全局变量

```c
static int g_persist_uring_sqpoll_ready = 0;
static struct io_uring g_persist_uring_sqpoll;
```

### 不再需要的逻辑

| 移除项 | 位置 | 原因 |
|--------|------|------|
| ALWAYS 下 `g_aof_dirty = 1` | `persist_append_raw` | per-command 已落盘，无需 dirty 标记 |
| ALWAYS 下 2ms timeout flush | `persist_append_raw` / `persist_autosnap_cron` | 不再有缓冲数据 |
| ALWAYS 下 `g_aof_buffered_since_ms` | `persist_append_raw` | 不再有缓冲数据 |

### 保持不变的 ALWAYS 引用（无害 no-op）

| 位置 | 代码 | 行为 |
|------|------|------|
| `persist_aof_flush_buffer:207` | `if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS)` | 保留 pwrite+fdatasync 路径，用于模式切换时 flush 残留 buffer；正常 ALWAYS 下 `g_aof_buf_len==0` 不会进入 |
| `persist_flush_pending:238` | `if (...ALWAYS && g_aof_buf_len > 0)` | `g_aof_buf_len` 在 ALWAYS 下始终为 0，快速返回 |
| `persist_autosnap_cron:915` | `if (...ALWAYS && g_aof_buf_len > 0)` | 同上，无害 no-op，不需要改动 |

### 不修改的文件

- `src/core/reactor.c`: `persist_flush_pending()` 在 ALWAYS 下仍被调用但无害（`g_aof_buf_len == 0` → 快速返回），不需要改动
- `src/core/proactor.c`: 网络 I/O 后端，与 persistence 无关
- `src/main/kvstore.c`: `handle_parsed_command` 调用 `persist_append_raw` 的接口不变

## 错误处理

| 错误场景 | 处理 |
|----------|------|
| SQPOLL ring 初始化失败（老内核、权限不足） | 降级到 pwrite+fdatasync |
| `io_uring_get_sqe` 返回 NULL（ring full） | 返回 -1，上层调用者处理 |
| `io_uring_wait_cqe` 失败 | 返回 -1 |
| write CQE 返回 <= 0 | 返回 -1 |
| fsync CQE 返回 < 0 | 返回 -1 |
| ALWAYS → EVERYSEC 切换 | 销毁 SQPOLL ring，释放内核线程 |
| EVERYSEC → ALWAYS 切换 | flush 残留 buffer，清空 `g_aof_buf_len` |

## 测试验证

1. **功能测试**：`tests/test_persist_aof_demo.c` — 验证 ALWAYS 模式下写入的数据在 crash 后可恢复
2. **性能测试**：`tools/bench/run_persist_bench.sh` — 对比 ALWAYS SQPOLL vs ALWAYS 旧实现 vs Redis always
3. **降级测试**：在无 SQPOLL 支持的内核上验证自动降级到 pwrite+fdatasync
4. **模式切换测试**：运行时 `CONFIG SET appendfsync always|everysec` 多次切换，验证无泄漏、无数据丢失
5. **并发测试**：`redis-benchmark -c 50 -P 1` 50 并发连接，验证 per-command 语义正确

## 风险

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| SQPOLL 内核线程空转耗 CPU | 中 | idle 时 CPU 100% | `sq_thread_idle=2000`（kernel 5.11+）；旧内核需手动杀 SQPOLL ring |
| io_uring SQPOLL per-command 延迟 > pwrite+fdatasync | 中 | 性能倒退 | 降级路径保留 pwrite+fdatasync；benchmark 对比后可选不启用 SQPOLL |
| `IOSQE_IO_LINK` 写失败导致 fsync 跳过 | 低 | 数据未落盘 | 写失败时函数返回 -1，上层不应回复客户端 |
| 旧内核不支持 `sq_thread_idle` | 高 | SQPOLL 线程永不休眠 | 可配置 `--aof-sqpoll-idle-ms` 或接受 CPU 开销 |

## 文件变更清单

| 文件 | 变更 |
|------|------|
| `src/persistence/kvs_persist.c` | 主要变更：新增 SQPOLL ring 管理 + `persist_aof_per_command_flush` + 改 `persist_append_raw`、`persist_set_aof_policy`、`persist_close` |
| `include/kvstore/kvstore.h` | 可能需要新增函数声明（如果 `persist_aof_per_command_flush` 需要对外暴露） |

预计新增 ~80 行，修改 ~40 行。
