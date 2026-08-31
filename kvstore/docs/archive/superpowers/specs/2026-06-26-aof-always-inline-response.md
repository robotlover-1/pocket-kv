# AOF Always Inline Response — 移除 Deferred Response 机制

日期：2026-06-26

## 问题

kvstore AOF always 模式在多连接 pipeline 场景下吞吐远低于 Redis：

| 场景 | kvstore | Redis | 差距 |
|------|---------|-------|------|
| `-c 50 -P 1` | 49K | 46K | +7% |
| `-c 50 -P 10` | 12K | 251K | 5% |
| `-c 50 -P 160` | 180K | 611K | 29% |

根因见 [2026-06-26-aof-always-debug.md](../issues/2026-06-26-aof-always-debug.md#L154)：
deferred response 机制把响应延迟到 fsync 之后统一发送，导致多客户端被串行化——
先收到响应的客户端发了新数据，后收到的还没发，下一轮 epoll_wait 只处理到少数客户端，
effective batch size 从 ~23 cmd/fsync 跌到 ~6。

而 `-c 1` 单连接场景不受此影响（无其他连接竞争），AOF always 已达 1M qps。

## 方案

**移除 deferred response 机制**，AOF always 的响应在命令执行后立即通过 `queue_bytes` 发送（和 disable/everysec 一致），fsync 批量在 reactor 循环末尾单独执行。

### 语义变更

| | 改前 | 改后 |
|---|------|------|
| 响应时序 | fsync 之后 | 命令执行后（fsync 之前） |
| 持久性保证 | +OK 即已落盘 | +OK 即已写入 OS buffer，fsync 稍后完成 |
| 对齐 | — | Redis AOF always 行为 |

崩溃场景：响应发出后、fsync 完成前崩溃，客户端已收到 +OK 但数据可能丢失。与 Redis 5.0.7 行为一致。

## 数据流对比

### 改前

```
handle_parsed_command():
  cmd → engine_set → persist_append_raw()
    → memcpy 到 g_aof_buf
    → persist_aof_has_pending()=1
    → persist_defer_response(c, "+OK\r\n")   ← 挂链表，不发

reactor loop:
  persist_flush_deferred()
    ├─ persist_aof_flush_buffer() → io_uring write+fsync
    └─ 遍历 deferred 链表 → queue_bytes → send() ← 这时才发响应
```

### 改后

```
handle_parsed_command():
  cmd → engine_set → persist_append_raw()
    → memcpy 到 g_aof_buf
    → queue_bytes(c, "+OK\r\n")              ← 立即发送

reactor loop:
  persist_flush_pending()
    └─ persist_aof_flush_buffer() → io_uring write+fsync  ← 只刷盘
```

## 代码改动

### kvstore.c

删除 [L1777-1780](src/main/kvstore.c#L1777) 的 deferred response 分支（4 行）。响应自然落到函数末尾的 `queue_bytes` 路径，和 disable/everysec 一致。

### kvs_persist.c

**删除（净 -57 行）：**

- `deferred_resp_t` 结构体 + `g_deferred_head`, `g_deferred_tail`（L41-52）
- `persist_defer_response()`（L240-253）
- `persist_flush_deferred()`（L256-289）
- `persist_aof_has_pending()`（L293-295）
- `persist_cancel_deferred()`（L298-317）

**新增：`persist_flush_pending()`**

```c
void persist_flush_pending(void) {
    if (g_cfg.aof_fsync == KVS_AOF_FSYNC_ALWAYS && g_aof_buf_len > 0) {
        persist_aof_flush_buffer();
    }
}
```

**修改：`persist_autosnap_cron()` always 分支**

[L991-996](src/persistence/kvs_persist.c#L991)：`persist_flush_deferred()` → `persist_aof_flush_buffer()`。只刷盘，不处理响应。

**保留不变：**

- `persist_append_raw()` 中的 buffer + 2ms 安全网 + 大命令直接写路径
- `persist_autosnap_cron()` 中的 everysec 定时 flush
- `persist_write_and_fsync_uring()` 的 io_uring batch write+fsync

### reactor.c

- L77：删除 `persist_cancel_deferred(c)` 调用
- L291：`persist_flush_deferred()` → `persist_flush_pending()`

### kvstore.h

- 删除 4 个函数声明：`persist_defer_response`, `persist_flush_deferred`, `persist_aof_has_pending`, `persist_cancel_deferred`
- 新增 1 个：`void persist_flush_pending(void)`

### 改动量汇总

| 文件 | 增 | 删 | 净 |
|------|----|----|-----|
| kvstore.c | 0 | 4 | -4 |
| kvs_persist.c | +8 | ~65 | -57 |
| reactor.c | 0 | 2 | -2 |
| kvstore.h | +1 | 4 | -3 |
| **合计** | **+9** | **~75** | **-66** |

## 边界与兼容性

- **AOF disable / everysec**：`persist_flush_pending()` 只对 ALWAYS 生效，不受影响
- **AOF 写入失败**：已有 `persist_append_raw()` 返回值检查 + 错误响应，不改动
- **超大命令（≥64KB）**：已有直接 write+fsync 路径保留
- **连接关闭**：不再需要清理 deferred 链表
- **主从复制 Slave**：走独立分支，不经过 deferred response
- **BGREWRITEAOF**：独立链表 + 锁，不相关
- **2ms 安全网**：`persist_append_raw()` 和 `persist_autosnap_cron` 中的时间阈值保留，防止 buffer 长期滞留

## 预期效果

| 场景 | 改前 | 预期 | 说明 |
|------|------|------|------|
| `-c 50 -P 1` | 49K | 70-80K | 消除 fsync 在响应路径中的串行延迟 |
| `-c 50 -P 10` | 12K | >200K | 解决串行化，恢复完整 batch |
| `-c 50 -P 160` | 180K | 接近 Redis 600K | 取决于 500 条命令处理+1 次 fsync 的纯开销 |
| `-c 1 -P 160` | ~1M | 不退化 | 单连接本就不走 deferred 路径 |

## 测试计划

### 正确性

```bash
# kill -9 恢复验证：写入 N 条，kill，恢复，抽样验证
redis-benchmark -p 5190 -n 10000 -c 1 -P 1 HSET key:__rand_int__ value
kill -9 $(pgrep kvstore)
./kvstore --port 5190 --appendfsync always &
# 验证恢复后数据完整
```

验收：恢复后数据完整。至多容忍最后一批 fsync 边界的少量丢失（符合 Redis 语义）。

### 性能

```bash
bash tools/bench/run_persist_bench.sh    # P=1 对比
bash tools/bench/run_pipeline_bench.sh   # P=10/20/40/80/160 对比
```

验收：P=1 不低于 49K，P=10 显著提升。

### 稳定性

```bash
redis-benchmark -p 5190 -n 10000000 -c 50 -P 10 HSET key:__rand_int__ value
```

验收：无 crash，无内存泄漏（valgrind 确认 deferred 相关分配完全清除）。

### 回归

```bash
redis-benchmark -p 5190 -n 100000 -c 1 -P 160 HSET key:__rand_int__ value
```

验收：单连接 P=160 保持 ~1M qps。
