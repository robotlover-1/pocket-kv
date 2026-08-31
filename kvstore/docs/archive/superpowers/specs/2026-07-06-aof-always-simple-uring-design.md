# AOF ALWAYS 逐命令 io_uring 刷盘（去掉 EVERYSEC）

## 目标

1. `appendfsync always` 做到严格 per-command 落盘：每条写命令执行后，write+fsync 完成才回复客户端
2. 去掉 EVERYSEC 策略，AOF 策略简化为：关闭 / ALWAYS 两项
3. 去掉 AOF 缓冲区（`g_aof_buf`）及相关的 2ms 组提交逻辑
4. 刷盘使用已有 io_uring ring（复用，不新开 SQPOLL）

## 背景

### 当前实现

- ALWAYS 使用 2ms group commit：命令缓冲到 64KB `g_aof_buf`，在 reactor 循环结束或 2ms 超时时批量 `pwrite+fdatasync`
- EVERYSEC 使用 io_uring 批量 write+fsync，1s 定时触发
- 不是严格 per-command

### 为什么要改

用户要求严格 per-command 语义，不做任何缓冲/批处理。EVERYSEC 不再需要。

### 为什么复用已有 ring 不用 SQPOLL

逐命令 fsync 的瓶颈是磁盘 fdatasync 本身（~0.3ms），不是提交 syscall 的开销。SQPOLL 额外占一个 CPU 核持续轮询，对延迟几乎无帮助，且需要处理 policy 运行时切换时的 ring init/teardown。复用已有 ring 实现更简单。

## 方案

只改少数文件，核心思路：**ALWAYS 模式下 `persist_append_raw` 直接对当前命令调用 io_uring write+fsync，阻塞等完成后再返回。**

### 改动清单

#### 1. `src/persistence/kvs_persist.c` — 核心改动

**删除：**

- `g_aof_buf`、`g_aof_buf_len`、`g_aof_dirty`、`g_aof_buffered_since_ms`、`g_aof_last_flush_ms` 等缓冲区状态变量
- `persist_aof_flush_buffer()` — 不再需要批量 flush
- `persist_flush_pending()` — 不再需要
- `persist_write_and_fsync_uring()` — 原来的批量 write+fsync 函数，逐命令场景不需要

**新增/修改：**

- `persist_append_raw()` — 简化为两条路径：
  - AOF 关闭：直接 return
  - ALWAYS：直接构造单次 io_uring write SQE + fsync SQE → `io_uring_submit_and_wait()` → 检查 cqe 结果 → 返回
- `persist_autosnap_cron()` — 去掉 ALWAYS 2ms / EVERYSEC 1s 超时检查，只保留 snapshot 逻辑
- `persist_set_aof_policy()` / `persist_get_aof_policy()` — 配合枚举简化
- 初始化/关闭逻辑 — 去掉 buffer 相关初始化

#### 2. `include/kvstore/kvstore.h` — 枚举简化

```c
typedef enum {
    KVS_AOF_FSYNC_OFF    = 0,
    KVS_AOF_FSYNC_ALWAYS = 1,
} kvs_aof_fsync_policy_t;
```

#### 3. `src/main/kvstore.c` — CLI/配置/命令解析

- `parse_appendfsync_policy()` — 去掉 `everysec`，只解析 `off`/`always`
- `--appendfsync` CLI 参数和 `appendfsync=` 配置解析对应更新
- `APPENDFSYNC` / `CONFIG APPENDFSYNC` 运行时命令更新
- 默认值保持 `KVS_AOF_FSYNC_ALWAYS`

#### 4. `src/core/reactor.c` — 去掉 flush 调用

- 去掉 `persist_flush_pending()` 调用（epoll_wait 之后那句）
- `persist_autosnap_cron()` 中不再负责 AOF flush

#### 5. `kvstore.conf` — 配置示例更新

```ini
appendfsync=always
```

### 数据流

```text
Client SET command
  → reactor reads command
  → execute command
  → persist_append_raw(resp_data, len)
      → AOF off? return
      → io_uring: prep write SQE (fd, data, len, offset)
      → io_uring: prep fsync SQE (fd)
      → io_uring_submit_and_wait(ring, 2)  // wait for both completions
      → update aof_write_offset
  → reply to client
```

### 错误处理

- io_uring submit 失败：记录日志，返回错误给客户端
- write 或 fsync cqe error：记录日志，返回错误给客户端
- 不静默失败

### 测试要点

- ALWAYS 模式下写命令，验证 write+fsync 确实发生（可通过 `strace` 或 AOF 文件内容验证）
- AOF 关闭模式下写命令，验证不写 AOF 文件，不影响主流程
- 并发写入场景，验证 AOF 文件完整性（无交错写入）

## 影响范围

| 文件 | 改动类型 |
| ---- | -------- |
| `src/persistence/kvs_persist.c` | 大量删减，核心逻辑重写 |
| `include/kvstore/kvstore.h` | 枚举 + config 结构体 |
| `src/main/kvstore.c` | CLI/配置解析简化 |
| `src/core/reactor.c` | 删掉 2 处调用 |
| `kvstore.conf` | 配置示例 |

不改 `tests/` 下的测试文件，待实现完成后由用户手动运行验证。
