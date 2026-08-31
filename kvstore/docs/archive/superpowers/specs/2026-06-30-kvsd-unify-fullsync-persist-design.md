# KVSD 统一全量同步与全量持久化 — 设计文档

日期: 2026-06-30

## 目标

全量同步（REPLSYNC）和全量持久化（SAVE/BGSAVE）使用相同的 KVSD 二进制格式。两者唯一差异：全量同步通过网络传输，全量持久化保存到本地磁盘。其余数据格式、命令处理、TTL 处理完全一致。

## 当前状态

| | 全量同步 | 全量持久化 |
|---|---|---|
| 格式 | RESP 文本 (`*3\r\n$4\r\nHSET\r\n...`) | KVSD 二进制 (`[1B engine][4B klen][key][4B vlen][value]`) |
| 生成函数 | `snapshot_all_sink()` → `kvs_snapshot_to_fp()` | `kvs_dump_to_fd()` |
| Slave/Recovery 加载 | `parse_resp_stream()` 逐条解析执行命令 | `replay_dump_file()` mmap 后直接调 engine API |
| TTL | 有（独立 EXPIRE/HEXPIRE 等 RESP 命令） | 无（恢复后 TTL 丢失） |
| 传输 | 网络（TCP/RDMA） | 本地磁盘 |

## 方案

直接替换生成器：全量同步不再调用 `kvs_snapshot_to_fp()`（RESP），改为调用 `kvs_dump_to_fd()`（KVSD）。Slave 端收到 KVSD 字节流写入临时文件，完成后调用 `replay_dump_file()` 加载。

## 整体数据流

```
全量持久化 (SAVE/BGSAVE):
  kvs_dump_to_fd(fd, offset) ──► 本地磁盘 (dump 文件)
  恢复: replay_dump_file(fd) ──► 内存 + TTL

全量同步 (REPLSYNC):
  Master: kvs_dump_to_fd(fd, snap_base_offset) ──► 临时文件
          └─ read ──► repl_send_chunked() ──► 网络 (TCP/RDMA)

  Slave:  网络字节流 ──► write() ──► 临时文件
          └─ replay_dump_file(fd) ──► 内存 + TTL
          └─ unlink 临时文件
```

- `kvs_dump_to_fd()` 是唯一的 KVSD 生成器
- `replay_dump_file()` 是唯一的 KVSD 加载器
- Master 生成和 Slave 加载走对称路径

## KVSD 格式扩展

### 新二进制布局

```
Header (8 bytes):
  [8B] uint64_t aof_offset        — 不变

Records (每条记录):
  [1B] uint8_t  engine_id         — 不变
  [1B] uint8_t  flags             — 新增
  [4B] uint32_t klen              — 不变
  [klen]        key               — 不变
  [4B] uint32_t vlen              — 不变
  [vlen]        value             — 不变
  [8B] uint64_t expire_ms         — 仅当 flags & 0x01
```

### flags 定义

```c
#define KVSD_FLAG_HAS_EXPIRE  0x01   // 该记录有 TTL，后跟 8 字节 expire_ms（绝对时间）
// bits 1-7 保留，写入时置 0，读取时忽略
```

### 写入逻辑 (kvs_dump_to_fd)

用现有 API 组合：`kvs_expire_ttl()` 返回剩余秒数（向上取整），`kvs_now_ms()` 取当前毫秒。存绝对毫秒时间戳——dump 文件可能在磁盘上存放很久后才恢复，不能存相对时间。

```c
long long ttl_sec = kvs_expire_ttl(&global_expire, _eng, key);
if (ttl_sec >= 0) {
    _flags = KVSD_FLAG_HAS_EXPIRE;
    _exp = (uint64_t)(kvs_now_ms() + ttl_sec * 1000);
}
```

秒级精度对 TTL 足够（RESP 协议本身也是秒级），不引入新 API。

### 加载逻辑 (replay_dump_file)

每条记录读取 engine_id、flags、klen、key、vlen、value 后：
- 调用对应 engine 的 set 函数
- 若 `flags & KVSD_FLAG_HAS_EXPIRE`，读取 8 字节 `expire_at_ms`，计算剩余 TTL：
  ```c
  long long ttl_ms = expire_at_ms - kvs_now_ms();
  if (ttl_ms > 0) kvs_expire_set(NULL, engine, key, ttl_ms);
  // ttl_ms <= 0 表示已过期，跳过
  ```

### 向后兼容

旧格式 KVSD 文件没有 flags 字节。不追求兼容——旧文件在开发环境中直接丢弃重新生成。`replay_dump_file()` 遇到非法 engine_id（旧格式的第一个 byte 被当作 engine_id 读取，值会超过 5）时报错退出。

## Master 端改动

### queue_snapshot() — kvstore.c:545-675

当前：打开临时文件 → `kvs_snapshot_to_fp(fp)` (RESP) → 关闭 → 打开读大小 → 关闭 → 打开读回发送。

改为：打开临时文件 fd → `kvs_dump_to_fd(fd, snap_base_offset)` (KVSD) → `lseek` 获取大小 → `lseek` 回到开头 → `read` 发送。

具体变更：
1. `fopen(tmp_path, "wb")` → `open(tmp_path, O_RDWR|O_CREAT|O_TRUNC, 0666)`
2. `kvs_snapshot_to_fp(fp)` → `kvs_dump_to_fd(fd, snap_base_offset)`
3. 删掉关闭后再打开读大小的逻辑，改为 `total_bytes = lseek(fd, 0, SEEK_END)` 然后 `lseek(fd, 0, SEEK_SET)`
4. `fread` → `read`，`fclose` → `close`
5. `snap_base_offset` 作为 aof_offset 参数传入 `kvs_dump_to_fd()`

不变：
- `+FULLRESYNC` header 格式
- `repl_send_chunked()` 调用
- RDMA 启动/停止
- `g_repl_fullsync_in_progress` + client capture 缓存
- `REPLDONE` 发送
- backlog gap 回放

### kvs_dump_to_fd() — kvstore.c:2146-2225

`DUMP_WRITE_KV` 宏扩展为包含 flags 和 TTL：

```c
#define DUMP_WRITE_KV_EX(engine_id, key, value) do {                \
    uint8_t  _eng = (uint8_t)(engine_id);                            \
    uint8_t  _flags = 0;                                             \
    uint32_t _klen = (uint32_t)strlen(key);                          \
    uint32_t _vlen = (uint32_t)strlen(value);                        \
    long long _ttl_sec = kvs_expire_ttl(&global_expire, _eng, key);  \
    uint64_t _exp = 0;                                               \
    if (_ttl_sec >= 0) { _flags = KVSD_FLAG_HAS_EXPIRE;              \
        _exp = (uint64_t)(kvs_now_ms() + _ttl_sec * 1000); }         \
    write(fd, &_eng, 1); write(fd, &_flags, 1);                    \
    write(fd, &_klen, 4); write(fd, key, _klen);                    \
    write(fd, &_vlen, 4); write(fd, value, _vlen);                  \
    if (_flags & KVSD_FLAG_HAS_EXPIRE)                               \
        write(fd, &_exp, 8);                                         \
} while(0)
```

## Slave 端改动

### 接收流程 — kvs_repl.c

当前：网络字节流 → `parse_resp_stream()` → `handle_parsed_command()` → engine set → 累计 parsed 数量 → 达到 total 后 `repl_slave_finish_fullsync()` → `kvs_dump_to_fd()` 落盘。

改为：网络字节流 → `write()` 到临时文件 → 累计接收字节数 → 达到 total_bytes 后 fsync → `repl_slave_finish_fullsync(tmp_fd)` → `replay_dump_file(tmp_fd)` → close + unlink。

临时文件路径：`snprintf(tmp, sizeof(tmp), "%s.fullsync.recv.tmp.%ld", g_cfg.dump_path, (long)getpid())`

### repl_slave_finish_fullsync() — kvs_repl.c:1902-1946

去掉内部的 `kvs_dump_to_fd()` 调用（内存→磁盘）。改为接收已落盘的临时文件 fd，直接调 `replay_dump_file(tmp_fd)`。

## replay_dump_file() 改动 — kvs_persist.c:319-423

1. 去 `static`，在 `include/kvstore/kvstore.h` 中声明
2. 每条记录读取：engine_id → flags(新) → klen → key → vlen → value → 条件读 expire_ms
3. 引擎分发逻辑不变：switch(engine_id) → engine_set()
4. 新增：若 flags & KVSD_FLAG_HAS_EXPIRE，读 8B expire_ms 并调 `kvs_expire_set()`
5. 增加 sanity check：engine_id > 5 时报错（旧格式文件或数据损坏）

## 废弃代码

以下函数在 KVSD 统一后不再被调用，加 `/* deprecated: unused after KVSD unification */` 注释标记，不删除：

- `snapshot_all_sink()` 及全部 `snapshot_*_sink()` 函数
- `kvs_snapshot_to_fp()`、`kvs_snapshot_to_fd()`
- `maybe_emit_expire_sink()`

## 文件改动汇总

| 文件 | 改动 |
|---|---|
| `src/main/kvstore.c` | `kvs_dump_to_fd()` — DUMP_WRITE_KV 扩展为含 flags+TTL；`queue_snapshot()` — 生成调用替换，FILE* → fd；废弃函数加注释 |
| `src/persistence/kvs_persist.c` | `replay_dump_file()` — 去 static，增加 flags+TTL 解析，增加 sanity check |
| `include/kvstore/kvstore.h` | 声明 `replay_dump_file()`；定义 `KVSD_FLAG_HAS_EXPIRE` |
| `src/replication/kvs_repl.c` | Slave 接收改为写临时文件，`repl_slave_finish_fullsync()` 改为调 `replay_dump_file()` |

## 错误处理

### Master 端

错误处理框架不变（`goto out` + cleanup）。新增场景：`kvs_dump_to_fd()` 失败 → unlink tmp → goto out → 返回 -1。等价于当前 `kvs_snapshot_to_fp()` 失败的处理。

### Slave 端

| 场景 | 处理 |
|---|---|
| 临时文件写失败（磁盘满） | unlink + 标记同步失败，slave 稍后重试 |
| 网络断开（收到字节 < total_bytes） | unlink + 重连后重新发起 REPLSYNC |
| `replay_dump_file()` 失败（数据损坏） | unlink + 报错 + 重试全量同步 |

### 全量同步期间的并发写入

不受影响。Master 端 `g_repl_fullsync_in_progress=1` + client capture 缓存 + backlog gap 回放机制保持不变。

## 验证方案

### 单元级 — KVSD 往返测试

| 场景 | 验证点 |
|---|---|
| 无 TTL KV | dump → load 后 key/value 一致 |
| 有 TTL KV | dump → load 后 TTL 存在且值正确 |
| 混合 TTL | 部分有 TTL、部分无，同在 dump 文件中 |
| 空库 | 只有 8B header，load 后内存为空 |
| 5 引擎全覆盖 | Array/Hash/Rbtree/Skiptable/Doc 的 KV 和 TTL |
| 旧格式文件 | 无 flags 字节，replay_dump_file() 应报错 |

### 集成 — 全量持久化不受影响

```
SET key1 val1
EXPIRE key1 100
SAVE
# kill & restart
GET key1  → "val1"
TTL key1  → > 0 (不再丢失 TTL)
```

### 集成 — 全量同步端到端

```
Master 写入一批数据（含 TTL）
Slave 连接 → 触发全量同步
Slave 完成同步后对比 key/value 和 TTL 一致性
```

### 回归

- 增量复制不受影响（部分 resync 不涉及 KVSD）
- RDMA 全量同步不受影响（只换数据格式）
- AOF 记录和回放不受影响
- BGSAVE fork 子进程不受影响

### 跨机测试

- Master: 192.168.233.128, Slave: 192.168.233.129
- SSH 密码: 2983372202
- 测试程序: `tests/test_repl_5w5w`（预存 5w → 全量同步 → 再存 5w 增量 → 数据一致性验证）
