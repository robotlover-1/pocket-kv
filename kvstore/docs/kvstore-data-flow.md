# KVStore 数据流文档

## 目录

1. [架构总览](#1-架构总览)
2. [客户端命令到主机的数据流](#2-客户端命令到主机的数据流)
3. [全量持久化（SAVE/BGSAVE）](#3-全量持久化savebgsave)
4. [增量持久化（AOF）](#4-增量持久化aof)
5. [全量同步（FULLRESYNC）](#5-全量同步fullresync)
6. [增量同步（eBPF+TCP / kprobe+RDMA）](#6-增量同步ebpf--tcp--kprobe--rdma)
7. [从机保存数据到磁盘](#7-从机保存数据到磁盘)
8. [关键数据结构](#8-关键数据结构)
9. [配置项速查](#9-配置项速查)

---

## 1. 架构总览

```
┌──────────┐                           ┌──────────┐
│  Client  │                           │  Client  │
│(redis-cli)│                          │(redis-cli)│
└────┬─────┘                           └────┬─────┘
     │ RESP                                  │ RESP
     ▼                                       ▼
┌─────────────────────────────────────────────────────────┐
│                       MASTER                           │
│  ┌──────────────────────────────────────────────────┐  │
│  │ 网络层: reactor(epoll) / proactor(io_uring) /    │  │
│  │         ntyco(coroutine)                         │  │
│  └──────────────────┬───────────────────────────────┘  │
│                     │                                  │
│  ┌──────────────────▼───────────────────────────────┐  │
│  │ RESP 协议解析 (parse_resp_stream)                 │  │
│  └──────────────────┬───────────────────────────────┘  │
│                     │                                  │
│  ┌──────────────────▼───────────────────────────────┐  │
│  │ 命令分发 (handle_parsed_command)                  │  │
│  └──┬────────┬─────────┬──────────┬────────────────┘  │
│     │        │         │          │                    │
│     ▼        ▼         ▼          ▼                    │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────────┐             │
│  │Storage│ │Expire│ │AOF   │ │Repl      │             │
│  │5引擎 │ │TTL   │ │持久化│ │Broadcast │             │
│  └──────┘ └──────┘ └──────┘ └────┬─────┘             │
│                                  │                    │
│  Transport: TCP / RDMA / eBPF / kprobe+RDMA           │
└──────────────────────────────────┼────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
              ┌──────────┐  ┌──────────┐  ┌──────────┐
              │  SLAVE-1 │  │  SLAVE-2 │  │  SLAVE-N │
              │ AOF+Dump │  │ AOF+Dump │  │ AOF+Dump │
              └──────────┘  └──────────┘  └──────────┘
```

**核心源文件：**


| 文件                                            | 职责                                                         |
| ----------------------------------------------- | ------------------------------------------------------------ |
| `src/main/kvstore.c`                            | 入口、RESP 解析、命令分发、SNAPSHOT/DUMP 生成                |
| `src/persistence/kvs_persist.c`                 | AOF 写入、SAVE/BGSAVE、BGREWRITEAOF、恢复                    |
| `src/replication/kvs_repl.c`                    | 全量同步、增量广播、backlog、transport 抽象层、RDMA 全量同步 |
| `src/replication/kvs_repl_ebpf.c`               | eBPF sockmap 加载、fd 注册、统计                             |
| `src/replication/kvs_repl_kprobe.c`             | kprobe+RDMA WRITE 增量同步、client_capture                   |
| `src/replication/bpf/repl_sockmap.bpf.c`        | BPF sk_msg 程序（sockmap 重定向）                            |
| `src/replication/bpf/repl_kprobe.bpf.c`         | BPF kprobe 程序（tcp_sendmsg 拦截）                          |
| `src/replication/bpf/repl_client_capture.bpf.c` | BPF kprobe/kretprobe（tcp_recvmsg 拦截，全量同步期间缓存）   |

---

## 2. 客户端命令到主机的数据流

### 2.1 总流程

```
Client 发送 RESP 命令 (如 *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n)
        │
        ▼
┌─── 网络层接收 ──────────────────────────────────────────────────┐
│  reactor: epoll_wait → EPOLLIN → recv → conn->inbuf            │
│  proactor: io_uring CQE → recv SQE → conn->inbuf               │
│  ntyco:    coroutine recv → conn->inbuf                         │
└──────────────────┬──────────────────────────────────────────────┘
                   │
                   ▼
┌─── RESP 协议解析 ───────────────────────────────────────────────┐
│  parse_resp_stream(c, buf, &len, from_replication=0)           │
│  解析 RESP 格式: *<n>\r\n$<len>\r\n<data>\r\n...              │
│  提取出 cmd, argv[], argl[], raw, rawlen                        │
└──────────────────┬──────────────────────────────────────────────┘
                   │
                   ▼
┌─── 命令分发 ────────────────────────────────────────────────────┐
│  handle_parsed_command(c, argc, argv, argl, raw, rawlen, 0)    │
│                                                                 │
│  kvs_ascii_upper(argv[0]) → "SET" / "HSET" / "RSET" 等       │
│                                                                 │
│  cmd_engine(cmd) → KVS_ENGINE_ARRAY / HASH / RBTREE / SKIPTABLE│
│  strip_prefix(cmd) → "SET" / "GET" / "DEL" 等                 │
└──────────────────┬──────────────────────────────────────────────┘
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
  ┌─────────┐ ┌─────────┐ ┌──────────────┐
  │ 读命令  │ │ 写命令  │ │ 管理命令      │
  │GET/HGET │ │SET/HSET │ │SAVE/INFO/... │
  └────┬────┘ └────┬────┘ └──────┬───────┘
       │           │             │
       ▼           ▼             ▼
  直接返回    完整写路径      直接执行
  engine_get  (见 2.2)       返回结果
  → queue_bytes
```

### 2.2 写命令处理路径（以 SET 为例）

```
handle_parsed_command(c, argc=3, argv=["SET","key","value"], from_replication=0)
    │
    ├─ 1. try_expire(engine, key)              清理可能过期的旧 key
    │
    ├─ 2. engine_set(engine, key, value)       写入存储引擎
    │      ├─ kvs_hash_set()    → 渐进式 rehash 的哈希表
    │      ├─ kvs_rbtree_set()  → 红黑树
    │      ├─ kvs_array_set()   → 定长数组
    │      ├─ kvs_skiptable_set() → 跳表
    │      └─ kvs_doc_set()     → 文档存储
    │
    ├─ 3. persist_note_write()                  g_dirty_counter++
    │      g_dirty_counter: 自上次 SAVE/BGSAVE 以来的写命令次数。
    │      用于 autosnap 自动快照判断（如 dirty>=10000 && elapsed>=3600s → BGSAVE）。
    │
    ├─ 4. persist_append_raw(raw, rawlen)       写入 AOF（见第 4 节）
    │
    ├─ 5. repl_broadcast(raw, rawlen)           广播到所有从机（见第 5、6 节）
    │      ├─ repl_backlog_feed()               写入 1MB 复制 backlog 环形缓冲
    │      ├─ repl_note_broadcast()             g_master_repl_offset += rawlen
    │      │      全局复制偏移量，单调递增的字节计数。slave 用它汇报同步进度。
    │      └─ 遍历 g_replicas 链表，只发给"已全量同步完、处于增量稳态"的 slave:
    │           ├─ 跳过 repl_draining:    该连接正在断开，发了也收不到
    │           ├─ 跳过 repl_fullsync_pending: 还没拿到全量基准数据
    │           ├─ 跳过 g_repl_fullsync_in_progress: 全局全量同步中，增量数据由
    │           │      client_capture BPF 缓存 (L1/L2)，全量完成后 flush
    │           ├─ 跳过 c->fwd_healthy=1: 已被 kprobe ringbuf 回调直接 send() 服务
    │           └─ 对每条有效连接: repl_realtime_send(c, raw, rawlen)
    │                └─ transport ops → send()
    │                     ├─ TCP / ebpf+tcp: queue_bytes → reactor on_write → send()
    │                     │      (fwd_healthy=1 时跳过，由 kprobe ringbuf 直发)
    │                     ├─ eBPF sockmap: queue_bytes → send() → BPF sk_msg 内核拦截重定向
    │                     └─ kprobe-rdma: 返回 -1，由内核态 kprobe 拦截 send() 后经
    │                               ringbuf→RDMA WRITE 转发; TCP 路径同时运行作保底
    │
    └─ 6. queue_bytes(c, response)              回复客户端 "+OK\r\n"
```

### 2.3 RESP 协议解析流程

```
parse_resp_stream(c, buf, &len, from_replication)
    │
    ├─ 逐字节扫描 buf[0..len]
    │
    ├─ 遇到 '+' → 简单字符串 (如 +FULLRESYNC ...)
    │     用于复制协议的控制消息
    │
    ├─ 遇到 '*' → 数组（RESP 命令）
    │     解析数组长度 n
    │     然后解析 n 个 bulk string ($<len>\r\n<data>\r\n)
    │     构建 argv[], argl[]
    │     调用 handle_parsed_command(c, argc, argv, argl, raw, rawlen, from_replication)
    │
    └─ from_replication 参数:
         ├─ 0: 来自客户端 → 执行持久化 + 广播
         └─ 1: 来自复制 → 只执行引擎操作，不广播，但写 AOF + 跟踪 offset
```

---

## 3. 全量持久化（SAVE/BGSAVE）

### 3.1 数据流概览

```
┌──────────────────────────────────────────────────────────┐
│                    SAVE / BGSAVE                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  SAVE (同步)                                              │
│    │                                                     │
│    ├─ kvs_dump_to_fd(fd, aof_offset)                     │
│    │    ├─ 写入 8 字节 aof_offset（AOF 跳过基准）         │
│    │    └─ 遍历 5 个存储引擎，写入二进制条目:             │
│    │         [1B engine_id][4B klen][key][4B vlen][value]│
│    │    └─ fsync                                          │
│    └─ persist_mark_snapshot_success()                     │
│                                                          │
│  BGSAVE (异步)                                            │
│    │                                                     │
│    ├─ fork()                                             │
│    ├─ 子进程:                                             │
│    │    ├─ kvs_dump_to_fd(tmp_fd, aof_offset)            │
│    │    ├─ fsync + rename(tmp → dump_path)               │
│    │    └─ _exit(0)                                       │
│    └─ 父进程:                                             │
│         ├─ 返回继续服务                                   │
│         ├─ g_bgsave_pid = pid                            │
│         └─ persist_bgsave_poll() 轮询子进程状态           │
│                                                          │
│  自动快照 (autosnap)                                      │
│    │                                                     │
│    ├─ persist_autosnap_cron() 周期性调用                  │
│    ├─ 检查规则: dirty >= changes && elapsed >= seconds    │
│    └─ 触发 persist_bgsave_start()                         │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 3.2 DUMP 文件格式（SAVE/BGSAVE 使用，二进制）

```
┌────────────────────────────────────────────────────────────────┐
│  [0..7]   uint64_t aof_offset    创建 dump 时的 AOF 文件大小   │
│                                  恢复时 AOF 从此处之后开始重放 │
├────────────────────────────────────────────────────────────────┤
│  [8..]    条目反复:                                            │
│    [1B]    uint8_t  engine_id    KVS_ENGINE_xxx                │
│    [4B]    uint32_t klen         key 长度                      │
│    [klen]  char[]   key          key 数据                      │
│    [4B]    uint32_t vlen         value 长度                    │
│    [vlen]  char[]   value        value 数据                    │
│                                                                │
│  DOC 引擎特殊处理:                                              │
│    value = "field1=val1 field2=val2 ..."                       │
│    (换行符 → 空格，避免破坏二进制格式)                         │
└────────────────────────────────────────────────────────────────┘
```

**aof_offset 的作用：**

```
时间线:  ───────┬────────────────┬─────────────────────→
               AOF 文件大小 = X  │  之后的新写命令
               dump 创建时刻     │
               
恢复时:  dump 恢复全量数据 → AOF 从偏移 X 处开始重放 → 恢复增量
         (aof_offset = X)       └─ 跳过这部分，因为 dump 已包含
```

### 3.3 SNAPSHOT 格式（全量同步、BGREWRITEAOF 使用，RESP 文本）

SNAPSHOT 是**另一种格式**，生成 RESP 命令序列，消费者（slave 或 AOF 重写目标文件）可以直接用 `parse_resp_stream` 解析。

生成方式：`snapshot_all_sink()` 遍历 5 个引擎，对每条 key 调用 `emit_cmd3_sink()` 生成标准 RESP：

```
  *3\r\n$4\r\nHSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n  ← hash 引擎
  *3\r\n$4\r\nRSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n  ← rbtree 引擎
  *3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n   ← array 引擎
  *3\r\n$4\r\nXSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n  ← skiptable 引擎
  *4\r\n$6\r\nDOCSET\r\n...                                          ← doc 引擎
  *3\r\n$6\r\nHEXPIRE\r\n...                                         ← 附带 TTL
```

**为什么两种格式？**
- DUMP（二进制）：紧凑，仅用于本地 SAVE/BGSAVE 落盘和恢复
- SNAPSHOT（RESP）：能被 slave 和 AOF 重写文件直接消费，无需额外解析器

### 3.4 恢复流程

```
persist_recover()
    │
    ├─ 1. replay_dump_file(dump_path)
    │      ├─ mmap dump 文件
    │      ├─ 读取 8 字节 aof_offset
    │      ├─ 循环解析二进制条目 → 分配到对应的存储引擎
    │      └─ 返回 aof_offset 作为 AOF 跳过量
    │
    ├─ 2. replay_file(aof_path, aof_offset)
    │      ├─ mmap AOF 文件（fallback: fread）
    │      ├─ 从 aof_offset 处开始（跳过 dump 已包含的部分）
    │      ├─ parse_resp_stream(NULL, buf, &len, from_replication=1)
    │      └─ 重放所有增量 RESP 命令
    │
    ├─ 3. kvs_active_expire_cycle(1000000)
    │      清理恢复过程中已过期的 TTL key
    │
    └─ 4. 重置 dirty_counter、snapshot_ms 等状态
```

**为什么 DUMP 格式是二进制的？**

- 更紧凑，写入/读取更快
- 8 字节 aof_offset 精确标记 AOF 恢复起点
- 恢复时先读 DUMP→再读 AOF 从 offset 开始，不重放冗余命令

**为什么 SNAPSHOT 格式是 RESP 的？**

- 全量同步时可直接发给 slave 执行（parse_resp_stream 兼容）
- BGREWRITEAOF 时可直接写入 AOF 文件

---

## 4. 增量持久化（AOF）

### 4.1 AOF 写入路径

```
写命令执行
    │
    ▼
persist_append_raw(raw, rawlen)          raw = 原始 RESP 字节
    │
    ├─ 如果 g_aof_fd < 0 (AOF 禁用) → 直接返回 0
    │
    ├─ 如果 rawlen >= AOF_BUF_SIZE(64KB):
    │    │  单个命令超过缓冲区容量（极少见），不经过缓冲
    │    ├─ 先 flush 现有缓冲区
    │    └─ 直接 io_uring write+fsync 到磁盘
    │
    └─ 否则:
         ├─ 如果 g_aof_buf_len + rawlen > 64KB: 先 flush 缓冲区腾空间
         ├─ memcpy(raw → g_aof_buf + g_aof_buf_len)
         ├─ g_aof_buf_len += rawlen
         ├─ g_aof_dirty = 1   ← 标记"缓冲区有待刷数据"
         │     EVERYSEC 模式用此标记判断是否需要刷盘；
         │     进程退出时 persist_close() 检查是否有未刷数据
         │
         └─ fsync 策略:
              ├─ ALWAYS:
              │    ├─ 每次 reactor 迭代结束时调 persist_flush_pending()
              │    │   (一次迭代可能处理多条命令，batch flush 减少 fsync 次数)
              │    └─ 距上次 flush 超过 2ms → 强制 flush（延迟上限）
              │
              └─ EVERYSEC:
                   └─ persist_autosnap_cron() 中: 距上次 flush ≥ 1000ms → persist_force_aof_flush()
```

### 4.2 AOF Flush 到磁盘

```
persist_aof_flush_buffer()
    │
    ├─ 1. io_uring 批量提交: write SQE + fsync SQE
    │      ├─ io_uring_get_sqe → prep_write
    │      ├─ io_uring_get_sqe → prep_fsync (IORING_FSYNC_DATASYNC)
    │      ├─ io_uring_submit_and_wait(2)
    │      └─ 收集 2 个 CQE，按返回值区分 write/fsync
    │
    ├─ Fallback (io_uring 失败):
    │      ├─ pwrite + fsync
    │      └─ 或 fdatasync
    │
    └─ 2. 更新 g_aof_write_offset, g_aof_buf_len = 0
```

### 4.3 BGREWRITEAOF（AOF 重写）

```
persist_bgrewriteaof_start()
    │
    ├─ 1. persist_force_aof_flush()        先刷盘当前 AOF
    │
    ├─ 2. fork()
    │
    ├─ 子进程:
    │    ├─ persist_write_aof_snapshot_to(tmp)
    │    │    └─ kvs_snapshot_to_fd(fd) → RESP 格式遍历所有 key
    │    └─ _exit(0)
    │
    └─ 父进程:
         ├─ 继续服务
         ├─ append_to_rewrite_buffer()      新写命令同时写 rewrite buffer
         ├─ persist_bgrewriteaof_poll()     轮询子进程
         └─ 子进程退出后:
              └─ finalize_rewrite_parent()
                   ├─ 打开 tmp 文件
                   ├─ 追加 rewrite buffer 中的所有新命令
                   ├─ fsync
                   ├─ rename(tmp → aof_path)
                   ├─ 重新打开 g_aof_fd
                   └─ 释放 rewrite buffer
```

### 4.4 AOF 缓冲时序图

```
时间轴 ──────────────────────────────────────────────────→

ALWAYS 模式:
  reactor loop:
    [处理请求] → persist_append_raw → 写 buffer
    [处理请求] → persist_append_raw → 写 buffer
    ...
    persist_flush_pending() → flush 全部 buffer → io_uring write+fsync
    [下一个 reactor 迭代]
  
  (最长延迟: ~2ms，由 persist_append_raw 中的超时机制保证)

EVERYSEC 模式:
  reactor loop:
    [处理请求] → persist_append_raw → 写 buffer
    ...
    persist_autosnap_cron() → 距上次 flush >= 1000ms → flush
  (最长延迟: ~1s)
```

### 4.5 Per-Command ALWAYS 尝试与分析（2026-06-29）

**目标：** 实现严格 per-command 的 `appendfsync always`——每条写命令执行后立即 write+fsync 落盘，完成后才回复客户端。使用 io_uring `submit_and_wait` 在一次 syscall 中提交 write+fsync。

**实现：** 新增 `persist_aof_per_command_flush()`，直接调用 `persist_write_and_fsync_uring()`（复用 EVERYSEC 已有的 io_uring batch write+fsync 函数）。ALWAYS 模式下 `persist_append_raw()` 不再缓冲，每条命令直接触发 io_uring write+fsync。

**strace 分析 Redis 5.0.7 的真相：**

测试方法：`strace -f -p <redis_pid> -e trace=fdatasync,write,openat,close -c` 统计 syscall 次数。

```bash
# 1. 启动 Redis always
redis-server --port 6391 --dir /tmp --save "" \
  --appendonly yes --appendfsync always --daemonize yes \
  --pidfile /tmp/r.pid --logfile /tmp/r.log --appendfilename aof_test.aof

# 2. strace attach 到 Redis 进程，统计 fdatasync/write/openat/close
sudo strace -f -p $(cat /tmp/r.pid) \
  -e trace=fdatasync,write,openat,close -c -o /tmp/r.txt &
STRACE_PID=$!

# 3. 跑 benchmark
redis-benchmark -p 6391 -n 100 -c 50 -P 1 -d 64 -r 10000 \
  HSET key:__rand_int__ __rand_int__ value

# 4. 停 strace，统计
sudo kill $STRACE_PID
grep -c fdatasync /tmp/r.txt
grep -c write     /tmp/r.txt
grep -c openat    /tmp/r.txt
grep -c close     /tmp/r.txt
```

测试结果：

| 测试场景 | fdatasync | write | openat | close | 命令数 | fsync:命令 |
|----------|-----------|-------|--------|-------|--------|------------|
| `-c 1` 逐个发送 10 条 HSET | **1** | 12 | 12 | 61 | 10 | 1:10 |
| `-c 50` 50 并发 100 条 HSET | **2** | 102 | 10 | 60 | 100 | 1:50 |

同时测量了本机 fdatasync 延迟（38 字节 write + fdatasync，1000 次平均 = **96µs**），用于计算 fsync 理论瓶颈。

Redis 源码中 `flushAppendOnlyFile()` 确实每条命令后都会调用，但因为 Redis 单线程事件循环的特性：一次 `epoll_wait` 返回多个就绪连接 → 全部读完、执行完 → `server.aof_buf` 已累积多条命令的 RESP 数据 → 一次 `write()+fdatasync()` 全部落盘。**实际效果是事件循环级别的 group commit，并非严格 per-command fsync。** 50 并发时 ~50 条命令共享 1 次 fdatasync（96µs），等效每命令 fsync 成本 ~2µs。单连接下由于 localhost 延迟极低，多条命令也能在同一事件循环内到达，10 条命令仅触发 1 次 fsync。

**为什么没采用 per-command：**

| | kvstore per-command | kvstore 2ms group commit | Redis 5.0.7 "always" |
|---|---|---|---|
| 机制 | 每条命令独立 io_uring write+fsync | 缓冲 2ms 内命令，批量 io_uring | 事件循环级隐式批量 write+fdatasync |
| HSET -c 1 QPS | ~2,857 | ~20,000* | ~2,174 |
| HSET -c 50 QPS | ~3,411 | ~56,838* | ~38,168 |
| fsync 数/100 命令 | 100 | ~2 (50:1 摊销) | ~2 (50:1 摊销) |
| 严格 per-command | ✅ | ❌ (最多丢 2ms 数据) | ❌ (最多丢一个事件循环的数据) |

> *group commit 数据来自 `benchmarks/data/persist_bench/aof_summary.csv`

kvstore per-command 在 `-c 50` 下几乎不随并发扩展（3.4k vs 单连接 2.9k），因为 reactor 单线程串行阻塞在每条命令的 io_uring write+fsync（~300µs/条）。50 条命令串行 = 15ms/轮 → ~3,300 QPS。

而 2ms group commit 将 ~50 条命令的 AOF 数据累积到 64KB buffer，一次 `io_uring_submit_and_wait` 批量 write+fsync，fsync 成本摊销到 50 条命令上，并发下可到 56k QPS。

**结论：** 保留 2ms group commit 方案。per-command 实现在单连接延迟上无优势（2.9k vs 2.2k Redis），并发下因串行 fsync 无法扩展。Redis 的 "always" 语义本身也非严格 per-command——它依赖事件循环隐式批量化。group commit 在该语义下是更好的性能/持久性平衡点。

**相关 commits：** `9ba215f` → `6dfebb9`（per-command 实现 + SQPOLL 尝试 + 修复 + 最终回退到 3cf6cd6 的 2ms group commit）

---

## 5. 全量同步（FULLRESYNC）

### 5.1 总体流程

```
SLAVE 启动                          MASTER
    │                                   │
    ├─ repl_slave_thread()              │
    │  后台线程循环                      │
    │                                   │
    ├─ TCP connect ────────────────────→ 接受连接
    │                                   │
    ├─ 发送 REPLSYNC <replid>           │
    │          <applied_offset> ────────→ handle_parsed_command()
    │          <durable_offset>          │   cmd = "REPLSYNC"
    │                                   │
    │                               ┌───┴──────────────┐
    │                               │ backlog 可续传?   │
    │                               │ replid 匹配?      │
    │                               │ offset 在范围内?   │
    │                               └───┬──────┬───────┘
    │                                   │ YES  │ NO
    │                                   ▼      ▼
    │                            +CONTINUE   queue_snapshot()
    │                            发送 backlog  (全量同步)
    │                                   │
    │                                   ▼
    │                          ┌─────────────────────┐
    │                          │ g_repl_fullsync_     │
    │                          │ in_progress = 1     │
    │                          │ (暂停实时广播)       │
    │                          └──────────┬──────────┘
    │                                     │
    │                          ┌──────────▼──────────┐
    │                          │ kvs_snapshot_to_fp()│
    │                          │ RESP 格式遍历全量数据│
    │                          └──────────┬──────────┘
    │                                     │
    │    +FULLRESYNC <replid>             │
    │    <snap_base_offset>   ←──────────┘
    │    <total_bytes>
    │                                     │
    │    snapshot data chunks ←───────────┘ (RDMA SEND 或 TCP)
    │                                     │
    ├─ parse_resp_stream(NULL,            │
    │   buf, len, from_replication=1)     │
    │   应用每条命令到引擎                  │
    │                                     │
    ├─ 达到 target_bytes:                  │
    │   repl_slave_finish_fullsync()       │
    │   ├─ kvs_dump_to_fd() 写 dump       │
    │   └─ repl_slave_state_save()         │
    │                                     │
    ├─ REPLACK <applied> ←──────────────→ 更新 ack offset
    │                  <durable>          │
    │                                     │
    └─ 进入增量同步模式 ←────────────────→ g_repl_fullsync_in_progress = 0
        (实时 AOF + 复制)                     恢复实时广播
```

### 5.2 全量同步期间的 client_capture

全量同步期间，客户端可能继续向 master 写入数据。这些数据需要被缓存，等全量同步完成后再发送给 slave。

```
全量同步期间 (g_repl_fullsync_in_progress = 1):

  BPF kprobe 挂载在 tcp_recvmsg 上
       │
  Client → Master 的 TCP 数据
       │
       ▼
  repl_client_capture.bpf.c (3 个 hook):
    ├─ kprobe/tcp_recvmsg:   保存 msg 指针到 per-CPU map
    ├─ kretprobe/tcp_recvmsg: 读返回值 + iovec 数据 → bpf_ringbuf_output
    ├─ kprobe/tcp_sendmsg:   探测 REPLDONE → 提前清零 FULLSYNC_IN_PROGRESS
    │
    ├─ L1 缓存 (内存):  数据写入 g_cache_head 链表 (4MB 上限)
    └─ L2 缓存 (磁盘):  L1 超限时 spill 到 /tmp/kvstore_fs_cache.<pid>
       
  全量同步完成后:
    ├─ g_repl_fullsync_in_progress = 0  (用户态)
    ├─ repl_client_capture_set_fullsync(0)  (通知 BPF)
    ├─ client_capture L1+L2 flush: 通过 repl_send_chunked 发送缓存数据
    ├─ c->fwd_healthy = 1  (启用 kprobe 直发)
    └─ BPF REPLDONE 检测: kprobe/tcp_sendmsg 在 REPLDONE 发出时提前切增量模式
```

### 5.3 部分重同步（CONTINUE）

**目的：** slave 短暂断开重连后，如果 backlog 还有它需要的增量数据，跳过全量同步，直接补发差额。

```
Slave 重连 → 发送 REPLSYNC <replid> <applied_offset> <durable_offset>

Master 检查（repl_backlog_can_continue）:
  ├─ replid 必须匹配（master 重启会重新生成 replid）
  ├─ applied_offset >= backlog.start_offset（slave 的断点还在 backlog 内）
  └─ applied_offset <= backlog.end_offset

  如果满足:
    → 发送 +CONTINUE <replid> <continue_offset>
    → repl_backlog_write_range(c, offset)
        从 backlog 环形缓冲中取出 [offset .. end] 的数据
        发送给 slave
        slave 收到后用 parse_resp_stream(from_replication=1) 逐条应用

  如果不满足（replid 变了 或 backlog 不够大）:
    → 发送 +FULLRESYNC → 全量同步
```

Backlog 是 1MB 环形缓冲，正常运行时**一直都**在记录。不是全量同步期间才启用的。它与 client_capture 的 BPF ringbuf 是不同层面的东西（见第 9 题）。

### 5.4 Replication Backlog

```
1MB 环形缓冲区 (g_repl_backlog):

  ┌─────────────────────────────────────────────┐
  │  环形缓冲区 (cap = 1MB)                      │
  │                                              │
  │  start_offset        head                    │
  │  ↓                   ↓                       │
  │  [已确认可丢弃][有效历史数据 (histlen)]       │
  │                                              │
  │  end_offset = start_offset + histlen         │
  └─────────────────────────────────────────────┘

  写入 (repl_backlog_feed):
    每次广播时追加 → end_offset += len, histlen += len
    如果溢出: head 前移，丢弃最旧数据

  读取 (repl_backlog_write_range):
    从 head + (offset - start_offset) 开始
    最多读到 end_offset
    环形 buffer 可能分两段读取
```

**Backlog vs BPF ringbuf（常见混淆点）：**

| | `g_repl_backlog` (1MB 用户态) | BPF ringbuf (内核→用户态) |
|---|---|---|
| **层** | 用户态环形缓冲 | 内核态共享内存 |
| **存什么** | 已广播出去的 RESP 命令原文 | kprobe/tcp_sendmsg 截获的网络数据 |
| **谁写入** | `repl_backlog_feed()` (每个写命令广播时) | BPF `bpf_ringbuf_submit()` (内核 kprobe) |
| **谁读取** | `repl_backlog_write_range()` (CONTINUE 时) | `ring_buffer__poll()` → `kprobe_ringbuf_cb()` |
| **用途** | 部分重同步 (CONTINUE) | kprobe+RDMA 零拷贝增量同步 |
| **生命周期** | 一直运行 | 仅 kprobe+RDMA 模式 |
| **与全量同步关系** | 一直记录；全量同步时暂停读取广播但继续写入 | 一直运行；与全量同步无关 |

**全量同步期间客户端数据的缓存**不是靠 backlog，而是靠 `client_capture`（BPF kprobe+ringbuf + L2 文件缓存，见 5.2 节）。

---

## 6. 增量同步（4 种模式）

增量同步有**4 种传输模式**，全量同步和增量同步的传输层**独立配置**。

### 6.1 传输模式总览

```
全量同步 (repl_fullsync_transport):   增量同步 (repl_realtime_transport):
  ┌──────────┬───────────────┐         ┌──────────────┬──────────────────────────────────┐
  │ 配置值   │ 传输方式       │         │ 配置值       │ 传输方式                          │
  ├──────────┼───────────────┤         ├──────────────┼──────────────────────────────────┤
  │ rdma     │ RDMA SEND      │         │ tcp (默认)   │ 纯 TCP send/recv                 │
  │ (默认)   │ fallback: TCP  │         │              │                                  │
  │ tcp      │ TCP send       │         │ ebpf/sockmap │ BPF sk_msg (hook sendmsg)        │
  └──────────┴───────────────┘         │              │ → sockmap redirect (本机/跨机)   │
                                        │ kprobe-rdma  │ BPF kprobe (hook tcp_sendmsg)      │
                                        │              │ → ringbuf → RDMA WRITE           │
                                        │ ebpf+tcp     │ BPF kprobe (hook tcp_recvmsg)      │
                                        │              │ client_capture → ringbuf          │
                                        │              │ → send(c->fd) 共用 slave TCP 连接  │
                                        └──────────────┴──────────────────────────────────┘

两个配置独立:
  repl_fullsync_transport = rdma    (全量用 RDMA)
  repl_realtime_transport = ebpf+tcp (增量用 client_capture kprobe)
```

**三种 BPF 程序对应不同模式：**

| BPF 程序 | hook 点 | 对应配置 | 数据去向 |
|----------|---------|----------|---------|
| `repl_sockmap.bpf.o` | sk_msg (sendmsg) | `repl_realtime_transport=ebpf/sockmap` | sockmap redirect |
| `repl_kprobe.bpf.o` | kprobe (tcp_sendmsg) | `repl_realtime_transport=kprobe-rdma` | ringbuf → RDMA WRITE |
| `repl_client_capture.bpf.o` | kprobe/kretprobe (tcp_recvmsg) + kprobe (tcp_sendmsg) | `kprobe_enabled=1` 且 role=MASTER | ringbuf → send(c->fd) 共用连接，fwd_healthy=1 时 repl_broadcast 跳过 |

### 6.2 模式 A: 纯 TCP

```
Master:                                   Slave:
  repl_broadcast(raw, rawlen)
    → repl_realtime_send()
    → repl_transport_tcp_send()
    → queue_bytes → reactor on_write
    → send(c->fd) ──── TCP ────────────→ recv(slave_fd)
                                          → parse_resp_stream(from_replication=1)
```

### 6.3 模式 B: eBPF sockmap（`repl_realtime_transport=ebpf/sockmap`）

**原理：** BPF sk_msg 程序在内核态拦截 `sendmsg`，通过 sockmap 将数据重定向到目标 socket，绕过用户态拷贝。

```
MASTER (sock_map[0] = master_fd)        SLAVE (sock_map[1] = slave_fd)

  repl_broadcast → send(master_fd)
      │
      ▼                              [内核态 — 本机场景]
  BPF sk_msg:                        同一台机器时 pinned BPF maps 共享:
    bpf_msg_redirect_map(msg,         sock_map[0] = master accept fd (master 进程注册)
      sock_map, redirect_key,         sock_map[1] = slave connect fd (slave 进程注册)
      BPF_F_INGRESS)                  BPF: sock_map[0] → sock_map[1]
    → 数据直入 slave socket 接收队列  → slave recv() 读到
    → 全程在内核态完成，无用户态转发
```

**跨机变体（`ebpf_forward=1`）：**
不带 `BPF_F_INGRESS`，数据进入 sock_map[redirect_key] 的发送队列，经 TCP 传到远端。redirect_key 对应的 fd 需由 `repl_ebpf_register_forward_fd()` 注册（当前未实现自动注册）。

### 6.4 模式 C: kprobe + RDMA WRITE（`repl_realtime_transport=kprobe-rdma`）

**原理：** BPF kprobe 挂载在 `tcp_sendmsg`，在内核态拦截 master→slave 方向的 TCP 数据，通过 BPF ringbuf 传到用户态后，用 RDMA WRITE 单边写入 slave 的 MR 环形缓冲区。

```
MASTER                                        SLAVE

[内核 BPF kprobe]
tcp_sendmsg() 被调用时:
  ├─ 过滤: PID/fd 匹配
  ├─ 从 msghdr->msg_iter->iov 读取数据
  ├─ 写入 BPF ringbuf: [4B len][payload]
  └─ bpf_ringbuf_submit()

[用户态 ringbuf 回调]
kprobe_ringbuf_cb():
  ├─ 解析 [4B len][payload]
  ├─ 获取 RDMA WRITE slot
  ├─ ibv_post_send RDMA_WRITE(data → slave MR slot)
  └─ ibv_post_send RDMA_WRITE(producer_head → slave MR header)

[TCP 路径同时运行作为保底]
send() → TCP → slave                  recv() → parse_resp_stream
                                         │
                                      repl_offset 去重
```

Slave 侧 MR 环形缓冲区结构见第 8.3 节。poll 线程循环读取 `producer_head`，消费 slot 数据 → `parse_resp_stream(from_replication=1)`。

### 6.5 模式 D: eBPF+TCP（`repl_realtime_transport=ebpf+tcp`）—— client_capture kprobe

**这是默认推荐配置**（配合 `repl_fullsync_transport=rdma`），全量用 RDMA，增量用这条 kprobe 转发路径。

**原理：** 在**内核态**拦截 `tcp_recvmsg`（客户端→master 方向），通过 BPF ringbuf 将原始 TCP 字节流直接转发给 slave，**绕过了 `parse_resp_stream → handle_parsed_command → repl_broadcast` 的用户态重建路径**。

**涉及的 BPF 程序：** `repl_client_capture.bpf.o`，hook `tcp_recvmsg`（与模式 C 的 `tcp_sendmsg` hook 不同）

**Master 初始化：**
```
kprobe_enabled=1 且 role=MASTER:
  → repl_client_capture_init()
    → 加载 repl_client_capture.bpf.o
    → attach kprobe 到 tcp_recvmsg
    → 启动 ringbuf poll 线程
    → g_repl_client_capture_active = 1
```

**Slave 初始化：**
```
kprobe_enabled=1 且 role=SLAVE:
  → 无需特殊初始化，增量数据通过同一条 TCP 连接 (slave_fd) 接收
  → parse_resp_stream(NULL, buf, &len, from_replication=1)
```

**完整数据流：**

```
Client 发送 SET key val
    │
    ▼
Master 内核 tcp_recvmsg()
    │
    ├─ kprobe 路径 (内核态拦截):
    │     BPF kprobe/kretprobe 在 tcp_recvmsg 拦截客户端原始 TCP 数据
    │       → BPF ringbuf: [4B len][payload]
    │       → 用户态 ring_buffer__poll (client_poll_thread, 5ms 间隔)
    │       → client_ringbuf_cb()
    │           │
    │           ├─ g_repl_fullsync_in_progress=1 (全量同步中):
    │           │    缓存到 L1 (内存链表, 4MB 上限) / L2 (临时文件)
    │           │
    │           └─ g_repl_fullsync_in_progress=0 (增量同步):
    │                遍历 replicas 链表，对 fwd_healthy=1 的 slave:
    │                  send(c->fd, payload, len)  ← 共用 slave TCP 连接 (c->fd)
    │
    └─ repl_broadcast 路径 (保底):
         parse_resp_stream → handle_parsed_command
           → repl_broadcast(raw, rawlen)
           → 对 fwd_healthy=1 的 slave 跳过 (已被 kprobe 路径服务)
           → 对 fwd_healthy=0 的 slave 走 TCP send()
```

**全量→增量切换（queue_snapshot 尾部）：**

```
① g_repl_fullsync_in_progress = 0        ← 用户态关标志
② repl_client_capture_set_fullsync(0)     ← 通知 BPF: 切换增量模式
   (BPF 侧可能更早切换: kprobe/tcp_sendmsg 探测到 REPLDONE 时主动清零)
③ repl_client_capture_flush_to_slave(c)   ← 刷 L1+L2 缓存
   └─ 成功 → c->fwd_healthy = 1, c->fwd_last_active = now
   └─ 失败 → c->fwd_healthy = 0 (回退到 repl_broadcast)
④ backlog gap 回放 (仅当无缓存数据时)
```

**fwd_healthy 健康检查 (repl_kprobe_fwd_health_check, 由 reactor 定时调用)：**

```
对每个 replica:
  fwd_healthy=1 且 fwd_last_active 超时 (5s) 且近期有写:
    → c->fwd_healthy = 0  ← 标记不健康，回退到 repl_broadcast
  fwd_healthy=0 且近期无写 (5s 空闲窗口):
    → c->fwd_healthy = 1  ← 恢复健康
```

**关键设计点：**

- **共用 TCP 连接**：kprobe 转发和 repl_broadcast 共用同一个 `c->fd`，不再有 port+13 独立连接
- **TCP 顺序保证**：REPLDONE 通过 TCP 发送（即使全量走 RDMA），确保后续 kprobe fwd 数据在 REPLDONE 之后到达 slave
- **BPF 侧提前切换**：`kprobe/tcp_sendmsg` 探测到 REPLDONE 时立即清除 FULLSYNC_IN_PROGRESS，比用户态更早
- **fwd_healthy 互斥**：kprobe 转发和 repl_broadcast 互斥——fwd_healthy=1 时 repl_broadcast 跳过该 slave，避免重复发送

### 6.6 传输层抽象

```
repl_transport_ops_t:
  ├─ TCP / eBPF+TCP:
  │    .send = repl_transport_tcp_send → queue_bytes → send()
  │    .connect_slave = socket + connect
  │    .disconnect_slave = close
  │    (ebpf+tcp 同样使用 tcp_ops，额外的 kprobe 转发不在 transport ops 层面)
  │
  ├─ eBPF sockmap:
  │    .send = repl_transport_ebpf_send → queue_bytes → send()
  │           (fd 已注册到 sockmap, BPF sk_msg 在内核态拦截并重定向)
  │    .connect_slave = TCP connect + repl_ebpf_register_fd()
  │    .disconnect_slave = repl_ebpf_unregister_fd() + close
  │
  ├─ RDMA:
  │    .send = repl_rdma_try_send
  │           → ibv_post_send (RDMA SEND, pipeline 模式)
  │    .connect_slave = RDMA CM 完整握手流程
  │    .disconnect_slave = RDMA 资源清理
  │
  └─ kprobe-rdma:
       .send = 返回 -1 (不做实际发送, kprobe 在内核拦截 tcp_sendmsg)
       .connect_slave = kprobe RDMA 建链
       .disconnect_slave = kprobe RDMA 清理

传输选择逻辑:
  repl_transport_ops_for_context(KVS_REPL_SEND_FULLSYNC):
    → 根据 repl_fullsync_transport 配置选择 (rdma/tcp)
  repl_transport_ops_for_context(KVS_REPL_SEND_REALTIME):
    → 根据 repl_realtime_transport 配置选择 (tcp/ebpf/kprobe-rdma/ebpf+tcp)

注意: eBPF+TCP 的 kprobe 转发路径不在 transport ops 框架内 —
      它通过 client_capture BPF (hook tcp_recvmsg) + 共用 slave TCP 连接 (c->fd) 实现。
      fwd_healthy=1 时 repl_broadcast 跳过该 slave，由 ringbuf 回调直接 send(c->fd)。

Fallback 机制:
  如果 RDMA/eBPF 发送失败 → 自动 fallback 到 TCP
  kprobe forward 启动失败 → c->fwd_healthy=0 → repl_broadcast 负责该 slave
  kprobe forward 健康检查失败 → c->fwd_healthy=0 → kprobe 路径停止转发, repl_broadcast 保底
  (fwd_healthy=1 时 repl_broadcast 跳过该 slave，避免重复发送)
```

---

## 7. 从机保存数据到磁盘

### 7.1 Slave 端写路径（全量同步→增量同步）

**全量和增量在 slave 侧是时序上的先后关系，不能同时进行。**

```
Slave 接收数据 (from_replication=1):
    │
    ▼
parse_resp_stream(NULL, buf, len, from_replication=1)
    │
    ▼
handle_parsed_command(c=NULL, ..., from_replication=1)
    │
    ├─ 1. 应用到存储引擎
    │
    ├─ 2. 持久化:
    │    ├─ 全量同步阶段 (g_slave_loading_fullsync = 1):
    │    │    └─ 不写 AOF — 全量数据通过 repl_slave_finish_fullsync()
    │    │       中的 kvs_dump_to_fd() 一次性写入二进制 dump
    │    │
    │    └─ 增量同步阶段 (g_slave_loading_fullsync = 0):
    │         ├─ persist_append_raw(raw, rawlen) → AOF 缓冲
    │         └─ repl_slave_note_durable(rawlen)
    │              g_slave_repl_durable_offset += rawlen
    │              repl_slave_state_save() 保存状态
    │
    └─ 3. 跟踪 offset:
         └─ repl_slave_note_applied(rawlen)
              g_slave_repl_applied_offset += rawlen
              全量同步阶段:
                g_slave_fullsync_loaded_bytes += rawlen
                当 loaded >= target → repl_slave_finish_fullsync()
                  ├─ g_slave_loading_fullsync = 0  ← 切换到增量模式
                  ├─ kvs_dump_to_fd() 写二进制 dump
                  └─ 之后收到的数据走增量 AOF 路径（上面的分支）
```

**时间线：**

```
Slave 时间线:
  ────────────────────────────────────────────────────────→
  [全量同步加载]               [增量同步]
  AOF 不写                     AOF 写入
  loaded_bytes 累计             applied_offset 累计
                               durable_offset 累计
                               
  repl_slave_finish_fullsync() ← 分界线: 写 dump + 切换模式
```

### 7.2 全量同步完成时的 Slave 处理

```
repl_slave_finish_fullsync()
    │
    ├─ 1. g_slave_loading_fullsync = 0
    │
    ├─ 2. kvs_dump_to_fd(dump_fd, 0)
    │      全量数据写入二进制 dump 文件
    │      之后的增量数据通过 AOF 持久化
    │
    ├─ 3. repl_slave_state_save()
    │      写入 repl_state 文件: <replid> <applied_offset> <durable_offset>
    │      (kvstore.aof.replstate)
    │
    └─ 4. repl_slave_send_ack()
           → REPLACK <applied> <durable> 发送给 master
```

### 7.3 Slave 状态持久化

```
Slave 重启恢复:
  ├─ persist_recover()
  │    ├─ replay_dump_file(dump_path)  → 恢复全量数据
  │    └─ replay_file(aof_path, aof_offset) → 重放增量 AOF
  │
  ├─ repl_slave_state_load()
  │    读取 kvstore.aof.replstate:
  │      <master_replid> <applied_offset> <durable_offset>
  │    用于向 master 请求部分重同步
  │
  └─ repl_slave_thread()
       连接 master
       发送 REPLSYNC <replid> <applied_offset> <durable_offset>
```

### 7.4 Slave AOF 的 AOF 重写

```
Slave 同样支持 BGREWRITEAOF:
  ├─ 因为 AOF 只追加不压缩，文件会持续增长
  ├─ 但 slave 的 AOF 来自 master 的增量同步
  ├─ 实际上：
  │   全量同步完成后写 dump → 充当 AOF 压缩的替代
  │   增量同步期间的 AOF 记录增量命令
  └─ 重启时：dump(全量) + AOF(增量) = 完整数据
```

---

## 8. 关键数据结构

### 8.1 连接 (conn_t)

```c
// include/kvstore/kvstore.h:248-267
typedef struct conn_s {
    int fd;                          // socket fd
    int is_listener;                 // 是否监听 socket
    int is_replica;                  // 是否 replicate 连接
    int repl_draining;               // 是否正在断开
    int repl_fullsync_pending;       // 是否等待全量同步
    int repl_transport_kind;         // 传输层类型
    unsigned long long repl_offset_sent;      // 已发送 offset
    unsigned long long repl_applied_offset_ack;  // slave 已应用 offset
    unsigned long long repl_durable_offset_ack;  // slave 已持久化 offset
    long long repl_last_send_ms;     // 上次发送时间
    long long repl_last_ack_ms;      // 上次 ACK 时间
    unsigned char inbuf[65536];      // 输入缓冲
    unsigned char out_ring[65536];   // 输出环形缓冲
    size_t out_ring_head;            // 读位置
    size_t out_ring_tail;            // 写位置
    size_t out_ring_len;             // 待发送字节数
    int fwd_healthy;                 // kprobe 转发健康状态
    time_t fwd_last_active;          // 上次成功转发时间戳
    struct conn_s *next_replica;     // replica 链表
} conn_t;
```

### 8.2 复制 Backlog

```c
// src/replication/kvs_repl.c:40-47
typedef struct repl_backlog_s {
    unsigned char *buf;              // 1MB 环形缓冲
    size_t cap;                      // 容量 (1MB)
    size_t histlen;                  // 有效历史长度
    size_t head;                     // 起始位置索引
    unsigned long long start_offset; // 起始 offset (全局)
    unsigned long long end_offset;   // 结束 offset (全局)
} repl_backlog_t;
```

### 8.3 kprobe RDMA 环形缓冲区

```c
// include/kvstore/replication/repl_kprobe.h
typedef struct __attribute__((packed)) kprobe_rdma_ringbuf_s {
    volatile uint64_t producer_head;   /* Master WRITE 更新 */
    volatile uint64_t consumer_tail;   /* Slave 本地更新 */
    unsigned char slots[KPROBE_RDMA_SLOT_COUNT * KPROBE_RDMA_SLOT_CAPACITY];
} kprobe_rdma_ringbuf_t;
// 每个 slot: [4B payload_len][payload_len bytes RESP 数据]
```

### 8.4 AOF 缓冲区

```c
// src/persistence/kvs_persist.c:36-39
#define AOF_BUF_SIZE 65536
static unsigned char g_aof_buf[AOF_BUF_SIZE];  // 64KB 写缓冲
static size_t g_aof_buf_len = 0;               // 当前缓冲长度
static long long g_aof_write_offset = 0;       // AOF 文件写入偏移
static int g_aof_dirty = 0;                    // 脏标记
```

### 8.5 关键全局状态

```c
// 持久化
int g_aof_fd;                        // AOF 文件 fd
pid_t g_bgsave_pid;                  // BGSAVE 子进程
unsigned long long g_dirty_counter;  // 自上次快照以来的写次数
long long g_last_snapshot_ms;        // 上次快照时间

// 复制 (Master 侧)
char g_master_replid[41];            // Master replication ID
unsigned long long g_master_repl_offset;  // 全局复制 offset
conn_t *g_replicas;                  // replica 连接链表
volatile int g_repl_fullsync_in_progress; // 全量同步进行中

// 复制 (Slave 侧)
unsigned long long g_slave_repl_applied_offset;  // 已应用 offset
unsigned long long g_slave_repl_durable_offset;  // 已持久化 offset
int g_slave_loading_fullsync;        // 是否在全量同步中
unsigned long long g_slave_fullsync_target_bytes; // 全量同步目标字节
```

---

## 9. 配置项速查


| 配置项                    | 默认值          | 说明                                       |
| ------------------------- | --------------- | ------------------------------------------ |
| `role`                    | master          | master/slave                               |
| `repl_transport_backend`  | tcp             | tcp/rdma/ebpf/sockmap/kprobe-rdma/ebpf+tcp |
| `repl_fullsync_transport` | rdma            | 全量同步传输层                             |
| `repl_realtime_transport` | tcp             | 增量同步传输层                             |
| `appendfsync`             | always          | always/everysec                            |
| `autosnap`                | (无)            | 自动快照规则，格式:`seconds:changes,...`   |
| `ebpf_enabled`            | 0               | 是否启用 eBPF                              |
| `ebpf_redirect`           | 0               | 是否启用 eBPF 重定向                       |
| `ebpf_forward`            | 0               | 是否启用 eBPF 跨机转发                     |
| `ebpf_pin_path`           | /sys/fs/bpf/... | BPF map pin 路径                           |
| `kprobe_enabled`          | 1               | 是否启用 kprobe+RDMA                       |
| `rdma_dev`                | siw0            | RDMA 设备名                                |
| `rdma_port`               | port+1          | RDMA 端口                                  |
| `rdma_recv_slots`         | 64              | RDMA 接收 slot 数                          |
| `rdma_chunk_size`         | 256KB           | RDMA 分块大小                              |
| `rdma_qp_wr_depth`        | 64              | QP work request 深度                       |

---

## 附录：源码索引


| 功能                  | 关键函数                                                      | 源文件:行号                               |
| --------------------- | ------------------------------------------------------------- | ----------------------------------------- |
| RESP 解析入口         | `parse_resp_stream()`                                         | `src/main/kvstore.c:1790`                 |
| 命令分发              | `handle_parsed_command()`                                     | `src/main/kvstore.c:1000`                 |
| 写命令处理            | `engine_set()` → `persist_append_raw()` + `repl_broadcast()` | `src/main/kvstore.c:1655-1776`            |
| AOF 追加              | `persist_append_raw()`                                        | `src/persistence/kvs_persist.c:569`       |
| AOF 刷盘              | `persist_aof_flush_buffer()`                                  | `src/persistence/kvs_persist.c:199`       |
| SAVE                  | `persist_save_dump()`                                         | `src/persistence/kvs_persist.c:608`       |
| BGSAVE                | `persist_bgsave_start()`                                      | `src/persistence/kvs_persist.c:660`       |
| BGREWRITEAOF          | `persist_bgrewriteaof_start()`                                | `src/persistence/kvs_persist.c:724`       |
| 恢复                  | `persist_recover()`                                           | `src/persistence/kvs_persist.c:619`       |
| DUMP 生成             | `kvs_dump_to_fd()`                                            | `src/main/kvstore.c:2126`                 |
| SNAPSHOT 生成         | `kvs_snapshot_to_fd()`                                        | `src/main/kvstore.c:2102`                 |
| 复制广播              | `repl_broadcast()`                                            | `src/main/kvstore.c:477`                  |
| 全量同步              | `queue_snapshot()`                                            | `src/replication/kvs_repl.c:538` (approx) |
| Backlog 续传          | `repl_backlog_send_continue()`                                | `src/replication/kvs_repl.c:2127`         |
| Slave 线程            | `repl_slave_thread()`                                         | `src/replication/kvs_repl.c`              |
| Slave 全量完成        | `repl_slave_finish_fullsync()`                                | `src/replication/kvs_repl.c:1902`         |
| eBPF sockmap 加载     | `repl_ebpf_load_object()`                                     | `src/replication/kvs_repl_ebpf.c:155`     |
| kprobe BPF 加载       | `kprobe_load_bpf()`                                           | `src/replication/kvs_repl_kprobe.c:114`   |
| kprobe ringbuf 回调   | `kprobe_ringbuf_cb()`                                         | `src/replication/kvs_repl_kprobe.c`       |
| Slave MR poll         | `kprobe_rdma_slave_poll()`                                    | `src/replication/kvs_repl_kprobe.c`       |
| client_capture 初始化 | `repl_client_capture_init()`                                  | `src/replication/kvs_repl_kprobe.c`       |
| 传输层选择            | `repl_transport_ops_for_context()`                            | `src/replication/kvs_repl.c:1583`         |
| 自动快照定时器        | `persist_autosnap_cron()`                                     | `src/persistence/kvs_persist.c:897`       |
| TTL 过期              | `kvs_active_expire_cycle()`                                   | `src/expire/kvs_expire.c`                 |
