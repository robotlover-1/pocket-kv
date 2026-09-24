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
| `src/replication/kvs_repl_ebpf.c`               | eBPF sockmap 加载、fd 注册、统计、转发背压查询               |
| `src/replication/kvs_repl_kprobe.c`             | kprobe+RDMA WRITE 增量同步、fwd_healthy 健康检查             |
| `src/ebpf_proxy/`                               | 独立 ebpf-proxy 进程：ebpf+tcp 增量转发、proxy_cache、session |
| `src/replication/bpf/repl_sockmap.bpf.c`        | BPF sk_msg 程序（sockmap 重定向）                            |
| `src/replication/bpf/repl_kprobe.bpf.c`         | BPF kprobe 程序（tcp_sendmsg 拦截）                          |
| `src/replication/bpf/repl_client_capture.bpf.c` | BPF `fexit/tcp_recvmsg` 单 hook（原始字节流 → ringbuf，ebpf-proxy 消费） |

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
    │      ├─ repl_backlog_feed()               写入 10MB 复制 backlog 环形缓冲
    │      │      唯一喂入点；无 Slave 时直接返回并标记历史不连续（不允许 partial resync）
    │      ├─ repl_note_broadcast()             g_master_repl_offset += rawlen
    │      │      全局复制偏移量，单调递增的字节计数。slave 用它汇报同步进度。
    │      └─ 遍历 g_replicas 链表，只发给"已全量同步完、处于增量稳态"的 slave:
    │           ├─ 跳过 repl_draining:    该连接正在断开，发了也收不到
    │           ├─ 跳过 repl_fullsync_pending: 还没拿到全量基准数据
    │           ├─ 跳过 g_repl_fullsync_in_progress: 全局全量同步中。字节已进 backlog
    │           │      （写路径统一喂入），ebpf+tcp 下由 ebpf-proxy 的 proxy_cache
    │           │      接住并全量结束后 flush（见 6.5）
    │           ├─ 跳过 c->repl_transport_kind == EBPF_TCP: 由独立 ebpf-proxy 进程转发
    │           ├─ 跳过 c->fwd_healthy=1: kprobe 转发路径（kprobe-rdma）已在内核态服务
    │           └─ 对每条有效连接: repl_realtime_send(c, raw, rawlen)
    │                └─ transport ops → send()
    │                     ├─ TCP: queue_bytes → reactor on_write → send()
    │                     ├─ ebpf+tcp: 不走这里（上面已跳过），由 ebpf-proxy 转发
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

### 5.2 全量同步期间客户端写入的缓存

全量同步期间，客户端可能继续向 master 写入数据。这些数据需要被缓存，等全量同步完成后再发送给 slave。

当前实现与传输层绑定，分两条路径：

```
① ebpf+tcp（推荐/默认）—— 由独立 ebpf-proxy 进程的 proxy_cache 承担

  repl_client_capture.bpf.c：单个 hook  fexit/tcp_recvmsg
       │  （client_ctl[7]=CAPTURE_ENABLE 为 0 时最前早退，不读数据不写 ringbuf）
       ▼
  BPF ringbuf (64MB) → ebpf-proxy 主线程消费
       │
       ├─ STATE == FORWARDING 且 slave 已连接 → 转发队列 → 转发线程 writev → Slave
       └─ STATE == BUFFERING（全量同步中）    → proxy_cache（节点带 session_id）

  全量边界（queue_snapshot）:
    ├─ g_repl_fullsync_in_progress = 1（用户态停广播）
    ├─ client_ctl[3] = 1 通知 proxy 切 BUFFERING
    ├─ master 等 proxy 置 client_ctl[12]=1 确认（最多 500ms）
    │    proxy 在此刻：清空转发队列 + 丢弃旧 proxy_cache
    ├─ snap_base_offset = master_repl_offset（确认后才取，边界干净）
    └─ repl_backlog_reset(snap_base_offset)（backlog 以快照边界重建）
  全量完成后（REPLDONE，master 收到 slave 的 REPLDONE）:
    ├─ client_ctl[3] = 0 → proxy 切 FORWARDING 并 flush 本 session 的 proxy_cache
    └─ ebpf+tcp 不再回放 backlog（避免与 proxy_cache 双路送达）

② kprobe-rdma —— 进程内健康检查 + 内核态转发
    ├─ 全量期间 g_repl_fullsync_in_progress=1，repl_broadcast 只喂 backlog 不下送
    ├─ 全量结束后 REPLDONE 走 TCP，随后按 backlog 补发
    └─ c->fwd_healthy=1 时由内核态 kprobe 路径服务，repl_broadcast 跳过该 slave
       （repl_kprobe_fwd_health_check 显式跳过 EBPF_TCP 的 replica）
```

> 历史上还有一套「进程内 client_capture」实现（L1 内存链表 4MB + L2 临时文件 spill、
> `client_ringbuf_cb`、`client_poll_thread`），代码中已移除，上面 ① 即其替代。

### 5.3 部分重同步（CONTINUE）

**目的：** slave 短暂断开重连后，如果 backlog 还有它需要的增量数据，跳过全量同步，直接补发差额。

```
Slave 重连 → 发送 REPLSYNC <replid> <applied_offset> <durable_offset>

Master 检查（repl_backlog_can_continue）:
  ├─ replid 必须匹配（master 重启会重新生成 replid）
  ├─ backlog_contiguous 必须为真（无 Slave 期间 Master 的写不会进 backlog，见下）
  ├─ backlog.end_offset >= master_repl_offset（backlog 必须覆盖到 Master 当前 offset）
  ├─ applied_offset >= backlog.start_offset（slave 的断点还在 backlog 内）
  └─ applied_offset <= backlog.end_offset

  如果满足:
    → 发送 +CONTINUE <replid> <continue_offset>
    → repl_backlog_write_range(c, offset)
        从 backlog 环形缓冲中取出 [offset .. end] 的数据
        发送给 slave
        slave 收到后用 parse_resp_stream(from_replication=1) 逐条应用

  如果不满足（replid 变了 / backlog 不够大 / 历史有缺口）:
    → 发送 +FULLRESYNC → 全量同步
```

**partial resync 也有 barrier（与 FULLRESYNC 同一套机制）：**

backlog replay 走 master→slave 的**控制连接**，而 eBPF 实时转发走 proxy 的**另一条** TCP
连接（slave 的 port+1）—— 两条连接之间没有顺序保证。不设屏障时：

```text
Slave 缺 [1000,1200)，master 正在 replay 旧命令 INCR a
同时新客户端执行 DEL a，经 proxy 实时通道直达 Slave
Slave 可能先收到 DEL a 再收到 INCR a → 最终状态错误
```

因此 partial resync 与 FULLRESYNC 共用 `repl_barrier_*`（client_ctl[3]/[12] 握手）：

```text
REPLSYNC offset=1000
  → ① repl_barrier_begin(): 先让 proxy 进 BUFFERING 并等它确认
  → ② repl_add_slave(): 此时才 CAPTURE_ENABLE=1（屏障必须早于开捕获，
       否则屏障生效前捕获的写会被实时转发，与随后的 replay 重叠）
  → ③ catchup_end = master_offset；replay [1000, catchup_end)
  → ④ 等 Slave 的 REPLACK 报告 applied >= catchup_end 才放行（repl_barrier_release）
       —— 保证 proxy flush cache 一定发生在 replay 数据被应用之后
  → ⑤ proxy 切回 FORWARDING，flush proxy_cache，恢复实时转发
```

> ⚠️ **FULLRESYNC 的放行条件不同**：必须由 REPLDONE 触发，**不能**用
> "applied >= catchup_end"。Slave 一收到 `+FULLRESYNC` 就把 applied 设成快照基准 offset，
> 此时它还在往临时文件里写快照；若据此提前放行，proxy 会立刻 flush cache，而 Slave 正处于
> `loading_fullsync` 状态——它会把收到的任何非控制行**当成 KVSD 字节写进快照文件**，
> 直接损坏全量数据（实测报 `replay_dump_file: invalid engine_id 64 at pos 8`）。
> 只有 REPLDONE 才代表"快照已加载完成"。
>
> 屏障握手失败（proxy 未确认 BUFFERING）一律 **fail-closed**：拒绝本次 resync 并
> shutdown 写方向让 Slave 重连重试，绝不在无屏障的情况下让两条连接并行。

**为什么必须查「连续性」：** `repl_backlog_feed()` 在无 Slave 时直接返回（不分配 / 不写入），
但 `repl_note_broadcast()` 仍会推进 `master_repl_offset`。于是：

```
Slave 最后 offset=1000，backlog=[500,1200]；Slave 全部断开
Master 又写到 1500 → backlog_end 停在 1200，1200~1500 成为复制历史缺口
Slave 重连请求 offset=1000：仍落在 [500,1200] 区间内
  只看区间 → 误判可续，缺口数据永久丢失
  加上连续性/覆盖检查 → can_continue=0 → FULLRESYNC
```

Backlog 是 10MB 环形缓冲，正常运行时（有 Slave 时）**一直都**在记录。它与
client_capture 的 BPF ringbuf 是不同层面的东西（见第 9 题）。建立 FULLRESYNC 边界时
会调 `repl_backlog_reset(snap_base_offset)` 丢弃旧历史并以快照边界重建——快照已经覆盖
base 之前的全部状态，旧字节留着只会让 partial resync 误判、或与 proxy_cache 双重回放。

### 5.4 Replication Backlog

```
10MB 环形缓冲区 (g_repl_backlog):

  ┌─────────────────────────────────────────────┐
  │  环形缓冲区 (cap = 10MB)                     │
  │                                              │
  │  start_offset        head                    │
  │  ↓                   ↓                       │
  │  [已确认可丢弃][有效历史数据 (histlen)]       │
  │                                              │
  │  end_offset = start_offset + histlen         │
  └─────────────────────────────────────────────┘

  写入 (repl_backlog_feed):
    每个写命令成功后追加 → end_offset += len, histlen += len
    喂入点唯一：写命令路径（repl_broadcast 内不再二次 feed，否则
    backlog_end_offset 会跑在 master_repl_offset 前面）
    无 Slave 时直接返回且标记 contiguous=0（不分配，省 10MB）
    如果溢出: head 前移，丢弃最旧数据（环形缓冲固有行为）
    建立 FULLRESYNC 边界时 repl_backlog_reset(snap_base_offset) 重建历史

  读取 (repl_backlog_write_range):
    从 head + (offset - start_offset) 开始
    最多读到 end_offset
    环形 buffer 可能分两段读取
```

**Backlog vs BPF ringbuf（常见混淆点）：**

| | `g_repl_backlog` (1MB 用户态) | BPF ringbuf (内核→用户态) |
|---|---|---|
| **层** | 用户态环形缓冲 | 内核态共享内存 |
| **存什么** | 已广播出去的 RESP 命令原文 | fexit/tcp_recvmsg 截获的客户端原始字节流 |
| **谁写入** | `repl_backlog_feed()` (每个写命令广播时) | BPF `bpf_ringbuf_submit()` (内核 kprobe) |
| **谁读取** | `repl_backlog_write_range()` (CONTINUE 时) | ebpf-proxy 的 `ring_buffer__poll()` → `ringbuf_callback()` |
| **用途** | 部分重同步 (CONTINUE) | ebpf-proxy 实时转发 / 全量期间缓存 |
| **生命周期** | 一直运行（无 Slave 时不分配，且标记历史不连续） | 仅 ebpf-proxy 进程运行期间 |
| **与全量同步关系** | 全量边界处 `repl_backlog_reset()` 以 snap_base_offset 重建 | 全量期间 proxy 走 BUFFERING → proxy_cache |

**全量同步期间客户端数据的缓存**分传输层而定：`ebpf+tcp` 由 `ebpf-proxy` 的 `proxy_cache` 承担
（见 6.5），`kprobe-rdma` 由内核态 kprobe 路径承担。**不是**靠 backlog —— backlog 只负责
「复制会话重连后按 offset 续传」，两者语义不同（见 5.2）。

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
| `repl_client_capture.bpf.o` | `fexit/tcp_recvmsg`（单 hook） | `repl_realtime_transport=ebpf+tcp` 且 role=MASTER | ringbuf → **独立 ebpf-proxy 进程** → TCP 转发到 slave 的 port+1；repl_broadcast 跳过 EBPF_TCP 副本 |

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

### 6.5 模式 D: eBPF+TCP（`repl_realtime_transport=ebpf+tcp`）—— 独立 ebpf-proxy 进程

**这是默认推荐配置**（配合 `repl_fullsync_transport=rdma`），全量走 RDMA，增量走 eBPF 捕获 + TCP 转发。

**原理：** 在**内核态**用 `fexit/tcp_recvmsg` 截获「客户端 → master」方向的原始 TCP 字节流，
经 BPF ringbuf 交给**独立的 `ebpf-proxy` 进程**，由它直接转发给 Slave；
**绕过 `parse_resp_stream → handle_parsed_command → repl_broadcast` 的用户态重建路径**。

注意：capture、转发、缓存都不在 master 进程里。master 只做三件事——拉起 proxy、
写 `client_ctl` 控制位、维护与自己 offset 对齐的 `repl_backlog`。

**涉及的 BPF 程序：** `repl_client_capture.bpf.o`，**只有一个 hook** `fexit/tcp_recvmsg`
（不是 kprobe/kretprobe 双 hook，也没有 `kprobe/tcp_sendmsg`）。

**Master 初始化（启动时）：**

```
role=MASTER 且 repl_realtime_transport 含 "ebpf":
  → spawn_ebpf_proxy()       posix_spawn build/ebpf_proxy（子进程继承 root 才能加载 BPF）
  → ebpf_proxy_init_bg()     后台线程轮询 proxy_cfg，就绪后写入 master pid / port

ebpf-proxy 进程:
  → bpf_object__open_file(repl_client_capture.bpf.o) + load
  → pin maps 到 /sys/fs/bpf/kvstore_repl_sockmap/
       client_ctl / proxy_cfg / client_stats / client_cache_ringbuf / client_tmpbuf
  → attach fexit/tcp_recvmsg，ring_buffer__new
  → client_ctl[1]=master_pid, client_ctl[2]=master_port
  → 启动心跳线程(100ms) / 诊断线程(EBPF_PROXY_DIAG=1 时) / 转发线程
```

**Slave 初始化：**

```
repl_realtime_transport 含 ebpf:
  → 在 master_port+1 上额外监听一个 proxy listener（发送 REPLSYNC 之前就绪）
  → 接受 ebpf-proxy 的连接，收到的数据按 from_replication=1 解析应用
  → 控制面仍走原来的 master_port 连接
```

**Master 收到 REPLSYNC 时（建立 session 边界）：**

```
handle_parsed_command("REPLSYNC"):
  → repl_add_slave(c)
  → 若是第一个 Slave：repl_session_begin()
        client_ctl[9] = session_id   ← 先定身份
        client_ctl[10]= 0            ← 解除作废
        client_ctl[8] = 1            ← 标记有效
        client_ctl[7] = 1            ← 最后才开捕获（BPF 从这里开始抓）
  → 向 proxy_cfg 写 slave_addr / slave_port(= master port+1)
  → can_continue ? 回 +CONTINUE + backlog 补发 : queue_snapshot() 走全量
```

**完整数据流：**

```
Client 发送 HSET key val
    │
    ▼
Master 内核 tcp_recvmsg()
    │
    ├─ fexit 路径（内核态）:
    │     fexit/tcp_recvmsg 在返回前读取 msghdr 指向的用户数据
    │       → 若 client_ctl[7]=CAPTURE_ENABLE 为 0 直接 return（无 Slave 时零开销）
    │       → bpf_ringbuf_output: [4B len][payload]
    │       → ebpf-proxy 主线程 ring_buffer__poll/consume
    │           ├─ STATE=FORWARDING 且 slave 已连接 → 转发队列（64MB，高水位 4MB）
    │           │      → 转发线程 writev → Slave 的 proxy listener(port+1)
    │           └─ STATE=BUFFERING（全量同步中）→ proxy_cache（节点打 session_id）
    │
    └─ repl_broadcast 路径（本模式下不转发，只记账）:
         parse_resp_stream → handle_parsed_command
           → repl_backlog_feed + repl_note_broadcast   ← backlog / offset 的唯一喂入点
           → repl_broadcast(raw, rawlen)
               → 遇到 repl_transport_kind == EBPF_TCP 的 replica 直接跳过（已由 proxy 转发）
```

**全量同步边界（queue_snapshot）：**

```
① g_repl_fullsync_in_progress = 1              ← 用户态停止增量下送
② client_ctl[3] = 1                            ← 通知 proxy 切 BUFFERING
③ repl_wait_ebpf_proxy_buffering(500ms)        ← 等 proxy 置 client_ctl[12]=1 确认
     proxy 此刻做的事：清空转发队列 + 丢弃上一个 session 的 proxy_cache
④ snap_base_offset = master_repl_offset        ← 确认后才取，边界干净
⑤ repl_backlog_reset(snap_base_offset)         ← backlog 以快照边界重建，历史重新连续
⑥ 生成快照 → RDMA WRITE 全量传输 → Slave load dump → Slave 回 REPLDONE

全量完成后（master 收到 REPLDONE）:
  → g_repl_fullsync_in_progress = 0
  → client_ctl[3] = 0 → proxy 切 FORWARDING 并 flush 本 session 的 proxy_cache
  → ebpf+tcp 不做 backlog 回放（proxy_cache 已经把增量送到，回放会双路重复）
```

**四层缓冲的职责边界：**

| 组件 | 容量/水位 | 定位 |
|---|---|---|
| BPF ringbuf `client_cache_ringbuf` | 64MB，高 32MB / 低 8MB | 内核 → proxy 的临时运输队列 |
| 转发队列 `g_pfwd_*` | 64MB，高 4MB / 低 1MB | proxy 主线程 → 转发线程的临时运输队列 |
| `proxy_cache` | 256MB 硬上限，192MB 高水位 | **同一 replication session 内** FULLRESYNC / 数据通道短暂故障的临时缓存 |
| `repl_backlog` | 10MB | Master 带 offset 的复制历史，用于**重新 REPLSYNC 后**的 partial resync |

一句话：ringbuf / 转发队列解决「怎么传」，`proxy_cache` 解决「这个 session 暂时传不了」，
`backlog` 解决「session 断了以后怎么续」。

**client_ctl 控制位（BPF / ebpf-proxy / master 三方共用，改键须同步三处）：**

| key | 名称 | 谁写 | 含义 |
|---|---|---|---|
| 3 | FULLSYNC_STATE | master | 1=全量同步中，proxy 切 BUFFERING |
| 4 | RINGBUF_BACKPRESSURE | BPF 高水位 / proxy 低水位清 | ringbuf 堆积 |
| 5 | PROXY_HEARTBEAT | proxy（100ms） | master 用它判断 proxy 是否存活 |
| 6 | FWDQ_BACKPRESSURE | proxy 入队/出队 | 转发队列堆积 |
| 7 | CAPTURE_ENABLE | master | 0 → BPF 最前早退，不做任何捕获 |
| 8 | SESSION_VALID | master | 当前 replication session 是否有效 |
| 9 | SESSION_ID | master | session 身份，cache 节点据此打标 |
| 10 | CACHE_INVALID | master / proxy | cache 作废，禁止 flush |
| 11 | CACHE_BACKPRESSURE | proxy | proxy_cache 超高水位 |
| 12 | PROXY_STATE | proxy | 1=BUFFERING，master 等它确认全量边界 |

（4/6/11 由 master 侧的 `repl_ebpf_backpressure()` 取或后决定是否暂停客户端读取；
key 5 的心跳用于判断 proxy 是否还活着，避免背压位陈旧导致 master 永久挂起。）

**事件处理表：**

| 事件 | capture | 转发队列 | proxy_cache | backlog | 恢复方式 |
|---|---|---|---|---|---|
| 无 Slave | OFF | 空 | 清空 | 不记录（标记不连续） | 新 Slave FULLRESYNC |
| 正常稳态 | ON | 使用 | 通常空 | 记录 | 实时转发 |
| FULLRESYNC | ON | 边界前清空 | 缓存边界后数据 | 以 snap_base 重建 | eBPF+TCP 只 flush cache |
| proxy 数据连接短断 | ON | 失败数据回 cache | 缓存 | 记录 | 同 session 重连后 flush cache |
| Slave 控制连接断 | OFF | 清空 | 作废 | 标记不连续 | 新 REPLSYNC |
| partial resync | ON | 恢复后继续 | 不用旧 cache | backlog replay | backlog |
| backlog 不连续 | ON | - | - | 不可信 | FULLRESYNC |
| proxy_cache 溢出 | — | — | 标记 session invalid | — | 上报 master → 断开 replica → 重新同步 |

**关键设计点：**

- **无 Slave 不做无用功**：`client_ctl[7]=0` 时 BPF 在读 msghdr / `bpf_probe_read_user` /
  `bpf_ringbuf_output` **之前**就返回，ringbuf 与 proxy_cache 都不会因为「没人要」而积压、
  进而通过背压拖慢正常客户端请求。
- **backlog 只在一个地方喂**：写命令成功后统一 `repl_backlog_feed` + `repl_note_broadcast`，
  `repl_broadcast()` 内部不再二次 feed，保证 `backlog_end_offset` 与 `master_repl_offset` 同步推进。
- **session 隔离**：`proxy_cache` 的节点带 `session_id`，跨 session 的节点在 flush 时直接丢弃。
  控制连接断开 → master 置 `SESSION_VALID=0` + `CACHE_INVALID=1`，proxy 立即丢弃旧 cache 且拒绝 flush。
- **只 flush 不重放**：eBPF+TCP 模式下增量只有 proxy→slave 一条路，backlog 不参与回放，
  避免同一段写被应用两次（对 INCR/DEL 这类非幂等命令是致命的）。
- **不静默丢数据**：`proxy_cache` 达到硬上限时不再丢最旧节点继续跑，而是标记 session 作废
  并上报 master；master 断开 replica 链路，Slave 重连后走 partial/full resync。
- **数据通道可自愈**：proxy 发送侧发现 EPIPE/ECONNRESET 会摘掉 fd（`proxy_slave_mark_down`），
  主循环据此重连；同 session 内重连成功后先 flush cache 再继续转发。

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

注意: eBPF+TCP 的增量转发路径不在 transport ops 框架内 —
      它由**独立的 ebpf-proxy 进程**通过 fexit/tcp_recvmsg + BPF ringbuf 捕获，
      再经自己的 TCP 连接（slave 的 master_port+1）转发。
      repl_broadcast 遇到 repl_transport_kind == EBPF_TCP 的 replica 直接跳过，
      避免与 proxy 双路送达。

Fallback 机制:
  如果 RDMA/eBPF 发送失败 → 自动 fallback 到 TCP
  kprobe forward 启动失败 → c->fwd_healthy=0 → repl_broadcast 负责该 slave
  kprobe forward 健康检查失败 → c->fwd_healthy=0 → kprobe 路径停止转发, repl_broadcast 保底
  (fwd_healthy=1 时 repl_broadcast 跳过该 slave，避免重复发送)
  ebpf-proxy 数据通道断开 → proxy 摘掉 fd 并重连；同 session 重连后先 flush cache 再继续
  ebpf-proxy 崩溃/未启动 → client_ctl map 不存在，master 读不到背压位，增量静默停止
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
// src/replication/kvs_repl.c
typedef struct repl_backlog_s {
    unsigned char *buf;              // 10MB 环形缓冲（无 Slave 时不分配）
    size_t cap;                      // 容量 (10MB)
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
| ebpf-proxy 入口       | `main()` / `main_loop()`                                      | `src/ebpf_proxy/main.c`                   |
| 传输层选择            | `repl_transport_ops_for_context()`                            | `src/replication/kvs_repl.c:1583`         |
| 自动快照定时器        | `persist_autosnap_cron()`                                     | `src/persistence/kvs_persist.c:897`       |
| TTL 过期              | `kvs_active_expire_cycle()`                                   | `src/expire/kvs_expire.c`                 |
