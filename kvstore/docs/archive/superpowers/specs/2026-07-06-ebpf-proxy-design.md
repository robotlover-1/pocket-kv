# eBPF Proxy 独立进程设计

> 日期: 2026-07-06（初版）, 2026-07-22（更新 fentry+fexit + batch send）
> 状态: 已实现，当前分支 `refactor/fentry-fexit-optimize`

## 1. 目标

将 eBPF client_capture（kprobe/tcp_recvmsg）从 kvstore master 进程中独立出来，作为单独的 `ebpf-proxy` 进程运行。同时修正全量同步完成（REPLDONE）的发送方：从 master 发送改为 slave 发送。

## 2. 背景与问题

### 2.1 当前架构问题

- eBPF 程序（sockmap、client_capture、kprobe）由 kvstore 主进程通过 libbpf 直接加载
- client_capture BPF 程序 hook `tcp_recvmsg`，捕获 master 收到的客户端写入数据
- eBPF 的生命周期与 kvstore 主进程绑定，不便于独立部署和调试

### 2.2 全量同步完成信号的设计问题

当前流程：
```
Master 发 KVSD → Master 发 REPLDONE
```

问题：master 发送完 KVSD 数据后立即发 REPLDONE，但不知道 slave 是否确实收到并完成了本地恢复。如果 slave 在接收 KVSD 过程中出错（磁盘满、数据损坏），master 已经认为全量同步完成，状态不一致。

## 3. 架构设计

### 3.1 组件

```
┌─────────────────────────── Master 机器 ──────────────────────────────────┐
│                                                                          │
│  ┌─────────────────────┐    ┌──────────────────────┐    ┌──────────────┐ │
│  │   kvstore master    │    │   ebpf-proxy 进程    │    │    slave     │ │
│  │                     │    │                      │    │              │ │
│  │  接收客户端 RESP    │    │  ringbuf poll +      │TCP │  接收增量数据│ │
│  │  处理命令           │    │  数据缓存/转发       │───→│  接收 KVSD   │ │
│  │  全量同步(KVSD)─────┼TCP─┼──────────────────────┼───→│  恢复后发    │ │
│  │                     │    │                      │    │  REPLDONE    │ │
│  │  recvmsg ◄──────────┼─kprobe─→ BPF client_capture   │              │ │
│  │  (客户端数据,       │    │ (内核态)             │    └──────────────┘ │
│  │   REPLSYNC,REPLDONE)│    │                      │                     │
│  └─────────────────────┘    └──────────────────────┘                     │
│               │                       │                                   │
│               └───────┬───────────────┘                                   │
│              /sys/fs/bpf/kvstore/ (共享 BPF maps)                        │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.2 组件职责

| 组件 | 进程 | 职责 |
|------|------|------|
| kvstore master | 主进程 | 接收客户端命令、处理业务逻辑、发送 KVSD 全量数据给 slave、解析 REPLDONE 更新 slave 状态 |
| ebpf-proxy | 独立进程 | attach kprobe 到 master tcp_recvmsg、截获增量数据转发 slave、检测 REPLSYNC/REPLDONE 管理缓存状态 |
| slave | 远端进程 | 接收增量数据 + KVSD、保存 KVSD 到本地并恢复后发送 REPLDONE |

### 3.3 通信路径

| 方向 | 机制 | 说明 |
|------|------|------|
| master → proxy（配置） | BPF maps (`proxy_cfg`, `client_ctl`) | master 将 PID、端口、slave 地址写入 pinned maps |
| BPF kernel → proxy（数据） | BPF ringbuf (`client_cache_ringbuf`) | kprobe 截获的 recvmsg 数据 |
| proxy → slave（增量） | 独立 TCP 连接 | proxy 将截获的客户端数据转发给 slave |
| master → slave（全量） | 独立 TCP/RDMA 连接 | master 直接发送 KVSD |
| slave → master（控制） | TCP | REPLSYNC/REPLDONE 经由 master recvmsg 被 eBPF 截获 |

## 4. 数据流与状态机

### 4.1 ebpf-proxy 状态机

```
        REPLSYNC 检测到
  FORWARDING ──────────→ BUFFERING
      ↑                      │
      │                      │ REPLDONE 检测到
      │                      │ (flush 缓存后)
      │                      ↓
      └──────────────────────┘
```

### 4.2 正常转发（FORWARDING 态）

```
Client → master recvmsg → BPF kprobe 抓数据 → ringbuf
                                                  ↓
                                            proxy poll
                                                  ↓
                                    [4B len][data] → proxy 直发 slave TCP
```

- BPF `kretprobe/tcp_recvmsg` 抓 iovec 数据后写 `[4B payload_len][payload]` 到 ringbuf
- proxy poll ringbuf，直接 `send(slave_fd, payload, payload_len, 0)`

### 4.3 全量同步中（BUFFERING 态）

```
Client → master recvmsg → BPF kprobe 抓 → ringbuf → proxy 挂到内存链表缓存
                                                      ↑
                                          Master → KVSD → slave (直接 TCP)
                                                      ↑
                                                proxy 不参与 KVSD 传输
```

- slave 发起全量同步前发送 REPLSYNC 给 master
- BPF kretprobe 检测到 REPLSYNC → 设置 `client_ctl[FULLSYNC_STATE] = 1`
- proxy poll 时发现状态切换 → 进入 BUFFERING，后续数据挂链表
- 缓存上限 256MB，超限时丢弃最旧节点（环形覆盖），记录丢计数

### 4.4 全量同步完成

```
Slave: KVSD 存本地 → 恢复到存储引擎 → 发 REPLDONE 给 master
         ↓
Master recvmsg ← BPF kprobe 检测到 REPLDONE
         ↓
BPF: client_ctl[FULLSYNC_STATE] = 0 + 写 magic(0xFFFFFFFF) 到 ringbuf
         ↓
Proxy: poll 到 magic → 遍历缓存链表 → 逐条 send 到 slave → 清空缓存 → FORWARDING
```

## 5. BPF Maps 设计

### 5.1 client_ctl（Array, max_entries=8）

| key | 名称 | 类型 | 说明 |
|-----|------|------|------|
| 1 | CTL_PID | u64 | master 进程 PID，0=禁用 |
| 2 | LISTEN_PORT | u64 | master 监听端口 |
| 3 | FULLSYNC_STATE | u64 | 0=正常转发, 1=全量同步中(缓存) |

Kernel 侧 BPF 负责写 key 3（检测 REPLSYNC/REPLDONE），userspace 负责读 key 3。

### 5.2 proxy_cfg（Hash, key=char[32], value=u64）

新建 map，定义在 `repl_client_capture.bpf.c` 中。虽然 BPF 内核代码不直接使用该 map，但定义在 BPF 程序中可以随 BPF 加载自动创建，无需额外 `bpf_map_create()` 调用。proxy 加载 BPF 程序时该 map 被创建并 pin，master 随后打开并写入。

| key | value 含义 |
|-----|-----------|
| "slave_addr" | slave IPv4 地址（32-bit host order） |
| "slave_port" | slave TCP 端口 |
| "master_pid" | master 进程 PID（master 启动后写入） |
| "master_port" | master 监听端口（master 启动后写入） |

master 在启动完成后打开 pinned `proxy_cfg` 写入 PID 和端口。slave 连接/配置变更时更新 slave 地址。

**时序问题处理（双向等待，支持任意启动顺序）：**

- proxy 启动后加载 BPF 并 pin maps（`proxy_cfg` 等），然后轮询 `proxy_cfg["master_pid"]` 直到非零（最长 30s）
- master 启动后用 `bpf_obj_get()` 打开已有 pinned `proxy_cfg`，如果 map 还不存在则重试（500ms 间隔，最长 30s），打开后写入配置
- master 使用 `bpf_obj_get()`（已在 `kvs_repl_ebpf.c` 中使用），不引入 `bpf_map_create()` 新依赖
- 结果：两个进程可以任意顺序启动，先启动的等后启动的

### 5.3 client_stats（Array, max_entries=16）

| key | 名称 | 说明 |
|-----|------|------|
| 0 | HIT | 总命中次数 |
| 1 | SKIP_PID | PID 不匹配跳过 |
| 2 | RB_ERR | ringbuf 写入错误 |
| 3 | DATA_OVR | 数据超过上限 |
| 4 | READ_ERR | probe_read 失败 |
| 5 | CACHED | 缓存条目数（全量同步期间） |
| 6 | RETPROBE_MISS | kretprobe 未找到 entry 保存的 msg 指针 |
| 7 | REPLDONE_DETECT | 检测到 REPLDONE |
| 8 | REPLSYNC_DETECT | 检测到 REPLSYNC（新增） |
| 9 | CACHE_DROPPED | 缓存满丢条目数（新增） |
| 10 | CACHE_MAX_BYTES | 缓存峰值字节数（新增） |

### 5.4 client_cache_ringbuf（Ringbuf, 4MB）

不变，kprobe 截获的数据写入此 ringbuf。

## 6. BPF 程序改动

文件：[src/replication/bpf/repl_client_capture.bpf.c](src/replication/bpf/repl_client_capture.bpf.c)

### 6.1 当前实现：fentry+fexit（kernel 6.1 trampoline）

kprobe/kretprobe 已替换为 fentry+fexit：

```c
// fentry/tcp_recvmsg: 保存 msg_ptr + msg_iter.count
SEC("fentry/tcp_recvmsg")
int fentry_tcp_recvmsg(__u64 *ctx) {
    // PID 过滤 (client_ctl[1])
    unsigned long msg_ptr = ctx[1];  // ctx[0]=sk, ctx[1]=msg
    // 读取 iov_iter head @ msg+32
    struct iov_head head;
    bpf_probe_read_kernel(&head, sizeof(head), (void *)(msg_ptr + 32));

    __u64 tid_key = bpf_get_current_pid_tgid();  // HASH key
    struct fexit_ctx e = { .msg_ptr = msg_ptr, .count_before = head._count };
    bpf_map_update_elem(&client_fexit_ctx, &tid_key, &e, BPF_ANY);
}

// fexit/tcp_recvmsg: delta 计算 retval，读数据写 ringbuf
SEC("fexit/tcp_recvmsg")
int fexit_tcp_recvmsg(__u64 *ctx) {
    __u64 tid_key = bpf_get_current_pid_tgid();
    struct fexit_ctx *ec = bpf_map_lookup_elem(&client_fexit_ctx, &tid_key);
    if (!ec || !ec->msg_ptr) goto out;

    retval = count_before - count_after;  // delta 计算实际接收字节数
    bpf_probe_read_user(entry + 4, data_len, user_ptr);
    bpf_ringbuf_output(&client_cache_ringbuf, entry, 4 + data_len, 0);
out:
    bpf_map_delete_elem(&client_fexit_ctx, &tid_key);
}
```

**为什么用 HASH map `client_fexit_ctx`**：kernel 6.1 PREEMPT_DYNAMIC 下 `tcp_recvmsg` 可能在 fentry 和 fexit 之间迁移 CPU。PERCPU_ARRAY 会导致 fexit 读到错误 slot，数据丢失 ~45%。HASH map key=`bpf_get_current_pid_tgid()` 不受 CPU 迁移影响，fexit 消费后 delete。

### 6.2 REPLSYNC/REPLDONE 检测

已移到 ebpf-proxy 用户态 ringbuf 回调中处理，减少 BPF 热路径开销。

## 7. ebpf-proxy 进程设计

### 7.1 命令行

```
ebpf-proxy --pin-path /sys/fs/bpf/kvstore_repl_sockmap
```

### 7.2 启动流程

1. 解析命令行参数（`--pin-path`、`--obj-path`）
2. `setrlimit(RLIMIT_MEMLOCK, RLIM_INFINITY)`
3. 加载 BPF 程序（`repl_client_capture.bpf.o`）→ maps 被创建
4. Pin maps 到 `--pin-path` 目录（`client_ctl`、`proxy_cfg`、`client_stats`、`client_cache_ringbuf`）
5. 轮询 `proxy_cfg["master_pid"]` 等待 master 写入配置（500ms 间隔，最长 30s，超时则退出报错）
6. 从 `proxy_cfg` 读取 master PID、master 端口、slave 地址
7. 将 master PID 写入 `client_ctl[1]`，master 端口写入 `client_ctl[2]`
8. Attach kprobe + kretprobe to `tcp_recvmsg`（通过 PID 过滤）
9. 连接 slave TCP（重试直到成功，指数退避初始 100ms/最大 5s）
10. 进入 `ring_buffer__poll` 主循环

**任意启动顺序支持：**
- proxy 先于 master 启动：步骤 5 等待 master 写入配置
- master 先于 proxy 启动：master 用 `bpf_obj_get()` 重试打开 pinned map（等待步骤 3-4 完成），打开后立即写入，proxy 在步骤 5 立刻读到
- master 重启：master 重新 `bpf_obj_get()` 打开已有 pinned map，写入新配置，proxy 主循环中检测到变化后更新

### 7.3 ringbuf 回调 + batch writev

数据不逐条 `send()`，改为攒批 `writev` (BATCH_MAX=64)：

```c
static struct iovec g_batch_iov[BATCH_MAX];
static int g_batch_count = 0;

void ringbuf_callback(void *data, size_t len) {
    uint32_t payload_len = *(uint32_t*)data;
    unsigned char *payload = (unsigned char*)data + 4;

    if (payload_len == 0xFFFFFFFF) {  // magic: REPLDONE flush signal
        batch_flush();
        flush_cache_to_slave();
        state = FORWARDING;
        return;
    }

    // 过滤 REPLSYNC/REPLACK/REPLDONE（首字节非 RESP 命令字符）
    if (is_repl_control_payload(payload, payload_len)) return;

    if (state == FORWARDING && proxy_slave_is_connected(&g_slave)) {
        g_batch_iov[g_batch_count++] = (struct iovec){payload, payload_len};
        if (g_batch_count >= BATCH_MAX) batch_flush();
    } else {
        append_to_cache(payload, payload_len);
    }
}

static void batch_flush(void) {
    if (g_batch_count == 0) return;
    writev(slave_fd, g_batch_iov, g_batch_count);  // 单次系统调用
    g_batch_count = 0;
}
```

### 7.4 缓存实现

- 单链表结构：`{struct cache_node *next; size_t len; unsigned char data[];}`
- 总大小上限 256MB
- 达到上限时从 head 丢弃最旧节点，`stats.dropped++`
- `flush_cache_to_slave()`：从 head 遍历，逐条 `send()`，成功后释放

### 7.5 主循环

```c
for (;;) {
    ring_buffer__poll(rb, 1 /* ms, 原 100ms */);

    batch_flush();  // 每次 poll 后 flush 批

    // 检查 fullsync_state 是否变化
    int fs = read_client_ctl(FULLSYNC_STATE);
    if (fs == 1 && state == FORWARDING) state = BUFFERING;
    if (fs == 0 && state == BUFFERING) { flush_cache_to_slave(); state = FORWARDING; }

    // 检查 slave 连接健康 + 重连
    if (slave_disconnected()) reconnect_slave();

    // 如果 FORWARDING 且 slave 在线且有缓存数据，尝试 flush
    if (state == FORWARDING && slave_connected() && cache_has_data())
        flush_cache_to_slave();

    if (shutdown_requested) goto cleanup;
}
```

### 7.6 Slave 重连策略

指数退避：初始 100ms → 200ms → 400ms → ... → 最大 5s。连接成功后重置退避计数器。

### 7.7 退出清理（SIGINT/SIGTERM）

1. 设置 `shutdown_requested = 1`
2. detach kprobe + kretprobe
3. 如果 state == BUFFERING：尝试 flush 剩余缓存到 slave（best effort，超时 3s）
4. 关闭 slave TCP 连接
5. 关闭所有 BPF map fds
6. 退出

## 8. Master 侧改动

### 8.1 删除

- `repl_ebpf_init()` 调用及相关 sockmap/kprobe 初始化逻辑（[kvstore.c:2373-2400](src/main/kvstore.c#L2373-L2400)）
- `kvs_repl_kprobe.c` 中 master 侧 kprobe 加载和 ringbuf poll 逻辑
- `queue_snapshot()` 末尾发送 REPLDONE 的逻辑（如果存在）

### 8.2 新增

- master 启动后用 `bpf_obj_get()` 打开 pinned `proxy_cfg` map（如不存在则重试，500ms 间隔最长 30s），写入 `master_pid`（自己的 PID）和 `master_port`（监听端口）
- slave 连接时，将 slave 地址写入 `proxy_cfg` map 的 `slave_addr` 和 `slave_port`
- `handle_parsed_command` 中新增 REPLDONE 响应：

```c
if (!strcasecmp(cmd, "REPLDONE")) {
    repl_on_slave_repldone(conn);  // 更新 slave 全量同步完成状态
    return resp_simple_string(response, cap, "OK");
}
```

### 8.4 slave 端发送 REPLSYNC

slave 建立连接后发送 REPLSYNC 请求全量同步。该命令经由 master 的 tcp_recvmsg 被 eBPF kprobe 截获，触发 BUFFERING 模式。现有的 REPLSYNC 发送逻辑（在 `slave_thread` 中）保持不变。

### 8.5 保留

- master 自己的 KVSD 发送逻辑不变
- master 自己的 TCP/RDMA 传输层代码不变

## 9. Slave 侧改动

### 9.1 全量同步完成流程修正

**当前错误流程：**
```
Master 发 KVSD → Master 发 REPLDONE
```

**修正后流程：**

1. Slave 发送 REPLSYNC 给 master
2. Master 开始发送 KVSD 给 slave（直接 TCP/RDMA）
3. Slave 接收 KVSD 写入临时文件（`.fullsync.recv.tmp`）
4. Slave 收到全部 KVSD → `fsync` → 关闭临时文件
5. Slave 重命名为正式 dump 文件
6. Slave 调用 `replay_dump_file()` 恢复到存储引擎
7. Slave 清理状态：`g_slave_loading_fullsync = 0`
8. **Slave 发送 REPLDONE 给 master** ← 关键改动
9. eBPF proxy 在 master recvmsg 中截获 REPLDONE → flush 缓存
10. Master 收到 REPLDONE → 标记 slave 同步完成

### 9.2 代码改动

`repl_slave_finish_fullsync()` 中，最后一步改为发送 REPLDONE：

```c
void repl_slave_finish_fullsync(void) {
    // ... KVSD 文件处理（不变）...
    // ... replay_dump_file（不变）...
    // ... repl_slave_state_save（不变）...
    repl_slave_send_repldone();  // 发送 *1\r\n$8\r\nREPLDONE\r\n
}
```

`repl_slave_send_repldone()` 通过 slave 到 master 的 TCP 连接发送 REPLDONE 命令。

## 10. 文件结构

```
src/
├── ebpf_proxy/                    # 新增：独立 eBPF proxy 进程
│   ├── main.c                     # 入口 + 命令行解析 + 主循环
│   ├── proxy_ringbuf.c            # ringbuf 回调 + 缓存/转发逻辑
│   ├── proxy_slave.c              # slave 连接管理 + 重连
│   └── Makefile                   # proxy 编译规则
├── replication/
│   ├── bpf/
│   │   └── repl_client_capture.bpf.c  # 修改：移除 sendmsg probe，新增 REPLSYNC 检测
│   ├── kvs_repl.c                     # 修改：repl_slave_finish_fullsync 发 REPLDONE
│   ├── kvs_repl_ebpf.c                # 删除/简化（不再加载 BPF 程序）
│   └── kvs_repl_kprobe.c              # 删除 master 侧 kprobe 加载代码
├── main/
│   └── kvstore.c                      # 修改：删除 eBPF/kprobe 初始化、新增 REPLDONE handler、写 proxy_cfg
└── include/
    └── kvstore/
        ├── kvstore.h                  # 不变
        └── replication/
            └── repl_kprobe.h          # 简化
```

根目录 `Makefile` 新增 `ebpf-proxy` 目标。

## 11. 构建

```makefile
# 新增 targets
EBPF_PROXY_SRC = src/ebpf_proxy/main.c src/ebpf_proxy/proxy_ringbuf.c src/ebpf_proxy/proxy_slave.c
EBPF_PROXY_OBJ = build/ebpf_proxy

ebpf-proxy: $(EBPF_PROXY_SRC)
	$(CC) $(CFLAGS) -o $(EBPF_PROXY_OBJ) $(EBPF_PROXY_SRC) $(LDFLAGS) -lbpf -lelf -lz

client_capture_bpf: src/replication/bpf/repl_client_capture.bpf.c
	clang -O2 -target bpf -g -c -o build/replication/bpf/repl_client_capture.bpf.o $<
```

## 12. 测试策略

### 12.1 单元测试

- 缓存链表：追加、flush、上限丢弃
- 重连退避算法

### 12.2 集成测试

- 启动 master → 启动 ebpf-proxy → slave 连入 → 客户端写入 → 验证 slave 收到数据
- 全量同步场景：写入一批数据 → slave 发起全量同步 → 全量同步期间继续写入 → 验证 REPLDONE 后增量数据完整
- 缓存上限触发：大量写入 → 验证丢弃行为和数据一致性
- ebpf-proxy 退出清理：验证 kprobe detach、缓存 flush、资源释放

### 12.3 现有测试回归

- `tests/test_repl_basic.c`
- `tests/test_repl_gap.c`

## 13. QPS 测试方法

测试程序：`tests/perf/test_ebpf_proxy_qps`

### 13.1 三种模式

| 模式 | master 行为 | 转发路径 |
|------|------------|---------|
| none | read(client) → echo | 不转发 |
| sync | read(client) → echo → write(slave_fd) | 同步转发，在 master 热路径 |
| ebpf | read(client) → echo | proxy 异步转发，BPF fentry+fexit 截获 |

### 13.2 QPS 测量口径

```
t_start = now_us()
  master_start()             # pthread, bind, listen
  run_qps_client()           # fork client: warmup + N 次 write/read
  master_stop()              # pthread_join
  [ebpf] ringbuf drain 检测  # 临时 ring_buffer reader poll
t_end = now_us()

wall_qps = N / (t_end - t_start) × 1e6
```

sync: `write(slave_fd)` 在 master 线程内同步完成。
ebpf: `ebpf_wait_ringbuf_drain()` 创建临时 ring_buffer reader，poll 直到 ringbuf 空。此时 proxy 最后一个 `writev` 已完成，数据到达 slave TCP 发送缓冲。**两者 t_end 等价。**

### 13.3 跨机测试

支持 `--master-host`、`--slave-host`、`--client-only`、`--no-client` 参数，可拆分部署：

```
Master+Proxy (128)              Slave+Client (129)
./test --mode ebpf              ./slave_receiver 15901 &
  --no-client                    ./test --client-only
  --no-local-slave                 --master-host 192.168.233.128
  --slave-host 192.168.233.129
```

### 13.4 当前跨机结果（client+slave on 129, 64B, 3 runs median）

| mode | QPS |
|------|-----|
| sync | 2,873 |
| ebpf | 2,896 |

4KB payload 时 ebpf 反超 sync 37%（3365 vs 2462）。详见 `docs/ebpf-forwarding-optimization-journey.md`。

## 14. 风险与未决事项

| 风险 | 缓解措施 |
|------|---------|
| fentry+fexit 间 CPU 迁移丢数据 | HASH map key=pid_tgid 替代 PERCPU_ARRAY |
| proxy 缓存 flush 期间 slave 断连 | best effort send，失败后丢弃缓存，记录 stats |
| master 和 proxy 竞争 BPF maps | client_ctl key 3 只由 BPF 程序写入，userspace 只读 |
| REPLSYNC/REPLDONE 跨 TCP segment 分片 | 用户态 ringbuf 回调中扫描匹配 |
