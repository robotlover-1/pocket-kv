# test_ebpf_proxy_qps: 统一 sync/ebpf QPS 测量口径

## 背景

`tests/perf/test_ebpf_proxy_qps.c` 测试三种模式（none/sync/ebpf）的转发 QPS。
当前 sync 和 ebpf 的 QPS 计算口径不一致：

- **sync**: 用客户端 echo QPS（`result.qps`），含 master `write_full()` 到 slave socket 缓冲区的时延，不含 slave 实际接收时间
- **ebpf**: 用 `N / (echo时间 + slave_drain时间)`，通过轮询 slave 的 `read()` 调用次数（`live_count`）判断收齐

## 问题

1. **口径不一致**：sync 用客户端视角的 echo 吞吐，ebpf 用系统视角的墙钟吞吐，不可直接对比
2. **slave 完成检测机制错误**：ebpf 用 `read()` 调用次数和 `g_req_count` 对比，但 TCP 是流协议，一次 `read()` 可能含多个请求的数据，没有一一对应关系
3. **轮询粒度过粗**：50ms 间隔，实际 drain 可能只需几 ms，误差可达 20%
4. **远程 slave 盲等**：`g_no_local_slave` 时直接 `usleep(1500000)`，完全不验证

## 设计方案

### 核心思路

统一测量口径：**从 master 启动到数据发送完毕的墙钟时间。**

"发送完毕" = TCP 连接优雅关闭（`close()` 后数据进入内核 TCP 栈，可靠送达 slave）。

### 为什么这个定义有效

两个模式的最后一段都是 TCP `write()` + `close()`：

- **sync**: `master_echo_sync` 中 `write_full(m->slave_fd, ...)` + 外部 `close(slave_fd)`
- **ebpf**: `ebpf-proxy` 内部 TCP `send()` + `proxy_stop()` / `slave_stop()` 中 `close()`

TCP 保证数据按序可靠送达——`close()` 返回后数据已在接收端内核缓冲区或网络中在途。

### 测量方式

```
t_start = now_us()
  ├── master_start()
  ├── run_qps_client()     // 客户端发 N 条并等 echo
  ├── master_stop()        // sync: 停止读取新请求
  ├── close(slave_fd)      // sync: 关闭与 slave 的 TCP 连接
  │   (ebpf: proxy_stop()  // ebpf: drain ringbuf + flush + close slave TCP)
  │   (ebpf: slave_stop()) // ebpf: 停止跨轮复用的 slave
t_end = now_us()

QPS = N / (t_end - t_start) × 1e6
```

### 具体改动

#### 1. 删除 `live_count` 轮询 ([L919-L946](tests/perf/test_ebpf_proxy_qps.c#L919-L946))

ebpf 模式中那段 50ms 粒度轮询 `*slave.live_count` 的代码全部删除。

#### 2. 删除 slave 共享内存 ([L77-L78](tests/perf/test_ebpf_proxy_qps.c#L77-L78))

`live_count` / `shm_fd` / `mmap` 及其在 `slave_process_main`、`slave_start`、`slave_stop` 中的相关代码移除。

#### 3. sync 模式 QPS 改用墙钟时间

```c
// 当前:
qps_vals[r] = result.qps;

// 改为:
double t_end = now_us();
double total_elapsed = t_end - t_wall_start;
qps_vals[r] = total_elapsed > 0 ? (double)result.completed / total_elapsed * 1e6 : 0;
```

`t_wall_start` 在 `master_start()` 之前记录。

#### 4. ebpf 模式 QPS 同样用墙钟时间

ebpf 模式中，`t_wall_start` 在 `master_start()` 前记录，`t_end` 在 `proxy_stop()` + `slave_stop()` 后记录。与步骤 3 使用完全相同的代码路径。

#### 5. 移除远程 slave 盲等

`g_no_local_slave` 路径的 `usleep(1500000)` 和 `usleep(2000000)` 删除。

### 影响范围

- 仅修改 `tests/perf/test_ebpf_proxy_qps.c` 一个文件
- 不涉及 eBPF proxy 进程、BPF 程序、slave 逻辑
- 不改变测试用法和命令行接口

### 验证标准

- [ ] `--mode all` 运行通过，三种模式均正常完成
- [ ] sync 和 ebpf 的 QPS 在同一口径下可直接对比
- [ ] 无 50ms 轮询或固定 usleep 等待
- [ ] 无 `live_count`、`shm_fd`、`mmap` 残留代码
