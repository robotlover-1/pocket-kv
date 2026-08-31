# 统一 sync/ebpf QPS 测量口径 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 统一 sync 和 ebpf 模式的 QPS 测量口径为"从 master 启动到数据发送完毕的墙钟时间"

**Architecture:** 删除 slave 共享内存（live_count/mmapp），删除 live_count 轮询逻辑，sync 和 ebpf 模式使用同一段 QPS 计算代码（`N / (t_end - t_start)`）。sync 以 `close(slave_fd)` 为终点，ebpf 以 ringbuf drain 等待后为终点

**Tech Stack:** C, pthread, TCP sockets, eBPF (不改动)

## Global Constraints

- 仅修改 `tests/perf/test_ebpf_proxy_qps.c`
- 不改变测试命令行接口
- 不改变 ebpf-proxy 进程、BPF 程序逻辑
- 不改变 slave 进程的核心逻辑（仅移除 live_count 引用）

---

### Task 1: 移除 slave_ctx_t 的 live_count 字段及所有引用

**Files:**
- Modify: `tests/perf/test_ebpf_proxy_qps.c:70-79,84-140,142-184,186-225`

**Interfaces:**
- Produces: `slave_ctx_t` 不再包含 `live_count` 和 `shm_fd`；`slave_process_main` 不再接受 `live_count` 参数；`slave_start`/`slave_stop` 不再创建/销毁共享内存

- [ ] **Step 1: 修改 `slave_ctx_t`，移除 `live_count` 和 `shm_fd`**

```c
// 当前 (L70-79):
typedef struct {
    int port;
    pid_t pid;
    int pipe_fd;
    int msg_count;
    long long total_bytes;
    volatile int *live_count;
    int shm_fd;
} slave_ctx_t;

// 改为:
typedef struct {
    int port;
    pid_t pid;
    int pipe_fd;
    int msg_count;
    long long total_bytes;
} slave_ctx_t;
```

- [ ] **Step 2: 修改 `slave_process_main`，移除 `live_count` 参数**

```c
// 当前 (L84):
static void slave_process_main(int port, int pipe_wr, volatile int *live_count) {

// 改为:
static void slave_process_main(int port, int pipe_wr) {
```

```c
// 当前 (L88):
    if (live_count) *live_count = 0;

// 改为: 删除该行
```

```c
// 当前 (L127):
            if (live_count) *live_count = msg_count;

// 改为: 删除该行
```

- [ ] **Step 3: 修改 `slave_start`，移除 mmap 和 live_count 传递**

```c
// 当前 (L146-152): 删除以下 7 行
    /* 共享内存 — 让父进程实时轮询 slave 的 msg_count */
    s->shm_fd = -1;
    s->live_count = mmap(NULL, sizeof(int), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (s->live_count == MAP_FAILED) {
        perror("slave mmap"); s->live_count = NULL;
    }
```

```c
// 当前 (L165):
        slave_process_main(port, pipe_fds[1], s->live_count);

// 改为:
        slave_process_main(port, pipe_fds[1]);
```

- [ ] **Step 4: 修改 `slave_stop`，移除 munmap**

```c
// 当前 (L220-223): 删除以下 4 行
    if (s->live_count && s->live_count != MAP_FAILED) {
        munmap((void *)s->live_count, sizeof(int));
        s->live_count = NULL;
    }
```

- [ ] **Step 5: 构建验证**

```bash
cd tests/perf && make test_ebpf_proxy_qps 2>&1
```

预期: 编译通过，无 `live_count`、`shm_fd`、`mmap`、`munmap` 相关错误

---

### Task 2: 统一 per-round QPS 测量为墙钟时间

**Files:**
- Modify: `tests/perf/test_ebpf_proxy_qps.c:910-951`

**Interfaces:**
- Consumes: Task 1 清理后的代码
- Produces: sync 和 ebpf 模式使用相同的 `N / (t_end - t_start)` 公式

- [ ] **Step 1: 替换 per-round QPS 计算逻辑**

```c
// 当前 (L910-951):
            double t_wall_start = now_us();
            if (master_start(&master, MASTER_PORT, slave_fd, thread_func) == 0) {
                qps_result_t result = run_qps_client();
                master_stop(&master);

                /* ebpf 模式: ... live_count 轮询 ... */
                if (use_ebpf && !g_no_local_slave && slave.live_count) {
                    int target = result.completed > 0 ? result.completed : g_req_count;
                    int last = 0, stale = 0;
                    for (int w = 0; w < 500; w++) {
                        int cur = *slave.live_count;
                        if (cur >= target) break;
                        if (cur == last) {
                            stale++;
                            if (stale > 40) break;  /* 2s 无进展则超时 */
                        } else {
                            stale = 0;
                            last = cur;
                        }
                        usleep(50000);  /* 50ms */
                    }
                    double t_wall_end = now_us();
                    double total_elapsed = t_wall_end - t_wall_start;
                    qps_vals[r] = total_elapsed > 0
                        ? (double)target / total_elapsed * 1e6 : 0;
                    fprintf(stderr, "[ebpf  ] round %d: echo_qps=%.0f "
                            "slave_msgs=%d/%d drain+echo_qps=%.0f "
                            "elapsed=%.0fus\n",
                            r + 1, result.qps,
                            *slave.live_count, target, qps_vals[r],
                            total_elapsed);
                } else {
                    qps_vals[r] = result.qps;
                }
                completed++;
            }

// 改为:
            double t_wall_start = now_us();
            if (master_start(&master, MASTER_PORT, slave_fd, thread_func) == 0) {
                qps_result_t result = run_qps_client();
                master_stop(&master);

                /* ebpf: 等 ringbuf drain，然后 proxy 继续 poll + forward */
                if (use_ebpf) {
                    if (!g_no_local_slave)
                        usleep(200000);   // 本地: ringbuf drain（poll ~100ms × 2）
                    else
                        usleep(500000);   // 远程: 额外网络延迟
                }

                double t_wall_end = now_us();
                double total_elapsed = t_wall_end - t_wall_start;
                int n_done = result.completed > 0 ? result.completed : g_req_count;
                qps_vals[r] = total_elapsed > 0
                    ? (double)n_done / total_elapsed * 1e6 : 0;
                completed++;

                fprintf(stderr, "[%-7s] round %d: echo_qps=%.0f wall_qps=%.0f "
                        "elapsed=%.0fus\n",
                        modes[i], r + 1, result.qps, qps_vals[r], total_elapsed);
            }
```

**注意**: sync 模式的 `close(slave_fd)` 保持在原位 (L952)，在 `t_wall_end` 之后执行，避免 fd 泄漏（`master_start` 失败时仍需关闭）。`close()` 是瞬时操作（仅排队 FIN），不影响 QPS 精度。

- [ ] **Step 2: 删除盲等 sleep**

```c
// 当前 (L968-971): 删除以下 4 行
            /* ebpf: 等待 proxy drain（远程 slave 仍需要固定等待） */
            if (use_ebpf && g_no_local_slave)
                usleep(1500000);
            /* 本地 slave: 已通过 live_count 轮询确认收齐，无需固定等待 */
```

ebpf drain 等待已在 Step 1 中统一处理。

- [ ] **Step 4: 构建验证**

```bash
cd tests/perf && make test_ebpf_proxy_qps 2>&1
```

预期: 编译通过，无 warning

---

### Task 3: 移除 main() 中的 live_count 残留引用

**Files:**
- Modify: `tests/perf/test_ebpf_proxy_qps.c:849-889,960-966,979-987`

**Interfaces:**
- Consumes: Task 1、Task 2 清理后的代码

- [ ] **Step 1: 移除 ebpf_slave 注释和 slave 跨轮复用中的 live_count 引用**

```c
// 当前 (L845-849): 注释提到 live_count 但与此无关，保留注释
// 当前 (L888): 注释 // 复用跨轮 slave，共享其 live_count
// 改为:
                slave = ebpf_slave;  /* 复用跨轮 slave */
```

- [ ] **Step 2: 简化 slave 跨轮复用时的日志输出**

```c
// 当前 (L960-966):
            } else if (slave_is_shared) {
                /* ebpf 跨轮 slave: 只打印，不停止 */
                if (r == 0 || r == g_rounds - 1)
                    fprintf(stderr, "[%-7s] round %d slave live_count=%d\n",
                            modes[i], r + 1,
                            ebpf_slave.live_count ? *ebpf_slave.live_count : -1);
            }

// 改为:
            } else if (slave_is_shared) {
                /* ebpf 跨轮 slave: 只打印，不停止 */
                if (r == 0 || r == g_rounds - 1)
                    fprintf(stderr, "[%-7s] round %d slave shared (pid=%d)\n",
                            modes[i], r + 1, ebpf_slave.pid);
            }
```

- [ ] **Step 3: 简化 ebpf 模式结束时的 slave 日志输出**

```c
// 当前 (L982-986):
        if (use_ebpf && !g_no_local_slave) {
            fprintf(stderr, "[%-7s] final slave: live_count=%d\n",
                    modes[i],
                    ebpf_slave.live_count ? *ebpf_slave.live_count : -1);
            slave_stop(&ebpf_slave);
        }

// 改为:
        if (use_ebpf && !g_no_local_slave) {
            fprintf(stderr, "[%-7s] final slave: msgs=%d bytes=%lld\n",
                    modes[i], ebpf_slave.msg_count, ebpf_slave.total_bytes);
            slave_stop(&ebpf_slave);
        }
```

- [ ] **Step 4: 构建验证**

```bash
cd tests/perf && make test_ebpf_proxy_qps 2>&1
```

预期: 编译通过。`grep -n "live_count" tests/perf/test_ebpf_proxy_qps.c` 无输出

---

### Task 4: 清理 run_one_mode（仅 warmup 用）中的废弃逻辑

**Files:**
- Modify: `tests/perf/test_ebpf_proxy_qps.c:560-682`

**Interfaces:**
- Consumes: Task 1、Task 2 清理后的代码
- Note: `run_one_mode` 仅在 `system_warmup()` 中以 mode="none" 调用，ebpf/sync 分支永不到达

- [ ] **Step 1: 移除 run_one_mode 中永远不会执行的 ebpf drain 代码**

`run_one_mode` 当前只在 `system_warmup` (L737) 以 `mode="none"` 调用，所以 `use_ebpf` 和 `use_sync` 始终为 0。保留函数签名不变（system_warmup 仍需要它），但删除 ebpf drain 的死代码：

```c
// 删除 L656-676 的整个 if (use_ebpf) { ... } 块
// 包括 usleep(1000000) 和 usleep(2000000)
```

```c
// 同时删除 L678-679 的无用注释:
    /* 注意：slave 统计在 slave_stop() 中读取（main 中调用），此处不读 */
    (void)out_slave_msgs;
```

- [ ] **Step 2: 构建验证**

```bash
cd tests/perf && make test_ebpf_proxy_qps 2>&1
```

预期: 编译通过

---

### Task 5: 编译验证 + 运行检查

**Files:**
- Test: `tests/perf/test_ebpf_proxy_qps` (已编译的二进制)

- [ ] **Step 1: 确认所有 live_count 残留已清理**

```bash
grep -n "live_count\|shm_fd\|MAP_FAILED\|munmap.*live\|mmap.*live" \
  /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf/test_ebpf_proxy_qps.c
```

预期: 无输出（所有 live_count 引用已移除）

- [ ] **Step 2: 完整构建**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore && make -j$(nproc) 2>&1 | tail -20
```

预期: 编译通过，无错误

- [ ] **Step 3: 运行测试（需 sudo，不实际执行，说明即可）**

```bash
sudo ./tests/perf/test_ebpf_proxy_qps --mode all --payload 64 --count 10000 --rounds 3
```

预期行为:
- none/sync/ebpf 三种模式均正常完成
- 日志中无 `live_count` 字样
- 每轮输出 `wall_qps` 和 `echo_qps`
- sync 和 ebpf 使用相同测量公式
