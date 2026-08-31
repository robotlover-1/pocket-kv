# Kprobe 主从转发性能优化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 优化 `kprobe_capture.bpf.c` 中 BPF 程序的 hook 开销和数据拷贝路径，将 kprobe 模式 QPS 从 sync 的 56%~79% 提升到 95%+。

**Architecture:** 三阶段渐进优化——阶段一调参+合并 helper 调用（低风险），阶段二换 fentry/fexit 机制+消除 BPF 内部拷贝（中风险），阶段三尝试从内核 sk_buff 直读数据（高风险）。每阶段完成后跑 benchmark 验证。

**Tech Stack:** C, BPF (libbpf), clang (BPF target), Linux 5.15, bpftool

## Global Constraints

- Linux 内核 5.15+（fentry/fexit 需要 trampoline 支持）
- CONFIG_DEBUG_INFO_BTF=y（vmlinux.h 生成和 BPF CO-RE 需要）
- libbpf ≥0.5.0（测试环境当前使用版本）
- 所有改动只影响 `tests/perf/` 下文件，不修改生产代码
- 每阶段完成后必须跑全量 benchmark 验证，QPS 下降 >5% 则回退
- 遵守项目 CLAUDE.md 规范：最小改动、保持代码风格、不引入不必要依赖

---

### Task 1: 调整 ringbuf/poll/SNDBUF 参数（方案 2）

**Files:**
- Modify: `tests/perf/kprobe_capture.bpf.c:49-50`
- Modify: `tests/perf/test_kprobe_repl_qps.c:145`
- Modify: `tests/perf/test_kprobe_repl_qps.c:452`

**Interfaces:**
- Consumes: 无
- Produces: 增大后的 ringbuf (4MB)、缩短的 poll 超时 (5ms)、增大的 slave SNDBUF (256KB)

- [ ] **Step 1: 增大 ringbuf 从 1MB 到 4MB**

在 `tests/perf/kprobe_capture.bpf.c` 第 50 行，把 `1 << 20` 改为 `1 << 22`：

```c
// 改前:
    __uint(max_entries, 1 << 20); /* 1MB */
// 改后:
    __uint(max_entries, 1 << 22); /* 4MB */
```

- [ ] **Step 2: 缩短 ringbuf poll 超时从 100ms 到 5ms**

在 `tests/perf/test_kprobe_repl_qps.c` 第 145 行：

```c
// 改前:
        int n = ring_buffer__poll(rb, 100);
// 改后:
        int n = ring_buffer__poll(rb, 5);
```

- [ ] **Step 3: 增大 slave SO_SNDBUF 从 64KB 到 256KB**

在 `tests/perf/test_kprobe_repl_qps.c` 第 452 行：

```c
// 改前:
                int sndbuf = 65536; /* 64KB */
// 改后:
                int sndbuf = 262144; /* 256KB */
```

- [ ] **Step 4: 编译 BPF 对象和测试程序**

```bash
cd tests/perf
make -f Makefile.perf clean
make -f Makefile.perf all
```

预期：编译无错误。

- [ ] **Step 5: 提交**

```bash
git add tests/perf/kprobe_capture.bpf.c tests/perf/test_kprobe_repl_qps.c
git commit -m "perf(kprobe): enlarge ringbuf 1MB→4MB, poll 100ms→5ms, SNDBUF 64KB→256KB"
```

---

### Task 2: 合并 bpf_probe_read_kernel 调用（方案 4）

**Files:**
- Modify: `tests/perf/kprobe_capture.bpf.c:86-113`（`read_iov_data` 函数）

**Interfaces:**
- Consumes: `read_iov_data` 的调用者 `kp_recv_return`（行 164）
- Produces: 改造后的 `read_iov_data`，签名不变，内部从 3 次 kernel probe read 减少到 2 次

- [ ] **Step 1: 重写 `read_iov_data` 函数**

在 `tests/perf/kprobe_capture.bpf.c`，替换第 86-114 行的 `read_iov_data` 函数：

```c
/* 从 msghdr→iov_iter 读取 iov 数据和 nr_segs（合并为一次 bpf_probe_read_kernel）
 * iov_iter 在 msghdr 内偏移 16 字节 (msg_name 8 + msg_namelen 4 + pad 4)
 * iov 指针在 iov_iter 内偏移 24 字节 (iter_type 1 + nofault 1 + data_source 1 + pad 5 + iov_offset 8 + count 8)
 * nr_segs 紧接在 iov 之后，偏移 32 字节
 * iov 和 nr_segs 在 msg+40 处相邻 16 字节，可一次读出 */
static __always_inline int read_iov_data(unsigned long msg_ptr,
    unsigned char *buf, int max_len)
{
    /* 1. 合并读取 iov 指针 + nr_segs（16 字节从 msg_ptr + 40） */
    struct { unsigned long iov_ptr; unsigned long _nr_segs; } head;
    if (bpf_probe_read_kernel(&head, sizeof(head),
            (const void *)(msg_ptr + 40)) != 0)
        return 0;
    if (!head.iov_ptr || head._nr_segs == 0) return 0;

    /* 2. 读取第一个 iovec */
    struct { unsigned long base; unsigned long len; } vec;
    if (bpf_probe_read_kernel(&vec, sizeof(vec),
            (const void *)head.iov_ptr) != 0)
        return 0;
    if (!vec.base || vec.len == 0) return 0;

    /* 3. 限制大小 */
    unsigned long long safe_len = vec.len;
    if (safe_len > (unsigned long long)max_len)
        safe_len = (unsigned long long)max_len;

    /* 4. 从用户空间读取实际数据 */
    if (bpf_probe_read_user(buf, (__u32)safe_len,
            (const void *)(unsigned long)vec.base) != 0)
        return 0;
    return (int)safe_len;
}
```

关键变化：原来的步骤 1+2（分别读 iov 和 nr_segs）合并为一次 `bpf_probe_read_kernel(&head, 16, ...)`，从 3 次 kernel probe read 降到 2 次。

- [ ] **Step 2: 编译 BPF 对象**

```bash
cd tests/perf
make -f Makefile.perf kprobe_capture.bpf.o
```

预期：clang 编译无错误。如果 BPF verifier 拒绝（`bpf_object__load` 失败），检查 `head` 结构体大小是否为 16 字节、偏移是否正确。

- [ ] **Step 3: 验证：运行 kprobe 模式 smoke test**

```bash
cd tests/perf
sudo ./test_kprobe_repl_qps --mode kprobe --payload 64 --count 5000
```

预期：无崩溃，kprobe QPS > 0，fwd count > 0。如果 kprobe 模式无法启动（BPF load 失败），打印 `dmesg | tail -20` 检查 verifier 日志。

- [ ] **Step 4: 提交**

```bash
git add tests/perf/kprobe_capture.bpf.c
git commit -m "perf(kprobe): merge bpf_probe_read_kernel calls, 3→2 per recv"
```

---

### Task 3: 阶段一 benchmark 验证

**Files:**
- 无修改（只运行测试）

**Interfaces:**
- Consumes: Task 1 + Task 2 的修改
- Produces: 阶段一 benchmark 结果

- [ ] **Step 1: 编译所有测试**

```bash
cd tests/perf
make -f Makefile.perf clean && make -f Makefile.perf all
```

预期：编译无错误。

- [ ] **Step 2: 运行全量 benchmark**

```bash
cd tests/perf
for p in 64 128 256 512 1024 2048 4096; do
    echo "=== payload=$p ==="
    sudo ./test_kprobe_repl_qps --mode all --payload $p --count 10000
    echo ""
done
```

- [ ] **Step 3: 检查结果**

对比阶段一目标：
- kprobe vs sync 比例是否达到 **65%~85%**（当前 56%~79%）
- kprobe fwd 丢失率是否明显下降（当前大 payload 丢 >50%）

如果任一 payload 的 kprobe QPS 比优化前下降 >5%，回退对应 task。

- [ ] **Step 4: 记录结果**

把 benchmark 输出追加到 commit message 或保存到 `benchmarks/data/phase1-bench.txt`。

- [ ] **Step 5: 提交（如结果达标）**

```bash
git add benchmarks/data/phase1-bench.txt  # 如果有
git commit -m "bench(kprobe): phase 1 benchmark results after param+probe_read merge"
```

---

### Task 4: 切换到 fentry/fexit（方案 1）

**Files:**
- Modify: `tests/perf/kprobe_capture.bpf.c`（整个文件）

**Interfaces:**
- Consumes: Task 2 的 `read_iov_data`（保持调用）
- Produces: 用 fentry/fexit 实现的捕获逻辑，替代 kprobe/kretprobe
  - `fentry_recv(ctx)` — 入口：保存 msg 指针到 entry_msg
  - `fexit_recv(ctx)` — 出口：读 iov 数据 → ringbuf

- [ ] **Step 1: 添加 `bpf_tracing.h` include**

在 `tests/perf/kprobe_capture.bpf.c` 第 14 行后添加：

```c
#include <bpf/bpf_tracing.h>
```

这提供 `BPF_PROG` 宏用于 fentry/fexit 程序的类型化参数访问。注意：如果 `bpf_tracing.h` 在当前 libbpf 版本中不可用，回退使用 `PT_REGS_PARM` 宏——但 fentry/fexit 的 SEC 名称仍使用新格式。

- [ ] **Step 2: 替换 entry hook：kprobe → fentry**

替换第 117-138 行的 `kp_recv_entry` 函数：

```c
/* ──── fentry: 在 tcp_recvmsg 入口保存 msg 指针 ──── */
SEC("fentry/tcp_recvmsg")
int BPF_PROG(fentry_recv, struct sock *sk, struct msghdr *msg, size_t len)
{
    __u64 enabled = ctl_get(CTL_ENABLED);
    if (!enabled) return 0;

    __u64 target_pid = ctl_get(CTL_PID);
    if (!target_pid) return 0;

    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != (__u32)target_pid) {
        stat_inc(STAT_SKIP_PID);
        return 0;
    }

    /* 保存 msg 指针（供 fexit 使用） */
    unsigned long msg_val = (unsigned long)msg;
    __u32 key = 0;
    bpf_map_update_elem(&entry_msg, &key, &msg_val, 0);

    return 0;
}
```

**关键变化**：
- `SEC("kprobe/tcp_recvmsg")` → `SEC("fentry/tcp_recvmsg")`
- `PT_REGS` 手动读寄存器 → `BPF_PROG` 宏类型化参数
- `ctx->si`（PT_REGS_PARM2）→ `msg` 参数直接访问
- PID 过滤保留

- [ ] **Step 3: 替换 return hook：kretprobe → fexit**

替换第 141-184 行的 `kp_recv_return` 函数：

```c
/* ──── fexit: 在 tcp_recvmsg 返回时读取数据写 ringbuf ──── */
SEC("fexit/tcp_recvmsg")
int BPF_PROG(fexit_recv, int retval, struct sock *sk, struct msghdr *msg, size_t len)
{
    __u64 enabled = ctl_get(CTL_ENABLED);
    if (!enabled) return 0;

    if (retval <= 0) return 0;

    __u32 key = 0;
    unsigned long *msg_ptr = bpf_map_lookup_elem(&entry_msg, &key);
    if (!msg_ptr || *msg_ptr == 0) return 0;

    __u32 size = (__u32)retval;
    if (size > CAPTURE_MAX_DATA) size = CAPTURE_MAX_DATA;

    unsigned char *buf = bpf_map_lookup_elem(&tmpbuf, &key);
    if (!buf) return 0;

    /* 头 4 字节 = payload 长度 */
    __u32 plen = 0;
    __builtin_memcpy(buf, &plen, 4);

    int data_len = read_iov_data(*msg_ptr, buf + 4, CAPTURE_MAX_DATA);
    if (data_len <= 0) return 0;

    plen = (__u32)data_len;
    __builtin_memcpy(buf, &plen, 4);

    int total = 4 + data_len;
    if (bpf_ringbuf_output(&ringbuf, buf, total, 0) != 0) {
        stat_inc(STAT_RB_ERR);
        return 0;
    }

    stat_inc(STAT_HIT);
    stat_add(STAT_BYTES, (__u64)data_len);

    /* 清除保存的 msg 指针 */
    unsigned long zero = 0;
    bpf_map_update_elem(&entry_msg, &key, &zero, 0);

    return 0;
}
```

**关键变化**：
- `SEC("kretprobe/tcp_recvmsg")` → `SEC("fexit/tcp_recvmsg")`
- `BPF_PROG` 的第一个参数是返回值 `retval`（替代原来的 `ctx->ax`）
- `msg` 参数从 entry_msg 读取改为由 BPF_PROG 直接传入（fallback：仍从 entry_msg 读以兼容旧逻辑）
- 其余逻辑不变

**注意**：如果 `BPF_PROG` 在当前环境不可用（libbpf 版本太旧），改用：
```c
SEC("fexit/tcp_recvmsg")
int fexit_recv(struct pt_regs *ctx) {
    int retval = (int)PT_REGS_RC(ctx);  // 等价于 ctx->ax
    // ... 其余同 kretprobe 版本
}
```
但 SEC 名称仍保持 `fexit/` 以使用 trampoline 机制。

- [ ] **Step 4: 更新测试程序的 kprobe attach 代码**

在 `tests/perf/test_kprobe_repl_qps.c`，需要修改 `load_kprobe_bpf` 函数（第 155-208 行）中的 attach 方式。fentry/fexit 程序在 `bpf_object__load()` 时会自动 attach（libbpf 1.0+），但 libbpf 0.5.0 可能需要手动 attach。

在 `tests/perf/test_kprobe_repl_qps.c` 第 176-196 行，把 kprobe attach 改为：

```c
    /* Attach fentry/fexit — 尝试 trampoline，失败回退 kprobe */
    struct bpf_program *prog;
    struct bpf_link *le = NULL, *lr = NULL;

    prog = bpf_object__find_program_by_name(obj, "fentry_recv");
    if (prog) {
        le = bpf_program__attach(prog);
        if (libbpf_get_error(le)) {
            fprintf(stderr, "kprobe: fentry attach failed: %ld, trying kprobe\n",
                libbpf_get_error(le));
            le = NULL;
            /* 回退: 尝试旧 kprobe */
            prog = bpf_object__find_program_by_name(obj, "kp_recv_entry");
            if (prog)
                le = bpf_program__attach_kprobe(prog, false, "tcp_recvmsg");
        }
    } else {
        /* 没有 fentry 程序，尝试旧 kprobe */
        prog = bpf_object__find_program_by_name(obj, "kp_recv_entry");
        if (prog)
            le = bpf_program__attach_kprobe(prog, false, "tcp_recvmsg");
    }

    prog = bpf_object__find_program_by_name(obj, "fexit_recv");
    if (prog) {
        lr = bpf_program__attach(prog);
        if (libbpf_get_error(lr)) {
            fprintf(stderr, "kprobe: fexit attach failed: %ld, trying kretprobe\n",
                libbpf_get_error(lr));
            lr = NULL;
            prog = bpf_object__find_program_by_name(obj, "kp_recv_return");
            if (prog)
                lr = bpf_program__attach_kprobe(prog, true, "tcp_recvmsg");
        }
    } else {
        prog = bpf_object__find_program_by_name(obj, "kp_recv_return");
        if (prog)
            lr = bpf_program__attach_kprobe(prog, true, "tcp_recvmsg");
    }
```

**注意**：先尝试 `bpf_program__attach`（通用 attach，libbpf 自动选择最佳方式）。如果失败，回退到旧 kprobe 方式并 fallback 到 `kp_recv_entry`/`kp_recv_return` 程序名。

**同时保留旧的 kprobe BPF 函数**（重命名但保留在文件中），作为 fallback。在 `kprobe_capture.bpf.c` 中保留原有命名：

```c
/* 旧 kprobe 版本保留作为 fallback，重命名为 kp_recv_entry/kp_recv_return */
SEC("kprobe/tcp_recvmsg")
int kp_recv_entry(struct pt_regs *ctx) { /* ...原代码不变... */ }

SEC("kretprobe/tcp_recvmsg")
int kp_recv_return(struct pt_regs *ctx) { /* ...原代码不变... */ }
```

这样新旧两个版本共存，test 程序优先尝试 fentry/fexit，失败自动回退。

- [ ] **Step 5: 编译 BPF 对象和测试程序**

```bash
cd tests/perf
make -f Makefile.perf clean && make -f Makefile.perf all
```

预期：编译无错误。如果 `bpf_tracing.h` 中 `BPF_PROG` 不可用，改用方案 B（SEC 用 fentry/fexit 但参数用 PT_REGS 读）。

- [ ] **Step 6: 验证 fentry/fexit 是否生效**

```bash
cd tests/perf
sudo ./test_kprobe_repl_qps --mode kprobe --payload 64 --count 5000 2>&1 | grep -E 'kprobe:|fentry|fexit|loaded'
```

预期输出应包含 `fentry` 或 `fexit` 字样（从 stderr 日志确认使用了 trampoline 路径而非 kprobe 回退）。如果输出显示 "trying kprobe" 回退，检查内核 BTF 是否可用：
```bash
ls /sys/kernel/btf/vmlinux
```
文件存在 = BTF 可用。

- [ ] **Step 7: 提交**

```bash
git add tests/perf/kprobe_capture.bpf.c tests/perf/test_kprobe_repl_qps.c
git commit -m "perf(kprobe): switch to fentry/fexit, keep kprobe fallback"
```

---

### Task 5: bpf_ringbuf_reserve 消除 BPF 内部拷贝（方案 3）

**Files:**
- Modify: `tests/perf/kprobe_capture.bpf.c`（`fexit_recv` 函数体）

**Interfaces:**
- Consumes: Task 4 的 `fexit_recv`、Task 2 的 `read_iov_data`
- Produces: 改写后的 `fexit_recv`，用 `bpf_ringbuf_reserve` + `bpf_ringbuf_commit` 替代 `tmpbuf` → `bpf_ringbuf_output` 的两次搬运

- [ ] **Step 1: 改写 fexit_recv 的数据输出路径**

在 `tests/perf/kprobe_capture.bpf.c`，修改 `fexit_recv` 函数中第 157-173 行（tmpbuf → ringbuf_output 部分）。

**关键思路**：retval 已知数据长度 → reserve ringbuf 空间 → 直接写 ringbuf（省去 tmpbuf 中间搬运）。

```c
    /* 以 retval 为上限 reserve ringbuf 空间（+4 字节头） */
    __u32 reserve_sz = (__u32)retval + 4;
    if (reserve_sz > (__u32)CAPTURE_MAX_DATA + 4)
        reserve_sz = (__u32)CAPTURE_MAX_DATA + 4;

    void *entry = bpf_ringbuf_reserve(&ringbuf, reserve_sz, 0);
    if (!entry) {
        stat_inc(STAT_RB_ERR);
        unsigned long zero = 0;
        bpf_map_update_elem(&entry_msg, &key, &zero, 0);
        return 0;
    }

    /* 直接在 ringbuf 预留空间内构造 [4B len | payload] */
    __u32 plen = 0;
    __builtin_memcpy(entry, &plen, 4);

    int data_len = read_iov_data(*msg_ptr, (unsigned char *)entry + 4,
                                 CAPTURE_MAX_DATA);
    if (data_len <= 0) {
        bpf_ringbuf_discard(entry, 0);
        unsigned long zero = 0;
        bpf_map_update_elem(&entry_msg, &key, &zero, 0);
        return 0;
    }

    /* 更新长度头 */
    plen = (__u32)data_len;
    __builtin_memcpy(entry, &plen, 4);

    bpf_ringbuf_commit(entry, 0);

    stat_inc(STAT_HIT);
    stat_add(STAT_BYTES, (__u64)data_len);

    /* 清除保存的 msg 指针 */
    unsigned long zero = 0;
    bpf_map_update_elem(&entry_msg, &key, &zero, 0);

    return 0;
```

**变化**：
- 删除了 `bpf_map_lookup_elem(&tmpbuf, &key)` — 不再使用 tmpbuf
- `bpf_ringbuf_reserve` 直接在 ringbuf 中分配空间
- `read_iov_data` 直接写入 ringbuf 空间（`entry + 4`）
- 失败时 `bpf_ringbuf_discard` 而非静默丢失
- 成功时 `bpf_ringbuf_commit` 使数据对消费者可见

**注意**：`bpf_ringbuf_reserve` 返回的指针在内核地址空间。`bpf_probe_read_user` 的目标必须指向用户空间地址或 BPF map 值——这里 `entry` 指向 ringbuf（内核空间），但 `read_iov_data` 的第二个参数 `buf` 是写入目标，`bpf_probe_read_user` 是从用户态 buf 读数据写入 `buf`。**这里没问题**：`bpf_probe_read_user(buf=entry+4, src=user_addr)` — buf 可以是内核地址（ringbuf reserve 返回的地址），src 是用户地址。BPF verifier 应该接受。

如果 BPF verifier 报错 "R2 type=ptr_ expected=map_value_or_null"，说明它不接受 ringbuf 指针作为 `probe_read_user` 的 dst。此时回退方案：
- 仍用 tmpbuf 做中间缓冲
- 只在 `bpf_ringbuf_reserve` + `__builtin_memcpy` 替代 `bpf_ringbuf_output`（消除 ringbuf_output 内部的第二次 memcpy）
- 这只能消除 BPF 内部一次拷贝，而非完全消除

- [ ] **Step 2: 如果 BPF verifier 拒绝，使用回退方案**

回退方案（tmpbuf → ringbuf_reserve + memcpy）：

```c
    /* 用 tmpbuf 做临时缓冲（BPF verifier 要求） */
    unsigned char *buf = bpf_map_lookup_elem(&tmpbuf, &key);
    if (!buf) return 0;

    __u32 plen = 0;
    __builtin_memcpy(buf, &plen, 4);

    int data_len = read_iov_data(*msg_ptr, buf + 4, CAPTURE_MAX_DATA);
    if (data_len <= 0) return 0;

    plen = (__u32)data_len;
    __builtin_memcpy(buf, &plen, 4);

    /* reserve → memcpy → commit（替代 bpf_ringbuf_output） */
    int total = 4 + data_len;
    void *entry = bpf_ringbuf_reserve(&ringbuf, total, 0);
    if (!entry) {
        stat_inc(STAT_RB_ERR);
        return 0;
    }
    __builtin_memcpy(entry, buf, total);
    bpf_ringbuf_commit(entry, 0);
```

收益：省去 `bpf_ringbuf_output` 内部的 spinlock+第二次 memcpy（`ringbuf_output` 内部也会 reserve→memcpy→commit，但我们先做了一次 memcpy 到 tmpbuf）。回退方案至少节省了 `ringbuf_output` 的第二次 memcpy。

- [ ] **Step 3: 编译 BPF 对象**

```bash
cd tests/perf
make -f Makefile.perf kprobe_capture.bpf.o
```

预期：编译无错误。如果 BPF verifier 拒绝 `bpf_ringbuf_reserve` 版本，应用回退方案后重新编译。

- [ ] **Step 4: 验证**

```bash
cd tests/perf
sudo ./test_kprobe_repl_qps --mode kprobe --payload 64 --count 5000
```

预期：kprobe 模式正常启动，无崩溃，fwd count > 0。

- [ ] **Step 5: 提交**

```bash
git add tests/perf/kprobe_capture.bpf.c
git commit -m "perf(kprobe): use bpf_ringbuf_reserve to eliminate BPF internal copy"
```

---

### Task 6: 阶段二 benchmark 验证

**Files:**
- 无修改

- [ ] **Step 1: 编译所有测试**

```bash
cd tests/perf
make -f Makefile.perf clean && make -f Makefile.perf all
```

- [ ] **Step 2: 运行全量 benchmark**

```bash
cd tests/perf
for p in 64 128 256 512 1024 2048 4096; do
    echo "=== payload=$p ==="
    sudo ./test_kprobe_repl_qps --mode all --payload $p --count 10000
    echo ""
done
```

- [ ] **Step 3: 检查结果**

对比阶段二目标：
- kprobe vs sync 比例是否达到 **80%~95%**
- fwd 丢失率应 <5%

如果 kprobe QPS 比阶段一下降 >5%：检查 fentry/fexit 是否实际生效（`dmesg` 确认无 kprobe 回退），检查 ringbuf_reserve 是否引入了额外开销。

- [ ] **Step 4: 记录结果**

保存 benchmark 输出到 `benchmarks/data/phase2-bench.txt`。

- [ ] **Step 5: 提交**

```bash
git add benchmarks/data/phase2-bench.txt
git commit -m "bench(kprobe): phase 2 benchmark after fentry/fexit + ringbuf_reserve"
```

---

### Task 7: 从 sk_buff 直接读数据（方案 5）

**Files:**
- Modify: `tests/perf/kprobe_capture.bpf.c`（新增 `read_skb_data` 函数，改写 `fexit_recv` 的数据源）

**Interfaces:**
- Consumes: Task 5 的 `fexit_recv`（ringbuf_reserve 路径）
- Produces: `read_skb_data(sk, buf, max_len)` — 从 `sk->sk_receive_queue` 的 sk_buff 链表直接读内核态数据

- [ ] **Step 1: 探测 sk_buff 可读性（可行性验证）**

先写一个探测性 BPF 程序确认 `tcp_recvmsg` 返回后 sk_buff 仍可读。

在 `fexit_recv` 中添加调试代码（**仅在开发分支，不提交**）：

```c
/* 探测: 读取 sk_receive_queue */
struct sk_buff *skb = NULL;
long err = bpf_probe_read_kernel(&skb, sizeof(skb),
    (const void *)((unsigned long)sk + __builtin_offsetof(struct sock, sk_receive_queue)));
/* __builtin_offsetof 可能不支持结构体嵌套字段，改用硬编码偏移 */
```

由于 BPF verifier 限制，改用 BPF CO-RE 方式：
```c
/* 通过 BTF 获取 sk->sk_receive_queue.next 的偏移 */
struct sk_buff *skb = NULL;
if (bpf_core_read(&skb, sizeof(skb), &sk->sk_receive_queue.next) != 0) {
    /* sk_receive_queue 不可读或为空，方案不可行 */
    return 0;
}
```

如果 `bpf_core_read` 成功读到非空 skb 指针，继续 Step 2。如果返回错误或 skb 为空（tcp_recvmsg 返回后 skb 已被消费），**跳过 Task 7，标记方案 5 不适用于 5.15**。

- [ ] **Step 2: 实现 `read_skb_data` 函数**

如果 Step 1 验证通过，新增 `read_skb_data` 函数（添加到 `read_iov_data` 之前）：

```c
/* 从 sk->sk_receive_queue 的 sk_buff 直接读数据
 * 避免 bpf_probe_read_user 的额外用户态拷贝
 * 适用范围: fexit/tcp_recvmsg（此时数据刚从内核态拷贝，skb 可能仍可读） */
static __always_inline int read_skb_data(struct sock *sk,
    unsigned char *buf, int max_len, int expected_len)
{
    if (!sk || max_len <= 0) return 0;

    /* 遍历 sk_receive_queue 找包含目标数据的 skb
     * tcp_recvmsg 可能消费了部分 skb，所以从链表尾部找 */
    struct sk_buff *skb = NULL;

    /* 读取 sk_receive_queue.next（第一个 skb） */
    if (bpf_core_read(&skb, sizeof(skb), &sk->sk_receive_queue.next) != 0)
        return 0;

    /* 遍历链表找最近使用的 skb */
    int found = 0;
#pragma unroll
    for (int i = 0; i < 8; i++) {  /* 最多遍历 8 个 */
        if (!skb || found) break;

        unsigned int data_len = 0;
        if (bpf_core_read(&data_len, sizeof(data_len), &skb->len) != 0)
            break;

        /* 找到匹配长度的 skb（简化匹配：长度匹配即认为是目标） */
        if (data_len >= (unsigned int)expected_len && data_len <= (unsigned int)max_len) {
            unsigned char *data = NULL;
            if (bpf_core_read(&data, sizeof(data), &skb->data) == 0 && data) {
                /* 从内核态 skb 数据区直接读 */
                unsigned long long safe_len = (unsigned long long)expected_len;
                if (safe_len > (unsigned long long)max_len)
                    safe_len = (unsigned long long)max_len;
                if (bpf_probe_read_kernel(buf, (__u32)safe_len, data) == 0) {
                    found = 1;
                    break;
                }
            }
        }

        /* 下一个 skb */
        if (bpf_core_read(&skb, sizeof(skb), &skb->next) != 0)
            break;
    }

    return found ? expected_len : 0;
}
```

**风险点**：
- `sk_receive_queue` 在 `tcp_recvmsg` 返回后可能已空（skb 被消费并释放）
- `bpf_core_read` 需要 CONFIG_DEBUG_INFO_BTF
- `skb->data` 指向的内存在 skb 被消费后可能无效

- [ ] **Step 3: 在 fexit_recv 中用 read_skb_data 替代 read_iov_data**

修改 `fexit_recv` 中的数据读取部分：

```c
    /* 尝试从 sk_buff 直接读（无额外拷贝）；失败则回退 read_iov_data */
    int data_len = read_skb_data(sk, (unsigned char *)entry + 4,
                                 CAPTURE_MAX_DATA, (int)retval);
    if (data_len <= 0) {
        /* 回退: 从用户态 buf 读（旧路径） */
        data_len = read_iov_data(*msg_ptr, (unsigned char *)entry + 4,
                                 CAPTURE_MAX_DATA);
    }
```

- [ ] **Step 4: 编译 BPF 对象**

```bash
cd tests/perf
make -f Makefile.perf kprobe_capture.bpf.o
```

如果 BPF verifier 拒绝 `bpf_core_read` 或 `skb` 访问：放宽 `#pragma unroll` 或减少遍历次数。如果反复失败，跳过 Task 7，记录原因。

- [ ] **Step 5: 验证**

```bash
cd tests/perf
sudo ./test_kprobe_repl_qps --mode kprobe --payload 64 --count 5000
```

预期（如果 sk_buff 路径生效）：kprobe 模式启动，fwd count > 0，且 fwd count 应等于 echo count（sk_buff 路径不应丢数据）。

如果 sk_buff 路径导致转发数据为空（read_skb_data 始终返回 0），说明 fallback 路径（read_iov_data）生效。检查 `dmesg` 中是否有 BPF 相关错误。

- [ ] **Step 6: 提交（如果验证通过）**

```bash
git add tests/perf/kprobe_capture.bpf.c
git commit -m "perf(kprobe): read from sk_buff directly to avoid bpf_probe_read_user copy"
```

如果方案 5 不可行：
```bash
git checkout tests/perf/kprobe_capture.bpf.c  # 回退 Task 7 改动
# 记录到设计文档的已知限制中
```

---

### Task 8: 最终 benchmark + 报告

**Files:**
- 无修改（或更新 README benchmark 数据）

- [ ] **Step 1: 编译所有测试**

```bash
cd tests/perf
make -f Makefile.perf clean && make -f Makefile.perf all
```

- [ ] **Step 2: 运行全量 benchmark**

```bash
cd tests/perf
for p in 64 128 256 512 1024 2048 4096; do
    echo "=== payload=$p ==="
    sudo ./test_kprobe_repl_qps --mode all --payload $p --count 10000
    echo ""
done
```

- [ ] **Step 3: 汇总结果**

整理 benchmark 结果表，格式对齐 README：

```
| Payload | none QPS | sync QPS | kprobe QPS | kprobe vs sync | kprobe fwd |
|---------|----------|----------|------------|----------------|------------|
```

- [ ] **Step 4: 更新 README benchmark 数据（如果结果显著改善）**

将新数据填入 README 的"eBPF kprobe 主从转发 QPS 对比"表格。

- [ ] **Step 5: 提交最终报告**

```bash
git add README.md benchmarks/data/phase3-bench.txt
git commit -m "bench(kprobe): final benchmark after all optimizations

Phase 1: ringbuf 4MB, poll 5ms, SNDBUF 256KB, merged probe_read_kernel
Phase 2: fentry/fexit trampoline, bpf_ringbuf_reserve
Phase 3: sk_buff direct read (if applicable)

kprobe vs sync: XX%~YY% (was 56%~79%)"
```

---

## 回退与风险控制

每个 task 的编译/验证步骤如果失败，按以下策略处理：

| 失败场景 | 处理 |
|---------|------|
| BPF verifier 拒绝 fentry/fexit | 保留 kprobe fallback，SEC 名称回退 |
| BPF verifier 拒绝 ringbuf_reserve | 使用回退方案（tmpbuf + ringbuf_reserve+memcpy） |
| sk_buff 路径不可读 | 跳过 Task 7，保留 read_iov_data 路径 |
| 任一阶段 kprobe QPS 下降 >5% | `git revert` 该阶段所有 commit |
| fentry/fexit 编译失败（libbpf 过旧） | 跳过 Task 4+5，仅保留阶段一优化 |
