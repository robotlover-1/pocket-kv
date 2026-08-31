# eBPF fentry/fexit 主从转发优化 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Master VM 内核升到 6.1，启用 fentry/fexit BPF hook 替代 kprobe，验证主从转发 QPS 超过 sync 模式。

**Architecture:** 复用已有的 `fentry_recv`/`fexit_recv` BPF 程序（写在 `kprobe_capture.bpf.c` 中，当前被禁用），新增 `load_fentry_bpf()` 用户态加载函数，在 `test_kprobe_repl_qps.c` 中增加 `fentry` 模式。不改生产代码，只改 `tests/perf/`。

**Tech Stack:** C, libbpf 1.1 (from kernel 6.1 source), clang, Linux 6.1 mainline kernel

## Global Constraints

- 内核版本：Master VM 需 ≥ 6.1，Slave VM 保持 5.15
- Ubuntu 版本不升级（保持 20.04）
- 不改 `src/` 下的生产代码
- 只改 `tests/perf/` 目录
- BPF 程序必须保留 kprobe fallback（fentry 加载失败时回退）

## File Map

| 文件 | 职责 | 本次 |
|------|------|------|
| `tests/perf/kprobe_capture.bpf.c` | BPF 程序（fentry+fexit+kprobe+kretprobe+tp），已含 fentry/fexit | 不改 |
| `tests/perf/test_kprobe_repl_qps.c` | 测试主程序，含 BPF 加载/QPS 客户端/slave 线程 | **改**：加 `load_fentry_bpf()`、`fentry` 模式 |
| `tests/perf/vmlinux.h` | BTF 生成的头文件 | **重建**（内核 6.1 的 BTF） |
| `tests/perf/Makefile.perf` | 编译脚本 | 不改（kprobe_capture.bpf.o 已包含 fentry） |
| `tests/perf/test_slave_receiver.c` | Slave 接收进程 | 不改 |
| `tests/perf/common.h` | 公共工具函数 | 不改 |

---

### Task 1: 升级 Master VM 内核到 6.1

**操作环境**：Master VM (192.168.233.128)，sudo 密码 `2983372202`

这些是手动运维操作，不在代码仓库中。在 Master VM 上执行：

- [ ] **Step 1: 下载 Ubuntu mainline 6.1.x 内核包**

```bash
# 在 Master VM 上执行
mkdir -p /tmp/kernel-6.1 && cd /tmp/kernel-6.1

# 从 Ubuntu mainline PPA 下载（以 6.1.100 为例，选择可用最新 6.1.x）
wget https://kernel.ubuntu.com/mainline/v6.1.100/amd64/linux-headers-6.1.100-0601100_6.1.100-0601100.202406210731_all.deb
wget https://kernel.ubuntu.com/mainline/v6.1.100/amd64/linux-headers-6.1.100-0601100-generic_6.1.100-0601100.202406210731_amd64.deb
wget https://kernel.ubuntu.com/mainline/v6.1.100/amd64/linux-image-unsigned-6.1.100-0601100-generic_6.1.100-0601100.202406210731_amd64.deb
wget https://kernel.ubuntu.com/mainline/v6.1.100/amd64/linux-modules-6.1.100-0601100-generic_6.1.100-0601100.202406210731_amd64.deb
```

> **注意**：如果 `6.1.100` 链接 404，去 https://kernel.ubuntu.com/mainline/ 找最新的 `v6.1.*` 目录，替换版本号。

- [ ] **Step 2: 安装内核**

```bash
cd /tmp/kernel-6.1
echo "2983372202" | sudo -S dpkg -i linux-headers-*.deb linux-image-*.deb linux-modules-*.deb
```

- [ ] **Step 3: 重启进入新内核**

```bash
echo "2983372202" | sudo -S reboot
```

- [ ] **Step 4: 验证内核版本**

```bash
uname -r
# 预期输出: 6.1.100-0601100-generic (或类似)
```

- [ ] **Step 5: 验证 BTF 可用**

```bash
ls -la /sys/kernel/btf/vmlinux
# 预期: 文件存在，大小 > 1MB
```

**失败处理**：如果内核 panic 或起不来，grub 引导时选择 `Advanced options → Ubuntu, with Linux 5.15.*` → 回退。进入方案 2（sockmap）。

---

### Task 2: 添加 `load_fentry_bpf()` 到 `test_kprobe_repl_qps.c`

**文件**: `tests/perf/test_kprobe_repl_qps.c`

在 `load_tp_bpf()` 函数之后（line 294 附近），`master_t` 结构体之前，插入新函数。

- [ ] **Step 1: 在 line 294 后插入 `load_fentry_bpf()` 函数**

```c
/* ========== fentry/fexit 加载（内核 6.1+）========== */
static void *load_fentry_bpf(void) {
    struct rlimit rl = {RLIM_INFINITY, RLIM_INFINITY};
    setrlimit(RLIMIT_MEMLOCK, &rl);

    struct bpf_object *obj = bpf_object__open(g_bpf_obj);
    if (!obj) { fprintf(stderr, "fentry: open failed\n"); return NULL; }

    /* 禁用 kprobe/kretprobe/tp 程序，只保留 fentry/fexit 的 autoload */
    { struct bpf_program *_p;
      _p = bpf_object__find_program_by_name(obj, "kp_recv_entry");
      if (_p) bpf_program__set_autoload(_p, false);
      _p = bpf_object__find_program_by_name(obj, "kp_recv_return");
      if (_p) bpf_program__set_autoload(_p, false);
      _p = bpf_object__find_program_by_name(obj, "tp_sys_enter_read");
      if (_p) bpf_program__set_autoload(_p, false);
      _p = bpf_object__find_program_by_name(obj, "tp_sys_exit_read");
      if (_p) bpf_program__set_autoload(_p, false); }

    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "fentry: load failed\n");
        bpf_object__close(obj);
        return NULL;
    }

    /* auto-attach fentry/fexit via trampoline */
    struct bpf_program *prog;
    struct bpf_link *le = NULL, *lx = NULL;

    prog = bpf_object__find_program_by_name(obj, "fentry_recv");
    if (prog && bpf_program__fd(prog) >= 0) {
        le = bpf_program__attach(prog);
        if (libbpf_get_error(le)) {
            fprintf(stderr, "fentry: attach fentry_recv failed: %ld\n", libbpf_get_error(le));
            le = NULL;
        }
    }
    prog = bpf_object__find_program_by_name(obj, "fexit_recv");
    if (prog && bpf_program__fd(prog) >= 0) {
        lx = bpf_program__attach(prog);
        if (libbpf_get_error(lx)) {
            fprintf(stderr, "fentry: attach fexit_recv failed: %ld\n", libbpf_get_error(lx));
            lx = NULL;
        }
    }

    if (!le || !lx) {
        fprintf(stderr, "fentry: attach failed\n");
        if (le) bpf_link__destroy(le);
        if (lx) bpf_link__destroy(lx);
        bpf_object__close(obj);
        return NULL;
    }

    /* 设置 PID 过滤 */
    struct bpf_map *ctl = bpf_object__find_map_by_name(obj, "ctl");
    if (ctl) {
        __u32 k0 = 0, k1 = 1;
        __u64 v1 = 1, vpid = (__u64)getpid();
        bpf_map_update_elem(bpf_map__fd(ctl), &k0, &v1, BPF_ANY);
        bpf_map_update_elem(bpf_map__fd(ctl), &k1, &vpid, BPF_ANY);
    }

    fprintf(stderr, "[fentry] loaded, attached fentry+fexit (PID=%d)\n", getpid());
    return obj;
}
```

- [ ] **Step 2: 验证编译通过**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
make -f Makefile.perf test_kprobe_repl_qps
```

预期：编译成功，无警告。

---

### Task 3: 添加 `fentry` 模式到 `run_one_mode()` 和 `main()`

**文件**: `tests/perf/test_kprobe_repl_qps.c`

需要改三处：`run_one_mode()` 的 mode 判断、BPF 加载分支、`main()` 的 modes 数组。

- [ ] **Step 1: 修改 `run_one_mode()` 中的 mode 判断（line 502-505）**

找到：
```c
    int use_kprobe = (strcmp(mode, "kprobe") == 0);
    int use_tp     = (strcmp(mode, "tp") == 0);
    int use_sync   = (strcmp(mode, "sync") == 0);
    int use_bpf    = use_kprobe || use_tp;
```

替换为：
```c
    int use_kprobe = (strcmp(mode, "kprobe") == 0);
    int use_tp     = (strcmp(mode, "tp") == 0);
    int use_fentry = (strcmp(mode, "fentry") == 0);
    int use_sync   = (strcmp(mode, "sync") == 0);
    int use_bpf    = use_kprobe || use_tp || use_fentry;
```

- [ ] **Step 2: 修改 BPF 加载分支（line 512-520），在 use_tp 块后插入 fentry 块**

找到：
```c
    if (use_kprobe) {
        bpf_obj = load_kprobe_bpf();
        if (!bpf_obj) { fprintf(stderr, "[%s] kprobe load failed, skip\n", mode_name); return r; }
    }
    if (use_tp) {
        bpf_obj = load_tp_bpf();
        if (!bpf_obj) { fprintf(stderr, "[%s] tp load failed, skip\n", mode_name); return r; }
    }
```

替换为：
```c
    if (use_kprobe) {
        bpf_obj = load_kprobe_bpf();
        if (!bpf_obj) { fprintf(stderr, "[%s] kprobe load failed, skip\n", mode_name); return r; }
    }
    if (use_tp) {
        bpf_obj = load_tp_bpf();
        if (!bpf_obj) { fprintf(stderr, "[%s] tp load failed, skip\n", mode_name); return r; }
    }
    if (use_fentry) {
        bpf_obj = load_fentry_bpf();
        if (!bpf_obj) {
            /* fentry 加载失败 → 回退 kprobe */
            fprintf(stderr, "[%s] fentry load failed, falling back to kprobe\n", mode_name);
            bpf_obj = load_kprobe_bpf();
            use_kprobe = 1;
            use_fentry = 0;
            if (!bpf_obj) { fprintf(stderr, "[%s] kprobe fallback also failed, skip\n", mode_name); return r; }
        }
    }
```

- [ ] **Step 3: 修改 `main()` 中 modes 数组（line 682），添加 "fentry"**

找到：
```c
    const char *modes[] = {"none", "sync", "kprobe", "tp"};
```

替换为：
```c
    const char *modes[] = {"none", "sync", "kprobe", "fentry", "tp"};
```

- [ ] **Step 4: 修改 loop 循环上限（line 683-685）**

找到：
```c
    for (int i = 0; i < 4; i++) {
        if (!do_all && strcmp(mode_str, modes[i]) != 0) continue;
```

替换为：
```c
    for (int i = 0; i < 5; i++) {
        if (!do_all && strcmp(mode_str, modes[i]) != 0) continue;
```

- [ ] **Step 5: 更新 usage() 中的 mode 说明（line 639）**

找到：
```c
        "  --mode, -m    none | sync | kprobe | tp | all (默认: all)\n"
```

替换为：
```c
        "  --mode, -m    none | sync | kprobe | fentry | tp | all (默认: all)\n"
```

- [ ] **Step 6: 编译验证**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
make -f Makefile.perf test_kprobe_repl_qps
```

预期：编译成功，无警告。

---

### Task 4: 重建 vmlinux.h（内核 6.1 BTF）

**文件**: `tests/perf/vmlinux.h`

内核升级后 BTF 信息变了，必须重新生成。

- [ ] **Step 1: 确认 bpftool 可用**

```bash
which bpftool || echo "bpftool not found"
```

如果没有 bpftool：
```bash
echo "2983372202" | sudo -S apt install -y linux-tools-common linux-tools-$(uname -r)
# 或使用 libbpf 自带的 bpftool:
ls /tmp/linux-src-6.1/tools/bpf/bpftool/
```

- [ ] **Step 2: 重新生成 vmlinux.h**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h
```

- [ ] **Step 3: 验证生成结果**

```bash
wc -l vmlinux.h
# 预期: > 50000 行（内核 6.1 的 BTF 比 5.15 大）
head -5 vmlinux.h
# 预期: #ifndef __VMLINUX_H__ 等
```

- [ ] **Step 4: 提交 vmlinux.h**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
git add tests/perf/vmlinux.h
git commit -m "chore(perf): regenerate vmlinux.h for kernel 6.1 BTF"
```

---

### Task 5: 构建 + 冒烟测试（BPF 加载）

在 Master VM 上验证 fentry BPF 能成功加载。

- [ ] **Step 1: 完整构建**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
make -f Makefile.perf clean
make -f Makefile.perf test_kprobe_repl_qps test_slave_receiver
```

预期：`kprobe_capture.bpf.o` 和 `test_kprobe_repl_qps` 均重新编译。

- [ ] **Step 2: 启动 Slave receiver（在 Slave VM 192.168.233.129 上）**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
./test_slave_receiver --port 15801
```

预期输出：
```
[slave] listening on 0.0.0.0:15801, delay=0us
```

- [ ] **Step 3: 仅运行 fentry 模式，100 请求冒烟**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode fentry --payload 64 --count 100 \
  --slave-host 192.168.233.129 --slave-port 15801
```

预期 stderr 输出包含：
```
[fentry] loaded, attached fentry+fexit (PID=xxxxx)
[fwd] ready, polling ringbuf
[client] connected after retries
[client] warmup done
[client] starting test loop, 100 reqs
[fwd] exiting, forwarded=100
```

确认输出表格中 fentry QPS > 0 且 slave msgs == 100。

**失败处理**：
- 如果 `[fentry] load failed`：检查 verifier 日志 `echo "2983372202" | sudo -S cat /sys/kernel/debug/tracing/trace_pipe`，判断是 BTF 类型问题还是其他
- 如果 `[fentry] attach failed`：`fentry_recv` 或 `fexit_recv` 的 SEC 目标函数在内核 BTF 中找不到，检查 `cat /proc/kallsyms | grep tcp_recvmsg`

- [ ] **Step 4: 提交冒烟通过的代码**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
git add tests/perf/test_kprobe_repl_qps.c
git commit -m "feat(perf): add fentry/fexit BPF mode to kprobe repl QPS test"
```

---

### Task 6: 数据完整性测试

验证 fentry 模式下 Slave 收到的数据完整。

- [ ] **Step 1: 在 Slave VM 上重启 receiver**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
./test_slave_receiver --port 15801
```

- [ ] **Step 2: 在 Master VM 上跑 1000 请求**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode fentry --payload 64 --count 1000 \
  --slave-host 192.168.233.129 --slave-port 15801
```

- [ ] **Step 3: 检查 slave 输出**

在 Slave VM 的 receiver 输出中确认：
```
[slave] connection #1 closed: 1000 msgs, 0.06 MB
```

即 `slave msgs == 1000`（收到全部消息）。

- [ ] **Step 4: 大 payload 也测一轮**

```bash
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode fentry --payload 4096 --count 500 \
  --slave-host 192.168.233.129 --slave-port 15801
```

预期：`slave msgs == 500`。

---

### Task 7: 完整 QPS 基准测试

完成所有 7 个 payload 的对比测试。

- [ ] **Step 1: 在 Slave VM 上重启 receiver（每次测试前）**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf
./test_slave_receiver --port 15801
```

- [ ] **Step 2: 在 Master VM 跑 `--mode all`（全 payload，64B 开始逐一测）**

每个 payload 单独跑一轮，因为 `--mode all` 串行跑所有 mode，但需要逐个 payload 对比。

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore/tests/perf

# 64B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 64 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801

# 128B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 128 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801

# 256B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 256 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801

# 512B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 512 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801

# 1024B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 1024 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801

# 2048B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 2048 --count 5000 \
  --slave-host 192.168.233.129 --slave-port 15801

# 4096B
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 4096 --count 5000 \
  --slave-host 192.168.233.129 --slave-port 15801
```

> **注意**：每次测试前在 Slave VM 重启 `test_slave_receiver`（Ctrl+C 后再启动），确保 msg 计数从零开始。

- [ ] **Step 3: 收集结果到文件**

将 stdout 输出保存：
```bash
# 示例：保存 64B 结果
echo "2983372202" | sudo -S ./test_kprobe_repl_qps --mode all --payload 64 --count 10000 \
  --slave-host 192.168.233.129 --slave-port 15801 2>&1 | tee /tmp/perf_fentry_64B.txt
```

类似地保存每个 payload 的结果到 `/tmp/perf_fentry_<payload>.txt`。

---

### Task 8: 结果对比与决策

- [ ] **Step 1: 整理对比表**

从每个 payload 的测试输出中提取 `RESULT` 行，对照 spec 中的成功标准：

| Payload | none QPS | sync QPS | kprobe QPS | fentry QPS | fentry vs sync | 判断 |
|---------|----------|----------|------------|------------|----------------|------|
| 64B     |          |          |            |            |                |      |
| 128B    |          |          |            |            |                |      |
| 256B    |          |          |            |            |                |      |
| 512B    |          |          |            |            |                |      |
| 1024B   |          |          |            |            |                |      |
| 2048B   |          |          |            |            |                |      |
| 4096B   |          |          |            |            |                |      |

- [ ] **Step 2: 按 spec 标准决策**

| 结果 | 条件 | 后续动作 |
|------|------|---------|
| ✅ 全胜 | fentry > sync，7/7 | 采纳方案 1。提交结果到 README benchmark 节 |
| ✅ 通过 | fentry > sync，≥ 5/7 | 采纳方案 1 |
| ⚠️ 持平 | fentry ≈ sync，差距 < 5% | 进入方案 3（sk_buff 直读） |
| ❌ 落后 | fentry < sync，差距 > 5% | 进入方案 3 |

- [ ] **Step 3: 更新 README benchmark 节并提交**

```bash
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
# 编辑 README.md，在 "eBPF kprobe 主从转发 QPS 对比" 节后追加 fentry 数据
git add README.md
git commit -m "bench(fentry): add fentry/fexit QPS benchmark results to README"
```

---

### 附录：回退命令

如果 fentry 加载失败需要回退 kprobe（代码已自动处理），或者内核起不来需要回退：

```bash
# 内核回退：grub 选 Advanced options → Linux 5.15.*
# 代码回退：
cd /home/pp/Desktop/ls_study/proj/9.1-kvstore
git checkout HEAD~1 -- tests/perf/test_kprobe_repl_qps.c
```
