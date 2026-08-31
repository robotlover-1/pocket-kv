# 本地主从同步测试（RDMA 全量 + eBPF+tcp 增量）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在本地 Soft-RoCE 环境下完成 master-slave 同步测试：RDMA 全量同步 + eBPF kprobe 捕获客户端写入→TCP 转发增量同步。

**Architecture:** Master（5160）通过 RDMA 发送全量快照到 Slave，同时 eBPF kprobe/kretprobe 挂载 tcp_recvmsg 捕获全量同步期间到达的客户端写入数据并缓存（L1 内存+L2 磁盘），REPLDONE 后 flush 缓存到 Slave，之后增量数据由 eBPF 直接转发。Slave 侧混合接收：RDMA 收全量 chunk + TCP 收 eBPF 转发的增量数据。

**Tech Stack:** C, libbpf, eBPF kprobe/kretprobe, Soft-RoCE (siw0), RDMA CM, TCP

---

### Task 1: 配置文件适配本地测试

**Files:**
- Modify: `kvstore.conf`
- Modify: `tests/test.conf`

- [ ] **Step 1: kvstore.conf — 设置本地地址**

将 `master_host` 改为 `127.0.0.1`：

```
master_host=127.0.0.1
```

- [ ] **Step 2: tests/test.conf — 设置本地地址和端口**

将 master/slave 地址改为 `127.0.0.1`，slave 端口改为 `5162`（避免与 master RDMA listener 5161 潜在冲突）：

```
host=127.0.0.1
master_host=127.0.0.1
master_port=5160
slave_host=127.0.0.1
slave_port=5162
```

- [ ] **Step 3: 验证配置文件**

```bash
grep -E "master_host|slave_host|slave_port" kvstore.conf tests/test.conf
```

预期输出：全部指向 `127.0.0.1`，slave_port=5162。

---

### Task 2: 编译验证

**Files:**
- Verify: `kvstore` binary
- Verify: `tests/test_repl_5w5w` binary
- Verify: BPF objects in `build/replication/bpf/`

- [ ] **Step 1: 强制全量重新编译**

```bash
make kvstore -B 2>&1 | tail -5
```

预期：最后一行 `gcc -o kvstore ...` 无 error。

- [ ] **Step 2: 编译测试二进制**

```bash
make tests/test_repl_5w5w -B 2>&1
```

预期：编译成功，生成 `tests/test_repl_5w5w`。

- [ ] **Step 3: 确认 BPF 对象文件存在**

```bash
ls -la build/replication/bpf/repl_client_capture.bpf.o build/replication/bpf/repl_kprobe.bpf.o
```

预期：两个文件都存在且不为空。

---

### Task 3: 清理旧进程和数据文件

**Files:**
- Cleanup: 残留进程和持久化文件

- [ ] **Step 1: 杀掉旧 kvstore 进程**

```bash
sudo kill $(lsof -ti:5160) 2>/dev/null
sudo kill $(lsof -ti:5162) 2>/dev/null
sleep 1
```

- [ ] **Step 2: 清理旧持久化文件**

```bash
rm -f kvstore.dump kvstore.aof kvstore-master.dump kvstore-master.aof kvstore-slave.dump kvstore-slave.aof
```

---

### Task 4: 启动 Master 并验证

**Files:**
- Run: `./kvstore`

- [ ] **Step 1: 启动 Master（需要 root 权限加载 BPF）**

```bash
sudo ./kvstore kvstore.conf --role master > /tmp/kvstore-master.log 2>&1 &
```

- [ ] **Step 2: 等待 Master 就绪**

```bash
sleep 2
```

- [ ] **Step 3: 验证 Master 启动成功**

```bash
grep -E "role:|listening|client_capture.*initialized|kprobe.*master" /tmp/kvstore-master.log | tail -10
```

预期输出：
- `role:master`
- `listening on port 5160`
- `client_capture: initialized` 或 `client capture init failed, continuing`（后者可接受）
- `kprobe rdma: master init OK`

- [ ] **Step 4: 验证 Master INFO 接口可用**

```bash
echo -e "*1\r\n\$4\r\nINFO\r\n" | nc -w 3 127.0.0.1 5160 | head -3
```

预期：返回 RESP 格式的 INFO 响应，包含 `role:master`。

---

### Task 5: 运行测试脚本（Phase 1 — 预存数据）

- [ ] **Step 1: 启动 test_repl_5w5w**

```bash
sudo ./tests/test_repl_5w5w --master-host 127.0.0.1 --master-port 5160 --slave-host 127.0.0.1 --slave-port 5162 --pre 50000 --post 50000 2>&1 | tee /tmp/test-repl.log
```

- [ ] **Step 2: 等待 Phase 1 完成**

观察输出：
```
Phase 1: 预存数据到 Master
  预存完成: 50000 keys in XX.XXs
  [PASS] 预存 50000 条数据到 Master
```

- [ ] **Step 3: 测试会暂停在 Phase 2**

输出：
```
Phase 2: 等待 Slave 连接
  请启动 Slave (另一终端):
  等待 Slave 连接...
```

---

### Task 6: 启动 Slave

**Files:**
- Run: `./kvstore --role slave`

- [ ] **Step 1: 在新终端启动 Slave**

```bash
sudo ./kvstore --port 5162 --role slave --master-host 127.0.0.1 --master-port 5160 --repl-fullsync-transport rdma --repl-realtime-transport ebpf+tcp > /tmp/kvstore-slave.log 2>&1 &
```

> 关键参数：Slave 使用独立端口 5162，--master-host 指向本地 Master。

- [ ] **Step 2: 等待 Slave 连接**

```bash
sleep 3
```

- [ ] **Step 3: 验证 Slave 日志**

```bash
grep -E "role:|REPLSYNC|slave.*connect|fullsync|FULLRESYNC|rdma" /tmp/kvstore-slave.log | tail -20
```

预期：包含 `FULLRESYNC`、RDMA 连接建立、`REPLDONE` 等关键事件。

---

### Task 7: 监控测试完成

**Files:**
- Monitor: test_repl_5w5w 输出

- [ ] **Step 1: 观察测试进度**

测试应依次通过：
- Phase 2: Slave 已连接
- Phase 3: 全量同步完成（RDMA）
- Phase 4: 增量数据写入 Master（50000 keys）
- Phase 5: 增量同步追赶完成
- Phase 5.5: kprobe+RDMA 状态检查（可能显示"未生效"，使用 eBPF+tcp 路径，属正常）
- Phase 6: 验证 Slave 数据一致性

- [ ] **Step 2: 检查各阶段通过情况**

```bash
grep -E "\[PASS\]|\[FAIL\]|全部通过|失败" /tmp/test-repl.log
```

---

### Task 8: 故障排查（如测试失败）

**常见问题和修复：**

- [ ] **RDMA 连接失败**

检查 RDMA 设备：
```bash
rdma link show
```

确认 `siw0` 状态为 `ACTIVE`。若失败，降级为 TCP 全量同步重试：
```bash
sudo kill $(lsof -ti:5160) 2>/dev/null
sudo kill $(lsof -ti:5162) 2>/dev/null
sudo ./kvstore kvstore.conf --role master --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp > /tmp/kvstore-master.log 2>&1 &
sudo ./kvstore --port 5162 --role slave --master-host 127.0.0.1 --master-port 5160 --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp > /tmp/kvstore-slave.log 2>&1 &
```

- [ ] **BPF 加载失败**

检查日志：
```bash
grep -i "bpf\|kprobe\|capture\|ebpf" /tmp/kvstore-master.log | tail -20
```

若 BPF 加载失败但程序继续运行，增量同步回退到 `repl_broadcast`（TCP 路径）。测试仍然应通过 Phase 6 数据一致性验证。

- [ ] **端口冲突（master RDMA 5161 vs slave TCP 5162 之外的冲突）**

```bash
sudo lsof -i :5160 -i :5161 -i :5162 | grep LISTEN
```

确认没有其他进程占用这些端口。

- [ ] **数据不一致**

检查 slave 端 offset：
```bash
echo -e "*1\r\n\$4\r\nINFO\r\n" | nc -w 3 127.0.0.1 5162 | tr '\r' '\n' | grep -E "slave_repl_offset|master_link"
```

确认 `master_link=up`，`slave_repl_offset` 接近 master 的 `master_repl_offset`。

---

### Task 9: 结果验证和清理

- [ ] **Step 1: 最终结果判定**

```bash
tail -20 /tmp/test-repl.log
```

预期输出：
```
全部通过!
PASS: XX   FAIL: 0
```

- [ ] **Step 2: 停止进程**

```bash
sudo kill $(lsof -ti:5160) 2>/dev/null
sudo kill $(lsof -ti:5162) 2>/dev/null
```

- [ ] **Step 3: 检查 master 日志关键指标**

```bash
grep -E "fullsync|flush|cache|capture|broadcast|repl_offset" /tmp/kvstore-master.log | tail -20
```

验证：
- `queue_snapshot - complete` 出现
- `client_capture: flush` 出现（如缓存了数据）
- 无 `send failed` 或 `error` 相关错误
