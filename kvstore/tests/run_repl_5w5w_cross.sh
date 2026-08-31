#!/bin/bash
# run_repl_5w5w_cross.sh — 跨机 5w+5w 主从同步测试（自动清理资源）
#
# 用法:
#   bash tests/run_repl_5w5w_cross.sh
#
# 依赖:
#   - 本机=Master(128)，远程=Slave(129)，均需 root
#   - sshpass 已装、129 上 kvstore 二进制已就位（或本脚本自动 scp 同步）
#   - 环境变量 SSHPASS 可覆盖 SSH/sudo 密码（默认 2983372202）
#
# 流程: 清理两端 → 起 Master → 跑测试 → 等"等待 Slave 连接" → 起 Slave → 等测试完成 → 清理两端

set -u
PROJ="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ"

SSHPASS="${SSHPASS:-2983372202}"
SLAVE_IP="192.168.233.129"
SLAVE_USER="pp"
SLAVE_DIR="/home/pp/Desktop/ls_study/proj/9.1-kvstore"   # 129 上的项目根(二进制直接在此)
MASTER_LOG="/tmp/master_repl.log"
TEST_LOG="/tmp/repl_5w5w_test.log"
SLAVE_LOG="/tmp/slave_repl.log"
MASTER_PID=""
TEST_PID=""

# ── 工具函数 ──
cleanup_128() {
    echo "[128] 清理 master/ebpf-proxy 进程 + dump/aof 文件 + bpffs pin"
    # master/ebpf-proxy 由 root（sudo）启动，pkill/rm 必须用 sudo 才能清理干净
    echo "$SSHPASS" | sudo -S -k pkill -x kvstore 2>/dev/null || true
    echo "$SSHPASS" | sudo -S -k pkill -x ebpf_proxy 2>/dev/null || true
    # 清掉 ebpf-proxy 残留的 bpffs pin，否则 master 会误判 proxy 在跑而跳过 spawn，增量同步失效
    echo "$SSHPASS" | sudo -S -k rm -rf /sys/fs/bpf/kvstore_repl_sockmap 2>/dev/null || true
    echo "$SSHPASS" | sudo -S -k rm -f "$PROJ/kvstore.dump" "$PROJ/kvstore.aof" "$PROJ/kvstore_transport.log" 2>/dev/null || true
}

cleanup_129() {
    echo "[129] 清理 slave 进程 + dump/aof 文件"
    sshpass -p "$SSHPASS" ssh -o StrictHostKeyChecking=no "$SLAVE_USER@$SLAVE_IP" \
        "echo '$SSHPASS' | sudo -S pkill -x kvstore 2>/dev/null; \
         echo '$SSHPASS' | sudo -S pkill -x ebpf_proxy 2>/dev/null; \
         echo '$SSHPASS' | sudo -S rm -f $SLAVE_DIR/kvstore.dump $SLAVE_DIR/kvstore.aof* 2>/dev/null; true" 2>/dev/null
}

wait_master_up() {
    echo "[128] 等待 master 就绪 (PING)..."
    for i in $(seq 1 30); do
        /opt/redis-7.2.9/bin/redis-cli -p 5160 PING 2>/dev/null | grep -q PONG && return 0
        sleep 1
    done
    echo "ERROR: master 未就绪"; tail -10 "$MASTER_LOG"; exit 1
}

# ── 1. 清理两端 ──
cleanup_128
cleanup_129
sleep 1

# ── 2. 构建必要产物 ──
echo "[128] 构建 kvstore / ebpf_proxy / test_repl_5w5w"
make kvstore build/ebpf_proxy tests/test_repl_5w5w >/dev/null 2>&1 || { echo "构建失败"; exit 1; }

# ── 3. 同步最新二进制到 129 ──
echo "[129] 同步最新 kvstore 二进制"
sshpass -p "$SSHPASS" scp -o StrictHostKeyChecking=no "$PROJ/kvstore" \
    "$SLAVE_USER@$SLAVE_IP:$SLAVE_DIR/kvstore" 2>/dev/null
sshpass -p "$SSHPASS" ssh -o StrictHostKeyChecking=no "$SLAVE_USER@$SLAVE_IP" \
    "chmod +x $SLAVE_DIR/kvstore" 2>/dev/null

# ── 4. 启动 master (root, 自动拉起 ebpf-proxy) ──
echo "[128] 启动 master (root)..."
echo "$SSHPASS" | sudo -S -k sh -c "./kvstore kvstore.conf --role master > $MASTER_LOG 2>&1 & echo \$!" 2>/dev/null
sleep 2
wait_master_up
echo "  master OK (pid=$(pgrep -x kvstore | head -1))"

# ── 5. 运行测试 (后台) ──
echo "[128] 运行 test_repl_5w5w..."
./tests/test_repl_5w5w --config tests/test.conf > "$TEST_LOG" 2>&1 &
TEST_PID=$!

# ── 6. 等测试进入 Phase 2 (等待 Slave 连接), 最多等 2 分钟 ──
echo "  等待测试预存数据并进入 '等待 Slave 连接'..."
slave_started=0
for i in $(seq 1 60); do
    if grep -q "等待 Slave 连接" "$TEST_LOG" 2>/dev/null; then
        echo "  测试已就绪，启动 slave..."
        sshpass -p "$SSHPASS" ssh -o StrictHostKeyChecking=no "$SLAVE_USER@$SLAVE_IP" \
            "cd $SLAVE_DIR && echo '$SSHPASS' | sudo -S -k bash -c 'ulimit -l unlimited; ./kvstore kvstore.conf --role slave > $SLAVE_LOG 2>&1 & echo slave_pid=\$!'" 2>/dev/null
        slave_started=1
        break
    fi
    sleep 2
done

if [ "$slave_started" -eq 0 ]; then
    echo "ERROR: 测试未进入等 slave 阶段（2 分钟超时）"
    tail -20 "$TEST_LOG"
    cleanup_128; cleanup_129
    exit 1
fi

# ── 7. 等测试完成 ──
echo "  等待测试完成..."
wait "$TEST_PID" 2>/dev/null
TEST_EXIT=$?

# ── 8. 汇总 + 清理 ──
echo ""
echo "================ 测试结果 ================"
grep -E "PASS:|FAIL:|全量同步|增量同步追赶|验证:|预存:" "$TEST_LOG" | tail -10
echo "==========================================="

cleanup_128
cleanup_129
echo "资源已清理。测试退出码: $TEST_EXIT"
exit $TEST_EXIT
