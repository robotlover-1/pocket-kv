#!/usr/bin/env bash
# run_throughput_compare.sh — RDMA vs sendfile vs iperf3 吞吐量对比
#
# 用法:
#   bash run_throughput_compare.sh
#
# 环境变量:
#   HOST          对端 IP（默认: 本机 loopback 测试，自动检测）
#   IB_DEV        IB 设备 (默认: rxe0)
#   IB_PORT       IB 端口 (默认: 1)
#   GID_IDX       GID 索引 (默认: 1)
#   PORT_BASE     起始端口 (默认: 18516)
#   LOCAL_TEST    1 = 本机自测，0 = 需要两台机器 (默认: 1)
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
IB_DEV="${IB_DEV:-rxe0}"
IB_PORT="${IB_PORT:-1}"
GID_IDX="${GID_IDX:-1}"
PORT_BASE="${PORT_BASE:-18516}"
LOCAL_TEST="${LOCAL_TEST:-1}"

# payload 大小列表（bytes）
SIZES=(4096 65536 262144 1048576)

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${RED}[WARN]${NC} $*"; }

# ---- 检查依赖 ----
check_dep() {
    local cmd="$1"
    if ! command -v "$cmd" &>/dev/null; then
        warn "$cmd 未安装，相关测试将跳过"
        return 1
    fi
    return 0
}

# ---- iperf3 TCP 测试 ----
run_iperf3() {
    info "--- iperf3 TCP 基准测试 ---"

    if ! check_dep iperf3; then
        echo "iperf3,ALL,SKIP,-" >> "$RESULT_FILE"
        return
    fi

    local port=$((PORT_BASE + 10))

    if [[ "$LOCAL_TEST" == "1" ]]; then
        # 本机测试
        iperf3 -s -p "$port" -1 &
        local pid=$!
        sleep 1
        local result
        result=$(iperf3 -c 127.0.0.1 -p "$port" -t 5 -f m 2>&1) || true
        wait "$pid" 2>/dev/null || true
    else
        # 远端测试（需要手动在对面运行 iperf3 -s）
        info "请在对端机器运行: iperf3 -s -p $port"
        local result
        result=$(iperf3 -c "$HOST" -p "$port" -t 5 -f m 2>&1) || true
    fi

    # 解析结果
    local bps
    bps=$(echo "$result" | grep -oP '[\d.]+ Mbits/sec' | tail -1 | awk '{print $1}')
    if [[ -z "$bps" ]]; then
        bps=$(echo "$result" | grep -oP '[\d.]+ Gbits/sec' | tail -1 | awk '{print $1}')
        if [[ -n "$bps" ]]; then
            bps=$(echo "$bps * 1000" | bc -l)
        else
            bps="N/A"
        fi
    fi

    if [[ "$bps" != "N/A" ]]; then
        local bps_num
        bps_num=$(printf "%.0f" "$(echo "$bps * 1000000" | bc -l)")
        echo "iperf3_tcp,ALL,$bps_num,$bps Mbits/sec" >> "$RESULT_FILE"
    else
        echo "iperf3_tcp,ALL,SKIP,-" >> "$RESULT_FILE"
    fi
}

# ---- RDMA 测试 ----
run_rdma() {
    info "--- RDMA 吞吐量测试 ---"

    if ! check_dep ibv_rc_pingpong; then
        warn "libibverbs 工具未安装"
    fi

    local server_pid=""

    for size in "${SIZES[@]}"; do
        local iters=5000
        if [[ $size -ge 1048576 ]]; then
            iters=1000
        fi

        for mode in write send; do
            local port=$((PORT_BASE + 20))
            local server_log="/tmp/rdma_server_${mode}_${size}.log"

            info "RDMA $mode, size=$size, iters=$iters"

            if [[ "$LOCAL_TEST" == "1" ]]; then
                # 本机：先启动服务端
                ./test_rdma_throughput --server --ib-dev "$IB_DEV" --ib-port "$IB_PORT" \
                    --gid-idx "$GID_IDX" --port "$port" --size "$size" \
                    > "$server_log" 2>&1 &
                server_pid=$!
                sleep 0.5

                # 客户端
                ./test_rdma_throughput --host "$HOST" --ib-dev "$IB_DEV" --ib-port "$IB_PORT" \
                    --gid-idx "$GID_IDX" --port "$port" --size "$size" --iters "$iters" \
                    --mode "$mode" 2>&1 || true

                wait "$server_pid" 2>/dev/null || true
            else
                info "请在对端运行服务端:"
                info "  ./test_rdma_throughput --server --ib-dev $IB_DEV --ib-port $IB_PORT --gid-idx $GID_IDX --port $port --size $size"

                ./test_rdma_throughput --host "$HOST" --ib-dev "$IB_DEV" --ib-port "$IB_PORT" \
                    --gid-idx "$GID_IDX" --port "$port" --size "$size" --iters "$iters" \
                    --mode "$mode" 2>&1 || true
            fi
        done
    done
}

# ---- sendfile 测试 ----
run_sendfile() {
    info "--- sendfile 吞吐量测试 ---"

    for size in "${SIZES[@]}"; do
        local iters=5000
        if [[ $size -ge 1048576 ]]; then
            iters=1000
        fi

        local port=$((PORT_BASE + 30))

        info "sendfile, size=$size, iters=$iters"

        if [[ "$LOCAL_TEST" == "1" ]]; then
            ./test_sendfile_throughput --server --port "$port" &
            local server_pid=$!
            sleep 0.5

            ./test_sendfile_throughput --host "$HOST" --port "$port" \
                --size "$size" --iters "$iters" 2>&1 || true

            wait "$server_pid" 2>/dev/null || true
        else
            info "请在对端运行服务端:"
            info "  ./test_sendfile_throughput --server --port $port"

            ./test_sendfile_throughput --host "$HOST" --port "$port" \
                --size "$size" --iters "$iters" 2>&1 || true
        fi
    done
}

# ---- 打印汇总表 ----
print_summary() {
    echo ""
    echo "================================================================================"
    echo "  吞吐量对比汇总"
    echo "================================================================================"
    echo ""

    printf "%-16s  %-10s  %-16s  %-10s\n" \
           "mode" "payload" "throughput" "vs_iperf3"
    printf "%-16s  %-10s  %-16s  %-10s\n" \
           "----------------" "----------" "----------------" "----------"

    # 获取 iperf3 基准值
    local baseline
    baseline=$(grep "^iperf3_tcp," "$RESULT_FILE" 2>/dev/null | head -1 | cut -d, -f3)
    if [[ -z "$baseline" || "$baseline" == "SKIP" ]]; then
        baseline="N/A"
    fi

    # 从日志中提取 RDMA 和 sendfile 结果
    for tag in "rdma_write" "rdma_send" "sendfile"; do
        local tp
        tp=$(grep -E "吞吐量:" /tmp/perf_thru_*.log 2>/dev/null | tail -1 | grep -oP '[\d.]+\s*(Gbps|Mbps|Kbps|bps)' | head -1)
        if [[ -n "$tp" ]]; then
            local ratio="N/A"
            if [[ "$baseline" != "N/A" && "$baseline" != "0" ]]; then
                # 简单占位，具体数值需要从程序输出解析
                ratio="see above"
            fi
        fi
    done

    echo ""
    echo "详细数据见上方各测试输出。"
    echo ""
}

# ---- 全量同步文件传输对比 ----
FULLSYNC_FILE="${FULLSYNC_FILE:-/tmp/fullsync_dump.bin}"
FULLSYNC_CHUNK="${FULLSYNC_CHUNK:-262144}"

run_fullsync_file_compare() {
    info "--- 全量同步文件传输对比 (sendfile vs RDMA SEND) ---"

    if [[ ! -f "$FULLSYNC_FILE" ]]; then
        warn "dump 文件 $FULLSYNC_FILE 不存在，正在生成..."
        python3 gen_fullsync_dump.py --keys 1000000 --output "$FULLSYNC_FILE"
        if [[ ! -f "$FULLSYNC_FILE" ]]; then
            warn "生成 dump 文件失败，跳过全量同步测试"
            return
        fi
    fi

    local file_size
    file_size=$(stat -c%s "$FULLSYNC_FILE" 2>/dev/null || stat -f%z "$FULLSYNC_FILE" 2>/dev/null)
    info "测试文件: $FULLSYNC_FILE"
    info "文件大小: $(echo "scale=2; $file_size / 1048576" | bc) MB"
    info "RDMA chunk: $FULLSYNC_CHUNK bytes"

    # ---- iperf3 TCP 基准线 ----
    info "--- iperf3 TCP 基准 ---"
    check_dep iperf3 && {
        local port=$((PORT_BASE + 40))
        iperf3 -s -p "$port" -1 &
        local pid=$!
        sleep 1
        local result
        result=$(iperf3 -c 127.0.0.1 -p "$port" -t 5 -f m 2>&1) || true
        wait "$pid" 2>/dev/null || true
        local bps
        bps=$(echo "$result" | grep -oP '[\d.]+ Mbits/sec' | tail -1 | awk '{print $1}')
        if [[ -n "$bps" ]]; then
            local bps_num
            bps_num=$(printf "%.0f" "$(echo "$bps * 1000000" | bc -l)")
            echo "iperf3_tcp_file,ALL,$bps_num,$bps Mbits/sec" >> "$RESULT_FILE"
        fi
    }

    echo ""
    info "--- sendfile (--file 模式) ---"
    local sf_port=$((PORT_BASE + 31))
    ./test_sendfile_throughput --server --port "$sf_port" &
    local sf_srv_pid=$!
    sleep 0.5
    ./test_sendfile_throughput --host "$HOST" --port "$sf_port" --file "$FULLSYNC_FILE" 2>&1 || true
    wait "$sf_srv_pid" 2>/dev/null || true

    echo ""
    info "--- RDMA WRITE (--file 模式) ---"
    local rdma_port=$((PORT_BASE + 21))
    local server_log="/tmp/rdma_file_server.log"
    # server 端 WRITE 目标 MR 须 ≥ 文件大小（client WRITE 到 remote_addr+off，off 0..file_size），
    # 且 write_mode=1 强制 file-backed 目标（--out）。旧代码 --size $FULLSYNC_CHUNK 是越界 WRITE bug。
    ./test_rdma_throughput --server --port "$rdma_port" --size "$file_size" \
        --out "/tmp/rdma_file_target.bin" \
        > "$server_log" 2>&1 &
    local rdma_srv_pid=$!
    sleep 0.5
    ./test_rdma_throughput --host "$HOST" --port "$rdma_port" \
        --file "$FULLSYNC_FILE" --chunk-size "$FULLSYNC_CHUNK" 2>&1 || true
    wait "$rdma_srv_pid" 2>/dev/null || true
    cat "$server_log" 2>/dev/null

    echo ""
    ok "全量同步文件传输对比完成。"
}

# ---- Main ----
FULLSYNC_MODE=0
if [[ "${1:-}" == "--fullsync-file" ]]; then
    FULLSYNC_MODE=1
fi

RESULT_FILE="/tmp/perf_thru_results.csv"
echo "mode,payload,throughput_bps,throughput_str" > "$RESULT_FILE"

# 获取本机 IP
if [[ "$LOCAL_TEST" == "1" ]]; then
    HOST="127.0.0.1"
elif [[ "$HOST" == "127.0.0.1" ]]; then
    HOST=$(ip -4 addr show up | grep -oP 'inet \K[\d.]+' | grep -v 127.0.0.1 | head -1)
    if [[ -z "$HOST" ]]; then
        warn "无法检测本机 IP，使用 127.0.0.1"
        HOST="127.0.0.1"
        LOCAL_TEST=1
    fi
fi

info "测试配置: HOST=$HOST, IB_DEV=$IB_DEV, IB_PORT=$IB_PORT, GID_IDX=$GID_IDX"
info "LOCAL_TEST=$LOCAL_TEST"

if [[ "$FULLSYNC_MODE" == "1" ]]; then
    echo ""
    run_fullsync_file_compare
else
    echo ""
    echo "测试顺序: iperf3 → RDMA write → RDMA send → sendfile"
    echo ""

    run_iperf3
    echo ""

    run_rdma
    echo ""

    run_sendfile
    echo ""

    print_summary
fi

ok "所有测试完成。"
