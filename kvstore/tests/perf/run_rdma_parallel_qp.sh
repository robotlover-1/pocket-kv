#!/usr/bin/env bash
# run_rdma_parallel_qp.sh — 并行 QP RDMA 文件传输吞吐（配合 RPS 破单核 RX 上限）
#
# 背景（2026-08-12 实验结论，见 docs/rdma-one-sided-mtu-optimization.md + 记忆）：
#   跨机 siw RDMA 硬瓶颈 = e1000 单 RX 队列 + 单 IRQ + per-packet 软件处理，
#   单 QP 卡 ~100k pps。RPS 把接收端软中断散到多核 + 并行多 QP 后，
#   跨机 siw @MTU9000 从单连接 3.38 Gbps（37% of sendfile）提到 3.7-4.3 Gbps（44%）。
#   新瓶颈转为发送端 vCPU 饱和。本脚本把「RPS + 并行 N QP」固化为可复现测量。
#
# 用法：
#   # 本机 loopback 冒烟（N=1 退化为单 QP）
#   bash run_rdma_parallel_qp.sh --num-qp 1 --size 8388608 --file /tmp/smoke.bin
#
#   # 跨机（128 发送 → 129 接收）：
#   #   接收端 129 先起 N 个 server（--size = dump 文件字节数）：
#   #     bash run_rdma_parallel_qp.sh --serve --num-qp 4 --size 85000008
#   #   发送端 128：
#   #     sudo bash run_rdma_parallel_qp.sh --host 192.168.233.129 --num-qp 4 \
#   #         --file /tmp/fullsync_dump.bin --set-rps
#
# 参数：
#   --host IP      对端 IP（默认 127.0.0.1）
#   --num-qp N     并行 QP 数（默认 1）
#   --file PATH    dump 文件（默认 /tmp/fullsync_dump.bin，client 端读取）
#   --size BYTES   server 端 WRITE 目标 MR 大小（--serve 必填，= dump 文件大小）
#   --chunk BYTES  client 每 WRITE 的分块（默认 262144）
#   --port-base P  起始端口（默认 18516，每 QP 占 1 端口）
#   --set-rps      测前把本机 ens33 rx-0 rps_cpus 设为所有核（需 root）
#   --serve        本机起 N 个 server 并等待（接收端跑）
#
# 注意：test_rdma_throughput 的 client 是 RDMA WRITE 到 remote_addr+off（off 0..file_size），
#       所以 server 端 --size（MR 大小）必须 ≥ 文件大小，否则越界 WRITE 报 REMOTE_ACCESS_ERR。
set -euo pipefail

HOST="127.0.0.1"
NUM_QP=1
FILE="/tmp/fullsync_dump.bin"
SIZE=""
CHUNK=262144
PORT_BASE=18516
SET_RPS=0
SERVE=0
BIN="$(cd "$(dirname "$0")" && pwd)/test_rdma_throughput"

usage() {
    sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host)      HOST="$2";      shift 2 ;;
        --num-qp)    NUM_QP="$2";    shift 2 ;;
        --file)      FILE="$2";      shift 2 ;;
        --size)      SIZE="$2";      shift 2 ;;
        --chunk)     CHUNK="$2";     shift 2 ;;
        --port-base) PORT_BASE="$2"; shift 2 ;;
        --set-rps)   SET_RPS=1;      shift ;;
        --serve)     SERVE=1;        shift ;;
        -h|--help)   usage ;;
        *) echo "未知参数: $1" >&2; exit 1 ;;
    esac
done

# ---- RPS：把 ens33 单 RX 队列软中断散到所有核（需 root）----
set_rps() {
    local path="/sys/class/net/ens33/queues/rx-0/rps_cpus"
    if [ ! -w "$path" ]; then
        echo "[RPS] $path 不可写，需 root 运行（sudo bash $0 ...）" >&2
        return 1
    fi
    local mask
    mask=$(printf '%x' $(( (1 << $(nproc)) - 1 )))
    echo "$mask" > "$path"
    echo "[RPS] ens33/rx-0 rps_cpus = $(cat "$path")"
}

# ---- 解析 client 输出「吞吐量: X.XX Gbps」为 Gbps 数值 ----
parse_gbps() {
    echo "$1" | awk '{
        val=$2; unit=$3;
        if (unit == "Gbps") print val;
        else if (unit == "Mbps") print val/1000;
        else if (unit == "Kbps") print val/1000000;
        else print val/1000000000;
    }'
}

# ---- server 模式：起 N 个 server 等待（接收端跑）----
if [ "$SERVE" -eq 1 ]; then
    if [ -z "$SIZE" ]; then
        echo "[serve] 错误：--serve 必填 --size <总字节数>（= dump 文件大小，WRITE 目标 MR 须 ≥ 文件大小）" >&2
        exit 1
    fi
    echo "[serve] 起 $NUM_QP 个 RDMA server（size=$SIZE port=$PORT_BASE..$((PORT_BASE+NUM_QP-1))）"
    pids=()
    for i in $(seq 0 $((NUM_QP - 1))); do
        local_port=$((PORT_BASE + i))
        # test_rdma_throughput 只测单边 WRITE（write_mode=1），server 必须给 file-backed 目标 --out
        "$BIN" --server --port "$local_port" --size "$SIZE" \
            --out "/tmp/rdma_target_${local_port}.bin" \
            > "/tmp/rdma_server_${local_port}.log" 2>&1 &
        pids+=($!)
    done
    echo "[serve] server PIDs: ${pids[*]}（Ctrl-C 退出）"
    wait
    exit 0
fi

# ---- client 模式：起 N 个 client 并发，聚合吞吐 ----
if [ ! -f "$FILE" ]; then
    echo "[client] dump 文件 $FILE 不存在，生成 1M key 样例..."
    python3 "$(dirname "$0")/gen_fullsync_dump.py" --keys 1000000 --output "$FILE"
fi
file_size=$(stat -c%s "$FILE")
file_mb=$(awk "BEGIN{printf \"%.1f\", $file_size/1048576}")
echo "[client] file=$FILE ($file_mb MB) host=$HOST num_qp=$NUM_QP chunk=$CHUNK"

if [ -n "$SIZE" ] && [ "$SIZE" -lt "$file_size" ]; then
    echo "[client] 错误：--size $SIZE < 文件大小 $file_size，server 端 MR 须 ≥ 文件大小" >&2
    exit 1
fi

if [ "$SET_RPS" -eq 1 ]; then
    set_rps || true
fi

pids=()
for i in $(seq 0 $((NUM_QP - 1))); do
    local_port=$((PORT_BASE + i))
    log="/tmp/rdma_client_${local_port}.log"
    "$BIN" --host "$HOST" --port "$local_port" \
        --file "$FILE" --chunk-size "$CHUNK" > "$log" 2>&1 &
    pids+=($!)
done

for p in "${pids[@]}"; do
    wait "$p" || true
done

# ---- 聚合 ----
total=0
echo ""
echo "=== 各 QP 吞吐 ==="
for i in $(seq 0 $((NUM_QP - 1))); do
    local_port=$((PORT_BASE + i))
    log="/tmp/rdma_client_${local_port}.log"
    line=$(grep "吞吐量:" "$log" | tail -1)
    if [ -n "$line" ]; then
        g=$(parse_gbps "$line")
        total=$(awk "BEGIN{printf \"%.3f\", $total + $g}")
        echo "  QP$i (port $local_port): $line  → $g Gbps"
    else
        echo "  QP$i (port $local_port): 无结果（见 $log）" >&2
    fi
done
echo "----------------------------------------"
echo "聚合吞吐: $total Gbps  (N=$NUM_QP)"
