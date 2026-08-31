#!/usr/bin/env bash
#
# test_persist_purge_conn.sh
#
# 回归检查：客户端在 AOF 批 fsync 尚未完成时断开连接，随后由服务端 reap/purge，
# 验证 persist_purge_conn 能把该 conn 从 in-flight 槽的 release 列表正确移除，
# 不产生 use-after-free / 崩溃（fallout：socket 关闭但 UAF 导致 segfault 或 hang）。
#
# 该脚本自托拉模式：自动后台启动一个 kvstore(--aof-fsync-per-command 异步逐批)，
# 跑完验证后优雅关闭（SIGTERM，途经 persist_close 的 drain 所有 in-flight 槽）。
#
# 用法:
#   直接运行: bash tools/persist/test_persist_purge_conn.sh [HOST] [PORT]
#   或经 Makefile: make check-persist-purge
#
# 通过标准:
#   1) 大量「写入批在 fsync 在途→立即断开」期间，服务端不崩溃（PING 持续可达，SAVE 返回 OK）。
#   2) 优雅关闭时 persist_close 能 drain 全部 in-flight 槽，不崩溃、干净退出。

set -u

HOST="${1:-127.0.0.1}"
PORT="${2:-5174}"
ITER=${ITER:-60}
VALUE_LEN=${VALUE_LEN:-65536}    # 64KB，group-commit 下留在当前槽不触发 4MB flush
BIN="${KVSTORE_BIN:-./kvstore}"
AOF_PATH=${AOF_PATH:-/tmp/kvstore_purge_conn.aof}
DUMP_PATH=${DUMP_PATH:-/tmp/kvstore_purge_conn.dump}
LOG_PATH=${LOG_PATH:-/tmp/kvstore_purge_conn.log}
FAIL=0
KVS_PID=""

big_value="$( { printf '%*s' "$VALUE_LEN" ''; } | tr ' ' 'x' )"

resp_set() { # $1 key $2 <<fd
    local key="$1"
    local -r fd="$2"
    printf '*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' \
        "${#key}" "$key" "${#big_value}" "$big_value" >&"$fd"
}

# 打开连接、PING、读一行响应；成功返回 0
ping_ok() {
    local fd reply
    exec {fd}<>/dev/tcp/"$HOST"/"$PORT" 2>/dev/null || return 1
    printf '*1\r\n$4\r\nPING\r\n' >&"$fd"
    reply="$( timeout 2 bash -c 'IFS= read -r line; printf "%s" "${line%$'"'"'\r'"'"'}"' <&"$fd" 2>/dev/null )"
    exec {fd}>&-
    exec {fd}<&-
    [ "$reply" = "+PONG" ]
}

echo "== persist purge-conn 回归检查 (host=$HOST port=$PORT iters=$ITER valuelen=$VALUE_LEN) =="

# 自托拉：若端口无服务，则后台启动 kvstore(默认异步 group-commit)
# —— group-commit 下每条命令先留在当前槽 g_cur_slot(release 挂 conn)，紧跟的 conn 关闭
#    正在该槽 release 列表时即命中 persist_purge_conn 的目标窗口。
if ! ping_ok 2>/dev/null; then
    pkill -x kvstore 2>/dev/null || true
    sleep 0.5
    "$BIN" --port "$PORT" --appendfsync always \
        --aof "$AOF_PATH" --dump "$DUMP_PATH" \
        --aof-fsync-group-commit >"$LOG_PATH" 2>&1 &
    KVS_PID=$!
    echo "已启动 kvstore pid=$KVS_PID (log=$LOG_PATH)"
fi

# 等待服务就绪
ok=0
for _ in $(seq 1 80); do
    if ping_ok 2>/dev/null; then ok=1; break; fi
    sleep 0.25
done
if [ "$ok" != 1 ]; then
    echo "FAIL: 服务未就绪"
    [ -n "$KVS_PID" ] && kill -9 "$KVS_PID" 2>/dev/null
    exit 1
fi
echo "服务已就绪"

# 主循环：每条连接在同一 fd 上流水写入 BURST 条大命令(累计跨 AOF_SLOT_MAX，强制 flush，
# 生成一个多 MB 的 write+fsync 在途批)，随后同一连接立即断开。
# 该 conn 挂在在途槽的 release 列表上；断开时批仍在 fsync 在途 → 命中 persist_purge_conn 目标路径。
BURST=${BURST:-96}
for i in $(seq 1 "$ITER"); do
    exec {cfd}<>/dev/tcp/"$HOST"/"$PORT" 2>/dev/null || {
        echo "FAIL: 无法连接 (iter $i)"; FAIL=1; break; }
    for _j in $(seq 1 "$BURST"); do
        resp_set "purge:conn:$i:$((i * BURST + _j))" "$cfd"
    done
    exec {cfd}>&-
    exec {cfd}<&-
    # 给服务一小段时间把该在途批 fsync 落盘并 reap（此刻应已把被关闭 conn 从 release 列表移除）
    sleep 0.004
done

# 断开后仍应健康：PING 可达
if ! ping_ok 2>/dev/null; then
    echo "FAIL: 循环断开后 PING 不通（服务可能崩溃/UAF/hang）"
    FAIL=1
else
    echo "循环断开后服务仍可 PING ✓"
fi

# SAVE 确认内核仍健康
save_reply="$( { exec {sfd}<>/dev/tcp/"$HOST"/"$PORT" 2>/dev/null \
    && printf '*1\r\n$4\r\nSAVE\r\n' >&"$sfd" \
    && timeout 2 bash -c 'IFS= read -r line; printf "%s" "${line%$'"'"'\r'"'"'}"' <&"$sfd" 2>/dev/null; exec {sfd}<&- 2>/dev/null; } )"
case "$save_reply" in
    +OK) echo "SAVE ✓";;
    *)   echo "FAIL: SAVE 无 +OK (reply=$save_reply)"; FAIL=1;;
esac

# 优雅关闭：若非外部托管，则 SIGTERM 触发 persist_close drain 全部 in-flight 槽
if [ -n "$KVS_PID" ]; then
    echo "优雅关闭 pid=$KVS_PID (SIGTERM → persist_close drain)..."
    kill -TERM "$KVS_PID" 2>/dev/null
    for _ in $(seq 1 100); do
        kill -0 "$KVS_PID" 2>/dev/null || break
        sleep 0.05
    done
    if kill -0 "$KVS_PID" 2>/dev/null; then
        echo "FAIL: 优雅关闭超时未退出（drain 卡住）"
        FAIL=1
        kill -9 "$KVS_PID" 2>/dev/null
    else
        wait "$KVS_PID" 2>/dev/null
        echo "优雅关闭干净退出 ✓"
    fi
    rm -f "$AOF_PATH" "$DUMP_PATH"
fi

if [ "$FAIL" = 1 ]; then
    echo
    echo "RESULT: FAIL (see $LOG_PATH)"
    exit 1
fi
echo
echo "RESULT: PASS (no UAF/crash on conn-close-during-inflight purge)"
exit 0
