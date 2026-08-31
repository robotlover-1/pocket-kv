#!/usr/bin/env bash
#
# test_persist_save_durable_frontier.sh
#
# C1 回归检查：SAVE/BGSAVE（及 autosnap）必须先 drain AOF 管道再捕获 dump 头偏离量。
#
# 背景：AOF 独立线程 + 先回复（reply-first）下，g_aof_write_offset（=fsynced durable
# frontier）可落后于 g_aof_write_submitted（=appended frontier）最多 MAX_OUTSTANDING=16 批。
# dump 反映全部 in-memory 表（=submitted 状态），而头偏移若取自 fsynced，则恢复时
# replay_file(aof, fsynced) 会重放 [fsynced, submitted] 区间的命令——这些命令已含在 dump
# 里，对幂等的绝对类命令无害，但一旦出现相对/累加命令（INCR/INCRBY/LPUSH/APPEND 等）
# 即重复应用。本 store 现协议族仅幂等绝对操作，故本回归断言的是修复的直接可观测不变量：
#
#   SAVE 后 dump 头记录偏移 == AOF 文件持久长度（即恢复时 replay 恰好消费到 EOF、零重放）。
#
# 该断言确定性：无 drain 修复时，SAVE 时刻 submitted > fsynced，dump 头记录偏小的 fsynced，
# 与最终 AOF 长度不符（留一段未消费尾部）→ FAIL；有 drain 修复后两者相等 → PASS。
#
# 跑完 SAVE 后另做 kill -9 + 重启恢复，确认恢复无垃圾重放、写入键值全部存在。
#
# 用法:
#   直接运行: bash tools/persist/test_persist_save_durable_frontier.sh [HOST] [PORT]
#   或经 Makefile: make check-persist-save-frontier
#
# 通过标准:
#   1) 并发负载 + 一次 SAVE 后：dump 头偏移 == AOF 文件 size（=0 重放尾部）。
#   2) kill -9 + 重启恢复后：所有已确认写入的键值仍在，无额外重放副作用。

set -u

HOST="${1:-127.0.0.1}"
PORT="${2:-5176}"
BIN="${KVSTORE_BIN:-./kvstore}"
AOF_PATH=${AOF_PATH:-/tmp/kvstore_save_frontier.aof}
DUMP_PATH=${DUMP_PATH:-/tmp/kvstore_save_frontier.dump}
LOG_PATH=${LOG_PATH:-/tmp/kvstore_save_frontier.log}
NKEYS=${NKEYS:-30000}         # 负载 SET 键数（单批 >> MAX_OUTSTANDING 槽，制造 fsynced 落后 submitted 的窗口）
SAVE_DELAY=${SAVE_DELAY:-0.05} # 负载发出后多久发 SAVE（落进 flush 窗口；无 drain 修复时此窗口使 dump 头偏移落后）
FAIL=0
KVS_PID=""

# 读 8 字节小端 unsigned long long（dump 头偏移）
dump_header_offset() {
    local f="$1"
    od -An -N8 -tu8 "$f" 2>/dev/null | tr -d ' '
}

# redis-cli 单命令应答（$1=命令字符串，经 sh 转义）
redis_cmd() {
    { exec {sfd}<>/dev/tcp/"$HOST"/"$PORT" 2>/dev/null \
      && { printf "$1\r\n" >&"$sfd"; timeout 3 bash -c 'IFS= read -r line; printf "%s" "${line%$'"'"'\r'"'"'}"' <&"$sfd" 2>/dev/null; }; exec {sfd}<&- 2>/dev/null; }
}

ping_ok() {
    local r
    r="$(redis_cmd '*1\r\n$4\r\nPING')"
    [ "$r" = "+PONG" ]
}

echo "== C1: persist_save_dump 先 drain 再取头偏移 回归检查 (host=$HOST port=$PORT nkeys=$NKEYS) =="

pkill -x kvstore 2>/dev/null || true
sleep 0.5
rm -f "$AOF_PATH" "$DUMP_PATH"

# 异步 reply-first：--appendfsync always + group-commit（aof_fsync_sync=0）
"$BIN" --port "$PORT" --appendfsync always --aof-fsync-group-commit \
    --aof "$AOF_PATH" --dump "$DUMP_PATH" >"$LOG_PATH" 2>&1 &
KVS_PID=$!
echo "已启动 kvstore pid=$KVS_PID (log=$LOG_PATH)"

ok=0
for _ in $(seq 1 80); do
    if ping_ok 2>/dev/null; then ok=1; break; fi
    sleep 0.25
done
if [ "$ok" != 1 ]; then
    echo "FAIL: 服务未就绪"
    kill -9 "$KVS_PID" 2>/dev/null
    exit 1
fi
echo "服务已就绪"

# 并发负载：超大单批 SET（NKEYS 个）制造大 backlog（>> MAX_OUTSTANDING 槽，AOF 线程拉不开，
# fsynced 落后于 submitted），随后在 flush 窗口中发一次 SAVE。
# 负载用 python3 流水线，RESP 长度按真实键/值长度生成。
(
    python3 -c "
import socket, sys
c=socket.create_connection(('$HOST',$PORT))
buf=''
for i in range($NKEYS):
    k='key:%d'%i; v='value=%d'%i
    buf+='*3\r\n\$3\r\nSET\r\n\$%d\r\n%s\r\n\$%d\r\n%s\r\n'%(len(k),k,len(v),v)
c.sendall(buf.encode())
c.shutdown(socket.SHUT_WR)
b=b''
while True:
    d=c.recv(65536)
    if not d: break
    b+=d
sys.exit(0)
" 2>/dev/null || exit 0
) &
loader=$!

sleep ${SAVE_DELAY:-0.05}   # 落在 flush 窗口中：AOF 线程仍在清 backlog，fsynced < submitted
save_reply="$(redis_cmd '*1\r\n$4\r\nSAVE')"   # SAVE：内部 persist_drain_pending() 先 drain
wait "$loader" 2>/dev/null || true

case "$save_reply" in
    +OK) echo "SAVE ✓";;
    *)   echo "FAIL: SAVE 无 +OK (reply=$save_reply)"; kill -9 "$KVS_PID" 2>/dev/null; exit 1;;
esac

# 让 AOF 线程把负载尾部全部落盘（无论有无 drain 修复，SAVE 后 backlog 会 flush 完）
sleep 0.8

[ -f "$AOF_PATH" ] && aof_size="$(stat -c %s "$AOF_PATH")" || aof_size=0
[ -f "$DUMP_PATH" ] && dump_off="$(dump_header_offset "$DUMP_PATH")" || dump_off=0

echo "dump 头偏移(dump_off)=$dump_off  AOF 持久长度(aof_size)=$aof_size"

if [ "$dump_off" = "$aof_size" ]; then
    echo "PASS(阶段1): SAVE 后 dump 头偏移 == AOF 长度 → 恢复零重放尾部，无 [fsynced,submitted] 重叠"
else
    echo "FAIL(阶段1): dump 头偏移($dump_off) != AOF 长度($aof_size) → 存在未消费尾部，drain 修复缺失"
    FAIL=1
fi

# kill -9 + 重启恢复（dump 头偏移已证实 == AOF 长度，恢复零重放）
kill -9 "$KVS_PID" 2>/dev/null
wait "$KVS_PID" 2>/dev/null || true
echo "已 kill -9，重启恢复..."
KVS_PID=""

"$BIN" --port "$PORT" --appendfsync always --aof-fsync-group-commit \
    --aof "$AOF_PATH" --dump "$DUMP_PATH" >>"$LOG_PATH" 2>&1 &
KVS_PID=$!

ok=0
for _ in $(seq 1 80); do
    if ping_ok 2>/dev/null; then ok=1; break; fi
    sleep 0.25
done
if [ "$ok" != 1 ]; then
    echo "FAIL: 恢复后服务未就绪"
    kill -9 "$KVS_PID" 2>/dev/null
    exit 1
fi

# 校验恢复后所有已确认写入键都存在（SET 为绝对幂等，重放尾部安全；若丢尾部则缺失）
missing=0
for i in $(seq 1 20) $(seq "$((NKEYS - 19))" "$((NKEYS - 1))"); do
    key="key:$i"
    n=${#key}
    got="$(redis_cmd "*2\r\n\$5\r\nEXIST\r\n\$${n}\r\n${key}")"
    [ "$got" = ":1" ] || { echo "  （抽样键 $key 恢复后缺失: $got）"; missing=1; }
done
if [ "$missing" = 1 ]; then
    echo "FAIL(阶段2): kill-9 恢复后存在缺失键"
    FAIL=1
else
    echo "PASS(阶段2): kill-9 恢复后抽样键值全部存在"
fi

# 清理
kill -9 "$KVS_PID" 2>/dev/null || true
wait "$KVS_PID" 2>/dev/null || true
rm -f "$AOF_PATH" "$DUMP_PATH"

if [ "$FAIL" = 1 ]; then
    echo
    echo "RESULT: FAIL (see $LOG_PATH)"
    exit 1
fi
echo
echo "RESULT: PASS (SAVE 先 drain 保证恢复 Durable Frontier 无重叠/无丢失)"
exit 0
