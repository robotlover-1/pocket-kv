#!/bin/bash
# ============================================================
# test_slave_crash_recovery.sh — Slave kill -9 重启恢复边界测试（非 root）
#
# 验证（方案 §14.4 / §28.5）：
#   1. 正常主从增量复制（TCP 路径，不依赖 eBPF）
#   2. kill -9 slave 后 master 继续写入 → backlog 覆盖断开区间
#   3. slave 重启后 REPLSYNC offset <= 实际恢复 frontier（dump 头 + AOF 回放）
#   4. 追平后数据与 master 一致（包括断开期间写入）
#
# 说明：本测试用 --repl-realtime-transport rdma 表示"普通 TCP 实时转发"
# （项目里 realtime=tcp 会走 ebpf+tcp 语义，需要 eBPF proxy）。
# kprobe_enabled=0 通过配置文件传入，避免 kprobe 健康检查干扰 TCP 副本。
#
# 用法：tools/tests/test_slave_crash_recovery.sh [kvstore 二进制路径]
# ============================================================
set -u

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/kvstore}
DIR=$(mktemp -d /tmp/kvs-crash-recovery.XXXXXX)
MPORT=${KVS_TEST_MASTER_PORT:-7700}
SPORT=${KVS_TEST_SLAVE_PORT:-7701}
FAIL=0

Cleanup() {
    [ -n "${MPID:-}" ] && kill -9 "$MPID" 2>/dev/null
    [ -n "${SPID:-}" ] && kill -9 "$SPID" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$DIR"
}
trap Cleanup EXIT

Fail() { echo "FAIL: $*"; FAIL=1; }
Info() { echo "== $*"; }

printf 'kprobe_enabled=0\n' > "$DIR/base.conf"

Resp() { # <port> <mode: cmd|info> <args...>
    python3 - "$@" <<'PY'
import socket, sys
port = int(sys.argv[1]); mode = sys.argv[2]; args = sys.argv[3:]
try:
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
except OSError as e:
    print("CONNECT_FAIL", e); raise SystemExit(2)

def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)

if mode == "info":
    s.sendall(b"*1\r\n$4\r\nINFO\r\n")
else:
    s.sendall(enc([x.encode() for x in args]))
s.settimeout(0.6)
d = b""
while True:
    try:
        x = s.recv(65536)
    except socket.timeout:
        break
    if not x:
        break
    d += x
s.close()
sys.stdout.write(d.decode(errors="replace"))
PY
}

StartMaster() {
    "$BIN" --role master --port "$MPORT" --dump "$DIR/master.dump" --aof "$DIR/master.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport rdma \
        --net reactor --config "$DIR/base.conf" >"$DIR/master.log" 2>&1 &
    MPID=$!
    for _ in $(seq 1 50); do
        Resp "$MPORT" cmd PING >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

StartSlave() {
    "$BIN" --role slave --port "$SPORT" --master-host 127.0.0.1 --master-port "$MPORT" \
        --dump "$DIR/slave.dump" --aof "$DIR/slave.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport rdma \
        --net reactor --config "$DIR/base.conf" >"$DIR/slave.log" 2>&1 &
    SPID=$!
    for _ in $(seq 1 50); do
        Resp "$SPORT" cmd PING >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

WriteRange() { # <from> <to>
    python3 - "$MPORT" "$1" "$2" <<'PY'
import socket, sys
port, a, b = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
s = socket.create_connection(("127.0.0.1", port), timeout=10)
def enc(x):
    return b"*%d\r\n" % len(x) + b"".join(b"$%d\r\n%s\r\n" % (len(v), v) for v in x)
for i in range(a, b):
    s.sendall(enc([b"SET", ("crash:k:%04d" % i).encode(), ("v%d" % i).encode()]))
# 读完全部 +OK（只读一个会提前关闭连接，导致最后几条丢失）
expected = b"+OK\r\n" * (b - a)
data = b""
while len(data) < len(expected):
    x = s.recv(65536)
    if not x: break
    data += x
s.close()
sys.exit(0 if data == expected else 1)
PY
}

Field() { # <info-text> <field>
    echo "$1" | tr -d '\r' | awk -F: -v k="$2" '$1==k {print $2; exit}'
}

[ -x "$BIN" ] || { echo "kvstore binary not found/executable: $BIN"; exit 1; }
command -v python3 >/dev/null || { echo "python3 required"; exit 1; }

Info "start master :$MPORT"; StartMaster || { echo "master start failed"; exit 1; }
Info "start slave  :$SPORT"; StartSlave  || { echo "slave start failed"; exit 1; }

Info "write 100 keys"; WriteRange 0 100 || Fail "initial write failed"
for _ in $(seq 1 50); do
    PRE_APPLIED=$(Field "$(Resp "$SPORT" info)" slave_repl_applied_offset)
    MASTER_OFF=$(Field "$(Resp "$MPORT" info)" master_repl_offset)
    [ -n "$PRE_APPLIED" ] && [ -n "$MASTER_OFF" ] && \
        [ "$PRE_APPLIED" = "$MASTER_OFF" ] && break
    sleep 0.1
done
[ -n "$PRE_APPLIED" ] && [ "$PRE_APPLIED" -gt 0 ] || Fail "slave did not apply initial writes"
[ "$(Resp "$SPORT" cmd GET crash:k:0099 | tr -d '\r')" = $'$3\nv99' ] || Fail "slave missing key before crash"

Info "kill -9 slave"
kill -9 "$SPID"; wait "$SPID" 2>/dev/null; SPID=

Info "write 100..160 while slave down"
WriteRange 100 160 || Fail "write while slave down failed"
sleep 1

Info "restart slave"
StartSlave || { Fail "slave restart failed"; }
sleep 2

INFO_AFTER=$(Resp "$SPORT" info)
FRONTIER=$(Field "$INFO_AFTER" slave_recovered_frontier)
RESUME=$(Field "$INFO_AFTER" slave_resume_offset)
APPLIED=$(Field "$INFO_AFTER" slave_repl_applied_offset)
echo "   recovered_frontier=$FRONTIER resume=$RESUME applied=$APPLIED"
[ -n "$FRONTIER" ] && [ "$FRONTIER" -gt 0 ] || Fail "recovered frontier is zero"
[ -n "$RESUME" ] && [ "$RESUME" -le "$FRONTIER" ] || Fail "resume > recovered frontier"

Info "wait for catch-up"
for _ in $(seq 1 60); do
    INFO_AFTER=$(Resp "$SPORT" info)
    APPLIED=$(Field "$INFO_AFTER" slave_repl_applied_offset)
    MASTER_OFF=$(Field "$(Resp "$MPORT" info)" master_repl_offset)
    [ -n "$APPLIED" ] && [ -n "$MASTER_OFF" ] && [ "$APPLIED" = "$MASTER_OFF" ] && break
    sleep 0.2
done
if [ "${APPLIED:-0}" != "${MASTER_OFF:-x}" ]; then
    Fail "slave applied ($APPLIED) != master offset ($MASTER_OFF)"
fi

Info "verify data"
for k in crash:k:0000 crash:k:0099 crash:k:0100 crash:k:0159; do
    idx=${k##*:}; idx=$((10#$idx)); val="v$idx"
    got=$(Resp "$SPORT" cmd GET "$k" | tr -d '\r')
    [ "$got" = "$(printf '$%d\n%s' "${#val}" "$val")" ] || Fail "slave GET $k => $got"
done

Info "slave INFO"
echo "$INFO_AFTER" | tr -d '\r' | grep -E \
    "slave_recovered_frontier|slave_resume_offset|slave_repl_applied_offset|repl_last_invalid_reason" || true

if [ "$FAIL" -ne 0 ]; then
    echo "RESULT: FAIL"
    exit 1
fi
echo "RESULT: PASS"
