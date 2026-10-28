#!/bin/bash
# ============================================================
# test_commit_gate_faults.sh — Commit Gate 故障注入 / 一致性回归
#
# 需要 root（加载 BPF）以及已构建的 kvstore / ebpf_proxy / capture bpf.o。
# 非 root 环境会打印 SKIP 并以 0 退出，方便 CI 无特权时跳过。
#
# 覆盖（方案 §2.1）：
#   1. 正常 ebpf+tcp 增量：pipeline + 随机分片发送（半包），最终一致
#   2. kill -9 ebpf-proxy：心跳过期 → session invalid → proxy 重启 → 重新同步
#   3. kill -9 slave：重连后 partial/full 追平，主从内容一致
#   4. 多轮循环（默认 5 轮），每轮核对 key-value / offset / commit 指标
#
# 用法：
#   sudo kvs/.../tools/tests/test_commit_gate_faults.sh [kvstore 二进制]
# 环境变量：
#   KVS_TEST_MASTER_PORT(7700) KVS_TEST_SLAVE_PORT(7701)   # 必须满足 slave=master+1
#   KVS_TEST_ROUNDS(5) KVS_TEST_EBPF_PIN(/sys/fs/bpf/kvs-commit-faults)
# ============================================================
set -u

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/kvstore}
PROXY_BIN=$ROOT/build/ebpf_proxy
CAPTURE_OBJ=$ROOT/build/replication/bpf/repl_client_capture.bpf.o
PIN=${KVS_TEST_EBPF_PIN:-/sys/fs/bpf/kvs-commit-faults}
MPORT=${KVS_TEST_MASTER_PORT:-7700}
SPORT=${KVS_TEST_SLAVE_PORT:-7701}
ROUNDS=${KVS_TEST_ROUNDS:-5}
DIR=$(mktemp -d /tmp/kvs-commit-faults.XXXXXX)
FAIL=0
KEYS_PER_ROUND=80

if [ "$(id -u)" -ne 0 ]; then
    echo "SKIP: Commit Gate fault injection requires root (BPF)."
    rm -rf "$DIR"
    exit 0
fi
if [ ! -x "$BIN" ] || [ ! -x "$PROXY_BIN" ] || [ ! -f "$CAPTURE_OBJ" ]; then
    echo "SKIP: missing artifacts (build kvstore, ebpf-proxy and capture bpf.o first)."
    rm -rf "$DIR"
    exit 0
fi
if [ "$SPORT" -ne $((MPORT + 1)) ]; then
    echo "NOTE: proxy connects to slave at master_port+1; using $MPORT/$((MPORT + 1))."
    SPORT=$((MPORT + 1))
fi

Cleanup() {
    [ -n "${MPID:-}" ] && kill -9 "$MPID" 2>/dev/null
    [ -n "${SPID:-}" ] && kill -9 "$SPID" 2>/dev/null
    if [ -f "$DIR/master.dump.ebpfproxy.pid" ]; then
        kill -9 "$(cat "$DIR/master.dump.ebpfproxy.pid" 2>/dev/null)" 2>/dev/null
    fi
    pkill -9 -f "$PROXY_BIN" 2>/dev/null
    wait 2>/dev/null
    if [ "$FAIL" -ne 0 ] && [ -d "$DIR" ]; then
        keep=/tmp/kvs-commit-faults-last
        rm -rf "$keep"; mkdir -p "$keep"
        cp -f "$DIR"/*.log "$keep"/ 2>/dev/null
        echo "logs kept in $keep"
    fi
    rm -rf "$PIN" "$DIR"
}
trap Cleanup EXIT

Fail() { echo "FAIL: $*"; FAIL=1; }
Info() { echo "== $*"; }

printf 'kprobe_enabled=0\n' > "$DIR/base.conf"
rm -rf "$PIN"; mkdir -p "$PIN"

Resp() { # <port> <cmd|info> <args...>
    python3 - "$@" <<'PY'
import socket, sys
port = int(sys.argv[1]); mode = sys.argv[2]; args = sys.argv[3:]
try:
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
except OSError as e:
    print("CONNECT_FAIL", e); raise SystemExit(2)

def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)

s.sendall(b"*1\r\n$4\r\nINFO\r\n" if mode == "info" else enc([x.encode() for x in args]))
s.settimeout(0.8)
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

Field() { echo "$1" | tr -d '\r' | awk -F: -v k="$2" '$1==k {print $2; exit}'; }

StartMaster() {
    "$BIN" --role master --port "$MPORT" --dump "$DIR/master.dump" --aof "$DIR/master.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
        --ebpf-pin-path "$PIN" --ebpf-proxy-bin "$PROXY_BIN" \
        --ebpf-client-capture-obj "$CAPTURE_OBJ" \
        --net reactor --config "$DIR/base.conf" >"$DIR/master.log" 2>&1 &
    MPID=$!
    for _ in $(seq 1 80); do
        Resp "$MPORT" cmd PING >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

StartSlave() {
    "$BIN" --role slave --port "$SPORT" --master-host 127.0.0.1 --master-port "$MPORT" \
        --dump "$DIR/slave.dump" --aof "$DIR/slave.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
        --net reactor --config "$DIR/base.conf" >"$DIR/slave.log" 2>&1 &
    SPID=$!
    for _ in $(seq 1 80); do
        Resp "$SPORT" cmd PING >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

# pipeline + 随机分片：同时覆盖半包重组与多命令同 recv。
WriteRound() { # <round>
    python3 - "$MPORT" "$1" "$KEYS_PER_ROUND" <<'PY'
import random, socket, sys, time
port, rnd, n = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
s = socket.create_connection(("127.0.0.1", port), timeout=10)
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
buf = b""
for i in range(n):
    key = ("cg:r%02d:k%04d" % (rnd, i)).encode()
    val = (("v%02d-%04d-" % (rnd, i)) + "x" * random.randint(0, 400)).encode()
    buf += enc([b"SET", key, val])
    buf += enc([b"GET", key])
    # 随机把当前字节流切成 1~3 段发送，制造半包
    parts = random.randint(1, 3)
    step = max(1, len(buf) // parts)
    for off in range(0, len(buf), step):
        s.sendall(buf[off:off + step])
        if random.random() < 0.15:
            time.sleep(0.001)
    buf = b""
s.settimeout(10)
want = b"+OK\r\n" * n
got = b""
while len(got) < len(want):
    x = s.recv(65536)
    if not x:
        break
    got += x
s.close()
sys.exit(0 if got.count(b"+OK\r\n") >= n else 1)
PY
}

WaitConverged() { # <max_iterations>
    local applied="" moff=""
    for _ in $(seq 1 "${1:-100}"); do
        applied=$(Field "$(Resp "$SPORT" info)" slave_repl_applied_offset)
        moff=$(Field "$(Resp "$MPORT" info)" master_repl_offset)
        if [ -n "$applied" ] && [ -n "$moff" ] && [ "$applied" = "$moff" ]; then
            return 0
        fi
        sleep 0.2
    done
    echo "   applied=${applied:-?} master=${moff:-?}"
    return 1
}

VerifyKeys() { # <round>
    python3 - "$MPORT" "$SPORT" "$1" "$KEYS_PER_ROUND" <<'PY'
import socket, sys
mp, sp, rnd, n = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
def get(port, key):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(enc([b"GET", key.encode()]))
    d = b""
    while not d.endswith(b"\r\n"):
        x = s.recv(65536)
        if not x: break
        d += x
    s.close()
    return d
bad = 0
for i in range(n):
    k = "cg:r%02d:k%04d" % (rnd, i)
    a, b = get(mp, k), get(sp, k)
    if a != b:
        bad += 1
        if bad <= 5:
            print("   mismatch", k, "master=", a[:80], "slave=", b[:80])
print("   compared %d keys, mismatches=%d" % (n, bad))
sys.exit(1 if bad else 0)
PY
}

Info "start master :$MPORT (ebpf+tcp)"; StartMaster || { echo "master start failed"; exit 1; }
Info "start slave  :$SPORT"; StartSlave  || { echo "slave start failed"; exit 1; }

# 等首个 session 建立并让 commit 通道连通
for _ in $(seq 1 100); do
    INFO_M=$(Resp "$MPORT" info)
    [ "$(Field "$INFO_M" connected_slaves)" = "1" ] && \
        [ "$(Field "$INFO_M" repl_commit_channel)" = "1" ] && break
    sleep 0.2
done
[ "$(Field "$(Resp "$MPORT" info)" repl_commit_active)" = "1" ] || Fail "commit gate not active"
[ "$(Field "$(Resp "$MPORT" info)" repl_commit_channel)" = "1" ] || Fail "commit channel not connected"

for r in $(seq 1 "$ROUNDS"); do
    Info "round $r: pipeline writes with fragmented sends"
    WriteRound "$r" || Fail "write round $r failed"
    WaitConverged 150 || Fail "round $r did not converge"
    VerifyKeys "$r" || Fail "round $r data mismatch"

    INFO_M=$(Resp "$MPORT" info)
    WRITES=$(Field "$INFO_M" repl_commit_writes)
    DESYNC=$(Field "$INFO_M" repl_commit_desync)
    GAP=$(Field "$INFO_M" repl_commit_gap)
    [ -n "$WRITES" ] && [ "$WRITES" -gt 0 ] || Fail "commit gate produced no WRITE descriptors"
    [ "${DESYNC:-0}" = "0" ] || Fail "unexpected commit desync=$DESYNC"
    [ "${GAP:-0}" = "0" ] || Fail "unexpected commit gap=$GAP"

    Info "round $r: kill -9 ebpf-proxy"
    PROXY_PID=$(cat "$DIR/master.dump.ebpfproxy.pid" 2>/dev/null || true)
    if [ -n "${PROXY_PID:-}" ]; then
        kill -9 "$PROXY_PID" 2>/dev/null
    else
        pkill -9 -f "$PROXY_BIN" 2>/dev/null
    fi
    WriteRound $((r + 100)) || Fail "write after proxy kill failed"
    # 心跳过期 → session invalid → proxy 重启（冷却 5s）→ slave 重连
    for _ in $(seq 1 120); do
        INFO_M=$(Resp "$MPORT" info)
        [ "$(Field "$INFO_M" repl_commit_channel)" = "1" ] && \
            [ "$(Field "$INFO_M" connected_slaves)" = "1" ] && break
        sleep 0.25
    done
    WaitConverged 200 || Fail "no convergence after proxy kill (round $r)"
    VerifyKeys $((r + 100)) || Fail "data mismatch after proxy kill (round $r)"
    INV=$(Field "$(Resp "$MPORT" info)" repl_session_invalid_count)
    [ -n "$INV" ] && [ "$INV" -ge 1 ] || Fail "proxy kill did not invalidate session"
    LAST=$(Field "$(Resp "$MPORT" info)" repl_last_invalid_reason)
    echo "   session_invalid_count=$INV last_reason=$LAST"

    Info "round $r: kill -9 slave"
    kill -9 "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; SPID=
    WriteRound $((r + 200)) || Fail "write while slave down failed"
    StartSlave || Fail "slave restart failed (round $r)"
    WaitConverged 200 || Fail "no convergence after slave restart (round $r)"
    VerifyKeys $((r + 200)) || Fail "data mismatch after slave restart (round $r)"
done

Info "final INFO (master)"
Resp "$MPORT" info | tr -d '\r' | grep -E \
    "master_repl_offset|connected_slaves|repl_commit_|repl_session_invalid|repl_last_invalid" || true
Info "final INFO (slave)"
Resp "$SPORT" info | tr -d '\r' | grep -E \
    "slave_repl_applied_offset|slave_repl_durable_offset|repl_last_invalid" || true

if [ "$FAIL" -ne 0 ]; then
    echo "RESULT: FAIL (logs: $DIR/master.log / $DIR/slave.log)"
    echo "NOTE: logs are removed by the EXIT trap; rerun with the trap disabled to inspect."
    exit 1
fi
echo "RESULT: PASS"
