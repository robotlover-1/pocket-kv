#!/bin/bash
# ============================================================
# test_commit_gate_faults.sh — Commit Gate 故障注入 / 一致性回归
#
# 需要 root（加载 BPF）以及已构建的 kvstore / ebpf_proxy / capture bpf.o。
# 非 root 环境会打印 SKIP 并以 0 退出，方便 CI 无特权时跳过。
#
# 覆盖（方案 §2.1 / CG-03）：
#   1. 正常 ebpf+tcp 增量：pipeline + 确定性随机分片（半包），最终一致
#   2. chunked GET 严格解析 RESP Bulk String，完整比对字节值（不再只读首行）
#   3. 同长度不同值 / 空值 / 缺失 key / 30KB / 31KB value
#      注意：>32KB 单命令会触发 ITER_UBUF 满载 recv 的 capture fault，
#      走 fail-closed + fullsync 恢复（见 docs/ebpf-replication-hardening.md）
#   4. kill -9 ebpf-proxy：心跳过期 → session invalid → proxy 重启 → 重新同步
#   5. kill -9 slave：重连后 partial/full 追平，主从内容一致
#   6. 多轮循环（默认 5 轮），每轮核对 key-value / offset / commit 指标
#
# 用法：
#   sudo tools/tests/test_commit_gate_faults.sh [kvstore 二进制]
# 环境变量：
#   KVS_TEST_MASTER_PORT(7700) KVS_TEST_SLAVE_PORT(7701)   # 必须 slave=master+1
#   KVS_TEST_ROUNDS(5) KVS_TEST_KEYS(80) KVS_TEST_SEED(20261028)
#   KVS_TEST_EBPF_PIN(/sys/fs/bpf/kvs-commit-faults)
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
KEYS_PER_ROUND=${KVS_TEST_KEYS:-80}
SEED=${KVS_TEST_SEED:-20261028}
DIR=$(mktemp -d /tmp/kvs-commit-faults.XXXXXX)
FAIL=0

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
    [ -n "${SPID:-}" ] && kill -CONT "$SPID" 2>/dev/null
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
echo "seed=$SEED rounds=$ROUNDS keys/round=$KEYS_PER_ROUND"

# ---- RESP 解码自检：同长度不同值必须可区分 ----
python3 - <<'PY' || { echo "RESP decoder self-test FAILED"; exit 1; }
class FakeSock:
    def __init__(self, b):
        self.b = b; self.i = 0
    def recv(self, n):
        if self.i >= len(self.b):
            return b""
        out = self.b[self.i:self.i + n]
        self.i += n
        return out

def recv_exact(sock, n):
    out = bytearray()
    while len(out) < n:
        part = sock.recv(n - len(out))
        if not part:
            raise EOFError("socket closed before full RESP payload")
        out.extend(part)
    return bytes(out)

def recv_line(sock, limit=128):
    out = bytearray()
    while not out.endswith(b"\r\n"):
        if len(out) >= limit:
            raise ValueError("RESP header too long")
        out.extend(recv_exact(sock, 1))
    return bytes(out[:-2])

def get_value(sock):
    header = recv_line(sock)
    if not header.startswith(b"$"):
        raise ValueError("unexpected RESP type: %r" % header)
    n = int(header[1:])
    if n == -1:
        return None
    if n < 0 or n > 64 * 1024 * 1024:
        raise ValueError("invalid bulk length: %d" % n)
    data = recv_exact(sock, n + 2)
    if not data.endswith(b"\r\n"):
        raise ValueError("invalid bulk terminator")
    return data[:-2]

assert get_value(FakeSock(b"$5\r\nAAAAA\r\n")) == b"AAAAA"
assert get_value(FakeSock(b"$5\r\nBBBBB\r\n")) == b"BBBBB"
assert get_value(FakeSock(b"$-1\r\n")) is None
assert get_value(FakeSock(b"$0\r\n\r\n")) == b""
print("RESP decoder self-test OK (same-length values distinguished)")
PY

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
        --net reactor --config "$DIR/base.conf" >>"$DIR/slave.log" 2>&1 &
    SPID=$!
    for _ in $(seq 1 80); do
        Resp "$SPORT" cmd PING >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

WriteRound() { # <round> [count]
    local n=${2:-$KEYS_PER_ROUND}
    python3 - "$MPORT" "$1" "$n" "$SEED" <<'PY' || return 1
import random, socket, sys, time
port, rnd, n, seed = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
rng = random.Random(seed * 1000 + rnd)
s = socket.create_connection(("127.0.0.1", port), timeout=10)
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
buf = b""
for i in range(n):
    key = ("cg:r%02d:k%04d" % (rnd, i)).encode()
    val = (("v%02d-%04d-" % (rnd, i)) + "x" * rng.randint(0, 400)).encode()
    buf += enc([b"SET", key, val])
    parts = rng.randint(1, 3)
    step = max(1, len(buf) // parts)
    for off in range(0, len(buf), step):
        s.sendall(buf[off:off + step])
        if rng.random() < 0.15:
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

WriteSpecial() {
    python3 - "$MPORT" "$SEED" <<'PY' || return 1
import random, socket, sys, time
port, seed = int(sys.argv[1]), int(sys.argv[2])
rng = random.Random(seed * 7 + 1)
s = socket.create_connection(("127.0.0.1", port), timeout=20)
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
items = [
    (b"cg:special:empty", b""),
    (b"cg:special:same:1", b"A" * 32),
    (b"cg:special:same:2", b"B" * 32),   # 同长度、不同值，用于验证工具不会假阳性
    (b"cg:special:big30", bytes(rng.getrandbits(8) for _ in range(30 * 1024))),
    (b"cg:special:big31", bytes(rng.getrandbits(8) for _ in range(31 * 1024))),
]
blob = b"".join(enc([b"SET", k, v]) for k, v in items)
for off in range(0, len(blob), 8192):
    s.sendall(blob[off:off + 8192])
    time.sleep(0.001)
s.settimeout(20)
want = b"+OK\r\n" * len(items)
got = b""
while len(got) < len(want):
    x = s.recv(65536)
    if not x:
        break
    got += x
s.close()
sys.exit(0 if got.count(b"+OK\r\n") >= len(items) else 1)
PY
}

# 严格完整读取 GET Bulk String，逐字节比对；支持空值/缺失 key/大 value。
VerifyValues() { # <master_port> <slave_port> <kind: round|special> <arg>
    python3 - "$1" "$2" "$3" "$4" "$KEYS_PER_ROUND" "$SEED" <<'PY' || return 1
import socket, sys
mp, sp = int(sys.argv[1]), int(sys.argv[2])
kind, arg = sys.argv[3], int(sys.argv[4])
count, seed = int(sys.argv[5]), int(sys.argv[6])

def recv_exact(sock, n):
    out = bytearray()
    while len(out) < n:
        part = sock.recv(n - len(out))
        if not part:
            raise EOFError("socket closed before full RESP payload")
        out.extend(part)
    return bytes(out)

def recv_line(sock, limit=128):
    out = bytearray()
    while not out.endswith(b"\r\n"):
        if len(out) >= limit:
            raise ValueError("RESP header too long")
        out.extend(recv_exact(sock, 1))
    return bytes(out[:-2])

def get_value(sock):
    header = recv_line(sock)
    if not header.startswith(b"$"):
        raise ValueError("unexpected RESP type: %r" % header)
    n = int(header[1:])
    if n == -1:
        return None
    if n < 0 or n > 64 * 1024 * 1024:
        raise ValueError("invalid bulk length: %d" % n)
    data = recv_exact(sock, n + 2)
    if not data.endswith(b"\r\n"):
        raise ValueError("invalid bulk terminator")
    return data[:-2]

def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)

if kind == "round":
    keys = ["cg:r%02d:k%04d" % (arg, i) for i in range(count)]
else:
    keys = ["cg:special:empty", "cg:special:same:1", "cg:special:same:2",
            "cg:special:big30", "cg:special:big31", "cg:special:missing"]

sm = socket.create_connection(("127.0.0.1", mp), timeout=20)
ss = socket.create_connection(("127.0.0.1", sp), timeout=20)
bad = 0
for k in keys:
    kb = k.encode()
    sm.sendall(enc([b"GET", kb]))
    ss.sendall(enc([b"GET", kb]))
    a = get_value(sm)
    b = get_value(ss)
    if a != b:
        bad += 1
        if bad <= 5:
            print("   mismatch %s: master_len=%s slave_len=%s master=%r slave=%r" %
                  (k, None if a is None else len(a), None if b is None else len(b),
                   None if a is None else a[:32], None if b is None else b[:32]))
sm.close(); ss.close()
print("   compared %d keys (%s), mismatches=%d" % (len(keys), kind, bad))
sys.exit(1 if bad else 0)
PY
}

WaitConverged() { # <max_iterations>
    local applied="" moff="" loading=""
    for _ in $(seq 1 "${1:-100}"); do
        local si
        si=$(Resp "$SPORT" info)
        applied=$(Field "$si" slave_repl_applied_offset)
        loading=$(Field "$si" slave_fullsync_loading)
        moff=$(Field "$(Resp "$MPORT" info)" master_repl_offset)
        # 注意：FULLRESYNC 一开始就把 applied 设成快照边界，必须等 loading=0
        # 才表示 dump 已真正加载进内存，否则 GET 会是空。
        if [ -n "$applied" ] && [ -n "$moff" ] && [ "$applied" = "$moff" ] &&
           [ "$loading" = "0" ]; then
            return 0
        fi
        sleep 0.2
    done
    echo "   applied=${applied:-?} master=${moff:-?} loading=${loading:-?}"
    return 1
}

Info "start master :$MPORT (ebpf+tcp)"; StartMaster || { echo "master start failed"; exit 1; }
Info "start slave  :$SPORT"; StartSlave  || { echo "slave start failed"; exit 1; }

for _ in $(seq 1 100); do
    INFO_M=$(Resp "$MPORT" info)
    [ "$(Field "$INFO_M" connected_slaves)" = "1" ] && \
        [ "$(Field "$INFO_M" repl_commit_channel)" = "1" ] && break
    sleep 0.2
done
[ "$(Field "$(Resp "$MPORT" info)" repl_commit_active)" = "1" ] || Fail "commit gate not active"
[ "$(Field "$(Resp "$MPORT" info)" repl_commit_channel)" = "1" ] || Fail "commit channel not connected"

Info "special values: empty/same-length/30KB/31KB/missing (<=32KB capture envelope)"
WriteSpecial || Fail "special value write failed"
WaitConverged 200 || Fail "special values did not converge"
VerifyValues "$MPORT" "$SPORT" special 0 || Fail "special value verification failed"

for r in $(seq 1 "$ROUNDS"); do
    Info "round $r: pipeline writes with fragmented sends (seed=$SEED)"
    WriteRound "$r" || Fail "write round $r failed"
    WaitConverged 200 || Fail "round $r did not converge"
    VerifyValues "$MPORT" "$SPORT" round "$r" || Fail "round $r data mismatch"

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
    for _ in $(seq 1 120); do
        INFO_M=$(Resp "$MPORT" info)
        [ "$(Field "$INFO_M" repl_commit_channel)" = "1" ] && \
            [ "$(Field "$INFO_M" connected_slaves)" = "1" ] && break
        sleep 0.25
    done
    WaitConverged 250 || Fail "no convergence after proxy kill (round $r)"
    VerifyValues "$MPORT" "$SPORT" round $((r + 100)) || Fail "data mismatch after proxy kill (round $r)"
    INV=$(Field "$(Resp "$MPORT" info)" repl_session_invalid_count)
    [ -n "$INV" ] && [ "$INV" -ge 1 ] || Fail "proxy kill did not invalidate session"
    LAST=$(Field "$(Resp "$MPORT" info)" repl_last_invalid_reason)
    echo "   session_invalid_count=$INV last_reason=$LAST"

    Info "round $r: kill -9 slave"
    kill -9 "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; SPID=
    WriteRound $((r + 200)) || Fail "write while slave down failed"
    StartSlave || Fail "slave restart failed (round $r)"
    WaitConverged 250 || Fail "no convergence after slave restart (round $r)"
    VerifyValues "$MPORT" "$SPORT" round $((r + 200)) || Fail "data mismatch after slave restart (round $r)"
done

Info "final INFO (master)"
Resp "$MPORT" info | tr -d '\r' | grep -E \
    "master_repl_offset|connected_slaves|repl_commit_|repl_session_invalid|repl_last_invalid" || true
Info "final INFO (slave)"
Resp "$SPORT" info | tr -d '\r' | grep -E \
    "slave_repl_applied_offset|slave_repl_durable_offset|repl_last_invalid" || true

if [ "$FAIL" -ne 0 ]; then
    echo "RESULT: FAIL"
    exit 1
fi
echo "RESULT: PASS"
