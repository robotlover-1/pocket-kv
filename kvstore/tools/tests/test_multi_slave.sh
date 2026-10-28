#!/bin/bash
# ============================================================
# test_multi_slave.sh — 2-Slave ebpf+tcp 增量复制 / 故障隔离回归
#
# 需要 root（加载 BPF）以及已构建的 kvstore / ebpf_proxy / capture bpf.o。
# 非 root 环境打印 SKIP 并以 0 退出。
#
# 验证（docs/multi-slave-design.md §9）：
#   1. 两个 Slave 同时 fullsync 后都能收到增量，主/从内容一致
#   2. B kill -9 期间 A 持续复制；B 重启（强制 fullsync）后追平，A 不受影响
#   3. B 全量重同步期间继续写入：A 实时收到，B 完成后内容一致
#   4. kill ebpf-proxy：自动拉起后 A/B 各自恢复，不跨 session 重放
#
# 用法：
#   sudo tools/tests/test_multi_slave.sh [kvstore 二进制]
# 环境变量：
#   KVS_TEST_MASTER_PORT(7800) KVS_TEST_SLAVE_A_PORT(7810)
#   KVS_TEST_SLAVE_B_PORT(7811) KVS_TEST_KEYS(80) KVS_TEST_SEED(20261028)
#   KVS_TEST_BARRIER_HOLD_MS(4000) KVS_TEST_FAKE_PROXY_PORT(7999)
#
# 说明：fullsync=tcp + realtime=ebpf+tcp 时，proxy 直连 Slave **主端口**
# （主端口写路径推进 replicated offset），Slave 在 REPLSYNC 第 5 参数上报该端口。
# 因此同机两个 Slave 用不同主端口即可，无需额外 proxy 端口。
#   KVS_TEST_EBPF_PIN(/sys/fs/bpf/kvs-multi-slave)
# ============================================================
set -u

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/kvstore}
PROXY_BIN=$ROOT/build/ebpf_proxy
CAPTURE_OBJ=$ROOT/build/replication/bpf/repl_client_capture.bpf.o
PIN=${KVS_TEST_EBPF_PIN:-/sys/fs/bpf/kvs-multi-slave}
MPORT=${KVS_TEST_MASTER_PORT:-7800}
APORT=${KVS_TEST_SLAVE_A_PORT:-7810}
BPORT=${KVS_TEST_SLAVE_B_PORT:-7811}
KEYS=${KVS_TEST_KEYS:-80}
SEED=${KVS_TEST_SEED:-20261028}
BARRIER_HOLD_MS=${KVS_TEST_BARRIER_HOLD_MS:-4000}
FAKE_PROXY_PORT=${KVS_TEST_FAKE_PROXY_PORT:-7999}
DIR=$(mktemp -d /tmp/kvs-multi-slave.XXXXXX)
FAIL=0
MPID=""; APID=""; BPID=""

if [ "$(id -u)" -ne 0 ]; then
    echo "SKIP: multi-slave test requires root (BPF)."
    rm -rf "$DIR"
    exit 0
fi
if [ ! -x "$BIN" ] || [ ! -x "$PROXY_BIN" ] || [ ! -f "$CAPTURE_OBJ" ]; then
    echo "SKIP: missing artifacts (build kvstore, ebpf-proxy and capture bpf.o first)."
    rm -rf "$DIR"
    exit 0
fi

Cleanup() {
    for p in "$MPID" "$APID" "$BPID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null
    done
    if [ -f "$DIR/master.dump.ebpfproxy.pid" ]; then
        kill -9 "$(cat "$DIR/master.dump.ebpfproxy.pid" 2>/dev/null)" 2>/dev/null
    fi
    pkill -9 -f "$PROXY_BIN" 2>/dev/null
    wait 2>/dev/null
    if [ "$FAIL" -ne 0 ] && [ -d "$DIR" ]; then
        keep=/tmp/kvs-multi-slave-last
        rm -rf "$keep"; mkdir -p "$keep"
        cp -f "$DIR"/*.log "$keep"/ 2>/dev/null
        echo "logs kept in $keep"
    fi
    rm -rf "$PIN" "$DIR"
}
trap Cleanup EXIT

Fail() { echo "FAIL: $*"; FAIL=1; }
Info() { echo "== $*"; }

Field() { echo "$1" | tr -d '\r' | awk -F: -v k="$2" '$1==k {print $2; exit}'; }

# 简单 RESP 客户端：info <port>；get <port> <key>（输出原始 bulk 字节的 hex）
RespPy() {
    python3 - "$@" <<'PY'
import socket, sys, time
mode = sys.argv[1]
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
def conn(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.settimeout(2)
    return s
def recv_all(s):
    d = b""
    while True:
        try:
            x = s.recv(65536)
        except socket.timeout:
            break
        if not x:
            break
        d += x
    return d
if mode == "info":
    s = conn(int(sys.argv[2])); s.sendall(enc([b"INFO"])); d = recv_all(s); s.close()
    sys.stdout.write(d.decode(errors="replace"))
elif mode == "ping":
    s = conn(int(sys.argv[2])); s.sendall(enc([b"PING"])); d = recv_all(s); s.close()
    sys.exit(0 if d.startswith(b"+PONG") else 1)
elif mode == "fake-replica":
    # 发送 partial REPLSYNC 后只读 replay，不回 REPLACK/REPLDONE：
    # Master 应只拒绝这个副本（per-replica barrier timeout），不影响 A/B。
    port = int(sys.argv[2]); replid = sys.argv[3]; off = sys.argv[4]; pport = sys.argv[5]
    s = conn(port)
    s.sendall(enc([b"REPLSYNC", replid.encode(), off.encode(), off.encode(), pport.encode()]))
    s.settimeout(0.5)
    end = time.time() + 12
    while time.time() < end:
        try:
            if not s.recv(65536):
                break
        except socket.timeout:
            pass
    s.close()
PY
}

WaitPort() { # <port> <tries>
    for _ in $(seq 1 "${2:-80}"); do
        RespPy ping "$1" >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    return 1
}

StartMaster() {
    KVS_REPL_BARRIER_HOLD_MAX_MS="$BARRIER_HOLD_MS" \
    "$BIN" --role master --port "$MPORT" --dump "$DIR/master.dump" --aof "$DIR/master.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
        --ebpf-pin-path "$PIN" --ebpf-proxy-bin "$PROXY_BIN" \
        --ebpf-client-capture-obj "$CAPTURE_OBJ" \
        --net reactor --config "$DIR/base.conf" >"$DIR/master.log" 2>&1 &
    MPID=$!
    WaitPort "$MPORT" 120
}

StartSlave() { # <name> <port>
    local name=$1 port=$2
    "$BIN" --role slave --port "$port" --master-host 127.0.0.1 --master-port "$MPORT" \
        --dump "$DIR/$name.dump" --aof "$DIR/$name.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
        --net reactor --config "$DIR/base.conf" >>"$DIR/$name.log" 2>&1 &
    local pid=$!
    WaitPort "$port" 120 || return 1
    echo "$pid"
}

SlaveReady() { # <port>
    local info; info=$(RespPy info "$1")
    local loading link
    loading=$(Field "$info" slave_fullsync_loading)
    link=$(Field "$info" master_link)
    [ "$loading" = "0" ] && [ "$link" = "up" ]
}

WaitSynced() { # <port> <tries>
    for _ in $(seq 1 "${2:-200}"); do
        SlaveReady "$1" && return 0
        sleep 0.1
    done
    return 1
}

WriteKeys() { # <prefix> <count>
    python3 - "$MPORT" "$1" "$2" "$SEED" <<'PY' || return 1
import random, socket, sys, time
port, prefix, n, seed = int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
rng = random.Random(seed ^ hash(prefix))
s = socket.create_connection(("127.0.0.1", port), timeout=10)
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
for i in range(n):
    key = ("%s:%04d" % (prefix, i)).encode()
    val = (("v-%s-%04d-" % (prefix, i)) + "x" * rng.randint(0, 300)).encode()
    msg = enc([b"SET", key, val])
    # 半包/流水：拆成 1~3 段发送
    parts = rng.randint(1, 3)
    step = max(1, len(msg) // parts)
    for off in range(0, len(msg), step):
        s.sendall(msg[off:off + step])
    if rng.random() < 0.2:
        time.sleep(0.001)
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

CheckKeys() { # <prefix> <count> [check_b=1]  (逐字节比对；B 离线时可只比 master/A)
    local check_b=${3:-1}
    python3 - "$MPORT" "$APORT" "$BPORT" "$1" "$2" "$check_b" <<'PY' || return 1
import socket, sys
mp, ap, bp, prefix, n = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], int(sys.argv[5])
check_b = int(sys.argv[6])
def enc(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
def get_all(port, keys):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(b"".join(enc([b"GET", k.encode()]) for k in keys))
    s.settimeout(5)
    # 读取直到所有 key 的 RESP 完成为止：按 bulk 长度解析
    out = []
    buf = b""
    while len(out) < len(keys):
        x = s.recv(65536)
        if not x:
            break
        buf += x
        while len(out) < len(keys):
            if not buf.startswith(b"$"):
                return None
            nl = buf.find(b"\r\n")
            if nl < 0:
                break
            ln = int(buf[1:nl])
            if ln < 0:
                out.append(None)
                buf = buf[nl + 2:]
                continue
            need = nl + 2 + ln + 2
            if len(buf) < need:
                break
            out.append(buf[nl + 2:nl + 2 + ln])
            buf = buf[need:]
    s.close()
    return out
keys = ["%s:%04d" % (prefix, i) for i in range(n)]
a = get_all(mp, keys); b = get_all(ap, keys)
c = get_all(bp, keys) if check_b else b
if a is None or b is None or c is None:
    print("resp parse failed"); sys.exit(1)
for i, k in enumerate(keys):
    if a[i] != b[i] or (check_b and a[i] != c[i]):
        print("mismatch key=%s master=%r A=%r B=%r" % (k, a[i], b[i], c[i]))
        sys.exit(1)
sys.exit(0)
PY
}

printf 'kprobe_enabled=0\n' > "$DIR/base.conf"
rm -rf "$PIN"; mkdir -p "$PIN"

Info "start master :$MPORT"
StartMaster || { Fail "master did not start"; exit 1; }
Info "start slave A :$APORT"
APID=$(StartSlave slaveA "$APORT") || { Fail "slave A did not start"; exit 1; }
Info "start slave B :$BPORT"
BPID=$(StartSlave slaveB "$BPORT") || { Fail "slave B did not start"; exit 1; }

Info "wait both slaves initial fullsync"
WaitSynced "$APORT" 300 || Fail "slave A did not become ready"
WaitSynced "$BPORT" 300 || Fail "slave B did not become ready"

Info "round1: write $KEYS keys, both slaves live"
WriteKeys r1 "$KEYS" || Fail "round1 write failed"
sleep 1
CheckKeys r1 "$KEYS" || Fail "round1 data mismatch"

Info "B kill -9; A must keep replicating"
kill -9 "$BPID" 2>/dev/null; BPID=""
WriteKeys r2 "$KEYS" || Fail "round2 write failed"
sleep 1
# A should have r2 even though B is down（B 离线，只比 master/A）
CheckKeys r2 "$KEYS" 0 || Fail "A did not receive round2 while B down"

Info "restart B forced fullsync (remove its dump/aof/replstate)"
rm -f "$DIR/slaveB.dump" "$DIR/slaveB.aof" "$DIR/slaveB.aof.replstate"
BPID=$(StartSlave slaveB "$BPORT") || Fail "slave B restart failed"
# 全量期间持续写入：A 必须实时收到（B 稍后补齐）
WriteKeys r3 "$KEYS" || Fail "round3 write failed"
WaitSynced "$BPORT" 600 || Fail "slave B did not finish fullsync/resync"
sleep 1
CheckKeys r3 "$KEYS" || Fail "round3 mismatch after B fullsync"
CheckKeys r2 "$KEYS" || Fail "A/B round2 mismatch after B fullsync"

Info "kill ebpf-proxy; auto respawn and per-replica recovery"
PIDFILE="$DIR/master.dump.ebpfproxy.pid"
if [ -f "$PIDFILE" ]; then
    kill -9 "$(cat "$PIDFILE")" 2>/dev/null
else
    pkill -9 -f "$PROXY_BIN" 2>/dev/null
fi
sleep 1
WriteKeys r4 "$KEYS" || Fail "round4 write failed (master unavailable?)"
# 等心跳/重连/重新同步完成
WaitSynced "$APORT" 600 || Fail "A did not recover after proxy restart"
WaitSynced "$BPORT" 600 || Fail "B did not recover after proxy restart"
sleep 1
CheckKeys r4 "$KEYS" || Fail "round4 mismatch after proxy restart"

Info "per-replica barrier timeout: silent replica must be rejected alone"
# 先写一小批，保证共享 backlog 有历史（REQ=start+1 落在 backlog 内且有缺口）
WriteKeys fakegap 16 || Fail "fakegap write failed"
sleep 0.5
I=$(RespPy info "$MPORT")
RID=$(Field "$I" master_replid)
BSTART=$(Field "$I" repl_backlog_start_offset)
INV0=$(Field "$I" repl_session_invalid_count)
if [ -n "$RID" ] && [ -n "$BSTART" ]; then
    REQ=$((BSTART + 1))
    GLINE=$(wc -l < "$DIR/master.log")
    RespPy fake-replica "$MPORT" "$RID" "$REQ" "$FAKE_PROXY_PORT" >"$DIR/fake.log" 2>&1 &
    FAKEPID=$!
    GATE_OK=0
    for _ in $(seq 1 200); do
        if tail -n +$((GLINE + 1)) "$DIR/master.log" | grep -q "reject this replica only"; then
            GATE_OK=1; break
        fi
        sleep 0.1
    done
    kill -9 "$FAKEPID" 2>/dev/null; FAKEPID=""
    INV1=$(Field "$(RespPy info "$MPORT")" repl_session_invalid_count)
    [ "$GATE_OK" -eq 1 ] || Fail "silent partial replica did not hit per-replica barrier timeout"
    [ "$INV1" = "$INV0" ] || Fail "silent replica timeout invalidated whole session ($INV0 -> $INV1)"
    SlaveReady "$APORT" || Fail "slave A lost link after silent-replica timeout"
    SlaveReady "$BPORT" || Fail "slave B lost link after silent-replica timeout"
    CheckKeys r4 "$KEYS" || Fail "data mismatch after silent-replica timeout"
else
    Fail "could not read master replid/backlog for silent-replica scenario"
fi

Info "final cross-check all rounds"
for r in r1 r2 r3; do
    CheckKeys "$r" "$KEYS" || Fail "final mismatch for $r"
done

if [ "$FAIL" -eq 0 ]; then
    echo "RESULT: PASS"
    exit 0
fi
echo "RESULT: FAIL"
exit 1
