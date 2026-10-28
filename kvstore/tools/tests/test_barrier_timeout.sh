#!/bin/bash
# ============================================================
# test_barrier_timeout.sh — partial resync 屏障 ACK 超时必须 fail-closed
#
# 验证 CG-02：
#   - Slave 在 partial resync 后不发送 REPLACK（SIGSTOP）时，Master 不得
#     调用 repl_barrier_release() 强制放行/flush；
#   - 超时后 session 失效，reason = BARRIER_TIMEOUT；
#   - SIGCONT 后 Slave 重连并追平，所有写入的完整 value 一致。
#
# 需要 root + eBPF 构建产物；非 root SKIP。
# 用法：sudo tools/tests/test_barrier_timeout.sh [kvstore]
# 环境变量：KVS_TEST_MASTER_PORT(7800) KVS_TEST_SLAVE_PORT(7801)
#           KVS_TEST_BARRIER_HOLD_MS(4000) KVS_TEST_ATTEMPTS(5)
# ============================================================
set -u

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${1:-$ROOT/kvstore}
PROXY_BIN=$ROOT/build/ebpf_proxy
CAPTURE_OBJ=$ROOT/build/replication/bpf/repl_client_capture.bpf.o
PIN=${KVS_TEST_EBPF_PIN:-/sys/fs/bpf/kvs-barrier-timeout}
MPORT=${KVS_TEST_MASTER_PORT:-7800}
SPORT=${KVS_TEST_SLAVE_PORT:-7801}
HOLD_MS=${KVS_TEST_BARRIER_HOLD_MS:-4000}
ATTEMPTS=${KVS_TEST_ATTEMPTS:-5}
GAP_KEYS=${KVS_TEST_GAP_KEYS:-200}
DIR=$(mktemp -d /tmp/kvs-barrier-timeout.XXXXXX)
FAIL=0

if [ "$(id -u)" -ne 0 ]; then echo "SKIP: requires root (BPF)."; rm -rf "$DIR"; exit 0; fi
if [ ! -x "$BIN" ] || [ ! -x "$PROXY_BIN" ] || [ ! -f "$CAPTURE_OBJ" ]; then
    echo "SKIP: missing artifacts."; rm -rf "$DIR"; exit 0
fi
if [ "$SPORT" -ne $((MPORT + 1)) ]; then SPORT=$((MPORT + 1)); fi

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
        keep=/tmp/kvs-barrier-timeout-last
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

Resp() {
    python3 - "$@" <<'PY'
import socket, sys
port=int(sys.argv[1]); mode=sys.argv[2]; args=sys.argv[3:]
try: s=socket.create_connection(("127.0.0.1",port),timeout=3)
except OSError as e: print("CONNECT_FAIL",e); raise SystemExit(2)
def enc(a): return b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)
s.sendall(b"*1\r\n$4\r\nINFO\r\n" if mode=="info" else enc([x.encode() for x in args]))
s.settimeout(0.8); d=b""
while True:
    try: x=s.recv(65536)
    except socket.timeout: break
    if not x: break
    d+=x
s.close(); sys.stdout.write(d.decode(errors="replace"))
PY
}

printf 'kprobe_enabled=0\n' > "$DIR/base.conf"
rm -rf "$PIN"; mkdir -p "$PIN"
echo "barrier_hold_ms=$HOLD_MS attempts=$ATTEMPTS gap_keys=$GAP_KEYS"

StartMaster() {
    KVS_REPL_BARRIER_HOLD_MAX_MS=$HOLD_MS \
    "$BIN" --role master --port "$MPORT" --dump "$DIR/master.dump" --aof "$DIR/master.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
        --ebpf-pin-path "$PIN" --ebpf-proxy-bin "$PROXY_BIN" \
        --ebpf-client-capture-obj "$CAPTURE_OBJ" \
        --net reactor --config "$DIR/base.conf" >"$DIR/master.log" 2>&1 &
    MPID=$!
    for _ in $(seq 1 80); do Resp "$MPORT" cmd PING >/dev/null 2>&1 && return 0; sleep 0.1; done
    return 1
}

StartSlave() {
    "$BIN" --role slave --port "$SPORT" --master-host 127.0.0.1 --master-port "$MPORT" \
        --dump "$DIR/slave.dump" --aof "$DIR/slave.aof" \
        --repl-transport tcp --repl-fullsync-transport tcp --repl-realtime-transport ebpf+tcp \
        --net reactor --config "$DIR/base.conf" >>"$DIR/slave.log" 2>&1 &
    SPID=$!
    for _ in $(seq 1 80); do Resp "$SPORT" cmd PING >/dev/null 2>&1 && return 0; sleep 0.1; done
    return 1
}

WriteKeys() { # <tag> <count> <value_size>
    python3 - "$MPORT" "$1" "$2" "$3" <<'PY' || return 1
import socket, sys
port,tag,n,sz=int(sys.argv[1]),sys.argv[2],int(sys.argv[3]),int(sys.argv[4])
s=socket.create_connection(("127.0.0.1",port),timeout=15)
def enc(a): return b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)
buf=b""
for i in range(n):
    k=("cg:bt:%s:k%04d"%(tag,i)).encode(); v=(tag.encode()*((sz//len(tag))+1))[:sz]
    buf+=enc([b"SET",k,v])
for off in range(0,len(buf),8192): s.sendall(buf[off:off+8192])
s.settimeout(15); want=b"+OK\r\n"*n; got=b""
while len(got)<len(want):
    x=s.recv(65536)
    if not x: break
    got+=x
s.close(); sys.exit(0 if got.count(b"+OK\r\n")>=n else 1)
PY
}

VerifyKeys() { # <tag> <count> <value_size>
    python3 - "$MPORT" "$SPORT" "$1" "$2" "$3" <<'PY' || return 1
import socket, sys
mp,sp=int(sys.argv[1]),int(sys.argv[2]); tag=sys.argv[3]; n=int(sys.argv[4]); sz=int(sys.argv[5])
def recv_exact(s,k):
    out=bytearray()
    while len(out)<k:
        p=s.recv(k-len(out))
        if not p: raise EOFError("eof")
        out.extend(p)
    return bytes(out)
def recv_line(s,limit=128):
    out=bytearray()
    while not out.endswith(b"\r\n"):
        if len(out)>=limit: raise ValueError("header too long")
        out.extend(recv_exact(s,1))
    return bytes(out[:-2])
def getv(s):
    h=recv_line(s)
    if not h.startswith(b"$"): raise ValueError("bad type %r"%h)
    ln=int(h[1:])
    if ln==-1: return None
    if ln<0 or ln>64*1024*1024: raise ValueError("bad len")
    d=recv_exact(s,ln+2)
    if not d.endswith(b"\r\n"): raise ValueError("bad term")
    return d[:-2]
def enc(a): return b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)
sm=socket.create_connection(("127.0.0.1",mp),timeout=15)
ss=socket.create_connection(("127.0.0.1",sp),timeout=15)
bad=0
for i in range(n):
    k=("cg:bt:%s:k%04d"%(tag,i)).encode()
    sm.sendall(enc([b"GET",k])); ss.sendall(enc([b"GET",k]))
    a,b=getv(sm),getv(ss)
    if a!=b:
        bad+=1
        if bad<=3: print("   mismatch %s master_len=%s slave_len=%s"%(k.decode(),None if a is None else len(a),None if b is None else len(b)))
sm.close(); ss.close(); print("   compared %d keys, mismatches=%d"%(n,bad))
sys.exit(1 if bad else 0)
PY
}

WaitConverged() {
    local a="" m="" l=""
    for _ in $(seq 1 "${1:-150}"); do
        local si
        si=$(Resp "$SPORT" info)
        a=$(Field "$si" slave_repl_applied_offset)
        l=$(Field "$si" slave_fullsync_loading)
        m=$(Field "$(Resp "$MPORT" info)" master_repl_offset)
        [ -n "$a" ] && [ -n "$m" ] && [ "$a" = "$m" ] && [ "$l" = "0" ] && return 0
        sleep 0.2
    done
    echo "   applied=${a:-?} master=${m:-?} loading=${l:-?}"; return 1
}

Info "start master :$MPORT"; StartMaster || { echo "master start failed"; exit 1; }
Info "start slave  :$SPORT"; StartSlave  || { echo "slave start failed"; exit 1; }
for _ in $(seq 1 100); do
    I=$(Resp "$MPORT" info)
    [ "$(Field "$I" connected_slaves)" = "1" ] && [ "$(Field "$I" repl_commit_channel)" = "1" ] && break
    sleep 0.2
done
WriteKeys base 20 64 || Fail "baseline write failed"
WaitConverged 100 || Fail "baseline did not converge"
INV0=$(Field "$(Resp "$MPORT" info)" repl_session_invalid_count)
echo "initial session_invalid_count=$INV0"

PASSED=0
for attempt in $(seq 1 "$ATTEMPTS"); do
    Info "attempt $attempt: create partial-resync barrier and freeze slave before REPLACK"
    kill -9 "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; SPID=
    # 小 gap，保证 replay 能写入 socket buffer，Master owner 线程不被阻塞
    WriteKeys gap$attempt "$GAP_KEYS" 256 || { Fail "gap write failed"; break; }
    GATE_LINE_BEFORE=$(wc -l < "$DIR/master.log")
    StartSlave || { Fail "slave restart failed"; break; }

    # 等 Master 进入 "等待 REPLACK" 的 gated barrier；出现后立即冻结 Slave
    GATED=0
    for _ in $(seq 1 300); do
        if tail -n +$((GATE_LINE_BEFORE + 1)) "$DIR/master.log" | grep -q "waiting slave REPLACK"; then
            GATED=1; break
        fi
        sleep 0.02
    done
    if [ "$GATED" -ne 1 ]; then
        echo "   no gated barrier observed (slave may have fullsynced); retry"
        kill -CONT "$SPID" 2>/dev/null
        continue
    fi
    kill -STOP "$SPID" 2>/dev/null
    GATE_POS=$(wc -l < "$DIR/master.log")

    # 等 fail-closed：invalid_count 增长且 reason=BARRIER_TIMEOUT
    OK=0
    for _ in $(seq 1 $(( (HOLD_MS / 1000 + 12) * 5 ))); do
        I=$(Resp "$MPORT" info)
        INV=$(Field "$I" repl_session_invalid_count)
        RSN=$(Field "$I" repl_last_invalid_reason)
        if [ -n "$INV" ] && [ "$INV" -gt "$INV0" ]; then
            if [ "$RSN" = "BARRIER_TIMEOUT" ]; then OK=1; fi
            break
        fi
        sleep 0.2
    done
    RELEASED=$(tail -n +$((GATE_POS + 1)) "$DIR/master.log" | grep -c "proxy barrier down" || true)
    echo "   invalid_count=${INV:-?} reason=${RSN:-?} release_after_gate=$RELEASED"
    if [ "$OK" -ne 1 ]; then
        Fail "barrier timeout did not invalidate with BARRIER_TIMEOUT"
    fi
    if [ "$RELEASED" -ne 0 ]; then
        Fail "barrier was force-released after timeout (must not flush unacked data)"
    fi
    [ "$OK" -eq 1 ] && [ "$RELEASED" -eq 0 ] && PASSED=1

    Info "resume slave and verify reconvergence"
    kill -CONT "$SPID" 2>/dev/null
    # 被 shutdown 的连接促使 Slave 重连；必要时等待新的 session
    WaitConverged 300 || Fail "no convergence after SIGCONT"
    VerifyKeys gap$attempt "$GAP_KEYS" 256 || Fail "data mismatch after barrier timeout recovery"
    VerifyKeys base 20 64 || Fail "baseline data mismatch after recovery"
    break
done

[ "$PASSED" -eq 1 ] || Fail "could not reproduce ACK-timeout scenario in $ATTEMPTS attempts"

Info "final INFO (master)"
Resp "$MPORT" info | tr -d '\r' | grep -E "repl_session_invalid|repl_last_invalid|repl_commit_(timeout|gap|desync)" || true

if [ "$FAIL" -ne 0 ]; then echo "RESULT: FAIL"; exit 1; fi
echo "RESULT: PASS"
