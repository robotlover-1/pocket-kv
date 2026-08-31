#!/usr/bin/env bash
set -euo pipefail

PROJ_DIR=/home/pp/Desktop/ls_study/proj/9.1-kvstore
BIN=$PROJ_DIR/kvstore
OUTDIR=$PROJ_DIR/benchmarks/data/persist_bench
TMPDIR=/tmp/kvstore_persist_bench
mkdir -p "$TMPDIR" "$OUTDIR"

pkill -f "kvstore.*5190" 2>/dev/null || true
sleep 1

# 验证 Array 引擎容量
echo "=== 验证引擎容量 ==="
rm -f kvstore.dump kvstore.aof
$BIN --port 5190 --role master --mem libc --net reactor --appendfsync everysec > $TMPDIR/kvstore.log 2>&1 &
for i in $(seq 1 30); do redis-cli -p 5190 PING >/dev/null 2>&1 && break; sleep 0.2; done
sleep 1
# SET 2000 keys 看 Array 是否满
python3 -c "
import sys
for i in range(2000):
    sys.stdout.buffer.write(b'*3\r\n\$3\r\nSET\r\n\$3\r\nk:%d\r\n\$3\r\nv:%d\r\n' % (i, i))
" 2>/dev/null | redis-cli -p 5190 --pipe > /dev/null 2>&1
# 检查 GET 第 1500 个 key
result=$(redis-cli -p 5190 GET "k:1500")
echo "GET k:1500 = '$result' (为空说明 Array 已满，SET 失败)"
pkill -f "kvstore.*5190" 2>/dev/null || true
sleep 1

# 现在用 HSET (Hash 引擎) 重新测试
run_test() {
    local label batch TOTAL iter
    label=$1
    batch=$2
    TOTAL=1000000
    iter=$((TOTAL / batch))
    
    echo ""
    echo "--- $label (batch=$batch, iter=$iter) ---"
    
    rm -f kvstore.dump kvstore.aof
    $BIN --port 5190 --role master --mem libc --net reactor --appendfsync everysec > $TMPDIR/kvstore.log 2>&1 &
    
    for i in $(seq 1 30); do
        if redis-cli -p 5190 PING >/dev/null 2>&1; then break; fi
        sleep 0.2
    done
    sleep 1
    
    local write_ms=0 save_ms=0 key=0
    for i in $(seq 1 $iter); do
        local ks=$((key + 1))
        local ke=$((key + batch))
        
        # 使用 HSET (Hash 引擎，无容量限制)
        python3 -c "import sys
for i in range($ks, $ke + 1):
    sys.stdout.buffer.write(b'*3\r\n\$4\r\nHSET\r\n\$15\r\nsavebench:k:%07d\r\n\$9\r\nv:%07d\r\n' % (i, i))
" 2>/dev/null > $TMPDIR/resp_${label}_${i}.txt
        
        local t0=$(date +%s%N)
        redis-cli -p 5190 --pipe < $TMPDIR/resp_${label}_${i}.txt > /dev/null 2>&1 || true
        local t1=$(date +%s%N)
        local wd=$(( (t1 - t0) / 1000000 ))
        write_ms=$((write_ms + wd))
        
        local ts0=$(date +%s%N)
        redis-cli -p 5190 SAVE > /dev/null 2>&1 || true
        local ts1=$(date +%s%N)
        local sd=$(( (ts1 - ts0) / 1000000 ))
        save_ms=$((save_ms + sd))
        
        key=$ke
        rm -f $TMPDIR/resp_${label}_${i}.txt
        printf "  %4d/%d keys=%d write=%dms save=%dms avg=%dms\n" "$i" "$iter" "$key" "$wd" "$sd" "$((save_ms / i))"
    done
    
    local total_ms=$((write_ms + save_ms))
    printf "%s,%d,%.2f,%d,%.2f,%.2f,%.1f\n" "$label" "$TOTAL" "$(echo "scale=2;$total_ms/1000"|bc)" "$iter" "$(echo "scale=2;$write_ms/1000"|bc)" "$(echo "scale=2;$save_ms/1000"|bc)" "$(echo "scale=1;$save_ms/$iter"|bc)" >> $OUTDIR/save_summary2.csv
    echo "  结果: 总$(echo "scale=2;$total_ms/1000"|bc)s 写入$(echo "scale=2;$write_ms/1000"|bc)s 保存$(echo "scale=2;$save_ms/1000"|bc)s 平均$(echo "scale=1;$save_ms/$iter"|bc)ms"
    
    pkill -f "kvstore.*5190" 2>/dev/null || true
    sleep 2
}

echo "scenario,total_keys,total_time_sec,save_count,write_time_sec,save_time_sec,avg_save_ms" > $OUTDIR/save_summary2.csv

run_test "100w_save_1" 1000000
run_test "10w_save_10" 100000
run_test "1w_save_100" 10000
run_test "1k_save_1000" 1000

echo ""
echo "=== 最终 SAVE 结果 (HSET) ==="
cat $OUTDIR/save_summary2.csv
