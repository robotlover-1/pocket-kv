#!/usr/bin/env python3
"""
SAVE 性能基准（独立套件）—— README「SAVE 性能测试」
- 每批重启空库 → 写 N 条 → 连续 SAVE save_count 次，取平均每次 SAVE
- 100w: save×1, 10 轮中位数（SAVE 次数少，靠多轮补稳定）
- 10w: save×10, 10 批取平均
- 1w:  save×100, 1 批取平均（批内 100 次已稳定）
- 1k:  save×1000, 1 批取平均（批内 1000 次已稳定）
- kvstore 与 redis 各自独立测
用法: python3 tools/bench/run_save_bench.py
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_common as bc

# (标签, N, 每批 SAVE 次数, 批次数)
# ── 1w/1k 两套参数：实际用（100/1000 次，正式测量）vs 测试用（10 次，快速验证）──
# 测试时注释掉「实际用」两行、取消注释「测试用」两行
SAVE_SCEN = [
    ("100w", 1000000, 1,    10),
    ("10w",  100000,  10,   10),
    # 实际用（正式）：1w save×100、1k save×1000
    # ("1w", 10000, 100, 1),
    # ("1k", 1000, 1000, 1),
    # 测试用（快速）：save×10
    ("1w",   10000,   10,   10),
    ("1k",   1000,    10,   10),
]

def save_scenario(label, N, save_count, batches):
    print(f"--- {label} (N={N}, save×{save_count}, batches={batches}) ---")
    out = {"N": N, "save_count": save_count, "batches": batches, "kv": {}, "rd": {}}
    for side, kind, extra, args in [
        ("kv", "kvstore", "--aof-disable", ["HSET", "key:__rand_int__", "value"]),
        ("rd", "redis",   "",              ["HSET", "key:__rand_int__", "__rand_int__", "value"]),
    ]:
        wqps, save_ms, size = [], [], []
        for b in range(batches):
            bc.start_kvstore(extra) if kind == "kvstore" else bc.start_redis(extra)
            port = bc.KV_PORT if kind == "kvstore" else bc.RD_PORT
            # memtier：写盘必须写够 N 条去重键（单连接顺序键），否则 SAVE 数据量远小于 N
            q = bc.save_write(port, N, args) if os.environ.get("USE_MEMTIER") else bc.one_bench(port, N, 1, args)
            if q: wqps.append(q)
            for i in range(save_count):
                t0 = time.time_ns()
                bc.sh(f"{bc.RBIN}/redis-cli -p {port} SAVE > /dev/null 2>&1", check=False)
                save_ms.append((time.time_ns() - t0) / 1e6)
            dump = f"{bc.PROJ}/kvstore.dump" if kind == "kvstore" else "/tmp/kv_bench_redis/dump.rdb"
            if os.path.exists(dump): size.append(os.path.getsize(dump))
            bc.cleanup()
        out[side] = {
            "wqps": bc.median(wqps),
            "save_ms": bc.median(save_ms),
            "size": bc.median(size),
        }
        print(f"  {side}: wqps={out[side]['wqps']:,.0f} save={out[side]['save_ms']:.1f}ms "
              f"size={out[side]['size']:,.0f}")
    return out

if __name__ == "__main__":
    print("=" * 60)
    print("SAVE 套件: 100w save×1 / 10w save×10 / 1w save×10 / 1k save×10")
    print("=" * 60)
    # memtier 单进程方差小，默认 3 批；redis-benchmark 保持原批数。BENCH_BATCHES 可覆盖。
    BATCHES = int(os.environ.get("BENCH_BATCHES", 3 if os.environ.get("USE_MEMTIER") else 10))
    res = {}
    for label, N, sc, batches in SAVE_SCEN:
        eff = min(batches, BATCHES)
        res[label] = save_scenario(label, N, sc, eff)
    bc.dump("bench_save", res)
