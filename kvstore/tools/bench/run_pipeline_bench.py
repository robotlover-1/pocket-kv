#!/usr/bin/env python3
"""
Pipeline 批量性能基准（独立套件）—— README「Pipeline 批量性能测试」
- redis-benchmark -n 1000000 -c 50 -P <N> -d 64 -r 1000000
- P=1: 6 配置各 10 轮中位数（独立测）
- P=10/20/40/80/160: 交错测 kv/redis 各 5 轮中位数（每轮重启空库）
用法: python3 tools/bench/run_pipeline_bench.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_common as bc

CFG = [
    ("kv_echo",          "kvstore", "--aof-disable",        ["echo", "__rand_int__"]),
    ("rd_echo",          "redis",   "",                     ["echo", "__rand_int__"]),
    ("kv_hset_disable",  "kvstore", "--aof-disable",        ["HSET", "key:__rand_int__", "value"]),
    ("kv_hset_always",   "kvstore", "--appendfsync always", ["HSET", "key:__rand_int__", "value"]),
    ("rd_hset_disable",  "redis",   "",                     ["HSET", "key:__rand_int__", "__rand_int__", "value"]),
    ("rd_hset_always",   "redis",   "--appendonly yes --appendfsync always", ["HSET", "key:__rand_int__", "__rand_int__", "value"]),
]
P_DEPTHS = [10, 20, 40, 80, 160]

if __name__ == "__main__":
    print("=" * 60)
    print("Pipeline 套件")
    print("=" * 60)
    # memtier 单进程方差小，默认 3 轮；redis-benchmark 保持原轮数。BENCH_ROUNDS 可覆盖。
    ROUNDS = int(os.environ.get("BENCH_ROUNDS", 3 if os.environ.get("USE_MEMTIER") else 10))
    P_ALL = [1] + P_DEPTHS
    res = {str(P): {key: [] for key, *_ in CFG} for P in P_ALL}
    # 统一标准：HSET 配置 populate N 键一次（真实数据量），各 P 复用同一张表测稳态 QPS（NO_POPULATE 跳过重复 populate）
    for key, kind, extra, args in CFG:
        print(f"--- {key}（{ROUNDS} 轮，populate 1M 键后 P-sweep）---")
        for i in range(ROUNDS):
            bc.start_kvstore(extra) if kind == "kvstore" else bc.start_redis(extra)
            port = bc.KV_PORT if kind == "kvstore" else bc.RD_PORT
            if args and args[0] == "HSET":
                bc.populate_hset(port, 1000000, args)
                os.environ["NO_POPULATE"] = "1"
            for P in P_ALL:
                q = bc.one_bench(port, 1000000, P, args)
                if q: res[str(P)][key].append(q)
                print(f"    {key} P={P} r{i+1}: {q:,.0f}" if q else f"    {key} P={P} r{i+1}: FAIL")
            os.environ.pop("NO_POPULATE", None)
            bc.cleanup()
    for P in P_ALL:
        for key, *_ in CFG:
            res[str(P)][key] = bc.median(res[str(P)][key])
        print(f"  P={P}: " + "  ".join(f"{k}={res[str(P)][k]:,.0f}" for k, *_ in CFG))
    bc.dump("bench_pipeline", res)
