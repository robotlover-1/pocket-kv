#!/usr/bin/env python3
"""
AOF 并发性能基准（独立套件）—— README「AOF 并发性能对比」
- redis-benchmark -n 1000000 -c 50 -P 1 -d 64 -r 1000000
- 6 配置各 10 轮中位数，每轮重启空库
- kvstore HSET 2-arg / redis HSET 3-arg
用法: python3 tools/bench/run_aof_bench.py
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

if __name__ == "__main__":
    print("=" * 60)
    print("AOF 并发套件: P=1,N=100W")
    print("=" * 60)
    # memtier 单进程方差小，默认 3 轮；redis-benchmark 保持原 10 轮。BENCH_ROUNDS 可覆盖。
    ROUNDS = int(os.environ.get("BENCH_ROUNDS", 3 if os.environ.get("USE_MEMTIER") else 10))
    print(f"6 配置各 {ROUNDS} 轮中位数")
    res = bc.measure_median(CFG, ROUNDS)
    bc.dump("bench_aof", res)
