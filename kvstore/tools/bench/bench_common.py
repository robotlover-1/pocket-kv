#!/usr/bin/env python3
"""三个基准脚本的公共基础设施（taskset 分离核、server 启停、redis-benchmark 封装）。"""
import subprocess, os, re, sys, time, statistics

PROJ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = f"{PROJ}/kvstore"
RBIN = os.environ.get("RBIN", "/opt/redis-7.2.9/bin")
KV_PORT, RD_PORT = 5190, 6390
SERVER_CORES = os.environ.get("SERVER_CORES", "2,3")
CLIENT_CORE = os.environ.get("CLIENT_CORE", "0")

def sh(cmd, check=False):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"cmd failed: {cmd}\n{r.stderr}\n{r.stdout}")
    return r

def cleanup():
    # 方括号技巧避免 pkill -f 自匹配
    sh("pkill -f 'kvstore.*519[0]'"); sh("pkill -f 'redis-server.*639[0]'"); time.sleep(1.0)

def wait_port(port, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = sh(f"{RBIN}/redis-cli -p {port} PING")
        if r.returncode == 0 and "PONG" in r.stdout: return True
        time.sleep(0.4)
    return False

def start_kvstore(extra):
    cleanup()
    for f in ["kvstore.dump", "kvstore.aof"]:
        p = f"{PROJ}/{f}"
        if os.path.exists(p): os.remove(p)
    sh(f"cd {PROJ} && taskset -c {SERVER_CORES} {BIN} --port {KV_PORT} --role master --mem libc --net reactor {extra} > /tmp/kv_bench.log 2>&1 &")
    if not wait_port(KV_PORT):
        sys.exit(f"kvstore 启动失败: {open('/tmp/kv_bench.log').read()[-300:]}")
    time.sleep(1)

def start_redis(extra):
    cleanup()
    wd = "/tmp/kv_bench_redis"
    os.makedirs(wd, exist_ok=True)
    sh(f"rm -rf {wd}/appendonlydir {wd}/dump.rdb {wd}/appendonly.aof")
    sh(f"taskset -c {SERVER_CORES} {RBIN}/redis-server --port {RD_PORT} --dir {wd} --save '' {extra} > /tmp/rd_bench.log 2>&1 &")
    if not wait_port(RD_PORT):
        sys.exit(f"redis 启动失败: {open('/tmp/rd_bench.log').read()[-300:]}")
    time.sleep(1)

def populate_hset(port, n, args):
    """populate N 条去重键（单连接顺序键）——统一标准的数据量 setup。
    Pipeline 重构用：populate 一次，各 P 复用同一张表（one_bench 设 NO_POPULATE 跳过）。"""
    MT = os.environ.get("MTBIN", "/usr/local/bin/memtier_benchmark")
    if args and args[0] == "HSET":
        if len(args) == 4:
            mcmd = "--command='hset __key__ __field__ __data__'"
        else:
            mcmd = "--command='hset __key__ __data__'"
        sh(f"taskset -c 0 {MT} -s 127.0.0.1 -p {port} -t 1 -c 1 -n {n} --pipeline=1 -d 16 "
           f"{mcmd} --command-key-pattern=S --key-maximum={n} > /tmp/kv_bench_populate.txt 2>&1", check=False)

def one_bench(port, n, P, args):
    of = "/tmp/kv_bench_tmp.txt"
    if os.environ.get("USE_MEMTIER"):
        # memtier 单进程多线程客户端（替代 redis-benchmark）：-t 2 -c 25 = 单进程 50 连接
        MT = os.environ.get("MTBIN", "/usr/local/bin/memtier_benchmark")
        if args and args[0] == "echo":
            mcmd = "--command='echo __data__'"            # ECHO <16B data>
        elif args and args[0] == "HSET":
            if len(args) == 4:                             # redis 3-arg: HSET key field value
                mcmd = "--command='hset __key__ __field__ __data__'"
            else:                                          # kvstore 2-arg: HSET key value
                mcmd = "--command='hset __key__ __data__'"
        else:
            mcmd = "--command='" + " ".join(args) + "'"
        # HSET 配置先 populate N 键（真实数据量）：memtier 多连接随机键 RNG 跨连接碰撞只写 ~20k 去重键，
        # 空表测的"100w"实际 ~2 万键。用单连接顺序键写 N 条去重键，再测稳态 QPS（与 SAVE 同口径）。
        # NO_POPULATE=1 时跳过（Pipeline 重构：populate 一次 + 各 P 复用同一张表）。
        if args and args[0] == "HSET" and not os.environ.get("NO_POPULATE"):
            sh(f"taskset -c 0 {MT} -s 127.0.0.1 -p {port} -t 1 -c 1 -n {n} --pipeline=1 -d 16 "
               f"{mcmd} --command-key-pattern=S --key-maximum={n} > /tmp/kv_bench_populate.txt 2>&1", check=False)
        # MT_TEST_TIME>0 用 --test-time（长窗口、高 P 稳定）；否则用 -n 固定请求数（SAVE 写盘需定数）
        tt = os.environ.get("MT_TEST_TIME")
        if tt and tt.isdigit() and int(tt) > 0:
            sh(f"taskset -c 0-1 {MT} -s 127.0.0.1 -p {port} -t 2 -c 50 --test-time={tt} "
               f"--pipeline={P} -d 16 {mcmd} --command-key-pattern=R --key-maximum=1000000 > {of} 2>&1", check=False)
        else:
            per_client = max(1, n // 100)   # -t 2 -c 50 = 100 客户端；总请求数 = n
            sh(f"taskset -c 0-1 {MT} -s 127.0.0.1 -p {port} -t 2 -c 50 -n {per_client} "
               f"--pipeline={P} -d 16 {mcmd} --command-key-pattern=R --key-maximum=1000000 > {of} 2>&1", check=False)
        m = re.search(r'^Totals\s+([\d.]+)', open(of).read(), re.M)
        return float(m.group(1)) if m else None
    sh(f"taskset -c {CLIENT_CORE} {RBIN}/redis-benchmark -h 127.0.0.1 -p {port} -n {n} -c 50 -P {P} -d 64 -r {n} {' '.join(args)} > {of} 2>&1", check=False)
    m = re.findall(r'([\d.]+) requests per second', open(of).read())
    return float(m[-1]) if m else None

def measure_median(keys, rounds, n=1000000):
    """对 keys 配置列表各测 rounds 轮（每轮重启空库），返回 {key: 中位数 QPS}。"""
    out = {}
    for key, kind, extra, args in keys:
        allq = []
        for i in range(rounds):
            start_kvstore(extra) if kind == "kvstore" else start_redis(extra)
            port = KV_PORT if kind == "kvstore" else RD_PORT
            q = one_bench(port, n, 1, args)
            if q: allq.append(q)
            print(f"    {key} r{i+1}: {q:,.0f}" if q else f"    {key} r{i+1}: FAIL")
            cleanup()
        out[key] = statistics.median(allq) if allq else None
        print(f"  MEDIAN {key}: {out[key]:,.0f}")
    return out

def save_write(port, N, args):
    """SAVE 写 QPS：**空表开始**，`memtier -t 2 -c 50 -n per_client` 固定请求数，
    与 AOF/Pipeline P=1 HSET 同口径（100w QPS 应一致 ~222k）。
    per_client 保底 5000 摊薄 100 连接 setup（memtier Ops/sec 含 setup，-n 小请求数时虚低；
    redis-benchmark 计时排除 setup 才各 N 稳定）。
    key-maximum=N → 数据量 ≈ N（小 N 键空间饱和到 N 去重键，100w 因请求数 1M < 5N 只 ~0.63N）。
    args 决定命令形状：kvstore HSET 2-arg / redis HSET 3-arg。"""
    of = "/tmp/kv_bench_tmp.txt"
    MT = os.environ.get("MTBIN", "/usr/local/bin/memtier_benchmark")
    if args and args[0] == "HSET" and len(args) == 4:
        mcmd = "--command='hset __key__ __field__ __data__'"   # redis 3-arg
    else:
        mcmd = "--command='hset __key__ __data__'"              # kvstore 2-arg
    tt = os.environ.get("MT_TEST_TIME", "5")
    # populate N 键（单连接顺序键写 N 去重键）→ 多连接 test-time 测稳态 QPS（统一标准，与 one_bench/one_bench HSET 一致）
    sh(f"taskset -c 0 {MT} -s 127.0.0.1 -p {port} -t 1 -c 1 -n {N} --pipeline=1 -d 16 "
       f"{mcmd} --command-key-pattern=S --key-maximum={N} > /tmp/kv_bench_populate.txt 2>&1", check=False)
    sh(f"taskset -c 0-1 {MT} -s 127.0.0.1 -p {port} -t 2 -c 50 --test-time={tt} --pipeline=1 -d 16 "
       f"{mcmd} --command-key-pattern=R --key-maximum={N} > {of} 2>&1", check=False)
    m = re.search(r'^Totals\s+([\d.]+)', open(of).read(), re.M)
    return float(m.group(1)) if m else None

def median(xs):
    return statistics.median(xs) if xs else None

def dump(path, data):
    import json, datetime
    od = f"{PROJ}/benchmarks/data"
    os.makedirs(od, exist_ok=True)
    date = datetime.date.today().strftime("%Y-%m-%d")
    p = f"{od}/{path}_{date}.json"
    with open(p, "w") as f:
        f.write(json.dumps(data, indent=2, default=str))
    print(f"\n结果: {p}")
