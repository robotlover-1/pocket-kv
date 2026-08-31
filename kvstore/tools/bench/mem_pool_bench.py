#!/usr/bin/env python3
"""内存池 VmSize/VmRSS 测试 — 三种内存后端写入/释放对比"""

import subprocess, os, sys, time, csv, argparse

PROJ_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
KVSTORE_BIN = os.path.join(PROJ_DIR, "kvstore")

# 采样点（写入进度 %， 释放进度 %）
WRITE_CHECKPOINTS = [1, 10, 50, 80, 100]   # 写入 N% 后采样
FREE_CHECKPOINTS  = [1, 10, 50, 80, 100]   # 释放 N% 后采样（剩余 100-N%）

TOTAL_KEYS = 1_000_000
BENCH_PORT = 5191


def find_kvstore_pid(port=5191):
    """通过 ss 查找监听 port 的 kvstore PID"""
    try:
        out = subprocess.check_output(
            ["ss", "-tlnp", f"sport = :{port}"], text=True, timeout=5
        )
        for line in out.splitlines():
            if f":{port}" in line:
                # pid=12345 格式
                import re
                m = re.search(r"pid=(\d+)", line)
                if m:
                    return int(m.group(1))
    except Exception:
        pass
    return None


def get_mem(pid):
    """从 /proc/<pid>/status 读取 VmSize(KB), VmRSS(KB)"""
    try:
        with open(f"/proc/{pid}/status") as f:
            vsz = rss = None
            for line in f:
                if line.startswith("VmSize:"):
                    vsz = int(line.split()[1])
                elif line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                if vsz and rss:
                    return vsz, rss
    except Exception:
        pass
    return None, None


def start_kvstore(backend, port=5191):
    """启动 kvstore，处理 jemalloc re-exec"""
    # 确保旧进程已清理
    old_pid = find_kvstore_pid(port)
    if old_pid:
        try:
            os.kill(old_pid, 9)
        except Exception:
            pass
        time.sleep(1)

    # 清理 AOF/dump
    for f in ["kvstore.aof", "kvstore.dump"]:
        p = os.path.join(PROJ_DIR, f)
        if os.path.exists(p):
            os.remove(p)

    extra = f"--port {port} --mem {backend} --aof-disable --role master"
    env = os.environ.copy()
    env.pop("KVS_MEM_JEMALLOC_ACTIVE", None)  # 清掉残留
    env.pop("LD_PRELOAD", None)
    # jemalloc: retain:false 归还 VmSize, decay_ms:0 立即 decay
    if backend == "jemalloc":
        env["MALLOC_CONF"] = "dirty_decay_ms:0,muzzy_decay_ms:0,background_thread:true,retain:false"

    proc = subprocess.Popen(
        f"{KVSTORE_BIN} {extra}",
        shell=True, cwd=PROJ_DIR,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=env,
    )

    # 等待服务就绪（需等 kvstore BPF proxy_cfg 重试循环 ~30s 完成）
    for _ in range(80):
        pid = find_kvstore_pid(port)
        if pid and pid != proc.pid:
            # jemalloc re-exec → PID 变了
            pass
        if pid:
            try:
                r = subprocess.run(
                    ["redis-cli", "-p", str(port), "PING"],
                    capture_output=True, text=True, timeout=3,
                )
                if "PONG" in r.stdout:
                    return pid
            except Exception:
                pass
        time.sleep(0.5)

    raise RuntimeError(f"kvstore ({backend}) 启动超时")


def gen_resp_hset(start, end):
    """生成 HSET key:N value 的 RESP 协议字节（3 元素: HSET key value）"""
    parts = []
    for i in range(start, end):
        key = f"key:{i}"
        klen = len(key)
        # *3: HSET key value  (kvstore hash engine: SET key value)
        parts.append(f"*3\r\n$4\r\nHSET\r\n${klen}\r\n{key}\r\n$5\r\nvalue\r\n")
    return "".join(parts)


def gen_resp_del(start, end):
    """生成 HDEL key:N 的 RESP 协议字节（2 元素: HDEL key）"""
    parts = []
    for i in range(start, end):
        key = f"key:{i}"
        klen = len(key)
        # *2: HDEL key  (kvstore hash engine: DEL key)
        parts.append(f"*2\r\n$4\r\nHDEL\r\n${klen}\r\n{key}\r\n")
    return "".join(parts)


def pipe_to_redis(port, data):
    """通过 redis-cli --pipe 发送 RESP 数据（单连接）"""
    if not data:
        return
    r = subprocess.run(
        ["redis-cli", "-p", str(port), "--pipe"],
        input=data, capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        print(f"  pipe error: {r.stderr.strip()[:200]}")


def pipe_to_redis_concurrent(port, start, end, del_mode, n_conn=50):
    """将 [start,end) 的 key 用 n_conn 个并发 redis-cli --pipe 发送，
    模拟 -c 50 多连接（发送期间 n_conn 个 1.28MB 连接缓冲并存，与 §3.1 随机 key 口径一致）。
    按 key 范围分片到各连接，发完即关（采样反映连接刚关闭后的归还状态）。"""
    total = end - start
    if total <= 0:
        return
    step = max(1, (total + n_conn - 1) // n_conn)
    procs = []
    for i in range(start, end, step):
        s = i
        e = min(i + step, end)
        data = gen_resp_del(s, e) if del_mode else gen_resp_hset(s, e)
        if not data:
            continue
        p = subprocess.Popen(
            ["redis-cli", "-p", str(port), "--pipe"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append((p, data))
    for p, data in procs:
        try:
            p.communicate(input=data.encode(), timeout=120)
        except Exception as e:
            print(f"  pipe error: {e}")


def run_test(backend, port=5191):
    """对单个内存后端执行完整测试"""
    print(f"\n{'='*60}")
    print(f"  测试内存后端: {backend}")
    print(f"{'='*60}")

    pid = start_kvstore(backend, port)
    print(f"  kvstore PID={pid} 就绪")

    results = []

    def sample(label, n_keys):
        vsz, rss = get_mem(pid)
        ratio = f"{rss/vsz*100:.1f}%" if vsz and vsz > 0 else "-"
        results.append({
            "backend": backend, "phase": label, "keys": n_keys,
            "VmSize_KB": vsz, "VmRSS_KB": rss, "VmRSS_pct": ratio,
        })
        print(f"  [{label:>12}] keys={n_keys:>7}  "
              f"VmSize={str(vsz):>8} KB  VmRSS={str(rss):>8} KB  "
              f"RSS/Size={ratio}")

    # 基线
    sample("baseline", 0)

    # === 写入阶段 ===
    print("  --- 写入阶段 (50 并发连接) ---")
    cumulative = 0
    prev = 0
    for pct in WRITE_CHECKPOINTS:
        target = TOTAL_KEYS * pct // 100
        batch = target - prev
        if batch > 0:
            pipe_to_redis_concurrent(port, prev, target, del_mode=False)
        cumulative = target
        prev = target
        sample(f"write_{pct}%", cumulative)

    # === 释放阶段 ===
    print("  --- 释放阶段 (50 并发连接) ---")
    start_key = 0
    for pct in FREE_CHECKPOINTS:
        target = TOTAL_KEYS * pct // 100
        batch = target - start_key
        if batch > 0:
            pipe_to_redis_concurrent(port, start_key, target, del_mode=True)
        start_key = target
        remaining = TOTAL_KEYS - target
        # 归还稳态：free100 后显式 MEMORY_PURGE（libc/custom→malloc_trim,
        # jemalloc→mallctl purge），使 free_100% 采样反映「已归还」而非瞬时量
        if pct == 100:
            subprocess.run(["redis-cli", "-p", str(port), "MEMORY_PURGE"],
                           capture_output=True, text=True, timeout=10)
            time.sleep(0.5)
        sample(f"free_{pct}%", remaining)

    # 停止 kvstore
    try:
        os.kill(pid, 15)
    except Exception:
        pass
    time.sleep(1)
    # 确保清理
    rpid = find_kvstore_pid(port)
    if rpid:
        try:
            os.kill(rpid, 9)
        except Exception:
            pass

    return results


def main():
    parser = argparse.ArgumentParser(description="内存池 VmSize/VmRSS 基准测试")
    parser.add_argument("--backends", nargs="+", default=["libc", "jemalloc", "custom"],
                        help="要测试的内存后端 (默认: libc jemalloc custom)")
    parser.add_argument("--port", type=int, default=5191, help="kvstore 端口 (默认 5191)")
    parser.add_argument("--output", default=None, help="CSV 输出路径")
    args = parser.parse_args()

    # 检查 kvstore 二进制
    if not os.path.exists(KVSTORE_BIN):
        print(f"ERROR: kvstore 二进制文件不存在: {KVSTORE_BIN}")
        sys.exit(1)

    # 检查 jemalloc
    jemalloc_paths = [
        "/lib/x86_64-linux-gnu/libjemalloc.so.2",
        "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
    ]
    jemalloc_found = any(os.path.exists(p) for p in jemalloc_paths)
    if not jemalloc_found and "jemalloc" in args.backends:
        print("WARNING: 未找到 libjemalloc.so.2，jemalloc 测试可能回退到 libc")

    all_results = []
    for backend in args.backends:
        try:
            results = run_test(backend, args.port)
            all_results.extend(results)
        except Exception as e:
            print(f"  FAIL: {backend} — {e}")

    # 输出汇总表
    print(f"\n{'='*80}")
    print("  汇总表")
    print(f"{'='*80}")

    header = f"{'Backend':<10} {'Phase':>12} {'Keys':>8} {'VmSize_KB':>12} {'VmRSS_KB':>12} {'RSS/Size':>10}"
    print(header)
    print("-" * len(header))
    for r in all_results:
        print(f"{r['backend']:<10} {r['phase']:>12} {r['keys']:>8} "
              f"{str(r['VmSize_KB']):>12} {str(r['VmRSS_KB']):>12} {r['VmRSS_pct']:>10}")

    # 保存 CSV
    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["backend", "phase", "keys", "VmSize_KB", "VmRSS_KB", "VmRSS_pct"])
            w.writeheader()
            w.writerows(all_results)
        print(f"\n  CSV 已保存: {args.output}")


if __name__ == "__main__":
    main()
