#!/usr/bin/env python3
import argparse
import csv
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

BACKENDS = ("libc", "jemalloc", "custom")
ENGINE_CMDS = ("SET", "RSET", "HSET", "XSET")


def build_resp(*parts: str) -> bytes:
    out = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        b = part.encode()
        out.append(f"${len(b)}\r\n".encode())
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


def read_line(sock: socket.socket) -> bytes:
    buf = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("socket closed")
        buf.extend(ch)
        if buf.endswith(b"\r\n"):
            return bytes(buf)


def recv_resp(sock: socket.socket):
    first = sock.recv(1)
    if not first:
        raise ConnectionError("empty response")
    if first in (b"+", b"-", b":"):
        return first + read_line(sock)
    if first == b"$":
        length_line = read_line(sock)
        length = int(length_line[:-2])
        if length == -1:
            return None
        data = bytearray()
        while len(data) < length + 2:
            chunk = sock.recv(length + 2 - len(data))
            if not chunk:
                raise ConnectionError("socket closed during bulk read")
            data.extend(chunk)
        return bytes(data[:-2])
    raise ValueError(f"unsupported RESP type: {first!r}")


def send_cmd(sock: socket.socket, *parts: str):
    sock.sendall(build_resp(*parts))
    return recv_resp(sock)


def wait_ready(port: int, timeout: float = 8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as sock:
                send_cmd(sock, "INFO")
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"server on port {port} did not become ready")


def parse_proc_status(pid: int):
    info = {}
    with open(f"/proc/{pid}/status", "r", encoding="utf-8") as f:
        for line in f:
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            info[k.strip()] = v.strip()
    return info


def parse_kb(field: str) -> int:
    num = field.split()[0]
    return int(num)


def parse_memstat(text: str):
    out = {}
    for line in text.strip().splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def cleanup_files(*paths: Path):
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def unique_prefix() -> str:
    return f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"


def append_csv(csv_path: Path, row: dict):
    exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def launch_server(binary: Path, backend: str, port: int, dump_path: Path, aof_path: Path):
    cmd = [
        str(binary), "--port", str(port), "--mem", backend,
        "--dump", dump_path.name, "--aof", aof_path.name,
    ]
    env = os.environ.copy()
    if backend == "jemalloc" and not env.get("LD_PRELOAD"):
        jem = "/lib/x86_64-linux-gnu/libjemalloc.so.2"
        if os.path.exists(jem):
            env["LD_PRELOAD"] = jem
    return subprocess.Popen(
        cmd,
        cwd=binary.parent,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )


def run_one_backend(binary: Path, backend: str, port: int, n: int, value_size: int, warmup: int,
                    settle: float, cmd_name: str, csv_path: Path, run_label: str):
    run_tag = f"{run_label}-{backend}-{unique_prefix()}"
    dump_path = binary.parent / f"bench_{run_tag}.dump"
    aof_path = binary.parent / f"bench_{run_tag}.aof"
    cleanup_files(dump_path, aof_path)

    proc = launch_server(binary, backend, port, dump_path, aof_path)
    try:
        wait_ready(port)
        payload = "x" * value_size

        with socket.create_connection(("127.0.0.1", port), timeout=10.0) as sock:
            for i in range(warmup):
                resp = send_cmd(sock, cmd_name, f"warm:{run_tag}:{i}", payload)
                if not resp or resp[:1] != b"+":
                    raise RuntimeError(f"unexpected warmup response for {backend}: {resp!r}")

            t0 = time.perf_counter()
            for i in range(n):
                resp = send_cmd(sock, cmd_name, f"key:{run_tag}:{i}", payload)
                if not resp or resp[:1] != b"+":
                    raise RuntimeError(f"unexpected {cmd_name} response for {backend}: {resp!r}")
            elapsed = time.perf_counter() - t0
            qps = n / elapsed if elapsed > 0 else 0.0

            time.sleep(settle)
            memstat_raw = send_cmd(sock, "MEMSTAT")
            info_raw = send_cmd(sock, "INFO")

        status = parse_proc_status(proc.pid)
        memstat = parse_memstat(memstat_raw.decode() if isinstance(memstat_raw, bytes) else "")
        info_text = info_raw.decode() if isinstance(info_raw, bytes) else str(info_raw)

        row = {
            "run_label": run_label,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "backend": backend,
            "cmd": cmd_name,
            "port": port,
            "ops": n,
            "value_size": value_size,
            "warmup": warmup,
            "elapsed_sec": round(elapsed, 6),
            "qps": round(qps, 2),
            "vmrss_kb": parse_kb(status.get("VmRSS", "0 kB")),
            "vmsize_kb": parse_kb(status.get("VmSize", "0 kB")),
            "vmdata_kb": parse_kb(status.get("VmData", "0 kB")),
            "mem_gap_kb": parse_kb(status.get("VmSize", "0 kB")) - parse_kb(status.get("VmRSS", "0 kB")),
            "info": info_text.strip(),
            "memstat_backend": memstat.get("backend", ""),
            "current_small_inuse": memstat.get("current_small_inuse", "0"),
            "peak_small_inuse": memstat.get("peak_small_inuse", "0"),
            "total_small_page_bytes": memstat.get("total_small_page_bytes", "0"),
            "current_large_inuse_bytes": memstat.get("current_large_inuse_bytes", "0"),
            "peak_large_inuse_bytes": memstat.get("peak_large_inuse_bytes", "0"),
            "active_large_map_bytes": memstat.get("active_large_map_bytes", "0"),
            "peak_active_large_map_bytes": memstat.get("peak_active_large_map_bytes", "0"),
            "fallback_alloc_calls": memstat.get("fallback_alloc_calls", "0"),
            "fallback_free_calls": memstat.get("fallback_free_calls", "0"),
            "current_fallback_inuse_bytes": memstat.get("current_fallback_inuse_bytes", "0"),
            "peak_fallback_inuse_bytes": memstat.get("peak_fallback_inuse_bytes", "0"),
            "large_alloc_calls": memstat.get("large_alloc_calls", "0"),
            "small_alloc_calls": memstat.get("small_alloc_calls", "0"),
            "current_requested_bytes": memstat.get("current_requested_bytes", "0"),
            "current_allocated_bytes": memstat.get("current_allocated_bytes", "0"),
            "internal_fragment_bytes": memstat.get("internal_fragment_bytes", "0"),
            "internal_fragment_rate": memstat.get("internal_fragment_rate", "0"),
            "small_page_used_bytes": memstat.get("small_page_used_bytes", "0"),
            "page_utilization": memstat.get("page_utilization", "0"),
        }

        append_csv(csv_path, row)

        print(
            f"{backend}: cmd={row['cmd']} qps={row['qps']} "
            f"vmrss_kb={row['vmrss_kb']} vmsize_kb={row['vmsize_kb']} mem_gap_kb={row['mem_gap_kb']} "
            f"internal_fragment_rate={row['internal_fragment_rate']} page_utilization={row['page_utilization']} "
            f"fallback_alloc_calls={row['fallback_alloc_calls']}"
        )
        return row
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)
        cleanup_files(dump_path, aof_path)


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark kvstore memory backends and optionally append to a CSV."
    )
    parser.add_argument("--binary", default="./kvstore", help="kvstore binary path")
    parser.add_argument("--csv", default="../data/bench_results_all.csv", help="cumulative csv path")
    parser.add_argument("--ops", type=int, default=50000, help="number of write ops per backend")
    parser.add_argument("--value-size", type=int, default=128, help="value payload size")
    parser.add_argument("--warmup", type=int, default=200, help="warmup ops before measuring")
    parser.add_argument("--settle", type=float, default=0.5, help="sleep before collecting MEMSTAT/INFO")
    parser.add_argument(
        "--cmd",
        default="HSET",
        choices=ENGINE_CMDS,
        help="write command to benchmark: SET(array), RSET(rbtree), HSET(hash), XSET(skiptable)"
    )
    parser.add_argument("--base-port", type=int, default=6500)
    parser.add_argument("--backends", default=",".join(BACKENDS), help="comma separated backends")
    parser.add_argument("--run-label", default="", help="label for this multi-backend run")
    parser.add_argument("--append", action="store_true", help="append to CSV instead of overwriting it")
    parser.add_argument("--plot", action="store_true", help="invoke plot_bench_grouped.py after writing results")
    parser.add_argument("--plot-script", default="plot_bench_grouped.py", help="grouped plot script path")
    args = parser.parse_args()

    binary = Path(args.binary).resolve()
    if not binary.exists():
        print(f"kvstore binary not found: {binary}", file=sys.stderr)
        return 1

    backends = [b.strip() for b in args.backends.split(",") if b.strip()]
    if not backends:
        print("no backends specified", file=sys.stderr)
        return 1

    run_label = args.run_label or f"{args.cmd.lower()}_ops{args.ops}_val{args.value_size}"
    csv_path = Path(args.csv).resolve()

    if csv_path.exists() and not args.append:
        csv_path.unlink()

    for idx, backend in enumerate(backends):
        run_one_backend(
            binary=binary,
            backend=backend,
            port=args.base_port + idx,
            n=args.ops,
            value_size=args.value_size,
            warmup=args.warmup,
            settle=args.settle,
            cmd_name=args.cmd,
            csv_path=csv_path,
            run_label=run_label,
        )

    print(f"wrote results to {csv_path}")

    if args.plot:
        plot_cmd = [sys.executable, args.plot_script, "--csv", str(csv_path), "--cmd", args.cmd]
        subprocess.run(plot_cmd, check=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())