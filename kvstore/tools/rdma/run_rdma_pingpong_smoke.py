#!/usr/bin/env python3
import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def stop_proc(proc):
    if not proc:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=1)
        return
    except Exception:
        pass
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=1)
    except Exception:
        pass


def main() -> int:
    ap = argparse.ArgumentParser(description="standalone RDMA rc pingpong smoke wrapper")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=18515)
    ap.add_argument("--iters", type=int, default=20)
    ap.add_argument("--size", type=int, default=256)
    ap.add_argument("--bin", default="ibv_rc_pingpong")
    ap.add_argument("--ib-dev", default="rxe0")
    ap.add_argument("--ib-port", type=int, default=1)
    ap.add_argument("--gid-idx", type=int, default=1)
    ap.add_argument("--work-dir", default="./artifacts/rdma/pingpong")
    args = ap.parse_args()

    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    server_log = root / "server.log"
    client_log = root / "client.log"
    result_txt = root / "result.txt"
    for p in (server_log, client_log, result_txt):
        if p.exists():
            p.unlink()

    common = [args.bin, "-d", args.ib_dev, "-i", str(args.ib_port), "-g", str(args.gid_idx), "-p", str(args.port), "-n", str(args.iters), "-s", str(args.size)]

    server = None
    slog = open(server_log, "ab")
    clog = open(client_log, "ab")
    try:
        server = subprocess.Popen(
            common,
            stdout=slog,
            stderr=slog,
            preexec_fn=os.setsid,
        )
        time.sleep(1.0)
        client = subprocess.run(
            common + [args.host],
            stdout=clog,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        try:
            server.wait(timeout=10)
        except Exception:
            pass

        server_text = server_log.read_text(errors="ignore") if server_log.exists() else ""
        client_text = client.stdout if isinstance(client.stdout, str) else ""
        result_txt.write_text("=== server ===\n" + server_text + "\n=== client ===\n" + client_text)

        ok = client.returncode == 0 and "Failed" not in server_text and "Failed" not in client_text
        print("RDMA_PINGPONG_SMOKE")
        print(f"server_log={server_log}")
        print(f"client_log={client_log}")
        print(f"result_txt={result_txt}")
        print(f"client_rc={client.returncode}")
        print(f"status={'pass' if ok else 'fail'}")
        return 0 if ok else 1
    finally:
        stop_proc(server)
        slog.close()
        clog.close()


if __name__ == "__main__":
    sys.exit(main())
