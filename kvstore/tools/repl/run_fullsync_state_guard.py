#!/usr/bin/env python3
"""
fullsync 状态机保护测试（不需要真正的 Slave/RDMA 对端）：

用一个 TCP 客户端冒充 v2 Slave 发送 REPLSYNC，Master 会创建活动 v2 job
并返回 `+FULLRESYNC ... rdma-write-v2 <epoch>`。随后注入：

  1. 无参 `REPLDONE`；
  2. 错误 tid 的 `REPLDONE 999`；
  3. 同一连接重复 `REPLSYNC`（v2）。

期望（P0 修复验收）：
  - 1/2 不得清 pending、不得释放 Barrier、不得增加 rdma_fs_total_completed；
  - 3 必须被明确拒绝并断开该连接；
  - Master 存活，活动 job 最终被 cancel/回收。

需要 RDMA 设备让 manager 能绑定 CM 端口（默认 siw0）；缺失时 SKIP/FAIL 由
--require-rdma 决定。
"""
import argparse
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def enc(*args):
    return (f"*{len(args)}\r\n" + "".join(f"${len(a)}\r\n{a}\r\n" for a in args)).encode()


def read_until(sock, marker, timeout=5.0):
    sock.settimeout(timeout)
    buf = b""
    try:
        while marker not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
    except socket.timeout:
        pass
    return buf


def read_all_until_eof(sock, timeout=5.0):
    sock.settimeout(timeout)
    buf = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                return buf, True
            buf += chunk
    except socket.timeout:
        return buf, False


def req(host, port, *args, timeout=3):
    s = socket.create_connection((host, port), timeout=timeout)
    try:
        s.sendall(enc(*args))
        data = b""
        s.settimeout(0.5)
        while True:
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            data += chunk
            if data.endswith(b"\r\n"):
                break
        return data
    finally:
        s.close()


def info(host, port):
    try:
        text = req(host, port, "INFO").decode(errors="ignore")
        return dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
    except Exception:
        return {}


def wait_master(host, port, tries=100):
    for _ in range(tries):
        try:
            if b"master" in req(host, port, "ROLE"):
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5290)
    ap.add_argument("--rdma-dev", default=os.environ.get("RDMA_DEV", "siw0"))
    ap.add_argument("--rdma-port", type=int, default=0)
    ap.add_argument("--require-rdma", action="store_true")
    ap.add_argument("--work-dir", default="./artifacts/repl/fullsync-state-guard")
    args = ap.parse_args()

    if not Path(f"/sys/class/infiniband/{args.rdma_dev}").exists():
        if args.require_rdma:
            print("FULLSYNC_STATE_GUARD_FAIL: required RDMA device missing")
            return 1
        print("FULLSYNC_STATE_GUARD_SKIP: RDMA device missing")
        return 0

    if args.rdma_port <= 0:
        args.rdma_port = args.port + 10

    bin_path = str(Path(args.bin).resolve())
    cwd = str(Path(bin_path).parent)
    wd = Path(args.work_dir).resolve()
    shutil.rmtree(wd, ignore_errors=True)
    wd.mkdir(parents=True, exist_ok=True)
    log = open(wd / "master.log", "wb")
    master = None
    rc = 1
    try:
        master = subprocess.Popen(
            [bin_path, "--port", str(args.port), "--role", "master",
             "--master-host", args.host, "--master-port", str(args.port),
             "--dump", str(wd / "master.dump"), "--aof", str(wd / "master.aof"),
             "--rdma-dev", args.rdma_dev,
             "--repl-transport", "tcp", "--repl-fullsync-transport", "rdma",
             "--repl-realtime-transport", "tcp",
             "--rdma-port", str(args.rdma_port),
             "--rdma-fullsync-pause-us", "1000"],
            cwd=cwd, stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        if not wait_master(args.host, args.port):
            print("FULLSYNC_STATE_GUARD_FAIL: master not ready")
            return 1
        for i in range(100):
            req(args.host, args.port, "HSET", f"k:{i}", f"v{i}")

        s = socket.create_connection((args.host, args.port), timeout=5)
        # 冒充 v2 Slave 发起全量（最后参数 rdma-fs-v2）
        s.sendall(enc("REPLSYNC", "?", "0", "0", "rdma-fs-v2"))
        hdr = read_until(s, b"\r\n")
        print("fullsync header:", hdr.strip().decode(errors="replace"))
        if b"rdma-write-v2" not in hdr:
            print("FULLSYNC_STATE_GUARD_FAIL: master did not enter v2 mode "
                  "(manager bind failed?)")
            return 1
        d = info(args.host, args.port)
        active_before = int(d.get("rdma_fs_active_jobs", "0") or 0)
        print("active_jobs after FULLRESYNC =", active_before)

        # 1) 无 tid REPLDONE：必须忽略
        s.sendall(enc("REPLDONE"))
        time.sleep(0.3)
        # 2) 错误 tid REPLDONE：必须忽略
        s.sendall(enc("REPLDONE", "999"))
        time.sleep(0.3)

        d = info(args.host, args.port)
        completed = int(d.get("rdma_fs_total_completed", "0") or 0)
        active_after = int(d.get("rdma_fs_active_jobs", "0") or 0)

        # 3) 同连接重复 REPLSYNC：必须被拒绝并断开写方向
        s.sendall(enc("REPLSYNC", "?", "0", "0", "rdma-fs-v2"))
        _, eof = read_all_until_eof(s, timeout=5.0)
        s.close()
        time.sleep(0.5)

        alive = master.poll() is None and b"master" in req(args.host, args.port, "ROLE")
        log_text = (wd / "master.log").read_text(errors="ignore")

        checks = {
            "v2_mode": b"rdma-write-v2" in hdr,
            "no_tid_ignored": "REPLDONE tid=0 rejected on active v2 transfer" in log_text,
            "bad_tid_ignored": "REPLDONE tid=999 rejected on active v2 transfer" in log_text,
            "dup_replsync_rejected": "duplicate REPLSYNC on active replica conn" in log_text,
            "connection_closed": eof,
            "completed_unchanged": completed == 0,
            "active_job_still_present_before_dup": active_after >= 1,
            "master_alive": alive,
        }
        print("checks:")
        for k, v in checks.items():
            print(f"  {k}={v}")
        if all(checks.values()):
            print("FULLSYNC_STATE_GUARD_PASS")
            rc = 0
        else:
            print("FULLSYNC_STATE_GUARD_FAIL")
            rc = 1
        return rc
    finally:
        if master and master.poll() is None:
            try:
                os.killpg(os.getpgid(master.pid), signal.SIGTERM)
            except Exception:
                master.terminate()
            try:
                master.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(master.pid), signal.SIGKILL)
                except Exception:
                    master.kill()
        log.close()
        if rc != 0:
            p = wd / "master.log"
            if p.exists():
                print("--- master.log tail ---")
                print("\n".join(p.read_text(errors="ignore").splitlines()[-40:]))


if __name__ == "__main__":
    sys.exit(main())
