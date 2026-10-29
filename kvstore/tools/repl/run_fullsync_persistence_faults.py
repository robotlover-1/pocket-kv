#!/usr/bin/env python3
"""
RV-01 回归：fullsync 本地持久化失败/崩溃必须 fail-closed，不得发送 REPLDONE。

对 fullsync=tcp（不需要 RDMA）逐点注入：
  FAIL_STEP:  fsync / replay / rename / patch / rebase / state
  CRASH_STEP: after_marker / after_rename / after_rebase

每个 case：
  1. 注入失败，Slave 必须打印 "fullsync commit FAILED at '<step>'" 且日志中
     没有 "slave sent REPLDONE"；
  2. 重启 Slave（不注入），必须因 pending marker 跳过本地 dump/AOF 并重新全量；
  3. 全量完成后全部 key 一致，marker 文件消失。
"""
import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

FAIL_STEPS = ["fsync", "replay", "rename", "patch", "rebase", "state"]
FAIL_LABELS = {
    "fsync": "fsync (injected)",
    "replay": "replay (injected)",
    "rename": "rename (injected)",
    "patch": "patch aof_offset (injected)",
    "rebase": "aof rebase (injected)",
    "state": "save replstate",
}
CRASH_STEPS = ["after_marker", "after_rename", "after_rebase"]


def enc(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a.encode() if isinstance(a, str) else bytes(a)
        out.append(b"$%d\r\n" % len(b) + b + b"\r\n")
    return b"".join(out)


def read_item(sock, buf):
    while b"\r\n" not in buf[0]:
        chunk = sock.recv(65536)
        if not chunk:
            return buf[0], buf
        buf[0] += chunk
    line, rest = buf[0].split(b"\r\n", 1)
    line += b"\r\n"
    buf[0] = rest
    if line[:1] == b"$":
        n = int(line[1:-2])
        if n < 0:
            return line, buf
        while len(buf[0]) < n + 2:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf[0] += chunk
        payload, buf[0] = buf[0][:n + 2], buf[0][n + 2:]
        return line + payload, buf
    if line[:1] == b"*":
        n = int(line[1:-2])
        out = [line]
        for _ in range(max(n, 0)):
            item, buf = read_item(sock, buf)
            out.append(item)
        return b"".join(out), buf
    return line, buf


def req(host, port, *args, timeout=5):
    s = socket.create_connection((host, port), timeout=timeout)
    try:
        s.sendall(enc(*args))
        return read_item(s, [b""])[0]
    finally:
        s.close()


def info(host, port):
    try:
        text = req(host, port, "INFO").decode(errors="ignore")
        return dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
    except Exception:
        return {}


def wait_ready(host, port, role, tries=200):
    for _ in range(tries):
        try:
            if role.encode() in req(host, port, "ROLE"):
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def preload(host, port, count):
    s = socket.create_connection((host, port), timeout=30)
    done = 0
    while done < count:
        n = min(200, count - done)
        out = []
        for i in range(done, done + n):
            k, v = f"k:{i}", f"v{i}"
            out.append(f"*3\r\n$4\r\nHSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n")
        s.sendall("".join(out).encode())
        seen = 0
        buf = b""
        while seen < n:
            c = s.recv(65536)
            if not c:
                raise RuntimeError("master closed during preload")
            buf += c
            seen = buf.count(b"+OK\r\n")
        done += n
    s.close()


def verify(host, port, count):
    s = socket.create_connection((host, port), timeout=30)
    buf = [b""]
    bad = 0
    try:
        for start in range(0, count, 500):
            n = min(500, count - start)
            out = []
            for i in range(start, start + n):
                k = f"k:{i}"
                out.append(f"*2\r\n$4\r\nHGET\r\n${len(k)}\r\n{k}\r\n")
            s.sendall("".join(out).encode())
            for i in range(start, start + n):
                item, _ = read_item(s, buf)
                try:
                    head, rest = item.split(b"\r\n", 1)
                    ln = int(head[1:])
                except Exception:
                    bad += 1
                    continue
                val = rest[:-2] if ln >= 0 else None
                if val != f"v{i}".encode():
                    bad += 1
    finally:
        s.close()
    return bad


def wait_keys(host, port, count, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if verify(host, port, count) == 0:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def stop(proc):
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            proc.terminate()
    if proc:
        try:
            proc.wait(timeout=5)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                proc.kill()


def wait_log(path, needles, timeout=30):
    deadline = time.time() + timeout
    text = ""
    while time.time() < deadline:
        if path.exists():
            text = path.read_text(errors="ignore")
            if all(n in text for n in needles):
                return text
        time.sleep(0.1)
    return text


def run_case(bin_path, base_port, count, mode, step):
    """mode=FAIL/CRASH。返回 (ok, detail)。"""
    wd = Path(f"/tmp/kvs-fs-fault-{mode.lower()}-{step}")
    shutil.rmtree(wd, ignore_errors=True)
    wd.mkdir(parents=True, exist_ok=True)
    mport = base_port
    sport = base_port + 2
    mlog = open(wd / "master.log", "wb")
    slog = open(wd / "slave.log", "wb")
    common = [bin_path, "--role", None, "--repl-transport", "tcp",
              "--repl-fullsync-transport", "tcp", "--repl-realtime-transport", "tcp",
              "--net", "reactor"]
    master = slave = None
    env = dict(os.environ)
    if mode == "FAIL":
        env["KVS_TEST_FULLSYNC_FAIL_STEP"] = step
    else:
        env["KVS_TEST_FULLSYNC_CRASH_STEP"] = step
    detail = ""
    try:
        master = subprocess.Popen(
            common[:2] + ["master"] + common[3:] +
            ["--port", str(mport), "--master-host", "127.0.0.1",
             "--master-port", str(mport), "--dump", str(wd / "master.dump"),
             "--aof", str(wd / "master.aof")],
            stdout=mlog, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
        if not wait_ready("127.0.0.1", mport, "master"):
            return False, "master not ready"
        preload("127.0.0.1", mport, count)

        slave = subprocess.Popen(
            common[:2] + ["slave"] + common[3:] +
            ["--port", str(sport), "--master-host", "127.0.0.1",
             "--master-port", str(mport), "--dump", str(wd / "slave.dump"),
             "--aof", str(wd / "slave.aof")],
            stdout=slog, stderr=subprocess.STDOUT, preexec_fn=os.setsid, env=env)
        if mode == "FAIL":
            if not wait_ready("127.0.0.1", sport, "slave"):
                return False, "slave not ready"

        if mode == "FAIL":
            label = FAIL_LABELS[step]
            needle = "fullsync commit FAILED at '" + label
            text = wait_log(wd / "slave.log", [needle], timeout=40)
            if needle not in text:
                return False, f"no injected failure log for {step} (expected {label})"
            if "slave sent REPLDONE" in text:
                return False, f"REPLDONE was sent despite {step} failure"
        else:
            deadline = time.time() + 40
            while time.time() < deadline and slave.poll() is None:
                time.sleep(0.1)
            if slave.poll() is None:
                return False, f"slave did not crash at {step}"
            if slave.returncode != 9:
                return False, f"unexpected crash code {slave.returncode} at {step}"
            text = (wd / "slave.log").read_text(errors="ignore")
            if f"TEST crash at fullsync step '{step}'" not in text:
                return False, f"no crash log for {step}"
            if "slave sent REPLDONE" in text:
                return False, f"REPLDONE was sent despite crash at {step}"

        stop(slave)
        slog.close()
        slog = open(wd / "slave2.log", "wb")

        # 重启（不注入）：必须跳过本地 dump/AOF，重新全量并一致
        env2 = dict(os.environ)
        env2.pop("KVS_TEST_FULLSYNC_FAIL_STEP", None)
        env2.pop("KVS_TEST_FULLSYNC_CRASH_STEP", None)
        slave = subprocess.Popen(
            common[:2] + ["slave"] + common[3:] +
            ["--port", str(sport), "--master-host", "127.0.0.1",
             "--master-port", str(mport), "--dump", str(wd / "slave.dump"),
             "--aof", str(wd / "slave.aof")],
            stdout=slog, stderr=subprocess.STDOUT, preexec_fn=os.setsid, env=env2)
        if not wait_ready("127.0.0.1", sport, "slave"):
            return False, "restarted slave not ready"
        if not wait_keys("127.0.0.1", sport, count, timeout=60):
            return False, "restarted slave did not converge"
        marker = Path(str(wd / "slave.dump") + ".fullsync_commit_pending")
        if marker.exists():
            return False, "commit marker was not removed after successful fullsync"
        detail = "recovered"
        return True, detail
    finally:
        stop(slave)
        stop(master)
        mlog.close()
        slog.close()
        if not detail:
            for name in ("master.log", "slave.log", "slave2.log"):
                p = wd / name
                if p.exists():
                    print(f"--- {name} tail ---")
                    print("\n".join(p.read_text(errors="ignore").splitlines()[-20:]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="./kvstore")
    ap.add_argument("--base-port", type=int, default=6000)
    ap.add_argument("--count", type=int, default=1000)
    args = ap.parse_args()
    bin_path = str(Path(args.bin).resolve())
    fails = 0
    port = args.base_port
    for mode, steps in (("FAIL", FAIL_STEPS), ("CRASH", CRASH_STEPS)):
        for i, step in enumerate(steps):
            ok, detail = run_case(bin_path, port + i * 10, args.count, mode, step)
            print(f"{mode:5s} {step:14s} -> {'PASS' if ok else 'FAIL'} {detail}")
            if not ok:
                fails += 1
    if fails:
        print(f"FULLSYNC_PERSISTENCE_FAULTS_FAIL ({fails} case(s))")
        return 1
    print("FULLSYNC_PERSISTENCE_FAULTS_PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
