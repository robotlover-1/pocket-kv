#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


def run(cmd):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
    return proc.returncode, proc.stdout


def pick_non_loopback_ipv4() -> str:
    rc, out = run(["ip", "-4", "addr", "show", "up"])
    if rc != 0:
        return ""
    candidates = []
    current = ""
    for line in out.splitlines():
        if re.match(r"^\d+:\s", line):
            current = line.split(":", 2)[1].strip().split("@", 1)[0]
            continue
        m = re.search(r"inet\s+(\d+\.\d+\.\d+\.\d+)/(\d+)", line)
        if not m:
            continue
        ip = m.group(1)
        if ip.startswith("127."):
            continue
        candidates.append((current, ip))
    for iface, ip in candidates:
        if iface.startswith("rxe") or iface.startswith("en") or iface.startswith("eth"):
            return ip
    return candidates[0][1] if candidates else ""


def main() -> int:
    ap = argparse.ArgumentParser(description="probe standalone RDMA smoke prerequisites")
    ap.add_argument("--out", default="./artifacts/rdma/probe/result.json")
    args = ap.parse_args()

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ibv_rc, ibv_out = run(["ibv_devices"])
    rdma_rc, rdma_out = run(["rdma", "link", "show"])
    host_ip = pick_non_loopback_ipv4()

    result = {
        "suggested_host": host_ip or None,
        "ibv_devices_ok": ibv_rc == 0,
        "ibv_devices_output": ibv_out.strip(),
        "rdma_link_ok": rdma_rc == 0,
        "rdma_link_output": rdma_out.strip(),
        "loopback_warning": host_ip == "",
    }
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")

    print("RDMA_STANDALONE_PROBE")
    print(f"result_json={out_path}")
    print(f"suggested_host={result['suggested_host'] or 'missing'}")
    print(f"ibv_devices_ok={'yes' if result['ibv_devices_ok'] else 'no'}")
    print(f"rdma_link_ok={'yes' if result['rdma_link_ok'] else 'no'}")
    if result["loopback_warning"]:
        print("warning=no non-loopback IPv4 detected; do not use 127.0.0.1 for RDMA route tests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
