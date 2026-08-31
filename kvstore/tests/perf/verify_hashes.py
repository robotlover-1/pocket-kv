#!/usr/bin/env python3
"""
verify_hashes.py — 内容级数据完整性校验

Master (hsetserver) 和 Slave (tcpsink) 各自对所有命令计算 FNV-1a hash。
此脚本:
  1. 加载 master hash 文件（所有模式合并）
  2. 加载 slave hash 文件（所有连接合并）
  3. 随机抽取 1/100 slave hash，检查是否在 master 集合中
  4. 报告匹配率

用法:
  python3 verify_hashes.py /tmp/master_hashes/ /tmp/slave_hashes/

目录结构:
  /tmp/master_hashes/
    master.none      (none 模式的 hash)
    master.sync      (sync 模式的 hash)
    master.ebpf      (ebpf 模式的 hash)

  /tmp/slave_hashes/
    slave_hashes.1   (第 1 个连接的 hash)
    slave_hashes.2   (第 2 个连接的 hash)
    ...

输出:
  - 总 hash 数
  - slave 总接收数
  - 抽样结果: 匹配数 / 抽样数
"""

import os
import sys
import random
import glob


def load_hashes(filepath):
    """加载 hash 文件（每行一个 FNV-1a 64-bit hash 的十进制字符串）"""
    hashes = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line:
                hashes.append(int(line))
    return hashes


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <master_hash_dir> <slave_hash_dir>")
        print(f"Example: {sys.argv[0]} /tmp/master/ /tmp/slave/")
        sys.exit(1)

    master_dir = sys.argv[1]
    slave_dir = sys.argv[2]

    # 加载 master hashes
    master_hashes = []
    master_files = sorted(glob.glob(os.path.join(master_dir, "*")))
    for f in master_files:
        hashes = load_hashes(f)
        master_hashes.extend(hashes)
        print(f"[master] {os.path.basename(f)}: {len(hashes)} commands")

    master_set = set(master_hashes)
    print(f"[master] total: {len(master_hashes)} commands, "
          f"{len(master_set)} unique\n")

    # 加载 slave hashes
    slave_hashes = []
    slave_files = sorted(glob.glob(os.path.join(slave_dir, "*")))
    for f in slave_files:
        hashes = load_hashes(f)
        slave_hashes.extend(hashes)
        print(f"[slave]  {os.path.basename(f)}: {len(hashes)} commands")

    print(f"[slave]  total: {len(slave_hashes)} commands\n")

    # 1/100 随机抽样
    sample_size = max(1, len(slave_hashes) // 100)
    samples = random.sample(slave_hashes, sample_size)

    matched = 0
    unmatched = []
    for i, h in enumerate(samples):
        if h in master_set:
            matched += 1
        else:
            unmatched.append((i, h))

    match_rate = matched / sample_size * 100 if sample_size > 0 else 0

    print(f"===== Verification Results =====")
    print(f"Sample size:  {sample_size} / {len(slave_hashes)} (1/{max(1, len(slave_hashes)//sample_size)})")
    print(f"Matched:      {matched}")
    print(f"Unmatched:    {len(unmatched)}")
    print(f"Match rate:   {match_rate:.1f}%")

    if unmatched:
        print(f"\nFirst 10 unmatched hashes:")
        for idx, h in unmatched[:10]:
            print(f"  sample[{idx}] = {h}")

    if match_rate >= 99.9:
        print("\n✓ DATA INTEGRITY VERIFIED")
        return 0
    elif match_rate >= 99.0:
        print("\n⚠ DATA INTEGRITY: MINOR DISCREPANCY (likely warmup/edge)")
        return 1
    else:
        print("\n✗ DATA INTEGRITY FAILED")
        return 2


if __name__ == "__main__":
    sys.exit(main())
