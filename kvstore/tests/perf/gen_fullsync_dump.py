#!/usr/bin/env python3
"""
gen_fullsync_dump.py — 生成 KVSD 二进制格式全量同步 dump 文件

格式（对齐生产 kvs_dump_to_fd()）:
  [8B aof_offset (uint64 LE)]
  Per record:
    [1B engine_id] [1B flags] [4B key_len (uint32 LE)] [key]
    [4B value_len (uint32 LE)] [value]
    [8B expire_at_ms (uint64 LE)]  — only if KVSD_FLAG_HAS_EXPIRE (0x01)

engine_id: 3 = Hash (KVS_ENGINE_HASH)
flags:     0 = no expire

用法:
  python3 gen_fullsync_dump.py --keys 1000000 --output /tmp/fullsync_dump.bin
  python3 gen_fullsync_dump.py --keys 100000 --key-len 16 --val-len 128
"""

import argparse
import struct
import os
import sys

# KVSD constants (align with kvstore.h)
KVS_ENGINE_HASH = 3
KVSD_FLAG_HAS_EXPIRE = 0x01


def gen_record(key: bytes, value: bytes, engine_id: int = KVS_ENGINE_HASH,
               flags: int = 0, expire_at_ms: int = 0) -> bytes:
    """Generate a single KVSD binary record."""
    buf = bytearray()
    buf.append(engine_id & 0xFF)
    buf.append(flags & 0xFF)
    buf.extend(struct.pack('<I', len(key)))
    buf.extend(key)
    buf.extend(struct.pack('<I', len(value)))
    buf.extend(value)
    if flags & KVSD_FLAG_HAS_EXPIRE:
        buf.extend(struct.pack('<Q', expire_at_ms))
    return bytes(buf)


def gen_header(aof_offset: int = 0) -> bytes:
    """Generate the 8-byte file header."""
    return struct.pack('<Q', aof_offset)


def main():
    parser = argparse.ArgumentParser(
        description='Generate KVSD binary dump file for fullsync benchmark')
    parser.add_argument('--keys', type=int, default=1_000_000,
                        help='Number of keys (default: 1000000)')
    parser.add_argument('--key-prefix', type=str, default='key',
                        help='Key prefix (default: "key")')
    parser.add_argument('--key-len', type=int, default=0,
                        help='Key suffix zero-pad width (default: auto, based on --keys)')
    parser.add_argument('--val-len', type=int, default=64,
                        help='Value length in bytes (default: 64)')
    parser.add_argument('--output', type=str,
                        default='/tmp/fullsync_dump.bin',
                        help='Output file path (default: /tmp/fullsync_dump.bin)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print progress')
    args = parser.parse_args()

    n_keys = args.keys
    key_prefix = args.key_prefix
    val_len = args.val_len

    # Auto key-len: enough digits to hold n_keys, at least 6
    if args.key_len > 0:
        key_width = args.key_len
    else:
        key_width = max(6, len(str(n_keys)))

    # Generate a fixed-size value (repeating printable pattern)
    value = b'V' * val_len
    # Pre-compute per-record overhead
    overhead_per_record = 1 + 1 + 4 + 4  # engine + flags + klen + vlen
    header_size = 8

    total_est = header_size + n_keys * (overhead_per_record + key_width + len(key_prefix) + val_len)
    print(f"Keys:     {n_keys:,}")
    print(f"Key fmt:  {key_prefix}:<{key_width}d>")
    print(f"Val len:  {val_len}")
    print(f"Est size: {total_est / (1024*1024):.1f} MB")
    print(f"Output:   {args.output}")

    written = 0
    with open(args.output, 'wb') as f:
        # 8-byte header
        f.write(gen_header(0))
        written += 8

        # Records
        fmt = f"{key_prefix}:{{:0{key_width}d}}"
        for i in range(n_keys):
            key = fmt.format(i).encode('ascii')
            rec = gen_record(key, value)
            f.write(rec)
            written += len(rec)

            if args.verbose and (i + 1) % 100_000 == 0:
                print(f"  ... {i + 1:,}/{n_keys:,} ({written / (1024*1024):.1f} MB)")

    actual_size = os.path.getsize(args.output)
    print(f"Written:  {actual_size:,} bytes ({actual_size / (1024*1024):.1f} MB)")
    print(f"Records:  {n_keys:,}")
    print("Done.")


if __name__ == '__main__':
    main()
