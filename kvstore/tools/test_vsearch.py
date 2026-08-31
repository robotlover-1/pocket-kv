#!/usr/bin/env python3
"""kvstore VSEARCH 冒烟测试：SET 二进制缓存条目 + VSEARCH 余弦 top-k"""
import socket, struct, sys

HOST, PORT = "127.0.0.1", 5160

def send_cmd(sock, *parts):
    data = b"*%d\r\n" % len(parts)
    for p in parts:
        data += b"$%d\r\n" % len(p) + p + b"\r\n"
    sock.sendall(data)

def read_resp(sock):
    def readline():
        b = b""
        while not b.endswith(b"\r\n"):
            ch = sock.recv(1)
            if not ch: raise EOFError
            b += ch
        return b[:-2]
    line = readline()
    if line[:1] == b"*":
        return [read_resp(sock) for _ in range(int(line[1:]))]
    if line[:1] == b"$":
        ln = int(line[1:])
        data = b""
        while len(data) < ln + 2:
            data += sock.recv(ln + 2 - len(data))
        return data[:-2]
    if line[:1] == b"+": return line[1:].decode()
    if line[:1] == b"-": raise Exception(line[1:].decode())
    if line[:1] == b":": return int(line[1:])
    return line

def record(q, a, vec):
    bq, ba = q.encode(), a.encode()
    return struct.pack("<I", len(bq)) + bq + struct.pack("<I", len(ba)) + ba + struct.pack("<I", len(vec)) + struct.pack("<%df" % len(vec), *vec)

def norm(v):
    import math
    n = math.sqrt(sum(x*x for x in v))
    return [x/n for x in v] if n else v

def main():
    # 三条缓存：两条语义相近（golang 学习），一条无关（天气）
    entries = [
        ("semcache:a", "golang 怎么学并发", "答1：看官方 tour + 写小项目", norm([1.0, 0.8, 0.6, 0.1])),
        ("semcache:b", "如何学习 Go 的并发", "答2：并发原语 + 实战", norm([0.9, 0.85, 0.7, 0.0])),
        ("semcache:c", "今天天气怎么样", "答3：晴转多云", norm([0.0, 0.1, 0.1, 1.0])),
    ]
    s = socket.create_connection((HOST, PORT))
    try:
        send_cmd(s, b"AUTH", b"123456")
        read_resp(s)
    except Exception:
        pass  # 若未启 requirepass，AUTH 可能返回错误，忽略
    for k, q, a, v in entries:
        # HSET → hash 引擎（global_hash），二进制感知，与 VSEARCH 扫描的表一致
        send_cmd(s, b"HSET", k.encode(), record(q, a, v))
        assert read_resp(s) in (b"OK", b"+OK", "OK"), "HSET 失败 " + k
    # 查询向量：接近 golang 学习
    qv = norm([0.95, 0.8, 0.65, 0.05])
    qb = struct.pack("<%df" % len(qv), *qv)
    send_cmd(s, b"VSEARCH", b"%d" % len(qv), qb, b"2")
    res = read_resp(s)
    print("VSEARCH 结果:", res)
    assert isinstance(res, list) and len(res) == 4, "期望 2 组 (key,score)，实际 %r" % res
    keys = [res[i] for i in range(0, len(res), 2)]
    scores = [float(res[i]) for i in range(1, len(res), 2)]
    assert keys[0] in (b"semcache:a", b"semcache:b"), "top1 应是相近条目，实际 %r" % keys
    assert keys[0] != b"semcache:c", "天气条目不应排第一"
    assert scores[0] > scores[-1], "分数应降序"
    # 空查询向量 → 空结果
    send_cmd(s, b"VSEARCH", b"256", struct.pack("<256f", *([0.0]*256)), b"2")
    res2 = read_resp(s)
    print("无关向量结果:", res2)
    s.close()
    print("PASS")

if __name__ == "__main__":
    main()
