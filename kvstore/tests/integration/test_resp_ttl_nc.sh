#!/usr/bin/env bash
set -euo pipefail
HOST="${1:-127.0.0.1}"
PORT="${2:-5000}"
NC_BIN="${NC_BIN:-nc}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"

run() {
  if command -v "$TIMEOUT_BIN" >/dev/null 2>&1; then
    printf '%b' "$1" | "$TIMEOUT_BIN" 2 "$NC_BIN" -q 1 "$HOST" "$PORT" 2>/dev/null | perl -0pe 's/\r//g' || true
  else
    printf '%b' "$1" | "$NC_BIN" -q 1 "$HOST" "$PORT" 2>/dev/null | perl -0pe 's/\r//g' || true
  fi
}

echo '[1] HSET/HGET'
run '*3\r\n$4\r\nHSET\r\n$4\r\nname\r\n$5\r\nalice\r\n'
run '*2\r\n$4\r\nHGET\r\n$4\r\nname\r\n'

echo '[2] HEXPIRE/HTTL'
run '*3\r\n$7\r\nHEXPIRE\r\n$4\r\nname\r\n$1\r\n2\r\n'
run '*2\r\n$4\r\nHTTL\r\n$4\r\nname\r\n'
sleep 3
run '*2\r\n$4\r\nHGET\r\n$4\r\nname\r\n'

echo '[3] HPERSIST'
run '*3\r\n$4\r\nHSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n'
run '*3\r\n$7\r\nHEXPIRE\r\n$2\r\nk1\r\n$2\r\n10\r\n'
run '*2\r\n$8\r\nHPERSIST\r\n$2\r\nk1\r\n'
run '*2\r\n$4\r\nHTTL\r\n$2\r\nk1\r\n'

echo '[4] pipeline with HTTL'
run '*3\r\n$4\r\nHSET\r\n$2\r\np1\r\n$2\r\nv1\r\n*3\r\n$7\r\nHEXPIRE\r\n$2\r\np1\r\n$1\r\n1\r\n*2\r\n$4\r\nHTTL\r\n$2\r\np1\r\n'
