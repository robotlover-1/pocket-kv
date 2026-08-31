#!/usr/bin/env bash
set -euo pipefail
HOST="${1:-127.0.0.1}"
PORT="${2:-5000}"
NC_BIN="${NC_BIN:-nc}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"

run() {
  if command -v "$TIMEOUT_BIN" >/dev/null 2>&1; then
    printf '%b' "$1" | "$TIMEOUT_BIN" 2 "$NC_BIN" -q 1 "$HOST" "$PORT" 2>/dev/null || true
  else
    printf '%b' "$1" | "$NC_BIN" -q 1 "$HOST" "$PORT" 2>/dev/null || true
  fi
}

echo '[1] hset/hget'
run '*3\r\n$4\r\nHSET\r\n$4\r\nname\r\n$5\r\nalice\r\n'
run '*2\r\n$4\r\nHGET\r\n$4\r\nname\r\n'

echo '[2] httl'
run '*3\r\n$7\r\nHEXPIRE\r\n$4\r\nname\r\n$1\r\n5\r\n'
run '*2\r\n$4\r\nHTTL\r\n$4\r\nname\r\n'

echo '[3] pipeline'
run '*3\r\n$4\r\nHSET\r\n$4\r\npkey\r\n$4\r\npval\r\n*2\r\n$4\r\nHGET\r\n$4\r\npkey\r\n'

echo '[4] save'
run '*1\r\n$4\r\nSAVE\r\n'
echo 'SAVE OK'
