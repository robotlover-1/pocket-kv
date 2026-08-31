#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-5000}"
NC_BIN="${NC_BIN:-nc}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"
SLEEP_BIN="${SLEEP_BIN:-sleep}"

PASS=0
FAIL=0

have_cmd() { command -v "$1" >/dev/null 2>&1; }

require_cmd() {
  if ! have_cmd "$1"; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

normalize() {
  perl -0pe 's/\r//g'
}

run_nc() {
  local payload="$1"
  if have_cmd "$TIMEOUT_BIN"; then
    printf '%b' "$payload" | "$TIMEOUT_BIN" 2 "$NC_BIN" "$HOST" "$PORT" 2>/dev/null || true
  else
    printf '%b' "$payload" | "$NC_BIN" "$HOST" "$PORT" 2>/dev/null || true
  fi
}

expect_eq() {
  local name="$1"
  local payload="$2"
  local expected="$3"

  local out exp
  out="$(run_nc "$payload" | normalize)"
  exp="$(printf '%b' "$expected" | normalize)"

  if [ "$out" = "$exp" ]; then
    PASS=$((PASS + 1))
    printf '[PASS] %s\n' "$name"
  else
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s\n' "$name"
    printf '  expected:\n%s\n' "$exp" | sed 's/^/    /'
    printf '  actual:\n%s\n' "$out" | sed 's/^/    /'
  fi
}

expect_one_of() {
  local name="$1"
  local payload="$2"
  shift 2

  local out
  out="$(run_nc "$payload" | normalize)"

  local ok=1 exp
  for exp in "$@"; do
    if [ "$out" = "$(printf '%b' "$exp" | normalize)" ]; then
      ok=0
      break
    fi
  done

  if [ "$ok" -eq 0 ]; then
    PASS=$((PASS + 1))
    printf '[PASS] %s\n' "$name"
  else
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s\n' "$name"
    printf '  actual:\n%s\n' "$out" | sed 's/^/    /'
  fi
}

half_packet_eq() {
  local name="$1"
  local part1="$2"
  local part2="$3"
  local expected="$4"

  local out_file
  out_file="$(mktemp)"

  exec 3<>/dev/tcp/"$HOST"/"$PORT" || {
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s\n' "$name"
    printf '  cannot connect to %s:%s\n' "$HOST" "$PORT"
    rm -f "$out_file"
    return
  }

  printf '%b' "$part1" >&3
  "$SLEEP_BIN" 1
  printf '%b' "$part2" >&3

  if have_cmd "$TIMEOUT_BIN"; then
    "$TIMEOUT_BIN" 2 cat <&3 >"$out_file" 2>/dev/null || true
  else
    cat <&3 >"$out_file" 2>/dev/null || true
  fi

  exec 3>&-
  exec 3<&-

  local out exp
  out="$(cat "$out_file" | normalize)"
  exp="$(printf '%b' "$expected" | normalize)"
  rm -f "$out_file"

  if [ "$out" = "$exp" ]; then
    PASS=$((PASS + 1))
    printf '[PASS] %s\n' "$name"
  else
    FAIL=$((FAIL + 1))
    printf '[FAIL] %s\n' "$name"
    printf '  expected:\n%s\n' "$exp" | sed 's/^/    /'
    printf '  actual:\n%s\n' "$out" | sed 's/^/    /'
  fi
}

banner() {
  printf '\n========== %s ==========\n' "$1"
}

require_cmd "$NC_BIN"
require_cmd perl

banner "Basic"
expect_eq "SET name alice" \
'*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n' \
'+OK\r\n'

expect_eq "GET name" \
'*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' \
'$5\r\nalice\r\n'

banner "Write queue / pipeline"
expect_eq "pipeline SET+GET+EXIST" \
'*3\r\n$3\r\nSET\r\n$4\r\npkey\r\n$4\r\npval\r\n*2\r\n$3\r\nGET\r\n$4\r\npkey\r\n*2\r\n$5\r\nEXIST\r\n$4\r\npkey\r\n' \
'+OK\r\n$4\r\npval\r\n:1\r\n'

expect_eq "pipeline 3 writes same connection" \
'*3\r\n$3\r\nSET\r\n$2\r\nq1\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$2\r\nq2\r\n$2\r\nv2\r\n*2\r\n$3\r\nGET\r\n$2\r\nq2\r\n' \
'+OK\r\n+OK\r\n$2\r\nv2\r\n'

banner "Half packet"
half_packet_eq "split SET command token" \
'*3\r\n$3\r\nSE' \
'T\r\n$4\r\nhpk1\r\n$4\r\nval1\r\n' \
'+OK\r\n'

expect_eq "GET hpk1 after split SET" \
'*2\r\n$3\r\nGET\r\n$4\r\nhpk1\r\n' \
'$4\r\nval1\r\n'

half_packet_eq "split GET token" \
'*2\r\n$3\r\nG' \
'ET\r\n$4\r\nhpk1\r\n' \
'$4\r\nval1\r\n'

banner "Error recovery"
expect_one_of "bad bulk len then good GET same connection" \
'*2\r\n$3\r\nGET\r\n$X\r\nname\r\n*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' \
'-ERR invalid bulk length\r\n$5\r\nalice\r\n' \
'-ERR protocol error\r\n$5\r\nalice\r\n'

expect_one_of "bad array header then good GET same connection" \
'x\r\n*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' \
'-ERR invalid resp type\r\n$5\r\nalice\r\n' \
'-ERR protocol error\r\n$5\r\nalice\r\n'

expect_one_of "wrong argc then recovery" \
'*1\r\n$3\r\nSET\r\n*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' \
'-ERR wrong number of arguments\r\n$5\r\nalice\r\n' \
'-ERR invalid request\r\n$5\r\nalice\r\n'

banner "Complex key/value"
expect_eq "SET key with spaces" \
'*3\r\n$3\r\nSET\r\n$11\r\nhello world\r\n$5\r\nvalue\r\n' \
'+OK\r\n'

expect_eq "GET key with spaces" \
'*2\r\n$3\r\nGET\r\n$11\r\nhello world\r\n' \
'$5\r\nvalue\r\n'

banner "Delete / miss"
expect_eq "DEL name" \
'*2\r\n$3\r\nDEL\r\n$4\r\nname\r\n' \
'+OK\r\n'

expect_one_of "GET deleted key" \
'*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' \
'$-1\r\n' \
'NO EXIST\r\n'

printf '\n========== SUMMARY ==========\n'
printf 'PASS: %d\n' "$PASS"
printf 'FAIL: %d\n' "$FAIL"

exit $([ "$FAIL" -eq 0 ]; echo $?)
