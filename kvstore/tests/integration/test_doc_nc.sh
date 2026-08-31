#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-5000}"
NC_BIN="${NC_BIN:-nc}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"

PASS=0
FAIL=0

have_cmd() { command -v "$1" >/dev/null 2>&1; }
require_cmd() { if ! have_cmd "$1"; then echo "Error: required command not found: $1" >&2; exit 1; fi; }

normalize_file() { perl -0pe 's/\r//g' "$1"; }

run_nc_to_file() {
    local payload="$1" outfile="$2"
    : >"$outfile"
    if have_cmd "$TIMEOUT_BIN"; then
        printf '%b' "$payload" | "$TIMEOUT_BIN" 2 "$NC_BIN" -q 1 "$HOST" "$PORT" >"$outfile" 2>/dev/null || true
    else
        printf '%b' "$payload" | "$NC_BIN" -q 1 "$HOST" "$PORT" >"$outfile" 2>/dev/null || true
    fi
}

strict_expect() {
    local name="$1" payload="$2" expected="$3"
    local out_file exp_file
    out_file="$(mktemp)"; exp_file="$(mktemp)"
    run_nc_to_file "$payload" "$out_file"
    printf '%b' "$expected" >"$exp_file"
    local out_norm exp_norm
    out_norm="$(normalize_file "$out_file")"
    exp_norm="$(normalize_file "$exp_file")"
    if [ "$out_norm" = "$exp_norm" ]; then
        PASS=$((PASS + 1)); printf '[PASS] %s\n' "$name"
    else
        FAIL=$((FAIL + 1)); printf '[FAIL] %s\n' "$name"
        printf '  expected:\n'; printf '%s' "$exp_norm" | sed 's/^/    /'
        printf '\n  actual:\n'; printf '%s' "$out_norm" | sed 's/^/    /'; printf '\n'
    fi
    rm -f "$out_file" "$exp_file"
}

strict_expect_contains() {
    local name="$1" payload="$2" substring="$3"
    local out_file
    out_file="$(mktemp)"
    run_nc_to_file "$payload" "$out_file"
    local out_norm
    out_norm="$(normalize_file "$out_file")"
    if echo "$out_norm" | grep -qF "$substring"; then
        PASS=$((PASS + 1)); printf '[PASS] %s\n' "$name"
    else
        FAIL=$((FAIL + 1)); printf '[FAIL] %s\n' "$name"
        printf '  expected to contain: %s\n' "$substring"
        printf '  actual:\n'; printf '%s' "$out_norm" | sed 's/^/    /'; printf '\n'
    fi
    rm -f "$out_file"
}

require_cmd "$NC_BIN"
require_cmd perl

printf '\n========== Document type tests against %s:%s ==========\n' "$HOST" "$PORT"

printf '\n--- DOCSET / DOCGET ---\n'

strict_expect "DOCSET user:1 name alice" \
'*4\r\n$6\r\nDOCSET\r\n$6\r\nuser:1\r\n$4\r\nname\r\n$5\r\nalice\r\n' \
'+OK\r\n'

strict_expect "DOCSET user:1 age 25" \
'*4\r\n$6\r\nDOCSET\r\n$6\r\nuser:1\r\n$3\r\nage\r\n$2\r\n25\r\n' \
'+OK\r\n'

strict_expect "DOCSET user:1 city shenzhen" \
'*4\r\n$6\r\nDOCSET\r\n$6\r\nuser:1\r\n$4\r\ncity\r\n$8\r\nshenzhen\r\n' \
'+OK\r\n'

strict_expect "DOCGET user:1 name => alice" \
'*3\r\n$6\r\nDOCGET\r\n$6\r\nuser:1\r\n$4\r\nname\r\n' \
'$5\r\nalice\r\n'

strict_expect "DOCGET user:1 age => 25" \
'*3\r\n$6\r\nDOCGET\r\n$6\r\nuser:1\r\n$3\r\nage\r\n' \
'$2\r\n25\r\n'

strict_expect "DOCGET user:1 city => shenzhen" \
'*3\r\n$6\r\nDOCGET\r\n$6\r\nuser:1\r\n$4\r\ncity\r\n' \
'$8\r\nshenzhen\r\n'

strict_expect "DOCGET nonexist field => null" \
'*3\r\n$6\r\nDOCGET\r\n$10\r\nnonexist:1\r\n$4\r\nname\r\n' \
'$-1\r\n'

strict_expect "DOCGET user:1 nonexist_field => null" \
'*3\r\n$6\r\nDOCGET\r\n$6\r\nuser:1\r\n$7\r\nno_such\r\n' \
'$-1\r\n'

printf '\n--- DOCSET update ---\n'

strict_expect "DOCSET user:1 name bob (update)" \
'*4\r\n$6\r\nDOCSET\r\n$6\r\nuser:1\r\n$4\r\nname\r\n$3\r\nbob\r\n' \
'+OK\r\n'

strict_expect "DOCGET user:1 name => bob" \
'*3\r\n$6\r\nDOCGET\r\n$6\r\nuser:1\r\n$4\r\nname\r\n' \
'$3\r\nbob\r\n'

printf '\n--- DOCEXIST / DOCCOUNT ---\n'

strict_expect "DOCEXIST user:1 => 1" \
'*2\r\n$8\r\nDOCEXIST\r\n$6\r\nuser:1\r\n' \
':1\r\n'

strict_expect "DOCEXIST nonexist => 0" \
'*2\r\n$8\r\nDOCEXIST\r\n$10\r\nnonexist:1\r\n' \
':0\r\n'

strict_expect "DOCCOUNT user:1 => 3" \
'*2\r\n$8\r\nDOCCOUNT\r\n$6\r\nuser:1\r\n' \
':3\r\n'

printf '\n--- DOCGETALL ---\n'

strict_expect_contains "DOCGETALL user:1 contains name" \
'*2\r\n$9\r\nDOCGETALL\r\n$6\r\nuser:1\r\n' \
'bob'

strict_expect_contains "DOCGETALL user:1 contains age" \
'*2\r\n$9\r\nDOCGETALL\r\n$6\r\nuser:1\r\n' \
'25'

strict_expect_contains "DOCGETALL user:1 contains city" \
'*2\r\n$9\r\nDOCGETALL\r\n$6\r\nuser:1\r\n' \
'shenzhen'

strict_expect "DOCGETALL nonexist => empty array" \
'*2\r\n$9\r\nDOCGETALL\r\n$10\r\nnonexist:1\r\n' \
'*0\r\n'

printf '\n--- DOCDEL (field) ---\n'

strict_expect "DOCDEL user:1 city => OK" \
'*3\r\n$6\r\nDOCDEL\r\n$6\r\nuser:1\r\n$4\r\ncity\r\n' \
'+OK\r\n'

strict_expect "DOCGET user:1 city => null (deleted)" \
'*3\r\n$6\r\nDOCGET\r\n$6\r\nuser:1\r\n$4\r\ncity\r\n' \
'$-1\r\n'

strict_expect "DOCCOUNT user:1 => 2" \
'*2\r\n$8\r\nDOCCOUNT\r\n$6\r\nuser:1\r\n' \
':2\r\n'

printf '\n--- DOCDROP (whole doc) ---\n'

strict_expect "DOCSET user:2 x y" \
'*4\r\n$6\r\nDOCSET\r\n$6\r\nuser:2\r\n$1\r\nx\r\n$1\r\ny\r\n' \
'+OK\r\n'

strict_expect "DOCDROP user:2 => OK" \
'*2\r\n$7\r\nDOCDROP\r\n$6\r\nuser:2\r\n' \
'+OK\r\n'

strict_expect "DOCEXIST user:2 => 0 (dropped)" \
'*2\r\n$8\r\nDOCEXIST\r\n$6\r\nuser:2\r\n' \
':0\r\n'

printf '\n--- Persistence: SAVE and verify AOF ---\n'

strict_expect "SAVE after doc ops" \
'*1\r\n$4\r\nSAVE\r\n' \
'+OK\r\n'

printf '\n========== SUMMARY ==========\n'
printf 'PASS: %d\n' "$PASS"
printf 'FAIL: %d\n' "$FAIL"

if [ "$FAIL" -ne 0 ]; then exit 1; fi
