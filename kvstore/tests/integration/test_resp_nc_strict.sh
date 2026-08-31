#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-5000}"
NC_BIN="${NC_BIN:-nc}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"
SLEEP_BIN="${SLEEP_BIN:-sleep}"

PASS=0
FAIL=0

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

require_cmd() {
    if ! have_cmd "$1"; then
        echo "Error: required command not found: $1" >&2
        exit 1
    fi
}

normalize_file() {
    local f="$1"
    perl -0pe 's/\r//g' "$f"
}

run_nc_to_file() {
    local payload="$1"
    local outfile="$2"

    : >"$outfile"
    if have_cmd "$TIMEOUT_BIN"; then
        printf '%b' "$payload" | "$TIMEOUT_BIN" 2 "$NC_BIN" "$HOST" "$PORT" >"$outfile" 2>/dev/null || true
    else
        printf '%b' "$payload" | "$NC_BIN" "$HOST" "$PORT" >"$outfile" 2>/dev/null || true
    fi
}

strict_expect() {
    local name="$1"
    local payload="$2"
    local expected="$3"

    local out_file exp_file
    out_file="$(mktemp)"
    exp_file="$(mktemp)"

    run_nc_to_file "$payload" "$out_file"
    printf '%b' "$expected" >"$exp_file"

    local out_norm exp_norm
    out_norm="$(normalize_file "$out_file")"
    exp_norm="$(normalize_file "$exp_file")"

    if [ "$out_norm" = "$exp_norm" ]; then
        PASS=$((PASS + 1))
        printf '[PASS] %s\n' "$name"
    else
        FAIL=$((FAIL + 1))
        printf '[FAIL] %s\n' "$name"
        printf '  expected:\n'
        printf '%s' "$exp_norm" | sed 's/^/    /'
        printf '\n  actual:\n'
        printf '%s' "$out_norm" | sed 's/^/    /'
        printf '\n'
    fi

    rm -f "$out_file" "$exp_file"
}

strict_expect_one_of() {
    local name="$1"
    local payload="$2"
    shift 2

    local out_file
    out_file="$(mktemp)"
    run_nc_to_file "$payload" "$out_file"
    local out_norm
    out_norm="$(normalize_file "$out_file")"

    local ok=1
    local expected
    for expected in "$@"; do
        local exp_file exp_norm
        exp_file="$(mktemp)"
        printf '%b' "$expected" >"$exp_file"
        exp_norm="$(normalize_file "$exp_file")"
        rm -f "$exp_file"

        if [ "$out_norm" = "$exp_norm" ]; then
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
        printf '  actual:\n'
        printf '%s' "$out_norm" | sed 's/^/    /'
        printf '\n  expected one of:\n'
        for expected in "$@"; do
            local exp_file exp_norm
            exp_file="$(mktemp)"
            printf '%b' "$expected" >"$exp_file"
            exp_norm="$(normalize_file "$exp_file")"
            rm -f "$exp_file"
            printf '%s' "$exp_norm" | sed 's/^/    /'
            printf '\n'
        done
    fi

    rm -f "$out_file"
}

strict_half_packet() {
    local name="$1"
    local part1="$2"
    local part2="$3"
    local expected="$4"

    local out_file exp_file
    out_file="$(mktemp)"
    exp_file="$(mktemp)"
    printf '%b' "$expected" >"$exp_file"

    exec 3<>/dev/tcp/"$HOST"/"$PORT" || {
        FAIL=$((FAIL + 1))
        printf '[FAIL] %s\n' "$name"
        printf '  cannot open /dev/tcp/%s/%s\n' "$HOST" "$PORT"
        rm -f "$out_file" "$exp_file"
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

    local out_norm exp_norm
    out_norm="$(normalize_file "$out_file")"
    exp_norm="$(normalize_file "$exp_file")"

    if [ "$out_norm" = "$exp_norm" ]; then
        PASS=$((PASS + 1))
        printf '[PASS] %s\n' "$name"
    else
        FAIL=$((FAIL + 1))
        printf '[FAIL] %s\n' "$name"
        printf '  expected:\n'
        printf '%s' "$exp_norm" | sed 's/^/    /'
        printf '\n  actual:\n'
        printf '%s' "$out_norm" | sed 's/^/    /'
        printf '\n'
    fi

    rm -f "$out_file" "$exp_file"
}

banner() {
    printf '\n========== %s ==========\n' "$1"
}

usage() {
    cat <<EOF
Usage:
  bash test_resp_nc_strict.sh [host] [port]

Examples:
  bash test_resp_nc_strict.sh
  bash test_resp_nc_strict.sh 127.0.0.1 5000

This strict version compares the full normalized response exactly.
Normalization:
  - strips CR characters
  - keeps LF and payload content
EOF
}

require_cmd "$NC_BIN"
require_cmd perl

banner "RESP strict tests against $HOST:$PORT"

# -------- Basic set/get/mod/del --------

strict_expect "HSET name alice" \
'*3\r\n$4\r\nHSET\r\n$4\r\nname\r\n$5\r\nalice\r\n' \
'+OK\r\n'

strict_expect "HGET name" \
'*2\r\n$4\r\nHGET\r\n$4\r\nname\r\n' \
'$5\r\nalice\r\n'

strict_expect_one_of "HEXIST name" \
'*2\r\n$6\r\nHEXIST\r\n$4\r\nname\r\n' \
':1\r\n' \
'+EXIST\r\n' \
'EXIST\r\n'

strict_expect "HMOD name bob" \
'*3\r\n$4\r\nHMOD\r\n$4\r\nname\r\n$3\r\nbob\r\n' \
'+OK\r\n'

strict_expect "HGET name after HMOD" \
'*2\r\n$4\r\nHGET\r\n$4\r\nname\r\n' \
'$3\r\nbob\r\n'

# -------- Complex key/value --------

banner "Complex key/value"

strict_expect "HSET key with spaces" \
'*3\r\n$4\r\nHSET\r\n$11\r\nhello world\r\n$5\r\nvalue\r\n' \
'+OK\r\n'

strict_expect "HGET key with spaces" \
'*2\r\n$4\r\nHGET\r\n$11\r\nhello world\r\n' \
'$5\r\nvalue\r\n'

strict_expect "HSET value with spaces" \
'*3\r\n$4\r\nHSET\r\n$8\r\nlong_key\r\n$16\r\nvalue with space\r\n' \
'+OK\r\n'

strict_expect "HGET value with spaces" \
'*2\r\n$4\r\nHGET\r\n$8\r\nlong_key\r\n' \
'$16\r\nvalue with space\r\n'

strict_expect "HSET JSON value" \
'*3\r\n$4\r\nHSET\r\n$9\r\nuser:{42}\r\n$25\r\n{"name":"pp","city":"SZ"}\r\n' \
'+OK\r\n'

strict_expect "HGET JSON value" \
'*2\r\n$4\r\nHGET\r\n$9\r\nuser:{42}\r\n' \
'$25\r\n{"name":"pp","city":"SZ"}\r\n'

# -------- Pipeline / sticky packet --------

banner "Pipeline / sticky packet"

strict_expect "pipeline HSET then HGET" \
'*3\r\n$4\r\nHSET\r\n$4\r\npkey\r\n$4\r\npval\r\n*2\r\n$4\r\nHGET\r\n$4\r\npkey\r\n' \
'+OK\r\n$4\r\npval\r\n'

strict_expect_one_of "pipeline HGET then HEXIST" \
'*2\r\n$4\r\nHGET\r\n$4\r\npkey\r\n*2\r\n$6\r\nHEXIST\r\n$4\r\npkey\r\n' \
'$4\r\npval\r\n:1\r\n' \
'$4\r\npval\r\n+EXIST\r\n' \
'$4\r\npval\r\nEXIST\r\n'

strict_expect_one_of "pipeline HSET HGET HDEL" \
'*3\r\n$4\r\nHSET\r\n$2\r\nk3\r\n$2\r\nv3\r\n*2\r\n$4\r\nHGET\r\n$2\r\nk3\r\n*2\r\n$4\r\nHDEL\r\n$2\r\nk3\r\n' \
'+OK\r\n$2\r\nv3\r\n+OK\r\n' \
'+OK\r\n$2\r\nv3\r\nOK\r\n'

# -------- Half packet --------

banner "Half packet"

strict_half_packet "half split in command token" \
'*3\r\n$4\r\nHSE' \
'T\r\n$4\r\nhpk1\r\n$4\r\nval1\r\n' \
'+OK\r\n'

strict_expect "HGET after half HSET" \
'*2\r\n$4\r\nHGET\r\n$4\r\nhpk1\r\n' \
'$4\r\nval1\r\n'

strict_half_packet "half split HGET command" \
'*2\r\n$4\r\nHG' \
'ET\r\n$4\r\nhpk1\r\n' \
'$4\r\nval1\r\n'

strict_half_packet "half packet plus pipeline" \
'*3\r\n$4\r\nHSET\r\n$4\r\nmix1\r\n$3\r\nva' \
'l\r\n*2\r\n$4\r\nHGET\r\n$4\r\nmix1\r\n' \
'+OK\r\n$3\r\nval\r\n'

# -------- Delete and miss --------

banner "Delete / miss"

strict_expect_one_of "HDEL name" \
'*2\r\n$4\r\nHDEL\r\n$4\r\nname\r\n' \
'+OK\r\n' \
'OK\r\n'

strict_expect_one_of "HGET deleted key" \
'*2\r\n$4\r\nHGET\r\n$4\r\nname\r\n' \
'$-1\r\n' \
'NO EXIST\r\n'

# -------- Invalid request --------

banner "Invalid request"

strict_expect_one_of "wrong argc for HSET" \
'*1\r\n$4\r\nHSET\r\n' \
'-ERR\r\n' \
'ERROR\r\n' \
'-ERR wrong number of arguments\r\n'

printf '\n========== SUMMARY ==========\n'
printf 'PASS: %d\n' "$PASS"
printf 'FAIL: %d\n' "$FAIL"

if [ "$FAIL" -ne 0 ]; then
    exit 1
fi