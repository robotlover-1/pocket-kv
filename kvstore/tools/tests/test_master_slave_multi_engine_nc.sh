#!/usr/bin/env bash
set -euo pipefail
HOST_A="${1:-127.0.0.1}"
PORT_A="${2:-5000}"
HOST_B="${3:-127.0.0.1}"
PORT_B="${4:-5001}"

send() {
  printf '%b' "$2" | nc -q 1 "$1" "$3"
}

echo "SET on master"
send "$HOST_A" '*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n' "$PORT_A"
sleep 1
echo "GET from slave"
send "$HOST_B" '*2\r\n$3\r\nGET\r\n$4\r\nname\r\n' "$PORT_B"

echo "RSET on master"
send "$HOST_A" '*3\r\n$4\r\nRSET\r\n$4\r\nuser\r\n$3\r\nbob\r\n' "$PORT_A"
sleep 1
send "$HOST_B" '*2\r\n$4\r\nRGET\r\n$4\r\nuser\r\n' "$PORT_B"

echo "HSET on master"
send "$HOST_A" '*3\r\n$4\r\nHSET\r\n$5\r\norder\r\n$2\r\nok\r\n' "$PORT_A"
sleep 1
send "$HOST_B" '*2\r\n$4\r\nHGET\r\n$5\r\norder\r\n' "$PORT_B"
