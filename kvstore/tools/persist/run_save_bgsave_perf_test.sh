#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-./kvstore}"
BUILD_CMD="${BUILD_CMD:-}"
MODE="${MODE:-save}"                       # save | bgsave
ENGINE="${ENGINE:-array}"                  # array | hash | rbtree | skiptable
PORT="${PORT:-6399}"
HOST="${HOST:-127.0.0.1}"

BASE_DIR="${BASE_DIR:-./tmp_save_bgsave_perf}"
RUN_TAG="${RUN_TAG:-${MODE}_${ENGINE}_$(date +%Y%m%d_%H%M%S)}"
WORK_DIR="${BASE_DIR}/${RUN_TAG}"

DUMP_FILE="${DUMP_FILE:-${WORK_DIR}/kvstore.dump}"
AOF_FILE="${AOF_FILE:-${WORK_DIR}/kvstore.aof}"
SERVER_LOG="${WORK_DIR}/server.log"
BASELINE_OUT="${WORK_DIR}/baseline.txt"
DURING_OUT="${WORK_DIR}/during.txt"
RESULT_OUT="${WORK_DIR}/result.txt"

DATASET_SIZE="${DATASET_SIZE:-1000}"
CLIENTS="${CLIENTS:-50}"
REQUESTS="${REQUESTS:-100000}"
TRIGGER_DELAY_SEC="${TRIGGER_DELAY_SEC:-2}"

REDIS_CLI="${REDIS_CLI:-redis-cli}"
REDIS_BENCHMARK="${REDIS_BENCHMARK:-redis-benchmark}"
NC_BIN="${NC_BIN:-nc}"

SERVER_PID=""
BENCH_PID=""

msg() {
  echo "[TEST] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

cleanup() {
  set +e
  if [[ -n "${BENCH_PID}" ]]; then kill "${BENCH_PID}" 2>/dev/null || true; fi
  if [[ -n "${SERVER_PID}" ]]; then kill "${SERVER_PID}" 2>/dev/null || true; fi
  wait 2>/dev/null || true
}
trap cleanup EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"
}

wait_ready() {
  local retry=100
  while (( retry > 0 )); do
    if "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" PING >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
    retry=$((retry - 1))
  done
  return 1
}

engine_cmd_prefix() {
  case "${ENGINE}" in
    array) echo "" ;;
    hash) echo "H" ;;
    rbtree) echo "R" ;;
    skiptable) echo "T" ;;
    *) fail "unsupported ENGINE=${ENGINE}" ;;
  esac
}

engine_set_cmd() {
  local pfx
  pfx="$(engine_cmd_prefix)"
  echo "${pfx}SET"
}

engine_get_cmd() {
  local pfx
  pfx="$(engine_cmd_prefix)"
  echo "${pfx}GET"
}

engine_capacity_hint() {
  case "${ENGINE}" in
    array) echo 1024 ;;
    *) echo 100000000 ;;
  esac
}

adjust_dataset_size_if_needed() {
  local cap
  cap="$(engine_capacity_hint)"
  if [[ "${ENGINE}" == "array" ]] && (( DATASET_SIZE > cap )); then
    msg "ENGINE=array only supports up to ${cap} keys; reducing DATASET_SIZE from ${DATASET_SIZE} to ${cap}"
    DATASET_SIZE="${cap}"
  fi
}

start_server() {
  mkdir -p "${WORK_DIR}"

  if [[ -n "${BUILD_CMD}" ]]; then
    msg "running build: ${BUILD_CMD}"
    bash -lc "${BUILD_CMD}"
  fi

  [[ -x "${BIN}" ]] || fail "binary not executable: ${BIN}"

  msg "starting server: ${BIN} --port ${PORT}"
  "${BIN}" --port "${PORT}" --role master --dump "${DUMP_FILE}" --aof "${AOF_FILE}" >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!

  wait_ready || {
    tail -n 100 "${SERVER_LOG}" >&2 || true
    fail "server not ready on ${HOST}:${PORT}"
  }
}

preload_dataset() {
  local set_cmd
  set_cmd="$(engine_set_cmd)"

  msg "preloading ${DATASET_SIZE} keys with ${set_cmd}"
  local ok=0
  local failn=0

  for i in $(seq 1 "${DATASET_SIZE}"); do
    if "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" "${set_cmd}" "bench:${ENGINE}:${i}" "value:${i}" >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      failn=$((failn + 1))
    fi
  done

  msg "preload finished: ok=${ok} fail=${failn}"
  if (( ok == 0 )); then
    tail -n 50 "${SERVER_LOG}" >&2 || true
    fail "preload inserted zero keys"
  fi
}

baseline_benchmark() {
  local bench_test
  case "${ENGINE}" in
    array) bench_test="get,set" ;;
    *) bench_test="ping_inline" ;;
  esac

  msg "running baseline benchmark (${bench_test})"
  "${REDIS_BENCHMARK}" -h "${HOST}" -p "${PORT}" -n "${REQUESTS}" -c "${CLIENTS}" -t "${bench_test}" -q >"${BASELINE_OUT}" 2>&1 || true
}

during_benchmark() {
  local bench_test
  case "${ENGINE}" in
    array) bench_test="get,set" ;;
    *) bench_test="ping_inline" ;;
  esac

  msg "starting during-save benchmark (${bench_test})"
  "${REDIS_BENCHMARK}" -h "${HOST}" -p "${PORT}" -n "${REQUESTS}" -c "${CLIENTS}" -t "${bench_test}" -q >"${DURING_OUT}" 2>&1 &
  BENCH_PID=$!
}

trigger_save() {
  local start_ms end_ms save_reply
  start_ms=$(date +%s%3N)

  case "${MODE}" in
    save)
      msg "triggering SAVE"
      save_reply="$("${REDIS_CLI}" -h "${HOST}" -p "${PORT}" SAVE 2>&1 | tr -d '\r' || true)"
      ;;
    bgsave)
      msg "triggering BGSAVE"
      save_reply="$("${REDIS_CLI}" -h "${HOST}" -p "${PORT}" BGSAVE 2>&1 | tr -d '\r' || true)"
      ;;
    *)
      fail "unsupported MODE=${MODE}"
      ;;
  esac

  echo "trigger_reply=${save_reply}" >>"${RESULT_OUT}"

  if [[ "${MODE}" == "bgsave" ]]; then
    local retry=600
    while (( retry > 0 )); do
      local info
      info="$("${REDIS_CLI}" -h "${HOST}" -p "${PORT}" INFO 2>/dev/null | tr -d '\r' || true)"
      if echo "${info}" | grep -Eq 'bgsave:(idle|done|ok|none)'; then
        break
      fi
      sleep 0.1
      retry=$((retry - 1))
    done
  fi

  end_ms=$(date +%s%3N)
  echo "save_wall_time_ms=$((end_ms - start_ms))" >>"${RESULT_OUT}"
}

summarize() {
  local dump_size
  dump_size="$(stat -c %s "${DUMP_FILE}" 2>/dev/null || echo 0)"

  {
    echo "mode=${MODE}"
    echo "engine=${ENGINE}"
    echo "port=${PORT}"
    echo "dataset_size=${DATASET_SIZE}"
    echo "clients=${CLIENTS}"
    echo "requests=${REQUESTS}"
    echo "dump_file=${DUMP_FILE}"
    echo "dump_size_bytes=${dump_size}"
    echo "server_log=${SERVER_LOG}"
    echo "baseline_output=${BASELINE_OUT}"
    echo "during_output=${DURING_OUT}"
  } >>"${RESULT_OUT}"

  echo
  echo "========== RESULT =========="
  cat "${RESULT_OUT}"
  echo
  echo "========== BASELINE =========="
  cat "${BASELINE_OUT}" || true
  echo
  echo "========== DURING =========="
  cat "${DURING_OUT}" || true
  echo
  echo "Artifacts saved in: ${WORK_DIR}"
}

main() {
  require_cmd "${REDIS_CLI}"
  require_cmd "${REDIS_BENCHMARK}"

  adjust_dataset_size_if_needed
  mkdir -p "${WORK_DIR}"
  : >"${RESULT_OUT}"

  start_server
  preload_dataset
  baseline_benchmark
  during_benchmark

  sleep "${TRIGGER_DELAY_SEC}"
  trigger_save

  if [[ -n "${BENCH_PID}" ]]; then
    wait "${BENCH_PID}" || true
    BENCH_PID=""
  fi

  summarize
}

main "$@"
