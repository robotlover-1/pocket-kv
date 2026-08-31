#!/usr/bin/env bash
set -u
set -o pipefail

BIN="./kvstore"
OUTDIR="./bench_out"
MEM_BACKEND="libc"

REQUESTS=10000
WARMUP_REQUESTS=5000
DATASET_SIZE=100000
VALUE_SIZE=16

CONCURRENCY_LIST="1,10,50,100,200,500,1000"
PIPELINE_LIST="1,4,8,16,32"
MAXCONN_LIST="100,500,1000,2000,5000"

PORT_REACTOR=6380
PORT_PROACTOR=6381
PORT_NTYCO=6382

LOGDIR=""
QPS_CSV=""
MAX_CSV=""
SUMMARY_TXT=""

PIDS=()

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --bin PATH                 kvstore binary path (default: ./kvstore)
  --outdir DIR               output directory (default: ./bench_out)
  --mem NAME                 memory backend: libc|jemalloc|custom (default: libc)
  --requests N               requests per benchmark run (default: 10000)
  --warmup N                 warmup requests (default: 5000)
  --dataset N                preload dataset size (default: 100000)
  --value-size N             SET payload size (default: 16)
  --concurrency LIST         comma-separated list, e.g. 1,10,50,100
  --pipelines LIST           comma-separated list, e.g. 1,4,8,16,32
  --maxconn LIST             comma-separated list, e.g. 100,500,1000
  -h, --help                 show this help
EOF
}

log() {
  echo "[$(date '+%F %T')] $*"
}

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bin) BIN="$2"; shift 2 ;;
      --outdir) OUTDIR="$2"; shift 2 ;;
      --mem) MEM_BACKEND="$2"; shift 2 ;;
      --requests) REQUESTS="$2"; shift 2 ;;
      --warmup) WARMUP_REQUESTS="$2"; shift 2 ;;
      --dataset) DATASET_SIZE="$2"; shift 2 ;;
      --value-size) VALUE_SIZE="$2"; shift 2 ;;
      --concurrency) CONCURRENCY_LIST="$2"; shift 2 ;;
      --pipelines) PIPELINE_LIST="$2"; shift 2 ;;
      --maxconn) MAXCONN_LIST="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

setup_dirs() {
  mkdir -p "$OUTDIR"
  LOGDIR="$OUTDIR/logs"
  mkdir -p "$LOGDIR"
  QPS_CSV="$OUTDIR/qps_results.csv"
  MAX_CSV="$OUTDIR/max_concurrency_results.csv"
  SUMMARY_TXT="$OUTDIR/summary.txt"
}

init_csv() {
  echo "model,group,cmd,concurrency,pipeline,requests,qps,avg_ms,p50_ms,p95_ms,p99_ms,errors,status,logfile" > "$QPS_CSV"
  echo "model,concurrency,status,qps,avg_ms,p50_ms,p95_ms,p99_ms,errors,logfile" > "$MAX_CSV"
  : > "$SUMMARY_TXT"
}

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

build_project() {
  log "building project"
  make clean >/dev/null 2>&1 || true
  make >/dev/null 2>&1 || die "build failed"
  [[ -x "$BIN" ]] || die "binary not found or not executable: $BIN"
}

wait_port() {
  local port="$1"
  local retries=50
  local i
  for ((i=0; i<retries; i++)); do
    if redis-cli -p "$port" PING >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_server() {
  local model="$1"
  local port="$2"
  local logfile="$LOGDIR/${model}_server.log"

  log "starting $model on port $port"
  "$BIN" --port "$port" --net "$model" --mem "$MEM_BACKEND" >"$logfile" 2>&1 &
  local pid=$!
  PIDS+=("$pid")

  wait_port "$port" || {
    tail -n 50 "$logfile" >&2 || true
    die "failed to start $model on port $port"
  }
}

stop_servers() {
  cleanup
  PIDS=()
}

preload_data() {
  local model="$1"
  local port="$2"

  log "preloading ${DATASET_SIZE} keys into ${model}:${port}"

  local i
  for ((i=1; i<=DATASET_SIZE; i++)); do
    redis-cli -p "$port" SET "key:$i" "value:$i" >/dev/null 2>&1 || die "preload failed at key:$i for $model:$port"
  done
}

warmup_model() {
  local model="$1"
  local port="$2"

  log "warming up ${model}:${port}"
  redis-benchmark -p "$port" -n "$WARMUP_REQUESTS" -c 10 -t get >/dev/null 2>&1 || true
}

extract_qps() {
  local file="$1"
  awk '/requests per second/ {print $(NF-3)}' "$file" | tail -n1
}

extract_errors() {
  local file="$1"
  local e
  e="$(sed -n 's/^  *errors:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$file" | tail -n1)"
  [[ -n "$e" ]] && { echo "$e"; return; }
  echo "0"
}

extract_avg_ms() {
  local file="$1"
  awk '/requests completed in/ {
    req=$2;
    sec=$(NF-1);
    if (req > 0 && sec > 0) printf "%.6f\n", (sec*1000.0)/req;
  }' "$file" | tail -n1
}

extract_percentile_ms() {
  local file="$1"
  local pct="$2"
  awk -v target="$pct" '
    $1 ~ /%/ && $2 == "<=" && $4 == "milliseconds" {
      gsub("%","",$1);
      if ($1+0 >= target && found == 0) {
        print $3;
        found = 1;
      }
    }
  ' "$file" | tail -n1
}

parse_benchmark_output() {
  local file="$1"
  local qps avg p50 p95 p99 errors
  qps="$(extract_qps "$file")"
  avg="$(extract_avg_ms "$file")"
  p50="$(extract_percentile_ms "$file" 50)"
  p95="$(extract_percentile_ms "$file" 95)"
  p99="$(extract_percentile_ms "$file" 99)"
  errors="$(extract_errors "$file")"

  [[ -z "$qps" ]] && qps="NA"
  [[ -z "$avg" ]] && avg="NA"
  [[ -z "$p50" ]] && p50="NA"
  [[ -z "$p95" ]] && p95="NA"
  [[ -z "$p99" ]] && p99="NA"
  [[ -z "$errors" ]] && errors="0"

  printf '%s,%s,%s,%s,%s,%s' "$qps" "$avg" "$p50" "$p95" "$p99" "$errors"
}

run_one_benchmark() {
  local model="$1"
  local port="$2"
  local group="$3"
  local cmd="$4"
  local conc="$5"
  local pipe_n="$6"
  local extra_args="$7"

  local outfile="$LOGDIR/${model}_${group}_${cmd}_c${conc}_p${pipe_n}.txt"
  log "run model=${model} group=${group} cmd=${cmd} c=${conc} P=${pipe_n}"

  local rc=0
  redis-benchmark -p "$port" -n "$REQUESTS" -c "$conc" -P "$pipe_n" $extra_args >"$outfile" 2>&1 || rc=$?

  local parsed qps avg p50 p95 p99 errors status
  parsed="$(parse_benchmark_output "$outfile")"
  IFS=',' read -r qps avg p50 p95 p99 errors <<< "$parsed"

  status="ok"
  if [[ $rc -ne 0 ]]; then
    status="fail"
  fi
  if [[ "$qps" == "NA" ]]; then
    status="fail"
  fi

  if [[ "$status" != "ok" ]]; then
    log "benchmark failed: ${outfile}"
  fi

  echo "${model},${group},${cmd},${conc},${pipe_n},${REQUESTS},${qps},${avg},${p50},${p95},${p99},${errors},${status},${outfile}" >> "$QPS_CSV"
}

run_qps_suite_for_model() {
  local model="$1"
  local port="$2"

  preload_data "$model" "$port"
  warmup_model "$model" "$port"

  local c p

  IFS=',' read -ra CONC_ARR <<< "$CONCURRENCY_LIST"
  for c in "${CONC_ARR[@]}"; do
    run_one_benchmark "$model" "$port" basic get "$c" 1 "-t get"
    run_one_benchmark "$model" "$port" basic set "$c" 1 "-t set -r ${DATASET_SIZE} -d ${VALUE_SIZE}"
  done

  IFS=',' read -ra PIPE_ARR <<< "$PIPELINE_LIST"
  for p in "${PIPE_ARR[@]}"; do
    run_one_benchmark "$model" "$port" pipeline get 100 "$p" "-t get"
    run_one_benchmark "$model" "$port" pipeline set 100 "$p" "-t set -r ${DATASET_SIZE} -d ${VALUE_SIZE}"
  done
}

run_max_concurrency_for_model() {
  local model="$1"
  local port="$2"
  local c outfile rc parsed qps avg p50 p95 p99 errors status

  IFS=',' read -ra MAXC_ARR <<< "$MAXCONN_LIST"
  for c in "${MAXC_ARR[@]}"; do
    outfile="$LOGDIR/${model}_maxconn_c${c}.txt"
    log "max-stable test model=${model} c=${c}"

    rc=0
    redis-benchmark -p "$port" -n "$REQUESTS" -c "$c" -t get >"$outfile" 2>&1 || rc=$?

    parsed="$(parse_benchmark_output "$outfile")"
    IFS=',' read -r qps avg p50 p95 p99 errors <<< "$parsed"

    status="ok"
    if [[ $rc -ne 0 ]]; then
      status="fail"
    fi
    if [[ "$qps" == "NA" ]]; then
      status="fail"
    fi

    echo "${model},${c},${status},${qps},${avg},${p50},${p95},${p99},${errors},${outfile}" >> "$MAX_CSV"
  done
}

write_summary() {
  {
    echo "KVStore benchmark summary"
    echo "========================="
    echo "binary: $BIN"
    echo "outdir: $OUTDIR"
    echo "mem backend: $MEM_BACKEND"
    echo "requests per run: $REQUESTS"
    echo "warmup requests: $WARMUP_REQUESTS"
    echo "dataset size: $DATASET_SIZE"
    echo "value size: $VALUE_SIZE"
    echo "concurrency list: $CONCURRENCY_LIST"
    echo "pipeline list: $PIPELINE_LIST"
    echo "max concurrency list: $MAXCONN_LIST"
    echo
    echo "results:"
    echo "  $QPS_CSV"
    echo "  $MAX_CSV"
  } >> "$SUMMARY_TXT"
}

main() {
  parse_args "$@"

  need_cmd make
  need_cmd redis-cli
  need_cmd redis-benchmark

  setup_dirs
  init_csv
  build_project

  start_server reactor "$PORT_REACTOR"
  sleep 1
  start_server proactor "$PORT_PROACTOR"
  sleep 1
  start_server ntyco "$PORT_NTYCO"
  sleep 1

  run_qps_suite_for_model reactor "$PORT_REACTOR"
  run_qps_suite_for_model proactor "$PORT_PROACTOR"
  run_qps_suite_for_model ntyco "$PORT_NTYCO"

  run_max_concurrency_for_model reactor "$PORT_REACTOR"
  run_max_concurrency_for_model proactor "$PORT_PROACTOR"
  run_max_concurrency_for_model ntyco "$PORT_NTYCO"

  write_summary

  log "benchmark complete"
  log "results: $QPS_CSV"
  log "results: $MAX_CSV"
}

main "$@"