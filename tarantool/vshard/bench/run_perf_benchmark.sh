#!/usr/bin/env bash
# run_perf_benchmark.sh — profile the C++ vshard proxy under perf record.
#
# Usage:
#   ./run_perf_benchmark.sh [OPTIONS]
#
# Options:
#   --proxy-binary PATH     path to userver-tarantool-vshard-sample (required)
#   --fibers N              benchmark concurrency level (default: 150)
#   --ops N                 total operations for the measured run (default: 100000)
#   --warmup-ops N          total operations for the warmup pass (default: 5000)
#   --cpp-proxy-port PORT   proxy listen port (default: 13306)
#   --rs1-master HOST:PORT  (default: 127.0.0.1:3301)
#   --rs2-master HOST:PORT  (default: 127.0.0.1:3303)
#   --perf-freq HZ          perf sampling frequency (default: 99)
#   --call-graph MODE       dwarf | fp | lbr (default: fp)
#   --output-dir DIR        where to write perf.data and flamegraph.svg
#                           (default: /tmp/vshard-perf-<timestamp>)
#   --flamegraph-dir DIR    path to FlameGraph scripts dir; if not set the
#                           script searches PATH then common install locations
#   --flamegraph-timeout N  seconds to allow for flamegraph generation (default: 120)
#   --no-flamegraph         skip flamegraph generation, only produce perf.data
#   --sudo                  run perf with sudo (required on most Linux systems)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
PROXY_BINARY=""
FIBERS=150
OPS=100000
WARMUP_OPS=5000
CPP_PROXY_PORT=13306
RS1_MASTER="127.0.0.1:3301"
RS2_MASTER="127.0.0.1:3303"
PERF_FREQ=99
CALL_GRAPH="fp"
OUTPUT_DIR=""
FLAMEGRAPH_DIR=""
FLAMEGRAPH_TIMEOUT=120
NO_FLAMEGRAPH=0
USE_SUDO=0

# ── arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --proxy-binary)   PROXY_BINARY="$2";   shift 2 ;;
        --fibers)         FIBERS="$2";         shift 2 ;;
        --ops)            OPS="$2";            shift 2 ;;
        --warmup-ops)     WARMUP_OPS="$2";     shift 2 ;;
        --cpp-proxy-port) CPP_PROXY_PORT="$2"; shift 2 ;;
        --rs1-master)     RS1_MASTER="$2";     shift 2 ;;
        --rs2-master)     RS2_MASTER="$2";     shift 2 ;;
        --perf-freq)      PERF_FREQ="$2";      shift 2 ;;
        --call-graph)     CALL_GRAPH="$2";     shift 2 ;;
        --output-dir)     OUTPUT_DIR="$2";     shift 2 ;;
        --flamegraph-dir)     FLAMEGRAPH_DIR="$2";     shift 2 ;;
        --flamegraph-timeout) FLAMEGRAPH_TIMEOUT="$2"; shift 2 ;;
        --no-flamegraph)      NO_FLAMEGRAPH=1;         shift   ;;
        --sudo)               USE_SUDO=1;              shift   ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ── validate ──────────────────────────────────────────────────────────────────
die() { echo "ERROR: $*" >&2; exit 1; }

[[ -n "${PROXY_BINARY}" ]] || die "--proxy-binary is required"
[[ -x "${PROXY_BINARY}" ]] || die "not executable: ${PROXY_BINARY}"

command -v tarantool >/dev/null 2>&1 || die "tarantool not found in PATH"
command -v nc        >/dev/null 2>&1 || die "nc (netcat) not found in PATH"
command -v perf      >/dev/null 2>&1 || die "perf not found in PATH"

# ── output directory ──────────────────────────────────────────────────────────
if [[ -z "${OUTPUT_DIR}" ]]; then
    OUTPUT_DIR="/tmp/vshard-perf-$(date '+%Y%m%d-%H%M%S')"
fi
mkdir -p "${OUTPUT_DIR}"
echo "Output directory: ${OUTPUT_DIR}"

# ── flamegraph scripts ────────────────────────────────────────────────────────
find_flamegraph() {
    # 1. Check PATH: if flamegraph.pl is on PATH, use its directory
    local in_path
    in_path="$(command -v flamegraph.pl 2>/dev/null || true)"
    if [[ -n "${in_path}" ]]; then
        dirname "${in_path}"
        return
    fi
    # 2. Fall back to common install locations
    for candidate in "${HOME}/FlameGraph" "/opt/FlameGraph" "/usr/share/flamegraph" \
                     "/usr/share/FlameGraph" "/usr/local/share/FlameGraph"; do
        if [[ -f "${candidate}/flamegraph.pl" ]]; then
            echo "${candidate}"
            return
        fi
    done
}

if [[ "${NO_FLAMEGRAPH}" -eq 0 ]]; then
    if [[ -z "${FLAMEGRAPH_DIR}" ]]; then
        FLAMEGRAPH_DIR="$(find_flamegraph)"
    fi
    if [[ -z "${FLAMEGRAPH_DIR}" || ! -f "${FLAMEGRAPH_DIR}/flamegraph.pl" ]]; then
        echo "WARN: FlameGraph scripts not found."
        echo "      Pass --flamegraph-dir <dir>, put flamegraph.pl on PATH, or:"
        echo "      git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph"
        echo "      Skipping flamegraph generation; perf.data will still be saved."
        NO_FLAMEGRAPH=1
    else
        echo "FlameGraph scripts: ${FLAMEGRAPH_DIR}"
    fi
fi

PERF_CMD="perf"
[[ "${USE_SUDO}" -eq 1 ]] && PERF_CMD="sudo perf"

# ── helpers ───────────────────────────────────────────────────────────────────
wait_port() {
    local host="${1%%:*}" port="${1##*:}" timeout="${2:-15}" i=0
    while ! nc -z "${host}" "${port}" 2>/dev/null; do
        sleep 0.3
        i=$((i + 1))
        if [[ $i -ge $((timeout * 3)) ]]; then return 1; fi
    done
}

# Send SIGTERM, wait up to $2 seconds for graceful exit, then SIGKILL.
kill_wait() {
    local pid="$1" timeout="${2:-5}"
    kill "${pid}" 2>/dev/null || return 0
    local i=0
    while kill -0 "${pid}" 2>/dev/null; do
        sleep 0.5
        i=$((i + 1))
        if [[ $i -ge $((timeout * 2)) ]]; then
            kill -9 "${pid}" 2>/dev/null || true
            break
        fi
    done
    wait "${pid}" 2>/dev/null || true
}

PROXY_PID=""
PERF_PID=""

cleanup() {
    if [[ -n "${PERF_PID}" ]]; then
        kill_wait "${PERF_PID}" 5
    fi
    if [[ -n "${PROXY_PID}" ]]; then
        kill_wait "${PROXY_PID}" 5
    fi
}
trap cleanup EXIT

# ── write proxy config ────────────────────────────────────────────────────────
SECDIST="${OUTPUT_DIR}/secdist.json"
STATIC_CFG="${OUTPUT_DIR}/static_config.yaml"
PROXY_LOG="${OUTPUT_DIR}/proxy.log"
PERF_DATA="${OUTPUT_DIR}/perf.data"
FLAMEGRAPH_SVG="${OUTPUT_DIR}/flamegraph.svg"

cat > "${SECDIST}" << JSON
{
    "tarantool_vshard_settings": {
        "tarantool-vshard": {
            "bucket_count": 3000,
            "user": "storage",
            "password": "storage",
            "replicasets": [
                {
                    "uuid": "cbf06940-0790-498b-948d-042b62cf3d29",
                    "nodes": [
                        {"host": "${RS1_MASTER%%:*}", "port": ${RS1_MASTER##*:}, "is_master": true}
                    ]
                },
                {
                    "uuid": "ac522f65-aa94-4134-9f64-51ee384f1a54",
                    "nodes": [
                        {"host": "${RS2_MASTER%%:*}", "port": ${RS2_MASTER##*:}, "is_master": true}
                    ]
                }
            ]
        }
    }
}
JSON

cat > "${STATIC_CFG}" << YAML
components_manager:
    components:
        iproto-vshard-server:
            port: ${CPP_PROXY_PORT}
            task_processor: main-task-processor
        tarantool-vshard:
            secdist_alias: tarantool-vshard
            initial_pool_size: 4
            max_pool_size: 16
        secdist: {}
        default-secdist-provider:
            config: ${SECDIST}
            missing-ok: false
        logging:
            fs-task-processor: fs-task-processor
            loggers:
                default:
                    file_path: ${PROXY_LOG}
                    level: warning
                    overflow_behavior: discard
        dynamic-config:
            fs-cache-path: ''
            defaults:
                USERVER_NO_LOG_SPANS:
                    prefixes: ['tarantool_']
                    names: []
        logging-configurator:
            limited-logging-enable: true
            limited-logging-interval: 1s
        dns-client:
            fs-task-processor: fs-task-processor
    coro_pool:
        initial_size: 500
        max_size: 2000
    task_processors:
        main-task-processor:
            worker_threads: 4
            thread_name: main-worker
        fs-task-processor:
            thread_name: fs-worker
            worker_threads: 2
    default_task_processor: main-task-processor
YAML

# ── start proxy ───────────────────────────────────────────────────────────────
echo ""
echo "Starting C++ proxy (port ${CPP_PROXY_PORT})..."
"${PROXY_BINARY}" --config "${STATIC_CFG}" > "${PROXY_LOG}" 2>&1 &
PROXY_PID=$!
echo "  proxy PID: ${PROXY_PID}"

if ! wait_port "localhost:${CPP_PROXY_PORT}" 15; then
    echo "ERROR: proxy did not start within 15s" >&2
    tail -20 "${PROXY_LOG}" >&2
    exit 1
fi
echo "  proxy ready ✓"

# ── warmup ────────────────────────────────────────────────────────────────────
echo ""
echo "Warmup: ${FIBERS} fibers × ${WARMUP_OPS} ops against localhost:${CPP_PROXY_PORT}..."
tarantool "${SCRIPT_DIR}/bench_vshard.lua" "localhost:${CPP_PROXY_PORT}" \
    "${FIBERS}" "${WARMUP_OPS}" 2>&1
echo "  warmup done ✓"

# ── start perf record ─────────────────────────────────────────────────────────
echo ""
echo "Starting perf record (freq=${PERF_FREQ} Hz, call-graph=${CALL_GRAPH})..."
echo "  perf.data → ${PERF_DATA}"

${PERF_CMD} record \
    -F "${PERF_FREQ}" \
    --call-graph "${CALL_GRAPH}" \
    -p "${PROXY_PID}" \
    -o "${PERF_DATA}" &
PERF_PID=$!
echo "  perf PID: ${PERF_PID}"

# Give perf a moment to attach
sleep 0.5

# ── measured benchmark run ────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════"
echo "Running benchmark: ${FIBERS} fibers × ${OPS} ops"
echo "════════════════════════════════════════════════════════"
tarantool "${SCRIPT_DIR}/bench_vshard.lua" "localhost:${CPP_PROXY_PORT}" \
    "${FIBERS}" "${OPS}" 2>&1
echo "════════════════════════════════════════════════════════"

# ── stop perf ─────────────────────────────────────────────────────────────────
echo ""
echo "Stopping perf record..."
kill -INT "${PERF_PID}" 2>/dev/null || true
kill_wait "${PERF_PID}" 10
PERF_PID=""
echo "  perf.data written: ${PERF_DATA}"

# ── stop proxy (no longer needed) ─────────────────────────────────────────────
echo "Stopping proxy..."
kill_wait "${PROXY_PID}" 5
PROXY_PID=""
echo "  proxy stopped ✓"

# ── generate flamegraph ───────────────────────────────────────────────────────
if [[ "${NO_FLAMEGRAPH}" -eq 0 ]]; then
    echo ""
    echo "Generating flamegraph (timeout=${FLAMEGRAPH_TIMEOUT}s)..."
    if timeout "${FLAMEGRAPH_TIMEOUT}" bash -c \
        "${PERF_CMD} script -i '${PERF_DATA}' 2>/dev/null \
         | '${FLAMEGRAPH_DIR}/stackcollapse-perf.pl' \
         | '${FLAMEGRAPH_DIR}/flamegraph.pl' > '${FLAMEGRAPH_SVG}'"; then
        echo "  flamegraph → ${FLAMEGRAPH_SVG}"
    else
        echo "WARN: flamegraph generation timed out or failed after ${FLAMEGRAPH_TIMEOUT}s"
        echo "      perf.data is still available for manual processing"
        rm -f "${FLAMEGRAPH_SVG}"
    fi
fi

# ── perf report hint ──────────────────────────────────────────────────────────
echo ""
echo "To browse interactively:"
if [[ "${USE_SUDO}" -eq 1 ]]; then
    echo "  sudo perf report -i ${PERF_DATA}"
else
    echo "  perf report -i ${PERF_DATA}"
fi
echo ""
echo "Done."
