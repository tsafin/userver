#!/usr/bin/env bash
# run_benchmarks.sh — run vshard throughput benchmarks and append results to
# benchmark_results.md.
#
# Usage:
#   ./run_benchmarks.sh [OPTIONS]
#
# Options:
#   --proxy-binary PATH   path to userver-tarantool-vshard-sample (required)
#   --lua-router HOST:PORT  address of the running Lua vshard router
#                           (default: localhost:3305)
#   --rs1-master HOST:PORT  RS1 master storage node (default: 127.0.0.1:3301)
#   --rs2-master HOST:PORT  RS2 master storage node (default: 127.0.0.1:3303)
#   --fiber-counts N,N,...  comma-separated concurrency levels (default: 10,20,50)
#   --fibers N            single concurrency level (alias for --fiber-counts N)
#   --ops N               total operations per benchmark run (default: 100000)
#   --output PATH         path to benchmark_results.md to update
#                         (default: this script's directory/benchmark_results.md)
#   --no-update           print results but do not update benchmark_results.md
#   --cpp-proxy-port PORT port for the temporary C++ proxy process (default: 13306)
#   --vshard-path PATH    path to vshard example dir (for 'make start/stop' if
#                         the cluster is not already running; also used to find
#                         the vshard Lua module for bench_vshard.lua)
#
# The script generates a temporary config, starts a C++ proxy on --cpp-proxy-port,
# runs bench_vshard.lua against both the Lua router and the C++ proxy for each
# fiber count level, runs the C++ in-process benchmark, then appends a new results
# section to the output file showing scalability across fiber counts.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ── defaults ──────────────────────────────────────────────────────────────────
PROXY_BINARY=""
LUA_ROUTER="localhost:3305"
RS1_MASTER="127.0.0.1:3301"
RS2_MASTER="127.0.0.1:3303"
FIBER_COUNTS="10,20,50,100,150"
OPS=100000
OUTPUT_FILE="${SCRIPT_DIR}/benchmark_results.md"
NO_UPDATE=0
CPP_PROXY_PORT=13306
VSHARD_PATH=""

# ── arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --proxy-binary)   PROXY_BINARY="$2";    shift 2 ;;
        --lua-router)     LUA_ROUTER="$2";      shift 2 ;;
        --rs1-master)     RS1_MASTER="$2";      shift 2 ;;
        --rs2-master)     RS2_MASTER="$2";      shift 2 ;;
        --fiber-counts)   FIBER_COUNTS="$2";    shift 2 ;;
        --fibers)         FIBER_COUNTS="$2";    shift 2 ;;
        --ops)            OPS="$2";             shift 2 ;;
        --output)         OUTPUT_FILE="$2";     shift 2 ;;
        --no-update)      NO_UPDATE=1;          shift   ;;
        --cpp-proxy-port) CPP_PROXY_PORT="$2";  shift 2 ;;
        --vshard-path)    VSHARD_PATH="$2";     shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "${PROXY_BINARY}" ]]; then
    echo "ERROR: --proxy-binary is required" >&2
    exit 1
fi

if [[ ! -x "${PROXY_BINARY}" ]]; then
    echo "ERROR: proxy binary not found or not executable: ${PROXY_BINARY}" >&2
    exit 1
fi

# ── helpers ───────────────────────────────────────────────────────────────────
die() { echo "ERROR: $*" >&2; exit 1; }

check_port() {
    local host="${1%%:*}"
    local port="${1##*:}"
    nc -z "${host}" "${port}" 2>/dev/null
}

wait_port() {
    local addr="$1"
    local timeout="${2:-10}"
    local host="${addr%%:*}"
    local port="${addr##*:}"
    local i=0
    while ! nc -z "${host}" "${port}" 2>/dev/null; do
        sleep 0.5
        i=$((i + 1))
        if [[ $i -ge $((timeout * 2)) ]]; then
            return 1
        fi
    done
    return 0
}

# Extract numeric ops/sec from bench_vshard.lua output line:
#   "Result: 100000 ops in 1.234 s = 81069 ops/sec  errors=0"
parse_lua_bench() {
    grep -oP 'Result: \d+ ops in .* = \K\d+(?= ops/sec)' || echo "0"
}

# ── verify required tools ─────────────────────────────────────────────────────
command -v tarantool >/dev/null 2>&1 || die "tarantool not found in PATH"
command -v nc        >/dev/null 2>&1 || die "nc (netcat) not found in PATH"

# ── auto-start vshard cluster if not running ─────────────────────────────────
CLUSTER_STARTED=0

cluster_nodes_up() {
    check_port "${RS1_MASTER}" && check_port "${RS2_MASTER}" && check_port "${LUA_ROUTER}"
}

if ! cluster_nodes_up; then
    if [[ -z "${VSHARD_PATH}" ]]; then
        die "Cluster nodes not reachable and --vshard-path not set. " \
            "Either start the cluster manually or pass --vshard-path <vshard-example-dir>."
    fi
    # Resolve: user may pass path to vshard root or to the example sub-dir
    VSHARD_EXAMPLE_DIR="${VSHARD_PATH}"
    if [[ -f "${VSHARD_PATH}/example/Makefile" ]]; then
        VSHARD_EXAMPLE_DIR="${VSHARD_PATH}/example"
    fi
    if [[ ! -f "${VSHARD_EXAMPLE_DIR}/Makefile" ]]; then
        die "Cannot find Makefile in ${VSHARD_EXAMPLE_DIR} — check --vshard-path"
    fi
    echo "Cluster not running — starting via 'make start' in ${VSHARD_EXAMPLE_DIR} ..."
    (cd "${VSHARD_EXAMPLE_DIR}" && make start)
    CLUSTER_STARTED=1
    echo "Waiting for cluster nodes to come up..."
    for node in "${RS1_MASTER}" "${RS2_MASTER}" "${LUA_ROUTER}"; do
        if ! wait_port "${node}" 20; then
            die "Timed out waiting for ${node} after 'make start'"
        fi
    done
    echo "  Cluster started ✓"
fi

# ── check that storage nodes and Lua router are up ───────────────────────────
echo "Checking cluster connectivity..."
for node in "${RS1_MASTER}" "${RS2_MASTER}" "${LUA_ROUTER}"; do
    if ! check_port "${node}"; then
        die "Cannot connect to ${node} — is the vshard cluster running?"
    fi
done
echo "  Lua router  : ${LUA_ROUTER}  ✓"
echo "  RS1 master  : ${RS1_MASTER}  ✓"
echo "  RS2 master  : ${RS2_MASTER}  ✓"
echo ""

# ── start C++ proxy temporarily ───────────────────────────────────────────────
CPP_PROXY_ADDR="localhost:${CPP_PROXY_PORT}"
CPP_PROXY_PID=""
CPP_PROXY_TMPDIR="$(mktemp -d)"
CPP_PROXY_LOG="${CPP_PROXY_TMPDIR}/proxy.log"

cleanup() {
    if [[ -n "${CPP_PROXY_PID}" ]]; then
        kill "${CPP_PROXY_PID}" 2>/dev/null || true
        wait "${CPP_PROXY_PID}" 2>/dev/null || true
    fi
    rm -rf "${CPP_PROXY_TMPDIR}"
    if [[ "${CLUSTER_STARTED}" -eq 1 && -n "${VSHARD_EXAMPLE_DIR:-}" ]]; then
        echo "Stopping vshard cluster (was auto-started)..."
        (cd "${VSHARD_EXAMPLE_DIR}" && make stop) || true
    fi
}
trap cleanup EXIT

# ── generate proxy config + secdist ──────────────────────────────────────────
CPP_PROXY_CONFIG="${CPP_PROXY_TMPDIR}/static_config.yaml"
CPP_SECDIST="${CPP_PROXY_TMPDIR}/secdist.json"

# secdist format must match what VshardProxyComponent expects.
cat > "${CPP_SECDIST}" << JSON
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
                    "uuid": "ac522f65-a15e-4b1b-af2b-3a0a67d36fef",
                    "nodes": [
                        {"host": "${RS2_MASTER%%:*}", "port": ${RS2_MASTER##*:}, "is_master": true}
                    ]
                }
            ]
        }
    }
}
JSON

# Static config mirrors what the functional tests' conftest.py generates.
cat > "${CPP_PROXY_CONFIG}" << YAML
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
            config: ${CPP_SECDIST}
            missing-ok: false

        logging:
            fs-task-processor: fs-task-processor
            loggers:
                default:
                    file_path: '${CPP_PROXY_LOG}'
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

echo "Starting C++ proxy (port ${CPP_PROXY_PORT})..."
"${PROXY_BINARY}" --config "${CPP_PROXY_CONFIG}" \
    > "${CPP_PROXY_LOG}" 2>&1 &
CPP_PROXY_PID=$!

if ! wait_port "${CPP_PROXY_ADDR}" 15; then
    echo "WARN: C++ proxy did not start on port ${CPP_PROXY_PORT} within 15s" >&2
    echo "      Last 20 log lines:" >&2
    tail -20 "${CPP_PROXY_LOG}" >&2
    echo "      Skipping C++ proxy benchmark." >&2
    CPP_PROXY_PID=""
fi

if [[ -n "${CPP_PROXY_PID}" ]]; then
    echo "  C++ proxy   : ${CPP_PROXY_ADDR}  ✓ (pid=${CPP_PROXY_PID})"
fi
echo ""

# ── run benchmarks ────────────────────────────────────────────────────────────
BENCH_SCRIPT="${SCRIPT_DIR}/bench_vshard.lua"
BENCH_BINARY="$(dirname "${PROXY_BINARY}")/userver-tarantool-vshard-bench"

# Parse fiber counts list
IFS=',' read -ra FIBER_COUNTS_ARR <<< "${FIBER_COUNTS}"

# Arrays to collect results indexed by fiber count
declare -A LUA_RESULTS
declare -A CPP_PROXY_RESULTS
declare -A CPP_INPROC_RW_RESULTS
declare -A CPP_INPROC_RO_RESULTS

for FIBERS in "${FIBER_COUNTS_ARR[@]}"; do
    FIBERS="${FIBERS//[[:space:]]/}"  # trim whitespace

    echo "════════════════════════════════════════════"
    echo "Fibers = ${FIBERS}: Lua router benchmark (${OPS} ops)..."
    echo "════════════════════════════════════════════"
    LUA_RAW="$(tarantool "${BENCH_SCRIPT}" "${LUA_ROUTER}" "${FIBERS}" "${OPS}" 2>&1 | tee /dev/stderr)"
    LUA_RESULTS["${FIBERS}"]="$(echo "${LUA_RAW}" | parse_lua_bench)"
    echo ""

    if [[ -n "${CPP_PROXY_PID}" ]]; then
        echo "════════════════════════════════════════════"
        echo "Fibers = ${FIBERS}: C++ proxy benchmark (${OPS} ops)..."
        echo "════════════════════════════════════════════"
        CPP_PROXY_RAW="$(tarantool "${BENCH_SCRIPT}" "${CPP_PROXY_ADDR}" "${FIBERS}" "${OPS}" 2>&1 | tee /dev/stderr)"
        CPP_PROXY_RESULTS["${FIBERS}"]="$(echo "${CPP_PROXY_RAW}" | parse_lua_bench)"
        echo ""
    else
        CPP_PROXY_RESULTS["${FIBERS}"]="N/A"
    fi

    if [[ -x "${BENCH_BINARY}" ]]; then
        echo "════════════════════════════════════════════"
        echo "Fibers = ${FIBERS}: C++ in-process benchmark (${OPS} ops)..."
        echo "════════════════════════════════════════════"
        INPROC_RAW="$(TARANTOOL_VSHARD_BENCH=1 \
            VSHARD_BENCH_FIBERS="${FIBERS}" \
            VSHARD_BENCH_OPS="${OPS}" \
            "${BENCH_BINARY}" --gtest_filter="VshardBench*" 2>&1 | tee /dev/stderr)"
        CPP_INPROC_RW_RESULTS["${FIBERS}"]="$(echo "${INPROC_RAW}" | grep -oP 'RW.*?\K\d+(?= op/s)' | head -1 || echo "N/A")"
        CPP_INPROC_RO_RESULTS["${FIBERS}"]="$(echo "${INPROC_RAW}" | grep -oP 'RO.*?\K\d+(?= op/s)' | head -1 || echo "N/A")"
        echo ""
    else
        echo "WARN: ${BENCH_BINARY} not found, skipping in-process benchmark" >&2
        CPP_INPROC_RW_RESULTS["${FIBERS}"]="N/A"
        CPP_INPROC_RO_RESULTS["${FIBERS}"]="N/A"
    fi
done

# ── compute speedup for last fiber count ──────────────────────────────────────
LAST_FIBERS="${FIBER_COUNTS_ARR[-1]}"
LUA_LAST="${LUA_RESULTS["${LAST_FIBERS}"]}"
CPP_LAST="${CPP_PROXY_RESULTS["${LAST_FIBERS}"]}"
if [[ "${LUA_LAST}" != "0" && "${CPP_LAST}" != "N/A" && "${CPP_LAST}" != "0" ]]; then
    SPEEDUP=$(python3 -c "print(f'{${CPP_LAST}/${LUA_LAST}:.2f}x')" 2>/dev/null || echo "?")
else
    SPEEDUP="N/A"
fi

# ── format markdown section ───────────────────────────────────────────────────
TODAY="$(date '+%Y-%m-%d')"
ROUND_TITLE="Round $(date '+%Y%m%d') — ${TODAY}"
GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo "unknown")"

# Build table rows for Lua router and C++ proxy
LUA_TABLE_ROWS=""
CPP_PROXY_TABLE_ROWS=""
CPP_INPROC_RW_ROWS=""
CPP_INPROC_RO_ROWS=""

for FIBERS in "${FIBER_COUNTS_ARR[@]}"; do
    FIBERS="${FIBERS//[[:space:]]/}"
    LUA_OPS="${LUA_RESULTS["${FIBERS}"]:-N/A}"
    CPP_OPS="${CPP_PROXY_RESULTS["${FIBERS}"]:-N/A}"
    RW_OPS="${CPP_INPROC_RW_RESULTS["${FIBERS}"]:-N/A}"
    RO_OPS="${CPP_INPROC_RO_RESULTS["${FIBERS}"]:-N/A}"

    if [[ "${LUA_OPS}" != "0" && "${LUA_OPS}" != "N/A" && "${CPP_OPS}" != "N/A" && "${CPP_OPS}" != "0" ]]; then
        SPD=$(python3 -c "print(f'**+{(${CPP_OPS}/${LUA_OPS}-1)*100:.0f}%**')" 2>/dev/null || echo "?")
    else
        SPD="N/A"
    fi

    LUA_TABLE_ROWS+="| ${FIBERS} | $(printf '%s' "${LUA_OPS}" | sed 's/\([0-9]\{1,3\}\)\([0-9]\{3\}\)$/\1,\2/') | $(printf '%s' "${CPP_OPS}" | sed 's/\([0-9]\{1,3\}\)\([0-9]\{3\}\)$/\1,\2/') | ${SPD} |
"
    CPP_INPROC_RW_ROWS+="| ${FIBERS} | $(printf '%s' "${RW_OPS}" | sed 's/\([0-9]\{1,3\}\)\([0-9]\{3\}\)$/\1,\2/') | $(printf '%s' "${LUA_OPS}" | sed 's/\([0-9]\{1,3\}\)\([0-9]\{3\}\)$/\1,\2/') |
"
done

MD_SECTION="
### ${ROUND_TITLE} (ops=${OPS}, git=${GIT_SHA})

Fiber counts tested: ${FIBER_COUNTS}

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
${LUA_TABLE_ROWS}
Fibers: ${FIBER_COUNTS} | Total ops per run: ${OPS} | Build: $(basename "${PROXY_BINARY}")
"

echo "════════════════════════════════════════════"
echo "Results summary:"
echo "${MD_SECTION}"
echo "════════════════════════════════════════════"

if [[ "${NO_UPDATE}" -eq 0 ]]; then
    echo "${MD_SECTION}" >> "${OUTPUT_FILE}"
    echo "Results appended to: ${OUTPUT_FILE}"
else
    echo "(--no-update: not writing to ${OUTPUT_FILE})"
fi
