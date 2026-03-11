#!/usr/bin/env bash
# bench_compare.sh — run net.box and userver connector benchmarks side-by-side.
#
# Self-contained: starts its own Tarantool instance on a random free port and
# stops it on exit.  No pre-existing Tarantool required.
#
# Usage:
#   ./bench_compare.sh
#
# Environment variables:
#   TTTEST_BINARY    — explicit path to userver-tarantool_tttest
#                      (auto-detected from common build dirs if not set)
#   BENCH_OUTPUT     — directory for individual output files (default: /tmp)
#
# Output:
#   Prints interleaved results to stdout.
#   Writes individual raw outputs to $BENCH_OUTPUT/netbox_bench.out
#   and $BENCH_OUTPUT/userver_bench.out for post-processing.
#
# Requirements:
#   tarantool (with net.box, Lua 5.1+)
#   userver-tarantool_tttest binary (built with USERVER_BUILD_TESTS=ON)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

HOST="127.0.0.1"
OUTPUT_DIR="${BENCH_OUTPUT:-/tmp}"

# ── locate userver-tarantool_tttest binary ────────────────────────────────────

if [[ -n "${TTTEST_BINARY:-}" ]]; then
    BINARY="${TTTEST_BINARY}"
else
    for candidate in \
        "${REPO_ROOT}/build_release/tarantool/userver-tarantool_tttest" \
        "${REPO_ROOT}/build_debug/tarantool/userver-tarantool_tttest" \
        "${REPO_ROOT}/tmp/build_tarantool/userver/tarantool/userver-tarantool_tttest"
    do
        if [[ -x "${candidate}" ]]; then
            BINARY="${candidate}"
            break
        fi
    done
fi

if [[ -z "${BINARY:-}" || ! -x "${BINARY}" ]]; then
    echo "ERROR: could not find userver-tarantool_tttest binary." >&2
    echo "  Build with: make build-release (or set TTTEST_BINARY)" >&2
    exit 1
fi

LUA_SCRIPT="${SCRIPT_DIR}/netbox_bench.lua"
if [[ ! -f "${LUA_SCRIPT}" ]]; then
    echo "ERROR: ${LUA_SCRIPT} not found" >&2
    exit 1
fi

if ! command -v tarantool &>/dev/null; then
    echo "ERROR: tarantool binary not found in PATH" >&2
    exit 1
fi

# ── pick a random free port ───────────────────────────────────────────────────

pick_free_port() {
    python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
"
}

PORT="$(pick_free_port)"

# ── start a dedicated Tarantool instance ─────────────────────────────────────

TT_WORKDIR="$(mktemp -d /tmp/tt_bench.XXXXXX)"
TT_PID=""

cleanup() {
    if [[ -n "${TT_PID}" ]]; then
        kill "${TT_PID}" 2>/dev/null || true
        wait "${TT_PID}" 2>/dev/null || true
    fi
    rm -rf "${TT_WORKDIR}"
}
trap cleanup EXIT

echo "Starting Tarantool on ${HOST}:${PORT} (workdir: ${TT_WORKDIR}) ..."

tarantool -e "
box.cfg{
    listen        = '${HOST}:${PORT}',
    work_dir      = '${TT_WORKDIR}',
    log_level     = 1,
    wal_mode      = 'none',
    memtx_memory  = 67108864,
}
box.once('schema', function()
    local s = box.schema.space.create('test', {if_not_exists=true})
    s:format({{name='id',type='unsigned'},{name='val',type='string'}})
    s:create_index('primary', {type='hash', parts={'id'}, if_not_exists=true})
    box.schema.user.grant('guest','read,write,execute,create,drop','universe',
                          nil, {if_not_exists=true})
end)
require('fiber').sleep(3600)
" &>/dev/null &
TT_PID=$!

# Wait until the port is accepting connections (up to 10 s)
for i in $(seq 1 20); do
    if tarantool -e "
local ok = pcall(function()
    local c = require('net.box').connect('${HOST}:${PORT}',{wait_connected=2})
    assert(c:ping())
    c:close()
end)
os.exit(ok and 0 or 1)
" &>/dev/null; then
        break
    fi
    sleep 0.5
    if [[ $i -eq 20 ]]; then
        echo "ERROR: Tarantool on port ${PORT} did not start in time" >&2
        exit 1
    fi
done
echo "OK (pid ${TT_PID})"
echo

# ── run net.box benchmark ─────────────────────────────────────────────────────

NETBOX_OUT="${OUTPUT_DIR}/netbox_bench.out"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  net.box benchmark  (Lua / Tarantool $(tarantool --version 2>&1 | head -1))"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
tarantool "${LUA_SCRIPT}" "${HOST}" "${PORT}" 2>/dev/null | tee "${NETBOX_OUT}"

echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  userver C++ connector benchmark"
echo "  binary: ${BINARY}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

USERVER_OUT="${OUTPUT_DIR}/userver_bench.out"
TARANTOOL_HOST="${HOST}" TARANTOOL_PORT="${PORT}" \
    "${BINARY}" --gtest_filter="TarantoolBench.InsertThroughput" 2>/dev/null \
    | tee "${USERVER_OUT}"

# ── summary table ─────────────────────────────────────────────────────────────

echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SUMMARY: key data points extracted from raw output"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo
echo "net.box (Lua) highlights:"
grep -E "sequential|fibers=64|fibers=32" "${NETBOX_OUT}" | head -8 || true

echo
echo "userver (ev_threads=1) highlights:"
grep -E "sequential|coro=64\)|coro=128\)" "${USERVER_OUT}" | head -12 || true

echo
echo "Raw outputs saved to:"
echo "  ${NETBOX_OUT}"
echo "  ${USERVER_OUT}"
