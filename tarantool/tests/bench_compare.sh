#!/usr/bin/env bash
# bench_compare.sh — run net.box and userver connector benchmarks side-by-side.
#
# Usage:
#   ./bench_compare.sh [host] [port] [binary]
#
# Defaults:
#   host   = 127.0.0.1
#   port   = 3301
#   binary = auto-detected from build_release or build_debug under the repo root
#
# Environment variables (override defaults):
#   TARANTOOL_HOST   — Tarantool host
#   TARANTOOL_PORT   — Tarantool port
#   TTTEST_BINARY    — explicit path to userver-tarantool_tttest
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

HOST="${TARANTOOL_HOST:-${1:-127.0.0.1}}"
PORT="${TARANTOOL_PORT:-${2:-3301}}"
OUTPUT_DIR="${BENCH_OUTPUT:-/tmp}"

# ── locate userver-tarantool_tttest binary ────────────────────────────────────

if [[ -n "${TTTEST_BINARY:-}" ]]; then
    BINARY="${TTTEST_BINARY}"
else
    # Search in common build locations
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

# ── connectivity check ────────────────────────────────────────────────────────

echo "Checking Tarantool connectivity at ${HOST}:${PORT} ..."
if ! tarantool -e "
local ok, err = pcall(function()
    local c = require('net.box').connect('${HOST}:${PORT}', {wait_connected=2})
    assert(c:ping(), 'ping failed')
    c:close()
end)
if not ok then print('FAIL: '..tostring(err)); os.exit(1) end
print('OK')
os.exit(0)
" 2>/dev/null | grep -q OK; then
    echo "ERROR: cannot reach Tarantool at ${HOST}:${PORT}" >&2
    exit 1
fi
echo "OK"
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
