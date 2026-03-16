#!/usr/bin/env tarantool
--
-- net.box benchmark: PING and REPLACE throughput.
-- Run: tarantool netbox_bench.lua [host] [port]
-- Compares sequential, pipelined (fibers), and dedicated-conn modes.
--

local fiber = require('fiber')
local net_box = require('net.box')
local clock = require('clock')

local host = arg and arg[1] or '127.0.0.1'
local port = tonumber(arg and arg[2]) or 13301
local addr = host .. ':' .. tostring(port)

local PING_N      = 10000   -- pings per coroutine/fiber
local REPLACE_N   = 5000    -- replaces per coroutine/fiber
local VAL32       = string.rep('x', 32)

-- ── helpers ──────────────────────────────────────────────────────────────────

local function fmt_rps(ops, elapsed_s)
    local rps = ops / elapsed_s
    if rps >= 1e6 then
        return string.format('%.2f M op/s  %.1f µs/op', rps/1e6, 1e6/rps)
    else
        return string.format('%.0f k op/s  %.1f µs/op', rps/1000, 1e6/rps)
    end
end

local function bench(label, ops, fn)
    local t0 = clock.monotonic()
    fn()
    local elapsed = clock.monotonic() - t0
    print(string.format('  %-44s %s  [%d ops / %.0f ms]',
        label, fmt_rps(ops, elapsed), ops, elapsed * 1000))
end

-- ── sequential ───────────────────────────────────────────────────────────────

local function run_sequential(conn, n)
    for i = 1, n do
        conn.space.kv:replace{i, VAL32}
    end
end

local function run_ping_sequential(conn, n)
    for i = 1, n do
        conn:ping()
    end
end

-- ── fiber-based (concurrent, shared conn) ────────────────────────────────────

local function run_fibers(fn, concurrency, n_each)
    local total = concurrency * n_each
    local done = 0
    local ch = fiber.channel(concurrency)

    for c = 1, concurrency do
        local base = (c - 1) * n_each
        fiber.create(function()
            fn(c, base, n_each)
            ch:put(true)
        end)
    end
    for _ = 1, concurrency do ch:get() end
    return total
end

-- ── dedicated-conn: one connection per fiber ─────────────────────────────────

local function run_dedicated(concurrency, n_each)
    local total = concurrency * n_each
    local ch = fiber.channel(concurrency)

    for c = 1, concurrency do
        local base = (c - 1) * n_each
        fiber.create(function()
            local conn = net_box.connect(addr, {wait_connected=true})
            for i = 1, n_each do
                conn.space.kv:replace{base + i, VAL32}
            end
            conn:close()
            ch:put(true)
        end)
    end
    for _ = 1, concurrency do ch:get() end
    return total
end

-- ── main ─────────────────────────────────────────────────────────────────────

print('\n╔══ net.box benchmark  addr=' .. addr .. ' ══════════════════════════╗')

-- Single shared connection
local conn = net_box.connect(addr, {wait_connected=true})
assert(conn:ping(), 'cannot reach tarantool at ' .. addr)

-- PING: sequential
bench('ping sequential (1 fiber, 1 conn)',
    PING_N, function() run_ping_sequential(conn, PING_N) end)

-- PING: concurrent fibers sharing one connection (pipelining)
for _, coro in ipairs({4, 8, 16, 32, 64}) do
    local n = PING_N
    bench(string.format('ping fibers (shared conn, fibers=%d)', coro),
        coro * n,
        function()
            run_fibers(function(_, _, each)
                for i = 1, each do conn:ping() end
            end, coro, n)
        end)
end

print('╠══════════════════════════════════════════════════════════════════╣')

-- REPLACE: sequential (1 conn)
bench('replace sequential (1 fiber, 1 conn)',
    REPLACE_N, function() run_sequential(conn, REPLACE_N) end)

-- REPLACE: concurrent fibers sharing one connection (pipelining)
for _, coro in ipairs({4, 8, 16, 32, 64}) do
    local n = REPLACE_N
    bench(string.format('replace fibers (shared conn, fibers=%d)', coro),
        coro * n,
        function()
            run_fibers(function(c, base, each)
                for i = 1, each do
                    conn.space.kv:replace{base + i, VAL32}
                end
            end, coro, n)
        end)
end

print('╠══════════════════════════════════════════════════════════════════╣')

-- REPLACE: dedicated connection per fiber
for _, coro in ipairs({4, 8, 16, 32, 64}) do
    local n = REPLACE_N
    bench(string.format('replace dedicated (conn=fibers=%d)', coro),
        coro * n,
        function() run_dedicated(coro, n) end)
end

conn:close()
print('╚══════════════════════════════════════════════════════════════════╝\n')
os.exit(0)
