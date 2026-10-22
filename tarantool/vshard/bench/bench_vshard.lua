local clock = require('clock')
local fiber = require('fiber')
local nb = require('net.box')

local URI = arg[1] or 'localhost:3305'
local FIBERS = tonumber(arg[2]) or 20
local OPS = tonumber(arg[3]) or 50000
local BUCKET_MAX = 1500  -- only use buckets with confirmed active status

print(string.format('[%s] fibers=%d ops=%d bucket_range=1-%d', URI, FIBERS, OPS, BUCKET_MAX))

local ops_per_fiber = math.floor(OPS / FIBERS)
local total_done = 0
local total_errors = 0
local latch = fiber.channel(0)

local t0 = clock.time()
local fiber_list = {}

for i = 1, FIBERS do
    local f = fiber.create(function()
        local c = nb.connect(URI, {wait_connected=5})
        if not c:wait_connected(3) then
            total_errors = total_errors + ops_per_fiber
            return
        end
        local bid = (i-1) * math.floor(BUCKET_MAX/FIBERS) + 1
        local errors = 0
        for _ = 1, ops_per_fiber do
            local b = (bid % BUCKET_MAX) + 1
            local ok = pcall(c.call, c, 'vshard.router.callrw',
                {b, 'box.space.customer:replace', {{b, b, 'x'}}})
            if not ok then errors = errors + 1 end
            bid = bid + 1
        end
        total_done = total_done + (ops_per_fiber - errors)
        total_errors = total_errors + errors
        c:close()
    end)
    f:set_joinable(true)
    fiber_list[i] = f
end

for i = 1, FIBERS do
    fiber_list[i]:join()
end

local elapsed = clock.time() - t0
print(string.format('Result: %d ops in %.3f s = %.0f ops/sec  errors=%d',
    total_done, elapsed, total_done/elapsed, total_errors))
os.exit(0)
