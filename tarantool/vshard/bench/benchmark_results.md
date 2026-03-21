# vshard Proxy Benchmark Results

## Setup

**Cluster topology:**
- RS1 (`cbf06940`, ports 3301/3302): 0 active buckets (not yet rebalanced)
- RS2 (`ac522f65`, ports 3303/3304): 1500 active buckets

**Compared systems (same hop count: client → router/proxy → storage):**
- **Baseline**: Lua vshard router (Tarantool process, port 3305)
- **C++ proxy**: `userver-tarantool-vshard-sample` release build (port 3306)

**Workload:**  
`vshard.router.callrw(bucket_id, 'box.space.customer:replace', {{bid, bid, 'x'}})`  
Bucket IDs 1–1500 (confirmed active on RS2), round-robin per fiber.  
Script: `bench_vshard.lua`

**Host:** WSL2 on Linux, loopback network

## Results

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | Speedup |
|-------:|---------------------:|--------------------:|--------:|
| 10 | 7,275 | 8,825 | **+21%** |
| 20 | 12,056 | 14,130 | **+17%** |
| 50 | 20,141 | 22,871 | **+14%** |

100,000 operations per run.

## Analysis

The C++ proxy is **14–21% faster** at identical concurrency levels. The margin shrinks at higher
concurrency because the bottleneck shifts to the storage nodes and loopback RTT — both routers
are bounded by the same two Tarantool storage processes.

The routing overhead contribution (bucket table lookup, replicaset selection, IPROTO framing) is
proportionally smaller at high concurrency, which is why the relative speedup decreases from 21%
at 10 fibers to 14% at 50 fibers.

## How to Reproduce

Start a 4-node vshard cluster (see `configs/` for static config and secdist) then:

```bash
# Baseline: Lua router on :3305
tarantool bench/bench_vshard.lua localhost:3305 20 100000

# C++ proxy on :3306
cp tarantool/vshard/configs/secdist.json /tmp/vshard_secdist.json
./build_release/userver/tarantool/vshard/userver-tarantool-vshard-sample \
    --config tarantool/vshard/configs/static_config.yaml &
tarantool bench/bench_vshard.lua localhost:3306 20 100000
```
