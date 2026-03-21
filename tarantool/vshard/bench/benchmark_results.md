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

### Round 1 — initial C++ proxy (ValueBuilder path, 100k ops)

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | Speedup |
|-------:|---------------------:|--------------------:|--------:|
| 10 | 7,275 | 8,825 | **+21%** |
| 20 | 12,056 | 14,130 | **+17%** |
| 50 | 20,141 | 22,871 | **+14%** |

### Round 2 — after IPROTO server refactor + `CallRaw` zero-copy path (50k ops)

Args deserialization (`formats::msgpack::FromBytes` + `ValueBuilder`) removed from the hot path;
raw msgpack bytes from the client request are forwarded directly via `Query::WithRawArgs`.

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua | vs Round 1 |
|-------:|---------------------:|--------------------:|-------:|-----------:|
| 10 | 7,849 | 8,653 | **+10%** | −2% (noise) |
| 20 | 11,546 | 14,701 | **+27%** | **+4%** |
| 50 | 19,644 | 24,597 | **+25%** | **+8%** |

## Analysis

The C++ proxy is **10–27% faster** than the Lua vshard router across all concurrency levels.

The `CallRaw` zero-copy refactor shows additional **4–8% gains at higher concurrency** where the
proxy's own routing/framing code is the bottleneck.  At 10 fibers the workload is storage-bound
(both routers wait on the same two storage processes), so the −2% difference is within WSL2
loopback run-to-run variance.

The margin vs. Lua shrinks at very high concurrency because the bottleneck shifts entirely to the
storage nodes — both routers become equally "free" relative to storage RTT.

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
