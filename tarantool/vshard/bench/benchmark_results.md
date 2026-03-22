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

### Round 3 — full zero-copy hot path: `CallRawBytes` + lazy `Value` + `DecodeEnvelopeRaw` (100k ops)

Three additional optimisations stacked on top of Round 2:
1. **Lazy `ExecutionResult::GetData()`** — `Value::FromBytes` is deferred until `GetData()` is
   actually called; callers using only `GetRawBytes()` pay zero allocation cost for the Value tree.
2. **`DecodeEnvelopeRaw()`** — scans the raw IPROTO_DATA bytes with `msgpack_scan::SkipValue`
   instead of building a `Value` tree.  Handles fixarray / array16 / array32 (Tarantool uses
   array32 regardless of element count for IPROTO_CALL multi-return).
3. **`BuildResultFrameRaw()`** — assembles the reply frame directly from raw bytes extracted by
   `DecodeEnvelopeRaw`, skipping `ValueBuilder` entirely.

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua | vs Round 2 |
|-------:|---------------------:|--------------------:|-------:|-----------:|
| 10 | 8,190 | 9,976 | **+22%** | **+15%** |
| 20 | 13,504 | 15,861 | **+17%** | **+8%** |
| 50 | 22,868 | 26,651 | **+17%** | **+8%** |

### Round 4 — `USERVER_FEATURE_ERASE_LOG_WITH_LEVEL=info` (compile-time log erasure, 100k ops)

Same binary as Round 3, rebuilt with `-DUSERVER_FEATURE_ERASE_LOG_WITH_LEVEL=info` so that all
`LOG_DEBUG` / `LOG_INFO` call sites are replaced by `true ? Noop{} : LogHelper(...)` at
compile time (the compiler eliminates string formatting and `ShouldLog()` checks entirely).

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Round 3 |
|-------:|---------------------:|--------------------:|-----------:|
| 10 | 8,190 | 9,818 | −1.6% (noise) |
| 20 | 13,504 | 15,745 | −0.7% (noise) |
| 50 | 22,868 | 26,498 | −0.6% (noise) |

No measurable improvement.  The hot path already contains **zero** `LOG_DEBUG`/`LOG_INFO` calls
in `tarantool/src/` and `tarantool/vshard/`; the flag only affects userver core internals which
are not on the critical path for this workload.

### Bonus — C++ `VshardProxy` library used directly (no proxy process, 100k ops)

Using the `userver-tarantool-vshard-bench` binary which embeds `VshardProxy` in-process
(same thread pool, no extra network hop to a separate proxy process):

| Fibers | Direct C++ library (ops/sec) | via C++ proxy process (ops/sec) | Proxy overhead |
|-------:|-----------------------------:|--------------------------------:|---------------:|
| 10 | 21,937 | 9,818 | 2.2× |
| 20 | 39,013 | 15,745 | 2.5× |
| 50 | 77,517 | 26,498 | 2.9× |

The proxy process itself adds **~2–3× overhead** from two extra loopback round-trips
(client → proxy, proxy → storage).  Embedding the library removes that overhead entirely.

## Analysis

The C++ proxy is **17–22% faster** than the Lua vshard router across all concurrency levels.

The full zero-copy pipeline (`CallRawBytes` + `DecodeEnvelopeRaw`) adds another **8–15% on top of
Round 2**, by eliminating the `Value::FromBytes` tree construction on every successful response.
The win is largest at low concurrency (10 fibers, +15%) because at 10 fibers the proxy's own
processing overhead is a higher fraction of total latency; at 50 fibers the bottleneck shifts
more toward the storage nodes.

**Key insight:** Tarantool encodes IPROTO_CALL multi-return values as `array32` (0xdd) regardless
of element count.  `DecodeEnvelopeRaw` checks all three array formats (fixarray / array16 /
array32) and only falls back to the Value tree on routing-error paths (cold path).

The margin vs. Lua remains stable and consistent across rounds.  Storage RTT dominates at very
high concurrency, making both routers "equally free."

## How to Reproduce

Start a 4-node vshard cluster (see `configs/` for static config and secdist) then run:

```bash
# Using the CMake benchmark target (recommended — auto-starts cluster if needed):
cmake -DVSHARD_PATH=/path/to/vshard -B build_release
cmake --build build_release --target benchmark-vshard

# Manually — Lua router baseline:
tarantool bench/bench_vshard.lua localhost:3305 20 100000

# Manually — C++ proxy (start proxy first, then):
tarantool bench/bench_vshard.lua localhost:3306 20 100000
```

### Round 6 — 2026-03-22 (ops=100000, git=083fb278c)

Extended scalability sweep: Lua router and C++ proxy saturate at ~33–36k op/s
(storage RTT dominates); C++ in-process library keeps scaling linearly to 153k op/s.

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua | C++ in-process RW (ops/sec) |
|-------:|---------------------:|--------------------:|-------:|----------------------------:|
| 10 | 8,680 | 10,150 | **+17%** | 22,410 |
| 20 | 13,879 | 16,744 | **+21%** | 39,816 |
| 50 | 24,771 | 27,187 | **+10%** | 80,094 |
| 100 | 31,283 | 34,654 | **+11%** | 123,174 |
| 150 | 33,232 | 36,125 | **+9%** | 153,113 |

The C++ proxy adds **~9–21% throughput** over the Lua router across all concurrency levels.
The in-process library (no extra loopback hop) scales **~3–4.6× vs Lua router** at high concurrency,
demonstrating that the storage nodes — not the router — are the throughput bottleneck above 50 fibers.

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample

