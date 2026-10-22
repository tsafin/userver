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


### Round 7 — 2026-03-24 (ops=100000, git=e5fb59c4a)

Phase 3+4: migrate `Pool::Execute/ForwardStorageCall/ForwardVshardCall/Ping` from
`Future/Promise` to `SyncPendingEntry` + `CollectSyncEntry`. Pool slot released
before waiting (pipelining preserved), Promise/Future heap allocation eliminated
from the synchronous hot path.

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua | C++ in-process RW (ops/sec) |
|-------:|---------------------:|--------------------:|-------:|----------------------------:|
| 10 | 9,016 | 10,581 | **+17%** | 23,800 |
| 20 | 14,366 | 16,796 | **+17%** | 43,083 |
| 50 | 25,214 | 27,074 | **+7%** | 79,717 |
| 100 | 30,432 | 36,779 | **+21%** | 135,540 |
| 150 | 33,048 | 39,787 | **+20%** | 168,505 |

**vs Round 6 (pre-Phase 3+4):** C++ proxy +4–10% at high concurrency (100–150 fibers);
in-process library +6–10% across the board, most visible at ≥100 fibers.
The gain is consistent with removing one heap-allocated shared future state per
request on the synchronous call path.

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample


### Round 20260325 — 2026-03-25 (ops=100000, git=ebfeaf02d)

Fiber counts tested: 10,20,50,100,150

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 10 | 9,409 | 11,117 | **+18%** |
| 20 | 15,375 | 18,313 | **+19%** |
| 50 | 24,355 | 29,064 | **+19%** |
| 100 | 33,938 | 35,734 | **+5%** |
| 150 | 37,338 | 42,588 | **+14%** |

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample


### Round 20260326 — 2026-03-26 (ops=100000, git=5ae943a56)

Fiber counts tested: 10,20,50,100,150

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 10 | 9,065 | 10,849 | **+20%** |
| 20 | 15,325 | 17,804 | **+16%** |
| 50 | 24,767 | 29,231 | **+18%** |
| 100 | 32,997 | 39,048 | **+18%** |
| 150 | 36,517 | 43,314 | **+19%** |

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample


### Round 20261022 — 2026-10-22 (ops=100000, git=0b6cedcd0)

Fiber counts tested: 10,20,50,100,150

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 10 | 9,532 | 10,730 | **+13%** |
| 20 | 15,250 | 16,889 | **+11%** |
| 50 | 24,185 | 28,931 | **+20%** |
| 100 | 33,963 | 38,114 | **+12%** |
| 150 | 29,600 | 44,560 | **+51%** |

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample

**Stand:** Tarantool upgraded 2.6.0 → 2.11.10 with `iproto_threads=4`,
`wal_queue_max_size=64MB` in `vshard/example/localcfg.lua` (measured storage
ceiling through `vshard.storage.call` + replace: 73.4k op/s vs 52.4k on 2.6 —
see `tarantool/docs/1Mops-plan.md` §1.1.2). C++ in-process RW at 150 fibers:
174,758 op/s.

Caveats: the Lua router dipped at 150 fibers (29.6k vs 36.5k in Round
20260326), inflating the +51% proxy-vs-Lua gap; the single Lua loadgen
process (~40–45k ceiling) still masks the proxy's own improvements — the
proxy gain vs Round 20260326 on identical paths is within noise (+3%).

### Round 20261022 — 2026-10-22 (ops=100000, git=0b6cedcd0)

Fiber counts tested: 10,20,50,100,150

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 10 | 6,954 | 8,222 | **+18%** |
| 20 | 14,403 | 16,267 | **+13%** |
| 50 | 24,260 | 22,551 | **+-7%** |
| 100 | 27,616 | 30,176 | **+9%** |
| 150 | 27,728 | 33,403 | **+20%** |

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample


### Round 20261022 — 2026-10-22 (ops=100000, git=0b6cedcd0)

Fiber counts tested: 10,20,50,100,150

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 10 | 8,948 | 10,075 | **+13%** |
| 20 | 14,588 | 17,292 | **+19%** |
| 50 | 24,880 | 29,230 | **+17%** |
| 100 | 26,708 | 24,046 | **+-10%** |
| 150 | 21,484 | 20,991 | **+-2%** |

Fibers: 10,20,50,100,150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample


### Round 20261022 — 2026-10-22 (ops=100000, git=0b6cedcd0)

Fiber counts tested: 150

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 150 | 37,991 | 44,429 | **+17%** |

Fibers: 150 | Total ops per run: 100000 | Build: userver-tarantool-vshard-sample


### Round 20261022b — Phase 1 evaluation, multi-process loadgen (ops=50000/proc, git=0b6cedcd0 + Phase 1 WIP)

**Methodology upgrade:** single-process Lua loadgen saturates at ~40-45k ops/s and,
being synchronous per fiber (1 in-flight request per connection), hides router-side
syscall savings (no pipelining => no read/write batching to coalesce). New protocol:
**4 parallel `bench_vshard.lua` processes x 100 fibers x 50000 ops**, aggregate sum.
Latency derived via Little's law: RTT ~= 400 inflight / aggregate ops-per-sec.

| Path (aggregate, 4 clients x 100 fibers) | ops/s | RTT |
|---|---:|---:|
| Direct `vshard.storage.call` -> RS2 master :3303 (no router) | 103k | ~3.9 ms |
| C++ proxy, baseline HEAD `0b6cedcd0` | 61.2k | ~6.5 ms |
| C++ proxy, **Phase 1** (buffered reads + write coalescing) | 59.8k | ~6.7 ms |
| Lua router :3305 | 49.5k | ~8.1 ms |

Findings:

1. **Phase 1 effect is not measurable on this stand**: baseline vs Phase 1
   within noise (~2%). The Lua loadgen is synchronous — one request in flight
   per connection — so multi-frame `RecvSome` batches and response coalescing
   never trigger; and throughput is latency-bound (Little's law), not
   syscall-bound. Phase 1 remains correct (200k ops, 0 errors, 169 unit tests
   green) and should pay off with pipelined clients (C++ loadgen) or
   async calls.
2. **The real gap is proxy-added latency, ~2.8 ms per request** (6.5 ms vs
   3.9 ms direct). With 4 worker threads at 60k ops/s the proxy is nowhere
   near CPU saturation — the cost is per-request task hops/wakeups in the
   coroutine scheduler (~6 hops: server task -> proxy -> flush -> reader ->
   ready -> collect), not msgpack or syscalls.
3. Earlier sweeps this round showed wild variance (proxy @150f: 44.5k -> 33k
   -> 21k) caused by environment noise (parallel shell activity, WSL2 load
   avg up to 7.9/8). A clean re-run restored 44.4k @150f. Sweep numbers are
   only comparable when the box is idle.

Next lever (revised order): attack per-request task hops and pool/entry
allocations (plan Phases 2-5), measure as **proxy-added RTT** on the
multi-process stand; add a pipelined C++ loadgen so batching phases can
show their effect.

### Round 20261022c — Concurrent request handling in iproto_server (Phase 1c), pipelined C++ loadgen (git=0b6cedcd0 + Phase 1 WIP)

**New tool:** `userver-tarantool-vshard-loadgen` (`vshard/loadgen/loadgen.cpp`) —
standalone pipelined IPROTO client built on the connector's `impl::Connection`
(`--conns N --depth K`: N connections × K in-flight requests). The Lua
loadgen was synchronous (1 in-flight per conn) and masked all router-side
concurrency; the C++ loadgen exposes them.

**Proxy change (Phase 1c):** `HandleConnection` no longer processes one
request per connection at a time. vshard CALL requests are parsed in the
reader loop (validation stays inline) and executed in detached handler tasks
(`engine::DetachUnscopedUnsafe(engine::AsyncNoSpan(...))`, bounded by an
`engine::Semaphore` of 512 in-flight per connection); replies are staged via
a thread-safe `ServerConn::Send` and pushed out by a per-connection flush
task (write coalescing, Phase 1b).

> **Debugging note:** a temporary `TaskWithResult` returned by
> `AsyncNoSpan` is destroyed at the end of the statement — and the
> `engine::Task` destructor *cancels and joins* the task, killing the
> handler before its first step. All requests hung forever. Detach
> explicitly.

Measurements (bench=loadgen `vshard.router.callrw`→customer replace,
bucket_max=1500; idle box; 169 unit tests green, 0 errors everywhere):

| Stand | ops/s |
|---|---:|
| proxy HEAD (serial per conn), 4 conns × depth 100 | 4.6k |
| proxy Phase 1c, 4 conns × depth 25 | 42.3k |
| proxy Phase 1c, 4 conns × depth 100 | **70.9k** |
| proxy Phase 1c, 16 conns × depth 50 | 77.2k |
| proxy Phase 1c, 16 conns × depth 100 | **88.1k** |
| Lua router, 16 conns × depth 100 (same client) | 69.5k |
| direct `vshard.storage.call` → :3303 (no router, 4 Lua procs) | ~103k |

net.box sanity: 32.6k @100 fibers (single Lua proc, still client-limited).

Findings:

1. Phase 1c removes the per-connection serialization: throughput now scales
   with total in-flight requests (conns × depth), not with connection count.
   At equal client setup the C++ proxy is now **+27% faster than the Lua
   router** and within ~15% of the no-router direct-storage path.
2. The pipeline plumbing (flush task coalescing) is now actually exercised:
   with 1600 in-flight, server-side batching engages on both directions.
3. Next ceiling per Little's law is the storage path itself; router-side,
   the remaining cost is the per-request task spawn + arg copy (Phases 2-3)
   and pool acquire churn (Phase 5).
