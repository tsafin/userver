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

### Round 20261023 — Phase 2 single-copy forwarding + connector hardening (git=d5e49cb1d + Phase 2)

Changes:

- **Phase 2**: `VshardProxy::CallForwardBytes` — the router CALL hot path now
  encodes the vshard.storage.call tuple once and forwards it via
  `ReplicasetPool::ForwardStorageCallDirect` →
  `Connection::SyncForwardStorageCall` (raw bytes, no `Query`, no
  `BuildQueryBody`, no pool acquire/release per request). Same MOVED/locked
  retry semantics as `DoCallRawBytes`, which is kept as a fallback.
- **Connector hardening**: `SendAndRegister` no longer silently overwrites a
  still-occupied pending slot (release builds compile UASSERT out) — the
  stale waiter now gets an explicit "pending slot overflow" error instead of
  hanging forever.
- **New tool in use:** pipelined loadgen (16 conns × 100 depth).

| Stand (idle box) | ops/s |
|---|---:|
| Phase 2 proxy, 16×100 (3 runs) | 84.8k / 84.5k / 83.2k |
| Phase 2 proxy, 8×100 | 75.3k |
| Phase 2 proxy, 4×100 | 65.8k |
| Phase 1c proxy, 16×100 (reference) | 88.1k |
| net.box sanity @100 fibers | 30.7k (single-proc, client-limited) |

Phase 2 is within run-to-run noise on this stand: with ~100k-class storage
RTT dominating, removing 2 copies + 2 allocs + pool churn per request does
not move aggregate throughput — it lowers proxy CPU per request, which will
matter once the stand has enough storage masters for the router to become
the bottleneck.

**Known issue found (connector, client-side, predates Phase 2):** under
deep pipelining (>~120 in-flight per connection) a request's response is
rarely lost; the unanswered entry stays registered, the slot ring wraps
after kPendingSlotCount=256 newer requests and the stale waiter used to get
a silent "Broken promise" (now: explicit "pending slot overflow" error).
Reproduced with the loadgen against the Lua router too — not related to the
proxy changes. Workaround until root-caused: keep loadgen depth ≤ 100 per
connection (16×100 = 1600 in-flight is enough to saturate this stand).

### Round 20261023b — 8 storage masters (Phase 0) + kPendingSlotCount 4096 (git=67a29b559 + slot fix)

Stand changes:

- vshard/example cluster extended from 2 to **8 replicasets**: RS3-RS8 added
  as master-only instances on 127.0.0.1:3311-3316 (storage_3.lua..storage_8.lua,
  localcfg.lua sharding, router replicasets, Makefile). 3000 buckets
  auto-rebalanced by vshard: 8 x 375, all available.
- Proxy secdist updated to 8 masters (run_benchmarks.sh template still
  generates 2 RS — update before next sweep).
- **Connector: kPendingSlotCount 256 -> 4096.** Root cause study of the
  "lost response" symptom showed nothing is ever lost at the byte level:
  all requests are flushed (staged == flushed bytes) and all responses are
  dispatched — but under deep pipelines (>~120 in-flight/conn) a small
  percentage of responses straggle behind 250+ later ones (tail latency on
  a saturated box / rebalancer activity). The 256-slot ring then wraps and
  evicts the still-registered straggler ("pending slot overflow" / formerly
  silent Broken promise). 4096 slots give ample span headroom
  (~64 KiB per connection). Task on file: characterize the straggler source.

| Stand (proxy + 13 tarantool instances + loadgen, 8 WSL2 cores, load ~7) | ops/s |
|---|---:|
| proxy, 16x100 | 87.6k / 88.1k |
| proxy, 16x200 | **101.7k** |
| proxy, 4x1000 | **105.1k** |
| proxy, 8x300 | 98.4k |
| proxy, 32x100 | 95.3k |
| Lua router, 16x200 (same client) | 76.3k |

Progression of proxy-path throughput over the quest: 43.3k (Round 7,
client-limited) -> 59.8k (multi-proc Lua) -> 88.1k (Phase 1c, 2 masters)
-> **105.1k (Phase 0+1c+2, 8 masters)**. Proxy vs Lua router: +33% at equal
load. Zero errors in all runs.

The box is now CPU-saturated (load avg ~7/8; proxy at ~1.2 cores, 13
tarantool processes): ~105k is the practical ceiling of this WSL2 stand,
not of the software. Reaching 1M needs the remaining Phase 0 item —
bare-metal / more cores / more masters — the router is no longer the
nearest bottleneck.

### Round 20261023c — wal_mode='none' + clean single-proxy measurements (git=1701e900a)

Stand: 8 masters, both replicas stopped, `wal_mode='none'` in
vshard/example/localcfg.lua (benchmark-only durability tradeoff, +23-25%
over wal_mode='write'). NOTE: always verify exactly one proxy process owns
:13306 — repeated restarts stack instances (SO_REUSEPORT) and skew results;
a 3-instance accident measured 130-132k while a single clean instance
measures 145-152k.

| Stand (8 WSL2 cores, load ~7, single proxy) | ops/s |
|---|---:|
| proxy, 16x200 | **145.7k** |
| proxy, 4x1000 | **151.8k** |
| proxy, 8x500 | 108.6k |
| Lua router, 16x200 (same client) | 108.8k |

**Proxy path progression across the quest: 43.3k (Round 7) -> 59.8k (multi-
proc Lua clients) -> 88.1k (Phase 1c) -> 105.1k (8 masters) -> 151.8k
(wal none). 3.5x total; proxy vs Lua router +34% at equal load.** Zero
errors everywhere; 169 unit tests green.

The box remains CPU-saturated (load ~7/8 across proxy + 9 tarantool
instances + loadgen): ~150k is the stand's hardware ceiling. Remaining
levers toward 1M: bare metal with more cores, more masters, and the
code-side Phases 3-5 (per-request allocations, wakeups, pool bypass)
to cut router CPU per request.

### Round 20261023d — proxy CPU per request: 4.84 -> 4.26 cpu-ms/1000 ops (-12%) (git=979262836 + CPU optimizations)

Changes (plan Phases 3b/4, applied where the profile pointed):

- Profile (perf, 16x200 load): proxy CPU is almost entirely userver
  scheduler machinery — TaskContext::DoStep + jump_fcontext (~15%),
  WaitListLight/MutexImpl/Awaiting bookkeeping, moodycamel queue ops,
  DetachedTasksSyncBlock::Dispose. Parsing/encoding are single digits.
- **Wakeup elision (Phase 4)**: `flush_event_` (Connection) and `out_event_`
  (ServerConn) are now signalled only while the flush task is actually
  parked (`flush_sleeping_`/`out_sleeping_` atomic flags, park-check-sleep
  ordering); producers skip the event entirely otherwise.
- **Spinlocks (Phase 3b)**: `pending_mutex_`, `staging_mutex_` (Connection)
  and `out_mutex_` (ServerConn) — critical sections are a couple of pointer
  stores — replaced futex-parked `engine::Mutex` with a 64-byte-aligned
  `SpinLock` (`storages/tarantool/impl/spinlock.hpp`).
- **Direct staging**: `SyncForwardStorageCall` now appends prefix + tuple
  straight into `staging_buf_` — one memcpy of the tuple instead of two
  copies via an intermediate body vector.

Measurement methodology: proxy CPU time (utime+stime from /proc) over a
fixed 200k-op run at moderate load (8 conns x 50 depth — box not saturated,
so proxy CPU/op is pure software cost), min of 3 runs per binary:

| binary | cpu-ms per 1000 ops |
|---|---:|
| before (979262836) | 4.84 |
| **after** | **4.26 (-12%)** |

At saturated load (~140k ops/s) aggregate throughput stays within the
box's noise band (133-148k) — the stand is CPU-starved overall, so proxy
savings shift CPU to tarantool rather than raising throughput. Sanity:
raw CALL ok, net.box 35.1k @100 fibers (single-proc, client-limited),
169 unit tests green, 0 errors.

Remaining proxy-CPU levers (plan Phase 3a/D): per-request detached handler
tasks are the biggest scheduler cost (task alloc + DetachedTasksSyncBlock +
queue ops); a persistent handler-coroutine pool per connection would remove
them, plus inline pending entries (make_shared per request).

### Round 20261023e — response staging + stack tuple (measured wins only)

- `ServerConn::SendResultFrame`: success replies are encoded directly into
  the connection's out_buf_ (two-phase preheader back-patch) — removes the
  per-reply frame vector + copy. Byte-identical to the old
  BuildResultFrameRaw output (verified).
- `DoCallForwardBytes`: the storage-call tuple is encoded into a 1 KiB
  UNINITIALIZED stack buffer (heap fallback for oversize) — removes one
  malloc per request. NB: `std::array{}"` zero-initialization made it
  SLOWER than the malloc it replaced — the `{}` mattered.

Measured (proxy CPU per 1000 ops, min-of-3, moderate load, single proxy
instance on the port — SO_REUSEPORT stacking skews everything):

| binary | cpu-ms/1000 ops |
|---|---:|
| ABC (10ab8d9f3) | ~4.1 |
| + SendResultFrame + stack tuple | **2.0-3.1** |

REJECTED experiment: handing the parsed frame buffer to the handler task by
move (zero-copy request handoff) measured ~25% WORSE — the reader has to
re-allocate the high-water buffer every frame and the handler frees it
cross-thread; three small copies (func, mode, args ≈ 100 B) are cheaper.

Sanity: byte-identical responses, 169 unit tests, 0 errors across 300k-op
pipelined runs and net.box bench (35.5k @100 fibers, client-limited).

### Round 20261023f — CPU-experiment sweep: no further in-framework gains (git=2966...296ed9df1)

Systematic evaluation of every remaining CPU lever (full spec:
`docs/cpu-experiments.md`; per-experiment specs + code in worktrees
`exp/e1-spinning`, `exp/e2-inline-slots`, `exp/e3-handler-pool`):

- E1 `spinning-iterations` (100..4000) + `worker_threads` (2..6): medians
  overlap, no effect.
- E2 inline pending entries: projected 2-4% — below the stand's noise
  floor; deferred.
- E3 persistent handler-coroutine pool: **fully implemented and
  validated** (169 tests, 0 errors, 148-149k ops/s) — proxy CPU medians
  identical to baseline (416-433 vs 420-439 ticks). The CV park/wake
  cycle costs the same as the per-request task spawn it replaces.
  **Rejected** to preserve simplicity.
- E4 execution-model alternatives (C++20 stackless, callbacks,
  thread-per-core + io_uring, AF_XDP): analysed, out of scope for this
  framework; documented with cost estimates.

**Decision: the proxy stays at HEAD. In-framework levers are exhausted;
proxy CPU per request is framework-intrinsic. The path to 1M ops/s is
hardware (10-12 masters on bare metal ≈ 80-100k each) — or a future
standalone thread-per-core/io_uring proxy, for which the protocol layer
(iproto_server.cpp + Connection) is already portable.**

### Round 20261023g — hygiene + trust + io_uring probe (git=39d5de4f7)

P1 hygiene:
- `run_benchmarks.sh --extra-master UUID:HOST:PORT` (repeatable) — the
  sweep protocol now covers the 8-RS stand.
- vshard example stand committed upstream (`~/src/vshard` f34d623).
- Experiment worktrees removed; branches `exp/e1-spinning`,
  `exp/e2-inline-slots`, `exp/e3-handler-pool`, `exp/p3-iouring-probe`
  keep the code + EXPERIMENTS.md.

P2 trust:
- Functional suite on the 8-RS stand: **56/72 pass**; 16 failures are
  stand-dependent (2-RS expectations hardcoded in tests, `router.sync`
  timeout under wal_mode=none) and reproduce 1:1 with the
  pre-optimization binary — not regressions. Conftest now auto-attaches
  live RS3-RS8 to secdist and fixtures expose `extra_masters`.
- ASAN build+run: in progress (separate toolchain configure).

P3 step 0 (io_uring floor probe): **blocked by the platform** — WSL2
kernel 6.18-microsoft delivers io_uring completions lazily/brokenly
(poll completion after ~15.4M busy-loop iterations; io_uring_submit
returns 0 with a non-full SQ). Code preserved in `exp/p3-iouring-probe`
(probe.c + uringtest.c + polltest.c + EXPERIMENTS-io_uring.md);
re-measure on bare metal. Side finding: direct storage with a lightweight
client = **243k ops/s** on one master (wal none) — the Lua-CALL path in
Tarantool costs ~2.4x over raw IPROTO replace.

Loadgen gained `--storage-direct` (vshard.storage.call arg format).

### Round 20261023h — ASAN clean (git=39d5de4f7)

AddressSanitizer build (RelWithDebInfo, USERVER_SANITIZE=addr; note:
configure requires -DUSERVER_IS_THE_ROOT_PROJECT=ON for the vshard
targets): 230k+ pipelined ops (incl. 16x200 depth-200 bursts) plus 8
parallel short-lived loadgen processes for connection churn (drain/close
paths) — **zero ASAN findings, zero request errors**, proxy survived all
teardowns. The concurrent request handling (Phase 1c) and forwarding path
(Phase 2) are memory-clean under ASAN.

### Round 20261024 — functional suite green on the 8-RS canonical stand (git=2b742b263)

The scaled stand exposed a real product bug:
- **callbre on replica-less replicasets raised "All replicas unavailable"**
  while Lua vshard falls back to the master; fixed in all 8 forward/execute
  methods of ReplicasetPool (any production master-only topology would hit
  this).
- Proxy error fidelity: BuildStorageCallErrorReturn now passes the storage
  IPROTO error code through (masked & 0x3FFF, matching box.error.code).

Cluster note: wal_mode='none' breaks vshard replication (replicas cannot
sync) — the functional suite requires the canonical wal_mode='write' stand;
localcfg.lua now switches via VSHARD_WAL_NONE=1. A storage_2_a restart in
the middle of the earlier rebalancing left split-brain bucket ownership
(router 375x8 vs storage_1_a holding 1500) — fixed by full rebootstrap;
keep an eye on bucket ownership after restarting RS instances mid-transfer.

Final state: **64 passed, 8 skipped** (documented stand/version skips),
stable across consecutive runs. TSAN run pending build fix.

### Round 20261024b — TSAN run (git=2b742b263 + tsan.hpp GCC fix)

- Fixed userver's `universal/.../compiler/impl/tsan.hpp` for GCC: it probed
  only `__has_feature(thread_sanitizer)` (Clang-only); GCC defines
  `__SANITIZE_THREAD__` instead, so every GCC+TSAN build failed with
  "Broken CMake: thread sanitizer is not used however Boost thinks it is".
  Also required `setarch -R` (TSAN + high-entropy ASLR mapping error).
- 30k pipelined ops (8x50) under TSAN: 31 reports, ALL in framework /
  3rd-party code — StackUsageMonitor telemetry (2), boost::lockfree
  tagged-pointer freelist of the connection pool (27; known TSAN false
  positives with bit-packed atomics), ResultStore plumbing (2). **Zero
  races attributable to vshard proxy or connector hot-path logic.**
  Caveat: TSAN is blind to stackful-coroutine switches, so cross-coroutine
  happens-before is under-reported; ASAN (clean) + functional coverage
  remain the primary correctness gates.
