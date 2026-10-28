# Road to 1M ops/sec: vshard C++ Router Overhead Analysis & Optimization Plan

**Date:** 2026-10-22
**Baseline:** Round 7 measurements (git `5ae943a56`, `benchmark_results.md`)

| Metric | Value |
|---|---|
| Lua router (150 fibers) | 36,517 ops/s |
| C++ proxy process (150 fibers) | 43,314 ops/s |
| C++ in-process library (150 fibers) | 168,505 ops/s |
| **Target** | **1,000,000 ops/s** |

The gap is **6x** even for the in-process library and **23x** for the proxy
process. This document analyzes where the time goes and proposes a phased plan
to reduce per-request overhead by a multiple, plus the stand-level changes
required to make 1M ops/s reachable at all.

---

## PART 1: Why 1M Was Not Reached — Stand-Level Ceiling

Before any code-level optimization, three properties of the benchmark stand
cap throughput regardless of router efficiency.

### 1.1 All traffic hits a single storage instance

`benchmark_results.md` (Setup): RS1 (`cbf06940`, :3301/:3302) has **0 active
buckets**; all 1500 active buckets live on RS2 (:3303). Tarantool is
single-threaded per instance — one tx core executes every
`box.space.customer:replace` plus WAL work for 100% of the load.

The benchmark cluster is the stock `vshard/example` setup
(`/home/tsafin/src/vshard/example/localcfg.lua`): `vshard.storage.cfg` passes
the config through **unmodified**, so no `box.cfg` tuning exists at all —
`wal_mode` defaults to `'write'`, `memtx_memory` to 256 MB, and the installed
Tarantool is **2.6.0**, which predates the storage-side parallelism features
(`iproto_threads`, `wal_queue_max_size`, parallel appliers — all 2.10+;
verified: `box.cfg{iproto_threads=2}` fails with `ER_UNKNOWN_CONFIG_OPTION`
on this build). Note that "parallel WAL" as such does not exist in Tarantool —
an xlog is sequential by design; what exists is one WAL writer thread with
automatic batching of queued commits.

### 1.1.1 Measured storage ceilings (2026-10-22, WSL2, RS2 master :3303)

Isolated per-path measurements (4 parallel loadgen processes so the client is
not the limiter; 8 processes make it worse):

| Path into one storage instance | ops/s |
|---|---:|
| Native IPROTO `replace`, `wal_mode='write'` (cluster default) | ~94k |
| Native IPROTO `replace`, `wal_mode='none'` | ~122k |
| Lua `CALL` (`eval`) doing the same replace | ~61k |
| `vshard.storage.call` (Lua wrapper + async replication to :3304) | ~52k |

Decomposition of the ~94k → ~52k drop on the real bench path:

- **WAL ('write' → 'none'): +23%** on the raw path (94k → 122k).
- **Lua CALL path vs native IPROTO replace: −35%** (94k → 61k) — the server
  pays for CALL decode + Lua function dispatch + `vshard.storage.call`
  wrapper (bucket checks) on the same tx thread.
- **Async replication to the local replica: −15%** (61k → 52k).

These ceilings bracket the end-to-end numbers: Round 7 shows C++ proxy at
43.3k and Lua router at 36.5k ops/s — both **below the 52k storage ceiling**.
Direct-to-storage measurement (single loadgen, 100 fibers): 39.4k ops/s,
i.e. the storage node — not the router — is already the primary bottleneck
for the proxy numbers at high concurrency.

Consequence: single-instance ceiling is ~50–95k depending on path, so **1M
ops/s requires 8–12 storage masters** regardless of tuning; tuning raises
the ceiling per node but does not remove the single-tx-thread limit.

### 1.1.2 Tarantool 2.6 → 2.11.10 upgrade (done 2026-10-22)

The official APT repository no longer carries 2.11 packages (the
`download.tarantool.org/release/2.11/ubuntu` path is empty), so 2.11.10 was
built from source (`git clone --recurse-submodules --branch 2.11.10` →
`cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5` for cmake-4 compatibility →
installed to `/usr/local/bin`, which shadows the apt 2.6 binary via PATH;
`tarantoolctl` uses `#!/usr/bin/env tarantool` and follows it). Sources kept
at `~/src/tarantool-src`.

Measured `vshard.storage.call` ceiling on RS2 master (4 loadgen processes):

| Storage config | ops/s | Δ vs 2.6 default |
|---|---:|---:|
| 2.6.0, no tuning (baseline) | 52.4k | — |
| 2.11.10, no tuning | 52.9–55.6k | ~0% |
| 2.11.10, `iproto_threads=4`, 50 fibers/client | 70.3k | **+34%** |
| 2.11.10, `iproto_threads=4`, 100 fibers/client | **73.4k** | **+40%** |

Takeaways:

- The upgrade alone changes nothing — `iproto_threads` defaults to 1, and the
  tx thread was never the *only* bottleneck: IPROTO decode/encode shared it.
- `iproto_threads=4` offloads framing work and needs a deeper request
  pipeline to pay off (50+ in-flight per client; the proxy's pipelined
  connections provide exactly that).
- `vshard/example/localcfg.lua` now carries `iproto_threads = 4` and
  `wal_queue_max_size = 64MB` (options silently ignored by 2.6 if ever run
  there again... they are *not* — 2.6 rejects them; only run the example
  cluster with 2.11+).
- The remaining gap 73k (vshard Lua path) vs ~94k+ (native IPROTO path on
  2.6, higher on 2.11) is the Lua CALL wrapper — unchanged by the upgrade.

### 1.2 The load generator is a single Lua process

`bench/bench_vshard.lua` runs in **one** Tarantool process: 150 fibers, all
net.box encode/decode on a single core (`bench_vshard.lua:20-42`). A single
net.box client saturates at ~40–50k synchronous calls/s. This matches the
observed plateau of both routers at 36–43k — the comparison
"Lua router vs C++ proxy" at ≥100 fibers is **client-limited**. The in-process
benchmark (C++ loadgen, `bench/bench_vshard.cpp`) reaches 4x more on the same
cluster, proving the point.

### 1.3 WSL2 + netfilter on loopback

The flamegraph (`bench/flamegraph.svg`) contains `nf_hook_slow` frames —
netfilter/iptables policy is evaluated for **every loopback packet**, twice
per proxy request. WSL2 adds a virtualized kernel and scheduler jitter on top.

### 1.4 Consequences

1M ops/s requires, independently of this plan's code phases:

- Buckets rebalanced across RS1 + more replicasets (6–8 storage masters).
- A C++ or multi-process load generator.
- Bare-metal Linux (or at least netfilter disabled on `lo`).

The code phases below reduce **router-side** cost per request so that, once
the stand is fixed, the router is not the next bottleneck.

---

## PART 2: Hot-Path Anatomy (C++ Proxy Process)

### 2.1 Request lifecycle

```mermaid
sequenceDiagram
    participant C as Client (net.box fiber)
    participant S as iproto_server<br/>HandleConnection
    participant P as VshardProxy<br/>DoCallRawBytes
    participant Pool as Pool<br/>ExecuteDirect
    participant Conn as Connection<br/>(per storage master)
    participant F as FlushLoop task
    participant R as ReaderLoop task
    participant ST as Storage (Tarantool)

    C->>S: IPROTO CALL frame
    Note over S: RecvExact(1 byte!) for length prefix,<br/>then RecvExact(body) — 2-3 recv syscalls
    S->>S: ParseIprotoRequest + ParseVshardRouterArgs
    S->>P: CallRawBytes(bucket, mode, func, raw_args)
    P->>P: BuildStorageCallQueryRawWithModeString<br/>(copy #1 of args into Query buf)
    P->>Pool: ExecuteDirect(deadline, query)
    Pool->>Pool: make_unique&lt;ConnectionPtr&gt; + Acquire()<br/>(semaphore + queue + stats)
    Pool->>Conn: SyncExecute(query)
    Note over Conn: BuildQueryBody — copy #2 of args into body vector<br/>make_shared&lt;PendingEntry&gt; + register in pending_slots_ (mutex)
    Conn->>Conn: staging_buf_.insert — copy #3 of args<br/>(staging_mutex_)
    Conn->>F: flush_event_.Send() (ev_async → futex wakeup)
    F->>ST: SendAll(batch) — coalesced send ✓
    ST-->>R: response frame
    Note over R: rbuf → reader_body_buf_ (copy #1 of response)<br/>ParseIprotoResponse → data_buf.assign (copy #2)
    R->>Conn: pending_mutex_ lookup → ready.Send() (futex wakeup)
    Note over Pool: CollectSyncEntry returns ExecutionResult
    P->>P: DecodeEnvelopeRaw → return_values_bytes (copy #3)
    P->>S: result_bytes
    S->>S: BuildResultFrameRaw (copy #4) + SendAll per response<br/>— no write batching ✗
    S-->>C: IPROTO OK frame
```

Key files:

| Stage | Code |
|---|---|
| Server accept/read loop | `vshard/impl/iproto_server.cpp:906-1342` (`HandleConnection`) |
| Length-prefix byte-at-a-time reads | `vshard/impl/iproto_server.cpp:928-962` (`RecvExact`) |
| Per-response unbatched send | `vshard/impl/iproto_server.cpp:1340-1342` |
| Router retry loop (raw bytes) | `vshard/impl/vshard_proxy.cpp:692-783` (`DoCallRawBytes`) |
| Storage-call query build (args copy #1) | `vshard/impl/vshard_proxy.cpp:456-477` |
| Pool acquire per request | `src/storages/tarantool/impl/pool.cpp:30-54` (`ExecuteDirect`) |
| Body build (args copy #2) | `src/storages/tarantool/impl/connection.cpp:686-762` (`BuildQueryBody`) |
| Pending entry allocation + staging (copy #3) | `src/storages/tarantool/impl/connection.cpp:836-878` (`StageSyncRequest`) |
| Flush coalescing (already good) | `src/storages/tarantool/impl/connection.cpp:449-498` (`FlushLoop`) |
| Reader loop (response copies #1-#2) | `src/storages/tarantool/impl/connection.cpp:305-419` (`ReaderLoop`) |
| Sync wait | `src/storages/tarantool/impl/connection.cpp:994-1007` (`CollectSyncEntry`) |
| Single-copy forward path (exists, unused for router.call) | `src/storages/tarantool/impl/connection.cpp:886-897` (`SyncForwardStorageCall`) |

### 2.2 Per-request overhead inventory (proxy process)

Measured/derived from code walk and the flamegraph:

| # | Overhead | Where | Count/request |
|---|---|---|---|
| 1 | `recv` syscall for **1-byte** length prefix | `iproto_server.cpp:928` | 1 |
| 2 | `recv` syscall for body (no read buffering) | `iproto_server.cpp:962` | 1 |
| 3 | `send` syscall per response (no coalescing) | `iproto_server.cpp:1342` | 1 |
| 4 | Pool `Acquire`/`Release`: semaphore futex + queue mutex + stats | `pool.cpp:39-41` | 1 |
| 5 | `make_shared<PendingEntry>` + variant + `SingleConsumerEvent` | `connection.cpp:845-849` | 1 alloc |
| 6 | `engine::Mutex` (futex-based) locks: `pending_mutex_` x2, `staging_mutex_` x1 | `connection.cpp:854, 862, 390, 466` | 3 |
| 7 | Wakeups: `flush_event_.Send()` (ev_async→futex), `ready.Send()` (task schedule), Yield in FlushLoop | `connection.cpp:554, 404, 459` | ~3 |
| 8 | Heap vectors: Query arg buf, body, `data_buf`, `return_values_bytes`, result frame | see 2.1 | ~5 allocs |
| 9 | memcpy of request args (3x) | see 2.1 | 3 |
| 10 | memcpy of response (4x) | see 2.1 | 4 |
| 11 | TCP loopback stack + netfilter per packet (2 hops) | kernel | per packet |

Flamegraph confirmation (`bench/flamegraph.svg`): the widest frames are
`x64_sys_call` (self-samples up to ~4.9% per stack), `futex_wake`,
`sem_post`, `try_to_wake_up`, `TaskContext::Sleep/Schedule`,
`TaskQueue::Push`, `skb_copy_datagram_iter`, `tcp_data_ready`,
`loopback_xmit`, `nf_hook_slow`. The profile is dominated by syscall and
wakeup traffic, not by msgpack processing — the earlier phases
(Value-tree elimination, Future/Promise removal, `SyncPendingEntry`) already
did their job.

At 1M ops/s the current design implies ~6M syscalls/s, ~5M mallocs/s,
~3M futex wakeups/s — infeasible even on bare metal. The plan below attacks
each multiplier.

### 2.3 What is already optimal

- **Send-side batching to storage** exists: `FlushLoop` coalesces all frames
  staged in the current scheduler slice into one `SendAll`
  (`connection.cpp:449-498`) — the net.box model.
- **Zero-copy parse**: `ParseIprotoResponse` / `DecodeEnvelopeRaw` scan raw
  msgpack without building `Value` trees on the hot path.
- **Routing** is a lock-free `rcu::ReadablePtr` read + array index.
- **Slot array** (`pending_slots_`, 256 entries) replaced the hash map.

---

## PART 3: Optimization Plan

Phases are ordered by (expected gain) / (risk). Each phase is benchmarked with
`bench/run_benchmarks.sh` and recorded as a new Round in
`benchmark_results.md`.

### Phase 0 — Fix the stand (prerequisite for the 1M goal)

1. Rebalance buckets so RS1 masters hold active buckets; grow to 8–14
   replicasets. Measured per-master ceiling is ~52–73k ops/s through the
   vshard path (see 1.1.1/1.1.2) ⇒ ~14 masters at the tuned vshard ceiling.
2. Tune the storage nodes — DONE 2026-10-22:
   - Tarantool upgraded 2.6.0 → **2.11.10** (built from source, installed to
     `/usr/local/bin`, sources at `~/src/tarantool-src`); measured effect
     with `iproto_threads=4`: **+40%** per master (52.4k → 73.4k, see 1.1.2).
   - `vshard/example/localcfg.lua` now sets `iproto_threads = 4` and
     `wal_queue_max_size = 64MB`. Requires Tarantool 2.10+.
   - `wal_mode = 'none'` for router benchmarks remains an option (+23% raw);
     not enabled by default to keep durability semantics.
   - For the benchmark workload, calling the space operation natively
     (IPROTO replace) instead of through a Lua CALL would recover the −35%
     Lua-path cost — not possible via vshard (which mandates the
     `vshard.storage.call` wrapper), but worth keeping in mind when
     interpreting numbers.
3. Replace the Lua loadgen with a C++ multi-process generator (or run several
   `bench_vshard.lua` processes in parallel and sum) — measured client
   ceiling of one Lua process is ~40–50k ops/s.
4. Run on bare-metal Linux with netfilter disabled on `lo`
   (`iptables -I INPUT -i lo -j ACCEPT` equivalents).

No connector code changes. Expected: removes the 168k in-process ceiling and
the ~43k client ceiling.

### Phase 1 — Server-side I/O: buffered reads + write coalescing

**Goal:** cut client-facing syscalls from ~3-4 to ~1 per request.
**Scope:** `vshard/impl/iproto_server.cpp` only — no connector changes.

#### 1a. Buffered request reading

Replace the byte-at-a-time `RecvExact` prefix reads with the same ring-buffer
pattern already used by `Connection::ReaderLoop` (`connection.cpp:305-345`):

```
algorithm BufferedRequestLoop(connection):
    rbuf: tnt::Buffer<16384>          # linked 16 KiB blocks
    rpos := rbuf.begin()

    function fill():
        n := socket.RecvSome(tmp[64 KiB])   # ONE syscall, gets whatever is there
        if n == 0: throw PeerClosed
        rbuf.write(tmp[0..n])

    function ensure(n):
        while not rbuf.has(rpos, n): fill()

    loop:
        ensure(1)
        lead := peek(rpos, 1)
        prefix_len := 1|2|3|5 depending on lead byte (0x7f/0xcc/0xcd/0xce)
        ensure(prefix_len)
        body_len := decode_prefix(rpos)
        ensure(prefix_len + body_len)
        req := parse_frame(rpos[0 .. prefix_len + body_len])   # zero-copy view into rbuf
        advance(rpos, prefix_len + body_len)
        rbuf.dropFront(consumed)
        handle(req)            # may loop back immediately if more bytes buffered
```

Multiple pipelined frames arriving in one TCP segment are parsed and handled
without further syscalls.

#### 1b. Response write coalescing

Mirror the client-side `FlushLoop` per connection:

```
algorithm ServerWriteCoalescing(connection):
    out_buf: vector<uint8_t>          # per-connection staging
    out_event: SingleConsumerEvent
    flush_task := spawn:
        while out_event.WaitForEvent():
            Yield()                   # let same-slice handlers finish staging
            loop:
                batch := take(out_buf) under spinlock
                if batch.empty(): break
                socket.SendAll(batch) # ONE syscall for N responses
```

Every reply path (`BuildResultFrameRaw` etc.) appends the frame to `out_buf`
and signals `out_event` instead of calling `SendAll` directly. Because
`HandleConnection` waits for the storage response before reading the next
client frame, responses of concurrent in-flight requests on the same
connection naturally accumulate while a reply is awaited.

**Expected:** 1.5–2x on the proxy-process path; the largest single win.

> **Status: implemented 2026-10-22 (uncommitted, on top of `0b6cedcd0`), incl.
> Phase 1c: concurrent request handling.** `ServerConn` in `iproto_server.cpp`:
> tntcxx `Buffer` ring + `RecvSome` request loop; thread-safe staged output +
> per-connection flush task (1b); vshard CALL requests execute in detached
> handler tasks bounded by a 512-per-connection `engine::Semaphore` (1c).
> Pitfall found the hard way: the `engine::Task` destructor cancels+joins —
> a temporary `TaskWithResult` from `AsyncNoSpan` kills the task before its
> first step; use `engine::DetachUnscopedUnsafe`.
>
> **Measured (new pipelined C++ loadgen, 16 conns × depth 100):**
> proxy **88.1k ops/s** vs Lua router 69.5k (+27%) vs direct storage ~103k;
> 4 conns × depth 100: 70.9k vs 4.6k on HEAD (serial). The synchronous Lua
> loadgen showed none of this — it kept 1 request in flight per connection
> and hid both the serialization bug and the batching wins (measured proxy
> RTT +2.8 ms was dominated by per-connection serialization, not scheduler
> hops as first assumed).

### Phase 2 — Single-copy request forwarding

**Goal:** eliminate 2 of 3 args copies and 2 heap vectors per request.

The single-copy primitive already exists — `Connection::SyncForwardStorageCall`
(`connection.cpp:886-897`) builds the storage-call body as a fixed 23-byte
prefix + the raw client TUPLE (one memcpy, no `Query`, no `BuildQueryBody`).
The vshard.router.call path does not use it: `CallRawBytes` goes through
`BuildStorageCallQueryRawWithModeString` (copy into Query buf,
`vshard_proxy.cpp:456-477`) → `BuildQueryBody` (copy into body) →
`StageSyncRequest` (copy into staging).

Change: add a raw-bytes variant of the forwarding path and dispatch
vshard.router.call* through it.

```
algorithm ForwardRouterCallRaw(bucket, mode, func_tuple_view):
    # func_tuple_view points into the server-side receive buffer (Phase 1a)
    rs := ResolveReplicaset(bucket)              # rcu read, no alloc
    conn_ptr := rs.AcquireMaster(deadline)       # see Phase 5 for bypass
    entry := conn.SyncForwardStorageCall(         # existing method!
        CallRouteInfo{tuple_begin, tuple_end, ...}, deadline)
    conn_ptr.release()
    return Connection.CollectSyncEntry(entry, deadline)
```

Required work:
- Bytes-out variant of `ForwardCall` (current one returns a `Value`,
  `vshard_proxy.cpp:815`) reusing `ParseCallForRoute` + the retry loop of
  `DoCallRawBytes`.
- `CallRouteInfo` must carry the mode string (for generic
  vshard.router.call parity) — extend `kStorageCallBodyPrefix` construction
  accordingly (prefix is built per mode string length; keep a small cache of
  prebuilt prefixes for "read"/"write").

**Expected:** +10–20%; removes ~2 mallocs and 2 memcpys per request.

### Phase 3 — Allocation-free pending entries, cheap locks, zero-copy result

#### 3a. Inline `SyncPendingEntry` in the slot array

`pending_slots_` (`connection.hpp:205-214`) already reserves a slot per
in-flight sync id, yet the entry itself is a separate `make_shared`
allocation (`connection.cpp:845-849`). Store it inline:

```cpp
struct PendingSlot {
    std::uint64_t sync_id{0};                    // 0 = free
    alignas(SyncPendingEntry) std::byte storage[sizeof(SyncPendingEntry)];
};
// register:    new (&slot.storage) SyncPendingEntry{};
// dispatch:    reinterpret_cast<SyncPendingEntry*>(&slot.storage)
// completion:  entry->~SyncPendingEntry(); slot.sync_id = 0;
```

The slot array is read/written under `pending_mutex_` (→ spinlock, 3b), so
lifetime is well-defined; late responses after timeout mark `abandoned`
exactly as today. `kPendingSlotCount` should become configurable
(default 4096) — it is the real in-flight limit.

#### 3b. Spinlocks instead of `engine::Mutex`

Critical sections on the hot path are ~50–100 ns (array store, vector
append). `engine::Mutex` parks on a futex under contention — a syscall per
lock. Replace `staging_mutex_` and `pending_mutex_` with
`userver::utils::cpu::Spinlock` (or a single `std::mutex`-free
seqlock design if profiles show contention).

#### 3c. Zero-copy response delivery

Today the response body is copied 4x (see 2.1). Target: 1 copy
(single `recv` into the ring buffer).

```cpp
struct ExecutionResult {
    // ... existing fields ...
    std::shared_ptr<const std::vector<uint8_t>> frame_owner;  // aliases reader buf
    std::span<const uint8_t> data;                            // view into it
};
```

- `ReaderLoop` keeps `reader_body_buf_` alive via `shared_ptr` and hands the
  waiting coroutine a span (frame boundary must not be overwritten until
  collected — double-buffer the ring, or copy only when the caller outlives
  the buffer, which `CollectSyncEntry` bounds).
- `DecodeEnvelopeRaw` already returns byte ranges; `DoCallRawBytes` returns
  the span instead of copying into `return_values_bytes`.
- `BuildResultFrameRaw` writes header + the span into the **server output
  staging buffer** (Phase 1b) — the only remaining copy is the one into the
  socket-bound batch, which `SendAll` needs anyway.

**Expected:** removes ~4 mallocs + 3 memcpys + 2–3 futex ops per request;
+15–30% at high concurrency.

### Phase 4 — Wakeup elision

Each request today performs ~3 wakeups: `flush_event_.Send()`,
`ready.Send()`, plus the `Yield()` hop in `FlushLoop`.

```
algorithm FastPathSend(frame_bytes):
    if staging was empty AND flush task is idle (atomic flag):
        # no one else staged; write directly, skip the wakeup entirely
        socket.SendAll(frame_bytes)          # 1 syscall, 0 wakeups
        return
    else:
        append to staging_buf_; if flush task idle: flush_event_.Send()
```

- `Send()` only when the flush task is actually parked (track with an atomic
  `flush_sleeping` flag toggled around `WaitForEvent()`).
- The single-sender fast path avoids the event entirely; multi-sender bursts
  still coalesce as today.

**Expected:** +5–15% (2 fewer futex syscalls per request at low batching).

### Phase 5 — Pool bypass for the hot path

`Pool::ExecuteDirect` acquires/releases a pool slot per request
(`pool.cpp:39-41`): semaphore futex + queue mutex + stats + 
`make_unique<ConnectionPtr>` — with 150 contenders on ≤16 connections this
is measurable churn (flamegraph: `sem_post`, `futex_wake`).

Since `Connection` is fully pipelined and internally synchronized
(`pending_mutex_`/`staging_mutex_` per connection), the acquire semaphore adds
no safety on the forwarding path — the natural in-flight bound is
`kPendingSlotCount`.

```
algorithm HotConnectionSet(replicaset):
    # dedicated master connection, never acquired/released
    # in-flight bounded by pending slot capacity (Phase 3a: 4096)
    hot: Connection                     # created lazily, health-checked by topology refresh
    cold_pool: Pool                     # unchanged, for ResolveSpaceId, ping, admin ops
```

Keep the pool for everything except `Forward*`/`SyncForward*`; route the hot
path through the dedicated connection. Alternatively (less invasive): raise
`initial/max_pool_size` to match fiber count and keep the API — measure both.

**Expected:** +10% at ≥50 fibers; also removes per-request stats atomics.

### Phase 6 — Unix sockets

The flamegraph shows full TCP loopback processing per packet:
`skb_copy_datagram_iter`, `tcp_data_ready`, `loopover_xmit`,
`nf_hook_slow`. Unix-domain sockets skip the TCP stack, netfilter, and
checksums entirely.

- Add `socket: unix:/path/to/...` endpoint support to `EndpointSettings`
  (client side) and the sample proxy listener (server side).
- Storage side: Tarantool 2.11+ supports `box.cfg.listen = "unix/:/path"`;
  point the C++ connector at it for proxy↔storage hops.

**Expected:** 1.3–2x on WSL2 (netfilter per packet); still significant on
bare metal. Cheap to implement, orthogonal to Phases 1–5.

### Phase 7 (research) — io_uring

Multishot `recv`/`send` + registered buffers would collapse
epoll_wait+recv into one submission and enable zero-copy socket writes.
userver currently has io_uring only for filesystem I/O; socket support is a
framework-level effort. Evaluate only after Phases 1–5 — if the remaining
syscall count per request is already ~1, the marginal gain may not justify
the framework work.

---

## PART 4: Expected Outcome Summary

| Phase | Removes per request | Est. effect (proxy path) | Risk |
|---|---|---|---|
| 0 (stand) | single-storage ceiling (~52k measured), loadgen ceiling | unblocks 1M path | none |
| 1a+1b | ~3 syscalls (server side) | 1.5–2x | low, local to iproto_server.cpp |
| 2 | 2 memcpys + 2 allocs | +10–20% | low |
| 3 | ~4 allocs + 3 memcpys + 2-3 futexes | +15–30% | medium (lifetime in 3c) |
| 4 | ~2 wakeups | +5–15% | medium (fast-path correctness) |
| 5 | pool acquire/release churn | +10% | low |
| 6 | TCP loopback stack | 1.3–2x | low |
| 7 | epoll+recv collapse | TBD | high (framework) |

Multiplicative estimate for the proxy process: **3–5x** cheaper than today —
i.e. the proxy path approaches the in-process library cost
(2.2–2.9x gap today collapses toward ~1.2–1.5x).

**End-to-end 1M projection:** 6–8 storage masters x ~150k each ≈ 0.9–1.2M
aggregate; router budget per request after Phases 1–6 ≈ 1–2 µs CPU, which at
8 load-generation cores sustains >1M ops/s. The stand (Phase 0), not the
router, remains the dominant risk.

## PART 5: Measurement Protocol

**Primary metric (since Round 20261022b): proxy-added RTT on the
multi-process stand.** The single-process sweep is client-limited (~40–45k
ceiling) and latency-bound — it cannot see router-side savings.

Multi-process spot check (primary):

```bash
# start proxy on :13306 (config template in run_benchmarks.sh)
for i in 1 2 3 4; do
    tarantool tarantool/vshard/bench/bench_vshard.lua 127.0.0.1:13306 100 50000 &
done; wait          # sum ops/sec; RTT ~= 400 / aggregate
```

Reference paths for the same invocation: direct `vshard.storage.call` to
:3303 (~103k agg, RTT ~3.9 ms) and Lua router :3305 (~49.5k agg, RTT ~8.1 ms).
Baseline proxy HEAD `0b6cedcd0`: ~61k agg, RTT ~6.5 ms. A phase is successful
when proxy RTT moves toward the direct path, not when the single-process
sweep moves.

Single-process sweep (comparability with historical rounds — only on an idle
box; verify `uptime` load < 1 first, sweeps showed 2x swings under noise):

```bash
bash tarantool/vshard/bench/run_benchmarks.sh \
    --proxy-binary build_release/userver/tarantool/vshard/userver-tarantool-vshard-sample \
    --fiber-counts 10,20,50,100,150 \
    --ops 100000
```

- Record a new Round in `benchmark_results.md` with git hash.
- Re-capture the flamegraph (`run_perf_benchmark.sh`) and verify the targeted
  frames shrank: Phase 1 → `x64_sys_call` count/request; Phase 3 → `futex_wake`
  + jemalloc frames; Phase 6 → `nf_hook_slow`/`tcp_*` disappearance.
- A phase is considered successful if the proxy-vs-in-process overhead ratio
  drops by the expected amount; if not, profile before proceeding.
