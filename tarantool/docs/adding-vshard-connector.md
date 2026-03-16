# userver vshard Proxy — Design & Implementation Guide

This document designs a **C++ reimplementation of the vshard router** as a userver service/module. The goal is to eliminate the Lua-based vshard router process entirely: the userver application itself acts as the router, talking directly to vshard storage replicasets over IPROTO. Protocol encoding/decoding reuses the userver Tarantool connector (Rec1–Rec6, `tarantool/`); no separate codec layer is needed.

> **Scope clarification**: this is not a "vshard connector" (which would call an existing Lua router). This is a **vshard proxy reimplementation** — all vshard router logic (bucket mapping, MOVED handling, topology discovery, scatter queries) is implemented in C++ inside the userver application.

---

## Table of Contents

1. [vshard Architecture Recap](#1-vshard-architecture-recap)
2. [IPROTO + MsgPack Protocol Deep Dive](#2-iproto--msgpack-protocol-deep-dive)
3. [Implementation Strategies — Evaluation](#3-implementation-strategies--evaluation)
4. [Recommended Approach](#4-recommended-approach)
5. [Repository Structure](#5-repository-structure)
6. [Routing Table Design](#6-routing-table-design)
7. [Bucket-ID Computation](#7-bucket-id-computation)
8. [Connection Topology Model](#8-connection-topology-model)
9. [Request Lifecycle and Error Handling](#9-request-lifecycle-and-error-handling)
10. [Read/Write Mode Semantics](#10-readwrite-mode-semantics)
11. [Scatter / Map-Reduce Queries](#11-scatter--map-reduce-queries)
12. [Topology Discovery & Refresh](#12-topology-discovery--refresh)
13. [Implementation Plan](#13-implementation-plan)
14. [Interfaces and Key Types](#14-interfaces-and-key-types)
15. [Zero-Copy Router Architecture](#15-zero-copy-router-architecture)
16. [Wire-Level Worked Examples](#16-wire-level-worked-examples)
17. [Testing Strategy](#17-testing-strategy)
18. [Checklist](#18-checklist)
19. [Non-Backward-Compatible Protocol Improvements](#19-non-backward-compatible-protocol-improvements)
20. [Open Questions](#20-open-questions)

---

## 1. vshard Architecture Recap

### Traditional Lua router topology (what we replace)

```
Application
  │
  ▼
┌──────────────────────────────────┐
│  vshard Router (Lua process)     │   knows bucket→replicaset mapping
│  vshard.router.callrw(bid, ...)  │
└──────────────┬───────────────────┘
               │  IPROTO CALL to storage
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Replicaset 1           Replicaset 2            Replicaset N        │
│  master + replicas      master + replicas        master + replicas  │
└─────────────────────────────────────────────────────────────────────┘
```

### What this module provides (C++ proxy, no Lua router)

```
┌────────────────────────────────────────────┐
│  userver Application                       │
│  ┌─────────────────────────────────────┐   │
│  │  VshardProxy (this module)          │   │
│  │  · CRC32 bucket computation         │   │
│  │  · routing_table (RCU)              │   │   ← replaces the Lua router process
│  │  · MOVED/TRANSFER retry             │   │
│  │  · topology refresh (PeriodicTask)  │   │
│  └──────────────┬──────────────────────┘   │
└─────────────────┼──────────────────────────┘
                  │  IPROTO direct to storage (tntcxx codec)
                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Replicaset 1           Replicaset 2            Replicaset N        │
│  master + replicas      master + replicas        master + replicas  │
│  buckets [1..10922]     buckets [10923..21844]   ...                │
└─────────────────────────────────────────────────────────────────────┘
```

**Core concepts:**

| Concept | Default | Notes |
|---|---|---|
| `BUCKET_COUNT` | 32 768 | Configured once, never changes. All clients must agree. |
| `bucket_id` | `uint32` in `[1, BUCKET_COUNT]` | Computed from the sharding key by the client. |
| Replicaset | 1 master + N replicas | One pool of connections in the client. |
| Router | Optional Tarantool node | Runs `vshard.router.*` Lua; can be bypassed. |
| MOVED | Error code | Returned when a bucket has migrated to a different replicaset. Client must refresh topology and retry. |
| TRANSFER | Error code | Bucket is mid-migration; client may retry with a delay. |

### 1.3 Router Data Structures (`router/init.lua`)

| Field | Type | Description |
|---|---|---|
| `route_map` | Lua table (hash) | `bucket_id → replicaset object`, lazily populated |
| `replicasets` | Lua table | `rs_id → replicaset object` |
| `total_bucket_count` | number | N (fixed at bootstrap) |
| `known_bucket_count` | number | How many buckets are cached locally |
| `discovery_fiber` | fiber | Background discovery loop |

### 1.4 Single-Bucket Call Path (`router_call_impl`)

```
client calls: vshard.router.call(bucket_id, 'write', 'my.func', args)

1. bucket_id range check  [O(1)]
2. route_map[bucket_id]   [Lua hash lookup]
   - HIT  → goto step 3
   - MISS → synchronous scan of all replicasets via bucket_stat RPC
3. net.box.call(master, 'vshard.storage.call',
                {bucket_id, mode, 'my.func', args})   [network]
4. Check response:
   - OK              → return result to caller
   - WRONG_BUCKET    → invalidate route_map[bucket_id], retry (+ follow
                        err.destination hint if present)
   - NON_MASTER      → update master in replicaset, retry
   - other error     → return error
5. Timeout loop (deadline = now + opts.timeout)
```

The storage entry point (`storage/init.lua: storage_call`):

```
vshard.storage.call(bucket_id, mode, func_name, args):
  1. bucket_ref(bucket_id, mode)   -- pin bucket, reject if SENDING/SENT
  2. local_call(func_name, args)   -- execute user function
  3. bucket_unref(bucket_id, mode)
  4. return ok, result...
```

### 1.5 Discovery (`discovery_service_f` / `discovery_f`)

A background fiber polls all replicasets via `vshard.storage.bucket_discovery`
at intervals (`DISCOVERY_WORK_INTERVAL = 1s` when incomplete,
`DISCOVERY_IDLE_INTERVAL = 10s` when fully mapped).  Results are merged into
`route_map` via `discovery_handle_buckets`.

On cache miss during a live call, a **synchronous** scan is done over all
replicasets (calling `bucket_stat(bucket_id)` on each) — expensive but rare
after warmup.

### 1.6 Consistent Map-Reduce (`router_map_callrw`)

Three-phase Ref-Map-Reduce:

```
1. Ref:  send vshard.storage.ref(rid, timeout) to all target replicasets
         in parallel (is_async=true).  Ref pins all buckets on that RS.
2. Map:  send func call to each replicaset in parallel.
3. Reduce: collect results into {rs_id → result} map.
Refs auto-expire on timeout (quota controlled by SCHED_REF_QUOTA).
```

### 1.7 Failover and Replica Selection

Each replicaset has a master + weighted replica list.  A background
`master_search_fiber` probes replicas to detect master changes.  On
`NON_MASTER` error the local master pointer is updated immediately.

Read calls support four modes: `callro` (master), `callbro` (balanced
replicas), `callre` (prefer replica), `callbre` (balanced prefer replica).
Weight-based round-robin is implemented in `replicaset_template_multicallro`.

### 1.8 Rebalancing (Storage-side Only)

The rebalancer runs on one elected storage node.  It:

1. Downloads bucket counts from all replicasets.
2. Computes ideal distribution using `rebalancer_build_routes`.
3. Initiates `bucket_send/bucket_recv` migrations (chunk-by-chunk, with
   `BUCKET_CHUNK_SIZE = 1000` tuples per step).

Buckets transition through: `ACTIVE → SENDING → SENT → GARBAGE` on the
source, and `RECEIVING → ACTIVE` on the destination.

---

## 2. IPROTO + MsgPack Protocol — userver Connector

All communication with vshard storages uses the Tarantool binary protocol (IPROTO), framed as 5-byte length-prefixed MsgPack packets. The base userver Tarantool connector (`tarantool/`) provides a complete, production-ready implementation of this layer (Rec1–Rec6); the vshard proxy reuses it directly.

### What the userver connector provides (✅ implemented, Rec1–Rec6)

| File | What it gives the vshard proxy |
|---|---|
| `tarantool/src/storages/tarantool/impl/iproto_frames.hpp` | `BuildPingFrame()` (Rec5), `ParseIprotoResponse()` zero-alloc scanner (Rec6), `msgpack_scan::ReadUint` + `SkipValue` helpers — **foundation for `ParseCallForRoute`** |
| `tarantool/src/storages/tarantool/impl/connection.hpp/cpp` | `Connection` + `ReaderLoop`: reads IPROTO frames into `tnt::Buffer` (Rec1), dispatches via `ParseIprotoResponse` (Rec6) |
| tntcxx `src/mpp/` (`Enc.hpp`, `Dec.hpp`, `Spec.hpp`) | Typed mpp encode/decode for `Insert`/`Replace`/`Select`/`Call` (Rec3) |
| tntcxx `src/Client/IprotoConstants.hpp` | All `Iproto::Key`, `Iproto::Type`, `Iproto::Error` constants |
| tntcxx `src/Client/Scramble.hpp` | CHAP-SHA1 auth |

The tntcxx I/O layer (`Connection.hpp`, `EpollNetProvider.hpp`) is **not used** — replaced by userver's `engine::io::Socket` + coroutine-based `ReaderLoop` / `WriterLoop`.

### vshard-specific IPROTO calls

For direct-to-storage calls (the vshard proxy routes to storages, not to a Lua router):

```cpp
// Encode: call application function directly on the storage replicaset
// bucket_id is used only for routing — it is NOT passed to the storage function
encoder_.encodeCall("myapp.get_user",
    std::make_tuple(user_id));   // application args only
```

For topology discovery (calling vshard Lua on a storage to learn bucket ownership):
```cpp
encoder_.encodeCall("vshard.storage.bucket_stat",
    std::make_tuple(bucket_id));
// or for bulk topology:
encoder_.encodeCall("vshard.router.info", std::make_tuple());
```

### Packet structure (for reference)

```
┌──────────────────────────────────────────────────────┐
│  5 bytes: MP_UINT32 = body_length (handled by        │
│           RequestEncoder::PREHEADER_SIZE)             │
├──────────────────────────────────────────────────────┤
│  Header  (MsgPack map — mpp::as_map)                 │
│    Iproto::REQUEST_TYPE = Iproto::CALL               │
│    Iproto::SYNC         = ++sync_id                  │
├──────────────────────────────────────────────────────┤
│  Body (MsgPack map)                                  │
│    Iproto::FUNCTION_NAME = "app.function"            │
│    Iproto::TUPLE         = [arg1, arg2, ...]         │
└──────────────────────────────────────────────────────┘
```

### vshard response envelope

vshard storage functions return results in a multi-return convention. The IPROTO `IPROTO_DATA` array wraps the application result in an additional layer:

```
On success:  IPROTO_DATA = [[ app_result, null ]]
On error:    IPROTO_DATA = [[ null, {code, type, message, destination} ]]
```

MOVED/TRANSFER errors are **in-band** (inside a successful `IPROTO_OK` response), not IPROTO-level errors. The proxy must decode the second element to detect them.

### 2.4 Mapping vshard Lua Concepts to C++ Primitives

| vshard Lua | C++ userver equivalent | Notes |
|---|---|---|
| `route_map[bucket_id]` | `std::array<uint16_t, N>` | 6 KB for N=3000, L1-resident |
| `replicasets[rs_id]` | `std::vector<ReplicasetInfo>` | indexed by uint16_t |
| `ReplicaInfo` (net.box conn) | `storages::tarantool::Client` | one Client per replicaset |
| `discovery_fiber` | `engine::PeriodicTask` | userver async periodic task |
| `master_search_fiber` | `engine::PeriodicTask` | same |
| Fiber yields / `lfiber.yield()` | `engine::current_task::Yield()` | userver coroutine yield |
| `net.box.call(…, is_async=true)` | `engine::AsyncNoSpan(…)` | launch task, collect futures |
| `bucket_id = crc32(msgpack(key)) % N + 1` | `BucketCalculator::Calculate` | see §7 |

#### API Compatibility Reference

| Lua API | C++ equivalent |
|---|---|
| `vshard.router.call(bucket_id, mode, func, args, opts)` | `VshardProxy::Call(bucket_id, mode, func, args, deadline)` |
| `vshard.router.map_callrw(func, args, opts)` | `VshardProxy::MapCallRW(func, args, deadline)` |
| `vshard.router.bucket_id_mpcrc32(key)` | `BucketCalculator::Calculate(key)` (mpcrc32 variant) |
| `vshard.router.bucket_id_strcrc32(key)` | `BucketCalculator::CalculateStrcrc32(key)` |
| `vshard.router.route(bucket_id)` | `VshardProxy::RouteInfo(bucket_id)` |

The C++ proxy calls the same **storage-side** RPCs as the Lua router (no storage changes needed):

```
net.box.call('vshard.storage.call', {bucket_id, mode, func_name, args})
net.box.call('vshard.storage.ref',  {rid, timeout})
net.box.call('vshard.storage.map_call', {func_name, args, opts})
net.box.call('vshard.storage.bucket_stat', {bucket_id})
net.box.call('vshard.storage.bucket_discovery', {from, to})
```

#### Error Handling Compatibility

vshard uses structured `vshard.error` objects on the wire (msgpack map with
`type`, `code`, `message`, `destination` fields).  The C++ proxy decodes
these and implements the same retry logic:

| vshard error code | C++ action |
|---|---|
| `WRONG_BUCKET` | Invalidate `route_map[bucket_id]`, retry (follow `destination`) |
| `BUCKET_IS_LOCKED` | Retry after yield |
| `TRANSFER_IS_IN_PROGRESS` | Retry after yield |
| `NON_MASTER` | Update master pointer, retry on new master |
| timeout | Return `DeadlineExceeded` |

---

## 3. Implementation Strategies — Evaluation

Since the goal is a **vshard proxy reimplementation** (no Lua router), strategies A and B (which rely on an existing Lua router) are either eliminated or demoted to optional compatibility mode.

### Strategy A — Via vshard Router (Proxy mode) — ❌ Not the goal

```
userver ──IPROTO──► vshard-router (Lua) ──IPROTO──► storage replicaset
```

Trivial to implement (just `encodeCall("vshard.router.callrw", {bucket_id, func, args})`), but defeats the purpose: still requires a Lua router process. **Keep as an optional `routing_mode: router_proxy`** for teams migrating from Lua routers.

---

### Strategy B — Smart Client with Topology Cache — ⚠️ Partial

```
userver ──discovery──► vshard-router (Lua, once at startup)
userver ──IPROTO──────► storage replicasets (direct hot path)
```

Eliminates the hot-path hop but still depends on a Lua router for bootstrap and MOVED refresh. Viable transitional strategy.

---

### Strategy C — Full Embedded Routing (Routerless) — ✅ Primary target

```
userver ──static/dynamic config──► replicaset topology
userver ──IPROTO──────────────────► storage replicasets (direct)
         (no Lua router at all)
```

The userver module **is** the vshard router. It:
1. Computes `bucket_id = CRC32(sharding_key) % BUCKET_COUNT + 1` locally.
2. Maintains a `routing_table: bucket_id → ReplicasetPool` via `rcu::Variable<RoutingTable>`.
3. On MOVED: queries `vshard.storage.bucket_stat(bid)` or a config source to refresh topology; retries once.
4. Periodically refreshes topology via `utils::PeriodicTask`.
5. Handles scatter queries (`MapCallRW`) via `utils::Async` fan-out to all replicasets.

| Pro | Con |
|---|---|
| No Lua router process at all | Must implement all vshard router logic in C++ |
| Maximum performance (direct storage) | MOVED storm risk during rebalancing |
| Full deadline/tracing/stats integration | Initial topology source required (config or storages) |
| No single-threaded Lua bottleneck | Must stay in sync with vshard bucket semantics |

---

### Strategy D — Static Topology — ✅ Good starting point

Strategy C with fixed topology: replicaset→bucket ranges in static config, no MOVED handling. Trivial to implement; promotes to C by adding `PeriodicTask`-based refresh.

---

### Comparison Matrix

| | A (Lua proxy) | B (Hybrid) | C (Embedded) ✅ | D (Static) ✅ |
|---|:---:|:---:|:---:|:---:|
| Lua router required | always | at startup | **never** | **never** |
| Extra network hop | ✗ | ✓ | ✓ | ✓ |
| Handles rebalancing | ✓ | ✓ | ✓ | ✗ |
| Implementation complexity | low | medium | **high** | **low** |
| vshard proxy reimplementation | ✗ | partial | **yes** | **yes (limited)** |
| Recommended phase | migration | transitional | **production** | **MVP** |

---

## 4. Recommended Approach

**Implement Strategy D first** (static topology) as the MVP, then **extend to Strategy C** (full embedded routing with MOVED + refresh). Strategy A is kept as `routing_mode: router_proxy` for migration.

```yaml
tarantool-vshard:
    routing_mode: embedded       # embedded | router_proxy
    bucket_count: 32768
    replicasets:
      - uuid: "7f8ef4b4-..."
        masters: ["tt-storage-1:3301"]
        replicas: ["tt-storage-1-replica:3301"]
        buckets: [1, 10922]      # static range (Strategy D)
    # Strategy C extensions:
    topology_refresh_interval: 60s
    moved_refresh_min_interval: 1s
    max_moved_retries: 1
```

### 4.1 Performance and Scalability: Why C++ Router is Faster

#### Routing hot path — per-call overhead

| Component | Lua vshard router | C++ userver router |
|---|---|---|
| Bucket ID computation | CRC32 via Lua FFI call to C | Direct C++ inline CRC32 |
| Route map lookup | Lua hash table: ~200 ns + GC pressure | `std::array` index: ~5 ns, L1 hit |
| Routing decision | Lua interpreter bytecode | Native machine code |
| Connection dispatch | `net.box` Lua layer + fiber context | Direct IPROTO write via userver |
| Per-request Lua GC pauses | Yes (periodic stop-the-world) | None |
| Context-switch cost | Tarantool fiber (~1-2 µs) | userver coroutine (~0.3 µs) |

#### Why the C++ router is faster

1. **L1-resident route table**: `std::array<uint16_t, 3000>` = 6 KB.  Fits in
   L1 cache; a single array access replaces a Lua hash table lookup with
   pointer-chasing.

2. **No Lua GC**: Lua uses a tri-color mark-and-sweep GC.  Under high RPS the
   GC runs frequently and causes latency spikes.  C++ has no equivalent cost
   for routing-path allocations (the route table is pre-allocated).

3. **Direct IPROTO**: `net.box` in Lua has a Lua-level callback chain for
   encoding/decoding.  The userver connector writes IPROTO directly from C++,
   with zero-allocation framing after Rec1–Rec6 optimizations.

4. **Parallelism**: `engine::AsyncNoSpan` spawns true asynchronous tasks, all
   sharing the same OS thread pool.  Lua fibers are cooperative and run on a
   single event-loop thread per Tarantool instance.

5. **Binary protocol path**: After warmup the entire routing path
   `BucketId → route_map[id] → client.Call(...)` involves zero heap
   allocations on the router side.

#### Scalability comparison

| Dimension | Lua vshard | C++ userver |
|---|---|---|
| CPU cores used for routing | 1 (Tarantool single-threaded) | N (userver multi-threaded) |
| Concurrent requests | Cooperative: only when yielding | True parallelism via coroutines |
| Connection pool | per net.box instance, ~1 conn/RS | configurable pool per RS |
| Max throughput (single node) | ~200K req/s (Tarantool benchmark) | >1M req/s (userver benchmark) |

---

## 5. Repository Structure

The vshard proxy lives as **`tarantool/vshard/`** — a standalone executable example inside the `tarantool/` top-level directory. It depends on `userver-tarantool` (the base connector) for `Connection`, `Pool`, `TntBuffer`, and tntcxx headers. There is no separate reusable library: all routing logic is compiled directly into the service executable, so consumers use it as a reference implementation or starting point.

```
userver/
└── tarantool/                         ← top-level (like postgresql/, redis/, clickhouse/)
    ├── CMakeLists.txt                 ← project(userver-tarantool); adds vshard via add_subdirectory(vshard)
    ├── include/ src/ functional_tests/ ...   ← base connector
    └── vshard/                        ← executable example, analogous to samples/clickhouse_service/
        ├── CMakeLists.txt             ← project(userver-tarantool-vshard-sample CXX)
        ├── vshard_proxy.cpp           ← service entry point: component registration, main()
        ├── impl/                      ← routing logic (internal, not exported as a library)
        │   ├── routing_table.hpp/cpp  ← bucket_id → ReplicasetPool* (rcu::Variable<RoutingTable>)
        │   ├── replicaset_pool.hpp/cpp ← one master Pool + N replica Pools from userver-tarantool
        │   ├── bucket_calculator.hpp/cpp ← CRC32(key) % BUCKET_COUNT + 1
        │   ├── topology_fetcher.hpp/cpp  ← builds RoutingTable from config / storage queries
        │   ├── vshard_envelope.hpp/cpp   ← decode [[app_result, vshard_error]] response envelope
        │   ├── vshard_error.hpp/cpp      ← parse vshard MOVED/TRANSFER error objects
        │   └── iproto_ext.hpp            ← WATCH/UNWATCH/EVENT opcodes missing from tntcxx
        ├── static_config.yaml
        ├── dynamic_config_fallback.json
        └── tests/
            ├── conftest.py
            └── test_vshard_basic.py
```

### `tarantool/vshard/CMakeLists.txt`

```cmake
project(userver-tarantool-vshard-sample CXX)

file(GLOB_RECURSE SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/impl/*.hpp)

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    userver-tarantool    # base connector: Connection, Pool, TntBuffer, tntcxx headers
    userver-core
)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

userver_sample_testsuite_add(
    TESTS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
if (TEST testsuite-${PROJECT_NAME})
    set_tests_properties(testsuite-${PROJECT_NAME} PROPERTIES ENVIRONMENT
        "TESTSUITE_TARANTOOL_SERVER_START_TIMEOUT=30.0")
endif()
```

> **Why not a library?** The vshard routing logic is tightly coupled to the application's deployment topology and config schema. Distributing it as a reusable library would require stable public headers, versioning, and a much larger API surface. As an executable example it is easy to fork, adapt, and extend — the intended usage pattern.

---

## 6. Routing Table Design

The routing table maps a `bucket_id` to the `ReplicasetPool` that owns it. The table is:
- **Read-path lock-free**: accessed via `rcu::Variable<RoutingTable>` — every request takes a hazard-pointer read snapshot.
- **Write-path serialised**: a single `utils::PeriodicTask` (plus on-MOVED refresh) holds the write lock.

```cpp
// impl/routing_table.hpp

struct ReplicasetInfo {
    std::string uuid;
    impl::ReplicasetPool pool;   // connections to master + replicas
};

struct RoutingTable {
    // bucket_id → index into replicasets
    // Stored as a flat array because bucket_id is uint32 in [1, BUCKET_COUNT]
    // and BUCKET_COUNT (32768) × 2 bytes = 64 KiB — fits in L2 cache.
    std::uint32_t bucket_count;
    std::vector<std::uint16_t> bucket_to_rs;    // indexed by bucket_id-1
    std::vector<ReplicasetInfo> replicasets;    // indexed by bucket_to_rs value
};
```

Access pattern on the hot path:
```cpp
auto snapshot = routing_table_.Read();   // O(1), no lock
auto rs_idx   = snapshot->bucket_to_rs[bucket_id - 1];
auto& pool    = snapshot->replicasets[rs_idx].pool;
// ... call pool.Execute(...)
```

Update pattern (on MOVED or periodic refresh):
```cpp
auto new_table = FetchTopology(routers_);   // calls vshard.router.info()
routing_table_.Assign(std::move(new_table));
```

`rcu::Variable<T>` from `<userver/rcu/rcu.hpp>` provides exactly these semantics.

---

## 7. Bucket-ID Computation

vshard uses CRC32 to map an application-level sharding key to a bucket:

```
bucket_id = crc32(key_bytes) % BUCKET_COUNT + 1
```

**Note**: the `crc32` here is the standard CRC-32 (IEEE 802.3 polynomial, same as `zlib::crc32`).  vshard uses Lua's `digest.crc32()`.

```cpp
// impl/bucket_calculator.hpp
#pragma once
#include <cstdint>
#include <string_view>

USERVER_NAMESPACE_BEGIN
namespace storages::tarantool::vshard {

class BucketCalculator {
 public:
    explicit BucketCalculator(std::uint32_t bucket_count)
        : bucket_count_{bucket_count} {}

    // Compute bucket_id for an arbitrary sharding key
    std::uint32_t Calculate(std::string_view key) const;

    // Convenience overloads for common key types
    std::uint32_t Calculate(std::uint64_t key) const;
    std::uint32_t Calculate(std::int64_t  key) const;

 private:
    std::uint32_t bucket_count_;
};

}  // namespace storages::tarantool::vshard
USERVER_NAMESPACE_END
```

```cpp
// impl/bucket_calculator.cpp
#include <boost/crc.hpp>       // or zlib, or a hand-rolled table

std::uint32_t BucketCalculator::Calculate(std::string_view key) const {
    boost::crc_32_type crc;
    crc.process_bytes(key.data(), key.size());
    return crc.checksum() % bucket_count_ + 1;
}

std::uint32_t BucketCalculator::Calculate(std::uint64_t key) const {
    // vshard big-endian encodes integer keys before CRC
    std::array<std::uint8_t, 8> buf;
    for (int i = 7; i >= 0; --i) { buf[i] = key & 0xFF; key >>= 8; }
    return Calculate({reinterpret_cast<const char*>(buf.data()), buf.size()});
}
```

**Custom sharding functions**: vshard supports pluggable bucket-id functions at the Lua level. For the C++ client the sharding function can be provided as a `std::function<std::uint32_t(std::string_view)>` injected into `BucketCalculator`, or by calling `vshard.router.bucket_id_strcrc32(key)` via the router once (at creation of the entity, not on every lookup).

### 7.1 mpcrc32 vs strcrc32

vshard ships two bucket-ID functions with subtly different semantics:

| Function | Key encoding before CRC32 | Use |
|---|---|---|
| `bucket_id_mpcrc32(key)` | Integer: CRC32 of **msgpack-encoded** bytes; String: CRC32 of raw bytes | **Default, recommended** |
| `bucket_id_strcrc32(key)` | `CRC32(tostring(key))` — integers stringified first | Legacy, deprecated |

For the C++ implementation, `mpcrc32` integer keys must be **msgpack-encoded before hashing**:

```cpp
// mpcrc32-compatible: matches vshard.router.bucket_id_mpcrc32()
// For integer keys: CRC32 of msgpack-encoded integer
// For string keys:  CRC32 of raw string bytes (no msgpack header added)
uint32_t BucketIdMpcrc32(std::string_view key, uint32_t bucket_count) {
    return crc32(key.data(), key.size()) % bucket_count + 1;
}
uint32_t BucketIdMpcrc32(int64_t key, uint32_t bucket_count) {
    // msgpack-encode key into small buffer, crc32 it
    uint8_t buf[9];
    size_t len = MsgpackEncodeInt(buf, key);
    return crc32(buf, len) % bucket_count + 1;
}
```

The `BucketCalculator` class implements both variants; `mpcrc32` is the default.

---

## 8. Connection Topology Model

```
VshardProxy
    │
    ├── RouterPool  (1+ connections to vshard router nodes)
    │   Used only for topology discovery / bootstrap
    │
    └── RoutingTable  (rcu::Variable<RoutingTable>)
            │
            ├── ReplicasetPool[0]  (uuid=A)
            │       ├── MasterPool  (min/max connections to master)
            │       └── ReplicaPool (min/max connections per replica)
            │
            ├── ReplicasetPool[1]  (uuid=B)
            │       ├── MasterPool
            │       └── ReplicaPool
            └── ...
```

`ReplicasetPool` extends the base connector's `impl::PoolImpl` (or wraps one master pool + N replica pools):

```cpp
class ReplicasetPool final {
 public:
    ExecutionResult Execute(CallMode, engine::Deadline, const Query&);

    bool IsAvailable() const;
    void WriteStatistics(utils::statistics::Writer&) const;

 private:
    std::shared_ptr<impl::Pool> master_;
    std::vector<std::shared_ptr<impl::Pool>> replicas_;
    mutable std::atomic<std::size_t> replica_idx_{0};  // round-robin
};
```

`CallMode::kReadWrite` → always `master_`.
`CallMode::kReadOnly`  → round-robin `replicas_` with fallback to `master_` if all replicas are unavailable.
`CallMode::kBestReadOnly` (BRO) → prefer nearest replica, tolerate failure silently.

---

## 9. Request Lifecycle and Error Handling

```
VshardProxy::CallRW(sharding_key, func, args)
  │
  ├─ 1. bucket_id = calculator_.Calculate(sharding_key)
  │
  ├─ 2. snapshot  = routing_table_.Read()   // lock-free RCU read
  │
  ├─ 3. pool      = snapshot->GetPool(bucket_id)
  │
  ├─ 4. span      = tracing::Span{"tarantool_vshard_call"}
  │     span.AddTag("vshard.bucket_id", bucket_id)
  │     span.AddTag("vshard.mode", "rw")
  │
  ├─ 5. raw_result = pool.Execute(CallMode::kReadWrite, deadline, query)
  │
  ├─ 6. Decode vshard envelope:
  │     if (vshard_error.code == MOVED):
  │       RefreshRoutingTable()          // triggers topology fetch
  │       retry once from step 2
  │     if (vshard_error.code == TRANSFER):
  │       engine::SleepFor(transfer_retry_delay_)
  │       retry once from step 2
  │     if (vshard_error.code != 0):
  │       throw VshardException{vshard_error}
  │
  └─ 7. return VshardResult{raw_result[0]}
```

Key invariants:
- **At most one retry per MOVED/TRANSFER** per request. If the second attempt also returns MOVED, propagate as an exception — topology is severely inconsistent.
- **Deadline is respected throughout**: `engine::Deadline` is threaded through `pool.Execute` and any `SleepFor`.
- **No retry on non-vshard errors** (IPROTO errors, network failures) — these propagate immediately.

---

## 10. Read/Write Mode Semantics

vshard defines four read modes (mirrors the router API):

| `CallMode` | Enum value | Semantics |
|---|---|---|
| `kReadWrite` | `rw` | Route to master of the bucket's replicaset |
| `kReadOnly` | `ro` | Route to any replica; prefer replica over master |
| `kBestReadOnly` | `bro` | Like `ro` but tolerate stale data (no strong consistency) |
| `kBestReadOnlyError` | `bre` | Like `bro` but raise an error if no replica available |

In **Router proxy mode** (Strategy A), these are passed as the second argument to `vshard.router.call`:
```
vshard.router.call(bucket_id, "write"/"read"/"prefer_replica"/"nearest_preferred", func, args)
```

In **Smart client mode** (Strategy B), they control which pool inside `ReplicasetPool` is used:
- `rw` / `bre` → `master_pool`
- `ro` / `bro` → round-robin `replica_pools`, fallback to master

---

## 11. Scatter / Map-Reduce Queries

vshard provides `vshard.router.map_callrw(func, args, opts)` to call a function on **all** replicasets and collect results. In embedded (proxy) mode this is implemented explicitly:

```cpp
// VshardProxy::MapCallRW
std::vector<VshardResult> VshardProxy::MapCallRW(
    std::string_view func, formats::json::Value args, CommandControl cc) {

    auto snapshot = routing_table_.Read();
    std::vector<engine::TaskWithResult<VshardResult>> tasks;
    tasks.reserve(snapshot->replicasets.size());

    for (const auto& rs : snapshot->replicasets) {
        tasks.emplace_back(utils::Async(
            "vshard_map_call",
            [&rs, &func, &args, &cc]() {
                return rs.pool.Execute(CallMode::kReadWrite,
                                       engine::Deadline::FromDuration(cc.timeout),
                                       Query::Call(func, args));
            }));
    }

    std::vector<VshardResult> results;
    for (auto& task : tasks) {
        results.push_back(task.Get());  // raises on any individual failure
    }
    return results;
}
```

For partial-failure semantics (`utils::GetAll` variant), use `engine::WaitAllChecked` or collect results with explicit error handling.

---

## 12. Topology Discovery & Refresh

Since the C++ proxy **is** the router (no Lua router process), topology must come from sources that exist independently of a Lua router. The right mechanism depends heavily on the Tarantool version in use.

### 12.0 Tarantool version split: 2.x vs 3.x

This is the most important architectural decision for the `TopologyFetcher`.

| Feature | Tarantool 2.x | Tarantool 3.x |
|---|---|---|
| Topology in static config | External (`cfg` file, manually managed) | **First-class YAML config** with `groups`/`replicasets`/`instances` hierarchy |
| Master/replica role detection | `box.info().ro` via IPROTO_CALL | **`IPROTO_WATCH "box.status"`** — push notification, no polling |
| Instance/replicaset identity | `box.info().replicaset.uuid` via call | **`IPROTO_WATCH "box.id"`** — push, includes `instance_name`, `replicaset_name` |
| Leader election | Manual or Raft | Built-in Raft; **`IPROTO_WATCH "box.election"`** broadcasts term + leader |
| vshard topology | `vshard.router.info()` / `vshard.storage.bucket_stat()` | Same vshard API, but also: **`sharding.roles` in config** declares storage/router membership |
| IPROTO_WATCH support | Not available (2.10+ only) | **Core feature** (IPROTO feature bit `WATCHERS = 3`) |
| Capability negotiation | None | **`IPROTO_ID` exchange** at connect — declares `VERSION` + `FEATURES` bitmap |

> **IPROTO_WATCH** (opcodes 74/75/76, added in Tarantool 2.10, core in 3.x):
> client sends `WATCH {EVENT_KEY: "box.status"}` once; server sends `EVENT {EVENT_KEY: "box.status", EVENT_DATA: {...}}` immediately and on every change. Client re-sends `WATCH` to acknowledge. `UNWATCH` to stop.

### 12.1 `IPROTO_WATCH` — what Tarantool 3 broadcasts natively

All four keys below are broadcast from `box.cc` via `box_broadcast()` on every state change and once at connection time (so a new client gets the current state immediately):

#### `"box.status"` — master/replica role
```msgpack
{
  "is_ro":     false,      ← true = replica, false = master (the key for routing!)
  "is_ro_cfg": false,      ← read_only from box.cfg
  "status":    "running",  ← "loading" | "running" | "orphan" | "hot_standby"
  "dd_version": "3.1.0"
}
```

#### `"box.id"` — instance + replicaset identity
```msgpack
{
  "id":               1,
  "instance_uuid":    "xxxxxxxx-...",
  "instance_name":    "storage-001",      ← Tarantool 3 named instances
  "replicaset_uuid":  "yyyyyyyy-...",
  "replicaset_name":  "storages-001",     ← Tarantool 3 named replicasets
  "cluster_name":     "my-cluster"
}
```

#### `"box.election"` — Raft leader tracking
```msgpack
{
  "term":   42,
  "role":   "leader",    ← "leader" | "follower" | "candidate"
  "is_ro":  false,
  "leader": 1            ← replica_id of current leader
}
```

#### `"box.schema"` — schema version changes
```msgpack
{ "version": 123 }
```

### 12.2 Topology sources — by Tarantool version

| Source | Tarantool 2.x | Tarantool 3.x | Notes |
|---|---|---|---|
| **Static config** | ✅ Both | ✅ Both | Replicaset addresses, initial bucket ranges |
| **`IPROTO_WATCH "box.status"`** | ⚠️ 2.10+ only | ✅ Core | `is_ro` field → master vs replica, **replaces polling** |
| **`IPROTO_WATCH "box.id"`** | ⚠️ 2.10+ only | ✅ Core | `replicaset_uuid` / `replicaset_name` → identity without Lua call |
| **`IPROTO_WATCH "box.election"`** | ⚠️ 2.10+ only | ✅ Core | Leader changes in Raft replicasets |
| **`box.info()`** via IPROTO_CALL | ✅ Both | ✅ Both | Fallback when WATCH not available |
| **`vshard.storage.bucket_stat(bid)`** | ✅ Both | ✅ Both | On MOVED — find new bucket owner |
| **`vshard.router.info()`** | ✅ Both | ✅ Both | Optional bootstrap if Lua router exists |

**Recommendation**: detect support via `IPROTO_ID` capability negotiation at connect time, use watchers when available, fall back to periodic `box.info()` polling on 2.x.

### 12.3 `IPROTO_ID` capability negotiation (Tarantool 3)

Before any call, send `IPROTO_ID` to discover what the server supports:

```cpp
// Encode IPROTO_ID request (type = 73 = 0x49)
// Body: { VERSION: client_version, FEATURES: [0, 1, 3] }
//   Feature 0 = STREAMS, 1 = TRANSACTIONS, 3 = WATCHERS

// Server responds with its own VERSION + FEATURES bitmap.
// If FEATURE_WATCHERS (bit 3) is set → use IPROTO_WATCH.
// Otherwise → fall back to PeriodicTask + box.info() calls.
```

`IprotoConstants.hpp` in the current tntcxx does **not** yet have `WATCH`/`UNWATCH`/`EVENT`/`ID` constants (they were added to tarantool at opcodes 74–77). These need to be added to the vshard proxy's own `impl/iproto_ext.hpp`.

### 12.4 Watch-based master tracking (Tarantool 3 path)

When `FEATURE_WATCHERS` is available, the `ReplicasetPool` subscribes to `box.status` on each connection:

```
ConnectionEstablished(host:port)
  │
  ├─ Send IPROTO_ID → check FEATURE_WATCHERS
  │
  ├─ [if WATCHERS] Send WATCH {EVENT_KEY: "box.status"}
  │   └─ On EVENT received: update is_master_ from event.data["is_ro"]
  │
  └─ [if no WATCHERS] Start PeriodicTask → call box.info() every 10s

ReplicasetPool::SelectMaster()
  └─ return pool where is_master_ == true  ← O(1), no network call
```

This eliminates **all periodic polling for master/replica role detection**. The `PeriodicTask` topology refresh interval can be extended from 60s to much longer (e.g. 10 minutes) or made purely MOVED-triggered.

### 12.5 Bootstrap (startup)

```
Tarantool 2.x (or no-watcher fallback):
  TopologyFetcher::FetchFromStorages()
    ├─ Build RoutingTable from static config
    ├─ Connect to each known host
    ├─ Call box.info() → is_ro, replicaset_uuid, instance_uuid
    └─ Assign master_ / replicas_[] based on is_ro

Tarantool 3.x (watcher path):
  TopologyFetcher::FetchWithWatchers()
    ├─ Build RoutingTable from static config (addresses from named instances)
    ├─ Connect to each known host
    ├─ Send IPROTO_ID + WATCH "box.id" + WATCH "box.status"
    ├─ Receive initial EVENT for both keys (server fires immediately)
    │   → box.id gives replicaset_uuid/name for routing table keying
    │   → box.status gives is_ro for master selection
    └─ Keep watch subscriptions alive for push updates
```

### 12.6 MOVED handling (per-request, during rebalancing)

```
Connection returns MOVED error: { bucket_id, destination_uuid }
  │
  ├─ Look up destination_uuid in current routing_table
  │   ├─ Found: update bucket_to_rs[bucket_id] → destination_rs, retry once
  │   └─ Not found: trigger TopologyRefresh (rate-limited), retry once
  │
TopologyRefresh::RefreshBucketOwner(bid)
  │
  └─ encoder_.encodeCall("vshard.storage.bucket_stat", std::make_tuple(bid))
     → { status: "active"|"sending"|"receiving", destination: UUID }
     → Update routing_table_ if destination is a known replicaset
     → If destination is unknown replicaset: need full re-bootstrap
```

### 12.7 Periodic refresh

```cpp
if (settings_.routing_mode == RoutingMode::kEmbedded) {
    if (watchers_supported_) {
        // Master role updates arrive via IPROTO_WATCH "box.status" push.
        // Only need periodic refresh for bucket ownership drift (rebalancing).
        // Can be much longer interval or even purely MOVED-triggered.
        refresh_task_.Start("vshard_topology_refresh",
            utils::PeriodicTask::Settings{topology_refresh_interval_},  // e.g. 10min
            [this] { RefreshBucketOwnership(); });
    } else {
        // Tarantool 2.x: must poll for master/replica changes too.
        refresh_task_.Start("vshard_topology_refresh",
            utils::PeriodicTask::Settings{topology_refresh_interval_},  // e.g. 60s
            [this] { RefreshTopologyFull(); });
    }
}
```

### 12.8 `vshard.storage.bucket_stat()` response structure

```
IPROTO_DATA = [[
  {                           ← app result (first return value)
    "id": 42,                 ← bucket_id
    "status": "active",       ← "active" | "sending" | "receiving" | "garbage"
    "destination": null       ← non-null when "sending": target replicaset UUID
  },
  null                        ← vshard error (null on success)
]]
```

Decoded via `mpp::decode` with the standard `TntBuffer` + `ResponseDecoder<TntBuffer>` — no separate codec.

### 12.9 What needs to be added to `impl/iproto_ext.hpp`

tntcxx's `IprotoConstants.hpp` (as of the current vendored version) is missing the Tarantool 2.10+/3.x additions. The vshard proxy needs its own extension header:

```cpp
// tarantool/vshard/impl/iproto_ext.hpp
#pragma once
#include <Client/IprotoConstants.hpp>

namespace IprotoExt {
    // Tarantool 2.10+ / 3.x additions
    enum Key {
        VERSION   = 0x54,
        FEATURES  = 0x55,
        TIMEOUT   = 0x56,
        EVENT_KEY = 0x57,
        EVENT_DATA= 0x58,
        AUTH_TYPE = 0x5b,
        REPLICASET_NAME = 0x5c,
        INSTANCE_NAME   = 0x5d,
    };

    enum Type {
        ID       = 73,   // IPROTO_ID — capability negotiation
        WATCH    = 74,   // subscribe to a key
        UNWATCH  = 75,   // unsubscribe
        EVENT    = 76,   // server push notification
        WATCH_ONCE = 77, // one-shot watch (no re-subscribe needed)
    };

    enum Feature {
        STREAMS      = 0,
        TRANSACTIONS = 1,
        ERROR_EXTENSION = 2,
        WATCHERS     = 3,
        PAGINATION   = 4,
        SPACE_AND_INDEX_NAMES = 5,
        WATCH_ONCE   = 6,
    };
}
```

---

## 13. Implementation Plan

### Phase 1 — Foundation (✅ complete — base Tarantool connector Rec1–Rec6)

The vshard proxy **does not** need its own MsgPack codec. It reuses the base connector's `tnt::Buffer` + `iproto_frames.hpp` directly. All items below are already implemented and tested (172 unit tests pass).

- [x] **`tnt::Buffer<16384>` recv buffer** (Rec1, `connection.cpp`) — scatter-gather receive, eliminates O(N) memmove
- [x] **`AppendTo` / direct staging** (Rec2) — zero-copy outgoing frame path in `SendAndRegister`
- [x] **Typed mpp API** (Rec3) — `MppEncode`/`MppDecode`/`Insert`/`Replace`/`Select`
- [x] **Typed `VspaceTuple` decode** (Rec4) — `ResolveSpaceId` via typed mpp, no tree-walk
- [x] **Compile-time PING frame** (Rec5, `iproto_frames.hpp`) — `BuildPingFrame(sync)`
- [x] **`ParseIprotoResponse` zero-alloc scanner** (Rec6, `iproto_frames.hpp`) — 47.7M op/s, 6.3× faster than `Value::FromBytes`; provides `msgpack_scan::ReadUint` + `SkipValue` reused by `ParseCallForRoute`
- [x] **`reader_body_buf_` reuse** — per-connection buffer eliminates per-response heap allocation
- [x] **vshard response envelope decoder** (`impl/vshard_envelope.hpp/cpp`)
  - Parse `Response.body.data` as a 2-element array `[app_result, vshard_error]`
  - Decode vshard error object: `code`, `type`, `message`, `destination` UUID
  - Unit tests: MOVED/TRANSFER detection, success unwrapping

- [ ] **Bucket calculator** (`impl/bucket_calculator.hpp/cpp`)
  - `CRC32(key_bytes) % BUCKET_COUNT + 1` — mpcrc32 and strcrc32 variants
  - Overloads for `string_view`, `uint64_t`, `int64_t`
  - Unit tests: validate against known Tarantool `digest.crc32()` outputs

### Phase 2 — Routing Table

- [ ] **RoutingTable data structure** (`impl/routing_table.hpp/cpp`)
  - `std::vector<uint16_t> bucket_to_rs` (flat, cache-friendly)
  - `std::vector<ReplicasetInfo>` with UUID, display name, pool
  - `rcu::Variable<RoutingTable>` as the holder in `VshardProxy`
  - Snapshot `Read()` / `Assign()` pattern
  - Unit tests: routing correctness, MOVED table updates

- [ ] **Replicaset pool** (`impl/replicaset_pool.hpp/cpp`)
  - Wraps one master `Pool` + N replica `Pool`s (from base connector)
  - `Execute(CallMode, deadline, query)` → dispatch to master or replica
  - Replica selection: atomic round-robin with `IsAvailable()` skip
  - `IsAvailable()` = `master_.IsAvailable() || any_replica_available()`
  - Statistics writer (connection counts, latencies per replicaset)

### Phase 3 — Topology Discovery

- [ ] **TopologyFetcher** (`impl/topology_fetcher.hpp/cpp`)
  - For Strategy D: builds `RoutingTable` from static config only (no network calls)
  - For Strategy C: calls `vshard.storage.bucket_stat(bid)` on storages (via base connector `Pool`) to discover bucket ownership; or optionally calls `vshard.router.info()` on a bootstrap router
  - Note: uses `encoder_.encodeCall("vshard.storage.bucket_stat", std::make_tuple(bid))` — no separate codec
  - Returns `RoutingTable`

- [ ] **Periodic refresh** in `VshardProxy`
  - `utils::PeriodicTask refresh_task_`
  - MOVED-triggered refresh with rate limiting (`engine::SingleConsumerEvent` + timestamp)
  - Testsuite: `testpoint::Testpoint("vshard_topology_refresh")` so tests can trigger it on demand

### Phase 3.5 — Tarantool 3 `IPROTO_WATCH` support (optional, version-gated)

This phase is fully optional — the proxy works correctly without it (falls back to polling). It significantly improves latency for master-failover detection when Tarantool 3.x storage nodes are used.

- [ ] **`impl/iproto_ext.hpp`** — WATCH/UNWATCH/EVENT/WATCH_ONCE opcodes + feature enum
  - Opcodes: `ID=73, WATCH=74, UNWATCH=75, EVENT=76, WATCH_ONCE=77`
  - Keys: `EVENT_KEY=0x57, EVENT_DATA=0x58, VERSION=0x54, FEATURES=0x55`
  - Features: `WATCHERS=3` (the feature bit we care about)
  - Not in tntcxx's `IprotoConstants.hpp` — lives only in vshard's impl headers

- [ ] **`IPROTO_ID` exchange in `Connection::Connect()`** (base connector change or vshard override)
  - After IPROTO greeting + IPROTO_AUTH, send `IPROTO_ID {VERSION: [3,0,0], FEATURES: [3]}`
  - Parse server's `VERSION` + `FEATURES` bitmap; store `watchers_supported_` on the connection
  - Fall back gracefully if server doesn't reply or replies with older VERSION

- [ ] **`WatchSubscription`** (`impl/watch_subscription.hpp/cpp`)
  - Manages a single `box.status` / `box.id` subscription on one connection
  - `Subscribe(key)`: sends `WATCH {EVENT_KEY: key}`
  - On receiving `EVENT {EVENT_KEY: key, EVENT_DATA: data}`:  
    — acknowledges by re-sending `WATCH {EVENT_KEY: key}` (required by protocol)  
    — invokes registered callback with `EVENT_DATA`
  - Out-of-band recv: `EVENT` frames arrive asynchronously, not correlated with a request sync_id. The recv loop must dispatch these to registered watchers rather than into a pending-response queue.
  - `Unsubscribe(key)`: sends `UNWATCH {EVENT_KEY: key}`

- [ ] **`box.status` → master/replica detection in `ReplicasetPool`**
  - On connection established + WATCH capable: subscribe to `box.status` on each connection
  - Callback updates atomic `is_master_` from `event_data["is_ro"].GetBool()`:  
    `is_ro == false` → this connection is to the **master**
  - `SelectMaster()` returns the pool whose latest `box.status` had `is_ro = false` — O(1), no RPC

- [ ] **`box.id` → replicaset name/UUID in `TopologyFetcher`**
  - Use `IPROTO_WATCH_ONCE {EVENT_KEY: "box.id"}` at startup (no long-lived sub needed)
  - Extracts `replicaset_uuid`, `replicaset_name`, `instance_name` without calling `box.info()`
  - Allows config to use human-readable Tarantool 3 instance names instead of raw UUIDs

- [ ] **`box.election` → Raft leader tracking (optional)**
  - Subscribe to `box.election` on each master connection
  - `role == "leader"` cross-checks the master selection
  - Useful for detecting split-brain during Raft re-election

### Phase 4 — VshardProxy

- [ ] **VshardProxy** (`impl/vshard_proxy.hpp/cpp` — internal to the executable)
  - `CallRW(sharding_key, func, args, cc)`
  - `CallRO(sharding_key, func, args, cc)`
  - `CallBRO(sharding_key, func, args, cc)`
  - `CallBRE(sharding_key, func, args, cc)`
  - `MapCallRW(func, args, cc)` — scatter to all replicasets
  - Direct `Call(bucket_id, mode, func, args, cc)` — when bucket_id is pre-computed
  - MOVED/TRANSFER retry logic (single retry, then throw)
  - Deadline propagation throughout

- [ ] **VshardResult** (`impl/vshard_result.hpp/cpp`)
  - Unwraps `[app_result, null]` envelope
  - `GetData()` → raw MsgPack buffer or `formats::json::Value`
  - Raises `VshardException` on in-band vshard errors

- [ ] **Exception hierarchy** (`impl/vshard_exceptions.hpp`)
  ```
  VshardException
  ├── MovedError         (bucket moved, includes destination UUID)
  ├── TransferError      (bucket mid-transfer)
  ├── NoAvailableReplicasetError
  └── VshardTimeoutError
  ```

### Phase 5 — Service wiring

- [ ] **`vshard_proxy.cpp`** — service entry point
  - `main()` using `utils::DaemonMain`
  - Register `components::TarantoolComponent` (base connector, N instances — one per replicaset)
  - Register a custom `VshardProxyComponent` (wraps `VshardProxy`, exposes HTTP handler for calls)
  - All routing config read from `static_config.yaml`; credentials from secdist

- [ ] **Static config schema** (`static_config.yaml`):
  ```yaml
  tarantool-vshard:
      routing_mode: smart          # smart | router_proxy | embedded
      bucket_count: 32768
      topology_refresh_interval: 60s
      moved_refresh_min_interval: 1s   # throttle for MOVED-triggered refresh
      max_moved_retries: 1
      routers:
          secdist_alias: my-vshard
      storages:
          initial_pool_size: 2
          max_pool_size: 10
          connect_timeout: 2s
          queue_timeout: 500ms
  ```

### Phase 6 — Testing

- [ ] Unit tests (no server): routing table, bucket calculator, MsgPack codec, vshard error parsing
- [ ] Integration tests (`*_vshardtest.cpp`): real vshard cluster via `testsuite/env --databases=tarantool-vshard`
- [ ] Functional tests (pytest): full service with vshard; tests for MOVED handling, rebalancing, topology refresh
- [ ] Chaos tests: pool exhaustion, network partition between client and storages, TRANSFER storm

---

## 14. Interfaces and Key Types

```cpp
// tarantool/vshard/impl/vshard_proxy.hpp  (internal to the executable)

// VshardProxy: the C++ reimplementation of the vshard router.
// All routing decisions (CRC32, bucket→replicaset lookup, MOVED retry)
// happen inside this class. No Lua router is involved on the hot path.
class VshardProxy final {
 public:
    // Route by sharding key: compute bucket_id = CRC32(key) % bucket_count + 1,
    // look up owning replicaset, call func directly on the storage master (RW)
    // or on a replica (RO/BRO/BRE). Handles MOVED with one automatic retry.
    VshardResult CallRW(std::string_view sharding_key,
                        std::string_view func,
                        formats::json::Value args,
                        OptionalCommandControl = {});

    VshardResult CallRO(std::string_view sharding_key,
                        std::string_view func,
                        formats::json::Value args,
                        OptionalCommandControl = {});

    // Same but with Best-Effort replica selection:
    // BRO: prefer replica, fall back to master silently on error
    // BRE: prefer replica, throw ReplicaUnavailableError on error
    VshardResult CallBRO(std::string_view sharding_key,
                         std::string_view func,
                         formats::json::Value args,
                         OptionalCommandControl = {});

    VshardResult CallBRE(std::string_view sharding_key,
                         std::string_view func,
                         formats::json::Value args,
                         OptionalCommandControl = {});

    // Pre-computed bucket_id variant (when caller already knows the bucket)
    VshardResult Call(BucketId bucket_id,
                      CallMode mode,
                      std::string_view func,
                      formats::json::Value args,
                      OptionalCommandControl = {});

    // Scatter: fan out to all replicasets in parallel, collect results.
    // Uses utils::Async fan-out; respects CommandControl deadline.
    std::vector<VshardResult> MapCallRW(std::string_view func,
                                        formats::json::Value args,
                                        OptionalCommandControl = {});

    BucketId ComputeBucketId(std::string_view sharding_key) const;
    std::uint32_t GetBucketCount() const noexcept;

    // Force an immediate topology refresh (e.g. after deployment).
    // Returns after the refresh completes.
    void RefreshTopology(engine::Deadline);

    void WriteStatistics(utils::statistics::Writer&) const;
};

// Bucket ID strong typedef
using BucketId = utils::StrongTypedef<struct BucketIdTag, std::uint32_t>;

enum class CallMode { kReadWrite, kReadOnly, kBestReadOnly, kBestReadOnlyError };

// Exception hierarchy (thrown by VshardProxy, not by the base connector)
//
// VshardException
// ├── MovedError           bucket migrated; destination UUID available; auto-retried once
// ├── TransferError        bucket mid-migration; auto-retried with backoff
// ├── NoReplicasetError    routing table has no entry for this bucket_id
// ├── ReplicaUnavailableError  all replicas down (BRE mode only)
// └── VshardStorageError   application-level error returned by the storage function
```

### 14.1 Internal Router Class Sketch

The `VshardProxy` delegates routing to an internal `VshardRouter` that owns
the route map and all replicaset connections:

```cpp
class VshardRouter {
public:
    // Compute bucket_id from a sharding key
    uint32_t BucketId(std::string_view key) const;
    uint32_t BucketId(int64_t key) const;

    // Single-bucket call (write or read)
    engine::Task<formats::msgpack::Value>
    Call(uint32_t bucket_id, CallMode mode,
         std::string_view func, formats::msgpack::Value args,
         engine::Deadline deadline);

    // Map-Reduce over all (or selected) buckets
    engine::Task<std::unordered_map<std::string, formats::msgpack::Value>>
    MapCallRw(std::string_view func, formats::msgpack::Value args,
              std::optional<std::vector<uint32_t>> bucket_ids,
              engine::Deadline deadline);

    // Exposed for testing / monitoring
    uint32_t TotalBucketCount() const;
    uint32_t KnownBucketCount() const;

private:
    struct ReplicasetInfo {
        std::string id;          // UUID or name
        std::unique_ptr<storages::tarantool::Client> master;
        std::vector<std::unique_ptr<storages::tarantool::Client>> replicas;
        std::vector<double> replica_weights;
        std::atomic<size_t> round_robin_counter{0};
    };

    // bucket_id → index into replicasets_ (0 = unknown)
    // std::array for N≤65535 (uint16_t), 128 KB max — L2-resident
    std::array<std::atomic<uint16_t>, kMaxBuckets> route_map_{};

    std::vector<ReplicasetInfo> replicasets_;
    std::atomic<uint32_t> known_bucket_count_{0};
    uint32_t total_bucket_count_{3000};

    engine::PeriodicTask discovery_task_;
    engine::PeriodicTask master_search_task_;

    void RunDiscovery();
    void RunMasterSearch();
    ReplicasetInfo* BucketResolve(uint32_t bucket_id,
                                  engine::Deadline deadline);
};
```

---

## 15. Zero-Copy Router Architecture

A vshard proxy sits between the application and storage nodes. For every request it:
1. Receives a full IPROTO packet from the client connection.
2. Decides which storage replicaset to send it to (routing decision).
3. Forwards the packet to the storage, receiving the response.
4. Optionally inspects the response for vshard-level errors (MOVED / TRANSFER).
5. Returns the response to the client.

Steps 1–5 are dominated by the **data payload** — the `IPROTO_TUPLE` (request args) and `IPROTO_DATA` (response data). These can be arbitrarily large and should **never be decoded or copied** on the critical path.

### 15.1 The structural insight: TUPLE bytes are identical

When routing `vshard.router.call` → `vshard.storage.call`, the `IPROTO_TUPLE`
argument array (`[bucket_id, mode, func_name, user_args]`) is **byte-for-byte
identical** in both packets.  Only `IPROTO_FUNCTION_NAME` changes.

```
Incoming (client → router):
  body = { 0x22: "vshard.router.call",   0x21: [bucket_id, mode, func, args] }
                                                 └────────────────────────────┘
Outgoing (router → storage):                           same bytes
  body = { 0x22: "vshard.storage.call",  0x21: [bucket_id, mode, func, args] }
```

The router therefore never needs to decode `mode`, `func`, or `args`.  It only
needs to scan **23 bytes** of the body to reach `bucket_id`, then forward the
raw TUPLE bytes as a scatter-gather reference.

### 15.2 Wire format — byte-level analysis

```
Incoming IPROTO CALL packet (client → router):

Offset  Size  Content
──────  ────  ─────────────────────────────────────────────────────────────────
  0       1   0xce                pre-header uint32 marker
  1       4   <packet_length>
  5       1   0x82                fixmap(2) — IPROTO header map
  6       2   0x00 0x0a           CODE = CALL
  8       1   0x01                key: SYNC
  9       9   0xcf <8 bytes>      uint64 sync_id
 18       1   0x82                fixmap(2) — body map
 19       1   0x22                key: IPROTO_FUNCTION_NAME
 20      19   0xb2 "vshard.router.call"   fixstr(18)
 39       1   0x21                key: IPROTO_TUPLE  ← 21 bytes into body
 40       1   0x94                fixarray(4)
 41+      ?   bucket_id           first element (fixuint ≤127, or uint8/16/32)
 …        ?   mode, func_name, user_args   ← never decoded by router
```

Scanning to reach `bucket_id` costs **23 bytes of body reads**.  With
`msgpack_scan::SkipValue` (already in `iproto_frames.hpp`) the code is trivial.

### 15.3 What must be decoded on the critical path

| Field | Decode? | Why |
|---|---|---|
| Length prefix (5 bytes) | Always | Framing |
| Header: `SYNC`, `REQUEST_TYPE` | Always | Route response; detect CALL vs PING |
| Body: `FUNCTION_NAME` key | Skip (find 0x22 then 0x21) | Only need to locate TUPLE |
| Body: `IPROTO_TUPLE` value | **Never** — pointer + length only | Forwarded verbatim |
| Response: `IPROTO_DATA` | **Never on success** | Forwarded verbatim via pointer |
| Response: vshard envelope 2nd elem | Peek only | 1-byte nil check on success path |
| Response: vshard error struct | Only on MOVED/TRANSFER | Rare path |

### 15.4 Zero-copy forwarding — `ParseCallForRoute`

The `ParseCallForRoute()` function (to be added to `iproto_frames.hpp`) reuses
the existing `msgpack_scan::ReadUint` and `SkipValue` helpers from Rec6:

```cpp
/// Result of minimal IPROTO CALL body scan for zero-copy routing.
struct CallRouteInfo {
    uint32_t      bucket_id;
    const uint8_t* tuple_begin;  ///< start of raw IPROTO_TUPLE msgpack value
    const uint8_t* tuple_end;    ///< == packet_end
};

/// Scan an IPROTO CALL body (starting at the body fixmap byte) to extract
/// bucket_id and locate the TUPLE value for scatter-gather forwarding.
/// Returns nullopt on malformed input.  Cost: ~23 bytes scanned.
std::optional<CallRouteInfo>
ParseCallForRoute(const uint8_t* p, std::size_t len) noexcept {
    using namespace msgpack_scan;
    std::size_t pos = 0;

    // Body fixmap header
    if (pos >= len) return std::nullopt;
    const uint8_t b = p[pos++];
    std::size_t body_n = 0;
    if ((b & 0xf0u) == 0x80u) body_n = b & 0x0fu;     // fixmap
    else if (b == 0xde && pos+2 <= len) { body_n = (std::size_t)p[pos]<<8|p[pos+1]; pos+=2; }
    else if (b == 0xdf && pos+4 <= len) {
        body_n = (std::size_t)p[pos]<<24|(std::size_t)p[pos+1]<<16|
                 (std::size_t)p[pos+2]<<8|p[pos+3]; pos+=4;
    }
    else return std::nullopt;

    const uint8_t* tuple_begin = nullptr;

    for (std::size_t i = 0; i < body_n; ++i) {
        // Read key
        auto [key, kpos] = ReadUint(p, len, pos);
        pos = kpos;

        if (key == 0x21) {   // IPROTO_TUPLE
            tuple_begin = p + pos;
            break;
        }
        // Skip non-TUPLE values
        pos = SkipValue(p, len, pos);
    }
    if (!tuple_begin) return std::nullopt;

    // Read array header to find first element (bucket_id)
    std::size_t apos = tuple_begin - p;
    const uint8_t ah = p[apos++];
    std::size_t arr_n = 0;
    if ((ah & 0xf0u) == 0x90u) arr_n = ah & 0x0fu;    // fixarray
    else if (ah == 0xdc && apos+2 <= len) { arr_n = (std::size_t)p[apos]<<8|p[apos+1]; apos+=2; }
    else if (ah == 0xdd && apos+4 <= len) {
        arr_n = (std::size_t)p[apos]<<24|(std::size_t)p[apos+1]<<16|
                (std::size_t)p[apos+2]<<8|p[apos+3]; apos+=4;
    }
    if (arr_n < 2) return std::nullopt;  // need at least [bucket_id, mode, ...]

    auto [bucket_id, after_bid] = ReadUint(p, len, apos);
    (void)after_bid;
    if (bucket_id == 0 || bucket_id > 65535u) return std::nullopt;

    return CallRouteInfo{
        static_cast<uint32_t>(bucket_id),
        tuple_begin,
        p + len
    };
}
```

### 15.5 Scatter-gather forwarding layout

The outgoing packet is assembled from four non-contiguous pieces using
`tnt::Buffer`'s iovec chain (no intermediate `memcpy`):

```
iov[0]   5 bytes  pre-header: 0xce + uint32(new_length)        ← computed
iov[1]  13 bytes  IPROTO header map: CODE=CALL, SYNC=new_sync  ← template
iov[2]  23 bytes  body prefix: fixmap(2) + "vshard.storage.call" + key 0x21
iov[3]   N bytes  raw TUPLE value bytes from incoming buffer   ← ZERO COPY
```

Total **new** bytes generated per request: **41 bytes**.  Forwarding efficiency:

| User args size | Bytes copied | Zero-copy forwarded |
|---|---|---|
| 16 B   | 42%  |  58% |
| 100 B  | 22%  |  78% |
| 1 KB   |  4%  | **96%** |
| 64 KB  | 0.1% | **99.9%** |

`tnt::Buffer::AppendView(ptr, len)` (to be added) inserts a non-owning scatter
segment into the iovec chain.  The bytes are transmitted via `writev(2)` at the
socket layer, referencing the receive buffer directly until send completes.

Pre-computed body prefix (compile-time constant):

```cpp
static constexpr uint8_t kStorageCallBodyPrefix[] = {
    0x82,                           // fixmap(2)
    0x22,                           // IPROTO_FUNCTION_NAME
    0xb3,                           // fixstr(19)
    'v','s','h','a','r','d','.','s','t','o','r','a','g','e','.','c','a','l','l',
    0x21,                           // IPROTO_TUPLE (value follows verbatim)
};  // 23 bytes total
```

The full scatter-gather write function using `tnt::Buffer`:

```cpp
void ForwardCall(CallRouteInfo& info, uint64_t new_sync,
                 tnt::Buffer& out) {
    const std::size_t tuple_size = info.tuple_end - info.tuple_begin;
    const std::size_t body_size  = sizeof(kStorageCallBodyPrefix) + tuple_size;
    const std::size_t hdr_size   = 13;  // fixmap(2) + CODE + SYNC(uint64)
    const std::size_t total      = hdr_size + body_size;

    // iov[0]: pre-header (5 bytes)
    out.AppendUint32BE(static_cast<uint32_t>(total));  // 0xce already in tnt::Buffer
    // iov[1]: header map with new sync (13 bytes)
    AppendIprotoCallHeader(out, new_sync);
    // iov[2]: body prefix — compile-time constant (23 bytes)
    out.AppendRange(kStorageCallBodyPrefix,
                    kStorageCallBodyPrefix + sizeof(kStorageCallBodyPrefix));
    // iov[3]: raw TUPLE bytes from incoming buffer — reference, no memcpy
    out.AppendView(info.tuple_begin, tuple_size);  // writev scatter segment
}
```

`AppendView` adds a non-owning scatter-gather segment to `tnt::Buffer`'s iovec
chain.  The bytes are transmitted via `writev(2)` without touching the data.

### 15.6 Response forwarding

```cpp
// Storage response arrives into storageBuf_
ParseIprotoResponse(body_ptr, body_len, resp);   // Rec6 scanner, zero-alloc

if (resp.code != 0) {
    // propagate IPROTO-level error to client (decode error stack, re-encode)
    ...
}

// Fast-path: peek vshard envelope without full decode
if (is_vshard_success(resp.data_begin, resp.data_end)) {
    // Forward raw DATA bytes: one writev segment (no memcpy)
    forward_raw_response(client_socket, client_sync,
                         resp.data_begin, resp.data_end);
} else {
    // Slow-path: decode vshard error, handle MOVED/TRANSFER
    handle_vshard_error(resp.data_begin, resp.data_end);
}
```

`ParseIprotoResponse` (already in `iproto_frames.hpp`, Rec6) returns
`data_begin`/`data_end` as raw pointers into the receive buffer — no
heap allocation.

### 15.7 vshard envelope peek — without full decode

The vshard response is `IPROTO_DATA = [[ app_result, vshard_error ]]`.  To
distinguish success from MOVED without decoding `app_result`, peek at the
second element:

```cpp
// is_vshard_success: scan to second element of inner array, check for nil
// Uses SkipValue to hop over app_result in O(1) time
// On success: second element is nil (0xc0) — 1 byte check
// On error:   second element is a map — decode only the error fields
inline bool is_vshard_success(const uint8_t* p, const uint8_t* end) noexcept {
    using namespace msgpack_scan;
    std::size_t len = end - p;
    std::size_t pos = 0;
    // outer array[0]
    if (pos >= len || (p[pos] & 0xf0u) != 0x90u) return true;  // not array → forward
    pos++;
    // inner array header
    if (pos >= len || (p[pos] & 0xf0u) != 0x90u) return true;
    pos++;
    // skip app_result (first element)
    pos = SkipValue(p, len, pos);
    // second element: nil = success
    return pos < len && p[pos] == 0xc0u;  // nil
}
```

### 15.8 IPROTO_VSHARD_CALL extension (non-backward-compatible, future)

For the `IPROTO_VSHARD_CALL` protocol extension (see §19.1 below),
`bucket_id` is placed in the **IPROTO header map** (new key), not in the body:

```
header = { CODE: VSHARD_CALL, SYNC: N, BUCKET_ID: bid, MODE: m }
body   = { FUNCTION_NAME: "my.func", TUPLE: [user_args] }
```

The router reads **only the ~20-byte header**, then forwards the entire body
zero-copy without any scanning.  This is the theoretically optimal form.

### 15.9 Summary of decode work per request

| Operation | Bytes decoded | Bytes skipped (zero-copy) |
|---|---|---|
| IPROTO header (sync, code) | ~20 | 0 |
| Scan body to find 0x21 key | ~23 (scan, no decode) | 0 |
| Extract `bucket_id` | 1–9 | 0 |
| Forward TUPLE value | 0 | entire args blob |
| IPROTO response header | ~20 | 0 |
| Forward `IPROTO_DATA` | 0 | entire data blob |
| Peek vshard envelope | 1 (nil check) | entire app_result blob |

On the critical success path: **only IPROTO headers, 23 scan bytes, and one nil check** touch CPU. Application args and data flow as raw memory references.

### 15.10 Impact on Rec6 Work

The `ParseIprotoResponse` scanner from Rec6 and the `msgpack_scan` helpers
(`ReadUint`, `SkipValue`) in `iproto_frames.hpp` are directly reused for
`ParseCallForRoute`.  The infrastructure is already in place; the zero-copy
router forwarding path is an extension, not a replacement.

---

## 16. Wire-Level Worked Examples

### Example 1 — CallRW in embedded (proxy) mode

Goal: call `user.get` on the replicaset owning bucket 42.

**Step 1: Encode IPROTO_CALL to storage**
```
Length prefix: 4 bytes (body size)

Header (MsgPack map, 3 keys):
  {0x00: 0x0a,     // IPROTO_CALL
   0x01: <sync>,   // IPROTO_SYNC = uint64
   0x05: <schema>} // IPROTO_SCHEMA_VERSION (optional)

Body (MsgPack map, 2 keys):
  {0x22: "user.get",        // IPROTO_FUNCTION_NAME
   0x21: [42, {arg: "x"}]}  // IPROTO_TUPLE = [bucket_id_hint?, args...]
```

Note: when calling application functions directly on storage (not via vshard Lua), `bucket_id` is not passed as an argument — the app function does not receive it. The routing was done by the **client** selecting the correct pool.  Only when calling `vshard.call` on a router is `bucket_id` an explicit argument.

**Step 2: Successful response**
```
Header: {0x00: 0x00, 0x01: <sync>, 0x05: <schema>}  // IPROTO_OK
Body:   {0x30: [["result_value"]]}                    // IPROTO_DATA
```
`VshardResult::GetData()` returns `"result_value"` (the first element of `IPROTO_DATA[0]`).

**Step 3: MOVED response** (bucket 42 has migrated to replicaset UUID-B)

When calling via `vshard.router.call` on router:
```
Header: {0x00: 0x00, ...}   // IPROTO_OK (vshard wraps errors in-band!)
Body:   {0x30: [[null, {
    "code": 1,
    "type": "ShardingError",
    "message": "Bucket 42 was moved to replica set 7f8ef4b4...",
    "destination": "7f8ef4b4-4934-11ed-bc22-0242ac130002"
}]]}
```
Client: extracts `destination` UUID, looks up `ReplicasetPool` for that UUID, retries there.

When using smart-client direct-to-storage mode (not via router), the storage itself may return:
```
Header: {0x00: 0x8001, ...}  // IPROTO_ERROR (bit 15 set)
Body:   {0x31: "Bucket 42 is not found on this storage"}
```
Client: triggers topology refresh, retries.

### Example 2 — MapCallRW (scatter)

```cpp
auto results = client.MapCallRW("stats.aggregate", formats::json::MakeObject());
// Spawns one coroutine per replicaset via utils::Async, waits all with engine::GetAll
```

Wire: N parallel IPROTO_CALL requests to N master pools, each independent.

---

## 17. Testing Strategy

### Test topology

Use `docker-compose` or `testsuite` to spin up a minimal vshard cluster:
- 2 replicasets × (1 master + 1 replica) = 4 storage nodes
- 1 vshard router
- `BUCKET_COUNT = 100` (small, for fast test iteration)

### Unit tests (`*_test.cpp`)

| Test | What it validates |
|---|---|
| `BucketCalculator` | CRC32 output matches Tarantool `digest.crc32()` for known inputs |
| `RoutingTable` | Bucket-to-replicaset lookup, atomic update |
| `MsgPackHelpers` | Encode/decode round-trip for all value types |
| `VshardErrorParser` | MOVED/TRANSFER detection and `destination` extraction |
| `ReplicaPoolSelector` | RW → master, RO → replica round-robin, BRO fallback |

### Integration tests (`*_vshardtest.cpp`)

| Test | What it validates |
|---|---|
| `BasicCallRW` | Simple application function call end-to-end |
| `MovedRetry` | Force bucket migration in test cluster; verify client retries successfully |
| `TransferRetry` | Simulate mid-transfer; verify retry with delay |
| `MapCallRW` | All replicasets respond; verify aggregation |
| `RouterFailover` | Kill one router replica; topology still discoverable via other |
| `PoolExhaustion` | Exhaust connections; verify queue_timeout error propagates correctly |
| `DeadlinePropagation` | Short deadline expires during topology refresh; verify no hang |

### Chaos tests (`functional_tests/chaos/`)

- `TcpGate` proxy in front of storages; test network partition, packet delay
- Kill master mid-request; verify `ReplicasetPool` failover

### Pytest functional tests

```python
# test_vshard_moved.py
async def test_moved_handled_transparently(service_client, vshard_cluster):
    """After moving a bucket the service should still answer correctly."""
    # Step 1: normal call
    resp = await service_client.post('/user/get', json={'id': '42'})
    assert resp.status == 200

    # Step 2: trigger bucket migration in the test cluster
    await vshard_cluster.move_bucket(bucket_id=42, destination_rs=1)

    # Step 3: call again — client must transparently retry after MOVED
    resp = await service_client.post('/user/get', json={'id': '42'})
    assert resp.status == 200
```

---

## 18. Checklist

### Protocol layer (✅ base connector Rec1–Rec6 complete)
- [x] `tnt::Buffer<16384>` scatter-gather recv buffer (Rec1)
- [x] `AppendTo` / direct staging send path (Rec2)
- [x] Typed mpp `MppEncode`/`MppDecode`/`Insert`/`Replace`/`Select` API (Rec3)
- [x] Typed `VspaceTuple` decode in `ResolveSpaceId` (Rec4)
- [x] `BuildPingFrame(sync)` compile-time PING frame in `iproto_frames.hpp` (Rec5)
- [x] `ParseIprotoResponse` zero-alloc scanner + `msgpack_scan::ReadUint`/`SkipValue` (Rec6)
- [x] `reader_body_buf_` per-connection reusable buffer
- [x] PR review bugs: uint64 narrowing, object round-trip loss, interval bounds UB
- [ ] vshard response envelope decoder: extract `[result, vshard_error]` using `msgpack_scan`
- [ ] vshard error object parser: `code`, `destination`, `type`, `message`
- [ ] Unit tests for envelope decoder with known-good MsgPack bytes

### Zero-copy forwarding (new, pending)
- [ ] `ParseCallForRoute()` in `iproto_frames.hpp` — scan 23 bytes, return `(bucket_id, tuple_span)`
- [ ] `tnt::Buffer::AppendView(ptr, len)` — non-owning scatter-gather segment
- [ ] `ForwardCall()` — 4-iovec writev: 41-byte prefix + raw TUPLE reference
- [ ] `is_vshard_success()` — 1-byte nil peek on response DATA
- [ ] Benchmark: zero-copy vs decode+reencode across payload sizes

### Routing
- [ ] `BucketCalculator`: CRC32 + pluggable custom function
- [ ] `RoutingTable`: lock-free RCU read, `uint16_t` flat array
- [ ] `TopologyFetcher`: calls `vshard.router.info()` + `vshard.router.buckets_info()`
- [ ] `utils::PeriodicTask` for background refresh
- [ ] MOVED-triggered refresh with 1s rate limiting
- [ ] Unit tests validating bucket assignment against Tarantool reference

### Connection layer
- [ ] `ReplicasetPool`: master pool + N replica pools
- [ ] `CallMode` dispatch: RW→master, RO→replica round-robin
- [ ] `IsAvailable()` aggregation across master + replicas
- [ ] Statistics per replicaset: connections, call latency, error rate

### Client layer
- [ ] `VshardProxy::CallRW/CallRO/CallBRO/CallBRE`
- [ ] `VshardProxy::Call(bucket_id, mode, ...)` (pre-computed bucket)
- [ ] `VshardProxy::MapCallRW` (scatter via `utils::Async` + `engine::GetAll`)
- [ ] MOVED retry (once), TRANSFER retry (once with sleep), then throw
- [ ] Deadline propagation into all async operations
- [ ] Tracing span per call with `bucket_id`, `mode`, `replicaset_uuid` tags

### Component
- [ ] `GetStaticConfigSchema()` with full property table
- [ ] Secdist integration (same JSON format as base connector)
- [ ] `routing_mode` switch: `embedded` / `router_proxy`
- [ ] Statistics registration: per-replicaset + MOVED/TRANSFER counters
- [ ] `kHasValidate<VshardProxyComponent> = true`
- [ ] Dynamic config key for default command control

### Tests
- [ ] Unit tests: all protocol/routing/calculator classes
- [ ] Integration tests: `*_vshardtest.cpp` against real cluster
- [ ] Functional tests: pytest with `pytest_userver.plugins.tarantool`
- [ ] Chaos tests: `TcpGate`-based network fault injection
- [ ] Sample service with documentation `/// [snippet]` markers

---

## 19. Non-Backward-Compatible Protocol Improvements

These changes require modifications to **both** router and storage side but
enable significantly higher efficiency.  Choosing any of them breaks wire
compatibility with standard vshard; all are opt-in extensions.

### 19.1 Bucket-Aware IPROTO Extension (`IPROTO_VSHARD_CALL`)

**Current**: `vshard.storage.call` is a regular Lua function call over IPROTO
`CALL`.  The storage-side `storage_call` wraps the user function, paying Lua
invocation overhead for every request.

**Proposed**: Add a dedicated IPROTO request type (or use an IPROTO stream +
header key) that carries `bucket_id` and `mode` directly in the binary
envelope:

```
IPROTO_VSHARD_CALL (new command code, e.g. 0x50):
  header:
    IPROTO_REQUEST_TYPE: 0x50
    IPROTO_SYNC: <sync>
    IPROTO_VSHARD_BUCKET_ID: <uint32>   # new key
    IPROTO_VSHARD_MODE: <uint8>         # 0=read, 1=write
  body:
    IPROTO_FUNCTION_NAME: "my.func"
    IPROTO_TUPLE: [args...]
```

The storage can then:
1. Look up bucket ownership in C (not Lua) using an in-memory array.
2. Ref/unref the bucket without any Lua call overhead.
3. Execute the stored function via the existing IPROTO_CALL path.

**Router zero-copy gain**: `bucket_id` is in the fixed-width header (~20
bytes).  The router reads only those bytes and forwards the entire body
**verbatim** — zero scanning, zero new bytes for the body:

```
iov[0]  5 bytes  pre-header (same length)
iov[1] 13 bytes  new header: CODE=CALL, SYNC=new_sync
iov[2]  N bytes  entire original body ← ZERO COPY, ZERO SCAN
```

**Estimated gain**: eliminates both the 23-byte body scan *and* one Lua
function call boundary per request (~1–3 µs saved per hop at high RPS).

### 19.2 Client-Side Routing (Eliminate Router Hop)

**Current path**: `client → router (net.box) → storage (net.box)` — two
network hops.

**Proposed**: Embed the routing logic in every application service as a
library (`VshardRouter` C++ class, §14.1).  The service computes `bucket_id`
locally and connects directly to the correct replicaset.

```
client app (with embedded VshardRouter) → storage   (1 hop)
```

The `VshardRouter` maintains the `route_map` locally and refreshes it via a
background task.  This is already how Strategy C works; adding the vshard
routing layer on top eliminates the dedicated router process entirely.

**Trade-offs**:
- Requires every service to maintain connection pools to all replicasets.
- Route table refresh must be robust (all services must react to bucket moves).
- Suitable for high-throughput services with many instances.

### 19.3 Direct Replicaset Connections with Typed mpp

With the typed mpp API from Rec3, the C++ router can produce IPROTO frames for
`vshard.storage.call` with zero heap allocations:

```cpp
// Zero-alloc IPROTO CALL frame for vshard.storage.call
auto frame = MakeIprotoCallFrame(sync, "vshard.storage.call",
    mpp::Array{bucket_id, mode_str, func_name,
               mpp::Array{/* user args */}});
```

Combined with the `tnt::Buffer` append chain (Rec1) the entire encode→send
path is alloc-free.

### 19.4 Streaming Bucket Migration via IPROTO Streams

**Current**: Bucket migration uses `net.box.call('vshard.storage.bucket_recv',
...)` — a Lua-to-Lua RPC with Lua-encoded tuples in chunks.

**Proposed**: Use IPROTO streams (Tarantool 2.10+) to stream migrating tuple
data directly as binary rows, bypassing Lua encoding:

```
IPROTO_STREAM:
  BEGIN / INSERT {bucket_data} / INSERT ... / COMMIT
  header: IPROTO_VSHARD_MIGRATION_META (source_rs, bucket_id, generation)
```

This turns a multi-round-trip Lua migration into a single streamed transaction,
reducing migration time and I/O amplification.

### 19.5 Bucket Count Scaling

Default `N = 3000` was chosen for Lua hash table performance.  With C++'s flat
array, `N = 65535` (fits in `uint16_t`) is equally cheap:

```cpp
std::array<uint16_t, 65535> route_map;  // 128 KB — still L2-resident
```

More buckets → finer-grained rebalancing, less data moved per bucket-migration
step.  This also reduces "hot bucket" probability in write-heavy workloads.

### 19.6 Summary of Protocol Changes

| Change | Backward compat | Gain |
|---|---|---|
| Embed routing in app (library) | No (removes router process) | Eliminate one RTT per request |
| `IPROTO_VSHARD_CALL` command | No (requires storage change) | Eliminate Lua call overhead on storage |
| Bucket count N=65535 | No (bootstrap change) | Finer rebalancing, less hotspots |
| IPROTO stream bucket migration | No (requires both sides) | Faster, lower-overhead migrations |
| Typed mpp zero-alloc frames | Yes (transparent to storage) | Lower allocator pressure on router |
| L1-resident `uint16_t` route table | Yes (router-only change) | ~40× faster route lookup |

---

## 20. Open Questions

1. **Cluster bootstrap**: Who creates the initial bucket distribution?  In
   vshard this is done via `vshard.router.bootstrap()`.  The C++ router should
   either expose an equivalent API or delegate to a Lua helper at startup.

2. **Rebalancer**: The rebalancer runs on storage side in Lua and is outside
   the router scope.  The C++ router interacts with rebalancing only through
   the `WRONG_BUCKET` / `TRANSFER_IS_IN_PROGRESS` error handling and
   `route_map` invalidation.  No C++ rebalancer is proposed.

3. **`return_raw` mode**: vshard supports returning undecoded msgpack objects
   from storage calls (useful for HTTP proxying).  The C++ equivalent is
   returning `formats::msgpack::Value` (already supported by the connector
   after Rec3/Rec4 changes).

4. **Named routers**: vshard supports multiple named router instances in one
   process.  The C++ `VshardRouterComponent` can be instantiated multiple
   times with different config keys.

5. **Async Map-Reduce result ordering**: Lua vshard returns an unordered map
   keyed by replicaset UUID/name.  The C++ API should match this to stay
   backward compatible, even though `std::unordered_map<std::string, ...>` is
   the natural equivalent.
