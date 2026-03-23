# Sync vs Async in the Tarantool Connector

## Context

After the Lua parity work on the vshard proxy was finished, the next step was
to look at steady-state performance of the C++ proxy under the benchmark flow
from `tarantool/vshard/bench/run_benchmarks.sh`.

The release binary was rebuilt and benchmarked against the local Tarantool
vshard example cluster. The benchmark is a client-side `net.box` workload that
hits the C++ proxy over IPROTO using many fibers.

By the time of this investigation:

- the correctness bug that caused one-off `bucket ... Not found` errors under
  fresh startup and high concurrency had already been fixed
- the benchmark was clean at `100` fibers and above
- the main question became: where is the steady-state CPU time spent, and what
  is the safest optimization path?

## Benchmark Setup

The relevant steady-state profile was collected as follows:

1. start the release `userver-tarantool-vshard-sample`
2. run a warmup benchmark against the already-running proxy
3. attach `perf` to the warm proxy process
4. run a second benchmark under the same load
5. inspect `perf report --children`

This matters because earlier short profiles included startup work and therefore
showed one-time stacktrace and logging initialization that was not relevant to
steady-state throughput.

The steady-state run used:

- `100` fibers
- `100000` operations
- bucket range `1..1500`

Observed result:

```text
100000 ops in 2.483 s = 40278 ops/sec  errors=0
```

## What Was Ruled Out

An earlier profile suggested that `boost_stacktrace` and logging code were hot.
That turned out to be misleading.

Once the proxy was warmed up before attaching `perf`:

- `boost_stacktrace` no longer showed up as a meaningful hot path
- `logging::LogExtra::Extend` disappeared from the top samples
- the application emitted `0` log lines during the profiled run

Conclusion:

- startup overhead was contaminating the earlier profile
- logging is not the main steady-state issue

## Steady-State Hot Paths

The steady-state profile pointed to these areas:

- `storages::tarantool::impl::Connection::ReaderLoop()`
- `vshard::impl::IprotoServer::ProcessSocket(...)`
- `vshard::impl::VshardProxy::CallRawBytes(...)`
- `storages::tarantool::impl::Pool::Execute(...)`
- `FdPoller::Impl::TryAppendAwaiter(...)`
- `TaskContext::DoStep()`
- `__send`
- `malloc`

This is an important result.

The profile is not dominated by one expensive vshard routing algorithm. The
cost is distributed across:

- connector-side request/response machinery
- coroutine scheduling and awaiter registration
- socket I/O
- allocation and request bookkeeping

## Why the Current "Sync" Path Is Not Actually Sync

At the API level, the Tarantool connector exposes both synchronous and
asynchronous operations. However, the current synchronous path is implemented
in terms of the asynchronous one.

### Pool::Execute

`storages::tarantool::impl::Pool::Execute()` currently does:

1. compute deadline
2. acquire a pooled connection
3. call `Connection::ExecuteAsync(...)`
4. release the connection back to the pool immediately
5. wait on `Future::wait_until(...)`
6. call `Future::get()`

This design is intentional. It allows a single connection to carry many
in-flight requests concurrently because the pool slot is released before the
response is received.

This is good for pipelining and throughput, but it means that every synchronous
request still pays for:

- `Promise<ExecutionResult>` creation (heap allocation of shared future state)
- `Future<ExecutionResult>` creation
- awaiter registration in `FdPoller`
- wakeup bookkeeping in the engine future state

### Connection::Execute

`storages::tarantool::impl::Connection::Execute()` is only a wrapper:

1. call `ExecuteAsync(...)`
2. `wait_until(...)`
3. `get()`

So moving call sites from `ExecuteAsync()` to `Execute()` does not remove the
future/promise machinery.

## Connector Internals That Match the Profile

### 1. SendAndRegister

`Connection::SendAndRegister(...)` is the core hot-path primitive.
It is called by `ExecuteAsync`, `ForwardStorageCallAsync`, and `ResolveSpaceId`.

It currently:

1. creates a `Promise<ExecutionResult>` (heap allocation of shared state)
2. obtains a `Future<ExecutionResult>`
3. generates a `sync_id` via atomic fetch-add
4. stores the promise into `pending_` under `pending_mutex_`
5. acquires `staging_mutex_`, appends the encoded frame into `staging_buf_` with
   back-patched preheader (zero intermediate allocation, one in-place build)
6. calls `flush_event_.Send()` to wake the flush coroutine
7. returns the future

Note: `ForwardVshardCallAsync` currently bypasses `SendAndRegister` and builds
the IPROTO_VSHARD_CALL frame directly into `staging_buf_` to avoid the body
vector allocation. This is an important precedent for the optimization.

The promise allocation is the core reason the synchronous API still behaves
like an async one internally.

### 2. ReaderLoop

`Connection::ReaderLoop()` is the receive-side demultiplexer.

For each response it currently does:

1. read into a ring buffer (`tnt::Buffer<16384>`) via `RecvSome`
2. copy body into reusable `reader_body_buf_` (flat buffer, grows to
   high-water-mark, zero alloc after warmup)
3. parse header and locate data/error spans via `ParseIprotoResponse`
4. on success: `data_buf.assign(resp.data_begin, resp.data_end)` — **this is a
   heap allocation per successful response** to copy data out of the reusable
   parse buffer
5. construct `ExecutionResult`
6. lock `pending_mutex_`, find entry by `sync_id`, move promise out, erase
7. `set_value(...)` on the promise — wakes the waiting coroutine

Key allocations on the hot path per response:
- `data_buf` heap allocation (step 4)
- promise state update, future wakeup (step 7)

### 3. FlushLoop

`Connection::FlushLoop()` batches staged frames.

The actual mechanism: a dedicated `flush_task_` waits on `flush_event_`
(a `SingleConsumerEvent`). When it wakes:

1. calls `engine::Yield()` once — yields the coroutine so all concurrent
   senders that have already called `flush_event_.Send()` get a chance to
   append their frames to `staging_buf_` before the flush task resumes
2. locks `staging_mutex_`, moves `staging_buf_` into a local `to_send` vector
3. sends `to_send` in one `SendAll` syscall

**Important defect identified in FlushLoop**: after `to_send = std::move(staging_buf_)`,
`staging_buf_` has zero capacity. The next sender to append frames will trigger
a heap allocation. This is a per-batch reallocation that happens on every flush
cycle and is not currently addressed.

That batching is useful and must not be discarded. It is likely one reason the
connector performs well enough despite the coroutine overhead.

## Why a Naive Rewrite Would Be Wrong

A tempting idea is:

- for synchronous calls, skip `ExecuteAsync()` and perform a direct blocking
  send/receive on the socket

That would be risky and likely incorrect in the current architecture.

The connection already owns:

- a dedicated flush task
- a dedicated reader task
- a shared `pending_` map for demultiplexing by `sync_id`

If a synchronous call bypassed that machinery and read from the socket directly,
it would fight with `ReaderLoop()` and break the protocol state machine.

So any safe optimization must preserve the existing ownership model:

- flush task remains the only send-side flusher
- reader task remains the only receive-side reader
- response delivery still goes through `pending_`

## The Real Optimization Candidate

The strongest candidate is not "make it truly blocking". It is:

- keep the current pipelined connection model
- but replace `Promise/Future` with a lighter sync waiter on the blocking path

In other words:

- async API continues to use `Promise/Future`
- sync API registers a cheaper waiter object in `pending_`
- `ReaderLoop()` resolves either kind of pending entry

### Concrete Pending Entry Design

The `pending_` map currently has type:

```cpp
std::unordered_map<uint64_t, engine::Promise<ExecutionResult>> pending_;
```

This needs to become a variant map. The two entry kinds:

```cpp
// Async path: existing behavior unchanged.
struct AsyncPendingEntry {
    engine::Promise<ExecutionResult> promise;
};

// Sync path: caller-owned shared state, no heap-allocated future state.
struct SyncPendingEntry {
    // Written by ReaderLoop before signalling ready.
    ExecutionResult result;
    std::exception_ptr exc;
    // Signalled by ReaderLoop when result/exc are ready.
    engine::SingleConsumerEvent ready;
    // Set to true by the caller when it times out or is cancelled,
    // so that a concurrent ReaderLoop delivery can detect it and skip.
    std::atomic<bool> abandoned{false};
};

using PendingEntry = std::variant<AsyncPendingEntry, SyncPendingEntry>;
```

The `pending_` map becomes:

```cpp
std::unordered_map<uint64_t, std::shared_ptr<PendingEntry>> pending_;
```

Using `shared_ptr` resolves the lifetime problem (see below).

### Lifetime and Cancellation Safety

The difficult part is not sending or waking. The difficult part is timeout and
cancellation safety.

Today, with `Future`:

- if the caller times out, the pending future state can safely remain alive
- a late response can still complete it without touching invalid memory

With a stack-local sync waiter, that is no longer true.

If the caller returns on timeout while `pending_` still points to a stack-local
waiter, then a later response would dereference a dead object.

**The `shared_ptr` solution**: both the caller and `pending_` hold a
`shared_ptr<PendingEntry>`. On timeout the caller:

1. sets `entry->get<SyncPendingEntry>().abandoned = true`
2. locks `pending_mutex_`, erases its sync_id entry
3. releases its `shared_ptr` — if ReaderLoop already grabbed and erased the
   entry, this was the last reference and the object is destroyed safely;
   if ReaderLoop has not yet arrived, the entry was already removed from the
   map so ReaderLoop will not find it and will discard the response

The reader loop:

1. locks `pending_mutex_`, moves the `shared_ptr` out of the map, unlocks
2. fills `result` / `exc` in the entry
3. calls `entry->get<SyncPendingEntry>().ready.Send()`
4. releases its local `shared_ptr`

This is safe in all cases:

| Scenario                   | Who holds the last ref | Destructor caller |
|----------------------------|------------------------|-------------------|
| success, no timeout        | caller after wake      | caller on scope exit |
| timeout before reader      | caller (map erased)    | caller on timeout |
| timeout races reader       | reader's local copy    | reader (caller's ref already gone) |
| connection broken          | WakeAllPending loop    | loop on entry release |

`WakeAllPending` must also be updated to visit both entry kinds.

## Low-Risk Optimizations to Address First

Before the larger waiter redesign, the following lower-risk changes have a
clear implementation path and well-understood risk:

### 1. Fix `staging_buf_` Capacity Loss in FlushLoop

In `FlushLoop`, after `to_send = std::move(staging_buf_)`, add:

```cpp
staging_buf_.reserve(to_send.capacity());
```

This must happen inside the `staging_mutex_` lock, before releasing it, to
prevent a concurrent sender from observing zero capacity and allocating first.
This eliminates one heap allocation per flush cycle on the steady-state path.

### 2. Reserve `pending_`

At connection construction, pre-reserve the pending map to the expected
concurrency level (e.g. 128 or configurable). This reduces hash table
rehashing under load:

```cpp
pending_.reserve(128);
```

### 3. Eliminate `data_buf` Allocation in ReaderLoop

Currently `ReaderLoop` does:

```cpp
data_buf.assign(resp.data_begin, resp.data_end);
```

This copies response data from `reader_body_buf_` into a new vector. This
allocation is necessary because `reader_body_buf_` is reused on the next
frame. However, with the sync waiter path, the result can be moved directly
into the waiter's `result` field rather than through a separate `data_buf`
allocation followed by a move into `ExecutionResult`. The async path still
needs the copy, but the sync path can take ownership of a slice.

This is a second-order optimization that requires the waiter redesign to be
in place first.

### 4. Measure Span Overhead

`Pool::Execute()` creates a tracing span for every request. Span construction
involves string operations and potentially atomic increments for parent-child
linkage. This should be measured separately to determine whether it is
significant at 40k ops/sec.

## Proposed Implementation Plan

### Phase 1: Quick Wins (No Architecture Change)

**Goal**: collect measurable baseline improvements with minimal risk.

**1.1 Fix `staging_buf_` capacity loss**

File: `tarantool/src/storages/tarantool/impl/connection.cpp`, `FlushLoop()`

In the inner drain loop, after `to_send = std::move(staging_buf_)`, add
`staging_buf_.reserve(to_send.capacity())` before releasing `staging_mutex_`.

Risk: none. This is a standard STL capacity pre-reservation.

**1.2 Reserve `pending_` at construction**

File: `connection.cpp`, `Connection::Connection()`

After the flush and reader tasks are started, add:

```cpp
pending_.reserve(128);
```

Or make the reserve size configurable from `EndpointSettings`.

Risk: none. Reserve on unordered_map is well-defined; existing entries and
iterators are not affected.

**1.3 Benchmark and re-profile**

Rerun the 100-fiber/100k-ops benchmark after each Phase 1 step. The expected
gain is small (single-digit percent) but establishes a clean new baseline.

---

### Phase 2: Introduce `PendingEntry` Abstraction

**Goal**: refactor `pending_` to support two completion modes without changing
observable behavior. The async path must remain identical after this phase.

**2.1 Define `SyncPendingEntry` and `AsyncPendingEntry`**

File: `connection.hpp` (private section) or a new internal header
`impl/pending_entry.hpp`

```cpp
struct AsyncPendingEntry {
    engine::Promise<ExecutionResult> promise;
};

struct SyncPendingEntry {
    ExecutionResult result;
    std::exception_ptr exc;
    engine::SingleConsumerEvent ready;
    std::atomic<bool> abandoned{false};

    SyncPendingEntry() = default;
    SyncPendingEntry(const SyncPendingEntry&) = delete;
    SyncPendingEntry& operator=(const SyncPendingEntry&) = delete;
};

using PendingEntry = std::variant<AsyncPendingEntry, SyncPendingEntry>;
```

**2.2 Change `pending_` type**

```cpp
// before
std::unordered_map<uint64_t, engine::Promise<ExecutionResult>> pending_;

// after
std::unordered_map<uint64_t, std::shared_ptr<PendingEntry>> pending_;
```

**2.3 Update `ReaderLoop` to dispatch on entry kind**

Replace the current promise-move-and-set block with:

```cpp
std::shared_ptr<PendingEntry> entry;
{
    std::lock_guard lock(pending_mutex_);
    auto it = pending_.find(sync_id);
    if (it != pending_.end()) {
        entry = std::move(it->second);
        pending_.erase(it);
    }
}
if (entry) {
    std::visit(overloaded{
        [&](AsyncPendingEntry& a) {
            if (code == 0)
                a.promise.set_value(std::move(result));
            else
                a.promise.set_exception(std::make_exception_ptr(...));
        },
        [&](SyncPendingEntry& s) {
            if (s.abandoned.load(std::memory_order_acquire)) return;
            if (code == 0)
                s.result = std::move(result);
            else
                s.exc = std::make_exception_ptr(...);
            s.ready.Send();
        }
    }, *entry);
}
```

**2.4 Update `WakeAllPending`**

```cpp
for (auto& [id, entry] : pending) {
    std::visit(overloaded{
        [&](AsyncPendingEntry& a) { a.promise.set_exception(ex); },
        [&](SyncPendingEntry& s)  {
            s.exc = ex;
            s.ready.Send();
        }
    }, *entry);
}
```

**2.5 Keep `SendAndRegister` using `AsyncPendingEntry`**

No change to `SendAndRegister` at this step. All existing callers continue to
go through the async path. This phase is a pure refactor — behavior is
identical.

**Verification**: all existing tests pass. Benchmark is unchanged (within noise).

---

### Phase 3: Add `SendAndWait` — the Sync Primitive

**Goal**: add a new `Connection` method that registers a `SyncPendingEntry` and
waits on it directly, bypassing `Promise/Future`.

**3.1 Add `SendAndWait` to `Connection`**

Signature (private):

```cpp
ExecutionResult Connection::SendAndWait(engine::Deadline deadline,
                                        uint32_t request_type,
                                        std::vector<uint8_t> body);
```

Implementation outline:

```cpp
ExecutionResult Connection::SendAndWait(engine::Deadline deadline,
                                        uint32_t request_type,
                                        std::vector<uint8_t> body) {
    if (deadline.IsReached()) {
        throw TarantoolException{"Request deadline exceeded before send"};
    }

    auto entry_ptr = std::make_shared<PendingEntry>(
        std::in_place_type<SyncPendingEntry>);
    auto& sync_entry = std::get<SyncPendingEntry>(*entry_ptr);

    const uint64_t sync_id = ++sync_counter_;
    {
        std::lock_guard lock(pending_mutex_);
        pending_.emplace(sync_id, entry_ptr);
    }

    // Stage frame — same as SendAndRegister but no promise.
    {
        std::lock_guard lock(staging_mutex_);
        const auto prehdr_pos = staging_buf_.size();
        staging_buf_.resize(prehdr_pos + 5);
        BuildHeader(staging_buf_, request_type, sync_id);
        staging_buf_.insert(staging_buf_.end(), body.begin(), body.end());
        const uint32_t len =
            static_cast<uint32_t>(staging_buf_.size() - prehdr_pos - 5);
        staging_buf_[prehdr_pos]     = 0xce;
        staging_buf_[prehdr_pos + 1] = static_cast<uint8_t>(len >> 24);
        staging_buf_[prehdr_pos + 2] = static_cast<uint8_t>(len >> 16);
        staging_buf_[prehdr_pos + 3] = static_cast<uint8_t>(len >> 8);
        staging_buf_[prehdr_pos + 4] = static_cast<uint8_t>(len);
    }
    flush_event_.Send();

    // Wait for delivery.
    const bool signalled = sync_entry.ready.WaitForEventUntil(deadline);
    if (!signalled) {
        // Timeout or cancellation: mark abandoned and remove from pending.
        sync_entry.abandoned.store(true, std::memory_order_release);
        {
            std::lock_guard lock(pending_mutex_);
            pending_.erase(sync_id);
        }
        // entry_ptr may still be held by ReaderLoop. The abandoned flag
        // ensures it will skip delivery. The object is destroyed when both
        // this scope and any concurrent reader release their shared_ptr.
        engine::current_task::CancellationPoint();
        throw TarantoolException{"SendAndWait deadline expired"};
    }

    if (sync_entry.exc) std::rethrow_exception(sync_entry.exc);
    return std::move(sync_entry.result);
}
```

**3.2 Add `ForwardStorageCallSync` and `ForwardVshardCallSync`**

Same pattern as `SendAndWait` but building the frame as `ForwardStorageCallAsync`
and `ForwardVshardCallAsync` do (zero intermediate body vector for the vshard
call variant). These replace the async variants on the synchronous pool path.

---

### Phase 4: Switch Pool Synchronous API to New Path

**Goal**: replace all `XxxAsync` + `wait_until` + `get` patterns in `Pool` with
direct `SendAndWait`-based calls.

Files to change: `pool.cpp`

Callers:

| Pool method             | Connection call to replace                  |
|-------------------------|---------------------------------------------|
| `Pool::Execute`         | `ExecuteAsync` → `SendAndWait`              |
| `Pool::ForwardStorageCall` | `ForwardStorageCallAsync` → `ForwardStorageCallSync` |
| `Pool::ForwardVshardCall`  | `ForwardVshardCallAsync` → `ForwardVshardCallSync`   |
| `Pool::Ping`            | `PingAsync` → `SendAndWait(kIprotoPing, {})`|

After this change:
- no `engine::Promise` or `engine::Future` is constructed on the synchronous
  hot path
- the connection pool slot is still released before waiting (pipelining
  preserved)
- the `SingleConsumerEvent` wait replaces the `Future::wait_until` wait

**Note on `Connection::Execute` and `Connection::Ping`**: these wrappers
currently call `ExecuteAsync`/`PingAsync` and wait. They should be updated to
call `SendAndWait` directly. If they are only called from tests or maintenance
paths, this is low priority.

---

### Phase 5: Benchmark and Profile

After Phase 4 is complete:

- rerun the 100-fiber / 100k-ops benchmark
- rerun `run_benchmarks.sh`
- rerun steady-state `perf record` + `perf report --children` on the warmed proxy

Success criteria:

- correctness unchanged (zero errors at 100 fibers)
- no regression in pipelining behavior (multiple in-flight requests per
  connection still work)
- `FdPoller::Impl::TryAppendAwaiter` and `TaskContext::DoStep` reduced in
  profile weight (they handle future await registration — should shrink)
- `malloc` reduced in profile weight
- measurable throughput gain over 40278 ops/sec baseline

A regression in any correctness test or a loss of pipelining throughput is a
blocker. Throughput gain smaller than noise is a signal that the bottleneck
has shifted elsewhere.

---

## Outstanding Risks and Open Questions

### Risk 1: `SyncPendingEntry` size

`SyncPendingEntry` contains an `ExecutionResult` (which includes a
`std::vector<uint8_t>` for the response data) and an `engine::SingleConsumerEvent`.
The entry is always heap-allocated via `shared_ptr`. If `SingleConsumerEvent`
itself allocates internally, the net allocation count may not change. This
should be verified by inspecting the userver `SingleConsumerEvent` implementation.

### Risk 2: `shared_ptr` control block allocation

Every `SendAndWait` call allocates a `shared_ptr<PendingEntry>` control block.
This is one allocation instead of the promise shared state allocation. The
sizes may be similar. Use `std::make_shared` (which combines control block and
object into one allocation) to minimize cost.

### Risk 3: `abandoned` race window

There is a brief window where `ReaderLoop` has moved the entry out of the map
and is about to call `ready.Send()`, while simultaneously the caller sets
`abandoned = true` and returns. In this case the reader sets the event on an
object that still lives (shared_ptr keeps it alive) but nobody waits for it.
This is correct — the event fires into the void and the object is destroyed
when the reader releases its shared_ptr. However, any assert on
`!abandoned || ready_was_signalled` in the caller must account for this race.

### Risk 4: `ForwardVshardCallAsync` already bypasses body allocation

The current `ForwardVshardCallAsync` directly stages into `staging_buf_`
rather than going through `SendAndRegister`. The `ForwardVshardCallSync`
replacement must preserve this zero-body-allocation property. Do not
naively route it through `SendAndWait(body)` — that would re-introduce
the body vector allocation that was deliberately eliminated.

### Open Question: Conn::ExecuteAsync callers in vshard paths

`VshardProxy::CallRawBytes` and related forwarding paths call into pool methods
that ultimately use `ExecuteAsync`. Verify whether these are always called from
a context that later waits synchronously (in which case they should migrate to
the sync path) or whether they are genuinely fire-and-forget (in which case
they must stay on the async path).

---

## Recommendation

The connector should not be "made synchronous" by bypassing its async
architecture. That would cut across the current ownership model and likely
introduce correctness issues.

The correct direction, ordered by risk and implementation complexity:

1. **Phase 1**: fix `staging_buf_` capacity loss and reserve `pending_` — safe,
   measurable, no design change
2. **Phase 2**: introduce `PendingEntry` variant as a pure refactor — zero
   behavior change, unblocks Phase 3
3. **Phase 3–4**: add `SendAndWait` and migrate pool sync callers — the main
   optimization, removes `Promise/Future` from the synchronous hot path
4. **Phase 5**: benchmark and profile to confirm gain or redirect effort

That is the most plausible path to a meaningful performance win without
undoing the connector design that already works well under concurrency.
