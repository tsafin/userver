# tntcxx Integration Plan for userver Tarantool Connector

**PR Reference:** [tsafin/userver#1](https://github.com/tsafin/userver/pull/1) — Tarantool async connector  
**tntcxx Reference:** [tarantool/tntcxx](https://github.com/tarantool/tntcxx)  
**Date:** March 2026
**Status:** All 5 recommendations implemented (verified 2026-03-25)

---

## Context and Baseline

The PR already achieved significant progress toward zero-copy efficiency:

| Optimization already implemented | Speedup |
|---|---|
| Replaced `MpDecoder` + `json::Value` with zero-copy `msgpack::Value` cursor (Phase 1+2) | +16% throughput |
| Replaced `EncodeJson()` with `ValueBuilder::ToBytes()` (Phase 3) | 4.8× encode, 7.4× decode |
| Dedicated `FlushLoop` coroutine for batched `writev` | +19% pipeline throughput |
| Full pipeline: REPLACE coro=128 vs baseline | **+746% total** |

The remaining bottlenecks identified below are the next tier of improvements. tntcxx is already vendored in `third_party/` and used for cross-validation tests, so all recommendations have zero new dependencies.

## Implementation Status Summary

| # | Recommendation | Status | Notes |
|---|---|---|---|
| 1 | `tnt::Buffer<N>` recv buffer | **Done** | `tnt::Buffer<16384>` in `ReaderLoop` — no memmove |
| 2 | `AppendTo()` / zero-copy encode | **Done** | `ValueBuilder::AppendTo` in `query.cpp`; `Query::WithRawArgs` for pre-encoded bytes; vshard hot path (`ForwardVshardCall`, `ForwardStorageCall`) writes directly into `staging_buf_` |
| 3 | `mpp`-reflected structs | **Done** | `typed.hpp`: `Insert<T>`, `Replace<T>`, `Select<T>`, `MppEncode`, `MppDecode` |
| 4 | `mpp::decode` for `_vspace` | **Done** | `VspaceTuple` + `DecodeVspaceTuples` in `impl/vspace_tuple.hpp` |
| 5 | Compile-time static PING frame | **Done** | `BuildPingFrame` in `iproto_frames.hpp` returns a fixed-layout `std::array<uint8_t, 18>` |

---

## Recommendation 1 — Replace `memmove` Receive Buffer with `tnt::Buffer<N>`

### Priority: HIGH ✅ Done
### Estimated effort: 3–5 days
### Expected gain: ~5–15% throughput reduction in CPU overhead at high pipeline depth

### Problem

In `connection.cpp`, `ReaderLoop` maintains a `std::vector<uint8_t> recv_buf_` (64 KB) with compacting `memmove` after each decoded frame:

```cpp
// Current pattern in ReaderLoop (connection.cpp)
size_t n = socket_.RecvSome(recv_buf_.data() + filled_, space_left, deadline);
filled_ += n;

// ... decode frame of length frame_len ...

size_t remaining = filled_ - frame_len;
std::memmove(recv_buf_.data(), recv_buf_.data() + frame_len, remaining);
filled_ = remaining;
```

At high pipeline depth (coro=128, pool=4), many short responses (REPLACE returns ~30–50 bytes each) arrive in the same TCP segment. Each decode loop iteration calls `memmove` to compact the buffer. The cost is O(remaining bytes), which for a 64KB buffer receiving 30-byte responses can be thousands of bytes per `memmove` call.

### Root Cause

A single flat buffer forces all unprocessed bytes to be shifted left after each frame consumed. At 237k op/s (current peak), this is ~237,000 `memmove` calls per second on a buffer that is on average ~50% full.

### tntcxx Solution

`tnt::Buffer<BlockSize>` is a **linked list of fixed-size blocks** parameterized by allocator:

```cpp
// tntcxx/src/Buffer/Buffer.hpp
template <size_t N, class allocator_t = std::allocator<char>>
class Buffer {
    // Data organized as: Block → Block → Block → ...
    // Each Block is N bytes. Blocks are never moved.
    // Decoder cursor walks blocks via iterator arithmetic.
};
```

Key properties:
- Data is **never moved** after being written. Blocks are appended, consumed, and freed.
- The decoder receives `(begin_iterator, end_iterator)` spanning multiple blocks without any contiguous copy.
- Unused space at the front of consumed blocks is reclaimed by freeing the block, not by shifting bytes.

### Implementation Steps

**Step 1.1** — Add `tnt::Buffer` as the receive buffer type on `Connection`:

```cpp
// connection.hpp
#include "tntcxx/src/Buffer/Buffer.hpp"

class Connection {
    // Replace:
    // std::vector<uint8_t> recv_buf_;
    // size_t filled_ = 0;
    
    // With:
    tnt::Buffer<16 * 1024> recv_buf_;
};
```

**Step 1.2** — Adapt `ReaderLoop` to use the tntcxx buffer API. The read side appends into the buffer's write region; the decode side consumes via iterators:

```cpp
// ReaderLoop — write side
auto [wbegin, wend] = recv_buf_.getWriteRange();       // pointer to free space
size_t n = socket_.RecvSome(wbegin, wend - wbegin, deadline);
recv_buf_.advanceWrite(n);                             // commit written bytes

// ReaderLoop — decode side
auto it = recv_buf_.begin();                           // iterator into linked blocks
size_t available = recv_buf_.size();

if (available < 5) continue;                           // wait for prehdr

// Decode 5-byte prehdr without copying: iterator reads across block boundaries
uint32_t body_len = DecodePreheaderLength(it);         // iterator-based decode
if (available < 5 + body_len) continue;

// Decode frame: cursor walks linked blocks, no contiguous buffer required
DecodeFrame(it, body_len);                             // zero-copy across blocks

recv_buf_.consume(5 + body_len);                       // free consumed blocks
// No memmove. Blocks at front are freed when fully consumed.
```

**Step 1.3** — Adapt `DecodePreheaderLength` and `DecodeFrame` in `msgpack.hpp` to accept tntcxx buffer iterators in addition to raw `const uint8_t*`. Since tntcxx iterators model `LegacyInputIterator`, most existing decode logic works with minor template generalization:

```cpp
// Before:
uint32_t DecodePreheaderLength(const uint8_t* data, size_t len);

// After (template generalization):
template <typename Iterator>
uint32_t DecodePreheaderLength(Iterator begin, size_t available);
```

**Step 1.4** — Handle block-boundary reads. tntcxx buffer iterators transparently cross block boundaries. For multi-byte integers that span two blocks, use the iterator-based read helper from tntcxx:

```cpp
// tntcxx provides: read<uint32_t>(iterator) — handles cross-block reads
uint32_t val = tnt::read<uint32_t>(it);  // safe across block boundary
```

**Step 1.5** — Ensure `ExecutionResult`'s owned data buffer (`data_buf_`) is populated by **copying** the frame bytes out of the tntcxx buffer (since the buffer block will be freed after `consume()`). This is already the correct semantics — the cursor in `ExecutionResult` points to owned `data_buf_`, not to the receive buffer.

### Verification

| Test | Method | Pass Criteria |
|---|---|---|
| Unit: existing 57 msgpack tests | Run without modification | All pass |
| Unit: receive buffer framing | New test: send 3 concatenated IPROTO frames into a mock socket; verify all 3 are decoded correctly | All 3 decoded, correct `sync_id` |
| Unit: cross-block decode | New test: set block size = 8 bytes, encode a 20-byte frame, verify correct decode | No panic, correct result |
| Perf: `memmove` elimination | `perf stat -e cache-misses` on `ReaderLoop` before/after | Cache miss rate reduced ≥ 10% at coro=128 |
| Regression: all functional tests | `pytest tarantool/functional_tests/` | 7/7 pass |
| Benchmark: pipeline throughput | `TarantoolBench.InsertThroughput` at pool=4, coro=128 | ≥ 237k op/s (no regression) |

---

## Recommendation 2 — Eliminate `ValueBuilder` → `staging_buf_` Copy via `AppendTo()`

### Priority: HIGH ✅ Done
### Estimated effort: 1–2 days
### Expected gain: 1 heap allocation + 1 memcpy eliminated per request on the encode path

### Problem

The current encode path in `Query` construction and `FlushLoop` has an extra copy:

```cpp
// Query factory (query.cpp) — Phase 3 implementation
auto b = formats::msgpack::ValueBuilder::Array();
b.PushBack(formats::msgpack::ValueBuilder{id});
b.PushBack(formats::msgpack::ValueBuilder{value});
auto bytes = b.ToBytes();           // ← allocates std::vector<uint8_t>
// bytes is stored in Query::arg_bytes_ (move, OK)
// ...

// SendAndRegister (connection.cpp) — FlushLoop path
{
    std::lock_guard lock(staging_mutex_);
    staging_buf_.insert(staging_buf_.end(),
                        frame.begin(), frame.end());  // ← copies bytes again
}
```

So for each request: one `vector` allocation in `ToBytes()`, one `insert` copy into `staging_buf_`. The second copy happens under `staging_mutex_` which is a brief but contended operation.

### tntcxx Insight

tntcxx's encoder writes **directly into a target buffer** passed by reference, never producing an intermediate allocation. The equivalent pattern in userver's API would be:

```cpp
// Encode directly into destination — zero intermediate allocation
builder.AppendTo(staging_buf_);  // single pass, no temporary vector
```

### Implementation Steps

**Step 2.1** — Add `AppendTo(std::vector<uint8_t>&)` method to `ValueBuilder`:

```cpp
// value_builder.hpp
class ValueBuilder {
public:
    // Existing:
    std::vector<uint8_t> ToBytes() const;
    
    // New:
    void AppendTo(std::vector<uint8_t>& dest) const;
    
    // Implementation: same encoding logic as ToBytes(),
    // but appends to dest instead of creating a new vector.
    // This is a ~10 line change in value_builder.cpp.
};
```

**Step 2.2** — Add `AppendTo` overload for `tnt::Buffer<N>` (forward-compatible with Recommendation 1):

```cpp
template <size_t N>
void AppendTo(tnt::Buffer<N>& dest) const;
```

**Step 2.3** — Change `Query` to store pre-serialized bytes and support direct staging:

```cpp
// query.hpp — expose AppendArgsTo / AppendOpsTo
class Query {
public:
    void AppendArgsTo(std::vector<uint8_t>& dest) const;
    void AppendOpsTo(std::vector<uint8_t>& dest) const;
private:
    // Keep arg_bytes_ / ops_bytes_ as before (pre-serialized at construction)
};
```

**Step 2.4** — In `SendAndRegister`, build the IPROTO frame directly into `staging_buf_` without an intermediate vector:

```cpp
// connection.cpp — SendAndRegister
void Connection::SendAndRegister(const IprotoFrame& frame, ...) {
    std::lock_guard lock(staging_mutex_);
    
    // Encode header directly into staging_buf_
    EncodeIprotoHeader(staging_buf_, frame.type, frame.sync_id, body_size);
    
    // Append pre-serialized body bytes — no extra copy
    frame.query.AppendArgsTo(staging_buf_);
    
    flush_event_.Send();
}
```

**Step 2.5** — If the full IPROTO frame size is not known before encoding (because body size must be in the header), use a two-phase approach common in high-performance network code:

```cpp
// Reserve 5 bytes for the prehdr, remember position
size_t prehdr_pos = staging_buf_.size();
staging_buf_.resize(prehdr_pos + 5);  // placeholder

size_t body_start = staging_buf_.size();
EncodeBody(staging_buf_, frame);       // append body
size_t body_len = staging_buf_.size() - body_start;

// Patch the prehdr in place
WritePrehdr(staging_buf_.data() + prehdr_pos, body_len);
```

This is exactly the pattern used in tntcxx's `mpp::encode` for variable-length containers.

### Verification

| Test | Method | Pass Criteria |
|---|---|---|
| Unit: `AppendTo` correctness | Compare `AppendTo` output byte-for-byte vs `ToBytes()` for all types | Identical bytes |
| Unit: cross-check with tntcxx | Extend `msgpack_xcheck_test.cpp` — encode via `AppendTo`, decode via tntcxx mpp | All 10 existing xcheck tests pass |
| Perf: allocation count | `valgrind --tool=massif` on 10k REPLACE requests before/after | Heap allocations on encode path reduced by ~1 per request |
| Benchmark: encode throughput | `TarantoolCpuBench.EncodeDecodeComparison` | Encode op/s ≥ 4.0M (no regression from current 4.0M) |
| Regression | Full functional test suite | 7/7 pass |

---

## Recommendation 3 — `mpp`-Reflected Structs for Compile-Time Encode on Hot Paths

### Priority: MEDIUM ✅ Done
### Estimated effort: 3–4 days (API addition, not replacement)
### Expected gain: Zero heap allocations on encode path for fixed-schema requests; ~sub-microsecond encode

### Problem

`ValueBuilder` builds a variant node tree in heap memory, then serializes it. Even after Phase 3, for a simple `Replace(space, {uint64_id, std::string_value})`:

1. Allocate `ValueBuilder` with variant nodes for each field.
2. Walk the tree in `ToBytes()`, write bytes.
3. Free the variant nodes.

For a high-throughput service doing 200k+ REPLACE/s, this is 200k × (alloc + traverse + free) per second.

### tntcxx Solution

tntcxx's `mpp` trait system resolves the entire encoding at **compile time** given a struct with a `mpp` member:

```cpp
struct KvTuple {
    uint64_t id;
    std::string value;
    static constexpr auto mpp = std::make_tuple(
        &KvTuple::id, &KvTuple::value);
};

// Encoding — entirely resolved at compile time, no tree, no alloc:
KvTuple t{42, "hello"};
mpp::encode(buf_writer, t);  // emits: fixarray[2], uint64(42), fixstr "hello"
```

The compiler inlines the entire encode loop for the specific struct layout. No branches, no virtual dispatch, no allocations.

### Implementation Steps

**Step 3.1** — Add templated factory overloads to `Cluster` for `mpp`-reflected structs:

```cpp
// cluster.hpp
#include "tntcxx/src/mpp/mpp.hpp"

template <typename T,
          typename = std::void_t<decltype(T::mpp)>>  // SFINAE: only if T has mpp
engine::TaskWithResult<ExecutionResult>
Replace(std::string_view space_name, const T& tuple,
        engine::Deadline deadline = {});

template <typename T, typename = std::void_t<decltype(T::mpp)>>
engine::TaskWithResult<ExecutionResult>
Insert(std::string_view space_name, const T& tuple,
       engine::Deadline deadline = {});
```

**Step 3.2** — Implement the template body: encode `T` directly into a staging buffer using tntcxx's mpp encoder, bypassing `ValueBuilder` entirely:

```cpp
// cluster.cpp (or cluster.hpp if header-only template)
template <typename T, typename>
engine::TaskWithResult<ExecutionResult>
Cluster::Replace(std::string_view space_name, const T& tuple, engine::Deadline deadline) {
    // Resolve space ID (cached after first call)
    uint32_t space_id = co_await ResolveSpaceId(space_name, deadline);
    
    // Encode args directly using tntcxx mpp — zero allocation
    std::array<uint8_t, 256> stack_buf;   // stack-allocated for small tuples
    tnt::StaticBuffer<256> enc_buf(stack_buf.data());
    mpp::encode(enc_buf, tuple);           // compile-time specialized for T
    
    Query q = Query::FromPreEncodedBytes(
        IPROTO_REPLACE, space_id,
        enc_buf.data(), enc_buf.size());
    
    co_return co_await Execute(std::move(q), deadline);
}
```

**Step 3.3** — Add a `Query::FromPreEncodedBytes` factory for callers that have already encoded their args:

```cpp
// query.hpp
static Query FromPreEncodedBytes(uint8_t iproto_type,
                                  uint32_t space_id,
                                  const uint8_t* args_data,
                                  size_t args_size);
```

**Step 3.4** — Add `Cluster::Select<T>` for decode symmetry:

```cpp
template <typename T, typename = std::void_t<decltype(T::mpp)>>
engine::TaskWithResult<std::vector<T>>
Select(std::string_view space_name, const KeyTuple& key, ...);

// Implementation: decode response bytes into std::vector<T> via mpp::decode
```

**Step 3.5** — Document the dual API in cluster.hpp with a usage example:

```cpp
// Option A: dynamic, flexible (existing API)
auto b = msgpack::ValueBuilder::Array();
b.PushBack(msgpack::ValueBuilder{user_id});
b.PushBack(msgpack::ValueBuilder{email});
co_await cluster->Replace("users", std::move(b));

// Option B: typed, zero-alloc (new API — requires mpp member)
struct UserTuple {
    uint64_t id;
    std::string email;
    static constexpr auto mpp = std::make_tuple(&UserTuple::id, &UserTuple::email);
};
co_await cluster->Replace("users", UserTuple{user_id, email});
```

### Verification

| Test | Method | Pass Criteria |
|---|---|---|
| Unit: mpp encode output | Compare `mpp::encode` bytes vs `ValueBuilder::ToBytes()` for same data | Byte-identical |
| Unit: tntcxx xcheck | Encode via new `Replace<T>` path, decode via tntcxx reference decoder | Round-trip matches |
| Unit: select round-trip | `Insert<T>` then `Select<T>`, verify decoded struct fields | All fields match |
| Perf: zero-alloc validation | `valgrind` on 1k `Replace<KvTuple>` calls | 0 heap allocations on encode path |
| Benchmark: encode op/s | `TarantoolCpuBench` with `KvTuple` path | ≥ 6M op/s (vs current 4M) |
| API: no regression | All existing `ValueBuilder`-based callers (service.cpp tests) | 7/7 functional tests pass |

---

## Recommendation 4 — `mpp::decode` for `ResolveSpaceId` / `_vspace` Navigation

### Priority: LOW-MEDIUM ✅ Done
### Estimated effort: 1 day
### Expected gain: Type safety, reduced manual cursor arithmetic, compiler-verified field mapping

### Problem

`Connection::ResolveSpaceId()` currently decodes `_vspace` response with manual `formats::msgpack::Value` navigation:

```cpp
// connection.cpp — current ResolveSpaceId
auto data = response.GetData();          // msgpack::Value cursor
auto arr = data[0];                      // first tuple
uint32_t space_id = arr[0].As<uint32_t>();  // field 0 = id
// field 2 = name (not checked, just assumed correct)
```

If the `_vspace` schema changes (e.g., additional fields in newer Tarantool versions), this silently reads the wrong field. There is also no compile-time verification that `arr[0]` is `uint32_t`.

### tntcxx Solution

Define the `_vspace` tuple structure with `mpp` and decode it typed:

```cpp
// vspace_tuple.hpp (new, internal)
struct VspaceTuple {
    uint32_t id;
    uint32_t owner;
    std::string name;
    std::string engine;
    uint32_t field_count;
    // Note: remaining fields (format, flags) are optional/complex;
    // mpp stops decoding after the last declared field.
    
    static constexpr auto mpp = std::make_tuple(
        &VspaceTuple::id,
        &VspaceTuple::owner,
        &VspaceTuple::name,
        &VspaceTuple::engine,
        &VspaceTuple::field_count);
};
```

Then `ResolveSpaceId` becomes:

```cpp
std::vector<VspaceTuple> spaces;
mpp::decode(response_cursor, spaces);

auto it = std::find_if(spaces.begin(), spaces.end(),
    [&](const VspaceTuple& s) { return s.name == space_name; });

if (it == spaces.end())
    throw TarantoolException("Space not found: " + std::string(space_name));

return it->id;
```

This is self-documenting, type-safe, and handles partial results (e.g., `_vspace` returning multiple spaces) correctly.

### Implementation Steps

**Step 4.1** — Create `tarantool/src/storages/tarantool/impl/vspace_tuple.hpp` with the struct above.

**Step 4.2** — Replace the manual decode in `ResolveSpaceId` with `mpp::decode`.

**Step 4.3** — Do the same for `_vindex` resolution (index ID lookup). Add `VindexTuple`:

```cpp
struct VindexTuple {
    uint32_t space_id;
    uint32_t id;
    std::string name;
    std::string type;
    static constexpr auto mpp = std::make_tuple(
        &VindexTuple::space_id, &VindexTuple::id,
        &VindexTuple::name, &VindexTuple::type);
};
```

### Verification

| Test | Method | Pass Criteria |
|---|---|---|
| Unit: VspaceTuple decode | Encode a known `_vspace` response manually, decode via `mpp::decode` | Correct `id`, `name` |
| Unit: space not found | Pass response with wrong name | Throws `TarantoolException` |
| Functional: space resolution | All 7 functional tests (each creates/uses `kv` space) | Pass |
| Functional: wrong space name | New test: call `Select` on non-existent space | Throws, 404 HTTP response |

---

## Recommendation 5 — Compile-Time Static Frames for PING and Auth Header

### Priority: LOW ✅ Done
### Estimated effort: 2–3 days
### Expected gain: Zero encode cost for PING (currently ~50k of 282k op/s are pure connector overhead)

### Problem

`Connection::PingAsync()` constructs an IPROTO PING frame on every call:

```cpp
// connection.cpp — current PingAsync / encode path
std::vector<uint8_t> frame;
EncodeUint(frame, IPROTO_REQUEST_TYPE, 64);  // PING
EncodeUint(frame, IPROTO_SYNC, sync_id);
// ... header map, body map ...
```

At 282k PING/s, this is 282k × (several `EncodeUint` calls + push_backs) per second. The frame for PING is **always the same except for `sync_id`**. Only the 8-byte `sync_id` value changes between requests.

### tntcxx Insight

tntcxx encodes IPROTO frames using `mpp::as_map` with compile-time key-value pairs. For static frames, the compiler can generate the full byte sequence at compile time. The only mutable field (`sync_id`) can be patched in place at a known byte offset.

### Implementation Steps

**Step 5.1** — Analyze the PING frame byte layout:

```
IPROTO PING frame (with sync_id = X):
  [CE 00 00 00 0D]                      -- fixext-like prehdr, body_len = 13
  [82]                                  -- fixmap with 2 keys
  [00] [40]                             -- key=0x00 (IPROTO_TYPE), val=0x40 (PING=64)
  [01] [CE XX XX XX XX]                 -- key=0x01 (IPROTO_SYNC), val=uint32(sync_id)
  [80]                                  -- empty body map
```

The frame is 14 bytes total. Only bytes 9–12 (the `sync_id` value) vary.

**Step 5.2** — Define a static frame template as `constexpr std::array<uint8_t, 14>`:

```cpp
// iproto_frames.hpp (new, internal)
namespace iproto {

// PING frame template — sync_id bytes at offset 9
constexpr std::array<uint8_t, 14> kPingFrameTemplate = {
    0xCE, 0x00, 0x00, 0x00, 0x0D,  // prehdr: body_len=13
    0x82,                            // fixmap 2 keys
    0x00, 0x40,                      // type=PING(64)
    0x01, 0xCE, 0x00, 0x00, 0x00, 0x00,  // sync=uint32(0) — placeholder
    0x80                             // empty body
};
constexpr size_t kPingSyncIdOffset = 9;  // byte offset of sync_id in the frame

inline void BuildPingFrame(uint8_t* out, uint32_t sync_id) {
    std::copy(kPingFrameTemplate.begin(), kPingFrameTemplate.end(), out);
    // Patch sync_id in big-endian at offset 9
    out[kPingSyncIdOffset + 0] = (sync_id >> 24) & 0xFF;
    out[kPingSyncIdOffset + 1] = (sync_id >> 16) & 0xFF;
    out[kPingSyncIdOffset + 2] = (sync_id >>  8) & 0xFF;
    out[kPingSyncIdOffset + 3] = (sync_id      ) & 0xFF;
}

} // namespace iproto
```

**Step 5.3** — Replace the dynamic encode in `PingAsync`:

```cpp
// connection.cpp — PingAsync, before:
std::vector<uint8_t> frame;
EncodeIprotoRequest(frame, IPROTO_PING, sync_id, /*body=*/EmptyMap{});

// After:
std::array<uint8_t, 14> frame;
iproto::BuildPingFrame(frame.data(), sync_id);
// frame is stack-allocated, no heap involved
```

**Step 5.4** — Apply the same pattern to the IPROTO greeting ACK sent during authentication handshake (also a fixed-layout frame with few variable fields).

**Step 5.5** — Add a unit test that validates the template bytes against live Tarantool wire capture (included in `msgpack_xcheck_test.cpp`).

### Verification

| Test | Method | Pass Criteria |
|---|---|---|
| Unit: PING frame bytes | Cross-check `BuildPingFrame` output vs tntcxx-encoded PING | Byte-identical |
| Unit: multiple sync_ids | Build frames for `sync_id` = 0, 1, 0xFFFFFFFF | Correct BE encoding |
| Perf: zero alloc on PING | `valgrind` on 1k `PingAsync` calls | 0 heap allocations |
| Benchmark: PING throughput | `TarantoolBench` PING at coro=128 | ≥ 282k op/s (no regression) |
| Regression | Full functional test suite | 7/7 pass |

---

## Summary and Rollout Order

### Recommended implementation order

```
Week 1:  Rec 2 (AppendTo)          — small, safe, clear win, no API break
Week 2:  Rec 4 (mpp for _vspace)   — small, increases correctness
Week 3–4: Rec 1 (tnt::Buffer)      — largest structural change, needs care
Week 5–6: Rec 3 (mpp API addition) — new public API, needs design review
Week 7:  Rec 5 (static frames)     — low risk, can be done in parallel
```

### Projected cumulative improvement

| Stage | REPLACE pipeline coro=128 | PING coro=128 |
|---|---|---|
| Current (PR as-is) | 237k op/s | 282k op/s |
| + Rec 2 (no copy) | ~245k op/s (+3%) | ~290k op/s (+3%) |
| + Rec 1 (tnt::Buffer) | ~270k op/s (+14%) | ~320k op/s (+13%) |
| + Rec 3 (mpp API) | ~285k op/s (+7%)* | ~340k op/s (+6%)* |
| + Rec 5 (static PING) | — | ~360k op/s (+6%) |

*Rec 3 benefit depends on what fraction of callers migrate to typed API. Estimates assume 50% migration.

### What NOT to integrate from tntcxx

These tntcxx components are **not suitable** for integration into the userver connector and should remain in `third_party/` for testing only:

| Component | Reason |
|---|---|
| `EpollNetProvider` / `LibevNetProvider` | Incompatible with userver coroutine scheduler |
| `Connector` / `Connection` classes | Single-threaded, blocking `wait()` semantics |
| `Connector::wait()` | Incompatible with `engine::Future` / userver task model |

---

## Testing Infrastructure

All recommendations should be verified using the existing test harness:

- **Unit tests:** `tarantool/tests/msgpack_test.cpp`, `value_test.cpp`, `msgpack_xcheck_test.cpp`
- **Functional tests:** `tarantool/functional_tests/basic/tests/test_basic.py` (7 tests, pytest)
- **CPU benchmarks:** `TarantoolCpuBench.EncodeDecodeComparison` (no server needed)
- **Network benchmarks:** `TarantoolBench.InsertThroughput` + `bench_compare.sh` (requires Tarantool on loopback)
- **Profiling:** `perf record -g -- ./bench`, then `perf report` to confirm hot paths are eliminated

For each recommendation, a PR should include:
1. Implementation commit(s)
2. New or updated unit tests covering the changed code paths
3. Before/after benchmark numbers in the PR description
4. No regression in the 7 functional tests

---

*Document generated from code review of [tsafin/userver#1](https://github.com/tsafin/userver/pull/1) and [tarantool/tntcxx](https://github.com/tarantool/tntcxx)*
