# `formats::msgpack` — Design Document

## Motivation

The Tarantool connector currently uses `formats::json::Value` as the in-process
data format for both query arguments and results.  Every call goes through a
double conversion:

```
user JSON  →  EncodeJson() → msgpack bytes → Tarantool
Tarantool  →  msgpack bytes → MpDecoder() → json::Value → user
```

Measured overhead: **~7–8 µs per operation** (4 µs encode + 3–4 µs decode),
which is significant compared to the 29 µs loopback TCP RTT.

Goal: define a `formats::msgpack::Value` / `ValueBuilder` API that mirrors the
`formats::json` surface so the connector (and callers) can work natively in
msgpack, eliminating all JSON conversion.

---

## Scope

### In scope
- `formats::msgpack::Value`   — zero-copy read view over encoded bytes
- `formats::msgpack::ValueBuilder` — accumulate values, encode to bytes
- Integration with `formats::parse` / `formats::serialize` ADL framework
- Tarantool extension types (UUID, Datetime) as first-class value kinds
- Replace `formats::json::Value` in `Query` and `ExecutionResult`
- Constexpr encoding functions (low-level, usable at compile time for static structures)

### Out of scope
- Full userver `formats` framework registration (iterators, path tracking) in v1
- BSON / CBOR compatibility
- Compile-time `ValueBuilder` (blocked by dynamic data)

---

## API Design

Located in `universal/include/userver/formats/msgpack/`.

### 1. `Value` — read-only, zero-copy

```cpp
namespace formats::msgpack {

class Value {
 public:
  /// Construct a "missing" value (default).
  Value() noexcept = default;

  /// Wrap raw msgpack bytes (zero-copy; caller owns the buffer).
  static Value FromBytes(const uint8_t* data, std::size_t len) noexcept;
  static Value FromBytes(std::span<const uint8_t> buf) noexcept;

  // ---- Type predicates ----
  bool IsMissing()  const noexcept;   // default-constructed / key not found
  bool IsNull()     const noexcept;   // msgpack nil (0xc0)
  bool IsBool()     const noexcept;
  bool IsInt64()    const noexcept;   // signed integer (any width)
  bool IsUInt64()   const noexcept;   // unsigned integer (any width)
  bool IsDouble()   const noexcept;
  bool IsString()   const noexcept;
  bool IsArray()    const noexcept;
  bool IsObject()   const noexcept;   // msgpack map
  bool IsExt()      const noexcept;   // msgpack extension type

  // Tarantool-specific ext kinds (IsExt() must be true)
  bool IsUuid()     const noexcept;   // ext type 2
  bool IsDatetime() const noexcept;   // ext type 4

  // ---- Map access ----
  // IPROTO uses integer keys; user data uses string keys — both supported.
  Value operator[](std::string_view key) const;
  Value operator[](uint64_t key)         const;  // IPROTO maps: {0x00: code, 0x01: sync, ...}
  Value operator[](std::size_t index)    const;  // array element

  bool HasMember(std::string_view key) const;
  bool HasMember(uint64_t key)         const;

  std::size_t GetSize()  const;   // array/map element count
  bool        IsEmpty()  const;

  // ---- Typed extraction ----
  template <typename T>
  T As() const;                          // throws on type mismatch

  template <typename T>
  T As(T default_val) const noexcept;   // returns default on mismatch/missing

  // ---- Iteration (array or map) ----
  class const_iterator;
  const_iterator begin() const;
  const_iterator end()   const;

  // ---- Extension type raw access ----
  int8_t                   GetExtType() const;
  std::span<const uint8_t> GetExtData() const;

  // ---- Convenience for Tarantool ext types ----
  TntUuid     AsUuid()     const;
  TntDatetime AsDatetime() const;

  // ---- Path for error messages ----
  std::string GetPath() const;

  using Builder = ValueBuilder;
};

}  // namespace formats::msgpack
```

**Key difference from `formats::json::Value`**: `operator[](uint64_t)` for
integer-keyed maps.  IPROTO headers and bodies exclusively use integer keys
(`kKeyCode = 0x00`, `kKeySync = 0x01`, `kKeyData = 0x30`, etc.).  The current
code works around this by converting them to strings (`body_map["48"]`) — a bug
vector and a performance cost.

**Storage**: `Value` is a non-owning cursor:

```cpp
class Value {
  const uint8_t* pos_{nullptr};   // start of this value in the buffer
  const uint8_t* end_{nullptr};   // end of the owning buffer
  // optional: depth / path string for error messages
};
```

`begin()` / `end()` return a cursor iterator that advances by calling the
skip-logic on each element.  No allocations during read.

---

### 2. `ValueBuilder` — mutable, variant tree, encodes on demand

```cpp
namespace formats::msgpack {

class ValueBuilder {
 public:
  // ---- Constructors ----
  ValueBuilder();                              // nil
  explicit ValueBuilder(bool v);
  explicit ValueBuilder(int64_t v);
  explicit ValueBuilder(uint64_t v);
  explicit ValueBuilder(double v);
  explicit ValueBuilder(std::string_view v);
  explicit ValueBuilder(const Value& v);       // copy from read value
  explicit ValueBuilder(TntUuid uuid);
  explicit ValueBuilder(TntDatetime dt);

  // ADL Serialize hook (same pattern as json::ValueBuilder)
  template <typename T>
  explicit ValueBuilder(T&& t);               // calls Serialize(t, To<ValueBuilder>{})

  // ---- Named factories ----
  static ValueBuilder Array();
  static ValueBuilder Object();               // empty map (string keys)
  static ValueBuilder IntKeyObject();         // empty map (integer keys, for IPROTO)
  static ValueBuilder Null();

  // ---- Modification — string-keyed maps ----
  ValueBuilder& operator[](std::string_view key);   // create/access member
  bool HasMember(std::string_view key) const;
  void Remove(std::string_view key);

  // ---- Modification — integer-keyed maps (IPROTO) ----
  ValueBuilder& operator[](uint64_t key);

  // ---- Modification — arrays ----
  void PushBack(ValueBuilder value);

  // ---- Query ----
  bool        IsNull()   const noexcept;
  bool        IsArray()  const noexcept;
  bool        IsObject() const noexcept;
  std::size_t GetSize()  const;
  bool        IsEmpty()  const;

  // ---- Extraction ----
  /// Encode to msgpack bytes and return an owning Value.
  Value ExtractValue();

  /// Encode to raw bytes without wrapping in Value.
  std::vector<uint8_t> ToBytes() const;
};

}  // namespace formats::msgpack
```

**Internal storage** — variant node tree (not a flat byte buffer, because msgpack
maps / arrays require the element count *before* the elements):

```cpp
struct Node {
  using Array  = std::vector<Node>;
  using StrMap = std::vector<std::pair<std::string, Node>>;
  using IntMap = std::vector<std::pair<uint64_t,    Node>>;

  std::variant<
    std::monostate,   // nil
    bool,
    int64_t,
    uint64_t,
    double,
    std::string,
    Array,
    StrMap,
    IntMap,
    TntUuid,
    TntDatetime
  > data;
};
```

`ToBytes()` performs a single recursive traversal, writing each node with the
correct msgpack format byte(s).  No intermediate JSON step.

---

### 3. Serialization helpers

```cpp
namespace formats::msgpack {

/// Encode a Value to a new byte vector.
std::vector<uint8_t> ToBytes(const Value& v);

/// Wrap raw bytes as a zero-copy Value (no parsing; lazy).
Value FromBytes(std::span<const uint8_t> buf);
Value FromBytes(const std::vector<uint8_t>& buf);

/// Interop: convert from/to JSON (for migration period).
Value                FromJson(const formats::json::Value& v);
formats::json::Value ToJson(const Value& v);

}  // namespace formats::msgpack
```

---

### 4. Low-level constexpr encoding functions

The existing `EncodeUint`, `EncodeStr`, etc. in `msgpack.hpp` are `inline`.
Mark them `constexpr` so they can be used in `consteval` contexts:

```cpp
// All can be constexpr if the output buffer supports constexpr push_back
// (e.g., std::array with a size counter, or std::vector in C++20).
constexpr void EncodeUint(auto& out, uint64_t v);
constexpr void EncodeStr(auto& out, std::string_view s);
constexpr void EncodeArray(auto& out, uint32_t count);
constexpr void EncodeFixMap(auto& out, uint8_t count);
```

This allows compile-time encoding of fixed IPROTO frames (e.g., PING header):

```cpp
// Compile-time IPROTO PING header (sync filled in at runtime separately)
consteval auto MakePingHeaderTemplate() {
    std::array<uint8_t, 9> buf{};
    // encode {0x00: 64, 0x01: 0} as a placeholder
    ...
    return buf;
}
```

---

## Integration with `formats` ADL framework

Add specializations in `universal/` so the `formats::parse` / `formats::serialize`
framework works with `formats::msgpack::Value`:

```cpp
namespace formats::parse {
// Existing: int64_t Parse(const json::Value&, To<int64_t>)
// New:
int64_t Parse(const msgpack::Value&, To<int64_t>);
// etc. for all primitive types
}

namespace formats::serialize {
// Existing: json::Value Serialize(int64_t, To<json::ValueBuilder>)
// New:
msgpack::ValueBuilder Serialize(int64_t, To<msgpack::ValueBuilder>);
// etc.
}
```

User types that already implement `Parse`/`Serialize` for `json::Value` will
need corresponding overloads for `msgpack::Value`.  The API is identical — only
the `Value` type differs.

---

## Connector migration plan

### Phase 1 — Internal only (zero public API change) ✅ IMPLEMENTED

Replace `MpDecoder` header decode in `connection.cpp` with `formats::msgpack::Value`
zero-copy cursor.  Fix integer-key access (was broken: keys stored as strings).

```cpp
// Before (broken: integer keys stored as strings by MpDecoder::DecodeMap):
const auto code    = header["0"].As<int64_t>(0);
const auto sync_id = header["1"].As<uint64_t>(0);

// After (correct, zero-copy):
auto header = formats::msgpack::Value::FromBytes(body_data, body_len);
const auto code    = header[kKeyCode].As<int64_t>(0);   // kKeyCode = 0x00
const auto sync_id = header[kKeySync].As<uint64_t>(0);  // kKeySync = 0x01
auto body_view     = header.NextSibling();               // skip past header in same buffer
```

**Result**: PING +97%, REPLACE +39% (see Performance Results section).

### Phase 2 — `ExecutionResult::GetData()` returns `msgpack::Value` ✅ IMPLEMENTED

Replace `MpDecoder::DecodeValue()` in the hot path with a byte copy into an
owned buffer in `ExecutionResult`, exposing a zero-copy cursor to callers.

```cpp
// Before (hot path in ReaderLoop, per request):
MpDecoder data_dec{data_cursor.GetRawPos(), data_cursor.GetRawEnd()};
data_val = data_dec.DecodeValue();   // allocates json::Value tree
ExecutionResult result{..., std::move(data_val)};

// After (Phase 2):
const auto data_cursor = body_view[kKeyData];
const auto next = data_cursor.NextSibling();
const uint8_t* data_end = next.IsMissing() ? data_cursor.GetRawEnd() : next.GetRawPos();
data_buf.assign(data_cursor.GetRawPos(), data_end);   // one memcpy, tight bounds
ExecutionResult result{..., std::move(data_buf)};    // result owns the bytes
```

`ExecutionResult` stores `std::vector<uint8_t> data_buf_` + a
`formats::msgpack::Value data_` cursor into it.  `GetData()` returns
`const formats::msgpack::Value&`.  Callers using `IsArray()`, `GetSize()`,
`operator[]`, `As<T>()` require **no changes** — the API is identical.

**Result**: REPLACE +16% on top of Phase 1 (total +63% vs baseline).

### Phase 3 — Query encode side (planned)

Replace `EncodeJson()` (JSON→msgpack conversion) in `connection.cpp` with
`formats::msgpack::ValueBuilder` built directly from the query arguments.  This
requires changing the `Query` public API:

```cpp
// Current (Query stores json::Value args):
Query Query::Replace(std::string space, formats::json::Value tuple);

// Phase 3 target:
Query Query::Replace(std::string space, formats::msgpack::ValueBuilder args);

// Deprecated bridge for migration:
Query Query::Replace(std::string space, formats::json::Value tuple) {
    return Replace(std::move(space), formats::msgpack::FromJson(tuple));
}
```

Estimated saving: ~2 µs/op (eliminates remaining `EncodeJson` overhead).

---

## Performance Results (Measured)

### Benchmark setup

- Hardware: WSL2, Intel Core i7, 4 coro-runner threads, pool=4 connections
- Tarantool: loopback TCP, raw PING RTT **~29 µs**
- Workload: REPLACE on a simple `{id, value}` space; PING for header-only baseline
- Tool: `TarantoolBench.InsertThroughput` in `userver-tarantool_tttest`

All numbers below are **pipeline coro=128** (128 concurrent coroutines sharing 4
connections), which maximises pipelining and is most sensitive to decode overhead.

### Stage results

| Stage | PING op/s | PING µs/op | REPLACE op/s | REPLACE µs/op | Notes |
|-------|-----------|------------|--------------|----------------|-------|
| Baseline (json::Value throughout) | 69 k | 14 µs | 38 k | 26 µs | EncodeJson + MpDecoder |
| Phase 1 — zero-copy header decode | 136 k | **7 µs** | 53 k | 18 µs | msgpack::Value header; integer keys |
| Phase 2 — zero-copy data decode   | 136 k | 7 µs | 62 k | **16 µs** | ExecutionResult stores raw bytes |

Phase 1 doubled PING throughput (+97%) because PING responses carry **no body** —
the entire old hot path was `MpDecoder` allocating a JSON node tree for a 7-byte
header map, all of which is now eliminated.

Phase 2 added +16% to REPLACE on top of Phase 1 (+63% total vs baseline), by
replacing `MpDecoder::DecodeValue()` for the data tuple with a `memcpy` of the
raw bytes followed by a zero-copy `msgpack::Value` cursor in `ExecutionResult`.

### Profiling analysis (perf record, WSL2 cpu-clock event)

After Phase 1 the profiler (`perf record --no-buildid -e cpu-clock:u -g -F 99`)
showed the following breakdown of `Connection::ReaderLoop()` CPU time:

| Hot spot | Share of ReaderLoop | Root cause |
|----------|---------------------|------------|
| `MpDecoder::DecodeValue()` | **77.5%** | `json::ValueBuilder` / `json::Value` destruction — atomic `shared_ptr::_M_release()` per node |
| `engine::Promise<ExecutionResult>::~Promise()` | 17.5% | `shared_ptr<FutureState>` refcount — intrinsic to coroutine scheduling |
| Everything else | 5% | memcpy, string ops, I/O epoll |

ReaderLoop itself accounted for ~47% of total user-space CPU; the remainder was
network I/O wait and coroutine scheduling. This is why the wall-clock gain from
Phase 2 (+16%) is smaller than the profiler fraction (77.5%) suggests — the
benchmark is I/O-bound at this concurrency level.

### Remaining bottleneck

After Phase 2 the per-op breakdown for REPLACE at coro=128 is approximately:

| Component | µs/op |
|-----------|-------|
| Tarantool server processing + loopback RTT | ~10 µs |
| userver coroutine scheduling (FutureState shared_ptr) | ~3 µs |
| IPROTO frame encode (`EncodeJson` → still JSON for query args) | ~2 µs |
| `data_buf.assign()` memcpy (Phase 2 overhead) | < 0.5 µs |
| **Total** | **~16 µs** |

The remaining Phase 3 opportunity is replacing `EncodeJson()` for query
arguments (the encode side of the Query API), which would save another ~2 µs.

---

## Performance estimate (original pre-implementation prediction)

| Step                  | Predicted cost | After Phase 1 (actual) | After Phase 2 (actual) |
|-----------------------|---------------|------------------------|------------------------|
| JSON→msgpack encode   | 4 µs          | unchanged              | ~2 µs (still EncodeJson) |
| msgpack→JSON decode   | 3–4 µs        | eliminated ✓           | eliminated ✓             |
| Net per-op saving     | —             | ~8 µs (PING), ~8 µs (REPLACE) | +2 µs more (REPLACE) |
| Connector overhead    | 158 µs        | ~150 µs (REPLACE)      | ~148 µs (REPLACE)      |

The prediction of ~7 µs total saving was roughly accurate; the actual end-to-end
improvement was ~10 µs/op for REPLACE (baseline 26 µs → Phase 2 16 µs).  The
bigger win was the +97% PING improvement, which was not predicted because PING
responses were not benchmarked in the original estimate.

---

## File layout

```
universal/
  include/userver/formats/msgpack/
    value.hpp           # Value (read cursor) + NextSibling() ✅ implemented
    value_builder.hpp   # ValueBuilder (variant tree builder) ✅ implemented
    serialize.hpp       # ToBytes / FromBytes ✅ implemented
    exception.hpp       # TypeMismatchException, OutOfBoundsException ✅ implemented
  src/formats/msgpack/
    value.cpp           # ✅ implemented (Skip, operator[], As<T> specialisations)
    value_builder.cpp   # ✅ implemented (EncodeNode recursive encoder)
    serialize.cpp       # ✅ implemented

tarantool/
  src/storages/tarantool/impl/
    msgpack.hpp         # keep: low-level EncodeXxx + MpDecoder for query encode side
    connection.cpp      # ✅ Phase 1+2: zero-copy header + data decode
  include/userver/storages/tarantool/
    query.hpp           # Phase 3: replace json::Value args → msgpack::ValueBuilder
    result.hpp          # ✅ Phase 2: GetData() now returns formats::msgpack::Value
```

---

## Open questions / decisions resolved and remaining

### Resolved

1. **Namespace**: ✅ `formats::msgpack` — placed in `universal/`, making it
   reusable by Redis and other connectors.

2. **Integer-keyed map**: ✅ Separate `IntKeyObject()` factory used for IPROTO
   encoding; regular `Object()` for string keys.  `operator[](size_t)` on `Value`
   does dual-dispatch at runtime (array index OR integer map lookup depending on
   the actual type).

3. **`Value` ownership in `ExecutionResult`**: ✅ `ExecutionResult` owns a
   `std::vector<uint8_t> data_buf_` and holds a zero-copy `msgpack::Value` cursor
   into it.  No `shared_ptr` per response — one allocation per result (the memcpy
   of the data bytes), no per-element refcounting.

4. **Phase 2 timing**: ✅ Implemented as part of the same PR.  The public API
   change (`GetData()` return type) is source-compatible for existing callers
   because `formats::msgpack::Value` exposes the same navigation API
   (`IsArray()`, `GetSize()`, `operator[]`, `As<T>()`).

### Still open

5. **`formats::parse` / `formats::serialize` framework**: full ADL integration
   would allow user-defined types to round-trip through msgpack without manual
   `As<T>()` calls.  Can be implemented in Phase 3 alongside the Query encode
   migration.

6. **Phase 3 timing**: The Query encode side (replacing `EncodeJson()`) is a
   breaking public API change (`Query::Replace()` etc. accept `json::Value` today).
   Should be a separate PR with deprecated JSON overloads as bridges.
