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

### Phase 1 — Internal only (zero public API change)

Replace `EncodeJson()` calls in `connection.cpp` with `ValueBuilder`:

```cpp
// Before:
EncodeFixMap(body, 2);
EncodeUint(body, kKeySpaceId); EncodeUint(body, space_id);
EncodeUint(body, kKeyTuple);   EncodeJson(body, query.GetArgs());  // JSON→msgpack

// After:
auto b = ValueBuilder::IntKeyObject();
b[kKeySpaceId] = ValueBuilder{space_id};
b[kKeyTuple]   = ValueBuilder{query.GetArgs()};   // still from json::Value, via FromJson()
auto body = b.ToBytes();
```

Fix integer-key access in the decoder:

```cpp
// Before (bug: integer keys stored as strings by MpDecoder):
const auto code    = header["0"].As<int64_t>(0);
const auto sync_id = header["1"].As<uint64_t>(0);

// After (correct):
const auto code    = header[kKeyCode].As<int64_t>(0);
const auto sync_id = header[kKeySync].As<uint64_t>(0);
```

### Phase 2 — Public API change

Replace `formats::json::Value` with `formats::msgpack::Value` in:
- `Query`: `GetArgs()`, `GetOps()`, factory methods `Replace()`, `Select()`, etc.
- `ExecutionResult`: `GetData()`

Provide migration helpers to avoid hard breaks:
```cpp
// Deprecated overload bridging the transition:
Query Query::Replace(std::string space, formats::json::Value tuple) {
    return Replace(std::move(space), formats::msgpack::FromJson(tuple));
}
```

---

## Performance estimate

| Step                  | Current cost | After Phase 1 | After Phase 2 |
|-----------------------|-------------|---------------|---------------|
| JSON→msgpack encode   | 4 µs        | ~1 µs (Node tree → bytes) | eliminated |
| msgpack→JSON decode   | 3–4 µs      | eliminated (cursor view)  | eliminated |
| Net per-op saving     | —           | ~3 µs         | ~7 µs         |
| Connector overhead    | 158 µs      | ~155 µs       | ~151 µs       |

The 7 µs saving per op at 70k op/s pipeline ceiling → theoretical +5% throughput.
The more significant win is at **low concurrency** (sequential) where the saving
is proportionally larger against the 250 µs total round-trip.

---

## File layout

```
universal/
  include/userver/formats/msgpack/
    value.hpp           # Value (read cursor) + const_iterator
    value_builder.hpp   # ValueBuilder (variant tree builder)
    serialize.hpp       # ToBytes / FromBytes / FromJson / ToJson
    exception.hpp       # TypeMismatchException, OutOfBoundsException
  src/formats/msgpack/
    value.cpp
    value_builder.cpp
    serialize.cpp

tarantool/
  src/storages/tarantool/impl/
    msgpack.hpp         # keep: low-level EncodeXxx / MpDecoder; mark constexpr
    connection.cpp      # migrate EncodeJson() → ValueBuilder; fix integer key access
  include/userver/storages/tarantool/
    query.hpp           # Phase 2: replace json::Value → msgpack::Value
    result.hpp          # Phase 2: replace json::Value → msgpack::Value
```

---

## Open questions / decisions needed

1. **Namespace**: `formats::msgpack` (generic userver lib, reusable by Redis and
   other connectors) vs `storages::tarantool::msgpack` (tarantool-only)?

2. **Integer-keyed map**: separate `IntKeyObject()` factory, or auto-detect based
   on the type of the first key inserted?

3. **`Value` ownership**: current design is non-owning (caller holds buffer).
   An owning `OwnedValue` wrapper (`shared_ptr<vector<uint8_t>>` + cursor) would
   simplify lifetimes at the cost of one allocation per response.

4. **Phase 2 timing**: separate PR (breaking change to `Query`/`ExecutionResult`)?

5. **`formats::parse` / `formats::serialize` framework**: full integration doubles
   the work but allows type-safe round-trips with user-defined types.  Can be
   deferred to Phase 2.
