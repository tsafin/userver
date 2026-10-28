# Tarantool MessagePack Extension Types — Implementation Plan

**Status: Fully implemented (verified 2026-03-25)**

All five Tarantool ext types are handled. Types live in
`universal/include/userver/formats/msgpack/tarantool_types.hpp` and are
exposed via `Value::As<T>()`, `ValueBuilder(T)`, and ADL hooks.
The phased plan below is preserved for historical reference.

---

## Background

Tarantool defines five MessagePack extension types beyond the standard msgpack
spec. The connector must handle all of them for full compatibility. This
document describes the current coverage gaps, the planned C++ types, the
on-demand (lazy) decode design, and a phased implementation roadmap.

---

## PostgreSQL API Comparison

Before defining Tarantool types, here is what the userver PostgreSQL driver
provides, so the Tarantool API can follow the same conventions.

### PostgreSQL datetime type system (userver)

| PG DB type              | C++ type                                       | Precision   | Timezone |
|-------------------------|------------------------------------------------|-------------|----------|
| `DATE`                  | `utils::datetime::Date` *(universal)*          | day         | —        |
| `TIME` (no tz)          | `utils::datetime::TimeOfDay<Duration>` *(univ)*| template    | no       |
| `TIMETZ` (with tz)      | ❌ not supported in userver                    | —           | —        |
| `TIMESTAMP` (no tz)     | `storages::postgres::TimePointWithoutTz`       | microsecond | no       |
| `TIMESTAMPTZ` (with tz) | `storages::postgres::TimePointTz`              | microsecond | yes      |
| `INTERVAL`              | `std::chrono::duration` (user-facing); internal `io::detail::Interval` | microsecond | — |

Key observations:
- `Date` and `TimeOfDay` live in `universal/` (shared across drivers).
- `TimePointTz` and `TimePointWithoutTz` are **strong typedefs** over the same
  underlying `system_clock::time_point`. The tz information is NOT stored in
  the value; it is a compile-time tag. PG normalises all timestamps to UTC on
  the wire.
- `TIMETZ` is deliberately unsupported ("shouldn't be mixed with timetz type").
- `Interval` internally stores `{months, days, microseconds}` but the
  public user API is `std::chrono::duration` (throws `UnsupportedInterval`
  if months ≠ 0).

### Tarantool datetime type system (ext type 4)

All four Tarantool date/time logical types share a **single wire format**:
ext type 4 (`MP_DATETIME`) carrying 8 or 16 bytes.

| Tarantool logical type | Characteristics                        | PG analogue        |
|------------------------|----------------------------------------|--------------------|
| `date`                 | seconds=midnight UTC, nsec=0, tz=0     | `DATE`             |
| `datetime` (no tz)     | arbitrary seconds, nsec=0, tz fields=0 | `TIMESTAMP`        |
| `datetime` (with tz)   | arbitrary seconds, nsec=0, tz≠0        | `TIMESTAMPTZ`      |
| `timestamp` (with tz)  | arbitrary seconds, **nsec≠0**, tz≠0    | `TIMESTAMPTZ` (ns) |

There is no Tarantool equivalent of `TIME` or `TIMETZ`.

Key differences from PostgreSQL:
1. **Precision**: PG microseconds → Tarantool **nanoseconds**
2. **Timezone storage**: PG normalises to UTC (tz is a DB-type tag); Tarantool
   stores **explicit `tzoffset` + `tzindex`** in every datetime value
3. **Sub-second flag**: Tarantool allows distinguishing "whole second" datetime
   from "fractional" datetime at the value level (via nsec field)

### Proposed Tarantool C++ type mapping

| Accessor            | C++ type returned     | Validates                       | PG analogue                |
|---------------------|-----------------------|---------------------------------|----------------------------|
| `AsDate()`          | `utils::datetime::Date` *(reuse!)* | seconds%86400==0 && nsec==0 && tz==0 | `Date` |
| `AsDatetimeTz()`    | `DatetimeTz`          | nsec==0                         | `TimePointTz` (+ tz fields)|
| `AsDatetimeWithoutTz()` | `DatetimeWithoutTz` | nsec==0 && tz==0              | `TimePointWithoutTz`       |
| `AsTimestampTz()`   | `TimestampTz`         | (any nsec)                      | `TimePointTz` (nanosecs)   |
| `AsTimestampWithoutTz()` | `TimestampWithoutTz` | tz==0                        | `TimePointWithoutTz` (ns)  |

`AsDatetime()` / `AsTimestamp()` without a Tz suffix are **convenience aliases**
that accept either tz or no-tz (i.e., the caller does not care about the tz
distinction, which is a common case). They are equivalent to
`AsTimestampTz()` / `AsTimestampWithoutTz()` relaxed on the tz constraint.

Where "tz==0" means `tzoffset == 0 && tzindex == 0`.

### `DatetimeTz` and `TimestampTz` structs

Unlike PG (where tz is a tag, not stored in the value), Tarantool's datetime
carries explicit tz fields. The structs must therefore hold them:

```cpp
// Analogous to TimePointTz but with nanoseconds + explicit tz fields.
struct DatetimeTz {
    std::chrono::time_point<std::chrono::system_clock,
                             std::chrono::seconds>     tp;      // UTC
    int16_t  tzoffset = 0;   // UTC offset in minutes
    uint16_t tzindex  = 0;   // Olson tz DB index (0 = not set)

    bool operator==(const DatetimeTz&) const noexcept;
};

// Analogue for fractional-second precision.
struct TimestampTz {
    std::chrono::time_point<std::chrono::system_clock,
                             std::chrono::nanoseconds>  tp;      // UTC
    int16_t  tzoffset = 0;
    uint16_t tzindex  = 0;

    bool operator==(const TimestampTz&) const noexcept;
};

// "Without tz" variants: strong typedefs (no tz fields needed).
using DatetimeWithoutTz = utils::StrongTypedef<
    struct DatetimeWithoutTzTag,
    std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>>;

using TimestampWithoutTz = utils::StrongTypedef<
    struct TimestampWithoutTzTag,
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>>;
```

`AsDatetimeTz()` throws `ConversionException` if `nsec != 0`.
`AsDatetimeWithoutTz()` throws `ConversionException` if `nsec != 0 || tz != 0`.
`AsDate()` throws `ConversionException` if `seconds % 86400 != 0 || nsec != 0 || tz != 0`.

---

| Ext | Name            | Wire format      | Current status                       |
|-----|-----------------|------------------|--------------------------------------|
|  1  | `MP_DECIMAL`    | `ext8/16/32`     | ✅ `Value::AsDecimalString()`, `As<std::string>` |
|  2  | `MP_UUID`       | `fixext16` (18B) | ✅ `TntUuid` in `tarantool_types.hpp`, `As<TntUuid>()`, `ValueBuilder(TntUuid)` |
|  3  | `MP_ERROR`      | `ext8/16/32` map | ✅ `TntErrorInfo`/`TntErrorFrame` in `error_info.hpp`, wired into `CommandException` |
|  4  | `MP_DATETIME`   | `fixext8/16`     | ✅ `DatetimeTz`, `DatetimeWithoutTz`, `TimestampTz`, `TimestampWithoutTz` in `tarantool_types.hpp` |
|  6  | `MP_INTERVAL`   | `ext8/16/32`     | ✅ `TntInterval` in `tarantool_types.hpp`, `As<TntInterval>()`, `ValueBuilder(TntInterval)` |

All types moved to `universal/include/userver/formats/msgpack/tarantool_types.hpp`
and exposed via the public `Value`/`ValueBuilder` API with ADL hooks in
`serialize_tarantool.hpp`.

---

## On-Demand (Lazy) Decoding

`formats::msgpack::Value` is a zero-copy cursor `{pos_, end_}` over a raw byte
buffer. Navigating to a value (e.g. `result[0][2]`) never allocates and never
parses the contents — it only advances pointers. Typed extraction happens only
on explicit call (`As<T>()`, `AsUuid()`, etc.).

For ext types the raw bytes are already accessible via `GetExtData()`. Decoding
the BCD nibbles for DECIMAL, or unpacking the interval varint fields, only runs
when the user calls `AsDecimal()` or `AsInterval()`.  If the field is never
accessed, the decode cost is zero.

```
Connection receives response bytes
        │
        ▼
formats::msgpack::Value cursor created  ← zero cost: just {pos, end}
        │
    operator[](kKeyData)               ← zero cost: pointer scan
        │
    operator[](0)                      ← zero cost: pointer scan (first tuple)
        │
    operator[](2)                      ← zero cost: pointer scan (third field)
        │
    .AsDecimal<4>()                    ← decode BCD bytes once, here
```

Contrast with the old `MpDecoder` path: every field was decoded eagerly into
`json::Value` nodes, including all tuple elements not accessed by the caller.

---

## New C++ Types

### 1. `TntUuid` — move to `universal/`

Already defined in `tarantool/src/…/impl/msgpack.hpp`. Move to
`universal/include/userver/formats/msgpack/tarantool_types.hpp`.

```cpp
struct TntUuid {
    std::array<uint8_t, 16> bytes{};  // big-endian RFC 4122

    std::string ToString() const;                      // "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    static TntUuid FromString(std::string_view s);     // parses the canonical form
    bool operator==(const TntUuid&) const noexcept;
};
```

No change to the struct layout. Only relocation + public header exposure.

### 2. Date/Datetime/Timestamp — four C++ types from one wire format

All four logical types decode from ext type 4 (`MP_DATETIME`). The accessor
choice determines which semantic is expected and what validation runs.

#### `utils::datetime::Date` — reuse existing universal type

`AsDate()` converts ext-4 bytes to the existing `utils::datetime::Date`
from `universal/include/userver/utils/datetime/date.hpp`. No new struct needed.

Validation: `seconds % 86400 == 0 && nsec == 0 && tzoffset == 0 && tzindex == 0`.

#### `DatetimeTz` and `DatetimeWithoutTz` — second-precision (no nsec)

```cpp
// Analogous to storages::postgres::TimePointTz but with explicit tz fields.
// Stored in universal/include/userver/formats/msgpack/tarantool_types.hpp
struct DatetimeTz {
    std::chrono::time_point<std::chrono::system_clock,
                             std::chrono::seconds>  tp;
    int16_t  tzoffset = 0;   // UTC offset in minutes
    uint16_t tzindex  = 0;   // Olson tz DB index (0 = not set)

    bool operator==(const DatetimeTz&) const noexcept;
};

// Analogous to storages::postgres::TimePointWithoutTz.
using DatetimeWithoutTz = utils::StrongTypedef<
    struct DatetimeWithoutTzTag,
    std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>>;
```

`AsDatetimeTz()` validates `nsec == 0` (throws `ConversionException` otherwise).
`AsDatetimeWithoutTz()` validates `nsec == 0 && tzoffset == 0 && tzindex == 0`.

#### `TimestampTz` and `TimestampWithoutTz` — nanosecond precision

```cpp
// Nanosecond-precision timestamp with explicit tz fields.
struct TimestampTz {
    std::chrono::time_point<std::chrono::system_clock,
                             std::chrono::nanoseconds> tp;
    int16_t  tzoffset = 0;
    uint16_t tzindex  = 0;

    bool operator==(const TimestampTz&) const noexcept;
};

// Without-tz variant: just the time_point (validates tz==0 on decode).
using TimestampWithoutTz = utils::StrongTypedef<
    struct TimestampWithoutTzTag,
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>>;
```

`AsTimestampTz()` accepts any nsec value.
`AsTimestampWithoutTz()` validates `tzoffset == 0 && tzindex == 0`.

#### Migration of the old `TntDatetime`

The existing `TntDatetime` in `msgpack.hpp` is a monolithic struct covering all
four logical types. It is replaced by the four types above and removed. The
encode functions are split:
- `EncodeDatetimeTz(DatetimeTz)` / `EncodeDatetimeWithoutTz(DatetimeWithoutTz)`
- `EncodeTimestampTz(TimestampTz)` / `EncodeTimestampWithoutTz(TimestampWithoutTz)`

All four encode to the same ext-4 wire format (fixext8 when extra fields are
zero, fixext16 otherwise). The decode path is shared in `DecodeExt4Bytes()`
returning a raw `{seconds, nsec, tzoffset, tzindex}` tuple; each accessor then
validates and constructs the appropriate C++ type.

### 3. `TntDecimal` — new type

Tarantool DECIMAL is a variable-precision BCD number. The wire payload is:

```
| scale (MP_INT/MP_UINT) | BCD bytes (packed decimal digits) |
```

where `scale` = number of fractional digits and the BCD bytes use 4-bit nibbles,
last nibble being the sign (`0x0c` = plus, `0x0d` = minus).

**Approach**: decode into `decimal64::Decimal<Prec, RoundPolicy>` from the
existing `universal/` library (used by the PostgreSQL driver for `NUMERIC`).
Because `decimal64::Decimal<Prec>` has a *compile-time* precision, use a
`FromBiased(int64_t value, int fractional_digit_count)` construction (same
pattern as the PostgreSQL numeric decoder):

```cpp
// Example: decode to Decimal<6> (microsecond precision, like PG)
auto d = cursor.AsDecimal<decimal64::Decimal<6>>();
```

For dynamic/unknown precision, provide a `std::string` fallback:

```cpp
std::string cursor.AsDecimalString();   // always available, no precision loss
```

**Wire decode algorithm** (BCD → int64):

```
1. Read scale (varint) from start of ext data
2. Iterate BCD bytes:
   - high_nibble = byte >> 4   → digit
   - low_nibble  = byte & 0x0f → digit or sign (last byte)
3. Assemble integer part + fractional part
4. Apply sign from final nibble
5. Construct Decimal<Prec>::FromBiased(value, scale)
```

### 4. `TntInterval` — new type

Tarantool INTERVAL is a packed sequence of (field_id, value) pairs, where
missing fields are omitted. Compare to PostgreSQL `detail::Interval` which has
three fixed fields (months, days, microseconds). The Tarantool version is richer:

```cpp
struct TntInterval {
    int64_t year       = 0;
    int64_t month      = 0;
    int64_t week       = 0;
    int64_t day        = 0;
    int64_t hour       = 0;
    int64_t minute     = 0;
    int64_t second     = 0;
    int64_t nanosecond = 0;
    int64_t adjust     = 0;   // DT_EXCESS=0, DT_LIMIT=1, DT_SNAP=2

    /// Convert to std::chrono::nanoseconds (ignores year/month/week/adjust)
    std::chrono::nanoseconds ToNanoseconds() const;

    /// Convert from chrono duration (sets only day/hour/minute/second/nanosecond)
    static TntInterval FromNanoseconds(std::chrono::nanoseconds ns);

    bool operator==(const TntInterval&) const noexcept;
};
```

Similarity to `storages::postgres::io::detail::Interval`:

| PG Interval     | Tnt TntInterval      | Notes                              |
|-----------------|----------------------|------------------------------------|
| `months`        | `year * 12 + month`  | PG combines; Tnt keeps separate    |
| `days`          | `week * 7 + day`     | PG combines; Tnt keeps separate    |
| `microseconds`  | `hour/minute/second/nanosecond` | Tnt splits by unit      |
| —               | `adjust`             | Overflow handling (no PG equiv)    |

`GetDuration()` analogy: `ToNanoseconds()` converts calendar-free fields to a
`std::chrono::nanoseconds` value, and throws `TarantoolException` if
year/month/week are non-zero (same policy as PG's `UnsupportedInterval`).

**Wire decode algorithm** (packed map):

```
1. Read count of non-null fields (MP_INT varint)
2. For count iterations:
   a. Read field_id (MP_INT varint, 0..8)
   b. Read value (MP_INT or MP_UINT varint)
   c. Assign to the matching TntInterval field
3. Fields not present in the packed map remain 0
```

### 5. `TntError` — new type (structured error)

Since Tarantool 2.4.1, error responses carry a structured `MP_ERROR` map
at IPROTO key `0x52` alongside the legacy `IPROTO_ERROR_24` string at `0x31`.

```cpp
struct TntErrorInfo {
    std::string type;       // MP_ERROR_TYPE (0x00)  e.g. "ClientError"
    std::string file;       // MP_ERROR_FILE (0x01)
    uint32_t    line = 0;   // MP_ERROR_LINE (0x02)
    std::string message;    // MP_ERROR_MESSAGE (0x03)
    uint32_t    err_no = 0; // MP_ERROR_ERRNO (0x04)
    uint32_t    code = 0;   // MP_ERROR_ERRCODE (0x05)
    // MP_ERROR_FIELDS (0x06) map: arbitrary extra KV pairs
};
```

The `CommandException` thrown by `ExecutionResult::AssertOk()` should be
enriched with the structured error info when available.

---

## API Changes to `formats::msgpack::Value`

Add to `value.hpp`:

```cpp
// ---- Tarantool ext type predicates (IsExt() must be true) ----
bool IsUuid()          const noexcept;   // ext type 2
bool IsDatetime()      const noexcept;   // ext type 4 (date, datetime, or timestamp)
bool IsDecimal()       const noexcept;   // ext type 1
bool IsInterval()      const noexcept;   // ext type 6
bool IsError()         const noexcept;   // ext type 3

// ---- Date/time accessors for ext type 4 ----
//
// All five decode the same wire bytes but validate different constraints:
//
//   AsDate()               seconds%86400==0, nsec==0, tz==0
//   AsDatetimeWithoutTz()  nsec==0, tz==0
//   AsDatetimeTz()         nsec==0          (tz preserved in struct)
//   AsTimestampWithoutTz() tz==0            (any nsec)
//   AsTimestampTz()        (any nsec, any tz; superset of above)
//
// All throw TypeMismatchException if !IsDatetime().
// AsDate / AsDatetime* throw ConversionException if nsec/tz constraints violated.
//
utils::datetime::Date  AsDate()               const;
DatetimeWithoutTz      AsDatetimeWithoutTz()  const;
DatetimeTz             AsDatetimeTz()         const;
TimestampWithoutTz     AsTimestampWithoutTz() const;
TimestampTz            AsTimestampTz()        const;

// ---- Other ext types ----
TntUuid      AsUuid()      const;   // throws TypeMismatchException if !IsUuid()
TntInterval  AsInterval()  const;   // throws TypeMismatchException if !IsInterval()

/// Decode DECIMAL ext bytes to decimal64::Decimal<Prec>.
/// @throws TypeMismatchException  if !IsDecimal()
/// @throws ConversionException    if scale > Prec (precision loss would occur)
template <typename DecimalT>
DecimalT AsDecimal() const;

/// Decode DECIMAL ext bytes to exact string representation (no precision loss).
std::string AsDecimalString() const;
```

Add to `value_builder.hpp`:

```cpp
// ---- Encode Tarantool ext types ----
explicit ValueBuilder(TntUuid uuid);
explicit ValueBuilder(utils::datetime::Date date);     // ext-4 fixext8, midnight UTC
explicit ValueBuilder(DatetimeTz dt);                  // ext-4, nsec omitted
explicit ValueBuilder(DatetimeWithoutTz dt);           // ext-4, nsec omitted, tz=0
explicit ValueBuilder(TimestampTz ts);                 // ext-4, nsec included when != 0
explicit ValueBuilder(TimestampWithoutTz ts);          // ext-4, nsec included, tz=0
explicit ValueBuilder(TntInterval interval);

template <typename DecimalT>
explicit ValueBuilder(DecimalT decimal);               // encodes as MP_DECIMAL BCD
```

---

## Explicit Specialisations for `Value::As<T>()`

Add to the explicit specialisation list in `value.hpp`:

```cpp
template <> utils::datetime::Date  Value::As<utils::datetime::Date>()  const;
template <> DatetimeTz             Value::As<DatetimeTz>()             const;
template <> DatetimeWithoutTz      Value::As<DatetimeWithoutTz>()      const;
template <> TimestampTz            Value::As<TimestampTz>()            const;
template <> TimestampWithoutTz     Value::As<TimestampWithoutTz>()     const;
template <> TntUuid                Value::As<TntUuid>()                const;
template <> TntInterval            Value::As<TntInterval>()            const;
```

So callers can use the uniform `As<T>()` syntax:

```cpp
auto date  = cursor.As<utils::datetime::Date>();       // "2024-03-09"
auto dt    = cursor.As<DatetimeTz>();                  // second precision + tz
auto ts    = cursor.As<TimestampTz>();                 // nanosecond precision + tz
auto uuid  = cursor.As<TntUuid>();
auto iv    = cursor.As<TntInterval>();
auto d     = cursor.As<decimal64::Decimal<6>>();
```

---

## `formats::parse` / `formats::serialize` ADL hooks

Once the types are in `universal/`, register them with the userver formats
framework so any component can use them transparently:

```cpp
// In universal/include/userver/formats/msgpack/serialize_tarantool.hpp

namespace formats::parse {
template <> utils::datetime::Date Parse(const msgpack::Value& v, To<utils::datetime::Date>);
template <> DatetimeTz            Parse(const msgpack::Value& v, To<DatetimeTz>);
template <> DatetimeWithoutTz     Parse(const msgpack::Value& v, To<DatetimeWithoutTz>);
template <> TimestampTz           Parse(const msgpack::Value& v, To<TimestampTz>);
template <> TimestampWithoutTz    Parse(const msgpack::Value& v, To<TimestampWithoutTz>);
template <> TntUuid               Parse(const msgpack::Value& v, To<TntUuid>);
template <> TntInterval           Parse(const msgpack::Value& v, To<TntInterval>);
template <typename D>             D Parse(const msgpack::Value& v, To<D>);  // decimal
}

namespace formats::serialize {
msgpack::ValueBuilder Serialize(utils::datetime::Date,   To<msgpack::ValueBuilder>);
msgpack::ValueBuilder Serialize(const DatetimeTz&,       To<msgpack::ValueBuilder>);
msgpack::ValueBuilder Serialize(DatetimeWithoutTz,       To<msgpack::ValueBuilder>);
msgpack::ValueBuilder Serialize(const TimestampTz&,      To<msgpack::ValueBuilder>);
msgpack::ValueBuilder Serialize(TimestampWithoutTz,      To<msgpack::ValueBuilder>);
msgpack::ValueBuilder Serialize(const TntUuid&,          To<msgpack::ValueBuilder>);
msgpack::ValueBuilder Serialize(const TntInterval&,      To<msgpack::ValueBuilder>);
template <typename D>
msgpack::ValueBuilder Serialize(const D& d,              To<msgpack::ValueBuilder>);
}
```

---

## Changes to `MpDecoder` (legacy JSON path)

After the new types are in place, update `MpDecoder::DecodeExt()` for the cases
it currently handles wrong:

| Ext | Old behaviour          | New behaviour                              |
|-----|------------------------|--------------------------------------------|
| 1   | skip → nil             | decode BCD → `std::string` decimal literal |
| 2   | UUID → `std::string`   | unchanged (string is still fine for JSON)  |
| 3   | skip → nil             | decode → JSON object with type/message/code |
| 4   | Datetime → JSON object | unchanged                                  |
| 6   | skip → nil             | decode → JSON object with each interval field |

MpDecoder is only used for the **query-encode side** (Phase 3 target) and will
be removed from the response-decode path entirely when Phase 3 is complete.

---

## Phased Implementation Plan

### Phase A — Type relocation and header (no new functionality)

1. Create `universal/include/userver/formats/msgpack/tarantool_types.hpp`
   - Move `TntUuid` from `tarantool/src/…/impl/msgpack.hpp`
   - Replace old `TntDatetime` with four new types:
     - `DatetimeTz` (struct: `time_point<seconds>` + tzoffset + tzindex)
     - `DatetimeWithoutTz` (strong typedef over `time_point<seconds>`)
     - `TimestampTz` (struct: `time_point<nanoseconds>` + tzoffset + tzindex)
     - `TimestampWithoutTz` (strong typedef over `time_point<nanoseconds>`)
   - `AsDate()` reuses `utils::datetime::Date` — no new struct needed
   - Add `TntInterval` struct (all-zero default)
   - Forward-declare `TntDecimal` helpers
2. Update `tarantool/src/…/impl/msgpack.hpp` to `#include` the new header;
   split `EncodeDateTime()` into four encode functions
3. Tests: `value_test.cpp` cases for `GetExtType()` / `GetExtData()` round-trips

**Estimated: 1 session, no benchmark impact**

### Phase B — On-demand decode in `formats::msgpack::Value`

1. Add `IsUuid()`, `IsDatetime()`, `IsDecimal()`, `IsInterval()`, `IsError()`
2. Add `AsDate()`, `AsDatetimeTz()`, `AsDatetimeWithoutTz()`,
   `AsTimestampTz()`, `AsTimestampWithoutTz()` with appropriate validation
3. Add `AsUuid()` (move decode logic from `MpDecoder`)
4. Add `AsInterval()` — implement packed-map varint decode
5. Add `AsDecimal<DecimalT>()` + `AsDecimalString()` — implement BCD decode
6. Add all `As<T>()` explicit specialisations
7. Add `ValueBuilder` constructors for all date/time types, uuid, interval, decimal
8. Tests: roundtrip encode → decode for all types; all validation edge cases
   (e.g. `AsDatetimeTz()` on nsec=1 → `ConversionException`;
          `AsDate()` on non-midnight → `ConversionException`)

**Estimated: 2 sessions**

### Phase C — Structured error support

1. Add `TntErrorInfo` struct
2. In `connection.cpp` `ReaderLoop`: if body map has key `0x52` (new
   `IPROTO_ERROR`), decode the structured error stack; otherwise fall back to
   `0x31` legacy string
3. Enrich `CommandException` with `TntErrorInfo` (optional field)
4. Tests: verify structured error fields are propagated

**Estimated: 1 session**

### Phase D — ADL hooks + `MpDecoder` fixes

1. Implement `formats::parse` / `formats::serialize` specialisations
2. Fix `MpDecoder::DecodeExt()` for ext types 1, 3, 6 (for the JSON fallback
   path that remains until Phase 3 of the msgpack migration removes it entirely)
3. Tests: JSON round-trip for DECIMAL (as string), INTERVAL (as object),
   ERROR (as object)

**Estimated: 1 session**

### Phase E — Performance evaluation

Benchmark after Phase B:

| Workload         | Expected change | Reason |
|------------------|-----------------|--------|
| REPLACE (no ext) | 0%              | No ext fields accessed |
| REPLACE + UUID   | +5–8%           | Avoids eager string formatting of UUIDs |
| REPLACE + Datetime | +3–5%         | Avoids eager JSON object construction |
| REPLACE + Decimal | +2–4%          | Avoids BCD→string→JSON conversion |

Profile target: `MpDecoder::DecodeExt()` should vanish from `perf report`
for the response path.

---

## File Layout (target state)

```
universal/
  include/userver/formats/msgpack/
    tarantool_types.hpp       # TntUuid, TntDate, TntDatetime, TntTimestamp, TntInterval (new)
    value.hpp                 # + AsDate, AsDatetime, AsTimestamp, AsUuid, AsInterval, AsDecimal
    value_builder.hpp         # + ValueBuilder(TntUuid), (TntDate), (TntDatetime), (TntTimestamp), etc.
    serialize_tarantool.hpp   # formats::parse / formats::serialize hooks (new)
  src/formats/msgpack/
    value.cpp                 # + AsDate/AsDatetime/AsTimestamp/AsUuid/AsInterval/AsDecimal impls
    value_builder.cpp         # + EncodeNode for new types
    tarantool_types.cpp       # TntUuid::ToString, TntDate/Datetime/Timestamp::ToTimePoint, etc. (new)
    value_test.cpp            # + Phase B tests

tarantool/
  src/storages/tarantool/impl/
    msgpack.hpp               # #include tarantool_types.hpp; keep low-level enc/dec
    connection.cpp            # + structured error decode (Phase C)
  include/userver/storages/tarantool/
    exceptions.hpp            # + TntErrorInfo field in CommandException (Phase C)
```

---

## Open Questions

1. **Four temporal types from one wire format**: The five accessors (`AsDate`,
   `AsDatetimeTz`, `AsDatetimeWithoutTz`, `AsTimestampTz`,
   `AsTimestampWithoutTz`) all decode ext type 4. This mirrors the PostgreSQL
   driver's pattern of `Date`, `TimePointTz`, `TimePointWithoutTz` where the
   C++ type carries semantic intent. The key deviation: Tarantool stores tz
   explicitly in the value, so `DatetimeTz` / `TimestampTz` are structs with tz
   fields, not just strong typedefs.

2. **`utils::datetime::Date` reuse**: The existing `Date` type from `universal/`
   works directly for Tarantool's date logical type (it wraps `sys_days`, is
   format-agnostic, and has JSON/format ADL hooks). No new struct needed —
   `AsDate()` returns `utils::datetime::Date`.

3. **No `TIME` type**: Tarantool has no equivalent of PostgreSQL `TIME` or
   `TIMETZ`. `utils::datetime::TimeOfDay` is not needed for the Tarantool driver.

4. **`decimal64::Decimal<Prec>` vs dynamic decimal**: Tarantool's BCD scale is
   dynamic (embedded in the wire data), but `Decimal<Prec>` has a compile-time
   `Prec`. `AsDecimal<Decimal<6>>()` (or whatever Prec the user chooses) will
   throw `ConversionException` if the wire scale > Prec. Providing
   `AsDecimalString()` as a lossless fallback is the right escape hatch. A
   future `TntDecimal` (dynamic-precision) could be introduced if needed.

5. **`TntInterval` and calendar arithmetic**: year/month/week are calendar
   units that cannot be converted to nanoseconds without knowing a reference
   date.  The `ToNanoseconds()` method will throw for non-zero year/month/week,
   mirroring PostgreSQL's `UnsupportedInterval`. Full calendar-aware arithmetic
   is out of scope.

6. **`TntErrorInfo` and public API**: structured error enrichment is a breaking
   change to `CommandException` if it gains new fields. Add as optional
   (`std::optional<TntErrorInfo>`) to remain source-compatible.

7. **Tarantool version compatibility**: `MP_DATETIME` (ext 4) is available since
   2.10.0; `MP_DECIMAL` (ext 1) since 2.3.1; `MP_INTERVAL` (ext 6) since 2.10.0;
   structured `MP_ERROR` (ext 3) since 2.4.1. The connector should tolerate
   receiving unknown ext types gracefully (current behaviour: → nil).
