#pragma once

/// @file userver/formats/msgpack/value.hpp
/// @brief Zero-copy cursor over a MessagePack-encoded byte buffer.

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <userver/formats/msgpack/exception.hpp>
#include <userver/formats/msgpack/tarantool_types.hpp>
#include <userver/formats/parse/to.hpp>
#include <userver/utils/datetime/date.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

class ValueBuilder;

/// @brief Zero-copy, non-owning cursor over a MessagePack buffer.
///
/// A `Value` is a lightweight pair of `{pos, end}` pointers into an existing
/// byte buffer.  No heap allocation occurs when reading map/array fields.
///
/// Integer-keyed maps (as used by Tarantool IPROTO) are first-class via
/// `operator[](uint64_t)`.
///
/// The caller must keep the underlying buffer alive for the lifetime of the
/// `Value` (and all children derived from it).
class Value {
public:
    /// Constructs a "missing" value (IsMissing() == true).
    Value() = default;

    Value(const Value&) = default;
    Value& operator=(const Value&) = default;
    Value(Value&&) noexcept = default;
    Value& operator=(Value&&) noexcept = default;

    // ------------------------------------------------------------------ //
    //  Factory                                                             //
    // ------------------------------------------------------------------ //

    /// @brief Create a Value that is a view over [data, data+len).
    /// @note No copy is made; the buffer must outlive this Value.
    static Value FromBytes(const uint8_t* data, std::size_t len) noexcept;

    // ------------------------------------------------------------------ //
    //  State predicates                                                    //
    // ------------------------------------------------------------------ //

    /// Returns true if the key was not found (no actual msgpack byte behind it).
    bool IsMissing() const noexcept { return pos_ == nullptr; }

    bool IsNull() const noexcept;
    bool IsBool() const noexcept;
    bool IsInt() const noexcept;
    bool IsUInt() const noexcept;
    bool IsDouble() const noexcept;
    bool IsString() const noexcept;
    bool IsArray() const noexcept;
    bool IsObject() const noexcept;
    /// @returns true for any ext type (fixext/ext8/ext16/ext32).
    bool IsExt() const noexcept;

    // ---- Tarantool ext type predicates ----
    bool IsUuid()     const noexcept;  ///< ext type 2
    bool IsDatetime() const noexcept;  ///< ext type 4
    bool IsDecimal()  const noexcept;  ///< ext type 1
    bool IsInterval() const noexcept;  ///< ext type 6

    // ---- Date/time accessors (all from ext type 4) ----
    utils::datetime::Date  AsDate()               const;
    DatetimeWithoutTz      AsDatetimeWithoutTz()  const;
    DatetimeTz             AsDatetimeTz()         const;
    TimestampWithoutTz     AsTimestampWithoutTz() const;
    TimestampTz            AsTimestampTz()        const;

    TntUuid                AsUuid()      const;  ///< ext type 2
    TntInterval            AsInterval()  const;  ///< ext type 6

    std::string AsDecimalString() const;  ///< ext type 1, always lossless

    /// @returns number of elements in an array or map; 0 for scalars.
    std::size_t GetSize() const;

    // ------------------------------------------------------------------ //
    //  Navigation                                                          //
    // ------------------------------------------------------------------ //

    /// @brief For arrays: returns the element at `index`.
    ///        For maps with integer keys: looks up `key` in the map.
    ///        In both cases the argument is interpreted according to the
    ///        actual runtime type of *this.
    /// @throw TypeMismatchException if *this is neither an array nor a map.
    /// @throw OutOfBoundsException  if *this is an array and index >= size.
    Value operator[](std::size_t index) const;

    /// @brief Looks up a string-keyed entry in a map.
    /// @returns a missing Value if the key is absent.
    /// @throw TypeMismatchException if *this is not a map.
    Value operator[](std::string_view key) const;

    // ------------------------------------------------------------------ //
    //  Extraction                                                          //
    // ------------------------------------------------------------------ //

    /// @brief Converts the value to T.
    ///
    /// For built-in types (bool, int, double, string) explicit specialisations
    /// defined in value.cpp are used.  For Tarantool ext types include
    /// `<userver/formats/msgpack/serialize_tarantool.hpp>` to enable the ADL
    /// `Parse()` hooks; for user-defined types, provide a
    /// `Parse(const Value&, formats::parse::To<T>)` function.
    ///
    /// @throw MemberMissingException  if IsMissing().
    /// @throw TypeMismatchException   if the type doesn't fit.
    /// @throw ConversionException     for numeric overflow / encoding errors.
    template <typename T>
    T As() const {
        return Parse(*this, formats::parse::To<T>{});
    }

private:
    // Non-template friend declarations make `Parse` a known name for
    // phase-1 lookup inside As<T>(), enabling ADL at instantiation time
    // for the generic template version (containers, user types, etc.).
    // Each friend is defined in value.cpp.
    friend bool        Parse(const Value&, formats::parse::To<bool>);
    friend int8_t      Parse(const Value&, formats::parse::To<int8_t>);
    friend int16_t     Parse(const Value&, formats::parse::To<int16_t>);
    friend int32_t     Parse(const Value&, formats::parse::To<int32_t>);
    friend int64_t     Parse(const Value&, formats::parse::To<int64_t>);
    friend uint8_t     Parse(const Value&, formats::parse::To<uint8_t>);
    friend uint16_t    Parse(const Value&, formats::parse::To<uint16_t>);
    friend uint32_t    Parse(const Value&, formats::parse::To<uint32_t>);
    friend uint64_t    Parse(const Value&, formats::parse::To<uint64_t>);
    friend float       Parse(const Value&, formats::parse::To<float>);
    friend double      Parse(const Value&, formats::parse::To<double>);
    friend std::string Parse(const Value&, formats::parse::To<std::string>);

public:

    /// @brief Returns As<T>() or @p default_val on any error (including missing).
    template <typename T>
    T As(T default_val) const noexcept {
        try {
            return As<T>();
        } catch (...) {
            return default_val;
        }
    }

    /// @brief Returns the raw ext bytes (without the ext header).
    /// @throw TypeMismatchException if not an ext type.
    std::string_view GetExtData() const;

    /// @brief Returns the ext type tag byte.
    /// @throw TypeMismatchException if not an ext type.
    int8_t GetExtType() const;

    // ------------------------------------------------------------------ //
    //  Path (for error messages)                                           //
    // ------------------------------------------------------------------ //

    std::string GetPath() const;

    // ------------------------------------------------------------------ //
    //  Aliases                                                             //
    // ------------------------------------------------------------------ //

    using Builder = ValueBuilder;
    using Exception = formats::msgpack::Exception;
    using ParseException = formats::msgpack::ParseException;
    using ExceptionWithPath = formats::msgpack::ExceptionWithPath;

    /// @brief Returns a cursor pointing to the byte immediately after this value.
    ///
    /// Useful when a buffer contains two consecutive msgpack values (e.g. the
    /// IPROTO header followed by the IPROTO body in the same receive buffer).
    /// Returns a missing Value if this value extends to the very end of the
    /// buffer (no bytes remain).
    Value NextSibling() const;

    /// @returns pointer to the first byte of this value in the buffer.
    const uint8_t* GetRawPos() const noexcept { return pos_; }

    /// @returns pointer past the end of the owning buffer.
    const uint8_t* GetRawEnd() const noexcept { return end_; }

private:
    Value(const uint8_t* pos, const uint8_t* end, std::string path) noexcept;

    /// Returns the first byte at pos_.
    uint8_t Lead() const noexcept { return *pos_; }

    /// Throws MemberMissingException if IsMissing().
    void CheckNotMissing() const;

    /// Throws TypeMismatchException(actual, expected).
    [[noreturn]] void ThrowTypeMismatch(int expected) const;

    const uint8_t* pos_{nullptr};  ///< nullptr → missing
    const uint8_t* end_{nullptr};
    std::string path_{};
};

// ---------------------------------------------------------------------- //
// Explicit specialisations (defined in value.cpp)                         //
// ---------------------------------------------------------------------- //

template <> bool        Value::As<bool>()        const;
template <> int8_t      Value::As<int8_t>()      const;
template <> int16_t     Value::As<int16_t>()     const;
template <> int32_t     Value::As<int32_t>()     const;
template <> int64_t     Value::As<int64_t>()     const;
template <> uint8_t     Value::As<uint8_t>()     const;
template <> uint16_t    Value::As<uint16_t>()    const;
template <> uint32_t    Value::As<uint32_t>()    const;
template <> uint64_t    Value::As<uint64_t>()    const;
template <> float       Value::As<float>()       const;
template <> double      Value::As<double>()      const;
template <> std::string Value::As<std::string>() const;

template <> utils::datetime::Date   Value::As<utils::datetime::Date>()   const;
template <> DatetimeTz              Value::As<DatetimeTz>()              const;
template <> DatetimeWithoutTz       Value::As<DatetimeWithoutTz>()       const;
template <> TimestampTz             Value::As<TimestampTz>()             const;
template <> TimestampWithoutTz      Value::As<TimestampWithoutTz>()      const;
template <> TntUuid                 Value::As<TntUuid>()                 const;
template <> TntInterval             Value::As<TntInterval>()             const;

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
