#pragma once

/// @file userver/formats/msgpack/value.hpp
/// @brief Zero-copy cursor over a MessagePack-encoded byte buffer.

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <userver/formats/msgpack/exception.hpp>

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
    /// @throw MemberMissingException  if IsMissing().
    /// @throw TypeMismatchException   if the type doesn't fit.
    /// @throw ConversionException     for numeric overflow / encoding errors.
    template <typename T>
    T As() const;

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

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
