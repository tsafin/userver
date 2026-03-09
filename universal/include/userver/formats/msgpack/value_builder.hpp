#pragma once

/// @file userver/formats/msgpack/value_builder.hpp
/// @brief In-memory builder for MessagePack values.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <userver/formats/msgpack/exception.hpp>
#include <userver/formats/msgpack/tarantool_types.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/utils/datetime/date.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

namespace impl {
struct Node;
}  // namespace impl

/// @brief Builds a MessagePack value in memory, then serialises it via
/// `ToBytes()`.
///
/// Typical usage:
/// @code
///   auto body = ValueBuilder::IntKeyObject();
///   body[0x10U] = ValueBuilder{space_id};           // uint key → uint value
///   body[0x21U] = ValueBuilder::Array();
///   body[0x21U].PushBack(ValueBuilder{"hello"});
///   std::vector<uint8_t> bytes = body.ToBytes();
/// @endcode
///
/// Integer-keyed objects (`IntKeyObject`) are for Tarantool IPROTO requests
/// where map keys are compact unsigned integers (0x00–0x3F).
///
/// @note `operator[]` returns a *proxy* `ValueBuilder` that borrows a
///       reference into the parent tree.  Proxies must not outlive their
///       parent and must not be stored across modifications to the same
///       parent that could reallocate its child storage.
class ValueBuilder {
public:
    // ------------------------------------------------------------------ //
    //  Construction from scalar types                                      //
    // ------------------------------------------------------------------ //

    /// Constructs a null value.
    ValueBuilder();

    explicit ValueBuilder(bool v);
    explicit ValueBuilder(int8_t v);
    explicit ValueBuilder(int16_t v);
    explicit ValueBuilder(int32_t v);
    explicit ValueBuilder(int64_t v);
    explicit ValueBuilder(uint8_t v);
    explicit ValueBuilder(uint16_t v);
    explicit ValueBuilder(uint32_t v);
    explicit ValueBuilder(uint64_t v);
    explicit ValueBuilder(float v);
    explicit ValueBuilder(double v);
    explicit ValueBuilder(std::string_view v);
    explicit ValueBuilder(const std::string& v);
    explicit ValueBuilder(const char* v);

    /// Constructs a ValueBuilder from an existing (decoded) msgpack Value.
    /// This re-encodes the value into the builder's node tree.
    explicit ValueBuilder(const Value& v);

    // ---- Tarantool ext type constructors -----------------------------------
    explicit ValueBuilder(TntUuid uuid);
    explicit ValueBuilder(utils::datetime::Date date);
    explicit ValueBuilder(DatetimeTz dt);
    explicit ValueBuilder(DatetimeWithoutTz dt);
    explicit ValueBuilder(TimestampTz ts);
    explicit ValueBuilder(TimestampWithoutTz ts);
    explicit ValueBuilder(TntInterval interval);

    // ------------------------------------------------------------------ //
    //  Copy / move                                                         //
    // ------------------------------------------------------------------ //

    ValueBuilder(const ValueBuilder& other);
    ValueBuilder& operator=(ValueBuilder rhs) noexcept;
    ValueBuilder(ValueBuilder&&) noexcept = default;

    ~ValueBuilder() = default;

    // ------------------------------------------------------------------ //
    //  Container factories                                                 //
    // ------------------------------------------------------------------ //

    /// @returns an empty array builder.
    static ValueBuilder Array();

    /// @returns an empty string-keyed map builder.
    static ValueBuilder Object();

    /// @returns an empty integer-keyed map builder (for IPROTO bodies).
    static ValueBuilder IntKeyObject();

    // ------------------------------------------------------------------ //
    //  Navigation / mutation                                               //
    // ------------------------------------------------------------------ //

    /// @brief Accesses (creates if absent) an element by integer key.
    /// @returns a proxy ValueBuilder pointing into this builder's tree.
    /// @throw TypeMismatchException if *this is not an IntKeyObject.
    ValueBuilder operator[](uint64_t key);

    /// @brief Accesses (creates if absent) an element by string key.
    /// @returns a proxy ValueBuilder pointing into this builder's tree.
    /// @throw TypeMismatchException if *this is not an Object.
    ValueBuilder operator[](std::string_view key);

    /// Appends a value to an array.
    /// @throw TypeMismatchException if *this is not an Array.
    void PushBack(ValueBuilder value);

    // ------------------------------------------------------------------ //
    //  Type predicates                                                     //
    // ------------------------------------------------------------------ //

    bool IsNull() const noexcept;
    bool IsBool() const noexcept;
    bool IsInt() const noexcept;
    bool IsUInt() const noexcept;
    bool IsDouble() const noexcept;
    bool IsString() const noexcept;
    bool IsArray() const noexcept;
    bool IsObject() const noexcept;

    std::size_t GetSize() const;

    // ------------------------------------------------------------------ //
    //  Serialisation                                                       //
    // ------------------------------------------------------------------ //

    /// Serialise the current value to a MessagePack byte vector.
    std::vector<uint8_t> ToBytes() const;

    /// Serialise to bytes and wrap them in a non-owning Value cursor.
    /// @note The returned Value borrows from the returned vector; the vector
    ///       must outlive the Value.
    Value ToValue(std::vector<uint8_t>& out) const;

    // ------------------------------------------------------------------ //
    //  Path (for error messages)                                           //
    // ------------------------------------------------------------------ //

    std::string GetPath() const;

    // ------------------------------------------------------------------ //
    //  Aliases                                                             //
    // ------------------------------------------------------------------ //

    using Exception = formats::msgpack::Exception;
    using ExceptionWithPath = formats::msgpack::ExceptionWithPath;

private:
    /// @param root  shared owner of the whole node tree.
    /// @param node  the specific node this builder instance manages.
    ValueBuilder(std::shared_ptr<impl::Node> root, impl::Node* node) noexcept;

    impl::Node& Node() noexcept { return *node_; }
    const impl::Node& Node() const noexcept { return *node_; }

    std::shared_ptr<impl::Node> root_;
    impl::Node* node_{nullptr};
};

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
