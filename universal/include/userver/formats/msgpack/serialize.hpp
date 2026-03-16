#pragma once

/// @file userver/formats/msgpack/serialize.hpp
/// @brief Helpers for serialising/deserialising MessagePack values.

#include <cstdint>
#include <vector>

#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

/// @brief Serialise @p value to a MessagePack byte vector.
std::vector<uint8_t> ToBytes(const ValueBuilder& value);

/// @brief Create a non-owning Value cursor over [data, data+len).
/// @note The buffer must outlive the returned Value.
Value FromBytes(const uint8_t* data, std::size_t len) noexcept;

/// @brief Create a non-owning Value cursor over the vector.
/// @note @p buf must outlive the returned Value.
Value FromBytes(const std::vector<uint8_t>& buf) noexcept;

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
