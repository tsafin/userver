#pragma once

/// @file storages/tarantool/impl/vspace_tuple.hpp
/// @brief Typed representation of a Tarantool _vspace / _vindex tuple,
///        decoded with tntcxx mpp for type safety.

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include <Buffer/Buffer.hpp>
#include <mpp/mpp.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

/// @brief Partial representation of a `_vspace` tuple (system view).
///
/// _vspace field order (Tarantool 2.x):
///   [0] id          uint32  space ID
///   [1] owner       uint32  owner UID
///   [2] name        string  space name
///   [3] engine      string  "memtx" | "vinyl"
///   [4] field_count uint32  declared field count (0 = unlimited)
///   [5] flags       map     space options (ignored here)
///   [6] format      array   field format descriptors (ignored here)
///
/// Only the first 5 fields are decoded; trailing fields are skipped by mpp.
struct VspaceTuple {
    uint32_t id{0};
    uint32_t owner{0};
    std::string name;
    std::string engine;
    uint32_t field_count{0};

    // mpp decode trait: fields decoded in the order listed.
    static constexpr auto mpp = std::make_tuple(
        &VspaceTuple::id,
        &VspaceTuple::owner,
        &VspaceTuple::name,
        &VspaceTuple::engine,
        &VspaceTuple::field_count);
};

/// @brief Decode a raw IPROTO_DATA byte array (outer array of tuples) into
///        a `std::vector<VspaceTuple>` using tntcxx mpp.
///
/// @param raw  Raw msgpack bytes of the entire IPROTO_DATA value
///             (e.g. from `ExecutionResult::GetRawBytes()`).
/// @returns    Decoded vector; empty if `raw` is empty.
inline std::vector<VspaceTuple> DecodeVspaceTuples(
        std::span<const uint8_t> raw) {
    if (raw.empty()) return {};

    tnt::Buffer<4096> buf;
    buf.write(tnt::Buffer<4096>::WData{
        reinterpret_cast<const char*>(raw.data()), raw.size()});

    auto iter = buf.begin<true>();
    std::vector<VspaceTuple> tuples;
    mpp::decode(iter, tuples);
    return tuples;
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
