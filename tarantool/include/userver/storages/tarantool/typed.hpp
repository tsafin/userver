#pragma once

/// @file userver/storages/tarantool/typed.hpp
/// @brief Typed mpp API for zero-allocation encode/decode of C++ structs.
///
/// ## Overview
///
/// The standard `Cluster::Insert(space, ValueBuilder)` path converts a C++
/// value to a `formats::msgpack::ValueBuilder`, serialises it, then sends it.
/// For hot-path operations this allocates a `ValueBuilder` tree and a
/// `std::vector<uint8_t>` for each request.
///
/// This header provides compile-time typed overloads that use tntcxx's
/// `mpp::encode` / `mpp::decode` directly:
///
/// * **MppEncode**: serialize any `T` with `static constexpr auto mpp = ...`
///   into `std::vector<uint8_t>` with a single allocation.
/// * **MppDecode**: deserialize an IPROTO_DATA array of tuples into
///   `std::vector<T>` with typed field access.
/// * **Insert<T>** / **Replace<T>**: encode `T` directly and call the
///   pre-encoded-bytes path, bypassing `ValueBuilder` entirely.
/// * **Select<T>**: execute a SELECT and decode the result as `std::vector<T>`.
///
/// ## Requirements for T
///
/// ```cpp
/// struct MyTuple {
///     uint32_t id;
///     std::string name;
///     double value;
///
///     static constexpr auto mpp = std::make_tuple(
///         &MyTuple::id, &MyTuple::name, &MyTuple::value);
/// };
/// ```
///
/// ## Example
///
/// ```cpp
/// #include <userver/storages/tarantool/typed.hpp>
///
/// namespace tt = storages::tarantool;
///
/// tt::Replace<MyTuple>(cluster, "my_space", {42, "hello", 3.14});
///
/// auto rows = tt::Select<MyTuple>(cluster, "my_space",
///     formats::msgpack::ValueBuilder::Array({42}));
/// for (const auto& r : rows) { /* r.id, r.name, r.value */ }
/// ```

#include <cstdint>
#include <span>
#include <vector>

#include <Buffer/Buffer.hpp>
#include <mpp/mpp.hpp>

#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/storages/tarantool/cluster.hpp>
#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

// ---- Low-level encode / decode helpers ------------------------------------

/// @brief Encode `t` as a msgpack array using tntcxx mpp.
///
/// `T` must expose `static constexpr auto mpp = std::make_tuple(&T::field, …)`
/// or be a `std::tuple<…>`.  The result is the raw wire bytes as if
/// `mpp::encode(buf, t)` were called.
template <typename T>
std::vector<uint8_t> MppEncode(const T& t) {
    tnt::Buffer<4096> buf;
    mpp::encode(buf, t);

    const std::size_t sz = buf.end<true>() - buf.begin<true>();
    std::vector<uint8_t> out(sz);
    auto iter = buf.begin<true>();
    iter.read(tnt::Buffer<4096>::RData{
        reinterpret_cast<char*>(out.data()), sz});
    return out;
}

/// @brief Decode an IPROTO_DATA array (outer array of tuples) from raw
///        msgpack bytes into `std::vector<T>`.
///
/// The same requirements as `MppEncode<T>` apply to `T`.
/// Pass `ExecutionResult::GetRawBytes()` as `raw`.
template <typename T>
std::vector<T> MppDecode(std::span<const uint8_t> raw) {
    if (raw.empty()) return {};

    tnt::Buffer<4096> buf;
    buf.write(tnt::Buffer<4096>::WData{
        reinterpret_cast<const char*>(raw.data()), raw.size()});

    auto iter = buf.begin<true>();
    std::vector<T> result;
    mpp::decode(iter, result);
    return result;
}

// ---- Typed Cluster overloads ----------------------------------------------

/// @brief Insert a typed tuple without going through ValueBuilder.
///
/// Encodes `tuple` via `mpp::encode`, wraps it in a `Query::WithRawArgs`,
/// and calls `Cluster::Execute`.  No intermediate `ValueBuilder` is
/// created; the only heap allocation is the final `std::vector<uint8_t>`
/// produced by `MppEncode`.
template <typename T>
ExecutionResult Insert(Cluster& cluster, std::string_view space,
                       const T& tuple, OptionalCommandControl cc = {}) {
    return cluster.Execute(
        Query::WithRawArgs(Query::Type::kInsert, std::string{space},
                           MppEncode(tuple)),
        cc);
}

/// @brief Insert-or-replace a typed tuple without going through ValueBuilder.
template <typename T>
ExecutionResult Replace(Cluster& cluster, std::string_view space,
                        const T& tuple, OptionalCommandControl cc = {}) {
    return cluster.Execute(
        Query::WithRawArgs(Query::Type::kReplace, std::string{space},
                           MppEncode(tuple)),
        cc);
}

/// @brief Execute a SELECT and decode the result tuples as `std::vector<T>`.
///
/// The key may be built with `ValueBuilder` (same as the standard API).
/// The returned tuples are decoded via `mpp::decode` — no `Value::As<>`
/// navigaton required.
template <typename T>
std::vector<T> Select(Cluster& cluster, std::string_view space,
                      formats::msgpack::ValueBuilder key,
                      OptionalCommandControl cc = {}) {
    auto result = cluster.Select(space, std::move(key), cc);
    return MppDecode<T>(result.GetRawBytes());
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
