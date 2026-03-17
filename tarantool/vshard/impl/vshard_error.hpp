#pragma once

/// @file vshard/impl/vshard_error.hpp
/// @brief Decode vshard in-band error objects from storage responses.

#include <cstdint>
#include <optional>
#include <string>

#include <userver/formats/msgpack/value.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// vshard error types as defined in vshard/error.lua
enum class VshardErrorType : uint32_t {
    kUnknown = 0,
    kWrongBucket = 32,   ///< Bucket has moved; destination may be in message
    kNonMaster  = 40,   ///< Request reached a replica, not the master
    kTransfer   = 33,   ///< Bucket is mid-transfer
    kNoRouteset = 24,   ///< No replicaset owns this bucket
};

/// Structured vshard error object from the response envelope second element.
///
/// vshard storage functions return `{app_result, vshard_error}`.
/// `vshard_error` is either `box.NULL` (success) or a Lua table with at least
/// `code` and `type` fields, plus optional `destination` (for MOVED).
struct VshardError {
    VshardErrorType type{VshardErrorType::kUnknown};
    uint32_t code{0};
    std::string message;
    std::optional<std::string> destination_uuid;  ///< Non-null on WRONG_BUCKET

    bool IsNull() const noexcept { return code == 0 && message.empty(); }
    bool IsWrongBucket() const noexcept {
        return type == VshardErrorType::kWrongBucket;
    }
    bool IsNonMaster() const noexcept {
        return type == VshardErrorType::kNonMaster;
    }
    bool IsTransfer() const noexcept {
        return type == VshardErrorType::kTransfer;
    }
};

/// Parse a vshard error from a decoded msgpack Value (the second element of
/// the response envelope). Returns a null error if `val` is nil or null.
VshardError ParseVshardError(const formats::msgpack::Value& val);

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
