#pragma once

/// @file vshard/impl/vshard_envelope.hpp
/// @brief Decode the vshard response envelope from vshard.storage.call.

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include <userver/formats/msgpack/value.hpp>
#include <userver/storages/tarantool/result.hpp>

#include <vshard/impl/vshard_error.hpp>
#include <vshard/impl/vshard_exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// Decoded vshard storage response envelope (Value-based, for non-hot paths).
struct VshardEnvelope {
    formats::msgpack::Value app_result;  ///< User function result (may be nil)
    VshardError vshard_error;            ///< Set only on routing errors (nil status)
};

/// Decode an @ref storages::tarantool::ExecutionResult into a VshardEnvelope.
/// Calls GetData() — builds the full Value tree.  Use DecodeEnvelopeRaw() on
/// the hot path to avoid the tree allocation.
VshardEnvelope DecodeEnvelope(const storages::tarantool::ExecutionResult& result);

// ---------------------------------------------------------------------------
// Zero-copy path: scan raw bytes, no Value tree
// ---------------------------------------------------------------------------

enum class RawEnvelopeStatus {
    kOk,
    kStorageCallError,
    kVshardError,
};

/// Result of a zero-copy raw envelope decode.
///
/// For kOk:
///   return_values_bytes contains the raw msgpack array of router return
///   values: [app_result].
///
/// For kStorageCallError:
///   return_values_bytes contains the raw msgpack array [nil, err].
///
/// For kVshardError:
///   vshard_error is set and the caller should enter the retry/error path.
struct RawEnvelopeResult {
    std::vector<uint8_t> return_values_bytes;
    VshardError vshard_error;
    RawEnvelopeStatus status{RawEnvelopeStatus::kOk};
};

/// Decode the vshard envelope without constructing a formats::msgpack::Value
/// tree.  Uses msgpack_scan::SkipValue to locate the app-result byte span
/// within the raw IPROTO_DATA buffer and copies it out.
///
/// Throws CommandException on IPROTO-level errors (via AssertOk).
/// Returns:
///   kOk                for successful storage calls,
///   kStorageCallError  for Lua-level storage/user-function errors,
///   kVshardError       for routing errors like WRONG_BUCKET / NON_MASTER.
RawEnvelopeResult DecodeEnvelopeRaw(
    const storages::tarantool::ExecutionResult& result);

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
