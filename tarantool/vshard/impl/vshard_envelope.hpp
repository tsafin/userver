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

/// Result of a zero-copy raw envelope decode.
///
/// On success (ok == true):
///   app_result_bytes holds a copy of the raw msgpack encoding of the user
///   function result extracted from the storage response envelope.  One
///   allocation — no Value tree is constructed.
///
/// On vshard routing error (ok == false, vshard_error set):
///   The error was parsed via the Value path (cold path).
struct RawEnvelopeResult {
    std::vector<uint8_t> app_result_bytes;  ///< raw msgpack of data[1] on success
    VshardError vshard_error;               ///< set on WRONG_BUCKET / NON_MASTER etc.
    bool ok{true};
};

/// Decode the vshard envelope without constructing a formats::msgpack::Value
/// tree.  Uses msgpack_scan::SkipValue to locate the app-result byte span
/// within the raw IPROTO_DATA buffer and copies it out.
///
/// Throws CommandException on IPROTO-level errors (via AssertOk).
/// Returns RawEnvelopeResult::ok == false (with vshard_error set) on routing
/// errors; throws CommandException on user-function errors.
RawEnvelopeResult DecodeEnvelopeRaw(
    const storages::tarantool::ExecutionResult& result);

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
