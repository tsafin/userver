#pragma once

/// @file vshard/impl/vshard_envelope.hpp
/// @brief Decode the vshard response envelope from vshard.storage.call.

#include <optional>
#include <span>

#include <userver/formats/msgpack/value.hpp>
#include <userver/storages/tarantool/result.hpp>

#include <vshard/impl/vshard_error.hpp>
#include <vshard/impl/vshard_exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// Decoded vshard storage response envelope.
///
/// `vshard.storage.call` returns two values via IPROTO_CALL (0x0A):
///
/// ```lua
/// return true,  user_result     -- success: status=true, data=user result
/// return false, error_object    -- user function raised an error
/// return nil,   vshard_error    -- routing error (WRONG_BUCKET, etc.)
/// ```
///
/// Tarantool packs these as a flat array in IPROTO_DATA:
/// ```
/// IPROTO_DATA = [status, result_or_error]
///   status = true  → success, result_or_error is the user function return
///   status = false → user error, result_or_error is the error object
///   status = nil   → vshard routing error, result_or_error is vshard_error
/// ```
struct VshardEnvelope {
    formats::msgpack::Value app_result;  ///< User function result (may be nil)
    VshardError vshard_error;            ///< Set only on routing errors (nil status)
};

/// Decode an @ref storages::tarantool::ExecutionResult into a VshardEnvelope.
///
/// Throws @ref storages::tarantool::CommandException if the base result is an
/// error (i.e. IPROTO-level error, not a vshard application error).
///
/// @throws VshardStorageError   if vshard_error is set and is not MOVED/TRANSFER
/// @throws MovedError           if vshard_error.type == kWrongBucket
/// @throws TransferError        if vshard_error.type == kTransfer
VshardEnvelope DecodeEnvelope(const storages::tarantool::ExecutionResult& result);

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
