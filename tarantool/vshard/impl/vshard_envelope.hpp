#pragma once

/// @file vshard/impl/vshard_envelope.hpp
/// @brief Decode the vshard response envelope: [[app_result, vshard_error]].

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
/// vshard storage functions return a 2-element tuple:
/// ```lua
/// return {app_result, nil}        -- success
/// return {nil,        vshard_err} -- vshard-level error (MOVED, TRANSFER …)
/// ```
///
/// Both elements are wrapped in the IPROTO_DATA outer array:
/// ```
/// IPROTO_DATA = [[app_result, vshard_error]]
/// ```
struct VshardEnvelope {
    formats::msgpack::Value app_result;  ///< First element (may be nil)
    VshardError vshard_error;            ///< Second element (null if success)
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
