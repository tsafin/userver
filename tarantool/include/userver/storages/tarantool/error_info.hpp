#pragma once

/// @file userver/storages/tarantool/error_info.hpp
/// @brief @copybrief storages::tarantool::TntErrorInfo

#include <cstdint>
#include <string>
#include <vector>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

/// @brief One frame in a Tarantool structured error stack.
///
/// Available when the server returns IPROTO_ERROR (key 0x52), introduced in
/// Tarantool 2.4. Frames are ordered innermost-first (the root cause is at
/// index 0).
struct TntErrorFrame {
    /// Error class name, e.g. "ClientError", "AccessDeniedError".
    std::string type;
    /// Source file where the error was raised (server-side path).
    std::string file;
    /// Source line in @ref file.
    uint32_t line{0};
    /// Human-readable error message.
    std::string message;
    /// System errno at the point the error was raised (0 if not applicable).
    uint32_t sys_errno{0};
    /// Tarantool error code (matches the legacy uint32 in IPROTO_ERROR_24).
    uint32_t errcode{0};
};

/// @brief Structured error information from IPROTO_ERROR (key 0x52).
///
/// Contains the full error stack. Available from Tarantool 2.4+; older servers
/// only provide the legacy string via key 0x31.
struct TntErrorInfo {
    /// Error frames, innermost (most recent) first.
    std::vector<TntErrorFrame> stack;

    /// Returns the message of the innermost error frame, or an empty string
    /// if the stack is empty.
    const std::string& Message() const noexcept {
        static const std::string kEmpty;
        return stack.empty() ? kEmpty : stack.front().message;
    }

    /// Returns the errcode of the innermost error frame, or 0.
    uint32_t Errcode() const noexcept {
        return stack.empty() ? 0u : stack.front().errcode;
    }
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
