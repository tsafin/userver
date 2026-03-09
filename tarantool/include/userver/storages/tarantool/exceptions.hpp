#pragma once

/// @file userver/storages/tarantool/exceptions.hpp
/// @brief Tarantool client exceptions

#include <optional>
#include <stdexcept>
#include <string>

#include <userver/storages/tarantool/error_info.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

/// Base exception for all Tarantool errors
class TarantoolException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/// Authentication failure
class TarantoolAuthException : public TarantoolException {
  using TarantoolException::TarantoolException;
};

/// Thrown when a Tarantool command returns an error response.
///
/// On Tarantool 2.4+ the exception also carries @ref GetErrorInfo with the
/// full structured error stack (IPROTO_ERROR, key 0x52). On older servers
/// only the legacy error string is available.
class CommandException : public TarantoolException {
 public:
  CommandException(uint32_t error_code, std::string message,
                   std::optional<TntErrorInfo> error_info = std::nullopt)
      : TarantoolException{message},
        error_code_{error_code},
        error_info_{std::move(error_info)} {}

  uint32_t GetErrorCode() const noexcept { return error_code_; }

  /// Returns structured error info if available (Tarantool 2.4+), else nullptr.
  const TntErrorInfo* GetErrorInfo() const noexcept {
      return error_info_ ? &*error_info_ : nullptr;
  }

 private:
  uint32_t error_code_;
  std::optional<TntErrorInfo> error_info_;
};

/// Thrown when all pools in the cluster are unavailable
class NoAvailablePoolError : public TarantoolException {
  using TarantoolException::TarantoolException;
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
