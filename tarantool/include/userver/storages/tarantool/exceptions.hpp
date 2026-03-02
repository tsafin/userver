#pragma once

/// @file userver/storages/tarantool/exceptions.hpp
/// @brief Tarantool client exceptions

#include <stdexcept>
#include <string>

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

/// Thrown when a Tarantool command returns an error response
class CommandException : public TarantoolException {
 public:
  CommandException(uint32_t error_code, std::string message)
      : TarantoolException{message},
        error_code_{error_code} {}

  uint32_t GetErrorCode() const noexcept { return error_code_; }

 private:
  uint32_t error_code_;
};

/// Thrown when all pools in the cluster are unavailable
class NoAvailablePoolError : public TarantoolException {
  using TarantoolException::TarantoolException;
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
