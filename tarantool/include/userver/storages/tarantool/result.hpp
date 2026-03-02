#pragma once

/// @file userver/storages/tarantool/result.hpp
/// @brief @copybrief storages::tarantool::ExecutionResult

#include <cstddef>

#include <userver/formats/json/value.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

/// @brief Result of a Tarantool operation.
///
/// Wraps the decoded response from the Tarantool server.
class ExecutionResult final {
 public:
  ExecutionResult() = default;

  explicit ExecutionResult(bool ok, uint32_t error_code,
                           std::string error_message,
                           formats::json::Value data);

  /// @throws CommandException if the server returned an error
  void AssertOk() const;

  /// Returns true if the operation succeeded
  bool IsOk() const noexcept { return ok_; }

  /// Returns the decoded data array from the response
  const formats::json::Value& GetData() const noexcept { return data_; }

  /// Converts the first element of the response tuple to `T`
  template <typename T>
  T As() const {
    AssertOk();
    return data_.As<T>();
  }

 private:
  bool ok_{true};
  uint32_t error_code_{0};
  std::string error_message_;
  formats::json::Value data_;
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
