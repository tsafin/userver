#pragma once

/// @file userver/storages/tarantool/result.hpp
/// @brief @copybrief storages::tarantool::ExecutionResult

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <userver/storages/tarantool/error_info.hpp>

#include <userver/formats/msgpack/value.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

/// @brief Result of a Tarantool operation.
///
/// Wraps the decoded response from the Tarantool server.
/// The data payload is exposed as a zero-copy MessagePack cursor backed by
/// an internally-owned byte buffer.
class ExecutionResult final {
 public:
  ExecutionResult() = default;

  explicit ExecutionResult(bool ok, uint32_t error_code,
                           std::string error_message,
                           std::vector<uint8_t> data_buf,
                           std::optional<TntErrorInfo> error_info = std::nullopt);

  // Move-only: data_ is a non-owning cursor into data_buf_, so copying would
  // leave the cursor pointing at the source's buffer (dangling after source
  // destruction).  Move is safe because std::vector move preserves the
  // internal buffer address.
  ExecutionResult(const ExecutionResult&) = delete;
  ExecutionResult& operator=(const ExecutionResult&) = delete;
  ExecutionResult(ExecutionResult&&) noexcept;
  ExecutionResult& operator=(ExecutionResult&&) noexcept;

  /// @throws CommandException if the server returned an error
  void AssertOk() const;

  /// Returns true if the operation succeeded
  bool IsOk() const noexcept { return ok_; }

  /// Returns the decoded data array from the response as a MessagePack cursor.
  /// The cursor is valid for the lifetime of this ExecutionResult.
  const formats::msgpack::Value& GetData() const noexcept { return data_; }

  /// Converts the value to `T` via formats::msgpack::Value::As<T>().
  template <typename T>
  T As() const {
    AssertOk();
    return data_.As<T>();
  }

 private:
  bool ok_{true};
  uint32_t error_code_{0};
  std::string error_message_;
  std::optional<TntErrorInfo> error_info_;
  std::vector<uint8_t> data_buf_;  ///< owns the raw msgpack bytes
  formats::msgpack::Value data_;   ///< zero-copy cursor into data_buf_
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
