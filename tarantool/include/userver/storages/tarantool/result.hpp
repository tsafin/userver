#pragma once

/// @file userver/storages/tarantool/result.hpp
/// @brief @copybrief storages::tarantool::ExecutionResult

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
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
  /// Lazily parsed on first call — callers that only use GetRawBytes() pay no
  /// allocation cost for the Value tree.
  const formats::msgpack::Value& GetData() const noexcept;  // lazy, see result.cpp

  /// Returns the raw MessagePack bytes of the IPROTO_DATA array.
  /// Useful for typed decode (e.g. via tntcxx mpp).
  std::span<const uint8_t> GetRawBytes() const noexcept {
    return {data_buf_.data(), data_buf_.size()};
  }

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
  std::vector<uint8_t> data_buf_;      ///< owns the raw msgpack bytes
  mutable formats::msgpack::Value data_;        ///< lazily-decoded cursor into data_buf_
  mutable bool data_parsed_{false};    ///< true once Value::FromBytes has been called
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
