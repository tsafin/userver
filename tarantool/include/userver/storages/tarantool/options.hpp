#pragma once

/// @file userver/storages/tarantool/options.hpp
/// @brief Options for Tarantool commands

#include <chrono>
#include <optional>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

/// Controls timeouts for a single Tarantool command
struct CommandControl final {
  /// Overall time limit for the operation (acquire connection + execute)
  std::chrono::milliseconds execute{500};

  explicit constexpr CommandControl(std::chrono::milliseconds execute)
      : execute{execute} {}
};

/// @brief storages::tarantool::CommandControl that may not be set.
using OptionalCommandControl = std::optional<CommandControl>;

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
