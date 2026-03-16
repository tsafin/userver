#pragma once

/// @file userver/storages/tarantool/fwd.hpp
/// @brief Forward declarations for popular Tarantool-related types

#include <memory>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

class Cluster;

using ClusterPtr = std::shared_ptr<Cluster>;

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
