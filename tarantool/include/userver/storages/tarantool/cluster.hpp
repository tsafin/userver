#pragma once

/// @file userver/storages/tarantool/cluster.hpp
/// @brief @copybrief storages::tarantool::Cluster

#include <atomic>
#include <memory>
#include <string_view>
#include <vector>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/components/component_fwd.hpp>
#include <userver/utils/statistics/writer.hpp>

#include <userver/storages/tarantool/fwd.hpp>
#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

namespace impl {
struct TarantoolSettings;
class Pool;
}  // namespace impl

/// @ingroup userver_clients
///
/// @brief Interface for executing queries on a Tarantool instance or cluster.
///
/// Usually retrieved from components::Tarantool.
class Cluster final {
 public:
  Cluster(clients::dns::Resolver& resolver,
          const impl::TarantoolSettings& settings,
          const components::ComponentConfig& config);
  ~Cluster();

  Cluster(const Cluster&) = delete;

  /// @brief Execute a stored procedure call
  ExecutionResult Call(std::string_view func_name, formats::json::Value args,
                       OptionalCommandControl = {});

  /// @brief Select tuples by key from a space
  ExecutionResult Select(std::string_view space, formats::json::Value key,
                         OptionalCommandControl = {});

  /// @brief Insert a tuple into a space
  ExecutionResult Insert(std::string_view space, formats::json::Value tuple,
                         OptionalCommandControl = {});

  /// @brief Insert or replace a tuple in a space
  ExecutionResult Replace(std::string_view space, formats::json::Value tuple,
                          OptionalCommandControl = {});

  /// @brief Delete tuples matching a key from a space
  ExecutionResult Delete(std::string_view space, formats::json::Value key,
                         OptionalCommandControl = {});

  /// @brief Update fields of a tuple identified by key
  ExecutionResult Update(std::string_view space, formats::json::Value key,
                         formats::json::Value ops,
                         OptionalCommandControl = {});

  /// @brief Update or insert a tuple
  ExecutionResult Upsert(std::string_view space, formats::json::Value tuple,
                         formats::json::Value ops,
                         OptionalCommandControl = {});

  /// Write cluster statistics
  void WriteStatistics(utils::statistics::Writer& writer) const;

 private:
  ExecutionResult DoExecute(OptionalCommandControl cc, const Query& query);

  impl::Pool& GetPool() const;

  std::vector<std::unique_ptr<impl::Pool>> pools_;
  mutable std::atomic<std::size_t> current_pool_idx_{0};
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
