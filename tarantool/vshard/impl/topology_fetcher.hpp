#pragma once

/// @file vshard/impl/topology_fetcher.hpp
/// @brief Build / refresh the routing table from static config or live storage.

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/components/component_config.hpp>
#include <userver/formats/json/value.hpp>

#include <vshard/impl/routing_table.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// Config for a single replicaset within the vshard cluster.
struct ReplicasetConfig {
    std::string uuid;        ///< Replicaset UUID (or human-readable name on Tnt 3)
    struct NodeConfig {
        std::string host;
        uint16_t port{3301};
        bool is_master{false};
    };
    std::vector<NodeConfig> nodes;
};

/// Full static vshard topology config (from secdist or YAML).
struct VshardTopologyConfig {
    uint32_t bucket_count{3000};
    std::vector<ReplicasetConfig> replicasets;

    /// Parse from secdist JSON:
    /// ```json
    /// { "bucket_count": 3000,
    ///   "replicasets": [
    ///     { "uuid": "...", "nodes": [
    ///         {"host": "...", "port": 3301, "is_master": true},
    ///         {"host": "...", "port": 3302}
    ///       ]
    ///     }
    ///   ],
    ///   "user": "guest", "password": ""
    /// }
    /// ```
    static VshardTopologyConfig Parse(const formats::json::Value& json);

    std::string user{"guest"};
    std::string password;
};

/// Constructs a @ref RoutingTable from a @ref VshardTopologyConfig.
///
/// Strategy D (static-config-only): bucket ranges assigned evenly across
/// replicasets as `ceil(bucket_count / num_replicasets)` per RS.
///
/// The returned table is passed to `RoutingTableHolder::Assign()` at startup,
/// then periodically refreshed by querying `vshard.storage.bucket_stat` on
/// a detected MOVED, or by calling `vshard.router.info()` if a Lua router is
/// available.
class TopologyFetcher final {
 public:
    TopologyFetcher(clients::dns::Resolver& resolver,
                    const components::ComponentConfig& pool_config,
                    const VshardTopologyConfig& config);
    ~TopologyFetcher();

    /// Build initial routing table from static config only (no RPCs).
    RoutingTable BuildFromConfig();

    /// Refresh routing by querying vshard.storage.bucket_stat for every bucket
    /// whose owner has sent MOVED to an unknown destination.
    /// On failure, logs a warning and returns the config-built table.
    RoutingTable RefreshFull();

 private:
    clients::dns::Resolver& resolver_;
    const components::ComponentConfig& pool_config_;
    VshardTopologyConfig config_;
    std::vector<std::shared_ptr<ReplicasetPool>> pools_;

    void BuildPools();
};

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
