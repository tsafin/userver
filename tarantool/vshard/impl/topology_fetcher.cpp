#include <vshard/impl/topology_fetcher.hpp>

#include <cmath>
#include <stdexcept>

#include <userver/formats/json/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/logging/log.hpp>

#include <storages/tarantool/impl/pool.hpp>
#include <storages/tarantool/impl/settings.hpp>
#include <vshard/impl/vshard_envelope.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

// ---------------------------------------------------------------------------
// VshardTopologyConfig::Parse
// ---------------------------------------------------------------------------

VshardTopologyConfig VshardTopologyConfig::Parse(
    const formats::json::Value& json) {
    VshardTopologyConfig cfg;
    cfg.bucket_count = json["bucket_count"].As<uint32_t>(3000);
    cfg.user = json["user"].As<std::string>("guest");
    cfg.password = json["password"].As<std::string>("");

    for (const auto& rs_json : json["replicasets"]) {
        ReplicasetConfig rs;
        rs.uuid = rs_json["uuid"].As<std::string>();
        for (const auto& node_json : rs_json["nodes"]) {
            ReplicasetConfig::NodeConfig node;
            node.host = node_json["host"].As<std::string>("127.0.0.1");
            node.port = node_json["port"].As<uint16_t>(3301);
            node.is_master = node_json["is_master"].As<bool>(false);
            rs.nodes.push_back(std::move(node));
        }
        cfg.replicasets.push_back(std::move(rs));
    }
    return cfg;
}

// ---------------------------------------------------------------------------
// TopologyFetcher
// ---------------------------------------------------------------------------

TopologyFetcher::TopologyFetcher(
    clients::dns::Resolver& resolver,
    const components::ComponentConfig& pool_config,
    const VshardTopologyConfig& config)
    : resolver_{resolver},
      pool_config_{pool_config},
      config_{config} {
    BuildPools();
}

TopologyFetcher::~TopologyFetcher() = default;

void TopologyFetcher::BuildPools() {
    pools_.reserve(config_.replicasets.size());

    const storages::tarantool::impl::AuthSettings auth{};
    // We rely on pool_config_ for timeout/pool-size defaults.

    for (const auto& rs_cfg : config_.replicasets) {
        std::shared_ptr<storages::tarantool::impl::Pool> master_pool;
        std::vector<std::shared_ptr<storages::tarantool::impl::Pool>> replica_pools;

        for (const auto& node : rs_cfg.nodes) {
            storages::tarantool::impl::EndpointSettings ep;
            ep.host = node.host;
            ep.port = node.port;

            storages::tarantool::impl::AuthSettings node_auth;
            node_auth.user     = config_.user;
            node_auth.password = config_.password;

            storages::tarantool::impl::PoolSettings ps{
                pool_config_, ep, node_auth};
            auto pool = std::make_shared<storages::tarantool::impl::Pool>(
                resolver_, std::move(ps));

            if (node.is_master) {
                master_pool = std::move(pool);
            } else {
                replica_pools.push_back(std::move(pool));
            }
        }

        if (!master_pool) {
            if (rs_cfg.nodes.empty()) {
                throw std::runtime_error{"Replicaset " + rs_cfg.uuid +
                                         " has no nodes"};
            }
            // Treat first node as master if none marked
            storages::tarantool::impl::EndpointSettings ep;
            ep.host = rs_cfg.nodes[0].host;
            ep.port = rs_cfg.nodes[0].port;

            storages::tarantool::impl::AuthSettings node_auth;
            node_auth.user     = config_.user;
            node_auth.password = config_.password;

            storages::tarantool::impl::PoolSettings ps{pool_config_, ep, node_auth};
            master_pool = std::make_shared<storages::tarantool::impl::Pool>(
                resolver_, std::move(ps));
        }

        auto rs_pool = std::make_shared<ReplicasetPool>(rs_cfg.uuid,
                                                         std::move(master_pool));
        for (auto& r : replica_pools) {
            rs_pool->AddReplica(std::move(r));
        }
        pools_.push_back(std::move(rs_pool));
    }
}

RoutingTable TopologyFetcher::BuildFromConfig() {
    RoutingTable table;
    table.bucket_count = config_.bucket_count;
    table.bucket_to_rs.assign(config_.bucket_count, 0);
    table.replicasets = pools_;

    const auto num_rs = static_cast<uint32_t>(pools_.size());
    if (num_rs == 0) {
        LOG_WARNING() << "TopologyFetcher: no replicasets configured";
        return table;
    }

    const uint32_t base  = config_.bucket_count / num_rs;
    const uint32_t extra = config_.bucket_count % num_rs;

    uint32_t next_bucket = 1;
    for (uint32_t i = 0; i < num_rs; ++i) {
        const uint32_t count = base + (i < extra ? 1 : 0);
        const uint16_t rs_idx = static_cast<uint16_t>(i + 1);
        for (uint32_t b = 0; b < count; ++b) {
            table.bucket_to_rs[next_bucket - 1] = rs_idx;
            ++next_bucket;
        }
    }

    LOG_INFO() << "TopologyFetcher: built routing table for "
               << config_.bucket_count << " buckets across " << num_rs
               << " replicasets";
    return table;
}

/// Probe a single bucket on a ReplicasetPool master by calling
/// vshard.storage.call(bucket_id, 'read', 'echo', {}).
/// Returns true if the RS owns the bucket (no WRONG_BUCKET response).
static bool ProbesBucket(ReplicasetPool& rs_pool, uint32_t bucket_id) {
    try {
        auto args_b = formats::msgpack::ValueBuilder::Array();
        args_b.PushBack(formats::msgpack::ValueBuilder{
            static_cast<uint64_t>(bucket_id)});
        args_b.PushBack(formats::msgpack::ValueBuilder{std::string{"read"}});
        args_b.PushBack(formats::msgpack::ValueBuilder{std::string{"echo"}});
        args_b.PushBack(formats::msgpack::ValueBuilder::Array());

        const auto raw = rs_pool.Execute(
            CallMode::kReadWrite,
            storages::tarantool::Query::Call("vshard.storage.call",
                                              std::move(args_b)));

        const auto env = DecodeEnvelope(raw);
        if (env.vshard_error.IsNull()) return true;
        return !env.vshard_error.IsWrongBucket();
    } catch (...) {
        return false;
    }
}

RoutingTable TopologyFetcher::RefreshFull() {
    // Probe bucket 1 on each RS master to discover the actual bucket allocation.
    // vshard bootstrap assigns even ranges but the first RS in the config might
    // not own buckets 1..N/2 — this probe resolves the actual assignment.
    const auto num_rs = static_cast<uint32_t>(pools_.size());
    if (num_rs == 0) {
        LOG_WARNING() << "TopologyFetcher::RefreshFull: no pools, using config";
        return BuildFromConfig();
    }

    // Probe each RS master with the first and the mid-point bucket.
    // This is sufficient to determine a simple contiguous-range split.
    const uint32_t mid = config_.bucket_count / 2 + 1;
    std::vector<uint32_t> rs_min_bucket(num_rs, 0);  // 0 = unknown

    for (uint32_t i = 0; i < num_rs; ++i) {
        auto& rs_pool = *pools_[i];
        if (ProbesBucket(rs_pool, 1)) {
            rs_min_bucket[i] = 1;
        } else if (ProbesBucket(rs_pool, mid)) {
            rs_min_bucket[i] = mid;
        }
    }

    // Build routing table from probe results.
    RoutingTable table;
    table.bucket_count = config_.bucket_count;
    table.bucket_to_rs.assign(config_.bucket_count, 0);
    table.replicasets = pools_;

    bool any_probed = false;
    for (uint32_t i = 0; i < num_rs; ++i) {
        if (rs_min_bucket[i] == 0) continue;
        any_probed = true;
        // Assign the RS's half of the bucket space.
        const uint32_t start = rs_min_bucket[i];
        const uint32_t end   = start + config_.bucket_count / num_rs - 1;
        const uint16_t rs_idx = static_cast<uint16_t>(i + 1);
        for (uint32_t b = start; b <= end && b <= config_.bucket_count; ++b) {
            table.bucket_to_rs[b - 1] = rs_idx;
        }
    }

    if (!any_probed) {
        LOG_WARNING() << "TopologyFetcher::RefreshFull: probes all failed, "
                         "falling back to static config";
        return BuildFromConfig();
    }

    // Fill any gaps left by failed probes using BuildFromConfig distribution.
    for (uint32_t b = 0; b < config_.bucket_count; ++b) {
        if (table.bucket_to_rs[b] == 0) {
            const uint32_t rs_idx = b / (config_.bucket_count / num_rs) + 1;
            table.bucket_to_rs[b] =
                static_cast<uint16_t>(std::min(rs_idx, num_rs));
        }
    }

    LOG_INFO() << "TopologyFetcher::RefreshFull: probed routing table for "
               << config_.bucket_count << " buckets across " << num_rs << " RS";
    return table;
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
