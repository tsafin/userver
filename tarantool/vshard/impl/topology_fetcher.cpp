#include <vshard/impl/topology_fetcher.hpp>

#include <cmath>
#include <stdexcept>

#include <userver/formats/json/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/tarantool/exceptions.hpp>

#include <storages/tarantool/impl/pool.hpp>
#include <storages/tarantool/impl/settings.hpp>

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

enum class BucketProbeStatus {
    kFound,
    kWrongBucket,
    kBackoff,
    kUnreachableReplicaset,
    kOtherError,
};

struct BucketProbeResult {
    BucketProbeStatus status{BucketProbeStatus::kWrongBucket};
    std::string error_message;
};

/// Probe a single bucket on a ReplicasetPool master by calling
/// vshard.storage.bucket_stat({bucket_id}).
static BucketProbeResult ProbeBucket(ReplicasetPool& rs_pool, uint32_t bucket_id) {
    try {
        auto args_b = formats::msgpack::ValueBuilder::Array();
        args_b.PushBack(formats::msgpack::ValueBuilder{
            static_cast<uint64_t>(bucket_id)});

        const auto raw = rs_pool.Execute(
            CallMode::kReadWrite,
            storages::tarantool::Query::Call("vshard.storage.bucket_stat",
                                              std::move(args_b)));

        // bucket_stat returns [stat, err].  If stat is non-nil the bucket is
        // on this RS.  If err has code WRONG_BUCKET, it's elsewhere.
        const auto& data = raw.GetData();
        if (data.IsArray() && data.GetSize() >= 1 && !data[0].IsMissing() &&
            !data[0].IsNull()) {
            return {BucketProbeStatus::kFound, {}};
        }
        if (data.IsArray() && data.GetSize() >= 2 && !data[1].IsMissing() &&
            !data[1].IsNull()) {
            const auto err_code = data[1]["code"].As<uint32_t>(0);
            if (err_code == 1) {
                return {BucketProbeStatus::kWrongBucket, {}};
            }
            if (err_code == 32) {
                return {BucketProbeStatus::kBackoff, {}};
            }
            const auto msg = data[1]["message"].As<std::string>("");
            return {BucketProbeStatus::kOtherError, msg};
        }
        return {BucketProbeStatus::kWrongBucket, {}};
    } catch (const storages::tarantool::TarantoolException& ex) {
        return {BucketProbeStatus::kUnreachableReplicaset, ex.what()};
    }
}

/// Call `vshard.storage.buckets_discovery({from=N})` on a single RS master.
/// Returns the list of active/pinned bucket IDs owned by this RS.
/// Uses pagination: repeats with next_from until all buckets are collected.
static std::vector<uint32_t> DiscoverBucketsOnRS(ReplicasetPool& rs_pool) {
    std::vector<uint32_t> result;
    uint64_t from = 1;

    for (int page = 0; page < 1000; ++page) {  // safety limit
        try {
            // Build args: [{from = N}]
            auto opts_map = formats::msgpack::ValueBuilder::Object();
            opts_map["from"] = formats::msgpack::ValueBuilder{from};
            auto args_b = formats::msgpack::ValueBuilder::Array();
            args_b.PushBack(std::move(opts_map));

            const auto raw = rs_pool.Execute(
                CallMode::kReadWrite,
                storages::tarantool::Query::Call(
                    "vshard.storage.buckets_discovery", std::move(args_b)));

            const auto& data = raw.GetData();
            // Response is a single-element array wrapping the return value:
            // [[{buckets=[...], next_from=N|nil}]]
            // data[0] is the map {buckets, next_from}
            if (!data.IsArray() || data.GetSize() < 1) break;

            const auto& resp = data[0];

            // Extract buckets array
            const auto& buckets_val = resp["buckets"];
            if (!buckets_val.IsMissing() && buckets_val.IsArray()) {
                const auto sz = buckets_val.GetSize();
                result.reserve(result.size() + sz);
                for (std::size_t i = 0; i < sz; ++i) {
                    result.push_back(buckets_val[i].As<uint32_t>());
                }
            } else {
                // Old-style response: plain array of bucket IDs (no .buckets key)
                if (resp.IsArray()) {
                    const auto sz = resp.GetSize();
                    result.reserve(result.size() + sz);
                    for (std::size_t i = 0; i < sz; ++i) {
                        result.push_back(resp[i].As<uint32_t>());
                    }
                }
                break;  // old-style has no pagination
            }

            // Check next_from for pagination
            const auto& next_from_val = resp["next_from"];
            if (next_from_val.IsMissing() || next_from_val.IsNull()) {
                break;  // no more buckets
            }
            from = next_from_val.As<uint64_t>();
        } catch (const std::exception& ex) {
            LOG_WARNING() << "TopologyFetcher: buckets_discovery on RS failed: "
                          << ex.what();
            break;
        }
    }

    return result;
}

RoutingTable TopologyFetcher::RefreshFull() {
    const auto num_rs = static_cast<uint32_t>(pools_.size());
    if (num_rs == 0) {
        LOG_WARNING() << "TopologyFetcher::RefreshFull: no pools, using config";
        return BuildFromConfig();
    }

    // Query each RS master with vshard.storage.buckets_discovery to get
    // the full list of active/pinned buckets it owns.
    RoutingTable table;
    table.bucket_count = config_.bucket_count;
    table.bucket_to_rs.assign(config_.bucket_count, 0);
    table.replicasets = pools_;

    uint32_t discovered = 0;
    for (uint32_t i = 0; i < num_rs; ++i) {
        const auto buckets = DiscoverBucketsOnRS(*pools_[i]);
        const uint16_t rs_idx = static_cast<uint16_t>(i + 1);
        for (const auto bid : buckets) {
            if (bid >= 1 && bid <= config_.bucket_count) {
                table.bucket_to_rs[bid - 1] = rs_idx;
                ++discovered;
            }
        }
        LOG_INFO() << "TopologyFetcher::RefreshFull: RS " << (i + 1)
                   << " reports " << buckets.size() << " buckets";
    }

    if (discovered == 0) {
        LOG_WARNING() << "TopologyFetcher::RefreshFull: no buckets discovered "
                         "from any RS, falling back to static config";
        return BuildFromConfig();
    }

    // Fill gaps for any undiscovered buckets using static config distribution.
    // This handles buckets in SENDING/RECEIVING state that aren't reported by
    // buckets_discovery.
    uint32_t gaps = 0;
    for (uint32_t b = 0; b < config_.bucket_count; ++b) {
        if (table.bucket_to_rs[b] == 0) {
            ++gaps;
        }
    }

    if (gaps > 0) {
        LOG_INFO() << "TopologyFetcher::RefreshFull: " << gaps
                   << " buckets not discovered (in transit?), leaving unmapped";
    }

    LOG_INFO() << "TopologyFetcher::RefreshFull: discovered " << discovered
               << "/" << config_.bucket_count << " buckets across " << num_rs
               << " RS";
    return table;
}

TopologyFetcher::BucketDiscoveryResult TopologyFetcher::DiscoverBucket(
    uint32_t bucket_id) {
    const auto num_rs = static_cast<uint32_t>(pools_.size());
    BucketDiscoveryResult result;
    for (uint32_t i = 0; i < num_rs; ++i) {
        const auto probe = ProbeBucket(*pools_[i], bucket_id);
        switch (probe.status) {
            case BucketProbeStatus::kFound:
                result.rs_idx = static_cast<uint16_t>(i + 1);
                return result;
            case BucketProbeStatus::kWrongBucket:
            case BucketProbeStatus::kBackoff:
                break;
            case BucketProbeStatus::kUnreachableReplicaset:
                result.unreachable = true;
                result.unreachable_replicaset_id = pools_[i]->GetUuid();
                result.error_message = probe.error_message;
                break;
            case BucketProbeStatus::kOtherError:
                result.unreachable = false;
                result.unreachable_replicaset_id.clear();
                result.error_message = probe.error_message;
                break;
        }
    }
    return result;
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
