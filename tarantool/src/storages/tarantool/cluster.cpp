#include <userver/storages/tarantool/cluster.hpp>

#include <userver/clients/dns/resolver.hpp>
#include <userver/components/component_config.hpp>
#include <userver/logging/log.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

#include <storages/tarantool/impl/pool.hpp>
#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

Cluster::Cluster(clients::dns::Resolver& resolver,
                 const impl::TarantoolSettings& settings,
                 const components::ComponentConfig& config) {
    pools_.reserve(settings.endpoints.size());
    for (const auto& endpoint : settings.endpoints) {
        impl::PoolSettings pool_settings{config, endpoint, settings.auth};
        pools_.push_back(
            std::make_unique<impl::Pool>(resolver, std::move(pool_settings)));
    }
    if (pools_.empty()) {
        throw std::runtime_error{"Tarantool cluster has no endpoints"};
    }
}

Cluster::~Cluster() = default;

impl::Pool& Cluster::GetPool() const {
    const auto size = pools_.size();
    for (std::size_t i = 0; i < size; ++i) {
        const auto idx =
            current_pool_idx_.fetch_add(1, std::memory_order_relaxed) % size;
        if (pools_[idx]->IsAvailable()) {
            return *pools_[idx];
        }
    }
    throw NoAvailablePoolError{"All Tarantool pools are unavailable"};
}

ExecutionResult Cluster::DoExecute(OptionalCommandControl cc,
                                   const Query& query) {
    return GetPool().Execute(cc, query);
}

ExecutionResult Cluster::Call(std::string_view func_name,
                              formats::msgpack::ValueBuilder args,
                              OptionalCommandControl cc) {
    return DoExecute(cc, Query::Call(std::string{func_name}, std::move(args)));
}

ExecutionResult Cluster::Select(std::string_view space,
                                formats::msgpack::ValueBuilder key,
                                OptionalCommandControl cc) {
    return DoExecute(
        cc, Query::Select(std::string{space}, std::move(key)));
}

ExecutionResult Cluster::Insert(std::string_view space,
                                formats::msgpack::ValueBuilder tuple,
                                OptionalCommandControl cc) {
    return DoExecute(
        cc, Query::Insert(std::string{space}, std::move(tuple)));
}

ExecutionResult Cluster::Replace(std::string_view space,
                                 formats::msgpack::ValueBuilder tuple,
                                 OptionalCommandControl cc) {
    return DoExecute(
        cc, Query::Replace(std::string{space}, std::move(tuple)));
}

ExecutionResult Cluster::Delete(std::string_view space,
                                formats::msgpack::ValueBuilder key,
                                OptionalCommandControl cc) {
    return DoExecute(
        cc, Query::Delete(std::string{space}, std::move(key)));
}

ExecutionResult Cluster::Update(std::string_view space,
                                formats::msgpack::ValueBuilder key,
                                formats::msgpack::ValueBuilder ops,
                                OptionalCommandControl cc) {
    return DoExecute(
        cc, Query::Update(std::string{space}, std::move(key), std::move(ops)));
}

ExecutionResult Cluster::Upsert(std::string_view space,
                                formats::msgpack::ValueBuilder tuple,
                                formats::msgpack::ValueBuilder ops,
                                OptionalCommandControl cc) {
    return DoExecute(
        cc, Query::Upsert(std::string{space}, std::move(tuple),
                          std::move(ops)));
}

void Cluster::WriteStatistics(utils::statistics::Writer& writer) const {
    for (const auto& pool : pools_) {
        pool->WriteStatistics(writer);
    }
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
