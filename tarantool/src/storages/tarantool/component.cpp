#include <userver/storages/tarantool/component.hpp>

#include <userver/clients/dns/component.hpp>
#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/utils/statistics/writer.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

#include <userver/storages/tarantool/cluster.hpp>

#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

namespace components {

Tarantool::Tarantool(const ComponentConfig& config,
                     const ComponentContext& context)
    : LoggableComponentBase{config, context},
      dns_{context.FindComponent<clients::dns::Component>()} {
    const auto& secdist =
        context.FindComponent<Secdist>().Get();
    const auto& settings_multi =
        secdist.Get<storages::tarantool::impl::TarantoolSettingsMulti>();
    const auto& settings =
        settings_multi.Get(
            storages::tarantool::impl::GetSecdistAlias(config));

    cluster_ = std::make_shared<storages::tarantool::Cluster>(
        dns_.GetResolver(), settings, config);

    auto& stats_storage =
        context.FindComponent<StatisticsStorage>();
    statistics_holder_ = stats_storage.GetStorage().RegisterWriter(
        "tarantool",
        [this](utils::statistics::Writer& writer) {
            if (cluster_) {
                cluster_->WriteStatistics(writer);
            }
        },
        {{"tarantool_alias",
          storages::tarantool::impl::GetSecdistAlias(config)}});
}

Tarantool::~Tarantool() {
    statistics_holder_.Unregister();
}

std::shared_ptr<storages::tarantool::Cluster> Tarantool::GetCluster() const {
    return cluster_;
}

yaml_config::Schema Tarantool::GetStaticConfigSchema() {
    return yaml_config::MergeSchemas<LoggableComponentBase>(R"(
type: object
description: Tarantool client component
additionalProperties: false
properties:
    secdist_alias:
        type: string
        description: key in secdist tarantool_settings
    initial_pool_size:
        type: integer
        description: connections created initially
        defaultDescription: 2
    max_pool_size:
        type: integer
        description: max connections per host
        defaultDescription: 10
    connect_timeout:
        type: string
        description: connect + handshake timeout
        defaultDescription: 2s
    queue_timeout:
        type: string
        description: wait for free connection timeout
        defaultDescription: 1s
)");
}

}  // namespace components

USERVER_NAMESPACE_END
