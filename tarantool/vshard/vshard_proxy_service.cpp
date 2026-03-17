/// @file vshard/vshard_proxy_service.cpp
/// @brief userver service entry point for the vshard proxy.

#include <unordered_map>

#include <userver/clients/dns/component.hpp>
#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/formats/common/items.hpp>
#include <userver/formats/json/value.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/utils/statistics/storage.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

#include <vshard/impl/vshard_proxy.hpp>

USERVER_NAMESPACE_BEGIN

namespace components {

// ---------------------------------------------------------------------------
// VshardProxyComponent
// ---------------------------------------------------------------------------

// clang-format off
/// @ingroup userver_components
///
/// @brief vshard router reimplemented in C++ on top of userver.
///
/// Manages the routing table and direct connections to vshard storage
/// replicasets. Exposes @ref storages::tarantool::vshard::VshardProxy.
///
/// ## Static options:
/// Name                           | Description                                              | Default
/// ------------------------------ | -------------------------------------------------------- | -------
/// secdist_alias                  | key in secdist `tarantool_vshard_settings`               | component name
/// topology_refresh_interval      | periodic full topology refresh interval                  | 60s
/// moved_refresh_min_interval     | minimum interval between MOVED-triggered refreshes       | 1s
/// max_moved_retries              | max retries per MOVED / TRANSFER per request             | 1
/// initial_pool_size              | connections created per node at startup                  | 2
/// max_pool_size                  | maximum connections per node                             | 10
/// connect_timeout                | TCP connect + IPROTO handshake timeout                   | 2s
/// queue_timeout                  | wait for a free connection                               | 1s
///
/// ## Secdist format (tarantool_vshard_settings):
/// @code{.json}
/// { "tarantool_vshard_settings": {
///     "my-vshard": {
///       "bucket_count": 3000,
///       "user": "guest",
///       "password": "",
///       "replicasets": [
///         { "uuid": "rs-001",
///           "nodes": [
///             {"host": "127.0.0.1", "port": 3301, "is_master": true},
///             {"host": "127.0.0.1", "port": 3302}
///           ]
///         }
///       ]
///     }
/// }}
/// @endcode
// clang-format on
class VshardProxyComponent final : public LoggableComponentBase {
 public:
    static constexpr std::string_view kName = "tarantool-vshard";

    VshardProxyComponent(const ComponentConfig& config,
                         const ComponentContext& context)
        : LoggableComponentBase{config, context},
          dns_{context.FindComponent<clients::dns::Component>()} {
        const auto& secdist = context.FindComponent<Secdist>().Get();

        const auto alias = config.HasMember("secdist_alias")
            ? config["secdist_alias"].As<std::string>()
            : std::string{config.Name()};
        // Read topology from secdist
        const auto& vshard_settings =
            secdist.Get<VshardSettingsMulti>().Get(alias);

        storages::tarantool::vshard::VshardProxySettings settings;
        settings.topology = vshard_settings;
        settings.topology_refresh_interval =
            config["topology_refresh_interval"].As<std::chrono::milliseconds>(
                std::chrono::milliseconds{60'000});
        settings.moved_refresh_min_interval =
            config["moved_refresh_min_interval"].As<std::chrono::milliseconds>(
                std::chrono::milliseconds{1'000});
        settings.max_moved_retries =
            config["max_moved_retries"].As<uint32_t>(1);

        proxy_ = std::make_shared<storages::tarantool::vshard::VshardProxy>(
            dns_.GetResolver(), config, std::move(settings));

        auto& stats = context.FindComponent<components::StatisticsStorage>();
        statistics_holder_ = stats.GetStorage().RegisterWriter(
            "tarantool_vshard",
            [this](utils::statistics::Writer& writer) {
                if (proxy_) proxy_->WriteStatistics(writer);
            },
            {{"vshard_alias", alias}});
    }

    ~VshardProxyComponent() override {
        statistics_holder_.Unregister();
    }

    std::shared_ptr<storages::tarantool::vshard::VshardProxy>
    GetProxy() const {
        return proxy_;
    }

    static yaml_config::Schema GetStaticConfigSchema() {
        return yaml_config::MergeSchemas<LoggableComponentBase>(R"(
type: object
description: vshard proxy component
additionalProperties: false
properties:
    secdist_alias:
        type: string
        description: key in secdist tarantool_vshard_settings
    topology_refresh_interval:
        type: string
        description: periodic routing table refresh interval
        defaultDescription: 60s
    moved_refresh_min_interval:
        type: string
        description: min interval between MOVED-triggered refreshes
        defaultDescription: 1s
    max_moved_retries:
        type: integer
        description: max MOVED/TRANSFER retries per request
        defaultDescription: 1
    initial_pool_size:
        type: integer
        description: connections per node at startup
        defaultDescription: 2
    max_pool_size:
        type: integer
        description: max connections per node
        defaultDescription: 10
    connect_timeout:
        type: string
        description: connect + handshake timeout
        defaultDescription: 2s
    queue_timeout:
        type: string
        description: time to wait for a free connection
        defaultDescription: 1s
)");
    }

 private:
    // Secdist adapter for vshard topology settings
    struct VshardSettingsMulti {
        explicit VshardSettingsMulti(const formats::json::Value& json) {
            for (const auto& [alias, val] :
                 formats::common::Items(json["tarantool_vshard_settings"])) {
                databases_[alias] =
                    storages::tarantool::vshard::impl::VshardTopologyConfig::Parse(
                        val);
            }
        }

        const storages::tarantool::vshard::impl::VshardTopologyConfig& Get(
            const std::string& alias) const {
            const auto it = databases_.find(alias);
            if (it == databases_.end()) {
                throw std::runtime_error{
                    "tarantool_vshard_settings: unknown alias '" + alias + "'"};
            }
            return it->second;
        }

        std::unordered_map<
            std::string,
            storages::tarantool::vshard::impl::VshardTopologyConfig>
            databases_;
    };

    clients::dns::Component& dns_;
    std::shared_ptr<storages::tarantool::vshard::VshardProxy> proxy_;
    utils::statistics::Entry statistics_holder_;
};

template <>
inline constexpr bool kHasValidate<VshardProxyComponent> = true;

}  // namespace components

USERVER_NAMESPACE_END

// ---------------------------------------------------------------------------
// Service entry point
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    const auto component_list =
        USERVER_NAMESPACE::components::MinimalServerComponentList()
            .Append<USERVER_NAMESPACE::components::VshardProxyComponent>();
    return USERVER_NAMESPACE::utils::DaemonMain(argc, argv, component_list);
}
