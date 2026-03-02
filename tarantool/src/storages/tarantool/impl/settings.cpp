#include "settings.hpp"

#include <fmt/format.h>

#include <userver/components/component_config.hpp>
#include <userver/storages/secdist/helpers.hpp>
#include <userver/utils/assert.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

AuthSettings::AuthSettings(const formats::json::Value& doc)
    : user{doc["user"].As<std::string>("guest")},
      password{doc["password"].As<std::string>("")} {}

TarantoolSettings::TarantoolSettings(const formats::json::Value& doc) {
    auth = AuthSettings{doc};
    const uint16_t port = doc["port"].As<uint16_t>(3301);
    for (const auto& host : doc["hosts"]) {
        endpoints.push_back({host.As<std::string>(), port});
    }
    UINVARIANT(!endpoints.empty(), "Empty list of hosts in tarantool secdist");
}

TarantoolSettingsMulti::TarantoolSettingsMulti(
    const formats::json::Value& doc) {
    const auto settings = doc["tarantool_settings"];
    secdist::CheckIsObject(settings, "tarantool_settings");

    for (const auto& [name, value] : Items(settings)) {
        databases_.emplace(name, TarantoolSettings{value});
    }
}

const TarantoolSettings& TarantoolSettingsMulti::Get(
    const std::string& alias) const {
    const auto it = databases_.find(alias);
    if (it == databases_.end()) {
        throw std::runtime_error{
            fmt::format("tarantool alias '{}' not found in secdist", alias)};
    }
    return it->second;
}

PoolSettings::PoolSettings(const components::ComponentConfig& config,
                           const EndpointSettings& ep,
                           const AuthSettings& auth_settings)
    : initial_pool_size{
          config["initial_pool_size"].As<std::size_t>(2)},
      max_pool_size{config["max_pool_size"].As<std::size_t>(10)},
      connect_timeout{
          config["connect_timeout"].As<std::chrono::milliseconds>(
              std::chrono::milliseconds{2000})},
      queue_timeout{
          config["queue_timeout"].As<std::chrono::milliseconds>(
              std::chrono::milliseconds{1000})},
      endpoint{ep},
      auth{auth_settings} {}

std::string GetSecdistAlias(const components::ComponentConfig& config) {
    return config.HasMember("secdist_alias")
               ? config["secdist_alias"].As<std::string>()
               : config.Name();
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
