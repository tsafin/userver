#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>

#include <userver/components/component_fwd.hpp>
#include <userver/formats/json/value.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

struct AuthSettings final {
    std::string user{"guest"};
    std::string password;

    AuthSettings() = default;
    explicit AuthSettings(const formats::json::Value&);
};

struct EndpointSettings final {
    std::string host{"127.0.0.1"};
    uint16_t port{3301};
};

struct PoolSettings final {
    std::size_t initial_pool_size{2};
    std::size_t max_pool_size{10};
    std::chrono::milliseconds connect_timeout{2000};
    std::chrono::milliseconds queue_timeout{1000};

    EndpointSettings endpoint;
    AuthSettings auth;

    PoolSettings(const components::ComponentConfig&,
                 const EndpointSettings&,
                 const AuthSettings&);
};

struct TarantoolSettings final {
    std::vector<EndpointSettings> endpoints;
    AuthSettings auth;

    TarantoolSettings() = default;
    explicit TarantoolSettings(const formats::json::Value&);
};

class TarantoolSettingsMulti final {
 public:
    explicit TarantoolSettingsMulti(const formats::json::Value&);
    const TarantoolSettings& Get(const std::string& alias) const;

 private:
    std::unordered_map<std::string, TarantoolSettings> databases_;
};

std::string GetSecdistAlias(const components::ComponentConfig&);

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
