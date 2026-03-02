#pragma once

/// @file userver/storages/tarantool/component.hpp
/// @brief @copybrief components::Tarantool

#include <userver/components/loggable_component_base.hpp>
#include <userver/utils/statistics/storage.hpp>

#include <userver/storages/tarantool/fwd.hpp>

USERVER_NAMESPACE_BEGIN

namespace components {

// clang-format off
/// @ingroup userver_components
///
/// @brief Tarantool client component
///
/// Provides access to a Tarantool instance.
///
/// ## Static options:
/// Name                | Description                                | Default
/// ------------------- | ------------------------------------------ | -------
/// secdist_alias       | key in secdist `tarantool_settings`        | component name
/// initial_pool_size   | connections created at startup             | 2
/// max_pool_size       | maximum simultaneous connections per host  | 10
/// connect_timeout     | TCP connect + IPROTO handshake timeout     | 2s
/// queue_timeout       | time to wait for a free connection         | 1s
///
/// ## Secdist format:
/// @code{.json}
/// { "tarantool_settings": {
///     "my-alias": {
///       "hosts": ["127.0.0.1"], "port": 3301,
///       "user": "guest", "password": ""
///     }
/// }}
/// @endcode
// clang-format on

class Tarantool final : public LoggableComponentBase {
 public:
  static constexpr std::string_view kName = "tarantool";

  Tarantool(const ComponentConfig&, const ComponentContext&);
  ~Tarantool() override;

  std::shared_ptr<storages::tarantool::Cluster> GetCluster() const;

  static yaml_config::Schema GetStaticConfigSchema();

 private:
  std::shared_ptr<storages::tarantool::Cluster> cluster_;
  utils::statistics::Entry statistics_holder_;
};

template <>
inline constexpr bool kHasValidate<Tarantool> = true;

}  // namespace components

USERVER_NAMESPACE_END
