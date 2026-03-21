#pragma once

/// @file vshard/impl/iproto_server.hpp
/// @brief Tarantool IPROTO server component for the vshard router service.
///
/// Accepts TCP connections speaking the Tarantool binary protocol (net.box
/// compatible). Dispatches `vshard.router.callrw` and `vshard.router.callro`
/// CALL requests through a @ref storages::tarantool::vshard::VshardProxy.

#include <memory>
#include <string>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/tcp_acceptor_base.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/yaml_config/schema.hpp>

#include <vshard/impl/vshard_proxy.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// @brief IPROTO server that proxies vshard.router.callrw / vshard.router.callro
/// to a @ref VshardProxy.
///
/// Exposes the same wire protocol as the Lua vshard router, so that standard
/// net.box clients can connect and call:
///
///   conn:call('vshard.router.callrw', {bucket_id, func_name, args})
///   conn:call('vshard.router.callro', {bucket_id, func_name, args})
///
/// ## Static config options:
/// Inherits all options from `tcp-acceptor-base` (port, task_processor, …).
///
/// ## Example static config:
/// ```yaml
/// iproto-vshard-server:
///   port: 3306
///   task_processor: main-task-processor
/// ```
class IprotoServer final : public components::TcpAcceptorBase {
 public:
    static constexpr std::string_view kName = "iproto-vshard-server";

    IprotoServer(const components::ComponentConfig& config,
                 const components::ComponentContext& context);

    void ProcessSocket(engine::io::Socket&& sock) override;

    static yaml_config::Schema GetStaticConfigSchema();

 private:
    std::shared_ptr<VshardProxy> proxy_;
};

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
