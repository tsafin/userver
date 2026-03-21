/// @file vshard/vshard_proxy_service.cpp
/// @brief userver service entry point for the C++ vshard router proxy.
///
/// Components:
///   - `VshardProxyComponent` (`tarantool-vshard`): routing + storage pools
///   - `IprotoServer` (`iproto-vshard-server`): Tarantool IPROTO TCP listener

#include <userver/clients/dns/component.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/utils/daemon_run.hpp>

#include <vshard/impl/iproto_server.hpp>
#include <vshard/vshard_proxy_component.hpp>

int main(int argc, char* argv[]) {
    const auto component_list =
        USERVER_NAMESPACE::components::MinimalComponentList()
            .Append<USERVER_NAMESPACE::clients::dns::Component>()
            .Append<USERVER_NAMESPACE::components::Secdist>()
            .Append<USERVER_NAMESPACE::components::DefaultSecdistProvider>()
            .Append<USERVER_NAMESPACE::components::VshardProxyComponent>()
            .Append<USERVER_NAMESPACE::storages::tarantool::vshard::impl::IprotoServer>();
    return USERVER_NAMESPACE::utils::DaemonMain(argc, argv, component_list);
}
