/// Functional test service for userver Tarantool connector.
///
/// Exposes four HTTP endpoints backed by a Tarantool 'kv' space:
///   GET    /kv?id=N        → select by id, return {"id":N,"value":"..."}
///   POST   /kv?id=N&value=V → insert tuple, return 200 OK
///   PUT    /kv?id=N&value=V → replace tuple, return 200 OK
///   DELETE /kv?id=N        → delete by id, return 200 OK

#include <userver/utest/using_namespace_userver.hpp>

#include <userver/clients/dns/component.hpp>
#include <userver/clients/http/component.hpp>
#include <userver/clients/http/component_list.hpp>
#include <userver/clients/http/middlewares/pipeline_component.hpp>
#include <userver/components/component.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/server/handlers/tests_control.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/storages/tarantool/cluster.hpp>
#include <userver/storages/tarantool/component.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/daemon_run.hpp>

namespace tarantool_test {

class HandlerKv final : public server::handlers::HttpHandlerBase {
 public:
    static constexpr const char* kName = "handler-kv";

    HandlerKv(const components::ComponentConfig& config,
              const components::ComponentContext& context)
        : server::handlers::HttpHandlerBase{config, context},
          tnt_{context
                   .FindComponent<components::Tarantool>("tarantool-database")
                   .GetCluster()} {}

    std::string HandleRequestThrow(
        const server::http::HttpRequest& request,
        server::request::RequestContext&) const override {

        const auto id_str = request.GetArg("id");
        if (id_str.empty()) {
            request.GetHttpResponse().SetStatus(
                server::http::HttpStatus::kBadRequest);
            return "missing id";
        }
        const uint64_t id = std::stoull(id_str);

        const auto method = request.GetMethod();

        if (method == server::http::HttpMethod::kGet) {
            auto res = tnt_->Select(
                "kv",
                formats::json::ValueBuilder{formats::json::MakeArray(id)}
                    .ExtractValue(),
                {});
            const auto& data = res.GetData();
            if (!data.IsArray() || data.GetSize() == 0 ||
                data[0].GetSize() == 0) {
                request.GetHttpResponse().SetStatus(
                    server::http::HttpStatus::kNotFound);
                return "not found";
            }
            const auto& tuple = data[0];
            return formats::json::ToString(
                formats::json::ValueBuilder{
                    formats::json::MakeObject(
                        "id", tuple[0].As<uint64_t>(),
                        "value", tuple[1].As<std::string>())}
                    .ExtractValue());

        } else if (method == server::http::HttpMethod::kPost) {
            const auto value = request.GetArg("value");
            tnt_->Insert(
                "kv",
                formats::json::ValueBuilder{formats::json::MakeArray(id, value)}
                    .ExtractValue(),
                {});
            return "ok";

        } else if (method == server::http::HttpMethod::kPut) {
            const auto value = request.GetArg("value");
            tnt_->Replace(
                "kv",
                formats::json::ValueBuilder{formats::json::MakeArray(id, value)}
                    .ExtractValue(),
                {});
            return "ok";

        } else if (method == server::http::HttpMethod::kDelete) {
            tnt_->Delete(
                "kv",
                formats::json::ValueBuilder{formats::json::MakeArray(id)}
                    .ExtractValue(),
                {});
            return "ok";
        }

        request.GetHttpResponse().SetStatus(
            server::http::HttpStatus::kMethodNotAllowed);
        return "method not allowed";
    }

 private:
    storages::tarantool::ClusterPtr tnt_;
};

}  // namespace tarantool_test

int main(int argc, char* argv[]) {
    const auto components_list =
        components::MinimalServerComponentList()
            .Append<tarantool_test::HandlerKv>()
            .Append<components::Tarantool>("tarantool-database")
            .Append<components::TestsuiteSupport>()
            .Append<server::handlers::TestsControl>()
            .AppendComponentList(clients::http::ComponentList())
            .Append<clients::dns::Component>()
            .Append<components::Secdist>()
            .Append<components::DefaultSecdistProvider>();

    return utils::DaemonMain(argc, argv, components_list);
}
