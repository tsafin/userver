#include "pool.hpp"

#include <userver/clients/dns/resolver.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/tracing/span.hpp>
#include <userver/tracing/tags.hpp>

#include <storages/tarantool/impl/connection.hpp>
#include <storages/tarantool/impl/connection_ptr.hpp>
#include <storages/tarantool/impl/pool_impl.hpp>
#include <storages/tarantool/impl/tracing_tags.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

Pool::Pool(clients::dns::Resolver& resolver, PoolSettings settings)
    : impl_{std::make_shared<PoolImpl>(resolver, std::move(settings))} {
    impl_->StartMaintenance();
}

Pool::~Pool() = default;

ExecutionResult Pool::Execute(OptionalCommandControl cc, const Query& query) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

    auto conn_ptr = impl_->Acquire(deadline);

    const auto& scope = [&]() -> const std::string& {
        switch (query.GetType()) {
            case Query::Type::kCall:    return scopes::kCall;
            case Query::Type::kSelect:  return scopes::kSelect;
            case Query::Type::kInsert:  return scopes::kInsert;
            case Query::Type::kReplace: return scopes::kReplace;
            case Query::Type::kDelete:  return scopes::kDelete;
            case Query::Type::kUpdate:  return scopes::kUpdate;
            case Query::Type::kUpsert:  return scopes::kUpsert;
        }
        return scopes::kCall;
    }();

    tracing::Span span{scope};
    span.AddTag(tracing::kDatabaseType, "tarantool");
    span.AddTag(tracing::kDatabaseInstance, impl_->GetHostName());
    query.FillSpanTags(span);

    const bool is_call = (query.GetType() == Query::Type::kCall);
    auto& req_stats = is_call ? impl_->GetStatistics().calls
                              : impl_->GetStatistics().crud;
    ++req_stats.total;

    try {
        auto result = conn_ptr->Execute(cc, query);
        if (!result.IsOk()) {
            ++req_stats.error;
        }
        return result;
    } catch (...) {
        ++req_stats.error;
        throw;
    }
}

void Pool::WriteStatistics(utils::statistics::Writer& writer) const {
    writer.ValueWithLabels(impl_->GetStatistics(),
                           {{"tarantool_instance", impl_->GetHostName()}});
}

bool Pool::IsAvailable() const { return impl_->IsAvailable(); }

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
