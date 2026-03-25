#include "pool.hpp"

#include <userver/clients/dns/resolver.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/tracing/span.hpp>
#include <userver/tracing/tags.hpp>

#include <storages/tarantool/impl/connection.hpp>
#include <storages/tarantool/impl/connection_ptr.hpp>
#include <storages/tarantool/impl/pool_impl.hpp>
#include <storages/tarantool/impl/tracing_tags.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

Pool::Pool(clients::dns::Resolver& resolver, PoolSettings settings)
    : impl_{std::make_shared<PoolImpl>(resolver, std::move(settings))} {
    impl_->StartMaintenance();
}

Pool::~Pool() = default;

ConnectionPtr Pool::Acquire(engine::Deadline deadline) {
    return impl_->Acquire(deadline);
}

ExecutionResult Pool::ExecuteDirect(engine::Deadline deadline,
                                     const Query& query) {
    const bool is_call = (query.GetType() == Query::Type::kCall);
    auto& req_stats = is_call ? impl_->GetStatistics().calls
                              : impl_->GetStatistics().crud;
    ++req_stats.total;

    std::shared_ptr<SyncPendingEntry> entry;
    try {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        entry = (*conn_ptr)->SyncExecute(deadline, query);
        conn_ptr.reset();
    } catch (...) {
        ++req_stats.error;
        throw;
    }
    try {
        auto result = Connection::CollectSyncEntry(std::move(entry), deadline);
        if (!result.IsOk()) ++req_stats.error;
        return result;
    } catch (...) {
        ++req_stats.error;
        throw;
    }
}

ExecutionResult Pool::Execute(OptionalCommandControl cc, const Query& query) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

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

    return ExecuteDirect(deadline, query);
}

engine::Future<ExecutionResult> Pool::ExecuteAsync(
    OptionalCommandControl cc, const Query& query) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

    tracing::Span span{scopes::kCall};
    span.AddTag(tracing::kDatabaseType, "tarantool");
    span.AddTag(tracing::kDatabaseInstance, impl_->GetHostName());
    query.FillSpanTags(span);

    auto& req_stats = impl_->GetStatistics().calls;
    ++req_stats.total;

    try {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        auto fut = (*conn_ptr)->ExecuteAsync(deadline, query);
        conn_ptr.reset();
        return fut;
    } catch (...) {
        ++req_stats.error;
        throw;
    }
}

ExecutionResult Pool::ForwardStorageCallDirect(const CallRouteInfo& info,
                                               engine::Deadline deadline) {
    auto& req_stats = impl_->GetStatistics().calls;
    ++req_stats.total;

    std::shared_ptr<SyncPendingEntry> entry;
    try {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        entry = (*conn_ptr)->SyncForwardStorageCall(info, deadline);
        conn_ptr.reset();
    } catch (...) {
        ++req_stats.error;
        throw;
    }
    try {
        auto result = Connection::CollectSyncEntry(std::move(entry), deadline);
        if (!result.IsOk()) ++req_stats.error;
        return result;
    } catch (...) {
        ++req_stats.error;
        throw;
    }
}

ExecutionResult Pool::ForwardStorageCall(const CallRouteInfo& info,
                                          OptionalCommandControl cc) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

    tracing::Span span{scopes::kCall};
    span.AddTag(tracing::kDatabaseType, "tarantool");
    span.AddTag(tracing::kDatabaseInstance, impl_->GetHostName());

    return ForwardStorageCallDirect(info, deadline);
}

ExecutionResult Pool::ForwardVshardCallDirect(uint32_t bucket_id, uint8_t mode,
                                               const uint8_t* body,
                                               std::size_t body_len,
                                               engine::Deadline deadline) {
    auto& req_stats = impl_->GetStatistics().calls;
    ++req_stats.total;

    std::shared_ptr<SyncPendingEntry> entry;
    try {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        entry = (*conn_ptr)->SyncForwardVshardCall(bucket_id, mode, body, body_len,
                                                   deadline);
        conn_ptr.reset();
    } catch (...) {
        ++req_stats.error;
        throw;
    }
    try {
        auto result = Connection::CollectSyncEntry(std::move(entry), deadline);
        if (!result.IsOk()) ++req_stats.error;
        return result;
    } catch (...) {
        ++req_stats.error;
        throw;
    }
}

ExecutionResult Pool::ForwardVshardCall(uint32_t bucket_id, uint8_t mode,
                                         const uint8_t* body,
                                         std::size_t body_len,
                                         OptionalCommandControl cc) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

    tracing::Span span{scopes::kCall};
    span.AddTag(tracing::kDatabaseType, "tarantool");
    span.AddTag(tracing::kDatabaseInstance, impl_->GetHostName());

    return ForwardVshardCallDirect(bucket_id, mode, body, body_len, deadline);
}

void Pool::Ping(OptionalCommandControl cc) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

    // Release the pool slot before waiting so many pings can be in-flight.
    std::shared_ptr<SyncPendingEntry> entry;
    {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        entry = (*conn_ptr)->SyncPing(deadline);
    }  // conn_ptr destroyed here → pool slot released (pipelining)

    auto result = Connection::CollectSyncEntry(std::move(entry), deadline);
    if (!result.IsOk())
        throw TarantoolException{"ping returned error"};
}

void Pool::WriteStatistics(utils::statistics::Writer& writer) const {
    writer.ValueWithLabels(impl_->GetStatistics(),
                           {{"tarantool_instance", impl_->GetHostName()}});
}

bool Pool::IsAvailable() const { return impl_->IsAvailable(); }

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
