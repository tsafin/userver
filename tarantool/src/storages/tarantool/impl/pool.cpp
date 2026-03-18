#include "pool.hpp"

#include <userver/clients/dns/resolver.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/future_status.hpp>
#include <userver/engine/task/cancel.hpp>
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

    const bool is_call = (query.GetType() == Query::Type::kCall);
    auto& req_stats = is_call ? impl_->GetStatistics().calls
                              : impl_->GetStatistics().crud;
    ++req_stats.total;

    // Phase 1: Acquire the pool slot, send the request, then immediately
    // release the pool slot (pipelining). Releasing before wait_until allows
    // the same connection to carry multiple in-flight requests concurrently:
    // the reader task demultiplexes responses by sync_id, so each Future is
    // resolved independently. bounded_push always succeeds here because the
    // total in-pool + given-away count never exceeds max_pool_size.
    engine::Future<ExecutionResult> fut;
    try {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        fut = (*conn_ptr)->ExecuteAsync(deadline, query);
        conn_ptr.reset();  // return connection to pool immediately (pipelining)
    } catch (...) {
        ++req_stats.error;
        throw;
    }

    // Phase 2: Wait for response. The pool slot is already free; other
    // coroutines can reuse the same connection while we wait.
    const auto status = fut.wait_until(deadline);
    if (status == engine::FutureStatus::kTimeout) {
        ++req_stats.error;
        throw TarantoolException{"execute deadline expired"};
    }
    if (status != engine::FutureStatus::kReady) {
        ++req_stats.error;
        engine::current_task::CancellationPoint();
        throw TarantoolException{"execute cancelled"};
    }

    try {
        auto result = fut.get();
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

    auto& req_stats = impl_->GetStatistics().calls;
    ++req_stats.total;

    engine::Future<ExecutionResult> fut;
    try {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        fut = (*conn_ptr)->ForwardStorageCallAsync(info, deadline);
        conn_ptr.reset();  // release pool slot immediately (pipelining)
    } catch (...) {
        ++req_stats.error;
        throw;
    }

    const auto status = fut.wait_until(deadline);
    if (status == engine::FutureStatus::kTimeout) {
        ++req_stats.error;
        throw TarantoolException{"forward deadline expired"};
    }
    if (status != engine::FutureStatus::kReady) {
        ++req_stats.error;
        engine::current_task::CancellationPoint();
        throw TarantoolException{"forward cancelled"};
    }

    try {
        auto result = fut.get();
        if (!result.IsOk()) ++req_stats.error;
        return result;
    } catch (...) {
        ++req_stats.error;
        throw;
    }
}

void Pool::Ping(OptionalCommandControl cc) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline::FromDuration(impl_->GetSettings().queue_timeout);

    // Same pipelining pattern as Execute(): release the pool slot before
    // waiting for the response so many pings can be in-flight simultaneously.
    engine::Future<ExecutionResult> fut;
    {
        auto conn_ptr = std::make_unique<ConnectionPtr>(impl_->Acquire(deadline));
        fut = (*conn_ptr)->PingAsync(deadline);
    }  // conn_ptr destroyed here → pool slot released (pipelining)

    const auto status = fut.wait_until(deadline);
    if (status == engine::FutureStatus::kTimeout)
        throw TarantoolException{"ping deadline expired"};
    if (status != engine::FutureStatus::kReady) {
        engine::current_task::CancellationPoint();
        throw TarantoolException{"ping cancelled"};
    }
    auto result = fut.get();
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
