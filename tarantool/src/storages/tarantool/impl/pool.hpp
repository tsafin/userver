#pragma once

#include <memory>
#include <string>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/future.hpp>
#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>
#include <userver/utils/statistics/writer.hpp>

#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/connection_ptr.hpp>
#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

class PoolImpl;

class Pool final {
 public:
    Pool(clients::dns::Resolver& resolver, PoolSettings settings);
    ~Pool();

    Pool(const Pool&) = delete;
    Pool(Pool&&) = default;

    ExecutionResult Execute(OptionalCommandControl cc, const Query& query);
    engine::Future<ExecutionResult> ExecuteAsync(
        OptionalCommandControl cc, const Query& query);

    ConnectionPtr Acquire(engine::Deadline deadline);

    /// Forward a pre-parsed IPROTO CALL body to storage as vshard.storage.call.
    /// Builds body from kStorageCallBodyPrefix + raw TUPLE bytes (one memcpy);
    /// no msgpack re-encoding. Returns the raw IPROTO response.
    ExecutionResult ForwardStorageCall(const CallRouteInfo& info,
                                       OptionalCommandControl cc);

    /// Forward an IPROTO_VSHARD_CALL to storage.
    /// Builds a fixmap(4) header with VSHARD_BUCKET_ID + VSHARD_MODE plus
    /// the pre-formatted body (one memcpy).  No body scanning required.
    ExecutionResult ForwardVshardCall(uint32_t bucket_id, uint8_t mode,
                                      const uint8_t* body, std::size_t body_len,
                                      OptionalCommandControl cc);

    /// Send an IPROTO PING and wait for the empty response.
    /// Useful for latency/throughput benchmarking without any server-side work.
    void Ping(OptionalCommandControl cc = std::nullopt);

    // ---- No-span direct variants -------------------------------------------
    // These skip tracing::Span creation (no random span-ID generation) for
    // high-throughput internal callers such as the vshard proxy that manage
    // tracing at a higher level.  The caller is responsible for computing the
    // deadline before calling.

    ExecutionResult ExecuteDirect(engine::Deadline deadline,
                                  const Query& query);

    ExecutionResult ForwardStorageCallDirect(const CallRouteInfo& info,
                                             engine::Deadline deadline);

    ExecutionResult ForwardVshardCallDirect(uint32_t bucket_id, uint8_t mode,
                                            const uint8_t* body,
                                            std::size_t body_len,
                                            engine::Deadline deadline);

    void WriteStatistics(utils::statistics::Writer& writer) const;

    bool IsAvailable() const;

 private:
    std::shared_ptr<PoolImpl> impl_;
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
