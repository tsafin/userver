#pragma once

#include <memory>
#include <string>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>
#include <userver/utils/statistics/writer.hpp>

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

    void WriteStatistics(utils::statistics::Writer& writer) const;

    bool IsAvailable() const;

 private:
    std::shared_ptr<PoolImpl> impl_;
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
