#pragma once

#include <userver/utils/statistics/percentile.hpp>
#include <userver/utils/statistics/recentperiod.hpp>
#include <userver/utils/statistics/relaxed_counter.hpp>
#include <userver/utils/statistics/writer.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::stats {

using Counter = utils::statistics::RelaxedCounter<uint64_t>;
using Percentile =
    utils::statistics::Percentile<2048, uint64_t, 16, 256>;
using RecentPeriod =
    utils::statistics::RecentPeriod<Percentile, Percentile>;

struct PoolConnectionStatistics final {
    Counter created{};
    Counter closed{};
    Counter active{};
    Counter busy{};
    Counter overload{};
};

struct PoolRequestStatistics final {
    Counter total{};
    Counter error{};
    RecentPeriod timings{};
};

struct PoolStatistics final {
    PoolConnectionStatistics connections{};
    PoolRequestStatistics calls{};
    PoolRequestStatistics crud{};
};

void DumpMetric(utils::statistics::Writer& writer,
                const PoolStatistics& stats);
void DumpMetric(utils::statistics::Writer& writer,
                const PoolRequestStatistics& stats);
void DumpMetric(utils::statistics::Writer& writer,
                const PoolConnectionStatistics& stats);

}  // namespace storages::tarantool::stats

USERVER_NAMESPACE_END
