#include "pool_statistics.hpp"

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::stats {

void DumpMetric(utils::statistics::Writer& writer,
                const PoolConnectionStatistics& stats) {
    writer["created"] = stats.created;
    writer["closed"] = stats.closed;
    writer["active"] = stats.active;
    writer["busy"] = stats.busy;
    writer["overload"] = stats.overload;
}

void DumpMetric(utils::statistics::Writer& writer,
                const PoolRequestStatistics& stats) {
    writer["total"] = stats.total;
    writer["error"] = stats.error;
    writer["timings"] = stats.timings;
}

void DumpMetric(utils::statistics::Writer& writer,
                const PoolStatistics& stats) {
    writer["connections"] = stats.connections;
    writer["calls"] = stats.calls;
    writer["crud"] = stats.crud;
}

}  // namespace storages::tarantool::stats

USERVER_NAMESPACE_END
