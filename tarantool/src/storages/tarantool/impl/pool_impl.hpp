#pragma once

#include <atomic>
#include <memory>

#include <userver/drivers/impl/connection_pool_base.hpp>
#include <userver/utils/datetime/steady_coarse_clock.hpp>
#include <userver/utils/periodic_task.hpp>

#include <storages/tarantool/impl/settings.hpp>
#include <storages/tarantool/stats/pool_statistics.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

class Connection;
class ConnectionPtr;

class PoolAvailabilityMonitor {
 public:
    using Clock = utils::datetime::SteadyCoarseClock;
    using TimePoint = Clock::time_point;

    bool IsAvailable() const;
    void AccountSuccess() noexcept;
    void AccountFailure() noexcept;

 private:
    std::atomic<TimePoint> last_success_{TimePoint{}};
    std::atomic<TimePoint> last_failure_{TimePoint{}};

    static_assert(std::atomic<TimePoint>::is_always_lock_free);
};

class PoolImpl final
    : public drivers::impl::ConnectionPoolBase<Connection, PoolImpl> {
 public:
    explicit PoolImpl(PoolSettings settings);
    ~PoolImpl();

    bool IsAvailable() const;

    ConnectionPtr Acquire(engine::Deadline deadline);
    void Release(Connection*);

    stats::PoolStatistics& GetStatistics() noexcept;
    const std::string& GetHostName() const noexcept;

    void StartMaintenance();

    const PoolSettings& GetSettings() const noexcept { return settings_; }

 private:
    friend class drivers::impl::ConnectionPoolBase<Connection, PoolImpl>;

    void AccountConnectionAcquired();
    void AccountConnectionReleased();
    void AccountConnectionCreated();
    void AccountConnectionDestroyed() noexcept;
    void AccountOverload();

    ConnectionUniquePtr DoCreateConnection(engine::Deadline deadline);

    void StopMaintenance();
    void MaintainConnections();

    PoolSettings settings_;
    stats::PoolStatistics stats_{};
    PoolAvailabilityMonitor availability_monitor_{};
    utils::PeriodicTask maintenance_task_;
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
