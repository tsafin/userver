#include "pool_impl.hpp"

#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>

#include <storages/tarantool/impl/connection.hpp>
#include <storages/tarantool/impl/connection_ptr.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

namespace {

constexpr std::size_t kMaxSimultaneouslyConnecting{5};
constexpr std::chrono::seconds kMaintenanceInterval{2};
constexpr std::chrono::seconds kPoolUnavailableThreshold{60};
static_assert(kPoolUnavailableThreshold > kMaintenanceInterval);

}  // namespace

bool PoolAvailabilityMonitor::IsAvailable() const {
    const auto last_success = last_success_.load();
    if (last_success == TimePoint{}) {
        return last_failure_.load() == TimePoint{};
    }
    const auto now = Clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(
               now - last_success) < kPoolUnavailableThreshold;
}

void PoolAvailabilityMonitor::AccountSuccess() noexcept {
    last_success_ = Clock::now();
}

void PoolAvailabilityMonitor::AccountFailure() noexcept {
    last_failure_ = Clock::now();
}

PoolImpl::PoolImpl(PoolSettings settings)
    : drivers::impl::ConnectionPoolBase<Connection, PoolImpl>{
          settings.max_pool_size, kMaxSimultaneouslyConnecting},
      settings_{std::move(settings)} {
    try {
        Init(settings_.initial_pool_size, settings_.connect_timeout);
    } catch (const std::exception&) {
        // Host may be temporarily unavailable; pool will retry on demand.
    }
}

PoolImpl::~PoolImpl() {
    StopMaintenance();
    Reset();
}

bool PoolImpl::IsAvailable() const {
    return availability_monitor_.IsAvailable();
}

ConnectionPtr PoolImpl::Acquire(engine::Deadline deadline) {
    auto pool_and_connection = AcquireConnection(deadline);
    return {std::move(pool_and_connection.pool_ptr),
            pool_and_connection.connection_ptr.release()};
}

void PoolImpl::Release(Connection* conn) {
    UASSERT(conn);
    if (!conn->IsBroken()) {
        availability_monitor_.AccountSuccess();
    }
    ReleaseConnection(ConnectionUniquePtr{conn});
}

stats::PoolStatistics& PoolImpl::GetStatistics() noexcept {
    return stats_;
}

const std::string& PoolImpl::GetHostName() const noexcept {
    return settings_.endpoint.host;
}

void PoolImpl::StartMaintenance() {
    maintenance_task_.Start(
        "tarantool_maintain",
        utils::PeriodicTask::Settings{
            std::chrono::duration_cast<std::chrono::milliseconds>(
                kMaintenanceInterval)},
        [this] { MaintainConnections(); });
}

void PoolImpl::StopMaintenance() {
    maintenance_task_.Stop();
}

void PoolImpl::MaintainConnections() {
    const auto try_push = [this] {
        try {
            PushConnection(engine::Deadline::FromDuration(
                settings_.connect_timeout));
        } catch (const std::exception& ex) {
            LOG_ERROR() << "Tarantool: failed to create connection: " << ex;
        }
    };

    auto conn_ptr = TryPop();
    if (!conn_ptr) {
        if (AliveConnectionsCountApprox() < settings_.initial_pool_size) {
            try_push();
        }
        return;
    }

    const bool broken = conn_ptr->IsBroken();
    if (!broken) {
        bool ping_ok = false;
        try {
            conn_ptr->Ping(engine::Deadline::FromDuration(
                settings_.connect_timeout));
            ping_ok = true;
        } catch (const std::exception& ex) {
            LOG_LIMITED_WARNING()
                << "Tarantool: ping failed for '"
                << settings_.endpoint.host << "': " << ex;
        }
        if (ping_ok) {
            availability_monitor_.AccountSuccess();
        }
    }
    DoRelease(std::move(conn_ptr));

    if (AliveConnectionsCountApprox() < settings_.initial_pool_size) {
        try_push();
    }
}

void PoolImpl::AccountConnectionAcquired() {
    ++stats_.connections.busy;
}

void PoolImpl::AccountConnectionReleased() {
    --stats_.connections.busy;
}

void PoolImpl::AccountConnectionCreated() {
    ++stats_.connections.created;
    ++stats_.connections.active;
}

void PoolImpl::AccountConnectionDestroyed() noexcept {
    ++stats_.connections.closed;
    --stats_.connections.active;
}

void PoolImpl::AccountOverload() {
    ++stats_.connections.overload;
}

PoolImpl::ConnectionUniquePtr PoolImpl::DoCreateConnection(
    engine::Deadline deadline) {
    try {
        return std::make_unique<Connection>(settings_.endpoint, settings_.auth,
                                           deadline);
    } catch (const std::exception&) {
        availability_monitor_.AccountFailure();
        throw;
    }
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
