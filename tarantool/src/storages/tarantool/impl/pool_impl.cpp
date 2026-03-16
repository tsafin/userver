#include "pool_impl.hpp"

#include <optional>

#include <userver/clients/dns/resolver.hpp>
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

PoolImpl::PoolImpl(clients::dns::Resolver& resolver, PoolSettings settings)
    : drivers::impl::ConnectionPoolBase<Connection, PoolImpl>{
          settings.max_pool_size, kMaxSimultaneouslyConnecting},
      resolver_{resolver},
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
    const auto try_grow = [this] {
        if (AliveConnectionsCountApprox() < settings_.initial_pool_size) {
            try {
                PushConnection(engine::Deadline::FromDuration(
                    settings_.connect_timeout));
            } catch (const std::exception& ex) {
                LOG_ERROR() << "Tarantool: failed to create connection: " << ex;
            }
        }
    };

    // Use proper Acquire() so the given_away_semaphore_ is decremented.
    // TryPop() bypasses the semaphore, which allows alive_connections to
    // temporarily exceed max_pool_size and causes bounded_push to fail in
    // DoRelease, which would drop a connection that may have in-flight
    // pipelined requests.
    constexpr auto kMaintenanceAcquireTimeout = std::chrono::milliseconds{200};
    std::optional<ConnectionPtr> conn;
    try {
        conn.emplace(Acquire(
            engine::Deadline::FromDuration(kMaintenanceAcquireTimeout)));
    } catch (const std::exception&) {
        // Pool is fully busy or unavailable; skip this maintenance cycle.
        try_grow();
        return;
    }

    if (!(*conn)->IsBroken()) {
        try {
            (*conn)->Ping(engine::Deadline::FromDuration(
                settings_.connect_timeout));
        } catch (const std::exception& ex) {
            LOG_LIMITED_WARNING()
                << "Tarantool: ping failed for '"
                << settings_.endpoint.host << "': " << ex;
        }
        // AccountSuccess is called by PoolImpl::Release() if not broken.
    }
    // conn goes out of scope → ~ConnectionPtr() → Release() →
    // ReleaseConnection() → semaphore unlocked properly.

    try_grow();
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
        return std::make_unique<Connection>(resolver_, settings_.endpoint,
                                            settings_.auth, deadline);
    } catch (const std::exception&) {
        availability_monitor_.AccountFailure();
        throw;
    }
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
