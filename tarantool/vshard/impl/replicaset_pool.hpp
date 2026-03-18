#pragma once

/// @file vshard/impl/replicaset_pool.hpp
/// @brief One vshard replicaset: a master pool + N replica pools.

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>
#include <userver/utils/statistics/writer.hpp>

#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/pool.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// Read/write mode for vshard calls.
enum class CallMode {
    kReadWrite,         ///< Route to master (rw)
    kReadOnly,          ///< Round-robin replicas; fall back to master (ro)
    kBestReadOnly,      ///< Like kReadOnly but silently fall back (bro)
    kBestReadOnlyError, ///< Like kReadOnly but throw if no replica (bre)
};

std::string_view ToString(CallMode) noexcept;

/// A vshard replicaset: one master Pool + zero-or-more replica Pools.
///
/// Routing rules:
/// - `kReadWrite` / `kBestReadOnlyError` → always `master_`
/// - `kReadOnly` → round-robin `replicas_`; fall back to master if unavailable
/// - `kBestReadOnly` → round-robin `replicas_`; fall back to master silently
class ReplicasetPool final {
 public:
    explicit ReplicasetPool(std::string uuid,
                            std::shared_ptr<storages::tarantool::impl::Pool> master);

    /// Add a replica pool. Call before first request.
    void AddReplica(std::shared_ptr<storages::tarantool::impl::Pool> replica);

    /// Execute a query on the appropriate pool based on call mode.
    storages::tarantool::ExecutionResult Execute(
        CallMode mode,
        const storages::tarantool::Query& query,
        storages::tarantool::OptionalCommandControl cc = {});

    /// Forward a pre-parsed IPROTO CALL body as vshard.storage.call.
    /// No msgpack re-encoding: TUPLE bytes are copied once from info.
    storages::tarantool::ExecutionResult ForwardStorageCall(
        CallMode mode,
        const storages::tarantool::impl::CallRouteInfo& info,
        storages::tarantool::OptionalCommandControl cc = {});

    bool IsAvailable() const;
    const std::string& GetUuid() const noexcept { return uuid_; }

    void WriteStatistics(utils::statistics::Writer& writer) const;

 private:
    storages::tarantool::impl::Pool& SelectReplica() const;

    std::string uuid_;
    std::shared_ptr<storages::tarantool::impl::Pool> master_;
    std::vector<std::shared_ptr<storages::tarantool::impl::Pool>> replicas_;
    mutable std::atomic<std::size_t> replica_idx_{0};
};

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
