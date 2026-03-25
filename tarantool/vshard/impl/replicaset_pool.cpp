#include <vshard/impl/replicaset_pool.hpp>

#include <userver/logging/log.hpp>
#include <userver/storages/tarantool/exceptions.hpp>

#include <vshard/impl/vshard_exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

std::string_view ToString(CallMode mode) noexcept {
    switch (mode) {
        case CallMode::kReadWrite:         return "rw";
        case CallMode::kReadOnly:          return "ro";
        case CallMode::kBestReadOnly:      return "bro";
        case CallMode::kBestReadOnlyError: return "bre";
    }
    return "unknown";
}

ReplicasetPool::ReplicasetPool(
    std::string uuid, std::string name,
    std::shared_ptr<storages::tarantool::impl::Pool> master,
    InstanceMeta master_meta)
    : uuid_{std::move(uuid)},
      name_{std::move(name)},
      master_{std::move(master)},
      master_meta_{std::move(master_meta)} {}

void ReplicasetPool::AddReplica(
    std::shared_ptr<storages::tarantool::impl::Pool> replica,
    InstanceMeta meta) {
    replicas_.push_back(std::move(replica));
    replica_metas_.push_back(std::move(meta));
}

storages::tarantool::impl::Pool& ReplicasetPool::SelectReplica() const {
    const auto sz = replicas_.size();
    for (std::size_t i = 0; i < sz; ++i) {
        const auto idx =
            replica_idx_.fetch_add(1, std::memory_order_relaxed) % sz;
        if (replicas_[idx]->IsAvailable()) {
            return *replicas_[idx];
        }
    }
    // All replicas unavailable: fall back to master
    return *master_;
}

storages::tarantool::ExecutionResult ReplicasetPool::Execute(
    CallMode mode,
    const storages::tarantool::Query& query,
    storages::tarantool::OptionalCommandControl cc) {

    switch (mode) {
        case CallMode::kReadWrite:
            return master_->Execute(cc, query);

        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->Execute(cc, query);
            }
            return SelectReplica().Execute(cc, query);

        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().Execute(cc, query);
    }
    return master_->Execute(cc, query);
}

engine::Future<storages::tarantool::ExecutionResult> ReplicasetPool::ExecuteAsync(
    CallMode mode,
    const storages::tarantool::Query& query,
    storages::tarantool::OptionalCommandControl cc) {

    switch (mode) {
        case CallMode::kReadWrite:
            return master_->ExecuteAsync(cc, query);

        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->ExecuteAsync(cc, query);
            }
            return SelectReplica().ExecuteAsync(cc, query);

        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().ExecuteAsync(cc, query);
    }
    return master_->ExecuteAsync(cc, query);
}

storages::tarantool::impl::ConnectionPtr ReplicasetPool::AcquireMaster(
    engine::Deadline deadline) {
    return master_->Acquire(deadline);
}

storages::tarantool::ExecutionResult ReplicasetPool::ForwardStorageCall(
    CallMode mode,
    const storages::tarantool::impl::CallRouteInfo& info,
    storages::tarantool::OptionalCommandControl cc) {
    switch (mode) {
        case CallMode::kReadWrite:
            return master_->ForwardStorageCall(info, cc);

        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->ForwardStorageCall(info, cc);
            }
            return SelectReplica().ForwardStorageCall(info, cc);

        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().ForwardStorageCall(info, cc);
    }
    return master_->ForwardStorageCall(info, cc);
}

storages::tarantool::ExecutionResult ReplicasetPool::ForwardVshardCall(
    const VshardCallInfo& info,
    const uint8_t* body, std::size_t body_len,
    storages::tarantool::OptionalCommandControl cc) {

    // Derive CallMode from the vshard mode byte (0=ro, 1=rw).
    const CallMode mode = (info.mode == 0) ? CallMode::kReadOnly
                                           : CallMode::kReadWrite;
    switch (mode) {
        case CallMode::kReadWrite:
            return master_->ForwardVshardCall(info.bucket_id, info.mode,
                                              body, body_len, cc);
        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->ForwardVshardCall(info.bucket_id, info.mode,
                                                  body, body_len, cc);
            }
            return SelectReplica().ForwardVshardCall(info.bucket_id, info.mode,
                                                     body, body_len, cc);
        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().ForwardVshardCall(info.bucket_id, info.mode,
                                                     body, body_len, cc);
    }
    return master_->ForwardVshardCall(info.bucket_id, info.mode,
                                      body, body_len, cc);
}

storages::tarantool::ExecutionResult ReplicasetPool::ExecuteDirect(
    CallMode mode,
    const storages::tarantool::Query& query,
    engine::Deadline deadline) {

    switch (mode) {
        case CallMode::kReadWrite:
            return master_->ExecuteDirect(deadline, query);

        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->ExecuteDirect(deadline, query);
            }
            return SelectReplica().ExecuteDirect(deadline, query);

        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().ExecuteDirect(deadline, query);
    }
    return master_->ExecuteDirect(deadline, query);
}

storages::tarantool::ExecutionResult ReplicasetPool::ForwardStorageCallDirect(
    CallMode mode,
    const storages::tarantool::impl::CallRouteInfo& info,
    engine::Deadline deadline) {

    switch (mode) {
        case CallMode::kReadWrite:
            return master_->ForwardStorageCallDirect(info, deadline);

        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->ForwardStorageCallDirect(info, deadline);
            }
            return SelectReplica().ForwardStorageCallDirect(info, deadline);

        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().ForwardStorageCallDirect(info, deadline);
    }
    return master_->ForwardStorageCallDirect(info, deadline);
}

storages::tarantool::ExecutionResult ReplicasetPool::ForwardVshardCallDirect(
    const VshardCallInfo& info,
    const uint8_t* body, std::size_t body_len,
    engine::Deadline deadline) {

    const CallMode mode = (info.mode == 0) ? CallMode::kReadOnly
                                           : CallMode::kReadWrite;
    switch (mode) {
        case CallMode::kReadWrite:
            return master_->ForwardVshardCallDirect(info.bucket_id, info.mode,
                                                    body, body_len, deadline);
        case CallMode::kReadOnly:
        case CallMode::kBestReadOnly:
            if (replicas_.empty()) {
                return master_->ForwardVshardCallDirect(info.bucket_id, info.mode,
                                                        body, body_len, deadline);
            }
            return SelectReplica().ForwardVshardCallDirect(info.bucket_id, info.mode,
                                                           body, body_len, deadline);
        case CallMode::kBestReadOnlyError:
            if (!IsReplicaAvailable()) {
                throw ReplicaUnavailableError{uuid_};
            }
            return SelectReplica().ForwardVshardCallDirect(info.bucket_id, info.mode,
                                                           body, body_len, deadline);
    }
    return master_->ForwardVshardCallDirect(info.bucket_id, info.mode,
                                            body, body_len, deadline);
}

bool ReplicasetPool::IsAvailable() const {
    if (master_ && master_->IsAvailable()) return true;
    for (const auto& r : replicas_) {
        if (r && r->IsAvailable()) return true;
    }
    return false;
}

bool ReplicasetPool::IsMasterAvailable() const {
    return master_ && master_->IsAvailable();
}

bool ReplicasetPool::HasReplica() const {
    return !replicas_.empty();
}

bool ReplicasetPool::IsReplicaAvailable() const {
    for (const auto& r : replicas_) {
        if (r && r->IsAvailable()) return true;
    }
    return false;
}

void ReplicasetPool::WriteStatistics(utils::statistics::Writer& writer) const {
    auto rs_writer = writer[uuid_];
    if (master_) {
        auto master_writer = rs_writer["master"];
        master_->WriteStatistics(master_writer);
    }
    for (std::size_t i = 0; i < replicas_.size(); ++i) {
        auto replica_writer = rs_writer["replica_" + std::to_string(i)];
        replicas_[i]->WriteStatistics(replica_writer);
    }
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
