#pragma once

/// @file vshard/impl/vshard_exceptions.hpp
/// @brief Exception hierarchy for vshard router errors.

#include <stdexcept>
#include <string>

#include <userver/storages/tarantool/exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard {

/// Base class for all vshard-level errors (distinct from raw IPROTO errors).
class VshardException : public storages::tarantool::TarantoolException {
    using TarantoolException::TarantoolException;
};

/// The bucket has migrated to a different replicaset.
/// The router automatically retries once after refreshing the routing table.
/// If the retry also fails, this exception propagates to the caller.
class MovedError final : public VshardException {
 public:
    MovedError(uint32_t bucket_id, std::string destination_uuid,
               const std::string& msg)
        : VshardException{msg},
          bucket_id_{bucket_id},
          destination_uuid_{std::move(destination_uuid)} {}

    uint32_t GetBucketId() const noexcept { return bucket_id_; }
    const std::string& GetDestinationUuid() const noexcept {
        return destination_uuid_;
    }

 private:
    uint32_t bucket_id_;
    std::string destination_uuid_;
};

/// The bucket is currently being transferred between replicasets.
/// The router retries with a short backoff.
class TransferError final : public VshardException {
 public:
    explicit TransferError(const std::string& msg) : VshardException{msg} {}
};

/// The routing table has no entry for the requested bucket_id.
/// Typically means topology discovery has not completed yet.
class NoReplicasetError final : public VshardException {
 public:
    explicit NoReplicasetError(uint32_t bucket_id)
        : VshardException{"No replicaset found for bucket_id=" +
                          std::to_string(bucket_id)},
          bucket_id_{bucket_id} {}

    uint32_t GetBucketId() const noexcept { return bucket_id_; }

 private:
    uint32_t bucket_id_;
};

/// Discovery could not probe a replicaset while resolving an unmapped bucket.
/// Mirrors Lua's UNREACHABLE_REPLICASET classification.
class UnreachableReplicasetError final : public VshardException {
 public:
    UnreachableReplicasetError(std::string replicaset_id, uint32_t bucket_id)
        : VshardException{"There is no active replicas in replicaset " +
                          replicaset_id},
          replicaset_id_{std::move(replicaset_id)},
          bucket_id_{bucket_id} {}

    const std::string& GetReplicasetId() const noexcept {
        return replicaset_id_;
    }
    uint32_t GetBucketId() const noexcept { return bucket_id_; }

 private:
    std::string replicaset_id_;
    uint32_t bucket_id_;
};

/// All replicasets were scanned during discovery, but none claimed the bucket.
/// Mirrors Lua's NO_ROUTE_TO_BUCKET classification.
class NoRouteToBucketError final : public VshardException {
 public:
    explicit NoRouteToBucketError(uint32_t bucket_id)
        : VshardException{"Bucket " + std::to_string(bucket_id) +
                          " cannot be found. Is rebalancing in progress?"},
          bucket_id_{bucket_id} {}

    uint32_t GetBucketId() const noexcept { return bucket_id_; }

 private:
    uint32_t bucket_id_;
};

/// All replicas are unavailable in BRE (best-read-only-error) mode.
class ReplicaUnavailableError final : public VshardException {
 public:
    explicit ReplicaUnavailableError(const std::string& replicaset_id)
        : VshardException{"All replicas unavailable for replicaset " +
                          replicaset_id},
          replicaset_id_{replicaset_id} {}

    const std::string& GetReplicasetId() const noexcept {
        return replicaset_id_;
    }

 private:
    std::string replicaset_id_;
};

/// Application-level error returned inside the vshard response envelope.
/// This wraps errors emitted by the storage Lua function itself.
class VshardStorageError final : public VshardException {
 public:
    VshardStorageError(uint32_t code, std::string type, const std::string& msg)
        : VshardException{msg},
          code_{code},
          type_{std::move(type)} {}

    VshardStorageError(
        uint32_t code, std::string type, std::string name,
        std::string msg,
        std::optional<std::string> replicaset = std::nullopt,
        std::optional<std::string> replica = std::nullopt,
        std::optional<std::string> master = std::nullopt)
        : VshardException{msg},
          code_{code},
          type_{std::move(type)},
          name_{std::move(name)},
          replicaset_{std::move(replicaset)},
          replica_{std::move(replica)},
          master_{std::move(master)} {}

    uint32_t GetCode() const noexcept { return code_; }
    const std::string& GetType() const noexcept { return type_; }
    const std::string& GetName() const noexcept { return name_; }
    const std::optional<std::string>& GetReplicaset() const noexcept {
        return replicaset_;
    }
    const std::optional<std::string>& GetReplica() const noexcept {
        return replica_;
    }
    const std::optional<std::string>& GetMaster() const noexcept {
        return master_;
    }

 private:
    uint32_t code_;
    std::string type_;
    std::string name_;
    std::optional<std::string> replicaset_;
    std::optional<std::string> replica_;
    std::optional<std::string> master_;
};

}  // namespace storages::tarantool::vshard

USERVER_NAMESPACE_END
