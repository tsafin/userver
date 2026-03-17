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

    uint32_t GetCode() const noexcept { return code_; }
    const std::string& GetType() const noexcept { return type_; }

 private:
    uint32_t code_;
    std::string type_;
};

}  // namespace storages::tarantool::vshard

USERVER_NAMESPACE_END
