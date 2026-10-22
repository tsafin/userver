#pragma once

/// @file vshard/impl/routing_table.hpp
/// @brief The bucket→replicaset mapping table (lock-free read via rcu::Variable).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <userver/rcu/rcu.hpp>

#include <vshard/impl/replicaset_pool.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// Immutable snapshot of the vshard routing table.
///
/// Indexed by `bucket_id - 1` (bucket IDs are 1-based in vshard).
/// `bucket_to_rs[i] == 0` means unknown / not yet discovered.
/// Values ≥1 are 1-based indices into `replicasets`.
struct RoutingTable {
    uint32_t bucket_count{0};

    /// bucket_to_rs[bucket_id - 1] → index+1 into replicasets (0 = unknown)
    std::vector<uint16_t> bucket_to_rs;

    /// All known replicasets. Index 0 in bucket_to_rs means "unknown".
    std::vector<std::shared_ptr<ReplicasetPool>> replicasets;

    /// Find the ReplicasetPool for a bucket, or nullptr if unknown.
    ReplicasetPool* FindReplicaset(uint32_t bucket_id) const noexcept {
        if (bucket_id < 1 || bucket_id > bucket_count) return nullptr;
        const auto idx = bucket_to_rs[bucket_id - 1];
        if (idx == 0) return nullptr;
        return replicasets[idx - 1].get();
    }

    /// Update the owner of a single bucket (used for fast MOVED handling).
    /// Returns false if the new_rs_idx is out of range.
    bool UpdateBucketOwner(uint32_t bucket_id, uint16_t new_rs_idx) noexcept {
        if (bucket_id < 1 || bucket_id > bucket_count) return false;
        if (new_rs_idx > replicasets.size()) return false;
        bucket_to_rs[bucket_id - 1] = new_rs_idx;
        return true;
    }

    /// Find replicaset index by UUID (1-based, 0 = not found).
    uint16_t FindReplicasetIndex(const std::string& uuid) const noexcept {
        for (std::size_t i = 0; i < replicasets.size(); ++i) {
            if (replicasets[i]->GetUuid() == uuid)
                return static_cast<uint16_t>(i + 1);
        }
        return 0;
    }
};

/// Holder for the live routing table.
///
/// Reads are lock-free via rcu::Variable::Read(); writes serialised externally
/// (only the discovery PeriodicTask and MOVED handler write).
class RoutingTableHolder final {
 public:
    RoutingTableHolder() : var_{RoutingTable{}} {}

    /// O(1) lock-free read snapshot.
    rcu::ReadablePtr<RoutingTable> Read() const { return var_.Read(); }

    /// Replace the entire table (full topology refresh).
    void Assign(RoutingTable table) {
        var_.Assign(std::move(table));
    }

    /// Atomic single-bucket update for MOVED handling.
    /// Applies the patch on the current table via StartWrite/Commit.
    void PatchBucketOwner(uint32_t bucket_id, const std::string& dest_uuid);

    /// Atomic single-bucket update by RS index (1-based).
    /// Used by on-demand bucket discovery when we already know the RS index.
    void PatchBucketOwnerByIndex(uint32_t bucket_id, uint16_t rs_idx);

 private:
    rcu::Variable<RoutingTable> var_;
};

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
