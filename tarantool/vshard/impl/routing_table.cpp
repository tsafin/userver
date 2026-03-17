#include <vshard/impl/routing_table.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

void RoutingTableHolder::PatchBucketOwner(uint32_t bucket_id,
                                           const std::string& dest_uuid) {
    // Copy current table, apply patch, swap.
    auto writer = var_.StartWrite();
    const auto rs_idx = writer->FindReplicasetIndex(dest_uuid);
    if (rs_idx != 0) {
        writer->UpdateBucketOwner(bucket_id, rs_idx);
    }
    writer.Commit();
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
