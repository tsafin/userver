#include <vshard/impl/vshard_proxy.hpp>

#include <chrono>
#include <cstdint>

#include <userver/engine/deadline.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/utils/async.hpp>

#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/msgpack_constants.hpp>
#include <vshard/impl/iproto_vshard_frames.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard {

VshardProxy::VshardProxy(clients::dns::Resolver& resolver,
                         const components::ComponentConfig& pool_config,
                         VshardProxySettings settings)
    : calculator_{settings.topology.bucket_count},
      settings_{std::move(settings)},
      fetcher_{std::make_unique<impl::TopologyFetcher>(
          resolver, pool_config, settings_.topology)} {
    // Build initial routing table synchronously at startup.
    routing_table_.Assign(fetcher_->BuildFromConfig());    StartRefreshTask();
}

VshardProxy::~VshardProxy() {
    refresh_task_.Stop();
}

void VshardProxy::StartRefreshTask() {
    refresh_task_.Start(
        "vshard_topology_refresh",
        utils::PeriodicTask::Settings{settings_.topology_refresh_interval},
        [this] {
            try {
                routing_table_.Assign(fetcher_->RefreshFull());
            } catch (const std::exception& ex) {
                LOG_WARNING() << "vshard topology refresh failed: " << ex.what();
            }
        });
}

// ---------------------------------------------------------------------------
// Hash-based routing wrappers
// ---------------------------------------------------------------------------

formats::msgpack::Value VshardProxy::CallRW(
    std::string_view sharding_key, std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {
    return DoCall(ComputeBucketId(sharding_key), impl::CallMode::kReadWrite,
                  func, std::move(args), cc);
}

formats::msgpack::Value VshardProxy::CallRO(
    std::string_view sharding_key, std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {
    return DoCall(ComputeBucketId(sharding_key), impl::CallMode::kReadOnly,
                  func, std::move(args), cc);
}

formats::msgpack::Value VshardProxy::CallRW(
    int64_t sharding_key, std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {
    return DoCall(ComputeBucketId(sharding_key), impl::CallMode::kReadWrite,
                  func, std::move(args), cc);
}

formats::msgpack::Value VshardProxy::CallRO(
    int64_t sharding_key, std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {
    return DoCall(ComputeBucketId(sharding_key), impl::CallMode::kReadOnly,
                  func, std::move(args), cc);
}

formats::msgpack::Value VshardProxy::Call(
    BucketId bucket_id, impl::CallMode mode, std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {
    return DoCall(bucket_id, mode, func, std::move(args), cc);
}

formats::msgpack::Value VshardProxy::CallRaw(
    BucketId bucket_id, impl::CallMode mode, std::string_view func,
    const uint8_t* args_data, std::size_t args_len,
    storages::tarantool::OptionalCommandControl cc) {
    auto query = BuildStorageCallQueryRaw(bucket_id, mode, func, args_data, args_len);
    return DoCallWithQuery(bucket_id, mode, query, cc);
}

// ---------------------------------------------------------------------------
// Core call implementation with MOVED/TRANSFER retry
// ---------------------------------------------------------------------------

storages::tarantool::Query VshardProxy::BuildStorageCallQuery(
    BucketId bucket_id, impl::CallMode mode, std::string_view func,
    formats::msgpack::ValueBuilder args) const {
    // vshard.storage.call signature: (bucket_id, mode, func_name, args)
    const std::string_view vshard_mode =
        (mode == impl::CallMode::kReadWrite) ? "write" : "read";
    formats::msgpack::ValueBuilder tuple_args;
    tuple_args.PushBack(formats::msgpack::ValueBuilder{bucket_id});
    tuple_args.PushBack(formats::msgpack::ValueBuilder{std::string{vshard_mode}});
    tuple_args.PushBack(formats::msgpack::ValueBuilder{std::string{func}});
    tuple_args.PushBack(std::move(args));

    return storages::tarantool::Query::Call("vshard.storage.call",
                                             std::move(tuple_args));
}

storages::tarantool::Query VshardProxy::BuildStorageCallQueryRaw(
    BucketId bucket_id, impl::CallMode mode, std::string_view func,
    const uint8_t* args_data, std::size_t args_len) const {
    // Build the msgpack array [bucket_id, mode_str, func_name, args_array]
    // manually, copying args_data verbatim — no Value tree constructed.
    const std::string_view vshard_mode =
        (mode == impl::CallMode::kReadWrite) ? "write" : "read";

    std::vector<uint8_t> buf;
    buf.reserve(1 + 5 + 1 + vshard_mode.size() + 1 + func.size() + args_len);

    // fixarray(4)
    buf.push_back(static_cast<uint8_t>(mp::kFixArrayMin | 4));

    // [0] bucket_id as uint32
    buf.push_back(mp::kUint32);
    buf.push_back(static_cast<uint8_t>(bucket_id >> 24));
    buf.push_back(static_cast<uint8_t>(bucket_id >> 16));
    buf.push_back(static_cast<uint8_t>(bucket_id >>  8));
    buf.push_back(static_cast<uint8_t>(bucket_id));

    // [1] mode string (always ≤ 5 chars — fits in fixstr)
    buf.push_back(static_cast<uint8_t>(mp::kFixStrMin | vshard_mode.size()));
    buf.insert(buf.end(), vshard_mode.begin(), vshard_mode.end());

    // [2] func name (fixstr or str8)
    if (func.size() <= 31) {
        buf.push_back(static_cast<uint8_t>(mp::kFixStrMin | func.size()));
    } else {
        buf.push_back(mp::kStr8);
        buf.push_back(static_cast<uint8_t>(func.size()));
    }
    buf.insert(buf.end(),
               reinterpret_cast<const uint8_t*>(func.data()),
               reinterpret_cast<const uint8_t*>(func.data()) + func.size());

    // [3] args: already msgpack-encoded value, copy verbatim
    buf.insert(buf.end(), args_data, args_data + args_len);

    return storages::tarantool::Query::WithRawArgs(
        storages::tarantool::Query::Type::kCall,
        "vshard.storage.call",
        std::move(buf));
}

formats::msgpack::Value VshardProxy::DoCall(
    BucketId bucket_id, impl::CallMode mode, std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {

    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    auto query = BuildStorageCallQuery(bucket_id, mode, func, std::move(args));
    return DoCallWithQuery(bucket_id, mode, query, cc);
}

formats::msgpack::Value VshardProxy::DoCallWithQuery(
    BucketId bucket_id, impl::CallMode mode,
    const storages::tarantool::Query& query,
    storages::tarantool::OptionalCommandControl cc) {

    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    // Snapshot routing table (lock-free read)
    auto snapshot = routing_table_.Read();
    impl::ReplicasetPool* rs = snapshot->FindReplicaset(bucket_id);
    if (!rs) throw NoReplicasetError{bucket_id};

    uint32_t attempt = 0;
    while (true) {
        storages::tarantool::ExecutionResult raw;
        try {
            raw = rs->Execute(mode, query, cc);
        } catch (const storages::tarantool::TarantoolException& ex) {
            LOG_WARNING() << "vshard.storage.call network error (bucket="
                          << bucket_id << " attempt=" << attempt
                          << "): " << ex.what();
            throw;
        }

        // Decode vshard envelope
        impl::VshardEnvelope env;
        try {
            env = impl::DecodeEnvelope(raw);
        } catch (const storages::tarantool::CommandException& ex) {
            throw;
        }

        if (env.vshard_error.IsNull()) {
            return env.app_result;
        }

        // MOVED: bucket migrated to another replicaset, or routing table wrong
        if (env.vshard_error.IsWrongBucket()) {
            if (attempt >= settings_.max_moved_retries) {
                throw MovedError{
                    bucket_id,
                    env.vshard_error.destination_uuid.value_or(""),
                    env.vshard_error.message};
            }
            ++attempt;

            // Apply fast local patch if destination is known
            if (env.vshard_error.destination_uuid.has_value()) {
                const auto& dest = *env.vshard_error.destination_uuid;
                routing_table_.PatchBucketOwner(bucket_id, dest);
                snapshot = routing_table_.Read();
                rs = snapshot->FindReplicaset(bucket_id);
            }

            // No destination UUID means our routing table is simply wrong
            // (e.g. static config assigned wrong RS to this bucket range).
            // Always trigger a live-probe refresh in this case.
            if (!env.vshard_error.destination_uuid.has_value() || !rs) {
                const auto now = std::chrono::steady_clock::now();
                if (now - last_moved_refresh_ >
                    settings_.moved_refresh_min_interval) {
                    last_moved_refresh_ = now;
                    try {
                        routing_table_.Assign(fetcher_->RefreshFull());
                    } catch (const std::exception& ex) {
                        LOG_WARNING() << "vshard MOVED refresh failed: " << ex.what();
                    }
                }
                // Always re-read: once one fiber triggers RefreshFull all
                // rate-limited fibers also pick up the updated routing table.
                snapshot = routing_table_.Read();
                rs = snapshot->FindReplicaset(bucket_id);
                if (!rs) throw NoReplicasetError{bucket_id};
            }
            continue;  // retry
        }

        // TRANSFER: bucket is mid-migration, short backoff retry
        if (env.vshard_error.IsTransfer()) {
            if (attempt >= settings_.max_moved_retries) {
                throw TransferError{env.vshard_error.message};
            }
            ++attempt;
            engine::SleepFor(std::chrono::milliseconds{100});
            continue;
        }

        // NON_MASTER: our routing table is stale — update and retry once
        if (env.vshard_error.IsNonMaster()) {
            if (attempt >= settings_.max_moved_retries) {
                throw VshardStorageError{
                    env.vshard_error.code, "NON_MASTER",
                    env.vshard_error.message};
            }
            ++attempt;
            try {
                routing_table_.Assign(fetcher_->RefreshFull());
            } catch (const std::exception& ex) {
                LOG_WARNING() << "vshard NON_MASTER refresh failed: " << ex.what();
            }
            snapshot = routing_table_.Read();
            rs = snapshot->FindReplicaset(bucket_id);
            if (!rs) throw NoReplicasetError{bucket_id};
            continue;
        }

        // Any other vshard error
        throw VshardStorageError{
            env.vshard_error.code,
            std::to_string(static_cast<uint32_t>(env.vshard_error.type)),
            env.vshard_error.message};
    }
}

// ---------------------------------------------------------------------------
// ForwardCall — zero-copy routing
// ---------------------------------------------------------------------------

formats::msgpack::Value VshardProxy::ForwardCall(
    const uint8_t* iproto_body,
    std::size_t body_len,
    impl::CallMode mode,
    storages::tarantool::OptionalCommandControl cc) {

    const auto route = storages::tarantool::impl::ParseCallForRoute(iproto_body, body_len);
    if (!route) {
        throw VshardException{
            "ForwardCall: malformed IPROTO body or invalid bucket_id"};
    }
    const BucketId bucket_id = route->bucket_id;

    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    auto snapshot = routing_table_.Read();
    impl::ReplicasetPool* rs = snapshot->FindReplicaset(bucket_id);
    if (!rs) throw NoReplicasetError{bucket_id};

    uint32_t attempt = 0;
    while (true) {
        storages::tarantool::ExecutionResult raw;
        try {
            raw = rs->ForwardStorageCall(mode, *route, cc);
        } catch (const storages::tarantool::TarantoolException& ex) {
            LOG_WARNING() << "vshard.storage.call forward error (bucket="
                          << bucket_id << " attempt=" << attempt
                          << "): " << ex.what();
            throw;
        }

        impl::VshardEnvelope env;
        try {
            env = impl::DecodeEnvelope(raw);
        } catch (const storages::tarantool::CommandException&) {
            throw;
        }

        if (env.vshard_error.IsNull()) {
            return env.app_result;
        }

        if (env.vshard_error.IsWrongBucket()) {
            if (attempt >= settings_.max_moved_retries) {
                throw MovedError{
                    bucket_id,
                    env.vshard_error.destination_uuid.value_or(""),
                    env.vshard_error.message};
            }
            ++attempt;

            if (env.vshard_error.destination_uuid.has_value()) {
                routing_table_.PatchBucketOwner(
                    bucket_id, *env.vshard_error.destination_uuid);
                snapshot = routing_table_.Read();
                rs = snapshot->FindReplicaset(bucket_id);
            }

            if (!env.vshard_error.destination_uuid.has_value() || !rs) {
                const auto now = std::chrono::steady_clock::now();
                if (now - last_moved_refresh_ >
                    settings_.moved_refresh_min_interval) {
                    last_moved_refresh_ = now;
                    try {
                        routing_table_.Assign(fetcher_->RefreshFull());
                    } catch (const std::exception& ex) {
                        LOG_WARNING() << "vshard MOVED refresh failed: "
                                      << ex.what();
                    }
                }
                // Always re-read: once one fiber triggers RefreshFull all
                // rate-limited fibers also get the updated routing table.
                snapshot = routing_table_.Read();
                rs = snapshot->FindReplicaset(bucket_id);
                if (!rs) throw NoReplicasetError{bucket_id};
            }
            continue;
        }

        if (env.vshard_error.IsTransfer()) {
            if (attempt >= settings_.max_moved_retries) {
                throw TransferError{env.vshard_error.message};
            }
            ++attempt;
            engine::SleepFor(std::chrono::milliseconds{100});
            continue;
        }

        if (env.vshard_error.IsNonMaster()) {
            if (attempt >= settings_.max_moved_retries) {
                throw VshardStorageError{
                    env.vshard_error.code, "NON_MASTER",
                    env.vshard_error.message};
            }
            ++attempt;
            try {
                routing_table_.Assign(fetcher_->RefreshFull());
            } catch (const std::exception& ex) {
                LOG_WARNING() << "vshard NON_MASTER refresh failed: "
                              << ex.what();
            }
            snapshot = routing_table_.Read();
            rs = snapshot->FindReplicaset(bucket_id);
            if (!rs) throw NoReplicasetError{bucket_id};
            continue;
        }

        throw VshardStorageError{
            env.vshard_error.code,
            std::to_string(static_cast<uint32_t>(env.vshard_error.type)),
            env.vshard_error.message};
    }
}

// ---------------------------------------------------------------------------
// ForwardVshardCall — header-only parse, zero body scan (IPROTO_VSHARD_CALL)
// ---------------------------------------------------------------------------

formats::msgpack::Value VshardProxy::ForwardVshardCall(
    const uint8_t* iproto_header,
    std::size_t header_len,
    const uint8_t* body,
    std::size_t body_len,
    storages::tarantool::OptionalCommandControl cc) {

    const auto info = impl::ParseVshardCallHeader(iproto_header, header_len);
    if (!info) {
        throw VshardException{
            "ForwardVshardCall: malformed IPROTO_VSHARD_CALL header"};
    }
    const BucketId bucket_id = info->bucket_id;

    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    auto snapshot = routing_table_.Read();
    impl::ReplicasetPool* rs = snapshot->FindReplicaset(bucket_id);
    if (!rs) throw NoReplicasetError{bucket_id};

    uint32_t attempt = 0;
    while (true) {
        storages::tarantool::ExecutionResult raw;
        try {
            raw = rs->ForwardVshardCall(*info, body, body_len, cc);
        } catch (const storages::tarantool::TarantoolException& ex) {
            LOG_WARNING() << "IPROTO_VSHARD_CALL forward error (bucket="
                          << bucket_id << " attempt=" << attempt
                          << "): " << ex.what();
            throw;
        }

        impl::VshardEnvelope env;
        try {
            env = impl::DecodeEnvelope(raw);
        } catch (const storages::tarantool::CommandException&) {
            throw;
        }

        if (env.vshard_error.IsNull()) {
            return env.app_result;
        }

        if (env.vshard_error.IsWrongBucket()) {
            if (attempt >= settings_.max_moved_retries) {
                throw MovedError{
                    bucket_id,
                    env.vshard_error.destination_uuid.value_or(""),
                    env.vshard_error.message};
            }
            ++attempt;

            if (env.vshard_error.destination_uuid.has_value()) {
                routing_table_.PatchBucketOwner(
                    bucket_id, *env.vshard_error.destination_uuid);
                snapshot = routing_table_.Read();
                rs = snapshot->FindReplicaset(bucket_id);
            }

            if (!env.vshard_error.destination_uuid.has_value() || !rs) {
                const auto now = std::chrono::steady_clock::now();
                if (now - last_moved_refresh_ >
                    settings_.moved_refresh_min_interval) {
                    last_moved_refresh_ = now;
                    try {
                        routing_table_.Assign(fetcher_->RefreshFull());
                    } catch (const std::exception& ex) {
                        LOG_WARNING() << "vshard MOVED refresh failed: "
                                      << ex.what();
                    }
                }
                // Always re-read: once one fiber triggers RefreshFull all
                // rate-limited fibers also get the updated routing table.
                snapshot = routing_table_.Read();
                rs = snapshot->FindReplicaset(bucket_id);
                if (!rs) throw NoReplicasetError{bucket_id};
            }
            continue;
        }

        if (env.vshard_error.IsTransfer()) {
            if (attempt >= settings_.max_moved_retries) {
                throw TransferError{env.vshard_error.message};
            }
            ++attempt;
            engine::SleepFor(std::chrono::milliseconds{100});
            continue;
        }

        if (env.vshard_error.IsNonMaster()) {
            if (attempt >= settings_.max_moved_retries) {
                throw VshardStorageError{
                    env.vshard_error.code, "NON_MASTER",
                    env.vshard_error.message};
            }
            ++attempt;
            try {
                routing_table_.Assign(fetcher_->RefreshFull());
            } catch (const std::exception& ex) {
                LOG_WARNING() << "vshard NON_MASTER refresh failed: "
                              << ex.what();
            }
            snapshot = routing_table_.Read();
            rs = snapshot->FindReplicaset(bucket_id);
            if (!rs) throw NoReplicasetError{bucket_id};
            continue;
        }

        throw VshardStorageError{
            env.vshard_error.code,
            std::to_string(static_cast<uint32_t>(env.vshard_error.type)),
            env.vshard_error.message};
    }
}

std::vector<formats::msgpack::Value> VshardProxy::MapCallRW(
    std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {

    auto snapshot = routing_table_.Read();
    std::vector<engine::TaskWithResult<formats::msgpack::Value>> tasks;
    tasks.reserve(snapshot->replicasets.size());

    // Serialize args once; each task will reconstruct from raw bytes.
    auto args_bytes = std::move(args).ToBytes();
    const std::string func_str{func};

    for (const auto& rs_ptr : snapshot->replicasets) {
        auto* rs = rs_ptr.get();
        tasks.emplace_back(utils::Async(
            "vshard_map_call",
            [rs, &func_str, &args_bytes, &cc]() -> formats::msgpack::Value {
                auto q = storages::tarantool::Query::WithRawArgs(
                    storages::tarantool::Query::Type::kCall,
                    func_str, args_bytes);
                const auto raw = rs->Execute(impl::CallMode::kReadWrite, q, cc);
                const auto env = impl::DecodeEnvelope(raw);
                if (!env.vshard_error.IsNull()) {
                    throw VshardStorageError{
                        env.vshard_error.code,
                        "MAP_CALL_ERROR",
                        env.vshard_error.message};
                }
                return env.app_result;
            }));
    }

    std::vector<formats::msgpack::Value> results;
    results.reserve(tasks.size());
    for (auto& t : tasks) {
        results.push_back(t.Get());
    }
    return results;
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

BucketId VshardProxy::ComputeBucketId(std::string_view key) const noexcept {
    return calculator_.BucketIdMpcrc32(key);
}

BucketId VshardProxy::ComputeBucketId(int64_t key) const noexcept {
    return calculator_.BucketIdMpcrc32(key);
}

uint32_t VshardProxy::GetBucketCount() const noexcept {
    return calculator_.GetBucketCount();
}

void VshardProxy::RefreshTopology() {
    routing_table_.Assign(fetcher_->RefreshFull());
}

void VshardProxy::WriteStatistics(utils::statistics::Writer& writer) const {
    auto snapshot = routing_table_.Read();
    auto vshard_writer = writer["vshard"];
    for (const auto& rs : snapshot->replicasets) {
        if (rs) rs->WriteStatistics(vshard_writer);
    }
}

}  // namespace storages::tarantool::vshard

USERVER_NAMESPACE_END
