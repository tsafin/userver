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

std::vector<uint8_t> VshardProxy::CallRawBytes(
    BucketId bucket_id, impl::CallMode mode, std::string_view func,
    const uint8_t* args_data, std::size_t args_len,
    storages::tarantool::OptionalCommandControl cc) {
    auto query = BuildStorageCallQueryRaw(bucket_id, mode, func, args_data, args_len);
    return DoCallRawBytes(bucket_id, mode, query, cc);
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

// ---------------------------------------------------------------------------
// Shared vshard error handler — used by all retry loops
// ---------------------------------------------------------------------------

VshardProxy::RetryAction VshardProxy::HandleVshardError(
    const impl::VshardError& err,
    BucketId bucket_id,
    uint32_t& attempt,
    rcu::ReadablePtr<impl::RoutingTable>& snapshot,
    impl::ReplicasetPool*& rs,
    engine::Deadline deadline) {

    // Check deadline before retrying (matches Lua: fiber_clock() >= tend)
    if (deadline.IsReachable() && deadline.IsReached()) {
        throw VshardException{"vshard.router.call timeout exceeded"};
    }

    // WRONG_BUCKET / BUCKET_IS_LOCKED: bucket migrated or locked during rebalance
    if (err.IsWrongBucket() || err.IsBucketIsLocked()) {
        if (attempt >= settings_.max_moved_retries) {
            throw MovedError{
                bucket_id,
                err.destination_uuid.value_or(""),
                err.message};
        }
        ++attempt;

        if (err.destination_uuid.has_value()) {
            routing_table_.PatchBucketOwner(bucket_id, *err.destination_uuid);
            snapshot = routing_table_.Read();
            rs = snapshot->FindReplicaset(bucket_id);
        }

        if (!err.destination_uuid.has_value() || !rs) {
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
            snapshot = routing_table_.Read();
            rs = snapshot->FindReplicaset(bucket_id);
            if (!rs) throw NoReplicasetError{bucket_id};
        }
        return RetryAction::kRetryImmediate;
    }

    // TRANSFER: bucket is mid-migration, exponential backoff retry
    if (err.IsTransfer()) {
        if (attempt >= settings_.max_moved_retries) {
            throw TransferError{err.message};
        }
        const auto backoff_ms = std::min(50u << attempt, 1000u);
        ++attempt;
        engine::SleepFor(std::chrono::milliseconds{backoff_ms});
        return RetryAction::kRetrySleep;
    }

    // NON_MASTER: routing table stale — refresh and retry
    if (err.IsNonMaster()) {
        if (attempt >= settings_.max_moved_retries) {
            throw VshardStorageError{err.code, "NON_MASTER", err.message};
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
        return RetryAction::kRetryImmediate;
    }

    // Unknown vshard error — throw immediately
    throw VshardStorageError{
        err.code,
        std::to_string(static_cast<uint32_t>(err.type)),
        err.message};
}

// ---------------------------------------------------------------------------
// ResolveReplicaset: FindReplicaset + on-demand DiscoverBucket fallback
// ---------------------------------------------------------------------------

impl::ReplicasetPool* VshardProxy::ResolveReplicaset(
    BucketId bucket_id,
    rcu::ReadablePtr<impl::RoutingTable>& snapshot) {

    auto* rs = snapshot->FindReplicaset(bucket_id);
    if (rs) return rs;

    // Bucket has no known owner — probe all RS masters with bucket_stat
    LOG_INFO() << "vshard bucket " << bucket_id
               << " has no route, running on-demand discovery";

    const auto rs_idx = fetcher_->DiscoverBucket(bucket_id);
    if (rs_idx == 0) return nullptr;

    // Update the routing table using RCU write
    routing_table_.PatchBucketOwnerByIndex(bucket_id, rs_idx);

    // Re-read the snapshot so the caller sees the updated table
    snapshot = routing_table_.Read();
    return snapshot->FindReplicaset(bucket_id);
}

// ---------------------------------------------------------------------------
// Core call implementations
// ---------------------------------------------------------------------------

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

    // Snapshot routing table (lock-free read), with on-demand discovery
    auto snapshot = routing_table_.Read();
    impl::ReplicasetPool* rs = ResolveReplicaset(bucket_id, snapshot);
    if (!rs) throw NoReplicasetError{bucket_id};

    // Build deadline from cc (Lua default: CALL_TIMEOUT_MIN = 0.5s)
    const auto deadline = cc
        ? engine::Deadline::FromDuration(cc->execute)
        : engine::Deadline::FromDuration(std::chrono::milliseconds{500});

    uint32_t attempt = 0;
    while (true) {
        // Update cc with remaining time for each attempt
        if (deadline.IsReachable()) {
            const auto left = deadline.TimeLeft();
            if (left <= engine::Deadline::Duration::zero()) {
                throw VshardException{"vshard.router.call timeout exceeded"};
            }
            cc = storages::tarantool::CommandControl{
                std::chrono::duration_cast<std::chrono::milliseconds>(left)};
        }
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

        HandleVshardError(env.vshard_error, bucket_id, attempt, snapshot, rs, deadline);
        // All retryable cases return here; throws handle the rest.
    }
}

std::vector<uint8_t> VshardProxy::DoCallRawBytes(
    BucketId bucket_id, impl::CallMode mode,
    const storages::tarantool::Query& query,
    storages::tarantool::OptionalCommandControl cc) {

    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    auto snapshot = routing_table_.Read();
    impl::ReplicasetPool* rs = ResolveReplicaset(bucket_id, snapshot);
    if (!rs) throw NoReplicasetError{bucket_id};

    // Build deadline from cc (Lua default: CALL_TIMEOUT_MIN = 0.5s)
    const auto deadline = cc
        ? engine::Deadline::FromDuration(cc->execute)
        : engine::Deadline::FromDuration(std::chrono::milliseconds{500});

    uint32_t attempt = 0;
    while (true) {
        // Update cc with remaining time for each attempt
        if (deadline.IsReachable()) {
            const auto left = deadline.TimeLeft();
            if (left <= engine::Deadline::Duration::zero()) {
                throw VshardException{"vshard.router.call timeout exceeded"};
            }
            cc = storages::tarantool::CommandControl{
                std::chrono::duration_cast<std::chrono::milliseconds>(left)};
        }
        storages::tarantool::ExecutionResult raw;
        try {
            raw = rs->Execute(mode, query, cc);
        } catch (const storages::tarantool::TarantoolException& ex) {
            LOG_WARNING() << "vshard.storage.call network error (bucket="
                          << bucket_id << " attempt=" << attempt
                          << "): " << ex.what();
            throw;
        }

        // Zero-copy decode: scan raw bytes, no Value tree on success path
        impl::RawEnvelopeResult env;
        try {
            env = impl::DecodeEnvelopeRaw(raw);
        } catch (const storages::tarantool::CommandException&) {
            throw;
        }

        if (env.ok) {
            return std::move(env.app_result_bytes);
        }

        HandleVshardError(env.vshard_error, bucket_id, attempt, snapshot, rs, deadline);
    }
}

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
    impl::ReplicasetPool* rs = ResolveReplicaset(bucket_id, snapshot);
    if (!rs) throw NoReplicasetError{bucket_id};

    // Build deadline from cc (Lua default: CALL_TIMEOUT_MIN = 0.5s)
    const auto deadline = cc
        ? engine::Deadline::FromDuration(cc->execute)
        : engine::Deadline::FromDuration(std::chrono::milliseconds{500});

    uint32_t attempt = 0;
    while (true) {
        if (deadline.IsReachable()) {
            const auto left = deadline.TimeLeft();
            if (left <= engine::Deadline::Duration::zero()) {
                throw VshardException{"vshard.router.call timeout exceeded"};
            }
            cc = storages::tarantool::CommandControl{
                std::chrono::duration_cast<std::chrono::milliseconds>(left)};
        }
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

        HandleVshardError(env.vshard_error, bucket_id, attempt, snapshot, rs, deadline);
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
    impl::ReplicasetPool* rs = ResolveReplicaset(bucket_id, snapshot);
    if (!rs) throw NoReplicasetError{bucket_id};

    // Build deadline from cc (Lua default: CALL_TIMEOUT_MIN = 0.5s)
    const auto deadline = cc
        ? engine::Deadline::FromDuration(cc->execute)
        : engine::Deadline::FromDuration(std::chrono::milliseconds{500});

    uint32_t attempt = 0;
    while (true) {
        if (deadline.IsReachable()) {
            const auto left = deadline.TimeLeft();
            if (left <= engine::Deadline::Duration::zero()) {
                throw VshardException{"vshard.router.call timeout exceeded"};
            }
            cc = storages::tarantool::CommandControl{
                std::chrono::duration_cast<std::chrono::milliseconds>(left)};
        }
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

        HandleVshardError(env.vshard_error, bucket_id, attempt, snapshot, rs, deadline);
    }
}

std::vector<formats::msgpack::Value> VshardProxy::MapCallRW(
    std::string_view func,
    formats::msgpack::ValueBuilder args,
    storages::tarantool::OptionalCommandControl cc) {

    auto snapshot = routing_table_.Read();
    const auto& replicasets = snapshot->replicasets;
    const auto num_rs = replicasets.size();
    if (num_rs == 0) {
        throw VshardException{"MapCallRW: no replicasets available"};
    }

    // Build deadline (Lua default: CALL_TIMEOUT_MIN = 0.5s)
    const auto timeout_ms = cc ? cc->execute : std::chrono::milliseconds{500};
    const auto deadline = engine::Deadline::FromDuration(timeout_ms);

    // Allocate a unique ref ID for this map-reduce operation
    const uint64_t rid = ref_id_.fetch_add(1, std::memory_order_relaxed);

    // Serialize user args once for sharing across tasks
    auto user_args_bytes = std::move(args).ToBytes();
    const std::string func_str{func};

    // Helper: build unref query and send to all RS (best-effort)
    auto unref_all = [&replicasets, rid, &cc]() {
        auto ua = formats::msgpack::ValueBuilder::Array();
        ua.PushBack(formats::msgpack::ValueBuilder{"storage_unref"});
        ua.PushBack(formats::msgpack::ValueBuilder{static_cast<int64_t>(rid)});
        auto unref_q = storages::tarantool::Query::WithRawArgs(
            storages::tarantool::Query::Type::kCall,
            "vshard.storage._call",
            std::move(ua).ToBytes());
        for (const auto& rs_ptr : replicasets) {
            try {
                rs_ptr->Execute(impl::CallMode::kReadWrite, unref_q, cc);
            } catch (...) {}
        }
    };

    // --- Ref stage: acquire refs on all RS masters ---
    // Calls vshard.storage._call('storage_ref', rid, timeout) on each RS.
    // This blocks the rebalancer from moving buckets during the map phase.
    {
        const double timeout_secs =
            std::chrono::duration<double>(deadline.TimeLeft()).count();

        auto ra = formats::msgpack::ValueBuilder::Array();
        ra.PushBack(formats::msgpack::ValueBuilder{"storage_ref"});
        ra.PushBack(formats::msgpack::ValueBuilder{static_cast<int64_t>(rid)});
        ra.PushBack(formats::msgpack::ValueBuilder{timeout_secs});
        auto ref_query = storages::tarantool::Query::WithRawArgs(
            storages::tarantool::Query::Type::kCall,
            "vshard.storage._call",
            std::move(ra).ToBytes());

        std::vector<engine::TaskWithResult<void>> ref_tasks;
        ref_tasks.reserve(num_rs);
        for (const auto& rs_ptr : replicasets) {
            auto* rs = rs_ptr.get();
            ref_tasks.emplace_back(utils::Async(
                "vshard_ref",
                [rs, &ref_query, &cc]() {
                    rs->Execute(impl::CallMode::kReadWrite, ref_query, cc);
                }));
        }

        try {
            for (auto& t : ref_tasks) {
                t.Get();
            }
        } catch (...) {
            unref_all();
            throw;
        }
    }

    // --- Map stage: execute user function on all RS ---
    // Calls vshard.storage._call('storage_map', rid, func, args) on each RS.
    // Build raw msgpack: fixarray(4) + 'storage_map' + rid + func + user_args
    std::vector<uint8_t> map_query_bytes;
    {
        using namespace storages::tarantool::impl::msgpack_scan;
        // array header (4 elements)
        map_query_bytes.push_back(mp::kFixArrayMin | 4);
        // 'storage_map' (11 chars)
        map_query_bytes.push_back(mp::kFixStrMin | 11);
        const char* sm = "storage_map";
        map_query_bytes.insert(map_query_bytes.end(), sm, sm + 11);
        // rid as uint64
        map_query_bytes.push_back(mp::kUint64);
        for (int i = 7; i >= 0; --i)
            map_query_bytes.push_back(static_cast<uint8_t>(rid >> (i * 8)));
        // func name
        const auto flen = func_str.size();
        if (flen <= 31) {
            map_query_bytes.push_back(
                static_cast<uint8_t>(mp::kFixStrMin | flen));
        } else {
            map_query_bytes.push_back(mp::kStr16);
            map_query_bytes.push_back(static_cast<uint8_t>(flen >> 8));
            map_query_bytes.push_back(static_cast<uint8_t>(flen));
        }
        map_query_bytes.insert(map_query_bytes.end(),
                               func_str.data(), func_str.data() + flen);
        // user args: already a valid msgpack value, embed verbatim
        map_query_bytes.insert(map_query_bytes.end(),
                               user_args_bytes.begin(), user_args_bytes.end());
    }

    std::vector<engine::TaskWithResult<formats::msgpack::Value>> map_tasks;
    map_tasks.reserve(num_rs);

    for (const auto& rs_ptr : replicasets) {
        auto* rs = rs_ptr.get();
        map_tasks.emplace_back(utils::Async(
            "vshard_map_call",
            [rs, &map_query_bytes, &cc]()
                -> formats::msgpack::Value {
                auto q = storages::tarantool::Query::WithRawArgs(
                    storages::tarantool::Query::Type::kCall,
                    "vshard.storage._call",
                    map_query_bytes);
                const auto raw =
                    rs->Execute(impl::CallMode::kReadWrite, q, cc);
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

    // Collect map results
    std::vector<formats::msgpack::Value> results;
    results.reserve(num_rs);
    std::exception_ptr map_error;
    for (auto& t : map_tasks) {
        try {
            results.push_back(t.Get());
        } catch (...) {
            if (!map_error) map_error = std::current_exception();
        }
    }

    // --- Unref stage: release refs on all RS (always, even on error) ---
    unref_all();

    if (map_error) {
        std::rethrow_exception(map_error);
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
