#include <vshard/impl/vshard_proxy.hpp>

#include <chrono>
#include <cctype>
#include <cstdint>
#include <algorithm>
#include <span>
#include <unordered_map>
#include <unordered_set>

#include <userver/engine/deadline.hpp>
#include <userver/engine/io/exception.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/utils/async.hpp>

#include <storages/tarantool/impl/connection.hpp>
#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/msgpack.hpp>
#include <vshard/impl/iproto_vshard_frames.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard {

namespace {

namespace tnt = storages::tarantool::impl;
constexpr std::string_view kThisFile = __FILE__;

std::vector<uint8_t> BuildStorageCallErrorReturn(std::string_view message) {
    static constexpr std::string_view kErrorType = "LuajitError";

    std::vector<uint8_t> buf;
    buf.reserve(96 + message.size());

    tnt::EncodeArray(buf, 2);
    buf.push_back(mp::kNil);

    tnt::EncodeFixMap(buf, 4);
    tnt::EncodeStr(buf, "base_type");
    tnt::EncodeStr(buf, kErrorType);
    tnt::EncodeStr(buf, "type");
    tnt::EncodeStr(buf, kErrorType);
    tnt::EncodeStr(buf, "message");
    tnt::EncodeStr(buf, message);
    tnt::EncodeStr(buf, "trace");
    tnt::EncodeArray(buf, 1);
    tnt::EncodeFixMap(buf, 2);
    tnt::EncodeStr(buf, "file");
    tnt::EncodeStr(buf, kThisFile);
    tnt::EncodeStr(buf, "line");
    tnt::EncodeUint(buf, __LINE__);

    return buf;
}

bool IsConnectivityErrorMessage(std::string_view message) {
    return message.find("Error while establishing connection") !=
               std::string_view::npos ||
           message.find("connection is broken") != std::string_view::npos ||
           message.find("Failed to connect to ") != std::string_view::npos;
}

std::string NormalizeNetboxClientErrorMessage(std::string_view message) {
    const auto detail_pos = message.find("Error while establishing connection");
    if (detail_pos != std::string_view::npos) {
        message = message.substr(0, detail_pos);
    }
    constexpr std::string_view kSocketPrefix = "Socket: ";
    if (message.substr(0, kSocketPrefix.size()) == kSocketPrefix) {
        message.remove_prefix(kSocketPrefix.size());
    }
    while (!message.empty() && std::isspace(message.back())) {
        message.remove_suffix(1);
    }
    if (!message.empty()) {
        return std::string{message};
    }
    return "Connection refused";
}

std::vector<uint8_t> BuildNetboxClientErrorReturn(std::string_view message) {
    static constexpr uint32_t kNoConnectionCode = 77;
    static constexpr std::string_view kErrorType = "ClientError";

    std::vector<uint8_t> buf;
    const auto normalized = NormalizeNetboxClientErrorMessage(message);
    buf.reserve(128 + normalized.size());

    tnt::EncodeArray(buf, 2);
    buf.push_back(mp::kNil);

    tnt::EncodeFixMap(buf, 5);
    tnt::EncodeStr(buf, "code");
    tnt::EncodeUint(buf, kNoConnectionCode);
    tnt::EncodeStr(buf, "base_type");
    tnt::EncodeStr(buf, kErrorType);
    tnt::EncodeStr(buf, "type");
    tnt::EncodeStr(buf, kErrorType);
    tnt::EncodeStr(buf, "message");
    tnt::EncodeStr(buf, normalized);
    tnt::EncodeStr(buf, "trace");
    tnt::EncodeArray(buf, 1);
    tnt::EncodeFixMap(buf, 2);
    tnt::EncodeStr(buf, "file");
    tnt::EncodeStr(buf, kThisFile);
    tnt::EncodeStr(buf, "line");
    tnt::EncodeUint(buf, __LINE__);

    return buf;
}

std::vector<uint8_t> CopyRawBytes(std::span<const uint8_t> raw) {
    return {raw.begin(), raw.end()};
}

std::vector<uint8_t> BuildNonEmptyBootstrapErrorReturn() {
    std::vector<uint8_t> payload;
    payload.reserve(96);

    tnt::EncodeArray(payload, 2);
    payload.push_back(mp::kNil);
    tnt::EncodeFixMap(payload, 4);
    tnt::EncodeStr(payload, "message");
    tnt::EncodeStr(payload, "Cluster is already bootstrapped");
    tnt::EncodeStr(payload, "type");
    tnt::EncodeStr(payload, "ShardingError");
    tnt::EncodeStr(payload, "code");
    tnt::EncodeUint(payload, 10);
    tnt::EncodeStr(payload, "name");
    tnt::EncodeStr(payload, "NON_EMPTY");
    return payload;
}

std::vector<uint8_t> BuildTimeoutClientErrorReturn() {
    std::vector<uint8_t> buf;
    buf.reserve(96);

    tnt::EncodeArray(buf, 2);
    buf.push_back(mp::kNil);
    tnt::EncodeFixMap(buf, 5);
    tnt::EncodeStr(buf, "code");
    tnt::EncodeUint(buf, 78);
    tnt::EncodeStr(buf, "base_type");
    tnt::EncodeStr(buf, "ClientError");
    tnt::EncodeStr(buf, "type");
    tnt::EncodeStr(buf, "ClientError");
    tnt::EncodeStr(buf, "message");
    tnt::EncodeStr(buf, "Timeout exceeded");
    tnt::EncodeStr(buf, "trace");
    tnt::EncodeArray(buf, 1);
    tnt::EncodeFixMap(buf, 2);
    tnt::EncodeStr(buf, "file");
    tnt::EncodeStr(buf, kThisFile);
    tnt::EncodeStr(buf, "line");
    tnt::EncodeUint(buf, __LINE__);
    return buf;
}

std::vector<uint8_t> BuildVshardStorageErrorReturn(
    const VshardStorageError& ex) {
    std::size_t field_count = 3;
    if (!ex.GetName().empty()) ++field_count;
    if (ex.GetReplicaset()) ++field_count;
    if (ex.GetReplica()) ++field_count;
    if (ex.GetMaster()) ++field_count;

    std::vector<uint8_t> buf;
    buf.reserve(160);

    tnt::EncodeArray(buf, 2);
    buf.push_back(mp::kNil);
    if (field_count <= 15) {
        tnt::EncodeFixMap(buf, static_cast<uint8_t>(field_count));
    } else {
        tnt::EncodeFixMap(buf, 15);
    }
    tnt::EncodeStr(buf, "code");
    tnt::EncodeUint(buf, ex.GetCode());
    tnt::EncodeStr(buf, "type");
    tnt::EncodeStr(buf, ex.GetType());
    tnt::EncodeStr(buf, "message");
    tnt::EncodeStr(buf, ex.what());
    if (!ex.GetName().empty()) {
        tnt::EncodeStr(buf, "name");
        tnt::EncodeStr(buf, ex.GetName());
    }
    if (ex.GetReplicaset()) {
        tnt::EncodeStr(buf, "replicaset");
        tnt::EncodeStr(buf, *ex.GetReplicaset());
    }
    if (ex.GetReplica()) {
        tnt::EncodeStr(buf, "replica");
        tnt::EncodeStr(buf, *ex.GetReplica());
    }
    if (ex.GetMaster()) {
        tnt::EncodeStr(buf, "master");
        tnt::EncodeStr(buf, *ex.GetMaster());
    }
    return buf;
}

std::vector<uint8_t> BuildVshardErrorReturn(const impl::VshardError& err) {
    std::size_t field_count = 4;
    if (err.bucket_id) ++field_count;
    if (err.destination_uuid) ++field_count;
    if (err.replicaset_uuid) ++field_count;
    if (err.replica_uuid) ++field_count;
    if (err.master_uuid) ++field_count;

    std::vector<uint8_t> buf;
    buf.reserve(160);

    tnt::EncodeArray(buf, 2);
    buf.push_back(mp::kNil);
    tnt::EncodeFixMap(buf, static_cast<uint8_t>(field_count));
    tnt::EncodeStr(buf, "code");
    tnt::EncodeUint(buf, err.code);
    tnt::EncodeStr(buf, "type");
    tnt::EncodeStr(buf, "ShardingError");
    tnt::EncodeStr(buf, "message");
    tnt::EncodeStr(buf, err.message);
    tnt::EncodeStr(buf, "name");
    tnt::EncodeStr(buf, err.name);
    if (err.bucket_id) {
        tnt::EncodeStr(buf, "bucket_id");
        tnt::EncodeUint(buf, *err.bucket_id);
    }
    if (err.destination_uuid) {
        tnt::EncodeStr(buf, "destination");
        tnt::EncodeStr(buf, *err.destination_uuid);
    }
    if (err.replicaset_uuid) {
        tnt::EncodeStr(buf, "replicaset");
        tnt::EncodeStr(buf, *err.replicaset_uuid);
    }
    if (err.replica_uuid) {
        tnt::EncodeStr(buf, "replica");
        tnt::EncodeStr(buf, *err.replica_uuid);
    }
    if (err.master_uuid) {
        tnt::EncodeStr(buf, "master");
        tnt::EncodeStr(buf, *err.master_uuid);
    }
    return buf;
}

[[noreturn]] void RethrowDirectCallNetworkError(
    const impl::ReplicasetPool& rs, BucketId bucket_id,
    const std::exception& ex) {
    LOG_WARNING() << "vshard direct call network error (bucket=" << bucket_id
                  << ", replicaset=" << rs.GetUuid() << "): " << ex.what();
    throw UnreachableReplicasetError{rs.GetUuid(), bucket_id};
}

void HandleDirectCallException(
    const impl::ReplicasetPool& rs, BucketId bucket_id,
    const std::exception& ex) {
    if (IsConnectivityErrorMessage(ex.what())) {
        RethrowDirectCallNetworkError(rs, bucket_id, ex);
    }
    throw;
}

struct DirectCallResult {
    bool ok{true};
    formats::msgpack::Value value;
    std::string message;
    std::optional<uint32_t> code;
};

DirectCallResult ParseDirectCallResult(
    const storages::tarantool::ExecutionResult& raw) {
    raw.AssertOk();
    const auto& data = raw.GetData();
    if (!data.IsArray() || data.GetSize() == 0) {
        throw storages::tarantool::TarantoolException{
            "Unexpected direct CALL reply shape"};
    }

    DirectCallResult result;
    if (data[0].IsNull()) {
        result.ok = false;
        if (data.GetSize() >= 2) {
            const auto& err = data[1];
            if (err.IsObject()) {
                result.message = err["message"].As<std::string>("");
                if (!err["code"].IsMissing()) {
                    result.code = err["code"].As<uint32_t>();
                }
            } else if (err.IsString()) {
                result.message = err.As<std::string>("");
            }
        }
        if (result.message.empty()) {
            result.message = "vshard direct call failed";
        }
        return result;
    }

    if (data.GetSize() >= 2) {
        result.value = data[1];
    }
    return result;
}

}  // namespace

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

std::vector<uint8_t> VshardProxy::CallRawBytesWithModeString(
    BucketId bucket_id, impl::CallMode mode, std::string_view mode_string,
    std::string_view func, const uint8_t* args_data, std::size_t args_len,
    storages::tarantool::OptionalCommandControl cc) {
    auto query = BuildStorageCallQueryRawWithModeString(
        bucket_id, mode_string, func, args_data, args_len);
    return DoCallRawBytes(bucket_id, mode, query, cc);
}

void VshardProxy::CallAsyncWithModeString(
    BucketId bucket_id, impl::CallMode mode, std::string_view mode_string,
    std::string_view func, const uint8_t* args_data, std::size_t args_len,
    storages::tarantool::OptionalCommandControl cc) {
    auto query = BuildStorageCallQueryRawWithModeString(
        bucket_id, mode_string, func, args_data, args_len);
    DoCallAsync(bucket_id, mode, query, cc);
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
    const std::string_view vshard_mode =
        (mode == impl::CallMode::kReadWrite) ? "write" : "read";
    return BuildStorageCallQueryRawWithModeString(
        bucket_id, vshard_mode, func, args_data, args_len);
}

storages::tarantool::Query VshardProxy::BuildStorageCallQueryRawWithModeString(
    BucketId bucket_id, std::string_view mode_string, std::string_view func,
    const uint8_t* args_data, std::size_t args_len) const {
    // Build the msgpack array [bucket_id, mode_str, func_name, args_array]
    // manually, copying args_data verbatim — no Value tree constructed.

    std::vector<uint8_t> buf;
    buf.reserve(1 + 5 + 1 + mode_string.size() + 1 + func.size() + args_len);

    // fixarray(4)
    buf.push_back(static_cast<uint8_t>(mp::kFixArrayMin | 4));

    // [0] bucket_id as uint32
    buf.push_back(mp::kUint32);
    buf.push_back(static_cast<uint8_t>(bucket_id >> 24));
    buf.push_back(static_cast<uint8_t>(bucket_id >> 16));
    buf.push_back(static_cast<uint8_t>(bucket_id >>  8));
    buf.push_back(static_cast<uint8_t>(bucket_id));

    // [1] mode string
    if (mode_string.size() <= 31) {
        buf.push_back(static_cast<uint8_t>(mp::kFixStrMin | mode_string.size()));
    } else {
        buf.push_back(mp::kStr8);
        buf.push_back(static_cast<uint8_t>(mode_string.size()));
    }
    buf.insert(buf.end(), mode_string.begin(), mode_string.end());

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

    if (err.IsBucketIsLocked()) {
        const auto left = deadline.TimeLeft();
        if (left <= engine::Deadline::Duration::zero()) {
            throw VshardException{"vshard.router.call timeout exceeded"};
        }
        const auto sleep_for = std::min(
            std::chrono::duration_cast<std::chrono::milliseconds>(left),
            std::chrono::milliseconds{10});
        engine::SleepFor(sleep_for);
        return RetryAction::kRetrySleep;
    }

    // WRONG_BUCKET: bucket migrated during rebalance.
    if (err.IsWrongBucket()) {
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
            throw VshardStorageError{
                err.code,
                "ShardingError",
                err.name.empty() ? "NON_MASTER" : err.name,
                err.message,
                err.replicaset_uuid,
                err.replica_uuid,
                err.master_uuid};
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

    const auto discovery = fetcher_->DiscoverBucket(bucket_id);
    if (discovery.HasOwner()) {
        routing_table_.PatchBucketOwnerByIndex(bucket_id, discovery.rs_idx);

        // Re-read the snapshot so the caller sees the updated table
        snapshot = routing_table_.Read();
        return snapshot->FindReplicaset(bucket_id);
    }
    if (discovery.HasUnreachableReplicaset()) {
        throw UnreachableReplicasetError{
            discovery.unreachable_replicaset_id, bucket_id};
    }
    if (discovery.HasOtherError()) {
        throw VshardException{discovery.error_message};
    }
    throw NoRouteToBucketError{bucket_id};
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
        } catch (const engine::io::IoException& ex) {
            RethrowDirectCallNetworkError(*rs, bucket_id, ex);
        } catch (const storages::tarantool::TarantoolException& ex) {
            HandleDirectCallException(*rs, bucket_id, ex);
        } catch (const std::exception& ex) {
            HandleDirectCallException(*rs, bucket_id, ex);
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
    bool waiting_on_locked_bucket = false;
    std::optional<impl::VshardError> last_locked_error;
    while (true) {
        // Update cc with remaining time for each attempt
        if (deadline.IsReachable()) {
            const auto left = deadline.TimeLeft();
            if (left <= engine::Deadline::Duration::zero()) {
                if (last_locked_error) {
                    return BuildVshardErrorReturn(*last_locked_error);
                }
                throw VshardException{"vshard.router.call timeout exceeded"};
            }
            cc = storages::tarantool::CommandControl{
                std::chrono::duration_cast<std::chrono::milliseconds>(left)};
        }
        storages::tarantool::ExecutionResult raw;
        try {
            raw = rs->Execute(mode, query, cc);
        } catch (const storages::tarantool::CommandException& ex) {
            return BuildStorageCallErrorReturn(ex.what());
        } catch (const engine::io::IoException& ex) {
            return BuildNetboxClientErrorReturn(ex.what());
        } catch (const storages::tarantool::TarantoolException& ex) {
            if (
                last_locked_error &&
                (std::string_view{ex.what()}.find("deadline exceeded") !=
                     std::string_view::npos ||
                 std::string_view{ex.what()}.find("deadline expired") !=
                     std::string_view::npos)
            ) {
                return BuildTimeoutClientErrorReturn();
            }
            if (IsConnectivityErrorMessage(ex.what())) {
                return BuildNetboxClientErrorReturn(ex.what());
            }
            throw;
        } catch (const std::exception& ex) {
            if (IsConnectivityErrorMessage(ex.what())) {
                return BuildNetboxClientErrorReturn(ex.what());
            }
            throw;
        }

        // Zero-copy decode: scan raw bytes, no Value tree on success path
        impl::RawEnvelopeResult env;
        try {
            env = impl::DecodeEnvelopeRaw(raw);
        } catch (const storages::tarantool::CommandException& ex) {
            return BuildStorageCallErrorReturn(ex.what());
        }

        if (env.status != impl::RawEnvelopeStatus::kVshardError) {
            return std::move(env.return_values_bytes);
        }
        waiting_on_locked_bucket = env.vshard_error.IsBucketIsLocked();
        if (waiting_on_locked_bucket) {
            last_locked_error = env.vshard_error;
        }

        try {
            HandleVshardError(
                env.vshard_error, bucket_id, attempt, snapshot, rs, deadline);
        } catch (const VshardStorageError& ex) {
            return BuildVshardStorageErrorReturn(ex);
        } catch (const VshardException& ex) {
            if (std::string_view{ex.what()} ==
                "vshard.router.call timeout exceeded") {
                return BuildTimeoutClientErrorReturn();
            }
            throw;
        }
    }
}

void VshardProxy::DoCallAsync(
    BucketId bucket_id, impl::CallMode mode,
    const storages::tarantool::Query& query,
    storages::tarantool::OptionalCommandControl cc) {

    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    auto snapshot = routing_table_.Read();
    impl::ReplicasetPool* rs = ResolveReplicaset(bucket_id, snapshot);
    if (!rs) throw NoReplicasetError{bucket_id};

    const auto deadline = cc
        ? engine::Deadline::FromDuration(cc->execute)
        : engine::Deadline::FromDuration(std::chrono::milliseconds{500});

    if (deadline.IsReachable()) {
        const auto left = deadline.TimeLeft();
        if (left <= engine::Deadline::Duration::zero()) {
            throw VshardException{"vshard.router.call timeout exceeded"};
        }
        cc = storages::tarantool::CommandControl{
            std::chrono::duration_cast<std::chrono::milliseconds>(left)};
    }

    auto future = rs->ExecuteAsync(mode, query, cc);
    static_cast<void>(future);
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
        } catch (const engine::io::IoException& ex) {
            RethrowDirectCallNetworkError(*rs, bucket_id, ex);
        } catch (const storages::tarantool::TarantoolException& ex) {
            HandleDirectCallException(*rs, bucket_id, ex);
        } catch (const std::exception& ex) {
            HandleDirectCallException(*rs, bucket_id, ex);
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
        } catch (const engine::io::IoException& ex) {
            RethrowDirectCallNetworkError(*rs, bucket_id, ex);
        } catch (const storages::tarantool::TarantoolException& ex) {
            HandleDirectCallException(*rs, bucket_id, ex);
        } catch (const std::exception& ex) {
            HandleDirectCallException(*rs, bucket_id, ex);
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

std::vector<VshardProxy::MapCallRWEntry> VshardProxy::MapCallRW(
    std::string_view func,
    const uint8_t* args_data, std::size_t args_len,
    storages::tarantool::OptionalCommandControl cc,
    std::optional<std::vector<BucketId>> bucket_ids) {

    auto snapshot = routing_table_.Read();
    std::vector<std::shared_ptr<impl::ReplicasetPool>> replicasets;
    if (bucket_ids) {
        std::unordered_set<std::string> seen;
        for (const auto bid : *bucket_ids) {
            const auto discovery = fetcher_->DiscoverBucket(bid);
            if (discovery.HasOwner()) {
                routing_table_.PatchBucketOwnerByIndex(bid, discovery.rs_idx);
            } else if (discovery.HasUnreachableReplicaset()) {
                throw UnreachableReplicasetError{
                    discovery.unreachable_replicaset_id, bid};
            } else if (discovery.HasOtherError()) {
                throw VshardException{discovery.error_message};
            } else {
                throw NoRouteToBucketError{bid};
            }

            auto local_snapshot = routing_table_.Read();
            auto* rs = local_snapshot->FindReplicaset(bid);
            if (!rs) throw NoReplicasetError{bid};
            const auto& uuid = rs->GetUuid();
            if (!seen.insert(uuid).second) continue;
            for (const auto& rs_ptr : local_snapshot->replicasets) {
                if (rs_ptr->GetUuid() == uuid) {
                    replicasets.push_back(rs_ptr);
                    break;
                }
            }
        }
    } else {
        replicasets = snapshot->replicasets;
    }

    std::sort(
        replicasets.begin(), replicasets.end(),
        [](const auto& lhs, const auto& rhs) {
            return lhs->GetUuid() < rhs->GetUuid();
        });

    const auto num_rs = replicasets.size();
    if (num_rs == 0) {
        throw VshardException{"MapCallRW: no replicasets available"};
    }

    // Build deadline (Lua default: CALL_TIMEOUT_MIN = 0.5s)
    const auto timeout_ms = cc ? cc->execute : std::chrono::milliseconds{500};
    const auto deadline = engine::Deadline::FromDuration(timeout_ms);

    // Allocate a unique ref ID for this map-reduce operation
    const uint64_t rid = ref_id_.fetch_add(1, std::memory_order_relaxed);

    struct SessionContext {
        std::shared_ptr<impl::ReplicasetPool> rs;
        storages::tarantool::impl::ConnectionPtr conn;
        bool ref_created{false};
    };

    std::vector<SessionContext> sessions;
    sessions.reserve(num_rs);
    for (auto& rs : replicasets) {
        sessions.push_back(SessionContext{rs, rs->AcquireMaster(deadline), false});
    }

    // Serialize user args once for sharing across tasks
    const std::vector<uint8_t> user_args_bytes{
        args_data, args_data + args_len};
    const std::string func_str{func};

    // Helper: build unref query and send to all sessions where ref is still held.
    auto unref_all = [&sessions, rid, deadline]() {
        std::vector<uint8_t> args;
        tnt::EncodeArray(args, 2);
        tnt::EncodeStr(args, "storage_unref");
        tnt::EncodeUint(args, rid);
        const auto unref_q = storages::tarantool::Query::WithRawArgs(
            storages::tarantool::Query::Type::kCall,
            "vshard.storage._call",
            std::move(args));
        for (auto& session : sessions) {
            if (!session.ref_created) continue;
            try {
                session.conn->Execute(
                    engine::Deadline::FromDuration(deadline.TimeLeft()), unref_q);
            } catch (...) {}
        }
    };

    // --- Ref stage: acquire refs on all RS masters ---
    // Calls vshard.storage._call('storage_ref', rid, timeout) on each RS.
    // This blocks the rebalancer from moving buckets during the map phase.
    {
        const double timeout_secs =
            std::chrono::duration<double>(deadline.TimeLeft()).count();

        std::vector<uint8_t> ref_args;
        tnt::EncodeArray(ref_args, 3);
        tnt::EncodeStr(ref_args, "storage_ref");
        tnt::EncodeUint(ref_args, rid);
        formats::msgpack::ValueBuilder{timeout_secs}.AppendTo(ref_args);
        const auto ref_query = storages::tarantool::Query::WithRawArgs(
            storages::tarantool::Query::Type::kCall,
            "vshard.storage._call",
            std::move(ref_args));

        std::vector<engine::TaskWithResult<void>> ref_tasks;
        ref_tasks.reserve(num_rs);
        for (auto& session : sessions) {
            ref_tasks.emplace_back(utils::Async(
                "vshard_ref",
                [&session, &ref_query, deadline]() {
                    try {
                        const auto raw = session.conn->Execute(
                            engine::Deadline::FromDuration(deadline.TimeLeft()),
                            ref_query);
                        const auto result = ParseDirectCallResult(raw);
                        if (!result.ok) {
                            throw MapCallRWException(
                                std::string{result.message},
                                std::string{session.rs->GetUuid()},
                                result.code);
                        }
                        session.ref_created = true;
                    } catch (const storages::tarantool::CommandException& ex) {
                        throw MapCallRWException{
                            ex.what(), session.rs->GetUuid(),
                            ex.GetErrorCode()};
                    } catch (const std::exception& ex) {
                        throw MapCallRWException{
                            ex.what(), session.rs->GetUuid()};
                    }
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

    std::vector<engine::TaskWithResult<MapCallRWEntry>> map_tasks;
    map_tasks.reserve(num_rs);

    for (auto& session : sessions) {
        map_tasks.emplace_back(utils::Async(
            "vshard_map_call",
            [&session, rid, &func_str, &user_args_bytes, deadline]()
                -> MapCallRWEntry {
                try {
                    std::vector<uint8_t> map_query_bytes;
                    map_query_bytes.reserve(
                        32 + func_str.size() + user_args_bytes.size());
                    tnt::EncodeArray(map_query_bytes, 4);
                    tnt::EncodeStr(map_query_bytes, "storage_map");
                    tnt::EncodeUint(map_query_bytes, rid);
                    tnt::EncodeStr(map_query_bytes, func_str);
                    map_query_bytes.insert(
                        map_query_bytes.end(),
                        user_args_bytes.begin(), user_args_bytes.end());
                    auto q = storages::tarantool::Query::WithRawArgs(
                        storages::tarantool::Query::Type::kCall,
                        "vshard.storage._call",
                        std::move(map_query_bytes));
                    const auto raw = session.conn->Execute(
                        engine::Deadline::FromDuration(deadline.TimeLeft()), q);
                    session.ref_created = false;
                    const auto result = ParseDirectCallResult(raw);
                    if (!result.ok) {
                        throw MapCallRWException(
                            std::string{result.message},
                            std::string{session.rs->GetUuid()},
                            result.code);
                    }
                    return MapCallRWEntry{
                        session.rs->GetUuid(), std::move(result.value)};
                } catch (const storages::tarantool::CommandException& ex) {
                    throw MapCallRWException{
                        ex.what(), session.rs->GetUuid(),
                        ex.GetErrorCode()};
                } catch (const MapCallRWException&) {
                    throw;
                } catch (const std::exception& ex) {
                    throw MapCallRWException{
                        ex.what(), session.rs->GetUuid()};
                }
            }));
    }

    // Collect map results
    std::vector<MapCallRWEntry> results;
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

VshardProxy::SyncResult VshardProxy::Sync(double timeout_seconds) {
    auto snapshot = routing_table_.Read();
    const auto& replicasets = snapshot->replicasets;
    std::optional<std::string> last_replicaset_id;

    const auto total_timeout = std::chrono::duration_cast<
        std::chrono::milliseconds>(std::chrono::duration<double>{timeout_seconds});
    const auto deadline = engine::Deadline::FromDuration(total_timeout);

    for (const auto& rs : replicasets) {
        const auto remaining = deadline.TimeLeft();
        const auto remaining_seconds =
            std::chrono::duration<double>(remaining).count();
        if (remaining_seconds < 0.0) {
            return SyncResult{false, true, last_replicaset_id};
        }
        last_replicaset_id = rs->GetUuid();

        auto args = formats::msgpack::ValueBuilder::Array();
        args.PushBack(formats::msgpack::ValueBuilder{remaining_seconds});
        const auto query = storages::tarantool::Query::Call(
            "vshard.storage.sync", std::move(args));

        try {
            rs->Execute(impl::CallMode::kReadWrite, query);
        } catch (const storages::tarantool::CommandException& ex) {
            if (std::string_view{ex.what()}.find("Timeout exceeded") !=
                std::string_view::npos) {
                return SyncResult{false, true, rs->GetUuid()};
            }
            throw;
        } catch (const storages::tarantool::TarantoolException& ex) {
            if (std::string_view{ex.what()}.find("execute deadline expired") !=
                std::string_view::npos) {
                return SyncResult{false, true, rs->GetUuid()};
            }
            throw;
        }
    }

    return SyncResult{true, false, std::nullopt};
}

std::vector<uint8_t> VshardProxy::Bootstrap(
    bool if_not_bootstrapped,
    storages::tarantool::OptionalCommandControl cc) {
    auto snapshot = routing_table_.Read();
    const auto num_rs = snapshot->replicasets.size();

    if (num_rs == 0) {
        return BuildStorageCallErrorReturn("No replicasets available");
    }

    std::optional<std::vector<uint8_t>> last_error;
    for (const auto& rs : snapshot->replicasets) {
        const auto raw = rs->Execute(
            impl::CallMode::kReadWrite,
            storages::tarantool::Query::Call(
                "vshard.storage.buckets_count",
                formats::msgpack::ValueBuilder::Array()),
            cc);
        raw.AssertOk();
        const auto& data = raw.GetData();
        if (!data.IsArray() || data.GetSize() == 0 || data[0].IsNull()) {
            if (!if_not_bootstrapped) {
                return CopyRawBytes(raw.GetRawBytes());
            }
            last_error = CopyRawBytes(raw.GetRawBytes());
            continue;
        }
        if (data[0].As<uint64_t>(0) > 0) {
            if (if_not_bootstrapped) {
                std::vector<uint8_t> result;
                tnt::EncodeArray(result, 1);
                result.push_back(mp::kTrue);
                return result;
            }
            return BuildNonEmptyBootstrapErrorReturn();
        }
    }
    if (last_error) return *last_error;

    const uint32_t base = calculator_.GetBucketCount() / num_rs;
    const uint32_t extra = calculator_.GetBucketCount() % num_rs;
    uint32_t next_bucket = 1;
    for (std::size_t i = 0; i < num_rs; ++i) {
        const uint32_t count = base + (i < extra ? 1 : 0);
        if (count == 0) continue;

        auto args = formats::msgpack::ValueBuilder::Array();
        args.PushBack(formats::msgpack::ValueBuilder{
            static_cast<uint64_t>(next_bucket)});
        args.PushBack(formats::msgpack::ValueBuilder{
            static_cast<uint64_t>(count)});
        const auto raw = snapshot->replicasets[i]->Execute(
            impl::CallMode::kReadWrite,
            storages::tarantool::Query::Call(
                "vshard.storage.bucket_force_create", std::move(args)),
            cc);
        raw.AssertOk();
        const auto& data = raw.GetData();
        if (!data.IsArray() || data.GetSize() == 0 || !data[0].As<bool>(false)) {
            return CopyRawBytes(raw.GetRawBytes());
        }
        next_bucket += count;
    }

    std::vector<uint8_t> result;
    tnt::EncodeArray(result, 1);
    result.push_back(mp::kTrue);
    return result;
}

formats::msgpack::Value VshardProxy::GetInfo(bool with_services) {
    constexpr int kStatusGreen = 0;
    constexpr int kStatusYellow = 1;
    constexpr int kStatusOrange = 2;
    constexpr int kStatusRed = 3;

    auto snapshot = routing_table_.Read();

    formats::msgpack::ValueBuilder state = formats::msgpack::ValueBuilder::Object();
    auto replicasets = formats::msgpack::ValueBuilder::Object();
    auto bucket = formats::msgpack::ValueBuilder::Object();
    auto alerts = formats::msgpack::ValueBuilder::Array();

    uint32_t available_ro = 0;
    uint32_t available_rw = 0;
    uint32_t unreachable = 0;
    uint32_t known_bucket_count = 0;
    int status = kStatusGreen;

    for (const auto& rs : snapshot->replicasets) {
        if (!rs) continue;

        const auto bucket_count = [&] {
            uint32_t count = 0;
            const auto rs_idx = snapshot->FindReplicasetIndex(rs->GetUuid());
            if (rs_idx == 0) return count;
            for (const auto owner : snapshot->bucket_to_rs) {
                if (owner == rs_idx) ++count;
            }
            return count;
        }();
        known_bucket_count += bucket_count;

        auto rs_info = formats::msgpack::ValueBuilder::Object();
        rs_info["uuid"] = formats::msgpack::ValueBuilder{rs->GetUuid()};
        rs_info["bucket"] = formats::msgpack::ValueBuilder::Object();

        auto master = formats::msgpack::ValueBuilder::Object();
        const auto& master_meta = rs->GetMasterMeta();
        if (!master_meta.host.empty()) {
            master["uri"] = formats::msgpack::ValueBuilder{
                "storage@" + master_meta.host + ":" +
                std::to_string(master_meta.port)};
            master["network_timeout"] = formats::msgpack::ValueBuilder{0.5};
        }
        if (!master_meta.uuid.empty()) {
            master["uuid"] = formats::msgpack::ValueBuilder{master_meta.uuid};
        }
        master["status"] = formats::msgpack::ValueBuilder{
            rs->IsMasterAvailable() ? "available" : "unreachable"};
        rs_info["master"] = std::move(master);

        auto replica = formats::msgpack::ValueBuilder::Object();
        if (rs->IsMasterAvailable()) {
            replica["uri"] = formats::msgpack::ValueBuilder{
                "storage@" + master_meta.host + ":" +
                std::to_string(master_meta.port)};
            replica["network_timeout"] = formats::msgpack::ValueBuilder{0.5};
            if (!master_meta.uuid.empty()) {
                replica["uuid"] = formats::msgpack::ValueBuilder{
                    master_meta.uuid};
            }
            replica["status"] = formats::msgpack::ValueBuilder{"available"};
        } else if (rs->HasReplica() && rs->IsReplicaAvailable()) {
            const auto* replica_meta = rs->GetReplicaMeta();
            if (replica_meta) {
                replica["uri"] = formats::msgpack::ValueBuilder{
                    "storage@" + replica_meta->host + ":" +
                    std::to_string(replica_meta->port)};
                replica["network_timeout"] = formats::msgpack::ValueBuilder{0.5};
                if (!replica_meta->uuid.empty()) {
                    replica["uuid"] = formats::msgpack::ValueBuilder{
                        replica_meta->uuid};
                }
            }
            replica["status"] = formats::msgpack::ValueBuilder{"available"};
        } else {
            replica["status"] = formats::msgpack::ValueBuilder{
                rs->HasReplica() ? "unreachable" : "missing"};
        }
        rs_info["replica"] = std::move(replica);

        auto rs_bucket = formats::msgpack::ValueBuilder::Object();
        const auto master_available = rs->IsMasterAvailable();
        const auto replica_available =
            rs->HasReplica() ? rs->IsReplicaAvailable() : master_available;

        if (!master_available) {
            if (!replica_available) {
                rs_bucket["unreachable"] = formats::msgpack::ValueBuilder{bucket_count};
                unreachable += bucket_count;
                status = kStatusRed;
            } else {
                rs_bucket["available_ro"] = formats::msgpack::ValueBuilder{bucket_count};
                available_ro += bucket_count;
                status = std::max(status, kStatusOrange);
            }
        } else {
            rs_bucket["available_rw"] = formats::msgpack::ValueBuilder{bucket_count};
            available_rw += bucket_count;
        }
        rs_info["bucket"] = std::move(rs_bucket);

        if (with_services) {
            auto services = formats::msgpack::ValueBuilder::Object();

            auto failover = formats::msgpack::ValueBuilder::Object();
            failover["name"] = formats::msgpack::ValueBuilder{"replicaset_failover"};
            failover["status"] = formats::msgpack::ValueBuilder{"ok"};
            failover["status_idx"] = formats::msgpack::ValueBuilder{0};
            failover["activity"] = formats::msgpack::ValueBuilder{"idling"};
            failover["error"] = formats::msgpack::ValueBuilder{""};
            auto failover_replicas = formats::msgpack::ValueBuilder::Object();
            for (const auto& meta : rs->GetAllInstanceMetas()) {
                if (meta.uuid.empty()) continue;
                auto replica_service = formats::msgpack::ValueBuilder::Object();
                replica_service["name"] =
                    formats::msgpack::ValueBuilder{"replica_failover"};
                replica_service["status"] = formats::msgpack::ValueBuilder{"ok"};
                replica_service["status_idx"] = formats::msgpack::ValueBuilder{0};
                replica_service["activity"] = formats::msgpack::ValueBuilder{"idling"};
                replica_service["error"] = formats::msgpack::ValueBuilder{""};
                failover_replicas[meta.uuid] = std::move(replica_service);
            }
            failover["replicas"] = std::move(failover_replicas);
            services["failover"] = std::move(failover);

            services["master_search"] = formats::msgpack::ValueBuilder::Array();
            rs_info["services"] = std::move(services);
        }

        replicasets[rs->GetUuid()] = std::move(rs_info);
    }

    const auto unknown = settings_.topology.bucket_count - known_bucket_count;
    bucket["available_ro"] = formats::msgpack::ValueBuilder{available_ro};
    bucket["available_rw"] = formats::msgpack::ValueBuilder{available_rw};
    bucket["unreachable"] = formats::msgpack::ValueBuilder{unreachable};
    bucket["unknown"] = formats::msgpack::ValueBuilder{unknown};
    if (unknown > 0) {
        status = std::max(status, kStatusYellow);
    }

    state["replicasets"] = std::move(replicasets);
    state["bucket"] = std::move(bucket);
    state["alerts"] = std::move(alerts);
    state["status"] = formats::msgpack::ValueBuilder{status};
    state["identification_mode"] =
        formats::msgpack::ValueBuilder{"uuid_as_key"};
    state["is_enabled"] = formats::msgpack::ValueBuilder{true};

    if (with_services) {
        auto services = formats::msgpack::ValueBuilder::Object();
        auto discovery = formats::msgpack::ValueBuilder::Object();
        discovery["name"] = formats::msgpack::ValueBuilder{"discovery"};
        discovery["status"] = formats::msgpack::ValueBuilder{"ok"};
        discovery["status_idx"] = formats::msgpack::ValueBuilder{0};
        discovery["activity"] = formats::msgpack::ValueBuilder{"idling"};
        discovery["error"] = formats::msgpack::ValueBuilder{""};
        services["discovery"] = std::move(discovery);
        state["services"] = std::move(services);
    }

    const auto bytes = state.ToBytes();
    return formats::msgpack::Value::FromBytes(bytes.data(), bytes.size());
}

std::string VshardProxy::Route(BucketId bucket_id) {
    if (bucket_id < 1 || bucket_id > calculator_.GetBucketCount()) {
        throw NoReplicasetError{bucket_id};
    }

    const auto discovery = fetcher_->DiscoverBucket(bucket_id);
    if (discovery.HasOwner()) {
        routing_table_.PatchBucketOwnerByIndex(bucket_id, discovery.rs_idx);
        auto snapshot = routing_table_.Read();
        auto* rs = snapshot->FindReplicaset(bucket_id);
        if (!rs) throw NoReplicasetError{bucket_id};
        return rs->GetUuid();
    }
    if (discovery.HasUnreachableReplicaset()) {
        throw UnreachableReplicasetError{
            discovery.unreachable_replicaset_id, bucket_id};
    }
    if (discovery.HasOtherError()) {
        throw VshardException{discovery.error_message};
    }
    throw NoRouteToBucketError{bucket_id};
}

std::vector<std::string> VshardProxy::GetReplicasetUUIDs() const {
    auto snapshot = routing_table_.Read();
    std::vector<std::string> uuids;
    uuids.reserve(snapshot->replicasets.size());
    for (const auto& rs : snapshot->replicasets) {
        uuids.push_back(rs->GetUuid());
    }
    return uuids;
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
