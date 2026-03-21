#pragma once

/// @file vshard/impl/vshard_proxy.hpp
/// @brief C++ reimplementation of the vshard router.

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/components/component_config.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/storages/tarantool/options.hpp>
#include <userver/utils/periodic_task.hpp>
#include <userver/utils/statistics/writer.hpp>

#include <vshard/impl/bucket_calculator.hpp>
#include <vshard/impl/replicaset_pool.hpp>
#include <vshard/impl/routing_table.hpp>
#include <vshard/impl/topology_fetcher.hpp>
#include <vshard/impl/vshard_envelope.hpp>
#include <vshard/impl/vshard_exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard {

/// Strong typedef for bucket identifiers (1-based, range [1, bucket_count]).
using BucketId = std::uint32_t;

/// Settings for VshardProxy construction.
struct VshardProxySettings {
    impl::VshardTopologyConfig topology;

    std::chrono::milliseconds topology_refresh_interval{60'000};
    std::chrono::milliseconds moved_refresh_min_interval{1'000};
    uint32_t max_moved_retries{1};
};

/// @brief C++ reimplementation of the vshard router.
///
/// Eliminates the Lua vshard-router process entirely.  The application calls
/// this class directly; all routing decisions (CRC32 hash → bucket_id →
/// replicaset lookup → MOVED retry) happen in C++ with no Lua involved on the
/// hot path.
///
/// Thread-safe. Routing table reads are lock-free (rcu::Variable).
class VshardProxy final {
 public:
    VshardProxy(clients::dns::Resolver& resolver,
                const components::ComponentConfig& pool_config,
                VshardProxySettings settings);
    ~VshardProxy();

    VshardProxy(const VshardProxy&) = delete;
    VshardProxy& operator=(const VshardProxy&) = delete;

    // ---- Hash-based routing (compute bucket_id from sharding key) -----------

    /// Route by string sharding key (mpcrc32).
    formats::msgpack::Value CallRW(
        std::string_view sharding_key, std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl = {});

    formats::msgpack::Value CallRO(
        std::string_view sharding_key, std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl = {});

    /// Same for integer sharding keys.
    formats::msgpack::Value CallRW(
        int64_t sharding_key, std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl = {});

    formats::msgpack::Value CallRO(
        int64_t sharding_key, std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl = {});

    // ---- Pre-computed bucket_id variants ------------------------------------

    /// Call with an already-computed bucket_id.
    formats::msgpack::Value Call(
        BucketId bucket_id, impl::CallMode mode, std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl = {});

    /// Call with pre-encoded raw msgpack args — no deserialization on the hot path.
    ///
    /// @param args_data  Pointer to a msgpack-encoded value for the args array
    ///                   (e.g. the raw bytes extracted from the client IPROTO_TUPLE
    ///                   by ParseVshardRouterArgs).  The bytes are copied once into
    ///                   the storage-call tuple; no Value tree is constructed.
    formats::msgpack::Value CallRaw(
        BucketId bucket_id, impl::CallMode mode, std::string_view func,
        const uint8_t* args_data, std::size_t args_len,
        storages::tarantool::OptionalCommandControl = {});

    /// Fully zero-copy path: returns raw msgpack bytes of the user function
    /// result with no Value tree constructed at any stage (args in, bytes out).
    std::vector<uint8_t> CallRawBytes(
        BucketId bucket_id, impl::CallMode mode, std::string_view func,
        const uint8_t* args_data, std::size_t args_len,
        storages::tarantool::OptionalCommandControl = {});

    // ---- Zero-copy forwarding -----------------------------------------------

    /// Forward a raw IPROTO CALL body directly to the appropriate storage node.
    ///
    /// @p iproto_body must point to the first byte of the IPROTO CALL body
    /// (the msgpack map that contains IPROTO_FUNCTION_NAME + IPROTO_TUPLE).
    /// ParseCallForRoute() scans ~23 bytes to extract bucket_id and locate
    /// the raw TUPLE, which is then forwarded without msgpack re-encoding.
    ///
    /// @param mode  Master vs replica routing; caller determines from context.
    /// @throws VshardException  on bad parse, routing failure, or retry limit.
    formats::msgpack::Value ForwardCall(
        const uint8_t* iproto_body,
        std::size_t body_len,
        impl::CallMode mode = impl::CallMode::kReadWrite,
        storages::tarantool::OptionalCommandControl = {});

    /// Forward an IPROTO_VSHARD_CALL to the appropriate storage node.
    ///
    /// Parses only the IPROTO header (~26 bytes) to extract bucket_id + mode;
    /// the body is forwarded entirely unchanged (zero body scan).
    /// Requires storage-side IPROTO_VSHARD_CALL handler.
    ///
    /// @param iproto_header  First byte of the IPROTO header map (after preheader).
    /// @param header_len     Bytes available in the header.
    /// @param body           IPROTO body bytes (FUNCTION_NAME + TUPLE).
    /// @param body_len       Size of body.
    /// @throws VshardException  on bad header, routing failure, or retry limit.
    formats::msgpack::Value ForwardVshardCall(
        const uint8_t* iproto_header,
        std::size_t header_len,
        const uint8_t* body,
        std::size_t body_len,
        storages::tarantool::OptionalCommandControl = {});

    // ---- Scatter / Map-Reduce -----------------------------------------------

    /// Fan out to all replicasets in parallel; collect all results.
    /// @throws VshardException if any single replicaset call fails.
    std::vector<formats::msgpack::Value> MapCallRW(
        std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl = {});

    // ---- Utilities ----------------------------------------------------------

    BucketId ComputeBucketId(std::string_view sharding_key) const noexcept;
    BucketId ComputeBucketId(int64_t sharding_key) const noexcept;

    uint32_t GetBucketCount() const noexcept;

    /// Force an immediate full topology refresh (e.g. after deployment).
    void RefreshTopology();

    void WriteStatistics(utils::statistics::Writer& writer) const;

 private:
    formats::msgpack::Value DoCall(
        BucketId bucket_id, impl::CallMode mode, std::string_view func,
        formats::msgpack::ValueBuilder args,
        storages::tarantool::OptionalCommandControl cc);

    /// Shared retry loop used by both DoCall and CallRaw.
    formats::msgpack::Value DoCallWithQuery(
        BucketId bucket_id, impl::CallMode mode,
        const storages::tarantool::Query& query,
        storages::tarantool::OptionalCommandControl cc);

    /// Shared retry loop returning raw bytes — no Value tree at any point.
    std::vector<uint8_t> DoCallRawBytes(
        BucketId bucket_id, impl::CallMode mode,
        const storages::tarantool::Query& query,
        storages::tarantool::OptionalCommandControl cc);

    /// Build the vshard.storage.call argument array:
    /// [bucket_id, mode_str, func_name, args]
    storages::tarantool::Query BuildStorageCallQuery(
        BucketId bucket_id, impl::CallMode mode, std::string_view func,
        formats::msgpack::ValueBuilder args) const;

    /// Same as BuildStorageCallQuery but accepts pre-encoded raw args bytes,
    /// avoiding a full msgpack deserialization round-trip.
    storages::tarantool::Query BuildStorageCallQueryRaw(
        BucketId bucket_id, impl::CallMode mode, std::string_view func,
        const uint8_t* args_data, std::size_t args_len) const;

    void StartRefreshTask();

    /// Return value from HandleVshardError — tells the retry loop what to do.
    enum class RetryAction {
        kRetryImmediate,   ///< Re-read routing snapshot and retry
        kRetrySleep,       ///< Sleep (backoff already done) then retry
        kThrow,            ///< Error was re-thrown; unreachable
    };

    /// Shared vshard error handler used by all retry loops.
    /// Handles WRONG_BUCKET, TRANSFER, BUCKET_IS_LOCKED, NON_MASTER.
    /// Throws on max retries or unknown error types.
    /// On retryable errors, updates routing table / sleeps as needed.
    RetryAction HandleVshardError(
        const impl::VshardError& err,
        BucketId bucket_id,
        uint32_t& attempt,
        rcu::ReadablePtr<impl::RoutingTable>& snapshot,
        impl::ReplicasetPool*& rs);

    /// Find the replicaset for a bucket. If the routing table has no mapping
    /// (bucket_to_rs == 0), probe all RS masters with bucket_stat to discover
    /// the owner on-demand, update the routing table, and return the pool.
    /// Returns nullptr only if truly no RS owns the bucket.
    impl::ReplicasetPool* ResolveReplicaset(
        BucketId bucket_id,
        rcu::ReadablePtr<impl::RoutingTable>& snapshot);

    impl::BucketCalculator calculator_;
    VshardProxySettings settings_;              // must be before fetcher_ (ctor init order)
    impl::RoutingTableHolder routing_table_;
    std::unique_ptr<impl::TopologyFetcher> fetcher_;

    utils::PeriodicTask refresh_task_;
    std::chrono::steady_clock::time_point last_moved_refresh_{};
};

}  // namespace storages::tarantool::vshard

USERVER_NAMESPACE_END
