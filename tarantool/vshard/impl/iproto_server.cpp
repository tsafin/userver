/// @file vshard/impl/iproto_server.cpp
/// @brief Tarantool IPROTO server for the vshard router proxy service.
///
/// ## Protocol reference (from vshard_iproto_analysis.md)
///
/// vshard clients speak standard Tarantool IPROTO.  The handshake sequence
/// performed by net.box is:
///   1. Server → Client : 128-byte greeting (line1: version+UUID, line2: base64 salt)
///   2. Client → Server : IPROTO_ID  (0x49) — feature capability negotiation
///   3. Client → Server : IPROTO_AUTH (0x07) — authentication
///   4. Client → Server : IPROTO_SELECT (0x01) on system spaces (_vspace/_vindex)
///                        — schema fetch; we return empty []
///   5. Client → Server : IPROTO_CALL (0x0A) — actual vshard.router.* calls
///
/// All vshard routing calls use IPROTO_CALL (0x0A) exclusively.
/// IPROTO_SELECT is handled by returning empty schema to satisfy net.box.

#include <vshard/impl/iproto_server.hpp>

#include <array>
#include <atomic>
#include <cctype>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <Client/IprotoConstants.hpp>

#include <Buffer/Buffer.hpp>

#include <fmt/format.h>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/semaphore.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/formats/msgpack/serialize.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/logging/log.hpp>
#include <userver/yaml_config/merge_schemas.hpp>
#include <userver/yaml_config/schema.hpp>

// Shared IPROTO frame utilities: ParseIprotoRequest, msgpack_scan::*, mp::k*
#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/spinlock.hpp>
#include <storages/tarantool/impl/msgpack.hpp>

// For VshardProxyComponent lookup
#include <vshard/vshard_proxy_component.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {

namespace tnt = storages::tarantool::impl;
constexpr std::string_view kThisFile = __FILE__;

// IPROTO request type codes — values not (yet) in tntcxx Iproto enum
constexpr uint8_t kTypeCall16 = 0x06;  // IPROTO_CALL_16 (legacy, pre-2.0)
constexpr uint8_t kTypeId     = 0x49;  // IPROTO_ID — feature negotiation (2.10+)
// IPROTO_ID body keys (Tarantool 2.10+, not in tntcxx enum yet)
constexpr uint8_t kKeyVersion  = 0x54;  // IPROTO_VERSION
constexpr uint8_t kKeyFeatures = 0x55;  // IPROTO_FEATURES

// Schema version reported in all responses
constexpr uint32_t kSchemaVersion = 78;

// ---------------------------------------------------------------------------
// Greeting (128 bytes Tarantool sends on connect)
// ---------------------------------------------------------------------------

// Line 1: "Tarantool <version> (Binary) <uuid>" left-justified to 63 chars + '\n'
// Line 2: base64(32-byte salt) padded to 63 chars + '\n'
// We use a dummy UUID and a dummy all-zero salt.
constexpr std::string_view kGreetingLine1 =
    "Tarantool 2.6.0 (Binary) 00000000-0000-0000-0000-000000000000  \n";
// base64(32 zero bytes) = 43 'A' + '=' (44 chars), padded to 63 chars + '\n'
constexpr std::string_view kGreetingLine2 =
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=                   \n";

static_assert(kGreetingLine1.size() == 64,
              "Greeting line 1 must be exactly 64 bytes");
static_assert(kGreetingLine2.size() == 64,
              "Greeting line 2 must be exactly 64 bytes");

// Build the full 128-byte greeting into a fixed buffer.
std::array<uint8_t, 128> MakeGreeting() {
    std::array<uint8_t, 128> g{};
    std::memcpy(g.data(),      kGreetingLine1.data(), 64);
    std::memcpy(g.data() + 64, kGreetingLine2.data(), 64);
    return g;
}

/// Build an IPROTO OK response for AUTH (empty body).
std::vector<uint8_t> BuildAuthOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    tnt::EncodeArray(body, 0);
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

/// Build an IPROTO OK response for PING (empty data).
std::vector<uint8_t> BuildPingOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    tnt::EncodeArray(body, 0);
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

/// Build an IPROTO OK response for IPROTO_ID: {version: 0, features: []}.
/// Satisfies net.box feature negotiation without advertising any features.
std::vector<uint8_t> BuildIdOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    tnt::EncodeFixMap(body, 2);
    body.push_back(kKeyVersion);
    body.push_back(0x00);  // version 0 (positive fixint)
    body.push_back(kKeyFeatures);
    tnt::EncodeArray(body, 0);
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

/// Build an IPROTO OK response for IPROTO_SELECT: empty tuple set {DATA: []}.
/// net.box fetches schema via SELECT on system spaces during connect; returning
/// empty data lets it proceed without a real box schema.
std::vector<uint8_t> BuildSelectEmptyFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    tnt::EncodeArray(body, 0);
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

// ---------------------------------------------------------------------------
// Request parsing — using shared msgpack_scan::* from iproto_frames.hpp
// No local duplicates; depth-limited SkipValue is inherited from the shared impl.
// ---------------------------------------------------------------------------

/// Parsed CALL body: function name + raw TUPLE bytes (zero-copy).
/// Pointers into the caller-owned frame buffer.
struct CallBody {
    std::string_view func_name;
    const uint8_t*   tuple_begin{nullptr};
    std::size_t      tuple_len{0};
};

/// Scan the body map of an IPROTO CALL request for FUNCTION_NAME and TUPLE.
/// Uses msgpack_scan::ReadStr/ReadUint/SkipValue — no local parser code.
static std::optional<CallBody>
ParseCallBody(const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    std::size_t pos = 0;
    if (pos >= len) return std::nullopt;
    const uint8_t b = p[pos++];
    std::size_t body_n = 0;
    if ((b & 0xf0u) == mp::kFixMapMin) {
        body_n = b & 0x0fu;
    } else if (b == mp::kMap16 && pos + 2 <= len) {
        body_n = (std::size_t)p[pos] << 8 | p[pos + 1]; pos += 2;
    } else {
        return std::nullopt;
    }

    CallBody result;
    for (std::size_t i = 0; i < body_n && pos < len; ++i) {
        auto [k, kp] = ReadUint(p, len, pos); pos = kp;
        if (k == Iproto::FUNCTION_NAME) {
            auto [s, sp] = ReadStr(p, len, pos); pos = sp;
            result.func_name = s;
        } else if (k == Iproto::TUPLE) {
            result.tuple_begin = p + pos;
            pos = SkipValue(p, len, pos);
            result.tuple_len = static_cast<std::size_t>((p + pos) - result.tuple_begin);
        } else {
            pos = SkipValue(p, len, pos);
        }
    }
    if (!result.tuple_begin) return std::nullopt;
    return result;
}

/// vshard.router.call* TUPLE: [bucket_id, inner_func_name, args].
/// args_begin/args_len are raw msgpack bytes — no deserialization needed.
struct VshardRouterArgs {
    uint32_t         bucket_id{0};
    std::string_view func_name;      ///< inner function, e.g. "box.space.customer:replace"
    const uint8_t*   args_begin{nullptr};  ///< raw msgpack value for the args array
    std::size_t      args_len{0};
    std::string_view storage_mode{"write"};  ///< exact mode forwarded to storage.call
    CallMode         mode{CallMode::kReadWrite};  ///< resolved call mode
    bool             prefer_replica{false};
    bool             balance{false};
    double           timeout{0.0};   ///< total timeout in seconds (0 = use default)
    double           request_timeout{0.0};  ///< per-attempt timeout in seconds
    bool             return_raw{false};
    bool             is_async{false};
    bool             opts_valid{true};
};

struct ParsedRouterOpts {
    double timeout{0.0};
    double request_timeout{0.0};
    bool return_raw{false};
    bool is_async{false};
    bool valid{true};
};

struct ParsedBootstrapArgs {
    bool valid{true};
    bool if_not_bootstrapped{false};
    double timeout{0.0};
};

struct ParsedMapCallRWArgs {
    std::string_view func_name;
    const uint8_t* args_begin{nullptr};
    std::size_t args_len{0};
    double timeout{0.0};
    bool opts_valid{true};
    std::optional<std::vector<BucketId>> bucket_ids;
};

struct ParsedInfoArgs {
    bool valid{true};
    bool with_services{false};
};

/// Parse call opts from a msgpack map at the current position.
/// Expects pos to point at a msgpack map.
/// Validates numeric timeout/request_timeout fields like Lua vshard does.
static ParsedRouterOpts ParseCallOpts(const uint8_t* p, std::size_t len,
                                      std::size_t& pos) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    ParsedRouterOpts opts;
    if (!IsMap(p, len, pos)) {
        pos = SkipValue(p, len, pos);
        opts.valid = false;
        return opts;
    }
    const auto map_len = ReadMapHeader(p, len, pos);
    for (std::size_t i = 0; i < map_len; ++i) {
        auto [key, kp] = ReadStr(p, len, pos); pos = kp;
        if (key == "timeout") {
            if (!IsNumber(p, len, pos)) {
                pos = SkipValue(p, len, pos);
                opts.valid = false;
                continue;
            }
            opts.timeout = ReadNumberAsDouble(p, len, pos);
        } else if (key == "request_timeout") {
            if (!IsNumber(p, len, pos)) {
                pos = SkipValue(p, len, pos);
                opts.valid = false;
                continue;
            }
            opts.request_timeout = ReadNumberAsDouble(p, len, pos);
        } else if (key == "return_raw") {
            if (pos >= len) {
                opts.return_raw = false;
                continue;
            }
            const auto val = p[pos];
            opts.return_raw = (val != mp::kNil && val != mp::kFalse);
            pos = SkipValue(p, len, pos);
        } else if (key == "is_async") {
            if (pos >= len) {
                opts.is_async = false;
                continue;
            }
            const auto val = p[pos];
            opts.is_async = (val != mp::kNil && val != mp::kFalse);
            pos = SkipValue(p, len, pos);
        } else {
            pos = SkipValue(p, len, pos);
        }
    }
    return opts;
}

static ParsedBootstrapArgs ParseBootstrapArgs(
    const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    ParsedBootstrapArgs out;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen == 0) return out;
    if (alen != 1 || !IsMap(p, len, pos)) {
        out.valid = false;
        return out;
    }

    const auto map_len = ReadMapHeader(p, len, pos);
    for (std::size_t i = 0; i < map_len; ++i) {
        auto [key, kp] = ReadStr(p, len, pos);
        pos = kp;
        if (key == "if_not_bootstrapped") {
            if (pos >= len) {
                out.if_not_bootstrapped = false;
                continue;
            }
            const auto val = p[pos];
            out.if_not_bootstrapped = (val != mp::kNil && val != mp::kFalse);
            pos = SkipValue(p, len, pos);
        } else if (key == "timeout") {
            if (!IsNumber(p, len, pos)) {
                pos = SkipValue(p, len, pos);
                out.valid = false;
                continue;
            }
            out.timeout = ReadNumberAsDouble(p, len, pos);
        } else {
            pos = SkipValue(p, len, pos);
        }
    }
    return out;
}

static std::optional<std::vector<BucketId>> ParseBucketIdArray(
    const uint8_t* p, std::size_t len, std::size_t& pos) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    const auto count = ReadArrayHeader(p, len, pos);
    std::vector<BucketId> bucket_ids;
    bucket_ids.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        if (!IsNumber(p, len, pos)) return std::nullopt;
        auto [bid, np] = ReadUint(p, len, pos);
        pos = np;
        bucket_ids.push_back(static_cast<BucketId>(bid));
    }
    return bucket_ids;
}

static ParsedMapCallRWArgs ParseMapCallRWArgs(
    const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    ParsedMapCallRWArgs out;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen < 2) {
        out.opts_valid = false;
        return out;
    }

    auto [fn, fp] = ReadStr(p, len, pos);
    pos = fp;
    if (fn.empty()) {
        out.opts_valid = false;
        return out;
    }
    out.func_name = fn;

    out.args_begin = p + pos;
    pos = SkipValue(p, len, pos);
    out.args_len = static_cast<std::size_t>((p + pos) - out.args_begin);

    if (alen < 3 || pos >= len) return out;
    if (!IsMap(p, len, pos)) {
        out.opts_valid = false;
        pos = SkipValue(p, len, pos);
        return out;
    }

    const auto map_len = ReadMapHeader(p, len, pos);
    for (std::size_t i = 0; i < map_len; ++i) {
        auto [key, kp] = ReadStr(p, len, pos);
        pos = kp;
        if (key == "timeout") {
            if (!IsNumber(p, len, pos)) {
                out.opts_valid = false;
                pos = SkipValue(p, len, pos);
                continue;
            }
            out.timeout = ReadNumberAsDouble(p, len, pos);
        } else if (key == "bucket_ids") {
            auto bucket_ids = ParseBucketIdArray(p, len, pos);
            if (!bucket_ids) {
                out.opts_valid = false;
                return out;
            }
            out.bucket_ids = std::move(bucket_ids);
        } else {
            pos = SkipValue(p, len, pos);
        }
    }

    return out;
}

static ParsedInfoArgs ParseInfoArgs(
    const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    ParsedInfoArgs out;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen == 0) return out;

    if (pos >= len) return out;
    const auto first = p[pos];
    if (first == mp::kNil || first == mp::kFalse) {
        return out;
    }
    if (first == mp::kTrue) {
        out.with_services = true;
        return out;
    }
    if (!IsMap(p, len, pos)) {
        out.valid = false;
        return out;
    }

    const auto map_len = ReadMapHeader(p, len, pos);
    for (std::size_t i = 0; i < map_len; ++i) {
        auto [key, kp] = ReadStr(p, len, pos);
        pos = kp;
        if (key == "with_services") {
            if (pos >= len) {
                out.with_services = false;
                return out;
            }
            const auto val = p[pos];
            out.with_services = (val != mp::kNil && val != mp::kFalse);
            pos = SkipValue(p, len, pos);
        } else {
            pos = SkipValue(p, len, pos);
        }
    }

    return out;
}

static std::optional<std::chrono::milliseconds> MillisecondsFromSeconds(
    double seconds) noexcept {
    if (seconds <= 0.0) return std::nullopt;
    return std::chrono::milliseconds{
        static_cast<int64_t>(seconds * 1000.0)};
}

/// Resolve CallMode from a mode string ("read" or "write").
static CallMode ModeFromString(std::string_view s) noexcept {
    // Lua vshard treats only exact "read" as read mode.
    // Any other string falls back to write mode.
    if (s == "read") return CallMode::kReadOnly;
    return CallMode::kReadWrite;
}

/// Resolve the final CallMode from base mode + prefer_replica + balance flags.
/// This matches the Lua vshard wrapper dispatch:
///   callro  = read, false, false → kReadOnly
///   callbro = read, false, true  → kBestReadOnly
///   callre  = read, true,  false → kReadOnly (prefer_replica, master fallback)
///   callbre = read, true,  true  → kBestReadOnlyError
static CallMode ResolveCallMode(CallMode base, bool prefer_replica,
                                bool balance) noexcept {
    if (base == CallMode::kReadWrite) return CallMode::kReadWrite;
    // Read modes: resolve based on flags
    if (prefer_replica && balance) return CallMode::kBestReadOnlyError;
    if (balance) return CallMode::kBestReadOnly;
    // prefer_replica alone or plain read → kReadOnly (replicas with master fallback)
    return CallMode::kReadOnly;
}

/// Parse the TUPLE array from a vshard.router.callrw/callro/etc request.
/// Format: [bucket_id, func_name, args_array]
static std::optional<VshardRouterArgs>
ParseVshardRouterArgs(const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen < 3) return std::nullopt;

    VshardRouterArgs out;

    auto [bid, bp] = ReadUint(p, len, pos); pos = bp;
    out.bucket_id = static_cast<uint32_t>(bid);

    auto [fn, fp] = ReadStr(p, len, pos); pos = fp;
    if (fn.empty()) return std::nullopt;
    out.func_name = fn;

    // args: record raw bytes without deserializing
    out.args_begin = p + pos;
    pos = SkipValue(p, len, pos);
    out.args_len = static_cast<std::size_t>((p + pos) - out.args_begin);

    // opts (optional 4th element): parse timeout
    if (alen >= 4 && pos < len) {
        const auto opts = ParseCallOpts(p, len, pos);
        out.timeout = opts.timeout;
        out.request_timeout = opts.request_timeout;
        out.return_raw = opts.return_raw;
        out.is_async = opts.is_async;
        out.opts_valid = opts.valid;
    }

    return out;
}

/// Parse the TUPLE array from a generic vshard.router.call request.
/// Format: [bucket_id, mode_string_or_opts_table, func_name, args_array[, opts]]
/// The 2nd element is either a string ("read"/"write") or a table with
/// {mode="read"/"write", prefer_replica=bool, balance=bool}.
static std::optional<VshardRouterArgs>
ParseVshardRouterCallArgs(const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen < 4) return std::nullopt;  // need at least [bucket_id, mode, func, args]

    VshardRouterArgs out;

    // 1. bucket_id
    auto [bid, bp] = ReadUint(p, len, pos); pos = bp;
    out.bucket_id = static_cast<uint32_t>(bid);

    // 2. mode: string or map
    if (IsStr(p, len, pos)) {
        auto [mode_str, mp2] = ReadStr(p, len, pos); pos = mp2;
        out.storage_mode = mode_str;
        out.mode = ModeFromString(mode_str);
    } else if (IsMap(p, len, pos)) {
        // Parse opts table: {mode=str, prefer_replica=bool, balance=bool}
        const auto save_pos = pos;
        const auto map_len = ReadMapHeader(p, len, pos);
        for (std::size_t i = 0; i < map_len; ++i) {
            auto [key, kp] = ReadStr(p, len, pos); pos = kp;
            if (key == "mode") {
                auto [val, vp] = ReadStr(p, len, pos); pos = vp;
                out.storage_mode = val;
                out.mode = ModeFromString(val);
            } else if (key == "prefer_replica") {
                // Read bool: true (0xc3) or false (0xc2)
                if (pos < len && p[pos] == 0xc3) out.prefer_replica = true;
                pos = SkipValue(p, len, pos);
            } else if (key == "balance") {
                if (pos < len && p[pos] == 0xc3) out.balance = true;
                pos = SkipValue(p, len, pos);
            } else {
                pos = SkipValue(p, len, pos);  // skip unknown key's value
            }
        }
        (void)save_pos;
    } else {
        return std::nullopt;  // invalid 2nd argument type
    }

    // 3. func_name
    auto [fn, fp] = ReadStr(p, len, pos); pos = fp;
    if (fn.empty()) return std::nullopt;
    out.func_name = fn;

    // 4. args: record raw bytes without deserializing
    out.args_begin = p + pos;
    pos = SkipValue(p, len, pos);
    out.args_len = static_cast<std::size_t>((p + pos) - out.args_begin);

    // 5. opts (optional) — parse timeout
    if (alen >= 5 && pos < len) {
        const auto opts = ParseCallOpts(p, len, pos);
        out.timeout = opts.timeout;
        out.request_timeout = opts.request_timeout;
        out.return_raw = opts.return_raw;
        out.is_async = opts.is_async;
        out.opts_valid = opts.valid;
    }

    // Resolve final CallMode from base mode + prefer_replica + balance flags
    out.mode = ResolveCallMode(out.mode, out.prefer_replica, out.balance);

    return out;
}

// ---------------------------------------------------------------------------
// Build the IPROTO OK response carrying the VshardProxy result value.
// ---------------------------------------------------------------------------

/// Map encoding header for N entries.
inline void PushMapHeader(std::vector<uint8_t>& buf, std::size_t n) {
    if (n <= 15) {
        buf.push_back(static_cast<uint8_t>(mp::kFixMapMin | (n & 0x0fu)));
    } else if (n <= 0xffffu) {
        buf.push_back(mp::kMap16);
        buf.push_back(static_cast<uint8_t>(n >> 8));
        buf.push_back(static_cast<uint8_t>(n));
    } else {
        buf.push_back(mp::kMap32);
        buf.push_back(static_cast<uint8_t>(n >> 24));
        buf.push_back(static_cast<uint8_t>(n >> 16));
        buf.push_back(static_cast<uint8_t>(n >> 8));
        buf.push_back(static_cast<uint8_t>(n));
    }
}

/// Read the first element of a 1-element msgpack array as a sharding key.
/// Returns {is_string=true, str_val, 0} for strings.
/// Returns {is_string=false, {}, int_val} for integers.
/// Returns std::nullopt if the tuple cannot be parsed.
struct TupleKey {
    bool is_string{false};
    std::string_view str_val;
    int64_t int_val{0};
};

struct OptionalTimeoutArg {
    bool valid{false};
    std::optional<double> timeout_seconds;
};

static std::optional<TupleKey> ReadTupleFirstKey(
    const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen < 1 || pos >= len) return std::nullopt;
    const uint8_t b = p[pos];
    // String
    if ((b & 0xe0u) == mp::kFixStrMin || b == mp::kStr8 ||
        b == mp::kStr16 || b == mp::kStr32) {
        auto [sv, np] = ReadStr(p, len, pos);
        (void)np;
        return TupleKey{true, sv, 0};
    }
    // Negative fixint (0xe0..0xff → -32..-1)
    if (b >= mp::kNegFixIntMin) {
        return TupleKey{false, {}, static_cast<int64_t>(static_cast<int8_t>(b))};
    }
    // Signed ints
    if (b == mp::kInt8 && pos + 1 < len)
        return TupleKey{false, {}, static_cast<int64_t>(static_cast<int8_t>(p[pos + 1]))};
    if (b == mp::kInt16 && pos + 2 < len) {
        int16_t v = static_cast<int16_t>((uint16_t)p[pos + 1] << 8 | p[pos + 2]);
        return TupleKey{false, {}, static_cast<int64_t>(v)};
    }
    if (b == mp::kInt32 && pos + 4 < len) {
        int32_t v = static_cast<int32_t>(
            (uint32_t)p[pos + 1] << 24 | (uint32_t)p[pos + 2] << 16 |
            (uint32_t)p[pos + 3] << 8 | p[pos + 4]);
        return TupleKey{false, {}, static_cast<int64_t>(v)};
    }
    if (b == mp::kInt64 && pos + 8 < len) {
        int64_t v = 0;
        for (int i = 1; i <= 8; ++i) v = (v << 8) | p[pos + i];
        return TupleKey{false, {}, v};
    }
    // Unsigned: use ReadUint (handles fixint, uint8/16/32/64)
    auto [uval, np] = ReadUint(p, len, pos);
    (void)np;
    return TupleKey{false, {}, static_cast<int64_t>(uval)};
}

static OptionalTimeoutArg ParseOptionalTimeoutArg(
    const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    std::size_t pos = 0;
    const auto alen = ReadArrayHeader(p, len, pos);
    if (alen == 0) return {true, std::nullopt};
    if (alen != 1 || !IsNumber(p, len, pos)) return {false, std::nullopt};
    return {true, ReadNumberAsDouble(p, len, pos)};
}

/// Build DATA response carrying a single uint32 (used for bucket_id_mpcrc32).
static std::vector<uint8_t> BuildUint32ResultFrame(uint64_t sync, uint32_t val) {
    std::vector<uint8_t> body;
    body.reserve(8);
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    tnt::EncodeArray(body, 1);
    tnt::EncodeUint(body, val);
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

static std::vector<uint8_t> BuildRouteResultFrame(
    uint64_t sync, std::string_view uuid) {
    std::vector<uint8_t> inner;
    inner.reserve(uuid.size() + 16);
    tnt::EncodeFixMap(inner, 1);
    tnt::EncodeStr(inner, "uuid");
    tnt::EncodeStr(inner, uuid);

    std::vector<uint8_t> body;
    body.reserve(3 + inner.size());
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    tnt::EncodeArray(body, 1);
    body.insert(body.end(), inner.begin(), inner.end());
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

static bool IsConnectivityErrorMessage(std::string_view message) {
    return message.find("Error while establishing connection") !=
               std::string_view::npos ||
           message.find("connection is broken") != std::string_view::npos ||
           message.find("Failed to connect to ") != std::string_view::npos;
}

static std::string NormalizeNetboxClientErrorMessage(std::string_view message) {
    const auto detail_pos = message.find("Error while establishing connection");
    if (detail_pos != std::string_view::npos) {
        message = message.substr(0, detail_pos);
    }
    constexpr std::string_view kSocketPrefix = "Socket: ";
    if (message.substr(0, kSocketPrefix.size()) == kSocketPrefix) {
        message.remove_prefix(kSocketPrefix.size());
    }
    while (!message.empty() &&
           std::isspace(static_cast<unsigned char>(message.back()))) {
        message.remove_suffix(1);
    }
    if (!message.empty()) {
        return std::string{message};
    }
    return "Connection refused";
}

static std::vector<uint8_t> BuildNetboxClientErrorReturn(
    std::string_view message) {
    static constexpr uint32_t kNoConnectionCode = 77;

    std::vector<uint8_t> payload;
    const auto normalized = NormalizeNetboxClientErrorMessage(message);
    payload.reserve(128 + normalized.size());

    tnt::EncodeArray(payload, 2);
    payload.push_back(mp::kNil);
    PushMapHeader(payload, 5);
    tnt::EncodeStr(payload, "code");
    tnt::EncodeUint(payload, kNoConnectionCode);
    tnt::EncodeStr(payload, "base_type");
    tnt::EncodeStr(payload, "ClientError");
    tnt::EncodeStr(payload, "type");
    tnt::EncodeStr(payload, "ClientError");
    tnt::EncodeStr(payload, "message");
    tnt::EncodeStr(payload, normalized);
    tnt::EncodeStr(payload, "trace");
    tnt::EncodeArray(payload, 1);
    PushMapHeader(payload, 2);
    tnt::EncodeStr(payload, "file");
    tnt::EncodeStr(payload, kThisFile);
    tnt::EncodeStr(payload, "line");
    tnt::EncodeUint(payload, __LINE__);
    return payload;
}

static std::vector<uint8_t> BuildTimeoutClientErrorReturn(
    std::optional<std::string_view> replicaset_id) {
    static constexpr uint32_t kTimeoutCode = 78;

    std::vector<uint8_t> payload;
    payload.reserve(192);

    tnt::EncodeArray(payload, 2);
    payload.push_back(mp::kNil);
    PushMapHeader(payload, replicaset_id ? 6 : 5);
    tnt::EncodeStr(payload, "code");
    tnt::EncodeUint(payload, kTimeoutCode);
    tnt::EncodeStr(payload, "base_type");
    tnt::EncodeStr(payload, "ClientError");
    tnt::EncodeStr(payload, "type");
    tnt::EncodeStr(payload, "ClientError");
    tnt::EncodeStr(payload, "message");
    tnt::EncodeStr(payload, "Timeout exceeded");
    if (replicaset_id) {
        tnt::EncodeStr(payload, "replicaset");
        tnt::EncodeStr(payload, *replicaset_id);
    }
    tnt::EncodeStr(payload, "trace");
    tnt::EncodeArray(payload, 1);
    PushMapHeader(payload, 2);
    tnt::EncodeStr(payload, "file");
    tnt::EncodeStr(payload, kThisFile);
    tnt::EncodeStr(payload, "line");
    tnt::EncodeUint(payload, __LINE__);
    return payload;
}

static std::vector<uint8_t> BuildMapCallRWClientErrorReturn(
    std::string_view message, uint32_t code,
    std::optional<std::string_view> replicaset_id) {
    std::vector<uint8_t> payload;
    payload.reserve(192 + message.size() +
                    (replicaset_id ? replicaset_id->size() : 0));

    tnt::EncodeArray(payload, replicaset_id ? 3 : 2);
    payload.push_back(mp::kNil);
    PushMapHeader(payload, 5);
    tnt::EncodeStr(payload, "code");
    tnt::EncodeUint(payload, code);
    tnt::EncodeStr(payload, "base_type");
    tnt::EncodeStr(payload, "ClientError");
    tnt::EncodeStr(payload, "type");
    tnt::EncodeStr(payload, "ClientError");
    tnt::EncodeStr(payload, "message");
    tnt::EncodeStr(payload, message);
    tnt::EncodeStr(payload, "trace");
    tnt::EncodeArray(payload, 1);
    PushMapHeader(payload, 2);
    tnt::EncodeStr(payload, "file");
    tnt::EncodeStr(payload, kThisFile);
    tnt::EncodeStr(payload, "line");
    tnt::EncodeUint(payload, __LINE__);
    if (replicaset_id) {
        tnt::EncodeStr(payload, *replicaset_id);
    }
    return payload;
}

static std::vector<uint8_t> BuildRouterShardingErrorReturn(
    uint32_t code, std::string_view name, std::string_view message,
    std::optional<std::string_view> replicaset_id,
    std::optional<uint32_t> bucket_id) {
    std::size_t field_count = 4;
    if (replicaset_id) ++field_count;
    if (bucket_id) ++field_count;

    std::vector<uint8_t> payload;
    payload.reserve(128 + message.size() + name.size());

    tnt::EncodeArray(payload, 2);
    payload.push_back(mp::kNil);
    PushMapHeader(payload, field_count);
    tnt::EncodeStr(payload, "message");
    tnt::EncodeStr(payload, message);
    tnt::EncodeStr(payload, "type");
    tnt::EncodeStr(payload, "ShardingError");
    tnt::EncodeStr(payload, "code");
    tnt::EncodeUint(payload, code);
    tnt::EncodeStr(payload, "name");
    tnt::EncodeStr(payload, name);
    if (replicaset_id) {
        tnt::EncodeStr(payload, "replicaset");
        tnt::EncodeStr(payload, *replicaset_id);
    }
    if (bucket_id) {
        tnt::EncodeStr(payload, "bucket_id");
        tnt::EncodeUint(payload, *bucket_id);
    }
    return payload;
}

/// Encode {DATA: [result_value]} and build the full OK frame.
[[maybe_unused]] static std::vector<uint8_t> BuildResultFrame(
    uint64_t sync, const formats::msgpack::Value& result) {
    const auto result_bytes =
        formats::msgpack::ToBytes(formats::msgpack::ValueBuilder{result});

    // body = fixmap(1) + {DATA: [result_value]}
    std::vector<uint8_t> body;
    body.reserve(3 + result_bytes.size());
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    tnt::EncodeArray(body, 1);  // net.box unpacks the outer array as multi-return
    body.insert(body.end(), result_bytes.begin(), result_bytes.end());

    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

/// Zero-copy variant: return_values_data is already a msgpack-encoded array of
/// router return values (for example [result] or [nil, err]).
static std::vector<uint8_t> BuildResultFrameRaw(
    uint64_t sync, const uint8_t* return_values_data, std::size_t return_values_len) {
    // body = fixmap(1) + {DATA: raw_return_values_array}
    std::vector<uint8_t> body;
    body.reserve(2 + return_values_len);
    tnt::EncodeFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    body.insert(
        body.end(), return_values_data, return_values_data + return_values_len);
    return tnt::BuildIprotoOkFrame(sync, kSchemaVersion, body.data(), body.size());
}

static std::vector<uint8_t> BuildAsyncFuturePlaceholderReturn() {
    std::vector<uint8_t> result;
    tnt::EncodeArray(result, 1);
    tnt::EncodeArray(result, 0);
    return result;
}

static std::string BuildReturnRawUnsupportedError() {
    return std::string{kThisFile} +
           ":" + std::to_string(__LINE__) +
           ": Msgpack object feature is not supported by current Tarantool version";
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

static constexpr std::size_t kMaxFrameSize = 16 * 1024 * 1024;  // 16 MB

// ---------------------------------------------------------------------------
// Per-connection I/O state: buffered reads + batched writes (Phase 1/1c)
// ---------------------------------------------------------------------------
//
// The receive side reads through a ring buffer (tnt::Buffer) filled by one
// RecvSome syscall per fill, replacing the previous scheme of one recv for
// the 1-byte length prefix plus another recv for the body (2-3 syscalls per
// request).  Frames are parsed from a reusable flat buffer that grows to the
// high-water mark of frame sizes, so after warmup there are no allocations.
//
// The send side is served by a dedicated flush task: request handlers stage
// replies in out_buf_ (thread-safe) and signal out_event_; FlushLoop drains
// the staging buffer with one SendAll per batch.  With a synchronous client
// this degenerates to one SendAll per reply (as before); with pipelined
// requests, replies accumulated within one scheduler slice coalesce into a
// single syscall.

class ServerConn {
 public:
    explicit ServerConn(engine::io::Socket sock) : sock_{std::move(sock)} {
        recv_tmp_.resize(kRecvTmpSize);
        frame_buf_.reserve(4096);
        out_buf_.reserve(4096);
    }

    // ---- Receive side (single reader coroutine only) ---------------------

    /// Read more bytes into the ring (blocking).  Throws on EOF or error.
    void Fill() {
        const auto n = sock_.RecvSome(recv_tmp_.data(), recv_tmp_.size(),
                                      engine::Deadline{});
        if (n == 0) throw std::runtime_error{"connection closed by peer"};
        rbuf_.write(RecvBuf::WData{reinterpret_cast<char*>(recv_tmp_.data()),
                                   n});
    }

    /// Ensure at least @p n unread bytes are available at rpos_.
    void Ensure(std::size_t n) {
        while (!rbuf_.has(rpos_, n)) Fill();
    }

    /// True if at least one unread byte is already buffered (i.e. the client
    /// has pipelined more data); used to decide whether to keep batching.
    bool HasBufferedInput() { return rbuf_.has(rpos_, 1); }

    /// Decode the next frame length from the buffered msgpack uint prefix,
    /// advancing past it.  IPROTO prefixes are msgpack-encoded uint32 values,
    /// so small frames use fixint (1 byte), uint8 (2), uint16 (3) or uint32
    /// (5) encodings.
    uint32_t ReadFrameLengthPrefix() {
        Ensure(1);
        uint8_t lead = 0;
        ReadBytes(&lead, 1);
        consumed_ += 1;
        if (lead <= 0x7f) return lead;  // positive fixint
        switch (lead) {
            case 0xcc: {  // uint8
                Ensure(1);
                uint8_t v = 0;
                ReadBytes(&v, 1);
                consumed_ += 1;
                return v;
            }
            case 0xcd: {  // uint16
                Ensure(2);
                uint8_t v[2] = {0, 0};
                ReadBytes(v, 2);
                consumed_ += 2;
                return (uint32_t(v[0]) << 8) | uint32_t(v[1]);
            }
            case 0xce: {  // uint32
                Ensure(4);
                uint8_t v[4] = {0, 0, 0, 0};
                ReadBytes(v, 4);
                consumed_ += 4;
                return (uint32_t(v[0]) << 24) | (uint32_t(v[1]) << 16) |
                       (uint32_t(v[2]) << 8) | uint32_t(v[3]);
            }
            default:
                throw std::runtime_error{
                    fmt::format("iproto_server: unexpected length prefix "
                                "byte 0x{:02x}",
                                lead)};
        }
    }

    /// Copy the next @p body_len bytes into the flat parse buffer and release
    /// the consumed ring bytes.  The returned pointer stays valid until the
    /// next ReadFrameBody() call.
    const uint8_t* ReadFrameBody(std::size_t body_len) {
        Ensure(body_len);
        if (frame_buf_.size() < body_len) frame_buf_.resize(body_len);
        rpos_.read(RecvBuf::RData{reinterpret_cast<char*>(frame_buf_.data()),
                                  body_len});
        consumed_ += body_len;
        // Release the consumed bytes; rpos_ has already advanced past them,
        // so no registered iterator points into the freed region.
        rbuf_.dropFront(consumed_);
        consumed_ = 0;
        return frame_buf_.data();
    }

    // ---- Send side (thread-safe: reader + request-handler coroutines) ----

    /// Stage an outgoing reply and wake the flush task (only if parked).
    void Send(const uint8_t* p, std::size_t n) {
        {
            std::lock_guard lock(out_mutex_);
            out_buf_.insert(out_buf_.end(), p, p + n);
        }
        if (out_sleeping_.load(std::memory_order_acquire)) {
            out_event_.Send();
        }
    }

    /// Flush task body: drains out_buf_ until the connection is closing.
    void FlushLoop() {
        for (;;) {
            DrainOut();
            out_sleeping_.store(true, std::memory_order_release);
            {
                std::lock_guard lock(out_mutex_);
                if (!out_buf_.empty()) {
                    out_sleeping_.store(false, std::memory_order_relaxed);
                    continue;
                }
            }
            if (!out_event_.WaitForEvent()) break;  // cancelled
            out_sleeping_.store(false, std::memory_order_release);
            if (closing_.load(std::memory_order_acquire)) break;
            // Yield so that other handlers staging in this scheduler slice
            // land in the same batch.
            engine::Yield();
        }
        DrainOut();
    }

    /// Mark the connection as closing and wake the flush task one last time.
    void StartClosing() {
        closing_.store(true, std::memory_order_release);
        out_event_.Send();
    }

    engine::io::Socket& Socket() noexcept { return sock_; }

 private:
    void ReadBytes(void* dst, std::size_t n) {
        rpos_.read(RecvBuf::RData{static_cast<char*>(dst), n});
    }

    void DrainOut() {
            while (true) {
            std::vector<uint8_t> batch;
            {
                std::lock_guard lock(out_mutex_);
                if (out_buf_.empty()) return;
                batch.swap(out_buf_);
            }
            sock_.SendAll(batch.data(), batch.size(), engine::Deadline{});
        }
    }

    using RecvBuf = ::tnt::Buffer<16384>;
    static constexpr std::size_t kRecvTmpSize = 65536;

    engine::io::Socket sock_;
    RecvBuf rbuf_;
    RecvBuf::iterator rpos_{rbuf_.begin()};
    std::vector<char> recv_tmp_;
    std::vector<uint8_t> frame_buf_;  // flat copy of the current frame body
    std::size_t consumed_{0};         // ring bytes consumed since last dropFront

    // Send side
    storages::tarantool::impl::SpinLock out_mutex_;
    engine::SingleConsumerEvent out_event_;
    std::atomic<bool> out_sleeping_{false};
    std::vector<uint8_t> out_buf_;    // staged replies (under out_mutex_)
    std::atomic<bool> closing_{false};
};

/// Max concurrently executing vshard CALL handler tasks per connection.
/// Bounds memory (each holds a small arg copy + result buffer) and gives
/// natural backpressure against unbounded client pipelining.
static constexpr std::size_t kMaxRequestsInFlight = 512;

static void HandleConnection(engine::io::Socket sock,
                              VshardProxy& proxy) {
    ServerConn conn{std::move(sock)};

    // 1. Send greeting (direct, before batching is active)
    const auto greeting = MakeGreeting();
    try {
        (void)conn.Socket().SendAll(greeting.data(), greeting.size(),
                                    engine::Deadline{});
    } catch (const std::exception& ex) {
        LOG_DEBUG() << "iproto_server: greeting send failed: " << ex.what();
        return;
    }

    // 2. Reply flush task: request handlers stage replies via conn.Send()
    //    and the flush task coalesces them into one SendAll per batch.
    engine::Semaphore in_flight{kMaxRequestsInFlight};
    engine::TaskWithResult<void> flush_task =
        engine::AsyncNoSpan([&conn] { conn.FlushLoop(); });

    // 3. Request loop — frames arrive through the ring buffer; replies are
    // staged by the per-request handler tasks and flushed by FlushLoop.
    while (true) {
        // Read frame length: variable-length msgpack uint
        uint32_t body_len = 0;
        try {
            body_len = conn.ReadFrameLengthPrefix();
        } catch (...) {
            break;
        }

        if (body_len > kMaxFrameSize) {
            LOG_WARNING() << "iproto_server: oversized frame " << body_len;
            break;
        }

        // Read body (flat copy out of the ring, zero alloc after warmup)
        const uint8_t* frame = nullptr;
        try {
            frame = conn.ReadFrameBody(body_len);
        } catch (...) {
            break;
        }

        // Parse header using shared ParseIprotoRequest from iproto_frames.hpp
        const auto req = storages::tarantool::impl::ParseIprotoRequest(
            frame, body_len);
        if (req.type == 0 && req.sync == 0) {
            LOG_WARNING() << "iproto_server: malformed frame, closing";
            break;
        }

        // Dispatch
        try {
            if (req.type == kTypeId) {
                const auto resp = BuildIdOkFrame(req.sync);
                (void)conn.Send(resp.data(), resp.size());

            } else if (req.type == Iproto::AUTH) {
                // Accept any credentials without verification.
                const auto resp = BuildAuthOkFrame(req.sync);
                (void)conn.Send(resp.data(), resp.size());

            } else if (req.type == Iproto::PING) {
                const auto resp = BuildPingOkFrame(req.sync);
                (void)conn.Send(resp.data(), resp.size());

            } else if (req.type == Iproto::SELECT) {
                // net.box fetches schema on connect; return empty data.
                const auto resp = BuildSelectEmptyFrame(req.sync);
                (void)conn.Send(resp.data(), resp.size());

            } else if (req.type == Iproto::CALL || req.type == kTypeCall16) {
                // Both CALL (0x0A) and legacy CALL_16 (0x06) carry
                // FUNCTION_NAME (0x22) + TUPLE (0x21) in the body.
                if (!req.body_begin) {
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion, "missing body");
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }

                const auto body = ParseCallBody(req.body_begin, req.body_len);
                if (!body || !body->tuple_begin) {
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion, "missing args");
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }

                // Map outer vshard function name to read/write mode.
                CallMode mode = CallMode::kReadOnly;
                bool is_generic_call = false;
                if (body->func_name == "vshard.router.callrw") {
                    mode = CallMode::kReadWrite;
                } else if (body->func_name == "vshard.router.callro") {
                    mode = CallMode::kReadOnly;
                } else if (body->func_name == "vshard.router.callbro") {
                    mode = CallMode::kBestReadOnly;
                } else if (body->func_name == "vshard.router.callre") {
                    mode = CallMode::kReadOnly;  // prefer_replica=true, master fallback
                } else if (body->func_name == "vshard.router.callbre") {
                    mode = CallMode::kBestReadOnlyError;  // prefer_replica + balance
                } else if (body->func_name == "vshard.router.call") {
                    is_generic_call = true;

                // ---- Utility functions (no routing) -------------------------

                } else if (body->func_name == "vshard.router.bucket_id_mpcrc32") {
                    // [key] → bucket_id
                    const auto key = ReadTupleFirstKey(
                        body->tuple_begin, body->tuple_len);
                    if (!key) {
                        const auto resp = tnt::BuildIprotoErrorFrame(req.sync,
                            kSchemaVersion,
                            "bad bucket_id_mpcrc32 args: expected [key]");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }
                    const uint32_t bid = key->is_string
                        ? proxy.ComputeBucketId(key->str_val)
                        : proxy.ComputeBucketId(key->int_val);
                    const auto resp = BuildUint32ResultFrame(req.sync, bid);
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.bucket_id_strcrc32") {
                    const auto key = ReadTupleFirstKey(
                        body->tuple_begin, body->tuple_len);
                    if (!key || !key->is_string) {
                        const auto resp = tnt::BuildIprotoErrorFrame(
                            req.sync, kSchemaVersion,
                            "bad bucket_id_strcrc32 args: expected [string_key]");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }
                    const uint32_t bid = proxy.ComputeBucketId(key->str_val);
                    const auto resp = BuildUint32ResultFrame(req.sync, bid);
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.route") {
                    const auto key = ReadTupleFirstKey(
                        body->tuple_begin, body->tuple_len);
                    if (!key || key->is_string || key->int_val < 1) {
                        const auto resp = tnt::BuildIprotoErrorFrame(
                            req.sync, kSchemaVersion,
                            "bad route args: expected [bucket_id]");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }
                    const auto uuid = proxy.Route(
                        static_cast<BucketId>(key->int_val));
                    const auto resp = BuildRouteResultFrame(req.sync, uuid);
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.routeall") {
                    const auto routeall = proxy.GetRouteAll();
                    const auto resp = BuildResultFrame(req.sync, routeall);
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.info") {
                    const auto info_args = ParseInfoArgs(
                        body->tuple_begin, body->tuple_len);
                    if (!info_args.valid) {
                        const auto resp = tnt::BuildIprotoErrorFrame(
                            req.sync, kSchemaVersion,
                            "bad info args: expected [] or [{with_services=...}]");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }
                    const auto info = proxy.GetInfo(info_args.with_services);
                    const auto resp = BuildResultFrame(req.sync, info);
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.sync") {
                    const auto timeout_arg = ParseOptionalTimeoutArg(
                        body->tuple_begin, body->tuple_len);
                    if (!timeout_arg.valid) {
                        const auto resp = tnt::BuildIprotoErrorFrame(
                            req.sync, kSchemaVersion,
                            "Usage: vshard.router.sync([timeout: number])");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }

                    const double timeout =
                        timeout_arg.timeout_seconds.value_or(1.0);
                    if (timeout < 0.0) {
                        const auto result_bytes =
                            BuildTimeoutClientErrorReturn(std::nullopt);
                        const auto resp = BuildResultFrameRaw(
                            req.sync, result_bytes.data(), result_bytes.size());
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }

                    const auto sync_result = proxy.Sync(timeout);
                    if (!sync_result.ok) {
                        const auto result_bytes = BuildTimeoutClientErrorReturn(
                            sync_result.failed_replicaset_id
                                ? std::optional<std::string_view>{
                                      *sync_result.failed_replicaset_id}
                                : std::nullopt);
                        const auto resp = BuildResultFrameRaw(
                            req.sync, result_bytes.data(), result_bytes.size());
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }

                    const std::array<uint8_t, 2> result_bytes{
                        static_cast<uint8_t>(mp::kFixArrayMin | 1), mp::kTrue};
                    const auto resp = BuildResultFrameRaw(
                        req.sync, result_bytes.data(), result_bytes.size());
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.bootstrap") {
                    const auto bootstrap_args = ParseBootstrapArgs(
                        body->tuple_begin, body->tuple_len);
                    if (!bootstrap_args.valid) {
                        const auto resp = tnt::BuildIprotoErrorFrame(
                            req.sync, kSchemaVersion,
                            "Usage: vshard.router.bootstrap({<options>})");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }

                    storages::tarantool::OptionalCommandControl cc;
                    if (const auto timeout_ms =
                            MillisecondsFromSeconds(bootstrap_args.timeout)) {
                        cc = storages::tarantool::CommandControl{*timeout_ms};
                    }
                    const auto result_bytes = proxy.Bootstrap(
                        bootstrap_args.if_not_bootstrapped, cc);
                    const auto resp = BuildResultFrameRaw(
                        req.sync, result_bytes.data(), result_bytes.size());
                    (void)conn.Send(resp.data(), resp.size());
                    continue;

                } else if (body->func_name == "vshard.router.map_callrw") {
                    const auto map_args = ParseMapCallRWArgs(
                        body->tuple_begin, body->tuple_len);
                    if (!map_args.opts_valid || map_args.func_name.empty() ||
                        !map_args.args_begin) {
                        const auto resp = tnt::BuildIprotoErrorFrame(
                            req.sync, kSchemaVersion,
                            "Usage: vshard.router.map_callrw(func, args[, opts])");
                        (void)conn.Send(resp.data(), resp.size());
                        continue;
                    }

                    storages::tarantool::OptionalCommandControl cc;
                    if (const auto timeout_ms =
                            MillisecondsFromSeconds(map_args.timeout)) {
                        cc = storages::tarantool::CommandControl{*timeout_ms};
                    }

                    try {
                        const auto results = proxy.MapCallRW(
                            map_args.func_name, map_args.args_begin,
                            map_args.args_len, cc, map_args.bucket_ids);

                        auto map = formats::msgpack::ValueBuilder::Object();
                        for (const auto& entry : results) {
                            auto values = formats::msgpack::ValueBuilder::Array();
                            if (!entry.value.IsNull()) {
                                values.PushBack(
                                    formats::msgpack::ValueBuilder{entry.value});
                            }
                            map[entry.replicaset_id] = std::move(values);
                        }
                        auto return_values = std::vector<uint8_t>{};
                        tnt::EncodeArray(return_values, 1);
                        const auto map_bytes = map.ToBytes();
                        return_values.insert(
                            return_values.end(), map_bytes.begin(), map_bytes.end());
                        const auto resp = BuildResultFrameRaw(
                            req.sync, return_values.data(), return_values.size());
                        (void)conn.Send(resp.data(), resp.size());
                    } catch (const VshardProxy::MapCallRWException& ex) {
                        const auto result_bytes = IsConnectivityErrorMessage(ex.what())
                            ? BuildMapCallRWClientErrorReturn(
                                  NormalizeNetboxClientErrorMessage(ex.what()),
                                  77, ex.GetReplicasetId())
                            : BuildMapCallRWClientErrorReturn(
                                  ex.what(),
                                  ex.GetErrorCode().value_or(32),
                                  ex.GetReplicasetId());
                        const auto resp = BuildResultFrameRaw(
                            req.sync, result_bytes.data(), result_bytes.size());
                        (void)conn.Send(resp.data(), resp.size());
                    }
                    continue;

                } else {
                    LOG_WARNING() << "iproto_server: unknown vshard function '"
                                  << body->func_name << "'";
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion, "unsupported vshard function");
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }

                // Parse vshard TUPLE.
                // Generic call: [bucket_id, mode_or_opts, func, args[, opts]]
                // Wrappers:     [bucket_id, func, args]
                std::optional<VshardRouterArgs> vargs;
                if (is_generic_call) {
                    vargs = ParseVshardRouterCallArgs(
                        body->tuple_begin, body->tuple_len);
                    if (vargs) {
                        mode = vargs->mode;
                    }
                } else {
                    vargs = ParseVshardRouterArgs(
                        body->tuple_begin, body->tuple_len);
                    if (vargs) {
                        vargs->mode = mode;
                    }
                }
                if (!vargs) {
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion, is_generic_call
                            ? "bad vshard.router.call args: expected [bucket_id, mode, func, args]"
                            : "bad vshard.router args: expected [bucket_id, func, args]");
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }
                if (!vargs->opts_valid) {
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion, is_generic_call
                            ? "Usage: call(bucket_id, mode, func, args, opts)"
                            : "Usage: call(bucket_id, func, args, opts)");
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }
                const auto total_timeout =
                    vargs->timeout > 0.0 ? vargs->timeout : 0.5;
                if (vargs->request_timeout > 0.0 &&
                    vargs->request_timeout > total_timeout) {
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion,
                        "request_timeout must be <= timeout");
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }

                // Fully zero-copy: raw args in, raw result bytes out —
                // no Value tree at any stage.
                storages::tarantool::OptionalCommandControl cc;
                if (const auto timeout_ms =
                        MillisecondsFromSeconds(vargs->timeout)) {
                    cc = storages::tarantool::CommandControl{*timeout_ms};
                }
                if (vargs->return_raw) {
                    const auto resp = tnt::BuildIprotoErrorFrame(
                        req.sync, kSchemaVersion,
                        BuildReturnRawUnsupportedError());
                    (void)conn.Send(resp.data(), resp.size());
                    continue;
                }

                // Execute concurrently with the reader loop so that pipelined
                // requests on this connection are in flight simultaneously.
                // Copy the request fields out of the reused parse buffer
                // before the reader overwrites it with the next frame.
                in_flight.lock_shared();
                engine::DetachUnscopedUnsafe(engine::AsyncNoSpan(
                    [&proxy, &conn, &in_flight, sync = req.sync, mode,
                     is_generic_call, is_async = vargs->is_async,
                     bucket_id = vargs->bucket_id,
                     func = std::string{vargs->func_name},
                     storage_mode = std::string{vargs->storage_mode},
                     args = std::vector<uint8_t>{
                         vargs->args_begin,
                         vargs->args_begin + vargs->args_len},
                     cc] {
                        std::vector<uint8_t> result_bytes;
                        try {
                            if (is_async) {
                                proxy.CallAsyncWithModeString(
                                    bucket_id, mode, storage_mode, func,
                                    args.data(), args.size(), cc);
                                result_bytes = BuildAsyncFuturePlaceholderReturn();
                            } else if (is_generic_call) {
                                result_bytes = proxy.CallForwardBytes(
                                    bucket_id, mode, storage_mode, func,
                                    args.data(), args.size(), cc);
                            } else {
                                result_bytes = proxy.CallForwardBytes(
                                    bucket_id, mode,
                                    mode == CallMode::kReadWrite ? "write"
                                                                 : "read",
                                    func, args.data(), args.size(), cc);
                            }
                        } catch (const storages::tarantool::vshard::
                                     UnreachableReplicasetError& ex) {
                            result_bytes = BuildRouterShardingErrorReturn(
                                8, "UNREACHABLE_REPLICASET", ex.what(),
                                ex.GetReplicasetId(), ex.GetBucketId());
                        } catch (const storages::tarantool::vshard::
                                     NoRouteToBucketError& ex) {
                            result_bytes = BuildRouterShardingErrorReturn(
                                9, "NO_ROUTE_TO_BUCKET", ex.what(), std::nullopt,
                                ex.GetBucketId());
                        } catch (const storages::tarantool::vshard::MovedError&
                                     ex) {
                            // Retries exhausted after MOVED redirect: return as
                            // vshard DATA so the caller sees
                            // [nil, {code=1, name='WRONG_BUCKET'}] just like
                            // the Lua router does, rather than an IPROTO error.
                            const auto& dest = ex.GetDestinationUuid();
                            result_bytes = BuildRouterShardingErrorReturn(
                                1, "WRONG_BUCKET", ex.what(),
                                dest.empty()
                                    ? std::optional<std::string_view>{}
                                    : std::optional<std::string_view>{dest},
                                ex.GetBucketId());
                        } catch (const storages::tarantool::vshard::
                                     TransferError& ex) {
                            // Bucket transfer still in progress after retries
                            // exhausted.
                            result_bytes = BuildRouterShardingErrorReturn(
                                32, "TRANSFER_IS_IN_PROGRESS", ex.what(),
                                std::nullopt, std::nullopt);
                        } catch (const std::exception& ex) {
                            if (IsConnectivityErrorMessage(ex.what())) {
                                result_bytes =
                                    BuildNetboxClientErrorReturn(ex.what());
                            } else {
                                // Match the old inline behaviour: unexpected
                                // errors become IPROTO error frames.
                                try {
                                    const auto err = tnt::BuildIprotoErrorFrame(
                                        sync, kSchemaVersion, ex.what());
                                    (void)conn.Send(err.data(), err.size());
                                } catch (...) {
                                    LOG_WARNING()
                                        << "iproto_server: request handler "
                                           "error: "
                                        << ex.what();
                                }
                                in_flight.unlock_shared();
                                return;
                            }
                        }

                        const auto resp = BuildResultFrameRaw(
                            sync, result_bytes.data(), result_bytes.size());
                        (void)conn.Send(resp.data(), resp.size());
                        in_flight.unlock_shared();
                    }));
                    // NB: the engine::Task destructor cancels and joins the
                    // task — a temporary TaskWithResult would kill the
                    // handler before its first step (observed: requests hung
                    // forever). DetachUnscopedUnsafe is safe here: handler
                    // references (proxy, conn) stay alive until the
                    // in_flight semaphore drain at connection close.

            } else {
                LOG_WARNING() << "iproto_server: unsupported request type 0x"
                              << static_cast<unsigned>(req.type);
                const auto resp = tnt::BuildIprotoErrorFrame(
                    req.sync, kSchemaVersion, "unsupported request type");
                (void)conn.Send(resp.data(), resp.size());
            }
        } catch (const std::exception& ex) {
            LOG_DEBUG() << "iproto_server: request error: " << ex.what();
            try {
                const auto resp = tnt::BuildIprotoErrorFrame(
                    req.sync, kSchemaVersion, ex.what());
                (void)conn.Send(resp.data(), resp.size());
            } catch (...) {
                break;  // can't write back, drop connection
            }
        }
    }

    // 4. Drain: wait until every in-flight handler task has completed so
    // nothing touches conn afterwards, then let the flush task push the
    // remaining staged replies and stop.
    for (std::size_t i = 0; i < kMaxRequestsInFlight; ++i) {
        in_flight.lock_shared();
    }
    conn.StartClosing();
    try {
        flush_task.Get();
    } catch (const std::exception& ex) {
        LOG_DEBUG() << "iproto_server: flush task ended: " << ex.what();
    }
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// IprotoServer component
// ---------------------------------------------------------------------------

IprotoServer::IprotoServer(const components::ComponentConfig& config,
                            const components::ComponentContext& context)
    : TcpAcceptorBase{config, context} {
    // The VshardProxyComponent is defined in vshard_proxy_service.cpp.
    // We look it up by its static kName.
    proxy_ = context
        .FindComponent<components::VshardProxyComponent>("tarantool-vshard")
        .GetProxy();
}

void IprotoServer::ProcessSocket(engine::io::Socket&& sock) {
    HandleConnection(std::move(sock), *proxy_);
}

yaml_config::Schema IprotoServer::GetStaticConfigSchema() {
    return yaml_config::MergeSchemas<TcpAcceptorBase>(R"(
type: object
description: IPROTO server that proxies vshard.router.callrw/callro to VshardProxy
additionalProperties: false
properties: {}
)");
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
