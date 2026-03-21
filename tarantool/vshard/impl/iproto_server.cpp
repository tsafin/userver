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
#include <cstring>
#include <optional>
#include <string_view>
#include <vector>

#include <Client/IprotoConstants.hpp>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/formats/msgpack/serialize.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/logging/log.hpp>
#include <userver/yaml_config/merge_schemas.hpp>
#include <userver/yaml_config/schema.hpp>

// Shared IPROTO frame utilities: ParseIprotoRequest, msgpack_scan::*, mp::k*
#include <storages/tarantool/impl/iproto_frames.hpp>

// For VshardProxyComponent lookup
#include <vshard/vshard_proxy_component.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {

// IPROTO request type codes — values not (yet) in tntcxx Iproto enum
constexpr uint8_t kTypeCall16 = 0x06;  // IPROTO_CALL_16 (legacy, pre-2.0)
constexpr uint8_t kTypeId     = 0x49;  // IPROTO_ID — feature negotiation (2.10+)
constexpr uint32_t kTypeError = 0x8000;  // OR'd with error code in response header

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

// ---------------------------------------------------------------------------
// msgpack helpers — minimal zero-dependency encoding using mp::k* constants
// ---------------------------------------------------------------------------

inline void PushU8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

inline void PushU32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(mp::kUint32);
    buf.push_back(static_cast<uint8_t>(v >> 24));
    buf.push_back(static_cast<uint8_t>(v >> 16));
    buf.push_back(static_cast<uint8_t>(v >> 8));
    buf.push_back(static_cast<uint8_t>(v));
}

inline void PushU64(std::vector<uint8_t>& buf, uint64_t v) {
    buf.push_back(mp::kUint64);
    for (int s = 56; s >= 0; s -= 8)
        buf.push_back(static_cast<uint8_t>(v >> s));
}

inline void PushFixMap(std::vector<uint8_t>& buf, uint8_t n) {
    buf.push_back(static_cast<uint8_t>(mp::kFixMapMin | (n & 0x0fu)));
}

inline void PushFixArray(std::vector<uint8_t>& buf, uint8_t n) {
    buf.push_back(static_cast<uint8_t>(mp::kFixArrayMin | (n & 0x0fu)));
}

inline void PushKV_u32(std::vector<uint8_t>& buf, uint8_t key, uint32_t val) {
    buf.push_back(key);
    PushU32(buf, val);
}

inline void PushKV_u64(std::vector<uint8_t>& buf, uint8_t key, uint64_t val) {
    buf.push_back(key);
    PushU64(buf, val);
}

inline void PushStr(std::vector<uint8_t>& buf, std::string_view s) {
    const auto len = s.size();
    if (len <= 31) {
        buf.push_back(static_cast<uint8_t>(mp::kFixStrMin | len));
    } else {
        buf.push_back(mp::kStr8);
        buf.push_back(static_cast<uint8_t>(len));
    }
    buf.insert(buf.end(),
               reinterpret_cast<const uint8_t*>(s.data()),
               reinterpret_cast<const uint8_t*>(s.data()) + len);
}

inline void PushNil(std::vector<uint8_t>& buf) {
    buf.push_back(mp::kNil);
}

inline void PushPreheader(std::vector<uint8_t>& buf, uint32_t body_len) {
    buf.push_back(mp::kUint32);
    buf.push_back(static_cast<uint8_t>(body_len >> 24));
    buf.push_back(static_cast<uint8_t>(body_len >> 16));
    buf.push_back(static_cast<uint8_t>(body_len >> 8));
    buf.push_back(static_cast<uint8_t>(body_len));
}

// ---------------------------------------------------------------------------
// Build IPROTO response frames
// ---------------------------------------------------------------------------

/// Build a wire-ready IPROTO OK response with the given body bytes
/// (already msgpack-encoded {0x30: data}).
std::vector<uint8_t> BuildOkFrame(uint64_t sync,
                                   const uint8_t* body_data,
                                   std::size_t body_len) {
    // Header: fixmap(3) + {REQUEST_TYPE: 0, SYNC: sync, SCHEMA_VERSION: ver}
    std::vector<uint8_t> hdr;
    hdr.reserve(20);
    PushFixMap(hdr, 3);
    hdr.push_back(static_cast<uint8_t>(Iproto::REQUEST_TYPE));
    hdr.push_back(0x00u);  // OK code = 0 (positive fixint)
    PushKV_u64(hdr, static_cast<uint8_t>(Iproto::SYNC), sync);
    PushKV_u32(hdr, static_cast<uint8_t>(Iproto::SCHEMA_VERSION), kSchemaVersion);

    const uint32_t total_body = static_cast<uint32_t>(hdr.size() + body_len);

    std::vector<uint8_t> frame;
    frame.reserve(5 + total_body);
    PushPreheader(frame, total_body);
    frame.insert(frame.end(), hdr.begin(), hdr.end());
    frame.insert(frame.end(), body_data, body_data + body_len);
    return frame;
}

/// Build a wire-ready IPROTO error response.
std::vector<uint8_t> BuildErrorFrame(uint64_t sync, std::string_view msg) {
    // Header: fixmap(3) + {REQUEST_TYPE: 0x8001, SYNC: sync, SCHEMA_VERSION: ver}
    std::vector<uint8_t> hdr;
    PushFixMap(hdr, 3);
    hdr.push_back(static_cast<uint8_t>(Iproto::REQUEST_TYPE));
    PushU32(hdr, kTypeError | 1u);
    PushKV_u64(hdr, static_cast<uint8_t>(Iproto::SYNC), sync);
    PushKV_u32(hdr, static_cast<uint8_t>(Iproto::SCHEMA_VERSION), kSchemaVersion);

    // Body: fixmap(1) + {ERROR_24: "error message"}
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::ERROR_24));
    PushStr(body, msg);

    const uint32_t total = static_cast<uint32_t>(hdr.size() + body.size());
    std::vector<uint8_t> frame;
    frame.reserve(5 + total);
    PushPreheader(frame, total);
    frame.insert(frame.end(), hdr.begin(), hdr.end());
    frame.insert(frame.end(), body.begin(), body.end());
    return frame;
}

/// Build an IPROTO OK response for AUTH (empty body).
std::vector<uint8_t> BuildAuthOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
}

/// Build an IPROTO OK response for PING (empty data).
std::vector<uint8_t> BuildPingOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
}

/// Build an IPROTO OK response for IPROTO_ID: {version: 0, features: []}.
/// Satisfies net.box feature negotiation without advertising any features.
std::vector<uint8_t> BuildIdOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    PushFixMap(body, 2);
    body.push_back(kKeyVersion);
    body.push_back(0x00);  // version 0 (positive fixint)
    body.push_back(kKeyFeatures);
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
}

/// Build an IPROTO OK response for IPROTO_SELECT: empty tuple set {DATA: []}.
/// net.box fetches schema via SELECT on system spaces during connect; returning
/// empty data lets it proceed without a real box schema.
std::vector<uint8_t> BuildSelectEmptyFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
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
    CallMode         mode{CallMode::kReadWrite};  ///< resolved call mode
    bool             prefer_replica{false};
    bool             balance{false};
};

/// Read the msgpack array header, return element count and advance pos.
/// Returns 0 on failure.
static std::size_t ReadArrayHeader(const uint8_t* p, std::size_t len,
                                    std::size_t& pos) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    if (pos >= len) return 0;
    const uint8_t ab = p[pos++];
    if ((ab & 0xf0u) == mp::kFixArrayMin) return ab & 0x0fu;
    if (ab == mp::kArray16 && pos + 2 <= len) {
        auto n = (std::size_t)p[pos] << 8 | p[pos + 1]; pos += 2; return n;
    }
    if (ab == mp::kArray32 && pos + 4 <= len) {
        auto n = (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 |
                 (std::size_t)p[pos + 2] << 8 | p[pos + 3]; pos += 4;
        return n;
    }
    return 0;
}

/// Read the msgpack map header, return element count and advance pos.
/// Returns 0 on failure (also valid for empty map, but that's fine).
static std::size_t ReadMapHeader(const uint8_t* p, std::size_t len,
                                  std::size_t& pos) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    if (pos >= len) return 0;
    const uint8_t mb = p[pos];
    if ((mb & 0xf0u) == mp::kFixMapMin) { ++pos; return mb & 0x0fu; }
    if (mb == mp::kMap16 && pos + 3 <= len) {
        ++pos;
        auto n = (std::size_t)p[pos] << 8 | p[pos + 1]; pos += 2; return n;
    }
    if (mb == mp::kMap32 && pos + 5 <= len) {
        ++pos;
        auto n = (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 |
                 (std::size_t)p[pos + 2] << 8 | p[pos + 3]; pos += 4;
        return n;
    }
    return 0;
}

/// Check if byte at pos is a msgpack string type.
static bool IsMsgpackStr(const uint8_t* p, std::size_t len,
                          std::size_t pos) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    if (pos >= len) return false;
    const uint8_t b = p[pos];
    return (b & 0xe0u) == mp::kFixStrMin || b == mp::kStr8 ||
           b == mp::kStr16 || b == mp::kStr32;
}

/// Check if byte at pos is a msgpack map type.
static bool IsMsgpackMap(const uint8_t* p, std::size_t len,
                          std::size_t pos) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    if (pos >= len) return false;
    const uint8_t b = p[pos];
    return (b & 0xf0u) == mp::kFixMapMin || b == mp::kMap16 || b == mp::kMap32;
}

/// Resolve CallMode from a mode string ("read" or "write").
static CallMode ModeFromString(std::string_view s) noexcept {
    if (s == "write") return CallMode::kReadWrite;
    return CallMode::kReadOnly;  // "read" or any other value
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
    if (IsMsgpackStr(p, len, pos)) {
        auto [mode_str, mp2] = ReadStr(p, len, pos); pos = mp2;
        out.mode = ModeFromString(mode_str);
    } else if (IsMsgpackMap(p, len, pos)) {
        // Parse opts table: {mode=str, prefer_replica=bool, balance=bool}
        const auto save_pos = pos;
        const auto map_len = ReadMapHeader(p, len, pos);
        for (std::size_t i = 0; i < map_len; ++i) {
            auto [key, kp] = ReadStr(p, len, pos); pos = kp;
            if (key == "mode") {
                auto [val, vp] = ReadStr(p, len, pos); pos = vp;
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

    // 5. opts (optional) — ignored for now, would carry timeout etc.

    // Resolve final CallMode from base mode + prefer_replica + balance flags
    out.mode = ResolveCallMode(out.mode, out.prefer_replica, out.balance);

    return out;
}

// ---------------------------------------------------------------------------
// Build the IPROTO OK response carrying the VshardProxy result value.
// ---------------------------------------------------------------------------

/// Encode {DATA: [result_value]} and build the full OK frame.
[[maybe_unused]] static std::vector<uint8_t> BuildResultFrame(
    uint64_t sync, const formats::msgpack::Value& result) {
    const auto result_bytes =
        formats::msgpack::ToBytes(formats::msgpack::ValueBuilder{result});

    // body = fixmap(1) + {DATA: [result_value]}
    std::vector<uint8_t> body;
    body.reserve(3 + result_bytes.size());
    PushFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    PushFixArray(body, 1);  // net.box unpacks the outer array as multi-return
    body.insert(body.end(), result_bytes.begin(), result_bytes.end());

    return BuildOkFrame(sync, body.data(), body.size());
}

/// Zero-copy variant: result_data is already a msgpack-encoded value.
static std::vector<uint8_t> BuildResultFrameRaw(
    uint64_t sync, const uint8_t* result_data, std::size_t result_len) {
    // body = fixmap(1) + {DATA: fixarray(1) + raw_result_bytes}
    std::vector<uint8_t> body;
    body.reserve(3 + result_len);
    PushFixMap(body, 1);
    body.push_back(static_cast<uint8_t>(Iproto::DATA));
    PushFixArray(body, 1);
    body.insert(body.end(), result_data, result_data + result_len);
    return BuildOkFrame(sync, body.data(), body.size());
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

static constexpr std::size_t kMaxFrameSize = 16 * 1024 * 1024;  // 16 MB

/// Read exactly `n` bytes from socket into `buf`, resizing as needed.
/// Returns false on EOF or error.
static bool RecvExact(engine::io::Socket& sock, std::vector<uint8_t>& buf,
                       std::size_t n) {
    buf.resize(n);
    const auto got = sock.RecvAll(buf.data(), n, engine::Deadline{});
    return got == n;
}

/// Append `n` bytes from socket to `buf`.
[[maybe_unused]] static bool RecvAppend(engine::io::Socket& sock,
                                         std::vector<uint8_t>& buf,
                                         std::size_t n) {
    const auto off = buf.size();
    buf.resize(off + n);
    const auto got = sock.RecvAll(buf.data() + off, n, engine::Deadline{});
    return got == n;
}

static void HandleConnection(engine::io::Socket sock,
                              VshardProxy& proxy) {
    // 1. Send greeting
    const auto greeting = MakeGreeting();
    try {
        (void)sock.SendAll(greeting.data(), greeting.size(), engine::Deadline{});
    } catch (const std::exception& ex) {
        LOG_DEBUG() << "iproto_server: greeting send failed: " << ex.what();
        return;
    }

    std::vector<uint8_t> frame_buf;
    frame_buf.reserve(4096);

    // 2. Request loop
    while (true) {
        // Read 5-byte preheader: mp::kUint32 marker + big-endian uint32 body_len
        try {
            if (!RecvExact(sock, frame_buf, 5)) break;
        } catch (...) {
            break;
        }
        if (frame_buf[0] != mp::kUint32) {
            LOG_WARNING() << "iproto_server: unexpected preheader byte "
                          << static_cast<int>(frame_buf[0]);
            break;
        }
        const uint32_t body_len =
            (uint32_t(frame_buf[1]) << 24) | (uint32_t(frame_buf[2]) << 16) |
            (uint32_t(frame_buf[3]) << 8)  |  uint32_t(frame_buf[4]);

        if (body_len > kMaxFrameSize) {
            LOG_WARNING() << "iproto_server: oversized frame " << body_len;
            break;
        }

        // Read body
        try {
            if (!RecvExact(sock, frame_buf, body_len)) break;
        } catch (...) {
            break;
        }

        // Parse header using shared ParseIprotoRequest from iproto_frames.hpp
        const auto req = storages::tarantool::impl::ParseIprotoRequest(
            frame_buf.data(), body_len);
        if (req.type == 0 && req.sync == 0) {
            LOG_WARNING() << "iproto_server: malformed frame, closing";
            break;
        }

        // Dispatch
        try {
            if (req.type == kTypeId) {
                const auto resp = BuildIdOkFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == Iproto::AUTH) {
                // Accept any credentials without verification.
                const auto resp = BuildAuthOkFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == Iproto::PING) {
                const auto resp = BuildPingOkFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == Iproto::SELECT) {
                // net.box fetches schema on connect; return empty data.
                const auto resp = BuildSelectEmptyFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == Iproto::CALL || req.type == kTypeCall16) {
                // Both CALL (0x0A) and legacy CALL_16 (0x06) carry
                // FUNCTION_NAME (0x22) + TUPLE (0x21) in the body.
                if (!req.body_begin) {
                    const auto resp = BuildErrorFrame(req.sync, "missing body");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
                    continue;
                }

                const auto body = ParseCallBody(req.body_begin, req.body_len);
                if (!body || !body->tuple_begin) {
                    const auto resp = BuildErrorFrame(req.sync, "missing args");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
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
                } else {
                    LOG_WARNING() << "iproto_server: unknown vshard function '"
                                  << body->func_name << "'";
                    const auto resp = BuildErrorFrame(
                        req.sync, "unsupported vshard function");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
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
                    const auto resp = BuildErrorFrame(
                        req.sync, is_generic_call
                            ? "bad vshard.router.call args: expected [bucket_id, mode, func, args]"
                            : "bad vshard.router args: expected [bucket_id, func, args]");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
                    continue;
                }

                // Fully zero-copy: raw args in, raw result bytes out —
                // no Value tree at any stage.
                const auto result_bytes = proxy.CallRawBytes(
                    vargs->bucket_id, mode,
                    vargs->func_name,
                    vargs->args_begin, vargs->args_len);

                const auto resp = BuildResultFrameRaw(
                    req.sync, result_bytes.data(), result_bytes.size());
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else {
                LOG_WARNING() << "iproto_server: unsupported request type 0x"
                              << static_cast<unsigned>(req.type);
                const auto resp = BuildErrorFrame(
                    req.sync, "unsupported request type");
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
            }
        } catch (const std::exception& ex) {
            LOG_DEBUG() << "iproto_server: request error: " << ex.what();
            try {
                const auto resp = BuildErrorFrame(req.sync, ex.what());
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
            } catch (...) {
                break;  // can't write back, drop connection
            }
        }
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
