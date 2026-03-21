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
#include <string_view>
#include <vector>

#include <Client/IprotoConstants.hpp>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/formats/msgpack/serialize.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/logging/log.hpp>
#include <userver/yaml_config/merge_schemas.hpp>
#include <userver/yaml_config/schema.hpp>

// For VshardProxyComponent lookup
#include <vshard/vshard_proxy_component.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {

// ---------------------------------------------------------------------------
// IPROTO header/body map keys (from tntcxx IprotoConstants.hpp Iproto enum)
// ---------------------------------------------------------------------------
constexpr uint8_t kKeyRequestType   = Iproto::REQUEST_TYPE;   // 0x00
constexpr uint8_t kKeySync          = Iproto::SYNC;           // 0x01
constexpr uint8_t kKeySchemaVersion = Iproto::SCHEMA_VERSION; // 0x05
constexpr uint8_t kKeyFunctionName  = Iproto::FUNCTION_NAME;  // 0x22
constexpr uint8_t kKeyTuple         = Iproto::TUPLE;          // 0x21
constexpr uint8_t kKeyData          = Iproto::DATA;           // 0x30
constexpr uint8_t kKeyError         = Iproto::ERROR_24;       // 0x31

// IPROTO request type codes
constexpr uint8_t kTypeSelect = Iproto::SELECT;   // 0x01 — schema fetch by net.box
constexpr uint8_t kTypeCall16 = 0x06;             // IPROTO_CALL_16 (legacy, pre-2.0)
constexpr uint8_t kTypeAuth   = Iproto::AUTH;     // 0x07
constexpr uint8_t kTypeCall   = Iproto::CALL;     // 0x0A — primary vshard call type
constexpr uint8_t kTypePing   = Iproto::PING;     // 0x40
constexpr uint8_t kTypeId     = 0x49;             // IPROTO_ID — feature negotiation (2.10+)
constexpr uint32_t kTypeError = 0x8000;           // OR'd with error code in response

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
// msgpack helpers — minimal zero-dependency encoding
// ---------------------------------------------------------------------------

// Append a single byte.
inline void PushU8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

// Append a msgpack uint32 (4-byte format 0xce + big-endian uint32).
inline void PushU32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(0xce);
    buf.push_back(static_cast<uint8_t>(v >> 24));
    buf.push_back(static_cast<uint8_t>(v >> 16));
    buf.push_back(static_cast<uint8_t>(v >> 8));
    buf.push_back(static_cast<uint8_t>(v));
}

// Append a msgpack uint64 (8-byte format 0xcf + big-endian uint64).
inline void PushU64(std::vector<uint8_t>& buf, uint64_t v) {
    buf.push_back(0xcf);
    for (int s = 56; s >= 0; s -= 8)
        buf.push_back(static_cast<uint8_t>(v >> s));
}

// Append a msgpack fixmap header of n entries (n <= 15).
inline void PushFixMap(std::vector<uint8_t>& buf, uint8_t n) {
    buf.push_back(0x80u | (n & 0x0fu));
}

// Append a msgpack fixarray header of n entries (n <= 15).
inline void PushFixArray(std::vector<uint8_t>& buf, uint8_t n) {
    buf.push_back(0x90u | (n & 0x0fu));
}

// Append a msgpack key (fixint < 128) + uint32 value.
inline void PushKV_u32(std::vector<uint8_t>& buf, uint8_t key, uint32_t val) {
    buf.push_back(key);
    PushU32(buf, val);
}

// Append a msgpack key + uint64 value.
inline void PushKV_u64(std::vector<uint8_t>& buf, uint8_t key, uint64_t val) {
    buf.push_back(key);
    PushU64(buf, val);
}

// Append a msgpack str (str8 or fixstr).
inline void PushStr(std::vector<uint8_t>& buf, std::string_view s) {
    const auto len = s.size();
    if (len <= 31) {
        buf.push_back(0xa0u | static_cast<uint8_t>(len));
    } else {
        buf.push_back(0xd9u);
        buf.push_back(static_cast<uint8_t>(len));
    }
    buf.insert(buf.end(),
               reinterpret_cast<const uint8_t*>(s.data()),
               reinterpret_cast<const uint8_t*>(s.data()) + len);
}

// Append a msgpack nil.
inline void PushNil(std::vector<uint8_t>& buf) {
    buf.push_back(0xc0u);
}

// Serialize the 5-byte IPROTO length header (0xCE + uint32 body_len).
inline void PushPreheader(std::vector<uint8_t>& buf, uint32_t body_len) {
    buf.push_back(0xce);
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
    // Header: fixmap(3) + {0x00: 0, 0x01: sync, 0x05: schema_version}
    std::vector<uint8_t> hdr;
    hdr.reserve(20);
    PushFixMap(hdr, 3);
    hdr.push_back(kKeyRequestType); hdr.push_back(0x00u);  // OK type = 0
    PushKV_u64(hdr, kKeySync, sync);
    PushKV_u32(hdr, kKeySchemaVersion, kSchemaVersion);

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
    // Header: fixmap(3) + {0x00: 0x8001, 0x01: sync, 0x05: schema_ver}
    std::vector<uint8_t> hdr;
    PushFixMap(hdr, 3);
    hdr.push_back(kKeyRequestType);
    PushU32(hdr, kTypeError | 1u);
    PushKV_u64(hdr, kKeySync, sync);
    PushKV_u32(hdr, kKeySchemaVersion, kSchemaVersion);

    // Body: fixmap(1) + {0x31: "error message"}
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(kKeyError);
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
    // Body: fixmap(1) + {0x30: []}
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(kKeyData);
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
}

/// Build an IPROTO OK response for PING (empty data).
std::vector<uint8_t> BuildPingOkFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(kKeyData);
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
}

/// Build an IPROTO OK response for IPROTO_ID: {version: 0, features: []}.
/// This satisfies net.box feature negotiation without advertising any features.
std::vector<uint8_t> BuildIdOkFrame(uint64_t sync) {
    // Body: fixmap(2) + {0x54: 0, 0x55: []}
    std::vector<uint8_t> body;
    PushFixMap(body, 2);
    body.push_back(kKeyVersion);
    body.push_back(0x00);  // version 0 (positive fixint)
    body.push_back(kKeyFeatures);
    PushFixArray(body, 0);  // empty features array
    return BuildOkFrame(sync, body.data(), body.size());
}

/// Build an IPROTO OK response for IPROTO_SELECT: empty tuple set {0x30: []}.
/// net.box sends SELECT to system spaces (_vspace=281, _vindex=289, _func=287)
/// during schema fetch.  Returning empty data tells it there are no spaces
/// defined, which is correct — our proxy has no box schema.
std::vector<uint8_t> BuildSelectEmptyFrame(uint64_t sync) {
    std::vector<uint8_t> body;
    PushFixMap(body, 1);
    body.push_back(kKeyData);
    PushFixArray(body, 0);
    return BuildOkFrame(sync, body.data(), body.size());
}

// ---------------------------------------------------------------------------

struct Span {
    const uint8_t* p;
    const uint8_t* end;
    bool ok() const { return p < end; }
    uint8_t read1() { return *p++; }
    uint8_t peek() const { return *p; }
};

// Read a uint from msgpack (positive fixint, uint8/16/32/64).
// Returns false on failure.
static bool ReadUint(Span& s, uint64_t& out) {
    if (!s.ok()) return false;
    const uint8_t b = s.read1();
    if (b < 0x80) { out = b; return true; }
    if (b == 0xcc) { if (s.end - s.p < 1) return false; out = *s.p++; return true; }
    if (b == 0xcd) { if (s.end - s.p < 2) return false;
        out = (uint64_t(s.p[0]) << 8) | s.p[1]; s.p += 2; return true; }
    if (b == 0xce) { if (s.end - s.p < 4) return false;
        out = (uint64_t(s.p[0])<<24)|(uint64_t(s.p[1])<<16)|(uint64_t(s.p[2])<<8)|s.p[3];
        s.p += 4; return true; }
    if (b == 0xcf) { if (s.end - s.p < 8) return false;
        out = 0;
        for (int i = 0; i < 8; ++i) out = (out << 8) | *s.p++;
        return true; }
    return false;
}

// Read a string from msgpack (fixstr, str8, str16, str32).
static bool ReadStr(Span& s, std::string_view& out) {
    if (!s.ok()) return false;
    const uint8_t b = s.read1();
    uint32_t len = 0;
    if ((b & 0xe0) == 0xa0) { len = b & 0x1fu; }
    else if (b == 0xd9) { if (!s.ok()) return false; len = *s.p++; }
    else if (b == 0xda) { if (s.end-s.p<2) return false;
        len = (uint32_t(*s.p)<<8)|s.p[1]; s.p+=2; }
    else if (b == 0xdb) { if (s.end-s.p<4) return false;
        len = (uint32_t(s.p[0])<<24)|(uint32_t(s.p[1])<<16)|(uint32_t(s.p[2])<<8)|s.p[3];
        s.p+=4; }
    else return false;
    if (static_cast<ptrdiff_t>(len) > s.end - s.p) return false;
    out = {reinterpret_cast<const char*>(s.p), len};
    s.p += len;
    return true;
}

// Skip one msgpack value (including nested containers).
static bool SkipValue(Span& s);

static bool SkipN(Span& s, uint32_t n) {
    for (uint32_t i = 0; i < n; ++i)
        if (!SkipValue(s)) return false;
    return true;
}

static bool SkipValue(Span& s) {
    if (!s.ok()) return false;
    const uint8_t b = s.read1();
    // positive fixint / negative fixint
    if (b < 0x80 || b >= 0xe0) return true;
    // fixstr
    if ((b & 0xe0) == 0xa0) { uint32_t n = b & 0x1f; s.p += n; return s.p <= s.end; }
    // fixarray
    if ((b & 0xf0) == 0x90) return SkipN(s, b & 0x0f);
    // fixmap
    if ((b & 0xf0) == 0x80) return SkipN(s, 2 * (b & 0x0f));
    switch (b) {
        case 0xc0: case 0xc2: case 0xc3: return true;  // nil, false, true
        case 0xcc: case 0xd0: s.p += 1; return s.p <= s.end;
        case 0xcd: case 0xd1: s.p += 2; return s.p <= s.end;
        case 0xce: case 0xd2: case 0xca: s.p += 4; return s.p <= s.end;
        case 0xcf: case 0xd3: case 0xcb: s.p += 8; return s.p <= s.end;
        case 0xd4: s.p += 2; return s.p <= s.end;
        case 0xd5: s.p += 3; return s.p <= s.end;
        case 0xd6: s.p += 5; return s.p <= s.end;
        case 0xd7: s.p += 9; return s.p <= s.end;
        case 0xd8: s.p += 17; return s.p <= s.end;
        case 0xc7: { if (!s.ok()) return false; uint32_t n = *s.p++; s.p += 1 + n; return s.p <= s.end; }
        case 0xc8: { if (s.end-s.p<2) return false; uint32_t n = (uint32_t(s.p[0])<<8)|s.p[1]; s.p += 2+1+n; return s.p <= s.end; }
        case 0xc9: { if (s.end-s.p<4) return false; uint32_t n = (uint32_t(s.p[0])<<24)|(uint32_t(s.p[1])<<16)|(uint32_t(s.p[2])<<8)|s.p[3]; s.p+=4+1+n; return s.p<=s.end; }
        case 0xd9: { if (!s.ok()) return false; uint32_t n = *s.p++; s.p += n; return s.p <= s.end; }
        case 0xda: { if (s.end-s.p<2) return false; uint32_t n=(uint32_t(s.p[0])<<8)|s.p[1]; s.p+=2+n; return s.p<=s.end; }
        case 0xdb: { if (s.end-s.p<4) return false; uint32_t n=(uint32_t(s.p[0])<<24)|(uint32_t(s.p[1])<<16)|(uint32_t(s.p[2])<<8)|s.p[3]; s.p+=4+n; return s.p<=s.end; }
        case 0xdc: { if (s.end-s.p<2) return false; uint32_t n=(uint32_t(s.p[0])<<8)|s.p[1]; s.p+=2; return SkipN(s,n); }
        case 0xdd: { if (s.end-s.p<4) return false; uint32_t n=(uint32_t(s.p[0])<<24)|(uint32_t(s.p[1])<<16)|(uint32_t(s.p[2])<<8)|s.p[3]; s.p+=4; return SkipN(s,n); }
        case 0xde: { if (s.end-s.p<2) return false; uint32_t n=(uint32_t(s.p[0])<<8)|s.p[1]; s.p+=2; return SkipN(s,2*n); }
        case 0xdf: { if (s.end-s.p<4) return false; uint32_t n=(uint32_t(s.p[0])<<24)|(uint32_t(s.p[1])<<16)|(uint32_t(s.p[2])<<8)|s.p[3]; s.p+=4; return SkipN(s,2*n); }
        default: return false;
    }
}

// ---------------------------------------------------------------------------
// Parse a CALL request body: extract sync, func_name, and args span.
// ---------------------------------------------------------------------------

struct ParsedRequest {
    uint64_t sync{0};
    uint8_t  type{0};
    std::string_view func_name;   // only set for CALL
    const uint8_t* args_begin{nullptr};  // start of args array value
    std::size_t args_len{0};
};

/// Parse the header+body of an IPROTO request (after the 5-byte preheader).
/// Returns false if the frame is malformed.
static bool ParseRequest(const uint8_t* data, std::size_t len,
                          ParsedRequest& out) {
    Span s{data, data + len};

    // --- header map ---
    if (!s.ok()) return false;
    const uint8_t hdr_byte = s.read1();
    if ((hdr_byte & 0xf0) != 0x80) return false;  // must be fixmap
    const uint32_t hdr_n = hdr_byte & 0x0f;

    for (uint32_t i = 0; i < hdr_n; ++i) {
        uint64_t key;
        if (!ReadUint(s, key)) return false;
        if (key == kKeyRequestType) {
            uint64_t t;
            if (!ReadUint(s, t)) return false;
            out.type = static_cast<uint8_t>(t);
        } else if (key == kKeySync) {
            if (!ReadUint(s, out.sync)) return false;
        } else {
            if (!SkipValue(s)) return false;
        }
    }

    // --- body map ---
    if (!s.ok()) return true;  // no body is fine (PING)
    const uint8_t body_byte = s.read1();
    if ((body_byte & 0xf0) != 0x80) return false;  // must be fixmap
    const uint32_t body_n = body_byte & 0x0f;

    for (uint32_t i = 0; i < body_n; ++i) {
        uint64_t key;
        if (!ReadUint(s, key)) return false;
        if (key == kKeyFunctionName) {
            if (!ReadStr(s, out.func_name)) return false;
        } else if (key == kKeyTuple) {
            // Record the start/length of the args array msgpack value
            out.args_begin = s.p;
            if (!SkipValue(s)) return false;
            out.args_len = static_cast<std::size_t>(s.p - out.args_begin);
        } else {
            if (!SkipValue(s)) return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Parse vshard.router.callrw/callro args: (bucket_id, func_name, args)
// The IPROTO TUPLE value is an array [ bucket_id, func_name, args ].
// ---------------------------------------------------------------------------

struct VshardRouterArgs {
    uint32_t bucket_id{0};
    std::string_view func_name;
    const uint8_t* args_begin{nullptr};
    std::size_t    args_len{0};
};

static bool ParseVshardRouterArgs(const uint8_t* data, std::size_t len,
                                   VshardRouterArgs& out) {
    Span s{data, data + len};
    // outer array
    if (!s.ok()) return false;
    const uint8_t ab = s.read1();
    uint32_t alen = 0;
    if ((ab & 0xf0) == 0x90) alen = ab & 0x0f;
    else if (ab == 0xdc) { if (s.end-s.p<2) return false; alen=(uint32_t(s.p[0])<<8)|s.p[1]; s.p+=2; }
    else if (ab == 0xdd) { if (s.end-s.p<4) return false; alen=(uint32_t(s.p[0])<<24)|(uint32_t(s.p[1])<<16)|(uint32_t(s.p[2])<<8)|s.p[3]; s.p+=4; }
    else return false;
    if (alen < 3) return false;

    // bucket_id
    uint64_t bid;
    if (!ReadUint(s, bid)) return false;
    out.bucket_id = static_cast<uint32_t>(bid);

    // func_name
    if (!ReadStr(s, out.func_name)) return false;

    // args (the rest, passed verbatim to vshard.storage.call)
    out.args_begin = s.p;
    if (!SkipValue(s)) return false;
    out.args_len = static_cast<std::size_t>(s.p - out.args_begin);
    return true;
}

// ---------------------------------------------------------------------------
// Build the IPROTO OK response carrying the VshardProxy result value.
// ---------------------------------------------------------------------------

/// Encode {0x30: [result_value]} and build the full OK frame.
static std::vector<uint8_t> BuildResultFrame(
    uint64_t sync, const formats::msgpack::Value& result) {
    const auto result_bytes = formats::msgpack::ToBytes(
        formats::msgpack::ValueBuilder{result});

    // body = fixmap(1) + {0x30: [result_value]}
    // The result is wrapped in a 1-element array as net.box unpacks it
    std::vector<uint8_t> body;
    body.reserve(3 + result_bytes.size());
    PushFixMap(body, 1);
    body.push_back(kKeyData);
    // Wrap result in a 1-element array (net.box unpacks multi-return)
    PushFixArray(body, 1);
    body.insert(body.end(), result_bytes.begin(), result_bytes.end());

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
static bool RecvAppend(engine::io::Socket& sock, std::vector<uint8_t>& buf,
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
        // Read 5-byte preheader: 0xCE + uint32 body_len
        try {
            if (!RecvExact(sock, frame_buf, 5)) break;
        } catch (...) {
            break;
        }
        if (frame_buf[0] != 0xce) {
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

        // Parse
        ParsedRequest req;
        if (!ParseRequest(frame_buf.data(), body_len, req)) {
            LOG_WARNING() << "iproto_server: malformed frame, closing";
            break;
        }

        // Dispatch
        try {
            if (req.type == kTypeId) {
                // Feature negotiation (Tarantool 2.10+) — respond with
                // version=0, empty features.  This unblocks net.box 2.6+.
                const auto resp = BuildIdOkFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == kTypeAuth) {
                // Accept any credentials without verification.
                // vshard connects anonymously by default; clients may send
                // AUTH regardless.
                const auto resp = BuildAuthOkFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == kTypePing) {
                const auto resp = BuildPingOkFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == kTypeSelect) {
                // net.box fetches schema via SELECT on system spaces
                // (_vspace=281, _vindex=289, _func=287, _vcollation=316).
                // We have no schema; returning empty data satisfies net.box
                // and lets it proceed to AUTH and CALL.
                const auto resp = BuildSelectEmptyFrame(req.sync);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else if (req.type == kTypeCall || req.type == kTypeCall16) {
                // IPROTO_CALL (0x0A) — new-style, used by Tarantool 1.7+
                // IPROTO_CALL_16 (0x06) — legacy, same wire format as CALL
                // Both use key 0x22 (FUNCTION_NAME) and 0x21 (TUPLE/args).
                if (req.args_begin == nullptr) {
                    const auto resp = BuildErrorFrame(req.sync, "missing args");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
                    continue;
                }

                // Map vshard router function name to read/write mode.
                // Per vshard analysis: callrw and call (default mode=write)
                // go to master; all callro/callbro/callre/callbre go to replica.
                CallMode mode = CallMode::kReadOnly;
                if (req.func_name == "vshard.router.callrw" ||
                    req.func_name == "vshard.router.call") {
                    mode = CallMode::kReadWrite;
                } else if (req.func_name != "vshard.router.callro" &&
                           req.func_name != "vshard.router.callbro" &&
                           req.func_name != "vshard.router.callre" &&
                           req.func_name != "vshard.router.callbre") {
                    LOG_WARNING() << "iproto_server: unknown vshard function '"
                                  << req.func_name << "'";
                    const auto resp = BuildErrorFrame(
                        req.sync, "unsupported vshard function");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
                    continue;
                }

                VshardRouterArgs vargs;
                if (!ParseVshardRouterArgs(req.args_begin, req.args_len, vargs)) {
                    const auto resp = BuildErrorFrame(
                        req.sync, "bad vshard.router args: expected [bucket_id, func, args]");
                    (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});
                    continue;
                }

                // Decode the args array into a ValueBuilder for DoCall
                auto args_value = formats::msgpack::FromBytes(
                    vargs.args_begin, vargs.args_len);
                auto args_builder = formats::msgpack::ValueBuilder{args_value};

                const auto result = proxy.Call(
                    vargs.bucket_id, mode,
                    std::string{vargs.func_name},
                    std::move(args_builder));

                const auto resp = BuildResultFrame(req.sync, result);
                (void)sock.SendAll(resp.data(), resp.size(), engine::Deadline{});

            } else {
                // Unknown request type — log and return error.
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
