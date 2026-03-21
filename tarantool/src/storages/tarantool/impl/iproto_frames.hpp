#pragma once

/// @file storages/tarantool/impl/iproto_frames.hpp
/// @brief Compile-time static IPROTO frame builders and a zero-allocation
///        IPROTO response scanner.
///
/// ## PING frame (Rec5)
///
/// IPROTO PING (type 0x40) always has an empty body; only the sync_id field
/// varies.  Pre-computing the frame template at compile time and patching the
/// sync bytes at runtime avoids ~8 push_back calls and a vector allocation on
/// every PING.
///
/// Frame layout (18 bytes total):
/// @code
///   [0]      0xce              prehdr: uint32-length marker
///   [1..4]   0x00 0x00 0x00 0x0d  prehdr length = 13
///   [5]      0x82              fixmap(2) — IPROTO header map
///   [6]      0x00              key 0x00 (IPROTO_CODE)
///   [7]      0x40              val 64   (IPROTO_PING)
///   [8]      0x01              key 0x01 (IPROTO_SYNC)
///   [9]      0xcf              uint64 marker
///   [10..17] sync_id           big-endian uint64
/// @endcode
///
/// ## IPROTO response scanner (Rec6)
///
/// `ParseIprotoResponse(p, len)` scans the response payload in-place without
/// allocating any heap memory.  It replaces the `Value::FromBytes()` + tree
/// navigation path in the hot `ReaderLoop` with direct byte arithmetic,
/// eliminating per-response `Node` allocations, `Skip()`, and `BoundsCheck()`
/// calls.

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

#include <Client/IprotoConstants.hpp>

#include <storages/tarantool/impl/msgpack_constants.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

/// Number of bytes in a serialised IPROTO PING frame.
constexpr std::size_t kPingFrameSize = 18;

/// Build a complete, wire-ready IPROTO PING frame on the stack.
/// @param sync_id  The request sync counter for this frame.
/// @returns        A 18-byte array ready to be appended to staging_buf_.
[[nodiscard]] inline std::array<uint8_t, kPingFrameSize> BuildPingFrame(uint64_t sync_id) noexcept {
    return {{
        // ── preheader ───────────────────────────────────────────────────────
        mp::kUint32,  // uint32 length marker
        0x00,
        0x00,
        0x00,
        0x0d,  // length = 13 (header only, no body)
        // ── IPROTO header map ───────────────────────────────────────────────
        static_cast<uint8_t>(mp::kFixMapMin | 2),  // fixmap(2)
        static_cast<uint8_t>(Iproto::REQUEST_TYPE),
        static_cast<uint8_t>(Iproto::PING),  // IPROTO_PING = 64
        static_cast<uint8_t>(Iproto::SYNC),
        mp::kUint64,  // uint64 marker for sync_id
        // ── sync_id (big-endian uint64) ─────────────────────────────────────
        static_cast<uint8_t>(sync_id >> 56),
        static_cast<uint8_t>(sync_id >> 48),
        static_cast<uint8_t>(sync_id >> 40),
        static_cast<uint8_t>(sync_id >> 32),
        static_cast<uint8_t>(sync_id >> 24),
        static_cast<uint8_t>(sync_id >> 16),
        static_cast<uint8_t>(sync_id >> 8),
        static_cast<uint8_t>(sync_id),
    }};
}

// ── Zero-allocation IPROTO response scanner ──────────────────────────────────

/// Parsed IPROTO response.  All pointer fields point into the caller-owned
/// buffer passed to `ParseIprotoResponse`; no heap allocation is performed.
struct IprotoResponse {
    uint64_t sync{0};
    int64_t code{0};
    const uint8_t* data_begin{nullptr};  ///< raw bytes of key 0x30 value
    const uint8_t* data_end{nullptr};
    const uint8_t* error_begin{nullptr};  ///< raw bytes of key 0x31 value
    const uint8_t* error_end{nullptr};
    const uint8_t* ext_error_begin{nullptr};  ///< raw bytes of key 0x52 value
    const uint8_t* ext_error_end{nullptr};
};

namespace msgpack_scan {

/// Read a msgpack uint (fixuint/uint8/16/32/64) at p[pos].
/// Returns {value, pos_after}.  Returns {0, pos+1} for non-uint types.
inline std::pair<uint64_t, std::size_t> ReadUint(const uint8_t* p, std::size_t len, std::size_t pos) noexcept {
    if (pos >= len) {
        return {0, len};
    }
    const uint8_t b = p[pos];
    if (b <= mp::kFixIntMax) {
        return {b, pos + 1};
    }
    if (b == mp::kUint8 && pos + 1 < len) {
        return {p[pos + 1], pos + 2};
    }
    if (b == mp::kUint16 && pos + 2 < len) {
        return {(uint64_t)p[pos + 1] << 8 | p[pos + 2], pos + 3};
    }
    if (b == mp::kUint32 && pos + 4 < len) {
        return {
            (uint64_t)p[pos + 1] << 24 | (uint64_t)p[pos + 2] << 16 | (uint64_t)p[pos + 3] << 8 | p[pos + 4],
            pos + 5};
    }
    if (b == mp::kUint64 && pos + 8 < len) {
        uint64_t v = 0;
        for (int i = 1; i <= 8; ++i) {
            v = v << 8 | p[pos + i];
        }
        return {v, pos + 9};
    }
    return {0, pos + 1};
}

/// Skip one msgpack value at p[pos], returning the position after it.
/// Depth-limited (max 16) to guard against malformed/adversarial input.
inline std::size_t SkipValue(const uint8_t* p, std::size_t len, std::size_t pos, int depth = 0) noexcept {
    if (pos >= len) {
        return len;
    }
    if (depth > 16) {
        return pos < len ? pos + 1 : len;
    }
    const uint8_t b = p[pos++];

    if (b <= mp::kFixIntMax || b >= mp::kNegFixIntMin) {
        return pos;  // fixuint/fixnegint
    }
    if ((b & 0xf0u) == mp::kFixMapMin) {  // fixmap(n)
        std::size_t n = (b & 0x0fu) * 2;
        for (std::size_t i = 0; i < n && pos < len; ++i) {
            pos = SkipValue(p, len, pos, depth + 1);
        }
        return pos;
    }
    if ((b & 0xf0u) == mp::kFixArrayMin) {  // fixarray(n)
        std::size_t n = b & 0x0fu;
        for (std::size_t i = 0; i < n && pos < len; ++i) {
            pos = SkipValue(p, len, pos, depth + 1);
        }
        return pos;
    }
    if ((b & 0xe0u) == mp::kFixStrMin) {  // fixstr(n)
        std::size_t n = b & 0x1fu;
        return pos + n <= len ? pos + n : len;
    }
    switch (b) {
        case mp::kNil:
        case mp::kFalse:
        case mp::kTrue:
            return pos;
        case mp::kUint8:
        case mp::kInt8:
            return pos + 1 <= len ? pos + 1 : len;
        case mp::kUint16:
        case mp::kInt16:
            return pos + 2 <= len ? pos + 2 : len;
        case mp::kUint32:
        case mp::kInt32:
        case mp::kFloat32:
            return pos + 4 <= len ? pos + 4 : len;
        case mp::kUint64:
        case mp::kInt64:
        case mp::kFloat64:
            return pos + 8 <= len ? pos + 8 : len;
        case mp::kFixExt1:
            return pos + 2 <= len ? pos + 2 : len;  // type(1)+data(1)
        case mp::kFixExt2:
            return pos + 3 <= len ? pos + 3 : len;
        case mp::kFixExt4:
            return pos + 5 <= len ? pos + 5 : len;
        case mp::kFixExt8:
            return pos + 9 <= len ? pos + 9 : len;
        case mp::kFixExt16:
            return pos + 17 <= len ? pos + 17 : len;
        case mp::kBin8:
        case mp::kStr8: {
            if (pos >= len) {
                return len;
            }
            std::size_t n = p[pos];
            return pos + 1 + n <= len ? pos + 1 + n : len;
        }
        case mp::kExt8: {  // len(1)+type(1)+data
            if (pos >= len) {
                return len;
            }
            std::size_t n = p[pos];
            return pos + 2 + n <= len ? pos + 2 + n : len;
        }
        case mp::kBin16:
        case mp::kStr16: {
            if (pos + 2 > len) {
                return len;
            }
            std::size_t n = (std::size_t)p[pos] << 8 | p[pos + 1];
            return pos + 2 + n <= len ? pos + 2 + n : len;
        }
        case mp::kExt16: {  // len(2)+type(1)+data
            if (pos + 2 > len) {
                return len;
            }
            std::size_t n = (std::size_t)p[pos] << 8 | p[pos + 1];
            return pos + 3 + n <= len ? pos + 3 + n : len;
        }
        case mp::kBin32:
        case mp::kStr32: {
            if (pos + 4 > len) {
                return len;
            }
            std::size_t n =
                (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 | (std::size_t)p[pos + 2] << 8 | p[pos + 3];
            return pos + 4 + n <= len ? pos + 4 + n : len;
        }
        case mp::kExt32: {  // len(4)+type(1)+data
            if (pos + 4 > len) {
                return len;
            }
            std::size_t n =
                (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 | (std::size_t)p[pos + 2] << 8 | p[pos + 3];
            return pos + 5 + n <= len ? pos + 5 + n : len;
        }
        case mp::kArray16: {
            if (pos + 2 > len) {
                return len;
            }
            std::size_t n = (std::size_t)p[pos] << 8 | p[pos + 1];
            pos += 2;
            for (std::size_t i = 0; i < n && pos < len; ++i) {
                pos = SkipValue(p, len, pos, depth + 1);
            }
            return pos;
        }
        case mp::kArray32: {
            if (pos + 4 > len) {
                return len;
            }
            std::size_t n =
                (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 | (std::size_t)p[pos + 2] << 8 | p[pos + 3];
            pos += 4;
            for (std::size_t i = 0; i < n && pos < len; ++i) {
                pos = SkipValue(p, len, pos, depth + 1);
            }
            return pos;
        }
        case mp::kMap16: {
            if (pos + 2 > len) {
                return len;
            }
            std::size_t n = ((std::size_t)p[pos] << 8 | p[pos + 1]) * 2;
            pos += 2;
            for (std::size_t i = 0; i < n && pos < len; ++i) {
                pos = SkipValue(p, len, pos, depth + 1);
            }
            return pos;
        }
        case mp::kMap32: {
            if (pos + 4 > len) {
                return len;
            }
            std::size_t n =
                ((std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 | (std::size_t)p[pos + 2] << 8 | p[pos + 3]
                ) *
                2;
            pos += 4;
            for (std::size_t i = 0; i < n && pos < len; ++i) {
                pos = SkipValue(p, len, pos, depth + 1);
            }
            return pos;
        }
        default:
            return pos;
    }
}

/// Read a msgpack string (fixstr / str8 / str16 / str32) at p[pos].
/// Returns {string_view into original buffer, pos_after}.
/// On non-string type or out-of-bounds returns {"", pos+1} (does not abort).
inline std::pair<std::string_view, std::size_t>
ReadStr(const uint8_t* p, std::size_t len, std::size_t pos) noexcept {
    if (pos >= len) return {{}, len};
    const uint8_t b = p[pos++];
    std::size_t slen = 0;
    if ((b & 0xe0u) == mp::kFixStrMin) {
        slen = b & 0x1fu;
    } else if (b == mp::kStr8 && pos < len) {
        slen = p[pos++];
    } else if (b == mp::kStr16 && pos + 2 <= len) {
        slen = (std::size_t)p[pos] << 8 | p[pos + 1];
        pos += 2;
    } else if (b == mp::kStr32 && pos + 4 <= len) {
        slen = (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 |
               (std::size_t)p[pos + 2] << 8 | p[pos + 3];
        pos += 4;
    } else {
        return {{}, pos};  // not a string — skip the type byte already consumed
    }
    if (pos + slen > len) return {{}, len};
    return {std::string_view{reinterpret_cast<const char*>(p + pos), slen}, pos + slen};
}

}  // namespace msgpack_scan

// ── IPROTO request scanner ────────────────────────────────────────────────────

/// Parsed IPROTO request header.  All pointer fields are into the caller-owned
/// buffer passed to `ParseIprotoRequest`; no heap allocation is performed.
struct IprotoRequest {
    uint64_t sync{0};
    uint8_t  type{0};
    const uint8_t* body_begin{nullptr};  ///< first byte of the body map; null for bodyless requests (PING)
    std::size_t    body_len{0};
};

/// Scan an IPROTO request payload (after the 5-byte preheader) without
/// allocating.  Extracts sync and request type from the header map and
/// records a pointer to the body map for the caller to parse further.
///
/// Analogous to ParseIprotoResponse() but for the server-receive direction.
///
/// @param p    Pointer to the first byte of the frame (IPROTO header map).
/// @param len  Number of bytes available from @p p.
/// @returns    Populated IprotoRequest; on malformed input type == 0.
[[nodiscard]] inline IprotoRequest
ParseIprotoRequest(const uint8_t* p, std::size_t len) noexcept {
    IprotoRequest r{};
    if (!p || len == 0) return r;
    std::size_t pos = 0;

    // ── header map: {0x00: type, 0x01: sync, ...} ────────────────────────────
    if (pos >= len) return r;
    const uint8_t hb = p[pos++];
    if ((hb & 0xf0u) != mp::kFixMapMin) return r;  // must be fixmap
    const int hdr_n = hb & 0x0f;
    for (int i = 0; i < hdr_n && pos < len; ++i) {
        auto [k, kp] = msgpack_scan::ReadUint(p, len, pos);
        pos = kp;
        if (k == Iproto::REQUEST_TYPE) {
            auto [v, vp] = msgpack_scan::ReadUint(p, len, pos);
            pos = vp;
            r.type = static_cast<uint8_t>(v);
        } else if (k == Iproto::SYNC) {
            auto [v, vp] = msgpack_scan::ReadUint(p, len, pos);
            pos = vp;
            r.sync = v;
        } else {
            pos = msgpack_scan::SkipValue(p, len, pos);
        }
    }

    // ── body pointer (may be absent for PING etc.) ────────────────────────────
    if (pos < len) {
        r.body_begin = p + pos;
        r.body_len   = len - pos;
    }
    return r;
}

/// Parse an IPROTO response payload (header map + body map) without allocation.
///
/// The payload must cover exactly one IPROTO message: the header map
/// (`{0x00: code, 0x01: sync}`) immediately followed by the body map
/// (`{0x30: data}` for success, `{0x31: error, 0x52: ext_error}` for errors).
///
/// All pointer fields in the returned struct point into [p, p+len); the caller
/// must ensure the buffer outlives the returned struct.
[[nodiscard]] inline IprotoResponse ParseIprotoResponse(const uint8_t* p, std::size_t len) noexcept {
    IprotoResponse r{};
    if (!p || len == 0) {
        return r;
    }
    std::size_t pos = 0;

    // ── header map: {0x00: code, 0x01: sync} ────────────────────────────────
    if (pos >= len) {
        return r;
    }
    const uint8_t hb = p[pos++];
    if ((hb & 0xf0u) != mp::kFixMapMin) {
        return r;  // expected fixmap
    }
    const int hdr_n = hb & 0x0f;
    for (int i = 0; i < hdr_n && pos < len; ++i) {
        auto [k, kp] = msgpack_scan::ReadUint(p, len, pos);
        pos = kp;
        auto [v, vp] = msgpack_scan::ReadUint(p, len, pos);
        pos = vp;
        if (k == Iproto::REQUEST_TYPE) {
            r.code = static_cast<int64_t>(v);
        } else if (k == Iproto::SYNC) {
            r.sync = v;
        }
    }

    // ── body map ─────────────────────────────────────────────────────────────
    if (pos >= len) {
        return r;
    }
    const uint8_t bb = p[pos++];
    std::size_t body_n = 0;
    if ((bb & 0xf0u) == mp::kFixMapMin) {
        body_n = bb & 0x0fu;
    } else if (bb == mp::kMap16) {
        if (pos + 2 > len) {
            return r;
        }
        body_n = (std::size_t)p[pos] << 8 | p[pos + 1];
        pos += 2;
    } else if (bb == mp::kMap32) {
        if (pos + 4 > len) {
            return r;
        }
        body_n = (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 | (std::size_t)p[pos + 2] << 8 | p[pos + 3];
        pos += 4;
    } else {
        return r;
    }

    for (std::size_t i = 0; i < body_n && pos < len; ++i) {
        auto [k, kp] = msgpack_scan::ReadUint(p, len, pos);
        pos = kp;
        const std::size_t vs = pos;
        pos = msgpack_scan::SkipValue(p, len, pos);
        switch (k) {
            case Iproto::DATA:
                r.data_begin = p + vs;
                r.data_end = p + pos;
                break;
            case Iproto::ERROR_24:
                r.error_begin = p + vs;
                r.error_end = p + pos;
                break;
            case Iproto::ERROR:
                r.ext_error_begin = p + vs;
                r.ext_error_end = p + pos;
                break;
            default:
                break;
        }
    }
    return r;
}

// ── Zero-copy CALL routing ────────────────────────────────────────────────────

/// Result of scanning an IPROTO CALL body for zero-copy vshard routing.
/// All pointers are into the caller-owned receive buffer; the buffer must
/// outlive this struct.
struct CallRouteInfo {
    uint32_t bucket_id;          ///< vshard bucket id (1-based)
    const uint8_t* tuple_begin;  ///< first byte of the IPROTO_TUPLE value
    const uint8_t* tuple_end;    ///< one-past-last byte (== packet_end)
};

/// Scan an IPROTO CALL body (starting at the body fixmap byte) to extract
/// the vshard bucket_id and locate the raw IPROTO_TUPLE bytes for
/// scatter-gather forwarding.  Cost: ~23 bytes scanned regardless of payload.
///
/// @param p    Pointer to the first byte of the IPROTO body map.
/// @param len  Number of bytes available from @p p.
/// @returns    Populated CallRouteInfo on success, or std::nullopt on any
///             parse error (malformed packet, missing TUPLE key, bucket out
///             of valid vshard range).
[[nodiscard]] inline std::optional<CallRouteInfo>
ParseCallForRoute(const uint8_t* p, std::size_t len) noexcept {
    using namespace msgpack_scan;
    std::size_t pos = 0;

    // ── body map header ───────────────────────────────────────────────────────
    if (pos >= len) return std::nullopt;
    const uint8_t b = p[pos++];
    std::size_t body_n = 0;
    if ((b & 0xf0u) == mp::kFixMapMin) {
        body_n = b & 0x0fu;
    } else if (b == mp::kMap16 && pos + 2 <= len) {
        body_n = (std::size_t)p[pos] << 8 | p[pos + 1];
        pos += 2;
    } else if (b == mp::kMap32 && pos + 4 <= len) {
        body_n = (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 |
                 (std::size_t)p[pos + 2] << 8 | p[pos + 3];
        pos += 4;
    } else {
        return std::nullopt;
    }

    // ── scan for IPROTO_TUPLE key (0x21) ─────────────────────────────────────
    const uint8_t* tuple_begin = nullptr;
    for (std::size_t i = 0; i < body_n && pos < len; ++i) {
        auto [key, kpos] = ReadUint(p, len, pos);
        pos = kpos;
        if (key == Iproto::TUPLE) {
            tuple_begin = p + pos;
            break;
        }
        pos = SkipValue(p, len, pos);  // skip non-TUPLE values
    }
    if (!tuple_begin) return std::nullopt;

    // ── decode the TUPLE array header to reach bucket_id ─────────────────────
    std::size_t apos = static_cast<std::size_t>(tuple_begin - p);
    if (apos >= len) return std::nullopt;
    const uint8_t ah = p[apos++];
    std::size_t arr_n = 0;
    if ((ah & 0xf0u) == mp::kFixArrayMin) {
        arr_n = ah & 0x0fu;
    } else if (ah == mp::kArray16 && apos + 2 <= len) {
        arr_n = (std::size_t)p[apos] << 8 | p[apos + 1];
        apos += 2;
    } else if (ah == mp::kArray32 && apos + 4 <= len) {
        arr_n = (std::size_t)p[apos] << 24 | (std::size_t)p[apos + 1] << 16 |
                (std::size_t)p[apos + 2] << 8 | p[apos + 3];
        apos += 4;
    } else {
        return std::nullopt;
    }
    if (arr_n < 2) return std::nullopt;  // need at least [bucket_id, mode, ...]

    auto [bucket_id, after_bid] = ReadUint(p, len, apos);
    (void)after_bid;
    if (bucket_id == 0 || bucket_id > 65535u) return std::nullopt;

    return CallRouteInfo{static_cast<uint32_t>(bucket_id), tuple_begin, p + len};
}

// ── Scatter-gather forwarding ─────────────────────────────────────────────────

/// Pre-computed body prefix for vshard.storage.call:
///   fixmap(2) + IPROTO_FUNCTION_NAME + fixstr(19) + "vshard.storage.call"
///             + IPROTO_TUPLE key (value = raw TUPLE bytes that follow)
/// 23 bytes total.  The TUPLE value bytes are supplied separately as iov[3].
inline constexpr uint8_t kStorageCallBodyPrefix[] = {
    static_cast<uint8_t>(mp::kFixMapMin | 2),   // fixmap(2)
    static_cast<uint8_t>(Iproto::FUNCTION_NAME),
    static_cast<uint8_t>(mp::kFixStrMin | 19),  // fixstr(19)
    'v', 's', 'h', 'a', 'r', 'd', '.', 's', 't', 'o', 'r', 'a', 'g', 'e', '.', 'c', 'a', 'l', 'l',
    static_cast<uint8_t>(Iproto::TUPLE),        // key 0x21; value = TUPLE bytes follow
};
static_assert(sizeof(kStorageCallBodyPrefix) == 23);

/// IPROTO header map for a CALL request with a given sync_id (13 bytes).
///   fixmap(2) + REQUEST_TYPE + CALL + SYNC + uint64(sync_id)
struct IprotoCallHeader {
    uint8_t bytes[13];
};

[[nodiscard]] inline IprotoCallHeader
BuildIprotoCallHeader(uint64_t sync_id) noexcept {
    IprotoCallHeader h{};
    h.bytes[0]  = static_cast<uint8_t>(mp::kFixMapMin | 2);
    h.bytes[1]  = static_cast<uint8_t>(Iproto::REQUEST_TYPE);
    h.bytes[2]  = static_cast<uint8_t>(Iproto::CALL);
    h.bytes[3]  = static_cast<uint8_t>(Iproto::SYNC);
    h.bytes[4]  = mp::kUint64;
    h.bytes[5]  = static_cast<uint8_t>(sync_id >> 56);
    h.bytes[6]  = static_cast<uint8_t>(sync_id >> 48);
    h.bytes[7]  = static_cast<uint8_t>(sync_id >> 40);
    h.bytes[8]  = static_cast<uint8_t>(sync_id >> 32);
    h.bytes[9]  = static_cast<uint8_t>(sync_id >> 24);
    h.bytes[10] = static_cast<uint8_t>(sync_id >> 16);
    h.bytes[11] = static_cast<uint8_t>(sync_id >>  8);
    h.bytes[12] = static_cast<uint8_t>(sync_id);
    return h;
}

/// 5-byte IPROTO preheader: 0xce (uint32 marker) + big-endian packet length.
struct IprotoPreheader {
    uint8_t bytes[5];
};

[[nodiscard]] inline IprotoPreheader
BuildIprotoPreheader(uint32_t packet_len) noexcept {
    IprotoPreheader ph{};
    ph.bytes[0] = mp::kUint32;
    ph.bytes[1] = static_cast<uint8_t>(packet_len >> 24);
    ph.bytes[2] = static_cast<uint8_t>(packet_len >> 16);
    ph.bytes[3] = static_cast<uint8_t>(packet_len >>  8);
    ph.bytes[4] = static_cast<uint8_t>(packet_len);
    return ph;
}

/// Holds the 41 bytes of generated header data for a forwarded storage call.
/// The TUPLE bytes are referenced from the original receive buffer (zero-copy).
///
/// Usage with engine::io::Socket::SendAll(IoData*, size_t, Deadline):
/// @code
///   auto frame = BuildStorageCallFrame(info, new_sync);
///   const engine::io::IoData iovs[] = {
///       {frame.preheader.bytes, sizeof(frame.preheader.bytes)},
///       {frame.call_header.bytes, sizeof(frame.call_header.bytes)},
///       {kStorageCallBodyPrefix, sizeof(kStorageCallBodyPrefix)},
///       {info.tuple_begin, static_cast<std::size_t>(info.tuple_end - info.tuple_begin)},
///   };
///   socket.SendAll(iovs, 4, deadline);
/// @endcode
struct StorageCallFrame {
    IprotoPreheader  preheader;    ///< 5 bytes: length prefix
    IprotoCallHeader call_header;  ///< 13 bytes: IPROTO header map
    // kStorageCallBodyPrefix (23 bytes) is a compile-time constant
    // TUPLE bytes are referenced from the original receive buffer
};

/// Build the 18 generated bytes for forwarding a vshard.router.call to
/// vshard.storage.call.  Only 41 bytes total are written to storage per
/// request; the TUPLE payload is forwarded zero-copy from the receive buffer.
[[nodiscard]] inline StorageCallFrame
BuildStorageCallFrame(const CallRouteInfo& info, uint64_t new_sync) noexcept {
    const std::size_t tuple_size = static_cast<std::size_t>(info.tuple_end - info.tuple_begin);
    const std::size_t body_size  = sizeof(kStorageCallBodyPrefix) + tuple_size;
    constexpr std::size_t kHdrSize = sizeof(IprotoCallHeader::bytes);  // 13
    const uint32_t total = static_cast<uint32_t>(kHdrSize + body_size);

    StorageCallFrame f{};
    f.preheader   = BuildIprotoPreheader(total);
    f.call_header = BuildIprotoCallHeader(new_sync);
    return f;
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
