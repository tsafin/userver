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

}  // namespace msgpack_scan

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

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
