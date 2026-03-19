#pragma once

/// @file vshard/impl/iproto_vshard_frames.hpp
/// @brief IPROTO_VSHARD_CALL protocol extension for userver vshard.
///
/// Defines a new IPROTO command code and header keys that carry bucket_id and
/// mode directly in the binary envelope, enabling the router to route by
/// reading only the IPROTO header (~26 bytes) without scanning the body at all.
///
/// Wire format (IPROTO_VSHARD_CALL):
/// @code
///   Preheader (5 bytes)   : 0xce + uint32(total_len)
///   Header  (fixmap(4))   : REQUEST_TYPE=0x50  SYNC=<uint64>
///                           VSHARD_BUCKET_ID=<uint32>  VSHARD_MODE=<uint8>
///   Body    (fixmap(2))   : FUNCTION_NAME="<func>"  TUPLE=[<args...>]
/// @endcode
///
/// Router behaviour:
///  1. Read header only → extract bucket_id + mode.
///  2. Look up replicaset (O(1) array lookup).
///  3. Forward: new preheader + new header (new SYNC) + original body zero-copy.
///
/// Storage behaviour (requires storage-side support):
///  - Handle IPROTO_VSHARD_CALL request type.
///  - Verify bucket ownership using bucket_id from header.
///  - Execute FUNCTION_NAME(TUPLE) and return result.

#include <cstdint>
#include <optional>

#include <Client/IprotoConstants.hpp>

#include <storages/tarantool/impl/iproto_frames.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

// ── IPROTO_VSHARD_CALL extension constants ────────────────────────────────────

/// New IPROTO request type: IPROTO_VSHARD_CALL (userver/vshard extension).
/// Chosen in the range 0x50–0x7f not assigned by Tarantool as of 3.x.
inline constexpr uint32_t kIprotoVshardCallType = 0x50;

/// IPROTO header key carrying the vshard bucket_id (uint32, 1-based).
inline constexpr uint32_t kIprotoVshardBucketIdKey = 0x5e;

/// IPROTO header key carrying the routing mode (uint8: 0=ro, 1=rw).
inline constexpr uint32_t kIprotoVshardModeKey = 0x5f;

// ── Parsed header ─────────────────────────────────────────────────────────────

/// Fields extracted from an IPROTO_VSHARD_CALL header map.
/// All pointers into the original buffer; buffer must outlive this struct.
struct VshardCallInfo {
    uint64_t sync;       ///< original SYNC from the incoming request
    uint32_t bucket_id;  ///< vshard bucket id (1-based, 1..65535)
    uint8_t  mode;       ///< 0 = read-only, 1 = read-write
};

/// Parse an IPROTO_VSHARD_CALL header map (starting at the fixmap byte,
/// i.e. after the preheader has been consumed).
///
/// @param p    Pointer to the first byte of the IPROTO header map.
/// @param len  Bytes available from @p p.
/// @returns    Populated VshardCallInfo on success, std::nullopt on any
///             parse error (wrong type, missing keys, out-of-range bucket_id).
[[nodiscard]] inline std::optional<VshardCallInfo>
ParseVshardCallHeader(const uint8_t* p, std::size_t len) noexcept {
    using namespace storages::tarantool::impl::msgpack_scan;
    std::size_t pos = 0;

    // ── header fixmap ─────────────────────────────────────────────────────────
    if (pos >= len) return std::nullopt;
    const uint8_t b = p[pos++];
    std::size_t n = 0;
    if ((b & 0xf0u) == 0x80u) {  // fixmap: high nibble = 0x8
        n = b & 0x0fu;
    } else {
        return std::nullopt;  // IPROTO headers are always small fixmaps
    }

    std::optional<uint64_t> sync;
    std::optional<uint32_t> bucket_id;
    std::optional<uint8_t>  mode;
    bool is_vshard_call = false;

    for (std::size_t i = 0; i < n && pos < len; ++i) {
        auto [key, kpos] = ReadUint(p, len, pos);
        pos = kpos;

        if (key == static_cast<uint64_t>(Iproto::REQUEST_TYPE)) {
            auto [val, vpos] = ReadUint(p, len, pos);
            pos = vpos;
            if (val != kIprotoVshardCallType) return std::nullopt;
            is_vshard_call = true;

        } else if (key == static_cast<uint64_t>(Iproto::SYNC)) {
            auto [val, vpos] = ReadUint(p, len, pos);
            pos = vpos;
            sync = static_cast<uint64_t>(val);

        } else if (key == kIprotoVshardBucketIdKey) {
            auto [val, vpos] = ReadUint(p, len, pos);
            pos = vpos;
            bucket_id = static_cast<uint32_t>(val);

        } else if (key == kIprotoVshardModeKey) {
            auto [val, vpos] = ReadUint(p, len, pos);
            pos = vpos;
            mode = static_cast<uint8_t>(val);

        } else {
            pos = SkipValue(p, len, pos);
        }
    }

    if (!is_vshard_call || !sync || !bucket_id || !mode) return std::nullopt;
    if (*bucket_id == 0 || *bucket_id > 65535u) return std::nullopt;

    return VshardCallInfo{*sync, *bucket_id, *mode};
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
