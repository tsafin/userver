#pragma once

/// @file storages/tarantool/impl/iproto_frames.hpp
/// @brief Compile-time static IPROTO frame builders for fixed-layout requests.
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
/// sync_id is always encoded as uint64 so the frame size is constant
/// regardless of the counter value, enabling simple memcpy into staging_buf_.

#include <array>
#include <cstdint>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

/// Number of bytes in a serialised IPROTO PING frame.
constexpr std::size_t kPingFrameSize = 18;

/// Build a complete, wire-ready IPROTO PING frame on the stack.
/// @param sync_id  The request sync counter for this frame.
/// @returns        A 18-byte array ready to be appended to staging_buf_.
[[nodiscard]] inline std::array<uint8_t, kPingFrameSize>
BuildPingFrame(uint64_t sync_id) noexcept {
    return {{
        // ── preheader ───────────────────────────────────────────────────────
        0xce,                                    // uint32 length marker
        0x00, 0x00, 0x00, 0x0d,                 // length = 13 (header only, no body)
        // ── IPROTO header map ───────────────────────────────────────────────
        0x82,                                    // fixmap(2)
        0x00, 0x40,                              // IPROTO_CODE = 64 (PING)
        0x01, 0xcf,                              // IPROTO_SYNC, uint64 marker
        // ── sync_id (big-endian uint64) ─────────────────────────────────────
        static_cast<uint8_t>(sync_id >> 56),
        static_cast<uint8_t>(sync_id >> 48),
        static_cast<uint8_t>(sync_id >> 40),
        static_cast<uint8_t>(sync_id >> 32),
        static_cast<uint8_t>(sync_id >> 24),
        static_cast<uint8_t>(sync_id >> 16),
        static_cast<uint8_t>(sync_id >>  8),
        static_cast<uint8_t>(sync_id),
    }};
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
