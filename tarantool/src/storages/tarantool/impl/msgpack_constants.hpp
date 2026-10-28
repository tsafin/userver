#pragma once

/// @file storages/tarantool/impl/msgpack_constants.hpp
/// @brief MsgPack format-byte constants — zero-dependency header.
///
/// Extracted here so that lean scanner headers (e.g. iproto_frames.hpp)
/// can use named constants without pulling in the heavyweight msgpack.hpp
/// (which transitively includes userver formats and fmt).

#include <cstdint>

/// MsgPack wire-format type bytes.
namespace mp {

// ── Integer formats ──────────────────────────────────────────────────────────
constexpr uint8_t kFixIntMax = 0x7f;     ///< largest positive fixint
constexpr uint8_t kNegFixIntMin = 0xe0;  ///< smallest negative fixint (-32)

// ── Nil / bool ───────────────────────────────────────────────────────────────
constexpr uint8_t kNil = 0xc0;
constexpr uint8_t kFalse = 0xc2;
constexpr uint8_t kTrue = 0xc3;

// ── Binary ───────────────────────────────────────────────────────────────────
constexpr uint8_t kBin8 = 0xc4;
constexpr uint8_t kBin16 = 0xc5;
constexpr uint8_t kBin32 = 0xc6;

// ── Ext ──────────────────────────────────────────────────────────────────────
constexpr uint8_t kExt8 = 0xc7;
constexpr uint8_t kExt16 = 0xc8;
constexpr uint8_t kExt32 = 0xc9;

// ── Float ────────────────────────────────────────────────────────────────────
constexpr uint8_t kFloat32 = 0xca;
constexpr uint8_t kFloat64 = 0xcb;

// ── Unsigned integers ────────────────────────────────────────────────────────
constexpr uint8_t kUint8 = 0xcc;
constexpr uint8_t kUint16 = 0xcd;
constexpr uint8_t kUint32 = 0xce;
constexpr uint8_t kUint64 = 0xcf;

// ── Signed integers ──────────────────────────────────────────────────────────
constexpr uint8_t kInt8 = 0xd0;
constexpr uint8_t kInt16 = 0xd1;
constexpr uint8_t kInt32 = 0xd2;
constexpr uint8_t kInt64 = 0xd3;

// ── Fixed-length ext ─────────────────────────────────────────────────────────
constexpr uint8_t kFixExt1 = 0xd4;
constexpr uint8_t kFixExt2 = 0xd5;
constexpr uint8_t kFixExt4 = 0xd6;
constexpr uint8_t kFixExt8 = 0xd7;
constexpr uint8_t kFixExt16 = 0xd8;

// ── String ───────────────────────────────────────────────────────────────────
constexpr uint8_t kStr8 = 0xd9;
constexpr uint8_t kStr16 = 0xda;
constexpr uint8_t kStr32 = 0xdb;

// ── Array ────────────────────────────────────────────────────────────────────
constexpr uint8_t kArray16 = 0xdc;
constexpr uint8_t kArray32 = 0xdd;

// ── Map ──────────────────────────────────────────────────────────────────────
constexpr uint8_t kMap16 = 0xde;
constexpr uint8_t kMap32 = 0xdf;

// ── Variable-length prefix ranges (mask + tag) ───────────────────────────────
constexpr uint8_t kFixMapMin = 0x80;    ///< fixmap: 0x80..0x8f  (n = b & 0x0f)
constexpr uint8_t kFixArrayMin = 0x90;  ///< fixarray: 0x90..0x9f (n = b & 0x0f)
constexpr uint8_t kFixStrMin = 0xa0;    ///< fixstr: 0xa0..0xbf  (n = b & 0x1f)

// ── Tarantool custom ext type IDs ────────────────────────────────────────────
constexpr int8_t kExtDecimal = 1;
constexpr int8_t kExtUuid = 2;
constexpr int8_t kExtError = 3;
constexpr int8_t kExtDatetime = 4;
constexpr int8_t kExtInterval = 6;

}  // namespace mp
