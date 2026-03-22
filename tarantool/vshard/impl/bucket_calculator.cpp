#include <vshard/impl/bucket_calculator.hpp>

#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <string_view>

#include <boost/crc.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {

// Tarantool's digest.crc32 uses CRC32C (Castagnoli polynomial 0x1EDC6F41)
// with initial value 0xFFFFFFFF and NO final XOR, matching tnt_crc32c().
// This is equivalent to boost::crc_optimal<32,0x1EDC6F41,0xFFFFFFFF,0,true,true>.
using TarantoolCrc32 =
    boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0x00000000, true, true>;

uint32_t Crc32Bytes(const void* data, std::size_t len) noexcept {
    TarantoolCrc32 crc;
    crc.process_bytes(data, len);
    return crc.checksum();
}

// Msgpack-encode a signed integer into buf[0..N-1].
// Returns number of bytes written.
// Follows the msgpack spec: smallest possible representation.
std::size_t MsgpackEncodeInt(uint8_t* buf, int64_t v) noexcept {
    if (v >= 0) {
        // positive fixint: 0x00–0x7f
        if (v <= 0x7f) { buf[0] = static_cast<uint8_t>(v); return 1; }
        if (v <= 0xff) { buf[0] = 0xcc; buf[1] = static_cast<uint8_t>(v); return 2; }
        if (v <= 0xffff) {
            buf[0] = 0xcd;
            buf[1] = static_cast<uint8_t>(v >> 8);
            buf[2] = static_cast<uint8_t>(v);
            return 3;
        }
        if (v <= 0xffffffff) {
            buf[0] = 0xce;
            buf[1] = static_cast<uint8_t>(v >> 24);
            buf[2] = static_cast<uint8_t>(v >> 16);
            buf[3] = static_cast<uint8_t>(v >> 8);
            buf[4] = static_cast<uint8_t>(v);
            return 5;
        }
        buf[0] = 0xcf;
        for (int i = 7; i >= 0; --i) {
            buf[1 + (7 - i)] = static_cast<uint8_t>(
                static_cast<uint64_t>(v) >> (i * 8));
        }
        return 9;
    } else {
        // negative fixint: -32..-1 → 0xe0–0xff
        if (v >= -32) { buf[0] = static_cast<uint8_t>(v & 0xff); return 1; }
        if (v >= -128) { buf[0] = 0xd0; buf[1] = static_cast<uint8_t>(v); return 2; }
        if (v >= -32768) {
            buf[0] = 0xd1;
            buf[1] = static_cast<uint8_t>(static_cast<int16_t>(v) >> 8);
            buf[2] = static_cast<uint8_t>(v);
            return 3;
        }
        if (v >= -2147483648LL) {
            buf[0] = 0xd2;
            buf[1] = static_cast<uint8_t>(static_cast<int32_t>(v) >> 24);
            buf[2] = static_cast<uint8_t>(static_cast<int32_t>(v) >> 16);
            buf[3] = static_cast<uint8_t>(static_cast<int32_t>(v) >> 8);
            buf[4] = static_cast<uint8_t>(v);
            return 5;
        }
        buf[0] = 0xd3;
        for (int i = 7; i >= 0; --i) {
            buf[1 + (7 - i)] = static_cast<uint8_t>(
                static_cast<uint64_t>(v) >> (i * 8));
        }
        return 9;
    }
}

// Msgpack-encode an unsigned integer into buf[0..N-1].
std::size_t MsgpackEncodeUint(uint8_t* buf, uint64_t v) noexcept {
    if (v <= 0x7f)        { buf[0] = static_cast<uint8_t>(v); return 1; }
    if (v <= 0xff)        { buf[0] = 0xcc; buf[1] = static_cast<uint8_t>(v); return 2; }
    if (v <= 0xffff) {
        buf[0] = 0xcd;
        buf[1] = static_cast<uint8_t>(v >> 8);
        buf[2] = static_cast<uint8_t>(v);
        return 3;
    }
    if (v <= 0xffffffff) {
        buf[0] = 0xce;
        buf[1] = static_cast<uint8_t>(v >> 24);
        buf[2] = static_cast<uint8_t>(v >> 16);
        buf[3] = static_cast<uint8_t>(v >> 8);
        buf[4] = static_cast<uint8_t>(v);
        return 5;
    }
    buf[0] = 0xcf;
    for (int i = 7; i >= 0; --i) {
        buf[1 + (7 - i)] = static_cast<uint8_t>(v >> (i * 8));
    }
    return 9;
}

}  // namespace

// ---- mpcrc32 ----------------------------------------------------------------

uint32_t BucketCalculator::BucketIdMpcrc32(std::string_view key) const noexcept {
    // String keys: hash raw bytes, no msgpack header.
    return Crc32ToId(Crc32Bytes(key.data(), key.size()));
}

uint32_t BucketCalculator::BucketIdMpcrc32(int64_t key) const noexcept {
    uint8_t buf[9];
    const auto len = MsgpackEncodeInt(buf, key);
    return Crc32ToId(Crc32Bytes(buf, len));
}

uint32_t BucketCalculator::BucketIdMpcrc32(uint64_t key) const noexcept {
    uint8_t buf[9];
    const auto len = MsgpackEncodeUint(buf, key);
    return Crc32ToId(Crc32Bytes(buf, len));
}

// ---- strcrc32 (legacy) -------------------------------------------------------

uint32_t BucketCalculator::BucketIdStrcrc32(std::string_view key) const noexcept {
    // For strings strcrc32 == mpcrc32: raw bytes, no msgpack header.
    return Crc32ToId(Crc32Bytes(key.data(), key.size()));
}

uint32_t BucketCalculator::BucketIdStrcrc32(int64_t key) const noexcept {
    // Integers are stringified with tostring() → decimal representation.
    char buf[21];  // max "-9223372036854775808\0"
    const auto [end, ec] = std::to_chars(buf, buf + sizeof(buf), key);
    return Crc32ToId(Crc32Bytes(buf, end - buf));
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
