#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <userver/formats/json/serialize.hpp>
#include <userver/formats/json/value.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/formats/msgpack/tarantool_types.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

#include <storages/tarantool/impl/msgpack_constants.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

// ---- Pull in shared Tarantool ext types ------------------------------------
using formats::msgpack::TntUuid;

// Legacy alias: TntDatetime == TimestampTz
using TntDatetime = formats::msgpack::TimestampTz;

inline void EncodeUint(std::vector<uint8_t>& out, uint64_t v) {
    if (v <= 0x7f) {
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xFF) {
        out.push_back(mp::kUint8);
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xFFFF) {
        out.push_back(mp::kUint16);
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xFFFFFFFF) {
        out.push_back(mp::kUint32);
        out.push_back(static_cast<uint8_t>(v >> 24));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    } else {
        out.push_back(mp::kUint64);
        for (int i = 7; i >= 0; --i) {
            out.push_back(static_cast<uint8_t>(v >> (8 * i)));
        }
    }
}

inline void EncodeStr(std::vector<uint8_t>& out, std::string_view s) {
    const auto len = s.size();
    if (len <= 31) {
        out.push_back(static_cast<uint8_t>(mp::kFixStrMin | len));
    } else if (len <= 0xFF) {
        out.push_back(mp::kStr8);
        out.push_back(static_cast<uint8_t>(len));
    } else if (len <= 0xFFFF) {
        out.push_back(mp::kStr16);
        out.push_back(static_cast<uint8_t>(len >> 8));
        out.push_back(static_cast<uint8_t>(len));
    } else {
        out.push_back(mp::kStr32);
        out.push_back(static_cast<uint8_t>(len >> 24));
        out.push_back(static_cast<uint8_t>(len >> 16));
        out.push_back(static_cast<uint8_t>(len >> 8));
        out.push_back(static_cast<uint8_t>(len));
    }
    out.insert(out.end(), s.begin(), s.end());
}

inline void EncodeNil(std::vector<uint8_t>& out) {
    out.push_back(mp::kNil);
}

inline void EncodeFixMap(std::vector<uint8_t>& out, uint8_t count) {
    out.push_back(static_cast<uint8_t>(mp::kFixMapMin | count));
}

inline void EncodeArray(std::vector<uint8_t>& out, uint32_t count) {
    if (count <= 15) {
        out.push_back(static_cast<uint8_t>(0x90 | count));
    } else if (count <= 0xFFFF) {
        out.push_back(mp::kArray16);
        out.push_back(static_cast<uint8_t>(count >> 8));
        out.push_back(static_cast<uint8_t>(count));
    } else {
        out.push_back(mp::kArray32);
        out.push_back(static_cast<uint8_t>(count >> 24));
        out.push_back(static_cast<uint8_t>(count >> 16));
        out.push_back(static_cast<uint8_t>(count >> 8));
        out.push_back(static_cast<uint8_t>(count));
    }
}

// ---- Minimal MsgPack encoder into std::vector<uint8_t> ----

// Parse "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" into TntUuid.
// Throws TarantoolException on malformed input.
inline TntUuid UuidFromString(std::string_view s) {
    if (s.size() != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-') {
        throw TarantoolException{fmt::format("Invalid UUID string: '{}'", s)};
    }
    TntUuid uuid;
    int out_idx = 0;
    for (int i = 0; i < 36; ++i) {
        if (s[i] == '-') {
            continue;
        }
        auto hexval = [](char c) -> uint8_t {
            if (c >= '0' && c <= '9') {
                return static_cast<uint8_t>(c - '0');
            }
            if (c >= 'a' && c <= 'f') {
                return static_cast<uint8_t>(c - 'a' + 10);
            }
            if (c >= 'A' && c <= 'F') {
                return static_cast<uint8_t>(c - 'A' + 10);
            }
            throw TarantoolException{fmt::format("Invalid UUID hex char: '{}'", c)};
        };
        uuid.bytes[out_idx++] = static_cast<uint8_t>(hexval(s[i]) << 4 | hexval(s[i + 1]));
        ++i;  // consume second nibble
    }
    return uuid;
}

// Format TntUuid as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
inline std::string UuidToString(const TntUuid& uuid) {
    const auto& b = uuid.bytes;
    return fmt::format(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}"
        "-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    );
}

// Encode UUID as MsgPack fixext16 (0xd8), ext type 2.
inline void EncodeUuid(std::vector<uint8_t>& out, const TntUuid& uuid) {
    out.push_back(mp::kFixExt16);
    out.push_back(static_cast<uint8_t>(mp::kExtUuid));
    out.insert(out.end(), uuid.bytes.begin(), uuid.bytes.end());
}

// Write a little-endian 32-bit value.
inline void WriteLE32(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>(v));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 24));
}

// Write a little-endian 64-bit value.
inline void WriteLE64(std::vector<uint8_t>& out, uint64_t v) {
    WriteLE32(out, static_cast<uint32_t>(v));
    WriteLE32(out, static_cast<uint32_t>(v >> 32));
}

// Encode TntDatetime (= TimestampTz) as MsgPack ext type 4.
// Delegates to the shared EncodeTimestampTz helper.
inline void EncodeDateTime(std::vector<uint8_t>& out, const TntDatetime& dt) {
    auto bytes = formats::msgpack::EncodeTimestampTz(dt);
    out.insert(out.end(), bytes.begin(), bytes.end());
}

// ---- Minimal MsgPack decoder ----

struct MpDecoder {
    const uint8_t* p;
    const uint8_t* end;

    uint8_t Read8() {
        if (p >= end) {
            throw TarantoolException{"MsgPack buffer overrun"};
        }
        return *p++;
    }

    uint16_t Read16() {
        uint16_t v = static_cast<uint16_t>(Read8()) << 8;
        v |= Read8();
        return v;
    }

    uint32_t Read32() {
        uint32_t v = static_cast<uint32_t>(Read8()) << 24;
        v |= static_cast<uint32_t>(Read8()) << 16;
        v |= static_cast<uint32_t>(Read8()) << 8;
        v |= Read8();
        return v;
    }

    uint64_t Read64() {
        uint64_t v = static_cast<uint64_t>(Read32()) << 32;
        v |= Read32();
        return v;
    }

    uint32_t ReadLE32() {
        uint32_t v = static_cast<uint32_t>(Read8());
        v |= static_cast<uint32_t>(Read8()) << 8;
        v |= static_cast<uint32_t>(Read8()) << 16;
        v |= static_cast<uint32_t>(Read8()) << 24;
        return v;
    }

    uint64_t ReadLE64() {
        uint64_t lo = ReadLE32();
        uint64_t hi = ReadLE32();
        return lo | (hi << 32);
    }

    formats::json::Value DecodeValue();
    formats::json::Value DecodeMap(uint32_t count);
    formats::json::Value DecodeArray(uint32_t count);
    formats::json::Value DecodeExt(uint32_t data_len);
    std::string DecodeStr(uint32_t len);
    TntUuid DecodeUuidBytes();
    TntDatetime DecodeDatetimeBytes(uint32_t data_len);
};

inline std::string MpDecoder::DecodeStr(uint32_t len) {
    if (p + len > end) {
        throw TarantoolException{"MsgPack string overrun"};
    }
    std::string s{reinterpret_cast<const char*>(p), len};
    p += len;
    return s;
}

inline formats::json::Value MpDecoder::DecodeArray(uint32_t count) {
    formats::json::ValueBuilder arr(formats::json::Type::kArray);
    for (uint32_t i = 0; i < count; ++i) {
        arr.PushBack(DecodeValue());
    }
    return arr.ExtractValue();
}

inline formats::json::Value MpDecoder::DecodeMap(uint32_t count) {
    formats::json::ValueBuilder obj(formats::json::Type::kObject);
    for (uint32_t i = 0; i < count; ++i) {
        auto key = DecodeValue();
        auto val = DecodeValue();
        std::string key_str;
        if (key.IsString()) {
            key_str = key.As<std::string>();
        } else {
            key_str = std::to_string(key.As<int64_t>());
        }
        obj[key_str] = val;
    }
    return obj.ExtractValue();
}

inline TntUuid MpDecoder::DecodeUuidBytes() {
    TntUuid uuid;
    if (p + 16 > end) {
        throw TarantoolException{"MsgPack UUID overrun"};
    }
    std::copy(p, p + 16, uuid.bytes.begin());
    p += 16;
    return uuid;
}

inline TntDatetime MpDecoder::DecodeDatetimeBytes(uint32_t data_len) {
    if (data_len != 8 && data_len != 16) {
        throw TarantoolException{fmt::format("Bad datetime ext size: {}", data_len)};
    }
    if (p + data_len > end) {
        throw TarantoolException{"MsgPack datetime overrun"};
    }
    const auto raw = formats::msgpack::DecodeExt4Bytes(p, data_len);
    p += data_len;
    const int64_t total_ns = raw.seconds * 1'000'000'000LL + static_cast<int64_t>(raw.nsec);
    TntDatetime ts;
    ts.tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>{
        std::chrono::nanoseconds{total_ns}};
    ts.tzoffset = raw.tzoffset;
    ts.tzindex = raw.tzindex;
    return ts;
}

// DecodeExt: called after the fixext/ext marker byte is consumed.
// data_len is the number of ext data bytes (not counting the type byte).
inline formats::json::Value MpDecoder::DecodeExt(uint32_t data_len) {
    const int8_t ext_type = static_cast<int8_t>(Read8());
    if (ext_type == mp::kExtUuid) {
        if (data_len != 16) {
            throw TarantoolException{fmt::format("Bad UUID ext size: {}", data_len)};
        }
        return formats::json::ValueBuilder{UuidToString(DecodeUuidBytes())}.ExtractValue();
    } else if (ext_type == mp::kExtDatetime) {
        const TntDatetime dt = DecodeDatetimeBytes(data_len);
        const int64_t total_ns = dt.tp.time_since_epoch().count();
        int64_t seconds = total_ns / 1'000'000'000LL;
        int64_t nsec_i = total_ns % 1'000'000'000LL;
        if (nsec_i < 0) {
            nsec_i += 1'000'000'000LL;
            --seconds;
        }
        formats::json::ValueBuilder obj(formats::json::Type::kObject);
        obj["seconds"] = seconds;
        obj["nsec"] = nsec_i;
        obj["tzoffset"] = static_cast<int64_t>(dt.tzoffset);
        obj["tzindex"] = static_cast<int64_t>(dt.tzindex);
        return obj.ExtractValue();
    } else if (ext_type == mp::kExtDecimal) {
        // Decode BCD decimal → JSON string, e.g. "123.45"
        if (p + data_len > end) {
            throw TarantoolException{"MsgPack decimal overrun"};
        }
        const std::string s = formats::msgpack::DecodeDecimalBytes(p, data_len);
        p += data_len;
        return formats::json::ValueBuilder{s}.ExtractValue();
    } else if (ext_type == mp::kExtError) {
        // Decode the nested msgpack map (structured error stack) as JSON
        if (p + data_len > end) {
            throw TarantoolException{"MsgPack error overrun"};
        }
        MpDecoder inner{p, p + data_len};
        p += data_len;
        return inner.DecodeValue();
    } else if (ext_type == mp::kExtInterval) {
        // Decode packed interval → JSON object with field names
        if (p + data_len > end) {
            throw TarantoolException{"MsgPack interval overrun"};
        }
        const uint8_t* iend = p + data_len;
        MpDecoder inner{p, iend};
        p = iend;

        const uint64_t count = static_cast<uint64_t>(inner.DecodeValue().As<int64_t>(0));
        static constexpr const char* kFields[] =
            {"year", "month", "week", "day", "hour", "minute", "second", "nanosecond", "adjust"};
        formats::json::ValueBuilder obj(formats::json::Type::kObject);
        for (const auto* n : kFields) {
            obj[n] = int64_t{0};
        }
        for (uint64_t i = 0; i < count; ++i) {
            const auto field_id = static_cast<uint64_t>(inner.DecodeValue().As<int64_t>(0));
            const int64_t val = inner.DecodeValue().As<int64_t>(0);
            if (field_id < 9) {
                obj[kFields[field_id]] = val;
            }
        }
        return obj.ExtractValue();
    } else {
        // Unknown ext: skip data and return nil
        if (p + data_len > end) {
            throw TarantoolException{"MsgPack ext overrun"};
        }
        p += data_len;
        return formats::json::Value{};
    }
}

inline formats::json::Value MpDecoder::DecodeValue() {
    uint8_t b = Read8();
    if (b <= 0x7f) {
        return formats::json::ValueBuilder{static_cast<int64_t>(b)}.ExtractValue();
    } else if ((b & 0xE0) == 0xA0) {
        return formats::json::ValueBuilder{DecodeStr(b & 0x1F)}.ExtractValue();
    } else if ((b & 0xF0) == 0x90) {
        return DecodeArray(b & 0x0F);
    } else if ((b & 0xF0) == 0x80) {
        return DecodeMap(b & 0x0F);
    } else if ((b & 0xE0) == 0xE0) {
        return formats::json::ValueBuilder{static_cast<int64_t>(static_cast<int8_t>(b))}.ExtractValue();
    }
    switch (b) {
        case mp::kNil:
            return formats::json::Value{};
        case mp::kFalse:
            return formats::json::ValueBuilder{false}.ExtractValue();
        case mp::kTrue:
            return formats::json::ValueBuilder{true}.ExtractValue();
        case mp::kUint8:
            return formats::json::ValueBuilder{static_cast<int64_t>(Read8())}.ExtractValue();
        case mp::kUint16:
            return formats::json::ValueBuilder{static_cast<int64_t>(Read16())}.ExtractValue();
        case mp::kUint32:
            return formats::json::ValueBuilder{static_cast<int64_t>(Read32())}.ExtractValue();
        case mp::kUint64:
            return formats::json::ValueBuilder{static_cast<uint64_t>(Read64())}.ExtractValue();
        case mp::kInt8:
            return formats::json::ValueBuilder{static_cast<int64_t>(static_cast<int8_t>(Read8()))}.ExtractValue();
        case mp::kInt16:
            return formats::json::ValueBuilder{static_cast<int64_t>(static_cast<int16_t>(Read16()))}.ExtractValue();
        case mp::kInt32:
            return formats::json::ValueBuilder{static_cast<int64_t>(static_cast<int32_t>(Read32()))}.ExtractValue();
        case mp::kInt64:
            return formats::json::ValueBuilder{static_cast<int64_t>(Read64())}.ExtractValue();
        case mp::kStr8:
            return formats::json::ValueBuilder{DecodeStr(Read8())}.ExtractValue();
        case mp::kStr16:
            return formats::json::ValueBuilder{DecodeStr(Read16())}.ExtractValue();
        case mp::kStr32:
            return formats::json::ValueBuilder{DecodeStr(Read32())}.ExtractValue();
        case mp::kArray16:
            return DecodeArray(Read16());
        case mp::kArray32:
            return DecodeArray(Read32());
        case 0xde:
            return DecodeMap(Read16());  // map16
        case 0xdf:
            return DecodeMap(Read32());  // map32
        case 0xcb: {                     // float64
            uint64_t bits = Read64();
            double d;
            std::memcpy(&d, &bits, 8);
            return formats::json::ValueBuilder{d}.ExtractValue();
        }
        case 0xca: {  // float32
            uint32_t bits = Read32();
            float f;
            std::memcpy(&f, &bits, 4);
            return formats::json::ValueBuilder{static_cast<double>(f)}.ExtractValue();
        }
        // MsgPack ext types
        case mp::kFixExt1:
            return DecodeExt(1);
        case mp::kFixExt2:
            return DecodeExt(2);
        case mp::kFixExt4:
            return DecodeExt(4);
        case mp::kFixExt8:
            return DecodeExt(8);
        case mp::kFixExt16:
            return DecodeExt(16);
        case mp::kExt8:
            return DecodeExt(Read8());
        case mp::kExt16:
            return DecodeExt(Read16());
        case mp::kExt32:
            return DecodeExt(Read32());
        default:
            throw TarantoolException{fmt::format("Unknown MsgPack byte: 0x{:02x}", b)};
    }
}

inline uint32_t DecodePreheaderLength(const uint8_t* buf) {
    if (buf[0] != 0xce) {
        throw TarantoolException{"Bad IPROTO preheader marker"};
    }
    return (static_cast<uint32_t>(buf[1]) << 24) | (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 8) | static_cast<uint32_t>(buf[4]);
}

// ---- Helpers to decode a complete buffer ----

inline formats::json::Value MsgPackDecode(const std::vector<uint8_t>& buf) {
    MpDecoder dec{buf.data(), buf.data() + buf.size()};
    return dec.DecodeValue();
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
