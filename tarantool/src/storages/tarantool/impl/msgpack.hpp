#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <userver/formats/json/value.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/formats/json/serialize.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

// MsgPack type markers (subset used by IPROTO)
namespace mp {

constexpr uint8_t kFixMapMin   = 0x80;
constexpr uint8_t kArray16     = 0xdc;
constexpr uint8_t kArray32     = 0xdd;
constexpr uint8_t kStr8        = 0xd9;
constexpr uint8_t kStr16       = 0xda;
constexpr uint8_t kStr32       = 0xdb;
constexpr uint8_t kFixStrMin   = 0xa0;
constexpr uint8_t kNil         = 0xc0;
constexpr uint8_t kFalse       = 0xc2;
constexpr uint8_t kTrue        = 0xc3;
constexpr uint8_t kUint8       = 0xcc;
constexpr uint8_t kUint16      = 0xcd;
constexpr uint8_t kUint32      = 0xce;
constexpr uint8_t kUint64      = 0xcf;
constexpr uint8_t kInt8        = 0xd0;
constexpr uint8_t kInt16       = 0xd1;
constexpr uint8_t kInt32       = 0xd2;
constexpr uint8_t kInt64       = 0xd3;
constexpr uint8_t kFixIntMin   = 0x00;  // 0..127

}  // namespace mp

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

// ---- Minimal MsgPack encoder into std::vector<uint8_t> ----

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
        for (int i = 7; i >= 0; --i)
            out.push_back(static_cast<uint8_t>(v >> (8 * i)));
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

// Forward declaration
inline void EncodeJson(std::vector<uint8_t>& out,
                       const formats::json::Value& v);

inline void EncodeJson(std::vector<uint8_t>& out,
                       const formats::json::Value& v) {
    if (v.IsNull()) {
        out.push_back(mp::kNil);
    } else if (v.IsBool()) {
        out.push_back(v.As<bool>() ? mp::kTrue : mp::kFalse);
    } else if (v.IsInt64()) {
        const int64_t i = v.As<int64_t>();
        if (i >= 0) {
            EncodeUint(out, static_cast<uint64_t>(i));
        } else {
            if (i >= -32) {
                out.push_back(static_cast<uint8_t>(i & 0xFF));
            } else if (i >= -128) {
                out.push_back(mp::kInt8);
                out.push_back(static_cast<uint8_t>(i));
            } else if (i >= -32768) {
                out.push_back(mp::kInt16);
                out.push_back(static_cast<uint8_t>(i >> 8));
                out.push_back(static_cast<uint8_t>(i));
            } else if (i >= -2147483648LL) {
                out.push_back(mp::kInt32);
                for (int s = 24; s >= 0; s -= 8)
                    out.push_back(static_cast<uint8_t>(i >> s));
            } else {
                out.push_back(mp::kInt64);
                for (int s = 56; s >= 0; s -= 8)
                    out.push_back(static_cast<uint8_t>(i >> s));
            }
        }
    } else if (v.IsUInt64()) {
        EncodeUint(out, v.As<uint64_t>());
    } else if (v.IsDouble()) {
        double d = v.As<double>();
        out.push_back(0xcb);  // float64
        uint64_t bits;
        std::memcpy(&bits, &d, 8);
        for (int s = 56; s >= 0; s -= 8)
            out.push_back(static_cast<uint8_t>(bits >> s));
    } else if (v.IsString()) {
        EncodeStr(out, v.As<std::string>());
    } else if (v.IsArray()) {
        EncodeArray(out, static_cast<uint32_t>(v.GetSize()));
        for (const auto& elem : v) EncodeJson(out, elem);
    } else if (v.IsObject()) {
        uint32_t count = 0;
        for (const auto& [k, val] : Items(v)) ++count;
        if (count <= 15)
            out.push_back(static_cast<uint8_t>(mp::kFixMapMin | count));
        else {
            out.push_back(0xde);  // map16
            out.push_back(static_cast<uint8_t>(count >> 8));
            out.push_back(static_cast<uint8_t>(count));
        }
        for (const auto& [k, val] : Items(v)) {
            EncodeStr(out, k);
            EncodeJson(out, val);
        }
    }
}

// ---- Minimal MsgPack decoder ----

struct MpDecoder {
    const uint8_t* p;
    const uint8_t* end;

    uint8_t Read8() {
        if (p >= end) throw TarantoolException{"MsgPack buffer overrun"};
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

    formats::json::Value DecodeValue();
    formats::json::Value DecodeMap(uint32_t count);
    formats::json::Value DecodeArray(uint32_t count);
    std::string DecodeStr(uint32_t len);
};

inline std::string MpDecoder::DecodeStr(uint32_t len) {
    if (p + len > end) throw TarantoolException{"MsgPack string overrun"};
    std::string s{reinterpret_cast<const char*>(p), len};
    p += len;
    return s;
}

inline formats::json::Value MpDecoder::DecodeArray(uint32_t count) {
    formats::json::ValueBuilder arr(formats::json::Type::kArray);
    for (uint32_t i = 0; i < count; ++i) arr.PushBack(DecodeValue());
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
        return formats::json::ValueBuilder{
            static_cast<int64_t>(static_cast<int8_t>(b))}.ExtractValue();
    }
    switch (b) {
        case mp::kNil:   return formats::json::Value{};
        case mp::kFalse: return formats::json::ValueBuilder{false}.ExtractValue();
        case mp::kTrue:  return formats::json::ValueBuilder{true}.ExtractValue();
        case mp::kUint8:  return formats::json::ValueBuilder{
            static_cast<int64_t>(Read8())}.ExtractValue();
        case mp::kUint16: return formats::json::ValueBuilder{
            static_cast<int64_t>(Read16())}.ExtractValue();
        case mp::kUint32: return formats::json::ValueBuilder{
            static_cast<int64_t>(Read32())}.ExtractValue();
        case mp::kUint64: return formats::json::ValueBuilder{
            static_cast<uint64_t>(Read64())}.ExtractValue();
        case mp::kInt8:  return formats::json::ValueBuilder{
            static_cast<int64_t>(static_cast<int8_t>(Read8()))}.ExtractValue();
        case mp::kInt16: return formats::json::ValueBuilder{
            static_cast<int64_t>(static_cast<int16_t>(Read16()))}.ExtractValue();
        case mp::kInt32: return formats::json::ValueBuilder{
            static_cast<int64_t>(static_cast<int32_t>(Read32()))}.ExtractValue();
        case mp::kInt64: return formats::json::ValueBuilder{
            static_cast<int64_t>(Read64())}.ExtractValue();
        case mp::kStr8:  return formats::json::ValueBuilder{
            DecodeStr(Read8())}.ExtractValue();
        case mp::kStr16: return formats::json::ValueBuilder{
            DecodeStr(Read16())}.ExtractValue();
        case mp::kStr32: return formats::json::ValueBuilder{
            DecodeStr(Read32())}.ExtractValue();
        case mp::kArray16: return DecodeArray(Read16());
        case mp::kArray32: return DecodeArray(Read32());
        case 0xde: return DecodeMap(Read16());   // map16
        case 0xdf: return DecodeMap(Read32());   // map32
        case 0xcb: {                             // float64
            uint64_t bits = Read64();
            double d; std::memcpy(&d, &bits, 8);
            return formats::json::ValueBuilder{d}.ExtractValue();
        }
        case 0xca: {                             // float32
            uint32_t bits = Read32();
            float f; std::memcpy(&f, &bits, 4);
            return formats::json::ValueBuilder{static_cast<double>(f)}.ExtractValue();
        }
        default:
            throw TarantoolException{
                fmt::format("Unknown MsgPack byte: 0x{:02x}", b)};
    }
}

inline uint32_t DecodePreheaderLength(const uint8_t* buf) {
    if (buf[0] != 0xce)
        throw TarantoolException{"Bad IPROTO preheader marker"};
    return (static_cast<uint32_t>(buf[1]) << 24) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) <<  8) |
            static_cast<uint32_t>(buf[4]);
}

// ---- Helpers to decode a complete buffer ----

inline formats::json::Value MsgPackDecode(const std::vector<uint8_t>& buf) {
    MpDecoder dec{buf.data(), buf.data() + buf.size()};
    return dec.DecodeValue();
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
