#include <userver/formats/msgpack/value.hpp>

#include <array>
#include <cassert>
#include <cstring>
#include <limits>

#include <fmt/format.h>

#include <userver/formats/msgpack/tarantool_types.hpp>
#include <userver/formats/parse/to.hpp>
#include <userver/utils/datetime/date.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

// ======================================================================== //
//  Internal type-code constants                                             //
// ======================================================================== //

namespace {

// Type IDs used in TypeMismatchException (must match TypeName() in exception.cpp)
constexpr int kTypeNull   = 0;
constexpr int kTypeBool   = 1;
constexpr int kTypeInt    = 2;
constexpr int kTypeUInt   = 3;
// 4 = float (unused in exceptions)
constexpr int kTypeDouble = 5;
constexpr int kTypeStr    = 6;
// 7 = bin
constexpr int kTypeArray  = 8;
constexpr int kTypeMap    = 9;
constexpr int kTypeExt    = 10;

// ======================================================================== //
//  Big-endian read helpers                                                  //
// ======================================================================== //

inline uint16_t Read16(const uint8_t* p) {
    uint16_t v{};
    std::memcpy(&v, p, 2);
    return __builtin_bswap16(v);
}

inline uint32_t Read32(const uint8_t* p) {
    uint32_t v{};
    std::memcpy(&v, p, 4);
    return __builtin_bswap32(v);
}

inline uint64_t Read64(const uint8_t* p) {
    uint64_t v{};
    std::memcpy(&v, p, 8);
    return __builtin_bswap64(v);
}

// ======================================================================== //
//  BoundsCheck                                                              //
// ======================================================================== //

void BoundsCheck(const uint8_t* pos, const uint8_t* end, std::size_t needed,
                 std::string_view path) {
    if (static_cast<std::size_t>(end - pos) < needed) {
        throw ParseException(
            fmt::format("Truncated msgpack at '{}': need {} bytes, have {}",
                        path, needed, static_cast<std::size_t>(end - pos)));
    }
}

// ======================================================================== //
//  Skip: advance past one complete msgpack value                            //
// ======================================================================== //

// Forward declaration for mutual recursion
const uint8_t* Skip(const uint8_t* p, const uint8_t* end,
                    std::string_view path);

// Skip N individual values (used for arrays / maps)
const uint8_t* SkipN(const uint8_t* p, const uint8_t* end, uint32_t n,
                     std::string_view path) {
    for (uint32_t i = 0; i < n; ++i) {
        p = Skip(p, end, path);
    }
    return p;
}

const uint8_t* Skip(const uint8_t* p, const uint8_t* end,
                    std::string_view path) {
    BoundsCheck(p, end, 1, path);
    const uint8_t b = *p++;

    // positive fixint  0x00–0x7f
    if (b <= 0x7f) return p;
    // negative fixint  0xe0–0xff
    if (b >= 0xe0) return p;
    // fixstr  0xa0–0xbf
    if ((b & 0xe0) == 0xa0) {
        uint32_t n = b & 0x1f;
        BoundsCheck(p, end, n, path);
        return p + n;
    }
    // fixarray  0x90–0x9f
    if ((b & 0xf0) == 0x90) {
        return SkipN(p, end, b & 0x0f, path);
    }
    // fixmap  0x80–0x8f
    if ((b & 0xf0) == 0x80) {
        return SkipN(p, end, (b & 0x0f) * 2u, path);
    }

    switch (b) {
        case 0xc0: return p;              // nil
        case 0xc2: return p;              // false
        case 0xc3: return p;              // true

        case 0xcc:                        // uint8
        case 0xd0:                        // int8
            BoundsCheck(p, end, 1, path); return p + 1;

        case 0xcd:                        // uint16
        case 0xd1:                        // int16
            BoundsCheck(p, end, 2, path); return p + 2;

        case 0xce:                        // uint32
        case 0xd2:                        // int32
        case 0xca:                        // float32
            BoundsCheck(p, end, 4, path); return p + 4;

        case 0xcf:                        // uint64
        case 0xd3:                        // int64
        case 0xcb:                        // float64
            BoundsCheck(p, end, 8, path); return p + 8;

        case 0xd9: {                      // str8
            BoundsCheck(p, end, 1, path);
            uint32_t n = *p++;
            BoundsCheck(p, end, n, path);
            return p + n;
        }
        case 0xda: {                      // str16
            BoundsCheck(p, end, 2, path);
            uint32_t n = Read16(p); p += 2;
            BoundsCheck(p, end, n, path);
            return p + n;
        }
        case 0xdb: {                      // str32
            BoundsCheck(p, end, 4, path);
            uint32_t n = Read32(p); p += 4;
            BoundsCheck(p, end, n, path);
            return p + n;
        }

        case 0xc4: {                      // bin8
            BoundsCheck(p, end, 1, path);
            uint32_t n = *p++;
            BoundsCheck(p, end, n, path);
            return p + n;
        }
        case 0xc5: {                      // bin16
            BoundsCheck(p, end, 2, path);
            uint32_t n = Read16(p); p += 2;
            BoundsCheck(p, end, n, path);
            return p + n;
        }
        case 0xc6: {                      // bin32
            BoundsCheck(p, end, 4, path);
            uint32_t n = Read32(p); p += 4;
            BoundsCheck(p, end, n, path);
            return p + n;
        }

        case 0xdc: {                      // array16
            BoundsCheck(p, end, 2, path);
            uint32_t n = Read16(p); p += 2;
            return SkipN(p, end, n, path);
        }
        case 0xdd: {                      // array32
            BoundsCheck(p, end, 4, path);
            uint32_t n = Read32(p); p += 4;
            return SkipN(p, end, n, path);
        }

        case 0xde: {                      // map16
            BoundsCheck(p, end, 2, path);
            uint32_t n = Read16(p); p += 2;
            return SkipN(p, end, n * 2u, path);
        }
        case 0xdf: {                      // map32
            BoundsCheck(p, end, 4, path);
            uint32_t n = Read32(p); p += 4;
            return SkipN(p, end, n * 2u, path);
        }

        // fixext
        case 0xd4: BoundsCheck(p, end, 2,  path); return p + 2;   // fixext1
        case 0xd5: BoundsCheck(p, end, 3,  path); return p + 3;   // fixext2
        case 0xd6: BoundsCheck(p, end, 5,  path); return p + 5;   // fixext4
        case 0xd7: BoundsCheck(p, end, 9,  path); return p + 9;   // fixext8
        case 0xd8: BoundsCheck(p, end, 17, path); return p + 17;  // fixext16

        case 0xc7: {                      // ext8
            BoundsCheck(p, end, 1, path);
            uint32_t n = *p++; p++;       // skip type byte
            BoundsCheck(p, end, n, path);
            return p + n;
        }
        case 0xc8: {                      // ext16
            BoundsCheck(p, end, 2, path);
            uint32_t n = Read16(p); p += 2; p++;  // skip type byte
            BoundsCheck(p, end, n, path);
            return p + n;
        }
        case 0xc9: {                      // ext32
            BoundsCheck(p, end, 4, path);
            uint32_t n = Read32(p); p += 4; p++;  // skip type byte
            BoundsCheck(p, end, n, path);
            return p + n;
        }

        default:
            throw ParseException(
                fmt::format("Unknown msgpack lead byte 0x{:02x} at '{}'",
                            b, path));
    }
}

// ======================================================================== //
//  MapElementCount / ArrayElementCount                                     //
// ======================================================================== //

uint32_t ReadMapCount(const uint8_t* p, const uint8_t* end,
                      std::string_view path, const uint8_t** body_out) {
    BoundsCheck(p, end, 1, path);
    const uint8_t b = *p++;
    if ((b & 0xf0) == 0x80) {
        if (body_out) *body_out = p;
        return b & 0x0f;
    }
    if (b == 0xde) {
        BoundsCheck(p, end, 2, path);
        uint32_t n = Read16(p);
        if (body_out) *body_out = p + 2;
        return n;
    }
    if (b == 0xdf) {
        BoundsCheck(p, end, 4, path);
        uint32_t n = Read32(p);
        if (body_out) *body_out = p + 4;
        return n;
    }
    throw TypeMismatchException(kTypeArray /*hack: actual*/, kTypeMap, path);
}

uint32_t ReadArrayCount(const uint8_t* p, const uint8_t* end,
                        std::string_view path, const uint8_t** body_out) {
    BoundsCheck(p, end, 1, path);
    const uint8_t b = *p++;
    if ((b & 0xf0) == 0x90) {
        if (body_out) *body_out = p;
        return b & 0x0f;
    }
    if (b == 0xdc) {
        BoundsCheck(p, end, 2, path);
        uint32_t n = Read16(p);
        if (body_out) *body_out = p + 2;
        return n;
    }
    if (b == 0xdd) {
        BoundsCheck(p, end, 4, path);
        uint32_t n = Read32(p);
        if (body_out) *body_out = p + 4;
        return n;
    }
    throw TypeMismatchException(kTypeMap /*hack: actual*/, kTypeArray, path);
}

// ======================================================================== //
//  Key reader helpers                                                       //
// ======================================================================== //

// Attempt to read an integer from p as a msgpack uint/int.
// Returns true and writes to *val; false if not an integer type.
bool TryReadUIntKey(const uint8_t* p, const uint8_t* end, std::string_view path,
                    uint64_t* val, const uint8_t** next) {
    BoundsCheck(p, end, 1, path);
    const uint8_t b = *p++;
    if (b <= 0x7f) { *val = b;              *next = p; return true; }
    if (b >= 0xe0) {
        *val = static_cast<uint64_t>(static_cast<int64_t>(static_cast<int8_t>(b)));
        *next = p;
        return true;
    }
    switch (b) {
        case 0xcc: BoundsCheck(p, end, 1, path); *val = *p;          *next = p+1; return true;
        case 0xcd: BoundsCheck(p, end, 2, path); *val = Read16(p);   *next = p+2; return true;
        case 0xce: BoundsCheck(p, end, 4, path); *val = Read32(p);   *next = p+4; return true;
        case 0xcf: BoundsCheck(p, end, 8, path); *val = Read64(p);   *next = p+8; return true;
        case 0xd0: BoundsCheck(p, end, 1, path);
            *val = static_cast<uint64_t>(static_cast<int8_t>(*p));   *next = p+1; return true;
        case 0xd1: BoundsCheck(p, end, 2, path);
            *val = static_cast<uint64_t>(static_cast<int16_t>(Read16(p))); *next = p+2; return true;
        case 0xd2: BoundsCheck(p, end, 4, path);
            *val = static_cast<uint64_t>(static_cast<int32_t>(Read32(p))); *next = p+4; return true;
        case 0xd3: BoundsCheck(p, end, 8, path);
            *val = static_cast<uint64_t>(static_cast<int64_t>(Read64(p))); *next = p+8; return true;
        default: return false;
    }
}

bool TryReadStrKey(const uint8_t* p, const uint8_t* end, std::string_view path,
                   std::string_view* sv, const uint8_t** next) {
    BoundsCheck(p, end, 1, path);
    const uint8_t b = *p++;
    uint32_t len = 0;
    if ((b & 0xe0) == 0xa0) {
        len = b & 0x1f;
    } else if (b == 0xd9) {
        BoundsCheck(p, end, 1, path); len = *p++;
    } else if (b == 0xda) {
        BoundsCheck(p, end, 2, path); len = Read16(p); p += 2;
    } else if (b == 0xdb) {
        BoundsCheck(p, end, 4, path); len = Read32(p); p += 4;
    } else {
        return false;
    }
    BoundsCheck(p, end, len, path);
    *sv = std::string_view{reinterpret_cast<const char*>(p), len};
    *next = p + len;
    return true;
}

}  // namespace

// ======================================================================== //
//  Value private constructor                                                //
// ======================================================================== //

Value::Value(const uint8_t* pos, const uint8_t* end, std::string path) noexcept
    : pos_(pos), end_(end), path_(std::move(path)) {}

// ======================================================================== //
//  Value factory                                                            //
// ======================================================================== //

/*static*/ Value Value::FromBytes(const uint8_t* data, std::size_t len) noexcept {
    return Value{data, data + len, "/"};
}

// ======================================================================== //
//  GetPath                                                                  //
// ======================================================================== //

std::string Value::GetPath() const { return path_.empty() ? "/" : path_; }

// ======================================================================== //
//  NextSibling                                                              //
// ======================================================================== //

Value Value::NextSibling() const {
    if (IsMissing()) return Value{};
    const uint8_t* next = Skip(pos_, end_, path_);
    if (next >= end_) return Value{};
    return Value{next, end_, path_};
}

// ======================================================================== //
//  CheckNotMissing / ThrowTypeMismatch                                      //
// ======================================================================== //

void Value::CheckNotMissing() const {
    if (IsMissing()) throw MemberMissingException(GetPath());
}

[[noreturn]] void Value::ThrowTypeMismatch(int expected) const {
    // Determine actual type from lead byte
    int actual = kTypeNull;
    if (!IsMissing() && pos_ != end_) {
        const uint8_t b = *pos_;
        if (b <= 0x7f || b >= 0xe0)               actual = kTypeInt;
        else if ((b & 0xe0) == 0xa0)               actual = kTypeStr;
        else if ((b & 0xf0) == 0x90 || b == 0xdc || b == 0xdd) actual = kTypeArray;
        else if ((b & 0xf0) == 0x80 || b == 0xde || b == 0xdf) actual = kTypeMap;
        else switch (b) {
            case 0xc0: actual = kTypeNull;   break;
            case 0xc2: case 0xc3: actual = kTypeBool; break;
            case 0xcc: case 0xcd: case 0xce: case 0xcf: actual = kTypeUInt;   break;
            case 0xd0: case 0xd1: case 0xd2: case 0xd3: actual = kTypeInt;    break;
            case 0xca: actual = kTypeDouble; break;
            case 0xcb: actual = kTypeDouble; break;
            case 0xd9: case 0xda: case 0xdb: actual = kTypeStr;    break;
            case 0xd4: case 0xd5: case 0xd6: case 0xd7: case 0xd8:
            case 0xc7: case 0xc8: case 0xc9: actual = kTypeExt;    break;
            default: actual = kTypeNull;
        }
    }
    throw TypeMismatchException(actual, expected, GetPath());
}

// ======================================================================== //
//  Type predicates                                                          //
// ======================================================================== //

bool Value::IsNull() const noexcept {
    if (IsMissing()) return false;
    return *pos_ == 0xc0;
}

bool Value::IsBool() const noexcept {
    if (IsMissing()) return false;
    return *pos_ == 0xc2 || *pos_ == 0xc3;
}

bool Value::IsInt() const noexcept {
    if (IsMissing()) return false;
    const uint8_t b = *pos_;
    return b <= 0x7f || b >= 0xe0 ||
           b == 0xcc || b == 0xcd || b == 0xce || b == 0xcf ||
           b == 0xd0 || b == 0xd1 || b == 0xd2 || b == 0xd3;
}

bool Value::IsUInt() const noexcept {
    if (IsMissing()) return false;
    const uint8_t b = *pos_;
    return (b <= 0x7f) ||
           b == 0xcc || b == 0xcd || b == 0xce || b == 0xcf;
}

bool Value::IsDouble() const noexcept {
    if (IsMissing()) return false;
    return *pos_ == 0xca || *pos_ == 0xcb;
}

bool Value::IsString() const noexcept {
    if (IsMissing()) return false;
    const uint8_t b = *pos_;
    return (b & 0xe0) == 0xa0 || b == 0xd9 || b == 0xda || b == 0xdb;
}

bool Value::IsArray() const noexcept {
    if (IsMissing()) return false;
    const uint8_t b = *pos_;
    return (b & 0xf0) == 0x90 || b == 0xdc || b == 0xdd;
}

bool Value::IsObject() const noexcept {
    if (IsMissing()) return false;
    const uint8_t b = *pos_;
    return (b & 0xf0) == 0x80 || b == 0xde || b == 0xdf;
}

bool Value::IsExt() const noexcept {
    if (IsMissing()) return false;
    const uint8_t b = *pos_;
    return b == 0xd4 || b == 0xd5 || b == 0xd6 || b == 0xd7 || b == 0xd8 ||
           b == 0xc7 || b == 0xc8 || b == 0xc9;
}

// ======================================================================== //
//  GetSize                                                                  //
// ======================================================================== //

std::size_t Value::GetSize() const {
    if (IsMissing()) return 0;
    if (IsArray()) {
        const uint8_t* body{};
        return ReadArrayCount(pos_, end_, GetPath(), &body);
    }
    if (IsObject()) {
        const uint8_t* body{};
        return ReadMapCount(pos_, end_, GetPath(), &body);
    }
    return 0;
}

// ======================================================================== //
//  operator[] — dispatches by runtime type                                 //
// ======================================================================== //

Value Value::operator[](std::size_t index) const {
    CheckNotMissing();
    if (IsObject()) {
        // Integer key lookup in a map
        const uint64_t key = static_cast<uint64_t>(index);
        const uint8_t* body{};
        const uint32_t n = ReadMapCount(pos_, end_, path_, &body);
        const uint8_t* p = body;

        for (uint32_t i = 0; i < n; ++i) {
            uint64_t k{};
            const uint8_t* val_start{};
            if (TryReadUIntKey(p, end_, path_, &k, &val_start)) {
                if (k == key) {
                    return Value{val_start, end_,
                                 fmt::format("{}[{}]", path_, key)};
                }
                p = Skip(val_start, end_, path_);
            } else {
                p = Skip(p, end_, path_);
                p = Skip(p, end_, path_);
            }
        }
        return Value{};  // missing
    }
    if (IsArray()) {
        const uint8_t* body{};
        const uint32_t n = ReadArrayCount(pos_, end_, path_, &body);
        if (index >= n) throw OutOfBoundsException(index, n, GetPath());
        const uint8_t* p = body;
        for (std::size_t i = 0; i < index; ++i) p = Skip(p, end_, path_);
        return Value{p, end_, fmt::format("{}[{}]", path_, index)};
    }
    ThrowTypeMismatch(kTypeArray);
}

Value Value::operator[](std::string_view key) const {
    CheckNotMissing();
    if (!IsObject()) ThrowTypeMismatch(kTypeMap);

    const uint8_t* body{};
    const uint32_t n = ReadMapCount(pos_, end_, path_, &body);
    const uint8_t* p = body;

    for (uint32_t i = 0; i < n; ++i) {
        std::string_view k;
        const uint8_t* val_start{};
        if (TryReadStrKey(p, end_, path_, &k, &val_start)) {
            if (k == key) {
                return Value{val_start, end_,
                             fmt::format("{}.{}", path_, key)};
            }
            p = Skip(val_start, end_, path_);
        } else {
            p = Skip(p, end_, path_);
            p = Skip(p, end_, path_);
        }
    }
    return Value{};
}

// ======================================================================== //
//  Ext type access                                                          //
// ======================================================================== //

int8_t Value::GetExtType() const {
    CheckNotMissing();
    if (!IsExt()) ThrowTypeMismatch(kTypeExt);
    const uint8_t b = *pos_;
    // For fixext1–fixext16, type byte is at pos_+1
    if (b >= 0xd4 && b <= 0xd8) return static_cast<int8_t>(*(pos_ + 1));
    // For ext8/ext16/ext32, type byte follows the length bytes
    if (b == 0xc7) return static_cast<int8_t>(*(pos_ + 2));
    if (b == 0xc8) return static_cast<int8_t>(*(pos_ + 3));
    if (b == 0xc9) return static_cast<int8_t>(*(pos_ + 5));
    throw ParseException("Bad ext lead byte");
}

std::string_view Value::GetExtData() const {
    CheckNotMissing();
    if (!IsExt()) ThrowTypeMismatch(kTypeExt);
    const uint8_t b = *pos_;
    const uint8_t* data{};
    uint32_t len{};
    if (b >= 0xd4 && b <= 0xd8) {
        static constexpr uint8_t kSizes[] = {1,2,4,8,16};
        len  = kSizes[b - 0xd4];
        data = pos_ + 2;   // skip lead + type byte
    } else if (b == 0xc7) {
        len  = *(pos_ + 1);
        data = pos_ + 3;
    } else if (b == 0xc8) {
        len  = Read16(pos_ + 1);
        data = pos_ + 4;
    } else {
        len  = Read32(pos_ + 1);
        data = pos_ + 6;
    }
    return {reinterpret_cast<const char*>(data), len};
}

// ======================================================================== //
//  As<T> specialisations                                                   //
// ======================================================================== //

namespace {

// Read a signed or unsigned integer from the msgpack value.
// Used by all the As<int*> / As<uint*> specialisations.
int64_t ReadSignedInt(const Value& v) {
    const uint8_t* p = v.GetRawPos();
    const uint8_t* e = v.GetRawEnd();
    const std::string path = v.GetPath();
    BoundsCheck(p, e, 1, path);
    const uint8_t b = *p++;
    if (b <= 0x7f) return static_cast<int64_t>(b);
    if (b >= 0xe0) return static_cast<int64_t>(static_cast<int8_t>(b));
    switch (b) {
        case 0xcc: BoundsCheck(p, e, 1, path); return static_cast<int64_t>(*p);
        case 0xcd: BoundsCheck(p, e, 2, path); return static_cast<int64_t>(Read16(p));
        case 0xce: BoundsCheck(p, e, 4, path); return static_cast<int64_t>(Read32(p));
        case 0xcf: BoundsCheck(p, e, 8, path); return static_cast<int64_t>(Read64(p));
        case 0xd0: BoundsCheck(p, e, 1, path); return static_cast<int8_t>(*p);
        case 0xd1: BoundsCheck(p, e, 2, path); return static_cast<int16_t>(Read16(p));
        case 0xd2: BoundsCheck(p, e, 4, path); return static_cast<int32_t>(Read32(p));
        case 0xd3: BoundsCheck(p, e, 8, path); return static_cast<int64_t>(Read64(p));
        default: break;
    }
    throw TypeMismatchException(kTypeDouble, kTypeInt, path);
}

uint64_t ReadUnsignedInt(const Value& v) {
    const int64_t s = ReadSignedInt(v);
    if (s < 0) {
        throw ConversionException(
            fmt::format("Cannot convert negative value {} to unsigned", s),
            v.GetPath());
    }
    return static_cast<uint64_t>(s);
}

double ReadDouble(const Value& v) {
    const uint8_t* p = v.GetRawPos();
    const uint8_t* e = v.GetRawEnd();
    BoundsCheck(p, e, 1, v.GetPath());
    const uint8_t b = *p++;
    if (b == 0xca) {
        BoundsCheck(p, e, 4, v.GetPath());
        uint32_t bits = Read32(p);
        float fv{};
        std::memcpy(&fv, &bits, 4);
        return static_cast<double>(fv);
    }
    if (b == 0xcb) {
        BoundsCheck(p, e, 8, v.GetPath());
        uint64_t bits = Read64(p);
        double dv{};
        std::memcpy(&dv, &bits, 8);
        return dv;
    }
    // Allow integer → double promotion
    if (b <= 0x7f || b >= 0xe0 ||
        (b >= 0xcc && b <= 0xcf) || (b >= 0xd0 && b <= 0xd3)) {
        return static_cast<double>(ReadSignedInt(v));
    }
    throw TypeMismatchException(kTypeStr, kTypeDouble, v.GetPath());
}

template <typename T>
T NarrowInt(int64_t v, std::string_view path) {
    constexpr auto lo = static_cast<int64_t>(std::numeric_limits<T>::min());
    constexpr auto hi = static_cast<int64_t>(std::numeric_limits<T>::max());
    if (v < lo || v > hi) {
        throw ConversionException(
            fmt::format("Integer {} overflows target type", v), path);
    }
    return static_cast<T>(v);
}

template <typename T>
T NarrowUInt(uint64_t v, std::string_view path) {
    if (v > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        throw ConversionException(
            fmt::format("Integer {} overflows target type", v), path);
    }
    return static_cast<T>(v);
}

}  // namespace

template <>
bool Value::As<bool>() const {
    CheckNotMissing();
    if (*pos_ == 0xc3) return true;
    if (*pos_ == 0xc2) return false;
    ThrowTypeMismatch(kTypeBool);
}

template <>
int8_t Value::As<int8_t>() const {
    CheckNotMissing();
    return NarrowInt<int8_t>(ReadSignedInt(*this), GetPath());
}

template <>
int16_t Value::As<int16_t>() const {
    CheckNotMissing();
    return NarrowInt<int16_t>(ReadSignedInt(*this), GetPath());
}

template <>
int32_t Value::As<int32_t>() const {
    CheckNotMissing();
    return NarrowInt<int32_t>(ReadSignedInt(*this), GetPath());
}

template <>
int64_t Value::As<int64_t>() const {
    CheckNotMissing();
    return ReadSignedInt(*this);
}

template <>
uint8_t Value::As<uint8_t>() const {
    CheckNotMissing();
    return NarrowUInt<uint8_t>(ReadUnsignedInt(*this), GetPath());
}

template <>
uint16_t Value::As<uint16_t>() const {
    CheckNotMissing();
    return NarrowUInt<uint16_t>(ReadUnsignedInt(*this), GetPath());
}

template <>
uint32_t Value::As<uint32_t>() const {
    CheckNotMissing();
    return NarrowUInt<uint32_t>(ReadUnsignedInt(*this), GetPath());
}

template <>
uint64_t Value::As<uint64_t>() const {
    CheckNotMissing();
    return ReadUnsignedInt(*this);
}

template <>
float Value::As<float>() const {
    CheckNotMissing();
    return static_cast<float>(ReadDouble(*this));
}

template <>
double Value::As<double>() const {
    CheckNotMissing();
    return ReadDouble(*this);
}

template <>
std::string Value::As<std::string>() const {
    CheckNotMissing();
    if (!IsString()) ThrowTypeMismatch(kTypeStr);

    const uint8_t* p = pos_;
    const uint8_t  b = *p++;
    uint32_t len{};

    if ((b & 0xe0) == 0xa0) {
        len = b & 0x1f;
    } else if (b == 0xd9) {
        len = *p++;
    } else if (b == 0xda) {
        len = Read16(p); p += 2;
    } else {
        len = Read32(p); p += 4;
    }
    return std::string{reinterpret_cast<const char*>(p), len};
}

// ======================================================================== //
//  Tarantool ext type predicates and accessors                             //
// ======================================================================== //

bool Value::IsUuid()     const noexcept { return IsExt() && GetExtType() == 2; }
bool Value::IsDatetime() const noexcept { return IsExt() && GetExtType() == 4; }
bool Value::IsDecimal()  const noexcept { return IsExt() && GetExtType() == 1; }
bool Value::IsInterval() const noexcept { return IsExt() && GetExtType() == 6; }

TntUuid Value::AsUuid() const {
    CheckNotMissing();
    if (!IsUuid()) ThrowTypeMismatch(kTypeExt);
    const auto data = GetExtData();
    if (data.size() != 16) throw ParseException{"UUID ext data must be exactly 16 bytes"};
    TntUuid uuid;
    std::memcpy(uuid.bytes.data(), data.data(), 16);
    return uuid;
}

DatetimeTz Value::AsDatetimeTz() const {
    CheckNotMissing();
    if (!IsDatetime()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    const auto raw = DecodeExt4Bytes(
        reinterpret_cast<const uint8_t*>(sv.data()),
        static_cast<uint32_t>(sv.size()));
    if (raw.nsec != 0) {
        throw ConversionException{
            "datetime has sub-second part; use AsTimestampTz()", GetPath()};
    }
    DatetimeTz dt;
    dt.tp       = std::chrono::time_point<
                      std::chrono::system_clock, std::chrono::seconds>{
                      std::chrono::seconds{raw.seconds}};
    dt.tzoffset = raw.tzoffset;
    dt.tzindex  = raw.tzindex;
    return dt;
}

DatetimeWithoutTz Value::AsDatetimeWithoutTz() const {
    CheckNotMissing();
    if (!IsDatetime()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    const auto raw = DecodeExt4Bytes(
        reinterpret_cast<const uint8_t*>(sv.data()),
        static_cast<uint32_t>(sv.size()));
    if (raw.nsec != 0) {
        throw ConversionException{
            "datetime has sub-second part; use AsTimestampWithoutTz()", GetPath()};
    }
    if (raw.tzoffset != 0 || raw.tzindex != 0) {
        throw ConversionException{
            "datetime has timezone info; use AsDatetimeTz()", GetPath()};
    }
    DatetimeWithoutTz dt;
    dt.tp = std::chrono::time_point<
                std::chrono::system_clock, std::chrono::seconds>{
                std::chrono::seconds{raw.seconds}};
    return dt;
}

TimestampTz Value::AsTimestampTz() const {
    CheckNotMissing();
    if (!IsDatetime()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    const auto raw = DecodeExt4Bytes(
        reinterpret_cast<const uint8_t*>(sv.data()),
        static_cast<uint32_t>(sv.size()));
    TimestampTz ts;
    const int64_t total_ns =
        raw.seconds * 1'000'000'000LL + static_cast<int64_t>(raw.nsec);
    ts.tp = std::chrono::time_point<
                std::chrono::system_clock, std::chrono::nanoseconds>{
                std::chrono::nanoseconds{total_ns}};
    ts.tzoffset = raw.tzoffset;
    ts.tzindex  = raw.tzindex;
    return ts;
}

TimestampWithoutTz Value::AsTimestampWithoutTz() const {
    CheckNotMissing();
    if (!IsDatetime()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    const auto raw = DecodeExt4Bytes(
        reinterpret_cast<const uint8_t*>(sv.data()),
        static_cast<uint32_t>(sv.size()));
    if (raw.tzoffset != 0 || raw.tzindex != 0) {
        throw ConversionException{
            "datetime has timezone info; use AsTimestampTz()", GetPath()};
    }
    TimestampWithoutTz ts;
    const int64_t total_ns =
        raw.seconds * 1'000'000'000LL + static_cast<int64_t>(raw.nsec);
    ts.tp = std::chrono::time_point<
                std::chrono::system_clock, std::chrono::nanoseconds>{
                std::chrono::nanoseconds{total_ns}};
    return ts;
}

utils::datetime::Date Value::AsDate() const {
    CheckNotMissing();
    if (!IsDatetime()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    const auto raw = DecodeExt4Bytes(
        reinterpret_cast<const uint8_t*>(sv.data()),
        static_cast<uint32_t>(sv.size()));
    if (raw.nsec != 0) {
        throw ConversionException{
            "datetime has sub-second part; not a plain date", GetPath()};
    }
    if (raw.tzoffset != 0 || raw.tzindex != 0) {
        throw ConversionException{
            "datetime has timezone info; not a plain date", GetPath()};
    }
    if (raw.seconds % 86400 != 0) {
        throw ConversionException{
            "datetime is not midnight-UTC; use AsDatetimeWithoutTz()", GetPath()};
    }
    using Days    = utils::datetime::Date::Days;
    using SysDays = utils::datetime::Date::SysDays;
    return utils::datetime::Date{SysDays{Days{raw.seconds / 86400LL}}};
}

TntInterval Value::AsInterval() const {
    CheckNotMissing();
    if (!IsInterval()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    const auto* p   = reinterpret_cast<const uint8_t*>(sv.data());
    const auto* end = p + sv.size();

    auto read_uint = [&]() -> uint64_t {
        if (p >= end) throw ParseException{"Interval: unexpected end of data"};
        const uint8_t b = *p++;
        if (b <= 0x7f) return b;
        if (b == 0xcc) {
            if (p >= end) throw ParseException{"Interval: truncated uint8"};
            return *p++;
        }
        if (b == 0xcd) {
            if (p + 2 > end) throw ParseException{"Interval: truncated uint16"};
            const uint16_t v = (static_cast<uint16_t>(p[0]) << 8) | p[1];
            p += 2;
            return v;
        }
        if (b == 0xce) {
            if (p + 4 > end) throw ParseException{"Interval: truncated uint32"};
            const uint32_t v =
                (static_cast<uint32_t>(p[0]) << 24) |
                (static_cast<uint32_t>(p[1]) << 16) |
                (static_cast<uint32_t>(p[2]) <<  8) | p[3];
            p += 4;
            return v;
        }
        if (b == 0xcf) {
            if (p + 8 > end) throw ParseException{"Interval: truncated uint64"};
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v = (v << 8) | *p++;
            return v;
        }
        throw ParseException{fmt::format("Interval: unexpected byte 0x{:02x} in uint", b)};
    };

    auto read_int = [&]() -> int64_t {
        if (p >= end) throw ParseException{"Interval: unexpected end of data"};
        const uint8_t b = *p;
        if (b <= 0x7f) { ++p; return b; }
        if ((b & 0xe0) == 0xe0) { ++p; return static_cast<int64_t>(static_cast<int8_t>(b)); }
        if (b == 0xd0) {
            p += 2;
            if (p - 1 >= end) throw ParseException{"Interval: truncated int8"};
            return static_cast<int64_t>(static_cast<int8_t>(*(p - 1)));
        }
        if (b == 0xd1) {
            p += 3;
            if (p - 2 >= end) throw ParseException{"Interval: truncated int16"};
            const int16_t v = static_cast<int16_t>(
                (static_cast<uint16_t>(*(p-2)) << 8) | *(p-1));
            return static_cast<int64_t>(v);
        }
        if (b == 0xd2) {
            p += 5;
            if (p - 4 >= end) throw ParseException{"Interval: truncated int32"};
            const int32_t v = static_cast<int32_t>(
                (static_cast<uint32_t>(*(p-4)) << 24) |
                (static_cast<uint32_t>(*(p-3)) << 16) |
                (static_cast<uint32_t>(*(p-2)) <<  8) | *(p-1));
            return static_cast<int64_t>(v);
        }
        if (b == 0xd3) {
            p += 9;
            if (p - 8 >= end) throw ParseException{"Interval: truncated int64"};
            int64_t v = 0;
            for (int i = 8; i >= 1; --i)
                v = (v << 8) | static_cast<int64_t>(*(p - i));
            return v;
        }
        // Fall through to uint read for non-negative values
        return static_cast<int64_t>(read_uint());
    };

    const uint64_t count = read_uint();
    TntInterval iv;
    for (uint64_t i = 0; i < count; ++i) {
        const uint64_t field_id = read_uint();
        const int64_t  val      = read_int();
        switch (field_id) {
            case 0: iv.year       = val; break;
            case 1: iv.month      = val; break;
            case 2: iv.week       = val; break;
            case 3: iv.day        = val; break;
            case 4: iv.hour       = val; break;
            case 5: iv.minute     = val; break;
            case 6: iv.second     = val; break;
            case 7: iv.nanosecond = val; break;
            case 8: iv.adjust     = val; break;
            default: break;  // unknown field, skip
        }
    }
    return iv;
}

std::string Value::AsDecimalString() const {
    CheckNotMissing();
    if (!IsDecimal()) ThrowTypeMismatch(kTypeExt);
    const auto sv = GetExtData();
    return DecodeDecimalBytes(
        reinterpret_cast<const uint8_t*>(sv.data()),
        static_cast<uint32_t>(sv.size()));
}

// ---- As<T> specialisations for Tarantool types ----------------------------

template <> utils::datetime::Date Value::As<utils::datetime::Date>() const {
    return AsDate();
}
template <> DatetimeTz Value::As<DatetimeTz>() const {
    return AsDatetimeTz();
}
template <> DatetimeWithoutTz Value::As<DatetimeWithoutTz>() const {
    return AsDatetimeWithoutTz();
}
template <> TimestampTz Value::As<TimestampTz>() const {
    return AsTimestampTz();
}
template <> TimestampWithoutTz Value::As<TimestampWithoutTz>() const {
    return AsTimestampWithoutTz();
}
template <> TntUuid Value::As<TntUuid>() const {
    return AsUuid();
}
template <> TntInterval Value::As<TntInterval>() const {
    return AsInterval();
}

// ---- Parse() friends (non-template ADL hooks for built-in scalar types) ----
// These thin wrappers call the corresponding As<T>() explicit specialisation.

bool        Parse(const Value& v, formats::parse::To<bool>)        { return v.As<bool>(); }
int8_t      Parse(const Value& v, formats::parse::To<int8_t>)      { return v.As<int8_t>(); }
int16_t     Parse(const Value& v, formats::parse::To<int16_t>)     { return v.As<int16_t>(); }
int32_t     Parse(const Value& v, formats::parse::To<int32_t>)     { return v.As<int32_t>(); }
int64_t     Parse(const Value& v, formats::parse::To<int64_t>)     { return v.As<int64_t>(); }
uint8_t     Parse(const Value& v, formats::parse::To<uint8_t>)     { return v.As<uint8_t>(); }
uint16_t    Parse(const Value& v, formats::parse::To<uint16_t>)    { return v.As<uint16_t>(); }
uint32_t    Parse(const Value& v, formats::parse::To<uint32_t>)    { return v.As<uint32_t>(); }
uint64_t    Parse(const Value& v, formats::parse::To<uint64_t>)    { return v.As<uint64_t>(); }
float       Parse(const Value& v, formats::parse::To<float>)       { return v.As<float>(); }
double      Parse(const Value& v, formats::parse::To<double>)      { return v.As<double>(); }
std::string Parse(const Value& v, formats::parse::To<std::string>) { return v.As<std::string>(); }

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
