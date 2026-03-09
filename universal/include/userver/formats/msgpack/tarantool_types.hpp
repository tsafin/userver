#pragma once

/// @file userver/formats/msgpack/tarantool_types.hpp
/// @brief Tarantool-specific ext type representations for formats::msgpack.

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <userver/formats/msgpack/exception.hpp>
#include <userver/utils/datetime/date.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

// ---- TntUuid ---------------------------------------------------------------

struct TntUuid {
    std::array<uint8_t, 16> bytes{};

    std::string ToString() const;
    static TntUuid FromString(std::string_view s);
    bool operator==(const TntUuid& o) const noexcept { return bytes == o.bytes; }
    bool operator!=(const TntUuid& o) const noexcept { return !(*this == o); }
};

// ---- Internal raw fields decoded from ext-4 bytes --------------------------
// Not part of the public API; used inside value.cpp and tarantool_types.cpp.
struct DatetimeRaw {
    int64_t  seconds   = 0;
    uint32_t nsec      = 0;
    int16_t  tzoffset  = 0;
    uint16_t tzindex   = 0;
};

// ---- Datetime types (ext type 4) -------------------------------------------

/// Point in time, second precision, with explicit timezone fields.
struct DatetimeTz {
    std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp{};
    int16_t  tzoffset = 0;
    uint16_t tzindex  = 0;

    bool operator==(const DatetimeTz& o) const noexcept {
        return tp == o.tp && tzoffset == o.tzoffset && tzindex == o.tzindex;
    }
    bool operator!=(const DatetimeTz& o) const noexcept { return !(*this == o); }
};

/// Point in time, second precision, without timezone (tzoffset==tzindex==0).
struct DatetimeWithoutTz {
    std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp{};

    bool operator==(const DatetimeWithoutTz& o) const noexcept {
        return tp == o.tp;
    }
    bool operator!=(const DatetimeWithoutTz& o) const noexcept { return !(*this == o); }
};

/// Point in time, nanosecond precision, with explicit timezone fields.
struct TimestampTz {
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> tp{};
    int16_t  tzoffset = 0;
    uint16_t tzindex  = 0;

    bool operator==(const TimestampTz& o) const noexcept {
        return tp == o.tp && tzoffset == o.tzoffset && tzindex == o.tzindex;
    }
    bool operator!=(const TimestampTz& o) const noexcept { return !(*this == o); }
};

/// Point in time, nanosecond precision, without timezone.
struct TimestampWithoutTz {
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds> tp{};

    bool operator==(const TimestampWithoutTz& o) const noexcept {
        return tp == o.tp;
    }
    bool operator!=(const TimestampWithoutTz& o) const noexcept { return !(*this == o); }
};

// ---- TntInterval (ext type 6) ----------------------------------------------

struct TntInterval {
    int64_t year       = 0;
    int64_t month      = 0;
    int64_t week       = 0;
    int64_t day        = 0;
    int64_t hour       = 0;
    int64_t minute     = 0;
    int64_t second     = 0;
    int64_t nanosecond = 0;
    int64_t adjust     = 0;  // 0=DT_EXCESS, 1=DT_LIMIT, 2=DT_SNAP

    /// Convert to nanoseconds (calendar-free fields only).
    /// @throws ConversionException if year/month/week are non-zero.
    std::chrono::nanoseconds ToNanoseconds() const;

    bool operator==(const TntInterval& o) const noexcept;
    bool operator!=(const TntInterval& o) const noexcept { return !(*this == o); }
};

// ---- Encode helpers (used by ValueBuilder) ---------------------------------

namespace impl {

inline void WriteLE32(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>(v));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 24));
}

inline void WriteLE64(std::vector<uint8_t>& out, uint64_t v) {
    WriteLE32(out, static_cast<uint32_t>(v));
    WriteLE32(out, static_cast<uint32_t>(v >> 32));
}

inline void EncodeExt4Raw(std::vector<uint8_t>& out, const DatetimeRaw& r) {
    const bool has_extra = (r.nsec != 0 || r.tzoffset != 0 || r.tzindex != 0);
    if (has_extra) {
        out.push_back(0xd8);  // fixext16
        out.push_back(0x04);  // MP_DATETIME
        WriteLE64(out, static_cast<uint64_t>(r.seconds));
        WriteLE32(out, r.nsec);
        out.push_back(static_cast<uint8_t>(static_cast<uint16_t>(r.tzoffset)));
        out.push_back(static_cast<uint8_t>(static_cast<uint16_t>(r.tzoffset) >> 8));
        out.push_back(static_cast<uint8_t>(r.tzindex));
        out.push_back(static_cast<uint8_t>(r.tzindex >> 8));
    } else {
        out.push_back(0xd7);  // fixext8
        out.push_back(0x04);  // MP_DATETIME
        WriteLE64(out, static_cast<uint64_t>(r.seconds));
    }
}

}  // namespace impl

// Encode functions (non-inline, defined in tarantool_types.cpp).
std::vector<uint8_t> EncodeUuid(const TntUuid& uuid);
std::vector<uint8_t> EncodeDate(utils::datetime::Date date);
std::vector<uint8_t> EncodeDatetimeTz(const DatetimeTz& dt);
std::vector<uint8_t> EncodeDatetimeWithoutTz(DatetimeWithoutTz dt);
std::vector<uint8_t> EncodeTimestampTz(const TimestampTz& ts);
std::vector<uint8_t> EncodeTimestampWithoutTz(TimestampWithoutTz ts);
std::vector<uint8_t> EncodeInterval(const TntInterval& iv);

// Decode helpers (defined in tarantool_types.cpp).
DatetimeRaw DecodeExt4Bytes(const uint8_t* data, uint32_t len);

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
