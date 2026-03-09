#include <userver/formats/msgpack/tarantool_types.hpp>

#include <cstring>

#include <fmt/format.h>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

namespace {

// Compact msgpack uint encoder (used for interval field IDs and counts).
void EncodeUIntMP(std::vector<uint8_t>& out, uint64_t v) {
    if (v <= 0x7f) {
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xff) {
        out.push_back(0xcc);
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xffff) {
        out.push_back(0xcd);
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xffffffff) {
        out.push_back(0xce);
        out.push_back(static_cast<uint8_t>(v >> 24));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    } else {
        out.push_back(0xcf);
        for (int s = 56; s >= 0; s -= 8)
            out.push_back(static_cast<uint8_t>(v >> s));
    }
}

// Compact msgpack int encoder (used for interval field values).
void EncodeIntMP(std::vector<uint8_t>& out, int64_t v) {
    if (v >= 0) {
        EncodeUIntMP(out, static_cast<uint64_t>(v));
    } else if (v >= -32) {
        out.push_back(static_cast<uint8_t>(v));  // negative fixint
    } else if (v >= -128) {
        out.push_back(0xd0);
        out.push_back(static_cast<uint8_t>(static_cast<int8_t>(v)));
    } else if (v >= -32768) {
        out.push_back(0xd1);
        out.push_back(static_cast<uint8_t>(static_cast<uint16_t>(static_cast<int16_t>(v)) >> 8));
        out.push_back(static_cast<uint8_t>(static_cast<int16_t>(v)));
    } else if (v >= -2147483648LL) {
        out.push_back(0xd2);
        auto u = static_cast<uint32_t>(static_cast<int32_t>(v));
        out.push_back(static_cast<uint8_t>(u >> 24));
        out.push_back(static_cast<uint8_t>(u >> 16));
        out.push_back(static_cast<uint8_t>(u >> 8));
        out.push_back(static_cast<uint8_t>(u));
    } else {
        out.push_back(0xd3);
        auto u = static_cast<uint64_t>(v);
        for (int s = 56; s >= 0; s -= 8)
            out.push_back(static_cast<uint8_t>(u >> s));
    }
}

}  // namespace

// ---- TntUuid ---------------------------------------------------------------

std::string TntUuid::ToString() const {
    const auto& b = bytes;
    return fmt::format(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}"
        "-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3],
        b[4], b[5],
        b[6], b[7],
        b[8], b[9],
        b[10], b[11], b[12], b[13], b[14], b[15]);
}

TntUuid TntUuid::FromString(std::string_view s) {
    if (s.size() != 36 ||
        s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-') {
        throw ParseException{fmt::format("Invalid UUID string: '{}'", s)};
    }
    TntUuid uuid;
    int out_idx = 0;
    for (int i = 0; i < 36; ++i) {
        if (s[i] == '-') continue;
        auto hexval = [&](char c) -> uint8_t {
            if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
            if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
            if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(c - 'A' + 10);
            throw ParseException{fmt::format("Invalid UUID hex char: '{}'", c)};
        };
        uint8_t hi = hexval(s[i]);
        if (i + 1 >= 36) throw ParseException{"UUID string truncated"};
        uint8_t lo = hexval(s[i + 1]);
        uuid.bytes[out_idx++] = static_cast<uint8_t>((hi << 4) | lo);
        ++i;
    }
    if (out_idx != 16) throw ParseException{"UUID string has wrong number of hex digits"};
    return uuid;
}

// ---- TntInterval -----------------------------------------------------------

std::chrono::nanoseconds TntInterval::ToNanoseconds() const {
    if (year != 0 || month != 0 || week != 0) {
        throw ConversionException{
            "Cannot convert interval with calendar fields (year/month/week) to nanoseconds",
            "/"};
    }
    const int64_t total_ns =
        day    * 86400LL * 1'000'000'000LL +
        hour   *  3600LL * 1'000'000'000LL +
        minute *    60LL * 1'000'000'000LL +
        second *         1'000'000'000LL +
        nanosecond;
    return std::chrono::nanoseconds{total_ns};
}

bool TntInterval::operator==(const TntInterval& o) const noexcept {
    return year       == o.year       &&
           month      == o.month      &&
           week       == o.week       &&
           day        == o.day        &&
           hour       == o.hour       &&
           minute     == o.minute     &&
           second     == o.second     &&
           nanosecond == o.nanosecond &&
           adjust     == o.adjust;
}

// ---- Encode functions ------------------------------------------------------

std::vector<uint8_t> EncodeUuid(const TntUuid& uuid) {
    std::vector<uint8_t> out;
    out.push_back(0xd8);  // fixext16
    out.push_back(0x02);  // ext type 2 = UUID
    out.insert(out.end(), uuid.bytes.begin(), uuid.bytes.end());
    return out;
}

std::vector<uint8_t> EncodeDate(utils::datetime::Date date) {
    using Days = utils::datetime::Date::Days;
    const int64_t day_count = date.GetSysDays().time_since_epoch().count();
    DatetimeRaw r;
    r.seconds = day_count * 86400LL;
    std::vector<uint8_t> out;
    impl::EncodeExt4Raw(out, r);
    return out;
}

std::vector<uint8_t> EncodeDatetimeTz(const DatetimeTz& dt) {
    DatetimeRaw r;
    r.seconds  = dt.tp.time_since_epoch().count();
    r.nsec     = 0;
    r.tzoffset = dt.tzoffset;
    r.tzindex  = dt.tzindex;
    std::vector<uint8_t> out;
    impl::EncodeExt4Raw(out, r);
    return out;
}

std::vector<uint8_t> EncodeDatetimeWithoutTz(DatetimeWithoutTz dt) {
    DatetimeRaw r;
    r.seconds  = dt.tp.time_since_epoch().count();
    r.nsec     = 0;
    r.tzoffset = 0;
    r.tzindex  = 0;
    std::vector<uint8_t> out;
    impl::EncodeExt4Raw(out, r);
    return out;
}

std::vector<uint8_t> EncodeTimestampTz(const TimestampTz& ts) {
    const int64_t total_ns = ts.tp.time_since_epoch().count();
    int64_t seconds = total_ns / 1'000'000'000LL;
    int64_t nsec_i  = total_ns % 1'000'000'000LL;
    if (nsec_i < 0) {
        nsec_i += 1'000'000'000LL;
        --seconds;
    }
    DatetimeRaw r;
    r.seconds  = seconds;
    r.nsec     = static_cast<uint32_t>(nsec_i);
    r.tzoffset = ts.tzoffset;
    r.tzindex  = ts.tzindex;
    std::vector<uint8_t> out;
    impl::EncodeExt4Raw(out, r);
    return out;
}

std::vector<uint8_t> EncodeTimestampWithoutTz(TimestampWithoutTz ts) {
    const int64_t total_ns = ts.tp.time_since_epoch().count();
    int64_t seconds = total_ns / 1'000'000'000LL;
    int64_t nsec_i  = total_ns % 1'000'000'000LL;
    if (nsec_i < 0) {
        nsec_i += 1'000'000'000LL;
        --seconds;
    }
    DatetimeRaw r;
    r.seconds  = seconds;
    r.nsec     = static_cast<uint32_t>(nsec_i);
    r.tzoffset = 0;
    r.tzindex  = 0;
    std::vector<uint8_t> out;
    impl::EncodeExt4Raw(out, r);
    return out;
}

std::vector<uint8_t> EncodeInterval(const TntInterval& iv) {
    // Collect non-zero fields: (field_id, value)
    struct Field { uint64_t id; int64_t val; };
    const Field fields[9] = {
        {0, iv.year},
        {1, iv.month},
        {2, iv.week},
        {3, iv.day},
        {4, iv.hour},
        {5, iv.minute},
        {6, iv.second},
        {7, iv.nanosecond},
        {8, iv.adjust},
    };

    // Build data payload: count + (id, val) pairs for non-zero fields
    std::vector<uint8_t> data;
    uint64_t count = 0;
    for (const auto& f : fields) {
        if (f.val != 0) ++count;
    }
    EncodeUIntMP(data, count);
    for (const auto& f : fields) {
        if (f.val != 0) {
            EncodeUIntMP(data, f.id);
            EncodeIntMP(data, f.val);
        }
    }

    // Wrap in ext8: 0xc7 <data_len> <type=6> <data...>
    std::vector<uint8_t> out;
    out.push_back(0xc7);  // ext8
    out.push_back(static_cast<uint8_t>(data.size()));
    out.push_back(0x06);  // ext type 6 = interval
    out.insert(out.end(), data.begin(), data.end());
    return out;
}

// ---- DecodeExt4Bytes -------------------------------------------------------

DatetimeRaw DecodeExt4Bytes(const uint8_t* data, uint32_t len) {
    if (len != 8 && len != 16) {
        throw ParseException{fmt::format("Bad datetime ext size: {}", len)};
    }

    auto read_le64 = [](const uint8_t* p) -> uint64_t {
        return static_cast<uint64_t>(p[0]) |
               (static_cast<uint64_t>(p[1]) << 8)  |
               (static_cast<uint64_t>(p[2]) << 16) |
               (static_cast<uint64_t>(p[3]) << 24) |
               (static_cast<uint64_t>(p[4]) << 32) |
               (static_cast<uint64_t>(p[5]) << 40) |
               (static_cast<uint64_t>(p[6]) << 48) |
               (static_cast<uint64_t>(p[7]) << 56);
    };
    auto read_le32 = [](const uint8_t* p) -> uint32_t {
        return static_cast<uint32_t>(p[0]) |
               (static_cast<uint32_t>(p[1]) << 8)  |
               (static_cast<uint32_t>(p[2]) << 16) |
               (static_cast<uint32_t>(p[3]) << 24);
    };

    DatetimeRaw r;
    r.seconds = static_cast<int64_t>(read_le64(data));
    if (len == 16) {
        r.nsec     = read_le32(data + 8);
        uint16_t raw_tz = static_cast<uint16_t>(data[12]) |
                          (static_cast<uint16_t>(data[13]) << 8);
        r.tzoffset = static_cast<int16_t>(raw_tz);
        r.tzindex  = static_cast<uint16_t>(data[14]) |
                     (static_cast<uint16_t>(data[15]) << 8);
    }
    return r;
}

}  // namespace formats::msgpack

// ---- DecodeDecimalBytes -------------------------------------------------------

namespace formats::msgpack {

std::string DecodeDecimalBytes(const uint8_t* data, uint32_t len) {
    const uint8_t* p   = data;
    const uint8_t* end = data + len;

    if (p >= end) throw ParseException{"Decimal: empty ext data"};

    // Read scale as msgpack uint/int (big-endian, per Tarantool spec)
    int64_t scale = 0;
    {
        const uint8_t b = *p++;
        if (b <= 0x7f) {
            scale = static_cast<int64_t>(b);
        } else if (b == 0xcc) {
            if (p >= end) throw ParseException{"Decimal: truncated scale"};
            scale = static_cast<int64_t>(*p++);
        } else if (b == 0xcd) {
            if (p + 2 > end) throw ParseException{"Decimal: truncated scale"};
            scale = static_cast<int64_t>(
                (static_cast<uint16_t>(p[0]) << 8) | p[1]);
            p += 2;
        } else if (b == 0xd0) {
            if (p >= end) throw ParseException{"Decimal: truncated scale"};
            scale = static_cast<int64_t>(static_cast<int8_t>(*p++));
        } else if (b == 0xd1) {
            if (p + 2 > end) throw ParseException{"Decimal: truncated scale"};
            scale = static_cast<int64_t>(static_cast<int16_t>(
                (static_cast<uint16_t>(p[0]) << 8) | p[1]));
            p += 2;
        } else {
            throw ParseException{fmt::format(
                "Decimal: unexpected scale byte 0x{:02x}", b)};
        }
    }

    // Parse BCD nibbles; last nibble is sign
    std::vector<uint8_t> digits;
    bool negative = false;

    while (p < end) {
        const uint8_t byte = *p++;
        const uint8_t hi   = (byte >> 4) & 0x0f;
        const uint8_t lo   = byte & 0x0f;

        if (p >= end) {
            // Last byte: lo nibble is sign
            digits.push_back(hi);
            negative = (lo == 0x0b || lo == 0x0d);
        } else {
            digits.push_back(hi);
            digits.push_back(lo);
        }
    }

    if (digits.empty()) return "0";

    // Build decimal string
    std::string all;
    all.reserve(digits.size());
    for (auto d : digits) all += static_cast<char>('0' + d);

    std::string result;
    const auto all_len = static_cast<int64_t>(all.size());

    if (scale <= 0) {
        const std::size_t nz = all.find_first_not_of('0');
        result = (nz == std::string::npos) ? "0" : all.substr(nz);
        if (scale < 0) result += std::string(static_cast<std::size_t>(-scale), '0');
    } else if (scale >= all_len) {
        result = "0.";
        result += std::string(static_cast<std::size_t>(scale - all_len), '0');
        result += all;
    } else {
        const std::size_t dot_pos = static_cast<std::size_t>(all_len - scale);
        std::string int_part  = all.substr(0, dot_pos);
        const std::string frac_part = all.substr(dot_pos);
        const std::size_t nz = int_part.find_first_not_of('0');
        int_part = (nz == std::string::npos) ? "0" : int_part.substr(nz);
        result = int_part + "." + frac_part;
    }

    if (negative && result != "0") result = "-" + result;
    return result;
}

}  // namespace formats::msgpack
USERVER_NAMESPACE_END
