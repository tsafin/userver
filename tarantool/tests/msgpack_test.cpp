#include <userver/utest/utest.hpp>

#include <storages/tarantool/impl/msgpack.hpp>

USERVER_NAMESPACE_BEGIN

using namespace storages::tarantool::impl;

// Helper: encode a JSON value and return raw bytes
static std::vector<uint8_t> Encode(const formats::json::Value& v) {
    std::vector<uint8_t> buf;
    EncodeJson(buf, v);
    return buf;
}

// Helper: round-trip a JSON value through encode+decode
static formats::json::Value RoundTrip(const formats::json::Value& v) {
    return MsgPackDecode(Encode(v));
}

// ---- EncodeUint / positive integers ----

TEST(MsgPackEncode, PositiveFixint) {
    std::vector<uint8_t> buf;
    EncodeUint(buf, 0);
    EXPECT_EQ(buf, (std::vector<uint8_t>{0x00}));

    buf.clear();
    EncodeUint(buf, 127);
    EXPECT_EQ(buf, (std::vector<uint8_t>{0x7f}));
}

TEST(MsgPackEncode, Uint8) {
    std::vector<uint8_t> buf;
    EncodeUint(buf, 128);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kUint8, 0x80}));

    buf.clear();
    EncodeUint(buf, 255);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kUint8, 0xFF}));
}

TEST(MsgPackEncode, Uint16) {
    std::vector<uint8_t> buf;
    EncodeUint(buf, 256);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kUint16, 0x01, 0x00}));

    buf.clear();
    EncodeUint(buf, 0xABCD);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kUint16, 0xAB, 0xCD}));
}

TEST(MsgPackEncode, Uint32) {
    std::vector<uint8_t> buf;
    EncodeUint(buf, 0x10000);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kUint32, 0x00, 0x01, 0x00, 0x00}));

    buf.clear();
    EncodeUint(buf, 0xDEADBEEF);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kUint32, 0xDE, 0xAD, 0xBE, 0xEF}));
}

TEST(MsgPackEncode, Uint64) {
    std::vector<uint8_t> buf;
    EncodeUint(buf, 0x100000000ULL);
    EXPECT_EQ(buf, (std::vector<uint8_t>{
        mp::kUint64, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}));
}

// ---- EncodeStr ----

TEST(MsgPackEncode, FixStr) {
    std::vector<uint8_t> buf;
    EncodeStr(buf, "hi");
    // fixstr: 0xa0 | 2, then 'h', 'i'
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xa2, 'h', 'i'}));

    buf.clear();
    EncodeStr(buf, "");
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xa0}));
}

TEST(MsgPackEncode, Str8) {
    // 32-byte string -> str8
    std::string s(32, 'x');
    std::vector<uint8_t> buf;
    EncodeStr(buf, s);
    EXPECT_EQ(buf[0], mp::kStr8);
    EXPECT_EQ(buf[1], 32);
    EXPECT_EQ(buf.size(), 34u);
}

// ---- EncodeArray ----

TEST(MsgPackEncode, FixArray) {
    std::vector<uint8_t> buf;
    EncodeArray(buf, 0);
    EXPECT_EQ(buf, (std::vector<uint8_t>{0x90}));

    buf.clear();
    EncodeArray(buf, 3);
    EXPECT_EQ(buf, (std::vector<uint8_t>{0x93}));
}

TEST(MsgPackEncode, Array16) {
    std::vector<uint8_t> buf;
    EncodeArray(buf, 16);
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kArray16, 0x00, 0x10}));
}

// ---- EncodeJson scalars ----

TEST(MsgPackEncodeJson, Null) {
    auto buf = Encode(formats::json::Value{});
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kNil}));
}

TEST(MsgPackEncodeJson, BoolTrue) {
    auto buf = Encode(formats::json::ValueBuilder{true}.ExtractValue());
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kTrue}));
}

TEST(MsgPackEncodeJson, BoolFalse) {
    auto buf = Encode(formats::json::ValueBuilder{false}.ExtractValue());
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kFalse}));
}

TEST(MsgPackEncodeJson, NegativeFixint) {
    // -1 -> negative fixint 0xff
    auto buf = Encode(formats::json::ValueBuilder{int64_t{-1}}.ExtractValue());
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xff}));

    // -32 -> negative fixint 0xe0
    buf = Encode(formats::json::ValueBuilder{int64_t{-32}}.ExtractValue());
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xe0}));
}

TEST(MsgPackEncodeJson, NegativeInt8) {
    auto buf = Encode(formats::json::ValueBuilder{int64_t{-33}}.ExtractValue());
    EXPECT_EQ(buf[0], mp::kInt8);
    EXPECT_EQ(buf.size(), 2u);
}

TEST(MsgPackEncodeJson, String) {
    auto buf = Encode(formats::json::ValueBuilder{std::string{"abc"}}.ExtractValue());
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xa3, 'a', 'b', 'c'}));
}

// ---- DecodeValue: scalar round-trips ----

TEST(MsgPackRoundTrip, Zero) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{0}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), 0);
}

TEST(MsgPackRoundTrip, PositiveFixint) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{42}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), 42);
}

TEST(MsgPackRoundTrip, Uint255) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{255}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), 255);
}

TEST(MsgPackRoundTrip, Uint65535) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{65535}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), 65535);
}

TEST(MsgPackRoundTrip, LargeUint) {
    auto v = RoundTrip(formats::json::ValueBuilder{uint64_t{0xDEADBEEFULL}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), static_cast<int64_t>(0xDEADBEEF));
}

TEST(MsgPackRoundTrip, NegativeFixint) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{-1}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), -1);
}

TEST(MsgPackRoundTrip, NegativeInt8) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{-100}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), -100);
}

TEST(MsgPackRoundTrip, NegativeInt16) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{-1000}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), -1000);
}

TEST(MsgPackRoundTrip, NegativeInt32) {
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{-100000}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), -100000);
}

TEST(MsgPackRoundTrip, NegativeInt64) {
    constexpr int64_t kVal = -(1LL << 40);
    auto v = RoundTrip(formats::json::ValueBuilder{kVal}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), kVal);
}

TEST(MsgPackRoundTrip, BoolTrue) {
    auto v = RoundTrip(formats::json::ValueBuilder{true}.ExtractValue());
    EXPECT_TRUE(v.As<bool>());
}

TEST(MsgPackRoundTrip, BoolFalse) {
    auto v = RoundTrip(formats::json::ValueBuilder{false}.ExtractValue());
    EXPECT_FALSE(v.As<bool>());
}

TEST(MsgPackRoundTrip, Null) {
    auto v = RoundTrip(formats::json::Value{});
    EXPECT_TRUE(v.IsNull());
}

TEST(MsgPackRoundTrip, EmptyString) {
    auto v = RoundTrip(formats::json::ValueBuilder{std::string{}}.ExtractValue());
    EXPECT_EQ(v.As<std::string>(), "");
}

TEST(MsgPackRoundTrip, ShortString) {
    auto v = RoundTrip(formats::json::ValueBuilder{std::string{"hello"}}.ExtractValue());
    EXPECT_EQ(v.As<std::string>(), "hello");
}

TEST(MsgPackRoundTrip, LongString) {
    // 256-char string -> str16
    std::string s(256, 'z');
    auto v = RoundTrip(formats::json::ValueBuilder{s}.ExtractValue());
    EXPECT_EQ(v.As<std::string>(), s);
}

TEST(MsgPackRoundTrip, Float64) {
    auto v = RoundTrip(formats::json::ValueBuilder{3.14}.ExtractValue());
    EXPECT_DOUBLE_EQ(v.As<double>(), 3.14);
}

// ---- Array round-trips ----

TEST(MsgPackRoundTrip, EmptyArray) {
    formats::json::ValueBuilder b(formats::json::Type::kArray);
    auto v = RoundTrip(b.ExtractValue());
    EXPECT_TRUE(v.IsArray());
    EXPECT_EQ(v.GetSize(), 0u);
}

TEST(MsgPackRoundTrip, MixedArray) {
    formats::json::ValueBuilder b(formats::json::Type::kArray);
    b.PushBack(int64_t{1});
    b.PushBack(std::string{"two"});
    b.PushBack(true);
    auto v = RoundTrip(b.ExtractValue());
    ASSERT_TRUE(v.IsArray());
    ASSERT_EQ(v.GetSize(), 3u);
    EXPECT_EQ(v[0].As<int64_t>(), 1);
    EXPECT_EQ(v[1].As<std::string>(), "two");
    EXPECT_TRUE(v[2].As<bool>());
}

TEST(MsgPackRoundTrip, NestedArray) {
    formats::json::ValueBuilder inner(formats::json::Type::kArray);
    inner.PushBack(int64_t{7});
    formats::json::ValueBuilder outer(formats::json::Type::kArray);
    outer.PushBack(inner.ExtractValue());
    auto v = RoundTrip(outer.ExtractValue());
    ASSERT_TRUE(v.IsArray());
    ASSERT_EQ(v.GetSize(), 1u);
    ASSERT_TRUE(v[0].IsArray());
    EXPECT_EQ(v[0][0].As<int64_t>(), 7);
}

// ---- Map round-trips ----

TEST(MsgPackRoundTrip, StringKeyMap) {
    formats::json::ValueBuilder b(formats::json::Type::kObject);
    b["name"] = std::string{"tarantool"};
    b["version"] = int64_t{3};
    auto v = RoundTrip(b.ExtractValue());
    ASSERT_TRUE(v.IsObject());
    EXPECT_EQ(v["name"].As<std::string>(), "tarantool");
    EXPECT_EQ(v["version"].As<int64_t>(), 3);
}

// ---- DecodePreheaderLength ----

TEST(MsgPackDecode, PreheaderLength) {
    // 0xce + big-endian uint32 = 0x0000000A (10)
    std::array<uint8_t, 5> buf = {0xce, 0x00, 0x00, 0x00, 0x0A};
    EXPECT_EQ(DecodePreheaderLength(buf.data()), 10u);

    // Large value
    std::array<uint8_t, 5> buf2 = {0xce, 0xDE, 0xAD, 0xBE, 0xEF};
    EXPECT_EQ(DecodePreheaderLength(buf2.data()), 0xDEADBEEFu);
}

TEST(MsgPackDecode, PreheaderBadMarker) {
    std::array<uint8_t, 5> buf = {0x00, 0x00, 0x00, 0x00, 0x05};
    EXPECT_THROW(DecodePreheaderLength(buf.data()),
                 storages::tarantool::TarantoolException);
}

// ---- Overrun protection ----

TEST(MsgPackDecode, BufferOverrunThrows) {
    // Just a uint16 marker with no data following
    std::vector<uint8_t> buf = {mp::kUint16};
    EXPECT_THROW(MsgPackDecode(buf), storages::tarantool::TarantoolException);
}

TEST(MsgPackDecode, UnknownByteThrows) {
    std::vector<uint8_t> buf = {0xc1};  // reserved/undefined in MsgPack
    EXPECT_THROW(MsgPackDecode(buf), storages::tarantool::TarantoolException);
}

// ---- UUID (ext type 2) ----

// Wire bytes from Tarantool docs example:
// d8 02 f6 42 3b df b4 9e 49 13 b3 61 07 40 c9 70 2e 4b
// -> UUID "f6423bdf-b49e-4913-b361-0740c9702e4b"
TEST(MsgPackUuid, DecodeFromRealWireBytes) {
    std::vector<uint8_t> buf = {
        0xd8, 0x02,
        0xf6, 0x42, 0x3b, 0xdf,
        0xb4, 0x9e,
        0x49, 0x13,
        0xb3, 0x61,
        0x07, 0x40, 0xc9, 0x70, 0x2e, 0x4b
    };
    auto v = MsgPackDecode(buf);
    ASSERT_TRUE(v.IsString());
    EXPECT_EQ(v.As<std::string>(), "f6423bdf-b49e-4913-b361-0740c9702e4b");
}

TEST(MsgPackUuid, EncodeBytes) {
    TntUuid uuid;
    uuid.bytes = {0xf6,0x42,0x3b,0xdf, 0xb4,0x9e, 0x49,0x13,
                  0xb3,0x61, 0x07,0x40,0xc9,0x70,0x2e,0x4b};
    std::vector<uint8_t> buf;
    EncodeUuid(buf, uuid);
    ASSERT_EQ(buf.size(), 18u);
    EXPECT_EQ(buf[0], 0xd8);   // fixext16
    EXPECT_EQ(buf[1], 0x02);   // ext type UUID
    EXPECT_EQ(std::vector<uint8_t>(buf.begin()+2, buf.end()),
              std::vector<uint8_t>(uuid.bytes.begin(), uuid.bytes.end()));
}

TEST(MsgPackUuid, UuidFromStringRoundTrip) {
    const std::string s = "f6423bdf-b49e-4913-b361-0740c9702e4b";
    const TntUuid uuid = UuidFromString(s);
    EXPECT_EQ(UuidToString(uuid), s);
}

TEST(MsgPackUuid, UuidFromStringAllZeros) {
    const TntUuid uuid = UuidFromString("00000000-0000-0000-0000-000000000000");
    for (auto b : uuid.bytes) EXPECT_EQ(b, 0);
    EXPECT_EQ(UuidToString(uuid), "00000000-0000-0000-0000-000000000000");
}

TEST(MsgPackUuid, UuidFromStringBadInput) {
    EXPECT_THROW(UuidFromString("not-a-uuid"),
                 storages::tarantool::TarantoolException);
    EXPECT_THROW(UuidFromString("f6423bdf-b49e-4913-b361-0740c9702e4bXX"),
                 storages::tarantool::TarantoolException);
}

TEST(MsgPackUuid, EncodeDecodeRoundTrip) {
    const std::string s = "12345678-1234-5678-1234-567812345678";
    TntUuid uuid = UuidFromString(s);
    std::vector<uint8_t> buf;
    EncodeUuid(buf, uuid);
    auto v = MsgPackDecode(buf);
    ASSERT_TRUE(v.IsString());
    EXPECT_EQ(v.As<std::string>(), s);
}

TEST(MsgPackUuid, DecodeOverrunThrows) {
    // fixext16 + type byte, but only 10 bytes of data instead of 16
    std::vector<uint8_t> buf = {0xd8, 0x02,
        0x01,0x02,0x03,0x04, 0x05,0x06, 0x07,0x08, 0x09,0x0a};
    EXPECT_THROW(MsgPackDecode(buf), storages::tarantool::TarantoolException);
}

// ---- Datetime (ext type 4) ----
// TntDatetime is now an alias for formats::msgpack::TimestampTz,
// which stores a chrono::time_point<system_clock, nanoseconds> + tzoffset/tzindex.

namespace {
// Helper: seconds since epoch → nanosecond time_point
auto TpFromSec(int64_t sec) {
    return std::chrono::time_point_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::from_time_t(static_cast<std::time_t>(sec)));
}
int64_t TpToSec(const TntDatetime& dt) {
    return std::chrono::duration_cast<std::chrono::seconds>(
               dt.tp.time_since_epoch()).count();
}
int64_t TpToNsec(const TntDatetime& dt) {
    return dt.tp.time_since_epoch().count() % 1'000'000'000LL;
}
} // namespace

TEST(MsgPackDatetime, EncodeDecodeSecondsOnly) {
    TntDatetime dt;
    dt.tp = TpFromSec(1672531200);  // 2023-01-01 00:00:00 UTC
    std::vector<uint8_t> buf;
    EncodeDateTime(buf, dt);
    ASSERT_EQ(buf.size(), 10u);   // fixext8 marker + type + 8 bytes
    EXPECT_EQ(buf[0], 0xd7);     // fixext8
    EXPECT_EQ(buf[1], 0x04);     // ext type DATETIME

    auto v = MsgPackDecode(buf);
    ASSERT_TRUE(v.IsObject());
    EXPECT_EQ(v["seconds"].As<int64_t>(), 1672531200);
    EXPECT_EQ(v["nsec"].As<int64_t>(), 0);
    EXPECT_EQ(v["tzoffset"].As<int64_t>(), 0);
    EXPECT_EQ(v["tzindex"].As<int64_t>(), 0);
}

TEST(MsgPackDatetime, EncodeDecodeWithNsec) {
    TntDatetime dt;
    dt.tp = TpFromSec(1672531200) + std::chrono::nanoseconds{123456789};
    std::vector<uint8_t> buf;
    EncodeDateTime(buf, dt);
    ASSERT_EQ(buf.size(), 18u);  // fixext16 marker + type + 16 bytes

    auto v = MsgPackDecode(buf);
    EXPECT_EQ(v["seconds"].As<int64_t>(), 1672531200);
    EXPECT_EQ(v["nsec"].As<int64_t>(),    123456789);
    EXPECT_EQ(v["tzoffset"].As<int64_t>(), 0);
}

TEST(MsgPackDatetime, EncodeDecodeWithTimezone) {
    TntDatetime dt;
    dt.tp       = TpFromSec(0);
    dt.tzoffset = 180;   // UTC+3
    dt.tzindex  = 42;
    std::vector<uint8_t> buf;
    EncodeDateTime(buf, dt);
    ASSERT_EQ(buf.size(), 18u);

    auto v = MsgPackDecode(buf);
    EXPECT_EQ(v["seconds"].As<int64_t>(),  0);
    EXPECT_EQ(v["tzoffset"].As<int64_t>(), 180);
    EXPECT_EQ(v["tzindex"].As<int64_t>(),  42);
}

TEST(MsgPackDatetime, NegativeSeconds) {
    TntDatetime dt;
    dt.tp = TpFromSec(-86400);  // 1 day before epoch
    std::vector<uint8_t> buf;
    EncodeDateTime(buf, dt);
    auto v = MsgPackDecode(buf);
    EXPECT_EQ(v["seconds"].As<int64_t>(), -86400);
}

TEST(MsgPackDatetime, NegativeTzoffset) {
    TntDatetime dt;
    dt.tp       = TpFromSec(1000000000) + std::chrono::nanoseconds{500000000};
    dt.tzoffset = -300;  // UTC-5
    std::vector<uint8_t> buf;
    EncodeDateTime(buf, dt);
    auto v = MsgPackDecode(buf);
    EXPECT_EQ(v["seconds"].As<int64_t>(),  1000000000);
    EXPECT_EQ(v["nsec"].As<int64_t>(),     500000000);
    EXPECT_EQ(v["tzoffset"].As<int64_t>(), -300);
}

// Unknown ext type is silently returned as nil
TEST(MsgPackExtType, UnknownExtIsNil) {
    // fixext4, ext type 99 (unknown), 4 bytes of data
    std::vector<uint8_t> buf = {0xd6, 99, 0xAA, 0xBB, 0xCC, 0xDD};
    auto v = MsgPackDecode(buf);
    EXPECT_TRUE(v.IsNull());
}

USERVER_NAMESPACE_END
