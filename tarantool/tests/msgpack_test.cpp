#include <userver/utest/utest.hpp>

#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/storages/tarantool/error_info.hpp>
#include <userver/storages/tarantool/typed.hpp>
#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/msgpack.hpp>
#include <storages/tarantool/impl/vspace_tuple.hpp>

USERVER_NAMESPACE_BEGIN

using namespace storages::tarantool::impl;

// Helper: build bytes via ValueBuilder and round-trip through MsgPackDecode
static std::vector<uint8_t> EncodeVb(formats::msgpack::ValueBuilder vb) {
    return vb.ToBytes();
}

static formats::json::Value RoundTrip(formats::msgpack::ValueBuilder vb) {
    return MsgPackDecode(EncodeVb(std::move(vb)));
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

// ---- ValueBuilder byte encoding tests ----

TEST(MsgPackEncodeVb, Null) {
    auto buf = EncodeVb(formats::msgpack::ValueBuilder{});
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kNil}));
}

TEST(MsgPackEncodeVb, BoolTrue) {
    auto buf = EncodeVb(formats::msgpack::ValueBuilder{true});
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kTrue}));
}

TEST(MsgPackEncodeVb, BoolFalse) {
    auto buf = EncodeVb(formats::msgpack::ValueBuilder{false});
    EXPECT_EQ(buf, (std::vector<uint8_t>{mp::kFalse}));
}

TEST(MsgPackEncodeVb, NegativeFixint) {
    auto buf = EncodeVb(formats::msgpack::ValueBuilder{int8_t{-1}});
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xff}));

    buf = EncodeVb(formats::msgpack::ValueBuilder{int8_t{-32}});
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xe0}));
}

TEST(MsgPackEncodeVb, NegativeInt8) {
    auto buf = EncodeVb(formats::msgpack::ValueBuilder{int8_t{-33}});
    EXPECT_EQ(buf[0], mp::kInt8);
    EXPECT_EQ(buf.size(), 2u);
}

TEST(MsgPackEncodeVb, String) {
    auto buf = EncodeVb(formats::msgpack::ValueBuilder{std::string_view{"abc"}});
    EXPECT_EQ(buf, (std::vector<uint8_t>{0xa3, 'a', 'b', 'c'}));
}

// ---- DecodeValue: scalar round-trips ----

TEST(MsgPackRoundTrip, Zero) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int64_t{0}});
    EXPECT_EQ(v.As<int64_t>(), 0);
}

TEST(MsgPackRoundTrip, PositiveFixint) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int64_t{42}});
    EXPECT_EQ(v.As<int64_t>(), 42);
}

TEST(MsgPackRoundTrip, Uint255) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int64_t{255}});
    EXPECT_EQ(v.As<int64_t>(), 255);
}

TEST(MsgPackRoundTrip, Uint65535) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int64_t{65535}});
    EXPECT_EQ(v.As<int64_t>(), 65535);
}

TEST(MsgPackRoundTrip, LargeUint) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{uint64_t{0xDEADBEEFULL}});
    EXPECT_EQ(v.As<int64_t>(), static_cast<int64_t>(0xDEADBEEF));
}

TEST(MsgPackRoundTrip, NegativeFixint) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int8_t{-1}});
    EXPECT_EQ(v.As<int64_t>(), -1);
}

TEST(MsgPackRoundTrip, NegativeInt8) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int8_t{-100}});
    EXPECT_EQ(v.As<int64_t>(), -100);
}

TEST(MsgPackRoundTrip, NegativeInt16) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int16_t{-1000}});
    EXPECT_EQ(v.As<int64_t>(), -1000);
}

TEST(MsgPackRoundTrip, NegativeInt32) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{int32_t{-100000}});
    EXPECT_EQ(v.As<int64_t>(), -100000);
}

TEST(MsgPackRoundTrip, NegativeInt64) {
    constexpr int64_t kVal = -(1LL << 40);
    auto v = RoundTrip(formats::msgpack::ValueBuilder{kVal});
    EXPECT_EQ(v.As<int64_t>(), kVal);
}

TEST(MsgPackRoundTrip, BoolTrue) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{true});
    EXPECT_TRUE(v.As<bool>());
}

TEST(MsgPackRoundTrip, BoolFalse) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{false});
    EXPECT_FALSE(v.As<bool>());
}

TEST(MsgPackRoundTrip, Null) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{});
    EXPECT_TRUE(v.IsNull());
}

TEST(MsgPackRoundTrip, EmptyString) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{std::string_view{""}});
    EXPECT_EQ(v.As<std::string>(), "");
}

TEST(MsgPackRoundTrip, ShortString) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{std::string_view{"hello"}});
    EXPECT_EQ(v.As<std::string>(), "hello");
}

TEST(MsgPackRoundTrip, LongString) {
    // 256-char string -> str16
    std::string s(256, 'z');
    auto v = RoundTrip(formats::msgpack::ValueBuilder{s});
    EXPECT_EQ(v.As<std::string>(), s);
}

TEST(MsgPackRoundTrip, Float64) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder{3.14});
    EXPECT_DOUBLE_EQ(v.As<double>(), 3.14);
}

// ---- Array round-trips ----

TEST(MsgPackRoundTrip, EmptyArray) {
    auto v = RoundTrip(formats::msgpack::ValueBuilder::Array());
    EXPECT_TRUE(v.IsArray());
    EXPECT_EQ(v.GetSize(), 0u);
}

TEST(MsgPackRoundTrip, MixedArray) {
    auto b = formats::msgpack::ValueBuilder::Array();
    b.PushBack(formats::msgpack::ValueBuilder{int64_t{1}});
    b.PushBack(formats::msgpack::ValueBuilder{std::string_view{"two"}});
    b.PushBack(formats::msgpack::ValueBuilder{true});
    auto v = RoundTrip(std::move(b));
    ASSERT_TRUE(v.IsArray());
    ASSERT_EQ(v.GetSize(), 3u);
    EXPECT_EQ(v[0].As<int64_t>(), 1);
    EXPECT_EQ(v[1].As<std::string>(), "two");
    EXPECT_TRUE(v[2].As<bool>());
}

TEST(MsgPackRoundTrip, NestedArray) {
    auto inner = formats::msgpack::ValueBuilder::Array();
    inner.PushBack(formats::msgpack::ValueBuilder{int64_t{7}});
    auto outer = formats::msgpack::ValueBuilder::Array();
    outer.PushBack(std::move(inner));
    auto v = RoundTrip(std::move(outer));
    ASSERT_TRUE(v.IsArray());
    ASSERT_EQ(v.GetSize(), 1u);
    ASSERT_TRUE(v[0].IsArray());
    EXPECT_EQ(v[0][0].As<int64_t>(), 7);
}

// ---- Map round-trips ----

TEST(MsgPackRoundTrip, StringKeyMap) {
    auto b = formats::msgpack::ValueBuilder::Object();
    b["name"] = formats::msgpack::ValueBuilder{std::string_view{"tarantool"}};
    b["version"] = formats::msgpack::ValueBuilder{int64_t{3}};
    auto v = RoundTrip(std::move(b));
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

// ============================================================================
// Structured error decoding (Phase C)
// ============================================================================

// Build a msgpack-encoded IPROTO_ERROR value (key 0x52) that looks like:
//   {0x00: [{0x00:"ClientError", 0x01:"eval", 0x02:42,
//            0x03:"No such space", 0x04:0, 0x05:36}]}
static std::vector<uint8_t> MakeStructuredErrorBody() {
    std::vector<uint8_t> buf;
    using namespace storages::tarantool::impl;

    // Outer IPROTO_ERROR map {0x52: <err_val>}
    EncodeFixMap(buf, 1);
    EncodeUint(buf, 0x52);

    // err_val = {0x00: <stack_array>}
    EncodeFixMap(buf, 1);
    EncodeUint(buf, 0x00);  // kErrStack

    // stack_array = [{frame}]
    EncodeArray(buf, 1);

    // frame = {0x00:"ClientError", 0x01:"eval", 0x02:42,
    //          0x03:"No such space", 0x04:0, 0x05:36}
    EncodeFixMap(buf, 6);
    EncodeUint(buf, 0x00); EncodeStr(buf, "ClientError");
    EncodeUint(buf, 0x01); EncodeStr(buf, "eval");
    EncodeUint(buf, 0x02); EncodeUint(buf, 42);
    EncodeUint(buf, 0x03); EncodeStr(buf, "No such space 'test'");
    EncodeUint(buf, 0x04); EncodeUint(buf, 0);
    EncodeUint(buf, 0x05); EncodeUint(buf, 36);

    return buf;
}

TEST(StructuredError, DecodesSingleFrame) {
    auto body_bytes = MakeStructuredErrorBody();
    auto body_val = formats::msgpack::Value::FromBytes(
        body_bytes.data(), body_bytes.size());

    const auto ext_err = body_val[0x52u];
    ASSERT_FALSE(ext_err.IsMissing());

    const auto stack = ext_err[0x00u];
    ASSERT_FALSE(stack.IsMissing());
    ASSERT_TRUE(stack.IsArray());
    ASSERT_EQ(stack.GetSize(), 1u);

    const auto frame = stack[0];
    EXPECT_EQ(frame[0x00u].As<std::string>(""), "ClientError");
    EXPECT_EQ(frame[0x01u].As<std::string>(""), "eval");
    EXPECT_EQ(frame[0x02u].As<uint32_t>(0u), 42u);
    EXPECT_EQ(frame[0x03u].As<std::string>(""), "No such space 'test'");
    EXPECT_EQ(frame[0x04u].As<uint32_t>(0u), 0u);
    EXPECT_EQ(frame[0x05u].As<uint32_t>(0u), 36u);
}

TEST(StructuredError, TntErrorInfoFields) {
    auto body_bytes = MakeStructuredErrorBody();
    auto body_val = formats::msgpack::Value::FromBytes(
        body_bytes.data(), body_bytes.size());

    // Simulate DecodeErrorInfo logic
    const auto ext_err = body_val[0x52u];
    ASSERT_FALSE(ext_err.IsMissing());

    storages::tarantool::TntErrorInfo info;
    const auto stack_val = ext_err[0x00u];
    for (std::size_t i = 0; i < stack_val.GetSize(); ++i) {
        const auto fv = stack_val[i];
        storages::tarantool::TntErrorFrame f;
        f.type       = fv[0x00u].As<std::string>("");
        f.file       = fv[0x01u].As<std::string>("");
        f.line       = fv[0x02u].As<uint32_t>(0u);
        f.message    = fv[0x03u].As<std::string>("");
        f.sys_errno  = fv[0x04u].As<uint32_t>(0u);
        f.errcode    = fv[0x05u].As<uint32_t>(0u);
        info.stack.push_back(std::move(f));
    }

    ASSERT_EQ(info.stack.size(), 1u);
    EXPECT_EQ(info.Message(), "No such space 'test'");
    EXPECT_EQ(info.Errcode(), 36u);
    EXPECT_EQ(info.stack[0].type, "ClientError");
    EXPECT_EQ(info.stack[0].file, "eval");
    EXPECT_EQ(info.stack[0].line, 42u);
}

TEST(StructuredError, EmptyInfoOnMissingKey) {
    // Body with only legacy 0x31 key — no 0x52
    std::vector<uint8_t> buf;
    using namespace storages::tarantool::impl;
    EncodeFixMap(buf, 1);
    EncodeUint(buf, 0x31);
    EncodeStr(buf, "some error");

    auto body_val = formats::msgpack::Value::FromBytes(buf.data(), buf.size());
    const auto ext_err = body_val[0x52u];
    EXPECT_TRUE(ext_err.IsMissing());
}

// ============================================================================
// JSON fallback path for MpDecoder::DecodeExt — Phase D
// (These test the MsgPackDecode / RoundTrip path which returns JSON values)
// ============================================================================

// Build a Decimal (ext type 1) encoding for "12.34":
//   scale=2, BCD digits [0,1,2,3,4] (leading zero) + positive sign (0x0C)
//   → bytes 0x01, 0x23, 0x4C  (3 BCD bytes + 1 scale byte = 4 data bytes)
//   Use fixext4 (0xd6): 4 data bytes after type byte.
static std::vector<uint8_t> MakeDecimalExt() {
    return {0xd6, 0x01,  // fixext4, type=kExtDecimal
            0x02,        // scale = 2
            0x01,        // BCD 0, 1
            0x23,        // BCD 2, 3
            0x4c};       // BCD digit 4, sign 0x0C = positive
}

TEST(MsgPackDecodeExt, DecimalDecodesToString) {
    auto v = MsgPackDecode(MakeDecimalExt());
    // Value should be the decimal string "12.34"
    EXPECT_FALSE(v.IsNull());
    EXPECT_EQ(v.As<std::string>(), "12.34");
}

// Build an interval ext (type 6) with day=1, hour=2
static std::vector<uint8_t> MakeIntervalExt() {
    std::vector<uint8_t> buf;
    using namespace storages::tarantool::impl;
    // Interval ext: ext8 format (variable length)
    // We'll build the data bytes first
    std::vector<uint8_t> data;
    // count = 2 (two non-zero fields: day=3, hour=4)
    data.push_back(0x02);           // count = 2
    data.push_back(0x03);           // field_id = 3 (day)
    data.push_back(0x01);           // value = 1
    data.push_back(0x04);           // field_id = 4 (hour)
    data.push_back(0x02);           // value = 2

    // Encode as ext8: 0xc7, len, type
    buf.push_back(0xc7);
    buf.push_back(static_cast<uint8_t>(data.size()));
    buf.push_back(0x06);  // kExtInterval
    buf.insert(buf.end(), data.begin(), data.end());
    return buf;
}

TEST(MsgPackDecodeExt, IntervalDecodesToJsonObject) {
    auto v = MsgPackDecode(MakeIntervalExt());
    // Should be a JSON object with named fields
    EXPECT_FALSE(v.IsNull());
    EXPECT_TRUE(v.IsObject());
    EXPECT_EQ(v["day"].As<int64_t>(),  1);
    EXPECT_EQ(v["hour"].As<int64_t>(), 2);
    EXPECT_EQ(v["year"].As<int64_t>(), 0);  // unset fields default to 0
}

// Build an error ext (type 3) with a single frame
static std::vector<uint8_t> MakeErrorExt() {
    std::vector<uint8_t> inner;
    using namespace storages::tarantool::impl;
    // {0x00: [{0x03:"disk error"}]}
    EncodeFixMap(inner, 1);
    EncodeUint(inner, 0x00);  // ERROR_STACK key
    EncodeArray(inner, 1);
    EncodeFixMap(inner, 1);
    EncodeUint(inner, 0x03);  // kErrMessage
    EncodeStr(inner, "disk error");

    std::vector<uint8_t> buf;
    buf.push_back(0xc7);
    buf.push_back(static_cast<uint8_t>(inner.size()));
    buf.push_back(0x03);  // kExtError
    buf.insert(buf.end(), inner.begin(), inner.end());
    return buf;
}

TEST(MsgPackDecodeExt, ErrorDecodesToJsonObject) {
    auto v = MsgPackDecode(MakeErrorExt());
    EXPECT_FALSE(v.IsNull());
    // Should be the decoded inner map: {"0": [{"3":"disk error"}]}
    EXPECT_TRUE(v.IsObject());
}

// ---- ValueBuilder::AppendTo ----

// AppendTo output must be byte-for-byte identical to ToBytes().
TEST(MsgPackValueBuilder, AppendToMatchesToBytes) {
    using VB = formats::msgpack::ValueBuilder;
    auto check = [](VB vb) {
        const auto expected = vb.ToBytes();
        std::vector<uint8_t> actual;
        vb.AppendTo(actual);
        EXPECT_EQ(actual, expected);
    };

    check(VB{});                              // null
    check(VB{true});
    check(VB{false});
    check(VB{int64_t{0}});
    check(VB{int64_t{42}});
    check(VB{int64_t{-1}});
    check(VB{int64_t{-1000}});
    check(VB{uint64_t{0xDEADBEEFULL}});
    check(VB{double{3.14}});
    check(VB{std::string_view{""}});
    check(VB{std::string_view{"hello world"}});

    // Array
    auto arr = VB::Array();
    arr.PushBack(VB{int64_t{1}});
    arr.PushBack(VB{std::string_view{"two"}});
    arr.PushBack(VB{true});
    check(arr);

    // IntKeyObject (IPROTO body pattern)
    auto obj = VB::IntKeyObject();
    obj[0x10U] = VB{uint32_t{512}};
    obj[0x21U] = VB{std::string_view{"test"}};
    check(obj);
}

// AppendTo appends to whatever is already in the buffer — it does not clear it.
TEST(MsgPackValueBuilder, AppendToPreservesExistingContent) {
    std::vector<uint8_t> buf = {0xAA, 0xBB};
    formats::msgpack::ValueBuilder{int64_t{42}}.AppendTo(buf);
    ASSERT_EQ(buf.size(), 3u);
    EXPECT_EQ(buf[0], 0xAA);
    EXPECT_EQ(buf[1], 0xBB);
    EXPECT_EQ(buf[2], 0x2A);  // msgpack positive fixint 42
}

// Calling AppendTo twice on the same builder should append two copies.
TEST(MsgPackValueBuilder, AppendToTwiceDoubles) {
    formats::msgpack::ValueBuilder vb{uint64_t{0}};
    const auto once = vb.ToBytes();
    std::vector<uint8_t> buf;
    vb.AppendTo(buf);
    vb.AppendTo(buf);
    ASSERT_EQ(buf.size(), once.size() * 2);
    EXPECT_EQ(std::vector<uint8_t>(buf.begin(), buf.begin() + once.size()), once);
    EXPECT_EQ(std::vector<uint8_t>(buf.begin() + once.size(), buf.end()), once);
}

// ---- BuildPingFrame ----

// prehdr: 0xce + 4-byte big-endian length
// header: fixmap{0x00:64, 0x01:sync_id}
// body:   empty
TEST(IprotoFrames, PingFrameSize) {
    const auto f = BuildPingFrame(1);
    EXPECT_EQ(f.size(), kPingFrameSize);
    EXPECT_EQ(kPingFrameSize, 18u);
}

TEST(IprotoFrames, PingFramePrehdrLength) {
    const auto f = BuildPingFrame(42);
    // bytes[0..4]: 0xce + uint32 big-endian length of the rest
    EXPECT_EQ(f[0], 0xce);
    const uint32_t len = (uint32_t(f[1]) << 24) | (uint32_t(f[2]) << 16)
                       | (uint32_t(f[3]) << 8)  |  uint32_t(f[4]);
    // header = 13 bytes, body = 0 bytes → total past prehdr = 13
    EXPECT_EQ(len, 13u);
}

TEST(IprotoFrames, PingFrameHeaderDecodes) {
    // Use a value fitting in int64 to avoid sign-bit issues in the decoder.
    constexpr uint64_t kSync = 0x0102030405060708ULL;
    const auto f = BuildPingFrame(kSync);

    // Parse the header map (bytes [5..17]) as msgpack
    auto hdr = formats::msgpack::Value::FromBytes(f.data() + 5, f.size() - 5);
    ASSERT_FALSE(hdr.IsMissing());

    // IPROTO_CODE (key 0x00) must be 64 (PING)
    EXPECT_EQ(hdr[uint64_t{0x00}].As<uint32_t>(), 64u);

    // IPROTO_SYNC (key 0x01) must round-trip exactly
    EXPECT_EQ(hdr[uint64_t{0x01}].As<uint64_t>(), kSync);
}

TEST(IprotoFrames, PingFrameSyncVaries) {
    const auto f1 = BuildPingFrame(1);
    const auto f2 = BuildPingFrame(2);
    // Only sync bytes differ (bytes 10–17); everything else is identical
    for (std::size_t i = 0; i < 10; ++i)
        EXPECT_EQ(f1[i], f2[i]) << "byte " << i << " should be identical";
    // At least one sync byte differs
    EXPECT_NE(std::vector<uint8_t>(f1.begin()+10, f1.end()),
              std::vector<uint8_t>(f2.begin()+10, f2.end()));
}

// ---- VspaceTuple / DecodeVspaceTuples ----

// Helper: encode a minimal _vspace-like response array using tntcxx mpp.
// Returns the msgpack bytes for: [[id, owner, name, engine, field_count]]
static std::vector<uint8_t> EncodeVspaceResponse(
        uint32_t id, uint32_t owner, const std::string& name,
        const std::string& engine, uint32_t field_count) {
    tnt::Buffer<4096> buf;
    mpp::encode(buf, std::make_tuple(
        std::make_tuple(id, owner, name, engine, field_count)
    ));
    std::vector<uint8_t> out;
    for (auto it = buf.begin(); it != buf.end(); ++it)
        out.push_back(it.get<uint8_t>());
    return out;
}

TEST(VspaceTuple, DecodesIdFromTypicalResponse) {
    const auto raw = EncodeVspaceResponse(512, 1, "kv", "memtx", 2);
    const auto tuples = DecodeVspaceTuples(
        {raw.data(), raw.size()});
    ASSERT_EQ(tuples.size(), 1u);
    EXPECT_EQ(tuples[0].id, 512u);
    EXPECT_EQ(tuples[0].owner, 1u);
    EXPECT_EQ(tuples[0].name, "kv");
    EXPECT_EQ(tuples[0].engine, "memtx");
    EXPECT_EQ(tuples[0].field_count, 2u);
}

TEST(VspaceTuple, EmptyResponseReturnsEmptyVector) {
    const auto tuples = DecodeVspaceTuples({});
    EXPECT_TRUE(tuples.empty());
}

TEST(VspaceTuple, SpaceIdZeroIsValid) {
    const auto raw = EncodeVspaceResponse(0, 0, "_space", "memtx", 7);
    const auto tuples = DecodeVspaceTuples(
        {raw.data(), raw.size()});
    ASSERT_EQ(tuples.size(), 1u);
    EXPECT_EQ(tuples[0].id, 0u);
    EXPECT_EQ(tuples[0].name, "_space");
}

TEST(VspaceTuple, LargeSpaceIdRoundTrips) {
    const auto raw = EncodeVspaceResponse(0xFFFFFFFFu, 1, "huge", "vinyl", 0);
    const auto tuples = DecodeVspaceTuples(
        {raw.data(), raw.size()});
    ASSERT_EQ(tuples.size(), 1u);
    EXPECT_EQ(tuples[0].id, 0xFFFFFFFFu);
    EXPECT_EQ(tuples[0].engine, "vinyl");
}

// ---- MppEncode / MppDecode (storages::tarantool typed API) ----

namespace {

struct Point {
    uint32_t x;
    uint32_t y;
    std::string label;

    static constexpr auto mpp = std::make_tuple(
        &Point::x, &Point::y, &Point::label);

    bool operator==(const Point& o) const noexcept {
        return x == o.x && y == o.y && label == o.label;
    }
};

// Encode a single Point as a msgpack array using ValueBuilder as oracle.
static std::vector<uint8_t> EncodePointVb(const Point& p) {
    auto vb = formats::msgpack::ValueBuilder::Array();
    vb.PushBack(formats::msgpack::ValueBuilder{p.x});
    vb.PushBack(formats::msgpack::ValueBuilder{p.y});
    vb.PushBack(formats::msgpack::ValueBuilder{p.label});
    return vb.ToBytes();
}

}  // namespace

TEST(MppTyped, MppEncodeMatchesValueBuilder) {
    const Point p{7, 42, "hello"};
    const auto mpp_bytes = storages::tarantool::MppEncode(p);
    const auto vb_bytes  = EncodePointVb(p);
    EXPECT_EQ(mpp_bytes, vb_bytes);
}

TEST(MppTyped, MppEncodeEmptyStringField) {
    const Point p{0, 0, ""};
    const auto mpp_bytes = storages::tarantool::MppEncode(p);
    const auto vb_bytes  = EncodePointVb(p);
    EXPECT_EQ(mpp_bytes, vb_bytes);
}

TEST(MppTyped, MppDecodeRoundTrip) {
    // Encode a vector of Points, then decode back.
    const std::vector<Point> original{
        {1, 2, "a"}, {100, 200, "foo"}, {0, 0, ""}};

    // Encode as outer array using tntcxx mpp (matches IPROTO_DATA layout).
    tnt::Buffer<4096> buf;
    mpp::encode(buf, original);
    std::vector<uint8_t> raw;
    for (auto it = buf.begin(); it != buf.end(); ++it)
        raw.push_back(it.get<uint8_t>());

    const auto decoded = storages::tarantool::MppDecode<Point>(
        {raw.data(), raw.size()});
    ASSERT_EQ(decoded.size(), original.size());
    for (std::size_t i = 0; i < original.size(); ++i)
        EXPECT_EQ(decoded[i], original[i]) << "row " << i;
}

TEST(MppTyped, MppDecodeEmptySpanReturnsEmpty) {
    const auto decoded = storages::tarantool::MppDecode<Point>({});
    EXPECT_TRUE(decoded.empty());
}

TEST(MppTyped, MppEncodeDecodeRoundTrip) {
    // MppDecode(MppEncode(single element wrapped in outer array))
    const Point p{99, 1, "roundtrip"};

    // Build outer array with one element manually (like IPROTO response).
    tnt::Buffer<4096> buf;
    mpp::encode(buf, std::make_tuple(p));  // outer fixarray1
    std::vector<uint8_t> raw;
    for (auto it = buf.begin(); it != buf.end(); ++it)
        raw.push_back(it.get<uint8_t>());

    const auto decoded = storages::tarantool::MppDecode<Point>(
        {raw.data(), raw.size()});
    ASSERT_EQ(decoded.size(), 1u);
    EXPECT_EQ(decoded[0], p);
}

USERVER_NAMESPACE_END
