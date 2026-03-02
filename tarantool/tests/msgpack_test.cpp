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
    auto v = RoundTrip(formats::json::ValueBuilder{int64_t{-1LL << 40}}.ExtractValue());
    EXPECT_EQ(v.As<int64_t>(), -1LL << 40);
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

USERVER_NAMESPACE_END
