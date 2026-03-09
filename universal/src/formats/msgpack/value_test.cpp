#include <userver/formats/msgpack/serialize.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/formats/msgpack/tarantool_types.hpp>
#include <userver/utils/datetime/date.hpp>

#include <gtest/gtest.h>

USERVER_NAMESPACE_BEGIN

using namespace formats::msgpack;

// ======================================================================== //
//  Value — basic scalar parsing                                             //
// ======================================================================== //

TEST(MsgpackValue, NullValue) {
    const uint8_t buf[] = {0xc0};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_TRUE(v.IsNull());
    EXPECT_FALSE(v.IsMissing());
    EXPECT_FALSE(v.IsBool());
}

TEST(MsgpackValue, BoolTrue) {
    const uint8_t buf[] = {0xc3};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_TRUE(v.IsBool());
    EXPECT_TRUE(v.As<bool>());
}

TEST(MsgpackValue, BoolFalse) {
    const uint8_t buf[] = {0xc2};
    EXPECT_FALSE(Value::FromBytes(buf, sizeof(buf)).As<bool>());
}

TEST(MsgpackValue, PositiveFixint) {
    const uint8_t buf[] = {42};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_TRUE(v.IsInt());
    EXPECT_EQ(v.As<int64_t>(), 42);
    EXPECT_EQ(v.As<uint64_t>(), 42u);
}

TEST(MsgpackValue, NegativeFixint) {
    const uint8_t buf[] = {0xff};  // -1
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_EQ(v.As<int64_t>(), -1);
}

TEST(MsgpackValue, Uint8) {
    const uint8_t buf[] = {0xcc, 200};
    EXPECT_EQ(Value::FromBytes(buf, sizeof(buf)).As<uint64_t>(), 200u);
}

TEST(MsgpackValue, Uint16) {
    const uint8_t buf[] = {0xcd, 0x01, 0x00};  // 256
    EXPECT_EQ(Value::FromBytes(buf, sizeof(buf)).As<uint32_t>(), 256u);
}

TEST(MsgpackValue, Uint32) {
    const uint8_t buf[] = {0xce, 0x00, 0x01, 0x00, 0x00};  // 65536
    EXPECT_EQ(Value::FromBytes(buf, sizeof(buf)).As<uint64_t>(), 65536u);
}

TEST(MsgpackValue, Int64Min) {
    // INT64_MIN = 0x8000000000000000
    const uint8_t buf[] = {0xd3, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    EXPECT_EQ(Value::FromBytes(buf, sizeof(buf)).As<int64_t>(),
              std::numeric_limits<int64_t>::min());
}

TEST(MsgpackValue, Double) {
    // 1.0 as float64
    const uint8_t buf[] = {0xcb, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    EXPECT_DOUBLE_EQ(Value::FromBytes(buf, sizeof(buf)).As<double>(), 1.0);
}

TEST(MsgpackValue, FixStr) {
    // fixstr "hi" = 0xa2 0x68 0x69
    const uint8_t buf[] = {0xa2, 0x68, 0x69};
    EXPECT_EQ(Value::FromBytes(buf, sizeof(buf)).As<std::string>(), "hi");
}

TEST(MsgpackValue, Str8) {
    // str8 with 3 bytes "abc"
    const uint8_t buf[] = {0xd9, 0x03, 0x61, 0x62, 0x63};
    EXPECT_EQ(Value::FromBytes(buf, sizeof(buf)).As<std::string>(), "abc");
}

// ======================================================================== //
//  Value — map with integer keys (IPROTO style)                             //
// ======================================================================== //

TEST(MsgpackValue, IntKeyMap_BasicLookup) {
    // fixmap {0: "hello"} = 0x81 0x00 0xa5 "hello"
    const uint8_t buf[] = {0x81, 0x00, 0xa5, 'h', 'e', 'l', 'l', 'o'};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_TRUE(v.IsObject());
    EXPECT_EQ(v.GetSize(), 1u);
    EXPECT_EQ(v[std::size_t{0}].As<std::string>(), "hello");
    EXPECT_TRUE(v[std::size_t{1}].IsMissing());
}

TEST(MsgpackValue, IntKeyMap_MultipleKeys) {
    // fixmap {0x00: 200 (uint8), 0x01: 42 (fixint)} = 0x82 0x00 0xcc 0xc8 0x01 0x2a
    const uint8_t buf[] = {0x82, 0x00, 0xcc, 0xc8, 0x01, 0x2a};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_EQ(v[std::size_t{0x00}].As<uint64_t>(), 200u);
    EXPECT_EQ(v[std::size_t{0x01}].As<int64_t>(), 42);
}

// ======================================================================== //
//  Value — array indexing                                                   //
// ======================================================================== //

TEST(MsgpackValue, ArrayIndex) {
    // fixarray [1, 2, 3] = 0x93 0x01 0x02 0x03
    const uint8_t buf[] = {0x93, 0x01, 0x02, 0x03};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_TRUE(v.IsArray());
    EXPECT_EQ(v.GetSize(), 3u);
    EXPECT_EQ(v[std::size_t{0}].As<int64_t>(), 1);
    EXPECT_EQ(v[std::size_t{1}].As<int64_t>(), 2);
    EXPECT_EQ(v[std::size_t{2}].As<int64_t>(), 3);
}

TEST(MsgpackValue, ArrayOutOfBounds) {
    const uint8_t buf[] = {0x91, 0x01};  // [1]
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_THROW(v[std::size_t{1}], OutOfBoundsException);
}

// ======================================================================== //
//  Value — default value on missing                                         //
// ======================================================================== //

TEST(MsgpackValue, AsDefault) {
    const uint8_t buf[] = {0x80};  // empty fixmap
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_EQ(v[std::size_t{99}].As<int64_t>(0), 0);
    EXPECT_EQ(v[std::size_t{99}].As<std::string>("none"), "none");
}

// ======================================================================== //
//  Value — string-keyed map                                                 //
// ======================================================================== //

TEST(MsgpackValue, StrKeyMap) {
    // fixmap {"ok": true}  = 0x81 0xa2 "ok" 0xc3
    const uint8_t buf[] = {0x81, 0xa2, 'o', 'k', 0xc3};
    const auto v = Value::FromBytes(buf, sizeof(buf));
    EXPECT_TRUE(v["ok"].As<bool>());
    EXPECT_TRUE(v["missing"].IsMissing());
}

// ======================================================================== //
//  Value — type errors                                                      //
// ======================================================================== //

TEST(MsgpackValue, TypeMismatch_NotMap) {
    const uint8_t buf[] = {0x01};  // integer 1
    EXPECT_THROW(Value::FromBytes(buf, sizeof(buf))[std::size_t{0}],
                 TypeMismatchException);
}

TEST(MsgpackValue, TypeMismatch_NotArray) {
    const uint8_t buf[] = {0x01};
    EXPECT_THROW(Value::FromBytes(buf, sizeof(buf))[std::size_t{0}],
                 TypeMismatchException);
}

// ======================================================================== //
//  ValueBuilder — scalars round-trip                                        //
// ======================================================================== //

TEST(MsgpackValueBuilder, NullRoundTrip) {
    auto bytes = ValueBuilder{}.ToBytes();
    EXPECT_EQ(bytes.size(), 1u);
    EXPECT_TRUE(Value::FromBytes(bytes.data(), bytes.size()).IsNull());
}

TEST(MsgpackValueBuilder, BoolRoundTrip) {
    {
        auto bytes = ValueBuilder{true}.ToBytes();
        EXPECT_TRUE(Value::FromBytes(bytes.data(), bytes.size()).As<bool>());
    }
    {
        auto bytes = ValueBuilder{false}.ToBytes();
        EXPECT_FALSE(Value::FromBytes(bytes.data(), bytes.size()).As<bool>());
    }
}

TEST(MsgpackValueBuilder, Int64RoundTrip) {
    {
        auto bytes = ValueBuilder{int64_t{-1}}.ToBytes();
        EXPECT_EQ(Value::FromBytes(bytes.data(), bytes.size()).As<int64_t>(), -1);
    }
    {
        auto bytes = ValueBuilder{int64_t{1000000}}.ToBytes();
        EXPECT_EQ(Value::FromBytes(bytes.data(), bytes.size()).As<int64_t>(), 1000000);
    }
}

TEST(MsgpackValueBuilder, UInt64RoundTrip) {
    auto bytes = ValueBuilder{uint64_t{0xdeadbeef}}.ToBytes();
    EXPECT_EQ(Value::FromBytes(bytes.data(), bytes.size()).As<uint64_t>(), 0xdeadbeefU);
}

TEST(MsgpackValueBuilder, DoubleRoundTrip) {
    auto bytes = ValueBuilder{3.14}.ToBytes();
    EXPECT_DOUBLE_EQ(Value::FromBytes(bytes.data(), bytes.size()).As<double>(), 3.14);
}

TEST(MsgpackValueBuilder, StringRoundTrip) {
    auto bytes = ValueBuilder{std::string_view{"hello world"}}.ToBytes();
    EXPECT_EQ(Value::FromBytes(bytes.data(), bytes.size()).As<std::string>(),
              "hello world");
}

// ======================================================================== //
//  ValueBuilder — IntKeyObject (IPROTO request body pattern)                //
// ======================================================================== //

TEST(MsgpackValueBuilder, IntKeyObject_BasicBuild) {
    auto body = ValueBuilder::IntKeyObject();
    body[std::size_t{0x10}] = ValueBuilder{uint32_t{512}};   // space_id = 512
    body[std::size_t{0x21}] = ValueBuilder{std::string_view{"data"}};

    const auto bytes = body.ToBytes();
    const auto v = Value::FromBytes(bytes.data(), bytes.size());

    EXPECT_TRUE(v.IsObject());
    EXPECT_EQ(v.GetSize(), 2u);
    EXPECT_EQ(v[std::size_t{0x10}].As<uint32_t>(), 512u);
    EXPECT_EQ(v[std::size_t{0x21}].As<std::string>(), "data");
}

TEST(MsgpackValueBuilder, IntKeyObject_NestedArray) {
    auto body = ValueBuilder::IntKeyObject();
    body[std::size_t{0x10}] = ValueBuilder{uint32_t{1}};

    auto tuple = ValueBuilder::Array();
    tuple.PushBack(ValueBuilder{std::string_view{"key1"}});
    tuple.PushBack(ValueBuilder{int64_t{99}});
    body[std::size_t{0x21}] = std::move(tuple);

    const auto bytes = body.ToBytes();
    const auto v = Value::FromBytes(bytes.data(), bytes.size());

    EXPECT_EQ(v[std::size_t{0x10}].As<uint32_t>(), 1u);
    const auto t = v[std::size_t{0x21}];
    EXPECT_TRUE(t.IsArray());
    EXPECT_EQ(t.GetSize(), 2u);
    EXPECT_EQ(t[std::size_t{0}].As<std::string>(), "key1");
    EXPECT_EQ(t[std::size_t{1}].As<int64_t>(), 99);
}

TEST(MsgpackValueBuilder, IntKeyObject_IdempotentAccess) {
    // Accessing same key twice should return the same slot
    auto body = ValueBuilder::IntKeyObject();
    body[std::size_t{5}] = ValueBuilder{int64_t{42}};
    // Access again: should see the updated value
    EXPECT_FALSE(body[std::size_t{5}].IsNull());
    EXPECT_EQ(body.GetSize(), 1u);
}

// ======================================================================== //
//  ValueBuilder — Object (string-keyed map)                                 //
// ======================================================================== //

TEST(MsgpackValueBuilder, StrKeyObject_RoundTrip) {
    auto obj = ValueBuilder::Object();
    obj[std::string_view{"name"}] = ValueBuilder{std::string_view{"tarantool"}};
    obj[std::string_view{"port"}] = ValueBuilder{uint32_t{3301}};

    const auto bytes = obj.ToBytes();
    const auto v = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_EQ(v["name"].As<std::string>(), "tarantool");
    EXPECT_EQ(v["port"].As<uint32_t>(), 3301u);
}

// ======================================================================== //
//  ValueBuilder — Array                                                     //
// ======================================================================== //

TEST(MsgpackValueBuilder, ArrayRoundTrip) {
    auto arr = ValueBuilder::Array();
    arr.PushBack(ValueBuilder{int64_t{1}});
    arr.PushBack(ValueBuilder{int64_t{2}});
    arr.PushBack(ValueBuilder{int64_t{3}});

    const auto bytes = arr.ToBytes();
    const auto v = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(v.IsArray());
    EXPECT_EQ(v.GetSize(), 3u);
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(v[i].As<int64_t>(), static_cast<int64_t>(i + 1));
    }
}

// ======================================================================== //
//  serialize.hpp free functions                                             //
// ======================================================================== //

TEST(MsgpackSerialize, ToBytesAndFromBytes) {
    auto builder = ValueBuilder{uint64_t{12345}};
    const auto bytes = ToBytes(builder);
    const auto v = FromBytes(bytes);
    EXPECT_EQ(v.As<uint64_t>(), 12345u);
}

// ======================================================================== //
//  Tarantool ext types — Phase A+B                                         //
// ======================================================================== //

TEST(MsgpackValue, UuidPredicateAndAs) {
    TntUuid uuid;
    uuid.bytes = {0x12,0x34,0x56,0x78,0x9a,0xbc,0xde,0xf0,
                  0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88};
    ValueBuilder vb{uuid};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsExt());
    EXPECT_TRUE(val.IsUuid());
    EXPECT_FALSE(val.IsDatetime());
    EXPECT_EQ(val.AsUuid(), uuid);
    EXPECT_EQ(val.As<TntUuid>(), uuid);
}

TEST(MsgpackValue, UuidToString) {
    TntUuid uuid;
    uuid.bytes = {0x12,0x34,0x56,0x78,0x9a,0xbc,0xde,0xf0,
                  0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88};
    EXPECT_EQ(uuid.ToString(), "12345678-9abc-def0-1122-334455667788");
}

TEST(MsgpackValue, UuidFromString) {
    const auto uuid = TntUuid::FromString("12345678-9abc-def0-1122-334455667788");
    EXPECT_EQ(uuid.bytes[0], 0x12);
    EXPECT_EQ(uuid.bytes[3], 0x78);
    EXPECT_EQ(uuid.bytes[15], 0x88);
}

TEST(MsgpackValue, DatetimeTzRoundTrip) {
    DatetimeTz dt;
    dt.tp = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::from_time_t(1741267200));
    dt.tzoffset = 180;
    dt.tzindex  = 0;
    ValueBuilder vb{dt};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsDatetime());
    EXPECT_FALSE(val.IsUuid());
    auto dt2 = val.AsDatetimeTz();
    EXPECT_EQ(dt2, dt);
    EXPECT_EQ(val.As<DatetimeTz>(), dt);
}

TEST(MsgpackValue, DatetimeWithoutTzRoundTrip) {
    DatetimeWithoutTz dt;
    dt.tp = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::from_time_t(1741267200));
    ValueBuilder vb{dt};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsDatetime());
    auto dt2 = val.AsDatetimeWithoutTz();
    EXPECT_EQ(dt2, dt);
    EXPECT_EQ(val.As<DatetimeWithoutTz>(), dt);
}

TEST(MsgpackValue, TimestampTzRoundTrip) {
    TimestampTz ts;
    ts.tp = std::chrono::time_point_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::from_time_t(1000)) +
            std::chrono::nanoseconds{123456789};
    ts.tzoffset = 60;
    ts.tzindex  = 0;
    ValueBuilder vb{ts};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsDatetime());
    auto ts2 = val.AsTimestampTz();
    EXPECT_EQ(ts2, ts);
}

TEST(MsgpackValue, TimestampWithoutTzRoundTrip) {
    TimestampWithoutTz ts;
    ts.tp = std::chrono::time_point_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::from_time_t(1000)) +
            std::chrono::nanoseconds{500000000};
    ValueBuilder vb{ts};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsDatetime());
    auto ts2 = val.AsTimestampWithoutTz();
    EXPECT_EQ(ts2, ts);
}

TEST(MsgpackValue, DatetimeTzThrowsOnNsec) {
    TimestampTz ts;
    ts.tp = std::chrono::time_point_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::from_time_t(1000)) +
            std::chrono::nanoseconds{500};
    ValueBuilder vb{ts};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_THROW(val.AsDatetimeTz(), ConversionException);
    EXPECT_NO_THROW(val.AsTimestampTz());
}

TEST(MsgpackValue, AsDateValidation) {
    DatetimeTz dt;
    dt.tp = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::from_time_t(1000));
    ValueBuilder vb{dt};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_THROW(val.AsDate(), ConversionException);
}

TEST(MsgpackValue, AsDateSuccess) {
    // 2025-01-01 00:00:00 UTC = 1735689600 seconds
    DatetimeWithoutTz dt;
    dt.tp = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::from_time_t(1735689600));
    ValueBuilder vb{dt};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    auto date = val.AsDate();
    EXPECT_EQ(date, utils::datetime::Date(2025, 1, 1));
    EXPECT_EQ(val.As<utils::datetime::Date>(), utils::datetime::Date(2025, 1, 1));
}

TEST(MsgpackValue, IntervalRoundTrip) {
    TntInterval iv;
    iv.day    = 3;
    iv.hour   = 2;
    iv.minute = 30;
    ValueBuilder vb{iv};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsInterval());
    EXPECT_FALSE(val.IsUuid());
    auto iv2 = val.AsInterval();
    EXPECT_EQ(iv2, iv);
    EXPECT_EQ(val.As<TntInterval>(), iv);
}

TEST(MsgpackValue, IntervalAllZeroRoundTrip) {
    TntInterval iv;
    ValueBuilder vb{iv};
    auto bytes = vb.ToBytes();
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsInterval());
    auto iv2 = val.AsInterval();
    EXPECT_EQ(iv2, iv);
}

TEST(MsgpackValue, IntervalToNanoseconds) {
    TntInterval iv;
    iv.day  = 1;
    iv.hour = 2;
    auto ns = iv.ToNanoseconds();
    EXPECT_EQ(ns.count(), (1LL * 86400 + 2LL * 3600) * 1'000'000'000LL);
}

TEST(MsgpackValue, IntervalCalendarThrows) {
    TntInterval iv;
    iv.year = 1;
    EXPECT_THROW(iv.ToNanoseconds(), ConversionException);
}

TEST(MsgpackValue, IntervalMonthThrows) {
    TntInterval iv;
    iv.month = 3;
    EXPECT_THROW(iv.ToNanoseconds(), ConversionException);
}

TEST(MsgpackValue, IntervalWeekThrows) {
    TntInterval iv;
    iv.week = 2;
    EXPECT_THROW(iv.ToNanoseconds(), ConversionException);
}

TEST(MsgpackValue, DecimalString) {
    // Tarantool example: -12.34
    // fixext4 (0xd6) type=1 (0x01) data={0x02,0x01,0x23,0x4d}
    const std::vector<uint8_t> bytes = {0xd6, 0x01, 0x02, 0x01, 0x23, 0x4d};
    auto val = Value::FromBytes(bytes.data(), bytes.size());
    EXPECT_TRUE(val.IsDecimal());
    EXPECT_FALSE(val.IsUuid());
    EXPECT_EQ(val.AsDecimalString(), "-12.34");
}

USERVER_NAMESPACE_END
