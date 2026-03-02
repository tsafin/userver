// Cross-validation: verify our MsgPack implementation byte-for-byte
// against the tntcxx mpp reference encoder/decoder.
//
// tntcxx does NOT implement Tarantool ext types (UUID, datetime) so
// those remain validated against the official Tarantool wire-format docs
// in msgpack_test.cpp.

#include <userver/utest/utest.hpp>

#include <storages/tarantool/impl/msgpack.hpp>

// tntcxx mpp and Buffer
#include <mpp/mpp.hpp>
#include <Buffer/Buffer.hpp>

USERVER_NAMESPACE_BEGIN

using namespace storages::tarantool::impl;

namespace {

using TntBuf = tnt::Buffer<16 * 1024>;

// Extract all bytes written to a tnt::Buffer into a std::vector<uint8_t>.
std::vector<uint8_t> TntBufToBytes(TntBuf& buf) {
    std::vector<uint8_t> out;
    for (auto itr = buf.begin(); itr != buf.end(); ++itr)
        out.push_back(itr.get<uint8_t>());
    return out;
}

// Load raw bytes into a fresh tnt::Buffer (for tntcxx decode).
TntBuf BytesToTntBuf(const std::vector<uint8_t>& bytes) {
    TntBuf buf;
    buf.write(TntBuf::WData{reinterpret_cast<const char*>(bytes.data()),
                            bytes.size()});
    return buf;
}

}  // namespace

// ---- tntcxx encodes → our MsgPackDecode verifies ----

TEST(MsgPackXCheck, TntEncodeUint0_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, uint64_t{0});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), 0);
}

TEST(MsgPackXCheck, TntEncodeUint127_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, uint64_t{127});
    auto bytes = TntBufToBytes(buf);
    // fixint: should be a single byte 0x7f
    ASSERT_EQ(bytes.size(), 1u);
    EXPECT_EQ(bytes[0], 0x7f);
    auto v = MsgPackDecode(bytes);
    EXPECT_EQ(v.As<int64_t>(), 127);
}

TEST(MsgPackXCheck, TntEncodeUint255_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, uint8_t{255});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), 255);
}

TEST(MsgPackXCheck, TntEncodeUint65535_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, uint16_t{65535});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), 65535);
}

TEST(MsgPackXCheck, TntEncodeUint4G_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, uint32_t{0xDEADBEEF});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), static_cast<int64_t>(0xDEADBEEF));
}

TEST(MsgPackXCheck, TntEncodeUint64Big_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, uint64_t{0x100000000ULL});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), static_cast<int64_t>(0x100000000ULL));
}

TEST(MsgPackXCheck, TntEncodeNegOne_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, int64_t{-1});
    // tntcxx uses int8 (0xd0 0xff) for small negatives; our encoder
    // uses negative fixint (0xff) — both are valid MsgPack encodings.
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), -1);
}

TEST(MsgPackXCheck, TntEncodeNeg100_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, int64_t{-100});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), -100);
}

TEST(MsgPackXCheck, TntEncodeNeg1000_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, int64_t{-1000});
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), -1000);
}

TEST(MsgPackXCheck, TntEncodeNegLarge_OurDecode) {
    TntBuf buf;
    constexpr int64_t kVal = -(1LL << 40);
    mpp::encode(buf, kVal);
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<int64_t>(), kVal);
}

TEST(MsgPackXCheck, TntEncodeFalse_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, false);
    auto bytes = TntBufToBytes(buf);
    ASSERT_EQ(bytes.size(), 1u);
    EXPECT_EQ(bytes[0], 0xc2);
    auto v = MsgPackDecode(bytes);
    EXPECT_FALSE(v.As<bool>());
}

TEST(MsgPackXCheck, TntEncodeTrue_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, true);
    auto bytes = TntBufToBytes(buf);
    ASSERT_EQ(bytes.size(), 1u);
    EXPECT_EQ(bytes[0], 0xc3);
    auto v = MsgPackDecode(bytes);
    EXPECT_TRUE(v.As<bool>());
}

TEST(MsgPackXCheck, TntEncodeNull_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, nullptr);
    auto bytes = TntBufToBytes(buf);
    ASSERT_EQ(bytes.size(), 1u);
    EXPECT_EQ(bytes[0], 0xc0);
    auto v = MsgPackDecode(bytes);
    EXPECT_TRUE(v.IsNull());
}

TEST(MsgPackXCheck, TntEncodeEmptyStr_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, "");
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<std::string>(), "");
}

TEST(MsgPackXCheck, TntEncodeShortStr_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, "hello");
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_EQ(v.As<std::string>(), "hello");
}

TEST(MsgPackXCheck, TntEncodeDouble_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, 3.14);
    auto v = MsgPackDecode(TntBufToBytes(buf));
    EXPECT_DOUBLE_EQ(v.As<double>(), 3.14);
}

TEST(MsgPackXCheck, TntEncodeArray_OurDecode) {
    TntBuf buf;
    mpp::encode(buf, std::make_tuple(int64_t{1}, int64_t{2}, int64_t{3}));
    auto v = MsgPackDecode(TntBufToBytes(buf));
    ASSERT_TRUE(v.IsArray());
    ASSERT_EQ(v.GetSize(), 3u);
    EXPECT_EQ(v[0].As<int64_t>(), 1);
    EXPECT_EQ(v[1].As<int64_t>(), 2);
    EXPECT_EQ(v[2].As<int64_t>(), 3);
}

// ---- Our encoder → tntcxx decode verifies ----

TEST(MsgPackXCheck, OurEncodeUint0_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeUint(bytes, 0);
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    uint64_t val = 99;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_EQ(val, 0u);
}

TEST(MsgPackXCheck, OurEncodeUint255_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeUint(bytes, 255);
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    uint64_t val = 0;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_EQ(val, 255u);
}

TEST(MsgPackXCheck, OurEncodeUint65535_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeUint(bytes, 65535);
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    uint64_t val = 0;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_EQ(val, 65535u);
}

TEST(MsgPackXCheck, OurEncodeUintLarge_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeUint(bytes, 0xDEADBEEFULL);
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    uint64_t val = 0;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_EQ(val, 0xDEADBEEFULL);
}

TEST(MsgPackXCheck, OurEncodeStr_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeStr(bytes, "tarantool");
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    std::string s;
    ASSERT_TRUE(mpp::decode(run, s));
    EXPECT_EQ(s, "tarantool");
}

TEST(MsgPackXCheck, OurEncodeJsonInt_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeJson(bytes, formats::json::ValueBuilder{int64_t{42}}.ExtractValue());
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    int64_t val = 0;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_EQ(val, 42);
}

TEST(MsgPackXCheck, OurEncodeJsonNegInt_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeJson(bytes, formats::json::ValueBuilder{int64_t{-1000}}.ExtractValue());
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    int64_t val = 0;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_EQ(val, -1000);
}

TEST(MsgPackXCheck, OurEncodeJsonBool_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeJson(bytes, formats::json::ValueBuilder{true}.ExtractValue());
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    bool val = false;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_TRUE(val);
}

TEST(MsgPackXCheck, OurEncodeJsonString_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeJson(bytes,
               formats::json::ValueBuilder{std::string{"hello"}}.ExtractValue());
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    std::string s;
    ASSERT_TRUE(mpp::decode(run, s));
    EXPECT_EQ(s, "hello");
}

TEST(MsgPackXCheck, OurEncodeJsonDouble_TntDecode) {
    std::vector<uint8_t> bytes;
    EncodeJson(bytes, formats::json::ValueBuilder{2.718}.ExtractValue());
    auto buf = BytesToTntBuf(bytes);
    auto run = buf.begin<true>();
    double val = 0.0;
    ASSERT_TRUE(mpp::decode(run, val));
    EXPECT_DOUBLE_EQ(val, 2.718);
}

// ---- Byte-for-byte comparison: tntcxx wire == our wire ----

TEST(MsgPackXCheck, WireBytesMatchForUints) {
    for (uint64_t v : {uint64_t{0}, uint64_t{1}, uint64_t{127},
                       uint64_t{128}, uint64_t{255}, uint64_t{256},
                       uint64_t{65535}, uint64_t{65536},
                       uint64_t{0xFFFFFFFF}, uint64_t{0x100000000ULL}}) {
        TntBuf tnt_buf;
        mpp::encode(tnt_buf, v);
        auto tnt_bytes = TntBufToBytes(tnt_buf);

        std::vector<uint8_t> our_bytes;
        EncodeUint(our_bytes, v);

        EXPECT_EQ(our_bytes, tnt_bytes) << "mismatch for value " << v;
    }
}

// Note: tntcxx encodes small negatives (-32..-1) as int8 (2 bytes);
// our encoder uses negative fixint (1 byte). Both are valid MsgPack.
// We verify semantic correctness (round-trip values), not byte identity.
TEST(MsgPackXCheck, WireBytesMatchForNegInts) {
    for (int64_t v : {int64_t{-1}, int64_t{-32}, int64_t{-33},
                      int64_t{-128}, int64_t{-129}, int64_t{-32768},
                      int64_t{-32769}, int64_t{-2147483648LL},
                      int64_t{-2147483649LL}}) {
        // Direction 1: tntcxx encode → our decode
        TntBuf tnt_buf;
        mpp::encode(tnt_buf, v);
        auto decoded = MsgPackDecode(TntBufToBytes(tnt_buf));
        EXPECT_EQ(decoded.As<int64_t>(), v) << "our decode failed for " << v;

        // Direction 2: our encode → tntcxx decode
        std::vector<uint8_t> our_bytes;
        EncodeJson(our_bytes, formats::json::ValueBuilder{v}.ExtractValue());
        auto buf2 = BytesToTntBuf(our_bytes);
        auto run = buf2.begin<true>();
        int64_t tnt_val = 0;
        ASSERT_TRUE(mpp::decode(run, tnt_val)) << "tntcxx decode failed for " << v;
        EXPECT_EQ(tnt_val, v) << "tntcxx decoded wrong value for " << v;
    }
}

TEST(MsgPackXCheck, WireBytesMatchForStrings) {
    const std::vector<std::string> cases = {
        "", "a", "hello", std::string(31, 'x'),
        std::string(32, 'y'), std::string(255, 'z')
    };
    for (const auto& s : cases) {
        TntBuf tnt_buf;
        mpp::encode(tnt_buf, s);
        auto tnt_bytes = TntBufToBytes(tnt_buf);

        std::vector<uint8_t> our_bytes;
        EncodeStr(our_bytes, s);

        EXPECT_EQ(our_bytes, tnt_bytes) << "mismatch for string of len " << s.size();
    }
}

USERVER_NAMESPACE_END
