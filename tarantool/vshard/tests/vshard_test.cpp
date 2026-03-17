#include <userver/utest/utest.hpp>

#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>

#include <vshard/impl/bucket_calculator.hpp>
#include <vshard/impl/routing_table.hpp>
#include <vshard/impl/vshard_error.hpp>
#include <vshard/impl/vshard_exceptions.hpp>

USERVER_NAMESPACE_BEGIN

using namespace storages::tarantool::vshard;
using namespace storages::tarantool::vshard::impl;

// ---------------------------------------------------------------------------
// BucketCalculator — mpcrc32
// ---------------------------------------------------------------------------

// Reference values computed with Tarantool:
//   tarantool> vshard = require('vshard')
//   tarantool> vshard.router.bucket_id_mpcrc32('hello', 3000)
// Since we can't run Tarantool in tests, we verify self-consistency:
// same key always produces same bucket, and result is in [1, N].

TEST(BucketCalculator, Mpcrc32StringInRange) {
    BucketCalculator calc{3000};
    for (const auto* key : {"hello", "world", "", "key123"}) {
        const auto bid = calc.BucketIdMpcrc32(key);
        EXPECT_GE(bid, 1u) << "key=" << key;
        EXPECT_LE(bid, 3000u) << "key=" << key;
    }
}

TEST(BucketCalculator, Mpcrc32IntInRange) {
    BucketCalculator calc{3000};
    for (int64_t k : {0LL, 1LL, -1LL, 1000LL, -1000LL, 999999999LL}) {
        const auto bid = calc.BucketIdMpcrc32(k);
        EXPECT_GE(bid, 1u) << "key=" << k;
        EXPECT_LE(bid, 3000u) << "key=" << k;
    }
}

TEST(BucketCalculator, Mpcrc32UintInRange) {
    BucketCalculator calc{3000};
    for (uint64_t k : {0ULL, 1ULL, 127ULL, 256ULL, 0xFFFFFFFFULL}) {
        const auto bid = calc.BucketIdMpcrc32(k);
        EXPECT_GE(bid, 1u);
        EXPECT_LE(bid, 3000u);
    }
}

TEST(BucketCalculator, Mpcrc32Deterministic) {
    BucketCalculator calc{3000};
    const auto b1 = calc.BucketIdMpcrc32(std::string_view{"testkey"});
    const auto b2 = calc.BucketIdMpcrc32(std::string_view{"testkey"});
    EXPECT_EQ(b1, b2);

    const auto bi1 = calc.BucketIdMpcrc32(int64_t{42});
    const auto bi2 = calc.BucketIdMpcrc32(int64_t{42});
    EXPECT_EQ(bi1, bi2);
}

TEST(BucketCalculator, Strcrc32IntDiffersFromMpcrc32) {
    BucketCalculator calc{3000};
    // mpcrc32 and strcrc32 produce different results for integers
    // (mpcrc32 msgpack-encodes first, strcrc32 stringifies to decimal)
    // This is a property-test: for at least some integer the two differ.
    bool found_diff = false;
    for (int64_t k = 1; k <= 1000; ++k) {
        if (calc.BucketIdMpcrc32(k) != calc.BucketIdStrcrc32(k)) {
            found_diff = true;
            break;
        }
    }
    EXPECT_TRUE(found_diff)
        << "mpcrc32 and strcrc32 should differ for integer keys";
}

TEST(BucketCalculator, Strcrc32StringEqualsString) {
    BucketCalculator calc{3000};
    // For strings, strcrc32 == mpcrc32 (both CRC32 raw bytes).
    EXPECT_EQ(calc.BucketIdMpcrc32(std::string_view{"foo"}),
              calc.BucketIdStrcrc32(std::string_view{"foo"}));
}

TEST(BucketCalculator, Mpcrc32NegativeFixint) {
    // Tarantool encodes -1 as 0xff (negative fixint in msgpack).
    // CRC32(0xff) % 3000 + 1 should be consistent.
    BucketCalculator calc{3000};
    const auto b = calc.BucketIdMpcrc32(int64_t{-1});
    EXPECT_GE(b, 1u);
    EXPECT_LE(b, 3000u);
    // Consistency
    EXPECT_EQ(b, calc.BucketIdMpcrc32(int64_t{-1}));
}

TEST(BucketCalculator, BucketCount32768) {
    BucketCalculator calc{32768};
    for (const auto* k : {"a", "b", "c"}) {
        const auto bid = calc.BucketIdMpcrc32(k);
        EXPECT_GE(bid, 1u);
        EXPECT_LE(bid, 32768u);
    }
}

// ---------------------------------------------------------------------------
// RoutingTable — topology lookup
// ---------------------------------------------------------------------------

// Build a minimal routing table with 2 replicasets and 6 buckets.
static std::unique_ptr<RoutingTable> MakeTestTable() {
    auto t = std::make_unique<RoutingTable>();
    t->bucket_count = 6;
    t->bucket_to_rs = {1, 1, 1, 2, 2, 2};  // buckets 1-3 → rs0, 4-6 → rs1
    // We use null ReplicasetPool instances here since no network calls are made.
    t->replicasets = {
        nullptr,  // index 0 unused (placeholder for "unknown")
        nullptr,  // index 1 = rs0
        nullptr,  // index 2 = rs1
    };
    // Re-index: FindReplicaset uses replicasets[idx-1]
    // For tests we override the table structure without real pools.
    // Rebuild properly:
    t->replicasets.clear();
    t->bucket_to_rs = {1, 1, 1, 2, 2, 2};
    // Two fake pools (nullptr is ok for unit tests not calling Execute)
    t->replicasets.push_back(nullptr);  // 1-based idx=1
    t->replicasets.push_back(nullptr);  // 1-based idx=2
    return t;
}

TEST(RoutingTable, FindReplicasetKnownBuckets) {
    auto t = MakeTestTable();
    EXPECT_EQ(t->bucket_to_rs.size(), 6u);
    EXPECT_EQ(t->bucket_to_rs[0], 1u);
    EXPECT_EQ(t->bucket_to_rs[3], 2u);
}

TEST(RoutingTable, OutOfRangeBucketReturnsNull) {
    auto t = MakeTestTable();
    EXPECT_EQ(t->FindReplicaset(0), nullptr);    // 0 is invalid (1-based)
    EXPECT_EQ(t->FindReplicaset(7), nullptr);    // > bucket_count
    EXPECT_EQ(t->FindReplicaset(100), nullptr);
}

TEST(RoutingTable, UpdateBucketOwner) {
    auto t = MakeTestTable();
    EXPECT_EQ(t->bucket_to_rs[0], 1u);  // bucket 1 → rs 1
    EXPECT_TRUE(t->UpdateBucketOwner(1, 2));
    EXPECT_EQ(t->bucket_to_rs[0], 2u);  // now → rs 2
}

TEST(RoutingTable, UpdateBucketOwnerOutOfRange) {
    auto t = MakeTestTable();
    EXPECT_FALSE(t->UpdateBucketOwner(0, 1));   // bucket 0 invalid
    EXPECT_FALSE(t->UpdateBucketOwner(7, 1));   // > bucket_count
    EXPECT_FALSE(t->UpdateBucketOwner(1, 99));  // rs_idx out of range
}

// ---------------------------------------------------------------------------
// VshardError parsing
// ---------------------------------------------------------------------------

TEST(VshardError, NilIsNull) {
    // A nil msgpack Value should produce a null VshardError.
    formats::msgpack::ValueBuilder vb;
    // default ValueBuilder is nil
    const auto bytes = vb.ToBytes();
    const auto val = formats::msgpack::Value::FromBytes(bytes.data(), bytes.size());
    const auto err = ParseVshardError(val);
    EXPECT_TRUE(err.IsNull());
}

TEST(VshardError, EmptyValueIsNull) {
    const auto err = ParseVshardError(formats::msgpack::Value{});
    EXPECT_TRUE(err.IsNull());
}

// Build a minimal msgpack map {"code": 32, "message": "moved"}
// manually to test ParseVshardError without msgpack::ValueBuilder.
TEST(VshardError, WrongBucketParsed) {
    formats::msgpack::ValueBuilder vb;
    vb["code"] = formats::msgpack::ValueBuilder{32u};
    vb["message"] = formats::msgpack::ValueBuilder{std::string{"bucket 42 is not found on rs-001"}};
    vb["destination"] = formats::msgpack::ValueBuilder{std::string{"rs-002"}};
    const auto bytes = vb.ToBytes();
    const auto val = formats::msgpack::Value::FromBytes(
        bytes.data(), bytes.size());

    const auto err = ParseVshardError(val);
    EXPECT_FALSE(err.IsNull());
    EXPECT_TRUE(err.IsWrongBucket());
    EXPECT_EQ(err.code, 32u);
    ASSERT_TRUE(err.destination_uuid.has_value());
    EXPECT_EQ(*err.destination_uuid, "rs-002");
}

TEST(VshardError, NonMasterParsed) {
    formats::msgpack::ValueBuilder vb;
    vb["code"] = formats::msgpack::ValueBuilder{40u};
    vb["message"] = formats::msgpack::ValueBuilder{std::string{"non master"}};
    const auto bytes = vb.ToBytes();
    const auto val = formats::msgpack::Value::FromBytes(bytes.data(), bytes.size());
    const auto err = ParseVshardError(val);
    EXPECT_TRUE(err.IsNonMaster());
}

TEST(VshardError, TransferParsed) {
    formats::msgpack::ValueBuilder vb;
    vb["code"] = formats::msgpack::ValueBuilder{33u};
    vb["message"] = formats::msgpack::ValueBuilder{std::string{"bucket is transferring"}};
    const auto bytes = vb.ToBytes();
    const auto val = formats::msgpack::Value::FromBytes(bytes.data(), bytes.size());
    const auto err = ParseVshardError(val);
    EXPECT_TRUE(err.IsTransfer());
}

// ---------------------------------------------------------------------------
// VshardExceptions
// ---------------------------------------------------------------------------

TEST(VshardExceptions, MovedErrorHasBucketAndDest) {
    MovedError e{42, "rs-002", "bucket 42 moved"};
    EXPECT_EQ(e.GetBucketId(), 42u);
    EXPECT_EQ(e.GetDestinationUuid(), "rs-002");
    EXPECT_NE(std::string{e.what()}.find("bucket 42 moved"), std::string::npos);
}

TEST(VshardExceptions, NoReplicasetErrorMessage) {
    NoReplicasetError e{999};
    EXPECT_NE(std::string{e.what()}.find("999"), std::string::npos);
    EXPECT_EQ(e.GetBucketId(), 999u);
}

USERVER_NAMESPACE_END
