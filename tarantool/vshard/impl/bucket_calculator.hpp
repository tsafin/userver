#pragma once

/// @file vshard/impl/bucket_calculator.hpp
/// @brief Bucket-ID computation matching vshard's mpcrc32 and strcrc32.

#include <cstdint>
#include <string_view>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

/// Compute bucket IDs from application sharding keys.
///
/// vshard ships two bucket-ID functions:
///
/// - **mpcrc32** (default, recommended): `CRC32(msgpack(key)) % N + 1`
///   For integer keys the key is msgpack-encoded before hashing;
///   for string keys the raw bytes are hashed directly (no msgpack header).
///
/// - **strcrc32** (legacy, deprecated): `CRC32(tostring(key)) % N + 1`
///   Integers are converted to their decimal string representation first.
///
/// Use @ref BucketIdMpcrc32 for all new code. Use @ref BucketIdStrcrc32 only
/// when the storage cluster was bootstrapped with the legacy function.
class BucketCalculator final {
 public:
    /// @param bucket_count Total number of buckets, fixed at cluster bootstrap
    ///                     (typically 3000 or 32768).
    explicit BucketCalculator(uint32_t bucket_count) noexcept
        : bucket_count_{bucket_count} {}

    // ---- mpcrc32 (default) --------------------------------------------------

    /// Compute bucket_id for a string key using mpcrc32.
    /// Matches `vshard.router.bucket_id_mpcrc32(key)` for string input.
    uint32_t BucketIdMpcrc32(std::string_view key) const noexcept;

    /// Compute bucket_id for a signed integer key using mpcrc32.
    /// The integer is msgpack-encoded into a small stack buffer before hashing.
    uint32_t BucketIdMpcrc32(int64_t key) const noexcept;

    /// Compute bucket_id for an unsigned integer key using mpcrc32.
    uint32_t BucketIdMpcrc32(uint64_t key) const noexcept;

    // ---- strcrc32 (legacy) --------------------------------------------------

    /// Compute bucket_id for a string key using strcrc32.
    /// Equivalent to mpcrc32 for strings (raw bytes, no msgpack header).
    uint32_t BucketIdStrcrc32(std::string_view key) const noexcept;

    /// Compute bucket_id for a signed integer using strcrc32.
    /// The integer is converted to its decimal string representation first.
    uint32_t BucketIdStrcrc32(int64_t key) const noexcept;

    // ---- generic default (mpcrc32) ------------------------------------------

    /// @{
    /// Convenience: delegates to mpcrc32 variants.
    uint32_t Calculate(std::string_view key) const noexcept {
        return BucketIdMpcrc32(key);
    }
    uint32_t Calculate(int64_t key) const noexcept {
        return BucketIdMpcrc32(key);
    }
    uint32_t Calculate(uint64_t key) const noexcept {
        return BucketIdMpcrc32(key);
    }
    /// @}

    uint32_t GetBucketCount() const noexcept { return bucket_count_; }

 private:
    uint32_t bucket_count_;

    uint32_t Crc32ToId(uint32_t crc) const noexcept {
        return crc % bucket_count_ + 1;
    }
};

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
