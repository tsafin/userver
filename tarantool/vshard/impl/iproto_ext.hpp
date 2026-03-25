#pragma once

/// @file vshard/impl/iproto_ext.hpp
/// @brief IPROTO constants missing from tntcxx IprotoConstants.hpp.
///
/// These opcodes and keys were added in Tarantool 2.10 / 3.x and are not yet
/// present in the vendored tntcxx. Used only by the vshard proxy implementation.

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace iproto_ext {

/// Additional IPROTO body/header map keys (Tarantool 2.10+ / 3.x)
enum Key : uint8_t {
    kVersion   = 0x54,
    kFeatures  = 0x55,
    kTimeout   = 0x56,
    kEventKey  = 0x57,
    kEventData = 0x58,
    kAuthType  = 0x5b,
    kReplicasetName = 0x5c,
    kInstanceName   = 0x5d,
};

/// Additional IPROTO request types (Tarantool 2.10+ / 3.x)
enum Type : uint32_t {
    kId        = 73,  ///< IPROTO_ID — capability negotiation
    kWatch     = 74,  ///< IPROTO_WATCH — subscribe to a key
    kUnwatch   = 75,  ///< IPROTO_UNWATCH — unsubscribe
    kEvent     = 76,  ///< IPROTO_EVENT — server push notification
    kWatchOnce = 77,  ///< IPROTO_WATCH_ONCE — one-shot watch
};

/// IPROTO feature bits (used in IPROTO_ID FEATURES array)
enum Feature : uint32_t {
    kStreams            = 0,
    kTransactions       = 1,
    kErrorExtension     = 2,
    kWatchers           = 3,
    kPagination         = 4,
    kSpaceAndIndexNames = 5,
    kWatchOnce          = 6,
};

}  // namespace iproto_ext

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
