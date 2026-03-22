# Lua vs C++ Vshard Semantic Review

This note summarizes the current comparison between the reference Lua router in
`~/src/vshard/vshard/router/init.lua` and the C++ implementation in
`tarantool/vshard/`.

It is intended as a reusable review artifact, separate from individual commit
messages.

## Current Status

- The current functional suite passes:
  - `71 passed`
- For the Tarantool `2.6.0` environment used here, the C++ proxy is now close
  to Lua vshard on the tested IPROTO router surface.
- The remaining gaps are narrow:
  - fuller `info()` parity for degraded-cluster states
  - true `return_raw` support on newer Tarantool runtimes with msgpack objects

## Implemented Parity Fixes

### 1. Generic `vshard.router.call()` mode parsing

Severity: Fixed

Status:
- Only exact `"read"` selects read mode.
- Any other mode string falls back to write mode, matching Lua.
- The original mode string is forwarded to `vshard.storage.call`.

References:
- [`~/src/vshard/vshard/router/init.lua:625`](/home/tsafin/src/vshard/vshard/router/init.lua#L625)
- [`~/src/vshard/vshard/router/init.lua:755`](/home/tsafin/src/vshard/vshard/router/init.lua#L755)
- [`tarantool/vshard/impl/iproto_server.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp)

### 2. Storage and user-function failures

Severity: Fixed

Status:
- Storage-side user-function failures are returned as Lua-style `nil, err`
  router data instead of top-level IPROTO errors.
- Differential tests require this behavior now.

References:
- [`~/src/vshard/vshard/router/init.lua:664`](/home/tsafin/src/vshard/vshard/router/init.lua#L664)
- [`tarantool/vshard/impl/vshard_envelope.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/vshard_envelope.cpp)
- [`tarantool/vshard/impl/vshard_proxy.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/vshard_proxy.cpp)

### 3. Discovery error classification

Severity: Fixed

Status:
- Discovery error precedence now follows the observed Lua behavior more closely.
- Cached-route connectivity failures are surfaced as Lua-style `nil, err`.
- Broader bucket discovery keeps the last meaningful probe failure instead of
  collapsing everything into generic no-route behavior.

References:
- [`~/src/vshard/vshard/router/init.lua:157`](/home/tsafin/src/vshard/vshard/router/init.lua#L157)
- [`tarantool/vshard/impl/topology_fetcher.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/topology_fetcher.cpp)

### 4. Call opts parsing

Severity: Mostly Fixed

Status:
- `timeout` and `request_timeout` are parsed and validated.
- `request_timeout > timeout` matches Lua validation behavior.
- `is_async` is implemented for the current tested IPROTO-visible behavior.
- `return_raw` matches Lua on Tarantool `2.6.0` by rejecting the option because
  msgpack-object support is unavailable on this runtime.

Important runtime note:
- This is full parity only for Tarantool `2.6.0`.
- On newer Tarantool versions with msgpack-object support, true `return_raw`
  behavior is still not implemented in C++.

References:
- [`~/src/vshard/vshard/router/init.lua:600`](/home/tsafin/src/vshard/vshard/router/init.lua#L600)
- [`~/src/vshard/vshard/router/init.lua:649`](/home/tsafin/src/vshard/vshard/router/init.lua#L649)
- [`tarantool/vshard/impl/iproto_server.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp)

### 5. Public router API coverage

Severity: Mostly Fixed

Status:
- The C++ IPROTO server now exposes and tests:
  - `vshard.router.callrw`
  - `vshard.router.callro`
  - `vshard.router.callbro`
  - `vshard.router.callre`
  - `vshard.router.callbre`
  - `vshard.router.call`
  - `vshard.router.map_callrw`
  - `vshard.router.bucket_id_strcrc32`
  - `vshard.router.bucket_id_mpcrc32`
  - `vshard.router.route`
  - `vshard.router.routeall`
  - `vshard.router.bootstrap`
  - `vshard.router.info`
  - `vshard.router.sync`

Remaining scope note:
- The broader Lua router module still contains internal/runtime behavior that
  is not reproduced as a one-to-one object model over IPROTO.

References:
- [`~/src/vshard/vshard/router/init.lua:1718`](/home/tsafin/src/vshard/vshard/router/init.lua#L1718)
- [`tarantool/vshard/impl/iproto_server.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp)

### 6. `map_callrw`

Severity: Fixed for the tested paths

Status:
- `storage_ref` and `storage_map` stay on the same Tarantool session per
  replicaset.
- Partial `bucket_ids` routing uses live discovery rather than stale cache.
- Differential tests cover:
  - plain call
  - timeout opts
  - partial `bucket_ids`
  - missing function parity

Remaining note:
- The suite now covers rebalance/error paths more broadly, but it still does
  not prove every possible timing-sensitive migration scenario.

### 7. `info()`

Severity: Mostly Fixed for healthy-cluster parity

Status:
- `vshard.router.info()` is exposed over IPROTO.
- Stable healthy-cluster fields are matched and tested, including:
  - bucket counters
  - `alerts`
  - `status`
  - `identification_mode`
  - `is_enabled`
  - master metadata and availability
  - active readable instance shape used for `replica`
  - `with_services` output shape, with normalization for dynamic `status_idx`
- Topology metadata such as configured UUIDs and names are retained and reused
  in the response.

Remaining gap:
- Full degraded-cluster parity is still not guaranteed for all Lua alert/status
  transitions and dynamic service state.

### 8. `route` and `routeall`

Severity: Mostly Fixed for IPROTO-visible parity

Status:
- `route()` is exposed and uses live bucket discovery to avoid stale-owner
  answers.
- `routeall()` no longer returns the old synthetic `{uuid: {uuid}}` placeholder.
- It now returns a richer serializable replicaset map with stable fields such
  as `uuid`, `master`, and `replica`.

Important runtime note:
- On Tarantool `2.6.0`, direct Lua IPROTO serialization of `routeall()` still
  fails because replicaset objects contain Lua functions.
- So strict differential comparison is limited to the serializable subset.

## What Current Tests Cover

Current passing tests cover:
- `callrw`
- `callro`
- `callbro`
- `callbre`
- `callre`
- generic `call`
- `map_callrw`
- `bucket_id_strcrc32`
- `bucket_id_mpcrc32`
- `bootstrap`
- `info`
- `route`
- `routeall` serializable subset parity
- `sync`
- strict missing-function parity
- invalid generic-call mode parity
- `request_timeout > timeout` validation parity
- observed `request_timeout` runtime parity on Tarantool `2.6.0`
- `is_async`
- `return_raw` rejection parity on Tarantool `2.6.0`
- discovery classification parity
- rebalance-path differential coverage for:
  - `WRONG_BUCKET`
  - `TRANSFER_IS_IN_PROGRESS`
  - `BUCKET_IS_LOCKED`
  - `NON_MASTER`
- differential error parity while ignoring implementation-specific
  `trace.file` and `trace.line`

References:
- [`tarantool/vshard/functional_tests/tests/test_routing.py`](/home/tsafin/src/userver/tarantool/vshard/functional_tests/tests/test_routing.py)
- [`tarantool/vshard/functional_tests/tests/test_errors.py`](/home/tsafin/src/userver/tarantool/vshard/functional_tests/tests/test_errors.py)
- [`tarantool/vshard/functional_tests/tests/test_differential.py`](/home/tsafin/src/userver/tarantool/vshard/functional_tests/tests/test_differential.py)

## Remaining Gaps

The meaningful remaining gaps are:

- full degraded-cluster `info()` parity for dynamic alert/status/service fields
- true `return_raw` support on Tarantool versions with msgpack-object support

## Bottom Line

For the current Tarantool `2.6.0` target and the tested IPROTO router surface,
the C++ implementation is now close to Lua vshard.

The remaining differences are no longer broad API holes. They are mostly:
- deeper monitoring/runtime-detail parity in `info()`
- newer-runtime `return_raw` support
