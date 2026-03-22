# Lua vs C++ Vshard Semantic Review

This note summarizes a systematic comparison between the reference Lua router in
`~/src/vshard/vshard/router/init.lua` and the C++ implementation in
`tarantool/vshard/`.

It is intended as a reusable review artifact, separate from the current pytest
status.

## Current Status

- The current functional suite passes for the exercised subset:
  - `49 passed`
- That passing result does not imply full semantic equivalence with Lua vshard.
- The C++ implementation is close for the tested `call*` hot path, but there
  are still observable API and behavior gaps outside that subset.

## Findings

### 1. Generic `vshard.router.call()` mode parsing is Lua-equivalent now

Severity: Fixed

Status:
- Fixed in the IPROTO path.
- Only exact `"read"` selects read mode.
- Any other string falls back to write mode, matching Lua.
- The exact original mode string is forwarded to `vshard.storage.call`.

References:
- [`~/src/vshard/vshard/router/init.lua:625`](/home/tsafin/src/vshard/vshard/router/init.lua#L625)
- [`~/src/vshard/vshard/router/init.lua:755`](/home/tsafin/src/vshard/vshard/router/init.lua#L755)
- [`tarantool/vshard/impl/iproto_server.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp)

## 2. Storage/user-function errors are surfaced like Lua now

Severity: Fixed

Status:
- Fixed in the IPROTO path.
- Storage-side user-function failures are returned as router-style
  `nil, err` data instead of top-level IPROTO errors.

References:
- [`~/src/vshard/vshard/router/init.lua:664`](/home/tsafin/src/vshard/vshard/router/init.lua#L664)
- [`tarantool/vshard/impl/vshard_envelope.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/vshard_envelope.cpp)
- [`tarantool/vshard/impl/vshard_proxy.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/vshard_proxy.cpp)

## 3. Discovery error classification is weaker than Lua

Severity: Medium

Lua behavior:
- Distinguishes:
  - bucket not found anywhere
  - unreachable replicaset during discovery
  - wrong bucket

Reference:
- [`~/src/vshard/vshard/router/init.lua:157`](/home/tsafin/src/vshard/vshard/router/init.lua#L157)

C++ behavior:
- Probe exceptions are swallowed during bucket discovery.
- Failure collapses to generic no-route behavior later.

Reference:
- [`tarantool/vshard/impl/topology_fetcher.cpp:152`](/home/tsafin/src/userver/tarantool/vshard/impl/topology_fetcher.cpp#L152)
- [`tarantool/vshard/impl/vshard_proxy.cpp:263`](/home/tsafin/src/userver/tarantool/vshard/impl/vshard_proxy.cpp#L263)

Impact:
- Less precise operational error reporting.

## 4. Option semantics are still only partially matched

Severity: Medium

Lua behavior:
- Supports and validates:
  - `timeout`
  - `request_timeout`
  - `return_raw`
  - `is_async`

Reference:
- [`~/src/vshard/vshard/router/init.lua:600`](/home/tsafin/src/vshard/vshard/router/init.lua#L600)

C++ behavior:
- Now parses and validates:
  - `timeout`
  - `request_timeout`
- Matches Lua for:
  - `request_timeout > timeout` validation
  - bad opts type for the supported timeout fields
- Still does not implement:
  - `return_raw`
  - `is_async`

Observed note for the local target setup:
- On Tarantool `2.6.0`, the differential test with
  `sleep(0.2), timeout=1.0, request_timeout=0.05` succeeded through both the
  Lua router and the C++ proxy.
- So the verified parity here is validation parity, not a proven independent
  runtime effect of `request_timeout` in this environment.

Reference:
- [`tarantool/vshard/impl/iproto_server.cpp`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp)

Impact:
- Same call shape does not imply same semantics.

## 5. Public router API coverage is partial

Severity: Medium

Lua exports:
- `callro`
- `callbro`
- `callrw`
- `callre`
- `callbre`
- `map_callrw`
- `route`
- `routeall`
- `bucket_id_strcrc32`
- `bucket_id_mpcrc32`
- `bootstrap`
- `info`
- `sync`
- more

Reference:
- [`~/src/vshard/vshard/router/init.lua:1718`](/home/tsafin/src/vshard/vshard/router/init.lua#L1718)

C++ IPROTO server currently exposes:
- `vshard.router.callrw`
- `vshard.router.callro`
- `vshard.router.callbro`
- `vshard.router.callre`
- `vshard.router.callbre`
- `vshard.router.call`
- `vshard.router.bucket_id_mpcrc32`
- `vshard.router.routeall`

Reference:
- [`tarantool/vshard/impl/iproto_server.cpp:823`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp#L823)

Notes:
- C++ has internal support for some broader functionality, for example
  `MapCallRW()`, but it is not exposed as Lua-compatible IPROTO API.

Reference:
- [`tarantool/vshard/impl/vshard_proxy.hpp:151`](/home/tsafin/src/userver/tarantool/vshard/impl/vshard_proxy.hpp#L151)

Impact:
- Full Lua router API equivalence is not yet present.

## 6. `routeall` is not semantically equivalent

Severity: Low

Lua behavior:
- Returns actual replicaset objects.

Reference:
- [`~/src/vshard/vshard/router/init.lua:1191`](/home/tsafin/src/vshard/vshard/router/init.lua#L1191)

C++ behavior:
- Returns synthesized map entries of the form `{uuid: {uuid: uuid}}`.

Reference:
- [`tarantool/vshard/impl/iproto_server.cpp:638`](/home/tsafin/src/userver/tarantool/vshard/impl/iproto_server.cpp#L638)

Impact:
- Acceptable over IPROTO for some clients, but not the same router API.

## What Current Tests Cover

Current passing tests cover mainly:
- `callrw`
- `callro`
- `callbro`
- `callbre`
- `callre`
- generic `call`
- `bucket_id_mpcrc32`
- synthetic `routeall`
- some happy-path differential comparison
- strict missing-function parity
- invalid generic-call mode parity
- `request_timeout > timeout` validation parity
- observed `request_timeout` runtime parity on Tarantool `2.6.0`

References:
- [`tarantool/vshard/functional_tests/tests/test_routing.py`](/home/tsafin/src/userver/tarantool/vshard/functional_tests/tests/test_routing.py)
- [`tarantool/vshard/functional_tests/tests/test_differential.py`](/home/tsafin/src/userver/tarantool/vshard/functional_tests/tests/test_differential.py)

## Coverage Gaps

The passing suite does not currently test:

- absent generic-call shard opts
- `return_raw`
- `is_async`
- forced `WRONG_BUCKET`
- forced `TRANSFER_IS_IN_PROGRESS`
- forced `BUCKET_IS_LOCKED`
- forced `NON_MASTER`
- `bucket_id_strcrc32`
- `route`
- `map_callrw`
- `bootstrap`
- `info`
- `sync`

## Recommended Next Tests

1. Add a generic `vshard.router.call()` mode matrix.
   - `mode='read'`
   - `mode='write'`
   - `mode='junk'`
   - `opts={mode='junk'}`
   - missing opts

2. Add opts tests for the remaining unsupported options.
   - `return_raw`
   - `is_async` if intended to be supported

3. Add API-coverage tests for utility methods.
   - `bucket_id_strcrc32`
   - `route`
   - `map_callrw`

4. Add rebalance/error-injection tests.
   - `WRONG_BUCKET`
   - `TRANSFER_IS_IN_PROGRESS`
   - `BUCKET_IS_LOCKED`
   - `NON_MASTER`

5. If full Lua API parity is a goal, add tests for:
   - `bootstrap`
   - `info`
   - `sync`

## Bottom Line

For the currently tested subset, the C++ proxy is good enough to pass
functional and differential pytest coverage.

For full Lua vshard router equivalence, it is not finished yet.

The biggest remaining semantic gaps are:
- discovery error classification
- partial opts support
- missing public APIs
