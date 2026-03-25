# userver vshard Router Proxy

A C++ reimplementation of the [Tarantool vshard](https://github.com/tarantool/vshard)
router built on top of the userver async framework.

## What is it?

[vshard](https://github.com/tarantool/vshard) is Tarantool's horizontal sharding
library. In the classic architecture every application must talk to a **Lua router
process** that maps a bucket ID to the correct storage replicaset and retries on
`MOVED`/`TRANSFER` errors.

This service replaces that Lua router with a **pure C++ implementation** that:

* connects directly to all storage nodes using the userver Tarantool connector;
* maintains a lock-free routing table (bucket → replicaset) that is refreshed
  periodically and on demand when a `MOVED` reply is received;
* exposes the **same Tarantool binary (IPROTO) wire protocol** on its listen port so
  that any `net.box` client can connect and call `vshard.router.callrw` /
  `vshard.router.callro` without code changes;
* alternatively, embeds `VshardProxy` as a C++ library directly inside your service
  to eliminate the proxy hop entirely.

### Architecture

```
 ┌──────────────────┐               ┌────────────────────────────────────┐
 │  net.box client  │  IPROTO :3306 │  userver-tarantool-vshard-sample   │
 │  (Lua / C++ / …) │ ─────────────▶│  IprotoServer                      │
 └──────────────────┘               │    └─ VshardProxy ──┐              │
                                    └────────────────────── │ ─────────────┘
                                                            │ IPROTO
                                          ┌─────────────────▼──────────────┐
                                          │  Tarantool vshard storage nodes │
                                          │  RS1 :3301/:3302  RS2 :3303/:3304│
                                          └─────────────────────────────────┘
```

Or embedded:

```
 ┌──────────────────────────────────────────┐
 │   Your userver service                   │
 │   VshardProxyComponent (tarantool-vshard)│
 │     └─ VshardProxy::CallRW(key, func, …) │ ──▶  storage nodes
 └──────────────────────────────────────────┘
```

## Building

### Prerequisites

* CMake ≥ 3.14
* C++17 compiler (GCC 10+ / Clang 12+)
* userver dependencies (see repository root `README.md` or use Docker)

### CMake configuration

The Tarantool driver is **off by default**.  Enable it and disable unneeded
drivers to keep configure/build times short:

```bash
cmake -B build_release \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DUSERVER_FEATURE_TARANTOOL=ON \
    -DUSERVER_FEATURE_POSTGRESQL=OFF \
    -DUSERVER_FEATURE_REDIS=OFF \
    -DUSERVER_FEATURE_GRPC=OFF \
    -DUSERVER_FEATURE_CLICKHOUSE=OFF \
    -DUSERVER_FEATURE_KAFKA=OFF \
    -DUSERVER_FEATURE_RABBITMQ=OFF \
    -DUSERVER_FEATURE_MYSQL=OFF \
    -DUSERVER_FEATURE_ROCKS=OFF \
    -DUSERVER_FEATURE_SQLITE=OFF \
    -DUSERVER_FEATURE_OTLP=OFF \
    -DUSERVER_FEATURE_ODBC=OFF \
    -DUSERVER_FEATURE_MONGODB=OFF \
    -DUSERVER_BUILD_TESTS=ON \
    .
```

> **Tip:** put these flags in `Makefile.local` (gitignored) so `make build-release`
> picks them up automatically:
> ```makefile
> CMAKE_RELEASE_FLAGS = \
>     -DCMAKE_BUILD_TYPE=RelWithDebInfo \
>     -DUSERVER_FEATURE_TARANTOOL=ON \
>     -DUSERVER_FEATURE_POSTGRESQL=OFF \
>     -DUSERVER_FEATURE_REDIS=OFF \
>     -DUSERVER_FEATURE_GRPC=OFF \
>     -DUSERVER_FEATURE_CLICKHOUSE=OFF \
>     -DUSERVER_FEATURE_KAFKA=OFF \
>     -DUSERVER_FEATURE_RABBITMQ=OFF \
>     -DUSERVER_FEATURE_MYSQL=OFF \
>     -DUSERVER_FEATURE_ROCKS=OFF \
>     -DUSERVER_FEATURE_SQLITE=OFF \
>     -DUSERVER_FEATURE_OTLP=OFF \
>     -DUSERVER_FEATURE_ODBC=OFF \
>     -DUSERVER_FEATURE_MONGODB=OFF \
>     -DUSERVER_BUILD_TESTS=ON \
>     $(CMAKE_COMMON_FLAGS)
> ```

### Build (standalone)

```bash
# From the repository root (after configuring as above):
make build-release          # RelWithDebInfo, optimised

# Or debug + sanitizers:
make build-debug
```

This produces two binaries under `build_release/userver/tarantool/vshard/`:

| Binary | Description |
|--------|-------------|
| `userver-tarantool-vshard-sample` | Proxy server process |
| `userver-tarantool-vshard-bench` | In-process throughput benchmark (GTest) |

And one test binary:

| Binary | Description |
|--------|-------------|
| `userver-tarantool-vshard-sample_unittest` | Unit tests (no live cluster required) |

To build only the proxy (faster iteration):

```bash
cd build_release
cmake --build . --target userver-tarantool-vshard-sample -j6
```

## Running the proxy

### 1. Start a vshard cluster

The configs assume the standard 4-node example cluster from the vshard repository:

```
RS1  master :3301  replica :3302
RS2  master :3303  replica :3304
```

The bundled vshard example can be started with:

```bash
# (requires tarantool + vshard rock installed)
cd /path/to/vshard/example
make start
```

### 2. Prepare secdist

Copy the template and adjust credentials / hosts if needed:

```bash
cp tarantool/vshard/configs/secdist.json /tmp/vshard_secdist.json
```

`secdist.json` format:

```json
{
  "tarantool_vshard_settings": {
    "tarantool-vshard": {
      "bucket_count": 3000,
      "user": "storage",
      "password": "storage",
      "replicasets": [
        {
          "uuid": "cbf06940-0790-498b-948d-042b62cf3d29",
          "nodes": [
            {"host": "127.0.0.1", "port": 3301, "is_master": true},
            {"host": "127.0.0.1", "port": 3302}
          ]
        },
        {
          "uuid": "ac522f65-aa94-4134-9f64-51ee384f1a54",
          "nodes": [
            {"host": "127.0.0.1", "port": 3303, "is_master": true},
            {"host": "127.0.0.1", "port": 3304}
          ]
        }
      ]
    }
  }
}
```

### 3. Start the proxy

```bash
build_release/userver/tarantool/vshard/userver-tarantool-vshard-sample \
    --config tarantool/vshard/configs/static_config.yaml
```

The proxy listens on **port 3306** (configurable in `static_config.yaml`).

### Static config reference

```yaml
components_manager:
    components:
        iproto-vshard-server:
            port: 3306                      # IPROTO listen port
            task_processor: main-task-processor

        tarantool-vshard:
            secdist_alias: tarantool-vshard # key in secdist
            initial_pool_size: 4            # connections per node at startup
            max_pool_size: 16               # max connections per node
            # topology_refresh_interval: 60s
            # moved_refresh_min_interval: 1s
            # max_moved_retries: 1

        default-secdist-provider:
            config: /tmp/vshard_secdist.json
```

## Testing

There are three levels of testing: unit tests (offline), functional tests
(spin up a real vshard cluster), and manual smoke tests.

### Unit tests (no cluster required)

```bash
# Build unit tests:
cmake --build build_debug --target userver-tarantool-vshard-sample_unittest -j6

# Run via ctest:
cd build_debug && ctest -V -R userver-tarantool-vshard-sample_unittest

# Or run the binary directly (supports --gtest_filter):
build_debug/userver/tarantool/vshard/userver-tarantool-vshard-sample_unittest
```

Tests cover: `BucketCalculator` (mpcrc32), `RoutingTable` (MOVED / TRANSFER),
`VshardError` parsing, `DecodeEnvelopeRaw` (fixarray / array16 / array32),
`IprotoFrames` (ReadStr / ReadUint / ParseIprotoRequest / ParseIprotoResponse).

### Functional tests (automated vshard cluster)

Functional tests use **pytest** and automatically start a 2-replicaset vshard
cluster (2 masters + Lua router) in temporary directories, then start the C++
proxy as a subprocess.  Tests compare C++ proxy responses against the Lua router
(**lock-step differential testing**).

**Prerequisites:**

* `tarantool` binary in `$PATH`
* [vshard](https://github.com/tarantool/vshard) Lua module installed or available
  locally (e.g. `~/src/vshard`)
* Python packages: `pytest`, `tarantool` (`pip install pytest tarantool`)

**Run via ctest (recommended):**

```bash
# Configure with vshard path auto-detection (looks in ~/src/vshard, /usr/share/tarantool):
cmake --build build_debug --target userver-tarantool-vshard-sample -j6
cd build_debug && ctest -V -R testsuite-userver-tarantool-vshard

# Or specify vshard path explicitly during CMake configure:
cmake -DVSHARD_LUA_PATH=/path/to/vshard ...
```

**Run manually with pytest:**

```bash
cd tarantool/vshard/functional_tests

pytest tests/ \
    --proxy-binary=../../../build_debug/userver/tarantool/vshard/userver-tarantool-vshard-sample \
    --vshard-path=~/src/vshard \
    -v
```

**Test structure:**

| File | Description |
|------|-------------|
| `test_differential.py` | Lock-step: sends identical requests to both Lua and C++ routers, compares responses |
| `test_routing.py` | C++ proxy routing: callrw, callro, callbro, callbre, callre, generic call |
| `test_errors.py` | Error handling: invalid buckets, missing functions, timeouts, wrong-bucket retry |

**What the test harness does automatically:**

1. Starts 2 Tarantool storage masters with vshard configured
2. Starts a Lua vshard router (for differential comparison)
3. Bootstraps vshard and waits for bucket distribution
4. Starts the C++ proxy pointing at the same cluster
5. Runs all tests, then tears everything down

### Run all tests at once

```bash
# Build everything and run both unit + functional tests:
cmake --build build_debug --target userver-tarantool-vshard-sample \
                          --target userver-tarantool-vshard-sample_unittest -j6
cd build_debug && ctest -V -R 'userver-tarantool-vshard'
```

This runs both `userver-tarantool-vshard-sample_unittest` (unit) and
`testsuite-userver-tarantool-vshard` (functional) in one command.

### Smoke test against a live cluster

For quick manual verification with an already-running cluster:

```bash
# With the proxy running on :3306 and a live cluster:
tarantool -e "
  local c = require('net.box').connect('127.0.0.1:3306')
  local ok, res = c:call('vshard.router.callrw',
      {100, 'box.space.customer:replace', {{100, 100, 'hello'}}})
  print('replace', ok, res)
  ok, res = c:call('vshard.router.callro',
      {100, 'box.space.customer:get', {{100}}})
  print('get', ok, res)
"
```

Expected output:
```
replace true [[100, 100, 'hello']]
get     true [[100, 100, 'hello']]
```

## Benchmarking

### Option A — Lua bench client against the proxy (proxy mode)

Measures end-to-end throughput through the proxy process:

```bash
# Usage: bench_vshard.lua  <host:port>  <fibers>  <ops>

# Against C++ proxy on :3306:
tarantool tarantool/vshard/bench/bench_vshard.lua 127.0.0.1:3306 50 100000

# Against Lua vshard router on :3305 (baseline):
tarantool tarantool/vshard/bench/bench_vshard.lua 127.0.0.1:3305 50 100000
```

### Option B — C++ bench binary (embedded library mode)

Connects the `VshardProxy` library **directly** to storage nodes — no proxy
process, no extra network hop.  Uses GTest + userver `engine::RunStandalone`.

```bash
TARANTOOL_VSHARD_BENCH=1 \
VSHARD_BENCH_FIBERS=50 \
VSHARD_BENCH_OPS=100000 \
  build_release/userver/tarantool/vshard/userver-tarantool-vshard-bench \
  --gtest_filter="VshardBench*"
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TARANTOOL_VSHARD_BENCH` | unset | Must be set to `1` to run (prevents accidental execution) |
| `VSHARD_BENCH_FIBERS` | 50 | Concurrent coroutines |
| `VSHARD_BENCH_OPS` | 200000 | Total operations |

### Benchmark results (WSL2, loopback, 100k ops)

See [`bench/benchmark_results.md`](bench/benchmark_results.md) for full history.
Full scalability sweep (2026-03-25, after hot-path optimizations):

| Fibers | Lua router (ops/sec) | C++ proxy (ops/sec) | vs Lua |
|-------:|---------------------:|--------------------:|-------:|
| 10 | 9,409 | 11,117 | **+18%** |
| 20 | 15,375 | 18,313 | **+19%** |
| 50 | 24,355 | 29,064 | **+19%** |
| 100 | 33,938 | 35,734 | **+5%** |
| 150 | 37,338 | 42,588 | **+14%** |

C++ in-process (embedded library, 150 fibers): **185,375 op/s RW / 175,486 op/s RO** — **3.7× faster** than the Lua router.

Key observations:
- The **C++ proxy** is consistently **14–19% faster** than the Lua vshard router at all concurrency levels.
- Both the Lua router and the C++ proxy saturate at ~33–42k op/s above 100 fibers — the **Tarantool storage RTT is the bottleneck**, not the router.
- The **C++ in-process library** (no proxy hop, no extra loopback) reaches **185k op/s** at 150 fibers — **~3.7× faster** than the Lua router.

### Option C — Automated benchmark via CMake target

Runs the full scalability sweep (Lua router + C++ proxy + in-process), appends a
new section to `bench/benchmark_results.md`.  Auto-starts the vshard cluster if
it is not already running (requires `--vshard-path`).

```bash
# Configure once (add to Makefile.local or pass on command line):
cmake -DVSHARD_PATH=/path/to/vshard \
      -DVSHARD_BENCH_FIBER_COUNTS=10,20,50,100,150 \
      -DVSHARD_BENCH_OPS=100000 \
      -B build_release .

# Run benchmark (cluster auto-started/stopped if VSHARD_PATH is set):
cmake --build build_release --target benchmark-vshard
```

CMake variables for the `benchmark-vshard` target:

| Variable | Default | Description |
|----------|---------|-------------|
| `VSHARD_PATH` | unset | Path to vshard repo root (or `example/` subdir); used for `make start`/`make stop` |
| `VSHARD_LUA_ROUTER` | `localhost:3305` | Address of the running Lua vshard router |
| `VSHARD_BENCH_FIBER_COUNTS` | `10,20,50,100,150` | Comma-separated fiber counts to sweep |
| `VSHARD_BENCH_OPS` | `100000` | Operations per run per fiber count |
| `VSHARD_BENCH_CPP_PORT` | `3306` | Port for the temporary C++ proxy process |

Or invoke the script directly:

```bash
bash tarantool/vshard/bench/run_benchmarks.sh \
    --proxy-binary build_release/userver/tarantool/vshard/userver-tarantool-vshard-sample \
    --vshard-path /path/to/vshard \
    --fiber-counts 10,20,50,100,150 \
    --ops 100000
```

## Embedding `VshardProxy` in your service

Add `VshardProxyComponent` to your component list and inject it where needed:

```cpp
#include <vshard/vshard_proxy_component.hpp>

// In main():
component_list
    .Append<components::VshardProxyComponent>();

// In your handler:
auto& proxy_comp = context.FindComponent<components::VshardProxyComponent>();
auto proxy = proxy_comp.GetProxy();

// Call by explicit bucket ID:
auto result = proxy->CallRW(bucket_id, "box.space.orders:replace",
                            formats::msgpack::ValueBuilder{...});

// Call by sharding key (mpcrc32 hash computed in C++):
auto result = proxy->CallRW("order-key-123", "box.space.orders:replace",
                            formats::msgpack::ValueBuilder{...});
```

### Zero-copy path

For maximum throughput when forwarding raw msgpack bytes (e.g. in a proxy):

```cpp
// Forward raw msgpack args, receive raw msgpack result bytes:
auto raw = proxy->CallRawBytes(bucket_id, VshardMode::kRW,
                               func_name, args_ptr, args_len);
// raw.app_result_bytes — the application result as raw msgpack, no Value tree
```

## Implementation notes

* **Routing table** — `rcu::Variable<RoutingTable>` gives lock-free reads; writers
  hold a mutex only during the swap.
* **MOVED / TRANSFER retries** — `DoCall` / `DoCallRawBytes` retry up to
  `max_moved_retries` times, refreshing the routing table between attempts.
* **Zero-copy hot path** — `CallRawBytes` + `DecodeEnvelopeRaw` avoid all
  `formats::msgpack::Value` allocations on the success path. Tarantool encodes
  multi-return IPROTO_CALL results as `array32` (not `fixarray`); the decoder
  handles all three formats (fixarray / array16 / array32).
* **Topology refresh** — a background `PeriodicTask` calls
  `vshard.router.buckets_discovery()` every `topology_refresh_interval` and on
  every `MOVED` reply (rate-limited by `moved_refresh_min_interval`).
