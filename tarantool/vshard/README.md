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
          "uuid": "ac522f65-a15e-4b1b-af2b-3a0a67d36fef",
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

### Unit tests (no cluster required)

```bash
cd build_debug
ctest -V -R userver-tarantool-vshard-sample_unittest
```

Tests cover: `BucketCalculator` (mpcrc32), `RoutingTable` (MOVED / TRANSFER),
`VshardError` parsing, `DecodeEnvelopeRaw` (fixarray / array16 / array32).

### Smoke test against a live cluster

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
Summary at 50 concurrent fibers:

| Mode | ops/sec | vs Lua router |
|------|--------:|---------------:|
| Lua vshard router (baseline) | 22,868 | 1.00× |
| **C++ proxy (IPROTO mode)** | **26,498** | **+17%** |
| C++ library embedded (no proxy hop) | 77,517 | +239% |

The proxy mode (+17%) wins over the Lua router with the same topology (client →
router/proxy → storage).  The embedded mode removes the proxy-process network
round-trip entirely and is ~3× faster still.

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
