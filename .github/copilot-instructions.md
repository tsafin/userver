# userver Copilot Instructions

userver is an open-source C++ async framework for building microservices, based on coroutines. All I/O is non-blocking — operations that would block a thread instead yield to the coroutine scheduler.

## Build & Test

Uses CMake with a Makefile wrapper. Build artifacts go to `build_debug/` or `build_release/`.

```bash
# Configure + build
make build-debug        # Debug + sanitizers (addr, ub)
make build-release      # Release

# Run all tests
make test-debug
make test-release

# Run a single test (after build)
cd build_debug && ctest -V -R <test_name_regex>

# In Docker (recommended for CI parity)
make docker-build-debug
make docker-test-debug

# Generate docs
make docs
```

Always use `sudo apt` (not plain `apt`) when installing system packages.

## Profiling with perf (WSL2)

WSL2 does not support hardware counters (`cycles`, `instructions`). Use software events and these mandatory flags:

```bash
# 1. Remove stale output (perf writes as root):
sudo rm -f /tmp/perf.data

# 2. Record — MUST use sudo, --no-buildid, -e cpu-clock:u, -o /tmp/perf.data:
sudo perf record --no-buildid -e cpu-clock:u -g -F 99 \
    -o /tmp/perf.data -- \
    env MY_ENV_VAR=value ./my_binary [args]

# 3. Verify perf.data was written (must be > 0 bytes):
ls -lh /tmp/perf.data

# 4. Report top functions (flat profile):
sudo perf report --stdio --no-children -i /tmp/perf.data 2>&1 | \
    grep -E "^\s+[0-9]" | head -30

# 5. Call-graph report (callers view):
sudo perf report --stdio -g fractal,0.5,caller -i /tmp/perf.data 2>&1 | head -80
```

**Key flags explained:**
- `sudo` — required, perf record fails silently without it in WSL2
- `--no-buildid` — prevents "Captured and wrote 0.000 MB (null)" silent failure
- `-e cpu-clock:u` — software event, `:u` = user-space only (avoids kernel noise)
- `-o /tmp/perf.data` — explicit output path (avoids permission issues in cwd)
- `env VAR=val` — pass environment variables to the profiled binary after `--`

**WSL2 limitations:**
- Hardware PMU counters (`-e cycles`, `-e instructions`, `-e branches`) show `<not supported>`
- `perf stat` works for task-clock and context-switches but not cycle counts

Key CMake flags (set via `CMAKE_DEBUG_FLAGS` / `CMAKE_RELEASE_FLAGS` in `Makefile.local` or directly):
- `-DUSERVER_SANITIZE="addr ub"` — enable sanitizers
- `-DUSERVER_FEATURE_POSTGRESQL=ON/OFF` — per-driver feature flags (same pattern for REDIS, MONGODB, GRPC, CLICKHOUSE, RABBITMQ, MYSQL)
- `-DUSERVER_NAMESPACE=my_ns` — override the `userver` namespace (also set `USERVER_NAMESPACE_BEGIN`/`USERVER_NAMESPACE_END`)
- `-DUSERVER_NO_WERROR=1` — disable `-Werror` (useful on macOS or non-CI builds)
- `-DUSERVER_IS_THE_ROOT_PROJECT=OFF` — when used as a submodule; disables building tests and samples

To use userver as a submodule:
```cmake
include(third_party/userver/cmake/SetupEnvironment.cmake)
add_subdirectory(third_party/userver)
```

## Repository Structure

```
universal/       # Coroutine-free utilities: formats (JSON/YAML/BSON), utils, strong typedefs
core/            # Coroutine engine, HTTP server/client, caches, task processors, logging
  include/userver/   # Public headers
  src/               # Internal implementation
  testing/           # utest/ubench helpers
  functional_tests/  # pytest-based integration tests
postgresql/      # Async PostgreSQL driver
redis/           # Async Redis driver
mongo/           # Async MongoDB driver (x86 only)
clickhouse/      # Async ClickHouse driver
grpc/            # Async gRPC driver
rabbitmq/        # Async RabbitMQ/AMQP driver
mysql/           # Async MySQL/MariaDB driver (experimental)
samples/         # Example services (hello_service, postgres_service, grpc_service, etc.)
testsuite/       # pytest_userver plugins for functional testing
third_party/     # Vendored deps (uboost_coro — vendored Boost.Context for coroutines)
cmake/           # CMake helpers and SetupEnvironment.cmake
scripts/         # Docs, codegen scripts, uctl admin tool
```

## Key Conventions

### Namespaces
All code is wrapped in `USERVER_NAMESPACE_BEGIN` / `USERVER_NAMESPACE_END` macros (defaults to `namespace userver`). This allows downstream projects to embed userver under a custom namespace. Always use these macros rather than literal namespace declarations.

### Header files
Public headers live under `include/userver/` with the path mirroring the namespace. Every public header starts with:
```cpp
#pragma once

/// @file userver/module/thing.hpp
/// @brief @copybrief module::Thing
```
Internal headers live under `src/` or `internal/` and are not installed.

### Unit tests
Use `#include <userver/utest/utest.hpp>` and the `UTEST()` / `UTEST_F()` / `UTEST_P()` macros — these run inside a coroutine context. Use plain `TEST()` only for tests that must not run in a coroutine. Benchmark files use `UBENCH()` from `<userver/ubench/ubench.hpp>`.

Test files are named `*_test.cpp`. Integration/chaos tests against real services are named `*_chtest.cpp`.

### Functional tests
Functional tests use `pytest` + `pytest_userver`. Each test directory has a `conftest.py` that declares which plugins to load:
```python
pytest_plugins = ['pytest_userver.plugins.core']
# For PostgreSQL: pytest_userver.plugins.postgresql
# For Redis:      pytest_userver.plugins.redis
# etc.
```
`pytest.ini` at the repo root configures `asyncio_mode = auto` and debug logging.

### Component system
Services are composed by registering components in a `ComponentList`:
```cpp
int main(int argc, char* argv[]) {
    const auto component_list =
        components::MinimalServerComponentList()
            .Append<MyHandler>();
    return utils::DaemonMain(argc, argv, component_list);
}
```
Each component declares `static constexpr std::string_view kName` used as its key in `static_config.yaml`.

### Configuration
- **Static config**: `static_config.yaml` (or `config.yaml`) — startup config for the `components_manager` tree.
- **Dynamic config**: runtime-adjustable values accessed via `dynamic_config::Source`; fallbacks in `dynamic_config_fallbacks.yaml`.
- **Secrets**: managed through `secdist` component (JSON file or env var `SECDIST_CONFIG`).

### Git commits
Do **not** add a `Co-authored-by:` trailer to commit messages.

### Async primitives
Never block a task-processor thread. Use engine primitives:
- `engine::AsyncNoSpan(...)` / `engine::Async(...)` to spawn tasks
- `engine::SleepFor(...)` instead of `std::this_thread::sleep_for`
- `engine::Mutex`, `engine::Semaphore`, `engine::SingleConsumerEvent` instead of std counterparts
- `fs::blocking::*` only on `fs-task-processor`; use `fs::async::*` on regular task processors
