/// @file bench/bench_vshard.cpp
/// @brief VshardProxy throughput benchmark.
///
/// Mirrors vshard example/generate_load.lua so results are directly comparable:
///   - 50 concurrent coroutines  (VSHARD_BENCH_FIBERS, default 50)
///   - total 200 000 ops         (VSHARD_BENCH_OPS,    default 200000)
///   - each calls echo() via vshard.storage.call, bucket_id round-robin
///
/// Run (cluster must be up — cd vshard/example && make start):
///   TARANTOOL_VSHARD_BENCH=1 ./userver-tarantool-vshard-bench
///       --gtest_filter="VshardBench*"
///
/// Storage nodes expected at:
///   RS1: master 127.0.0.1:3301  replica 127.0.0.1:3302
///   RS2: master 127.0.0.1:3303  replica 127.0.0.1:3304
///   (user=storage  password=storage — from vshard/example/localcfg.lua)

#pragma GCC diagnostic ignored "-Wvolatile"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <userver/clients/dns/resolver.hpp>
#include <userver/components/component_config.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/run_standalone.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/formats/yaml/serialize.hpp>
#include <userver/static_config/dns_client.hpp>
#include <userver/yaml_config/yaml_config.hpp>

#include <vshard/impl/vshard_proxy.hpp>

USERVER_NAMESPACE_BEGIN

namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

components::ComponentConfig MakePoolConfig(std::size_t pool_size) {
    const auto yaml = formats::yaml::FromString(
        "initial_pool_size: " + std::to_string(pool_size) + "\n"
        "max_pool_size: "      + std::to_string(pool_size) + "\n"
        "connect_timeout_ms: 5000\n"
        "queue_timeout_ms: 5000\n");
    return components::ComponentConfig{yaml_config::YamlConfig{yaml, {}}};
}

storages::tarantool::vshard::VshardProxySettings MakeVshardSettings() {
    using RS   = storages::tarantool::vshard::impl::ReplicasetConfig;
    using Node = RS::NodeConfig;

    storages::tarantool::vshard::impl::VshardTopologyConfig topo;
    topo.bucket_count = 3000;
    topo.user = "storage";
    topo.password = "storage";

    RS rs1;
    rs1.uuid = "cbf06940-0790-498b-948d-042b62cf3d29";
    rs1.nodes.push_back(Node{"127.0.0.1", 3301, /*is_master=*/true});
    rs1.nodes.push_back(Node{"127.0.0.1", 3302, false});

    RS rs2;
    rs2.uuid = "ac522f65-aa94-4134-9f64-51ee384f1a54";
    rs2.nodes.push_back(Node{"127.0.0.1", 3303, /*is_master=*/true});
    rs2.nodes.push_back(Node{"127.0.0.1", 3304, false});

    topo.replicasets.push_back(std::move(rs1));
    topo.replicasets.push_back(std::move(rs2));

    storages::tarantool::vshard::VshardProxySettings s;
    s.topology = std::move(topo);
    s.topology_refresh_interval = std::chrono::seconds{60};
    s.moved_refresh_min_interval = std::chrono::seconds{1};
    s.max_moved_retries = 1;
    return s;
}

struct BenchResult {
    std::size_t ops;
    std::chrono::microseconds elapsed;
    std::size_t errors{0};
    double rps()       const { return ops * 1e6 / elapsed.count(); }
    double us_per_op() const { return static_cast<double>(elapsed.count()) / ops; }
};

void Print(const std::string& label, const BenchResult& r) {
    std::cout << "  " << std::left  << std::setw(48) << label
              << std::right << std::setw(9) << static_cast<long>(r.rps()) << " op/s"
              << "  " << std::setw(6) << static_cast<long>(r.us_per_op()) << " µs/op"
              << "  errors=" << r.errors
              << "\n" << std::flush;
}

BenchResult RunBench(storages::tarantool::vshard::VshardProxy& proxy,
                     int fibers, std::size_t per_fiber,
                     storages::tarantool::vshard::impl::CallMode mode,
                     int bucket_count = 3000) {
    std::atomic<std::size_t> errors{0};

    const auto t0 = std::chrono::steady_clock::now();

    std::vector<engine::TaskWithResult<void>> tasks;
    tasks.reserve(static_cast<std::size_t>(fibers));

    for (int f = 0; f < fibers; ++f) {
        const int start_bid = (f * (bucket_count / fibers)) % bucket_count + 1;
        tasks.push_back(engine::AsyncNoSpan(
            [&proxy, mode, start_bid, per_fiber, bucket_count, &errors] {
                int bid = start_bid;
                for (std::size_t i = 0; i < per_fiber; ++i) {
                    try {
                        auto args = formats::msgpack::ValueBuilder::Array();
                        args.PushBack(formats::msgpack::ValueBuilder{
                            static_cast<uint64_t>(bid)});
                        proxy.Call(static_cast<uint32_t>(bid), mode, "echo",
                                   std::move(args));
                    } catch (const std::exception&) {
                        ++errors;
                    }
                    bid = (bid % bucket_count) + 1;
                }
            }));
    }
    for (auto& t : tasks) t.Get();

    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0);
    return {static_cast<std::size_t>(fibers) * per_fiber, elapsed,
            errors.load()};
}

static int EnvInt(const char* name, int def) {
    const char* v = std::getenv(name);
    return v ? std::stoi(v) : def;
}

// ---------------------------------------------------------------------------
// Lua baseline numbers (measured locally with generate_load.lua)
// ---------------------------------------------------------------------------
constexpr double kLuaReplaceOps = 50556.0;   // op/s, 50 fibers, replace
constexpr double kLuaGetOps     = 65821.0;   // op/s, 50 fibers, get

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

TEST(VshardBench, Throughput) {
    if (!std::getenv("TARANTOOL_VSHARD_BENCH")) {
        GTEST_SKIP() << "Set TARANTOOL_VSHARD_BENCH=1 to run this benchmark";
    }

    const int fibers    = EnvInt("VSHARD_BENCH_FIBERS", 50);
    const int total_ops = EnvInt("VSHARD_BENCH_OPS", 200000);
    const int per_fiber = total_ops / fibers;

    std::cout << "\n╔══ VshardProxy C++ benchmark ════════════════════════════╗\n";
    std::cout << "  fibers=" << fibers << "  total_ops=" << total_ops
              << "  per_fiber=" << per_fiber << "\n\n";

    engine::TaskProcessorPoolsConfig engine_cfg;
    engine_cfg.ev_threads_num = 4;
    engine_cfg.initial_coro_pool_size = 200;
    engine_cfg.max_coro_pool_size = 1000;

    engine::RunStandalone(4, engine_cfg, [&] {
        ::userver::static_config::DnsClient dns_cfg{};
        clients::dns::Resolver resolver{
            engine::current_task::GetTaskProcessor(), dns_cfg};

        auto pool_cfg = MakePoolConfig(/*pool_size=*/4);
        auto vshard_s = MakeVshardSettings();

        std::cout << "  Connecting (RS1:3301/3302, RS2:3303/3304, user=storage)...\n";
        storages::tarantool::vshard::VshardProxy proxy{
            resolver, pool_cfg, std::move(vshard_s)};

        std::cout << "  Warmup (" << fibers << " fibers × 20 ops)...\n\n";
        RunBench(proxy, fibers, 20,
                 storages::tarantool::vshard::impl::CallMode::kReadWrite);

        std::cout << "── read-write (vshard.storage.call + echo) ─────────────\n";
        const auto rw = RunBench(proxy, fibers,
                                  static_cast<std::size_t>(per_fiber),
                                  storages::tarantool::vshard::impl::CallMode::kReadWrite);
        Print("C++ VshardProxy RW (fibers=" + std::to_string(fibers) + ")", rw);

        std::cout << "\n── read-only  (vshard.storage.call + echo) ─────────────\n";
        const auto ro = RunBench(proxy, fibers,
                                  static_cast<std::size_t>(per_fiber),
                                  storages::tarantool::vshard::impl::CallMode::kReadOnly);
        Print("C++ VshardProxy RO (fibers=" + std::to_string(fibers) + ")", ro);

        std::cout << "\n── Lua vshard router baseline ───────────────────────────\n";
        std::cout << "  replace : " << static_cast<long>(kLuaReplaceOps)
                  << " op/s   558 µs/op  (generate_load.lua --fibers 50)\n";
        std::cout << "  get     : " << static_cast<long>(kLuaGetOps)
                  << " op/s   460 µs/op  (generate_load.lua --fibers 50)\n\n";

        if (rw.errors == 0 && ro.errors == 0) {
            std::cout << std::fixed << std::setprecision(2);
            std::cout << "  RW speedup vs Lua router: " << rw.rps() / kLuaReplaceOps << "x\n";
            std::cout << "  RO speedup vs Lua router: " << ro.rps() / kLuaGetOps     << "x\n";
        }
        std::cout << "╚═════════════════════════════════════════════════════════╝\n";
    });
}

}  // namespace

USERVER_NAMESPACE_END
