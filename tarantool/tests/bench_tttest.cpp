/// @brief Throughput benchmark for the Tarantool userver connector.
///
/// Run: TARANTOOL_HOST=127.0.0.1 TARANTOOL_PORT=3301 ./userver-tarantool_tttest --gtest_filter="TarantoolBench*"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <iomanip>

#include <userver/clients/dns/resolver.hpp>
#include <userver/components/component_config.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/run_standalone.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/formats/yaml/serialize.hpp>
#include <userver/yaml_config/yaml_config.hpp>

#include <userver/storages/tarantool/query.hpp>
#include <storages/tarantool/impl/pool.hpp>
#include <storages/tarantool/impl/settings.hpp>
#include <storages/tarantool/impl/msgpack.hpp>
#include <userver/static_config/dns_client.hpp>

USERVER_NAMESPACE_BEGIN

namespace {

// ---- timing helpers --------------------------------------------------------

struct BenchResult {
    std::size_t ops;
    std::chrono::microseconds elapsed;
    double rps() const { return ops * 1e6 / elapsed.count(); }
    double us_per_op() const { return static_cast<double>(elapsed.count()) / ops; }
};

template <typename Fn>
BenchResult Time(std::size_t n, Fn&& fn) {
    const auto t0 = std::chrono::steady_clock::now();
    fn();
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0);
    return {n, elapsed};
}

void Print(const std::string& label, const BenchResult& r) {
    std::cout << "  " << std::left << std::setw(42) << label
              << std::right << std::setw(8) << static_cast<long>(r.rps()) << " op/s"
              << "  " << std::setw(5) << static_cast<long>(r.us_per_op()) << " µs/op"
              << "  [" << r.ops << " / " << r.elapsed.count()/1000 << "ms]\n"
              << std::flush;
}

// ---- settings helpers ------------------------------------------------------

storages::tarantool::impl::PoolSettings MakePool(
        const std::string& host, int port, std::size_t pool_size) {
    const auto yaml = formats::yaml::FromString(
        "initial_pool_size: " + std::to_string(pool_size) + "\n"
        "max_pool_size: "      + std::to_string(pool_size) + "\n"
        "connect_timeout_ms: 5000\n"
        "queue_timeout_ms: 5000\n");
    components::ComponentConfig cfg{yaml_config::YamlConfig{yaml, {}}};
    storages::tarantool::impl::EndpointSettings ep;
    ep.host = host;
    ep.port = static_cast<uint16_t>(port);
    return storages::tarantool::impl::PoolSettings{cfg, ep, {}};
}

formats::json::Value MakeTuple(uint64_t id, const std::string& val) {
    formats::json::ValueBuilder b{formats::json::Type::kArray};
    b.PushBack(id);
    b.PushBack(val);
    return b.ExtractValue();
}

// ---- micro-benchmark: pure JSON-encode cost --------------------------------

void BenchJsonEncode(std::size_t n, const std::string& val) {
    volatile std::size_t sink = 0;
    const auto r = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            auto v = MakeTuple(i, val);
            std::vector<uint8_t> buf;
            storages::tarantool::impl::EncodeJson(buf, v);
            sink += buf.size();
        }
    });
    Print("json→msgpack encode only (no network)", r);
    (void)sink;
}

// ---- network benchmark -----------------------------------------------------

void BenchPool(const std::string& host, int port,
               std::size_t pool_size, int concurrency,
               std::size_t per_coro, const std::string& val,
               clients::dns::Resolver& resolver,
               const std::string& label) {
    auto pool = std::make_unique<storages::tarantool::impl::Pool>(
        resolver, MakePool(host, port, pool_size));

    // Concurrent warmup: run one REPLACE per coroutine in parallel so that
    // every connection in the pool has its space-id cache populated before
    // the timed section begins.
    {
        std::vector<engine::TaskWithResult<void>> warmup_tasks;
        warmup_tasks.reserve(concurrency);
        for (int c = 0; c < concurrency; ++c) {
            warmup_tasks.push_back(engine::AsyncNoSpan([&pool, c] {
                for (int w = 0; w < 5; ++w)
                    pool->Execute(std::nullopt,
                        storages::tarantool::Query::Replace(
                            "kv", MakeTuple(uint64_t(c * 5 + w), "w")));
            }));
        }
        for (auto& t : warmup_tasks) t.Get();
    }

    const auto r = Time(std::size_t(concurrency) * per_coro, [&] {
        std::vector<engine::TaskWithResult<void>> tasks;
        tasks.reserve(concurrency);
        for (int c = 0; c < concurrency; ++c) {
            const uint64_t base = uint64_t(c) * per_coro;
            tasks.push_back(engine::AsyncNoSpan([&pool, base, per_coro, &val] {
                for (std::size_t i = 0; i < per_coro; ++i)
                    pool->Execute(std::nullopt,
                        storages::tarantool::Query::Replace(
                            "kv", MakeTuple(base + i, val)));
            }));
        }
        for (auto& t : tasks) t.Get();
    });
    Print(label, r);
}

// ---- test ------------------------------------------------------------------

TEST(TarantoolBench, InsertThroughput) {
    const char* host_env = std::getenv("TARANTOOL_HOST");
    const char* port_env = std::getenv("TARANTOOL_PORT");
    const std::string host = host_env ? host_env : "127.0.0.1";
    const int port = port_env ? std::stoi(port_env) : 3301;

    constexpr std::size_t kPerCoro = 2000;  // fixed per-coroutine op count
    const std::string val32(32, 'x');

    // ── Section 1: pure CPU cost (no network) ──────────────────────────────
    std::cout << "\n╔══ CPU cost (no network) ═══════════════════════════════╗\n";
    BenchJsonEncode(kPerCoro, val32);
    std::cout << "╚════════════════════════════════════════════════════════╝\n\n";

    // ── Section 2: vary ev threads ────────────────────────────────────────
    for (std::size_t ev_threads : {1u, 2u, 4u}) {
        engine::TaskProcessorPoolsConfig cfg;
        cfg.ev_threads_num = ev_threads;
        cfg.initial_coro_pool_size = 200;
        cfg.max_coro_pool_size = 1000;

        std::cout << "╔══ ev_threads=" << ev_threads
                  << "  host=" << host << ":" << port << " ══════════════════╗\n";

        engine::RunStandalone(4, cfg, [&] {
            ::userver::static_config::DnsClient dns_cfg{};
            clients::dns::Resolver resolver{
                engine::current_task::GetTaskProcessor(), dns_cfg};

            // sequential baseline
            BenchPool(host, port, 1, 1, kPerCoro, val32, resolver,
                      "sequential  (pool=1,  coro=1)");

            // pipelining: small fixed pool, varying coro count demonstrates
            // that a single connection can serve many concurrent in-flight ops.
            for (int coro : {4, 8, 16, 32, 64, 128}) {
                BenchPool(host, port, 4, coro, kPerCoro, val32, resolver,
                          "pipeline    (pool=4,  coro=" + std::to_string(coro) + ")");
            }

            // scale pool=coro (one dedicated connection per coroutine)
            for (int coro : {4, 8, 16, 32, 64, 128}) {
                BenchPool(host, port, coro, coro, kPerCoro, val32, resolver,
                          "concurrent  (pool=" + std::to_string(coro) +
                          ", coro=" + std::to_string(coro) + ")");
            }
        });
        std::cout << "╚════════════════════════════════════════════════════════╝\n\n";
    }
}

}  // namespace

USERVER_NAMESPACE_END
