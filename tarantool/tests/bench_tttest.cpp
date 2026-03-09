/// @brief Throughput benchmark for the Tarantool userver connector.
///
/// Run: TARANTOOL_HOST=127.0.0.1 TARANTOOL_PORT=3301 ./userver-tarantool_tttest --gtest_filter="TarantoolBench*"
/// CPU-only benchmarks: ./userver-tarantool_tttest --gtest_filter="TarantoolCpuBench*"

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
#include <userver/formats/json/value_builder.hpp>
#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
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

formats::msgpack::ValueBuilder MakeTuple(uint64_t id, const std::string& val) {
    auto b = formats::msgpack::ValueBuilder::Array();
    b.PushBack(formats::msgpack::ValueBuilder{id});
    b.PushBack(formats::msgpack::ValueBuilder{val});
    return b;
}

// ---- Local re-implementation of old JSON→msgpack encode path ---------------
// (Mirrors the removed EncodeJson to compare against the new ValueBuilder path)

static void EncodeJsonLocal(std::vector<uint8_t>& out,
                            const formats::json::Value& v) {
    using namespace storages::tarantool::impl;
    if (v.IsNull()) {
        out.push_back(0xc0);  // nil
    } else if (v.IsBool()) {
        out.push_back(v.As<bool>() ? 0xc3 : 0xc2);
    } else if (v.IsInt64()) {
        EncodeUint(out, static_cast<uint64_t>(v.As<int64_t>()));
    } else if (v.IsUInt64()) {
        EncodeUint(out, v.As<uint64_t>());
    } else if (v.IsDouble()) {
        double d = v.As<double>();
        out.push_back(0xcb);
        uint64_t bits; std::memcpy(&bits, &d, 8);
        for (int s = 56; s >= 0; s -= 8) out.push_back(static_cast<uint8_t>(bits >> s));
    } else if (v.IsString()) {
        EncodeStr(out, v.As<std::string>());
    } else if (v.IsArray()) {
        EncodeArray(out, static_cast<uint32_t>(v.GetSize()));
        for (const auto& elem : v) EncodeJsonLocal(out, elem);
    }
}

// ---- CPU micro-benchmarks: old JSON path vs new ValueBuilder path ----------

void BenchEncodeComparison(std::size_t n, const std::string& val) {
    using namespace storages::tarantool::impl;
    volatile std::size_t sink = 0;

    // --- Old path: build JSON Value, then EncodeJson ---
    const auto r_old = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            formats::json::ValueBuilder b{formats::json::Type::kArray};
            b.PushBack(static_cast<int64_t>(i));
            b.PushBack(val);
            auto jv = b.ExtractValue();
            std::vector<uint8_t> buf;
            EncodeJsonLocal(buf, jv);
            sink += buf.size();
        }
    });
    Print("encode: JSON Value → EncodeJson (OLD)", r_old);

    // --- New path: ValueBuilder → ToBytes ---
    const auto r_new = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            auto b = formats::msgpack::ValueBuilder::Array();
            b.PushBack(formats::msgpack::ValueBuilder{static_cast<uint64_t>(i)});
            b.PushBack(formats::msgpack::ValueBuilder{val});
            auto bytes = b.ToBytes();
            sink += bytes.size();
        }
    });
    Print("encode: ValueBuilder → ToBytes (NEW)", r_new);

    const double speedup = static_cast<double>(r_old.elapsed.count()) /
                           static_cast<double>(r_new.elapsed.count());
    std::cout << "  speedup: " << std::fixed << std::setprecision(2) << speedup << "x\n";
    (void)sink;
}

// ---- CPU decode comparison: MsgPackDecode(JSON) vs Value::FromBytes --------

void BenchDecodeComparison(std::size_t n) {
    using namespace storages::tarantool::impl;

    // Build a sample response: array of [uint64, string, uuid_ext]
    TntUuid uuid;
    uuid.bytes = {0x12,0x34,0x56,0x78, 0x12,0x34, 0x56,0x78,
                  0x12,0x34, 0x56,0x78,0x9a,0xbc,0xde,0xf0};

    std::vector<uint8_t> row;
    EncodeArray(row, 3);
    EncodeUint(row, 42u);
    EncodeStr(row, "hello_value");
    EncodeUuid(row, uuid);

    volatile std::size_t sink = 0;

    // --- Old path: MsgPackDecode → JSON, access UUID as string ---
    const auto r_old = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            auto v = MsgPackDecode(row);
            sink += v[0].As<int64_t>();
            sink += v[1].As<std::string>().size();
            sink += v[2].As<std::string>().size();  // UUID decoded eagerly
        }
    });
    Print("decode: MsgPackDecode → json::Value (OLD)", r_old);

    // --- New path: Value::FromBytes, lazy ext decode ---
    const auto r_new = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            auto v = formats::msgpack::Value::FromBytes(row.data(), row.size());
            sink += v[0].As<uint64_t>();
            sink += v[1].As<std::string>().size();
            sink += v[2].AsUuid().bytes[0];  // lazy UUID decode only when accessed
        }
    });
    Print("decode: msgpack::Value::FromBytes (NEW)", r_new);

    const double speedup = static_cast<double>(r_old.elapsed.count()) /
                           static_cast<double>(r_new.elapsed.count());
    std::cout << "  speedup: " << std::fixed << std::setprecision(2) << speedup << "x\n";
    (void)sink;
}

// ---- CPU decode: lazy (don't access UUID) vs eager -------------------------

void BenchDecodeSkipExt(std::size_t n) {
    using namespace storages::tarantool::impl;

    // Same row as above
    TntUuid uuid;
    uuid.bytes = {0x12,0x34,0x56,0x78, 0x12,0x34, 0x56,0x78,
                  0x12,0x34, 0x56,0x78,0x9a,0xbc,0xde,0xf0};
    std::vector<uint8_t> row;
    EncodeArray(row, 3);
    EncodeUint(row, 42u);
    EncodeStr(row, "hello_value");
    EncodeUuid(row, uuid);

    volatile std::size_t sink = 0;

    // --- Old path: MsgPackDecode always decodes UUID even if not used ---
    const auto r_old = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            auto v = MsgPackDecode(row);
            sink += v[0].As<int64_t>();
            sink += v[1].As<std::string>().size();
            // UUID field not accessed — but MsgPackDecode parsed it eagerly
        }
    });
    Print("decode skip ext: MsgPackDecode (OLD)", r_old);

    // --- New path: Value::FromBytes never decodes UUID if not accessed ---
    const auto r_new = Time(n, [&] {
        for (std::size_t i = 0; i < n; ++i) {
            auto v = formats::msgpack::Value::FromBytes(row.data(), row.size());
            sink += v[0].As<uint64_t>();
            sink += v[1].As<std::string>().size();
            // UUID field not accessed — zero decode cost
        }
    });
    Print("decode skip ext: msgpack::FromBytes (NEW)", r_new);

    const double speedup = static_cast<double>(r_old.elapsed.count()) /
                           static_cast<double>(r_new.elapsed.count());
    std::cout << "  speedup (lazy skip): " << std::fixed << std::setprecision(2) << speedup << "x\n";
    (void)sink;
}

// ---- ping benchmark (measures pure client+protocol overhead, no server work) -

void BenchPing(const std::string& host, int port,
               std::size_t pool_size, int concurrency,
               std::size_t per_coro,
               clients::dns::Resolver& resolver,
               const std::string& label) {
    auto pool = std::make_unique<storages::tarantool::impl::Pool>(
        resolver, MakePool(host, port, pool_size));

    // Warmup: establish all connections
    {
        std::vector<engine::TaskWithResult<void>> warmup_tasks;
        warmup_tasks.reserve(concurrency);
        for (int c = 0; c < concurrency; ++c)
            warmup_tasks.push_back(engine::AsyncNoSpan([&pool] {
                pool->Ping();
            }));
        for (auto& t : warmup_tasks) t.Get();
    }

    const auto r = Time(std::size_t(concurrency) * per_coro, [&] {
        std::vector<engine::TaskWithResult<void>> tasks;
        tasks.reserve(concurrency);
        for (int c = 0; c < concurrency; ++c) {
            tasks.push_back(engine::AsyncNoSpan([&pool, per_coro] {
                for (std::size_t i = 0; i < per_coro; ++i)
                    pool->Ping();
            }));
        }
        for (auto& t : tasks) t.Get();
    });
    Print(label, r);
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

// ---- test: CPU only (no Tarantool required) --------------------------------

TEST(TarantoolCpuBench, EncodeDecodeComparison) {
    constexpr std::size_t kN = 200'000;
    const std::string val32(32, 'x');

    std::cout << "\n╔══ Phase 3 encode: old JSON path vs new ValueBuilder ════╗\n";
    BenchEncodeComparison(kN, val32);
    std::cout << "╠══ Phase B/D decode: MsgPackDecode vs FromBytes ══════════╣\n";
    BenchDecodeComparison(kN);
    std::cout << "╠══ Phase B/D decode (skip ext): lazy zero-cost skip ══════╣\n";
    BenchDecodeSkipExt(kN);
    std::cout << "╚════════════════════════════════════════════════════════╝\n";
}

// ---- test: network throughput ----------------------------------------------

TEST(TarantoolBench, InsertThroughput) {
    const char* host_env = std::getenv("TARANTOOL_HOST");
    const char* port_env = std::getenv("TARANTOOL_PORT");
    const std::string host = host_env ? host_env : "127.0.0.1";
    const int port = port_env ? std::stoi(port_env) : 3301;

    constexpr std::size_t kPerCoro = 2000;  // fixed per-coroutine op count
    const std::string val32(32, 'x');

    // ── Section 1: pure CPU cost (no network) ──────────────────────────────
    std::cout << "\n╔══ CPU cost (no network) ═══════════════════════════════╗\n";
    BenchEncodeComparison(kPerCoro, val32);
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

            // ── PING: pure protocol RTT, zero server-side work ─────────────
            // Shows the ceiling imposed by the connector itself.
            // IPROTO PING (type 64) has an empty body; the server replies with
            // an empty header instantly — any throughput shortfall vs REPLACE
            // is purely connector overhead, not server processing time.
            BenchPing(host, port, 1, 1, kPerCoro, resolver,
                      "ping        (pool=1,  coro=1)");
            for (int coro : {4, 8, 16, 32, 64, 128}) {
                BenchPing(host, port, 4, coro, kPerCoro, resolver,
                          "ping        (pool=4,  coro=" + std::to_string(coro) + ")");
            }

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
