/// @file loadgen/loadgen.cpp
/// @brief Standalone pipelined IPROTO load generator for the vshard proxy.
///
/// Unlike bench_vshard.lua (synchronous net.box, 1 in-flight request per
/// connection, single-process ceiling ~40-50k ops/s), this generator keeps
/// `--depth` requests in flight per connection at all times, so router-side
/// read batching / write coalescing actually engage and the client does not
/// mask the server's throughput.
///
/// Speaks the same CALL the Lua bench uses:
///   conn:call('vshard.router.callrw', {bucket, 'box.space.customer:replace',
///                                      {{bucket, bucket, 'x'}}})
///
/// Usage:
///   userver-tarantool-vshard-loadgen [--host H] [--port P]
///       [--conns N] [--depth K] [--ops M] [--bucket-max B]
///       [--threads T] [--user U] [--password P] [--timeout-ms MS]
///
/// Defaults: host=127.0.0.1 port=13306 conns=4 depth=100 ops=200000
///           bucket-max=1500 threads=4 timeout-ms=5000
///
/// Exit code: 0 if errors == 0, 1 otherwise.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <userver/clients/dns/resolver.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/run_standalone.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/static_config/dns_client.hpp>

#include <storages/tarantool/impl/connection.hpp>
#include <userver/storages/tarantool/query.hpp>

USERVER_NAMESPACE_BEGIN

namespace {

struct Args {
    std::string host = "127.0.0.1";
    uint16_t port = 13306;
    int conns = 4;
    int depth = 100;
    long ops = 200000;
    int bucket_max = 1500;
    int threads = 4;
    std::string user = "storage";
    std::string password = "storage";
    long timeout_ms = 5000;
    std::string router_func = "vshard.router.callrw";
    std::string space_op = "box.space.customer:replace";
};

Args ParseArgs(int argc, char** argv) {
    Args a;
    auto need = [&](int i) -> const char* {
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << argv[i] << "\n";
            std::exit(2);
        }
        return argv[i + 1];
    };
    for (int i = 1; i < argc; ++i) {
        const std::string k = argv[i];
        if (k == "--host")             a.host = need(i++);
        else if (k == "--port")        a.port = static_cast<uint16_t>(std::stoi(need(i++)));
        else if (k == "--conns")       a.conns = std::stoi(need(i++));
        else if (k == "--depth")       a.depth = std::stoi(need(i++));
        else if (k == "--ops")         a.ops = std::stol(need(i++));
        else if (k == "--bucket-max")  a.bucket_max = std::stoi(need(i++));
        else if (k == "--threads")     a.threads = std::stoi(need(i++));
        else if (k == "--user")        a.user = need(i++);
        else if (k == "--password")    a.password = need(i++);
        else if (k == "--timeout-ms")  a.timeout_ms = std::stol(need(i++));
        else if (k == "--router-func") a.router_func = need(i++);
        else if (k == "--space-op")    a.space_op = need(i++);
        else {
            std::cerr << "unknown arg: " << k << "\n";
            std::exit(2);
        }
    }
    return a;
}

std::atomic<bool> g_stop{false};

void RunWorker(storages::tarantool::impl::Connection& conn, const Args& a,
               int slot_index, long ops_this_slot,
               std::atomic<long>& done, std::atomic<long>& errors) {
    uint32_t bid = static_cast<uint32_t>(slot_index % a.bucket_max) + 1;

    for (long i = 0; i < ops_this_slot && !g_stop.load(std::memory_order_relaxed); ++i) {
        const auto deadline =
            engine::Deadline::FromDuration(std::chrono::milliseconds{a.timeout_ms});
        try {
            auto args = formats::msgpack::ValueBuilder::Array();
            args.PushBack(formats::msgpack::ValueBuilder{static_cast<uint64_t>(bid)});
            args.PushBack(formats::msgpack::ValueBuilder{a.space_op});
            auto tuple = formats::msgpack::ValueBuilder::Array();
            tuple.PushBack(formats::msgpack::ValueBuilder{static_cast<uint64_t>(bid)});
            tuple.PushBack(formats::msgpack::ValueBuilder{static_cast<uint64_t>(bid)});
            tuple.PushBack(formats::msgpack::ValueBuilder{"x"});
            auto inner = formats::msgpack::ValueBuilder::Array();
            inner.PushBack(std::move(tuple));
            args.PushBack(std::move(inner));

            auto query = storages::tarantool::Query::Call(a.router_func, std::move(args));
            auto fut = conn.ExecuteAsync(deadline, query);
            auto res = fut.get();
            if (!res.IsOk()) ++errors;
        } catch (const std::exception& ex) {
            ++errors;
            if (errors.load() < 10) {
                std::cerr << "op error: " << ex.what() << "\n";
            } else if (errors.load() == done.load() + errors.load()) {
                // everything failed — likely the server is gone
                g_stop.store(true, std::memory_order_relaxed);
            }
        }
        bid = (bid % static_cast<uint32_t>(a.bucket_max)) + 1;
        ++done;
    }
}

int Run(const Args& a) {
    using storages::tarantool::impl::AuthSettings;
    using storages::tarantool::impl::Connection;
    using storages::tarantool::impl::EndpointSettings;

    engine::TaskProcessorPoolsConfig pools;
    pools.ev_threads_num = 2;
    pools.initial_coro_pool_size =
        std::min(2000, a.conns * a.depth + 200);
    pools.max_coro_pool_size = 4000;

    engine::RunStandalone(a.threads, pools, [&a] {
        ::userver::static_config::DnsClient dns_cfg{};
        clients::dns::Resolver resolver{
            engine::current_task::GetTaskProcessor(), dns_cfg};

        std::vector<std::unique_ptr<Connection>> conns;
        conns.reserve(static_cast<std::size_t>(a.conns));
        for (int c = 0; c < a.conns; ++c) {
            EndpointSettings ep;
            ep.host = a.host;
            ep.port = a.port;
            AuthSettings auth;
            auth.user = a.user;
            auth.password = a.password;
            conns.push_back(std::make_unique<Connection>(
                resolver, ep, auth,
                engine::Deadline::FromDuration(std::chrono::milliseconds{5000})));
        }
        std::cerr << "connected " << a.conns << " conns to " << a.host << ":"
                  << a.port << "\n";

        const int slots = a.conns * a.depth;
        const long ops_per_slot = a.ops / slots;

        std::atomic<long> done{0};
        std::atomic<long> errors{0};

        // warmup: 50 ops per slot
        {
            std::vector<engine::TaskWithResult<void>> warm;
            warm.reserve(static_cast<std::size_t>(slots));
            for (int s = 0; s < slots; ++s) {
                warm.push_back(engine::AsyncNoSpan([&a, &conns, s, &done, &errors] {
                    Args quiet = a;
                    RunWorker(*conns[static_cast<std::size_t>(s % a.conns)],
                              quiet, s, 50, done, errors);
                }));
            }
            for (auto& t : warm) t.Get();
        }
        if (errors.load() != 0) {
            std::cerr << "warmup produced " << errors.load()
                      << " errors — aborting\n";
            std::exit(1);
        }
        done.store(0);

        std::cerr << "warmup ok; slots=" << slots
                  << " ops/slot=" << ops_per_slot << "\n";

        const auto t0 = std::chrono::steady_clock::now();
        std::vector<engine::TaskWithResult<void>> tasks;
        tasks.reserve(static_cast<std::size_t>(slots));
        for (int s = 0; s < slots; ++s) {
            tasks.push_back(engine::AsyncNoSpan([&a, &conns, s, ops_per_slot,
                                                 &done, &errors] {
                RunWorker(*conns[static_cast<std::size_t>(s % a.conns)], a, s,
                          ops_per_slot, done, errors);
            }));
        }
        for (auto& t : tasks) t.Get();
        const auto elapsed = std::chrono::steady_clock::now() - t0;

        const long total = done.load();
        const long errs = errors.load();
        const double secs =
            std::chrono::duration_cast<std::chrono::duration<double>>(elapsed)
                .count();
        std::cout << "Result: " << total << " ops in " << secs << " s = "
                  << (secs > 0 ? static_cast<long>(total / secs) : 0)
                  << " ops/sec  errors=" << errs
                  << "  (conns=" << a.conns << " depth=" << a.depth << ")\n";
    });
    return 0;
}

int LoadgenMain(int argc, char** argv) {
    const Args a = ParseArgs(argc, argv);
    return Run(a);
}

}  // namespace

USERVER_NAMESPACE_END

int main(int argc, char** argv) {
    return USERVER_NAMESPACE::LoadgenMain(argc, argv);
}
