#include "connection.hpp"

#include <array>
#include <cstring>
#include <stdexcept>

#include <netinet/tcp.h>

#include <fmt/format.h>
#include <openssl/sha.h>

#include <userver/clients/dns/resolver.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/exception.hpp>
#include <userver/engine/future_status.hpp>
#include <userver/engine/io/exception.hpp>
#include <userver/engine/io/sockaddr.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/logging/log.hpp>
#include <userver/tracing/span.hpp>
#include <userver/tracing/tags.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

#include <storages/tarantool/impl/msgpack.hpp>
#include <storages/tarantool/impl/tracing_tags.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

namespace {

// IPROTO request type codes
constexpr uint32_t kIprotoSelect  = 1;
constexpr uint32_t kIprotoInsert  = 2;
constexpr uint32_t kIprotoReplace = 3;
constexpr uint32_t kIprotoUpdate  = 4;
constexpr uint32_t kIprotoDelete  = 5;
constexpr uint32_t kIprotoCall    = 10;
constexpr uint32_t kIprotoAuth    = 7;
constexpr uint32_t kIprotoPing    = 64;
constexpr uint32_t kIprotoUpsert  = 9;

// IPROTO header/body keys
constexpr uint32_t kKeyCode         = 0x00;
constexpr uint32_t kKeySync         = 0x01;
constexpr uint32_t kKeySpaceId      = 0x10;
constexpr uint32_t kKeyIndexId      = 0x11;
constexpr uint32_t kKeyLimit        = 0x12;
constexpr uint32_t kKeyOffset       = 0x13;
constexpr uint32_t kKeyIterator     = 0x14;
constexpr uint32_t kKeyKey          = 0x20;
constexpr uint32_t kKeyTuple        = 0x21;
constexpr uint32_t kKeyFunctionName = 0x22;
constexpr uint32_t kKeyUserName     = 0x23;
constexpr uint32_t kKeyTupleOps     = 0x28;

// IPROTO response keys
constexpr uint32_t kKeyData         = 0x30;
constexpr uint32_t kKeyError        = 0x31;

// Greeting constants
constexpr std::size_t kGreetingSize  = 128;
constexpr std::size_t kSaltOffset    = 64;
constexpr std::size_t kSaltLength    = 44;  // base64-encoded, 32 bytes decoded
constexpr std::size_t kPreheaderSize = 5;   // 0xce + 4 bytes length

// ---- IPROTO frame builder ----

void BuildHeader(std::vector<uint8_t>& out, uint32_t request_type,
                 uint64_t sync) {
    EncodeFixMap(out, 2);
    EncodeUint(out, kKeyCode);  EncodeUint(out, request_type);
    EncodeUint(out, kKeySync);  EncodeUint(out, sync);
}

std::vector<uint8_t> BuildFrame(uint32_t request_type, uint64_t sync,
                                const std::vector<uint8_t>& body) {
    std::vector<uint8_t> frame;
    frame.reserve(5 + 20 + body.size());
    frame.resize(5);
    BuildHeader(frame, request_type, sync);
    frame.insert(frame.end(), body.begin(), body.end());
    const uint32_t len = static_cast<uint32_t>(frame.size() - 5);
    frame[0] = 0xce;
    frame[1] = static_cast<uint8_t>(len >> 24);
    frame[2] = static_cast<uint8_t>(len >> 16);
    frame[3] = static_cast<uint8_t>(len >> 8);
    frame[4] = static_cast<uint8_t>(len);
    return frame;
}

// ---- Base64 decoder (for greeting salt) ----

static const char* kBase64Chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const std::array<uint8_t, 256>& GetBase64Table() {
    static const auto kTable = [] {
        std::array<uint8_t, 256> t;
        t.fill(0xFF);
        for (int i = 0; i < 64; ++i)
            t[static_cast<uint8_t>(kBase64Chars[i])] = static_cast<uint8_t>(i);
        t[static_cast<uint8_t>('=')] = 0;
        return t;
    }();
    return kTable;
}

std::vector<uint8_t> Base64Decode(std::string_view s) {
    const auto& table = GetBase64Table();
    std::vector<uint8_t> out;
    out.reserve(s.size() * 3 / 4);
    uint32_t val = 0;
    int bits = 0;
    for (char c : s) {
        uint8_t idx = table[static_cast<uint8_t>(c)];
        if (idx == 0xFF) continue;
        val = (val << 6) | idx;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<uint8_t>(val >> bits));
        }
    }
    return out;
}

// ---- CHAP-SHA1 auth scramble ----

std::vector<uint8_t> Scramble(const std::string& password,
                               const std::vector<uint8_t>& salt) {
    std::array<uint8_t, 20> hash1;
    SHA1(reinterpret_cast<const uint8_t*>(password.data()),
         password.size(), hash1.data());

    std::array<uint8_t, 20> hash2;
    SHA1(hash1.data(), 20, hash2.data());

    std::array<uint8_t, 40> salted;
    std::copy(salt.begin(), salt.begin() + 20, salted.begin());
    std::copy(hash2.begin(), hash2.end(), salted.begin() + 20);

    std::array<uint8_t, 20> hash3;
    SHA1(salted.data(), 40, hash3.data());

    std::vector<uint8_t> result(20);
    for (int i = 0; i < 20; ++i)
        result[i] = hash1[i] ^ hash3[i];
    return result;
}

}  // namespace

// ---- Connection implementation ----

Connection::Connection(clients::dns::Resolver& resolver,
                       const EndpointSettings& endpoint,
                       const AuthSettings& auth,
                       engine::Deadline connect_deadline) {
    tracing::Span span{scopes::kConnect};
    span.AddTag(tracing::kDatabaseType, "tarantool");
    span.AddTag(tracing::kDatabaseInstance, endpoint.host);

    const auto addrs = resolver.Resolve(endpoint.host, connect_deadline);

    std::exception_ptr last_exc;
    for (auto addr : addrs) {
        addr.SetPort(endpoint.port);
        engine::io::Socket sock{addr.Domain(),
                                engine::io::SocketType::kStream};
        try {
            sock.Connect(addr, connect_deadline);
            socket_ = std::move(sock);
            break;
        } catch (...) {
            last_exc = std::current_exception();
        }
    }
    if (socket_.Fd() == -1) {
        if (last_exc) std::rethrow_exception(last_exc);
        throw TarantoolException{
            fmt::format("Failed to connect to {}:{}", endpoint.host,
                        endpoint.port)};
    }
    // Disable Nagle's algorithm: IPROTO frames are small and complete, and we
    // want them sent immediately rather than buffered waiting for ACKs.
    socket_.SetOption(IPPROTO_TCP, TCP_NODELAY, 1);

    // Read 128-byte greeting (direct, before reader task starts)
    std::vector<uint8_t> greeting(kGreetingSize);
    static_cast<void>(socket_.RecvAll(greeting.data(), kGreetingSize,
                                      connect_deadline));
    const std::string salt_b64{
        reinterpret_cast<const char*>(greeting.data() + kSaltOffset),
        kSaltLength};

    DoAuth(auth, connect_deadline, salt_b64);

    // Start flush + reader background tasks – must be last (after auth).
    flush_task_ = engine::AsyncNoSpan([this] { FlushLoop(); });
    reader_task_ = engine::AsyncNoSpan([this] { ReaderLoop(); });
}

Connection::~Connection() {
    broken_.store(true, std::memory_order_release);
    // Wake the flush task in case it is sleeping in WaitForEvent(), so it
    // can notice cancellation promptly rather than waiting for the next Send().
    flush_event_.Send();
    // SyncCancel requests cancellation and blocks until the task finishes.
    flush_task_.SyncCancel();
    // reader_task_ RecvSome() will throw engine::io::IoCancelled, which
    // triggers WakeAllPending for any in-flight requests.
    reader_task_.SyncCancel();
}

// ---- Auth (direct socket reads, called before reader task) ----

void Connection::DoAuth(const AuthSettings& auth,
                        engine::Deadline deadline,
                        const std::string& salt_b64) {
    if (auth.user.empty() || auth.user == "guest") return;

    auto salt     = Base64Decode(salt_b64);
    auto scramble = Scramble(auth.password, salt);

    std::vector<uint8_t> body;
    EncodeFixMap(body, 2);
    EncodeUint(body, kKeyUserName); EncodeStr(body, auth.user);
    EncodeUint(body, kKeyTuple);
    EncodeArray(body, 2);
    EncodeStr(body, "chap-sha1");
    body.push_back(0xc4);
    body.push_back(static_cast<uint8_t>(scramble.size()));
    body.insert(body.end(), scramble.begin(), scramble.end());

    const uint64_t sync_id = ++sync_counter_;
    auto frame = BuildFrame(kIprotoAuth, sync_id, body);
    static_cast<void>(socket_.SendAll(frame.data(), frame.size(), deadline));

    std::vector<uint8_t> prehdr(kPreheaderSize);
    static_cast<void>(socket_.RecvAll(prehdr.data(), kPreheaderSize, deadline));
    const uint32_t body_len = DecodePreheaderLength(prehdr.data());
    std::vector<uint8_t> resp(body_len);
    static_cast<void>(socket_.RecvAll(resp.data(), body_len, deadline));

    MpDecoder dec{resp.data(), resp.data() + body_len};
    auto header = dec.DecodeValue();
    if (header["0"].As<int64_t>(0) != 0) {
        throw TarantoolAuthException{"authentication failed"};
    }
}

// ---- Background reader task ----

void Connection::ReaderLoop() {
    // Streaming read buffer: a single RecvSome call fills as many bytes as the
    // socket has available.  When the server has pipelined many responses back-
    // to-back, we decode all of them out of this buffer without extra syscalls,
    // reducing recv overhead from 2×N to ~N/K (K = avg responses per segment).
    constexpr size_t kReadBufSize = 65536;
    std::vector<uint8_t> rbuf(kReadBufSize);
    size_t rpos = 0;  // start of unconsumed data
    size_t rend = 0;  // end of received data

    // Refill buffer: compact if more than half is consumed, grow if needed,
    // then read as much as the socket has available (at least 1 byte).
    auto fill = [&] {
        if (rpos > kReadBufSize / 2) {
            const size_t avail = rend - rpos;
            std::memmove(rbuf.data(), rbuf.data() + rpos, avail);
            rpos = 0;
            rend = avail;
        }
        if (rbuf.size() < rend + kReadBufSize) rbuf.resize(rend + kReadBufSize);
        const size_t n = socket_.RecvSome(rbuf.data() + rend,
                                          rbuf.size() - rend, {});
        if (n == 0) throw std::runtime_error{"connection closed by peer"};
        rend += n;
    };

    auto ensure_bytes = [&](size_t n) {
        while (rend - rpos < n) fill();
    };

    try {
        while (true) {
            ensure_bytes(kPreheaderSize);
            const uint32_t body_len = DecodePreheaderLength(rbuf.data() + rpos);
            rpos += kPreheaderSize;

            ensure_bytes(body_len);
            // Decode directly from the buffer (zero-copy): set body_data before
            // advancing rpos so the pointer stays valid during decode.
            const uint8_t* body_data = rbuf.data() + rpos;
            MpDecoder dec{body_data, body_data + body_len};
            rpos += body_len;

            auto header = dec.DecodeValue();
            const auto sync_id = header["1"].As<uint64_t>(0);
            const auto code    = header["0"].As<int64_t>(0);

            formats::json::Value data_val{};
            std::string error_msg;
            if (code != 0) {
                if (dec.p < dec.end) {
                    auto body_map = dec.DecodeValue();
                    error_msg = body_map["49"].As<std::string>("tarantool error");
                }
            } else {
                if (dec.p < dec.end) {
                    auto body_map = dec.DecodeValue();
                    data_val = body_map["48"];
                }
            }
            ExecutionResult result{code == 0,
                                   static_cast<uint32_t>(
                                       std::max<int64_t>(0, code)),
                                   std::move(error_msg),
                                   std::move(data_val)};

            // Dispatch to the waiting coroutine
            engine::Promise<ExecutionResult> promise;
            bool found = false;
            {
                std::lock_guard lock(pending_mutex_);
                auto it = pending_.find(sync_id);
                if (it != pending_.end()) {
                    promise = std::move(it->second);
                    pending_.erase(it);
                    found = true;
                }
            }
            if (found) {
                promise.set_value(std::move(result));
            }
        }
    } catch (const engine::io::IoCancelled&) {
        // Normal shutdown: destructor called reader_task_.SyncCancel()
        WakeAllPending(std::current_exception());
    } catch (const engine::TaskCancelledException&) {
        // Task cancelled via userver task cancellation mechanism
        WakeAllPending(std::current_exception());
    } catch (...) {
        broken_.store(true, std::memory_order_release);
        WakeAllPending(std::current_exception());
    }
}

void Connection::WakeAllPending(std::exception_ptr ex) {
    std::unordered_map<uint64_t, engine::Promise<ExecutionResult>> pending;
    {
        std::lock_guard lock(pending_mutex_);
        pending = std::move(pending_);
    }
    for (auto& [id, p] : pending) {
        try {
            p.set_exception(ex);
        } catch (...) {}
    }
}

// ---- Flush coroutine ----

void Connection::FlushLoop() {
    // WaitForEvent() returns false when the task is cancelled (normal shutdown).
    while (flush_event_.WaitForEvent()) {
        // Yield once: all concurrent senders that have already staged their frame
        // and called flush_event_.Send() will continue executing (they return the
        // future and then yield at wait_until()).  By the time we resume, every
        // same-scheduler-slice sender has its frame in staging_buf_, so the
        // subsequent SendAll coalesces the entire batch into one syscall —
        // the same "batch-everything-then-flush" model as Tarantool net.box.
        engine::Yield();

        // Inner drain loop: keep sending until staging_buf_ is empty.
        // New frames arriving during SendAll call flush_event_.Send() again;
        // those will be picked up by the next outer-loop iteration.
        for (;;) {
            std::vector<uint8_t> to_send;
            {
                std::lock_guard lock(staging_mutex_);
                if (staging_buf_.empty()) break;
                to_send = std::move(staging_buf_);
            }

            if (broken_.load(std::memory_order_acquire)) {
                WakeAllPending(std::make_exception_ptr(
                    TarantoolException{"connection is broken"}));
                return;
            }

            try {
                // Use default (unreachable) deadline: individual request deadlines are
                // enforced by the caller's wait_until(); the flush task itself
                // has no per-request deadline.
                socket_.SendAll(to_send.data(), to_send.size(), engine::Deadline{});
            } catch (const engine::TaskCancelledException&) {
                return;
            } catch (...) {
                broken_.store(true, std::memory_order_release);
                WakeAllPending(std::current_exception());
                return;
            }
        }
    }
    // Task was cancelled (normal shutdown path).
}

// ---- Core pipelining primitive ----

engine::Future<ExecutionResult> Connection::SendAndRegister(
        engine::Deadline /*deadline*/,
        uint32_t request_type,
        std::vector<uint8_t> body) {

    engine::Promise<ExecutionResult> promise;
    auto future = promise.get_future();

    const uint64_t sync_id = ++sync_counter_;
    {
        std::lock_guard lock(pending_mutex_);
        pending_.emplace(sync_id, std::move(promise));
    }

    auto frame = BuildFrame(request_type, sync_id, body);

    // Stage the encoded frame; the flush coroutine sends it after coalescing
    // with frames from all other concurrent callers.
    {
        std::lock_guard lock(staging_mutex_);
        staging_buf_.insert(staging_buf_.end(), frame.begin(), frame.end());
    }
    flush_event_.Send();  // Signal the flush coroutine (non-blocking, multi-producer safe)

    return future;
}

// ---- Space ID resolver ----

uint32_t Connection::ResolveSpaceId(const std::string& space_name,
                                    engine::Deadline deadline) {
    {
        std::lock_guard lock(space_cache_mutex_);
        auto it = space_id_cache_.find(space_name);
        if (it != space_id_cache_.end()) return it->second;
    }

    constexpr uint32_t kVSpaceId = 281;
    constexpr uint32_t kVSpaceNameIndexId = 2;

    std::vector<uint8_t> body;
    EncodeFixMap(body, 6);
    EncodeUint(body, kKeySpaceId);  EncodeUint(body, kVSpaceId);
    EncodeUint(body, kKeyIndexId);  EncodeUint(body, kVSpaceNameIndexId);
    EncodeUint(body, kKeyLimit);    EncodeUint(body, 1);
    EncodeUint(body, kKeyOffset);   EncodeUint(body, 0);
    EncodeUint(body, kKeyIterator); EncodeUint(body, 0);  // EQ
    EncodeUint(body, kKeyKey);
    EncodeArray(body, 1);
    EncodeStr(body, space_name);

    auto future = SendAndRegister(deadline, kIprotoSelect, body);
    const auto status = future.wait_until(deadline);
    if (status != engine::FutureStatus::kReady) {
        throw TarantoolException{
            fmt::format("deadline expired resolving space '{}'", space_name)};
    }

    auto result = future.get();
    if (!result.IsOk()) {
        throw TarantoolException{
            fmt::format("failed to resolve space '{}'", space_name)};
    }
    auto data = result.GetData();
    if (!data.IsArray() || data.GetSize() == 0) {
        throw TarantoolException{
            fmt::format("space '{}' not found", space_name)};
    }
    const auto space_id = data[0][0].As<uint32_t>();

    {
        std::lock_guard lock(space_cache_mutex_);
        space_id_cache_[space_name] = space_id;
    }
    return space_id;
}

// ---- ExecuteAsync ----

engine::Future<ExecutionResult> Connection::ExecuteAsync(
        engine::Deadline deadline, const Query& query) {
    uint32_t space_id = 0;
    if (query.GetType() != Query::Type::kCall) {
        space_id = ResolveSpaceId(query.GetSpaceOrFunc(), deadline);
    }

    std::vector<uint8_t> body;
    uint32_t request_type = kIprotoCall;

    switch (query.GetType()) {
        case Query::Type::kCall: {
            request_type = kIprotoCall;
            EncodeFixMap(body, 2);
            EncodeUint(body, kKeyFunctionName);
            EncodeStr(body, query.GetSpaceOrFunc());
            EncodeUint(body, kKeyTuple);
            EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kSelect: {
            request_type = kIprotoSelect;
            EncodeFixMap(body, 6);
            EncodeUint(body, kKeySpaceId);   EncodeUint(body, space_id);
            EncodeUint(body, kKeyIndexId);   EncodeUint(body, 0);
            EncodeUint(body, kKeyLimit);     EncodeUint(body, query.GetLimit());
            EncodeUint(body, kKeyOffset);    EncodeUint(body, 0);
            EncodeUint(body, kKeyIterator);  EncodeUint(body, 0);
            EncodeUint(body, kKeyKey);       EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kInsert: {
            request_type = kIprotoInsert;
            EncodeFixMap(body, 2);
            EncodeUint(body, kKeySpaceId); EncodeUint(body, space_id);
            EncodeUint(body, kKeyTuple);   EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kReplace: {
            request_type = kIprotoReplace;
            EncodeFixMap(body, 2);
            EncodeUint(body, kKeySpaceId); EncodeUint(body, space_id);
            EncodeUint(body, kKeyTuple);   EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kDelete: {
            request_type = kIprotoDelete;
            EncodeFixMap(body, 3);
            EncodeUint(body, kKeySpaceId); EncodeUint(body, space_id);
            EncodeUint(body, kKeyIndexId); EncodeUint(body, 0);
            EncodeUint(body, kKeyKey);     EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kUpdate: {
            request_type = kIprotoUpdate;
            EncodeFixMap(body, 4);
            EncodeUint(body, kKeySpaceId);  EncodeUint(body, space_id);
            EncodeUint(body, kKeyIndexId);  EncodeUint(body, 0);
            EncodeUint(body, kKeyKey);      EncodeJson(body, query.GetArgs());
            EncodeUint(body, kKeyTupleOps); EncodeJson(body, query.GetOps());
            break;
        }
        case Query::Type::kUpsert: {
            request_type = kIprotoUpsert;
            EncodeFixMap(body, 3);
            EncodeUint(body, kKeySpaceId);  EncodeUint(body, space_id);
            EncodeUint(body, kKeyTuple);    EncodeJson(body, query.GetArgs());
            EncodeUint(body, kKeyTupleOps); EncodeJson(body, query.GetOps());
            break;
        }
    }

    return SendAndRegister(deadline, request_type, std::move(body));
}

// ---- Execute (sync wrapper) ----

ExecutionResult Connection::Execute(engine::Deadline deadline,
                                    const Query& query) {
    auto future = ExecuteAsync(deadline, query);
    const auto status = future.wait_until(deadline);
    if (status == engine::FutureStatus::kTimeout)
        throw TarantoolException{"execute deadline expired"};
    if (status == engine::FutureStatus::kCancelled)
        throw engine::TaskCancelledException{
            engine::TaskCancellationReason::kUserRequest};
    return future.get();
}

// ---- Ping ----

engine::Future<ExecutionResult> Connection::PingAsync(engine::Deadline deadline) {
    std::vector<uint8_t> body;
    return SendAndRegister(deadline, kIprotoPing, std::move(body));
}

void Connection::Ping(engine::Deadline deadline) {
    std::vector<uint8_t> body;  // empty body for ping
    auto future = SendAndRegister(deadline, kIprotoPing, body);
    const auto status = future.wait_until(deadline);
    if (status != engine::FutureStatus::kReady) {
        broken_.store(true, std::memory_order_release);
        throw TarantoolException{"ping timeout or cancelled"};
    }
    auto result = future.get();
    if (!result.IsOk()) {
        broken_.store(true, std::memory_order_release);
        throw TarantoolException{"ping returned error"};
    }
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
