#include "connection.hpp"

#include <array>
#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>

#include <fmt/format.h>
#include <openssl/sha.h>

#include <userver/engine/io/sockaddr.hpp>
#include <userver/engine/io/socket.hpp>
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
constexpr uint32_t kKeyErrorStack   = 0x52;

// Greeting constants
constexpr std::size_t kGreetingSize    = 128;
constexpr std::size_t kSaltOffset      = 64;
constexpr std::size_t kSaltLength      = 44;  // base64-encoded, 32 bytes decoded
constexpr std::size_t kPreheaderSize   = 5;   // 0xce + 4 bytes length

// ---- IPROTO frame builder ----

// Prepend the 5-byte IPROTO length prefix (0xce + big-endian uint32)
void BuildHeader(std::vector<uint8_t>& out, uint32_t request_type,
                 uint64_t sync) {
    EncodeFixMap(out, 2);
    EncodeUint(out, kKeyCode);   EncodeUint(out, request_type);
    EncodeUint(out, kKeySync);   EncodeUint(out, sync);
}

std::vector<uint8_t> BuildFrame(uint32_t request_type, uint64_t sync,
                                const std::vector<uint8_t>& body) {
    std::vector<uint8_t> frame;
    frame.reserve(5 + 20 + body.size());
    // Reserve 5 bytes for length prefix
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

std::vector<uint8_t> Base64Decode(std::string_view s) {
    static uint8_t table[256];
    static bool init = false;
    if (!init) {
        std::memset(table, 0xFF, sizeof(table));
        for (int i = 0; i < 64; ++i)
            table[static_cast<uint8_t>(kBase64Chars[i])] = static_cast<uint8_t>(i);
        table[static_cast<uint8_t>('=')] = 0;
        init = true;
    }
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
    // scramble = SHA1(password) XOR SHA1(salt[0..20] + SHA1(SHA1(password)))
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

Connection::Connection(const EndpointSettings& endpoint,
                       const AuthSettings& auth,
                       engine::Deadline connect_deadline) {
    tracing::Span span{scopes::kConnect};
    span.AddTag(tracing::kDatabaseType, "tarantool");
    span.AddTag(tracing::kDatabaseInstance, endpoint.host);

    // Resolve host synchronously (blocking) and connect
    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* result = nullptr;
    const auto port_str = std::to_string(endpoint.port);
    const int rc = ::getaddrinfo(endpoint.host.c_str(), port_str.c_str(),
                                 &hints, &result);
    if (rc != 0) {
        throw TarantoolException{
            fmt::format("getaddrinfo failed for {}:{} - {}", endpoint.host,
                        endpoint.port, ::gai_strerror(rc))};
    }
    struct addrinfo* rp = result;
    std::exception_ptr last_exc;
    for (; rp != nullptr; rp = rp->ai_next) {
        engine::io::Sockaddr addr{rp->ai_addr};
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
    ::freeaddrinfo(result);
    if (socket_.Fd() == -1) {
        if (last_exc) std::rethrow_exception(last_exc);
        throw TarantoolException{
            fmt::format("Failed to connect to {}:{}", endpoint.host,
                        endpoint.port)};
    }

    // Read 128-byte greeting
    recv_buf_.resize(kGreetingSize);
    static_cast<void>(socket_.RecvAll(recv_buf_.data(), kGreetingSize, connect_deadline));

    // Extract salt (bytes 64..107 are base64-encoded)
    const std::string salt_b64{
        reinterpret_cast<const char*>(recv_buf_.data() + kSaltOffset),
        kSaltLength};
    recv_buf_.clear();

    DoAuth(auth, connect_deadline, salt_b64);
}

void Connection::DoAuth(const AuthSettings& auth,
                        engine::Deadline deadline,
                        const std::string& salt_b64) {
    if (auth.user.empty() || auth.user == "guest") {
        // guest user — skip auth
        return;
    }

    auto salt = Base64Decode(salt_b64);
    auto scramble = Scramble(auth.password, salt);

    std::vector<uint8_t> body;
    EncodeFixMap(body, 2);
    EncodeUint(body, kKeyUserName); EncodeStr(body, auth.user);
    EncodeUint(body, kKeyTuple);
    // CHAP-SHA1 tuple: ["chap-sha1", scramble_bytes]
    EncodeArray(body, 2);
    EncodeStr(body, "chap-sha1");
    // encode scramble as raw binary (mp bin8)
    body.push_back(0xc4);  // bin8
    body.push_back(static_cast<uint8_t>(scramble.size()));
    body.insert(body.end(), scramble.begin(), scramble.end());

    auto frame = BuildFrame(kIprotoAuth, ++sync_counter_, body);
    SendAll(frame.data(), frame.size(), deadline);

    // Read response
    recv_buf_.resize(kPreheaderSize);
    static_cast<void>(socket_.RecvAll(recv_buf_.data(), kPreheaderSize, deadline));
    const uint32_t body_len = DecodePreheaderLength(recv_buf_.data());
    recv_buf_.clear();
    recv_buf_.resize(body_len);
    static_cast<void>(socket_.RecvAll(recv_buf_.data(), body_len, deadline));

    MpDecoder dec{recv_buf_.data(), recv_buf_.data() + body_len};
    // IPROTO: header map then body map
    auto header = dec.DecodeValue();
    recv_buf_.clear();

    // Check response code (header key 0x00 = REQUEST_TYPE, used as status code in response)
    const auto code = header["0"].As<int64_t>(0);
    if (code != 0) {
        throw TarantoolAuthException{"authentication failed"};
    }
}

ExecutionResult Connection::Execute(OptionalCommandControl cc,
                                    const Query& query) {
    const engine::Deadline deadline =
        cc ? engine::Deadline::FromDuration(cc->execute)
           : engine::Deadline{};

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
            EncodeUint(body, kKeySpaceId);   EncodeUint(body, 0);  // space by name NYI
            EncodeUint(body, kKeyIndexId);   EncodeUint(body, 0);
            EncodeUint(body, kKeyLimit);     EncodeUint(body, query.GetLimit());
            EncodeUint(body, kKeyOffset);    EncodeUint(body, 0);
            EncodeUint(body, kKeyIterator);  EncodeUint(body, 0);  // EQ
            EncodeUint(body, kKeyKey);       EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kInsert: {
            request_type = kIprotoInsert;
            EncodeFixMap(body, 2);
            EncodeUint(body, kKeySpaceId); EncodeUint(body, 0);
            EncodeUint(body, kKeyTuple);   EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kReplace: {
            request_type = kIprotoReplace;
            EncodeFixMap(body, 2);
            EncodeUint(body, kKeySpaceId); EncodeUint(body, 0);
            EncodeUint(body, kKeyTuple);   EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kDelete: {
            request_type = kIprotoDelete;
            EncodeFixMap(body, 3);
            EncodeUint(body, kKeySpaceId); EncodeUint(body, 0);
            EncodeUint(body, kKeyIndexId); EncodeUint(body, 0);
            EncodeUint(body, kKeyKey);     EncodeJson(body, query.GetArgs());
            break;
        }
        case Query::Type::kUpdate: {
            request_type = kIprotoUpdate;
            EncodeFixMap(body, 4);
            EncodeUint(body, kKeySpaceId);  EncodeUint(body, 0);
            EncodeUint(body, kKeyIndexId);  EncodeUint(body, 0);
            EncodeUint(body, kKeyKey);      EncodeJson(body, query.GetArgs());
            EncodeUint(body, kKeyTupleOps); EncodeJson(body, query.GetOps());
            break;
        }
        case Query::Type::kUpsert: {
            request_type = kIprotoUpsert;
            EncodeFixMap(body, 3);
            EncodeUint(body, kKeySpaceId);  EncodeUint(body, 0);
            EncodeUint(body, kKeyTuple);    EncodeJson(body, query.GetArgs());
            EncodeUint(body, kKeyTupleOps); EncodeJson(body, query.GetOps());
            break;
        }
    }

    auto frame = BuildFrame(request_type, ++sync_counter_, body);
    try {
        SendAll(frame.data(), frame.size(), deadline);

        recv_buf_.resize(kPreheaderSize);
        static_cast<void>(socket_.RecvAll(recv_buf_.data(), kPreheaderSize, deadline));
        const uint32_t body_len = DecodePreheaderLength(recv_buf_.data());
        recv_buf_.clear();
        recv_buf_.resize(body_len);
        static_cast<void>(socket_.RecvAll(recv_buf_.data(), body_len, deadline));

        MpDecoder dec{recv_buf_.data(), recv_buf_.data() + body_len};
        // IPROTO response: header map (contains code), then body map (contains DATA/ERROR)
        auto header = dec.DecodeValue();
        recv_buf_.clear();

        const auto code = header["0"].As<int64_t>(0);
        if (code != 0) {
            // Decode body for error message
            formats::json::Value body_val{};
            if (dec.p < dec.end) {
                body_val = dec.DecodeValue();
            }
            const auto msg = body_val["49"].As<std::string>("tarantool error");
            return ExecutionResult{false, static_cast<uint32_t>(code), msg, {}};
        }
        // Decode body for data
        formats::json::Value body_val{};
        if (dec.p < dec.end) {
            body_val = dec.DecodeValue();
        }
        auto data = body_val["48"];  // kKeyData = 0x30 = 48
        return ExecutionResult{true, 0, {}, std::move(data)};
    } catch (...) {
        broken_ = true;
        throw;
    }
}

void Connection::Ping(engine::Deadline deadline) {
    std::vector<uint8_t> body;  // empty body for ping
    auto frame = BuildFrame(kIprotoPing, ++sync_counter_, body);
    try {
        SendAll(frame.data(), frame.size(), deadline);
        recv_buf_.resize(kPreheaderSize);
        static_cast<void>(socket_.RecvAll(recv_buf_.data(), kPreheaderSize, deadline));
        const uint32_t body_len = DecodePreheaderLength(recv_buf_.data());
        recv_buf_.clear();
        if (body_len > 0) {
            recv_buf_.resize(body_len);
            static_cast<void>(socket_.RecvAll(recv_buf_.data(), body_len, deadline));
            recv_buf_.clear();
        }
    } catch (...) {
        broken_ = true;
        throw;
    }
}

void Connection::SendAll(const void* buf, std::size_t n,
                         engine::Deadline deadline) {
    static_cast<void>(socket_.SendAll(buf, n, deadline));
}

void Connection::RecvExact(std::size_t n, engine::Deadline deadline) {
    const std::size_t offset = recv_buf_.size();
    recv_buf_.resize(offset + n);
    static_cast<void>(socket_.RecvAll(recv_buf_.data() + offset, n, deadline));
}

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
