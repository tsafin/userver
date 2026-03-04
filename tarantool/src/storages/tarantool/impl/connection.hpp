#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/io/socket.hpp>

#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>

#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

/// @brief Owns an engine::io::Socket and handles IPROTO framing.
///
/// One request per connection at a time; pipelining is provided by the pool
/// (multiple connections). All I/O methods suspend the coroutine without
/// blocking the OS thread.
class Connection final {
 public:
  Connection(clients::dns::Resolver& resolver,
             const EndpointSettings& endpoint, const AuthSettings& auth,
             engine::Deadline connect_deadline);

  ExecutionResult Execute(OptionalCommandControl cc, const Query& query);

  void Ping(engine::Deadline deadline);

  bool IsBroken() const noexcept { return broken_; }

 private:
  void DoAuth(const AuthSettings& auth, engine::Deadline deadline,
              const std::string& salt);

  /// Resolve space name to numeric space ID (cached after first lookup).
  uint32_t ResolveSpaceId(const std::string& space_name,
                          engine::Deadline deadline);

  /// Send `n` bytes from `buf`, suspending the coroutine as needed
  void SendAll(const void* buf, std::size_t n, engine::Deadline deadline);

  engine::io::Socket socket_;

  /// Send/receive buffers (raw bytes)
  std::vector<uint8_t> send_buf_;
  std::vector<uint8_t> recv_buf_;

  std::unordered_map<std::string, uint32_t> space_id_cache_;

  uint64_t sync_counter_{0};
  bool broken_{false};
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
