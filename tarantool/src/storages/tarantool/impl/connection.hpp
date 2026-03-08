#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/future.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/task/task_with_result.hpp>

#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>

#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

/// @brief Owns an engine::io::Socket with full IPROTO pipelining support.
///
/// Multiple coroutines may call Execute() / ExecuteAsync() concurrently on the
/// same Connection: a background reader task demultiplexes responses by their
/// IPROTO sync field.  Callers need not hold any external lock between the
/// send and the receive phases.
class Connection final {
 public:
  Connection(clients::dns::Resolver& resolver,
             const EndpointSettings& endpoint, const AuthSettings& auth,
             engine::Deadline connect_deadline);

  ~Connection();

  /// Synchronous execute: sends the request AND waits for the response.
  /// Convenience wrapper around ExecuteAsync().
  ExecutionResult Execute(engine::Deadline deadline, const Query& query);

  /// Asynchronous execute: sends the request and returns a Future that
  /// resolves once the reader task delivers the response.  The caller may
  /// release its pool slot (or any other external lock) immediately after
  /// this returns, before calling wait_until() on the returned Future.
  engine::Future<ExecutionResult> ExecuteAsync(engine::Deadline deadline,
                                               const Query& query);

  void Ping(engine::Deadline deadline);

  bool IsBroken() const noexcept {
    return broken_.load(std::memory_order_acquire);
  }

 private:
  void DoAuth(const AuthSettings& auth, engine::Deadline deadline,
              const std::string& salt_b64);

  /// Resolve space name to numeric space ID (cached after first lookup).
  uint32_t ResolveSpaceId(const std::string& space_name,
                          engine::Deadline deadline);

  /// Core pipelining primitive: registers a pending entry, sends the frame
  /// atomically (serialised by send_mutex_), and returns the Future.
  /// The frame is built with the allocated sync_id baked in.
  engine::Future<ExecutionResult> SendAndRegister(engine::Deadline deadline,
                                                  uint32_t request_type,
                                                  std::vector<uint8_t> body);

  /// Background loop – runs for the lifetime of the connection and dispatches
  /// every incoming IPROTO frame to the waiting coroutine via its Promise.
  void ReaderLoop();

  /// Wake every entry currently in pending_ with the supplied exception.
  void WakeAllPending(std::exception_ptr ex);

  engine::io::Socket socket_;

  /// Serialises concurrent frame writes.
  engine::Mutex send_mutex_;
  /// Guards the pending_ map.
  engine::Mutex pending_mutex_;
  /// In-flight requests: sync_id → promise waiting for the response.
  std::unordered_map<uint64_t, engine::Promise<ExecutionResult>> pending_;

  /// Background reader task started at the end of the constructor.
  engine::TaskWithResult<void> reader_task_;

  /// Space-name → numeric-id cache (populated lazily on first Execute).
  std::unordered_map<std::string, uint32_t> space_id_cache_;
  engine::Mutex space_cache_mutex_;

  std::atomic<uint64_t> sync_counter_{0};
  std::atomic<bool> broken_{false};
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
