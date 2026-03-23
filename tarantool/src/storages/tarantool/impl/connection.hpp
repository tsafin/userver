#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <userver/clients/dns/resolver_fwd.hpp>
#include <userver/engine/deadline.hpp>
#include <userver/engine/future.hpp>
#include <userver/engine/io/socket.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/engine/single_consumer_event.hpp>
#include <userver/engine/task/task_with_result.hpp>

#include <userver/storages/tarantool/options.hpp>
#include <userver/storages/tarantool/query.hpp>
#include <userver/storages/tarantool/result.hpp>

#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

// ---- Pending-request entry types ----------------------------------------
//
// Each in-flight IPROTO request registers one PendingEntry in pending_.
// Phase 2 introduces two completion modes:
//
//   AsyncPendingEntry — used by ExecuteAsync / ForwardVshardCallAsync.
//     The reader task calls promise.set_value() or set_exception() to wake
//     the waiting coroutine via its Future.
//
//   SyncPendingEntry — used by SendAndWait (Phase 3+).
//     The reader task writes directly into result/exc and fires a
//     SingleConsumerEvent.  No heap-allocated future state is involved.
//     Lifetime is managed by shared_ptr so late responses from the reader
//     are safe even after the caller has timed out.

struct AsyncPendingEntry {
    engine::Promise<ExecutionResult> promise;
};

struct SyncPendingEntry {
    ExecutionResult result;
    std::exception_ptr exc;
    engine::SingleConsumerEvent ready;
    /// Set by the caller on timeout/cancellation to tell the reader to
    /// discard any late delivery rather than touching the freed result.
    std::atomic<bool> abandoned{false};

    SyncPendingEntry() = default;
    SyncPendingEntry(const SyncPendingEntry&) = delete;
    SyncPendingEntry& operator=(const SyncPendingEntry&) = delete;
};

using PendingEntry = std::variant<AsyncPendingEntry, SyncPendingEntry>;

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

  /// Asynchronous ping: sends IPROTO_PING and returns a Future resolved when
  /// the empty response arrives.  Pool slot may be released before wait_until.
  engine::Future<ExecutionResult> PingAsync(engine::Deadline deadline);

  void Ping(engine::Deadline deadline);

  /// Asynchronous zero-copy storage call: builds the vshard.storage.call body
  /// from kStorageCallBodyPrefix + raw TUPLE bytes (one memcpy, no msgpack
  /// re-encoding), registers the request and returns a Future.
  engine::Future<ExecutionResult> ForwardStorageCallAsync(
      const CallRouteInfo& info, engine::Deadline deadline);

  /// Asynchronous IPROTO_VSHARD_CALL forward: builds a 4-entry header map
  /// (REQUEST_TYPE=0x50, SYNC, VSHARD_BUCKET_ID, VSHARD_MODE) + body bytes
  /// (one memcpy).  Used by VshardProxy::ForwardVshardCall().
  engine::Future<ExecutionResult> ForwardVshardCallAsync(
      uint32_t bucket_id, uint8_t mode,
      const uint8_t* body, std::size_t body_len,
      engine::Deadline deadline);

  bool IsBroken() const noexcept {
    return broken_.load(std::memory_order_acquire);
  }

 private:
  void DoAuth(const AuthSettings& auth, engine::Deadline deadline,
              const std::string& salt_b64);

  /// Resolve space name to numeric space ID (cached after first lookup).
  uint32_t ResolveSpaceId(const std::string& space_name,
                          engine::Deadline deadline);

  /// Core async pipelining primitive: registers an AsyncPendingEntry, stages
  /// the encoded frame into staging_buf_, and signals the flush coroutine.
  /// The returned Future resolves once the reader task delivers the response.
  engine::Future<ExecutionResult> SendAndRegister(engine::Deadline deadline,
                                                  uint32_t request_type,
                                                  std::vector<uint8_t> body);

  void FlushLoop();

  /// Background loop – runs for the lifetime of the connection and dispatches
  /// every incoming IPROTO frame to the waiting coroutine via its Promise.
  void ReaderLoop();

  /// Wake every entry currently in pending_ with the supplied exception.
  void WakeAllPending(std::exception_ptr ex);

  engine::io::Socket socket_;

  // ---- Send-side batching (flush coroutine) --------------------------------
  // Each sender appends its encoded frame to staging_buf_ under staging_mutex_
  // and signals flush_event_.  The dedicated flush_task_ coroutine waits on
  // that event, calls engine::Yield() once to let all concurrent senders
  // finish staging their frames, then drains the entire buffer and sends it in
  // a single SendAll call.  This coalesces N concurrent sends into ~1 syscall
  // (same "batch everything then send" model as Tarantool net.box).
  engine::Mutex staging_mutex_;
  std::vector<uint8_t> staging_buf_;
  engine::SingleConsumerEvent flush_event_;

  /// Guards the pending_ map.
  engine::Mutex pending_mutex_;
  /// In-flight requests: sync_id → shared pending entry (async or sync waiter).
  std::unordered_map<uint64_t, std::shared_ptr<PendingEntry>> pending_;

  /// Background flush task: drains staging_buf_ and sends to socket.
  /// Declared before reader_task_ so it is SyncCancel'd first in ~Connection.
  engine::TaskWithResult<void> flush_task_;

  /// Background reader task started at the end of the constructor.
  engine::TaskWithResult<void> reader_task_;

  /// Reusable flat buffer for copying each IPROTO response body out of the
  /// non-contiguous tnt::Buffer before parsing.  Owned exclusively by
  /// ReaderLoop — no locking needed.  capacity() grows monotonically to the
  /// largest response seen, so after warmup there are zero malloc calls per
  /// response on this buffer.
  std::vector<uint8_t> reader_body_buf_;

  /// Space-name → numeric-id cache (populated lazily on first Execute).
  std::unordered_map<std::string, uint32_t> space_id_cache_;
  engine::Mutex space_cache_mutex_;

  std::atomic<uint64_t> sync_counter_{0};
  std::atomic<bool> broken_{false};
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
