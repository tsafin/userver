#pragma once

#include <atomic>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl {

/// Minimal OS-thread spinlock for tiny, never-blocking critical sections
/// (slot-array store, vector append).  Unlike engine::Mutex it does not park
/// the coroutine on a futex — under contention it burns a few CPU cycles,
/// which is strictly cheaper than a wakeup round-trip for ~50 ns sections.
/// Compatible with std::lock_guard.
class alignas(64) SpinLock final {
 public:
  void lock() noexcept {
    while (flag_.test_and_set(std::memory_order_acquire)) {
#if defined(__x86_64__)
      __builtin_ia32_pause();
#endif
    }
  }

  void unlock() noexcept { flag_.clear(std::memory_order_release); }

 private:
  std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
};

}  // namespace storages::tarantool::impl

USERVER_NAMESPACE_END
