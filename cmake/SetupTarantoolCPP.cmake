# tntcxx is a git submodule at third_party/tntcxx (header-only library).
# Upstream: https://github.com/tarantool/tntcxx.git (pinned commit: 467efeb)
if (TARGET tntcxx)
  return()
endif()

add_library(tntcxx INTERFACE)
target_include_directories(tntcxx INTERFACE
    ${USERVER_ROOT_DIR}/third_party/tntcxx/src)
