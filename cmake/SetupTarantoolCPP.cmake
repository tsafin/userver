# tntcxx is vendored in third_party/tntcxx (header-only library).
# See third_party/Readme.md for the upstream commit reference.
if (TARGET tntcxx)
  return()
endif()

add_library(tntcxx INTERFACE)
target_include_directories(tntcxx INTERFACE
    ${USERVER_ROOT_DIR}/third_party/tntcxx/src)
