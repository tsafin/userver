option(USERVER_DOWNLOAD_PACKAGE_TNTCXX
    "Download and setup tntcxx" ${USERVER_DOWNLOAD_PACKAGES})

if (NOT USERVER_DOWNLOAD_PACKAGE_TNTCXX)
  find_package(tntcxx REQUIRED)
  return()
endif()

find_package(tntcxx QUIET)
if (tntcxx_FOUND)
  return()
endif()

include(FetchContent)
FetchContent_Declare(
  tntcxx_external_project
  GIT_REPOSITORY https://github.com/tarantool/tntcxx.git
  GIT_TAG        master
  TIMEOUT        20
  SOURCE_DIR     ${USERVER_ROOT_DIR}/third_party/tntcxx
)
FetchContent_GetProperties(tntcxx_external_project)
if (NOT tntcxx_external_project_POPULATED)
  message(STATUS "Downloading tntcxx from remote")
  FetchContent_Populate(tntcxx_external_project)
endif()

# tntcxx is header-only; expose as an interface target
add_library(tntcxx INTERFACE)
target_include_directories(tntcxx INTERFACE
    ${USERVER_ROOT_DIR}/third_party/tntcxx/src)
set(tntcxx_VERSION "0.1" CACHE STRING "Version of tntcxx")
