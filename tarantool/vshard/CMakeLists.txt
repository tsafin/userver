project(userver-tarantool-vshard-sample CXX)

file(GLOB SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/impl/*.cpp)

file(GLOB UNIT_TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/*_test.cpp)
list(REMOVE_ITEM SOURCES ${UNIT_TEST_SOURCES})

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    userver-tarantool
    userver-core
    tntcxx
)

target_include_directories(${PROJECT_NAME} PRIVATE
    # vshard/impl/foo.hpp resolves with tarantool/ as the root
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    # expose tarantool internal headers (pool.hpp, settings.hpp) needed by
    # topology_fetcher.cpp
    ${CMAKE_SOURCE_DIR}/tarantool/src
)

if (USERVER_IS_THE_ROOT_PROJECT AND UNIT_TEST_SOURCES)
    add_executable(${PROJECT_NAME}_unittest ${UNIT_TEST_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/impl/bucket_calculator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/impl/vshard_error.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/impl/vshard_envelope.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/impl/routing_table.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/impl/replicaset_pool.cpp
    )

    target_include_directories(${PROJECT_NAME}_unittest PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_SOURCE_DIR}/tarantool/src
        $<TARGET_PROPERTY:userver-tarantool,INCLUDE_DIRECTORIES>
    )
    target_link_libraries(${PROJECT_NAME}_unittest
        userver-utest
        userver-tarantool
        tntcxx
    )
    target_compile_definitions(${PROJECT_NAME}_unittest PRIVATE
        DEFAULT_DYNAMIC_CONFIG_FILENAME="${CMAKE_SOURCE_DIR}/tarantool/tests/dynamic_config_fallback.json")
    add_google_tests(${PROJECT_NAME}_unittest)
endif()

# ---------------------------------------------------------------------------
# Vshard throughput benchmark (requires live vshard cluster)
# ---------------------------------------------------------------------------

if (USERVER_IS_THE_ROOT_PROJECT)
    file(GLOB BENCH_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/bench/*.cpp)
    if (BENCH_SOURCES)
        add_executable(userver-tarantool-vshard-bench ${BENCH_SOURCES}
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/bucket_calculator.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/vshard_error.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/vshard_envelope.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/routing_table.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/replicaset_pool.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/topology_fetcher.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/impl/vshard_proxy.cpp
        )
        target_include_directories(userver-tarantool-vshard-bench PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
            ${CMAKE_SOURCE_DIR}/tarantool/src
            $<TARGET_PROPERTY:userver-tarantool,INCLUDE_DIRECTORIES>
        )
        target_link_libraries(userver-tarantool-vshard-bench
            userver-utest
            userver-tarantool
            tntcxx
        )
        target_compile_definitions(userver-tarantool-vshard-bench PRIVATE
            DEFAULT_DYNAMIC_CONFIG_FILENAME="${CMAKE_SOURCE_DIR}/tarantool/tests/dynamic_config_fallback.json")
    endif()

    # -----------------------------------------------------------------------
    # Functional (pytest) tests — requires a live vshard cluster + vshard Lua
    # module.  The test harness in conftest.py spins up Tarantool instances
    # automatically.
    #
    # Usage:
    #   cd build_debug && ctest -V -R testsuite-userver-tarantool-vshard
    #
    # To override the vshard Lua module path:
    #   -DVSHARD_LUA_PATH=/path/to/vshard
    # -----------------------------------------------------------------------
    set(VSHARD_FUNCTIONAL_TESTS_DIR
        "${CMAKE_CURRENT_SOURCE_DIR}/functional_tests/tests")

    if(EXISTS "${VSHARD_FUNCTIONAL_TESTS_DIR}/conftest.py")
        if(NOT VSHARD_LUA_PATH)
            # Auto-detect common locations.
            foreach(_candidate "$ENV{HOME}/src/vshard"
                               "/usr/share/tarantool"
                               "/usr/local/share/tarantool")
                if(EXISTS "${_candidate}/vshard/init.lua")
                    set(VSHARD_LUA_PATH "${_candidate}")
                    break()
                endif()
            endforeach()
        endif()

        if(VSHARD_LUA_PATH)
            find_program(PYTEST_EXECUTABLE NAMES pytest pytest-3 py.test)
            if(PYTEST_EXECUTABLE)
                add_test(
                    NAME testsuite-userver-tarantool-vshard
                    COMMAND "${PYTEST_EXECUTABLE}"
                        "${VSHARD_FUNCTIONAL_TESTS_DIR}"
                        --proxy-binary=$<TARGET_FILE:userver-tarantool-vshard-sample>
                        --vshard-path=${VSHARD_LUA_PATH}
                        -v
                    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/functional_tests"
                )
                set_tests_properties(testsuite-userver-tarantool-vshard PROPERTIES
                    TIMEOUT 120
                    LABELS "functional;tarantool;vshard"
                )
                message(STATUS "vshard functional tests: enabled "
                               "(vshard path: ${VSHARD_LUA_PATH})")
            else()
                message(STATUS "vshard functional tests: DISABLED (pytest not found)")
            endif()
        else()
            message(STATUS "vshard functional tests: DISABLED "
                           "(vshard Lua module not found, set -DVSHARD_LUA_PATH=...)")
        endif()
    endif()

    # -----------------------------------------------------------------------
    # Throughput benchmark target — runs bench_vshard.lua against both the
    # Lua vshard router and the C++ proxy, and the in-process C++ benchmark.
    # Appends a new results section to bench/benchmark_results.md.
    #
    # The cluster is started automatically via 'make start' in VSHARD_PATH if
    # the nodes are not already listening, and stopped afterwards.
    #
    # Usage:
    #   cmake --build build_release --target benchmark-vshard
    #
    # Override ports/paths at cmake configure time with:
    #   -DVSHARD_PATH=/path/to/vshard          (vshard repo root or example dir)
    #   -DVSHARD_LUA_ROUTER=localhost:3305
    #   -DVSHARD_BENCH_FIBER_COUNTS=10,20,50
    #   -DVSHARD_BENCH_OPS=100000
    #   -DVSHARD_BENCH_CPP_PORT=13306
    # -----------------------------------------------------------------------
    if(NOT DEFINED VSHARD_LUA_ROUTER)
        set(VSHARD_LUA_ROUTER "localhost:3305")
    endif()
    if(NOT DEFINED VSHARD_BENCH_FIBER_COUNTS)
        set(VSHARD_BENCH_FIBER_COUNTS "10,20,50")
    endif()
    if(NOT DEFINED VSHARD_BENCH_OPS)
        set(VSHARD_BENCH_OPS 100000)
    endif()
    if(NOT DEFINED VSHARD_BENCH_CPP_PORT)
        set(VSHARD_BENCH_CPP_PORT 3306)
    endif()
    # VSHARD_PATH is optional; when set the script can auto-start the cluster
    set(_vshard_path_arg "")
    if(DEFINED VSHARD_PATH)
        set(_vshard_path_arg "--vshard-path" "${VSHARD_PATH}")
    endif()

    if(TARGET userver-tarantool-vshard-sample)
        set(_bench_script "${CMAKE_CURRENT_SOURCE_DIR}/bench/run_benchmarks.sh")
        set(_bench_output "${CMAKE_CURRENT_SOURCE_DIR}/bench/benchmark_results.md")

        add_custom_target(benchmark-vshard
            COMMAND bash "${_bench_script}"
                --proxy-binary $<TARGET_FILE:userver-tarantool-vshard-sample>
                --lua-router   "${VSHARD_LUA_ROUTER}"
                --fiber-counts "${VSHARD_BENCH_FIBER_COUNTS}"
                --ops          "${VSHARD_BENCH_OPS}"
                --cpp-proxy-port "${VSHARD_BENCH_CPP_PORT}"
                --output       "${_bench_output}"
                ${_vshard_path_arg}
            DEPENDS userver-tarantool-vshard-sample
            COMMENT "Running vshard throughput benchmarks (fibers=${VSHARD_BENCH_FIBER_COUNTS}, ops=${VSHARD_BENCH_OPS})"
            VERBATIM
        )
        message(STATUS "vshard benchmark target: enabled (use 'cmake --build . --target benchmark-vshard')")
    else()
        message(STATUS "vshard benchmark target: DISABLED (userver-tarantool-vshard-sample not available)")
    endif()
endif()
