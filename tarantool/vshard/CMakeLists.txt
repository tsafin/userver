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
endif()
