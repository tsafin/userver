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
    )
    target_compile_definitions(${PROJECT_NAME}_unittest PRIVATE
        DEFAULT_DYNAMIC_CONFIG_FILENAME="${CMAKE_SOURCE_DIR}/tarantool/tests/dynamic_config_fallback.json")
    add_google_tests(${PROJECT_NAME}_unittest)
endif()
