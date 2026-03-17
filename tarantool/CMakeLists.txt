project(userver-tarantool CXX)

file(GLOB_RECURSE SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.hpp)

file(GLOB_RECURSE UNIT_TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp)
list(REMOVE_ITEM SOURCES ${UNIT_TEST_SOURCES})

file(GLOB_RECURSE TT_TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*_tttest.cpp)
list(REMOVE_ITEM SOURCES ${TT_TEST_SOURCES})

file(GLOB_RECURSE FUNCTIONAL_TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/functional_tests/*)
list(REMOVE_ITEM SOURCES ${FUNCTIONAL_TEST_SOURCES})

add_library(${PROJECT_NAME} STATIC ${SOURCES})

if (USERVER_CONAN)
    find_package(tntcxx REQUIRED)
    target_link_libraries(${PROJECT_NAME} PRIVATE tntcxx::tntcxx)
else()
    include(SetupTarantoolCPP)
    target_link_libraries(${PROJECT_NAME} PRIVATE tntcxx)
endif()

target_link_libraries(${PROJECT_NAME}
    PUBLIC userver-core
)

target_include_directories(${PROJECT_NAME}
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

if (USERVER_IS_THE_ROOT_PROJECT)
    # Also include universal formats/msgpack tests (compiled only here since
    # USERVER_BUILD_TESTS=OFF in the tarantool-as-root-project build).
    file(GLOB_RECURSE UNIVERSAL_MSGPACK_TEST_SOURCES
        ${CMAKE_SOURCE_DIR}/universal/src/formats/msgpack/*_test.cpp)
    list(APPEND UNIT_TEST_SOURCES ${UNIVERSAL_MSGPACK_TEST_SOURCES})

    if (UNIT_TEST_SOURCES)
        add_executable(${PROJECT_NAME}_unittest ${UNIT_TEST_SOURCES})
        target_include_directories(${PROJECT_NAME}_unittest PRIVATE
            $<TARGET_PROPERTY:${PROJECT_NAME},INCLUDE_DIRECTORIES>
            $<TARGET_PROPERTY:userver-universal,INCLUDE_DIRECTORIES>)
        if (USERVER_CONAN)
            target_link_libraries(${PROJECT_NAME}_unittest userver-utest ${PROJECT_NAME} tntcxx::tntcxx)
        else()
            target_link_libraries(${PROJECT_NAME}_unittest userver-utest ${PROJECT_NAME} tntcxx)
        endif()
        target_compile_definitions(${PROJECT_NAME}_unittest PRIVATE
            DEFAULT_DYNAMIC_CONFIG_FILENAME="${CMAKE_CURRENT_SOURCE_DIR}/tests/dynamic_config_fallback.json")
        add_google_tests(${PROJECT_NAME}_unittest)
    endif()

    if (TT_TEST_SOURCES)
        add_executable(${PROJECT_NAME}_tttest ${TT_TEST_SOURCES})
        target_include_directories(${PROJECT_NAME}_tttest PRIVATE
            $<TARGET_PROPERTY:${PROJECT_NAME},INCLUDE_DIRECTORIES>)
        target_link_libraries(${PROJECT_NAME}_tttest userver-utest ${PROJECT_NAME})
        add_test(${PROJECT_NAME}_tttest
            env
            ${CMAKE_BINARY_DIR}/testsuite/env
            --databases=tarantool
            run --
            ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}_tttest
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test-results/${PROJECT_NAME}_tttest.xml
        )
        set_tests_properties(${PROJECT_NAME}_tttest PROPERTIES ENVIRONMENT
            "TESTSUITE_TARANTOOL_SERVER_START_TIMEOUT=30.0")
    endif()

    add_subdirectory(functional_tests)
    add_subdirectory(vshard)
endif()
