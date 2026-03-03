project(userver-tarantool-tests-basic CXX)

add_executable(${PROJECT_NAME} service.cpp)
target_link_libraries(${PROJECT_NAME} userver-tarantool)

userver_testsuite_add(
    SERVICE_TARGET ${PROJECT_NAME}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests
    PYTHON_BINARY ${TESTSUITE_PYTHON_BINARY}
    PYTEST_ARGS
        --service-config=${CMAKE_CURRENT_SOURCE_DIR}/static_config.yaml
        --service-source-dir=${CMAKE_CURRENT_SOURCE_DIR}
        --service-binary=${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}
        --config-fallback=${CMAKE_CURRENT_SOURCE_DIR}/dynamic_config_fallback.json
        ${PYTEST_ARGS_CONFIG_VARS}
)

if (TEST testsuite-${PROJECT_NAME})
    set_tests_properties(testsuite-${PROJECT_NAME} PROPERTIES
        ENVIRONMENT "TESTSUITE_TARANTOOL_START_TIMEOUT=20.0")
endif()

