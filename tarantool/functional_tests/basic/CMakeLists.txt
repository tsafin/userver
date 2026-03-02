userver_testsuite_add_simple(
    NAME       tarantool-basic
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    PYTHON_BINARY ${PYTHON}
    PYTEST_ARGS
        --service-binary $<TARGET_FILE:tarantool-basic-service>
        --service-config ${CMAKE_CURRENT_SOURCE_DIR}/static_config.yaml
)
