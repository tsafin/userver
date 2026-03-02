"""
Pytest plugin providing Tarantool fixtures for userver functional tests.
"""

import dataclasses
import pytest


@dataclasses.dataclass
class TarantoolConnInfo:
    host: str
    port: int


def pytest_addoption(parser):
    parser.addoption(
        '--tarantool-port',
        default='13301',
        help='Tarantool port for functional tests',
    )


@pytest.fixture(scope='session')
def tarantool_port(pytestconfig):
    return int(pytestconfig.getoption('--tarantool-port', default=13301))


@pytest.fixture(scope='session')
def tarantool_conn_info(tarantool_port) -> TarantoolConnInfo:
    """Connection info for a running Tarantool instance.

    Assumes Tarantool is already started (by CI, docker-compose, or testsuite).
    Override this fixture to customize host/port.
    """
    return TarantoolConnInfo(host='localhost', port=tarantool_port)
