"""
Pytest plugin providing Tarantool fixtures for userver functional tests.

Registers a 'tarantool' service that testsuite can auto-start via a shell
script (following the same ScriptService pattern as clickhouse/redis plugins).
The script path is provided by the ``tarantool_service_script`` fixture which
tests override to point at their ``scripts/service-tarantool``.
"""

import dataclasses
import pathlib
import typing

import pytest
import tarantool

from testsuite.environment import service, utils


pytest_plugins = ['pytest_userver.plugins.core']


DEFAULT_TARANTOOL_PORT = 13301


@dataclasses.dataclass
class TarantoolConnInfo:
    host: str
    port: int


class ServiceSettings(typing.NamedTuple):
    port: int
    script: str      # path to scripts/service-tarantool
    init_lua: str    # path to tarantool_init.lua
    tmpdir: str

    def get_connection_info(self) -> TarantoolConnInfo:
        return TarantoolConnInfo(host='localhost', port=self.port)


def pytest_addoption(parser):
    parser.addoption(
        '--tarantool-port',
        default=str(DEFAULT_TARANTOOL_PORT),
        help='Tarantool port for functional tests',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'tarantool_store: per-test tarantool space data initialization',
    )


def pytest_service_register(register_service):
    register_service('tarantool', _create_tarantool_service)


def _create_tarantool_service(
    service_name,
    working_dir,
    settings: ServiceSettings | None = None,
    env: dict[str, str] | None = None,
):
    if settings is None:
        raise RuntimeError(
            'tarantool service requires a ServiceSettings instance; '
            'override the tarantool_service_settings fixture',
        )
    return service.ScriptService(
        service_name=service_name,
        script_path=settings.script,
        working_dir=working_dir,
        environment={
            'TARANTOOL_PORT': str(settings.port),
            'TARANTOOL_TMPDIR': settings.tmpdir,
            'TARANTOOL_INIT_LUA': settings.init_lua,
            **(env or {}),
        },
        check_ports=[settings.port],
        start_timeout=utils.getenv_float(
            key='TESTSUITE_TARANTOOL_START_TIMEOUT',
            default=15.0,
        ),
    )


@pytest.fixture(scope='session')
def tarantool_port(pytestconfig) -> int:
    return int(
        pytestconfig.getoption('--tarantool-port', default=DEFAULT_TARANTOOL_PORT),
    )


@pytest.fixture(scope='session')
def tarantool_service_script() -> pathlib.Path | None:
    """Path to the ``scripts/service-tarantool`` shell script.

    Override in your conftest.py to point at the test-local script:

        @pytest.fixture(scope='session')
        def tarantool_service_script(service_source_dir):
            return service_source_dir / 'scripts' / 'service-tarantool'
    """
    return None


@pytest.fixture(scope='session')
def tarantool_service_settings(
    tarantool_port,
    tarantool_service_script,
    tmp_path_factory,
) -> ServiceSettings | None:
    """Session-scoped Tarantool ServiceSettings.

    Returns None when no service script is configured (external Tarantool).
    Override ``tarantool_service_script`` fixture to enable auto-start.
    """
    if tarantool_service_script is None:
        return None
    script = pathlib.Path(tarantool_service_script)
    init_lua = str(script.parent.parent / 'tarantool_init.lua')
    tmpdir = str(tmp_path_factory.mktemp('tarantool'))
    return ServiceSettings(
        port=tarantool_port,
        script=str(script),
        init_lua=init_lua,
        tmpdir=tmpdir,
    )


@pytest.fixture(scope='session')
def tarantool_service(
    pytestconfig,
    tarantool_service_settings,
    ensure_service_started,
):
    """Ensures Tarantool service is started for the test session.

    Analogous to the ``redis_service`` fixture from
    ``testsuite.databases.redis.pytest_plugin``.
    """
    if tarantool_service_settings is not None:
        ensure_service_started('tarantool', settings=tarantool_service_settings)


@pytest.fixture(scope='session')
def tarantool_conn_info(
    tarantool_service,
    tarantool_service_settings,
    tarantool_port,
) -> TarantoolConnInfo:
    """Connection info for the Tarantool instance."""
    if tarantool_service_settings is not None:
        return tarantool_service_settings.get_connection_info()
    return TarantoolConnInfo(host='localhost', port=tarantool_port)


@pytest.fixture(scope='session')
def tarantool_spaces_to_truncate() -> list[str]:
    """List of space names to truncate between tests.

    Override in your conftest.py to specify which spaces to clean:

        @pytest.fixture(scope='session')
        def tarantool_spaces_to_truncate():
            return ['kv', 'accounts']
    """
    return []


@pytest.fixture
def tarantool_store(tarantool_conn_info, tarantool_spaces_to_truncate):
    """Per-test direct Tarantool connection with automatic space cleanup.

    Analogous to the ``redis_store`` fixture (which calls ``flushall()``).
    Truncates all spaces listed in ``tarantool_spaces_to_truncate`` after
    each test, ensuring a clean state for the next test.

    Example usage::

        async def test_something(service_client, tarantool_store):
            tarantool_store.insert('kv', (42, 'hello'))
            resp = await service_client.get('/kv?id=42')
            assert resp.status == 200
    """
    conn = tarantool.Connection(
        host=tarantool_conn_info.host,
        port=tarantool_conn_info.port,
        user='guest',
        password='',
        connect_now=True,
    )
    try:
        yield conn
    finally:
        for space_name in tarantool_spaces_to_truncate:
            try:
                conn.call(f'box.space.{space_name}:truncate')
            except Exception:  # pylint: disable=broad-except
                pass
        conn.close()
