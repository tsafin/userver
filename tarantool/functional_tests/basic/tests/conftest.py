"""Conftest for Tarantool basic functional tests."""
import json
import pathlib

import pytest

pytest_plugins = ['pytest_userver.plugins.tarantool']


@pytest.fixture(scope='session')
def tarantool_service_script(service_source_dir) -> pathlib.Path:
    return service_source_dir / 'scripts' / 'service-tarantool'


@pytest.fixture(scope='session')
def service_env(tarantool_conn_info) -> dict:
    secdist_config = {
        'tarantool_settings': {
            'tarantool-database': {
                'hosts': [tarantool_conn_info.host],
                'port': tarantool_conn_info.port,
                'user': 'guest',
                'password': '',
            },
        },
    }
    return {'SECDIST_CONFIG': json.dumps(secdist_config)}
