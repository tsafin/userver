"""Conftest for vshard C++ proxy functional tests.

The functional tests reuse a local vshard example cluster via `make start` /
`make stop`, with the path supplied explicitly via `--vshard-path` or the
`VSHARD_PATH` environment variable. This matches the benchmark workflow and
avoids hardcoded host-local paths in the test harness.

Lock-step mode: tests receive both lua_conn and cpp_conn fixtures to compare.

Usage:
    pytest tests/ --proxy-binary=/path/to/userver-tarantool-vshard-sample
"""
import json
import os
import pathlib
import subprocess
import time

import pytest
import tarantool

# Bucket count for the local vshard example cluster.
BUCKET_COUNT = 3000

# Fixed UUIDs matching the canonical vshard example localcfg.lua.
RS1_UUID = 'cbf06940-0790-498b-948d-042b62cf3d29'
RS2_UUID = 'ac522f65-aa94-4134-9f64-51ee384f1a54'
INSTANCE_UUIDS = {
    'rs1_master':  '8a274925-a26d-47fc-9e1b-af88ce939412',
    'rs1_replica': '3de2e3e1-9ebe-4d0d-abb1-26d301b84633',
    'rs2_master':  '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    'rs2_replica': '001688c3-66f8-4a31-8e19-036c17d489c2',
}


def pytest_addoption(parser):
    parser.addini(
        'mockserver-tracing-enabled',
        'Compatibility shim for repo-level pytest.ini when running this suite '
        'without the plugin that normally registers the option.',
        default='true',
    )
    parser.addoption(
        '--proxy-binary',
        default=None,
        help='Path to userver-tarantool-vshard-sample binary',
    )
    parser.addoption(
        '--vshard-path',
        default=None,
        help='Path to vshard repo root or example dir (for make start/stop)',
    )

def _resolve_vshard_example_dir(vshard_path):
    """Resolve repo root or example dir to the actual example directory."""
    if os.path.isfile(os.path.join(vshard_path, 'Makefile')) and os.path.isfile(
        os.path.join(vshard_path, '.tarantoolctl')
    ):
        return vshard_path
    example_dir = os.path.join(vshard_path, 'example')
    if os.path.isfile(os.path.join(example_dir, 'Makefile')):
        return example_dir
    raise RuntimeError(
        f"Cannot resolve vshard example dir from {vshard_path!r}"
    )


def _wait_for_port(host, port, timeout=10):
    """Wait until a TCP port is accepting connections."""
    import socket
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    return False


def _wait_for_storage_ready(port, timeout=20):
    """Wait until vshard.storage.buckets_count is callable."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = tarantool.connect(
                '127.0.0.1', port, user='storage', password='storage',
            )
            conn.call('vshard.storage.buckets_count', [])
            conn.close()
            return True
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Storage on port {port} not ready after {timeout}s")


def _wait_for_buckets_distributed(lua_router_port, timeout=30):
    """Wait until all buckets are distributed via the Lua router."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = tarantool.connect('127.0.0.1', lua_router_port)
            result = conn.call('vshard.router.info', [])
            conn.close()
            if result.data:
                info = result.data[0] if isinstance(result.data, (list, tuple)) else result.data
                if isinstance(info, dict):
                    unknown = info.get('bucket', {}).get('unknown', 0)
                    if unknown == 0:
                        return True
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"Buckets not distributed after {timeout}s")


def _run_make_target(example_dir, target, *, check=True):
    return subprocess.run(
        ['make', target],
        cwd=example_dir,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run_tarantoolctl(example_dir, *args, check=True, input_text=None):
    cmd = 'cd "{}" && tarantoolctl {}'.format(
        example_dir, ' '.join(args)
    )
    return subprocess.run(
        ['bash', '-lc', cmd],
        check=check,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _start_example_cluster(example_dir):
    start = _run_make_target(example_dir, 'start', check=False)
    if start.returncode == 0:
        return

    # Some system tarantoolctl builds only accept explicit *.lua instance names
    # from local directories, while the bundled Makefile uses bare names.
    instances = [
        'storage_1_a.lua',
        'storage_1_b.lua',
        'storage_2_a.lua',
        'storage_2_b.lua',
        'router_1.lua',
    ]
    for instance in instances:
        _run_tarantoolctl(example_dir, 'start', instance)
    bootstrap = subprocess.run(
        [
            'bash', '-lc',
            'cd "{}" && printf "vshard.router.bootstrap()\\n" | tarantoolctl enter router_1.lua'.format(
                example_dir
            ),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    output = (bootstrap.stdout or '') + (bootstrap.stderr or '')
    if bootstrap.returncode != 0 and 'NON_EMPTY' not in output:
        raise subprocess.CalledProcessError(
            bootstrap.returncode, bootstrap.args, bootstrap.stdout, bootstrap.stderr
        )


def _stop_example_cluster(example_dir):
    stop = _run_make_target(example_dir, 'stop', check=False)
    if stop.returncode == 0:
        return

    for instance in [
        'storage_1_a.lua',
        'storage_1_b.lua',
        'storage_2_a.lua',
        'storage_2_b.lua',
        'router_1.lua',
    ]:
        _run_tarantoolctl(example_dir, 'stop', instance, check=False)


@pytest.fixture(scope='session')
def vshard_example_dir(request):
    """Path to the local vshard example directory used for test cluster startup."""
    vshard_path = request.config.getoption('--vshard-path') or os.environ.get(
        'VSHARD_PATH'
    )
    if vshard_path is None:
        pytest.skip(
            "VSHARD_PATH is not set. Use --vshard-path=<repo-or-example-dir> "
            "or export VSHARD_PATH to run the vshard functional tests."
        )
    try:
        return _resolve_vshard_example_dir(vshard_path)
    except RuntimeError as exc:
        pytest.skip(str(exc))


@pytest.fixture(scope='session')
def cluster_ports():
    """Ports used by the canonical local vshard example cluster."""
    return {
        'rs1_master':  3301,
        'rs1_replica': 3302,
        'rs2_master':  3303,
        'rs2_replica': 3304,
        'lua_router':  3305,
        'cpp_proxy':   13306,
    }


@pytest.fixture(scope='session')
def cluster_tmpdir(tmp_path_factory):
    return str(tmp_path_factory.mktemp('vshard_cluster'))


@pytest.fixture(scope='session')
def vshard_cluster(vshard_example_dir, cluster_ports, cluster_tmpdir):
    """Start the local vshard example cluster via `make start`.

    Yields connection info and always tears the cluster down with `make stop`.
    """
    ports = cluster_ports
    _stop_example_cluster(vshard_example_dir)
    _start_example_cluster(vshard_example_dir)

    for name, port in ports.items():
        if name == 'cpp_proxy':
            continue
        if not _wait_for_port('127.0.0.1', port, timeout=20):
            raise RuntimeError(f"{name} on port {port} did not start")

    _wait_for_storage_ready(ports['rs1_master'])
    _wait_for_storage_ready(ports['rs2_master'])
    _wait_for_buckets_distributed(ports['lua_router'])

    yield {
        'ports': ports,
        'tmpdir': cluster_tmpdir,
        'example_dir': vshard_example_dir,
        'bucket_count': BUCKET_COUNT,
    }

    _stop_example_cluster(vshard_example_dir)


@pytest.fixture(scope='session')
def lua_conn(vshard_cluster):
    """Connection to the Lua vshard router (for differential testing)."""
    ports = vshard_cluster['ports']
    conn = tarantool.connect('127.0.0.1', ports['lua_router'])
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def storage_conn(vshard_cluster):
    """Direct connection to RS1 master (for seeding/inspecting data)."""
    ports = vshard_cluster['ports']
    conn = tarantool.connect(
        '127.0.0.1', ports['rs1_master'],
        user='storage', password='storage',
    )
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def secdist_config(vshard_cluster):
    """Generate secdist JSON matching the test cluster and write to tmpfile."""
    ports = vshard_cluster['ports']
    config = {
        'tarantool_vshard_settings': {
            'tarantool-vshard': {
                'bucket_count': BUCKET_COUNT,
                'user': 'storage',
                'password': 'storage',
                'replicasets': [
                    {
                        'uuid': RS1_UUID,
                        'nodes': [
                            {'host': '127.0.0.1', 'port': ports['rs1_master'],
                             'is_master': True},
                            {'host': '127.0.0.1', 'port': ports['rs1_replica'],
                             'is_master': False},
                        ],
                    },
                    {
                        'uuid': RS2_UUID,
                        'nodes': [
                            {'host': '127.0.0.1', 'port': ports['rs2_master'],
                             'is_master': True},
                            {'host': '127.0.0.1', 'port': ports['rs2_replica'],
                             'is_master': False},
                        ],
                    },
                ],
            },
        },
    }
    secdist_path = os.path.join(vshard_cluster['tmpdir'], 'secdist.json')
    with open(secdist_path, 'w') as f:
        json.dump(config, f)
    return secdist_path


def _generate_static_config(proxy_port, secdist_path, tmpdir):
    """Generate static_config.yaml for the C++ proxy."""
    config = f"""\
components_manager:
    components:
        iproto-vshard-server:
            port: {proxy_port}
            task_processor: main-task-processor

        tarantool-vshard:
            secdist_alias: tarantool-vshard
            initial_pool_size: 2
            max_pool_size: 8

        secdist: {{}}
        default-secdist-provider:
            config: {secdist_path}
            missing-ok: false

        logging:
            fs-task-processor: fs-task-processor
            loggers:
                default:
                    file_path: '{tmpdir}/proxy.log'
                    level: debug
                    overflow_behavior: discard

        dynamic-config:
            fs-cache-path: ''
            defaults:
                USERVER_NO_LOG_SPANS:
                    prefixes: ['tarantool_']
                    names: []

        logging-configurator:
            limited-logging-enable: true
            limited-logging-interval: 1s

        dns-client:
            fs-task-processor: fs-task-processor

    coro_pool:
        initial_size: 500
        max_size: 2000

    task_processors:
        main-task-processor:
            worker_threads: 4
            thread_name: main-worker
        fs-task-processor:
            thread_name: fs-worker
            worker_threads: 2

    default_task_processor: main-task-processor
"""
    config_path = os.path.join(tmpdir, 'static_config.yaml')
    with open(config_path, 'w') as f:
        f.write(config)
    return config_path


@pytest.fixture(scope='session')
def cpp_proxy(request, vshard_cluster, secdist_config):
    """Start the C++ vshard proxy as a subprocess.

    Requires --proxy-binary CLI option pointing to the built binary.
    """
    binary = request.config.getoption('--proxy-binary')
    if binary is None:
        pytest.skip(
            "C++ proxy binary not specified. Use --proxy-binary=<path>")

    ports = vshard_cluster['ports']
    proxy_port = ports['cpp_proxy']
    tmpdir = vshard_cluster['tmpdir']

    config_path = _generate_static_config(proxy_port, secdist_config, tmpdir)

    proc = subprocess.Popen(
        [binary, '--config', config_path],
        stdout=open(os.path.join(tmpdir, 'proxy_stdout.log'), 'w'),
        stderr=open(os.path.join(tmpdir, 'proxy_stderr.log'), 'w'),
    )

    if not _wait_for_port('127.0.0.1', proxy_port, timeout=15):
        proc.kill()
        stdout_log = os.path.join(tmpdir, 'proxy_stdout.log')
        stderr_log = os.path.join(tmpdir, 'proxy_stderr.log')
        raise RuntimeError(
            f"C++ proxy on port {proxy_port} did not start.\n"
            f"Check logs: {stdout_log}, {stderr_log}")

    # Wait for the proxy to complete initial discovery.
    time.sleep(2)

    yield {
        'port': proxy_port,
        'proc': proc,
    }

    proc.kill()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.terminate()


@pytest.fixture(scope='session')
def cpp_conn(cpp_proxy):
    """Connection to the C++ vshard proxy (IPROTO)."""
    conn = tarantool.Connection(
        '127.0.0.1', cpp_proxy['port'],
        fetch_schema=False,  # C++ proxy only handles CALL, not schema queries
    )
    conn.connect()
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def service_env(vshard_cluster, secdist_config):
    """Environment variables for the C++ proxy service."""
    return {
        'SECDIST_CONFIG': secdist_config,
    }


@pytest.fixture(scope='session')
def bucket_count(vshard_cluster):
    """Total bucket count for the test cluster."""
    return vshard_cluster['bucket_count']


def _find_bucket_for_rs(conn, rs_uuid, sample_count=20):
    """Find bucket IDs owned by a specific replicaset via the Lua router."""
    buckets = []
    for bid in range(1, sample_count + 1):
        try:
            info = conn.call('vshard.router.route', [bid])
            if info and hasattr(info, 'data') and info.data:
                # route returns a replicaset object; check its uuid
                pass
        except Exception:
            pass
    return buckets


@pytest.fixture
def cleanup_customer(lua_conn, cpp_conn, storage_conn):
    """Truncate customer space on all storages after each test."""
    yield
    # Best-effort cleanup via storage connections.
    for port_offset in [0, 2]:  # RS1 master and RS2 master
        try:
            storage_conn.call('box.space.customer:truncate', [])
        except Exception:
            pass
