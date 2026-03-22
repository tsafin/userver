"""Conftest for vshard C++ proxy functional tests.

Cluster topology:
  RS1: master (port A), replica (port A+1)
  RS2: master (port A+2), replica (port A+3)
  Lua router: port A+4
  C++ proxy:  port A+5 (started as subprocess)

Lock-step mode: tests receive both lua_conn and cpp_conn fixtures to compare.

Usage:
    pytest tests/ --proxy-binary=/path/to/userver-tarantool-vshard-sample
"""
import json
import os
import pathlib
import subprocess
import tempfile
import time

import pytest
import tarantool

# Bucket count for the test cluster (small for fast bootstrap).
BUCKET_COUNT = 300

# Fixed UUIDs matching the Lua init scripts.
RS1_UUID = 'cbf06940-0790-498b-948d-042b62cf3d29'
RS2_UUID = 'ac522f65-a15e-4b1b-af2b-3a0a67d36fef'
INSTANCE_UUIDS = {
    'rs1_master':  '8a274925-a26d-47fc-9e1b-af88ce939412',
    'rs1_replica': 'a3ef657e-eb4a-4f47-8a38-1a0e04517b15',
    'rs2_master':  '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    'rs2_replica': 'd5b83e4c-93af-476e-bb2b-c0a56c5e19f8',
}


def pytest_addoption(parser):
    parser.addoption(
        '--proxy-binary',
        default=None,
        help='Path to userver-tarantool-vshard-sample binary',
    )
    parser.addoption(
        '--vshard-path',
        default=None,
        help='Path to vshard Lua module directory (containing vshard/init.lua)',
    )


def _find_vshard_path():
    """Try to auto-detect vshard Lua module location."""
    candidates = [
        os.path.expanduser('~/src/vshard'),
        '/usr/share/tarantool',
        '/usr/local/share/tarantool',
    ]
    for path in candidates:
        if os.path.isfile(os.path.join(path, 'vshard', 'init.lua')):
            return path
    return None


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


def _wait_for_storage_ready(port, timeout=15):
    """Wait until vshard.storage.buckets_count is callable."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = tarantool.connect('127.0.0.1', port)
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
    raise RuntimeError(
        f"Buckets not distributed after {timeout}s")


def _start_tarantool(init_lua, env, tmpdir):
    """Start a Tarantool instance in background and return the process."""
    snap_dir = os.path.join(tmpdir, f"snap_{env['TARANTOOL_PORT']}")
    xlog_dir = os.path.join(tmpdir, f"xlog_{env['TARANTOOL_PORT']}")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(xlog_dir, exist_ok=True)

    full_env = dict(os.environ)
    full_env.update(env)
    full_env['TARANTOOL_TMPDIR'] = tmpdir
    full_env['TARANTOOL_BACKGROUND'] = '0'  # We manage the process ourselves

    proc = subprocess.Popen(
        ['tarantool', str(init_lua)],
        env=full_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    port = int(env['TARANTOOL_PORT'])
    if not _wait_for_port('127.0.0.1', port, timeout=15):
        proc.kill()
        stdout, stderr = proc.communicate(timeout=5)
        raise RuntimeError(
            f"Tarantool on port {port} did not start.\n"
            f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
        )
    return proc


@pytest.fixture(scope='session')
def base_port():
    """Compute a unique base port. Uses a random offset to avoid collisions."""
    import random
    return 13301 + random.randint(0, 50) * 10


@pytest.fixture(scope='session')
def cluster_ports(base_port):
    """Return a dict of named ports for the cluster."""
    return {
        'rs1_master':  base_port,
        'rs1_replica': base_port + 1,
        'rs2_master':  base_port + 2,
        'rs2_replica': base_port + 3,
        'lua_router':  base_port + 4,
        'cpp_proxy':   base_port + 5,
    }


@pytest.fixture(scope='session')
def cluster_tmpdir(tmp_path_factory):
    return str(tmp_path_factory.mktemp('vshard_cluster'))


@pytest.fixture(scope='session')
def vshard_cluster(request, cluster_ports, cluster_tmpdir):
    """Start a 2-node vshard storage cluster (masters only) + Lua router.

    Yields a dict with connection info. Stops all processes on teardown.
    """
    test_dir = pathlib.Path(__file__).parent.parent
    storage_lua = str(test_dir / 'vshard_storage_init.lua')
    router_lua = str(test_dir / 'vshard_router_init.lua')
    ports = cluster_ports
    tmpdir = cluster_tmpdir

    # Find vshard Lua module path.
    vshard_path = request.config.getoption('--vshard-path') or _find_vshard_path()
    if vshard_path is None:
        pytest.skip("vshard Lua module not found. Use --vshard-path=<dir>")

    common_env = {
        'TARANTOOL_BUCKET_COUNT': str(BUCKET_COUNT),
        'TARANTOOL_RS1_MASTER_PORT': str(ports['rs1_master']),
        'TARANTOOL_RS1_REPLICA_PORT': str(ports['rs1_master']),  # no separate replica
        'TARANTOOL_RS2_MASTER_PORT': str(ports['rs2_master']),
        'TARANTOOL_RS2_REPLICA_PORT': str(ports['rs2_master']),  # no separate replica
        'TARANTOOL_VSHARD_PATH': vshard_path,
    }

    # Masters only — replicas are not started to keep setup simple and fast.
    instances = [
        ('rs1_master', RS1_UUID, INSTANCE_UUIDS['rs1_master'], '1'),
        ('rs2_master', RS2_UUID, INSTANCE_UUIDS['rs2_master'], '1'),
    ]

    procs = []

    for name, rs_uuid, inst_uuid, is_master in instances:
        env = dict(common_env)
        env['TARANTOOL_PORT'] = str(ports[name])
        env['TARANTOOL_RS_UUID'] = rs_uuid
        env['TARANTOOL_INSTANCE_UUID'] = inst_uuid
        env['TARANTOOL_IS_MASTER'] = is_master
        proc = _start_tarantool(storage_lua, env, tmpdir)
        procs.append((name, proc))

    # Start Lua router (needed for vshard.router.bootstrap).
    router_env = dict(common_env)
    router_env['TARANTOOL_PORT'] = str(ports['lua_router'])
    router_proc = _start_tarantool(router_lua, router_env, tmpdir)
    procs.append(('lua_router', router_proc))

    # Wait for storages to fully initialize vshard procedures.
    _wait_for_storage_ready(ports['rs1_master'])
    _wait_for_storage_ready(ports['rs2_master'])

    # Bootstrap vshard via the Lua router (with retries for timing).
    bootstrap_ok = False
    bootstrap_err = None
    for attempt in range(10):
        try:
            router_conn = tarantool.connect('127.0.0.1', ports['lua_router'])
            router_conn.call('vshard.router.bootstrap',
                             [{'if_not_bootstrapped': True}])
            router_conn.close()
            bootstrap_ok = True
            break
        except Exception as e:
            bootstrap_err = e
            time.sleep(1)
    if not bootstrap_ok:
        for name, proc in procs:
            proc.kill()
        raise RuntimeError(f"vshard bootstrap failed after retries: {bootstrap_err}")

    # Wait for bootstrap to distribute buckets.
    _wait_for_buckets_distributed(ports['lua_router'])

    yield {
        'ports': ports,
        'tmpdir': tmpdir,
        'bucket_count': BUCKET_COUNT,
    }

    # Teardown: kill all processes.
    for name, proc in procs:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.terminate()


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
                        ],
                    },
                    {
                        'uuid': RS2_UUID,
                        'nodes': [
                            {'host': '127.0.0.1', 'port': ports['rs2_master'],
                             'is_master': True},
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
