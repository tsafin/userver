"""Error handling tests for the C++ vshard proxy.

Tests WRONG_BUCKET retry, invalid bucket IDs, missing functions,
and other error scenarios.
"""
import os
import subprocess
import time
import uuid

import pytest
import tarantool

import conftest as test_conftest


def _call_outcome(conn, func_name, args):
    try:
        return ('data', conn.call(func_name, args).data)
    except Exception as exc:
        return ('exc', str(exc))


def _strip_trace_locations(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key == 'trace' and isinstance(item, list):
                result[key] = [
                    {
                        subkey: _strip_trace_locations(subitem)
                        for subkey, subitem in frame.items()
                        if subkey not in ('file', 'line')
                    }
                    if isinstance(frame, dict) else _strip_trace_locations(frame)
                    for frame in item
                ]
            else:
                result[key] = _strip_trace_locations(item)
        return result
    if isinstance(value, list):
        return [_strip_trace_locations(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_trace_locations(item) for item in value)
    return value


def _find_bucket_on_storage(port, bucket_count=3000):
    conn = tarantool.connect(
        '127.0.0.1', port, user='storage', password='storage',
    )
    try:
        for bucket_id in range(1, bucket_count + 1):
            result = conn.call('vshard.storage.bucket_stat', [bucket_id])
            if result.data and result.data[0] is not None:
                return bucket_id
    finally:
        conn.close()
    raise AssertionError(f"Could not find bucket on storage port {port}")


def _start_standalone_proxy(binary, secdist_config, tmp_path, proxy_port):
    config_path = test_conftest._generate_static_config(
        proxy_port, secdist_config, str(tmp_path)
    )
    config_text = open(config_path, 'r').read()
    config_text = config_text.replace(
        "            max_pool_size: 8\n",
        "            max_pool_size: 8\n"
        "            topology_refresh_interval: 100ms\n"
        "            moved_refresh_min_interval: 100ms\n",
        1,
    )
    with open(config_path, 'w') as config_file:
        config_file.write(config_text)
    proc = subprocess.Popen(
        [binary, '--config', config_path],
        stdout=open(os.path.join(tmp_path, 'proxy_stdout.log'), 'w'),
        stderr=open(os.path.join(tmp_path, 'proxy_stderr.log'), 'w'),
    )
    if not test_conftest._wait_for_port('127.0.0.1', proxy_port, timeout=15):
        proc.kill()
        raise RuntimeError(f"Standalone proxy on port {proxy_port} did not start")
    time.sleep(2)
    return proc


class TestInvalidBucket:
    """Requests with out-of-range or invalid bucket IDs."""

    def test_bucket_zero(self, cpp_conn):
        """Bucket 0 is invalid (1-based indexing)."""
        with pytest.raises(Exception):
            cpp_conn.call('vshard.router.callrw',
                          [0, 'echo', ['test']])

    def test_bucket_exceeds_count(self, cpp_conn, bucket_count):
        """Bucket > bucket_count should fail."""
        with pytest.raises(Exception):
            cpp_conn.call('vshard.router.callrw',
                          [bucket_count + 1, 'echo', ['test']])

    def test_bucket_negative(self, cpp_conn):
        """Negative bucket ID should fail."""
        with pytest.raises(Exception):
            cpp_conn.call('vshard.router.callrw',
                          [-1, 'echo', ['test']])


class TestMissingFunction:
    """Calls to non-existent storage functions."""

    def test_nonexistent_function(self, lua_conn, cpp_conn):
        """Calling a function that doesn't exist on storage."""
        bid = 10
        lua_result = lua_conn.call(
            'vshard.router.callrw', [bid, 'nonexistent_function_xyz', []]
        )
        cpp_result = cpp_conn.call(
            'vshard.router.callrw', [bid, 'nonexistent_function_xyz', []]
        )

        assert lua_result.data[0] is None
        assert _strip_trace_locations(cpp_result.data) == \
            _strip_trace_locations(lua_result.data)


class TestUnknownFunction:
    """Calls to unknown vshard.router.* function names."""

    def test_unknown_router_function(self, cpp_conn):
        """vshard.router.nonexistent should fail."""
        with pytest.raises(Exception):
            cpp_conn.call('vshard.router.nonexistent', [1, 'echo', []])


class TestWrongBucketRetry:
    """WRONG_BUCKET error should trigger route refresh and retry.

    We can't easily force WRONG_BUCKET without rebalancing,
    but we can verify that normal operations succeed even when
    the routing table starts stale (the proxy does discovery).
    """

    def test_consecutive_writes_different_buckets(self, cpp_conn, bucket_count):
        """Write to all bucket ranges — proxy should discover routes."""
        step = max(1, bucket_count // 20)
        for bid in range(1, bucket_count + 1, step):
            row = [bid, bid, f'wb_{bid}']
            result = cpp_conn.call(
                'vshard.router.callrw',
                [bid, 'box.space.customer:replace', [row]],
            )
            assert result.data is not None, f"Failed for bucket {bid}"


class TestDiscoveryClassification:
    """Discovery-only failures should match Lua router error classification."""

    def test_unreachable_replicaset_matches_lua(
        self,
        request,
        lua_conn,
        vshard_cluster,
        secdist_config,
        tmp_path,
    ):
        binary = request.config.getoption('--proxy-binary')
        if binary is None:
            pytest.skip("C++ proxy binary not specified")

        example_dir = vshard_cluster['example_dir']
        bucket_id = _find_bucket_on_storage(vshard_cluster['ports']['rs2_master'])
        proxy_port = 14000 + (uuid.uuid4().int % 1000)
        proc = None
        conn = None

        test_conftest._run_tarantoolctl(
            example_dir, 'stop', 'storage_2_a.lua', check=False
        )
        test_conftest._run_tarantoolctl(
            example_dir, 'stop', 'storage_2_b.lua', check=False
        )
        time.sleep(1)
        try:
            proc = _start_standalone_proxy(
                binary, secdist_config, tmp_path, proxy_port
            )
            conn = tarantool.Connection(
                '127.0.0.1', proxy_port, fetch_schema=False
            )
            conn.connect()

            args = [bucket_id, 'echo', ['probe']]
            lua_result = lua_conn.call('vshard.router.callrw', args)
            cpp_result = conn.call('vshard.router.callrw', args)

            assert lua_result.data[0] is None
            assert _strip_trace_locations(cpp_result.data) == \
                _strip_trace_locations(lua_result.data)
        finally:
            if conn is not None:
                conn.close()
            if proc is not None:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.terminate()
            test_conftest._run_tarantoolctl(
                example_dir, 'start', 'storage_2_a.lua', check=False
            )
            test_conftest._run_tarantoolctl(
                example_dir, 'start', 'storage_2_b.lua', check=False
            )
            test_conftest._wait_for_storage_ready(
                vshard_cluster['ports']['rs2_master']
            )


class TestTimeout:
    """Per-call timeout tests."""

    def test_short_timeout_succeeds(self, cpp_conn):
        """Normal operation with explicit short timeout should work."""
        bid = 10
        # 5 second timeout — more than enough for a simple call.
        result = cpp_conn.call(
            'vshard.router.call',
            [bid, 'write', 'box.space.customer:replace',
             [[bid, bid, 'timeout_test']], {'timeout': 5.0}],
        )
        assert result.data is not None

    def test_request_timeout_must_not_exceed_timeout(self, lua_conn, cpp_conn):
        """Lua-compatible validation: request_timeout must be <= timeout."""
        args = [
            10, 'write', 'echo', ['ok'],
            {'timeout': 0.2, 'request_timeout': 0.3},
        ]

        with pytest.raises(Exception, match='request_timeout must be <= timeout'):
            lua_conn.call('vshard.router.call', args)
        with pytest.raises(Exception, match='request_timeout must be <= timeout'):
            cpp_conn.call('vshard.router.call', args)

    def test_request_timeout_is_per_attempt_not_total(self, lua_conn, cpp_conn):
        """request_timeout behavior should match Lua on this Tarantool version."""
        args = [
            10, 'write', 'sleep', [0.2],
            {'timeout': 1.0, 'request_timeout': 0.05},
        ]

        lua_outcome = _call_outcome(lua_conn, 'vshard.router.call', args)
        cpp_outcome = _call_outcome(cpp_conn, 'vshard.router.call', args)

        assert cpp_outcome == lua_outcome
