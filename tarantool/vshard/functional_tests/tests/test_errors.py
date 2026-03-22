"""Error handling tests for the C++ vshard proxy.

Tests WRONG_BUCKET retry, invalid bucket IDs, missing functions,
and other error scenarios.
"""
import pytest


def _call_outcome(conn, func_name, args):
    try:
        return ('data', conn.call(func_name, args).data)
    except Exception as exc:
        return ('exc', str(exc))


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
        assert cpp_result.data == lua_result.data


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
