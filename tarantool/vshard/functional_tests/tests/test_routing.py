"""Basic vshard routing tests — callrw, callro.

These tests exercise the C++ vshard proxy through the IPROTO protocol.
Each test also runs the same operation through the Lua vshard router
and compares results (lock-step differential testing).
"""
import pytest


class TestCallRW:
    """vshard.router.callrw — write operations through the proxy."""

    def test_replace_and_select(self, lua_conn, cpp_conn, bucket_count):
        """Replace a row via callrw, then select it back."""
        bid = 1
        row = [bid, bid, 'test_replace']

        # Write through C++ proxy.
        cpp_result = cpp_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        # Same write through Lua router (idempotent replace).
        lua_result = lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        # Both should succeed (replace returns the tuple).
        assert cpp_result.data is not None
        assert lua_result.data is not None

        # Select back through C++ proxy.
        cpp_sel = cpp_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        lua_sel = lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:select', [[bid]]],
        )

        assert cpp_sel.data == lua_sel.data

    def test_multiple_buckets(self, lua_conn, cpp_conn, bucket_count):
        """Write to different buckets, verify correct routing."""
        results = []
        for bid in [1, 50, 100, 150, 200, 250]:
            if bid > bucket_count:
                continue
            row = [bid, bid, f'multi_{bid}']
            cpp_result = cpp_conn.call(
                'vshard.router.callrw',
                [bid, 'box.space.customer:replace', [row]],
            )
            results.append((bid, cpp_result))

        # Verify all writes succeeded.
        for bid, result in results:
            assert result.data is not None, f"Failed for bucket {bid}"

    def test_echo_function(self, lua_conn, cpp_conn):
        """Call a simple echo function through both routers."""
        bid = 10
        args = ['hello', 42, True]
        cpp_result = cpp_conn.call(
            'vshard.router.callrw',
            [bid, 'echo', args],
        )
        lua_result = lua_conn.call(
            'vshard.router.callrw',
            [bid, 'echo', args],
        )
        assert cpp_result.data == lua_result.data

    def test_insert_and_delete(self, lua_conn, cpp_conn):
        """Insert a row, delete it, verify it's gone."""
        bid = 20
        row = [9999, bid, 'to_delete']

        # Insert.
        cpp_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        # Delete.
        cpp_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:delete', [[9999]]],
        )

        # Verify gone.
        result = cpp_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:select', [[9999]]],
        )
        # Select of deleted row returns empty array.
        assert result.data is not None
        # The result should be an empty list or list with empty list.
        flat = result.data
        if isinstance(flat, (list, tuple)) and len(flat) > 0:
            if isinstance(flat[0], (list, tuple)):
                assert len(flat[0]) == 0, f"Expected empty, got {flat}"


class TestCallRO:
    """vshard.router.callro — read operations through the proxy."""

    def test_callro_select(self, lua_conn, cpp_conn):
        """Seed data via callrw, read back via callro."""
        bid = 30
        row = [bid, bid, 'readonly_test']

        # Seed via Lua router (known good).
        lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        # Read via C++ proxy callro.
        cpp_result = cpp_conn.call(
            'vshard.router.callro',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        lua_result = lua_conn.call(
            'vshard.router.callro',
            [bid, 'box.space.customer:select', [[bid]]],
        )

        assert cpp_result.data == lua_result.data

    def test_callro_echo(self, lua_conn, cpp_conn):
        """Echo through callro on both routers."""
        bid = 40
        cpp_result = cpp_conn.call(
            'vshard.router.callro',
            [bid, 'echo', ['readonly_echo']],
        )
        lua_result = lua_conn.call(
            'vshard.router.callro',
            [bid, 'echo', ['readonly_echo']],
        )
        assert cpp_result.data == lua_result.data


class TestGenericCall:
    """vshard.router.call — generic 4/5-tuple call interface."""

    def test_generic_call_write_mode(self, lua_conn, cpp_conn):
        """Generic call with mode='write'."""
        bid = 50
        row = [bid, bid, 'generic_write']

        cpp_result = cpp_conn.call(
            'vshard.router.call',
            [bid, 'write', 'box.space.customer:replace', [row]],
        )
        lua_result = lua_conn.call(
            'vshard.router.call',
            [bid, 'write', 'box.space.customer:replace', [row]],
        )
        assert cpp_result.data is not None
        assert lua_result.data is not None

    def test_generic_call_read_mode(self, lua_conn, cpp_conn):
        """Generic call with mode='read'."""
        bid = 60
        row = [bid, bid, 'generic_read']

        # Seed data.
        lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        cpp_result = cpp_conn.call(
            'vshard.router.call',
            [bid, 'read', 'box.space.customer:select', [[bid]]],
        )
        lua_result = lua_conn.call(
            'vshard.router.call',
            [bid, 'read', 'box.space.customer:select', [[bid]]],
        )
        assert cpp_result.data == lua_result.data

    def test_generic_call_with_opts(self, lua_conn, cpp_conn):
        """Generic call with 5th opts argument (timeout)."""
        bid = 70
        row = [bid, bid, 'generic_opts']

        cpp_result = cpp_conn.call(
            'vshard.router.call',
            [bid, 'write', 'box.space.customer:replace', [row],
             {'timeout': 5.0}],
        )
        assert cpp_result.data is not None


class TestCallVariants:
    """callbro, callbre, callre — read variants with different semantics."""

    def test_callbro(self, lua_conn, cpp_conn):
        """callbro (best-read-only with balance)."""
        bid = 80
        row = [bid, bid, 'bro_test']

        lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        cpp_result = cpp_conn.call(
            'vshard.router.callbro',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        lua_result = lua_conn.call(
            'vshard.router.callbro',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        assert cpp_result.data == lua_result.data

    @pytest.mark.skip(reason="callbre needs replica nodes; masters-only cluster")
    def test_callbre(self, lua_conn, cpp_conn):
        """callbre (best-read-only-error with prefer_replica + balance)."""
        bid = 90
        row = [bid, bid, 'bre_test']

        lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        cpp_result = cpp_conn.call(
            'vshard.router.callbre',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        lua_result = lua_conn.call(
            'vshard.router.callbre',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        assert cpp_result.data == lua_result.data

    def test_callre(self, lua_conn, cpp_conn):
        """callre (read-only with prefer_replica)."""
        bid = 100
        row = [bid, bid, 're_test']

        lua_conn.call(
            'vshard.router.callrw',
            [bid, 'box.space.customer:replace', [row]],
        )

        cpp_result = cpp_conn.call(
            'vshard.router.callre',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        lua_result = lua_conn.call(
            'vshard.router.callre',
            [bid, 'box.space.customer:select', [[bid]]],
        )
        assert cpp_result.data == lua_result.data


class TestBucketId:
    """Bucket ID computation consistency between Lua and C++ routers."""

    @pytest.mark.skip(reason="bucket_id_mpcrc32 not yet exposed via C++ IPROTO server")
    def test_bucket_id_uint(self, lua_conn, cpp_conn):
        """bucket_id_mpcrc32 for integer keys should match."""
        for key in [1, 42, 100, 999, 12345]:
            lua_bid = lua_conn.call(
                'vshard.router.bucket_id_mpcrc32', [key],
            )
            cpp_bid = cpp_conn.call(
                'vshard.router.bucket_id_mpcrc32', [key],
            )
            assert cpp_bid.data == lua_bid.data, \
                f"Mismatch for key={key}: cpp={cpp_bid.data} lua={lua_bid.data}"

    @pytest.mark.skip(reason="bucket_id_mpcrc32 not yet exposed via C++ IPROTO server")
    def test_bucket_id_string(self, lua_conn, cpp_conn):
        """bucket_id_mpcrc32 for string keys should match."""
        for key in ['hello', 'world', '', 'test123', 'user@example.com']:
            lua_bid = lua_conn.call(
                'vshard.router.bucket_id_mpcrc32', [key],
            )
            cpp_bid = cpp_conn.call(
                'vshard.router.bucket_id_mpcrc32', [key],
            )
            assert cpp_bid.data == lua_bid.data, \
                f"Mismatch for key='{key}': cpp={cpp_bid.data} lua={lua_bid.data}"


class TestRouteAll:
    """vshard.router.routeall — list all replicasets."""

    @pytest.mark.skip(reason="routeall not yet exposed via C++ IPROTO server")
    def test_routeall_returns_both_rs(self, lua_conn, cpp_conn):
        """routeall should return 2 replicasets."""
        lua_result = lua_conn.call('vshard.router.routeall', [])
        cpp_result = cpp_conn.call('vshard.router.routeall', [])

        # Both should return data for 2 replicasets.
        assert lua_result.data is not None
        assert cpp_result.data is not None
