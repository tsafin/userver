"""Lock-step differential tests: compare Lua router vs C++ proxy byte-for-byte.

Each test sends the EXACT same request to both routers and asserts
the responses are equivalent. This catches any behavioral divergence
between the Lua vshard reference and our C++ implementation.
"""
import pytest


class TestDifferentialCallRW:
    """Differential callrw tests — write path."""

    @pytest.mark.parametrize('bid', [1, 10, 50, 100, 150, 200, 250])
    def test_replace_differential(self, lua_conn, cpp_conn, bid, bucket_count):
        """Replace on each bucket produces identical results."""
        if bid > bucket_count:
            pytest.skip(f"bucket {bid} > bucket_count {bucket_count}")

        row = [bid, bid, f'diff_{bid}']
        args = [bid, 'box.space.customer:replace', [row]]

        lua_result = lua_conn.call('vshard.router.callrw', args)
        cpp_result = cpp_conn.call('vshard.router.callrw', args)

        assert cpp_result.data == lua_result.data, \
            f"bid={bid}: cpp={cpp_result.data} != lua={lua_result.data}"

    def test_insert_with_return(self, lua_conn, cpp_conn):
        """Insert returns the inserted tuple identically."""
        bid = 5
        row = [77777, bid, 'diff_insert']
        args = [bid, 'box.space.customer:insert', [row]]

        # Clean up first.
        for conn in [lua_conn, cpp_conn]:
            try:
                conn.call('vshard.router.callrw',
                          [bid, 'box.space.customer:delete', [[77777]]])
            except Exception:
                pass

        lua_result = lua_conn.call('vshard.router.callrw', args)
        # Delete so C++ can insert fresh.
        lua_conn.call('vshard.router.callrw',
                      [bid, 'box.space.customer:delete', [[77777]]])
        cpp_result = cpp_conn.call('vshard.router.callrw', args)

        assert cpp_result.data == lua_result.data


class TestDifferentialCallRO:
    """Differential callro tests — read path."""

    @pytest.mark.parametrize('bid', [1, 50, 100, 200])
    def test_select_differential(self, lua_conn, cpp_conn, bid, bucket_count):
        """Select returns identical results from both routers."""
        if bid > bucket_count:
            pytest.skip(f"bucket {bid} > bucket_count {bucket_count}")

        # Seed data.
        row = [bid, bid, f'diff_ro_{bid}']
        lua_conn.call('vshard.router.callrw',
                      [bid, 'box.space.customer:replace', [row]])

        args = [bid, 'box.space.customer:select', [[bid]]]

        lua_result = lua_conn.call('vshard.router.callro', args)
        cpp_result = cpp_conn.call('vshard.router.callro', args)

        assert cpp_result.data == lua_result.data, \
            f"bid={bid}: cpp={cpp_result.data} != lua={lua_result.data}"


class TestDifferentialGenericCall:
    """Differential tests for vshard.router.call (generic 4-tuple)."""

    def test_generic_read(self, lua_conn, cpp_conn):
        """Generic call with mode='read' matches."""
        bid = 15
        row = [bid, bid, 'diff_generic']
        lua_conn.call('vshard.router.callrw',
                      [bid, 'box.space.customer:replace', [row]])

        args = [bid, 'read', 'box.space.customer:select', [[bid]]]
        lua_result = lua_conn.call('vshard.router.call', args)
        cpp_result = cpp_conn.call('vshard.router.call', args)

        assert cpp_result.data == lua_result.data

    def test_generic_write(self, lua_conn, cpp_conn):
        """Generic call with mode='write' matches."""
        bid = 25
        row = [bid, bid, 'diff_generic_w']
        args = [bid, 'write', 'box.space.customer:replace', [row]]

        lua_result = lua_conn.call('vshard.router.call', args)
        cpp_result = cpp_conn.call('vshard.router.call', args)

        assert cpp_result.data == lua_result.data


class TestDifferentialEcho:
    """Differential echo tests — verify argument passing fidelity."""

    @pytest.mark.parametrize('args,desc', [
        (['hello'], 'string'),
        ([42], 'integer'),
        ([3.14], 'float'),
        ([True], 'boolean'),
        ([[1, 2, 3]], 'array'),
        ([{'key': 'value'}], 'map'),
        (['', 0, False, []], 'mixed_empty'),
    ])
    def test_echo_types(self, lua_conn, cpp_conn, args, desc):
        """Echo different data types and compare."""
        bid = 35
        lua_result = lua_conn.call('vshard.router.callrw',
                                   [bid, 'echo', args])
        cpp_result = cpp_conn.call('vshard.router.callrw',
                                   [bid, 'echo', args])

        assert cpp_result.data == lua_result.data, \
            f"type={desc}: cpp={cpp_result.data} != lua={lua_result.data}"


class TestDifferentialCallVariants:
    """Differential tests for callbro, callbre, callre."""

    def _seed_and_read(self, lua_conn, cpp_conn, bid, func_name):
        """Seed data, then read via the specified function."""
        row = [bid, bid, f'diff_{func_name}']
        lua_conn.call('vshard.router.callrw',
                      [bid, 'box.space.customer:replace', [row]])

        args = [bid, 'box.space.customer:select', [[bid]]]
        lua_result = lua_conn.call(func_name, args)
        cpp_result = cpp_conn.call(func_name, args)
        return lua_result, cpp_result

    def test_callbro_differential(self, lua_conn, cpp_conn):
        """callbro results match."""
        lua_r, cpp_r = self._seed_and_read(
            lua_conn, cpp_conn, 45, 'vshard.router.callbro')
        assert cpp_r.data == lua_r.data

    @pytest.mark.skip(reason="callbre needs replica nodes; masters-only cluster")
    def test_callbre_differential(self, lua_conn, cpp_conn):
        """callbre results match."""
        lua_r, cpp_r = self._seed_and_read(
            lua_conn, cpp_conn, 55, 'vshard.router.callbre')
        assert cpp_r.data == lua_r.data

    def test_callre_differential(self, lua_conn, cpp_conn):
        """callre results match."""
        lua_r, cpp_r = self._seed_and_read(
            lua_conn, cpp_conn, 65, 'vshard.router.callre')
        assert cpp_r.data == lua_r.data
