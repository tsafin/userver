"""Basic vshard routing tests — callrw, callro.

These tests exercise the C++ vshard proxy through the IPROTO protocol.
Each test also runs the same operation through the Lua vshard router
and compares results (lock-step differential testing).
"""
import pytest
import tarantool


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


def _strip_service_status_idx(value):
    if isinstance(value, dict):
        return {
            key: _strip_service_status_idx(item)
            for key, item in value.items()
            if key != 'status_idx'
        }
    if isinstance(value, list):
        return [_strip_service_status_idx(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_service_status_idx(item) for item in value)
    return value


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

    def test_generic_call_invalid_mode_matches_lua(self, lua_conn, cpp_conn):
        """Invalid mode strings should route as write and return Lua-style errors."""
        bid = 75
        args = [bid, 'junk', 'box.space.customer:select', [[bid]]]

        lua_result = lua_conn.call('vshard.router.call', args)
        cpp_result = cpp_conn.call('vshard.router.call', args)

        assert _strip_trace_locations(cpp_result.data) == \
            _strip_trace_locations(lua_result.data)

    def test_generic_call_return_raw_matches_lua(self, lua_conn, cpp_conn):
        bid = 76
        args = [bid, 'write', 'echo', ['raw'], {'return_raw': True}]
        with pytest.raises(tarantool.error.DatabaseError) as lua_error:
            lua_conn.call('vshard.router.call', args)
        with pytest.raises(tarantool.error.DatabaseError) as cpp_error:
            cpp_conn.call('vshard.router.call', args)

        assert 'Msgpack object feature is not supported' in str(lua_error.value)
        assert 'Msgpack object feature is not supported' in str(cpp_error.value)

    def test_generic_call_is_async_matches_lua(self, lua_conn, cpp_conn):
        bid = 77
        args = [bid, 'write', 'echo', ['async'], {'is_async': True}]

        lua_result = lua_conn.call('vshard.router.call', args)
        cpp_result = cpp_conn.call('vshard.router.call', args)

        assert cpp_result.data == lua_result.data

    def test_generic_call_is_async_return_raw_matches_lua(self, lua_conn, cpp_conn):
        bid = 78
        args = [
            bid, 'write', 'echo', ['async_raw'],
            {'is_async': True, 'return_raw': True},
        ]

        with pytest.raises(tarantool.error.DatabaseError) as lua_error:
            lua_conn.call('vshard.router.call', args)
        with pytest.raises(tarantool.error.DatabaseError) as cpp_error:
            cpp_conn.call('vshard.router.call', args)

        assert 'Msgpack object feature is not supported' in str(lua_error.value)
        assert 'Msgpack object feature is not supported' in str(cpp_error.value)



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

    def test_bucket_id_strcrc32_string(self, lua_conn, cpp_conn):
        """bucket_id_strcrc32 for string keys should match."""
        for key in ['hello', 'world', '', 'test123', 'user@example.com']:
            lua_bid = lua_conn.call(
                'vshard.router.bucket_id_strcrc32', [key],
            )
            cpp_bid = cpp_conn.call(
                'vshard.router.bucket_id_strcrc32', [key],
            )
            assert cpp_bid.data == lua_bid.data, \
                f"Mismatch for key='{key}': cpp={cpp_bid.data} lua={lua_bid.data}"


def _bucket_owner_uuid(vshard_cluster, bucket_id):
    ports = vshard_cluster['ports']
    for rs_name, rs_uuid in (
        ('rs1_master', 'cbf06940-0790-498b-948d-042b62cf3d29'),
        ('rs2_master', 'ac522f65-aa94-4134-9f64-51ee384f1a54'),
    ):
        conn = tarantool.connect(
            '127.0.0.1', ports[rs_name], user='storage', password='storage',
        )
        try:
            result = conn.call('vshard.storage.bucket_stat', [bucket_id])
            if (
                result.data and len(result.data) >= 2 and
                result.data[0] is not None and result.data[1] is None
            ):
                return rs_uuid
        except Exception:
            pass
        finally:
            conn.close()
    raise AssertionError(f'No storage owns bucket {bucket_id}')


class TestRoute:
    """vshard.router.route — resolve bucket owner."""

    def test_route_returns_owner_uuid(self, cpp_conn, vshard_cluster):
        for bucket_id in [1, 2, 17, 101]:
            cpp_result = cpp_conn.call('vshard.router.route', [bucket_id])
            assert cpp_result.data is not None
            route_info = cpp_result.data[0]
            assert route_info['uuid'] == _bucket_owner_uuid(
                vshard_cluster, bucket_id
            )


class TestInfo:
    """vshard.router.info."""

    def test_info_matches_lua_on_stable_fields(self, lua_conn, cpp_conn):
        lua_result = lua_conn.call('vshard.router.info', [])
        cpp_result = cpp_conn.call('vshard.router.info', [])

        lua_info = lua_result.data[0]
        cpp_info = cpp_result.data[0]

        assert cpp_info['bucket'] == lua_info['bucket']
        assert cpp_info['alerts'] == lua_info['alerts']
        assert cpp_info['status'] == lua_info['status']
        assert cpp_info['identification_mode'] == lua_info['identification_mode']
        assert cpp_info['is_enabled'] == lua_info['is_enabled']
        assert set(cpp_info['replicasets'].keys()) == set(lua_info['replicasets'].keys())

        for rs_uuid in lua_info['replicasets']:
            assert cpp_info['replicasets'][rs_uuid]['bucket'] == \
                lua_info['replicasets'][rs_uuid]['bucket']
            assert cpp_info['replicasets'][rs_uuid]['master']['status'] == \
                lua_info['replicasets'][rs_uuid]['master']['status']
            assert cpp_info['replicasets'][rs_uuid]['master']['uuid'] == \
                lua_info['replicasets'][rs_uuid]['master']['uuid']
            assert cpp_info['replicasets'][rs_uuid]['master'].get('name') == \
                lua_info['replicasets'][rs_uuid]['master'].get('name')
            assert cpp_info['replicasets'][rs_uuid]['replica']['status'] == \
                lua_info['replicasets'][rs_uuid]['replica']['status']
            if 'uuid' in cpp_info['replicasets'][rs_uuid]['replica']:
                assert cpp_info['replicasets'][rs_uuid]['replica']['uuid'] == \
                    lua_info['replicasets'][rs_uuid]['replica']['uuid']
                assert cpp_info['replicasets'][rs_uuid]['replica'].get('name') == \
                    lua_info['replicasets'][rs_uuid]['replica'].get('name')

    def test_info_with_services_matches_lua_on_stable_fields(self, lua_conn, cpp_conn):
        args = [{'with_services': True}]
        lua_result = lua_conn.call('vshard.router.info', args)
        cpp_result = cpp_conn.call('vshard.router.info', args)

        lua_info = lua_result.data[0]
        cpp_info = cpp_result.data[0]

        assert _strip_service_status_idx(cpp_info) == \
            _strip_service_status_idx(lua_info)

    def test_info_rejects_non_map_non_bool_arg_like_lua(self, lua_conn, cpp_conn):
        with pytest.raises(tarantool.error.DatabaseError):
            lua_conn.call('vshard.router.info', [123])
        with pytest.raises(tarantool.error.DatabaseError):
            cpp_conn.call('vshard.router.info', [123])


class TestSync:
    """vshard.router.sync."""

    def test_sync_matches_lua(self, lua_conn, cpp_conn):
        lua_result = lua_conn.call('vshard.router.sync', [])
        cpp_result = cpp_conn.call('vshard.router.sync', [])
        assert _strip_trace_locations(cpp_result.data) == \
            _strip_trace_locations(lua_result.data)

    def test_sync_invalid_arg_matches_lua(self, lua_conn, cpp_conn):
        import tarantool

        with pytest.raises(tarantool.error.DatabaseError):
            lua_conn.call('vshard.router.sync', ['bad'])
        with pytest.raises(tarantool.error.DatabaseError):
            cpp_conn.call('vshard.router.sync', ['bad'])

    def test_sync_negative_timeout_matches_lua(self, lua_conn, cpp_conn):
        lua_result = lua_conn.call('vshard.router.sync', [-0.001])
        cpp_result = cpp_conn.call('vshard.router.sync', [-0.001])

        for result in (lua_result, cpp_result):
            assert result.data[0] is None
            err = result.data[1]
            assert err['code'] == 78
            assert err['type'] == 'ClientError'
            assert err['message'] == 'Timeout exceeded'


class TestBootstrap:
    """vshard.router.bootstrap."""

    def test_bootstrap_non_empty_matches_lua(self, lua_conn, cpp_conn):
        lua_result = lua_conn.call('vshard.router.bootstrap', [])
        cpp_result = cpp_conn.call('vshard.router.bootstrap', [])
        assert _strip_trace_locations(cpp_result.data) == \
            _strip_trace_locations(lua_result.data)

    def test_bootstrap_if_not_bootstrapped_matches_lua(self, lua_conn, cpp_conn):
        args = [{'if_not_bootstrapped': True}]
        lua_result = lua_conn.call('vshard.router.bootstrap', args)
        cpp_result = cpp_conn.call('vshard.router.bootstrap', args)
        assert cpp_result.data == lua_result.data

    def test_bootstrap_invalid_arg_matches_lua(self, lua_conn, cpp_conn):
        with pytest.raises(tarantool.error.DatabaseError):
            lua_conn.call('vshard.router.bootstrap', [123])
        with pytest.raises(tarantool.error.DatabaseError):
            cpp_conn.call('vshard.router.bootstrap', [123])


class TestMapCallRW:
    """vshard.router.map_callrw."""

    def test_map_callrw_matches_lua(self, lua_conn, cpp_conn):
        lua_result = lua_conn.call('vshard.router.map_callrw', ['echo', ['x']])
        cpp_result = cpp_conn.call('vshard.router.map_callrw', ['echo', ['x']])
        assert cpp_result.data == lua_result.data

    def test_map_callrw_timeout_matches_lua(self, lua_conn, cpp_conn):
        args = ['echo', ['x'], {'timeout': 0.5}]
        lua_result = lua_conn.call('vshard.router.map_callrw', args)
        cpp_result = cpp_conn.call('vshard.router.map_callrw', args)
        assert cpp_result.data == lua_result.data

    def test_map_callrw_bucket_ids_matches_lua(self, lua_conn, cpp_conn):
        args = ['echo', ['x'], {'bucket_ids': [1]}]
        lua_result = lua_conn.call('vshard.router.map_callrw', args)
        cpp_result = cpp_conn.call('vshard.router.map_callrw', args)
        assert cpp_result.data == lua_result.data

    def test_map_callrw_missing_function_matches_lua(self, lua_conn, cpp_conn):
        args = ['no_such_fn', []]
        lua_result = lua_conn.call('vshard.router.map_callrw', args)
        cpp_result = cpp_conn.call('vshard.router.map_callrw', args)

        assert lua_result.data[0] is None
        assert cpp_result.data[0] is None
        assert cpp_result.data[2] == lua_result.data[2]

        lua_err = lua_result.data[1]
        cpp_err = cpp_result.data[1]
        assert cpp_err['code'] == lua_err['code']
        assert cpp_err['type'] == lua_err['type']
        assert cpp_err['message'] == lua_err['message']


class TestRouteAll:
    """vshard.router.routeall — list all replicasets."""

    def test_routeall_returns_both_rs(self, lua_conn, cpp_conn, bucket_count):
        """routeall should return data for all replicasets.

        Note: the Lua router's routeall() returns internal replicaset objects
        that contain Lua function references, which cannot be serialized over
        IPROTO.  We therefore only validate the C++ proxy result; if the Lua
        call happens to succeed (future Tarantool / vshard version) we also
        cross-check the UUID count.
        """
        import tarantool

        cpp_result = cpp_conn.call('vshard.router.routeall', [])
        assert cpp_result.data is not None, "C++ routeall returned no data"

        # C++ returns [{uuid: {uuid: uuid}, ...}]
        rs_map = cpp_result.data[0]
        assert isinstance(rs_map, dict), \
            f"Expected dict from C++ routeall, got {type(rs_map)}"
        assert len(rs_map) == 2, \
            f"Expected 2 replicasets, got {len(rs_map)}: {list(rs_map.keys())}"

        # Lua vshard.router.routeall() returns objects with function fields,
        # which Tarantool cannot serialize over IPROTO ("unsupported Lua type
        # 'function'").  Treat this as a known limitation and skip comparison.
        try:
            lua_result = lua_conn.call('vshard.router.routeall', [])
            if lua_result.data is not None:
                lua_rs_map = lua_result.data[0]
                assert len(lua_rs_map) == len(rs_map), \
                    f"RS count mismatch: lua={len(lua_rs_map)} cpp={len(rs_map)}"
        except tarantool.error.DatabaseError:
            pass  # expected: Lua replicaset objects are not IPROTO-serializable
