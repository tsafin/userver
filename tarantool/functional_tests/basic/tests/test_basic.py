"""Basic Tarantool connector functional tests: CRUD round-trip."""
import pytest


async def test_insert_and_select(service_client, tarantool_store):
    resp = await service_client.post('/kv?id=1&value=hello')
    assert resp.status == 200
    assert resp.text == 'ok'

    resp = await service_client.get('/kv?id=1')
    assert resp.status == 200
    data = resp.json()
    assert data['id'] == 1
    assert data['value'] == 'hello'


async def test_replace(service_client, tarantool_store):
    resp = await service_client.post('/kv?id=2&value=original')
    assert resp.status == 200

    resp = await service_client.put('/kv?id=2&value=replaced')
    assert resp.status == 200

    resp = await service_client.get('/kv?id=2')
    assert resp.status == 200
    assert resp.json()['value'] == 'replaced'


async def test_delete(service_client, tarantool_store):
    resp = await service_client.post('/kv?id=3&value=to_delete')
    assert resp.status == 200

    resp = await service_client.delete('/kv?id=3')
    assert resp.status == 200

    resp = await service_client.get('/kv?id=3')
    assert resp.status == 404


async def test_select_missing(service_client, tarantool_store):
    resp = await service_client.get('/kv?id=9999')
    assert resp.status == 404


async def test_missing_id_arg(service_client):
    resp = await service_client.get('/kv')
    assert resp.status == 400


async def test_multiple_entries(service_client, tarantool_store):
    for i in range(10, 15):
        resp = await service_client.post(f'/kv?id={i}&value=val{i}')
        assert resp.status == 200

    for i in range(10, 15):
        resp = await service_client.get(f'/kv?id={i}')
        assert resp.status == 200
        assert resp.json()['value'] == f'val{i}'


async def test_direct_store_seed(service_client, tarantool_store):
    """Test that tarantool_store fixture can seed data directly."""
    tarantool_store.insert('kv', (100, 'seeded'))

    resp = await service_client.get('/kv?id=100')
    assert resp.status == 200
    assert resp.json()['value'] == 'seeded'
