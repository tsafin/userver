-- Tarantool initialization script for userver functional tests.
-- Creates a simple key-value space and starts listening.

local port = tonumber(os.getenv('TARANTOOL_PORT')) or 3301

box.cfg{
    listen = port,
    log = os.getenv('TARANTOOL_LOG') or 'tarantool.log',
    pid_file = os.getenv('TARANTOOL_PID') or 'tarantool.pid',
    background = (os.getenv('TARANTOOL_BACKGROUND') == '1'),
    memtx_memory = 64 * 1024 * 1024,
    wal_mode = 'none',
}

-- Allow guest full access for tests.
box.once('init_access', function()
    box.schema.user.grant('guest', 'read,write,execute,create,drop',
                          'universe', nil, {if_not_exists = true})
end)

-- Key-value space: id (unsigned, PK), value (string).
box.once('create_kv', function()
    local kv = box.schema.space.create('kv', {
        format = {
            {name = 'id',    type = 'unsigned'},
            {name = 'value', type = 'string'},
        },
        if_not_exists = true,
    })
    kv:create_index('primary', {
        type = 'hash',
        parts = {'id'},
        if_not_exists = true,
    })
end)
