-- vshard Lua router for lock-step differential testing.
-- Started alongside the C++ proxy so tests can compare results.
--
-- Env vars:
--   TARANTOOL_PORT         -- listen port for the Lua router
--   TARANTOOL_RS1_MASTER_PORT, TARANTOOL_RS1_REPLICA_PORT
--   TARANTOOL_RS2_MASTER_PORT, TARANTOOL_RS2_REPLICA_PORT
--   TARANTOOL_BUCKET_COUNT -- total bucket count (default 300)
--   TARANTOOL_TMPDIR       -- working directory

local port = tonumber(os.getenv('TARANTOOL_PORT')) or 3305
local tmpdir = os.getenv('TARANTOOL_TMPDIR') or '/tmp/vshard_test'
local bucket_count = tonumber(os.getenv('TARANTOOL_BUCKET_COUNT')) or 300

local rs1_master_port = tonumber(os.getenv('TARANTOOL_RS1_MASTER_PORT')) or 3301
local rs1_replica_port = tonumber(os.getenv('TARANTOOL_RS1_REPLICA_PORT')) or 3302
local rs2_master_port = tonumber(os.getenv('TARANTOOL_RS2_MASTER_PORT')) or 3303
local rs2_replica_port = tonumber(os.getenv('TARANTOOL_RS2_REPLICA_PORT')) or 3304

-- Same UUIDs as storage_init.lua.
local RS1_UUID = 'cbf06940-0790-498b-948d-042b62cf3d29'
local RS2_UUID = 'ac522f65-a15e-4b1b-af2b-3a0a67d36fef'

local INSTANCE_UUIDS = {
    ['rs1_master']  = '8a274925-a26d-47fc-9e1b-af88ce939412',
    ['rs1_replica'] = 'a3ef657e-eb4a-4f47-8a38-1a0e04517b15',
    ['rs2_master']  = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    ['rs2_replica'] = 'd5b83e4c-93af-476e-bb2b-c0a56c5e19f8',
}

box.cfg{
    listen = port,
    log = tmpdir .. '/router_' .. port .. '.log',
    pid_file = tmpdir .. '/router_' .. port .. '.pid',
    background = (os.getenv('TARANTOOL_BACKGROUND') == '1'),
    memtx_memory = 32 * 1024 * 1024,
    wal_mode = 'none',
}

box.once('init_guest_access_router', function()
    box.schema.user.grant('guest', 'read,write,execute,create,drop',
                          'universe', nil, {if_not_exists = true})
end)

local vshard = require('vshard')

local cfg = {
    sharding = {
        [RS1_UUID] = {
            replicas = {
                [INSTANCE_UUIDS['rs1_master']] = {
                    uri = 'storage:storage@127.0.0.1:' .. rs1_master_port,
                    name = 'rs1_master',
                    master = true,
                },
                [INSTANCE_UUIDS['rs1_replica']] = {
                    uri = 'storage:storage@127.0.0.1:' .. rs1_replica_port,
                    name = 'rs1_replica',
                    master = false,
                },
            },
        },
        [RS2_UUID] = {
            replicas = {
                [INSTANCE_UUIDS['rs2_master']] = {
                    uri = 'storage:storage@127.0.0.1:' .. rs2_master_port,
                    name = 'rs2_master',
                    master = true,
                },
                [INSTANCE_UUIDS['rs2_replica']] = {
                    uri = 'storage:storage@127.0.0.1:' .. rs2_replica_port,
                    name = 'rs2_replica',
                    master = false,
                },
            },
        },
    },
    bucket_count = bucket_count,
}

vshard.router.cfg(cfg)
