-- vshard storage initialization script for functional tests.
-- Each storage instance receives its role (RS UUID, master flag) via env vars:
--   TARANTOOL_PORT         -- listen port
--   TARANTOOL_RS_UUID      -- replicaset UUID for this instance
--   TARANTOOL_INSTANCE_UUID -- instance UUID
--   TARANTOOL_IS_MASTER    -- "1" if master, "0" if replica
--   TARANTOOL_REPLICATION_SOURCE -- URI of master to replicate from (replica only)
--   TARANTOOL_RS1_MASTER_PORT, TARANTOOL_RS1_REPLICA_PORT
--   TARANTOOL_RS2_MASTER_PORT, TARANTOOL_RS2_REPLICA_PORT
--   TARANTOOL_BUCKET_COUNT -- total bucket count (default 300)
--   TARANTOOL_TMPDIR       -- working directory
--   TARANTOOL_BACKGROUND   -- "1" to daemonize
--   TARANTOOL_VSHARD_PATH  -- path to vshard Lua module (optional)

-- Allow specifying vshard module location via env var.
local vshard_path = os.getenv('TARANTOOL_VSHARD_PATH')
if vshard_path then
    package.path = vshard_path .. '/?.lua;' ..
                   vshard_path .. '/?/init.lua;' .. package.path
end

local port = tonumber(os.getenv('TARANTOOL_PORT')) or 3301
local tmpdir = os.getenv('TARANTOOL_TMPDIR') or '/tmp/vshard_test'
local background = os.getenv('TARANTOOL_BACKGROUND') == '1'
local bucket_count = tonumber(os.getenv('TARANTOOL_BUCKET_COUNT')) or 300
local instance_uuid = os.getenv('TARANTOOL_INSTANCE_UUID')
local rs_uuid = os.getenv('TARANTOOL_RS_UUID')
local is_master = os.getenv('TARANTOOL_IS_MASTER') == '1'
local replication_source = os.getenv('TARANTOOL_REPLICATION_SOURCE')

local rs1_master_port = tonumber(os.getenv('TARANTOOL_RS1_MASTER_PORT')) or 3301
local rs1_replica_port = tonumber(os.getenv('TARANTOOL_RS1_REPLICA_PORT')) or 3302
local rs2_master_port = tonumber(os.getenv('TARANTOOL_RS2_MASTER_PORT')) or 3303
local rs2_replica_port = tonumber(os.getenv('TARANTOOL_RS2_REPLICA_PORT')) or 3304

-- Fixed UUIDs for reproducible tests.
local RS1_UUID = 'cbf06940-0790-498b-948d-042b62cf3d29'
local RS2_UUID = 'ac522f65-a15e-4b1b-af2b-3a0a67d36fef'

local INSTANCE_UUIDS = {
    ['rs1_master']  = '8a274925-a26d-47fc-9e1b-af88ce939412',
    ['rs1_replica'] = 'a3ef657e-eb4a-4f47-8a38-1a0e04517b15',
    ['rs2_master']  = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    ['rs2_replica'] = 'd5b83e4c-93af-476e-bb2b-c0a56c5e19f8',
}

-- Build vshard config table used by both router and storage.
-- If replica port equals master port, we use single-node replicasets.
local function make_vshard_cfg()
    local rs1_replicas = {
        [INSTANCE_UUIDS['rs1_master']] = {
            uri = 'storage:storage@127.0.0.1:' .. rs1_master_port,
            name = 'rs1_master',
            master = true,
        },
    }
    if rs1_replica_port ~= rs1_master_port then
        rs1_replicas[INSTANCE_UUIDS['rs1_replica']] = {
            uri = 'storage:storage@127.0.0.1:' .. rs1_replica_port,
            name = 'rs1_replica',
            master = false,
        }
    end

    local rs2_replicas = {
        [INSTANCE_UUIDS['rs2_master']] = {
            uri = 'storage:storage@127.0.0.1:' .. rs2_master_port,
            name = 'rs2_master',
            master = true,
        },
    }
    if rs2_replica_port ~= rs2_master_port then
        rs2_replicas[INSTANCE_UUIDS['rs2_replica']] = {
            uri = 'storage:storage@127.0.0.1:' .. rs2_replica_port,
            name = 'rs2_replica',
            master = false,
        }
    end

    return {
        sharding = {
            [RS1_UUID] = { replicas = rs1_replicas },
            [RS2_UUID] = { replicas = rs2_replicas },
        },
        bucket_count = bucket_count,
    }
end

local box_cfg = {
    listen = port,
    replication_connect_quorum = 0,
    instance_uuid = instance_uuid,
    replicaset_uuid = rs_uuid,
    log = tmpdir .. '/tarantool_' .. port .. '.log',
    pid_file = tmpdir .. '/tarantool_' .. port .. '.pid',
    memtx_dir = tmpdir .. '/snap_' .. port,
    wal_dir = tmpdir .. '/xlog_' .. port,
    background = background,
    memtx_memory = 64 * 1024 * 1024,
}

-- Replicas need replication source and read_only=true.
if replication_source then
    box_cfg.replication = {replication_source}
    box_cfg.read_only = not is_master
end

box.cfg(box_cfg)

-- Create storage user.
box.once('init_storage_user', function()
    box.schema.user.create('storage', {password = 'storage', if_not_exists = true})
    box.schema.user.grant('storage', 'super', nil, nil, {if_not_exists = true})
end)

-- Grant guest access for testing convenience.
box.once('init_guest_access', function()
    box.schema.user.grant('guest', 'read,write,execute,create,drop',
                          'universe', nil, {if_not_exists = true})
end)

-- Application space: customer(id, bucket_id, name).
box.once('create_customer_space', function()
    local s = box.schema.space.create('customer', {
        format = {
            {name = 'id',        type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name',      type = 'string'},
        },
        if_not_exists = true,
    })
    s:create_index('primary', {
        parts = {'id'},
        if_not_exists = true,
    })
    s:create_index('bucket_id', {
        parts = {'bucket_id'},
        unique = false,
        if_not_exists = true,
    })
end)

-- Helper function for tests: echo back arguments.
rawset(_G, 'echo', function(...)
    return ...
end)

-- Helper: return current instance info.
rawset(_G, 'get_instance_info', function()
    return {
        uuid = box.info.uuid,
        rs_uuid = box.info.cluster.uuid,
        is_master = (box.info.ro == false),
        port = port,
    }
end)

-- Configure vshard storage.
local vshard = require('vshard')
vshard.storage.cfg(make_vshard_cfg(), instance_uuid)

-- Expose vshard globally so IPROTO CALL can resolve 'vshard.storage.*' functions.
rawset(_G, 'vshard', vshard)

-- Export config builder for router init.
rawset(_G, 'make_vshard_cfg', make_vshard_cfg)
