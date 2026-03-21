# VShard Router IPROTO Commands Analysis

## Overview

The vshard router acts as both an IPROTO **server** (receiving requests from clients) and an IPROTO **client** (sending requests to storage nodes). It uses Tarantool's `net.box` library for all remote communication.

---

## PART 1: ROUTER AS IPROTO SERVER (Receiving Client Requests)

### 1.1 Router API Methods (Public Functions)

The vshard router exposes these public methods that clients can call via IPROTO:

#### Routing & Calling Functions
- **`router.call(bucket_id, mode, func, args, opts)`** - Generic call through routing layer
- **`router.callro(bucket_id, func, args, opts)`** - Call for read-only access (replica selection)
- **`router.callbro(bucket_id, func, args, opts)`** - Call for read-only with load balancing
- **`router.callrw(bucket_id, func, args, opts)`** - Call for read-write access (master only)
- **`router.callre(bucket_id, func, args, opts)`** - Call for read access preferring replicas
- **`router.callbre(bucket_id, func, args, opts)`** - Call for read access with replica preference + balancing
- **`router.map_callrw(func, args, opts)`** - Map-reduce: call on all buckets in a map

#### Bucket Management
- **`router.route(bucket_id)`** - Get replicaset for a bucket
- **`router.routeall()`** - Get all replicasets
- **`router.bucket_id(key, space)`** - Calculate bucket_id from key
- **`router.bucket_id_strcrc32(key)`** - Calculate bucket_id using string CRC32
- **`router.bucket_id_mpcrc32(key)`** - Calculate bucket_id using msgpack CRC32
- **`router.bucket_count()`** - Get total bucket count
- **`router.buckets_info()`** - Get info on all buckets
- **`router.sync([timeout])`** - Wait for sync across replicasets

#### Discovery & Configuration
- **`router.cfg(cfg)`** - Configure router
- **`router.bootstrap()`** - Bootstrap cluster
- **`router.discovery_wakeup()`** - Wake up discovery fiber
- **`router.discovery_set(mode)`** - Set discovery mode
- **`router.master_search_wakeup()`** - Wake up master search
- **`router.info()`** - Get router info
- **`router.enable()`** - Enable router
- **`router.disable([reason])`** - Disable router

### 1.2 Request Protocol (Router Receives)

Clients connect to the router via net.box and call router functions:

```lua
-- Client code (via net.box)
local conn = net.box.connect('router_uri')

-- Makes IPROTO_CALL request with:
-- - func_name = 'vshard.router.callrw' (or other method)
-- - args = {bucket_id, user_func, {user_args}, {opts}}
-- - timeout = opts.timeout or default
result, err = conn:call('vshard.router.callrw', {1001, 'myfunc', {'arg1'}, {}})
```

**IPROTO Request Type:** `IPROTO_CALL` (type 0x0B)

**Frame Header (IPROTO):**
```
[0x00 0x00 0x00 XX] - Body length (little-endian)
[0xCE 0x00 0x00 XX] - Type=0xCE (request), Code=0x0B (CALL)
```

**Request Body (MessagePack):**
```
{
  0xCE,              // IPROTO_REQUEST (type marker)
  0x0B,              // IPROTO_CALL
  sync_id,           // Request sync ID
  timeout,           // Request timeout (if set by client)
  function_name,     // "vshard.router.callrw" or similar
  args               // [bucket_id, func_name, func_args, opts]
}
```

**Example Function Call Flow:**
```
Client calls: conn:call('vshard.router.callrw', {1001, 'insert', {doc}, {}})
  │
  ├─ IPROTO_CALL packet with func="vshard.router.callrw"
  │
  ├─ Router receives via net.box listener
  │
  ├─ Router unpacks args: bucket_id=1001, func='insert', args={doc}, opts={}
  │
  ├─ Router calls internal: router_callrw(router, 1001, 'insert', {doc}, {})
  │
  ├─ Router resolves bucket 1001 → replicaset via route_map
  │
  └─ Returns result via IPROTO_RESPONSE
```

### 1.3 Router Request Processing

When router receives a call:

1. **Router Wrapper (`router_make_api`)** - Validates router state, checks if disabled
2. **Route Resolution** - Finds replicaset for bucket_id from `router.route_map`
3. **Replicaset Selection** - For read calls, picks replica; for write calls, uses master
4. **Remote Call via replicaset object** - Calls replicaset:callrw/callro/etc.

---

## PART 2: ROUTER AS IPROTO CLIENT (Sending Requests to Storage)

### 2.1 Router → Storage Communication

The router sends calls to storage nodes via `net.box` connections. All communication uses:

**IPROTO Request Type:** `IPROTO_CALL` (type 0x0B)

### 2.2 Storage Functions Router Calls

#### User-Facing Call
```lua
replicaset:callrw('vshard.storage.call', {bucket_id, mode, func_name, args}, opts)
```

**Storage Function:** `vshard.storage.call(bucket_id, mode, name, args)`

**Args:**
- `bucket_id`: Numeric bucket ID (1 to bucket_count)
- `mode`: 'read' or 'write' 
- `name`: User function name (e.g., 'myfunc')
- `args`: User function arguments (array)

**Returns:**
- `ok` (boolean): true if user function succeeded
- `ret1, ret2, ret3`: User function return values (max 3 values to avoid nil truncation)
- If error: `nil, error_object`

#### Internal Service Calls
```lua
replicaset:callrw('vshard.storage._call', {service_name, ...}, opts)
```

**Storage Function:** `vshard.storage._call(service_name, ...)`

**Service names (internal):**
- `'bucket_recv'` - Receive bucket data during rebalancing
- `'bucket_send'` - Send bucket data to another replicaset
- `'bucket_stat'` - Get bucket status
- `'buckets_discovery'` - Discover bucket distribution
- `'recovery_bucket_stat'` - Recovery status of bucket
- `'rebalancer_apply_routes'` - Apply bucket routes
- `'rebalancer_request_state'` - Request rebalancer state
- `'storage_ref'` - Create storage reference
- `'storage_unref'` - Drop storage reference
- `'storage_ref_make_with_buckets'` - Create ref with bucket validation
- `'storage_ref_check_with_buckets'` - Check ref with buckets
- `'storage_map'` - Map-reduce operation
- `'info'` - Get storage info (for vconnect handshake)

### 2.3 Key Storage Functions Called by Router

#### 2.3.1 `vshard.storage.bucket_recv(bucket_id, from, data, opts)`

**Called by:** Router during rebalancing (via `bucket_send` on source storage)

**Purpose:** Receive bucket data during rebalancing

**Args:**
```lua
{
  bucket_id = number,        -- Destination bucket ID
  from = string,             -- Source replicaset ID
  data = {                   -- Bucket data (chunked)
    {space_name, tuples},
    {space_name, tuples},
    ...
  },
  opts = {
    timeout = number,        -- Default: 10 seconds
    is_last = boolean        -- Last chunk marker
  }
}
```

**Flow:**
1. Check destination is writable (ACTIVE or create RECEIVING)
2. Insert tuples into spaces
3. If `is_last=true`, mark bucket ACTIVE

**Errors Returned:**
- `WRONG_BUCKET` - Bucket in wrong state
- `TOO_MANY_RECEIVING` - Too many concurrent receives
- `REPLICASET_IS_LOCKED` - Replicaset locked
- `BUCKET_RECV_DATA_ERROR` - Insert failed

#### 2.3.2 `vshard.storage.bucket_send(bucket_id, destination, opts)`

**Called by:** Rebalancer/manual operations

**Purpose:** Send bucket to another replicaset

**Args:**
```lua
{
  bucket_id = number,         -- Source bucket ID
  destination = string,       -- Target replicaset ID
  opts = {
    timeout = number          -- Default: 10 seconds
  }
}
```

**Flow:**
1. Fetch all tuples for bucket from all sharded spaces
2. Call `bucket_recv` on destination in chunks (1000 tuples per chunk)
3. Mark bucket SENDING, then SENT on source
4. Send final empty message with `is_last=true`

**Errors Returned:**
- `WRONG_BUCKET` - Bucket not in correct state
- `BUCKET_IS_PINNED` - Bucket is pinned
- `MOVE_TO_SELF` - Destination is same as source
- `NO_SUCH_REPLICASET` - Destination not found

#### 2.3.3 `vshard.storage.buckets_discovery(opts)`

**Called by:** Router discovery fiber

**Purpose:** Discover bucket distribution

**Args:**
```lua
{
  offset = number,           -- Pagination offset
  limit = number             -- Pagination limit (iterator)
}
```

**Returns:**
```lua
{
  buckets = {
    {id = bucket_id, status = 'active'|'sending'|'receiving'|...},
    ...
  },
  next_from = number         -- Next offset for pagination
}
```

#### 2.3.4 `vshard.storage.bucket_stat(bucket_id)`

**Called by:** Router during discovery/debugging

**Purpose:** Get detailed bucket status

**Returns:**
```lua
{
  id = bucket_id,
  status = 'active'|'sending'|'receiving'|...,
  is_transfering = true|false
}
```

#### 2.3.5 `vshard.storage._call('info', ...)`

**Called by:** Router during connection handshake (vconnect)

**Purpose:** Validate storage instance (name mismatch detection)

**Used for:** Named replica verification

---

## PART 3: Error Handling & Retry Logic

### 3.1 VShard Error Codes

The router handles these specific error codes returned from storage:

| Code | Name | Meaning | Retry? | Action |
|------|------|---------|--------|--------|
| 1 | `WRONG_BUCKET` | Bucket moved to another replicaset | YES | Reset cache, jump to new destination |
| 2 | `NON_MASTER` | Called write on replica | YES | Update master, retry on new master |
| 7 | `TRANSFER_IS_IN_PROGRESS` | Bucket is being transferred | YES | Reset cache, retry |
| 22 | `BUCKET_IS_LOCKED` | Bucket locked by GC or rebalancing | YES | Reset cache, retry |
| 32 | `REPLICASET_IN_BACKOFF` | Replicaset in backoff state | NO | Fail, no retry |
| 34 | `BUCKET_IS_CORRUPTED` | Bucket data corrupted | NO | Fail, critical error |

### 3.2 Retry Logic in `router_call_impl`

```lua
-- router/init.lua: router_call_impl()
repeat
  replicaset, err = bucket_resolve(router, bucket_id)
  if replicaset then
    -- Send call to storage
    storage_call_status, call_status, call_error = 
      replicaset[call](replicaset, 'vshard.storage.call', 
                       {bucket_id, mode, func, args}, opts)
    
    if storage_call_status then
      return call_status  -- Success
    end
    
    err = lerror.make(call_status)
    
    -- Handle retryable errors
    if err.code == WRONG_BUCKET or 
       err.code == BUCKET_IS_LOCKED or 
       err.code == TRANSFER_IS_IN_PROGRESS then
      
      bucket_reset(router, bucket_id)  -- Clear cache entry
      
      if err.destination then
        -- Jump to destination replicaset if provided
        replicaset = router.replicasets[err.destination]
        if replicaset then
          bucket_set(router, bucket_id, replicaset.id)
          goto replicaset_is_found
        end
      end
    elseif err.code == NON_MASTER then
      -- Update master and retry
      replicaset:update_master(err.replica, err.master)
    else
      return nil, err  -- Non-retryable error
    end
  end
  fiber.yield()
until fiber_clock() > tend  -- Timeout deadline
```

**Timeout Handling:**
- `opts.timeout` - Total time for retries (default: 0.5 seconds, max: 64 seconds)
- `opts.request_timeout` - Individual request timeout (must be ≤ timeout)
- If timeout exceeded, returns `TIMEOUT` error

### 3.3 Return Value Format

**Success (from `vshard.storage.call`):**
```lua
-- Returns: {true, user_func_result1, user_func_result2, user_func_result3}
return true, result1, result2, result3
```

**User Function Error (from `vshard.storage.call`):**
```lua
-- Returns: {false, error_object}
return false, error_object
```

**VShard Internal Error:**
```lua
-- Returns: {nil, vshard_error_object}
return nil, {
  type = 'ShardingError',
  code = 1,                    -- WRONG_BUCKET
  name = 'WRONG_BUCKET',
  message = 'Cannot perform action with bucket 1001, reason: ...',
  bucket_id = 1001,
  reason = '...',
  destination = 'rs-2'         -- Optional, where bucket moved
}
```

---

## PART 4: Connection Setup & Authentication

### 4.1 Net.Box Connection Creation

**Router to Storage Connection:**

```lua
-- replicaset.lua: replica_connect()
local conn = netbox.connect(replica.uri, {
  reconnect_after = 0.5,     -- Reconnect interval
  wait_connected = false,    -- Non-blocking connect
  fetch_schema = true        -- Fetch schema from storage
})

-- Set up callbacks
conn:on_connect(netbox_on_connect)
conn:on_disconnect(netbox_on_disconnect)
```

### 4.2 Authentication

**Default:** Anonymous connections (no authentication)

**Configuration:** Via box.cfg
```lua
box.cfg {
  listen = 'router:3301',
  -- Optional auth setup on storage
  -- User with execute privileges on vshard functions required
}
```

### 4.3 IPROTO Handshake (VConnect)

After connection established, router performs "vconnect" validation:

```lua
-- replicaset.lua: conn_vconnect_start()
local opts = {is_async = true}
vconn.future = conn:call('vshard.storage._call', {'info'}, opts)

-- Later: conn_vconnect_check()
-- Validates:
-- 1. Call completed successfully
-- 2. Response format is valid
-- 3. For named replicas: instance names match config
```

**Error:** `VHANDSHAKE_NOT_COMPLETE` if handshake fails

### 4.4 Connection Reuse

- Connections are **persistent** per (router → storage) pair
- Stored in `replica.conn` object
- Reconnected if dropped
- Not re-authenticated (relies on Tarantool session persistence)

---

## PART 5: Wire Format Examples

### 5.1 Router Calls Storage for User Function

**Router → Storage IPROTO_CALL:**

```
func: 'vshard.storage.call'
args: [
  1001,                      -- bucket_id
  'write',                   -- mode
  'insert_document',         -- user function name
  [{id=1, data='test'}]      -- user function args
]
timeout: 5.0                 -- seconds
```

**Storage Response (Success):**

```
[true,                       -- user function succeeded
 {                           -- user function return value
   id: 1,
   data: 'test'
 }
]
```

**Storage Response (User Error):**

```
[false,                      -- user function failed
 {                           -- user function error
   type: 'ClientError',
   message: 'Duplicate key in index'
 }
]
```

**Storage Response (Routing Error - Bucket Moved):**

```
[nil,                        -- routing error
 {                           -- vshard error
   type: 'ShardingError',
   code: 1,
   name: 'WRONG_BUCKET',
   bucket_id: 1001,
   destination: 'rs-2',      -- Bucket moved here
   reason: 'Bucket is on replicaset rs-2',
   message: 'Cannot perform action with bucket 1001, reason: Bucket is on replicaset rs-2'
 }
]
```

### 5.2 Router Calls Storage for Bucket Transfer

**Router → Storage for bucket_recv (chunked transfer):**

```
func: 'vshard.storage.bucket_recv'
args: [
  1001,                      -- bucket_id
  'rs-1',                    -- source replicaset
  [                          -- data (space_name, tuples)
    ['documents', [
      [1, 'doc1', 100],
      [2, 'doc2', 200],
      ...
    ]],
    ['indexes', [
      [1, 1, 'idx1', 'btree'],
      ...
    ]]
  ],
  {timeout: 10}              -- options
]
```

**Last Chunk:**

```
args: [
  1001,
  'rs-1',
  [],                        -- empty data (no more chunks)
  {
    timeout: 10,
    is_last: true           -- Final chunk marker
  }
]
```

### 5.3 Router Discovery Request

**Router → Storage for bucket discovery:**

```
func: 'vshard.storage.buckets_discovery'
args: [{
  offset: 0,                 -- or next_from from previous response
  limit: 100                 -- batch size
}]
timeout: 10.0
```

**Storage Response:**

```
{
  buckets: [
    {id: 1, status: 'active'},
    {id: 2, status: 'sending'},
    {id: 3, status: receiving'},
    ...
  ],
  next_from: 100            -- Use as offset for next call
}
```

---

## PART 6: Exported Storage Functions Reference

### Public Storage API Functions

Called via `vshard.storage.call(bucket_id, mode, name, args)`:

| Function | Mode | Purpose |
|----------|------|---------|
| `bucket_force_create(bid, count)` | write | Create bucket range (bootstrap) |
| `bucket_force_drop(bid)` | write | Drop bucket forcefully |
| `bucket_collect(bid)` | write | Collect bucket garbage |
| `bucket_recv(...)` | write | Receive bucket data |
| `bucket_send(bid, dst, opts)` | write | Send bucket to replicaset |
| `bucket_stat(bid)` | read | Get bucket status |
| `bucket_pin(bid)` | write | Pin bucket (prevent moves) |
| `bucket_unpin(bid)` | write | Unpin bucket |
| `bucket_ref(bid, mode)` | - | Create reference for transaction |
| `bucket_unref(bid, mode)` | - | Drop reference |
| `buckets_info()` | read | Get all buckets info |
| `buckets_count()` | read | Get bucket count |
| `buckets_discovery(opts)` | read | Discover bucket distribution |
| `sync(timeout)` | read | Wait for replication |
| `info()` | read | Get storage instance info |
| `sharded_spaces()` | read | Get list of sharded spaces |
| `rebalancer_request_state()` | read | Get rebalancer state |
| `recovery_wakeup()` | write | Wake recovery fiber |
| `garbage_collector_wakeup()` | write | Wake GC fiber |

### Internal Service API Functions

Called via `vshard.storage._call(service_name, ...)`:

| Function | Purpose |
|----------|---------|
| `info` | Validate storage instance (vconnect handshake) |
| `bucket_recv` | Internal: receive bucket chunks |
| `recovery_bucket_stat` | Get recovery status of bucket |
| `rebalancer_apply_routes` | Apply rebalancer routes |
| `rebalancer_request_state` | Get rebalancer state |
| `storage_ref` | Create reference |
| `storage_unref` | Drop reference |
| `storage_ref_make_with_buckets` | Create ref with bucket validation |
| `storage_ref_check_with_buckets` | Check ref buckets |
| `storage_map` | Map-reduce operation |

---

## PART 7: Router Configuration

### Configuration Parameters

```lua
vshard.router.cfg({
  sharding = {
    ['rs-1'] = {
      replicas = {
        ['replica-1'] = {uri = 'storage1:3301'},
        ['replica-2'] = {uri = 'storage2:3301'}
      }
    }
  },
  bucket_count = 3000,                    -- Total buckets
  zone = 'zone1',                         -- Router zone
  weights = {zone1 = {zone1 = 0, ...}},   -- Load weights
  sync_timeout = 1.0,                     -- Sync timeout
  failover_ping_timeout = 5.0,            -- Health check
  discovery_mode = 'on',                  -- auto-discovery
  identification_mode = 'uuid_as_key',    -- or 'name_as_key'
  -- ... other options
})
```

---

## Summary Table: IPROTO Commands

| Command | Direction | Source → Dest | Function | Protocol |
|---------|-----------|---------------|----------|----------|
| `CALL` | Client → Router | Client → Router | `vshard.router.callrw/callro/etc` | IPROTO_CALL (0x0B) |
| `CALL` | Router → Storage | Router → Storage | `vshard.storage.call` | IPROTO_CALL (0x0B) |
| `CALL` | Router → Storage | Router → Storage | `vshard.storage.bucket_recv` | IPROTO_CALL (0x0B) |
| `CALL` | Router → Storage | Router → Storage | `vshard.storage.buckets_discovery` | IPROTO_CALL (0x0B) |
| `CALL` | Router → Storage | Router → Storage | `vshard.storage._call` (internal) | IPROTO_CALL (0x0B) |
| Responses | - | - | Success/Error objects | MessagePack |

---

## Key Insights

1. **All communication uses IPROTO_CALL (0x0B)** - No special IPROTO requests beyond standard net.box CALL
2. **Routing decisions are made by router** - Storage doesn't make routing decisions
3. **Bucket cache in router** - `route_map` stores bucket → replicaset mapping
4. **Error-driven cache updates** - WRONG_BUCKET errors update route_map
5. **Connection pooling** - One persistent connection per (router → storage) pair
6. **Anonymous auth** - No authentication by default; can be enabled via box.cfg
7. **Async support** - Router can make async calls via `is_async` option
8. **Timeout management** - Two-level: request-level and individual-request-level
9. **Futures for async** - Router wraps net.box futures for async results
10. **Chunked bucket transfers** - Large buckets sent in 1000-tuple chunks


---

## APPENDIX: Source Code File Summary

### Files Analyzed

1. **`vshard/router/init.lua`** (1856 lines)
   - Router API functions: `call`, `callro`, `callrw`, `callbro`, `callre`, `callbre`
   - Route resolution and bucket caching (`bucket_resolve`, `bucket_set`, `bucket_reset`)
   - Retry logic and error handling
   - Router instance management
   - Discovery fiber for bucket distribution

2. **`vshard/storage/init.lua`** (4287 lines)
   - Storage API: `call`, `_call`, `bucket_recv`, `bucket_send`, `bucket_stat`, `buckets_discovery`
   - Service API functions for internal operations
   - Bucket state management (ACTIVE, SENDING, RECEIVING, GARBAGE, etc.)
   - Rebalancer and GC logic
   - Master/replica management

3. **`vshard/replicaset.lua`** (2253 lines)
   - `replica_call()` - Low-level net.box call wrapper
   - Connection management: `replica_connect()`, `netbox_on_connect()`, `netbox_on_disconnect()`
   - `replicaset_master_call()` - Call on master replica
   - `replicaset_template_multicallro()` - Replica selection and load balancing
   - vconnect handshake (named replica validation)

4. **`vshard/cfg.lua`** (582 lines)
   - Configuration validation and schema
   - Sharding configuration templates
   - Connection URI validation

5. **`vshard/error.lua`** (346 lines)
   - VShard error codes and messages (WRONG_BUCKET, NON_MASTER, etc.)
   - Error object construction and serialization

6. **`vshard/storage/exports.lua`** (172 lines)
   - Function deployment to box.schema.func
   - Storage function exports management

7. **`vshard/init.lua`** (10 lines)
   - Module entry point

8. **`vshard/consts.lua`** (85 lines)
   - Constants: bucket states, timeouts, limits

---

## APPENDIX: Key Code References

### Bucket Resolution (router/init.lua:114-142)

```lua
local function bucket_set(router, bucket_id, rs_id)
    local replicaset = router.replicasets[rs_id]
    local old_replicaset = router.route_map[bucket_id]
    if old_replicaset ~= replicaset then
        if old_replicaset then
            old_replicaset.bucket_count = old_replicaset.bucket_count - 1
        else
            router.known_bucket_count = router.known_bucket_count + 1
        end
        replicaset.bucket_count = replicaset.bucket_count + 1
    end
    router.route_map[bucket_id] = replicaset
    return replicaset
end

local function bucket_reset(router, bucket_id)
    local replicaset = router.route_map[bucket_id]
    if replicaset then
        replicaset.bucket_count = replicaset.bucket_count - 1
        router.known_bucket_count = router.known_bucket_count - 1
    end
    router.route_map[bucket_id] = nil
end
```

### Replica Call (replicaset.lua:669-722)

```lua
local function replica_call(replica, func, args, opts)
    assert(opts and opts.timeout)
    replica.activity_ts = fiber_clock()
    local conn = replica.conn
    -- ... wait for connection to establish ...
    local ok, err = conn_vconnect_check_or_close(conn)
    if not ok then
        return false, nil, lerror.make(err)
    end
    local net_status, storage_status, retval, error_object =
        pcall(conn.call, conn, func, args, opts)
    -- ... handle errors ...
    return true, storage_status, retval, error_object
end
```

### Storage Call (storage/init.lua:3141-3180)

```lua
local function storage_call(bucket_id, mode, name, args)
    local ok_ref, err = bucket_ref(bucket_id, mode)
    if not ok_ref then
        return nil, err
    end
    local ok, ret1, ret2, ret3 = local_call(name, args)
    if not ok then
        ret1 = lerror.make(ret1)
    end
    ok_ref, err = bucket_unref(bucket_id, mode)
    if not ok_ref then
        if not ok then
            err.prev = ret1
        end
        return nil, err
    end
    -- Truncate nils to avoid box.NULL in responses
    if ret3 == nil then
        if ret2 == nil then
            if ret1 == nil then
                return ok
            end
            return ok, ret1
        end
        return ok, ret1, ret2
    end
    return ok, ret1, ret2, ret3
end
```

### Service Call API (storage/init.lua:3371-3401)

```lua
service_call_api = setmetatable({
    bucket_recv = bucket_recv,
    bucket_test_gc = bucket_test_gc,
    rebalancer_apply_routes = rebalancer_apply_routes,
    rebalancer_request_state = rebalancer_request_state,
    recovery_bucket_stat = recovery_bucket_stat,
    storage_ref = storage_ref,
    storage_ref_make_with_buckets = storage_ref_make_with_buckets,
    storage_ref_check_with_buckets = storage_ref_check_with_buckets,
    storage_unref = storage_unref,
    storage_map = storage_map,
    info = storage_service_info,
    test_api = service_call_test_api,
}, { /* metatable for serialization */ })

local function service_call(service_name, ...)
    return service_call_api[service_name](...)
end
```

---

## APPENDIX: Timing & Performance

### Timeouts

| Timeout | Default | Max | Usage |
|---------|---------|-----|-------|
| `CALL_TIMEOUT_MIN` | 0.5s | 64s | Minimum request timeout |
| `CALL_TIMEOUT_MAX` | 64s | ∞ | Maximum request timeout |
| `MASTER_SEARCH_TIMEOUT` | 5s | - | Master discovery timeout |
| `DISCOVERY_TIMEOUT` | 10s | - | Bucket discovery timeout |
| `DEFAULT_SYNC_TIMEOUT` | 1s | - | router.sync() timeout |
| `DEFAULT_BUCKET_SEND_TIMEOUT` | 10s | - | Bucket transfer timeout |
| `DEFAULT_BUCKET_RECV_TIMEOUT` | 10s | - | Bucket receive timeout |
| `RECONNECT_TIMEOUT` | 0.5s | - | Connection reconnect delay |
| `REPLICA_BACKOFF_INTERVAL` | 5s | - | Backoff after error |

### Performance Constraints

- **Bucket Chunk Size:** 1000 tuples per `bucket_recv` call
- **Lua Chunk Size:** 100000 bytes
- **Bucket Discovery Limit:** Paginated (default 100 buckets per iteration)
- **Rebalancer Sending:** Default 1 bucket/fiber
- **Rebalancer Receiving:** Default 100 buckets max
- **Connection Fetch Schema:** Optional (default true)

---

## APPENDIX: Vshard Error Codes Reference

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 1 | `WRONG_BUCKET` | Bucket moved to another replicaset | YES |
| 2 | `NON_MASTER` | Called write on replica, need to retry on master | YES |
| 3 | `BUCKET_ALREADY_EXISTS` | Bucket exists on destination | NO |
| 4 | `NO_SUCH_REPLICASET` | Replicaset not found | NO |
| 5 | `MOVE_TO_SELF` | Source == Destination | NO |
| 6 | `MISSING_MASTER` | No master in replicaset | NO |
| 7 | `TRANSFER_IS_IN_PROGRESS` | Bucket transfer in progress | YES |
| 8 | `UNREACHABLE_REPLICASET` | No active replicas in replicaset | NO |
| 9 | `NO_ROUTE_TO_BUCKET` | Bucket not found in cluster | NO |
| 10 | `NON_EMPTY` | Cluster already bootstrapped | NO |
| 11 | `UNREACHABLE_MASTER` | Master unreachable | NO |
| 12 | `OUT_OF_SYNC` | Replica out of sync | NO |
| 13 | `HIGH_REPLICATION_LAG` | Replica lag too high | NO |
| 14 | `UNREACHABLE_REPLICA` | Replica inactive | NO |
| 15 | `LOW_REDUNDANCY` | Only one active replica | NO |
| 16 | `INVALID_REBALANCING` | Sending and receiving simultaneously | NO |
| 17 | `SUBOPTIMAL_REPLICA` | Read replica not optimal | NO |
| 18 | `UNKNOWN_BUCKETS` | Buckets not discovered | NO |
| 19 | `REPLICASET_IS_LOCKED` | Replicaset locked | NO |
| 20 | `OBJECT_IS_OUTDATED` | Object outdated after reload | NO |
| 21 | `ROUTER_ALREADY_EXISTS` | Router with name exists | NO |
| 22 | `BUCKET_IS_LOCKED` | Bucket locked by GC/rebalancer | YES |
| 23 | `INVALID_CFG` | Configuration error | NO |
| 24 | `BUCKET_IS_PINNED` | Bucket is pinned | NO |
| 25 | `TOO_MANY_RECEIVING` | Too many receiving buckets | NO |
| 26 | `STORAGE_IS_REFERENCED` | Storage has active references | NO |
| 27 | `STORAGE_REF_ADD` | Can't add storage ref | NO |
| 28 | `STORAGE_REF_USE` | Can't use storage ref | NO |
| 29 | `STORAGE_REF_DEL` | Can't delete storage ref | NO |
| 30 | `BUCKET_RECV_DATA_ERROR` | Bucket data receive failed | NO |
| 32 | `REPLICASET_IN_BACKOFF` | Replicaset in backoff state | NO |
| 33 | `STORAGE_IS_DISABLED` | Storage is disabled | NO |
| 34 | `BUCKET_IS_CORRUPTED` | Bucket data corrupted | NO |
| 35 | `ROUTER_IS_DISABLED` | Router is disabled | NO |
| 36 | `BUCKET_GC_ERROR` | Error during bucket GC | NO |
| 37 | `STORAGE_CFG_IS_IN_PROGRESS` | Storage configuration in progress | NO |
| 38 | `ROUTER_CFG_IS_IN_PROGRESS` | Router configuration in progress | NO |
| 39 | `BUCKET_INVALID_UPDATE` | Invalid bucket state update | NO |
| 40 | `VHANDSHAKE_NOT_COMPLETE` | Handshake with replica incomplete | NO |
| 41 | `INSTANCE_NAME_MISMATCH` | Server name mismatch | NO |

---

## Document Generation

**Analysis Date:** 2024
**Source:** vshard router Lua source code
**Scope:** Complete IPROTO command analysis for router (server & client)
**Coverage:** 646 lines of comprehensive reference documentation

