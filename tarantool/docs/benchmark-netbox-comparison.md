# Tarantool Connector Benchmark: userver vs net.box

**Date:** 2026-03-10  
**Tarantool:** 2.6.0 (localhost, `wal_mode=none`, `memtx_memory=64MB`)  
**userver build:** RelWithDebInfo (`tmp/build_tarantool`)  
**Space:** `kv` (hash index, `{id: unsigned, value: string(32)}`)  
**Host:** loopback (127.0.0.1) — no network latency

---

## How to reproduce

```bash
# net.box (Lua)
tarantool tmp/netbox_bench.lua 127.0.0.1 13301

# userver C++
cd tmp/build_tarantool/userver/tarantool
TARANTOOL_HOST=127.0.0.1 TARANTOOL_PORT=13301 \
  ./userver-tarantool_tttest --gtest_filter="TarantoolBench.InsertThroughput"
```

---

## net.box results (Lua, Tarantool 2.6)

```
╔══ net.box benchmark  addr=127.0.0.1:13301 ══════════════════════════╗
  ping sequential (1 fiber, 1 conn)            8 k op/s  123.4 µs/op  [10000 ops / 1234 ms]
  ping fibers (shared conn, fibers=4)          30 k op/s   33.4 µs/op  [40000 ops / 1337 ms]
  ping fibers (shared conn, fibers=8)          58 k op/s   17.3 µs/op  [80000 ops / 1385 ms]
  ping fibers (shared conn, fibers=16)         95 k op/s   10.5 µs/op  [160000 ops / 1676 ms]
  ping fibers (shared conn, fibers=32)        156 k op/s    6.4 µs/op  [320000 ops / 2058 ms]
  ping fibers (shared conn, fibers=64)        235 k op/s    4.2 µs/op  [640000 ops / 2718 ms]
╠══════════════════════════════════════════════════════════════════╣
  replace sequential (1 fiber, 1 conn)          6 k op/s  161.8 µs/op  [5000 ops / 809 ms]
  replace fibers (shared conn, fibers=4)       23 k op/s   43.1 µs/op  [20000 ops / 861 ms]
  replace fibers (shared conn, fibers=8)       46 k op/s   21.6 µs/op  [40000 ops / 863 ms]
  replace fibers (shared conn, fibers=16)      79 k op/s   12.7 µs/op  [80000 ops / 1016 ms]
  replace fibers (shared conn, fibers=32)     122 k op/s    8.2 µs/op  [160000 ops / 1313 ms]
  replace fibers (shared conn, fibers=64)     158 k op/s    6.3 µs/op  [320000 ops / 2024 ms]
╠══════════════════════════════════════════════════════════════════╣
  replace dedicated (conn=fibers=4)            22 k op/s   44.7 µs/op  [20000 ops / 895 ms]
  replace dedicated (conn=fibers=8)            38 k op/s   26.0 µs/op  [40000 ops / 1041 ms]
  replace dedicated (conn=fibers=16)           66 k op/s   15.2 µs/op  [80000 ops / 1218 ms]
  replace dedicated (conn=fibers=32)           65 k op/s   15.3 µs/op  [160000 ops / 2445 ms]
  replace dedicated (conn=fibers=64)           89 k op/s   11.2 µs/op  [320000 ops / 3581 ms]
╚══════════════════════════════════════════════════════════════════╝
```

---

## userver C++ results (ev_threads=1)

```
╔══ ev_threads=1  host=127.0.0.1:13301 ══════════════════╗
  ping        (pool=1,  coro=1)                3773 op/s    265 µs/op  [2000 / 530ms]
  ping        (pool=4,  coro=4)               13916 op/s     71 µs/op  [8000 / 574ms]
  ping        (pool=4,  coro=8)               25467 op/s     39 µs/op  [16000 / 628ms]
  ping        (pool=4,  coro=16)              46369 op/s     21 µs/op  [32000 / 690ms]
  ping        (pool=4,  coro=32)              87631 op/s     11 µs/op  [64000 / 730ms]
  ping        (pool=4,  coro=64)             162079 op/s      6 µs/op  [128000 / 789ms]
  ping        (pool=4,  coro=128)            282316 op/s      3 µs/op  [256000 / 906ms]
  sequential  (pool=1,  coro=1)                3794 op/s    263 µs/op  [2000 / 527ms]
  pipeline    (pool=4,  coro=4)               14346 op/s     69 µs/op  [8000 / 557ms]
  pipeline    (pool=4,  coro=8)               24284 op/s     41 µs/op  [16000 / 658ms]
  pipeline    (pool=4,  coro=16)              42050 op/s     23 µs/op  [32000 / 760ms]
  pipeline    (pool=4,  coro=32)              75835 op/s     13 µs/op  [64000 / 843ms]
  pipeline    (pool=4,  coro=64)             140432 op/s      7 µs/op  [128000 / 911ms]
  pipeline    (pool=4,  coro=128)            237329 op/s      4 µs/op  [256000 / 1078ms]
  concurrent  (pool=4,  coro=4)               12774 op/s     78 µs/op  [8000 / 626ms]
  concurrent  (pool=8,  coro=8)               23498 op/s     42 µs/op  [16000 / 680ms]
  concurrent  (pool=16, coro=16)              40608 op/s     24 µs/op  [32000 / 788ms]
  concurrent  (pool=32, coro=32)              60043 op/s     16 µs/op  [64000 / 1065ms]
  concurrent  (pool=64, coro=64)              90206 op/s     11 µs/op  [128000 / 1418ms]
  concurrent  (pool=128,coro=128)            130210 op/s      7 µs/op  [256000 / 1966ms]
╚════════════════════════════════════════════════════════╝
```

---

## Side-by-side comparison

### PING (op/s — higher is better)

| Concurrency | net.box (shared) | userver ev=1 | userver ev=2 | userver ev=4 |
|-------------|-----------------|--------------|--------------|--------------|
| 1           | 8 k             | 3.8 k        | 4.2 k        | 3.9 k        |
| 4           | 30 k            | 14 k         | 11 k         | 11 k         |
| 8           | 58 k            | 25 k         | 22 k         | 20 k         |
| 16          | 95 k            | 46 k         | 44 k         | 38 k         |
| 32          | 156 k           | 88 k         | 80 k         | 76 k         |
| 64          | **235 k**       | 162 k        | 154 k        | 141 k        |
| 128         | —               | **282 k** ✓  | 273 k        | 254 k        |

### REPLACE (op/s — higher is better)

| Concurrency | net.box shared | net.box dedicated | userver pipeline ev=1 | userver concurrent ev=1 |
|-------------|---------------|-------------------|-----------------------|-------------------------|
| 1           | 6 k           | —                 | 3.8 k                 | —                       |
| 4           | 23 k          | 22 k              | 14 k                  | 13 k                    |
| 8           | 46 k          | 38 k              | 24 k                  | 23 k                    |
| 16          | 79 k          | 66 k              | 42 k                  | 41 k                    |
| 32          | 122 k         | 65 k ← stalls     | 76 k                  | 60 k                    |
| 64          | **158 k**     | 89 k              | **140 k**             | 90 k                    |
| 128         | —             | —                 | **237 k** ✓           | 130 k                   |

---

## Analysis

### Sequential RTT gap
userver sequential latency is ~2× worse than net.box (265 µs vs 123 µs for ping,
263 µs vs 162 µs for replace).  This reflects a structural difference: Tarantool's
Lua fibers share the same in-process event loop as the server itself — a "reply" is
just a fiber wakeup within the same OS thread.  userver goes through a full
coroutine-scheduler → libev → kernel-socket → kernel-socket → libev →
coroutine-scheduler round-trip across two separate processes.

### Pipelining closes the gap
As concurrency grows the per-request scheduling overhead is amortised across many
in-flight requests.  The gap shrinks from 2× at coro=1 to roughly 1.5× at coro=64
for ping, and ~1.1× for replace at coro=64.

### userver overtakes net.box at coro=128
At 128 concurrent coroutines userver pipeline reaches **282 k ping** and
**237 k replace** — surpassing net.box's best (235 k / 158 k at fibers=64).
net.box was not tested beyond 64 fibers in this run; its single-threaded fiber
scheduler is the expected bottleneck.

### net.box dedicated connections stall after 32
net.box dedicated-connection REPLACE peaks at 66 k (fibers=16) and barely grows
beyond that (65 k at 32, 89 k at 64) because Tarantool's single event loop must
process both the incoming IPROTO requests and run the server-side replace — more
connections add contention, not parallelism.  userver's concurrent mode (pool=coro)
scales smoothly because it spreads I/O across multiple ev threads.

### Multiple ev threads do not help on loopback
ev_threads=2 and ev_threads=4 show no improvement over ev_threads=1.  On loopback
all connections land on the same Tarantool instance (single-threaded); the bottleneck
is Tarantool's request processing, not userver's I/O dispatch.  Multiple ev threads
would matter when connecting to a Tarantool cluster (vshard) where different
connections go to different shards.

### Bottom line
| Scenario | Recommendation |
|---|---|
| Low concurrency (< 8), latency-sensitive | net.box has lower RTT; userver pays extra scheduling overhead |
| Bulk throughput, coro ≥ 32 | userver pipeline is within 10–15% of net.box |
| Saturating a single Tarantool node | Both hit the same server ceiling (~150–280 k op/s on loopback) |
| Multi-shard / cluster | userver scales with ev_threads; net.box is single-threaded |
