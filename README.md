# Redis-cache-replica

A Redis-inspired distributed in-memory key-value store built from scratch in Python, implementing the core systems behind a distributed cache without relying on Redis itself.

The project evolved from a single-node TCP key-value server into a multi-node distributed system supporting **WAL-based persistence**, **TTL and LRU caching**, **consistent hashing**, **horizontal sharding**, **request routing**, **primary-replica replication**, **version-based stale-write protection**, **failover**, and **node recovery**.

Rather than treating the database or cache as a black box, **Mini-Redis** explores how these mechanisms work together under concurrent workloads, node failures, replication lag, and recovery scenarios.

```
Storage → Persistence → Caching → Sharding → Routing → Replication → Failover → Recovery
```

---

## Features

### Core Storage & Protocol
- **In-memory key-value store over TCP**: Full custom wire protocol with line-delimited Redis-like responses (`+`, `-`, `:`, `$`).
- **Thread-safe concurrent operations**: Fine-grained locking protects all reads, writes, touches, and evictions.
- **Commands supported**: `SET`, `GET`, `DEL`, `EXISTS`, `EXPIRE`, `VERSION`, `REPLSET`, `PING`, `INFO`, `KEYS`.

### Persistence & Durability
- **Write-Ahead Logging (WAL)**: JSONL append-only log with immediate `fsync` guarantees zero data loss on unexpected power cuts or `kill -9`.
- **Crash Recovery & Replay**: On restart, nodes automatically replay mutations in sequence, restoring data and key version counters while skipping already-expired entries.

### Caching Strategies
- **TTL-based expiration**: Passive eviction on key access (`GET`, `EXISTS`), complemented by an active background sweeper thread.
- **LRU eviction with configurable capacity**: Evicts least recently used keys when memory limit is reached; access patterns (`GET`, `TOUCH`) renew recency.
- **LRU Crash Consistency**: WAL replaying maintains exact access ordering and capacity limits so post-crash eviction states remain deterministic.

### Distributed Clustering & Replication
- **Consistent Hashing**: Hash ring with 150 virtual nodes (vnodes) ensures uniform shard distribution and minimizes reshuffling when nodes join or leave.
- **Router-based Request Routing**: Transparent gateway that routes client operations to the primary node owning the shard.
- **Primary-Replica Replication**: Writes to primary are versioned and replicated to designated replicas (`REPLSET`).
- **Version-based Stale & Duplicate Protection**: Replicas reject outdated or duplicate replication packets (`+IGNORED`).
- **Health Checks & Automatic Failover**: If a primary goes down, the router detects failure, reroutes the hash ring, and promotes the replica to primary.
- **Node Recovery & Rejoin**: Recovered nodes automatically rejoin the cluster topology without service interruption.

---

## Architecture

A **Router** sits in front of multiple independent Mini-Redis nodes. Each node owns a shard of the keyspace (determined by consistent hashing) and is backed by its own Store, WAL, TTL sweeper, and LRU eviction — plus a replica for fault tolerance.

```
                         Client
                           │
                           │ TCP
                           ▼
                    ┌──────────────┐
                    │    Router    │
                    │              │
                    │  Hash Ring   │
                    │  Routing     │
                    │  Replication │
                    │  Failover    │
                    └──────┬───────┘
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
         ┌───────┐     ┌───────┐     ┌───────┐
         │ Node1 │     │ Node2 │     │ Node3 │
         │ :6380 │     │ :6381 │     │ :6382 │
         └───┬───┘     └───┬───┘     └───┬───┘
             │             │             │
             ▼             ▼             ▼
          ┌──────┐      ┌──────┐      ┌──────┐
          │Store │      │Store │      │Store │
          └──┬───┘      └──┬───┘      └──┬───┘
             │             │             │
        ┌────┼────┐   ┌────┼────┐   ┌────┼────┐
        ▼    ▼    ▼   ▼    ▼    ▼   ▼    ▼    ▼
       WAL  TTL  LRU WAL  TTL  LRU WAL  TTL  LRU
```

### Request Flow
1. **Client Request**: Client sends a command to the Router.
2. **Hash Ring Lookup**: Router uses the consistent hash ring to determine the primary node responsible for the key.
3. **Primary Forwarding**: Router forwards the request to the primary node.
4. **Write & Replication**: For a write, the primary executes the operation, increments the key's version, and commits to WAL. The Router then sends a versioned `REPLSET <key> <value> <version>` request to the replica.
5. **Failover**: If the primary becomes unavailable, the Router detects the failure, promotes the replica, and reassigns traffic on the hash ring.

---

## Wire Protocol

Mini-Redis implements a clear, text-based TCP protocol:

| Command | Reply | Meaning |
|---|---|---|
| `PING` | `+PONG` | Health check |
| `SET <key> <value> [ttl]` | `+OK` | Store/update a value (optional TTL in seconds) |
| `GET <key>` | `+<value>` / `$-1` | Get value; `$-1` = key not found or expired |
| `DEL <key>` | `:1` / `:0` | Delete key; 1 = deleted, 0 = not found |
| `EXISTS <key>` | `:1` / `:0` | Check whether key exists and is unexpired |
| `EXPIRE <key> <seconds>` | `:1` / `:0` | Set TTL on an existing key |
| `VERSION <key>` | `:<version>` | Get current key mutation version |
| `REPLSET <key> <value> <ver>` | `+OK` / `+IGNORED` | Version-aware replica update |
| `TOUCH <key>` | `:1` / `:0` | Update LRU recency of a key |
| `INFO` | `+<info_string>` | Node/Router status and telemetry |
| `KEYS` | `+<key1 key2 ...>` | List unexpired keys |
| `-ERR <message>` | — | Invalid command or internal error |

---

## Directory Structure

```
redis-cache-replica/
├── server/
│   ├── __init__.py
│   ├── store.py          # thread-safe in-memory store (LRU + TTL)
│   ├── node.py           # TCP server: protocol parsing, WAL wiring
│   ├── wal.py            # write-ahead log + crash-recovery replay
│   └── ttl.py            # active TTL expiry sweeper (background thread)
├── cluster/
│   ├── __init__.py
│   ├── hash_ring.py      # consistent hashing with virtual nodes
│   ├── router.py         # client-facing routing layer
│   └── replication.py    # primary-replica sync, versioning, failover
├── client/
│   ├── __init__.py
│   └── client.py         # Python client library
├── tests/
│   ├── __init__.py
│   ├── test_store.py                # unit tests: LRU, TTL, core ops
│   ├── test_concurrent_clients.py     # many real socket clients at once
│   ├── test_crash_recovery.py           # kill -9 + WAL replay
│   ├── test_lru_crash_consistency.py      # eviction stays consistent across a crash
│   ├── test_hash_ring.py                     # key distribution + reshuffle-on-resize
│   ├── test_replication.py                     # primary/replica sync + failover
│   └── load_test.py                               # throughput/latency under concurrent load
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Getting Started

### 1. Requirements
- Python 3.9+ (Python 3.11+ recommended)
- Standard library only (no third-party dependencies required!)

### 2. Running a Single Node
Start a standalone TCP node on port 6380 with WAL persistence and LRU capacity:
```bash
python -m server.node --host 127.0.0.1 --port 6380 --wal data/node_6380.wal --capacity 1000
```

Interact via `nc` (netcat), telnet, or Python:
```bash
nc 127.0.0.1 6380
SET user:1 Alice
+OK
GET user:1
+Alice
VERSION user:1
:1
```

### 3. Running with Docker Compose
To launch the full 3-primary + 3-replica distributed cluster with a Router:
```bash
docker compose up --build
```

Services started:
- Router: `localhost:6379`
- Primaries: `localhost:6380`, `localhost:6381`, `localhost:6382`
- Replicas: `localhost:6383`, `localhost:6384`, `localhost:6385`

---

## Python Client Library

Use the built-in `MiniRedisClient`:

```python
from client.client import MiniRedisClient

# Connect to Router (port 6379) or any standalone Node
client = MiniRedisClient(host="127.0.0.1", port=6379)

with client:
    # Basic SET and GET
    client.set("product:101", "Laptop")
    print(client.get("product:101"))  # "Laptop"

    # Version tracking
    print(client.version("product:101"))  # 1

    # TTL Expiration (5 seconds)
    client.set("session:token", "xyz987", ttl=5.0)
    print(client.exists("session:token"))  # True

    # Delete
    client.delete("product:101")
    print(client.get("product:101"))  # None
```

---

## Running the Test Suite

The test suite covers unit operations, concurrency, process crash recovery, LRU consistency, consistent hashing distribution, and replication failover:

```bash
# Run all tests using Python's built-in unittest discovery
python -m unittest discover tests -v
```

### Individual Test Suites
```bash
# 1. Store unit tests (LRU eviction, TTL expiry, versioning)
python -m unittest tests/test_store.py

# 2. Real concurrent TCP socket clients
python -m unittest tests/test_concurrent_clients.py

# 3. Crash recovery (subprocess kill -9 + WAL replay verification)
python -m unittest tests/test_crash_recovery.py

# 4. LRU eviction consistency across crashes
python -m unittest tests/test_lru_crash_consistency.py

# 5. Consistent hash ring distribution and minimal reshuffling
python -m unittest tests/test_hash_ring.py

# 6. Primary-replica replication, stale write protection, and failover
python -m unittest tests/test_replication.py
```

### Running the Load Benchmark
```bash
python tests/load_test.py --concurrency 10 --requests 2000 --read-ratio 0.7
```

Sample output:
```
==================================================
      MINI-REDIS LOAD & LATENCY BENCHMARK
==================================================
Concurrency level:       10 clients
Completed requests:      2000
Total duration:          1.824 s
Throughput:              1096.49 ops/sec
Errors:                  0
--------------------------------------------------
Latency Distribution (ms):
  Min:                   1.21 ms
  Avg:                   7.94 ms
  Median (p50):          7.85 ms
  p95:                   8.42 ms
  p99:                   8.89 ms
  Max:                   9.35 ms
==================================================
```

---

## License
MIT License
