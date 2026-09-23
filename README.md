# Distributed Key-Value Store with Raft Consensus

A distributed key-value store built from scratch in C++, implementing the **Raft consensus algorithm** for leader election and log replication. The applied state machine is in memory, while the Raft WAL and snapshots provide durable recovery. Inspired by systems like etcd and Consul, this project demonstrates consensus, replication, failure detection, and automatic recovery.

## Features

- **Leader election** via Raft — term-based voting with randomized election timeouts to minimize split votes
- **Log-based replication** — writes are appended to a replicated log and only applied once confirmed by a majority of nodes
- **Automatic failover** — if the leader crashes, the remaining nodes detect the failure and elect a new leader without manual intervention
- **Automatic log catch-up** — a node that falls behind (e.g., after a restart) automatically receives and replays missing log entries
- **gRPC-based communication** — all inter-node and client-server communication uses Protocol Buffers over gRPC
- **Chaos testing** — includes a script that randomly kills and restarts cluster nodes to verify resilience
- **Durable recovery** — committed log entries and Raft election metadata survive process restarts
- **Snapshots and compaction** — committed state is periodically snapshotted to bound WAL growth
- **Optional mutual TLS** — client and peer connections can require verified certificates
- **Reloadable membership** — peer endpoints can be changed through a watched configuration file
- **Operator console** — a responsive browser dashboard for inspecting keys and cluster activity

## Architecture

Each node in the cluster runs identical code and can be in one of three Raft states:

- **Follower** — the default state; listens for heartbeats/log entries from a leader
- **Candidate** — a node that hasn't heard from a leader within its election timeout, and is requesting votes to become leader
- **Leader** — the node currently handling client writes and replicating them to followers

Nodes communicate over gRPC using the following core RPCs (defined in `proto/kvstore.proto`):

| RPC | Purpose |
|---|---|
| `Get` / `Set` / `Delete` | Client-facing key-value operations |
| `RequestVote` | Used by candidates during leader election |
| `AppendEntries` | Used by the leader to replicate log entries and send heartbeats |
| `Heartbeat` | Lightweight liveness signal from leader to followers |

Writes flow through a **replicated log**: an entry is first appended locally, then sent to all peers. Only once a **majority** of nodes have acknowledged the entry is it marked as *committed* and applied to the in-memory key-value store — this is what guarantees consistency across the cluster even in the presence of node failures.

## Tech Stack

- **C++17**
- **gRPC** + **Protocol Buffers** for RPC and serialization
- **CMake** as the build system
- **vcpkg** for dependency management

## Getting Started

See [SETUP.md](./SETUP.md) for full, step-by-step instructions on setting up the toolchain, building the project, and running a multi-node cluster.

### Linux and macOS

Install CMake, a C++17 compiler, gRPC, Protocol Buffers, and GoogleTest from the
platform package manager, then use the same portable commands:

```bash
cmake -S . -B build
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

### Quick overview

```powershell
# Build
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=C:\dev\vcpkg\scripts\buildsystems\vcpkg.cmake
cmake --build build

# Run a 3-node cluster (in three separate terminals)
.\build\Debug\server.exe 50051 50052 50053
.\build\Debug\server.exe 50052 50051 50053
.\build\Debug\server.exe 50053 50051 50052

# Run the client against any node
.\build\Debug\client.exe
```

For repeatable deployments, start a node from a config file:

```text
./build/server --config config/node.conf
```

The config supports `port`, comma-separated `peers`, `data_root`, `peers_file`,
and the TLS fields `ca`, `cert`, and `key`. Explicit command-line values override
config-file values.

### Chaos testing

```powershell
.\chaos_test.ps1
```

This starts a 3-node cluster and repeatedly kills/restarts a random node to verify the cluster recovers automatically.

## Project Structure

```
distributed-kv-store/
├── proto/
│   └── kvstore.proto        # Service and message definitions
├── server.cpp                # Raft node implementation (election + replication + KV store)
├── client.cpp                 # Simple gRPC client
├── chaos_test.ps1            # Automated resilience testing script
├── CMakeLists.txt
└── SETUP.md
```

## Runtime configuration

Each node stores its state under `data/node_<port>/`:

- `raft.meta` stores the current term and vote.
- `raft.snapshot` stores compacted state after the committed log grows sufficiently.
- `raft.wal` contains length-prefixed protobuf log records and is replayed on startup.

For mTLS, pass the CA, certificate, and private key to both servers and clients:

```powershell
.\build\Debug\server.exe 50051 50052 50053 --ca ca.pem --cert node-50051.pem --key node-50051-key.pem
.\build\Debug\client.exe --address localhost:50051 --ca ca.pem --cert client.pem --key client-key.pem
```

The server flushes committed log records to `raft.wal` before acknowledging writes.
After the compaction threshold is reached, committed state is written to
`raft.snapshot` and the obsolete WAL prefix is removed. Restart recovery loads the
snapshot first and then replays the remaining WAL suffix.

TLS configuration is intentionally strict: `ca`, `cert`, and `key` must either all
be present or all be omitted. When present, the server requires verified client
certificates and the gateway/client use the same CA trust root.

To reload membership without restarting a node, pass `--peers-file`. Put one `host:port`
per line in the file and edit it while the node is running:

```text
localhost:50052
localhost:50053
```

```powershell
.\build\Debug\server.exe 50051 --peers-file peers-50051.txt
```

## Operator console

The `web/` directory contains a lightweight dashboard for exploring the store from a browser.
The Python gateway forwards browser CRUD requests to the real C++ gRPC node.

```powershell
# Terminal 1: serve the UI
python -m http.server 4173 --directory web

# Terminal 2: install and run the REST-to-gRPC gateway
python -m pip install -r requirements.txt
python gateway.py --target localhost:50051 --port 8080
```

Open `http://localhost:4173` after starting the gateway and the Raft node. The console includes
live health status, key CRUD requests, node topology, replication summary, and recent activity.
The current protobuf API has no list-keys RPC, so the browser remembers keys written through
the console and reads each one back through the gateway.

The gateway also exposes `/health` for transport liveness, `/ready` for leader readiness,
`/metrics` for Prometheus-compatible request counters, and `/api/cluster` for Raft term,
commit index, and leader state.

## License

MIT
