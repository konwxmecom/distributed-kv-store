# Test, Setup, and Run Guide

This guide explains how to build, test, run, and verify the Distributed KV Store from a clean local machine or a GitHub Codespace. The existing `README.md` and `SETUP.md` remain unchanged; this file adds a complete testing and operator workflow.

## 1. Project Requirements

### Linux, macOS, or GitHub Codespaces

Install:

- CMake 3.15 or newer
- A C++17 compiler
- gRPC and Protocol Buffers development packages
- GoogleTest development packages
- Python 3.10 or newer
- Git
- `curl`

On Ubuntu/Debian, the package names are commonly:

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config \
  libgrpc++-dev libprotobuf-dev protobuf-compiler \
  protobuf-compiler-grpc libgtest-dev python3 python3-pip curl
```

If the machine uses vcpkg, configure CMake with the vcpkg toolchain file instead of installing the C++ dependencies from the OS packages.

## 2. Get the Repository

```bash
git clone https://github.com/konwxmecom/distributed-kv-store.git
cd distributed-kv-store
```

To update an existing checkout:

```bash
git pull --ff-only origin main
```

## 3. Install Python Gateway Dependencies

The dashboard gateway translates HTTP requests into gRPC requests. Install its dependencies with:

```bash
python3 -m pip install -r requirements.txt
```

If the environment prevents global pip installs, use a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Use the same activated environment whenever you run `gateway.py`.

## 4. Configure and Build the C++ Project

A clean build can be created with:

```bash
cmake -S . -B build -DBUILD_BENCHMARKS=ON
cmake --build build --parallel
```

The main binaries are:

```text
build/server
build/client
build/raft_tests
build/runtime_config_tests
build/kvstore_benchmark
```

If CMake cannot find gRPC, Protobuf, or GoogleTest, configure with vcpkg:

```bash
cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DBUILD_BENCHMARKS=ON
cmake --build build --parallel
```

## 5. Run Automated Tests

Run the complete CTest suite:

```bash
rm -rf build/data/node_18051 build/data/node_18052 build/data/node_18053 \
  build/data/node_18054 build/data/node_18055 build/data/node_18056 \
  build/data/node_18057 build/data/node_18058
ctest --test-dir build --output-on-failure
```

The suite currently includes:

- `runtime_config_tests`: configuration parsing, invalid values, comments, and TLS validation
- `raft_tests`: multi-node startup, leader election, replication, persistence, failover, and follower forwarding behavior

Run one test target directly when debugging:

```bash
./build/runtime_config_tests
./build/raft_tests
```

CTest with verbose output:

```bash
ctest --test-dir build -V
```

The integration tests start temporary server processes and use test ports. The cleanup command above prevents old WAL/snapshot state from affecting the persistence and follower-forwarding tests. Stop any manually started test cluster before rerunning tests if a port is reported as busy.

Run the gateway's account-isolation, backup/restore, and topology tests with:

```bash
python3 -m unittest tests/test_gateway.py -v
```

## 6. Run a Manual Three-Node Cluster

Open three terminals from the project root.

Terminal 1:

```bash
./build/server 50051 50052 50053
```

Terminal 2:

```bash
./build/server 50052 50051 50053
```

Terminal 3:

```bash
./build/server 50053 50051 50052
```

Wait until one node reports:

```text
*** BECAME LEADER ***
```

The nodes persist state under `data/node_<port>/`. For a disposable manual run, use a separate data root:

```bash
rm -rf /tmp/kvstore-manual
./build/server 50051 50052 50053 --data-root /tmp/kvstore-manual
```

## 7. Run the Complete Dashboard Stack

After building and installing Python dependencies, the default launcher starts three nodes:

```bash
./start.sh
```

Pass one or more distinct ports to choose the static cluster membership. For example, start a
five-node cluster with:

```bash
./start.sh 50051 50052 50053 50054 50055
```

The launcher starts:

- one Raft node per supplied port (three default nodes on `50051`, `50052`, and `50053`)
- the static dashboard on port `4173`
- the HTTP gateway on port `8080`, targeting the first node

Override `UI_PORT`, `GATEWAY_PORT`, `GATEWAY_TARGET`, or `RUNTIME_DATA` through environment
variables. The ports define a static membership at startup; this does not implement safe live
Raft membership changes.

### Enable isolated dashboard users

Create a user interactively; passwords are stored as salted PBKDF2 hashes in a mode-`0600` file:

```bash
python3 create_user.py "$HOME/.config/raft-kv/users.json" alice
export GATEWAY_SESSION_SECRET="$(openssl rand -hex 32)"
USERS_FILE="$HOME/.config/raft-kv/users.json" ./start.sh 50051 50052 50053
```

Run `create_user.py` again for each account and restart the gateway so it reloads the user file.
Each dashboard account has an isolated key namespace. Keep the gateway port private; the demo's
no-auth mode is only for trusted local development.
Existing keys without a user namespace are not automatically migrated when authentication is
enabled; export them before switching existing data to account isolation.

### Backup and restore

Use the dashboard download/upload buttons while signed in. Backups contain one account's key/value
entries; restore merges values and leaves keys not listed in the backup unchanged. Store downloaded
backups outside the repository and test restores against a disposable cluster first.

Open the dashboard at:

```text
http://localhost:4173
```

The gateway health endpoint is:

```text
http://localhost:8080/api/health
```

The launcher uses `/tmp/distributed-kv-store-dashboard` for fresh dashboard runtime data, so old Raft metadata does not interfere with the demo cluster. Logs are written to:

```text
/tmp/kv-node-50051.log
/tmp/kv-node-50052.log
/tmp/kv-node-50053.log
/tmp/kv-gateway.log
/tmp/kv-ui.log
```

## 8. Dashboard and Gateway Smoke Tests

Check that the UI server is reachable:

```bash
curl -I http://127.0.0.1:4173/
```

Check gateway connectivity to a gRPC node:

```bash
curl http://127.0.0.1:8080/api/health
```

Expected result contains:

```json
{"connected": true}
```

Check cluster status:

```bash
curl http://127.0.0.1:8080/api/cluster
```

Check readiness:

```bash
curl -i http://127.0.0.1:8080/ready
```

A ready cluster returns HTTP `200` and identifies the leader. The gateway root URL (`http://localhost:8080/`) is not a dashboard page and intentionally returns `{"error":"not found"}`; use `/api/health` or `/api/cluster` instead.

Test a write:

```bash
curl -i -X PUT http://127.0.0.1:8080/api/entry \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello","value":"distributed"}'
```

Read it back:

```bash
curl 'http://127.0.0.1:8080/api/entry?key=hello'
```

List keys:

```bash
curl http://127.0.0.1:8080/api/keys
```

Delete it:

```bash
curl -i -X DELETE 'http://127.0.0.1:8080/api/entry?key=hello'
```

## 9. Benchmarking

Build benchmarks with `-DBUILD_BENCHMARKS=ON`, then run the benchmark against a running node:

```bash
./build/kvstore_benchmark localhost:50051 1000
```

The first argument is the gRPC address and the second is the number of operations.

## 10. Sanitizer Builds

Use separate build directories so sanitizer flags do not affect the normal build.

AddressSanitizer:

```bash
cmake -S . -B build-asan -DENABLE_ASAN=ON -DBUILD_BENCHMARKS=OFF
cmake --build build-asan --parallel
ctest --test-dir build-asan --output-on-failure
```

UndefinedBehaviorSanitizer:

```bash
cmake -S . -B build-ubsan -DENABLE_UBSAN=ON -DBUILD_BENCHMARKS=OFF
cmake --build build-ubsan --parallel
ctest --test-dir build-ubsan --output-on-failure
```

ThreadSanitizer can be enabled similarly, but support depends on the compiler and platform:

```bash
cmake -S . -B build-tsan -DENABLE_TSAN=ON -DBUILD_BENCHMARKS=OFF
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
```

## 11. GitHub Codespaces or Other Online Environments

The application can run inside GitHub Codespaces or another remote development container. Start it normally:

```bash
./start.sh
```

In VS Code, open the **Ports** panel and forward ports:

- `4173` for the dashboard
- `8080` for the gateway, if direct API access is needed

Open the forwarded `4173` URL from the Ports panel. Do not replace it with the container's internal `localhost` URL when browsing from your own computer.

The gateway binds to `0.0.0.0:8080` so a forwarded port can reach it. Forward both ports and keep the gateway private unless public access is intentional. The dashboard automatically maps a Codespaces `-4173.app.github.dev` URL to the matching `-8080.app.github.dev` gateway URL and sends the browser's Codespaces credentials. For other remote hostnames, allow the dashboard origin when starting the gateway and supply its URL:

```text
https://<forwarded-dashboard-host>/?api=https://<forwarded-gateway-host>
```

Start the gateway with the dashboard origin explicitly allowed:

```bash
python3 gateway.py --target localhost:50051 --port 8080 --allowed-origin https://<forwarded-dashboard-host>
```

The gateway must be forwarded publicly or privately according to the environment's security settings. Do not expose a development gateway publicly without authentication or network restrictions.

## 12. Stop the Demo Services

The launcher runs services in the background. Stop the project stack with:

```bash
./stop.sh
```

This stops Raft nodes, the gateway, and the dashboard server launched from this repository; it preserves data under `RUNTIME_DATA`.

To start the default stack again:

```bash
./start.sh
```

## 13. Troubleshooting

### Gateway offline in the dashboard

Check the gateway directly:

```bash
curl -i http://127.0.0.1:8080/api/health
```

If it is not reachable, start it manually:

```bash
python3 gateway.py --host 0.0.0.0 --target localhost:50051 --port 8080
```

Check `/tmp/kv-gateway.log` for import or port errors.

### Port already in use

Find the process using a port:

```bash
ss -ltnp | grep -E ':5005[123]|:8080|:4173'
```

Stop only the stale project process, then rerun `./start.sh`.

### No leader is elected

Inspect all node logs:

```bash
cat /tmp/kv-node-50051.log
cat /tmp/kv-node-50052.log
cat /tmp/kv-node-50053.log
```

Make sure all three nodes use the same three peer ports and that no duplicate cluster is already running.

### CMake cannot find dependencies

Verify the tools and packages:

```bash
cmake --version
g++ --version
protoc --version
python3 --version
```

Then reconfigure with the correct package manager or vcpkg toolchain file.

## 14. Recommended Verification Checklist

Before considering a local or online run complete:

```bash
cmake --build build --parallel
ctest --test-dir build --output-on-failure
./start.sh
curl -f http://127.0.0.1:4173/
curl -f http://127.0.0.1:8080/api/health
curl -f http://127.0.0.1:8080/api/cluster
```

Finally open `http://localhost:4173` locally, or the forwarded dashboard URL in Codespaces, and create, read, and delete one test key from the dashboard.
