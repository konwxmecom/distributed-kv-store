#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

RUNTIME_DATA="/tmp/distributed-kv-store-dashboard"
mkdir -p "$RUNTIME_DATA"

# Start the three-node backend cluster
./build/server 50051 50052 50053 --data-root "$RUNTIME_DATA" > /tmp/kv-node-50051.log 2>&1 &
./build/server 50052 50051 50053 --data-root "$RUNTIME_DATA" > /tmp/kv-node-50052.log 2>&1 &
./build/server 50053 50051 50052 --data-root "$RUNTIME_DATA" > /tmp/kv-node-50053.log 2>&1 &

# Start UI
python3 -m http.server 4173 --directory web > /tmp/kv-ui.log 2>&1 &

# Start gateway
python3 gateway.py --host 0.0.0.0 --target localhost:50051 --port 8080 > /tmp/kv-gateway.log 2>&1 &

echo "Cluster, UI, and gateway started."
echo "Open: http://localhost:4173"
echo "Gateway: http://localhost:8080"
