#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if (($# > 0)); then
	node_ports=("$@")
else
	node_ports=(50051 50052 50053)
fi

declare -A seen_ports=()
for node_port in "${node_ports[@]}"; do
	if [[ ! "$node_port" =~ ^[1-9][0-9]{0,4}$ ]] || ((node_port > 65535)); then
		echo "Invalid node port: $node_port" >&2
		exit 2
	fi
	if [[ -n "${seen_ports[$node_port]:-}" ]]; then
		echo "Duplicate node port: $node_port" >&2
		exit 2
	fi
	seen_ports[$node_port]=1
done

RUNTIME_DATA="${RUNTIME_DATA:-/tmp/distributed-kv-store-dashboard}"
UI_PORT="${UI_PORT:-4173}"
GATEWAY_PORT="${GATEWAY_PORT:-8080}"
GATEWAY_TARGET="${GATEWAY_TARGET:-localhost:${node_ports[0]}}"
mkdir -p "$RUNTIME_DATA"

# Start one Raft node per requested port, with all other ports as its peers.
for node_port in "${node_ports[@]}"; do
	peer_ports=()
	for peer_port in "${node_ports[@]}"; do
		if [[ "$peer_port" != "$node_port" ]]; then
			peer_ports+=("$peer_port")
		fi
	done
	./build/server "$node_port" "${peer_ports[@]}" --data-root "$RUNTIME_DATA" > "/tmp/kv-node-${node_port}.log" 2>&1 &
done

# Start UI and gateway.
python3 -m http.server "$UI_PORT" --directory web > /tmp/kv-ui.log 2>&1 &
python3 gateway.py --host 0.0.0.0 --target "$GATEWAY_TARGET" --port "$GATEWAY_PORT" > /tmp/kv-gateway.log 2>&1 &

echo "Started Raft nodes on ports: ${node_ports[*]}"
echo "Dashboard: http://localhost:${UI_PORT}"
echo "Gateway: http://localhost:${GATEWAY_PORT} -> ${GATEWAY_TARGET}"
