#!/usr/bin/env python3
"""Small HTTP gateway for the KVStore gRPC service."""

import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import grpc

ROOT = Path(__file__).resolve().parent
PROTO = ROOT / "proto" / "kvstore.proto"
REQUEST_COUNTERS = {
    "get": 0,
    "set": 0,
    "delete": 0,
    "health": 0,
    "cluster": 0,
}


def dashboard_origins():
    origins = {"http://localhost:4173", "http://127.0.0.1:4173"}
    codespace = os.environ.get("CODESPACE_NAME")
    forwarding_domain = os.environ.get("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
    if codespace:
        origins.add(f"https://{codespace}-4173.{forwarding_domain}")
    return origins


def read_text(path):
    return pathlib.Path(path).read_text(encoding="utf-8")


def load_generated_client():
    output_dir = Path(tempfile.mkdtemp(prefix="kvstore-proto-"))
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO.parent}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        str(PROTO),
    ]
    subprocess.run(command, check=True, cwd=ROOT)
    sys.path.insert(0, str(output_dir))
    import kvstore_pb2  # type: ignore
    import kvstore_pb2_grpc  # type: ignore

    return kvstore_pb2, kvstore_pb2_grpc


class GatewayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    pb2 = None
    stub = None
    allowed_origins = set()

    def _send_cors_headers(self):
        origin = self.headers.get("Origin")
        if origin in self.allowed_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Vary", "Origin")

    @staticmethod
    def _record_metric(method, status_code):
        if method in REQUEST_COUNTERS:
            REQUEST_COUNTERS[method] += 1

    def _send(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        return json.loads(self._read_body() or b"{}")

    def _read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length)

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            try:
                self.stub.Heartbeat(self.pb2.HeartbeatRequest(leader_port=0, leader_term=0), timeout=2)
                self._record_metric("health", 200)
                self._send(200, {"status": "ok", "connected": True, "target": self.server.target})
            except grpc.RpcError as error:
                self._record_metric("health", 503)
                self._send(503, {"status": "error", "connected": False, "error": error.details()})
            return

        if parsed.path == "/metrics":
            lines = ["# HELP kvstore_requests_total Total HTTP requests handled by the gateway.", "# TYPE kvstore_requests_total counter"]
            for method, count in REQUEST_COUNTERS.items():
                lines.append(f'kvstore_requests_total{{method="{method}"}} {count}')
            body = "\n".join(lines) + "\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))
            return

        if parsed.path == "/api/health":
            try:
                self.stub.Heartbeat(self.pb2.HeartbeatRequest(leader_port=0, leader_term=0), timeout=2)
                self._record_metric("health", 200)
                self._send(200, {"connected": True, "target": self.server.target})
            except grpc.RpcError as error:
                self._record_metric("health", 503)
                self._send(503, {"connected": False, "error": error.details()})
            return

        if parsed.path == "/ready":
            try:
                response = self.stub.GetClusterStatus(self.pb2.ClusterStatusRequest(), timeout=2)
                ready = response.is_leader
                self._record_metric("health", 200 if ready else 503)
                self._send(200 if ready else 503, {
                    "status": "ready" if ready else "not_ready",
                    "leader_id": response.leader_id,
                    "current_term": response.current_term,
                })
            except grpc.RpcError as error:
                self._record_metric("health", 503)
                self._send(503, {"status": "not_ready", "error": error.details()})
            return

        if parsed.path == "/api/keys":
            try:
                response = self.stub.ListKeys(self.pb2.ListKeysRequest(), timeout=5)
                self._record_metric("get", 200)
                self._send(200, {"keys": list(response.keys)})
            except grpc.RpcError as error:
                self._record_metric("get", 502)
                self._send(502, {"error": error.details()})
            return

        if parsed.path == "/api/cluster":
            try:
                response = self.stub.GetClusterStatus(self.pb2.ClusterStatusRequest(), timeout=5)
                self._record_metric("cluster", 200)
                self._send(200, {
                    "node_id": response.node_id,
                    "leader_id": response.leader_id,
                    "current_term": response.current_term,
                    "commit_index": response.commit_index,
                    "is_leader": response.is_leader,
                })
            except grpc.RpcError as error:
                self._record_metric("cluster", 502)
                self._send(502, {"error": error.details()})
            return

        if parsed.path == "/api/entry":
            key = parse_qs(parsed.query).get("key", [""])[0]
            if not key:
                self._record_metric("get", 400)
                self._send(400, {"error": "key is required"})
                return
            try:
                response = self.stub.Get(self.pb2.GetRequest(key=key), timeout=2)
                self._record_metric("get", 200)
                self._send(200, {"key": key, "value": response.value, "found": response.found})
            except grpc.RpcError as error:
                self._record_metric("get", 502)
                self._send(502, {"error": error.details()})
            return

        self._send(404, {"error": "not found"})

    def do_POST(self):
        if urlparse(self.path).path != "/api/entry":
            self._send(404, {"error": "not found"})
            return
        origin = self.headers.get("Origin")
        if origin and origin not in self.allowed_origins:
            self._send(403, {"error": "origin is not allowed"})
            return
        try:
            body = parse_qs(self._read_body().decode("utf-8"), keep_blank_values=True)
            key = body.get("key", [""])[0].strip()
            value = body.get("value", [""])[0]
            self._set_entry(key, value)
        except (UnicodeDecodeError, ValueError) as error:
            self._record_metric("set", 400)
            self._send(400, {"error": str(error)})

    def do_PUT(self):
        if urlparse(self.path).path != "/api/entry":
            self._send(404, {"error": "not found"})
            return
        try:
            body = self._read_json()
            key, value = str(body.get("key", "")).strip(), str(body.get("value", ""))
            self._set_entry(key, value)
        except (TypeError, json.JSONDecodeError) as error:
            self._record_metric("set", 400)
            self._send(400, {"error": str(error)})

    def _set_entry(self, key, value):
        if not key or not value:
            self._record_metric("set", 400)
            self._send(400, {"error": "key and value are required"})
            return
        try:
            response = self.stub.Set(self.pb2.SetRequest(key=key, value=value), timeout=5)
            if not response.success:
                self._record_metric("set", 409)
                self._send(409, {"success": False, "error": "write was not committed"})
                return
            self._record_metric("set", 200)
            self._send(200, {"success": True, "key": key, "value": value})
        except grpc.RpcError as error:
            self._record_metric("set", 502)
            self._send(502, {"success": False, "error": error.details()})

    def do_DELETE(self):
        parsed = urlparse(self.path)
        key = parse_qs(parsed.query).get("key", [""])[0]
        if parsed.path != "/api/entry" or not key:
            self._record_metric("delete", 400)
            self._send(400, {"error": "key is required"})
            return
        try:
            response = self.stub.Delete(self.pb2.DeleteRequest(key=key), timeout=5)
            self._record_metric("delete", 200)
            self._send(200, {"success": response.success, "key": key})
        except grpc.RpcError as error:
            self._record_metric("delete", 502)
            self._send(502, {"success": False, "error": error.details()})

    def log_message(self, format, *args):
        print(f"[gateway] {self.address_string()} - {format % args}")


def main():
    parser = argparse.ArgumentParser(description="HTTP gateway for the Raft KVStore")
    parser.add_argument("--target", default="localhost:50051", help="gRPC node address")
    parser.add_argument("--port", type=int, default=8080, help="HTTP gateway port")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP gateway bind address")
    parser.add_argument("--allowed-origin", action="append", default=[], help="additional dashboard origin allowed to call the gateway")
    parser.add_argument("--ca", help="CA certificate for mTLS")
    parser.add_argument("--cert", help="client certificate for mTLS")
    parser.add_argument("--key", help="client private key for mTLS")
    args = parser.parse_args()

    pb2, pb2_grpc = load_generated_client()
    tls_files = (args.ca, args.cert, args.key)
    if any(tls_files) and not all(tls_files):
        parser.error("--ca, --cert, and --key must be provided together")
    if all(tls_files):
        credentials = grpc.ssl_channel_credentials(
            root_certificates=read_text(args.ca).encode(),
            certificate_chain=read_text(args.cert).encode(),
            private_key=read_text(args.key).encode(),
        )
        channel = grpc.secure_channel(args.target, credentials)
    else:
        channel = grpc.insecure_channel(args.target)
    GatewayHandler.pb2 = pb2
    GatewayHandler.stub = pb2_grpc.KVStoreStub(channel)
    GatewayHandler.allowed_origins = dashboard_origins().union(args.allowed_origin)
    server = ThreadingHTTPServer((args.host, args.port), GatewayHandler)
    server.target = args.target
    print(f"KV gateway listening on http://127.0.0.1:{args.port} -> {args.target}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        channel.close()


if __name__ == "__main__":
    main()
