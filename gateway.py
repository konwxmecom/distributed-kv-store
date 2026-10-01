#!/usr/bin/env python3
"""Small HTTP gateway for the KVStore gRPC service."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import os
import pathlib
import re
import secrets
import subprocess
import sys
import tempfile
import time
from http.cookies import CookieError, SimpleCookie
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
    "backup": 0,
    "restore": 0,
    "auth_failure": 0,
}
PASSWORD_ITERATIONS = 310_000
SESSION_COOKIE = "raft_session"
SESSION_TTL_SECONDS = 8 * 60 * 60
MAX_REQUEST_BYTES = 10 * 1024 * 1024


def create_password_record(password):
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS)
    return {
        "salt": base64.b64encode(salt).decode("ascii"),
        "digest": base64.b64encode(digest).decode("ascii"),
        "iterations": PASSWORD_ITERATIONS,
    }


def load_users(path):
    users_path = Path(path)
    if os.name == "posix" and users_path.stat().st_mode & 0o077:
        raise ValueError("users file must be private (chmod 600)")
    users = json.loads(users_path.read_text(encoding="utf-8"))
    if not isinstance(users, dict) or not users:
        raise ValueError("users file must contain at least one user")
    for username, record in users.items():
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", username):
            raise ValueError(f"invalid username in users file: {username!r}")
        if not isinstance(record, dict) or not {"salt", "digest", "iterations"} <= record.keys():
            raise ValueError(f"invalid password record for user {username!r}")
    return users


def verify_password(record, password):
    try:
        salt = base64.b64decode(record["salt"], validate=True)
        expected = base64.b64decode(record["digest"], validate=True)
        iterations = int(record["iterations"])
    except (KeyError, TypeError, ValueError):
        return False
    if not 100_000 <= iterations <= 2_000_000:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def issue_session(username, secret):
    expires = int(time.time()) + SESSION_TTL_SECONDS
    payload = f"{username}:{expires}"
    signature = hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{payload}:{signature}".encode("utf-8")).decode("ascii").rstrip("=")


def read_session(token, secret, users):
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded).decode("utf-8")
        username, expires, signature = payload.rsplit(":", 2)
        expected = hmac.new(secret, f"{username}:{expires}".encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected) or int(expires) <= int(time.time()):
            return None
        return username if username in users else None
    except (ValueError, UnicodeDecodeError):
        return None


def dashboard_origins():
    ui_port = os.environ.get("UI_PORT", "4173")
    origins = {f"http://localhost:{ui_port}", f"http://127.0.0.1:{ui_port}"}
    codespace = os.environ.get("CODESPACE_NAME")
    forwarding_domain = os.environ.get("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
    if codespace:
        origins.add(f"https://{codespace}-{ui_port}.{forwarding_domain}")
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
    users = {}
    session_secret = b""

    def _current_user(self):
        if not self.users:
            return ""
        cookies = SimpleCookie()
        try:
            cookies.load(self.headers.get("Cookie", ""))
        except CookieError:
            return None
        session = cookies.get(SESSION_COOKIE)
        if not session:
            return None
        return read_session(session.value, self.session_secret, self.users)

    def _require_user(self):
        username = self._current_user()
        if username is None:
            self._send(401, {"error": "authentication required"})
            return None
        return username

    @staticmethod
    def _storage_key(username, key):
        return f"user/{username}/{key}" if username else key

    def _origin_allowed(self):
        origin = self.headers.get("Origin")
        return not origin or origin in self.allowed_origins

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

    def _send(self, status, payload, headers=()):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        for name, value in headers:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        return json.loads(self._read_body() or b"{}")

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise ValueError("invalid Content-Length") from None
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise ValueError("request body exceeds 10 MiB")
        return self.rfile.read(length)

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        username = ""
        if parsed.path.startswith("/api/") and parsed.path != "/api/health":
            username = self._require_user()
            if username is None:
                return
        if parsed.path == "/metrics" and self.users:
            username = self._require_user()
            if username is None:
                return

        if parsed.path == "/health":
            try:
                self.stub.Heartbeat(self.pb2.HeartbeatRequest(leader_port=0, leader_term=0), timeout=2)
                self._record_metric("health", 200)
                self._send(200, {
                    "status": "ok",
                    "connected": True,
                    "target": self.server.target,
                    "auth_required": bool(self.users),
                    "authenticated": not self.users or self._current_user() is not None,
                })
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
                self._send(200, {
                    "connected": True,
                    "target": self.server.target,
                    "auth_required": bool(self.users),
                    "authenticated": not self.users or self._current_user() is not None,
                })
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

        if parsed.path == "/api/nodes":
            node_stubs = self.server.node_stubs
            with ThreadPoolExecutor(max_workers=max(1, min(len(node_stubs), 16))) as executor:
                nodes = list(executor.map(self._read_node_status, node_stubs))
            leader = next((node["node_id"] for node in nodes if node["is_leader"]), -1)
            for node in nodes:
                node["leader_id"] = leader
            self._send(200, {"nodes": nodes})
            return

        if parsed.path == "/api/backup":
            try:
                response = self.stub.ListKeys(self.pb2.ListKeysRequest(), timeout=5)
                prefix = self._storage_key(username, "")
                entries = {}
                for stored_key in response.keys:
                    if not stored_key.startswith(prefix):
                        continue
                    key = stored_key[len(prefix):]
                    item = self.stub.Get(self.pb2.GetRequest(key=stored_key), timeout=5)
                    if item.found:
                        entries[key] = item.value
                self._record_metric("backup", 200)
                self._send(200, {
                    "format": "raft-kv-backup-v1",
                    "created_at": int(time.time()),
                    "user": username or "default",
                    "entries": entries,
                }, (("Content-Disposition", "attachment; filename=raft-kv-backup.json"),))
            except grpc.RpcError as error:
                self._record_metric("backup", 502)
                self._send(502, {"error": error.details()})
            return

        if parsed.path == "/api/keys":
            try:
                response = self.stub.ListKeys(self.pb2.ListKeysRequest(), timeout=5)
                self._record_metric("get", 200)
                prefix = self._storage_key(username, "")
                visible_keys = [key[len(prefix):] for key in response.keys if key.startswith(prefix)]
                self._send(200, {"keys": visible_keys})
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
                stored_key = self._storage_key(username, key)
                response = self.stub.Get(self.pb2.GetRequest(key=stored_key), timeout=2)
                self._record_metric("get", 200)
                self._send(200, {"key": key, "value": response.value, "found": response.found})
            except grpc.RpcError as error:
                self._record_metric("get", 502)
                self._send(502, {"error": error.details()})
            return

        self._send(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if not self._origin_allowed():
            self._send(403, {"error": "origin is not allowed"})
            return
        if path == "/api/login":
            if not self.users:
                self._send(400, {"error": "authentication is not configured"})
                return
            try:
                form = parse_qs(self._read_body().decode("utf-8"), keep_blank_values=True)
                username = form.get("username", [""])[0]
                password = form.get("password", [""])[0]
            except (UnicodeDecodeError, ValueError):
                username, password = "", ""
            record = self.users.get(username)
            if not record or not verify_password(record, password):
                self._record_metric("auth_failure", 401)
                self._send(401, {"error": "invalid username or password"})
                return
            token = issue_session(username, self.session_secret)
            secure = "; Secure" if self.headers.get("Origin", "").startswith("https://") else ""
            cookie = f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}{secure}"
            self._send(200, {"user": username}, (("Set-Cookie", cookie),))
            return
        if path == "/api/logout":
            self._send(200, {"success": True}, (("Set-Cookie", f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),))
            return
        username = self._require_user()
        if username is None:
            return
        if path == "/api/restore":
            try:
                form = parse_qs(self._read_body().decode("utf-8"), keep_blank_values=True)
                backup = json.loads(form.get("backup", [""])[0])
                if not isinstance(backup, dict) or backup.get("format") != "raft-kv-backup-v1":
                    raise ValueError("unsupported backup format")
                entries = backup.get("entries")
                if not isinstance(entries, dict):
                    raise ValueError("backup entries must be an object")
                if len(entries) > 10_000:
                    raise ValueError("backup contains too many entries")
                if self.users and backup.get("user") != username:
                    self._send(403, {"error": "backup belongs to a different user"})
                    return
                failures = []
                for key, value in entries.items():
                    if not isinstance(key, str) or not key or not isinstance(value, str):
                        failures.append(str(key))
                        continue
                    stored_key = self._storage_key(username, key)
                    response = self.stub.Set(self.pb2.SetRequest(key=stored_key, value=value), timeout=5)
                    if not response.success:
                        failures.append(key)
                self._record_metric("restore", 200 if not failures else 409)
                self._send(200 if not failures else 409, {"restored": len(entries) - len(failures), "failed_keys": failures})
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                self._record_metric("restore", 400)
                self._send(400, {"error": str(error)})
            except grpc.RpcError as error:
                self._record_metric("restore", 502)
                self._send(502, {"error": error.details()})
            return
        if path != "/api/entry":
            self._send(404, {"error": "not found"})
            return
        try:
            body = parse_qs(self._read_body().decode("utf-8"), keep_blank_values=True)
            key = body.get("key", [""])[0].strip()
            if body.get("action", ["set"])[0] == "delete":
                self._delete_entry(username, key)
                return
            value = body.get("value", [""])[0]
            self._set_entry(self._storage_key(username, key), value, key)
        except (UnicodeDecodeError, ValueError) as error:
            self._record_metric("set", 400)
            self._send(400, {"error": str(error)})

    def do_PUT(self):
        if not self._origin_allowed():
            self._send(403, {"error": "origin is not allowed"})
            return
        username = self._require_user()
        if username is None:
            return
        if urlparse(self.path).path != "/api/entry":
            self._send(404, {"error": "not found"})
            return
        try:
            body = self._read_json()
            key, value = str(body.get("key", "")).strip(), str(body.get("value", ""))
            self._set_entry(self._storage_key(username, key), value, key)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            self._record_metric("set", 400)
            self._send(400, {"error": str(error)})

    def _set_entry(self, key, value, display_key):
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
            self._send(200, {"success": True, "key": display_key, "value": value})
        except grpc.RpcError as error:
            self._record_metric("set", 502)
            self._send(502, {"success": False, "error": error.details()})

    def _delete_entry(self, username, key):
        if not key:
            self._record_metric("delete", 400)
            self._send(400, {"error": "key is required"})
            return
        try:
            response = self.stub.Delete(self.pb2.DeleteRequest(key=self._storage_key(username, key)), timeout=5)
            if not response.success:
                self._record_metric("delete", 409)
                self._send(409, {"success": False, "error": "delete was not committed"})
                return
            self._record_metric("delete", 200)
            self._send(200, {"success": True, "key": key})
        except grpc.RpcError as error:
            self._record_metric("delete", 502)
            self._send(502, {"success": False, "error": error.details()})

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if not self._origin_allowed():
            self._send(403, {"error": "origin is not allowed"})
            return
        username = self._require_user()
        if username is None:
            return
        key = parse_qs(parsed.query).get("key", [""])[0]
        if parsed.path != "/api/entry":
            self._send(404, {"error": "not found"})
            return
        self._delete_entry(username, key)

    def log_message(self, format, *args):
        print(f"[gateway] {self.address_string()} - {format % args}")

    def _read_node_status(self, node):
        address, stub = node
        try:
            status = stub.GetClusterStatus(self.pb2.ClusterStatusRequest(), timeout=1)
            return {
                "address": address,
                "node_id": status.node_id,
                "reachable": True,
                "is_leader": status.is_leader,
                "current_term": status.current_term,
                "commit_index": status.commit_index,
            }
        except grpc.RpcError:
            return {"address": address, "node_id": -1, "reachable": False, "is_leader": False}


def main():
    parser = argparse.ArgumentParser(description="HTTP gateway for the Raft KVStore")
    parser.add_argument("--target", default="localhost:50051", help="gRPC node address")
    parser.add_argument("--port", type=int, default=8080, help="HTTP gateway port")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP gateway bind address")
    parser.add_argument("--allowed-origin", action="append", default=[], help="additional dashboard origin allowed to call the gateway")
    parser.add_argument("--users-file", help="private JSON file of PBKDF2 password records; enables per-user key isolation")
    parser.add_argument("--node", action="append", default=[], help="additional cluster node address for topology; may be repeated")
    parser.add_argument("--ca", help="CA certificate for mTLS")
    parser.add_argument("--cert", help="client certificate for mTLS")
    parser.add_argument("--key", help="client private key for mTLS")
    args = parser.parse_args()

    GatewayHandler.users = {}
    GatewayHandler.session_secret = b""
    if args.users_file:
        try:
            GatewayHandler.users = load_users(args.users_file)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            parser.error(str(error))
        secret = os.environ.get("GATEWAY_SESSION_SECRET", "")
        if len(secret.encode("utf-8")) < 32:
            parser.error("GATEWAY_SESSION_SECRET must contain at least 32 bytes when --users-file is enabled")
        GatewayHandler.session_secret = secret.encode("utf-8")

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
    channels = [channel]
    node_addresses = list(dict.fromkeys([args.target, *args.node]))
    node_stubs = [(args.target, GatewayHandler.stub)]
    for address in node_addresses:
        if address == args.target:
            continue
        node_channel = grpc.secure_channel(address, credentials) if all(tls_files) else grpc.insecure_channel(address)
        channels.append(node_channel)
        node_stubs.append((address, pb2_grpc.KVStoreStub(node_channel)))
    server = ThreadingHTTPServer((args.host, args.port), GatewayHandler)
    server.target = args.target
    server.node_stubs = node_stubs
    print(f"KV gateway listening on http://127.0.0.1:{args.port} -> {args.target}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        for open_channel in channels:
            open_channel.close()


if __name__ == "__main__":
    main()
