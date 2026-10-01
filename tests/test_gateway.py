import http.cookiejar
import json
import os
import threading
import tempfile
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import urlencode

import gateway


class Message:
    def __init__(self, **values):
        self.__dict__.update(values)


class FakeProto:
    HeartbeatRequest = Message
    ClusterStatusRequest = Message
    ListKeysRequest = Message
    GetRequest = Message
    SetRequest = Message
    DeleteRequest = Message


class FakeStub:
    def __init__(self, node_id=1, leader=False, store=None):
        self.node_id = node_id
        self.leader = leader
        self.store = store if store is not None else {}

    def Heartbeat(self, request, timeout):
        return Message(alive=True)

    def GetClusterStatus(self, request, timeout):
        return Message(node_id=self.node_id, is_leader=self.leader, current_term=4, commit_index=12, leader_id=self.node_id if self.leader else -1)

    def ListKeys(self, request, timeout):
        return Message(keys=sorted(self.store))

    def Get(self, request, timeout):
        return Message(found=request.key in self.store, value=self.store.get(request.key, ""))

    def Set(self, request, timeout):
        self.store[request.key] = request.value
        return Message(success=True)

    def Delete(self, request, timeout):
        return Message(success=self.store.pop(request.key, None) is not None)


class GatewayHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._originals = {
            "users": gateway.GatewayHandler.users,
            "secret": gateway.GatewayHandler.session_secret,
            "origins": gateway.GatewayHandler.allowed_origins,
            "pb2": gateway.GatewayHandler.pb2,
            "stub": gateway.GatewayHandler.stub,
        }
        gateway.GatewayHandler.users = {
            "alice": gateway.create_password_record("alice-password-123"),
            "bob": gateway.create_password_record("bob-password-456"),
        }
        gateway.GatewayHandler.session_secret = b"test-session-secret-with-more-than-32-bytes"
        gateway.GatewayHandler.allowed_origins = {"http://127.0.0.1:4173"}
        gateway.GatewayHandler.pb2 = FakeProto
        cls.store = {}
        gateway.GatewayHandler.stub = FakeStub(store=cls.store)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), gateway.GatewayHandler)
        cls.server.target = "test-node"
        cls.server.node_stubs = [
            ("node-a:5001", FakeStub(node_id=5001, leader=True)),
            ("node-b:5002", FakeStub(node_id=5002)),
            ("node-c:5003", FakeStub(node_id=5003)),
        ]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join()
        cls.server.server_close()
        gateway.GatewayHandler.users = cls._originals["users"]
        gateway.GatewayHandler.session_secret = cls._originals["secret"]
        gateway.GatewayHandler.allowed_origins = cls._originals["origins"]
        gateway.GatewayHandler.pb2 = cls._originals["pb2"]
        gateway.GatewayHandler.stub = cls._originals["stub"]

    def setUp(self):
        self.store.clear()

    def test_dashboard_origin_uses_configured_ui_port(self):
        environment = {
            "UI_PORT": "19173",
            "CODESPACE_NAME": "example-space",
            "GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN": "app.github.dev",
        }
        with patch.dict(gateway.os.environ, environment):
            self.assertEqual(gateway.dashboard_origins(), {
                "http://localhost:19173",
                "http://127.0.0.1:19173",
                "https://example-space-19173.app.github.dev",
            })

    def test_users_file_must_be_private(self):
        with tempfile.TemporaryDirectory() as directory:
            path = gateway.Path(directory) / "users.json"
            path.write_text(json.dumps({"alice": gateway.create_password_record("password-123456")}), encoding="utf-8")
            os.chmod(path, 0o644)
            if os.name == "posix":
                with self.assertRaisesRegex(ValueError, "must be private"):
                    gateway.load_users(path)
            os.chmod(path, 0o600)
            self.assertIn("alice", gateway.load_users(path))

    def client(self):
        cookies = http.cookiejar.CookieJar()
        return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))

    def request(self, opener, path, data=None, method=None, origin="http://127.0.0.1:4173"):
        body = urlencode(data).encode("utf-8") if isinstance(data, dict) else data
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            headers={"Origin": origin} if origin else {},
            method=method,
        )
        try:
            with opener.open(request) as response:
                payload = response.read()
                return response.status, response.headers, payload
        except urllib.error.HTTPError as error:
            try:
                return error.code, error.headers, error.read()
            finally:
                error.close()

    def login(self, opener, username, password):
        return self.request(opener, "/api/login", {"username": username, "password": password})

    def test_authentication_and_per_user_key_isolation(self):
        anonymous = self.client()
        status, _, body = self.request(anonymous, "/api/keys")
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body)["error"], "authentication required")

        alice = self.client()
        self.assertEqual(self.login(alice, "alice", "wrong-password")[0], 401)
        self.assertEqual(self.login(alice, "alice", "alice-password-123")[0], 200)
        self.assertEqual(self.request(alice, "/api/entry", {"key": "shared", "value": "alice-value"}, "POST")[0], 200)

        bob = self.client()
        self.assertEqual(self.login(bob, "bob", "bob-password-456")[0], 200)
        status, _, body = self.request(bob, "/api/keys")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["keys"], [])
        status, _, body = self.request(bob, "/api/entry?key=shared")
        self.assertEqual(status, 200)
        self.assertFalse(json.loads(body)["found"])
        status, _, _ = self.request(bob, "/api/entry", {"action": "delete", "key": "shared"}, "POST")
        self.assertEqual(status, 409)
        self.assertIn("user/alice/shared", self.store)
        self.assertNotIn("user/bob/shared", self.store)

        self.assertEqual(self.request(alice, "/api/logout", {}, "POST")[0], 200)
        self.assertEqual(self.request(alice, "/api/keys")[0], 401)

    def test_backup_restore_and_live_node_status(self):
        alice = self.client()
        self.assertEqual(self.login(alice, "alice", "alice-password-123")[0], 200)
        self.assertEqual(self.request(alice, "/api/entry", {"key": "persist", "value": "value"}, "POST")[0], 200)

        status, _, body = self.request(alice, "/api/backup")
        self.assertEqual(status, 200)
        backup = json.loads(body)
        self.assertEqual(backup["format"], "raft-kv-backup-v1")
        self.assertEqual(backup["entries"], {"persist": "value"})

        bob = self.client()
        self.assertEqual(self.login(bob, "bob", "bob-password-456")[0], 200)
        self.assertEqual(self.request(bob, "/api/restore", {"backup": json.dumps(backup)}, "POST")[0], 403)

        self.assertEqual(self.request(alice, "/api/entry", {"action": "delete", "key": "persist"}, "POST")[0], 200)
        status, _, body = self.request(alice, "/api/restore", {"backup": json.dumps(backup)}, "POST")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["restored"], 1)
        self.assertEqual(self.store["user/alice/persist"], "value")

        status, _, body = self.request(alice, "/api/nodes")
        self.assertEqual(status, 200)
        nodes = json.loads(body)["nodes"]
        self.assertEqual(len(nodes), 3)
        self.assertTrue(next(node for node in nodes if node["node_id"] == 5001)["is_leader"])
        self.assertTrue(all(node["reachable"] for node in nodes))

    def test_mutation_rejects_untrusted_origin(self):
        client = self.client()
        self.login(client, "alice", "alice-password-123")
        status, _, _ = self.request(client, "/api/entry", {"key": "blocked", "value": "no"}, "POST", "https://untrusted.example")
        self.assertEqual(status, 403)
        self.assertEqual(self.store, {})


if __name__ == "__main__":
    unittest.main()
