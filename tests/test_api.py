import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from dispatch.api import MAX_BODY, create_server
from dispatch.store import EventStore


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = EventStore(Path(self.directory.name) / "events.db")
        self.server = create_server(self.store, 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request(self, method="POST", path="/events", body=None, headers=None):
        if body is None:
            body = json.dumps({"destination": "https://example.com/hook", "payload": {"order": 1}})
        if headers is None:
            headers = {"Content-Type": "application/json", "Idempotency-Key": "order-1"}
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=5)
        try:
            connection.request(method, path, body, headers)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_enqueue_and_lookup(self):
        status, event = self.request()
        self.assertEqual(status, 202)
        self.assertEqual(event["status"], "pending")
        self.assertEqual(self.request("GET", "/events/" + event["id"]), (200, event))
        self.assertEqual(self.store.get(event["id"]).payload, {"order": 1})

    def test_duplicate_and_conflict(self):
        original = self.request()
        self.assertEqual(self.request(), original)
        self.assertEqual(self.request(body=json.dumps({"destination": "https://example.com/hook", "payload": {"order": 2}}))[0], 409)

    def test_unknown_routes_and_events(self):
        for method, path in [("GET", "/"), ("GET", "/events/missing"), ("POST", "/other")]:
            with self.subTest(method=method, path=path):
                self.assertEqual(self.request(method, path)[0], 404)

    def test_bad_json_and_event_shapes(self):
        for body in ["{", "[]", "null", "{}", '{"destination":"https://example.com","payload":[]}', '{"destination":"https://example.com","payload":{"x":NaN}}']:
            with self.subTest(body=body):
                self.assertEqual(self.request(body=body)[0], 400)
        self.assertEqual(self.request()[0], 202)

    def test_invalid_destination(self):
        for destination in ["http://example.com", "https://", "https://user:pass@example.com", "https://example.com/#secret", "https://example.com:99999", "https://exam ple.com", 123]:
            with self.subTest(destination=destination):
                body = json.dumps({"destination": destination, "payload": {}})
                self.assertEqual(self.request(body=body)[0], 400)

    def test_missing_or_oversized_key(self):
        for headers in [{"Content-Type": "application/json"}, {"Content-Type": "application/json", "Idempotency-Key": "x" * 257}]:
            with self.subTest(headers=headers):
                self.assertEqual(self.request(headers=headers)[0], 400)

    def test_body_size_limit(self):
        self.assertEqual(self.request(body="x" * (MAX_BODY + 1))[0], 413)

    def test_content_type_required(self):
        self.assertEqual(self.request(headers={"Idempotency-Key": "order-1", "Content-Type": "text/plain"})[0], 415)

    def test_invalid_length_and_transfer_encoding(self):
        for extra in [{"Content-Length": "-1"}, {"Content-Length": "bad"}, {"Transfer-Encoding": "chunked"}]:
            headers = {"Content-Type": "application/json", "Idempotency-Key": "order-1", **extra}
            with self.subTest(extra=extra):
                self.assertEqual(self.request(headers=headers)[0], 400)

    def test_missing_and_duplicate_lengths(self):
        for lengths, expected in [([], 411), (["1", "1"], 400), (["9" * 5000], 413)]:
            with self.subTest(lengths=len(lengths)):
                connection = http.client.HTTPConnection(*self.server.server_address, timeout=5)
                try:
                    connection.putrequest("POST", "/events")
                    connection.putheader("Content-Type", "application/json")
                    connection.putheader("Idempotency-Key", "order-1")
                    for length in lengths:
                        connection.putheader("Content-Length", length)
                    connection.endheaders()
                    response = connection.getresponse()
                    self.assertEqual(response.status, expected)
                    response.read()
                finally:
                    connection.close()

    def test_storage_failure_returns_unavailable(self):
        with patch.object(self.store, "enqueue", side_effect=sqlite3.OperationalError("private database path")):
            self.assertEqual(self.request(), (503, {"error": "storage_unavailable"}))
        with patch.object(self.store, "get", side_effect=sqlite3.OperationalError("private database path")):
            self.assertEqual(self.request("GET", "/events/1"), (503, {"error": "storage_unavailable"}))


if __name__ == "__main__":
    unittest.main()
