import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from dispatch.api import create_server
from dispatch.retries import RetryPolicy
from dispatch.store import EventStore


class DeliveryStatusApiTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.now = 1000
        self.store = EventStore(self.path, clock=lambda: self.now)
        self.event = self.store.enqueue("private-key", "https://example.com/private", {"secret": "private-payload"})
        self.server = create_server(self.store, 0)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.05), daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def get(self, path=None):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            connection.request("GET", path or f"/events/{self.event.id}/delivery")
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_pending_state_omits_payload_destination_and_idempotency_key(self):
        status, state = self.get()
        self.assertEqual(status, 200)
        self.assertEqual(state, {"event_id": self.event.id, "status": "pending", "retry_at": None, "attempt_count": 0, "attempts_truncated": False, "attempts": []})
        self.assertNotIn("private", json.dumps(state))

    def test_active_attempt_includes_persisted_lease_expiry(self):
        claim = self.store.claim_next(lease_seconds=45)
        status, state = self.get()
        self.assertEqual(status, 200)
        self.assertEqual(state["status"], "delivering")
        self.assertEqual(state["attempt_count"], 1)
        self.assertEqual(state["attempts"][0]["id"], claim.attempt_id)
        self.assertEqual(state["attempts"][0]["lease_expires_at"], 1045)
        self.assertIsNone(state["attempts"][0]["finished_at"])

    def test_retry_and_success_show_complete_ordered_outcomes(self):
        first = self.store.claim_next(lease_seconds=45)
        self.store.finish_attempt(first.attempt_id, status_code=503, retry_policy=RetryPolicy())
        status, state = self.get()
        self.assertEqual(status, 200)
        self.assertEqual(state["status"], "pending")
        self.assertEqual(state["retry_at"], 1030)
        self.assertEqual(state["attempts"][0]["status_code"], 503)
        self.assertIsNone(state["attempts"][0]["lease_expires_at"])
        self.now = 1030
        second = self.store.claim_next(lease_seconds=45)
        self.store.finish_attempt(second.attempt_id, status_code=204)
        _, state = self.get()
        self.assertEqual(state["status"], "delivered")
        self.assertIsNone(state["retry_at"])
        self.assertEqual([attempt["id"] for attempt in state["attempts"]], [first.attempt_id, second.attempt_id])
        self.assertEqual([attempt["status_code"] for attempt in state["attempts"]], [503, 204])

    def test_read_does_not_recover_expired_attempt(self):
        self.store.claim_next(lease_seconds=45)
        self.now = 1045
        _, before = self.get()
        self.assertEqual(before["status"], "delivering")
        self.assertIsNone(before["attempts"][0]["finished_at"])
        self.store.recover_expired(retry_policy=RetryPolicy())
        _, after = self.get()
        self.assertEqual(after["status"], "pending")
        self.assertEqual(after["retry_at"], 1075)
        self.assertEqual(after["attempts"][0]["error"], "lease_expired")
        self.assertIsNone(after["attempts"][0]["lease_expires_at"])

    def test_large_history_returns_latest_hundred_with_total(self):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executemany("INSERT INTO attempts (id, event_id, finished_at, status_code) VALUES (?, ?, CURRENT_TIMESTAMP, 503)", [(str(index), self.event.id) for index in range(102)])
            connection.commit()
        _, state = self.get()
        self.assertEqual(state["attempt_count"], 102)
        self.assertTrue(state["attempts_truncated"])
        self.assertEqual(len(state["attempts"]), 100)
        self.assertEqual(state["attempts"][0]["id"], "2")
        self.assertEqual(state["attempts"][-1]["id"], "101")

    def test_unknown_and_malformed_delivery_routes_return_not_found(self):
        for path in ["/events/missing/delivery", "/events//delivery", f"/events/{self.event.id}/delivery/", f"/events/{self.event.id}/delivery?limit=1"]:
            with self.subTest(path=path):
                self.assertEqual(self.get(path), (404, {"error": "not_found"}))

    def test_storage_error_does_not_expose_internal_path(self):
        with patch.object(self.store, "delivery_state", side_effect=sqlite3.OperationalError("private database path")):
            self.assertEqual(self.get(), (503, {"error": "storage_unavailable"}))


if __name__ == "__main__":
    unittest.main()
