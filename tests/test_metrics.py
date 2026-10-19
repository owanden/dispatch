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


class QueueMetricsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.now = 1700000000
        self.store = EventStore(self.path, clock=lambda: self.now)
        self.server = create_server(self.store, 0)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.05), daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def get(self, path="/metrics"):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def enqueue(self, key):
        return self.store.enqueue(key, "https://example.com/private", {"secret": "private-payload"})

    def test_empty_queue_reports_zero_counts_and_no_age(self):
        self.assertEqual(self.get(), (200, {
            "captured_at": self.now, "events": {"pending": 0, "delivering": 0, "delivered": 0, "failed": 0},
            "ready_events": 0, "scheduled_retries": 0, "unfinished_attempts": 0,
            "expired_leases": 0, "recovered_attempts": 0, "oldest_ready_age_seconds": None,
        }))

    def test_counts_distinguish_ready_scheduled_active_and_terminal_events(self):
        for key, status in [("delivered", 204), ("failed", 400), ("retry", 503)]:
            self.enqueue(key)
            claim = self.store.claim_next(lease_seconds=45)
            self.store.finish_attempt(claim.attempt_id, status_code=status, retry_policy=RetryPolicy())
        self.enqueue("active")
        self.store.claim_next(lease_seconds=45)
        self.enqueue("ready")
        status, metrics = self.get()
        self.assertEqual(status, 200)
        self.assertEqual(metrics["events"], {"pending": 2, "delivering": 1, "delivered": 1, "failed": 1})
        self.assertEqual(metrics["ready_events"], 1)
        self.assertEqual(metrics["scheduled_retries"], 1)
        self.assertEqual(metrics["unfinished_attempts"], 1)
        self.assertEqual(metrics["expired_leases"], 0)

    def test_retry_readiness_changes_at_due_time(self):
        self.enqueue("retry")
        claim = self.store.claim_next()
        self.store.finish_attempt(claim.attempt_id, status_code=503, retry_policy=RetryPolicy())
        self.now += 29.9
        self.assertEqual(self.get()[1]["scheduled_retries"], 1)
        self.now += 0.1
        metrics = self.get()[1]
        self.assertEqual(metrics["ready_events"], 1)
        self.assertEqual(metrics["scheduled_retries"], 0)

    def test_expired_metrics_are_read_only_and_recovery_count_is_historical(self):
        event = self.enqueue("active")
        self.store.claim_next(lease_seconds=45)
        self.now += 45
        metrics = self.get()[1]
        self.assertEqual(metrics["expired_leases"], 1)
        self.assertEqual(metrics["recovered_attempts"], 0)
        self.assertEqual(self.store.get(event.id).status, "delivering")
        self.store.recover_expired(retry_policy=RetryPolicy())
        metrics = self.get()[1]
        self.assertEqual(metrics["expired_leases"], 0)
        self.assertEqual(metrics["recovered_attempts"], 1)
        self.assertEqual(metrics["unfinished_attempts"], 0)
        self.assertEqual(metrics["scheduled_retries"], 1)

    def test_oldest_ready_age_uses_creation_time_and_is_nonnegative(self):
        event = self.enqueue("ready")
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE events SET created_at = datetime(?, 'unixepoch') WHERE id = ?", (self.now - 120, event.id))
            connection.commit()
        self.assertEqual(self.get()[1]["oldest_ready_age_seconds"], 120)
        self.now -= 121
        self.assertEqual(self.get()[1]["oldest_ready_age_seconds"], 0)

    def test_metrics_omit_event_identifiers_and_contents(self):
        event = self.enqueue("private-key")
        encoded = json.dumps(self.get()[1])
        self.assertNotIn(event.id, encoded)
        self.assertNotIn("private", encoded)
        self.assertNotIn("example.com", encoded)

    def test_storage_error_and_unknown_metric_routes(self):
        with patch.object(self.store, "queue_metrics", side_effect=sqlite3.OperationalError("private database path")):
            self.assertEqual(self.get(), (503, {"error": "storage_unavailable"}))
        for path in ["/metrics/", "/metrics?format=text"]:
            with self.subTest(path=path):
                self.assertEqual(self.get(path), (404, {"error": "not_found"}))


if __name__ == "__main__":
    unittest.main()
