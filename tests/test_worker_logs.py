import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.worker import Worker


class WorkerLogTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.now = 1000
        self.store = EventStore(self.path, clock=lambda: self.now)
        self.event = self.store.enqueue("private-key", "https://example.com/private", {"secret": "private-payload"})
        self.policy = DestinationPolicy(["https://example.com"])

    def worker(self, outcome):
        return Worker(self.store, self.policy, executor=lambda *args, **kwargs: outcome)

    def messages(self, logs):
        return [json.loads(record.getMessage()) for record in logs.records]

    def test_completed_attempt_logs_persisted_outcome_and_duration_without_contents(self):
        with self.assertLogs("dispatch.worker", level="INFO") as logs:
            self.assertTrue(self.worker({"status_code": 204}).process_next())
        message, = self.messages(logs)
        self.assertEqual(message["type"], "attempt_finished")
        self.assertEqual(message["event_id"], self.event.id)
        self.assertEqual(message["attempt_id"], self.store.attempts(self.event.id)[0].id)
        self.assertEqual(message["outcome"], {"status_code": 204})
        self.assertGreaterEqual(message["elapsed_seconds"], 0)
        self.assertIsInstance(message["timestamp"], (int, float))
        self.assertEqual(self.store.get(self.event.id).status, "delivered")
        self.assertNotIn("private", json.dumps(message))
        self.assertNotIn("example.com", json.dumps(message))

    def test_finished_attempt_is_distinct_from_final_delivery_status(self):
        with self.assertLogs("dispatch.worker", level="INFO") as logs:
            self.worker({"status_code": 503}).process_next()
        self.assertEqual(self.messages(logs)[0]["outcome"], {"status_code": 503})
        self.assertEqual(self.store.get(self.event.id).status, "pending")
        self.assertEqual(self.store.retry_at(self.event.id), 1030)

    def test_classified_transport_failure_is_logged_as_completed_attempt(self):
        with self.assertLogs("dispatch.worker", level="INFO") as logs:
            self.worker({"error": "request_timeout"}).process_next()
        self.assertEqual(self.messages(logs)[0]["outcome"], {"error": "request_timeout"})
        self.assertIsNotNone(self.store.attempts(self.event.id)[0].finished_at)

    def test_failed_outcome_storage_does_not_log_success_or_exception_text(self):
        with self.assertLogs("dispatch.worker", level="INFO") as logs, patch.object(self.store, "finish_attempt", side_effect=sqlite3.OperationalError("private path")):
            with self.assertRaises(sqlite3.OperationalError):
                self.worker({"status_code": 204}).process_next()
        message, = self.messages(logs)
        self.assertEqual(message["type"], "attempt_completion_failed")
        self.assertEqual(message["exception_type"], "OperationalError")
        self.assertNotIn("private", json.dumps(message))
        self.assertIsNone(self.store.attempts(self.event.id)[0].finished_at)

    def test_unexpected_execution_failure_remains_unfinished(self):
        def fail(*args, **kwargs):
            raise RuntimeError("private destination")
        worker = Worker(self.store, self.policy, executor=fail)
        with self.assertLogs("dispatch.worker", level="INFO") as logs:
            with self.assertRaises(RuntimeError):
                worker.process_next()
        message, = self.messages(logs)
        self.assertEqual(message["type"], "attempt_execution_failed")
        self.assertEqual(message["exception_type"], "RuntimeError")
        self.assertNotIn("private", json.dumps(message))
        self.assertIsNone(self.store.attempts(self.event.id)[0].finished_at)

    def test_recovery_logs_committed_count_without_claiming_future_retry(self):
        self.store.claim_next(lease_seconds=45)
        self.now = 1045
        with self.assertLogs("dispatch.worker", level="INFO") as logs:
            self.assertFalse(self.worker({"status_code": 204}).process_next())
        message, = self.messages(logs)
        self.assertEqual(message["type"], "claims_recovered")
        self.assertEqual(message["count"], 1)
        self.assertEqual(self.store.attempts(self.event.id)[0].error, "lease_expired")

    def test_cli_keeps_json_logs_on_stderr_and_summary_on_stdout(self):
        result = subprocess.run([
            sys.executable, "-m", "dispatch.worker", "--db", str(self.path),
            "--allow-origin", "https://other.example",
        ], capture_output=True, text=True, check=True, timeout=10)
        self.assertEqual(json.loads(result.stdout), {"processed": True})
        message = json.loads(result.stderr)
        self.assertEqual(message["type"], "attempt_finished")
        self.assertEqual(message["outcome"], {"error": "delivery_rejected"})
        self.assertNotIn("private", result.stderr)
        self.assertEqual(self.store.get(self.event.id).status, "failed")


if __name__ == "__main__":
    unittest.main()
