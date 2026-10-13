import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from dispatch.destinations import DestinationPolicy
from dispatch.retries import RetryPolicy
from dispatch.store import AttemptConflict, EventStore
from dispatch.worker import Worker


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.now = 1000
        self.store = EventStore(self.path, clock=lambda: self.now)
        self.event = self.store.enqueue("key", "https://example.com/hook", {})
        self.policy = RetryPolicy()

    def finish(self, **outcome):
        claim = self.store.claim_next()
        self.store.finish_attempt(claim.attempt_id, retry_policy=self.policy, **outcome)
        return claim

    def test_retry_is_not_claimed_early_and_survives_restart(self):
        self.finish(status_code=503)
        self.assertEqual(self.store.retry_at(self.event.id), 1030)
        self.assertIsNone(self.store.claim_next())
        restarted = EventStore(self.path, clock=lambda: self.now)
        self.now = 1029.9
        self.assertIsNone(restarted.claim_next())
        self.now = 1030
        self.assertEqual(restarted.claim_next().event.id, self.event.id)
        self.assertIsNone(restarted.retry_at(self.event.id))

    def test_backoff_and_attempt_limit_keep_complete_history(self):
        self.finish(error="request_timeout")
        self.now = 1030
        self.finish(status_code=429)
        self.assertEqual(self.store.retry_at(self.event.id), 1090)
        self.now = 1090
        self.finish(status_code=503)
        self.assertEqual(self.store.get(self.event.id).status, "failed")
        self.assertIsNone(self.store.retry_at(self.event.id))
        self.assertIsNone(self.store.claim_next())
        self.assertEqual(len(self.store.attempts(self.event.id)), 3)
        self.assertTrue(all(attempt.finished_at for attempt in self.store.attempts(self.event.id)))

    def test_success_after_retry_clears_schedule(self):
        self.finish(status_code=500)
        self.now = 1030
        self.finish(status_code=204)
        self.assertEqual(self.store.get(self.event.id).status, "delivered")
        self.assertIsNone(self.store.retry_at(self.event.id))
        self.assertEqual(len(self.store.attempts(self.event.id)), 2)

    def test_terminal_failures_are_not_retried(self):
        for outcome in [{"status_code": 400}, {"status_code": 401}, {"status_code": 404}, {"error": "delivery_rejected"}, {"error": "certificate_verification_failed"}, {"error": "invalid_event"}]:
            with self.subTest(outcome=outcome):
                self.assertIsNone(self.policy.next_delay(1, **outcome))

    def test_transient_errors_and_statuses_are_retryable(self):
        for outcome in [{"status_code": 408}, {"status_code": 429}, {"status_code": 503}, {"error": "request_timeout"}, {"error": "delivery_deadline_exceeded"}, {"error": "dns_error"}, {"error": "connection_error"}]:
            with self.subTest(outcome=outcome):
                self.assertEqual(self.policy.next_delay(1, **outcome), 30)

    def test_delay_is_capped_and_configuration_is_validated(self):
        policy = RetryPolicy(max_attempts=1000, base_delay=30, max_delay=60)
        self.assertEqual(policy.next_delay(999, status_code=500), 60)
        self.assertIsNone(policy.next_delay(1000, status_code=500))
        for options in [{"max_attempts": 0}, {"max_attempts": True}, {"base_delay": float("nan")}, {"max_delay": float("inf")}, {"base_delay": -1}, {"max_delay": 1}]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                RetryPolicy(**options)

    def test_duplicate_completion_does_not_change_retry_time(self):
        claim = self.finish(status_code=503)
        self.now = 1010
        with self.assertRaises(AttemptConflict):
            self.store.finish_attempt(claim.attempt_id, status_code=500, retry_policy=self.policy)
        self.assertEqual(self.store.retry_at(self.event.id), 1030)

    def test_retry_schedule_failure_rolls_back_completion(self):
        claim = self.store.claim_next()
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("CREATE TRIGGER reject_retry BEFORE INSERT ON retry_schedule BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.finish_attempt(claim.attempt_id, status_code=503, retry_policy=self.policy)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")
        self.assertIsNone(self.store.attempts(self.event.id)[0].finished_at)
        self.assertIsNone(self.store.retry_at(self.event.id))

    def test_ready_new_event_is_not_blocked_by_scheduled_retry(self):
        self.finish(status_code=503)
        new = self.store.enqueue("new", "https://example.com/hook", {})
        self.assertEqual(self.store.claim_next().event.id, new.id)

    def test_worker_schedules_retry_and_preserves_event_identity(self):
        seen = []
        def execute(event, policy, timeout, deadline):
            seen.append(event.id)
            return {"status_code": 503 if len(seen) == 1 else 204}
        worker = Worker(self.store, DestinationPolicy(["https://example.com"]), executor=execute)
        self.assertTrue(worker.process_next())
        self.assertFalse(worker.process_next())
        self.now = 1030
        self.assertTrue(worker.process_next())
        self.assertEqual(seen, [self.event.id, self.event.id])
        self.assertEqual(self.store.get(self.event.id).status, "delivered")


if __name__ == "__main__":
    unittest.main()
