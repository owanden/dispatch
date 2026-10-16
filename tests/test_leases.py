import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from dispatch.destinations import DestinationPolicy
from dispatch.retries import RetryPolicy
from dispatch.store import AttemptConflict, EventStore
from dispatch.worker import Worker


class LeaseRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.now = 1000
        self.store = EventStore(self.path, clock=lambda: self.now)
        self.event = self.store.enqueue("key", "https://example.com/hook", {})
        self.policy = RetryPolicy()

    def test_live_claim_is_not_recovered_before_expiry(self):
        claim = self.store.claim_next(lease_seconds=45)
        self.now = 1044.9
        self.assertEqual(self.store.recover_expired(retry_policy=self.policy), 0)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")
        self.store.finish_attempt(claim.attempt_id, status_code=204)
        self.now = 1100
        self.assertEqual(self.store.recover_expired(retry_policy=self.policy), 0)
        self.assertEqual(self.store.get(self.event.id).status, "delivered")

    def test_expiry_survives_restart_and_respects_backoff(self):
        self.store.claim_next(lease_seconds=45)
        self.now = 1045
        restarted = EventStore(self.path, clock=lambda: self.now)
        self.assertEqual(restarted.recover_expired(retry_policy=self.policy), 1)
        self.assertEqual(restarted.attempts(self.event.id)[0].error, "lease_expired")
        self.assertEqual(restarted.retry_at(self.event.id), 1075)
        self.assertIsNone(restarted.claim_next())
        self.now = 1075
        self.assertEqual(restarted.claim_next(lease_seconds=45).event.id, self.event.id)

    def test_stale_completion_cannot_overwrite_new_attempt(self):
        old = self.store.claim_next(lease_seconds=45)
        self.now = 1045
        self.store.recover_expired(retry_policy=self.policy)
        self.now = 1075
        new = self.store.claim_next(lease_seconds=45)
        with self.assertRaises(AttemptConflict):
            self.store.finish_attempt(old.attempt_id, status_code=204)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")
        self.store.finish_attempt(new.attempt_id, status_code=503)
        self.assertEqual(self.store.get(self.event.id).status, "failed")
        self.assertEqual([attempt.error for attempt in self.store.attempts(self.event.id)], ["lease_expired", None])

    def test_recovery_respects_total_attempt_limit(self):
        self.store.claim_next(lease_seconds=45)
        self.now = 1045
        self.store.recover_expired(retry_policy=RetryPolicy(max_attempts=1))
        self.assertEqual(self.store.get(self.event.id).status, "failed")
        self.assertIsNone(self.store.retry_at(self.event.id))
        self.assertIsNone(self.store.claim_next())

    def test_concurrent_recovery_finishes_expired_attempt_once(self):
        self.store.claim_next(lease_seconds=45)
        self.now = 1045
        def recover(_):
            return EventStore(self.path, clock=lambda: self.now).recover_expired(retry_policy=self.policy)
        with ThreadPoolExecutor(max_workers=8) as executor:
            self.assertEqual(sum(executor.map(recover, range(24))), 1)
        self.assertEqual(len(self.store.attempts(self.event.id)), 1)
        self.assertEqual(self.store.retry_at(self.event.id), 1075)

    def test_recovery_storage_failure_rolls_back_and_remains_recoverable(self):
        self.store.claim_next(lease_seconds=45)
        self.now = 1045
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("CREATE TRIGGER reject_recovery BEFORE INSERT ON retry_schedule BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.recover_expired(retry_policy=self.policy)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")
        self.assertIsNone(self.store.attempts(self.event.id)[0].finished_at)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("DROP TRIGGER reject_recovery")
        self.assertEqual(self.store.recover_expired(retry_policy=self.policy), 1)

    def test_unleased_legacy_claim_is_not_recovered_implicitly(self):
        self.store.claim_next()
        self.now = 9999
        self.assertEqual(self.store.recover_expired(retry_policy=self.policy), 0)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")

    def test_worker_recovers_then_delivers_when_retry_is_due(self):
        self.store.claim_next(lease_seconds=45)
        worker = Worker(self.store, DestinationPolicy(["https://example.com"]), executor=lambda *args, **kwargs: {"status_code": 204})
        self.now = 1045
        self.assertFalse(worker.process_next())
        self.now = 1075
        self.assertTrue(worker.process_next())
        self.assertEqual(self.store.get(self.event.id).status, "delivered")
        self.assertEqual(len(self.store.attempts(self.event.id)), 2)

    def test_worker_lease_exceeds_delivery_deadline(self):
        worker = Worker(self.store, DestinationPolicy(), deadline=20)
        with patch.object(self.store, "claim_next", return_value=None) as claim:
            self.assertFalse(worker.process_next())
            claim.assert_called_once_with(lease_seconds=50)

    def test_invalid_lease_does_not_claim_event(self):
        for lease in [0, -1, float("nan"), float("inf"), True, "45"]:
            with self.subTest(lease=lease), self.assertRaises(ValueError):
                self.store.claim_next(lease_seconds=lease)
        self.assertEqual(self.store.get(self.event.id).status, "pending")
        self.assertEqual(self.store.attempts(self.event.id), [])


if __name__ == "__main__":
    unittest.main()
