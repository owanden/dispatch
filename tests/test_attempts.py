import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from dispatch.store import AttemptConflict, EventStore


class AttemptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.store = EventStore(self.path)
        self.event = self.store.enqueue("order-1", "https://example.com/hook", {"order": 1})

    def test_claim_and_completion_survive_restart(self):
        claim = self.store.claim_next()
        self.assertEqual(claim.event.id, self.event.id)
        self.assertEqual(claim.event.status, "delivering")
        reopened = EventStore(self.path)
        attempt = reopened.attempts(self.event.id)[0]
        self.assertEqual(attempt.id, claim.attempt_id)
        self.assertIsNone(attempt.finished_at)
        self.assertIsNone(reopened.claim_next())
        reopened.finish_attempt(claim.attempt_id, status_code=204)
        final = EventStore(self.path)
        self.assertEqual(final.get(self.event.id).status, "delivered")
        self.assertEqual(final.attempts(self.event.id)[0].status_code, 204)
        self.assertIsNotNone(final.attempts(self.event.id)[0].finished_at)

    def test_concurrent_claims_create_one_attempt(self):
        with ThreadPoolExecutor(max_workers=8) as executor:
            claims = list(executor.map(lambda _: EventStore(self.path).claim_next(), range(24)))
        self.assertEqual(len([claim for claim in claims if claim is not None]), 1)
        self.assertEqual(len(self.store.attempts(self.event.id)), 1)

    def test_claims_pending_events_in_insertion_order(self):
        second = self.store.enqueue("order-2", "https://example.com/hook", {})
        self.assertEqual(self.store.claim_next().event.id, self.event.id)
        self.assertEqual(self.store.claim_next().event.id, second.id)
        self.assertIsNone(self.store.claim_next())

    def test_failed_outcomes_are_recorded(self):
        claim = self.store.claim_next()
        self.store.finish_attempt(claim.attempt_id, error="request_timeout")
        attempt = self.store.attempts(self.event.id)[0]
        self.assertEqual(attempt.error, "request_timeout")
        self.assertIsNone(attempt.status_code)
        self.assertEqual(self.store.get(self.event.id).status, "failed")
        self.assertIsNone(self.store.claim_next())

    def test_http_failure_retains_status(self):
        claim = self.store.claim_next()
        self.store.finish_attempt(claim.attempt_id, status_code=503)
        self.assertEqual(self.store.get(self.event.id).status, "failed")
        self.assertEqual(self.store.attempts(self.event.id)[0].status_code, 503)

    def test_duplicate_and_unknown_completion_cannot_overwrite_outcome(self):
        claim = self.store.claim_next()
        self.store.finish_attempt(claim.attempt_id, status_code=200)
        for attempt_id in [claim.attempt_id, "missing"]:
            with self.subTest(attempt_id=attempt_id), self.assertRaises(AttemptConflict):
                self.store.finish_attempt(attempt_id, status_code=500)
        self.assertEqual(self.store.get(self.event.id).status, "delivered")
        self.assertEqual(self.store.attempts(self.event.id)[0].status_code, 200)

    def test_invalid_outcome_does_not_finish_attempt(self):
        claim = self.store.claim_next()
        for outcome in [{}, {"status_code": 200, "error": "timeout"}, {"status_code": True}, {"status_code": 999}, {"error": ""}, {"error": "x" * 257}]:
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                self.store.finish_attempt(claim.attempt_id, **outcome)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")
        self.assertIsNone(self.store.attempts(self.event.id)[0].finished_at)

    def test_claim_failure_rolls_back_event_transition(self):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("CREATE TRIGGER reject_attempt BEFORE INSERT ON attempts BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.claim_next()
        self.assertEqual(self.store.get(self.event.id).status, "pending")
        self.assertEqual(self.store.attempts(self.event.id), [])

    def test_completion_failure_rolls_back_attempt_outcome(self):
        claim = self.store.claim_next()
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("CREATE TRIGGER reject_completion BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.finish_attempt(claim.attempt_id, status_code=204)
        self.assertIsNone(self.store.attempts(self.event.id)[0].finished_at)
        self.assertIsNone(self.store.attempts(self.event.id)[0].status_code)
        self.assertEqual(self.store.get(self.event.id).status, "delivering")

    def test_existing_events_database_is_preserved(self):
        path = Path(self.directory.name) / "legacy.db"
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("CREATE TABLE events (id TEXT PRIMARY KEY, key TEXT UNIQUE NOT NULL, destination TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
            connection.execute("INSERT INTO events (id, key, destination, payload) VALUES ('old', 'old-key', 'https://example.com', '{}')")
            connection.commit()
        upgraded = EventStore(path)
        self.assertEqual(upgraded.get("old").payload, {})
        self.assertEqual(upgraded.claim_next().event.id, "old")


if __name__ == "__main__":
    unittest.main()
