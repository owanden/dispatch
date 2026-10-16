import signal
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest.mock import patch

from dispatch.destinations import DestinationPolicy
from dispatch.retries import RetryPolicy
from dispatch.store import EventStore
from dispatch.worker import Worker, shutdown_signals


class ContinuousWorkerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = EventStore(Path(self.directory.name) / "events.db")
        self.policy = DestinationPolicy(["https://example.com"])
        self.stop = Event()

    def enqueue(self, key):
        return self.store.enqueue(key, "https://example.com/hook", {})

    def test_drains_ready_events_and_stops_after_active_attempt(self):
        first = self.enqueue("first")
        second = self.enqueue("second")
        third = self.enqueue("third")
        delivered = []
        def execute(event, policy, timeout, deadline):
            delivered.append(event.id)
            if event.id == second.id:
                self.stop.set()
            return {"status_code": 204}
        worker = Worker(self.store, self.policy, executor=execute)
        self.assertEqual(worker.run(self.stop), 2)
        self.assertEqual(delivered, [first.id, second.id])
        self.assertEqual(self.store.get(second.id).status, "delivered")
        self.assertIsNotNone(self.store.attempts(second.id)[0].finished_at)
        self.assertEqual(self.store.get(third.id).status, "pending")

    def test_idle_shutdown_wakes_without_waiting_for_poll_interval(self):
        entered = Event()
        def empty(**kwargs):
            entered.set()
            return None
        worker = Worker(self.store, self.policy)
        with patch.object(self.store, "claim_next", side_effect=empty), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(worker.run, self.stop, 30)
            try:
                self.assertTrue(entered.wait(2))
                self.stop.set()
                self.assertEqual(future.result(timeout=2), 0)
            finally:
                self.stop.set()

    def test_polls_and_processes_due_retry_without_manual_invocation(self):
        event = self.enqueue("retry")
        outcomes = []
        def execute(event, policy, timeout, deadline):
            outcomes.append(event.id)
            if len(outcomes) == 2:
                self.stop.set()
                return {"status_code": 204}
            return {"status_code": 503}
        worker = Worker(self.store, self.policy, executor=execute, retry_policy=RetryPolicy(base_delay=0.05, max_delay=0.05))
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(worker.run, self.stop, 0.01)
            try:
                self.assertEqual(future.result(timeout=3), 2)
            finally:
                self.stop.set()
        self.assertEqual(outcomes, [event.id, event.id])
        self.assertEqual(self.store.get(event.id).status, "delivered")
        self.assertEqual(len(self.store.attempts(event.id)), 2)

    def test_preexisting_shutdown_does_not_claim(self):
        event = self.enqueue("pending")
        self.stop.set()
        self.assertEqual(Worker(self.store, self.policy).run(self.stop), 0)
        self.assertEqual(self.store.get(event.id).status, "pending")
        self.assertEqual(self.store.attempts(event.id), [])

    def test_invalid_poll_interval_does_not_claim(self):
        event = self.enqueue("pending")
        worker = Worker(self.store, self.policy)
        for interval in [0, -1, float("nan"), float("inf"), True, "1"]:
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                worker.run(self.stop, interval)
        self.assertEqual(self.store.get(event.id).status, "pending")

    def test_storage_failure_exits_loop(self):
        with patch.object(self.store, "claim_next", side_effect=RuntimeError("store failure")):
            with self.assertRaisesRegex(RuntimeError, "store failure"):
                Worker(self.store, self.policy).run(self.stop)

    def test_shutdown_signal_sets_stop_and_restores_handlers(self):
        before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
        with shutdown_signals(self.stop):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            self.assertTrue(self.stop.is_set())
        for signum, handler in before.items():
            self.assertEqual(signal.getsignal(signum), handler)


if __name__ == "__main__":
    unittest.main()
