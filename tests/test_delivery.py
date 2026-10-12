import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from dispatch.delivery import _run_command, run_delivery
from dispatch.destinations import DestinationPolicy
from dispatch.store import Event, EventStore
from dispatch.worker import Worker


class DeliveryDeadlineTests(unittest.TestCase):
    def test_stalled_child_is_terminated_at_deadline(self):
        started = time.monotonic()
        outcome = _run_command([sys.executable, "-c", "import time; time.sleep(30)"], {}, 0.2)
        self.assertEqual(outcome, {"error": "delivery_deadline_exceeded"})
        self.assertLess(time.monotonic() - started, 5)

    def test_child_result_returns_before_deadline(self):
        self.assertEqual(_run_command([sys.executable, "-c", 'print(\'{"status_code":204}\')'], {}, 5), {"status_code": 204})

    def test_crashed_child_does_not_expose_stderr(self):
        with self.assertRaisesRegex(RuntimeError, "^delivery process failed$"):
            _run_command([sys.executable, "-c", "import sys; sys.stderr.write('private URL'); sys.exit(1)"], {}, 5)

    def test_invalid_child_results_are_rejected(self):
        for output in ['not JSON', '[]', '{"status_code":true}', '{"status_code":999}', '{"error":"private URL"}', '{"status_code":200,"error":"dns_error"}']:
            with self.subTest(output=output), self.assertRaisesRegex(RuntimeError, "invalid delivery process result"):
                _run_command([sys.executable, "-c", "print(" + repr(output) + ")"], {}, 5)

    def test_real_delivery_child_rejects_unlisted_origin_without_network(self):
        event = Event("1", "key", "https://example.com/hook", {}, "delivering")
        self.assertEqual(run_delivery(event, DestinationPolicy(), deadline=5), {"error": "delivery_rejected"})

    def test_worker_persists_deadline_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(Path(directory) / "events.db")
            event = store.enqueue("key", "https://example.com/hook", {})
            def stalled(event, policy, timeout, deadline):
                return _run_command([sys.executable, "-c", "import time; time.sleep(30)"], {}, deadline)
            worker = Worker(store, DestinationPolicy(["https://example.com"]), deadline=0.2, executor=stalled)
            self.assertTrue(worker.process_next())
            self.assertEqual(store.get(event.id).status, "failed")
            self.assertEqual(store.attempts(event.id)[0].error, "delivery_deadline_exceeded")
            self.assertIsNotNone(store.attempts(event.id)[0].finished_at)

    def test_invalid_deadline_is_rejected_before_claiming(self):
        with patch("dispatch.worker.EventStore") as store:
            for deadline in [0, -1, float("nan"), float("inf"), True, "5"]:
                with self.subTest(deadline=deadline), self.assertRaises(ValueError):
                    Worker(store, DestinationPolicy(), deadline=deadline)
            store.claim_next.assert_not_called()


if __name__ == "__main__":
    unittest.main()
