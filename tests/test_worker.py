import http.client
import socket
import sqlite3
import ssl
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from dispatch.delivery import delivery_outcome
from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.transport import DeliveryRejected
from dispatch.worker import Worker


def inline_delivery(event, policy, timeout, deadline):
    return delivery_outcome(event, policy, timeout)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.store = EventStore(self.path)
        self.policy = DestinationPolicy(["https://example.com"])
        self.worker = Worker(self.store, self.policy, timeout=2, executor=inline_delivery)

    def enqueue(self, key="order-1"):
        return self.store.enqueue(key, "https://example.com/hook", {"order": 1})

    def test_empty_queue_does_not_invoke_transport(self):
        with patch("dispatch.delivery.deliver") as transport:
            self.assertFalse(self.worker.process_next())
            transport.assert_not_called()

    def test_success_records_status_and_persists_after_restart(self):
        event = self.enqueue()
        with patch("dispatch.delivery.deliver", return_value=204) as transport:
            self.assertTrue(self.worker.process_next())
            claimed_event = transport.call_args.args[0]
            self.assertEqual(claimed_event.id, event.id)
            self.assertEqual(claimed_event.status, "delivering")
            self.assertEqual(transport.call_args.kwargs, {"timeout": 2})
        restarted = EventStore(self.path)
        self.assertEqual(restarted.get(event.id).status, "delivered")
        self.assertEqual(restarted.attempts(event.id)[0].status_code, 204)
        self.assertFalse(self.worker.process_next())

    def test_receiver_failure_records_http_status(self):
        event = self.enqueue()
        with patch("dispatch.delivery.deliver", return_value=503):
            self.assertTrue(self.worker.process_next())
        self.assertEqual(self.store.get(event.id).status, "failed")
        self.assertEqual(self.store.attempts(event.id)[0].status_code, 503)

    def test_transport_failures_record_categories_without_exception_text(self):
        errors = [
            (DeliveryRejected("secret URL"), "delivery_rejected"),
            (ssl.SSLCertVerificationError("secret certificate"), "certificate_verification_failed"),
            (ssl.SSLError("secret host"), "tls_error"),
            (TimeoutError("secret address"), "request_timeout"),
            (socket.gaierror("secret domain"), "dns_error"),
            (ConnectionError("secret address"), "connection_error"),
            (http.client.BadStatusLine("secret response"), "http_protocol_error"),
            (ValueError("secret payload"), "invalid_event"),
        ]
        for index, (error, expected) in enumerate(errors):
            event = self.enqueue(str(index))
            with self.subTest(error=error), patch("dispatch.delivery.deliver", side_effect=error):
                self.assertTrue(self.worker.process_next())
                attempt = self.store.attempts(event.id)[0]
                self.assertEqual(attempt.error, expected)
                self.assertIsNone(attempt.status_code)
                self.assertEqual(self.store.get(event.id).status, "failed")

    def test_unlisted_stored_event_is_rejected_without_dns(self):
        event = self.store.enqueue("order-1", "https://other.example/hook", {})
        with patch("dispatch.transport.socket.getaddrinfo") as dns:
            self.assertTrue(self.worker.process_next())
            dns.assert_not_called()
        self.assertEqual(self.store.attempts(event.id)[0].error, "delivery_rejected")

    def test_concurrent_workers_deliver_one_claim_once(self):
        event = self.enqueue()
        def process(_):
            return Worker(EventStore(self.path), self.policy, executor=inline_delivery).process_next()
        with patch("dispatch.delivery.deliver", return_value=204) as transport, ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(process, range(24)))
            self.assertEqual(sum(results), 1)
            transport.assert_called_once()
        self.assertEqual(len(self.store.attempts(event.id)), 1)

    def test_outcome_storage_failure_preserves_unfinished_claim(self):
        event = self.enqueue()
        with patch("dispatch.delivery.deliver", return_value=204), patch.object(self.store, "finish_attempt", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaises(sqlite3.OperationalError):
                self.worker.process_next()
        self.assertEqual(self.store.get(event.id).status, "delivering")
        self.assertIsNone(self.store.attempts(event.id)[0].finished_at)
        with patch("dispatch.delivery.deliver") as transport:
            self.assertFalse(self.worker.process_next())
            transport.assert_not_called()

    def test_unexpected_failure_is_not_converted_to_completed_attempt(self):
        event = self.enqueue()
        with patch("dispatch.delivery.deliver", side_effect=RuntimeError("unexpected failure")):
            with self.assertRaises(RuntimeError):
                self.worker.process_next()
        self.assertEqual(self.store.get(event.id).status, "delivering")
        self.assertIsNone(self.store.attempts(event.id)[0].finished_at)

    def test_invalid_timeout_is_rejected_before_claiming(self):
        event = self.enqueue()
        for timeout in [0, -1, float("nan"), float("inf"), True, "5"]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                Worker(self.store, self.policy, timeout)
        self.assertEqual(self.store.get(event.id).status, "pending")
        self.assertEqual(self.store.attempts(event.id), [])


if __name__ == "__main__":
    unittest.main()
