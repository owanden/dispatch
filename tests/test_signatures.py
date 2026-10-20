import base64
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dispatch.destinations import DestinationPolicy
from dispatch.signatures import load_signing_key, signature_headers, verify_signature
from dispatch.store import EventStore
from dispatch.worker import Worker


class SignatureTests(unittest.TestCase):
    def setUp(self):
        self.key = bytes(range(32))
        self.body = b'{"order":1}'
        self.timestamp = 1700000000
        self.headers = signature_headers(self.key, "event-1", self.body, timestamp=self.timestamp)

    def verify(self, **changes):
        values = dict(key=self.key, event_id="event-1", body=self.body, timestamp=self.headers["X-Dispatch-Timestamp"], signature=self.headers["X-Dispatch-Signature"], now=self.timestamp)
        values.update(changes)
        return verify_signature(**values)

    def test_valid_signature_and_exact_clock_boundaries(self):
        self.assertTrue(self.verify())
        self.assertTrue(self.verify(now=self.timestamp + 300))
        self.assertTrue(self.verify(now=self.timestamp - 300))
        self.assertFalse(self.verify(now=self.timestamp + 301))
        self.assertFalse(self.verify(now=self.timestamp - 301))

    def test_body_event_timestamp_and_key_tampering_are_rejected(self):
        for changes in [{"body": b'{"order":2}'}, {"body": b'{ "order":1}'}, {"event_id": "event-2"}, {"timestamp": str(self.timestamp + 1)}, {"key": b"x" * 32}]:
            with self.subTest(changes=list(changes)):
                self.assertFalse(self.verify(**changes))

    def test_missing_malformed_or_ambiguous_headers_are_rejected(self):
        for changes in [{"signature": None}, {"signature": "v2=" + "0" * 64}, {"signature": "v1=" + "g" * 64}, {"signature": self.headers["X-Dispatch-Signature"] + ",v1=other"}, {"timestamp": None}, {"timestamp": "01700000000"}, {"timestamp": "1" * 1000}, {"timestamp": "1700000000,1700000000"}, {"event_id": "event\n1"}]:
            with self.subTest(changes=list(changes)):
                self.assertFalse(self.verify(**changes))

    def test_signature_version_and_shape_are_explicit(self):
        self.assertEqual(self.headers["X-Dispatch-Timestamp"], "1700000000")
        self.assertRegex(self.headers["X-Dispatch-Signature"], r"^v1=[0-9a-f]{64}$")

    def test_signing_rejects_invalid_identifiers_body_and_timestamp(self):
        for event_id, body, stamp in [("bad\nheader", self.body, self.timestamp), ("event-1", {}, self.timestamp), ("event-1", self.body, -1), ("event-1", self.body, True)]:
            with self.subTest(event_id=event_id), self.assertRaises(ValueError):
                signature_headers(self.key, event_id, body, timestamp=stamp)

    def test_invalid_keys_and_verification_configuration_are_errors(self):
        for key in [b"short", b"x" * 129, "x" * 32]:
            with self.subTest(key_type=type(key).__name__), self.assertRaises(ValueError):
                self.verify(key=key)
        for tolerance in [-1, float("nan"), float("inf"), True]:
            with self.subTest(tolerance=tolerance), self.assertRaises(ValueError):
                self.verify(tolerance=tolerance)

    def test_environment_key_is_optional_and_strictly_validated(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(load_signing_key())
            os.environ["DISPATCH_SIGNING_KEY"] = base64.b64encode(self.key).decode("ascii")
            self.assertEqual(load_signing_key(), self.key)
            for encoded in ["", "not-base64!", base64.b64encode(b"short").decode("ascii")]:
                os.environ["DISPATCH_SIGNING_KEY"] = encoded
                with self.assertRaises(ValueError) as error:
                    load_signing_key()
                if encoded:
                    self.assertNotIn(encoded, str(error.exception))

    def test_invalid_environment_fails_before_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(Path(directory) / "events.db")
            event = store.enqueue("key", "https://example.com/hook", {})
            with patch.dict(os.environ, {"DISPATCH_SIGNING_KEY": "invalid!"}), self.assertRaises(ValueError):
                Worker(store, DestinationPolicy(["https://example.com"]))
            self.assertEqual(store.get(event.id).status, "pending")
            self.assertEqual(store.attempts(event.id), [])


if __name__ == "__main__":
    unittest.main()
