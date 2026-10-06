import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dispatch.store import EventStore, IdempotencyConflict


class EventStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "events.db"
        self.store = EventStore(self.path)

    def test_event_survives_restart(self):
        event = self.store.enqueue("order-1", "https://example.com/hook", {"order": 1})
        self.assertEqual(EventStore(self.path).get(event.id), event)
        self.assertEqual(event.status, "pending")

    def test_same_key_and_equivalent_payload_return_original(self):
        original = self.store.enqueue("order-1", "https://example.com/hook", {"a": 1, "b": 2})
        repeated = self.store.enqueue("order-1", "https://example.com/hook", {"b": 2, "a": 1})
        self.assertEqual(original, repeated)

    def test_changed_payload_is_rejected_without_overwriting(self):
        original = self.store.enqueue("order-1", "https://example.com/hook", {"order": 1})
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue("order-1", "https://example.com/hook", {"order": 2})
        self.assertEqual(self.store.get(original.id), original)

    def test_changed_destination_is_rejected(self):
        self.store.enqueue("order-1", "https://example.com/hook", {})
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue("order-1", "https://other.example/hook", {})

    def test_concurrent_duplicates_create_one_event(self):
        def enqueue(_):
            return EventStore(self.path).enqueue("order-1", "https://example.com/hook", {"order": 1})

        with ThreadPoolExecutor(max_workers=8) as executor:
            events = list(executor.map(enqueue, range(24)))
        self.assertEqual(len({event.id for event in events}), 1)

    def test_unknown_id_returns_none(self):
        self.assertIsNone(self.store.get("missing"))

    def test_invalid_input_does_not_reserve_key(self):
        for key, destination, payload in [
            (" ", "https://example.com/hook", {}),
            ("order-1", "", {}),
            ("order-1", "https://example.com/hook", []),
            ("order-1", "https://example.com/hook", {"value": float("nan")}),
        ]:
            with self.subTest(key=key, destination=destination, payload=payload):
                with self.assertRaises(ValueError):
                    self.store.enqueue(key, destination, payload)
        self.assertEqual(self.store.enqueue("order-1", "https://example.com/hook", {}).status, "pending")


if __name__ == "__main__":
    unittest.main()
