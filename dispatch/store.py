import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4


class IdempotencyConflict(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    id: str
    key: str
    destination: str
    payload: dict
    status: str


class EventStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with self._connect() as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE,
                    destination TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'delivering', 'delivered', 'failed')),
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def enqueue(self, key: str, destination: str, payload: dict) -> Event:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("key is required")
        if not isinstance(destination, str) or not destination.strip():
            raise ValueError("destination is required")
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute("SELECT * FROM events WHERE key = ?", (key,)).fetchone()
            if existing:
                if existing["destination"] != destination or existing["payload"] != encoded:
                    raise IdempotencyConflict(key)
                return self._event(existing)
            event_id = str(uuid4())
            connection.execute(
                "INSERT INTO events (id, key, destination, payload) VALUES (?, ?, ?, ?)",
                (event_id, key, destination, encoded),
            )
            return Event(event_id, key, destination, json.loads(encoded), "pending")

    def get(self, event_id: str) -> Event | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
            return self._event(row) if row else None

    @staticmethod
    def _event(row) -> Event:
        return Event(row["id"], row["key"], row["destination"], json.loads(row["payload"]), row["status"])
