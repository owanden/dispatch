import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4


class IdempotencyConflict(ValueError):
    pass


class AttemptConflict(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    id: str
    key: str
    destination: str
    payload: dict
    status: str


@dataclass(frozen=True)
class DeliveryClaim:
    event: Event
    attempt_id: str


@dataclass(frozen=True)
class Attempt:
    id: str
    event_id: str
    started_at: str
    finished_at: str | None
    status_code: int | None
    error: str | None


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
            connection.execute("""
                CREATE TABLE IF NOT EXISTS attempts (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL REFERENCES events(id),
                    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at TEXT,
                    status_code INTEGER CHECK (status_code BETWEEN 100 AND 599),
                    error TEXT,
                    CHECK (
                        (finished_at IS NULL AND status_code IS NULL AND error IS NULL)
                        OR (finished_at IS NOT NULL AND (
                            (status_code IS NOT NULL AND error IS NULL)
                            OR (status_code IS NULL AND error IS NOT NULL)
                        ))
                    )
                )
            """)
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS active_attempt ON attempts(event_id) WHERE finished_at IS NULL")
            connection.execute("CREATE INDEX IF NOT EXISTS pending_events ON events(status, created_at)")

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
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

    def claim_next(self) -> DeliveryClaim | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM events WHERE status = 'pending' ORDER BY created_at, rowid LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            attempt_id = str(uuid4())
            connection.execute("UPDATE events SET status = 'delivering' WHERE id = ?", (row["id"],))
            connection.execute("INSERT INTO attempts (id, event_id) VALUES (?, ?)", (attempt_id, row["id"]))
            claimed = connection.execute("SELECT * FROM events WHERE id = ?", (row["id"],)).fetchone()
            return DeliveryClaim(self._event(claimed), attempt_id)

    def finish_attempt(self, attempt_id: str, *, status_code: int | None = None, error: str | None = None):
        if (status_code is None) == (error is None):
            raise ValueError("exactly one outcome is required")
        if status_code is not None and (type(status_code) is not int or not 100 <= status_code <= 599):
            raise ValueError("invalid HTTP status")
        if error is not None and (not isinstance(error, str) or not error.strip() or len(error) > 256):
            raise ValueError("invalid error category")
        status = "delivered" if status_code is not None and 200 <= status_code < 300 else "failed"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("""
                SELECT attempts.event_id FROM attempts JOIN events ON events.id = attempts.event_id
                WHERE attempts.id = ? AND attempts.finished_at IS NULL AND events.status = 'delivering'
            """, (attempt_id,)).fetchone()
            if row is None:
                raise AttemptConflict(attempt_id)
            connection.execute("""
                UPDATE attempts SET finished_at = CURRENT_TIMESTAMP, status_code = ?, error = ? WHERE id = ?
            """, (status_code, error, attempt_id))
            connection.execute("UPDATE events SET status = ? WHERE id = ?", (status, row["event_id"]))

    def attempts(self, event_id: str) -> list[Attempt]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM attempts WHERE event_id = ? ORDER BY started_at, rowid", (event_id,)
            ).fetchall()
            return [Attempt(**dict(row)) for row in rows]

    @staticmethod
    def _event(row) -> Event:
        return Event(row["id"], row["key"], row["destination"], json.loads(row["payload"]), row["status"])
