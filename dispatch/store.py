import json
import math
import sqlite3
import time
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
    def __init__(self, path: str | Path, *, clock=time.time):
        self.path = str(path)
        self.clock = clock
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
            connection.execute("""
                CREATE TABLE IF NOT EXISTS retry_schedule (
                    event_id TEXT PRIMARY KEY REFERENCES events(id),
                    not_before REAL NOT NULL CHECK (not_before >= 0)
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS claim_leases (
                    attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
                    expires_at REAL NOT NULL
                )
            """)
            connection.execute("CREATE INDEX IF NOT EXISTS lease_expiry ON claim_leases(expires_at)")

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

    def claim_next(self, *, lease_seconds=None) -> DeliveryClaim | None:
        if lease_seconds is not None and (type(lease_seconds) not in (int, float) or not math.isfinite(lease_seconds) or lease_seconds <= 0):
            raise ValueError("lease duration must be positive and finite")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT events.* FROM events LEFT JOIN retry_schedule ON retry_schedule.event_id = events.id
                WHERE events.status = 'pending' AND COALESCE(retry_schedule.not_before, 0) <= ?
                ORDER BY COALESCE(retry_schedule.not_before, 0), events.created_at, events.rowid LIMIT 1""",
                (self.clock(),),
            ).fetchone()
            if row is None:
                return None
            attempt_id = str(uuid4())
            connection.execute("UPDATE events SET status = 'delivering' WHERE id = ?", (row["id"],))
            connection.execute("DELETE FROM retry_schedule WHERE event_id = ?", (row["id"],))
            connection.execute("INSERT INTO attempts (id, event_id) VALUES (?, ?)", (attempt_id, row["id"]))
            if lease_seconds is not None:
                connection.execute("INSERT INTO claim_leases (attempt_id, expires_at) VALUES (?, ?)", (attempt_id, self.clock() + lease_seconds))
            claimed = connection.execute("SELECT * FROM events WHERE id = ?", (row["id"],)).fetchone()
            return DeliveryClaim(self._event(claimed), attempt_id)

    def finish_attempt(self, attempt_id: str, *, status_code: int | None = None, error: str | None = None, retry_policy=None):
        if (status_code is None) == (error is None):
            raise ValueError("exactly one outcome is required")
        if status_code is not None and (type(status_code) is not int or not 100 <= status_code <= 599):
            raise ValueError("invalid HTTP status")
        if error is not None and (not isinstance(error, str) or not error.strip() or len(error) > 256):
            raise ValueError("invalid error category")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._finish_attempt(connection, attempt_id, status_code, error, retry_policy)

    def _finish_attempt(self, connection, attempt_id, status_code, error, retry_policy):
        status = "delivered" if status_code is not None and 200 <= status_code < 300 else "failed"
        row = connection.execute("""
            SELECT attempts.event_id FROM attempts JOIN events ON events.id = attempts.event_id
            WHERE attempts.id = ? AND attempts.finished_at IS NULL AND events.status = 'delivering'
        """, (attempt_id,)).fetchone()
        if row is None:
            raise AttemptConflict(attempt_id)
        connection.execute("""
            UPDATE attempts SET finished_at = CURRENT_TIMESTAMP, status_code = ?, error = ? WHERE id = ?
        """, (status_code, error, attempt_id))
        if retry_policy is not None:
            count = connection.execute("SELECT COUNT(*) FROM attempts WHERE event_id = ?", (row["event_id"],)).fetchone()[0]
            delay = retry_policy.next_delay(count, status_code=status_code, error=error)
            if delay is not None:
                status = "pending"
                connection.execute("INSERT INTO retry_schedule (event_id, not_before) VALUES (?, ?)", (row["event_id"], self.clock() + delay))
        connection.execute("UPDATE events SET status = ? WHERE id = ?", (status, row["event_id"]))
        connection.execute("DELETE FROM claim_leases WHERE attempt_id = ?", (attempt_id,))

    def recover_expired(self, *, retry_policy=None):
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute("""
                SELECT claim_leases.attempt_id FROM claim_leases
                JOIN attempts ON attempts.id = claim_leases.attempt_id
                JOIN events ON events.id = attempts.event_id
                WHERE claim_leases.expires_at <= ? AND attempts.finished_at IS NULL
                    AND events.status = 'delivering'
                ORDER BY claim_leases.expires_at LIMIT 100
            """, (self.clock(),)).fetchall()
            for row in rows:
                self._finish_attempt(connection, row["attempt_id"], None, "lease_expired", retry_policy)
            return len(rows)

    def queue_metrics(self):
        with self._connect() as connection:
            connection.execute("BEGIN")
            now = self.clock()
            events = {status: 0 for status in ("pending", "delivering", "delivered", "failed")}
            for row in connection.execute("SELECT status, COUNT(*) AS count FROM events GROUP BY status"):
                events[row["status"]] = row["count"]
            ready = connection.execute("""
                SELECT COUNT(*) AS count, MIN(CAST(strftime('%s', events.created_at) AS REAL)) AS oldest
                FROM events LEFT JOIN retry_schedule ON retry_schedule.event_id = events.id
                WHERE events.status = 'pending' AND COALESCE(retry_schedule.not_before, 0) <= ?
            """, (now,)).fetchone()
            attempts = connection.execute("""
                SELECT COALESCE(SUM(finished_at IS NULL), 0) AS unfinished,
                    COALESCE(SUM(error = 'lease_expired'), 0) AS recovered FROM attempts
            """).fetchone()
            expired = connection.execute("""
                SELECT COUNT(*) FROM claim_leases JOIN attempts ON attempts.id = claim_leases.attempt_id
                JOIN events ON events.id = attempts.event_id
                WHERE claim_leases.expires_at <= ? AND attempts.finished_at IS NULL AND events.status = 'delivering'
            """, (now,)).fetchone()[0]
            return {
                "captured_at": now, "events": events,
                "ready_events": ready["count"], "scheduled_retries": events["pending"] - ready["count"],
                "unfinished_attempts": attempts["unfinished"], "expired_leases": expired,
                "recovered_attempts": attempts["recovered"],
                "oldest_ready_age_seconds": max(0, now - ready["oldest"]) if ready["oldest"] is not None else None,
            }

    def delivery_state(self, event_id: str):
        with self._connect() as connection:
            connection.execute("BEGIN")
            event = connection.execute("SELECT id, status FROM events WHERE id = ?", (event_id,)).fetchone()
            if event is None:
                return None
            retry = connection.execute("SELECT not_before FROM retry_schedule WHERE event_id = ?", (event_id,)).fetchone()
            count = connection.execute("SELECT COUNT(*) FROM attempts WHERE event_id = ?", (event_id,)).fetchone()[0]
            rows = connection.execute("""
                SELECT attempts.*, claim_leases.expires_at AS lease_expires_at FROM attempts
                LEFT JOIN claim_leases ON claim_leases.attempt_id = attempts.id
                WHERE attempts.event_id = ? ORDER BY attempts.rowid DESC LIMIT 100
            """, (event_id,)).fetchall()
            return {
                "event_id": event["id"], "status": event["status"],
                "retry_at": retry["not_before"] if retry else None,
                "attempt_count": count, "attempts_truncated": count > len(rows),
                "attempts": [dict(row) for row in reversed(rows)],
            }

    def retry_at(self, event_id: str) -> float | None:
        with self._connect() as connection:
            row = connection.execute("SELECT not_before FROM retry_schedule WHERE event_id = ?", (event_id,)).fetchone()
            return row["not_before"] if row else None

    def attempts(self, event_id: str) -> list[Attempt]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM attempts WHERE event_id = ? ORDER BY started_at, rowid", (event_id,)
            ).fetchall()
            return [Attempt(**dict(row)) for row in rows]

    @staticmethod
    def _event(row) -> Event:
        return Event(row["id"], row["key"], row["destination"], json.loads(row["payload"]), row["status"])
