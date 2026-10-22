import argparse
import json
import os
import platform
import queue
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from demo import certificate, local_executor, require, start
from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.worker import Worker


def child(mode, db, cert, port, event_id):
    store = EventStore(db)
    origin = f"https://receiver.example:{port}"
    execute = local_executor(cert, port, origin)

    def gated(event, policy, timeout, deadline):
        outcome = execute(event, policy, timeout, deadline)
        require(outcome == {"status_code": 204}, "receiver did not accept the first request")
        print(json.dumps({"type": "accepted"}), flush=True)
        require(sys.stdin.read(1) != "", "crash gate closed without termination")
        return outcome

    worker = Worker(store, DestinationPolicy([origin]), timeout=2, deadline=2,
                    executor=gated if mode == "pause" else execute)
    if mode == "pause":
        require(worker.process_next(), "event was not claimed")
        return
    require(not worker.process_next(), "restarted worker delivered before lease expiry")
    print(json.dumps({"type": "early_poll"}), flush=True)
    recovered = False
    cutoff = time.monotonic() + 100
    while time.monotonic() < cutoff:
        if worker.process_next():
            print(json.dumps({"type": "delivered"}), flush=True)
            return
        state = store.delivery_state(event_id)
        if state["status"] == "pending" and not recovered:
            require(state["attempts"][0]["error"] == "lease_expired", "unexpected recovery outcome")
            print(json.dumps({"type": "recovered", "retry_at": state["retry_at"], "observed_at": time.time()}), flush=True)
            recovered = True
        time.sleep(0.1)
    raise RuntimeError("restarted worker exceeded recovery time limit")


def launch(mode, db, cert, port, event_id):
    process = subprocess.Popen([
        sys.executable, str(Path(__file__).resolve()), "--child", mode,
        "--db", str(db), "--cert", str(cert), "--port", str(port), "--event-id", event_id,
    ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    messages = queue.Queue()

    def read():
        try:
            for line in process.stdout:
                messages.put(json.loads(line))
        finally:
            messages.put(None)

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    return process, messages


def receive(messages, kind, timeout):
    try:
        message = messages.get(timeout=timeout)
    except queue.Empty:
        raise RuntimeError(f"worker timed out before {kind}") from None
    require(message is not None and message.get("type") == kind, f"worker did not report {kind}")
    return message


def run(directory):
    cert, key = certificate(directory)
    receiver_db = directory / "receiver.db"
    with closing(sqlite3.connect(receiver_db)) as connection, connection:
        connection.execute("CREATE TABLE effects (event_id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            event_id = self.headers["X-Dispatch-Event-ID"]
            with closing(sqlite3.connect(receiver_db)) as connection, connection:
                connection.execute("INSERT OR IGNORE INTO effects VALUES (?, ?)", (event_id, json.dumps(payload, sort_keys=True)))
            requests.append({"event_id": event_id, "payload": payload, "received_at": time.time()})
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    target = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    target.socket = context.wrap_socket(target.socket, server_side=True)
    target_thread = start(target)
    processes = []
    before = time.monotonic()
    try:
        db = directory / "events.db"
        origin = f"https://receiver.example:{target.server_port}"
        event = EventStore(db).enqueue("crash-demo", origin + "/webhook", {"case": "synthetic-worker-crash"})
        print("Enqueue: one persisted synthetic event", flush=True)
        first, messages = launch("pause", db, cert, target.server_port, event.id)
        processes.append(first)
        receive(messages, "accepted", 15)
        state = EventStore(db).delivery_state(event.id)
        require(state["status"] == "delivering" and state["attempt_count"] == 1, "claim did not persist")
        require(state["attempts"][0]["finished_at"] is None, "first attempt already completed")
        expires_at = state["attempts"][0]["lease_expires_at"]
        require(len(requests) == 1, "receiver did not observe exactly one first request")
        print("Receiver: HTTP 204 accepted; delivery child exited; worker paused before outcome storage", flush=True)
        first.kill()
        first.wait(timeout=10)
        require(first.returncode != 0, "worker was not forcibly terminated")
        print("Crash: fixture-owned worker killed and exit confirmed", flush=True)
        reopened = EventStore(db)
        require(reopened.get(event.id).status == "delivering" and reopened.attempts(event.id)[0].finished_at is None,
                "crash did not preserve unfinished attempt")
        print("Reopen: event delivering, unfinished attempt and lease retained", flush=True)
        resumed, messages = launch("resume", db, cert, target.server_port, event.id)
        processes.append(resumed)
        receive(messages, "early_poll", 15)
        require(EventStore(db).get(event.id).status == "delivering", "claim recovered before its lease expired")
        print("Restart: separate worker skipped the unexpired claim; waiting for real 32-second lease", flush=True)
        recovery = receive(messages, "recovered", 45)
        require(recovery["observed_at"] >= expires_at, "claim recovered early")
        print("Recovery: lease_expired persisted; waiting for default real 30-second retry delay", flush=True)
        receive(messages, "delivered", 45)
        resumed.wait(timeout=10)
        require(resumed.returncode == 0, "restarted worker failed")
        store = EventStore(db)
        attempts = store.attempts(event.id)
        require(store.get(event.id).status == "delivered", "event was not delivered")
        require(len(attempts) == 2 and attempts[0].error == "lease_expired" and attempts[0].status_code is None
                and attempts[1].status_code == 204 and attempts[1].error is None, "unexpected stored attempt history")
        require(len(requests) == 2 and all(item["event_id"] == event.id and item["payload"] == event.payload for item in requests),
                "receiver identity or payload changed")
        require(requests[1]["received_at"] >= recovery["retry_at"], "retry reached receiver before its schedule")
        with closing(sqlite3.connect(receiver_db)) as connection, connection:
            effects = connection.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
        require(effects == 1, "receiver did not deduplicate its simulated effect")
        print("Redelivery: two real HTTPS acceptances with one stable event ID", flush=True)
        print("Stored attempt history: lease_expired, HTTP 204", flush=True)
        print("Receiver deduplication: one simulated SQLite effect row", flush=True)
        return {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "environment": {"os": platform.system(), "python": platform.python_version(), "sqlite": sqlite3.sqlite_version},
            "event_id": event.id, "killed_worker_returncode": first.returncode,
            "worker_deadline_seconds": 2, "lease_seconds": 32, "retry_delay_seconds": 30,
            "lease_expires_at": expires_at, "recovery": recovery,
            "elapsed_seconds": time.monotonic() - before,
            "state": store.get(event.id).status, "attempts": [asdict(attempt) for attempt in attempts],
            "receiver_requests": requests, "receiver_effect_rows": effects,
        }
    finally:
        for process in processes:
            if process.stdin:
                process.stdin.close()
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            process.stdout.close()
            process.stderr.close()
        target.shutdown()
        target.server_close()
        target_thread.join(timeout=3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child", choices=["pause", "resume"], help=argparse.SUPPRESS)
    parser.add_argument("--db", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--cert", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--event-id", help=argparse.SUPPRESS)
    args = parser.parse_args()
    require("DISPATCH_SIGNING_KEY" not in os.environ, "unset DISPATCH_SIGNING_KEY for this unsigned local fixture")
    if args.child:
        child(args.child, args.db, args.cert, args.port, args.event_id)
        return
    print("Dispatch crash demo: real worker kill, real lease expiry and retry waiting", flush=True)
    with tempfile.TemporaryDirectory(prefix="dispatch-crash-") as directory:
        report = run(Path(directory))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print("PASS: real worker termination and recovery without clock advancement", flush=True)


if __name__ == "__main__":
    main()
