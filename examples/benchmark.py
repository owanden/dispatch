import argparse
import http.client
import json
import math
import os
import platform
import sqlite3
import ssl
import subprocess
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from demo import certificate, local_executor, require, start
from dispatch.api import create_server
from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.worker import Worker


def distribution(values):
    ordered = sorted(values)
    return {"count": len(values), **{
        name: ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] if ordered else None
        for name, fraction in (("p50_seconds", 0.5), ("p95_seconds", 0.95), ("max_seconds", 1))
    }}


def run(directory, count, retry_every):
    cert, key = certificate(directory)
    received = []
    seen = Counter()
    epoch = time.perf_counter()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            payload = json.loads(body)
            event_id = self.headers["X-Dispatch-Event-ID"]
            seen[event_id] += 1
            status = 503 if retry_every and (payload["index"] + 1) % retry_every == 0 and seen[event_id] == 1 else 204
            received.append({"event_id": event_id, "status": status, "at_seconds": time.perf_counter() - epoch, "body_bytes": len(body)})
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

    target = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    target.socket = context.wrap_socket(target.socket, server_side=True)
    origin = f"https://receiver.example:{target.server_port}"
    store = EventStore(directory / "events.db")
    api = create_server(store, 0, [origin])
    servers = [(target, start(target)), (api, start(api))]
    try:
        events = []
        enqueue_started = time.perf_counter()
        for index in range(count):
            body = json.dumps({"destination": origin + "/webhook", "payload": {"index": index, "data": "x" * 1024}})
            connection = http.client.HTTPConnection(*api.server_address, timeout=5)
            before = time.perf_counter()
            try:
                connection.request("POST", "/events", body, {"Content-Type": "application/json", "Idempotency-Key": f"benchmark-{index}"})
                response = connection.getresponse()
                event = json.loads(response.read())
                require(response.status == 202, "enqueue rejected")
            finally:
                connection.close()
            accepted = time.perf_counter()
            events.append({"event_id": event["id"], "index": index, "accepted_seconds": accepted - epoch, "enqueue_seconds": accepted - before})
        enqueue_elapsed = time.perf_counter() - enqueue_started
        execute = local_executor(cert, target.server_port, origin)
        attempts = []

        def measured(event, policy, timeout, deadline):
            before = time.perf_counter()
            outcome = execute(event, policy, timeout, deadline)
            attempts.append({"event_id": event.id, "outcome": outcome, "child_seconds": time.perf_counter() - before})
            return outcome

        worker = Worker(store, DestinationPolicy([origin]), executor=measured)
        worker_started = time.perf_counter()
        completed = set()
        cutoff = worker_started + 120 + count * 5
        while len(completed) < count:
            require(time.perf_counter() < cutoff, "workload exceeded time limit")
            before = time.perf_counter()
            if worker.process_next():
                attempt = attempts[-1]
                attempt["worker_seconds"] = time.perf_counter() - before
                event_id = attempt["event_id"]
                if store.get(event_id).status == "delivered":
                    completed.add(event_id)
                    item = next(item for item in events if item["event_id"] == event_id)
                    item["delivered_seconds"] = time.perf_counter() - epoch
                    item["enqueue_to_delivered_seconds"] = item["delivered_seconds"] - item["accepted_seconds"]
                else:
                    require(store.get(event_id).status == "pending", "unexpected permanent failure")
            else:
                time.sleep(0.05)
        worker_elapsed = time.perf_counter() - worker_started
        recovery = []
        for item in events:
            history = store.attempts(item["event_id"])
            expected = [503, 204] if retry_every and (item["index"] + 1) % retry_every == 0 else [204]
            require([attempt.status_code for attempt in history] == expected, "unexpected durable attempt history")
            requests = [request for request in received if request["event_id"] == item["event_id"]]
            require([request["status"] for request in requests] == expected, "receiver history differs")
            if len(requests) == 2:
                delay = requests[1]["at_seconds"] - requests[0]["at_seconds"]
                require(delay >= 30, "retry arrived early")
                recovery.append(delay)
        return {
            "schema_version": 1,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "environment": {"python": platform.python_version(), "os": platform.system(), "os_release": platform.release(), "architecture": platform.machine(), "logical_cpus": os.cpu_count(), "sqlite": sqlite3.sqlite_version, "tls": ssl.OPENSSL_VERSION, "certificate_tool": subprocess.check_output(["openssl", "version"], text=True).strip()},
            "workload": {"events": count, "payload_data_bytes": 1024, "retry_every": retry_every, "workers": 1, "poll_seconds": 0.05, "retry_delay_seconds": 30, "signing_enabled": "DISPATCH_SIGNING_KEY" in os.environ, "transport": "local HTTPS with example-only pinned loopback resolution", "enqueue": "sequential HTTP batch before worker starts"},
            "summary": {"delivered": len(completed), "failed": 0, "attempts": len(attempts), "receiver_status_counts": dict(Counter(str(request["status"]) for request in received)), "enqueue_batch_seconds": enqueue_elapsed, "enqueue_events_per_second": count / enqueue_elapsed, "worker_drain_seconds": worker_elapsed, "delivered_events_per_worker_second": count / worker_elapsed, "enqueue": distribution([item["enqueue_seconds"] for item in events]), "enqueue_to_delivered": distribution([item["enqueue_to_delivered_seconds"] for item in events]), "child": distribution([item["child_seconds"] for item in attempts]), "worker_attempt": distribution([item["worker_seconds"] for item in attempts]), "receiver_503_to_204": distribution(recovery)},
            "events": events, "attempts": attempts, "receiver_requests": received,
        }
    finally:
        for server, thread in servers:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--retry-every", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.events <= 1000 or not 0 <= args.retry_every <= args.events:
        parser.error("events must be 1..1000; retry-every must be 0..events (0 disables failures)")
    with tempfile.TemporaryDirectory(prefix="dispatch-benchmark-") as directory:
        report = run(Path(directory), args.events, args.retry_every)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
