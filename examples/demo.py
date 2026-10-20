import http.client
import json
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dispatch.api import create_server
from dispatch.delivery import _run_command
from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.worker import Worker


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def certificate(directory):
    openssl = shutil.which("openssl")
    if openssl is None:
        raise RuntimeError("OpenSSL executable is required for the local HTTPS demo")
    cert, key, config = [directory / name for name in ("receiver.pem", "receiver-key.pem", "openssl.cnf")]
    config.write_text("[req]\ndistinguished_name=dn\n[dn]\n")
    subprocess.run([
        openssl, "req", "-config", str(config), "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(cert), "-days", "2", "-subj", "/CN=receiver.example",
        "-addext", "subjectAltName=DNS:receiver.example",
    ], capture_output=True, check=True, timeout=15)
    return cert, key


def receiver(cert, key, requests):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((self.headers["X-Dispatch-Event-ID"], payload))
            self.send_response(503 if len(requests) == 1 else 204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    return server


def start(server):
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05), daemon=True)
    thread.start()
    return thread


def request(server, method, path, body=None):
    connection = http.client.HTTPConnection(*server.server_address, timeout=3)
    headers = {"Content-Type": "application/json", "Idempotency-Key": "demo-order-1"}
    try:
        connection.request(method, path, json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def local_executor(cert, port, origin):
    def execute(event, policy, timeout, deadline):
        code = (
            "import os,socket; from unittest.mock import patch; "
            f"os.environ['SSL_CERT_FILE']={str(cert)!r}; "
            "from dispatch.delivery import main; "
            f"address=(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',{port})); "
            "patch('dispatch.transport.resolve_destination',return_value=address).start(); main()"
        )
        return _run_command([sys.executable, "-c", code], {
            "event": asdict(event), "origins": [origin], "timeout": timeout,
        }, deadline)
    return execute


def run(directory):
    cert, key = certificate(directory)
    requests = []
    target = receiver(cert, key, requests)
    target_thread = start(target)
    api = None
    api_thread = None
    try:
        origin = f"https://receiver.example:{target.server_port}"
        clock = [1700000000]
        db = directory / "events.db"
        store = EventStore(db, clock=lambda: clock[0])
        api = create_server(store, 0, [origin])
        api_thread = start(api)
        payload = {"order_id": "demo-001", "total_cents": 4200}
        body = {"destination": origin + "/webhook", "payload": payload}
        status, event = request(api, "POST", "/events", body)
        require(status == 202 and event["status"] == "pending", "enqueue failed")
        print("Enqueue: pending")
        _, duplicate = request(api, "POST", "/events", body)
        require(duplicate["id"] == event["id"], "duplicate event created")
        print("Idempotency: repeated key returned the same event")
        executor = local_executor(cert, target.server_port, origin)
        worker = Worker(store, DestinationPolicy([origin]), executor=executor)
        require(worker.process_next(), "first attempt was not processed")
        _, state = request(api, "GET", f"/events/{event['id']}/delivery")
        require(state["status"] == "pending" and state["attempts"][0]["status_code"] == 503, "retry was not persisted")
        require(state["retry_at"] == clock[0] + 30, "incorrect retry schedule")
        print("First delivery: HTTP 503; retry persisted for 30 seconds later")
        require(not worker.process_next(), "retry ran before its scheduled time")
        print("Early poll: no eligible event")
        restarted = EventStore(db, clock=lambda: clock[0])
        require(len(restarted.attempts(event["id"])) == 1 and restarted.retry_at(event["id"]) == state["retry_at"], "restart lost delivery state")
        print("Restart: event, attempt history, and retry schedule retained")
        clock[0] += 30
        worker = Worker(restarted, DestinationPolicy([origin]), executor=executor)
        require(worker.process_next(), "retry was not processed")
        _, state = request(api, "GET", f"/events/{event['id']}/delivery")
        require(state["status"] == "delivered" and [attempt["status_code"] for attempt in state["attempts"]] == [503, 204], "unexpected final outcome")
        require(requests == [(event["id"], payload), (event["id"], payload)], "receiver payload or identity changed")
        print("Second delivery: HTTP 204; delivered")
        print("Receiver: two real HTTPS requests with one stable event ID")
        print("Attempt history: 503, 204")
        _, metrics = request(api, "GET", "/metrics")
        require(metrics["events"] == {"pending": 0, "delivering": 0, "delivered": 1, "failed": 0}, "incorrect queue metrics")
        print("Queue: delivered=1, pending=0, delivering=0, failed=0")
        print("PASS: local HTTPS demo; retry clock advanced without a real 30-second wait")
    finally:
        for server, thread in ((api, api_thread), (target, target_thread)):
            if server is not None:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)


def main():
    print("Dispatch local demo: temporary HTTPS receiver and accelerated retry clock")
    with tempfile.TemporaryDirectory(prefix="dispatch-demo-") as directory:
        run(Path(directory))


if __name__ == "__main__":
    main()
