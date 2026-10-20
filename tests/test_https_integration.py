import base64
import json
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue
from unittest.mock import patch

from dispatch.delivery import _run_command
from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.signatures import verify_signature
from dispatch.worker import Worker


class HTTPSIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        openssl = shutil.which("openssl")
        if openssl is None:
            raise RuntimeError("OpenSSL executable is required for HTTPS integration tests")
        cls.cert_directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.cert_directory.cleanup)
        cls.cert = Path(cls.cert_directory.name) / "receiver.pem"
        cls.key = Path(cls.cert_directory.name) / "receiver-key.pem"
        config = Path(cls.cert_directory.name) / "openssl.cnf"
        config.write_text("[req]\ndistinguished_name=dn\n[dn]\n")
        subprocess.run([
            openssl, "req", "-config", str(config), "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(cls.key), "-out", str(cls.cert), "-days", "2",
            "-subj", "/CN=receiver.example", "-addext", "subjectAltName=DNS:receiver.example",
        ], check=True, capture_output=True, timeout=15)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.now = 1000
        self.store = EventStore(Path(self.directory.name) / "events.db", clock=lambda: self.now)
        self.requests = Queue()
        self.status = 204
        self.signing_key = None
        receiver = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if receiver.signing_key is not None and not verify_signature(
                    receiver.signing_key, self.headers.get("X-Dispatch-Event-ID"), body,
                    self.headers.get("X-Dispatch-Timestamp"), self.headers.get("X-Dispatch-Signature"),
                ):
                    self.send_response(401)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                receiver.requests.put((self.path, dict(self.headers), json.loads(body)))
                self.send_response(receiver.status)
                if receiver.status == 302:
                    self.send_header("Location", "https://other.example/redirect")
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert, self.key)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.05), daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.port = self.server.server_port

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def worker(self, host="receiver.example", trusted=True, signing_key=None):
        origin = f"https://{host}:{self.port}"
        def execute(event, policy, timeout, deadline):
            code = (
                "import socket; from unittest.mock import patch; "
                "from dispatch.delivery import main; "
                f"address=(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',{self.port})); "
                "patch('dispatch.transport.resolve_destination',return_value=address).start(); main()"
            )
            request = {"event": asdict(event), "origins": [origin], "timeout": timeout}
            certificate = str(self.cert) if trusted else str(Path(self.directory.name) / "untrusted.pem")
            environment = {"SSL_CERT_FILE": certificate}
            if signing_key is not None:
                environment["DISPATCH_SIGNING_KEY"] = base64.b64encode(signing_key).decode("ascii")
            with patch.dict(os.environ, environment):
                if signing_key is None:
                    os.environ.pop("DISPATCH_SIGNING_KEY", None)
                return _run_command([sys.executable, "-c", code], request, deadline)
        return Worker(self.store, DestinationPolicy([origin]), timeout=2, deadline=5, executor=execute)

    def enqueue(self, host="receiver.example"):
        return self.store.enqueue("order-1", f"https://{host}:{self.port}/hook?order=1", {"order": 1, "name": "café"})

    def test_https_delivery_persists_success_and_receiver_payload(self):
        event = self.enqueue()
        self.assertTrue(self.worker().process_next())
        path, headers, payload = self.requests.get_nowait()
        self.assertEqual(path, "/hook?order=1")
        self.assertEqual(headers["Host"], f"receiver.example:{self.port}")
        self.assertEqual(headers["X-Dispatch-Event-ID"], event.id)
        self.assertEqual(payload, event.payload)
        self.assertEqual(self.store.get(event.id).status, "delivered")
        self.assertEqual(self.store.attempts(event.id)[0].status_code, 204)

    def test_untrusted_certificate_is_terminal_before_http_request(self):
        event = self.enqueue()
        self.assertTrue(self.worker(trusted=False).process_next())
        self.assertTrue(self.requests.empty())
        self.assertEqual(self.store.get(event.id).status, "failed")
        self.assertEqual(self.store.attempts(event.id)[0].error, "certificate_verification_failed")
        self.assertIsNone(self.store.retry_at(event.id))

    def test_hostname_mismatch_is_terminal_before_http_request(self):
        event = self.enqueue("wrong.example")
        self.assertTrue(self.worker(host="wrong.example").process_next())
        self.assertTrue(self.requests.empty())
        self.assertEqual(self.store.get(event.id).status, "failed")
        self.assertEqual(self.store.attempts(event.id)[0].error, "certificate_verification_failed")

    def test_https_redirect_is_not_followed(self):
        self.status = 302
        event = self.enqueue()
        self.assertTrue(self.worker().process_next())
        self.assertEqual(self.requests.qsize(), 1)
        self.assertEqual(self.store.attempts(event.id)[0].error, "delivery_rejected")
        self.assertEqual(self.store.get(event.id).status, "failed")

    def test_https_retry_preserves_identity_and_attempt_history(self):
        self.status = 503
        event = self.enqueue()
        worker = self.worker()
        self.assertTrue(worker.process_next())
        self.assertEqual(self.store.get(event.id).status, "pending")
        self.assertEqual(self.store.retry_at(event.id), 1030)
        self.assertFalse(worker.process_next())
        self.now = 1030
        self.status = 204
        self.assertTrue(worker.process_next())
        first = self.requests.get_nowait()
        second = self.requests.get_nowait()
        self.assertEqual(first[1]["X-Dispatch-Event-ID"], second[1]["X-Dispatch-Event-ID"])
        self.assertEqual([attempt.status_code for attempt in self.store.attempts(event.id)], [503, 204])
        self.assertEqual(self.store.get(event.id).status, "delivered")


    def test_signed_https_request_is_verified_on_raw_body(self):
        self.signing_key = bytes(range(32))
        event = self.enqueue()
        self.assertTrue(self.worker(signing_key=self.signing_key).process_next())
        self.assertEqual(self.store.get(event.id).status, "delivered")
        _, headers, payload = self.requests.get_nowait()
        self.assertTrue(headers["X-Dispatch-Signature"].startswith("v1="))
        self.assertEqual(payload, event.payload)

    def test_wrong_signing_key_is_rejected_by_receiver(self):
        self.signing_key = bytes(range(32))
        event = self.enqueue()
        self.assertTrue(self.worker(signing_key=b"x" * 32).process_next())
        self.assertEqual(self.store.attempts(event.id)[0].status_code, 401)
        self.assertEqual(self.store.get(event.id).status, "failed")
        self.assertTrue(self.requests.empty())

    def test_unsigned_request_is_rejected_when_receiver_requires_signatures(self):
        self.signing_key = bytes(range(32))
        event = self.enqueue()
        self.assertTrue(self.worker().process_next())
        self.assertEqual(self.store.attempts(event.id)[0].status_code, 401)
        self.assertTrue(self.requests.empty())


if __name__ == "__main__":
    unittest.main()
