import argparse
import json
import socket
import sqlite3
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from dispatch.store import EventStore, IdempotencyConflict


MAX_BODY = 65536


def valid_destination(value):
    if not isinstance(value, str) or any(character.isspace() for character in value):
        return False
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme == "https"
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and (parsed.port is None or 1 <= parsed.port <= 65535)
        )
    except ValueError:
        return False


def reject_constant(value):
    raise ValueError("invalid JSON constant")


def create_server(store, port=8080):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, format, *args):
            pass

        def respond(self, status, body):
            encoded = json.dumps(body, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            try:
                parts = urlsplit(self.path)
            except ValueError:
                self.respond(404, {"error": "not_found"})
                return
            if parts.query or not parts.path.startswith("/events/"):
                self.respond(404, {"error": "not_found"})
                return
            event_id = parts.path.removeprefix("/events/")
            try:
                event = store.get(event_id)
            except sqlite3.Error:
                self.respond(503, {"error": "storage_unavailable"})
                return
            self.respond(200, asdict(event)) if event else self.respond(404, {"error": "not_found"})

        def do_POST(self):
            if self.path != "/events":
                self.respond(404, {"error": "not_found"})
                return
            if self.headers.get("Transfer-Encoding"):
                self.respond(400, {"error": "unsupported_transfer_encoding"})
                return
            lengths = self.headers.get_all("Content-Length", [])
            if not lengths:
                self.respond(411, {"error": "content_length_required"})
                return
            if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
                self.respond(400, {"error": "invalid_content_length"})
                return
            if len(lengths[0]) > 10:
                self.respond(413, {"error": "payload_too_large"})
                return
            length = int(lengths[0])
            if length > MAX_BODY:
                self.respond(413, {"error": "payload_too_large"})
                return
            if self.headers.get_content_type() != "application/json":
                self.respond(415, {"error": "json_required"})
                return
            keys = self.headers.get_all("Idempotency-Key", [])
            if len(keys) != 1 or not keys[0].strip() or len(keys[0]) > 256:
                self.respond(400, {"error": "invalid_idempotency_key"})
                return
            try:
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError("incomplete body")
                body = json.loads(raw.decode("utf-8"), parse_constant=reject_constant)
                if (
                    not isinstance(body, dict)
                    or set(body) != {"destination", "payload"}
                    or not valid_destination(body["destination"])
                    or not isinstance(body["payload"], dict)
                ):
                    raise ValueError("invalid event")
                event = store.enqueue(keys[0], body["destination"], body["payload"])
            except IdempotencyConflict:
                self.respond(409, {"error": "idempotency_conflict"})
            except (ValueError, UnicodeError, RecursionError):
                self.respond(400, {"error": "invalid_event"})
            except socket.timeout:
                self.respond(408, {"error": "request_timeout"})
            except sqlite3.Error:
                self.respond(503, {"error": "storage_unavailable"})
            else:
                self.respond(202, asdict(event))

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="events.db")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    with create_server(EventStore(args.db), args.port) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
