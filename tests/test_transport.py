import io
import socket
import ssl
import unittest
from unittest.mock import MagicMock, patch

from dispatch.destinations import DestinationPolicy
from dispatch.store import Event
from dispatch.transport import DeliveryRejected, PinnedHTTPSConnection, deliver, resolve_destination


PUBLIC = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))
PRIVATE = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.event = Event("event-1", "order-1", "https://example.com/hook?order=1", {"order": 1}, "pending")
        self.policy = DestinationPolicy(["https://example.com"])

    def test_rejects_unlisted_destination_before_dns_or_connection(self):
        with patch("dispatch.transport.socket.getaddrinfo") as dns, patch("dispatch.transport.PinnedHTTPSConnection") as connection:
            with self.assertRaises(DeliveryRejected):
                deliver(self.event, DestinationPolicy())
            dns.assert_not_called()
            connection.assert_not_called()

    def test_rejects_private_or_mixed_dns_results(self):
        for addresses in [[PRIVATE], [PUBLIC, PRIVATE], []]:
            with self.subTest(addresses=addresses), patch("dispatch.transport.socket.getaddrinfo", return_value=addresses), patch("dispatch.transport.PinnedHTTPSConnection") as connection:
                with self.assertRaises(DeliveryRejected):
                    deliver(self.event, self.policy)
                connection.assert_not_called()

    def test_rechecks_dns_on_each_attempt(self):
        with patch("dispatch.transport.socket.getaddrinfo", side_effect=[[PUBLIC], [PRIVATE]]), patch("dispatch.transport.PinnedHTTPSConnection") as factory:
            factory.return_value.getresponse.return_value.status = 204
            self.assertEqual(deliver(self.event, self.policy), 204)
            with self.assertRaises(DeliveryRejected):
                deliver(self.event, self.policy)
            self.assertEqual(factory.call_count, 1)

    def test_sends_payload_and_returns_status(self):
        with patch("dispatch.transport.socket.getaddrinfo", return_value=[PUBLIC]) as dns, patch("dispatch.transport.PinnedHTTPSConnection") as factory:
            connection = factory.return_value
            connection.getresponse.return_value.status = 204
            self.assertEqual(deliver(self.event, self.policy, timeout=2), 204)
            factory.assert_called_once_with("example.com", 443, PUBLIC, 2)
            dns.assert_called_once_with("example.com", 443, type=socket.SOCK_STREAM)
            connection.request.assert_called_once_with("POST", "/hook?order=1", b'{"order":1}', {"Content-Type": "application/json", "X-Dispatch-Event-ID": "event-1"})
            connection.close.assert_called_once()

    def test_redirect_is_refused_without_another_request(self):
        for status in [301, 302, 303, 307, 308]:
            with self.subTest(status=status), patch("dispatch.transport.socket.getaddrinfo", return_value=[PUBLIC]), patch("dispatch.transport.PinnedHTTPSConnection") as factory:
                connection = factory.return_value
                connection.getresponse.return_value.status = status
                with self.assertRaises(DeliveryRejected):
                    deliver(self.event, self.policy)
                self.assertEqual(connection.request.call_count, 1)
                connection.close.assert_called_once()

    def test_timeout_and_tls_failures_close_connection(self):
        for error in [socket.timeout(), ssl.SSLCertVerificationError("invalid certificate")]:
            with self.subTest(error=error), patch("dispatch.transport.socket.getaddrinfo", return_value=[PUBLIC]), patch("dispatch.transport.PinnedHTTPSConnection") as factory:
                factory.return_value.getresponse.side_effect = error
                with self.assertRaises(type(error)):
                    deliver(self.event, self.policy)
                factory.return_value.close.assert_called_once()

    def test_receiver_errors_are_returned_for_attempt_recording(self):
        with patch("dispatch.transport.socket.getaddrinfo", return_value=[PUBLIC]), patch("dispatch.transport.PinnedHTTPSConnection") as factory:
            factory.return_value.getresponse.return_value.status = 503
            self.assertEqual(deliver(self.event, self.policy), 503)

    def test_invalid_timeout_is_rejected_before_dns(self):
        with patch("dispatch.transport.socket.getaddrinfo") as dns:
            for timeout in [0, -1, float("nan"), float("inf"), "5"]:
                with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                    deliver(self.event, self.policy, timeout)
            dns.assert_not_called()

    def test_ipv6_public_result_and_scoped_result(self):
        public = (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:4700:4700::1111", 443, 0, 0))
        scoped = (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:4700:4700::1111", 443, 0, 2))
        with patch("dispatch.transport.socket.getaddrinfo", return_value=[public]):
            self.assertEqual(resolve_destination("example.com", 443), public)
        with patch("dispatch.transport.socket.getaddrinfo", return_value=[scoped]):
            with self.assertRaises(DeliveryRejected):
                resolve_destination("example.com", 443)

    def test_connection_pins_ip_and_verifies_original_hostname(self):
        context = ssl.create_default_context()
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        with patch("dispatch.transport.ssl.create_default_context", return_value=context), patch("dispatch.transport.socket.socket") as factory, patch.object(context, "wrap_socket", return_value=MagicMock()) as wrap:
            connection = PinnedHTTPSConnection("example.com", 443, PUBLIC, 2)
            connection.connect()
            factory.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM, 6)
            factory.return_value.settimeout.assert_called_once_with(2)
            factory.return_value.connect.assert_called_once_with(("8.8.8.8", 443))
            wrap.assert_called_once_with(factory.return_value, server_hostname="example.com")
            connection.close()

    def test_http_serialization_uses_original_host_and_json_body(self):
        context = ssl.create_default_context()
        tls_socket = MagicMock()
        tls_socket.makefile.return_value = io.BytesIO(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        with patch("dispatch.transport.socket.getaddrinfo", return_value=[PUBLIC]), patch("dispatch.transport.socket.socket") as raw, patch("dispatch.transport.ssl.create_default_context", return_value=context), patch.object(context, "wrap_socket", return_value=tls_socket):
            self.assertEqual(deliver(self.event, self.policy), 204)
            wire = b"".join(call.args[0] for call in tls_socket.sendall.call_args_list)
            self.assertIn(b"POST /hook?order=1 HTTP/1.1\r\n", wire)
            self.assertIn(b"Host: example.com\r\n", wire)
            self.assertIn(b"Content-Length: 11\r\n", wire)
            self.assertTrue(wire.endswith(b'{"order":1}'))
            raw.return_value.connect.assert_called_once_with(("8.8.8.8", 443))
            tls_socket.close.assert_called_once()

    def test_failed_tls_handshake_closes_raw_socket(self):
        context = MagicMock()
        context.wrap_socket.side_effect = ssl.SSLError("handshake failed")
        with patch("dispatch.transport.ssl.create_default_context", return_value=context), patch("dispatch.transport.socket.socket") as factory:
            connection = PinnedHTTPSConnection("example.com", 443, PUBLIC, 2)
            with self.assertRaises(ssl.SSLError):
                connection.connect()
            factory.return_value.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
