import http.client
import json
import math
import socket
import ssl
from urllib.parse import quote

from dispatch.destinations import parse_destination, public_address


class DeliveryRejected(ValueError):
    pass


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, port, address, timeout):
        self.context = ssl.create_default_context()
        super().__init__(host, port, timeout=timeout, context=self.context)
        self.address = address

    def connect(self):
        family, socktype, protocol, _, sockaddr = self.address
        raw = socket.socket(family, socktype, protocol)
        try:
            raw.settimeout(self.timeout)
            raw.connect(sockaddr)
            self.sock = self.context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


def resolve_destination(host, port):
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    if not addresses:
        raise DeliveryRejected("no destination addresses")
    for family, socktype, _, _, sockaddr in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6) or socktype != socket.SOCK_STREAM:
            raise DeliveryRejected("unsupported destination address")
        try:
            public_address(sockaddr[0])
        except ValueError as error:
            raise DeliveryRejected("nonpublic DNS result") from error
        if sockaddr[1] != port or (family == socket.AF_INET6 and sockaddr[3] != 0):
            raise DeliveryRejected("invalid destination address")
    return addresses[0]


def deliver(event, policy, timeout=5):
    if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    parsed, (host, port) = parse_destination(event.destination)
    if not policy.allows(event.destination):
        raise DeliveryRejected("destination not allowed")
    payload = json.dumps(event.payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
    target = quote(parsed.path or "/", safe="/%:@!$&'()*+,;=-._~")
    if parsed.query:
        target += "?" + quote(parsed.query, safe="/%?:@!$&'()*+,;=-._~")
    address = resolve_destination(host, port)
    connection = PinnedHTTPSConnection(host, port, address, timeout)
    try:
        connection.request("POST", target, payload, {
            "Content-Type": "application/json",
            "X-Dispatch-Event-ID": event.id,
        })
        response = connection.getresponse()
        if 300 <= response.status < 400:
            raise DeliveryRejected("redirect refused")
        return response.status
    finally:
        connection.close()
