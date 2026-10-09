import ipaddress
import re
from urllib.parse import urlsplit


LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", re.ASCII)


def public_address(value):
    address = ipaddress.ip_address(value)
    if (
        not address.is_global
        or address.is_multicast
        or getattr(address, "is_site_local", False)
        or getattr(address, "ipv4_mapped", None) is not None
    ):
        raise ValueError("nonpublic destination address")
    return str(address)


def parse_destination(value):
    if (
        not isinstance(value, str)
        or not value
        or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value)
        or "\\" in value
        or "#" in value
    ):
        raise ValueError("invalid destination")
    parsed = urlsplit(value)
    host = parsed.hostname
    port = parsed.port if parsed.port is not None else 443
    if (
        parsed.scheme != "https"
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or not 1 <= port <= 65535
    ):
        raise ValueError("invalid destination")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if (
            len(host) > 253
            or "." not in host
            or not any(character.isalpha() for character in host)
            or not all(LABEL.fullmatch(label) for label in host.split("."))
            or host.endswith((".localhost", ".local", ".internal"))
        ):
            raise ValueError("invalid destination host")
    else:
        host = public_address(str(address))
    return parsed, (host, port)


class DestinationPolicy:
    def __init__(self, origins=()):
        allowed = set()
        for value in origins:
            parsed, origin = parse_destination(value)
            if parsed.path not in ("", "/") or parsed.query:
                raise ValueError("allowed origins cannot contain paths or queries")
            allowed.add(origin)
        self.origins = frozenset(allowed)

    def allows(self, destination):
        _, origin = parse_destination(destination)
        return origin in self.origins
