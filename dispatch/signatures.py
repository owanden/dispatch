import base64
import binascii
import hashlib
import hmac
import math
import os
import re
import time


EVENT_ID = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)
TIMESTAMP = re.compile(r"0|[1-9][0-9]{0,19}", re.ASCII)
SIGNATURE = re.compile(r"v1=[0-9a-f]{64}", re.ASCII)


def validate_key(key):
    if not isinstance(key, bytes) or not 32 <= len(key) <= 128:
        raise ValueError("signing key must contain 32 to 128 bytes")
    return key


def load_signing_key():
    encoded = os.environ.get("DISPATCH_SIGNING_KEY")
    if encoded is None:
        return None
    try:
        return validate_key(base64.b64decode(encoded, validate=True))
    except (ValueError, binascii.Error):
        raise ValueError("DISPATCH_SIGNING_KEY must encode 32 to 128 bytes as base64") from None


def signature_headers(key, event_id, body, *, timestamp=None):
    validate_key(key)
    if not isinstance(event_id, str) or not EVENT_ID.fullmatch(event_id) or not isinstance(body, bytes):
        raise ValueError("invalid signed request")
    timestamp = int(time.time()) if timestamp is None else timestamp
    if type(timestamp) is not int or timestamp < 0 or not TIMESTAMP.fullmatch(str(timestamp)):
        raise ValueError("invalid signing timestamp")
    stamp = str(timestamp)
    message = b"v1\n" + stamp.encode("ascii") + b"\n" + event_id.encode("ascii") + b"\n" + body
    digest = hmac.new(key, message, hashlib.sha256).hexdigest()
    return {"X-Dispatch-Timestamp": stamp, "X-Dispatch-Signature": "v1=" + digest}


def verify_signature(key, event_id, body, timestamp, signature, *, now=None, tolerance=300):
    validate_key(key)
    if type(tolerance) not in (int, float) or not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError("tolerance must be finite and nonnegative")
    now = time.time() if now is None else now
    if type(now) not in (int, float) or not math.isfinite(now):
        raise ValueError("verification time must be finite")
    if (
        not isinstance(event_id, str) or not EVENT_ID.fullmatch(event_id)
        or not isinstance(body, bytes)
        or not isinstance(timestamp, str) or not TIMESTAMP.fullmatch(timestamp)
        or not isinstance(signature, str) or not SIGNATURE.fullmatch(signature)
    ):
        return False
    stamp = int(timestamp)
    if abs(now - stamp) > tolerance:
        return False
    expected = signature_headers(key, event_id, body, timestamp=stamp)["X-Dispatch-Signature"]
    return hmac.compare_digest(expected, signature)
