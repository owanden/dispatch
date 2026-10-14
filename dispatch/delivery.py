import http.client
import json
import math
import os
import socket
import ssl
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

from dispatch.destinations import DestinationPolicy
from dispatch.store import Event
from dispatch.transport import DeliveryRejected, deliver


ERRORS = frozenset({
    "delivery_rejected", "certificate_verification_failed", "tls_error",
    "request_timeout", "dns_error", "connection_error", "http_protocol_error", "invalid_event",
})


def validate_timeout(timeout):
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    return timeout


def delivery_outcome(event, policy, timeout):
    try:
        status = deliver(event, policy, timeout=timeout)
    except DeliveryRejected:
        return {"error": "delivery_rejected"}
    except ssl.SSLCertVerificationError:
        return {"error": "certificate_verification_failed"}
    except ssl.SSLError:
        return {"error": "tls_error"}
    except TimeoutError:
        return {"error": "request_timeout"}
    except socket.gaierror:
        return {"error": "dns_error"}
    except OSError:
        return {"error": "connection_error"}
    except http.client.HTTPException:
        return {"error": "http_protocol_error"}
    except ValueError:
        return {"error": "invalid_event"}
    return {"status_code": status}


def _run_command(command, request, deadline):
    validate_timeout(deadline)
    isolation = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
    try:
        result = subprocess.run(
            command, input=json.dumps(request, allow_nan=False), capture_output=True,
            text=True, timeout=deadline, check=True,
            cwd=Path(__file__).resolve().parent.parent, **isolation,
        )
    except subprocess.TimeoutExpired:
        return {"error": "delivery_deadline_exceeded"}
    except subprocess.CalledProcessError:
        raise RuntimeError("delivery process failed") from None
    try:
        outcome = json.loads(result.stdout)
    except ValueError:
        raise RuntimeError("invalid delivery process result") from None
    if not isinstance(outcome, dict) or not (
        (set(outcome) == {"status_code"} and type(outcome["status_code"]) is int and 100 <= outcome["status_code"] <= 599)
        or (set(outcome) == {"error"} and isinstance(outcome["error"], str) and outcome["error"] in ERRORS)
    ):
        raise RuntimeError("invalid delivery process result")
    return outcome


def run_delivery(event, policy, timeout=5, deadline=15):
    validate_timeout(timeout)
    validate_timeout(deadline)
    origins = [f"https://{'[' + host + ']' if ':' in host else host}:{port}" for host, port in policy.origins]
    request = {"event": asdict(event), "origins": origins, "timeout": timeout}
    return _run_command([sys.executable, "-m", "dispatch.delivery"], request, deadline)


def main():
    request = json.load(sys.stdin)
    event = Event(**request["event"])
    policy = DestinationPolicy(request["origins"])
    timeout = validate_timeout(request["timeout"])
    print(json.dumps(delivery_outcome(event, policy, timeout)))


if __name__ == "__main__":
    main()
