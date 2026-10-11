import argparse
import http.client
import json
import math
import socket
import ssl

from dispatch.destinations import DestinationPolicy
from dispatch.store import EventStore
from dispatch.transport import DeliveryRejected, deliver


def validate_timeout(timeout):
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    return timeout


class Worker:
    def __init__(self, store, policy, timeout=5):
        self.timeout = validate_timeout(timeout)
        self.store = store
        self.policy = policy

    def process_next(self):
        claim = self.store.claim_next()
        if claim is None:
            return False
        try:
            status = deliver(claim.event, self.policy, timeout=self.timeout)
        except DeliveryRejected:
            outcome = {"error": "delivery_rejected"}
        except ssl.SSLCertVerificationError:
            outcome = {"error": "certificate_verification_failed"}
        except ssl.SSLError:
            outcome = {"error": "tls_error"}
        except TimeoutError:
            outcome = {"error": "request_timeout"}
        except socket.gaierror:
            outcome = {"error": "dns_error"}
        except OSError:
            outcome = {"error": "connection_error"}
        except http.client.HTTPException:
            outcome = {"error": "http_protocol_error"}
        except ValueError:
            outcome = {"error": "invalid_event"}
        else:
            outcome = {"status_code": status}
        self.store.finish_attempt(claim.attempt_id, **outcome)
        return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="events.db")
    parser.add_argument("--allow-origin", action="append", required=True)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()
    try:
        timeout = validate_timeout(args.timeout)
        policy = DestinationPolicy(args.allow_origin)
    except ValueError as error:
        parser.error(str(error))
    worker = Worker(EventStore(args.db), policy, timeout)
    print(json.dumps({"processed": worker.process_next()}))


if __name__ == "__main__":
    main()
