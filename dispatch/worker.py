import argparse
import json
import logging
import time
import signal
from contextlib import contextmanager
from threading import Event

from dispatch.delivery import run_delivery, validate_timeout
from dispatch.destinations import DestinationPolicy
from dispatch.retries import RetryPolicy
from dispatch.store import EventStore


logger = logging.getLogger("dispatch.worker")


def record(kind, *, level=logging.INFO, **fields):
    logger.log(level, json.dumps({"timestamp": time.time(), "type": kind, **fields}, allow_nan=False))


@contextmanager
def shutdown_signals(stop_event):
    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda signum, frame: stop_event.set())
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)



class Worker:
    def __init__(self, store, policy, timeout=5, deadline=15, *, executor=run_delivery, retry_policy=None):
        self.timeout = validate_timeout(timeout)
        self.deadline = validate_timeout(deadline)
        self.store = store
        self.policy = policy
        self.executor = executor
        self.retry_policy = RetryPolicy() if retry_policy is None else retry_policy

    def process_next(self):
        recovered = self.store.recover_expired(retry_policy=self.retry_policy)
        if recovered:
            record("claims_recovered", count=recovered)
        claim = self.store.claim_next(lease_seconds=self.deadline + 30)
        if claim is None:
            return False
        started = time.monotonic()
        identifiers = {"event_id": claim.event.id, "attempt_id": claim.attempt_id}
        try:
            outcome = self.executor(claim.event, self.policy, timeout=self.timeout, deadline=self.deadline)
        except Exception as error:
            record("attempt_execution_failed", level=logging.ERROR, **identifiers, exception_type=type(error).__name__)
            raise
        try:
            self.store.finish_attempt(claim.attempt_id, retry_policy=self.retry_policy, **outcome)
        except Exception as error:
            record("attempt_completion_failed", level=logging.ERROR, **identifiers, exception_type=type(error).__name__)
            raise
        record("attempt_finished", **identifiers, outcome=outcome, elapsed_seconds=time.monotonic() - started)
        return True


    def run(self, stop_event, poll_interval=1):
        validate_timeout(poll_interval)
        processed = 0
        while not stop_event.is_set():
            if self.process_next():
                processed += 1
            else:
                stop_event.wait(poll_interval)
        return processed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="events.db")
    parser.add_argument("--allow-origin", action="append", required=True)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--deadline", type=float, default=15)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=1)
    args = parser.parse_args()
    try:
        poll_interval = validate_timeout(args.poll_interval)
        timeout = validate_timeout(args.timeout)
        deadline = validate_timeout(args.deadline)
        policy = DestinationPolicy(args.allow_origin)
        retry_policy = RetryPolicy(max_attempts=args.max_attempts)
    except ValueError as error:
        parser.error(str(error))
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    worker = Worker(EventStore(args.db), policy, timeout, deadline, retry_policy=retry_policy)
    stop_event = Event()
    with shutdown_signals(stop_event):
        processed = worker.run(stop_event, poll_interval) if args.continuous else (False if stop_event.is_set() else worker.process_next())
    print(json.dumps({"processed": processed}))


if __name__ == "__main__":
    main()
