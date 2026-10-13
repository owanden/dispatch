import math
from dataclasses import dataclass


TRANSIENT_ERRORS = frozenset({"request_timeout", "delivery_deadline_exceeded", "dns_error", "connection_error"})


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 30
    max_delay: float = 300

    def __post_init__(self):
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 1000:
            raise ValueError("max_attempts must be between 1 and 1000")
        for delay in (self.base_delay, self.max_delay):
            if type(delay) not in (int, float) or not math.isfinite(delay) or delay <= 0:
                raise ValueError("retry delays must be positive and finite")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be at least base_delay")

    def next_delay(self, attempt_count, *, status_code=None, error=None):
        retryable = status_code in (408, 429) or (status_code is not None and 500 <= status_code < 600) or error in TRANSIENT_ERRORS
        if not retryable or attempt_count >= self.max_attempts:
            return None
        delay = self.base_delay
        for _ in range(attempt_count - 1):
            delay = min(delay * 2, self.max_delay)
            if delay == self.max_delay:
                break
        return delay
