# Dispatch

Durable webhook delivery service.

Implemented: SQLite event storage and idempotent enqueueing. Reusing a key with a different payload or destination raises a conflict. Pending events survive process restarts.

Requires Python 3.12 or newer. No external dependencies.

```sh
python -m unittest discover -s tests -v
```

Delivery workers and HTTP endpoints are pending.
