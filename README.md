# Dispatch

Durable webhook delivery service.

SQLite event storage, idempotent enqueueing, and a local HTTP API. Reusing a key with a different payload or destination returns a conflict. Pending events survive process restarts.

Requires Python 3.12 or newer. No external dependencies.

```sh
python -m dispatch.api --db events.db --port 8080
python -m unittest discover -s tests -v
```

The API binds to `127.0.0.1`. `POST /events` accepts a JSON object with `destination` (HTTPS URL) and `payload` (object), plus an `Idempotency-Key` header of up to 256 characters. Bodies are limited to 64 KiB. Successful requests return 202 with the stored event. Conflicting reuse returns 409. `GET /events/{id}` returns the event or 404.

Delivery workers are pending. Destinations are stored without making outbound requests. Authentication and network destination restrictions are pending; the API is intended for local use.
