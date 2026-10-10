# Dispatch

Durable webhook delivery service.

SQLite event storage, idempotent enqueueing, and a local HTTP API. Reusing a key with a different payload or destination returns a conflict. Pending events survive process restarts.

Requires Python 3.12 or newer. No external dependencies.

```sh
python -m dispatch.api --db events.db --port 8080 --allow-origin https://example.com
python -m unittest discover -s tests -v
```

The API binds to `127.0.0.1`. `POST /events` accepts a JSON object with `destination` (HTTPS URL) and `payload` (object), plus an `Idempotency-Key` header of up to 256 characters. Bodies are limited to 64 KiB. Successful requests return 202 with the stored event. Conflicting reuse returns 409. `GET /events/{id}` returns the event or 404.

Enqueueing requires an explicitly permitted HTTPS origin. Repeat `--allow-origin` for multiple origins. Each entry contains a hostname and optional port, without a path or query. Hostnames are case-insensitive, and omitted ports equal 443. Subdomains and other ports require separate entries. Unlisted origins return 403; invalid destinations return 400. No configured origins means all enqueue requests are denied. Private IP literals, local hostnames, credentials, fragments, and ambiguous URL syntax are rejected.

The delivery transport rechecks the origin policy, rejects DNS answers containing nonpublic addresses, and connects directly to a validated address while verifying the original TLS hostname. It sends JSON with an X-Dispatch-Event-ID header and refuses redirects. Each attempt resolves the destination again. Socket operations use a configurable timeout (five seconds by default); system DNS resolution and the total request duration do not yet have a wall-clock deadline.

Storage supports transactional pending-event claims and durable delivery attempts. A claim moves one event to delivering and creates its unfinished attempt together. Completing an attempt records an HTTP status or error category and updates the event to delivered for 2xx responses or failed otherwise. Duplicate completion is rejected. Existing event databases are extended without replacing their records. Claims do not expire yet; interrupted attempts remain delivering until crash recovery is implemented.

Delivery workers, automatic retries, and crash recovery are pending. The API only stores events; it does not invoke the transport. Authentication is pending; the API is intended for local use.
