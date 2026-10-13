# Dispatch

Durable webhook delivery service.

SQLite event storage, idempotent enqueueing, and a local HTTP API. Reusing a key with a different payload or destination returns a conflict. Pending events survive process restarts.

Requires Python 3.12 or newer. No external dependencies.

```sh
python -m dispatch.api --db events.db --port 8080 --allow-origin https://example.com
python -m dispatch.worker --db events.db --allow-origin https://example.com --timeout 5 --deadline 15 --max-attempts 3
python -m unittest discover -s tests -v
```

The API binds to `127.0.0.1`. `POST /events` accepts a JSON object with `destination` (HTTPS URL) and `payload` (object), plus an `Idempotency-Key` header of up to 256 characters. Bodies are limited to 64 KiB. Successful requests return 202 with the stored event. Conflicting reuse returns 409. `GET /events/{id}` returns the event or 404.

Enqueueing requires an explicitly permitted HTTPS origin. Repeat `--allow-origin` for multiple origins. Each entry contains a hostname and optional port, without a path or query. Hostnames are case-insensitive, and omitted ports equal 443. Subdomains and other ports require separate entries. Unlisted origins return 403; invalid destinations return 400. No configured origins means all enqueue requests are denied. Private IP literals, local hostnames, credentials, fragments, and ambiguous URL syntax are rejected.

The delivery transport rechecks the origin policy, rejects DNS answers containing nonpublic addresses, and connects directly to a validated address while verifying the original TLS hostname. It sends JSON with an X-Dispatch-Event-ID header and refuses redirects. Each attempt resolves the destination again. Socket operations use a configurable timeout (five seconds by default). The worker runs each delivery in a child process with a separate --deadline (15 seconds by default), covering child startup after process creation, DNS resolution, and the HTTP exchange. Expiry terminates and reaps the child before recording delivery_deadline_exceeded. Operating-system process creation and cleanup can add overhead; database operations are outside this delivery deadline. Direct calls to the low-level transport retain only socket timeouts.

Storage supports transactional pending-event claims and durable delivery attempts. A claim moves one event to delivering and creates its unfinished attempt together. Completing an attempt records an HTTP status or error category and updates the event to delivered for 2xx responses or failed otherwise. Duplicate completion is rejected. Existing event databases are extended without replacing their records. Claims do not expire yet; interrupted attempts remain delivering until crash recovery is implemented.

The worker command processes at most one pending event per invocation and prints whether an event was processed. It requires its own explicit permitted origins, rechecks stored destinations, invokes the transport, and records HTTP outcomes or fixed error categories without storing raw exception text. Transient failures are scheduled for retry and remain pending until due; each worker invocation processes at most one due event. HTTP 408, 429, and 5xx responses, plus timeout, deadline, DNS, and connection errors, are retryable. Policy rejection, certificate errors, and other permanent failures remain failed. --max-attempts includes the first attempt and defaults to 3; set it to 1 to disable retries. Backoff starts at 30 seconds, doubles after each retryable failure, and is capped at 300 seconds. Exhausted events remain failed with their complete attempt history. Schedules survive restarts and are saved atomically with outcomes. If outcome storage fails or execution is interrupted, the claim remains unfinished; repeating the command does not immediately resend that event.

Continuous worker polling, claim expiration, and crash recovery are pending. Retry attempts retain the same X-Dispatch-Event-ID; receivers should deduplicate it because a timeout or lost response can occur after acceptance. An unfinished claim does not prove whether the receiver accepted the request. The API only stores events; it does not invoke the transport. Authentication is pending; the API is intended for local use.
