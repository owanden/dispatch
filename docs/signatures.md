# Signed deliveries

Set `DISPATCH_SIGNING_KEY` in the worker environment to a base64-encoded random secret containing 32 to 128 decoded bytes. Generate at least 32 random bytes with a cryptographic random generator and share the key securely with the receiver. Keep it outside source control, command arguments, and logs. The worker rejects malformed configuration before claiming an event. Restart workers when changing keys; coordinated rotation requires receiver-side handling.

When the variable is absent, requests remain unsigned. A receiver requiring authentication must reject missing signatures. Signing authenticates delivery requests; it does not authenticate the enqueue/status API. HTTPS, destination restrictions, and TLS verification still apply.

## Wire format

Each signed request contains:

- `X-Dispatch-Event-ID`: stable event identifier, unchanged across retries.
- `X-Dispatch-Timestamp`: canonical decimal Unix seconds, generated for each attempt.
- `X-Dispatch-Signature`: `v1=` followed by the lowercase hex HMAC-SHA256 digest.

The exact signed bytes are:

```text
v1\n<timestamp>\n<event-id>\n<raw request body>
```

Here `\n` is a single LF byte. There is no extra trailing newline. Timestamp and event ID use ASCII. Verify the original body bytes before JSON parsing or serialization; whitespace changes invalidate the signature. Event IDs contain 1 to 128 ASCII letters, digits, underscores, or hyphens. Timestamps contain no leading zeroes except `0` itself.

## Receiver verification

The receiver can use `dispatch.signatures.verify_signature` with a decoded key:

```python
from dispatch.signatures import verify_signature

valid = verify_signature(
    key,
    event_id,
    raw_body,
    timestamp_header,
    signature_header,
)
```

Read exactly one occurrence of each header; reject duplicates before calling the verifier. The helper rejects malformed/missing values and checks the HMAC with `hmac.compare_digest`. Its default clock tolerance is 300 seconds in either direction, inclusive. Keep sender/receiver clocks synchronized. `now` and `tolerance` keyword arguments support explicit receiver policy and deterministic validation. Invalid key or verifier configuration raises `ValueError`; invalid requests return `False`.

Timestamp freshness bounds replay age but does not prevent replay inside that window. Persistently deduplicate the stable event ID before applying effects, including across retries. Failed authentication must not apply effects. A worker uses one key for all its permitted origins; only share a worker/key among receivers within the same trust boundary. There is no per-destination key registry, durable replay cache, or automatic key rotation.

Tests cover body/event/timestamp tampering, wrong keys, clock boundaries, missing/malformed headers, invalid configuration before claims, and signed/unsigned requests against a real local HTTPS receiver. They use synthetic keys and temporary certificates.
