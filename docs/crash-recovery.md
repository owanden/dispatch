# Worker crash and redelivery demonstration

Run from a checkout with Python 3.12+ and the OpenSSL executable:

```sh
python examples/crash_demo.py --output reports/crash-demo.json
```

Allow roughly a minute for real lease expiry and retry waiting. No clock is advanced and no external receiver or Python package is required. The fixture requires DISPATCH_SIGNING_KEY to be unset; it never clears or prints a configured key.

## Failure boundary

The fixture starts an independent local HTTPS receiver and stores one synthetic event in Dispatch's temporary SQLite database. A separate worker process uses the unchanged Worker class with a two-second delivery deadline, giving its normal deadline-plus-30 lease formula a 32-second lease. The local example executor pins its destination to loopback and trusts an ephemeral certificate; production destination policy and child entry points are unchanged.

The receiver commits a simulated effect row before returning 204. Once the isolated delivery child has returned successfully and exited, an example-only gate pauses the worker before it can store that outcome. The parent kills only this fixture-owned worker and waits for its nonzero exit. This is a real process termination at an injected boundary, not a graceful shutdown, clock jump or claim manufactured directly in SQL. Gating after the delivery child exits avoids orphaning an active delivery process.

A fresh store and another worker process then observe the original event still delivering, with an unfinished attempt and retained lease. An early poll does nothing. Actual lease expiry closes the attempt with lease_expired and persists the default 30-second retry delay. The new worker waits until that schedule permits another real HTTPS request. Final history is lease_expired followed by 204; the first successful HTTP response was never stored as a completed Dispatch outcome.

## Receiver deduplication

Both requests carry the same stable event ID and identical synthetic payload. The fixture receiver uses a primary key on event_id and INSERT OR IGNORE in a separate SQLite database. It accepts both requests but retains one simulated effect row. Dispatch does not perform receiver deduplication.

This illustrates why the receiver must couple its idempotency record to its own effect. It does not prove exactly-once delivery, atomic effects in an external system, production readiness, receiver crash recovery, or any cross-database transaction. The receiver stays running throughout the worker restart. Do not copy the fixture's unauthenticated receiver into production.

## Retained observations and checks

[Recorded run](crash-demo.json) retains synthetic event/attempt identities, actual timing observations, nonzero killed-worker exit, stored history and receiver counts. [Console output](crash-demo-output.txt) records the observed sequence. Timings describe one local functional run, not a recovery-time guarantee or throughput benchmark. The two-second configured deadline is shorter than the worker CLI's default 15 seconds; the default CLI lease would be 45 seconds before retry backoff.

The integration check starts the whole command from outside the checkout, confirms both completed history rows, stable identity, one effect row and no early recovery/redelivery, and permits 140 seconds for startup and waits:

```sh
python -m unittest discover -s tests -p test_crash_demo.py -v
```

The complete source suite includes this check in CI. Temporary worker/receiver databases, certificates, process pipes and loopback servers are cleaned up. Termination is scoped to process handles created by the fixture; no running service is discovered or targeted. This checks worker-process failure with normal SQLite transactions, not host power loss, disk faults, a database crash, arbitrary crash points or cloud deployment.
