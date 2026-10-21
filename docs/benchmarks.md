# Local delivery measurements

Two single-run samples on October 20, 2026 exercise the unchanged production runtime from `ff232bd` with `examples/benchmark.py`. Raw observations and environment versions are retained in [baseline JSON](benchmarks/windows-baseline.json) and [retry JSON](benchmarks/windows-retry.json). These small local samples describe this workload; they do not establish production capacity, a scalability limit, or a statistically stable comparison.

## Reproduce

Requires Python 3.12+ and the OpenSSL executable, with no external Python packages or services. Run the workloads separately:

```sh
python examples/benchmark.py --events 20 --retry-every 0 --output reports/baseline.json
python examples/benchmark.py --events 20 --retry-every 5 --output reports/retry.json
```

The second command waits for the real default 30-second retry delay. No clock is advanced. Each command creates an empty temporary SQLite database, an HTTP API, and a local HTTPS receiver with ephemeral certificates, then removes them. Outputs contain synthetic event IDs and measurements. Existing reports are overwritten only at the explicitly supplied output path. `--events` accepts 1..1000; `--retry-every 0` disables injected failures. The script exits unsuccessfully on unexpected outcomes, history mismatches, early retries, or its bounded workload timeout.

The API sequentially accepts a batch of distinct events before one worker starts draining. Payloads contain an index and 1,024 ASCII data bytes, plus JSON structure. A new isolated delivery process and TLS connection serve each attempt. Every fifth event in the failure run receives 503 on its first request and 204 on its retry; all other requests receive 204. Durable attempt histories are checked against receiver observations. The benchmark polls every 50 ms while idle, using normal retry policy and wall-clock scheduling. Signing was absent in both recorded runs.

As in the demo, only the example delivery child maps its destination to loopback and trusts the temporary certificate. TLS hostname checks remain active, but public DNS lookup and WAN costs are excluded. Production destination policy is unchanged.

## Recorded samples

| Measurement | No injected failures | Every fifth event fails once |
|---|---:|---:|
| Delivered / permanently failed | 20 / 0 | 20 / 0 |
| HTTPS attempts | 20 | 24 |
| Receiver responses | 20 x 204 | 20 x 204; 4 x 503 |
| Enqueue batch wall time | 0.342 s | 0.246 s |
| Enqueue request p50 / p95 | 11.96 / 34.21 ms | 11.55 / 13.06 ms |
| Worker drain wall time | 6.770 s | 37.188 s |
| Delivered events / worker drain second | 2.954 | 0.538 |
| Worker attempt p50 / p95 | 333.54 / 362.98 ms | 339.03 / 358.21 ms |
| Child execution p50 / p95 | 311.36 / 339.02 ms | 314.76 / 332.95 ms |
| Accepted-to-delivered p50 / p95 | 3.620 / 6.466 s | 4.230 / 35.591 s |
| Receiver 503-to-204 p50 / max | n/a | 30.381 / 30.389 s |

Environment: Windows 11, AMD64, Python 3.12.8. Exact SQLite, TLS library, certificate-tool versions and logical CPU count are in each raw report. Database and certificate setup/server startup are outside measured batch/drain intervals. Hostnames, user paths, and credentials are omitted. The operating system and local filesystem, other machine activity, interpreter startup, and SQLite connection/transaction costs influence these results.

## Repeated trials, October 21

Three fresh pairs ran sequentially in baseline/retry order using the unchanged harness and source checkout `edba88cf1a8aa3121d7b279c62a6800c4e1ee10c`. Each run used a new temporary database, receiver and certificate, the same 20-event payload, one worker and no signing. There were no warmups or simultaneous benchmark runs. All six reports have the same recorded environment as the original samples: Windows 11 AMD64, 16 logical CPUs, Python 3.12.8, SQLite 3.45.3 and OpenSSL 3.1.0. Other host activity was not controlled.

Run the two commands in the reproduction section three times, using distinct output filenames. The retained files below are the direct harness outputs. Counts, event identities, receiver sequences, retry intervals and all distribution summaries were independently recomputed from their observations.

| Mode / trial (raw JSON) | Drain (s) | Delivered / attempts | Attempt p95 (ms) | Accepted-to-delivered p95 (s) | Retry interval min..max (s) |
|---|---:|---:|---:|---:|---:|
| [baseline 1](benchmarks/windows-repeated-2026-10-21/baseline-1.json) | 5.143 | 20 / 20 | 266.47 | 4.918 | n/a |
| [retry 1](benchmarks/windows-repeated-2026-10-21/retry-1.json) | 35.519 | 20 / 24 | 562.44 | 34.384 | 30.253..30.587 |
| [baseline 2](benchmarks/windows-repeated-2026-10-21/baseline-2.json) | 4.999 | 20 / 20 | 261.52 | 4.742 | n/a |
| [retry 2](benchmarks/windows-repeated-2026-10-21/retry-2.json) | 35.933 | 20 / 24 | 274.99 | 34.822 | 30.255..30.310 |
| [baseline 3](benchmarks/windows-repeated-2026-10-21/baseline-3.json) | 6.736 | 20 / 20 | 800.23 | 6.503 | n/a |
| [retry 3](benchmarks/windows-repeated-2026-10-21/retry-3.json) | 35.374 | 20 / 24 | 339.93 | 34.502 | 30.245..30.653 |

Worker drain across the three runs per mode: baseline: median 5.143 s, observed range 4.999..6.736 s; retry: median 35.519 s, observed range 35.374..35.933 s. These are medians of per-run drain durations, not pooled latency percentiles or confidence intervals. Every run delivered 20 events without permanent failures; the retry runs each persisted four 503-to-204 sequences with real retry delays of at least 30 seconds.

The baseline spread is noticeable on this host. No implementation was optimized between trials, so the faster samples are not evidence of a speedup. Three small repetitions do not establish stable capacity, explain host variance, or replace a concurrency/size sweep. The retry workload remains dominated by its intentional waiting interval. No WAN, DNS, signing, crash-recovery or production claim follows from these results.

## Metric definitions and limits

- Enqueue latency spans the client HTTP request through reading the complete 202 response. Batch throughput divides event count by the complete sequential enqueue interval.
- Worker drain starts before the first poll and ends after the last delivered-state check. It includes attempts, storage, status checks, retry waiting, idle polling, and small driver overhead. Its throughput is a workload completion rate; the retry run deliberately spends time waiting.
- Worker attempt duration spans one successful `process_next()` call, including recovery scan, claim, child, and outcome storage. Child duration wraps the isolated executor, including process startup, serialization, TLS request, response parsing, and process cleanup. It also includes the example child's mock-resolution setup/import overhead. This does not separately profile interpreter startup or TLS and is not a timing of the unmodified production child entry point.
- Accepted-to-delivered latency starts after each enqueue response and ends after the worker persists delivery and the driver checks it. It includes waiting for the rest of the batch to enqueue and earlier events to finish; it is not isolated transport latency.
- Recovery interval here means the receiver's first 503 observation to its successful retry observation, both on a monotonic clock in the same process. It measures scheduled transient-error retry, not crash recovery, lease expiry, outage detection, or exactly-once effects.
- Percentiles use nearest rank on raw per-event/per-attempt observations. A p95 from 20 or 24 observations is descriptive only. There are no warmups, repeated trials, concurrency sweeps, large databases, external network calls, signature verification measurements, or production traffic in these samples.

The child interval accounts for most observed worker-attempt time in this workload. That suggests process/transport profiling before throughput optimization, but these samples cannot attribute the cost to a specific component. The failure workload confirms persisted 503-to-204 outcomes and real retry spacing; it does not prove crash durability, delivery uniqueness, or receiver deduplication. Further measurements should add repeated trials and larger workloads before drawing capacity conclusions.
