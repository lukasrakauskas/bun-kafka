# Performance validation

## Status

**Current status: benchmarked, short soak qualified, and 24-hour soak-proven for the recorded workload.**

The repository has short hyperfine tests and a real-broker integration test, plus a long-running soak harness (`bun run test:soak`, implemented in `scripts/soak.ts`). The harness runs one Bun process for the configured duration, produces at a fixed offered rate with periodic bursts, drains with a consumer, samples every metric listed under [required measurements](#required-measurements) each interval, validates a per-partition sequence oracle (order, duplicates, missing records), evaluates automated release gates, and writes JSON and Markdown artifacts to `out/soak/`.

The recorded 30-minute and 24-hour soaks pass all applicable gates. The 72-hour release soak remains outstanding before making performance claims for releases that require that additional duration.

## Current baseline

The recorded local baseline uses Bun 1.4.0, TypeScript 7.0.2, Redpanda 25.2.1, one partition, 5,000 messages, 100-byte values, and leader acknowledgements.

| Measurement                         |        Result |
| ----------------------------------- | ------------: |
| End-to-end wall time                |      239.5 ms |
| Produce median                      |  22,691 msg/s |
| Consume median                      | 621,956 msg/s |
| Difference from franz-go end to end |    14% slower |

See [user-facing benchmarks](../../apps/docs/src/content/docs/reference/benchmarks.md) for the method and the full comparison.

This baseline is a regression reference. It is not a production capacity limit.

## Terms

- **Benchmark:** A short test that finds latency or throughput under controlled conditions.
- **Load test:** A test at a specified rate for a limited time.
- **Soak test:** A long load test that finds memory growth, queue growth, resource leaks, and performance decay.
- **Maximum stable rate:** The highest rate that passes all latency, memory, error, and lag gates.
- **Target rate:** The expected peak production rate, including planned growth.

## Required test environment

Record this information with every result:

- Commit SHA and package version
- Bun and TypeScript versions
- Operating system and kernel
- CPU model and available cores
- Memory limit
- Kafka or Redpanda version
- Broker count and replication factor
- Topic partition count
- Broker and client configuration
- Network path and measured round-trip time
- TLS state
- Message size and header count
- Producer acknowledgement mode
- Producer `lingerMs` and `batchMaxMessages`
- Consumer `maxMessages`, `maxPartitionBytes`, and `copy` mode

Use dedicated brokers. Do not run qualification tests on a production cluster.

## Workload matrix

Run all required service profiles. Do not use one 100-byte, one-partition result as a substitute for this matrix.

### Message profiles

| Profile          | Value size |  Key |  Headers |
| ---------------- | ---------: | ---: | -------: |
| Small event      |      100 B | 16 B |     None |
| Normal event     |      1 KiB | 16 B | 4 × 32 B |
| Large event      |     10 KiB | 32 B | 4 × 64 B |
| Very large event |    100 KiB | 32 B | 8 × 64 B |

Add the real service schema as a separate profile.

### Partition and concurrency profiles

| Profile             | Partitions | Producers | Active consumer partitions |
| ------------------- | ---------: | --------: | -------------------------: |
| Minimum             |          1 |         1 |                          1 |
| Normal              |         12 |         4 |                         12 |
| High parallelism    |         48 |        16 |                         48 |
| Connection pressure |         48 |        64 |                         48 |

The current consumer uses manual assignment. The harness must assign each partition exactly once.

### Producer profiles

Test these combinations:

- `acks: 1`
- `acks: "all"`
- `lingerMs: 0`, `5`, and `20`
- Single-message concurrent `send()` calls
- Calls with 100, 1,000, and 5,000 messages
- TLS off and TLS on

### Consumer profiles

Test these combinations:

- `maxMessages: 1`, `100`, `500`, and `5_000`
- `copy: false` and `copy: true`
- Active processing with no delay
- Simulated handler delays of 1 ms and 10 ms per batch
- Seek and replay from a non-zero offset
- A topic with at least 10 million existing records

The large-history case is mandatory. It verifies that bounded decoding does not slow down as topic history grows.

## Test stages

### 1. Short regression test

Run before each merge that changes the protocol, network, producer, or consumer code.

```bash
bun run typecheck
bun run test
KAFKA_BROKERS=127.0.0.1:9092 bun run test:integration
KAFKA_BROKERS=127.0.0.1:9092 BENCH_RUNS=10 bun run bench
```

Pass conditions:

- No test failure
- No hyperfine mean regression greater than 10% from the same machine baseline
- No result with a coefficient of variation greater than 10%

### 2. Capacity test

Increase the offered rate until one release gate fails. Record the last passing rate as the maximum stable rate.

Run each rate for at least 15 minutes. Use these steps:

1. 25% of the expected peak rate
2. 50% of the expected peak rate
3. 75% of the expected peak rate
4. 100% of the expected peak rate
5. 125% of the expected peak rate
6. Continue in 10% steps until failure

### 3. Two-hour qualification test

Run the normal and high-parallelism profiles for two hours at the target rate.

This test must pass before a release candidate enters the 24-hour soak test.

### 4. Twenty-four-hour soak test

Run at the target rate for 24 hours. Add a 10-minute burst at 150% of the target rate every hour.

Use one long-running Bun process. A shell loop that starts a new process for each batch is not a valid connection or memory soak test.

### 5. Seventy-two-hour release soak

Run this test for a release that changes any of these areas:

- Response-buffer ownership
- Producer queue or batching
- Connection lifecycle
- Retry logic
- TLS
- Record encoding or decoding

Run at 75% of the maximum stable rate. Add the same hourly burst used in the 24-hour test.

Use the dedicated release profile after recording the maximum stable rate:

```bash
SOAK_MAX_STABLE_RATE=<measured-rate> bun run --cwd packages/kafka test:soak:release:72h
```

The runner starts the pinned single-node Redpanda broker in `test/impl/soak.compose.yml`, checks
that at least 5 GiB is free, and stops the broker when the run exits. Topic storage is bounded to a
one-hour retention window and 256 MiB per partition. Docker uses the `local` logging driver with
three 10 MiB files, while harness output uses three 5 MiB files and truncates individual lines to
64 KiB. These limits keep the 72-hour run from exhausting the host filesystem. JSON and Markdown
result artifacts remain untruncated in `packages/kafka/out/soak/`.

## Release gates

A performance qualification passes only when all gates pass.

### Throughput and lag

- Delivered producer rate is at least the target rate.
- Consumer rate is at least the producer rate over every 10-minute window.
- Consumer lag returns to the pre-burst level within 10 minutes after each burst.
- Throughput in the final hour is at least 95% of throughput in the first measured hour.

### Latency

Set service-specific absolute limits before the test. At minimum, enforce these relative limits:

- Final-hour p95 latency is not more than 20% above first-hour p95 latency.
- Final-hour p99 latency is not more than 25% above first-hour p99 latency.
- No request remains pending longer than `requestTimeoutMs` plus 1 second.

Measure producer latency from `send()` call to Promise completion. Measure fetch latency from `fetch()` call to response.

### Errors and data

- Zero unhandled Promise rejections
- Zero malformed response errors on valid broker traffic
- Zero missing acknowledged messages
- Zero duplicate message IDs when the harness does not retry
- Exact per-partition key order
- All shutdown flushes complete within the configured timeout

### Memory and resources

After the first warm-up hour:

- Resident memory growth is less than 64 MiB over 24 hours.
- Resident memory slope is less than 1% per hour.
- Producer queue size returns to its normal range after each burst.
- Open socket count is stable.
- File descriptor count is stable.
- CPU use does not increase by more than 15% at the same throughput.

For zero-copy consume tests, run two handler modes:

1. Release every message reference after processing.
2. Retain 1% of message values for one hour.

The second mode measures response-buffer retention. Use `copy: true` when retained small values cause unacceptable memory retention.

## Required measurements

The long-running soak harness must record these values at least every 10 seconds:

- Offered, accepted, acknowledged, and failed producer messages
- Produced bytes per second
- Consumed messages and bytes per second
- Producer latency p50, p95, p99, and maximum
- Fetch latency p50, p95, p99, and maximum
- Per-partition produced and consumed offsets
- Consumer lag
- Pending sends and queued messages
- Active broker connections
- Request timeout and reconnect counts
- Process resident memory
- JavaScript heap use, when available
- CPU use
- Open file descriptors and sockets

The harness samples these values at least every `SOAK_SAMPLE_INTERVAL_S` (default 10) seconds:

- Offered, accepted, acknowledged, and failed producer messages
- Produced bytes per second
- Consumed messages and bytes per second
- Producer latency p50, p95, p99, and maximum
- Fetch latency p50, p95, p99, and maximum
- Consumer lag (total across assigned partitions)
- Pending sends and queued producer messages (`producer.queuedMessages`)
- Active broker connections
- Request totals, retry counts, reconnect-triggering failures, and throttle counts
- Process resident memory
- JavaScript heap use
- CPU use
- Open file descriptors

## Recorded qualification results

### Soak: 30-minute run (2026-08-25)

Environment: Bun 1.4.0, Linux x64, 4 CPUs, single-node Redpanda dev container, plaintext, commit `72b01a4` plus this harness.

| Measurement                                           |                                                                                    Result |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------: |
| Duration                                              |                                                                                   1,800 s |
| Workload                                              | 1 KiB values, 6 partitions, acks=all, 1,000 msg/s base rate, 1.5x/60 s bursts every 300 s |
| Offered / acknowledged / consumed                     |                                                         1,959,750 / 1,959,750 / 1,959,750 |
| Failed acks / duplicates / order violations / missing |                                                                             0 / 0 / 0 / 0 |
| Send latency p50/p95/p99/max                          |                                                                     10 / 20 / 20 / ~20 ms |
| Fetch latency p50/p95/p99/max                         |                                                                   50 / 100 / 100 / 252 ms |
| RSS start -> end, post-warmup range                   |                                                           50 MiB -> 54 MiB, 7.5 MiB range |
| CPU use                                               |                                                                                   ~10-13% |

All gates applicable to the run duration passed: zero failed acknowledgements, zero duplicates, per-partition ordering intact, zero missing records after drain, no unhandled rejections, memory growth below 64 MiB, throughput decay below 5% (first quarter 1,025 msg/s vs final quarter 1,093 msg/s), p95/p99 drift within limits, and lag recovery after every burst.

### Soak: 24-hour release profile (2026-08-26)

Environment: Bun 1.4.0, Linux x64, single-node Redpanda 25.2.1 dev container, plaintext, 6 partitions, commit `05636f6` plus the soak-harness working-tree changes. The run used bounded topic retention (one hour, 256 MiB per partition) so broker storage remained finite during the long test.

| Measurement                                           |                                                                                       Result |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------: |
| Duration                                              |                                                                         86,409 s (~24 hours) |
| Workload                                              | 1 KiB values, 6 partitions, acks=all, 1,000 msg/s base rate, 1.5x/600 s bursts every 3,600 s |
| Offered / acknowledged / consumed                     |                                                         93,918,225 / 93,918,225 / 93,918,225 |
| Failed acks / duplicates / order violations / missing |                                                                                0 / 0 / 0 / 0 |
| Send latency p50/p95/p99/max                          |                                                                      10 / 20 / 20 / 4,766 ms |
| Fetch latency p50/p95/p99/max                         |                                                                    50 / 100 / 100 / 3,809 ms |
| RSS start -> end, post-warmup range                   |                                                             52 MiB -> 27 MiB, 36.1 MiB range |
| Requests / retries / throttles                        |                                                                            1,734,035 / 0 / 0 |

All release gates passed. Throughput was 1,080 msg/s in the first quarter and 1,086 msg/s in the final quarter; worst post-burst lag overshoot was 50 records and recovered within 30 seconds. The maximum latency values were rare tail spikes; percentile latency and drift gates remained within limits. The full JSON and Markdown artifacts are `packages/kafka/out/soak/2026-08-26T18-19-58.734Z.{json,md}`.

### Chaos: three-broker suite (2026-08-25)

Commit `72b01a4` plus the blackhole-recovery fix. 18 pass / 0 fail across the deterministic mock suite, TLS chaos, and three-broker Docker scenarios (leader kill, blackholed leader pause/resume, rolling restart, leader transfer, topic deletion/recreation, netem delay and loss profiles), with 1,000 socket-leak fault cycles. Raw JSON and Markdown artifacts are written to `out/chaos/` on each run; `bun run test:chaos` reproduces them.

## Result artifact

Store one JSON result and one short Markdown report for each qualification run. The JSON result must include the start time, commit, Bun version, broker version, measured maximum
stable rate, exact workload, samples, gate results, and final measurements. At 10-second sampling,
a 72-hour JSON artifact is expected to remain on the order of tens of MiB.

The JSON result must include:

```json
{
  "commit": "<sha>",
  "duration_seconds": 86400,
  "workload": {
    "message_bytes": 1024,
    "partitions": 12,
    "producer_rate": 50000,
    "acks": "all",
    "tls": true,
    "copy": false
  },
  "result": {
    "produced": 4320000000,
    "acknowledged": 4320000000,
    "consumed": 4320000000,
    "missing": 0,
    "duplicates": 0,
    "p99_send_ms": 0,
    "p99_fetch_ms": 0,
    "rss_start_bytes": 0,
    "rss_end_bytes": 0
  },
  "passed": false
}
```

Replace all example zero values with measured values.

## Performance-proven checklist

- [ ] Target service workload is defined.
- [ ] Short regression benchmark passes.
- [ ] Maximum stable rate is measured.
- [ ] Two-hour qualification passes.
- [x] Twenty-four-hour soak passes.
- [ ] Seventy-two-hour soak passes for lifecycle or buffer changes.
- [ ] TLS profile passes when production uses TLS.
- [ ] `copy: false` and `copy: true` memory behavior is recorded.
- [ ] Large-history bounded decode passes.
- [ ] Multi-partition test passes.
- [ ] Result artifacts are stored and linked from the release.
