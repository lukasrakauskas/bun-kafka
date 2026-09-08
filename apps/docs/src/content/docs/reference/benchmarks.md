---
title: Benchmarks
description: Review recorded bun-kafka throughput and codec results.
---

These results are a snapshot, not a capacity promise. Network, broker, partition, message, and
durability settings can change throughput significantly. Benchmark your own workload before you
set production limits.

## Test setup

Measured on 2026-09-08 at commit `4b88c455f611d32b86b26b014d733d741a6c210d`, on an Intel
Core i5-4570 (four cores), 7.7 GiB RAM, Linux 6.12.63, Bun 1.4.0, hyperfine 1.20.0,
Go 1.27.1, and Rust 1.96.1. Redpanda 25.2.1 ran locally in Docker with two broker
threads and a 2 GiB memory budget. This is a shared host with other services running;
these measurements are not a controlled regression comparison with previous snapshots.

Each lane runs sequentially, with two warmups and ten measured processes. Wall-time
means and sample standard deviations come from hyperfine; timed-section tables report
medians from ten separate direct runs. All samples, including outliers, are retained in
[the raw results and reproduction instructions](https://github.com/lukasrakauskas/bun-kafka/tree/main/packages/kafka/bench/results/2026-09-08).

## End-to-end client comparison

Each run starts a process, auto-creates a fresh one-partition topic, produces 5,000
100-byte records with leader acknowledgements, manually consumes them from offset zero,
and shuts down.

| Client       | Mean wall time [ms] | Relative to franz-go |
| ------------ | ------------------: | -------------------: |
| franz-go     |         218.2 ± 2.1 |                 1.00 |
| bun-kafka    |         278.5 ± 2.6 |                 1.28 |
| rdkafka-rust |         724.3 ± 2.8 |                 3.32 |

In this test, bun-kafka took 28% longer than franz-go; the Rust lane took 2.60 times
as long as bun-kafka end to end.

Ten direct runs gave these median timed sections:

| Client       | Produce [msg/s] | Consume [msg/s] |
| ------------ | --------------: | --------------: |
| franz-go     |          24,027 |       1,386,589 |
| bun-kafka    |          20,947 |         371,738 |
| rdkafka-rust |          24,082 |           9,840 |

The produce section includes topic discovery and creation. The Rust lane uses
`BaseConsumer::poll` one message at a time. The Go and Bun lanes drain fetched batches, so this
compares these client implementations and APIs, not language speed alone.

The alternate `produce_consume.ts` Bun entry point was also rerun: median produce
20,988 msg/s and consume 373,288 msg/s. It implements the same workload.

### KafkaJS (different workload)

KafkaJS 2.2.4 on Bun measured median produce **57,236 msg/s** and consume **92,372 msg/s**
for 5,000 100-byte records. This lane explicitly creates its topic before timing production,
sends batches of 1,000 with KafkaJS's default all-replica acknowledgements, and uses a consumer
group with `eachMessage`. It is not an equivalent comparison with the manual-assignment,
leader-acknowledgement lanes above.

## Warmed client comparison (separate host)

A separate 2026-09-08 run used the merged library at `235cb5b`, an AMD Ryzen 7 5700X3D,
31.3 GiB RAM, Bun 1.4.2, Go 1.27.0, Rust 1.93.1, and local Redpanda 25.2.1. Do not
compare these numbers directly with the Intel-host startup results above.

Topics and clients are prepared before timing. Each persistent client produces and consumes
100 warmup windows, then runs the measured workload. Settings match at the application level:
one partition, leader acknowledgements, no idempotence or compression, zero linger, bounded
send windows, and 1 MiB fetch limits. Native wire batch boundaries can differ. These are finite
workloads with content validation, not maximum sustainable capacity measurements.

For 1,000 windows of 100 records × 100 bytes, medians of three processes were:

| Client / send API         | Produce [msg/s] | Consume [msg/s] | Window ack p99 [ms] | Peak RSS [MiB] |
| ------------------------- | --------------: | --------------: | ------------------: | -------------: |
| bun-kafka / individual    |         256,111 |       1,126,038 |               1.344 |           93.0 |
| bun-kafka / bulk          |         249,416 |       1,173,956 |               1.804 |          199.8 |
| franz-go / individual     |         456,746 |       3,723,650 |               0.484 |           21.7 |
| rdkafka-rust / individual |         289,149 |       1,972,433 |               0.607 |           12.4 |

Consumer resume and one priming window are outside the consume timer. Ack percentiles measure
window enqueue-through-final-ack duration, not per-record end-to-end latency. RSS is sampled
client-process memory. Bun did not beat either native lane on throughput in these workloads.

[Full settings, p50/p95/p99, CPU/RSS, small-message and large-response results, and raw samples](https://github.com/lukasrakauskas/bun-kafka/tree/main/packages/kafka/bench/results/2026-09-08-steady)
also record these limits:

- All 12 one-second broker-pause trials completed with exact record validation. After hard restart,
  Go completed three trials; Bun surfaced connection errors and the Rust benchmark stopped on
  `AllBrokersDown`. Retry policies and fail-fast error handling differ, so this is not an inherent
  client recovery ranking.
- Retaining 8 MiB of sparse payloads referenced 128.05 MiB of backing allocations with
  `copy: false`, versus 8 MiB with `copy: true`. This is an ownership measurement on the merged
  code, not an isolated regression attributed to the response-framing change.

## Codec comparison

Both lanes process 20,000 records through encode and decode; timings include process startup.

| Command               |   Mean [ms] | Min [ms] | Max [ms] |
| --------------------- | ----------: | -------: | -------: |
| One record per batch  | 145.3 ± 9.8 |    137.6 |    167.5 |
| 100 records per batch | 88.7 ± 26.3 |     75.8 |    162.6 |

The one-record lane took 1.64 times as long on average. Hyperfine flagged statistical
outliers in the 100-record lane; its large variance limits conclusions about that ratio.

## Compressed decoding

Median timed decode duration for 100,000 records, in 100-record batches with 1,024-byte
values. Encoding and 100 warmup decodes are outside the timed section. Period 0 uses
deterministic incompressible bytes; periods 1 and 8 use repeated bytes and eight-byte patterns.

| Codec  | Period 0 [ms] | Period 1 [ms] | Period 8 [ms] |
| ------ | ------------: | ------------: | ------------: |
| gzip   |         520.5 |         115.3 |         121.1 |
| snappy |         383.5 |         196.5 |         318.7 |
| lz4    |         379.0 |         200.7 |         241.2 |
| zstd   |         375.4 |         118.9 |         184.4 |

## Producer dispatch

Synthetic routing, encoding, dispatch, and acknowledgement decoding with a mocked cluster;
no broker or network cost. Each process warms up twice, then measures ten bursts of 10,000
single-message sends (100-byte values). Both lanes produced ten mock requests for 100,000 records.

| Linger [ms] | Median timed duration [ms] |
| ----------- | -------------------------: |
| 0           |                      183.1 |
| 5           |                      180.2 |

## TCP transport

Synthetic sequential loopback request/reply, not a Kafka broker benchmark. Each process warms
up with 100 round trips, then measures 10,000 requests and validates response size and contents.

| Response payload | Median timed duration [ms] |
| ---------------- | -------------------------: |
| 1 KiB            |                      322.4 |
| 64 KiB           |                      647.7 |
| 1 MiB            |                    9,815.8 |
