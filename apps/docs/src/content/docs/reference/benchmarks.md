---
title: Benchmarks
description: Review recorded bun-kafka throughput and codec results.
---

These results are a snapshot, not a capacity promise. Network, broker, partition, message, and
durability settings can change throughput significantly. Benchmark your own workload before you
set production limits.

## Test setup

Measured on 2026-08-24 with Bun 1.4.0, TypeScript 7.0.2, hyperfine 1.20.0, Go
1.27.0, Rust 1.93.1, and a local Redpanda 25.2.1 broker.

## End-to-end client comparison

Each run starts a process, auto-creates a fresh one-partition topic, produces 5,000 100-byte records with leader acknowledgements, manually consumes them from offset zero, and shuts down.

| Client       | Mean wall time [ms] | Relative to franz-go |
| ------------ | ------------------: | -------------------: |
| franz-go     |         210.7 ± 0.7 |                 1.00 |
| bun-kafka    |         239.5 ± 0.7 |                 1.14 |
| rdkafka-rust |         718.5 ± 1.6 |                 3.41 |

In this test, bun-kafka was 14% slower than franz-go and 3.00 times faster than the
rdkafka-rust lane end to end.

Ten direct runs gave these median timed sections:

| Client       | Produce [msg/s] | Consume [msg/s] |
| ------------ | --------------: | --------------: |
| franz-go     |          24,543 |       2,196,377 |
| bun-kafka    |          22,691 |         621,956 |
| rdkafka-rust |          24,486 |           9,923 |

The produce section includes topic discovery and creation. The Rust lane uses
`BaseConsumer::poll` one message at a time. The Go and Bun lanes drain fetched batches, so this
compares these client implementations and APIs, not language speed alone.

## Codec comparison

Both lanes process 20,000 records through encode and decode.

| Command               |  Mean [ms] | Min [ms] | Max [ms] |
| --------------------- | ---------: | -------: | -------: |
| One record per batch  | 88.6 ± 3.2 |     83.7 |     94.1 |
| 100 records per batch | 51.0 ± 1.4 |     48.3 |     53.5 |

Batching 100 records was 1.74 times faster than one-record batches.
