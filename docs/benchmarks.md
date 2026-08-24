# Bun-native benchmark

Measured with Bun 1.4.0, TypeScript 7.0.2, hyperfine 1.20.0, Go 1.27.0, Rust 1.93.1, and local Redpanda 25.2.1 on 2026-08-24.

```bash
bun run bench:build
KAFKA_BROKERS=127.0.0.1:19092 \
BENCH_RUNS=10 BENCH_COUNT=20000 BENCH_KAFKA_COUNT=5000 \
bun run bench
```

## End-to-end client comparison

Each run starts a process, auto-creates a fresh one-partition topic, produces 5,000 100-byte records with leader acknowledgements, manually consumes them from offset zero, and shuts down.

| Client | Mean wall time [ms] | Relative to franz-go |
|---|---:|---:|
| franz-go | 211.3 ± 1.0 | 1.00 |
| bun-kafka | 268.5 ± 1.5 | 1.27 |
| rdkafka-rust | 718.1 ± 2.2 | 3.40 |

bun-kafka was 27% slower than franz-go and 2.67 times faster than this rdkafka-rust lane end to end.

Ten direct runs gave these median timed sections:

| Client | Produce [msg/s] | Consume [msg/s] |
|---|---:|---:|
| franz-go | 24,543 | 2,196,377 |
| bun-kafka | 20,958 | 270,752 |
| rdkafka-rust | 24,486 | 9,923 |

The produce section includes topic discovery and creation. The Rust lane uses `BaseConsumer::poll` one message at a time. The Go and Bun lanes drain fetched batches, so this measures these client implementations and APIs, not language speed alone.

## Codec comparison

Both lanes process 20,000 records through encode and decode.

| Command | Mean [ms] | Min [ms] | Max [ms] |
|---|---:|---:|---:|
| One record per batch | 179.0 ± 4.7 | 171.4 | 188.3 |
| 100 records per batch | 129.2 ± 1.6 | 126.9 | 131.7 |

Batching 100 records was 1.39 times faster than one-record batches.
