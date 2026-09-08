# Consumer drain fast path

Base revision: `9e1173e` (merged producer-retention PR #62).
The only library change is in `src/consumer/message-decoder.ts`.

When no deserializer is configured, a decoded fragment belongs to one partition
and contains ordered messages. Update its position once, using the last returned
offset plus one, rather than constructing a partition key and updating the map
for every record. Reuse the decoder's output array when the accumulated output
is empty. Append subsequent fragments without an argument spread, so large fetches
do not depend on the JavaScript argument-count limit.

Custom deserializers retain per-record position updates, including the update
before a callback throws. Empty or filtered fragments do not advance positions.
The wire decoder, checksum validation, producer, and payload copy policy are unchanged.

## Evidence and limits

Fresh CPU profiles on the base revision identified CRC32C as about half of sampled
CPU time for large records. An eight-byte slicing experiment was much slower on
Bun 1.4.2 and was removed. The original four-byte CRC implementation is unchanged.
The next experiment removed raw consumer conversion work; the initial three-run
Bun-only comparison showed about 13–14% higher small-record consume throughput.
The final comparison below uses the final refactored code and includes native clients.
Do not combine these exploratory and final samples into one estimate.

`before.json` and `after.json` contain all 72 successful process samples and every
window duration: three repetitions, three profiles, four client/API lanes, two
revisions. The unchanged `bench/run-steady.ts` validates exact payload bytes, keys,
partitions, ordering, offsets, and delivery results. No samples were removed.

Host and settings match the [original steady benchmark](../2026-09-08-steady/README.md):
Ryzen 7 5700X3D, Linux 7.1.9-arch1-2, Bun 1.4.2, Go 1.27.0, Rust 1.93.1,
franz-go 1.18.1, rdkafka-rust 0.37.0. The approved dedicated Redpanda 25.2.1
broker uses two threads and 2 GiB RAM on `127.0.0.1:19092`.
Each complete before suite ran before the after suite. Client order rotates
within each suite; revision order is not interleaved. This shared desktop has
run variation. Three repetitions are descriptive, not statistical proof.

Medians of three process results, calculated before rounding:

| Profile        | Bun send API | Consume before msg/s | Consume after msg/s | RSS before MiB | RSS after MiB |
| -------------- | ------------ | -------------------: | ------------------: | -------------: | ------------: |
| small-burst    | individual   |            1,263,432 |           1,342,847 |          101.1 |          96.2 |
| small-burst    | bulk         |            1,187,364 |           1,325,570 |           98.2 |          95.1 |
| large-response | individual   |                8,916 |               8,835 |           80.3 |          80.9 |
| large-response | bulk         |                8,742 |               8,825 |           78.8 |          79.1 |

Small-record consume throughput increased 6.3% and 11.6%. Large-record throughput
is essentially unchanged; CRC cost still dominates that workload. RSS is sampled
whole-process peak RSS, not consumer-only memory. No general memory or producer
speed improvement is claimed. One-record consume rates are omitted because the
tiny backlog largely measures buffer draining, as in the original benchmark.

Native comparisons from the after suite:

| Profile        | Client / API     | Produce msg/s | Consume msg/s | RSS MiB |
| -------------- | ---------------- | ------------: | ------------: | ------: |
| small-burst    | Bun / individual |       291,320 |     1,342,847 |    96.2 |
| small-burst    | Bun / bulk       |       320,111 |     1,325,570 |    95.1 |
| small-burst    | franz-go         |       505,513 |     4,233,128 |    22.4 |
| small-burst    | rdkafka-rust     |       325,377 |     2,299,871 |    12.4 |
| large-response | Bun / individual |         4,354 |         8,835 |    80.9 |
| large-response | Bun / bulk       |         4,358 |         8,825 |    79.1 |
| large-response | franz-go         |         7,180 |        39,011 |    22.3 |
| large-response | rdkafka-rust     |         9,336 |        11,720 |    10.2 |

Bun still does not beat either native client on throughput or RSS in these profiles.

## Checks and reproduction

`test/unit/message-decoder.test.ts` checks partial drains, partition boundaries,
large bigint offsets, the number of position updates, zero-copy payload ownership,
filtered records, CRC errors, and position visibility when a deserializer throws.
The position-update-count regression fails on the base revision and passes after
the change. Existing tests cover custom deserializers, commits, seek, and copy modes.

From the repository root, `bun run test` (115 tests) and `bun run typecheck` pass.
In `packages/kafka`, `bunx oxlint --fix`, `bunx oxfmt`, and strict lint pass.
No broker faults were injected for this change.

With the approved broker running and native binaries built, run from
`packages/kafka` at each revision using a fresh output directory:

```sh
KAFKA_BROKERS=127.0.0.1:19092 BENCH_REPEATS=3 \
  BENCH_OUTPUT=/tmp/consumer-drain-new bun bench/run-steady.ts
bun test test/unit/message-decoder.test.ts
```

For these runs, `GO_BENCH` and `RUST_BENCH` selected the binaries retained from
PR #62 under `~/.local/state/bun-kafka/profile-66acbad-20260908/native-build/`.
All local profiling and exploratory evidence, including rejected CRC experiments,
is retained at `~/.local/state/bun-kafka/consumer-drain-9e1173e/`.
The pre-existing local `bun.lock` change is excluded.
