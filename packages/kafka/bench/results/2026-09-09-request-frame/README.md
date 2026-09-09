# Exact-sized request frames

Base revision: `21cc5e3` (merged consumer-drain PR #63).
The only library change is `writeRequestFrame()` in `src/protocol/frame.ts`.

Encode the client ID once, check its signed 16-bit UTF-8 byte limit, and allocate
exactly the header plus body length. Return the local writer's owned view instead
of copying it with `result()`. This removes buffer growth and the final full-frame
copy. Request bodies and frames still have independent storage. Flexible-header
tags, wire bytes, and correlation IDs are unchanged. Response encoding is unchanged.

## Evidence

Fresh CPU profiles on the base revision show native `slice` at about 10–13% and
`set` at about 11–15% of sampled time in the large-record runs. These functions
serve several paths, so those percentages are not attributed solely to framing.
CRC32C remains the largest sampled cost. The isolated benchmark below measures
framing separately from record encoding, CRC calculation, and broker traffic.

All results below were collected on 2026-09-09. Earlier temporary exploratory
samples were lost across a restart and are not used in these comparisons.

### Isolated framing

`bench/bun-request-frame.ts` prepares the body before timing, warms up 1,000 frames,
then measures framing while changing correlation IDs. It checks frame length,
backing-buffer length, final correlation ID, payload bytes, and a length checksum.
Each sample is a fresh process. Before/after order alternates between five repeats.
All 30 samples are retained in `framing.json`.

Medians of five process results, calculated before rounding:

| Body bytes | Frames/process | Before ms | After ms | Before frames/s | After frames/s |
| ---------- | -------------: | --------: | -------: | --------------: | -------------: |
| 100        |      1,000,000 |    509.61 |   439.14 |       1,962,273 |      2,277,183 |
| 11,000     |        100,000 |    246.92 |   122.94 |         404,987 |        813,388 |
| 524,288    |          2,000 |    190.68 |    80.05 |          10,489 |         24,984 |

Framing throughput increased about 16%, 101%, and 138%. These are **not Kafka
produce rates**. The test uses a fixed ASCII client ID, classic headers, and an
already encoded body. It does not include socket writes, record encoding, delivery
acknowledgements, or consumption. Functional tests also cover flexible headers and
UTF-8 client IDs.

### Matched broker workloads

The unchanged `bench/run-steady.ts` ran at each revision with three repetitions
and all four client/API lanes. `before.json` and `after.json` retain all 72
successful samples and every window duration. Every process checked exact
payloads, keys, partitions, ordering, offsets, and delivery results.

Host: Ryzen 7 5700X3D, Linux 7.1.9-arch1-2, Bun 1.4.2, Go 1.27.0, Rust 1.93.1,
franz-go 1.18.1, rdkafka-rust 0.37.0. The approved dedicated Redpanda 25.2.1
broker uses two threads and 2 GiB RAM on `127.0.0.1:19092`.
Settings, warmup, validation, and RSS sampling match the
[original steady benchmark](../2026-09-08-steady/README.md).

Medians of three process results; RSS is sampled whole-process peak RSS:

| Profile        | Bun send API | Produce before msg/s | Produce after msg/s | RSS before MiB | RSS after MiB |
| -------------- | ------------ | -------------------: | ------------------: | -------------: | ------------: |
| small-latency  | individual   |                6,059 |               6,380 |           59.0 |          58.9 |
| small-latency  | bulk         |                5,913 |               6,154 |           59.5 |          58.6 |
| small-burst    | individual   |              291,385 |             288,892 |           94.4 |          93.8 |
| small-burst    | bulk         |              307,023 |             311,422 |           96.0 |          91.6 |
| large-response | individual   |                4,248 |               4,500 |           74.0 |          74.1 |
| large-response | bulk         |                4,329 |               4,338 |           77.2 |          76.5 |

End-to-end results are much less conclusive than the isolated framing test.
Large-record individual production increased 5.9%, but bulk was essentially
unchanged and small-burst individual production slightly decreased. No general
RSS or consumer throughput gain is claimed. Complete before suites ran before
after suites; revision order was not interleaved. Client order rotates within
suites. This shared host has visible variation: unchanged Rust large-record
production changed from 8,779 to 9,718 msg/s. Three repetitions are descriptive,
not statistical proof of small differences.

Native comparisons from the after suite:

| Profile        | Client / API     | Produce msg/s | Consume msg/s | RSS MiB |
| -------------- | ---------------- | ------------: | ------------: | ------: |
| small-burst    | Bun / individual |       288,892 |     1,371,736 |    93.8 |
| small-burst    | Bun / bulk       |       311,422 |     1,324,589 |    91.6 |
| small-burst    | franz-go         |       474,737 |     4,144,953 |    22.5 |
| small-burst    | rdkafka-rust     |       313,102 |     2,072,505 |    12.4 |
| large-response | Bun / individual |         4,500 |         8,385 |    74.1 |
| large-response | Bun / bulk       |         4,338 |         8,542 |    76.5 |
| large-response | franz-go         |         6,917 |        31,451 |    21.3 |
| large-response | rdkafka-rust     |         9,718 |        11,575 |    10.2 |

Bun still does not beat either native client on throughput or RSS here.

## Checks and reproduction

`test/unit/request-frame.test.ts` checks wire bytes, exact-sized buffers, ownership
across input mutation and later frames, empty and large bodies, classic/flexible
headers, Unicode IDs, and the UTF-8 byte limit. These are compatibility checks;
they also pass on the original implementation. Existing real-TCP backpressure
tests check short writes, complete bytes, and correlation IDs.

`bun run test` (117 tests), `bun run typecheck`, package `bunx oxlint --fix`,
`bunx oxfmt`, and `bunx oxlint --deny-warnings --format=agent` pass.
No broker fault injection was performed.

From `packages/kafka`, run each revision in fresh processes. Alternate revision
order for the isolated test and retain every result:

```sh
bun bench/bun-request-frame.ts 100 1000000
bun bench/bun-request-frame.ts 11000 100000
bun bench/bun-request-frame.ts 524288 2000
# With the approved dedicated broker running and native benchmarks built:
KAFKA_BROKERS=127.0.0.1:19092 BENCH_REPEATS=3 \
  BENCH_OUTPUT=/tmp/request-frame-new bun bench/run-steady.ts
```

For the before revision, use the same new benchmark script with the original
`frame.ts`. Native binaries were selected with `GO_BENCH` and `RUST_BENCH` from
`~/.local/state/bun-kafka/profile-66acbad-20260908/native-build/`.
Profiles, source snapshots, individual framing samples, and test logs are retained
at `~/.local/state/bun-kafka/request-frame-21cc5e3-20260909/`.
The user's pre-existing `bun.lock` change is unchanged and excluded.
