# Release completed producer inputs

Base revision: `66acbad`. The only library change is `Producer.send()` using
`Promise.withResolvers()` instead of a promise executor that captures `input`.
The encoder and consumer are unchanged. No dependency is added.

## Evidence

A production-phase heap snapshot showed this reference path:

```text
root → SlimPromiseReaction chain → AsyncFunctionGenerator → Promise
     → Function → JSLexicalEnvironment.input → messages → value
```

The large bulk probe held 1,600 payload buffers after GC, about 100 MiB.
The original promise executor shares its lexical environment with the scheduled
flush callback. A sequence of awaited sends can retain completed inputs through
that environment while flush work continues. The replacement creates the promise
without an executor closure that captures the input.

The regression in `test/unit/producer-scheduling.test.ts` sends sequentially with
an asynchronous mock response, clears mock call arguments, and checks weak payload
references while keeping send promises alive. It fails on the original producer,
passes with the change, and passed ten additional fresh test runs. Existing tests
cover batching, delivery success and failure, and subsequent sends after failure.

A separate instrumented large-bulk run measured ArrayBuffer memory after production
and explicit GC: 100.21 MiB before, 0.56 MiB after. These are diagnostic runs, not
throughput samples. Exit heap profiles alone did not explain peak RSS.

## Matched results

`before.json` and `after.json` retain all 72 successful process samples and every
window duration. Both use the unchanged `bench/run-steady.ts`, three repetitions,
and all four client lanes. Before is the original producer; after contains only
this producer change. Each complete before suite ran before the after suite.
Client order rotates within each suite, but revision order is not interleaved.

Host: Ryzen 7 5700X3D, Linux 7.1.9-arch1-2, Bun 1.4.2, Go 1.27.0,
Rust 1.93.1, franz-go 1.18.1, rdkafka-rust 0.37.0. Dedicated Redpanda 25.2.1
uses two threads and 2 GiB RAM, at `127.0.0.1:19092`.
Settings, warmup, validation, CPU/RSS sampling, and workload limits are the same as
[the original steady benchmark](../2026-09-08-steady/README.md).

Medians of three process results; throughput is calculated before rounding.
RSS is sampled whole-process peak RSS, not live heap size.

| Profile        | Bun send API | Produce before msg/s | Produce after msg/s | RSS before MiB | RSS after MiB |
| -------------- | ------------ | -------------------: | ------------------: | -------------: | ------------: |
| small-latency  | individual   |                6,105 |               6,362 |           64.0 |          64.0 |
| small-latency  | bulk         |                5,999 |               6,175 |           64.2 |          63.9 |
| small-burst    | individual   |              288,906 |             299,317 |           95.3 |         102.4 |
| small-burst    | bulk         |              283,779 |             315,479 |          206.1 |         100.9 |
| large-response | individual   |                4,424 |               4,341 |          113.7 |          78.3 |
| large-response | bulk         |                3,664 |               4,402 |          235.3 |          81.3 |

Small-burst bulk production increased 11.2%, with 51.0% lower RSS.
Large-response bulk production increased 20.2%, with 65.4% lower RSS.
Individual small-burst RSS increased and large-response individual throughput
slightly decreased. This is not an improvement in every metric.

Native comparisons from the after suite:

| Profile        | Client / API     | Produce msg/s | Consume msg/s | RSS MiB |
| -------------- | ---------------- | ------------: | ------------: | ------: |
| small-burst    | Bun / individual |       299,317 |     1,236,858 |   102.4 |
| small-burst    | Bun / bulk       |       315,479 |     1,233,902 |   100.9 |
| small-burst    | franz-go         |       494,549 |     4,504,689 |    23.1 |
| small-burst    | rdkafka-rust     |       318,347 |     2,405,319 |    12.4 |
| large-response | Bun / individual |         4,341 |         8,947 |    78.3 |
| large-response | Bun / bulk       |         4,402 |         9,031 |    81.3 |
| large-response | franz-go         |         6,841 |        28,595 |    23.9 |
| large-response | rdkafka-rust     |         9,235 |        11,959 |    10.2 |

Bun does not beat either native client on throughput or RSS here. This shared
host has visible run variation: unchanged Go large-response production changed
from 9,206 to 6,841 msg/s between suites. Three repetitions are descriptive,
not proof of small speed differences. No consumer speed improvement is claimed.

## Checks and reproduction

From the repository root: `bun run test` and `bun run typecheck` pass (112 tests).
In `packages/kafka`, `bunx oxlint --fix`, `bunx oxfmt`, and
`bunx oxlint --deny-warnings --format=agent` pass.
Every benchmark process checked exact payloads, keys, offsets, order, and delivery
results. No fault injection was performed for this change.

With the approved dedicated broker running and native binaries built, run from
`packages/kafka` at each revision, with a fresh output directory:

```sh
KAFKA_BROKERS=127.0.0.1:19092 BENCH_REPEATS=3 \
  BENCH_OUTPUT=/tmp/producer-retention-new bun bench/run-steady.ts
bun test test/unit/producer-scheduling.test.ts
```

All local profiling and exploratory evidence is also retained at
`~/.local/state/bun-kafka/profile-66acbad-20260908/`: CPU profiles, exit heap
profiles, production-phase snapshots, memory probes and their source, initial
runs, test logs, and the rejected encoder experiment. That experiment removed a
batch allocation but did not show a repeatable RSS benefit; it is not included
in this change. The production snapshots were taken during that encoder
experiment; the separate before/after GC measurements used the original encoder.
The user's pre-existing `bun.lock` change is unchanged.
