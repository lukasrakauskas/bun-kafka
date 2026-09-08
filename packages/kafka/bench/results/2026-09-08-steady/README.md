# Warmed client, fault, and retained-memory measurements

Issue: [#57](https://github.com/lukasrakauskas/bun-kafka/issues/57).
Library revision: `235cb5b30f70b759b5635b81889dad91c4bd34d6` (all of #52–#56 and #58 are merged).
Benchmark sources are added with this snapshot; library source is unchanged.

Measured on 2026-09-08 on an AMD Ryzen 7 5700X3D (8 cores / 16 threads), 31.3 GiB RAM,
Linux 7.1.9-arch1-2, Bun 1.4.2, Go 1.27.0, Rust 1.93.1, franz-go 1.18.1,
rdkafka-rust 0.37.0 with bundled librdkafka 2.12.1. The dedicated local Redpanda 25.2.1
container uses two threads and 2 GiB RAM. This is a shared desktop, without CPU pinning.
Do not compare these numbers directly with the separate Intel-host startup snapshot.

## Scope and method

These are warmed, finite, bounded-window workloads, not maximum sustainable capacity tests.
All three clients use these settings:

- A fresh, pre-created topic with one partition and replication factor one; manual partition zero.
- Leader acknowledgements (`acks=1`), no idempotence, no compression, zero linger.
- Decimal keys cycling through 0–63. Values contain a little-endian 64-bit sequence number,
  followed by `x` bytes. Each send allocates its value; all consumers verify every byte, key,
  partition, and exact offset/sequence. Validation cost is included, with native byte comparison
  rather than an extra payload copy in the Bun validator.
- One outstanding application window. Every delivery in that window must succeed before the
  next window starts. Go and Rust have one in-flight produce request per broker and a 1 MiB
  wire batch limit. Bun has no equivalent byte-limit option; the application window caps its
  payload at 512 KiB. Native clients may split a window into more wire batches. **Application
  batch sizes match; exact wire batch boundaries do not.**
- Manual consumption without group joining or commits; `read_uncommitted`; fetch wait 10 ms,
  minimum one byte, total and per-partition maximum 1 MiB. Bun uses `copy: false`. Rust uses
  a 1 MiB local queue limit, one queued minimum message, and 1 ms fetch-queue backoff.
- A 120-second client deadline and a 130-second parent-process kill deadline. Delivery callbacks
  are checked in Go and Rust; a successful flush alone is not accepted as delivery evidence.

Each process first produces and consumes 100 warmup windows with the same clients it will
measure. Its consumer is then paused, with a 50 ms settling period before measured production.
This prevents native consumers from reading the measured backlog during production. After
production, consumption resumes and one window is read outside the consume timer. `resume_ms`
records this separate cost; `consume_count = (windows - 1) * batch`. This also prevents Rust's
roughly 0.6–0.8 second resume/first-data delay from being mislabeled as steady consume cost.
The first fetch can fill a local buffer: the one-record profile's tiny backlog mostly measures
buffer draining, so its consume rate is deliberately omitted from the tables.

Bun has both individual `send` calls collected with `Promise.all` and one bulk `send` per window.
Go and Rust use their individual-record APIs. There are three process repetitions per lane,
with client order rotated between repetitions. All 36 measured samples and all window durations
are retained in `samples.json`; no measured outliers were removed. Development pilots were used
to correct linger, resume timing, and validation-copy differences before this recorded set.

| Profile        | Value bytes | Records/window | Measured windows | Produced records |
| -------------- | ----------: | -------------: | ---------------: | ---------------: |
| small-latency  |         100 |              1 |            1,000 |            1,000 |
| small-burst    |         100 |            100 |            1,000 |          100,000 |
| large-response |      65,536 |              8 |              200 |            1,600 |

## Throughput and acknowledgement latency

Each cell is the median of the three process results. Throughput is computed from each process's
unrounded duration, then reduced. p50/p95/p99 use nearest rank within each process, then the
median across processes: these are **window enqueue-through-final-ack durations**, including
value allocation, not per-record end-to-end latency or pooled percentiles. One-record windows
also give individual-send acknowledgement latency. These small sample sets are descriptive,
not statistical proof of small differences.

| Profile        | Client / send API         | Produce msg/s | Consume msg/s | Ack p50 ms | Ack p95 ms | Ack p99 ms |
| -------------- | ------------------------- | ------------: | ------------: | ---------: | ---------: | ---------: |
| small-latency  | bun-kafka / individual    |         5,358 |             — |      0.169 |      0.265 |      0.309 |
| small-latency  | bun-kafka / bulk          |         5,442 |             — |      0.165 |      0.270 |      0.317 |
| small-latency  | franz-go / individual     |         7,822 |             — |      0.118 |      0.187 |      0.232 |
| small-latency  | rdkafka-rust / individual |         8,809 |             — |      0.104 |      0.167 |      0.193 |
| small-burst    | bun-kafka / individual    |       256,111 |     1,126,038 |      0.335 |      0.576 |      1.344 |
| small-burst    | bun-kafka / bulk          |       249,416 |     1,173,956 |      0.340 |      0.584 |      1.804 |
| small-burst    | franz-go / individual     |       456,746 |     3,723,650 |      0.203 |      0.320 |      0.484 |
| small-burst    | rdkafka-rust / individual |       289,149 |     1,972,433 |      0.333 |      0.501 |      0.607 |
| large-response | bun-kafka / individual    |         3,393 |         7,446 |      2.024 |      3.983 |      5.025 |
| large-response | bun-kafka / bulk          |         2,906 |         7,495 |      2.634 |      4.357 |      4.908 |
| large-response | franz-go / individual     |         6,486 |        26,162 |      1.093 |      2.111 |      3.169 |
| large-response | rdkafka-rust / individual |         7,493 |        11,409 |      0.944 |      1.481 |      2.244 |

On these workloads, Bun does **not** beat either native lane on produce or consume throughput.
The large-response individual-send consume results correspond to approximately 465 MiB/s for
Bun, 1,635 MiB/s for Go, and 713 MiB/s for Rust. These are local broker-to-validator rates,
not disk or remote-network capacity claims. No percentage gains from earlier PRs are added.

## CPU and RSS

Linux `/proc/<pid>/stat` supplies process CPU time across threads at 100 ticks/second.
`VmRSS` is sampled every 20 ms, plus at phase boundaries. CPU covers measured production,
resume/priming, and consumption; it excludes client initialization and the first 100 warmup
windows. Small event-delivery/sampling delays remain. RSS is total client-process RSS, including
runtime memory and allocations retained from warmup, not a heap-only or broker measurement.
Sampled peaks can miss short spikes. Failure runs have sampled RSS but no final CPU measurement.

| Profile        | Client / send API         | CPU ms | Sampled peak RSS MiB | Resume/prime ms |
| -------------- | ------------------------- | -----: | -------------------: | --------------: |
| small-latency  | bun-kafka / individual    |    170 |                 59.6 |            1.01 |
| small-latency  | bun-kafka / bulk          |    190 |                 59.6 |            1.05 |
| small-latency  | franz-go / individual     |     80 |                 18.4 |            0.71 |
| small-latency  | rdkafka-rust / individual |     40 |                  8.8 |          838.67 |
| small-burst    | bun-kafka / individual    |    590 |                 93.0 |            2.05 |
| small-burst    | bun-kafka / bulk          |    680 |                199.8 |            2.08 |
| small-burst    | franz-go / individual     |    190 |                 21.7 |            2.25 |
| small-burst    | rdkafka-rust / individual |    180 |                 12.4 |          601.09 |
| large-response | bun-kafka / individual    |    690 |                105.0 |            2.88 |
| large-response | bun-kafka / bulk          |    720 |                239.7 |            1.65 |
| large-response | franz-go / individual     |    360 |                 23.1 |            1.13 |
| large-response | rdkafka-rust / individual |     70 |                 10.2 |          664.28 |

The Bun bulk runs retained substantially more RSS than the individual runs in this snapshot.
The measurements do not identify the allocation or GC cause; no library optimization is inferred.

## Slow broker and connection-loss outcomes

`pause.json` and `restart.json` each contain 12 trials: the large-response profile, four API lanes,
three repetitions. The user ran these privileged fault commands against only Compose project
`bun-kafka-bench-57`. Injection begins after warmup, when measured production starts.

- **Pause:** Docker pauses the broker for one second, then unpauses it. Command overhead made
  the measured pause/unpause interval about 1.08–1.10 seconds. All 12 trials completed and
  validated all 2,400 records (800 warmup plus 1,600 measured), in order. Each run had a measured
  acknowledgement window of at least 500 ms, proving that the pause affected active work.
  The longest window was about 1.08–1.10 seconds. The application retains at most one 512 KiB
  payload window while blocked; this is not an unbounded-concurrency load test.
- **Hard restart:** `docker compose restart --timeout 0 redpanda` closes broker connections;
  the runner then waits for container health. Restart plus health took 2.51–2.54 seconds.
  Go completed all three trials, validating all records, with longest ack windows of
  4.70–4.74 seconds. Both Bun API lanes exited in all three repetitions with retriable connection
  errors. Rust exited in all three repetitions when its consumer returned `AllBrokersDown`.
  Exact errors and nonzero exit codes are retained, not replaced with successful throughput.

**The restart results describe these fail-fast applications and default retry policies, not an
inherent client recovery ranking.** Bun's default producer policy allows three retries; the native
producer retry policies are different. The Rust benchmark treats every returned consumer error,
including a transient `AllBrokersDown` notification, as an error that stops the run. A Rust
application can handle that event and keep polling. Bun applications can choose a longer retry
budget. These probes do not establish loss or duplicate counts for failed runs. With RF=1,
leader acknowledgements, and idempotence disabled, they are not a durability or exactly-once test.
The network fault covered here is connection loss from broker restart, not packet loss, a
multi-broker partition, or a separate network emulator.

The bounded-window probe does not itself guarantee a short socket write. The merged transport
regression does: `test/unit/*backpressure*` sends two 16 MiB payloads to a paused/throttled real TCP
reader, asserts short writes and drain activity, and checks complete bytes and correlation IDs.

## Retained response ownership

`retained.json` contains three fresh processes per ownership mode. Each process produces 128 MiB
(2,048 records × 64 KiB), fetches with 1 MiB limits, and keeps only the first value from each
nonempty fetch. All values are checked during consumption and again after consumer close and GC.
The observed 128 retained values total 8 MiB. Unique `value.buffer` byte lengths give the exact
backing allocation still referenced; they are not inferred from RSS.

| Mode          | Retained payload MiB | Referenced backing MiB | Median held-minus-baseline RSS MiB |
| ------------- | -------------------: | ---------------------: | ---------------------------------: |
| `copy: false` |                    8 |                 128.05 |                              146.5 |
| `copy: true`  |                    8 |                      8 |                               25.2 |

Keeping sparse zero-copy values can therefore keep much larger response allocations alive.
This measures response ownership on the merged code, not a before/after regression attributed
to #56 or solely to complete-chunk borrowing. RSS also includes allocator/runtime overhead and
need not drop when values are released; `baseline`, `held`, and `released` snapshots are retained.
Use `copy: true` when small long-lived payloads would otherwise pin large responses.

## Reproduce

Prerequisites: Linux with `/proc`, Bun, Go, Rust/Cargo, a C/C++ toolchain and the existing Rust
package's native build prerequisites. No new library dependency is added. Never use a production
broker: the programs create and delete fresh topics; fault modes interrupt the dedicated broker.
From the repository root, start the disposable broker only with operator approval:

```sh
sudo docker compose -p bun-kafka-bench-57 \
  -f packages/kafka/test/impl/soak.compose.yml up -d --wait
cd packages/kafka
mkdir -p native/build
(cd bench/go && go build -o ../../native/build/steady-go ./steady)
(cd bench/rust && cargo build --release --locked --bin steady)
# Select the desired installed Go version first if using mise shims.

KAFKA_BROKERS=127.0.0.1:19092 BENCH_REPEATS=3 \
  BENCH_OUTPUT=/tmp/steady-new bun bench/run-steady.ts
for copy in 0 1; do
  for run in 1 2 3; do
    BENCH_COPY=$copy KAFKA_BROKERS=127.0.0.1:19092 \
      bun bench/bun-retained.ts > /tmp/retained-$copy-$run.json
  done
done

# In the same terminal, authorize sudo for these dedicated-project commands.
sudo -v
BENCH_FAULT=pause BENCH_OUTPUT=/tmp/steady-pause-new bun bench/run-steady.ts
BENCH_FAULT=restart BENCH_OUTPUT=/tmp/steady-restart-new bun bench/run-steady.ts
```

The runner refuses an existing output directory, saves after every trial, kills timed-out
children, and deletes only its fresh topic after each trial, including failures. Use a new
output path for every invocation. `GO_BENCH` and `RUST_BENCH` can select already-built binaries.
Fault modes require `127.0.0.1:19092` and use only the fixed `bun-kafka-bench-57` project.
Keep the terminal running during a pause so that the `finally` block can unpause the broker.
If the whole runner is forcibly killed during a pause, recover it explicitly:

```sh
sudo docker compose -p bun-kafka-bench-57 -f test/impl/soak.compose.yml unpause redpanda
```

After all work, the operator can remove just this project's container and volume:

```sh
# From the repository root:
sudo docker compose -p bun-kafka-bench-57 \
  -f packages/kafka/test/impl/soak.compose.yml down -v
```

## Checks

Validate the saved counts, window percentiles, failure outcomes, and referenced backing sizes
without a broker (from `packages/kafka`):

```sh
bun bench/results/2026-09-08-steady/check.ts
```

- All 36 baseline trials, 12 pause trials, 12 restart outcomes, and six memory trials are retained.
- Successful trials verify delivery callbacks/results and exact record content/order. A negative
  probe pre-seeded a corrupt payload; all three client executables rejected it before measurement.
- Combined merged library: 111 unit/features tests pass, including real TCP backpressure; typecheck,
  package formatting, and strict lint pass. The runner is included in package typechecking.
- Native builds, `go vet ./steady`, and `cargo clippy --release --locked --bin steady -- -D warnings`
  pass. The documentation site builds (with its existing missing-sitemap-site warning).
- This is a competitive baseline and an ownership/fault assessment, not evidence that Bun beats
  franz-go or librdkafka, not a combined percentage speedup, and not an exhaustive workload matrix.
