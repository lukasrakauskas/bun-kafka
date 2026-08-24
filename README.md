# bun-kafka

Kafka client for [Bun](https://bun.sh) on top of [librdkafka](https://github.com/confluentinc/librdkafka).

One core API. Two interchangeable native backends:

| Backend | How | When |
|--------|-----|------|
| **ffi** (default) | `bun:ffi` → `librdkafka` | zero build step |
| **napi** | small Node-API addon → `librdkafka` | more stable native boundary |

```ts
import { Producer, Consumer, useNative } from "bun-kafka";

// optional: await useNative("napi")
const producer = new Producer({ "bootstrap.servers": "127.0.0.1:9092" });
producer.send({ topic: "demo", key: "k", value: "hello", headers: { a: "b" } });
await producer.flush();
await producer.close();

const consumer = new Consumer({
  "bootstrap.servers": "127.0.0.1:9092",
  "group.id": "demo",
  "auto.offset.reset": "earliest",
});
consumer.subscribe("demo");

for await (const msg of consumer) {
  console.log(msg.topic, msg.offset, new TextDecoder().decode(msg.value!));
  consumer.commitMessage(msg);
  break;
}
await consumer.close();

// high-throughput path: one async suspend per drained batch
for await (const batch of consumer.batches({ batchSize: 512 })) {
  for (const msg of batch) {
    // msg.value is valid until the batch body returns
  }
}
```

## Requirements

- Bun 1.1+
- librdkafka 1.6+ recommended (developed/tested with **2.14.x**)
  - macOS: `brew install librdkafka`
  - Debian/Ubuntu: `apt install librdkafka-dev`
- Kafka-compatible broker (Apache Kafka, Redpanda, etc.)

Optional env:

| Variable | Meaning |
|----------|---------|
| `KAFKA_BROKERS` | Bootstrap servers (tests/benches; default `127.0.0.1:9092`) |
| `BUN_KAFKA_NATIVE` | `ffi` (default) or `napi` |
| `LIBRDKAFKA_PATH` | Full path to `librdkafka` shared library |
| `NODE_API_INCLUDE` | Directory with `node_api.h` (NAPI build) |

## Supported Kafka / broker versions

Protocol support comes from **librdkafka**, not a separate wire implementation. With a current librdkafka (2.x):

| Broker | Support |
|--------|---------|
| **Apache Kafka 0.11+** | Baseline for headers and current produce/consume paths |
| **Apache Kafka 1.x / 2.x / 3.x / 4.x** | Supported via librdkafka ApiVersion negotiation |
| **Redpanda** | Supported (Kafka API compatible) |
| **Confluent Platform** | Supported when Kafka protocol is exposed normally |

Feature availability depends on broker version (librdkafka negotiates APIs):

| Feature | Typical minimum broker |
|---------|-------------------------|
| Message headers | Kafka **0.11** |
| Message timestamps | Kafka **0.10** |
| Consumer groups (HL subscribe) | Kafka **0.9**+ (modern brokers recommended) |
| Idempotent / transactional produce | Kafka **0.11**+ (see [Not implemented](#not-implemented-yet)) |

**Practical recommendation:** Kafka **2.8+** or current Redpanda. Older clusters work if librdkafka can connect; some features degrade or stay unavailable.

Built against librdkafka **2.14.2** in this repo (`version()` reports the linked library).

## Features

### Producer

| Feature | Status |
|---------|--------|
| `send` / `sendBatch` | Yes |
| Keys, values (`string` \| `Uint8Array` \| `ArrayBuffer`) | Yes |
| Headers | Yes |
| Explicit partition | Yes |
| Timestamp | Yes |
| `flush` / `poll` / `outQueueLength` | Yes |
| Topic-handle cache + classic `produce` fast path | Yes (FFI) |
| Compression, linger, batching via config | Yes (librdkafka conf) |
| Acks (`acks=0/1/all`) via config | Yes |
| Delivery report callbacks | No (poll/flush only) |
| Transactions / idempotent producer helpers | No (raw conf only) |
| Partitioner callbacks | No |

### Consumer

| Feature | Status |
|---------|--------|
| Subscribe (consumer groups) | Yes |
| Manual `assign` / `assignment` | Yes |
| Async iteration `for await (const msg of consumer)` | Yes |
| `messages({ batchSize, timeoutMs, eachMessageCommit })` | Yes |
| **`batches()`** — yield drained `KafkaMessage[]` | Yes |
| `poll` / `pollBatch` | Yes |
| Commit (sync/async), `commitMessage`, batch commit | Yes |
| `committed` / `position` | Yes |
| `seek` | Yes |
| `pause` / `resume` | Yes |
| `storeOffsets` | Yes |
| Watermarks (`query` / `get`) | Yes |
| `offsetsForTimes` | Yes |
| Regex / wildcard subscribe (`^pattern`) | Yes (librdkafka) |
| Headers (lazy on FFI) | Yes |
| Zero-copy key/value until `msg.done()` (FFI) | Yes |
| Rebalance callbacks / cooperative assignor hooks | No |
| Incremental assign/unassign API | No |
| Exactly-once / transactional consume helpers | No |

### Admin

| Feature | Status |
|---------|--------|
| Cluster `metadata` (brokers, topics, partitions) | Yes |
| `clusterId` | Yes |
| Create/delete topics, ACLs, configs, etc. | No |

### Platform / runtime

| Feature | Status |
|---------|--------|
| Bun | Yes |
| `ffi` backend | Yes (default) |
| `napi` backend | Yes (`bun run build:napi`) |
| Swap backend at runtime (`useNative`) | Yes |
| Node.js as primary runtime | No |
| Windows | Untested |

### Not implemented yet

- Admin CRUD (create/delete topics, alter configs, ACLs)
- Transactions / EOS helpers
- Delivery-report and rebalance **callbacks**
- Schema Registry / Avro / Protobuf helpers
- OAuth bearer helpers beyond raw librdkafka conf
- Metrics/stats callback surface

## Native backends

```ts
import { useNative, getDriver, version } from "bun-kafka";

await useNative("ffi");   // default
await useNative("napi");  // requires: bun run build:napi
console.log(getDriver().kind, version());
```

Both backends implement the same `NativeDriver` contract; `Producer` / `Consumer` / `Admin` stay identical.

## API overview

- **Producer** — `send`, `sendBatch`, `flush`, `poll`, `outQueueLength`, `close`
- **Consumer** — `subscribe`, `unsubscribe`, `assign`, `messages`, **`batches`**, `poll`, `pollBatch`, `commit` / `commitMessage`, `seek`, `pause` / `resume`, watermarks, `committed` / `position`, `offsetsForTimes`, `memberId`, `close`
- **Admin** — `metadata`, `clusterId`, `close`
- **Errors** — `KafkaError` (`code`, `fatal`, `retriable`)
- **Constants** — `RD_KAFKA_PARTITION_UA`, `RD_KAFKA_OFFSET_*`, common resp err codes

Config keys are standard librdkafka property names (`bootstrap.servers`, `group.id`, `enable.auto.commit`, `linger.ms`, …).

### Consume styles

```ts
// 1) one message per iteration
for await (const msg of consumer.messages({ timeoutMs: 100, batchSize: 64 })) {
  // ...
}

// 2) drained batches (fewer async suspensions; same librdkafka poll-one underneath)
for await (const batch of consumer.batches({ batchSize: 512, eachBatchCommit: true })) {
  for (const msg of batch) {
    // ...
  }
}

// 3) manual
const batch = consumer.pollBatch(1000, 512);
for (const msg of batch) {
  try {
    // ...
  } finally {
    msg.done();
  }
}
```

> **Note:** librdkafka’s high-level consumer still returns one message per `consumer_poll`. `batches()` accumulates ready queue messages in-process (same approach as the [official accumulate example](https://github.com/confluentinc/librdkafka/blob/master/examples/rdkafka_consume_batch.cpp)). Broker fetch batching always happens inside librdkafka regardless of poll style.

## Tests

```bash
bun test                                    # ffi
bun run build:napi && BUN_KAFKA_NATIVE=napi bun test
KAFKA_BROKERS=127.0.0.1:9092 bun test test/batches.test.ts test/perf.batches.test.ts
```

## Benchmarks

```bash
bun run build:napi       # optional napi lane
bun run bench:build      # go + rust comparison bins
BENCH_COUNT=10000 bun run bench

# messages() vs batches()
KAFKA_BROKERS=127.0.0.1:9092 bun bench/batch_vs_msg.ts 20000
```

Compares bun-kafka/ffi, bun-kafka/napi, franz-go, rdkafka (Rust), and kafkajs if installed.

Notes: `native/build/PERF.md`, tables: `native/build/hyperfine.md`.
