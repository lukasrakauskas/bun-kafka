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
```

## Requirements

- Bun 1.1+
- librdkafka (`brew install librdkafka` / `apt install librdkafka-dev`)
- Kafka/Redpanda on `KAFKA_BROKERS` (default `127.0.0.1:9092`)

## Native backends

```ts
import { useNative, getDriver } from "bun-kafka";

await useNative("ffi");   // default
await useNative("napi");  // requires: bun run build:napi
console.log(getDriver().kind);
```

Env: `BUN_KAFKA_NATIVE=ffi|napi`, `LIBRDKAFKA_PATH=/path/to/librdkafka.dylib`.

## API surface

- **Producer** — `send`, `sendBatch`, `flush`, `poll`, `outQueueLength`, `close`
- **Consumer** — `subscribe`, `assign`, async iteration (`for await`), `poll`, `commit` / `commitMessage`, `seek`, `pause`/`resume`, watermarks, `committed`/`position`, `offsetsForTimes`
- **Admin** — `metadata`, `clusterId`

## Tests

```bash
bun test                 # ffi
bun run build:napi && BUN_KAFKA_NATIVE=napi bun test
```

## Benchmarks (hyperfine)

```bash
bun run build:napi       # optional napi lane
bun run bench:build      # go + rust comparison bins
BENCH_COUNT=10000 bun run bench
```

Compares:

- bun-kafka/ffi
- bun-kafka/napi (if built)
- franz-go
- rdkafka (Rust)
- kafkajs (if installed)

Results: `native/build/hyperfine.md`.
