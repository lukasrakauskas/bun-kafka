# bun-kafka

A zero-dependency Kafka client for Bun. It implements the Kafka wire protocol in TypeScript and uses `Bun.connect()` for TCP and TLS.

It does not load `librdkafka`, Node-API, `bun:ffi`, Node streams, or a socket package. It requires Bun 1.4 or newer.

## Install

```bash
bun add bun-kafka
```

## Produce

```ts
import { Kafka } from "bun-kafka";

const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
const producer = kafka.producer();

await producer.send({
  topic: "events",
  acks: "all",
  messages: [
    { key: "123", value: JSON.stringify({ action: "login" }), headers: { source: "api" } },
  ],
});

await kafka.disconnect();
```

One `send()` call makes record batches by topic partition. Concurrent calls are collected for 5 ms or 1,000 messages. Configure this with `kafka.producer({ lingerMs, batchMaxMessages })`.

## Consume

This first native release uses manual partition assignment. It does not join a consumer group.

```ts
const consumer = kafka.consumer({ fromBeginning: true });

await consumer.assign([
  { topic: "events", partition: 0, offset: "earliest" },
]);

for await (const message of consumer) {
  console.log(new TextDecoder().decode(message.value!));
  if (message.offset >= 9n) break;
}

await consumer.close();
```

You can also assign all partitions of one or more topics:

```ts
await consumer.subscribe({ topics: ["events"], fromBeginning: true });
const batch = await consumer.fetch({ maxMessages: 500, maxWaitMs: 100 });
```

`fetch()`, `seek()`, `pause()`, `resume()`, `assignment()`, `position()`, and `watermarks()` are available. Payloads are stable zero-copy views by default. Set `copy: true` on `fetch()` when you need separate buffers.

## Metadata

```ts
const admin = kafka.admin();
const metadata = await admin.metadata();       // all topics
const one = await admin.metadata(["events"]); // selected topics
```

## Configuration

```ts
const kafka = new Kafka({
  brokers: ["kafka-1:9093", "kafka-2:9093"],
  clientId: "orders",
  requestTimeoutMs: 30_000,
  maxResponseBytes: 100 * 1024 * 1024,
  tls: true,
});
```

`tls` also accepts Bun TLS values such as `serverName`, `ca`, `cert`, `key`, and `rejectUnauthorized`.

## Current protocol support

| Feature | Status |
|---|---|
| Bun TCP and TLS | Yes |
| Metadata API | Yes |
| Produce with acks 1/all and automatic batching | Yes |
| Kafka record batches (magic 2) | Yes |
| Keys, values, timestamps, and headers | Yes |
| CRC32C validation | Yes |
| Kafka-compatible Murmur2 partitioning | Yes |
| Bounded, zero-copy manual consume and offset lookup | Yes |
| Consumer groups and offset commits | Not yet |
| SASL | Not yet |
| Compression | Not yet |
| Transactions and idempotent produce | Not yet |
| Admin topic changes | Not yet |

The fixed API versions require Kafka 0.11 or newer. Use a current Kafka or Redpanda release.

The old librdkafka client remains in the repository for comparison. It is not part of the root package export.

## Test

```bash
bun test test/bun-native.test.ts
KAFKA_BROKERS=127.0.0.1:9092 bun test test/bun-integration.test.ts
```

The unit test includes a Bun TCP mock broker. The integration test reuses the producer, metadata, record, and consumer acceptance cases against a real broker.

## Benchmark

Install [hyperfine](https://github.com/sharkdp/hyperfine), then run:

```bash
bun run bench
bun run bench:build # optional Go and Rust comparison binaries
KAFKA_BROKERS=127.0.0.1:9092 bun run bench
```

Results are written to:

- `native/build/bun-native-hyperfine.md`
- `native/build/bun-native-hyperfine.json`

See [the recorded baseline](docs/benchmarks.md).

The offline lanes process the same record count with one-record and 100-record batches. If `KAFKA_BROKERS` is set, hyperfine also runs a produce/consume broker round trip.
