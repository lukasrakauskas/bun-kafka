# bun-kafka

A zero-dependency Kafka client for Bun. It implements the Kafka wire protocol in TypeScript and uses `Bun.connect()` for TCP and TLS.

It has no runtime dependencies and requires Bun 1.4 or newer.

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

One `send()` call makes record batches by topic partition. Concurrent calls are collected for 5 ms or 1,000 messages. Configure this with `kafka.producer({ lingerMs, batchMaxMessages, compression: "zstd", idempotent: true })`. Idempotent mode forces all-replica acknowledgements and makes client retries duplicate-safe.

## Consume

Manual assignment is available without a group:

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

Use `groupId` for eager range assignment and committed offsets:

```ts
const grouped = kafka.consumer({ groupId: "workers", fromBeginning: true });
await grouped.subscribe("events");
const messages = await grouped.fetch();
await grouped.commitOffsets();
```

`fetch()`, `seek()`, `pause()`, `resume()`, `assignment()`, `position()`, `watermarks()`, `commitOffsets()`, and `committed()` are available. Payloads are stable zero-copy views by default. Set `copy: true` on `fetch()` when you need separate buffers.

## Metadata

```ts
const admin = kafka.admin();
const metadata = await admin.metadata();       // all topics
const one = await admin.metadata(["events"]); // selected topics
await admin.createTopics([{ name: "audit", numPartitions: 3 }]);
await admin.createPartitions([{ name: "audit", count: 6 }]);
await admin.deleteTopics(["audit"]);
```

## Configuration

```ts
const kafka = new Kafka({
  brokers: ["kafka-1:9093", "kafka-2:9093"],
  clientId: "orders",
  requestTimeoutMs: 30_000,
  connectTimeoutMs: 10_000,
  maxResponseBytes: 100 * 1024 * 1024,
  retry: { maxRetries: 3 },
  onEvent: (event) => console.log(event), // retry and broker-throttle events
  tls: true,
  // sasl: { mechanism: "plain", username: "user", password: "secret" },
  // sasl: { mechanism: "scram-sha-256", username: "user", password: "secret" },
  // sasl: { mechanism: "oauthbearer", token: async () => getToken() },
});
```

`tls` also accepts Bun TLS values such as `serverName`, `ca`, `cert`, `key`, and `rejectUnauthorized`.

## Current protocol support

| Feature | Status |
|---|---|
| Bun TCP and TLS | Yes |
| Metadata API | Yes |
| Produce with acks 1/all and automatic batching | Yes |
| Gzip and Zstandard record-batch compression | Yes |
| Bounded request retries, events, and metadata refresh | Partial |
| Kafka record batches (magic 2) | Yes |
| Keys, values, timestamps, and headers | Yes |
| CRC32C validation | Yes |
| Kafka-compatible Murmur2 partitioning | Yes |
| Bounded, zero-copy manual consume and offset lookup | Yes |
| Topic and config administration | Yes |
| Basic consumer groups and offset commits | Partial |
| Cooperative rebalancing and static membership | Not yet |
| SASL/PLAIN and SCRAM-SHA-256/512 | Yes |
| SASL/OAUTHBEARER static or provider token | Yes |
| Timed SASL reauthentication and Kerberos | Not yet |
| Gzip and Zstandard compression | Yes |
| Snappy and LZ4 compression | Not yet |
| Idempotent produce | Yes |
| Transactions | Not yet |
| Admin topic and config changes | Yes |

The fixed API versions require Kafka 0.11 or newer. Use a current Kafka or Redpanda release.

## Readiness documentation

- [Recorded benchmarks](docs/benchmarks.md)
- [Performance validation and soak tests](docs/performance-validation.md)
- [Broker failure and chaos testing](docs/chaos-testing.md)
- [Kafka feature completeness and client comparison](docs/feature-completeness.md)

The native client has short-run benchmark and integration evidence. It does not yet have the soak and chaos evidence required by these release gates.

## Test

```bash
bun test test/bun-native.test.ts
KAFKA_BROKERS=127.0.0.1:9092 bun test test/bun-integration.test.ts
bun run test:chaos:mock
bun run test:chaos
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
