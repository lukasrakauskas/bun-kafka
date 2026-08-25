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

Set `partitionAssigner: "cooperative-sticky"` for KIP-429 incremental rebalancing: members retain owned partitions across rebalances and only moved partitions change hands.

### Transactions

```ts
const producer = kafka.producer({ transactionalId: "orders-1" });
await producer.beginTransaction();
await producer.send({ topic: "events", messages: [{ value: "batch-1" }] });
await producer.commitTransaction();   // or abortTransaction()
```

Transactional producers resolve their transaction coordinator, force all-replica acknowledgements, register touched partitions with `AddPartitionsToTxn`, and bump the producer epoch after every transaction. Consumers pair with them through `kafka.consumer({ isolationLevel: "read_committed" })`, which filters aborted transaction records and control markers.

Fetches use incremental sessions (KIP-227): idle partitions are pruned from requests via `forgotten_topics_data` while the broker keeps monitoring them for new data.

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
| Produce with acks 0/1/all and automatic batching | Yes |
| Gzip, Zstandard, Snappy, and LZ4 record-batch compression | Yes |
| Bounded request retries, events, and metadata refresh | Partial |
| Kafka record batches (magic 2) | Yes |
| Keys, values, timestamps, and headers | Yes |
| CRC32C validation | Yes |
| Kafka-compatible Murmur2 partitioning and custom partitioners | Yes |
| Bounded, zero-copy manual consume and offset lookup | Yes |
| Topic and config administration | Yes |
| Consumer groups with static membership (KIP-345) | Yes |
| Cooperative-sticky rebalancing (KIP-429) | Yes |
| Incremental fetch sessions (KIP-227) | Yes |
| Read-committed isolation | Yes |
| SASL/PLAIN and SCRAM-SHA-256/512 | Yes |
| SASL/OAUTHBEARER static or provider token | Yes |
| Timed OAuth reauthentication (KIP-368) | Yes |
| Kerberos/GSSAPI | Not yet |
| Idempotent produce | Yes |
| Transactions | Yes |
| Admin topic, config, group, ACL, quota, and delegation-token changes | Yes |
| Health/stats hooks and logging | Yes |
| Proxy support | No |
| Per-message delivery callbacks | Yes |

The fixed API versions require Kafka 0.11 or newer. Use a current Kafka or Redpanda release.

## Readiness documentation

- [Recorded benchmarks](docs/benchmarks.md)
- [Performance validation and soak tests](docs/performance-validation.md)
- [Broker failure and chaos testing](docs/chaos-testing.md)
- [Kafka feature completeness and client comparison](docs/feature-completeness.md)

The native client has short-run benchmark evidence, a passing three-broker chaos qualification (`out/chaos/`), and a passing 30-minute soak run (`out/soak/`). The 24-hour release soak gates defined in the performance validation doc remain outstanding.

## Test

```bash
bun run test            # unit + feature suites (mock broker)
bun run test:coverage   # same, with code coverage report
KAFKA_BROKERS=127.0.0.1:9092 bun test test/impl/bun-integration.test.ts
bun run test:chaos:mock
bun run test:chaos
bun run test:soak
```

Tests are split by scope: `test/unit/` (wire protocol primitives), `test/features/` (client features against a mock broker), and `test/impl/` (implementation tests against a real broker, chaos, perf, soak). The unit/feature suites use a Bun TCP mock broker.

## Soak

`SOAK_DURATION_S=1800 SOAK_RATE=1000 bun run test:soak` runs one long-lived Bun process that produces at a fixed offered rate, drains with a consumer, samples latency/memory/socket/CPU metrics every 10 seconds, injects periodic bursts, validates per-partition sequence integrity, evaluates release-gate checks, and writes JSON and Markdown artifacts to `out/soak/`. See [performance validation](docs/performance-validation.md) for the full gate definitions.

## Releasing

1. Bump `version` in `package.json`.
2. In CHANGELOG.md, rename `[Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and start a fresh `[Unreleased]` section.
3. Tag `vX.Y.Z` and push the tag.

A release requires green CI plus the soak gates defined in [performance validation](docs/performance-validation.md).

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
