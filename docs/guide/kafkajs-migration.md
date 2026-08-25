# Migrating from kafkajs

bun-kafka ships a drop-in-shaped compatibility layer so existing kafkajs code runs under Bun
without a rewrite:

```ts
import { Kafka } from "bun-kafka/kafkajs-compat";

const kafka = new Kafka({
  clientId: "app",
  brokers: ["kafka-1:9092"],
  logLevel: 2,
});
```

The compat layer implements the kafkajs surface (producer `send`/`sendBatch`, consumer
`run()`/`eachMessage`/`eachBatch` with auto-commit, admin methods, log levels, retry options) on
top of the same native client — zero dependencies, no Node polyfills.

## Mapping

| kafkajs                                                                                        | bun-kafka native                                                                          |
| ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `new Kafka({ brokers, clientId, ssl, sasl, retry, logLevel })`                                 | Same shape; `ssl` → `tls`; log levels via the compat layer                                |
| `kafka.producer()` + `send({ topic, messages })`                                               | identical call signature; resolves per-partition results                                  |
| `messages[].value` strings/Buffer                                                              | `string \| Uint8Array \| ArrayBuffer \| null` (Buffers are Uint8Arrays)                   |
| `kafka.consumer({ groupId })` + `consumer.run({ eachMessage, eachBatch, autoCommitInterval })` | compat layer supports it natively; or use pull-style `fetch()` / async iteration          |
| `consumer.pause([...])` / `resume([...])` / `seek(...)`                                        | same semantics in both layers                                                             |
| `admin.createTopics()` / `fetchTopicOffsets()` / `resetOffsets()` / `describeCluster()`        | `createTopics` / `topicOffsets` / `resetGroupOffsets` / `metadata` (+ compat equivalents) |
| Transactions: `producer.transaction()`                                                         | `beginTransaction()/commitTransaction()/abortTransaction()` + `sendOffsetsToTransaction`  |

## Why move to the native API

The compat layer exists to unblock migration and run third-party code. The native client gives
you:

- Pull-based bounded batches (`fetch({ maxMessages })`) with explicit backpressure.
- Zero-copy message views by default (less GC pressure at high throughput).
- Incremental fetch sessions, static membership, cooperative-sticky assignment as first-class
  options (all documented in [consuming](consuming.md)).
- Direct typed access to everything: quotas, ACLs, delegation tokens, health checks.

## Behavior differences to know

- Default commits are manual in the native layer (`autoCommit: false`) — commit when your work
  is durable.
- Errors are typed `KafkaError` with protocol codes instead of kafkajs error classes; the compat
  layer translates for vendored code.
- Compression codecs are always available (no optional native codec installs).
