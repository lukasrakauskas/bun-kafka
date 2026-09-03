---
title: Migrating from kafkajs
description: Move an existing kafkajs application to bun-kafka in small steps.
---

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

## Compatibility events

Every event includes `{ id, type, timestamp }`. The additional payloads are:

| Client   | Events                                                      | Additional payload                                                                                               |
| -------- | ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Consumer | `CONNECT`, `DISCONNECT`, `STOP`, `FETCH_START`, `HEARTBEAT` | None. `HEARTBEAT` fires when a message/batch handler calls its `heartbeat()` helper.                             |
| Consumer | `CRASH`                                                     | `{ error, groupId }`                                                                                             |
| Consumer | `REBALANCING`                                               | `{ groupId, memberId }`, emitted when a core group join starts, including heartbeat-triggered rejoins            |
| Consumer | `GROUP_JOIN`                                                | `{ groupId, memberId, generationId, memberAssignment, duration }`, emitted only after a core group join succeeds |
| Consumer | `FETCH`                                                     | `{ numberOfMessages }`                                                                                           |
| Consumer | `START_BATCH_PROCESS`                                       | `{ topic, partition, size }`                                                                                     |
| Consumer | `END_BATCH_PROCESS`                                         | `{ topic, partition }`                                                                                           |
| Consumer | `COMMIT_OFFSETS`                                            | `{ groupId, topics }`                                                                                            |
| Producer | `CONNECT`, `DISCONNECT`                                     | None                                                                                                             |
| Producer | `REQUEST`                                                   | `{ apiKey, apiVersion, broker, duration, error? }` for each completed request attempt                            |
| Producer | `REQUEST_TIMEOUT`                                           | `{ apiKey, apiVersion, broker, duration, error }`                                                                |
| Producer | `REQUEST_QUEUE_SIZE`                                        | `{ queueSize }` after `send` or `sendBatch` queues messages                                                      |
| Admin    | `CONNECT`, `DISCONNECT`                                     | None                                                                                                             |
| Admin    | `REQUEST`                                                   | `{ apiKey, apiVersion, broker, duration, error? }` for each completed request attempt                            |
| Admin    | `REQUEST_TIMEOUT`                                           | `{ apiKey, apiVersion, broker, duration, error }`                                                                |

Request listeners can observe retries as multiple attempts. bun-kafka does not expose KafkaJS's request/response
payloads, correlation IDs, request sizes, or the internal `ApiVersions` negotiation frame; those payload fields
are unsupported. Listener exceptions are ignored so instrumentation cannot change client behavior.

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
