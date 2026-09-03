---
title: Configuration reference
description: Find client, producer, consumer, retry, and shutdown options.
---

## Kafka options

Passed to `new Kafka(options)`:

| Option             | Type                          | Default       | What it does                                                                                               |
| ------------------ | ----------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------- |
| `brokers`          | `string[]`                    | required      | Bootstrap list, e.g. `["b1:9092"]`. Addresses accept `host:port` or `kafka://host:port`; default port 9092 |
| `clientId`         | `string`                      | `"bun-kafka"` | Sent with every request; shows up in broker logs and quota rules                                           |
| `rackId`           | `string`                      | —             | Enables rack-aware follower fetching on Kafka 2.4+ (KIP-392)                                               |
| `tls`              | `boolean \| Bun.TLSOptions`   | —             | Enable TLS; see [security](security.md)                                                                    |
| `sasl`             | object                        | —             | PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / OAUTHBEARER                                                        |
| `requestTimeoutMs` | number                        | `30000`       | Per-request timeout; pending requests reject as retriable on expiry                                        |
| `connectTimeoutMs` | number                        | `10000`       | Bounds a single TCP/TLS connect attempt                                                                    |
| `maxResponseBytes` | number                        | `104857600`   | Frame size guard against malformed brokers                                                                 |
| `retry`            | `RetryOptions`                | below         | Bounded retry policy for retriable errors                                                                  |
| `onEvent`          | `(event: KafkaEvent) => void` | —             | Receives `{type:"retry"}`, `{type:"throttle"}`, `{type:"stats"}` events                                    |
| `statsIntervalMs`  | number                        | —             | Emit a stats event on this interval                                                                        |
| `logger`           | `Partial<Logger>`             | —             | `debug/info/warn/error` hooks used by retry, throttle, and auth paths                                      |

### RetryOptions

| Option             | Default | Meaning                             |
| ------------------ | ------- | ----------------------------------- |
| `maxRetries`       | `3`     | Retries after the first attempt     |
| `initialBackoffMs` | `50`    | First backoff (with 50–100% jitter) |
| `maxBackoffMs`     | `2000`  | Backoff ceiling                     |

Retriable conditions: connection loss, timeouts, leader elections, coordinator moves, throttling
— the standard retriable error codes from the protocol.

## Producer options

`kafka.producer({...})` — full details in [producing](producing.md):

| Option                 | Default                                    |
| ---------------------- | ------------------------------------------ |
| `lingerMs`             | `5`                                        |
| `batchMaxMessages`     | `1000`                                     |
| `compression`          | `"none"` (`gzip`, `snappy`, `lz4`, `zstd`) |
| `idempotent`           | `false`                                    |
| `partitioner`          | key → Murmur2, keyless → round-robin       |
| `transactionalId`      | — (enables transactions)                   |
| `transactionTimeoutMs` | `60000`                                    |

Per-request: `acks` (`1` default), `timeoutMs`, `compression` override.

## Consumer options

`kafka.consumer({...})` — full details in [consuming](consuming.md):

| Option                                  | Default                                                 |
| --------------------------------------- | ------------------------------------------------------- |
| `fromBeginning`                         | `false`                                                 |
| `groupId`                               | — (manual assignment without it)                        |
| `groupInstanceId`                       | — (static membership, KIP-345)                          |
| `partitionAssigner`                     | `"range"` (`"cooperative-sticky"` = KIP-429)            |
| `isolationLevel`                        | `"read_uncommitted"`                                    |
| `sessionTimeoutMs`                      | broker-typical `30000` range enforced by cluster config |
| `rebalanceTimeoutMs`                    | `60000`                                                 |
| `heartbeatIntervalMs`                   | derived from session timeout                            |
| `autoCommit`                            | `false`                                                 |
| `fetchMaxBytes`                         | `52428800`                                              |
| `keyDeserializer` / `valueDeserializer` | raw bytes                                               |

### Fetch options

Per-`fetch()` knobs: `maxWaitMs` (`100`), `minBytes` (`1`), `maxBytes`, `maxPartitionBytes`,
`maxMessages`, `copy`.

## Graceful shutdown

```ts
import { installShutdown } from "bun-kafka";

installShutdown([producer, consumer, kafka]); // flush + close on SIGTERM/SIGINT
```

Order matters: producers flush first. Pass `kafka` last so connections close after clients.
Returns an uninstall function if you need to remove the handlers in tests.
