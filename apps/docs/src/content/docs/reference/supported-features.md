---
title: Supported features
description: Check whether bun-kafka supports the Kafka features your application needs.
---

bun-kafka covers the common producer, consumer, administration, and security workflows. Use this
page to check fit before you adopt it.

## Client capabilities

| Area                | Supported                                                                                  |
| ------------------- | ------------------------------------------------------------------------------------------ |
| Produce             | Acknowledgements `0`, `1`, and `"all"`; automatic batching; delivery callbacks             |
| Delivery safety     | Idempotent producers and Kafka transactions                                                |
| Compression         | Gzip, Zstandard, Snappy, and LZ4                                                           |
| Consume             | Manual partitions, whole-topic assignment, and consumer groups                             |
| Group management    | Range and cooperative-sticky assignment, static membership, committed offsets              |
| Transactional reads | `read_committed` isolation                                                                 |
| Administration      | Topics, partitions, configs, groups, offsets, records, quotas, ACLs, and delegation tokens |
| Security            | TLS, mTLS, SASL/PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER                                  |
| Operations          | Typed errors, bounded retries, stats events, logging hooks, and broker health checks       |
| kafkajs migration   | Compatibility API through `bun-kafka/kafkajs-compat`                                       |

## Current limits

bun-kafka does not support:

- Kerberos/GSSAPI authentication
- Kafka connections through an HTTP or SOCKS proxy
- The KIP-848 next-generation consumer group protocol
- Rack-aware replica fetching

The classic consumer group protocol remains supported by current Kafka releases.

## Runtime and broker support

- Bun 1.4 or newer
- Apache Kafka 0.11 or newer
- Kafka-compatible Redpanda brokers

bun-kafka checks protocol support when it connects and returns a clear error if a broker does not
support a required API version. See [Kafka compatibility](kafka-compatibility.md) for details.
