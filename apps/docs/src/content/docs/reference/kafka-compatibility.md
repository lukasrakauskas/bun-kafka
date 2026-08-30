---
title: Kafka compatibility
description: Supported Kafka and Redpanda versions, with a quick cluster compatibility check.
---

## Supported brokers

bun-kafka supports Apache Kafka 0.11 or newer and current Kafka-compatible Redpanda releases. It
uses stable Kafka protocol versions that have been available since Kafka 0.11.

The client checks each broker when it connects. If the broker does not support a required API,
the request fails with a typed error instead of sending an incompatible frame.

## Check a cluster

Use the script included with Apache Kafka:

```bash
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

For an application-level check, ask bun-kafka for broker health:

```ts
const report = await kafka.healthCheck();

if (!report.ok) {
  console.error(report.brokers);
}
```

A successful connection and health check confirm that bun-kafka can negotiate with the brokers.
Run a produce and consume smoke test as well to verify authentication, authorization, and topic
configuration.

## Compatibility limits

Broker compatibility does not mean that every Kafka feature is implemented. Review
[Supported features](supported-features.md), especially if you need Kerberos, proxies, KIP-848,
or rack-aware fetching.
