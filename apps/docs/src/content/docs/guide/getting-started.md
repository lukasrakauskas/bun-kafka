---
title: Getting started
description: Install bun-kafka and send and receive your first Kafka record.
---

This guide takes you from installation to a working producer and consumer.

## Requirements

- [Bun](https://bun.sh) 1.4 or newer
- Apache Kafka 0.11 or newer, or a Kafka-compatible Redpanda broker

## Install bun-kafka

```bash
bun add bun-kafka
```

## Start a local broker

Skip this step if you already have a broker. For local development, start Redpanda in Docker:

```bash
docker run -d --name redpanda -p 9092:9092 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --mode dev-container \
  --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr=PLAINTEXT://127.0.0.1:9092
```

## Send a record

Create `example.ts`:

```ts
import { Kafka } from "bun-kafka";

const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
const producer = kafka.producer();

await producer.send({
  topic: "hello",
  messages: [{ key: "greeting", value: "hello world" }],
});

await kafka.disconnect();
```

Run it:

```bash
bun example.ts
```

Most development brokers create the topic automatically. If yours does not, create `hello` with
your broker tools or the [admin API](admin.md).

## Read the record

Replace `example.ts` with:

```ts
import { Kafka } from "bun-kafka";

const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
const consumer = kafka.consumer({ fromBeginning: true });

await consumer.assign([{ topic: "hello", partition: 0, offset: "earliest" }]);

const [message] = await consumer.fetch({ maxMessages: 1, maxWaitMs: 5_000 });
if (!message) throw new Error("No record received");

console.log(new TextDecoder().decode(message.value!));

await consumer.close();
await kafka.disconnect();
```

Run it again. The output is `hello world`.

## Choose your next guide

- [Producing](producing.md): delivery guarantees, batching, compression, and partitioning
- [Consuming](consuming.md): consumer groups, offsets, and flow control
- [Security](security.md): TLS and SASL authentication
- [Configuration reference](configuration.md): all client, producer, and consumer options
