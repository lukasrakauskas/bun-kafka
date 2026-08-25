# Getting started

## Requirements

- [Bun](https://bun.sh) 1.4 or newer
- Any Apache Kafka 0.11+ or Redpanda broker reachable over TCP

## Install

```bash
bun add bun-kafka
```

The package has zero runtime dependencies: it speaks the Kafka wire protocol directly through
`Bun.connect()`.

## Start a local broker

Redpanda in one container is the fastest way to get a Kafka-compatible cluster:

```bash
docker run -d --name redpanda -p 9092:9092 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --mode dev-container \
  --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr=PLAINTEXT://127.0.0.1:9092
```

## Produce your first record

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

Topics are created automatically by most dev brokers. If yours does not, create the topic first
with the admin API (see [administration](admin.md)).

## Consume your first records

Manual assignment needs no consumer group:

```ts
const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
const consumer = kafka.consumer({ fromBeginning: true });

await consumer.assign([{ topic: "hello", partition: 0, offset: "earliest" }]);

for await (const message of consumer) {
  console.log(message.offset, new TextDecoder().decode(message.value!));
  if (message.offset >= 9n) break;
}

await consumer.close();
await kafka.disconnect();
```

`message.value` is a `Uint8Array` view into the receive buffer — decode it with
`TextDecoder`, `JSON.parse`, or your format of choice.

## Next steps

- [Producing](producing.md): batching, compression, idempotence, delivery callbacks
- [Consuming](consuming.md): consumer groups, offsets, rebalancing
- [Transactions](transactions.md): exactly-once patterns
- [Configuration reference](configuration.md): every option
