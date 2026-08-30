import { Kafka } from "../index.ts";

const brokers = (process.env.KAFKA_BROKERS ?? "127.0.0.1:9092").split(",");
const topic =
  process.argv[2] ?? `bun-native-bench-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
const count = Number(process.argv[3] ?? 10_000);
const payload = new Uint8Array(Number(process.env.MSG_SIZE ?? 100)).fill(0x78);
const kafka = new Kafka({ brokers, clientId: "bun-kafka-bench", requestTimeoutMs: 120_000 });

try {
  const producer = kafka.producer();
  const startedProduce = performance.now();
  await producer.send({
    topic,
    messages: Array.from({ length: count }, (_, i) => ({
      partition: 0,
      key: String(i % 64),
      value: payload,
    })),
  });
  const produceMs = performance.now() - startedProduce;

  const consumer = kafka.consumer();
  await consumer.assign([{ topic, partition: 0, offset: "earliest" }]);
  const startedConsume = performance.now();
  let consumed = 0;
  while (consumed < count) {
    consumed += (
      await consumer.fetch({
        maxWaitMs: 10,
        maxMessages: count - consumed,
        maxPartitionBytes: 50 * 1024 * 1024,
      })
    ).length;
  }
  const consumeMs = performance.now() - startedConsume;

  console.log(
    JSON.stringify({
      lib: "bun-kafka/native",
      count,
      produce_ms: Number(produceMs.toFixed(2)),
      consume_ms: Number(consumeMs.toFixed(2)),
      produce_msg_s: Math.round((count * 1000) / produceMs),
      consume_msg_s: Math.round((count * 1000) / consumeMs),
    }),
  );
} finally {
  await kafka.disconnect();
}
