/** Optional kafkajs comparison if dependency is present. */
import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "127.0.0.1:9092").split(",");
const topic = process.argv[2] ?? `bench-kjs-${Date.now()}`;
const count = Number(process.argv[3] ?? 10000);
const payload = "x".repeat(Number(process.env.MSG_SIZE ?? 100));

const kafka = new Kafka({ clientId: "bench", brokers });
const admin = kafka.admin();
await admin.connect();
try {
  await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] });
} catch {}
await admin.disconnect();

const producer = kafka.producer();
await producer.connect();
const t0 = performance.now();
const batch: { key: string; value: string }[] = [];
for (let i = 0; i < count; i++) {
  batch.push({ key: String(i % 64), value: payload });
  if (batch.length >= 1000) {
    await producer.send({ topic, messages: batch.splice(0) });
  }
}
if (batch.length) {
  await producer.send({ topic, messages: batch });
}
const produceMs = performance.now() - t0;
await producer.disconnect();

const consumer = kafka.consumer({ groupId: `kjs-${crypto.randomUUID()}` });
await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });
const t1 = performance.now();
let n = 0;
await new Promise<void>((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error("timeout")), 120_000);
  consumer
    .run({
      eachMessage: async () => {
        n++;
        if (n >= count) {
          clearTimeout(timer);
          resolve();
        }
      },
    })
    .catch(reject);
});
const consumeMs = performance.now() - t1;
await consumer.disconnect();

console.log(
  JSON.stringify({
    lib: "kafkajs",
    topic,
    count,
    produce_ms: Number(produceMs.toFixed(2)),
    consume_ms: Number(consumeMs.toFixed(2)),
    produce_msg_s: Number((count / (produceMs / 1000)).toFixed(0)),
    consume_msg_s: Number((count / (consumeMs / 1000)).toFixed(0)),
  }),
);
