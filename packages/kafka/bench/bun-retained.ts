import assert from "node:assert/strict";
import { Kafka } from "../index.ts";

const kafka = new Kafka({ brokers: (process.env.KAFKA_BROKERS ?? "127.0.0.1:19092").split(",") });
const admin = kafka.admin();
const topic = `retained-57-${crypto.randomUUID()}`;
const copy = process.env.BENCH_COPY === "1";
const size = 65536;
const count = 2048;
const payload = new Uint8Array(size).fill(0x78);
const retained: Uint8Array[] = [];
const deadline = setTimeout(() => {
  throw new Error("retained-memory deadline");
}, 120_000);
const snapshot = () => {
  Bun.gc(true);
  return process.memoryUsage();
};

async function consume(): Promise<void> {
  const consumer = kafka.consumer({ autoCommit: false });
  await consumer.assign([{ topic, partition: 0, offset: 0n }]);
  let received = 0;
  while (received < count) {
    const messages = await consumer.fetch({
      maxWaitMs: 10,
      maxBytes: 1024 * 1024,
      maxPartitionBytes: 1024 * 1024,
      maxMessages: count - received,
      copy,
    });
    for (const message of messages) {
      assert.equal(message.offset, BigInt(received++));
      assert(message.value && Buffer.compare(message.value, payload) === 0);
    }
    if (messages.length) retained.push(messages[0]!.value!);
  }
  await consumer.close();
}

await admin.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }]);
try {
  const producer = kafka.producer({ lingerMs: 0 });
  for (let i = 0; i < count; i += 8) {
    await producer.send({
      topic,
      acks: 1,
      messages: Array.from({ length: 8 }, () => ({ partition: 0, value: payload })),
    });
  }
  await producer.close();
  const baseline = snapshot();
  await consume();
  const held = snapshot();
  for (const value of retained) assert.equal(Buffer.compare(value, payload), 0);
  const backingBytes = [...new Set(retained.map((value) => value.buffer))].reduce(
    (n, buffer) => n + buffer.byteLength,
    0,
  );
  const values = retained.length;
  retained.length = 0;
  const released = snapshot();
  console.log(
    JSON.stringify({
      copy,
      count,
      size,
      retained_values: values,
      retained_value_bytes: values * size,
      retained_backing_bytes: backingBytes,
      baseline,
      held,
      released,
    }),
  );
} finally {
  clearTimeout(deadline);
  await admin.deleteTopics([topic]);
  await kafka.disconnect();
}
