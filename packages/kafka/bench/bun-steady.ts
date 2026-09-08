import assert from "node:assert/strict";
import { Kafka } from "../index.ts";

// Run through run-steady.ts: it creates a fresh topic and enforces a process deadline.
const topic = process.env.BENCH_TOPIC!;
const size = Number(process.env.MSG_SIZE ?? 100);
const batch = Number(process.env.BENCH_BATCH ?? 100);
const windows = Number(process.env.BENCH_WINDOWS ?? 1000);
const warmup = 100;
assert(topic && [size, batch, windows].every((n) => Number.isSafeInteger(n) && n > 0));
assert(size >= 8 && size * batch <= 512 * 1024 && windows > 1 && windows <= 10000);
const kafka = new Kafka({ brokers: (process.env.KAFKA_BROKERS ?? "127.0.0.1:19092").split(",") });
const producer = kafka.producer({
  lingerMs: 0,
  batchMaxMessages: 1_000_000,
  compression: "none",
  idempotent: false,
});
const consumer = kafka.consumer({
  autoCommit: false,
  isolationLevel: "read_uncommitted",
  fetchMaxBytes: 1024 * 1024,
});
const payload = new Uint8Array(size).fill(0x78);
const keys = Array.from({ length: 64 }, (_, i) => new TextEncoder().encode(String(i)));
let sent = 0;
let received = 0;
const deadline = setTimeout(() => {
  throw new Error("benchmark deadline exceeded");
}, 120_000);

async function produce(): Promise<void> {
  const messages = Array.from({ length: batch }, () => {
    const seq = sent++;
    const value = payload.slice();
    new DataView(value.buffer).setBigUint64(0, BigInt(seq), true);
    return { partition: 0, key: keys[seq % 64]!, value };
  });
  if (process.env.BENCH_MODE === "bulk") {
    await producer.send({ topic, messages, acks: 1 });
  } else {
    await Promise.all(
      messages.map((message) => producer.send({ topic, messages: [message], acks: 1 })),
    );
  }
}

async function consume(target: number): Promise<void> {
  while (received < target) {
    const messages = await consumer.fetch({
      maxWaitMs: 10,
      minBytes: 1,
      maxBytes: 1024 * 1024,
      maxPartitionBytes: 1024 * 1024,
      maxMessages: target - received,
      copy: false,
    });
    for (const message of messages) {
      const seq = received++;
      assert.equal(message.offset, BigInt(seq));
      assert.equal(message.partition, 0);
      assert.equal(message.value?.byteLength, size);
      assert.equal(
        new DataView(message.value!.buffer, message.value!.byteOffset).getBigUint64(0, true),
        BigInt(seq),
      );
      assert.equal(Buffer.compare(message.key!, keys[seq % 64]!), 0);
      assert.equal(Buffer.compare(message.value!.subarray(8), payload.subarray(8)), 0);
    }
  }
}

try {
  await consumer.assign([{ topic, partition: 0, offset: 0n }]);
  for (let i = 0; i < warmup; i++) await produce();
  await consume(sent);
  consumer.pause([{ topic, partition: 0 }]);
  await Bun.sleep(50);
  console.log(JSON.stringify({ phase: "start" }));
  const latency: number[] = [];
  const started = performance.now();
  for (let i = 0; i < windows; i++) {
    const before = performance.now();
    await produce();
    latency.push(performance.now() - before);
  }
  const produceMs = performance.now() - started;
  const resumeStart = performance.now();
  consumer.resume([{ topic, partition: 0 }]);
  await consume((warmup + 1) * batch);
  const resumeMs = performance.now() - resumeStart;
  const consumeStart = performance.now();
  await consume(sent);
  console.log(
    JSON.stringify({
      phase: "result",
      lib: "bun-kafka",
      count: windows * batch,
      produce_ms: produceMs,
      consume_ms: performance.now() - consumeStart,
      consume_count: (windows - 1) * batch,
      resume_ms: resumeMs,
      window_ack_ms: latency,
      validated: received,
    }),
  );
  // Let the runner sample /proc before shutdown.
  await Bun.sleep(100);
} finally {
  clearTimeout(deadline);
  await kafka.disconnect();
}
