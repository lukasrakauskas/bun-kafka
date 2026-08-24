/**
 * Usage: bun bench/produce_consume.ts <topic> <count>
 * Env: KAFKA_BROKERS, BUN_KAFKA_NATIVE=ffi|napi, ACKS, MSG_SIZE
 */
import { Producer, Consumer, useNative } from "../src/index.ts";

const brokers = process.env.KAFKA_BROKERS ?? "127.0.0.1:9092";
const topic = process.argv[2] ?? `bench-${Date.now()}`;
const count = Number(process.argv[3] ?? 10000);
const kind = (process.env.BUN_KAFKA_NATIVE as "ffi" | "napi") || "ffi";
const deadlineMs = Number(process.env.BENCH_DEADLINE_MS ?? 120_000);

await useNative(kind);

const payload = Buffer.alloc(Number(process.env.MSG_SIZE ?? 100), 0x78); // reusable bytes, no per-msg encode

const p = new Producer({
  "bootstrap.servers": brokers,
  acks: process.env.ACKS ?? "1",
  "linger.ms": 5,
  "batch.num.messages": 10000,
  "queue.buffering.max.messages": 1000000,
  "socket.nagle.disable": true,
});
const t0 = performance.now();
for (let i = 0; i < count; i++) {
  p.send({ topic, key: String(i % 64), value: payload });
}
await p.flush(deadlineMs);
const produceMs = performance.now() - t0;
await p.close();

const c = new Consumer({
  "bootstrap.servers": brokers,
  "group.id": `bench-${crypto.randomUUID()}`,
  "enable.auto.commit": true,
  "auto.offset.reset": "earliest",
  "fetch.min.bytes": 1,
  "fetch.wait.max.ms": 10,
});
c.subscribe(topic);
const t1 = performance.now();
let n = 0;
const stopAt = t1 + deadlineMs;
for await (const _ of c.messages({ timeoutMs: 10, batchSize: 512 })) {
  n++;
  if (n >= count) break;
  if (performance.now() > stopAt) throw new Error(`consume timeout at ${n}/${count}`);
}
const consumeMs = performance.now() - t1;
await c.close();

if (n < count) throw new Error(`only consumed ${n}/${count}`);

console.log(JSON.stringify({
  lib: `bun-kafka/${kind}`,
  topic,
  count,
  produce_ms: +produceMs.toFixed(2),
  consume_ms: +consumeMs.toFixed(2),
  produce_msg_s: +(count / (produceMs / 1000)).toFixed(0),
  consume_msg_s: +(count / (consumeMs / 1000)).toFixed(0),
}));
