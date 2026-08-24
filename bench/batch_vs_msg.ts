/** Compare messages() vs batches() wall time. */
import { Producer, Consumer, useNative } from "../src/index.ts";

const brokers = process.env.KAFKA_BROKERS ?? "127.0.0.1:19092";
const count = Number(process.argv[2] ?? 20000);
const kind = (process.env.BUN_KAFKA_NATIVE as "ffi" | "napi") || "ffi";
await useNative(kind);

const topic = `batch-bench-${Date.now()}`;
const payload = Buffer.alloc(Number(process.env.MSG_SIZE ?? 100), 0x78);

const p = new Producer({ "bootstrap.servers": brokers, acks: "1", "linger.ms": 5 });
for (let i = 0; i < count; i++) p.send({ topic, key: String(i % 64), value: payload });
await p.flush(120_000);
await p.close();

async function run(mode: "messages" | "batches") {
  const c = new Consumer({
    "bootstrap.servers": brokers,
    "group.id": `bb-${mode}-${crypto.randomUUID()}`,
    "enable.auto.commit": true,
    "auto.offset.reset": "earliest",
    "fetch.wait.max.ms": 10,
  });
  c.subscribe(topic);
  const t0 = performance.now();
  let n = 0;
  if (mode === "messages") {
    for await (const _ of c.messages({ timeoutMs: 10, batchSize: 512 })) {
      if (++n >= count) break;
    }
  } else {
    for await (const batch of c.batches({ timeoutMs: 10, batchSize: 512 })) {
      n += batch.length;
      if (n >= count) break;
    }
  }
  const ms = performance.now() - t0;
  await c.close();
  return { mode, ms, msg_s: count / (ms / 1000), n };
}

const msg = await run("messages");
const bat = await run("batches");
console.log(JSON.stringify({
  lib: `bun-kafka/${kind}`,
  count,
  messages: { ms: +msg.ms.toFixed(2), msg_s: +msg.msg_s.toFixed(0) },
  batches: { ms: +bat.ms.toFixed(2), msg_s: +bat.msg_s.toFixed(0) },
  speedup: +(msg.ms / bat.ms).toFixed(2),
}));
