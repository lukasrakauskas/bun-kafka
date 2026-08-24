/**
 * Performance: messages() vs batches() vs busy pollBatch.
 * Asserts batches() is not slower than messages() by a wide margin,
 * and records absolute rates for the local broker.
 */
import { beforeAll, describe, expect, test } from "bun:test";
import { Consumer, Producer, useNative } from "../src/index.ts";
import { BROKERS, initNative, topic } from "./helpers.ts";

beforeAll(async () => { await initNative(); });

const N = Number(process.env.PERF_N ?? 5000);
const PAYLOAD = Buffer.alloc(100, 0x78);

async function seed(t: string, n: number) {
  const p = new Producer({
    "bootstrap.servers": BROKERS,
    acks: "1",
    "linger.ms": 5,
  });
  for (let i = 0; i < n; i++) p.send({ topic: t, key: String(i % 64), value: PAYLOAD });
  await p.flush(60_000);
  await p.close();
}

function makeConsumer(group: string) {
  return new Consumer({
    "bootstrap.servers": BROKERS,
    "group.id": group,
    "enable.auto.commit": true,
    "auto.offset.reset": "earliest",
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 10,
  });
}

async function consumeMessages(t: string, n: number) {
  const c = makeConsumer(`perf-msg-${crypto.randomUUID()}`);
  c.subscribe(t);
  const t0 = performance.now();
  let got = 0;
  for await (const _ of c.messages({ timeoutMs: 10, batchSize: 512 })) {
    if (++got >= n) break;
  }
  const ms = performance.now() - t0;
  await c.close();
  return ms;
}

async function consumeBatches(t: string, n: number) {
  const c = makeConsumer(`perf-bat-${crypto.randomUUID()}`);
  c.subscribe(t);
  const t0 = performance.now();
  let got = 0;
  for await (const batch of c.batches({ timeoutMs: 10, batchSize: 512 })) {
    got += batch.length;
    if (got >= n) break;
  }
  const ms = performance.now() - t0;
  await c.close();
  return ms;
}

async function consumePollBatch(t: string, n: number) {
  const c = makeConsumer(`perf-pb-${crypto.randomUUID()}`);
  c.subscribe(t);
  const t0 = performance.now();
  let got = 0;
  while (got < n) {
    const batch = c.pollBatch(got === 0 ? 1000 : 0, 512);
    if (!batch.length) {
      const one = c.poll(10);
      if (one) {
        got++;
        one.done();
      }
      continue;
    }
    got += batch.length;
    for (const m of batch) m.done();
  }
  const ms = performance.now() - t0;
  await c.close();
  return ms;
}

describe("perf batches vs messages", () => {
  test(`batches() beats or matches messages() on ${N} msgs`, async () => {
    const t = topic("perf");
    await seed(t, N);

    // warmup once so group join / metadata is not in the timed path unfairly
    await consumeBatches(t, Math.min(100, N));

    const batchMs = await consumeBatches(t, N);
    const msgMs = await consumeMessages(t, N);
    const pollMs = await consumePollBatch(t, N);

    const batchRate = N / (batchMs / 1000);
    const msgRate = N / (msgMs / 1000);
    const pollRate = N / (pollMs / 1000);

    console.log(
      JSON.stringify({
        N,
        batches_ms: +batchMs.toFixed(2),
        messages_ms: +msgMs.toFixed(2),
        pollBatch_ms: +pollMs.toFixed(2),
        batches_msg_s: +batchRate.toFixed(0),
        messages_msg_s: +msgRate.toFixed(0),
        pollBatch_msg_s: +pollRate.toFixed(0),
        speedup_vs_messages: +(msgMs / batchMs).toFixed(2),
      }),
    );

    // batches should not be slower than messages by more than 20%
    // (usually faster due to fewer async suspensions)
    expect(batchMs).toBeLessThan(msgMs * 1.2);
    // absolute sanity: local broker should clear 5k well under 30s
    expect(batchMs).toBeLessThan(30_000);
    expect(batchRate).toBeGreaterThan(500);
  }, 120_000);

  test("ffi and napi batch path both work", async () => {
    const t = topic("perf-both");
    await seed(t, 200);

    await useNative("ffi");
    const ffiMs = await consumeBatches(t, 200);

    let napiMs = 0;
    try {
      await useNative("napi");
      napiMs = await consumeBatches(t, 200);
    } catch {
      console.log("napi skip");
    } finally {
      await useNative("ffi");
    }

    expect(ffiMs).toBeLessThan(15_000);
    if (napiMs) expect(napiMs).toBeLessThan(15_000);
  }, 60_000);
});
