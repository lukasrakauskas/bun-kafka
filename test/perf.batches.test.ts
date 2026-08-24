import { describe, expect, test } from "bun:test";
import { consumer, producer, topic } from "./helpers.ts";

const N = Number(process.env.PERF_N ?? 5_000);

async function seed(name: string, count: number) {
  const p = producer();
  await p.send({
    topic: name,
    messages: Array.from({ length: count }, (_, i) => ({ key: String(i % 64), value: "x".repeat(100) })),
  });
  await p.close();
}

async function consumeMessages(name: string, count: number) {
  const c = consumer({ fromBeginning: true });
  await c.subscribe(name);
  const start = performance.now();
  let received = 0;
  for await (const _ of c) if (++received >= count) break;
  await c.close();
  return performance.now() - start;
}

async function consumeBatches(name: string, count: number) {
  const c = consumer({ fromBeginning: true });
  await c.subscribe(name);
  const start = performance.now();
  let received = 0;
  while (received < count) received += (await c.fetch({ maxWaitMs: 10, maxMessages: 512 })).length;
  await c.close();
  return performance.now() - start;
}

describe("Bun consumer batch performance", () => {
  test(`fetch batches and messages on ${N} records`, async () => {
    const name = topic("perf");
    await seed(name, N);
    const batchMs = await consumeBatches(name, N);
    const messageMs = await consumeMessages(name, N);
    expect(batchMs).toBeLessThan(messageMs * 1.2);
    expect(batchMs).toBeLessThan(30_000);
  }, 120_000);
});
