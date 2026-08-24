import { afterEach, beforeAll, describe, expect, test } from "bun:test";
import {
  consumer as makeConsumer,
  dec,
  initNative,
  produceN,
  producer as makeProducer,
  topic,
  waitTopic,
} from "./helpers.ts";

beforeAll(async () => { await initNative(); });

const open: { close: () => Promise<void> | void }[] = [];
function track<T extends { close: () => Promise<void> | void }>(c: T): T {
  open.push(c);
  return c;
}
afterEach(async () => {
  while (open.length) {
    const c = open.pop()!;
    try { await c.close(); } catch {}
  }
});

describe("Consumer.batches", () => {
  test("yields arrays and covers all messages", async () => {
    const t = topic("batch");
    const p = track(makeProducer());
    await produceN(p, t, 50, "b");
    await p.close();
    await waitTopic(t);

    const c = track(makeConsumer());
    c.subscribe(t);

    const got: string[] = [];
    let batches = 0;
    for await (const batch of c.batches({ timeoutMs: 200, batchSize: 16 })) {
      batches++;
      expect(Array.isArray(batch)).toBe(true);
      expect(batch.length).toBeGreaterThan(0);
      expect(batch.length).toBeLessThanOrEqual(16);
      for (const m of batch) got.push(dec(m.value)!);
      if (got.length >= 50) break;
    }
    expect(got.length).toBe(50);
    expect(got[0]).toBe("b-0");
    expect(got[49]).toBe("b-49");
    expect(batches).toBeGreaterThan(1);
    await c.close();
  });

  test("eachBatchCommit commits high-water per partition", async () => {
    const t = topic("batch-c");
    const group = `g-batch-${crypto.randomUUID()}`;
    const p = track(makeProducer());
    await produceN(p, t, 10);
    await p.close();
    await waitTopic(t);

    const c = track(makeConsumer({ "group.id": group }));
    c.subscribe(t);
    let lastOff = -1n;
    let part = 0;
    for await (const batch of c.batches({ eachBatchCommit: true, timeoutMs: 200, batchSize: 4 })) {
      for (const m of batch) {
        lastOff = m.offset;
        part = m.partition;
      }
      if (lastOff >= 9n) break;
    }
    const committed = c.committed([{ topic: t, partition: part }], 10_000);
    expect(committed[0]?.offset).toBe(lastOff + 1n);
    await c.close();
  });
});
