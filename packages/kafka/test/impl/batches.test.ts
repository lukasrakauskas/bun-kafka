import { afterEach, describe, expect, test } from "bun:test";
import {
  consumer as makeConsumer,
  dec,
  produceN,
  producer as makeProducer,
  topic,
} from "../helpers.ts";

const open: { close(): Promise<void> }[] = [];
function track<T extends { close(): Promise<void> }>(client: T): T {
  open.push(client);
  return client;
}
afterEach(async () => {
  while (open.length) {
    try {
      await open.pop().close();
    } catch {}
  }
});

describe("Bun consumer batches", () => {
  test("fetches bounded batches and covers all messages", async () => {
    const name = topic("batch");
    const p = track(makeProducer());
    await produceN(p, name, 50, "b");
    await p.close();

    const c = track(makeConsumer({ fromBeginning: true }));
    await c.assign([{ topic: name, partition: 0, offset: "earliest" }]);
    const got: string[] = [];
    let batches = 0;
    while (got.length < 50) {
      const batch = await c.fetch({ maxWaitMs: 200, maxMessages: 16 });
      if (!batch.length) {
        continue;
      }
      batches++;
      expect(batch.length).toBeLessThanOrEqual(16);
      for (const message of batch) {
        got.push(dec(message.value));
      }
    }
    expect(got).toHaveLength(50);
    expect(got[0]).toBe("b-0");
    expect(got[49]).toBe("b-49");
    expect(batches).toBeGreaterThan(1);
  }, 60_000);
});
