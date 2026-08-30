import { describe, expect, test } from "bun:test";
import { consumer, dec, produceN, producer, topic, waitFor } from "../helpers.ts";

describe("Consumer", () => {
  test("subscribe + fetch consumes all messages", async () => {
    const name = topic("iter");
    const p = producer();
    await produceN(p, name, 25, "hello");
    await p.close();

    const c = consumer({ fromBeginning: true });
    try {
      await c.subscribe(name);
      const got: string[] = [];
      while (got.length < 25) {
        got.push(
          ...(await c.fetch({ maxWaitMs: 200, maxMessages: 25 - got.length })).map((m) =>
            dec(m.value),
          ),
        );
      }
      expect(got[0]).toBe("hello-0");
      expect(got[24]).toBe("hello-24");
    } finally {
      await c.close();
    }
  }, 60_000);

  test("for-await directly on consumer", async () => {
    const name = topic("forawait");
    const p = producer();
    await produceN(p, name, 5);
    await p.close();

    const c = consumer({ fromBeginning: true });
    try {
      await c.subscribe([name]);
      let count = 0;
      for await (const message of c) {
        expect(message.topic).toBe(name);
        expect(message.key).toBeTruthy();
        expect(message.headers.i).toBeTruthy();
        if (++count >= 5) {
          break;
        }
      }
      expect(count).toBe(5);
    } finally {
      await c.close();
    }
  }, 60_000);

  test("assign specific partition + seek + position", async () => {
    const name = topic("assign");
    const p = producer();
    await produceN(p, name, 20);
    await p.close();

    const c = consumer();
    try {
      await c.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const first = await waitFor(() =>
        c.fetch({ maxWaitMs: 500, maxMessages: 1 }).then((messages) => messages[0]),
      );
      expect(first.partition).toBe(0);
      const start = first.offset;
      await c.fetch({ maxWaitMs: 500, maxMessages: 4 });
      c.seek({ topic: name, partition: 0, offset: start });
      const again = (await c.fetch({ maxWaitMs: 500, maxMessages: 1 }))[0];
      expect(again.offset).toBe(start);
      expect(c.position(name, 0)).toBe(start + 1n);
    } finally {
      await c.close();
    }
  }, 60_000);

  test("pause and resume", async () => {
    const name = topic("pause");
    const p = producer();
    await produceN(p, name, 5);
    await p.close();

    const c = consumer();
    try {
      await c.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      c.pause([{ topic: name, partition: 0 }]);
      expect(await c.fetch({ maxWaitMs: 20 })).toHaveLength(0);
      c.resume([{ topic: name, partition: 0 }]);
      expect(await c.fetch({ maxWaitMs: 1_000, maxMessages: 1 })).toHaveLength(1);
    } finally {
      await c.close();
    }
  }, 60_000);

  test("query watermark offsets", async () => {
    const name = topic("wm");
    const p = producer();
    await produceN(p, name, 7);
    await p.close();

    const c = consumer();
    try {
      const watermarks = await c.watermarks(name, 0);
      expect(watermarks.high - watermarks.low).toBeGreaterThanOrEqual(7n);
    } finally {
      await c.close();
    }
  }, 60_000);

  test("closed consumer rejects", async () => {
    const c = consumer();
    await c.close();
    expect(() => c.subscribe("x")).toThrow();
  });
});
