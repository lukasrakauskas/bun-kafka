import { beforeAll, describe, expect, test } from "bun:test";
import { RD_KAFKA_OFFSET_BEGINNING } from "../src/index.ts";
import { consumer, dec, initNative, produceN, producer, topic, waitFor } from "./helpers.ts";

beforeAll(async () => { await initNative(); });

describe("Consumer", () => {
  test("subscribe + async iterable consumes all messages", async () => {
    const t = topic("iter");
    const p = producer();
    await produceN(p, t, 25, "hello");
    await p.close();

    const c = consumer({ "auto.offset.reset": "earliest" });
    c.subscribe(t);

    const got: string[] = [];
    const deadline = Date.now() + 30_000;
    for await (const msg of c.messages({ timeoutMs: 200 })) {
      got.push(dec(msg.value)!);
      if (got.length >= 25 || Date.now() > deadline) break;
    }
    expect(got.length).toBe(25);
    expect(got[0]).toBe("hello-0");
    expect(got[24]).toBe("hello-24");
    await c.close();
  });

  test("for-await directly on consumer", async () => {
    const t = topic("forawait");
    const p = producer();
    await produceN(p, t, 5);
    await p.close();

    const c = consumer();
    c.subscribe([t]);
    let n = 0;
    for await (const msg of c) {
      expect(msg.topic).toBe(t);
      expect(msg.key).toBeTruthy();
      expect(msg.headers.i).toBeTruthy();
      n++;
      if (n >= 5) break;
    }
    expect(n).toBe(5);
    await c.close();
  });

  test("assign specific partition + seek + position", async () => {
    const t = topic("assign");
    const p = producer();
    await produceN(p, t, 20);
    await p.close();

    const c = consumer({ "enable.auto.offset.store": false });
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);

    const first = await waitFor(() => c.poll(500), { timeoutMs: 20_000 });
    expect(first.partition).toBe(0);
    const startOff = first.offset;
    first.done();

    for (let i = 0; i < 4; i++) {
      const m = await waitFor(() => c.poll(500));
      m.done();
    }

    c.seek([{ topic: t, partition: 0, offset: startOff }]);
    const again = await waitFor(() => c.poll(500));
    expect(again.offset).toBe(startOff);
    again.done();

    const pos = c.position([{ topic: t, partition: 0 }]);
    expect(pos[0]?.topic).toBe(t);
    await c.close();
  });

  test("commit + committed offsets", async () => {
    const t = topic("commit");
    const group = `g-commit-${crypto.randomUUID()}`;
    const p = producer();
    await produceN(p, t, 10);
    await p.close();

    const c = consumer({ "group.id": group });
    c.subscribe(t);
    const msgs: { topic: string; partition: number; offset: bigint }[] = [];
    for await (const msg of c.messages({ timeoutMs: 200 })) {
      msgs.push({ topic: msg.topic, partition: msg.partition, offset: msg.offset });
      if (msgs.length >= 10) break;
    }
    expect(msgs.length).toBe(10);
    const last = msgs[msgs.length - 1]!;
    c.commit([{ topic: last.topic, partition: last.partition, offset: last.offset + 1n }]);

    const committed = c.committed([{ topic: t, partition: last.partition }], 10_000);
    expect(committed[0]?.offset).toBe(last.offset + 1n);
    await c.close();
  });

  test("eachMessageCommit option", async () => {
    const t = topic("each");
    const group = `g-each-${crypto.randomUUID()}`;
    const p = producer();
    await produceN(p, t, 3);
    await p.close();

    const c = consumer({ "group.id": group });
    c.subscribe(t);
    let lastOff = -1n;
    let part = 0;
    for await (const msg of c.messages({ eachMessageCommit: true, timeoutMs: 200 })) {
      lastOff = msg.offset;
      part = msg.partition;
      if (msg.offset >= 2n) break;
    }
    const committed = c.committed([{ topic: t, partition: part }], 10_000);
    expect(committed[0]?.offset).toBe(lastOff + 1n);
    await c.close();
  });

  test("pause and resume", async () => {
    const t = topic("pause");
    const p = producer();
    await produceN(p, t, 5);
    await p.close();

    const c = consumer();
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const m1 = await waitFor(() => c.poll(500));
    m1.done();
    c.pause([{ topic: t, partition: 0 }]);
    c.resume([{ topic: t, partition: 0 }]);
    const m2 = await waitFor(() => c.poll(1000));
    expect(m2).toBeTruthy();
    m2.done();
    await c.close();
  });

  test("query watermark offsets", async () => {
    const t = topic("wm");
    const p = producer();
    await produceN(p, t, 7);
    await p.close();

    const c = consumer();
    const wm = c.queryWatermarkOffsets(t, 0, 10_000);
    expect(wm.high - wm.low).toBeGreaterThanOrEqual(7n);
    await c.close();
  });

  test("subscription listing", async () => {
    const t = topic("sub");
    const c = consumer();
    c.subscribe([t]);
    const sub = c.subscription();
    expect(sub.some((s) => s.topic === t)).toBe(true);
    c.unsubscribe();
    await c.close();
  });

  test("memberId API", async () => {
    const c = consumer();
    const id = c.memberId();
    expect(id === null || typeof id === "string").toBe(true);
    await c.close();
  });

  test("closed consumer rejects", async () => {
    const c = consumer();
    await c.close();
    expect(() => c.subscribe("x")).toThrow();
  });
});
