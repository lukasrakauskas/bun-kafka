/**
 * Exhaustive public-API feature coverage.
 * Every Producer / Consumer / Admin / module export used at least once.
 */
import { afterEach, beforeAll, describe, expect, test } from "bun:test";
import {
  Consumer,
  KafkaError,
  version,
  useNative,
  getDriver,
  setDriver,
  ffiDriver,
  RD_KAFKA_PARTITION_UA,
  RD_KAFKA_OFFSET_BEGINNING,
  RD_KAFKA_OFFSET_END,
  RD_KAFKA_OFFSET_STORED,
  RD_KAFKA_OFFSET_INVALID,
  RD_KAFKA_RESP_ERR_NO_ERROR,
  RD_KAFKA_RESP_ERR__PARTITION_EOF,
  RD_KAFKA_RESP_ERR__TIMED_OUT,
} from "../src/index.ts";
import {
  BROKERS,
  consumer as makeConsumer,
  dec,
  initNative,
  produceN,
  producer as makeProducer,
  topic,
  waitFor,
  waitTopic,
  admin as makeAdmin,
} from "./helpers.ts";

beforeAll(async () => {
  await initNative();
});

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

function producer(extra: Record<string, string | number | boolean> = {}) {
  return track(makeProducer(extra));
}
function consumer(extra: Record<string, string | number | boolean> = {}) {
  return track(makeConsumer(extra));
}
function admin() {
  return track(makeAdmin());
}
async function seed(t: string, n: number) {
  const p = producer();
  await produceN(p, t, n);
  await p.close();
  await waitTopic(t);
}

describe("module exports", () => {
  test("version + constants + driver swap", async () => {
    const v = version();
    expect(v.string.length).toBeGreaterThan(0);
    expect(v.number).toBeGreaterThan(0);
    expect(RD_KAFKA_PARTITION_UA).toBe(-1);
    expect(RD_KAFKA_OFFSET_BEGINNING).toBe(-2);
    expect(RD_KAFKA_OFFSET_END).toBe(-1);
    expect(RD_KAFKA_OFFSET_STORED).toBe(-1000);
    expect(RD_KAFKA_OFFSET_INVALID).toBe(-1001);
    expect(RD_KAFKA_RESP_ERR_NO_ERROR).toBe(0);
    expect(RD_KAFKA_RESP_ERR__PARTITION_EOF).toBe(-191);
    expect(RD_KAFKA_RESP_ERR__TIMED_OUT).toBe(-185);
    const prev = getDriver();
    setDriver(ffiDriver);
    expect(getDriver().kind).toBe("ffi");
    expect(getDriver().err2str(0).length).toBeGreaterThan(0);
    setDriver(prev);
    await useNative("ffi");
    expect(getDriver().kind).toBe("ffi");
  });

  test("KafkaError shape", () => {
    const e = new KafkaError(-185, "timed out", { fatal: false, retriable: true });
    expect(e).toBeInstanceOf(Error);
    expect(e.code).toBe(-185);
    expect(e.retriable).toBe(true);
  });
});

describe("Producer features", () => {
  test("send partition/timestamp/headers + poll + flush + outQueueLength", async () => {
    const t = topic("feat-prod");
    const p = producer({ acks: "1" });
    p.send({
      topic: t,
      partition: 0,
      key: "k",
      value: "v",
      timestamp: Date.now(),
      headers: { h: "1", empty: null },
    });
    expect(typeof p.poll(0)).toBe("number");
    expect(p.outQueueLength()).toBeGreaterThanOrEqual(0);
    await p.flush(15_000);
    expect(p.outQueueLength()).toBe(0);
    await p.close();
    await waitTopic(t);

    const c = consumer();
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const msg = await waitFor(() => c.poll(500));
    expect(dec(msg.value)).toBe("v");
    expect(dec(msg.key)).toBe("k");
    expect(dec(msg.headers.h ?? null)).toBe("1");
    expect(msg.partition).toBe(0);
    expect(msg.timestamp > 0n).toBe(true);
    msg.done();
    await c.close();
  });

  test("sendBatch multiple messages", async () => {
    const t = topic("feat-sbatch");
    const p = producer();
    p.sendBatch([{ topic: t, value: "a" }, { topic: t, value: "b" }, { topic: t, value: "c" }]);
    await p.flush(15_000);
    await p.close();
    await waitTopic(t);

    const c = consumer();
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const got: string[] = [];
    while (got.length < 3) {
      const m = await waitFor(() => c.poll(500));
      got.push(dec(m.value)!);
      m.done();
    }
    expect(got.sort().join("")).toBe("abc");
    await c.close();
  });

  test("closed producer throws", async () => {
    const p = producer();
    await p.close();
    expect(() => p.send({ topic: "x", value: "y" })).toThrow();
    expect(() => p.poll(0)).toThrow();
    expect(() => p.outQueueLength()).toThrow();
  });
});

describe("Consumer features", () => {
  test("subscribe string|array, subscription, unsubscribe", async () => {
    const t1 = topic("sub1");
    const t2 = topic("sub2");
    const c = consumer();
    c.subscribe(t1);
    expect(c.subscription().some((s) => s.topic === t1)).toBe(true);
    c.unsubscribe();
    c.subscribe([t1, t2]);
    const sub = c.subscription().map((s) => s.topic);
    expect(sub).toContain(t1);
    expect(sub).toContain(t2);
    c.unsubscribe();
    await c.close();
  });

  test("assign, assignment, poll, pollBatch, seek, position", async () => {
    const t = topic("feat-assign");
    await seed(t, 15);

    const c = consumer({ "enable.auto.offset.store": false });
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    expect(c.assignment().some((a) => a.topic === t && a.partition === 0)).toBe(true);

    const first = await waitFor(() => c.poll(500));
    const start = first.offset;
    first.done();

    let drained = 0;
    const deadline = Date.now() + 10_000;
    while (drained < 5 && Date.now() < deadline) {
      const batch = c.pollBatch(200, 8);
      drained += batch.length;
      for (const m of batch) m.done();
      if (!batch.length) await Bun.sleep(20);
    }
    expect(drained).toBeGreaterThanOrEqual(1);

    c.seek([{ topic: t, partition: 0, offset: start }]);
    const again = await waitFor(() => c.poll(500));
    expect(again.offset).toBe(start);
    again.done();

    const pos = c.position([{ topic: t, partition: 0 }]);
    expect(pos[0]?.topic).toBe(t);
    await c.close();
  });

  test("commit, commitMessage, committed", async () => {
    const t = topic("feat-commit");
    const group = `g-feat-c-${crypto.randomUUID()}`;
    await seed(t, 5);

    const c = consumer({ "group.id": group });
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const msgs = [];
    while (msgs.length < 5) msgs.push(await waitFor(() => c.poll(500)));

    c.commitMessage(msgs[0]!);
    const last = msgs[msgs.length - 1]!;
    c.commit([{ topic: last.topic, partition: last.partition, offset: last.offset + 1n }], false);
    c.commit([{ topic: last.topic, partition: last.partition, offset: last.offset + 1n }], true);

    const committed = c.committed([{ topic: t, partition: last.partition }], 10_000);
    expect(committed[0]?.offset).toBe(last.offset + 1n);
    for (const m of msgs) m.done();
    await c.close();
  });

  test("messages() eachMessageCommit + ctor defaults", async () => {
    const t = topic("feat-each");
    const group = `g-each-${crypto.randomUUID()}`;
    await seed(t, 3);

    const c = track(
      new Consumer(
        {
          "bootstrap.servers": BROKERS,
          "group.id": group,
          "enable.auto.commit": false,
          "auto.offset.reset": "earliest",
        },
        { timeoutMs: 200, eachMessageCommit: true },
      ),
    );
    c.subscribe(t);
    let last = -1n;
    let part = 0;
    for await (const m of c) {
      last = m.offset;
      part = m.partition;
      if (m.offset >= 2n) break;
    }
    const committed = c.committed([{ topic: t, partition: part }], 10_000);
    expect(committed[0]?.offset).toBe(last + 1n);
    await c.close();
  });

  test("batches() + eachBatchCommit", async () => {
    const t = topic("feat-batches");
    const group = `g-bat-${crypto.randomUUID()}`;
    await seed(t, 12);

    const c = consumer({ "group.id": group });
    c.subscribe(t);
    let n = 0;
    let lastOff = -1n;
    let part = 0;
    let batches = 0;
    for await (const batch of c.batches({
      timeoutMs: 200,
      batchSize: 5,
      eachBatchCommit: true,
      throwOnError: false,
    })) {
      batches++;
      expect(batch.length).toBeGreaterThan(0);
      expect(batch.length).toBeLessThanOrEqual(5);
      for (const m of batch) {
        n++;
        lastOff = m.offset;
        part = m.partition;
      }
      if (n >= 12) break;
    }
    expect(n).toBe(12);
    expect(batches).toBeGreaterThan(1);
    const committed = c.committed([{ topic: t, partition: part }], 10_000);
    expect(committed[0]?.offset).toBe(lastOff + 1n);
    await c.close();
  });

  test("pause and resume", async () => {
    const t = topic("feat-pause");
    await seed(t, 5);
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

  test("storeOffsets", async () => {
    const t = topic("feat-store");
    const group = `g-store-${crypto.randomUUID()}`;
    await seed(t, 4);
    const c = consumer({
      "group.id": group,
      "enable.auto.offset.store": false,
      "enable.auto.commit": false,
    });
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const msg = await waitFor(() => c.poll(500));
    const next = msg.offset + 1n;
    const part = msg.partition;
    msg.done();
    c.storeOffsets([{ topic: t, partition: part, offset: next }]);
    c.commit([{ topic: t, partition: part, offset: next }], false);
    const committed = c.committed([{ topic: t, partition: part }], 10_000);
    expect(committed[0]?.offset).toBe(next);
    await c.close();
  });

  test("queryWatermarkOffsets and getWatermarkOffsets", async () => {
    const t = topic("feat-wm");
    await seed(t, 6);
    const c = consumer();
    const q = c.queryWatermarkOffsets(t, 0, 10_000);
    expect(q.high - q.low).toBeGreaterThanOrEqual(6n);
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const m = await waitFor(() => c.poll(500));
    m.done();
    const g = c.getWatermarkOffsets(t, 0);
    expect(typeof g.low).toBe("bigint");
    expect(typeof g.high).toBe("bigint");
    await c.close();
  });

  test("offsetsForTimes", async () => {
    const t = topic("feat-oft");
    const before = Date.now();
    await seed(t, 3);
    const c = consumer();
    const res = c.offsetsForTimes([{ topic: t, partition: 0, offset: before - 60_000 }], 10_000);
    expect(res[0]?.topic).toBe(t);
    expect(res[0]?.offset !== undefined).toBe(true);
    await c.close();
  });

  test("assignmentLost + rebalanceProtocol + memberId", async () => {
    const c = consumer();
    expect(typeof c.assignmentLost()).toBe("boolean");
    expect(typeof c.rebalanceProtocol()).toBe("string");
    expect(c.memberId() === null || typeof c.memberId() === "string").toBe(true);
    await c.close();
  });

  test("empty assign clears assignment", async () => {
    const t = topic("feat-clear");
    await seed(t, 1);
    const c = consumer();
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    expect(c.assignment().length).toBeGreaterThan(0);
    c.assign([]);
    expect(c.assignment().length).toBe(0);
    await c.close();
  });

  test("closed consumer rejects ops", async () => {
    const c = consumer();
    await c.close();
    expect(() => c.subscribe("x")).toThrow();
    expect(() => c.poll(0)).toThrow();
    expect(() => c.pollBatch(0, 1)).toThrow();
  });
});

describe("Admin features", () => {
  test("metadata allTopics true/false, clusterId, close", async () => {
    const t = topic("feat-admin");
    await seed(t, 1);
    const a = admin();
    const all = a.metadata({ allTopics: true, timeoutMs: 15_000 });
    expect(all.brokers.length).toBeGreaterThan(0);
    expect(all.brokers[0]!.host.length).toBeGreaterThan(0);
    expect(all.topics.some((x) => x.name === t)).toBe(true);
    const tm = all.topics.find((x) => x.name === t)!;
    expect(tm.partitions.length).toBeGreaterThan(0);

    const local = a.metadata({ allTopics: false, timeoutMs: 10_000 });
    expect(Array.isArray(local.topics)).toBe(true);

    const id = a.clusterId(10_000);
    expect(id === null || typeof id === "string").toBe(true);
    await a.close();
    expect(() => a.metadata()).toThrow();
  });
});

describe("message lifecycle", () => {
  test("done() idempotent; binary key/value/headers", async () => {
    const t = topic("feat-bin");
    const p = producer();
    p.send({
      topic: t,
      key: new Uint8Array([1, 2, 3, 4]),
      value: new Uint8Array([9, 8, 7]),
      headers: { bin: new Uint8Array([5]) },
    });
    await p.flush(15_000);
    await p.close();
    await waitTopic(t);

    const c = consumer();
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const msg = await waitFor(() => c.poll(500));
    expect([...msg.key!]).toEqual([1, 2, 3, 4]);
    expect([...msg.value!]).toEqual([9, 8, 7]);
    expect([...msg.headers.bin!]).toEqual([5]);
    expect(msg.topic).toBe(t);
    expect(typeof msg.timestampType).toBe("number");
    msg.done();
    msg.done();
    await c.close();
  });
});
