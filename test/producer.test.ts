import { beforeAll, describe, expect, test } from "bun:test";
import { KafkaError } from "../src/index.ts";
import { admin, initNative, produceN, producer, topic, BROKERS } from "./helpers.ts";

beforeAll(async () => { await initNative(); });

describe("Producer", () => {
  test("send + flush delivers messages", async () => {
    const t = topic("prod");
    const p = producer();
    await produceN(p, t, 10);
    expect(p.outQueueLength()).toBe(0);
    await p.close();

    const a = admin();
    const meta = a.metadata({ timeoutMs: 10_000 });
    expect(meta.brokers.length).toBeGreaterThan(0);
    const found = meta.topics.find((x) => x.name === t);
    expect(found).toBeTruthy();
    expect(found!.partitions.length).toBeGreaterThan(0);
    await a.close();
  });

  test("sendBatch with headers and binary key/value", async () => {
    const t = topic("bin");
    const p = producer();
    p.sendBatch([{
      topic: t,
      key: new Uint8Array([1, 2, 3]),
      value: new Uint8Array([4, 5, 6]),
      headers: { a: "b", empty: null },
    }]);
    await p.flush();
    await p.close();
  });

  test("invalid config throws KafkaError", () => {
    expect(() => producer({ "not.a.real.config.property": "x" } as any)).toThrow(KafkaError);
  });

  test("closed producer rejects send", async () => {
    const p = producer();
    await p.close();
    expect(() => p.send({ topic: "x", value: "y" })).toThrow();
  });

  test("uses explicit bootstrap servers", async () => {
    const p = producer({ "bootstrap.servers": BROKERS });
    p.send({ topic: topic("boot"), value: "1" });
    await p.flush();
    await p.close();
  });
});
