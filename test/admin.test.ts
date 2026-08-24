import { beforeAll, describe, expect, test } from "bun:test";
import { admin, initNative, produceN, producer, topic } from "./helpers.ts";

beforeAll(async () => { await initNative(); });

describe("Admin", () => {
  test("metadata returns brokers and topics", async () => {
    const t = topic("meta");
    const p = producer();
    await produceN(p, t, 1);
    await p.close();

    const a = admin();
    const meta = a.metadata({ allTopics: true, timeoutMs: 15_000 });
    expect(meta.brokers.length).toBeGreaterThan(0);
    expect(meta.brokers[0]!.host.length).toBeGreaterThan(0);
    expect(meta.topics.some((x) => x.name === t)).toBe(true);
    const id = a.clusterId(10_000);
    expect(id === null || typeof id === "string").toBe(true);
    await a.close();
  }, 30_000);
});
