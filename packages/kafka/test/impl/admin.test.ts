import { describe, expect, test } from "bun:test";
import { admin, produceN, producer, topic } from "../helpers.ts";

describe("Admin", () => {
  test("metadata returns brokers and topics", async () => {
    const name = topic("meta");
    const p = producer();
    await produceN(p, name, 1);
    await p.close();

    const client = admin();
    try {
      const metadata = await client.metadata([name]);
      expect(metadata.brokers.length).toBeGreaterThan(0);
      expect(metadata.brokers[0].host.length).toBeGreaterThan(0);
      expect(metadata.topics.some((item) => item.name === name)).toBe(true);
    } finally {
      await client.close();
    }
  }, 30_000);
});
