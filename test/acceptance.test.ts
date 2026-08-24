import { describe, expect, test } from "bun:test";
import { Kafka } from "../index.ts";
import { BROKERS, dec, topic } from "./helpers.ts";

describe("acceptance", () => {
  test("full produce/consume pipeline", async () => {
    const kafka = new Kafka({ brokers: BROKERS.split(",") });
    const name = topic("accept");
    try {
      await kafka.producer().send({
        topic: name,
        acks: "all",
        messages: [{
          key: "user-1",
          value: JSON.stringify({ hello: "world", n: 42 }),
          headers: { "content-type": "application/json", "x-trace": "abc" },
          timestamp: Date.now(),
        }],
      });

      const consumer = kafka.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const message = (await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 1 }))[0];
      expect(message).toBeTruthy();
      expect(dec(message!.key)).toBe("user-1");
      expect(JSON.parse(dec(message!.value)!)).toEqual({ hello: "world", n: 42 });
      expect(dec(message!.headers["content-type"])).toBe("application/json");
      expect(dec(message!.headers["x-trace"])).toBe("abc");
      expect(message!.offset).toBeGreaterThanOrEqual(0n);
      expect(message!.timestamp).toBeGreaterThan(0n);

      const metadata = await kafka.admin().metadata([name]);
      expect(metadata.topics.some((item) => item.name === name)).toBe(true);
    } finally {
      await kafka.disconnect();
    }
  }, 60_000);

  test("high throughput batch roundtrip", async () => {
    const kafka = new Kafka({ brokers: BROKERS.split(",") });
    const name = topic("tput");
    const count = 1_000;
    try {
      await kafka.producer().send({
        topic: name,
        messages: Array.from({ length: count }, (_, i) => ({ key: String(i % 16), value: `v-${i}` })),
      });
      const consumer = kafka.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      let received = 0;
      const start = performance.now();
      while (received < count) received += (await consumer.fetch({ maxWaitMs: 100, maxMessages: count - received })).length;
      expect(received).toBe(count);
      expect(performance.now() - start).toBeLessThan(60_000);
    } finally {
      await kafka.disconnect();
    }
  }, 120_000);
});
