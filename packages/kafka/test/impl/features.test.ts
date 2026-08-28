import { describe, expect, test } from "bun:test";
import { Kafka, KafkaError } from "../../index.ts";
import { BROKERS, dec, topic } from "../helpers.ts";

describe("Bun Kafka features", () => {
  test("KafkaError exposes code and retry state", () => {
    const error = new KafkaError(7, "timed out", { retriable: true });
    expect(error).toBeInstanceOf(Error);
    expect(error.code).toBe(7);
    expect(error.retriable).toBe(true);
  });

  test("producer, consumer, seek, pause, and metadata work together", async () => {
    const kafka = new Kafka({ brokers: BROKERS.split(",") });
    const name = topic("features");
    try {
      await kafka.producer().send({
        topic: name,
        messages: [
          { key: "a", value: "one", headers: { h: "1" }, partition: 0 },
          { key: "b", value: "two", partition: 0 },
          { key: "c", value: "three", partition: 0 },
        ],
      });

      const consumer = kafka.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      expect(consumer.assignment()).toHaveLength(1);
      const first = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 3, copy: true });
      expect(first.map((message) => dec(message.value))).toEqual(["one", "two", "three"]);
      expect(dec(first[0].headers.h)).toBe("1");

      consumer.seek({ topic: name, partition: 0, offset: 1n });
      consumer.pause([{ topic: name, partition: 0 }]);
      expect(await consumer.fetch({ maxWaitMs: 10 })).toHaveLength(0);
      consumer.resume([{ topic: name, partition: 0 }]);
      expect((await consumer.fetch({ maxWaitMs: 1_000, maxMessages: 1 }))[0].offset).toBe(1n);

      const metadata = await kafka.admin().metadata([name]);
      expect(metadata.topics[0]?.name).toBe(name);
      expect((await consumer.watermarks(name, 0)).high).toBeGreaterThanOrEqual(3n);
    } finally {
      await kafka.disconnect();
    }
  }, 60_000);
});
