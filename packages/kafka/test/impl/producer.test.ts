import { describe, expect, test } from "bun:test";
import { BunProducer, KafkaError } from "../../index.ts";
import { admin, producer, topic } from "../helpers.ts";

describe("Producer", () => {
  test("send delivers messages", async () => {
    const name = topic("prod");
    const p = producer();
    await p.send({ topic: name, messages: [{ value: "m" }] });
    await p.close();

    const client = admin();
    try {
      const metadata = await client.metadata([name]);
      expect(metadata.brokers.length).toBeGreaterThan(0);
      expect(metadata.topics.find((item) => item.name === name)?.partitions.length).toBeGreaterThan(
        0,
      );
    } finally {
      await client.close();
    }
  }, 30_000);

  test("send supports headers and binary key/value", async () => {
    const p = producer();
    await p.send({
      topic: topic("bin"),
      messages: [
        {
          key: new Uint8Array([1, 2, 3]),
          value: new Uint8Array([4, 5, 6]),
          headers: { a: "b", empty: null },
        },
      ],
    });
    await p.close();
  }, 30_000);

  test("invalid batching options throw", () => {
    expect(() => new BunProducer({ brokers: ["127.0.0.1:9092"] }, { lingerMs: -1 })).toThrow(
      RangeError,
    );
  });

  test("closed producer rejects send", async () => {
    const p = producer();
    await p.close();
    expect(() => p.send({ topic: "x", messages: [{ value: "y" }] })).toThrow();
  });

  test("Kafka errors retain their type", () => {
    expect(new KafkaError(1)).toBeInstanceOf(Error);
  });
});
