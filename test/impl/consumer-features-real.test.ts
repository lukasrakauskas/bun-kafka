import { describe, expect, test } from "bun:test";
import { Kafka, type KafkaMessage } from "../../index.ts";
import { admin, topic } from "../helpers.ts";

function decode(value: Uint8Array | null | unknown): string | null {
  return value == null ? null : typeof value === "string" ? value : new TextDecoder().decode(value as Uint8Array);
}

describe("Regex subscription and deserializers", () => {
  test("subscribe expands RegExp patterns against cluster metadata", async () => {
    const name = topic("regex");
    const client = admin();
    await client.createTopics([{ name: `${name}-one`, numPartitions: 1 }, { name: `${name}-two`, numPartitions: 1 }]);
    await client.close();
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    try {
      const consumer = kafka.consumer();
      await consumer.subscribe({ topics: [new RegExp(`^${name}`), `${name}-one`] });
      const assigned = consumer.assignment().map((item) => item.topic).sort();
      expect(assigned).toEqual([`${name}-one`, `${name}-two`]);
      await consumer.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);

  test("key and value deserializers transform delivered messages", async () => {
    const name = topic("serde");
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    try {
      const producer = kafka.producer();
      await producer.send({ topic: name, messages: [{ key: "k1", value: JSON.stringify({ n: 1 }) }, { key: "k2", value: JSON.stringify({ n: 2 }) }] });
      await producer.close();

      const consumer = kafka.consumer({
        keyDeserializer: (data) => data === null ? null : decode(data),
        valueDeserializer: (data) => data === null ? null : JSON.parse(decode(data)!),
      });
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const messages = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 2, copy: true });
      expect(messages[0]!.key).toBe("k1");
      expect(messages[0]!.value).toEqual({ n: 1 });
      expect(messages[1]!.value).toEqual({ n: 2 });
      await consumer.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);

  test("plain consumers still receive zero-copy byte payloads", async () => {
    const name = topic("plain");
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    try {
      const producer = kafka.producer();
      await producer.send({ topic: name, messages: [{ value: "bytes" }] });
      await producer.close();
      const consumer = kafka.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const messages: KafkaMessage[] = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 1, copy: true });
      expect(messages[0]!.value).toBeInstanceOf(Uint8Array);
      await consumer.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);

});
