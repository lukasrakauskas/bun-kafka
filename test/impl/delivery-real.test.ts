import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";

const BROKER = "127.0.0.1:9092";
import { admin, dec, topic } from "../helpers.ts";

describe("Producer delivery options (real broker)", () => {
  test("custom partitioner routes every message", async () => {
    const name = topic("part");
    const client = admin();
    await client.createTopics([{ name, numPartitions: 3 }]);
    await client.close();
    const p = admin();
    void p;
    const kafka = new Kafka({ brokers: [BROKER] });
    try {
      const producer = kafka.producer({
        partitioner: ({ partitionCount }) => Math.min(partitionCount - 1, 2),
      });
      await producer.send({
        topic: name,
        messages: [{ key: "a", value: "one" }, { value: "two" }, { key: "c", value: "three" }],
      });
      await producer.close();

      const consumer = kafka.consumer();
      await consumer.assign([{ topic: name, partition: 2, offset: "earliest" }]);
      const messages = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 3, copy: true });
      expect(messages.map((message) => dec(message.value))).toEqual(["one", "two", "three"]);
      expect(messages.every((message) => message.partition === 2)).toBe(true);

      const other = kafka.consumer();
      await other.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      expect(await other.fetch({ maxWaitMs: 200 })).toHaveLength(0);
      await consumer.close();
      await other.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);
  test("invalid custom partitioners throw", async () => {
    const name = topic("badpart");
    const client = admin();
    await client.createTopics([{ name, numPartitions: 1 }]);
    await client.close();
    const kafka = new Kafka({ brokers: [BROKER] });
    try {
      const producer = kafka.producer({ partitioner: () => 99 });
      await expect(
        producer.send({ topic: name, messages: [{ value: "x" }] }),
      ).rejects.toBeInstanceOf(RangeError);
      const producer2 = kafka.producer({ partitioner: () => Number.NaN });
      await expect(
        producer2.send({ topic: name, messages: [{ value: "x" }] }),
      ).rejects.toBeInstanceOf(RangeError);
// SAFETY: the surrounding test fixture provides the documented shape.
      expect(() => kafka.producer({ partitioner: "nope" as never })).toThrow(RangeError);
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);
  test("per-message delivery callbacks receive results once", async () => {
    const name = topic("cb");
    const client = admin();
    await client.createTopics([{ name, numPartitions: 2 }]);
    await client.close();
    const kafka = new Kafka({ brokers: [BROKER] });
    try {
      const delivered: string[] = [];
      const producer = kafka.producer({ lingerMs: 0 });
      await producer.send({
        topic: name,
        messages: [1, 2, 3].map((n) => ({
          key: `k${n}`,
          value: `v${n}`,
          onDelivery: (error, result) => {
            expect(error).toBeNull();
            expect(result!.topic).toBe(name);
            expect(result!.baseOffset).toBeGreaterThanOrEqual(0n);
            delivered.push(`k${n}`);
          },
        })),
      });
      expect(delivered.sort()).toEqual(["k1", "k2", "k3"]);
      await producer.close();

      // Every callback-fired message really exists on the broker.
      const consumer = kafka.consumer();
      await consumer.assign([
        { topic: name, partition: 0, offset: "earliest" },
        { topic: name, partition: 1, offset: "earliest" },
      ]);
      let seen: string[] = [];
      for (let i = 0; i < 10 && seen.length < 3; i++) {
        const batch = await consumer.fetch({ maxWaitMs: 300, maxMessages: 10, copy: true });
        seen = seen.concat(batch.map((message) => dec(message.key)));
      }
      expect(seen.sort()).toEqual(["k1", "k2", "k3"]);
      await consumer.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);
});
