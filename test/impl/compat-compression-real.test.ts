import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { CompressionTypes, Kafka as CompatKafka } from "../../src/kafkajs-compat/index.ts";
import { BROKERS, dec, topic } from "../helpers.ts";

describe("Compat producer compression overrides (real broker)", () => {
  test("per-send codec switches stay isolated and ordered within one producer", async () => {
    const name = topic("codec");
    const client = new Kafka({ brokers: BROKERS.split(",") });
    const admin = client.admin();
    await admin.createTopics([{ name, numPartitions: 1 }]);
    await admin.close();

    // kafkajs pins compression at producer construction; the compat layer
    // forwards per-send overrides instead. Interleave codecs inside one
    // linger window and assert nothing bleeds between batches.
    const compat = new CompatKafka({ brokers: BROKERS.split(",") });
    const producer = compat.producer();
    try {
      await producer.connect();
      await producer.send({
        topic: name,
        compression: CompressionTypes.GZIP,
        messages: [
          { key: "a", value: "gzip-1" },
          { key: "b", value: "gzip-2" },
        ],
      });
      await producer.send({
        topic: name,
        compression: CompressionTypes.Snappy,
        messages: [{ key: "c", value: "snappy-1" }],
      });
      await producer.send({ topic: name, messages: [{ key: "d", value: "plain-1" }] });
      await producer.send({
        topic: name,
        compression: CompressionTypes.GZIP,
        messages: [{ key: "e", value: "gzip-3" }],
      });
      await producer.disconnect();

      const consumer = client.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const messages = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 10, copy: true });
      expect(messages.map((message) => dec(message.value))).toEqual([
        "gzip-1",
        "gzip-2",
        "snappy-1",
        "plain-1",
        "gzip-3",
      ]);
      expect(messages.every((message) => message.partition === 0)).toBe(true);
      await consumer.close();
    } finally {
      await client.disconnect();
    }
  }, 60_000);
});
