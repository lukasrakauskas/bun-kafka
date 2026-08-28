import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";

const brokers = process.env.KAFKA_BROKERS?.split(",");
const integration = brokers ? describe : describe.skip;
const decode = (value: Uint8Array | null) =>
  value === null ? null : new TextDecoder().decode(value);

integration("Bun native Kafka integration", () => {
  test("produces, reads metadata, and manually consumes record batches", async () => {
    const kafka = new Kafka({ brokers: brokers!, clientId: "bun-kafka-test" });
    const topic = `bun-native-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
    try {
      const producer = kafka.producer({ idempotent: true, compression: "zstd" });
      const sent = await producer.send({
        topic,
        acks: "all",
        messages: Array.from({ length: 20 }, (_, i) => ({
          key: `key-${i}`,
          value: `value-${i}`,
          headers: { index: String(i) },
          partition: 0,
        })),
      });
      expect(sent[0]?.baseOffset).toBeGreaterThanOrEqual(0n);
      await Promise.all(
        Array.from({ length: 25 }, (_, i) =>
          producer.send({
            topic,
            messages: [{ partition: 0, value: `batched-${i}` }],
          }),
        ),
      );

      const admin = kafka.admin();
      const metadata = await admin.metadata([topic]);
      expect(metadata.brokers.length).toBeGreaterThan(0);
      expect(
        metadata.topics.find((item) => item.name === topic)?.partitions.length,
      ).toBeGreaterThan(0);
      expect((await admin.metadata()).topics.some((item) => item.name === topic)).toBe(true);

      const consumer = kafka.consumer({ fromBeginning: true });
      await consumer.assign([{ topic, partition: 0, offset: "earliest" }]);
      const received = [];
      while (received.length < 20) {
        received.push(
          ...(await consumer.fetch({
            maxWaitMs: 100,
            maxMessages: Math.min(7, 20 - received.length),
          })),
        );
      }

      expect(received).toHaveLength(20);
      expect(decode(received[0]!.value)).toBe("value-0");
      expect(decode(received[19]!.headers.index)).toBe("19");
      const batched = [];
      while (batched.length < 25) {
        batched.push(
          ...(await consumer.fetch({
            maxWaitMs: 100,
            maxMessages: Math.min(6, 25 - batched.length),
          })),
        );
      }
      expect(decode(batched[0]!.value)).toBe("batched-0");
      expect(decode(batched[24]!.value)).toBe("batched-24");

      consumer.seek({ topic, partition: 0, offset: 10n });
      const replay = await consumer.fetch({ maxWaitMs: 100, maxMessages: 1 });
      expect(replay[0]?.offset).toBe(10n);
      expect((await consumer.watermarks(topic, 0)).high).toBeGreaterThanOrEqual(45n);

      const grouped = kafka.consumer({
        groupId: `group-${crypto.randomUUID()}`,
        fromBeginning: true,
      });
      await grouped.subscribe(topic);
      expect((await grouped.fetch({ maxWaitMs: 100, maxMessages: 1 }))[0]?.offset).toBe(0n);
      await grouped.commitOffsets();
      expect((await grouped.committed([{ topic, partition: 0 }]))[0]?.offset).toBe(1n);
      await grouped.close();
    } finally {
      await kafka.disconnect();
    }
  }, 60_000);
});
