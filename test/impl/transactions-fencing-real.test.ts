import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { BROKERS, dec, topic } from "../helpers.ts";

describe("Transactional producer fencing (real broker)", () => {
  test("a zombie producer is fenced with PRODUCER_FENCED after epoch takeover", async () => {
    const name = topic("fence");
    const client = new Kafka({ brokers: BROKERS.split(",") });
    try {
      const admin = client.admin();
      await admin.createTopics([{ name, numPartitions: 1 }]);
      await admin.close();

      const transactionalId = `txn-${name}`;
      // Zombie completes one transaction normally at epoch 0.
      const zombie = client.producer({ transactionalId });
      await zombie.beginTransaction();
      await zombie.send({ topic: name, messages: [{ key: "a", value: "zombie-first" }] });
      await zombie.commitTransaction();

      // A second instance of the same transactional.id bumps the producer
      // epoch and commits its own record.
      const current = client.producer({ transactionalId });
      await current.beginTransaction();
      await current.send({ topic: name, messages: [{ key: "c", value: "current" }] });
      await current.commitTransaction();

      // The zombie keeps its stale epoch; the broker must fence every
      // subsequent write. Apache answers PRODUCER_FENCED (90), Redpanda
      // INVALID_PRODUCER_EPOCH (47) — same stale-epoch signal.
      await zombie.beginTransaction();
      const fenced = await zombie.send({ topic: name, messages: [{ key: "z", value: "zombie-zombie" }] }).then(() => null, (error: { code?: number }) => error);
      expect([47, 90]).toContain(fenced?.code);

      // Only records from valid epochs are visible under read_committed.
      const consumer = client.consumer({ isolationLevel: "read_committed" });
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const messages = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 10, copy: true });
      expect(messages.map((message) => dec(message.value))).toEqual(["zombie-first", "current"]);
      await consumer.close();

      await zombie.close();
      await current.close();
    } finally {
      await client.disconnect();
    }
  }, 60_000);
});
