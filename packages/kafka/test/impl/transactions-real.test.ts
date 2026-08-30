import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { BROKERS, dec, topic } from "../helpers.ts";

describe("Transactions (real broker)", () => {
  test("committed transactions are visible under read_committed", async () => {
    const name = topic("txn");
    const client = kafka(BROKERS.split(","));
    try {
      const admin = client.admin();
      await admin.createTopics([{ name, numPartitions: 1 }]);
      await admin.close();

      const producer = client.producer({ transactionalId: `txn-${name}` });
      await producer.beginTransaction();
      await producer.send({
        topic: name,
        messages: [
          { key: "k1", value: "v1" },
          { key: "k2", value: "v2" },
        ],
      });
      await producer.commitTransaction();

      const consumer = client.consumer({ isolationLevel: "read_committed" });
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const messages = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 5, copy: true });
      expect(messages.map((m) => dec(m.value))).toEqual(["v1", "v2"]);
      await consumer.close();
      await producer.close();
    } finally {
      await client.disconnect();
    }
  }, 60_000);

  test("aborted transactions are invisible under read_committed but visible uncommitted", async () => {
    const name = topic("txna");
    const committedName = topic("txnc");
    const client = kafka(BROKERS.split(","));
    try {
      const admin = client.admin();
      await admin.createTopics([
        { name, numPartitions: 1 },
        { name: committedName, numPartitions: 1 },
      ]);
      await admin.close();

      // First commit a baseline record so offsets advance.
      const producer = client.producer({ transactionalId: `txn-${committedName}` });
      await producer.beginTransaction();
      await producer.send({ topic: name, messages: [{ value: "committed-record" }] });
      await producer.commitTransaction();

      // Then send an aborted record.
      const aborter = client.producer({ transactionalId: `txna-${name}` });
      await aborter.beginTransaction();
      await aborter.send({ topic: name, messages: [{ value: "aborted-record" }] });
      await aborter.abortTransaction();

      const committed = client.consumer({ isolationLevel: "read_committed" });
      await committed.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const visible = await committed.fetch({ maxWaitMs: 5_000, maxMessages: 10, copy: true });
      expect(visible.map((m) => dec(m.value))).toEqual(["committed-record"]);

      const uncommitted = client.consumer({ isolationLevel: "read_uncommitted" });
      await uncommitted.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const all = await uncommitted.fetch({ maxWaitMs: 5_000, maxMessages: 10, copy: true });
      expect(all.some((m) => m.offset >= visible[visible.length - 1].offset + 1n)).toBe(true);

      await committed.close();
      await uncommitted.close();
      await producer.close();
      await aborter.close();
    } finally {
      await client.disconnect();
    }
  }, 60_000);
});

function kafka(brokers: string[]): Kafka {
  return new Kafka({ brokers });
}
