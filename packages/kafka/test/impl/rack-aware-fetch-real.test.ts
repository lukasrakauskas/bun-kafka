import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { BROKERS, dec, topic } from "../helpers.ts";

const rack = process.env.KAFKA_RACK_ID;
const multiRack = rack ? describe : describe.skip;

multiRack("Rack-aware fetching (real multi-rack broker)", () => {
  test("fetches acknowledged records from a broker-selected follower", async () => {
    const name = topic("rack-fetch");
    const kafka = new Kafka({ brokers: BROKERS.split(","), rackId: rack });
    try {
      const admin = kafka.admin();
      await admin.createTopics([{ name, numPartitions: 1, replicationFactor: 2 }], {
        waitForLeaders: true,
      });
      const leader = (await admin.metadata([name])).topics[0]!.partitions[0]!.leader;
      await admin.close();

      const consumer = kafka.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: 0n }]);
      for (let attempt = 0; attempt < 5; attempt++) {
        await consumer.fetch({ maxWaitMs: 200 });
      }

      const producer = kafka.producer();
      await producer.send({ topic: name, messages: [{ value: "nearby" }] });
      await producer.close();

      for (let attempt = 0; attempt < 10; attempt++) {
        const messages = await consumer.fetch({ maxWaitMs: 500 });
        if (messages.length) {
          expect(messages.map((message) => dec(message.value))).toEqual(["nearby"]);
          expect(messages[0]!.brokerId).not.toBe(leader);
          return;
        }
      }
      throw new Error("rack-aware consumer did not receive the acknowledged record");
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);
});
