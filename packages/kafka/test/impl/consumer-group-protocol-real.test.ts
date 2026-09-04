import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { BROKERS, dec, topic, waitFor } from "../helpers.ts";

const kafka4 = process.env.KAFKA_KIP_848 === "1" ? describe : describe.skip;

kafka4("KIP-848 consumer groups (Kafka 4.x)", () => {
  test("rebalances two members and commits with broker-side assignment", async () => {
    const name = topic("kip-848");
    const groupId = `kip-848-${Date.now()}`;
    const kafka = new Kafka({ brokers: BROKERS.split(",") });
    const first = kafka.consumer({ groupId, groupProtocol: "consumer", fromBeginning: true });
    const second = kafka.consumer({ groupId, groupProtocol: "consumer", fromBeginning: true });
    try {
      const admin = kafka.admin();
      await admin.createTopics([{ name, numPartitions: 2 }], { waitForLeaders: true });
      await admin.close();
      const producer = kafka.producer();
      await producer.send({
        topic: name,
        messages: [
          { partition: 0, value: "zero" },
          { partition: 1, value: "one" },
        ],
      });
      await producer.close();

      await first.subscribe(name);
      await waitFor(() => (first.assignment().length === 2 ? true : undefined), {
        timeoutMs: 30_000,
      });
      await second.subscribe(name);
      await waitFor(
        () => {
          const left = first.assignment().map(({ partition }) => partition);
          const right = second.assignment().map(({ partition }) => partition);
          return left.length > 0 &&
            right.length > 0 &&
            left.length + right.length === 2 &&
            new Set([...left, ...right]).size === 2
            ? true
            : undefined;
        },
        { timeoutMs: 30_000 },
      );

      const messages = (
        await Promise.all([
          first.fetch({ maxWaitMs: 5_000, maxMessages: 2 }),
          second.fetch({ maxWaitMs: 5_000, maxMessages: 2 }),
        ])
      ).flat();
      expect([...new Set(messages.map(({ value }) => dec(value)))].sort()).toEqual(["one", "zero"]);
      await Promise.all([first.commitOffsets(), second.commitOffsets()]);

      await second.close();
      await waitFor(() => (first.assignment().length === 2 ? true : undefined), {
        timeoutMs: 30_000,
      });
      expect(
        first
          .assignment()
          .map(({ partition }) => partition)
          .sort(),
      ).toEqual([0, 1]);
    } finally {
      await first.close();
      await second.close();
      await kafka.disconnect();
    }
  }, 90_000);
});
