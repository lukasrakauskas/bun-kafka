import { describe, expect, test } from "bun:test";
import { admin, produceN, producer, topic } from "../helpers.ts";

describe("Admin", () => {
  test("manages partition reassignments", async () => {
    const name = topic("partition-admin");
    const client = admin();
    try {
      const metadata = await client.metadata();
      const replicas = [metadata.brokers[0]!.id];
      const created = await client.createTopics(
        [{ name, numPartitions: 1, replicationFactor: 1 }],
        { waitForLeaders: true },
      );
      expect(created[0]?.error).toBe(0);

      expect(await client.listPartitionReassignments([{ topic: name, partition: 0 }])).toEqual([]);
      expect(
        await client.alterPartitionReassignments([{ topic: name, partition: 0, replicas }]),
      ).toEqual([{ topic: name, partition: 0, error: 0, message: null }]);
      // ElectLeaders stays mock-covered: CI's Redpanda v24.3 does not expose API 43.
    } finally {
      await client.deleteTopics([name]).catch(() => {});
      await client.close();
    }
  }, 30_000);

  test("metadata returns brokers and topics", async () => {
    const name = topic("meta");
    const p = producer();
    await produceN(p, name, 1);
    await p.close();

    const client = admin();
    try {
      const metadata = await client.metadata([name]);
      expect(metadata.brokers.length).toBeGreaterThan(0);
      expect(metadata.brokers[0].host.length).toBeGreaterThan(0);
      expect(metadata.topics.some((item) => item.name === name)).toBe(true);
    } finally {
      await client.close();
    }
  }, 30_000);
});
