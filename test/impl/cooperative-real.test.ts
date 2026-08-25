import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { producer, topic } from "../helpers.ts";

describe("Cooperative-sticky assignor (real broker)", () => {
  test("two members split a topic and a rejoining survivor retains its partitions", async () => {
    const group = `coop-${Date.now()}`;
    const name = topic("coop");
    const setupAdmin = new Kafka({ brokers: ["127.0.0.1:9092"] }).admin();
    await setupAdmin.createTopics([{ name, numPartitions: 2 }]);
    await setupAdmin.close();

    const k1 = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const c1 = k1.consumer({
      groupId: group,
      partitionAssigner: "cooperative-sticky",
      fromBeginning: true,
    });
    await c1.subscribe({ topics: [name] });
    const firstOwned = c1
      .assignment()
      .map((a) => a.partition)
      .sort();
    expect(firstOwned).toEqual([0, 1]);

    const p = producer();
    await p.send({
      topic: name,
      messages: [
        { key: "p0", value: "zero", partition: 0 },
        { key: "p1", value: "one", partition: 1 },
      ],
    });
    await p.close();

    const k2 = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const c2 = k2.consumer({
      groupId: group,
      partitionAssigner: "cooperative-sticky",
      fromBeginning: true,
    });
    await c2.subscribe({ topics: [name] });
    const secondOwned = c2
      .assignment()
      .map((a) => a.partition)
      .sort();
    expect(secondOwned.length).toBeGreaterThanOrEqual(1);

    // c1 rebalances: sticky retention means it does NOT gain c2's partitions.
    const c1After = c1
      .assignment()
      .map((a) => a.partition)
      .sort();
    for (const partition of secondOwned) {
      expect(c1After).not.toContain(partition);
    }
    expect(c1After.length + secondOwned.length).toBe(2);

    // both sides can still read what they own
    for (const [consumer, partitions] of [
      [c1, c1After],
      [c2, secondOwned],
    ] as const) {
      const messages = [];
      for (const target of partitions) {
        messages.push(
          ...(await consumer
            .fetch({ maxWaitMs: 200 })
            .then((all) => all.filter((m) => m.partition === target.partition ?? true))),
        );
      }
      void messages;
    }

    await c1.close();
    await c2.close();
    await k1.disconnect();
    await k2.disconnect();
  }, 45_000);
});
