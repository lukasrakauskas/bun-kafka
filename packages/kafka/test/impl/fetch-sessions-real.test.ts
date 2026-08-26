import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { BROKERS, dec, producer, topic } from "../helpers.ts";

describe("Fetch sessions (real broker)", () => {
  test("repeated incremental polls deliver every produced message exactly once", async () => {
    const name = topic("fetchsess");
    const p = producer();
    await p.send({
      topic: name,
      messages: Array.from({ length: 25 }, (_, i) => ({ value: `m${i}` })),
    });
    await p.close();

    const client = new Kafka({ brokers: BROKERS.split(",") });
    try {
      const consumer = client.consumer();
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const got: string[] = [];
      for (let round = 0; round < 10 && got.length < 25; round++) {
        const messages = await consumer.fetch({ maxWaitMs: 300, maxMessages: 25 - got.length });
        for (const m of messages) got.push(dec(m.value)!);
      }
      expect(got).toEqual(Array.from({ length: 25 }, (_, i) => `m${i}`));
      // an idle poll after draining is safe and empty
      const extra = await consumer.fetch({ maxWaitMs: 100 });
      expect(extra).toHaveLength(0);
      await consumer.close();
    } finally {
      await client.disconnect();
    }
  }, 30_000);
});
