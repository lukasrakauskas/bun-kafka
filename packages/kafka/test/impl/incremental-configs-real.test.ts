import { describe, expect, test } from "bun:test";

// Run explicitly against a free broker supporting IncrementalAlterConfigs
// (Apache Kafka 2.3+, current Redpanda): KAFKA_BROKERS=127.0.0.1:9094 bun test test/impl/incremental-configs-real.test.ts

const brokers = (process.env.KAFKA_BROKERS ?? "127.0.0.1:9092").split(",");
const RETENTION = "retention.ms";
const RETENTION_SET = "987654321";

describe("Incremental configs (real broker)", () => {
  test("set, append-sibling safety, and delete-to-default round trip", async () => {
    const client = await import("../../index.ts").then((m) => new m.Kafka({ brokers }));
    try {
      const admin = client.admin();
      const name = `inc-cfg-${Date.now()}`;
      await admin.createTopics([{ name, numPartitions: 1 }]);
      await new Promise((r) => setTimeout(r, 300));
      const find = async () =>
        (await admin.describeConfigs([{ resourceType: 2, resourceName: name }]))[0].configs.find(
          (c) => c.name === RETENTION,
        );

      const results = await admin.incrementalAlterConfigs([
        {
          resourceType: 2,
          resourceName: name,
          ops: [{ name: RETENTION, operation: "set", value: RETENTION_SET }],
        },
      ]);
      expect(results[0].error).toBe(0);
      expect((await find())?.value).toBe(RETENTION_SET);

      // A second incremental call must not clobber the first entry (replace-mode would).
      await admin.incrementalAlterConfigs([
        {
          resourceType: 2,
          resourceName: name,
          ops: [{ name: "max.message.bytes", operation: "set", value: "2097152" }],
        },
      ]);
      const configs = (await admin.describeConfigs([{ resourceType: 2, resourceName: name }]))[0]
        .configs;
      expect(configs.find((c) => c.name === RETENTION).value).toBe(RETENTION_SET);
      expect(configs.find((c) => c.name === "max.message.bytes").value).toBe("2097152");

      await admin.incrementalAlterConfigs([
        {
          resourceType: 2,
          resourceName: name,
          ops: [{ name: RETENTION, operation: "delete" }],
        },
      ]);
      expect(await find()).not.toBe(RETENTION_SET);

      const bad = await admin.incrementalAlterConfigs([
        {
          resourceType: 2,
          resourceName: name,
          ops: [{ name: "not.a.config", operation: "set", value: "x" }],
        },
      ]);
      expect(bad[0].error).not.toBe(0);
      expect(bad[0].message).toContain("not.a.config");

      await admin.deleteTopics([name]).catch(() => {});
    } finally {
      await client.disconnect();
    }
  }, 30_000);
});
