import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";

// Run explicitly against a free broker:
// KAFKA_BROKERS=127.0.0.1:9094 bun test test/impl/alter-configs-real.test.ts
const brokers = (process.env.KAFKA_BROKERS ?? "127.0.0.1:9092").split(",");

describe("AlterConfigs (real broker)", () => {
  test("validateOnly does not apply the proposed config", async () => {
    const kafka = new Kafka({ brokers });
    const admin = kafka.admin();
    const name = `validate-config-${Date.now()}`;
    const original = "60000";
    const proposed = "987654321";
    try {
      await admin.createTopics([{ name, numPartitions: 1, configs: { "retention.ms": original } }]);
      await Bun.sleep(300);
      const results = await admin.alterConfigs(
        [{ resourceType: 2, resourceName: name, configs: { "retention.ms": proposed } }],
        { validateOnly: true },
      );
      expect(results[0]?.error).toBe(0);
      const configs = await admin.describeConfigs([{ resourceType: 2, resourceName: name }]);
      expect(configs[0]?.configs.find((config) => config.name === "retention.ms")?.value).toBe(
        original,
      );
    } finally {
      await admin.deleteTopics([name]).catch(() => {});
      await kafka.disconnect();
    }
  }, 30_000);
});
