import { describe, expect, test } from "bun:test";

describe("Client quotas (real broker)", () => {
  test("quota alter and describe round trip", async () => {
    const client = await import("../../index.ts").then(
      (m) => new m.Kafka({ brokers: ["127.0.0.1:9092"] }),
    );
    try {
      const a = client.admin();
      const entity = [{ entityType: "client-id", entityName: `quota-${Date.now()}` }];
      const altered = await a.alterClientQuotas([
        { entity, ops: [{ key: "producer_byte_rate", value: 204800 }] },
      ]);
      expect(altered[0]!.error).toBe(0);
      const described = await a.describeClientQuotas([
        { entityType: entity[0]!.entityType, matchType: "exact", match: entity[0]!.entityName },
      ]);
      expect(described.length).toBe(1);
      expect(described[0]!.values[0]!.name).toBe("producer_byte_rate");
      expect(described[0]!.values[0]!.value).toBe(204800);
      // cleanup
      await a.alterClientQuotas([{ entity, ops: [{ key: "producer_byte_rate", remove: true }] }]);
      const after = await a.describeClientQuotas([
        { entityType: entity[0]!.entityType, matchType: "exact", match: entity[0]!.entityName },
      ]);
      expect(after.length).toBe(0);
      await a.close();
    } finally {
      await client.disconnect();
    }
  }, 30_000);
});
