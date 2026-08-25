import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";

describe("ACLs (real broker)", () => {
  test("createAcls, describeAcls, and deleteAcls round-trip", async () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    try {
      const a = kafka.admin();
      const resourceName = `acl-${Date.now()}`;
      const binding = {
        resourceType: 2, // TOPIC
        resourceName,
        principal: "User:bun-kafka-test",
        host: "*",
        operation: 3, // READ
        permissionType: 3, // ALLOW
      };
      await a.createAcls([binding]);
      const listed = await a.describeAcls({
        resourceType: 2,
        resourceName,
        operation: 3,
        permissionType: 3,
      });
      expect(listed.error).toBe(0);
      expect(
        listed.acls.some(
          (acl) => acl.principal === "User:bun-kafka-test" && acl.resourceName === resourceName,
        ),
      ).toBe(true);
      const removed = await a.deleteAcls([
        {
          resourceType: 2,
          resourceName,
          principal: "User:bun-kafka-test",
          operation: 3,
          permissionType: 3,
        },
      ]);
      expect(removed[0]?.error).toBe(0);
      expect(removed[0]?.acls.length).toBeGreaterThanOrEqual(1);
      const after = await a.describeAcls({
        resourceType: 2,
        resourceName,
        operation: 3,
        permissionType: 3,
      });
      expect(after.acls.some((acl) => acl.principal === "User:bun-kafka-test")).toBe(false);
      await a.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);
});
