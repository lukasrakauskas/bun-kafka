import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer } from "../../src/bun/protocol.ts";
import { admin, topic } from "../helpers.ts";

const apiVersions = () => new Writer().i16(0).array(Array.from({ length: 64 }, (_, key) => key), (writer, key) => writer.i16(key).i16(0).i16(20));

function metadataBody(listenerPort: number) {
  return new Writer()
    .array([{ id: 1, host: "127.0.0.1", port: listenerPort }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
    .string("test-cluster-id")
    .i32(1)
    .array([{ name: "events" }], (writer, item) => writer.i16(0).string(item.name).bool(false).array([0], (pw) => pw.i16(0).i32(0).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))));
}

describe("Admin: group and record management", () => {
  test("metadata exposes the cluster id", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const body = key === 18 ? apiVersions() : metadataBody(listener.port);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const metadata = await kafka.admin().metadata();
      expect(metadata.clusterId).toBe("test-cluster-id");
      expect(kafka.admin().metadata()).resolves.toBeTruthy();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("listGroups, describeGroups, deleteGroups, deleteRecords decode responses", async () => {
    let sawListGroups = false;
    let sawDescribeGroups = false;
    let sawDeleteGroups = false;
    let sawDeleteRecords = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          let offset = 0;
          while (offset < request.byteLength) {
            const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
            const frameView = new DataView(request.buffer, request.byteOffset + offset + 4, 8);
            const key = frameView.getInt16(0);
            const correlation = viewCorrelation(request.buffer, request.byteOffset + offset + 4 + 8 - 4);
            if (key === 16) sawListGroups = true;
            if (key === 15) sawDescribeGroups = true;
            if (key === 42) sawDeleteGroups = true;
            if (key === 21) sawDeleteRecords = true;
            let body: Writer;
            if (key === 18) body = apiVersions();
            else if (key === 3) body = metadataBody(listener.port);
            else if (key === 16) body = new Writer().i32(0).i16(0).array(["workers"], (writer, g) => writer.string(g).string("consumer").string("Stable"));
            else if (key === 15) body = new Writer().i32(0)
              .array(["workers"], (writer, g) => writer
                .i16(0).string(null)
                .string(g).string("Stable").string("consumer").string("range")
                .array(["member-1"], (mWriter, m) => mWriter.string(m).string("app-1").string("host-1").bytes(null).bytes(new Uint8Array([0, 1, 2]))));
            else if (key === 42) body = new Writer().i32(0).array(["workers"], (writer, g) => writer.string(g).i16(0));
            else if (key === 21) body = new Writer().i32(0).array(["events"], (tWriter, name) => tWriter.string(name).array([{ index: 0, low: 7n }], (pWriter, p) => pWriter.i32(p.index).i64(p.low).i16(0)));
            else body = new Writer().i16(0);
            const response = new Writer().i32(0).i32(correlation).raw(body.result());
            response.patchI32(0, response.length - 4);
            socket.write(response.result());
            offset += 4 + size;
          }
        },
      },
    });
    function viewCorrelation(buffer: ArrayBuffer, at: number): number {
      return new DataView(buffer, at, 4).getInt32(0);
    }
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const a = kafka.admin();
      const groups = await a.listGroups();
      expect(groups).toEqual([{ groupId: "workers", protocolType: "consumer", state: "Stable" }]);
      const described = await a.describeGroups(["workers"]);
      expect(described[0]).toMatchObject({ groupId: "workers", state: "Stable", protocolType: "consumer" });
      expect(described[0]?.members[0]?.clientId).toBe("app-1");
      expect(Array.from(described[0]?.members[0]?.memberAssignment ?? [])).toEqual([0, 1, 2]);
      expect((await a.deleteGroups(["workers"]))[0]).toMatchObject({ name: "workers", error: 0 });
      const deleted = await a.deleteRecords([{ name: "events", partitions: [{ index: 0, offset: 7n }] }]);
      expect(deleted[0]).toMatchObject({ name: "events", index: 0, lowWatermark: 7n, error: 0 });
      expect(sawListGroups && sawDescribeGroups && sawDeleteGroups && sawDeleteRecords).toBe(true);
      await a.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("createAcls, describeAcls, and deleteAcls round-trip against the broker", async () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    try {
      const a = kafka.admin();
      const resourceName = topic("acl");
      const binding = {
        resourceType: 2, // TOPIC
        resourceName,
        principal: "User:bun-kafka-test",
        host: "*",
        operation: 3, // READ
        permissionType: 3, // ALLOW
      };
      await a.createAcls([binding]);
      const listed = await a.describeAcls({ resourceType: 2, resourceName, operation: 3, permissionType: 3 });
      expect(listed.error).toBe(0);
      expect(listed.acls.some((acl) => acl.principal === "User:bun-kafka-test" && acl.resourceName === resourceName)).toBe(true);
      const removed = await a.deleteAcls([{ resourceType: 2, resourceName, principal: "User:bun-kafka-test", operation: 3, permissionType: 3 }]);
      expect(removed[0]?.error).toBe(0);
      expect(removed[0]?.acls.length).toBeGreaterThanOrEqual(1);
      const after = await a.describeAcls({ resourceType: 2, resourceName, operation: 3, permissionType: 3 });
      expect(after.acls.some((acl) => acl.principal === "User:bun-kafka-test")).toBe(false);
      await a.close();
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);
});
