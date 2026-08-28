import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { encoder, type KafkaEncoder } from "../../src/protocol/index.ts";

const TEST_PRINCIPAL = "User:bun-kafka-test";

const apiVersions = () =>
  encoder()
    .i16(0)
    .array(
      Array.from({ length: 64 }, (_, key) => key),
      (writer, key) => writer.i16(key).i16(0).i16(20),
    );

function metadataBody(listenerPort: number) {
  return encoder()
    .array([{ id: 1, host: "127.0.0.1", port: listenerPort }], (writer, b) =>
      writer.i32(b.id).string(b.host).i32(b.port).string(null),
    )
    .string("test-cluster-id")
    .i32(1)
    .array([{ name: "events" }], (writer, item) =>
      writer
        .i16(0)
        .string(item.name)
        .bool(false)
        .array([0], (pw) =>
          pw
            .i16(0)
            .i32(0)
            .i32(1)
            .array([1], (w) => w.i32(1))
            .array([1], (w) => w.i32(1)),
        ),
    );
}

function handleFrames(
  socket: Bun.Socket,
  request: Uint8Array,
  bodyFor: (key: number) => KafkaEncoder,
  observe: (key: number) => void = () => {},
): void {
  let offset = 0;
  while (offset < request.byteLength) {
    const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
    const frame = request.subarray(offset, offset + 4 + size);
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    const key = view.getInt16(4);
    observe(key);
    const response = encoder().i32(0).i32(view.getInt32(8)).raw(bodyFor(key).result());
    response.patchI32(0, response.length - 4);
    socket.write(response.result());
    offset += 4 + size;
  }
}

function adminGroupBody(key: number, port: number): KafkaEncoder {
  if (key === 18) {
    return apiVersions();
  }
  if (key === 3) {
    return metadataBody(port);
  }
  if (key === 16) {
    return encoder()
      .i32(0)
      .i16(0)
      .array(["workers"], (writer, group) => writer.string(group).string("consumer"));
  }
  if (key === 15) {
    return encoder()
      .i32(0)
      .array(["workers"], (writer, group) =>
        writer
          .i16(0)
          .string(null)
          .string(group)
          .string("Stable")
          .string("consumer")
          .string("range")
          .array(["member-1"], (memberWriter, member) =>
            memberWriter
              .string(member)
              .string("app-1")
              .string("host-1")
              .bytes(null)
              .bytes(new Uint8Array([0, 1, 2])),
          ),
      );
  }
  if (key === 42) {
    return encoder()
      .i32(0)
      .array(["workers"], (writer, group) => writer.string(group).i16(0));
  }
  if (key === 21) {
    return encoder()
      .i32(0)
      .array(["events"], (writer, name) =>
        writer
          .string(name)
          .array([{ index: 0, low: 7n }], (partitionWriter, partition) =>
            partitionWriter.i32(partition.index).i64(partition.low).i16(0),
          ),
      );
  }
  return encoder().i16(0);
}

function aclBody(key: number, port: number): KafkaEncoder {
  if (key === 18) {
    return apiVersions();
  }
  if (key === 3) {
    return metadataBody(port);
  }
  if (key === 30) {
    return encoder()
      .i32(0)
      .array(["acl"], (writer) => writer.i16(0).string(null));
  }
  if (key === 29) {
    return encoder()
      .i32(0)
      .i16(0)
      .string(null)
      .array([{ type: 2, name: "acl-topic" }], (writer, resource) =>
        writer
          .i8(resource.type)
          .string(resource.name)
          .array([{ principal: TEST_PRINCIPAL, host: "*" }], (aclWriter, acl) =>
            aclWriter.string(acl.principal).string(acl.host).i8(3).i8(3),
          ),
      );
  }
  if (key === 31) {
    return encoder()
      .i32(0)
      .array([true], (writer) =>
        writer
          .i16(0)
          .string(null)
          .array([{ error: 0, principal: TEST_PRINCIPAL }], (aclWriter, acl) =>
            aclWriter
              .i16(acl.error)
              .string(null)
              .i8(2)
              .string("acl-topic")
              .string(acl.principal)
              .string("*")
              .i8(3)
              .i8(3),
          ),
      );
  }
  return encoder().i16(0);
}

type OffsetState = { listOffsets: number };

function listOffsetBody(state: OffsetState): KafkaEncoder {
  const request = state.listOffsets++;
  let resolved = 7n;
  if (request === 0) {
    resolved = 5n;
  } else if (request === 1) {
    resolved = 9n;
  }
  return encoder().array(["events"], (writer, name) =>
    writer
      .string(name)
      .array([0], (partitionWriter, partition) =>
        partitionWriter.i32(partition).i16(0).i64(0).i64(resolved),
      ),
  );
}

function offsetBody(key: number, port: number, state: OffsetState): KafkaEncoder {
  if (key === 18) {
    return apiVersions();
  }
  if (key === 3) {
    return metadataBody(port);
  }
  if (key === 10) {
    return encoder().i16(0).i32(1).string("127.0.0.1").i32(port);
  }
  if (key === 2) {
    return listOffsetBody(state);
  }
  if (key === 9) {
    return encoder()
      .array(["events"], (writer, name) =>
        writer
          .string(name)
          .array([0], (partitionWriter, partition) =>
            partitionWriter.i32(partition).i64(12).string(null).i16(0),
          ),
      )
      .i16(0);
  }
  if (key === 8) {
    return encoder().array(["events"], (writer, name) =>
      writer
        .string(name)
        .array([0], (partitionWriter, partition) => partitionWriter.i32(partition).i16(0)),
    );
  }
  if (key === 15) {
    return encoder()
      .i32(0)
      .array(["g"], (writer, group) =>
        writer
          .i16(0)
          .string(group)
          .string("Dead")
          .string(null)
          .string(null)
          .array([], (memberWriter) => memberWriter),
      );
  }
  return encoder().i16(0);
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
          const response = encoder().i32(0).i32(correlation).raw(body.result());
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
          handleFrames(
            socket,
            request,
            (key) => adminGroupBody(key, listener.port),
            (key) => {
              if (key === 16) {
                sawListGroups = true;
              }
              if (key === 15) {
                sawDescribeGroups = true;
              }
              if (key === 42) {
                sawDeleteGroups = true;
              }
              if (key === 21) {
                sawDeleteRecords = true;
              }
            },
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const a = kafka.admin();
      const groups = await a.listGroups();
      expect(groups).toEqual([{ groupId: "workers", protocolType: "consumer", state: "" }]);
      const described = await a.describeGroups(["workers"]);
      expect(described[0]).toMatchObject({
        groupId: "workers",
        state: "Stable",
        protocolType: "consumer",
      });
      expect(described[0]?.members[0]?.clientId).toBe("app-1");
      expect(Array.from(described[0]?.members[0]?.memberAssignment ?? [])).toEqual([0, 1, 2]);
      expect((await a.deleteGroups(["workers"]))[0]).toMatchObject({ name: "workers", error: 0 });
      const deleted = await a.deleteRecords([
        { name: "events", partitions: [{ index: 0, offset: 7n }] },
      ]);
      expect(deleted[0]).toMatchObject({ name: "events", index: 0, lowWatermark: 7n, error: 0 });
      expect(sawListGroups && sawDescribeGroups && sawDeleteGroups && sawDeleteRecords).toBe(true);
      await a.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("createAcls, describeAcls, and deleteAcls decode responses", async () => {
    let sawCreate = false;
    let sawDescribe = false;
    let sawDelete = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          handleFrames(
            socket,
            request,
            (key) => aclBody(key, listener.port),
            (key) => {
              if (key === 30) {
                sawCreate = true;
              }
              if (key === 29) {
                sawDescribe = true;
              }
              if (key === 31) {
                sawDelete = true;
              }
            },
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const a = kafka.admin();
      const binding = {
        resourceType: 2,
        resourceName: "acl-topic",
        principal: TEST_PRINCIPAL,
        host: "*",
        operation: 3,
        permissionType: 3,
      };
      expect(await a.createAcls([binding])).toEqual([{ error: 0, message: null }]);
      const listed = await a.describeAcls({
        resourceType: 2,
        resourceName: "acl-topic",
        operation: 3,
        permissionType: 3,
      });
      expect(listed).toMatchObject({ error: 0, message: null });
      expect(listed.acls[0]).toEqual({
        resourceType: 2,
        resourceName: "acl-topic",
        principal: TEST_PRINCIPAL,
        host: "*",
        operation: 3,
        permissionType: 3,
      });
      const removed = await a.deleteAcls([
        {
          resourceType: 2,
          resourceName: "acl-topic",
          principal: TEST_PRINCIPAL,
          operation: 3,
          permissionType: 3,
        },
      ]);
      expect(removed[0]).toMatchObject({ error: 0 });
      expect(removed[0]?.acls[0]).toMatchObject({
        error: 0,
        resourceType: 2,
        resourceName: "acl-topic",
        principal: TEST_PRINCIPAL,
      });
      expect(sawCreate && sawDescribe && sawDelete).toBe(true);
      await a.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("group offset administration, watermarks, and broker-tolerant describeGroups", async () => {
    let sawOffsetCommit = 0;
    let sawFindCoordinator = false;
    let listOffsets = 0;
    let describedWithoutMessage = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const state = { listOffsets };
          handleFrames(
            socket,
            request,
            (key) => offsetBody(key, listener.port, state),
            (key) => {
              if (key === 10) {
                sawFindCoordinator = true;
              }
              if (key === 8) {
                sawOffsetCommit++;
              }
              if (key === 15) {
                describedWithoutMessage = true;
              }
            },
          );
          listOffsets = state.listOffsets;
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const a = kafka.admin();
      expect(await a.topicOffsets("events")).toEqual([{ partition: 0, low: 5n, high: 9n }]);
      await a.setGroupOffsets("workers", [
        { topic: "events", partitions: [{ partition: 0, offset: 12n }] },
      ]);
      expect(sawOffsetCommit).toBe(1);
      expect(await a.groupOffsets("workers", ["events"])).toEqual([
        { topic: "events", partitions: [{ partition: 0, offset: 12n, metadata: null }] },
      ]);
      await a.resetGroupOffsets("workers", "events", true);
      expect(sawOffsetCommit).toBe(2);
      expect(await a.offsetByTimestamp("events", 0, Date.now() - 1000)).toBe(7n);
      const [described] = await a.describeGroups(["g"]);
      expect(described).toMatchObject({
        groupId: "g",
        state: "Dead",
        protocolType: "",
        protocol: null,
      });
      expect(sawFindCoordinator).toBe(true);
      expect(describedWithoutMessage).toBe(true);
      await a.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
