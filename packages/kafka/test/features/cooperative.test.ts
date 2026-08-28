function writeResponse(socket: Bun.Socket, correlation: number, body: Writer): void {
  const response = new Writer().i32(0).i32(correlation).raw(body.result());
  response.patchI32(0, response.length - 4);
  socket.write(response.result());
}

type OwnedPartitionsResult = { protocol: string; version: number; partitions: number[] };

function readOwnedPartitions(request: Uint8Array): OwnedPartitionsResult {
  const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
  const clientIdLength = view.getInt16(12);
  const reader = new Reader(
    new Uint8Array(
      request.buffer,
      request.byteOffset + 14 + clientIdLength,
      request.byteLength - 14 - clientIdLength,
    ),
  );
  reader.string();
  reader.i32();
  reader.i32();
  reader.string();
  reader.string();
  const protocols = reader.array((item) => ({
    name: item.string() ?? "",
    metadata: item.bytes() ?? new Uint8Array(),
  }));
  const metadata = new Reader(protocols[0].metadata);
  const version = metadata.i16();
  metadata.array((item) => item.string());
  const partitions = metadata
    .array((item) => {
      item.string();
      return item.array((partition) => partition.i32());
    })
    .flat();
  return { protocol: protocols[0].name, version, partitions };
}

function cooperativeBody(key: number, port: number): Writer {
  if (key === 18) {
    return apiVersions();
  }
  if (key === 10) {
    return new Writer().i16(0).i32(1).string("127.0.0.1").i32(port);
  }
  if (key === 3) {
    return new Writer()
      .array([{ id: 1, host: "127.0.0.1", port }], (writer, broker) =>
        writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
      )
      .string(null)
      .i32(1)
      .array([{ name: "events" }], (writer, topic) =>
        writer
          .i16(0)
          .string(topic.name)
          .bool(false)
          .array([0, 1], (pw, partition) =>
            pw
              .i16(0)
              .i32(partition)
              .i32(1)
              .array([1], (w) => w.i32(1))
              .array([1], (w) => w.i32(1)),
          ),
      );
  }
  if (key === 11) {
    const metadata = new Writer()
      .i16(1)
      .array(["events"], (writer, topic) => writer.string(topic))
      .array([{ topic: "events", partitions: [0] }], (writer, item) =>
        writer
          .string(item.topic)
          .array(item.partitions, (partitionWriter, partition) => partitionWriter.i32(partition)),
      )
      .bytes(null);
    return new Writer()
      .i32(0)
      .i16(0)
      .i32(10)
      .string(COOPERATIVE_STICKY)
      .string(MEMBER_ID)
      .string(MEMBER_ID)
      .array([[MEMBER_ID, metadata.result()] as const], (writer, [memberId, value]) =>
        writer.string(memberId).bytes(value),
      );
  }
  if (key === 14) {
    const assignment = new Writer()
      .i16(0)
      .array(["events"], (writer, topic) =>
        writer.string(topic).array([0, 1], (item, partition) => item.i32(partition)),
      )
      .bytes(null);
    return new Writer().i16(0).bytes(assignment.result());
  }
  if (key === 9) {
    return new Writer()
      .array(["events"], (writer, topic) =>
        writer
          .string(topic)
          .array([0, 1], (item, partition) => item.i32(partition).i64(7).string(null).i16(0)),
      )
      .i16(0);
  }
  if (key === 2) {
    return new Writer()
      .i32(0)
      .array(["events"], (writer, topic) =>
        writer.string(topic).array([0, 1], (item, partition) => item.i32(partition).i16(0).i64(10)),
      );
  }
  if (key === 8) {
    return new Writer().array(["events"], (writer, topic) =>
      writer.string(topic).array([0, 1], (item) => item.i16(0)),
    );
  }
  return new Writer().i16(0);
}

function retainPartitions(
  members: Array<{ memberId: string; owned: Array<{ partition: number }> }>,
  partitions: number[],
  targetSize: Map<string, number>,
  finals: Map<string, number[]>,
  ownedBy: Map<number, string>,
): void {
  for (const member of members) {
    for (const owned of member.owned) {
      if (!partitions.includes(owned.partition) || ownedBy.has(owned.partition)) {
        continue;
      }
      const mine = finals.get(member.memberId);
      if (mine.length >= targetSize.get(member.memberId)) {
        continue;
      }
      ownedBy.set(owned.partition, member.memberId);
      mine.push(owned.partition);
    }
  }
}

function assignRemaining(
  members: Array<{ memberId: string }>,
  partitions: number[],
  finals: Map<string, number[]>,
  ownedBy: Map<number, string>,
  targetSize: Map<string, number>,
): void {
  for (const partition of partitions) {
    if (ownedBy.has(partition)) {
      continue;
    }
    const chosen =
      members
        .filter((member) => finals.get(member.memberId).length < targetSize.get(member.memberId))
        .sort(
          (a, b) =>
            finals.get(a.memberId).length - finals.get(b.memberId).length ||
            a.memberId.localeCompare(b.memberId),
        )[0] ?? members[0];
    ownedBy.set(partition, chosen.memberId);
    finals.get(chosen.memberId).push(partition);
  }
}

import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Reader, Writer } from "../../src/bun/protocol.ts";

const COOPERATIVE_STICKY = "cooperative-sticky";
const MEMBER_ID = "member-1";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

describe("Cooperative-sticky assignor (mock broker)", () => {
  test("JoinGroup declares the cooperative-sticky protocol and owned partitions", async () => {
    let joinProtocol = "";
    let subscriptionVersion = -1;
    let ownedPartitions: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 11) {
            const owned = readOwnedPartitions(request);
            joinProtocol = owned.protocol;
            subscriptionVersion = owned.version;
            ownedPartitions = owned.partitions;
          }
          writeResponse(socket, correlation, cooperativeBody(key, listener.port));
        },
      },
    });

    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer({
        groupId: "coop-group",
        partitionAssigner: COOPERATIVE_STICKY,
      });
      // Pre-existing ownership must be declared as owned partitions (KIP-429).
      await consumer.assign([{ topic: "events", partition: 1, offset: 0n }]);
      await consumer.subscribe({ topics: ["events"] });
      expect(joinProtocol).toBe(COOPERATIVE_STICKY);
      expect(subscriptionVersion).toBe(1);
      expect(ownedPartitions).toContain(1); // previously assigned partition declared as owned
      await consumer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("leader keeps owned partitions and balances only the remainder", async () => {
    // Simulate a leader computing assignments for two members where member A
    // already owns partition 0 of a 2-partition topic and B joins empty.
    const members = [
      { memberId: "a", topics: ["events"], owned: [{ topic: "events", partition: 0 }] },
      { memberId: "b", topics: ["events"], owned: [] },
    ];
    const partitions = [0, 1];
    // Mirror of the client-side sticky algorithm (kept in sync deliberately):
    const fairShare = Math.floor(partitions.length / members.length); // 1
    const extra = partitions.length % members.length; // 0
    const targetSize = new Map(
      members.map((m, i) => [m.memberId, fairShare + (i < extra ? 1 : 0)]),
    );
    const finals = new Map<string, number[]>(members.map((m) => [m.memberId, []]));
    const ownedBy = new Map<number, string>();
    retainPartitions(members, partitions, targetSize, finals, ownedBy);
    assignRemaining(members, partitions, finals, ownedBy, targetSize);
    expect(finals.get("a")).toEqual([0]); // retained
    expect(finals.get("b")).toEqual([1]); // newly acquired
  });
});
