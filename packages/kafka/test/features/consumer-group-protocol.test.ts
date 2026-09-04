import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { waitFor } from "../helpers.ts";
import {
  decoder,
  encoder,
  readConsumerGroupHeartbeatResponse,
  readResponsePayload,
  writeConsumerGroupHeartbeatRequest,
  writeResponseFrame,
  type KafkaDecoder,
  type KafkaEncoder,
} from "../../src/protocol/index.ts";

const TOPIC_ID = Uint8Array.from({ length: 16 }, (_, index) => index);

function writeResponse(
  socket: Bun.Socket,
  correlation: number,
  body: KafkaEncoder,
  flexible: boolean,
): void {
  const response = encoder().i32(0).i32(correlation);
  if (flexible) {
    response.tags();
  }
  response.raw(body.result()).patchI32(0, response.length - 4);
  socket.write(response.result());
}

function apiVersions(lastKey = 68): KafkaEncoder {
  return encoder()
    .i16(0)
    .array(
      Array.from({ length: lastKey + 1 }, (_, key) => key),
      (writer, key) => writer.i16(key).i16(0).i16(20),
    );
}

function metadataV2(port: number): KafkaEncoder {
  return encoder()
    .array([1], (writer, broker) => writer.i32(broker).string("127.0.0.1").i32(port).string(null))
    .string("cluster")
    .i32(1)
    .array(["events"], (writer, topic) =>
      writer
        .i16(0)
        .string(topic)
        .bool(false)
        .array([0, 1], (partitionWriter, partition) =>
          partitionWriter
            .i16(0)
            .i32(partition)
            .i32(1)
            .array([1], (item) => item.i32(1))
            .array([1], (item) => item.i32(1)),
        ),
    );
}

function metadataV10(port: number): KafkaEncoder {
  return encoder()
    .i32(0)
    .compactArray([1], (writer, broker) =>
      writer.i32(broker).compactString("127.0.0.1").i32(port).compactString(null).tags(),
    )
    .compactString("cluster")
    .i32(1)
    .compactArray(["events"], (writer, topic) =>
      writer
        .i16(0)
        .compactString(topic)
        .raw(TOPIC_ID)
        .bool(false)
        .compactArray([0, 1], (partitionWriter, partition) =>
          partitionWriter
            .i16(0)
            .i32(partition)
            .i32(1)
            .i32(0)
            .compactArray([1], (item) => item.i32(1))
            .compactArray([1], (item) => item.i32(1))
            .compactArray([], () => {})
            .tags(),
        )
        .i32(-0x8000_0000)
        .tags(),
    )
    .i32(-0x8000_0000)
    .tags();
}

function heartbeatError(error: number): KafkaEncoder {
  return encoder()
    .i32(0)
    .i16(error)
    .compactString(null)
    .compactString(null)
    .i32(0)
    .i32(0)
    .i8(-1)
    .tags();
}

function heartbeatResponse(sequence: number): KafkaEncoder {
  const partitions = sequence === 1 ? [0, 1] : sequence === 2 ? [0] : undefined;
  const body = encoder()
    .i32(0)
    .i16(0)
    .compactString(null)
    .compactString(sequence === 1 ? "member-1" : null)
    .i32(sequence === 1 ? 1 : 2)
    .i32(10)
    .i8(partitions ? 1 : -1);
  if (partitions) {
    body
      .compactArray([partitions], (topicWriter, values) =>
        topicWriter
          .raw(TOPIC_ID)
          .compactArray(values, (partitionWriter, partition) => partitionWriter.i32(partition))
          .tags(),
      )
      .tags();
  }
  return body.tags();
}

function offsetFetchResponse(partitions: number[]): KafkaEncoder {
  return encoder()
    .i32(0)
    .compactArray(["workers"], (groupWriter, groupId) =>
      groupWriter
        .compactString(groupId)
        .compactArray(["events"], (topicWriter, topic) =>
          topicWriter
            .compactString(topic)
            .compactArray(partitions, (partitionWriter, partition) =>
              partitionWriter.i32(partition).i64(7).i32(-1).compactString(null).i16(0).tags(),
            )
            .tags(),
        )
        .i16(0)
        .tags(),
    )
    .tags();
}

function offsetCommitResponse(): KafkaEncoder {
  return encoder()
    .i32(0)
    .compactArray(["events"], (topicWriter, topic) =>
      topicWriter
        .compactString(topic)
        .compactArray([0, 1], (partitionWriter, partition) =>
          partitionWriter.i32(partition).i16(0).tags(),
        )
        .tags(),
    )
    .tags();
}

type HeartbeatRequest = {
  memberEpoch: number;
  rebalanceTimeoutMs: number;
  subscribedTopics: string[];
  serverAssignor: string | null;
  topicPartitionsLength: number;
  owned: number[];
};

function readHeartbeatRequest(reader: KafkaDecoder): HeartbeatRequest {
  reader.compactString();
  reader.compactString();
  const memberEpoch = reader.i32();
  reader.compactString();
  reader.compactString();
  const rebalanceTimeoutMs = reader.i32();
  const subscribedTopics = reader.compactArray((item) => item.compactString() ?? "");
  const serverAssignor = reader.compactString();
  const topicPartitionsLength = reader.uvarint();
  const owned = Array.from({ length: Math.max(0, topicPartitionsLength - 1) }, () => {
    reader.raw(16);
    const partitions = reader.compactArray((partition) => partition.i32());
    reader.skipTags();
    return partitions;
  }).flat();
  reader.skipTags();
  return {
    memberEpoch,
    rebalanceTimeoutMs,
    subscribedTopics,
    serverAssignor,
    topicPartitionsLength,
    owned,
  };
}

describe("KIP-848 consumer group protocol", () => {
  test("encodes and decodes the ConsumerGroupHeartbeat v0 fixture", () => {
    const request = writeConsumerGroupHeartbeatRequest({
      groupId: "workers",
      memberId: "",
      memberEpoch: 0,
      rebalanceTimeoutMs: 60_000,
      subscribedTopics: ["events"],
      serverAssignor: "uniform",
    });
    expect([...request.toBytes()]).toEqual([
      8, 119, 111, 114, 107, 101, 114, 115, 1, 0, 0, 0, 0, 0, 0, 0, 0, 234, 96, 2, 7, 101, 118, 101,
      110, 116, 115, 8, 117, 110, 105, 102, 111, 114, 109, 0, 0,
    ]);

    const responseBytes = heartbeatResponse(1).result();
    const response = readResponsePayload(writeResponseFrame(1, responseBytes).subarray(4)).body;
    expect(readConsumerGroupHeartbeatResponse(response)).toEqual({
      throttleMs: 0,
      error: 0,
      message: null,
      memberId: "member-1",
      memberEpoch: 1,
      heartbeatIntervalMs: 10,
      assignment: [{ topicId: TOPIC_ID, partitions: [0, 1] }],
    });
  });

  test("uses broker assignments, acknowledges rebalances, and keeps classic APIs unused", async () => {
    const requests: HeartbeatRequest[] = [];
    const versions = new Map<number, number[]>();
    let heartbeatSequence = 0;
    let heartbeatAttempts = 0;
    let coordinatorAttempts = 0;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, data) {
          let offset = 0;
          while (offset < data.byteLength) {
            const size = new DataView(data.buffer, data.byteOffset + offset).getInt32(0);
            const frame = data.subarray(offset + 4, offset + 4 + size);
            const reader = decoder(frame);
            const key = reader.i16();
            const version = reader.i16();
            const correlation = reader.i32();
            reader.string();
            const flexible =
              key === 68 ||
              (key === 3 && version >= 9) ||
              ((key === 8 || key === 9) && version >= 6);
            if (flexible) {
              reader.skipTags();
            }
            versions.set(key, [...(versions.get(key) ?? []), version]);
            let body: KafkaEncoder;
            if (key === 18) {
              body = apiVersions();
            } else if (key === 10) {
              body = encoder()
                .i16(coordinatorAttempts++ ? 0 : 15)
                .i32(1)
                .string("127.0.0.1")
                .i32(listener.port);
            } else if (key === 68) {
              requests.push(readHeartbeatRequest(reader));
              body = heartbeatAttempts++
                ? heartbeatResponse(++heartbeatSequence)
                : heartbeatError(16);
            } else if (key === 3) {
              body = version === 10 ? metadataV10(listener.port) : metadataV2(listener.port);
            } else if (key === 9) {
              body = offsetFetchResponse(heartbeatSequence < 2 ? [0, 1] : [0]);
            } else if (key === 8) {
              body = offsetCommitResponse();
            } else {
              body = encoder().i16(0);
            }
            writeResponse(socket, correlation, body, flexible);
            offset += size + 4;
          }
        },
      },
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      retry: { maxRetries: 2, initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    try {
      const consumer = kafka.consumer({
        groupId: "workers",
        groupProtocol: "consumer",
        groupRemoteAssignor: "uniform",
        fromBeginning: true,
      });
      await consumer.subscribe("events");
      expect(consumer.assignment().map(({ partition }) => partition)).toEqual([0, 1]);
      await consumer.commitOffsets();
      await waitFor(
        () =>
          consumer.assignment().length === 1 && consumer.assignment()[0]?.partition === 0
            ? true
            : undefined,
        { timeoutMs: 2_000, intervalMs: 5 },
      );
      await consumer.close();

      expect(coordinatorAttempts).toBe(3);
      expect(versions.get(68)?.every((version) => version === 0)).toBe(true);
      expect(versions.get(9)).toContain(9);
      expect(versions.get(8)).toContain(9);
      expect(versions.has(11)).toBe(false);
      expect(versions.has(12)).toBe(false);
      expect(versions.has(13)).toBe(false);
      expect(versions.has(14)).toBe(false);
      expect(requests[0]).toEqual({
        memberEpoch: 0,
        rebalanceTimeoutMs: 60_000,
        subscribedTopics: ["events"],
        serverAssignor: "uniform",
        topicPartitionsLength: 1,
        owned: [],
      });
      expect(requests[1]?.topicPartitionsLength).toBe(1);
      expect(requests[2]?.owned).toEqual([0, 1]);
      expect(requests.at(-1)?.memberEpoch).toBe(-1);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("rejects consumer protocol before sending it to an older broker", async () => {
    let sentHeartbeat = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, data) {
          let offset = 0;
          while (offset < data.byteLength) {
            const size = new DataView(data.buffer, data.byteOffset + offset).getInt32(0);
            const view = new DataView(data.buffer, data.byteOffset + offset + 4, size);
            const key = view.getInt16(0);
            const correlation = view.getInt32(4);
            sentHeartbeat ||= key === 68;
            const body =
              key === 18
                ? apiVersions(63)
                : key === 3
                  ? metadataV2(listener.port)
                  : encoder().i16(0).i32(1).string("127.0.0.1").i32(listener.port);
            writeResponse(socket, correlation, body, false);
            offset += size + 4;
          }
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    const consumer = kafka.consumer({ groupId: "workers", groupProtocol: "consumer" });
    try {
      await expect(consumer.subscribe("events")).rejects.toMatchObject({
        code: 35,
        message: expect.stringContaining("does not support API 68"),
      });
      expect(sentHeartbeat).toBe(false);
    } finally {
      await consumer.close();
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("keeps migration explicit and rejects protocol-specific option mixing", async () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:1"] });
    try {
      expect(() =>
        kafka.consumer({ groupProtocol: "consumer", heartbeatIntervalMs: 1_000 }),
      ).toThrow("require groupProtocol=classic");
      expect(() => kafka.consumer({ groupRemoteAssignor: "uniform" })).toThrow(
        "requires groupProtocol=consumer",
      );
    } finally {
      await kafka.disconnect();
    }
  });
});
