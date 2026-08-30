import { describe, expect, test } from "bun:test";
import { Admin, KafkaErrorCode } from "../../index.ts";
import type { Cluster } from "../../src/bun/cluster.ts";
import { ConnectionMetrics } from "../../src/bun/connection/metrics.ts";
import { RequestTracker } from "../../src/bun/connection/requests.ts";
import { topicMetadataRetryable } from "../../src/bun/producer/metadata.ts";
import {
  fetchWatermarks,
  resolveTopicPatterns,
  topicAssignments,
} from "../../src/consumer/subscription.ts";
import {
  emptyRequestBody,
  encoder,
  readResponsePayload,
  RequestBody,
  writeResponseFrame,
} from "../../src/protocol/index.ts";
import { Writer } from "../../src/protocol/wire/writer.ts";

describe("Module boundaries", () => {
  test("request and response bodies preserve wire primitives", () => {
    const bytes = Uint8Array.of(1);
    const request = new RequestBody(bytes);
    expect(request.toBytes()).toBe(bytes);
    expect(request.result()).toBe(bytes);
    expect(RequestBody.from(request)).toBe(request);
    expect(RequestBody.from(encoder().i8(2)).toBytes()).toEqual(Uint8Array.of(2));
    expect(RequestBody.from(new Writer().i8(3)).toBytes()).toEqual(Uint8Array.of(3));

    const encoded = encoder()
      .i8(-1)
      .i16(-2)
      .i32(-3)
      .u32(4)
      .i64(5n)
      .f64(6.5)
      .bool(true)
      .string("s")
      .bytes(Uint8Array.of(7))
      .array([8], (writer, value) => writer.i8(value))
      .raw(Uint8Array.of(9))
      .varInt(-10)
      .varLong(-11n)
      .uvarint(12)
      .compactString("c")
      .compactBytes(Uint8Array.of(13))
      .compactArray([14], (writer, value) => writer.i8(value))
      .tags();
    const frame = writeResponseFrame(17, encoded.result());
    const { correlationId, body } = readResponsePayload(frame.subarray(4));

    expect(correlationId).toBe(17);
    expect(body.reader()).toBeDefined();
    expect([
      body.i8(),
      body.i16(),
      body.i32(),
      body.u32(),
      body.i64(),
      body.f64(),
      body.bool(),
      body.string(),
      body.bytes()?.[0],
      body.array((reader) => reader.i8())[0],
      body.raw(1)[0],
      body.varInt(),
      body.varLong(),
      body.uvarint(),
      body.compactString(),
      body.compactBytes()?.[0],
      body.compactArray((reader) => reader.i8())[0],
    ]).toEqual([-1, -2, -3, 4, 5n, 6.5, true, "s", 7, 8, 9, -10, -11n, 12, "c", 13, 14]);
    expect(body.skipTags()).toBe(body);
    expect(body.remaining).toBe(0);
    expect(body.data.byteLength).toBeGreaterThan(0);
  });

  test("admin lifecycle and producer metadata errors stay bounded", async () => {
    const admin = new Admin({ brokers: ["127.0.0.1:1"] });
    await admin.disconnect();
    await admin.close();
    expect(() => admin.metadata()).toThrow("Admin is closed");

    const tracker = new RequestTracker("127.0.0.1:1", "test", new ConnectionMetrics());
    await expect(
      tracker.request({ write: () => 1 } as unknown as Bun.Socket, 3, 0, emptyRequestBody(), 1),
    ).rejects.toThrow("Kafka request 3 timed out");

    const metadata = { name: "events", err: KafkaErrorCode.LEADER_NOT_AVAILABLE, partitions: [] };
    expect(topicMetadataRetryable(metadata)).toBe(true);
    expect(
      topicMetadataRetryable({ ...metadata, err: KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION }),
    ).toBe(true);
    expect(
      topicMetadataRetryable({ ...metadata, err: KafkaErrorCode.INVALID_TOPIC_EXCEPTION }),
    ).toBe(false);
  });

  test("subscriptions resolve patterns, assignments, and watermarks", async () => {
    const metadata = {
      brokers: [],
      topics: [
        { name: "events", err: 0, partitions: [{ id: 0, err: 0, leader: 1 }] },
        { name: "exact", err: 0, partitions: [] },
        { name: "failed", err: 3, partitions: [] },
      ],
    };
    let response = 0;
    const cluster = {
      metadata: () => Promise.resolve(metadata),
      topic: () => Promise.resolve(metadata.topics[0]),
      request: () => {
        const offset = [2n, 9n][response++];
        const body = encoder().array(["events"], (topicWriter, topic) =>
          topicWriter
            .string(topic)
            .array([offset], (partitionWriter, value) =>
              partitionWriter.i32(0).i16(0).i64(0).i64(value),
            ),
        );
        return Promise.resolve(
          readResponsePayload(writeResponseFrame(0, body.result()).subarray(4)).body,
        );
      },
    } as unknown as Cluster;

    expect(await resolveTopicPatterns(cluster, [/^eve/g, "exact"])).toEqual(["events", "exact"]);
    expect(await topicAssignments(cluster, ["events"], true)).toEqual([
      { topic: "events", partition: 0, offset: "earliest" },
    ]);
    expect(await fetchWatermarks(cluster, "events", 0)).toEqual({ low: 2n, high: 9n });
  });
});
