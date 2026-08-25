import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer } from "../../src/bun/protocol.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

describe("Bun native Kafka client (mock brokers)", () => {
  test("uses Bun TCP framing for metadata", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const metadata = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) => {
              writer.i32(broker.id).string(broker.host).i32(broker.port).string(null);
            })
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, topic) => {
              writer
                .i16(0)
                .string(topic.name)
                .bool(false)
                .array([0], (partitionWriter, partition) => {
                  partitionWriter
                    .i16(0)
                    .i32(partition)
                    .i32(1)
                    .array([1], (itemWriter, id) => itemWriter.i32(id))
                    .array([1], (itemWriter, id) => itemWriter.i32(id));
                });
            });
          const response = new Writer()
            .i32(0)
            .i32(correlation)
            .raw((key === 18 ? apiVersions() : metadata).result());
          response.patchI32(0, response.length - 4);
          const bytes = response.result();
          socket.write(bytes.subarray(0, 7));
          socket.write(bytes.subarray(7));
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const metadata = await kafka.admin().metadata(["events"]);
      expect(metadata.brokers).toEqual([{ id: 1, host: "127.0.0.1", port: listener.port }]);
      expect(metadata.topics[0]?.partitions[0]).toEqual({ err: 0, id: 0, leader: 1 });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("authenticates with SASL/PLAIN before Kafka requests", async () => {
    const requestKeys: number[] = [];
    let authPayload = "";
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          requestKeys.push(key);
          const correlation = view.getInt32(8);
          if (key === 36)
            authPayload = new TextDecoder().decode(request.subarray(request.length - 10));
          const body =
            key === 18
              ? apiVersions()
              : key === 17
                ? new Writer()
                    .i16(0)
                    .array(["PLAIN"], (writer, mechanism) => writer.string(mechanism))
                : key === 36
                  ? new Writer().i16(0).string(null).bytes(new Uint8Array()).i64(0)
                  : new Writer()
                      .array(
                        [{ id: 1, host: "127.0.0.1", port: listener.port }],
                        (writer, broker) =>
                          writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
                      )
                      .string(null)
                      .i32(1)
                      .array([{ name: "events" }], (writer, topic) =>
                        writer
                          .i16(0)
                          .string(topic.name)
                          .bool(false)
                          .array([0], (partitionWriter) => {
                            partitionWriter
                              .i16(0)
                              .i32(0)
                              .i32(1)
                              .array([1], (itemWriter) => itemWriter.i32(1))
                              .array([1], (itemWriter) => itemWriter.i32(1));
                          }),
                      );
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      sasl: { mechanism: "plain", username: "user", password: "pass" },
    });
    try {
      await kafka.admin().metadata(["events"]);
      expect(requestKeys).toEqual([18, 17, 36, 3]);
      expect(authPayload).toContain("\0user\0pass");
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("reconnects retriable requests and emits retry events", async () => {
    let offsetAttempts = 0;
    const events: unknown[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 2 && offsetAttempts++ === 0) {
            socket.end();
            return;
          }
          const body =
            key === 18
              ? apiVersions()
              : key === 3
                ? new Writer()
                    .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                      writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
                    )
                    .string(null)
                    .i32(1)
                    .array(["events"], (writer, topic) =>
                      writer
                        .i16(0)
                        .string(topic)
                        .bool(false)
                        .array([0], (partitions, partition) =>
                          partitions
                            .i16(0)
                            .i32(partition)
                            .i32(1)
                            .array([1], (item) => item.i32(1))
                            .array([1], (item) => item.i32(1)),
                        ),
                    )
                : new Writer().array(["events"], (writer, topic) =>
                    writer
                      .string(topic)
                      .array([0], (partitions, partition) =>
                        partitions.i32(partition).i16(0).i64(0).i64(5),
                      ),
                  );
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      retry: { initialBackoffMs: 0, maxBackoffMs: 0 },
      onEvent: (event) => events.push(event),
    });
    try {
      const consumer = kafka.consumer();
      await consumer.assign([{ topic: "events", partition: 0, offset: "earliest" }]);
      expect(consumer.position("events", 0)).toBe(5n);
      expect(events).toContainEqual(
        expect.objectContaining({ type: "retry", apiKey: 2, attempt: 1 }),
      );
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("joins a consumer group and uses committed offsets", async () => {
    const requestKeys: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          requestKeys.push(key);
          const correlation = view.getInt32(8);
          const metadata = () =>
            new Writer()
              .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
              )
              .string(null)
              .i32(1)
              .array(["events"], (writer, topic) =>
                writer
                  .i16(0)
                  .string(topic)
                  .bool(false)
                  .array([0], (partitionWriter, partition) =>
                    partitionWriter
                      .i16(0)
                      .i32(partition)
                      .i32(1)
                      .array([1], (item) => item.i32(1))
                      .array([1], (item) => item.i32(1)),
                  ),
              );
          const memberMetadata = new Writer()
            .i16(0)
            .array(["events"], (writer, topic) => writer.string(topic))
            .bytes(null)
            .result();
          const assignment = new Writer()
            .i16(0)
            .array(["events"], (writer, topic) =>
              writer.string(topic).array([0], (item, partition) => item.i32(partition)),
            )
            .bytes(null)
            .result();
          const body =
            key === 18
              ? apiVersions()
              : key === 10
                ? new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port)
                : key === 3
                  ? metadata()
                  : key === 11
                    ? new Writer()
                        .i32(0)
                        .i16(0)
                        .i32(1)
                        .string("range")
                        .string("member-1")
                        .string("member-1")
                        .array(["member-1"], (writer, member) =>
                          writer.string(member).bytes(memberMetadata),
                        )
                    : key === 14
                      ? new Writer().i16(0).bytes(assignment)
                      : key === 9
                        ? new Writer()
                            .array(["events"], (writer, topic) =>
                              writer
                                .string(topic)
                                .array([0], (item, partition) =>
                                  item.i32(partition).i64(12).string(null).i16(0),
                                ),
                            )
                            .i16(0)
                        : key === 8
                          ? new Writer().array(["events"], (writer, topic) =>
                              writer
                                .string(topic)
                                .array([0], (item, partition) => item.i32(partition).i16(0)),
                            )
                          : new Writer().i16(0);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    const consumer = kafka.consumer({ groupId: "workers" });
    try {
      await consumer.subscribe("events");
      expect(consumer.assignment()).toEqual([{ topic: "events", partition: 0, offset: 12n }]);
      await consumer.commitOffsets();
      expect(requestKeys).toContain(10);
      expect(requestKeys).toContain(11);
      expect(requestKeys).toContain(14);
      expect(requestKeys).toContain(9);
      expect(requestKeys).toContain(8);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("admin topic and config APIs decode responses", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const body =
            key === 18
              ? apiVersions()
              : key === 3
                ? new Writer()
                    .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                      writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
                    )
                    .string(null)
                    .i32(1)
                    .array([], () => {})
                : key === 32
                  ? new Writer()
                      .i32(0)
                      .array([{ resourceType: 2, name: "events" }], (writer, resource) =>
                        writer
                          .i16(0)
                          .string(null)
                          .i8(resource.resourceType)
                          .string(resource.name)
                          .array(
                            [{ name: "cleanup.policy", value: "delete" }],
                            (configWriter, config) =>
                              configWriter
                                .string(config.name)
                                .string(config.value)
                                .bool(false)
                                .bool(false)
                                .bool(false),
                          ),
                      )
                  : key === 33
                    ? new Writer()
                        .i32(0)
                        .array(["events"], (writer, name) =>
                          writer.i16(0).string(null).i8(2).string(name),
                        )
                    : new Writer()
                        .i32(key === 19 ? 7 : 0)
                        .array([{ name: "events", error: 0, message: null }], (writer, result) => {
                          writer.string(result.name).i16(result.error);
                          if (key !== 20) writer.string(result.message);
                        });
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const events: unknown[] = [];
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      onEvent: (event) => events.push(event),
    });
    const admin = kafka.admin();
    try {
      expect((await admin.createTopics([{ name: "events", numPartitions: 1 }]))[0]?.error).toBe(0);
      expect((await admin.deleteTopics(["events"]))[0]?.name).toBe("events");
      expect((await admin.createPartitions([{ name: "events", count: 2 }]))[0]?.error).toBe(0);
      expect(
        (await admin.describeConfigs([{ resourceType: 2, resourceName: "events" }]))[0]?.configs[0]
          ?.value,
      ).toBe("delete");
      expect(
        (
          await admin.alterConfigs([
            { resourceType: 2, resourceName: "events", configs: { "cleanup.policy": "compact" } },
          ])
        )[0]?.error,
      ).toBe(0);
      expect(events).toContainEqual({ type: "throttle", apiKey: 19, durationMs: 7 });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("rejects invalid batching, retry, timeout, and fetch limits", async () => {
    expect(() => new Kafka({ brokers: ["127.0.0.1:1"], connectTimeoutMs: 0 })).toThrow(RangeError);
    expect(
      () => new Kafka({ brokers: ["127.0.0.1:1"], maxResponseBytes: Number.POSITIVE_INFINITY }),
    ).toThrow(RangeError);
    expect(() => new Kafka({ brokers: ["127.0.0.1:1"], retry: { maxRetries: -1 } })).toThrow(
      RangeError,
    );
    const kafka = new Kafka({ brokers: ["127.0.0.1:1"] });
    expect(() => kafka.producer({ lingerMs: -1 })).toThrow(RangeError);
    expect(() => kafka.producer({ compression: "zstd", idempotent: true })).not.toThrow();
    const consumer = kafka.consumer();
    await expect(consumer.fetch({ maxMessages: Number.NaN })).rejects.toBeInstanceOf(RangeError);
    await kafka.disconnect();
  });
});
