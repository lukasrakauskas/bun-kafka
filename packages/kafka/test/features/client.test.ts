import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Kafka as CompatKafka } from "../../src/kafkajs-compat/index.ts";
import { encoder, type KafkaEncoder } from "../../src/protocol/index.ts";

const UNREACHABLE_BROKER = "127.0.0.1:1";

function plainAuthBody(key: number, port: number): KafkaEncoder {
  if (key === 18) {
    return apiVersions();
  }
  if (key === 17) {
    return encoder()
      .i16(0)
      .array(["PLAIN"], (writer, mechanism) => writer.string(mechanism));
  }
  if (key === 36) {
    return encoder().i16(0).string(null).bytes(new Uint8Array()).i64(0);
  }
  return encoder()
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
        .array([0], (partitionWriter) =>
          partitionWriter
            .i16(0)
            .i32(0)
            .i32(1)
            .array([1], (item) => item.i32(1))
            .array([1], (item) => item.i32(1)),
        ),
    );
}

function writeClientResponse(
  socket: Bun.Socket,
  request: Uint8Array,
  body: KafkaEncoder,
  flexible = false,
): void {
  const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
  const response = encoder().i32(0).i32(view.getInt32(8));
  if (flexible) {
    response.tags();
  }
  response.raw(body.result());
  response.patchI32(0, response.length - 4);
  socket.write(response.result());
}

function clientGroupBody(key: number, port: number): KafkaEncoder {
  const memberMetadata = encoder()
    .i16(0)
    .array(["events"], (writer, topic) => writer.string(topic))
    .bytes(null)
    .result();
  const assignment = encoder()
    .i16(0)
    .array(["events"], (writer, topic) =>
      writer.string(topic).array([0], (item, partition) => item.i32(partition)),
    )
    .bytes(null)
    .result();
  if (key === 18) {
    return apiVersions();
  }
  if (key === 10) {
    return encoder().i16(0).i32(1).string("127.0.0.1").i32(port);
  }
  if (key === 3) {
    return encoder()
      .array([{ id: 1, host: "127.0.0.1", port }], (writer, broker) =>
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
  }
  if (key === 11) {
    return encoder()
      .i32(0)
      .i16(0)
      .i32(1)
      .string("range")
      .string("member-1")
      .string("member-1")
      .array(["member-1"], (writer, member) => writer.string(member).bytes(memberMetadata));
  }
  if (key === 14) {
    return encoder().i16(0).bytes(assignment);
  }
  if (key === 9) {
    return encoder()
      .array(["events"], (writer, topic) =>
        writer
          .string(topic)
          .array([0], (item, partition) => item.i32(partition).i64(12).string(null).i16(0)),
      )
      .i16(0);
  }
  if (key === 8) {
    return encoder().array(["events"], (writer, topic) =>
      writer.string(topic).array([0], (item, partition) => item.i32(partition).i16(0)),
    );
  }
  return encoder().i16(0);
}

function clientAdminBody(key: number, port: number): KafkaEncoder {
  if (key === 18) {
    return apiVersions();
  }
  if (key === 3) {
    return encoder()
      .array([{ id: 1, host: "127.0.0.1", port }], (writer, broker) =>
        writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
      )
      .string(null)
      .i32(1)
      .array([], () => {});
  }
  if (key === 45) {
    return encoder()
      .i32(0)
      .i16(0)
      .compactString(null)
      .compactArray(["events"], (topic) =>
        topic
          .compactString("events")
          .compactArray([0], (partition) => partition.i32(0).i16(0).compactString(null).tags())
          .tags(),
      )
      .tags();
  }
  if (key === 46) {
    return encoder()
      .i32(0)
      .i16(0)
      .compactString(null)
      .compactArray(["events"], (topic) =>
        topic
          .compactString("events")
          .compactArray([0], (partition) =>
            partition
              .i32(0)
              .compactArray([1], (replica, id) => replica.i32(id))
              .compactArray([], () => {})
              .compactArray([], () => {})
              .tags(),
          )
          .tags(),
      )
      .tags();
  }
  if (key === 43) {
    return encoder()
      .i32(0)
      .i16(0)
      .array(["events"], (topic) =>
        topic.string("events").array([0], (partition) => partition.i32(0).i16(0).string(null)),
      );
  }
  if (key === 32) {
    return encoder()
      .i32(0)
      .array([{ resourceType: 2, name: "events" }], (writer, resource) =>
        writer
          .i16(0)
          .string(null)
          .i8(resource.resourceType)
          .string(resource.name)
          .array([{ name: "cleanup.policy", value: "delete" }], (configWriter, config) =>
            configWriter
              .string(config.name)
              .string(config.value)
              .bool(false)
              .bool(false)
              .bool(false),
          ),
      );
  }
  if (key === 33) {
    return encoder()
      .i32(0)
      .array(["events"], (writer, name) => writer.i16(0).string(null).i8(2).string(name));
  }
  return encoder()
    .i32(key === 19 ? 7 : 0)
    .array([{ name: "events", error: 0, message: null }], (writer, result) => {
      writer.string(result.name).i16(result.error);
      if (key !== 20) {
        writer.string(result.message);
      }
    });
}

const apiVersions = () =>
  encoder()
    .i16(0)
    .array(
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
          const metadata = encoder()
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
          const response = encoder()
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
          if (key === 36) {
            authPayload = new TextDecoder().decode(request.subarray(request.length - 10));
          }
          writeClientResponse(socket, request, plainAuthBody(key, listener.port));
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
          let body: KafkaEncoder;
          if (key === 18) {
            body = apiVersions();
          } else if (key === 3) {
            body = encoder()
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
              );
          } else {
            body = encoder().array(["events"], (writer, topic) =>
              writer
                .string(topic)
                .array([0], (partitions, partition) =>
                  partitions.i32(partition).i16(0).i64(0).i64(5),
                ),
            );
          }
          const response = encoder().i32(0).i32(correlation).raw(body.result());
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
          requestKeys.push(view.getInt16(4));
          writeClientResponse(socket, request, clientGroupBody(view.getInt16(4), listener.port));
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

  test("compat group events follow real joins and rebalances", async () => {
    const requestKeys: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          requestKeys.push(view.getInt16(4));
          writeClientResponse(socket, request, clientGroupBody(view.getInt16(4), listener.port));
        },
      },
    });
    const consumer = new CompatKafka({ brokers: [`127.0.0.1:${listener.port}`] }).consumer({
      groupId: "workers",
    });
    const events: unknown[] = [];
    consumer.on(consumer.events.REBALANCING, () => {
      throw new Error("listener failed");
    });
    consumer.on(consumer.events.REBALANCING, (event) => events.push(event));
    consumer.on(consumer.events.GROUP_JOIN, (event) => events.push(event));
    try {
      await consumer.subscribe({ topic: "events" });
      await consumer.subscribe({ topic: "events" });
      expect(requestKeys.filter((key) => key === 11)).toHaveLength(2);
      expect(events).toEqual([
        expect.objectContaining({
          type: "consumer.rebalancing",
          groupId: "workers",
          memberId: "",
        }),
        expect.objectContaining({
          type: "consumer.group_join",
          groupId: "workers",
          memberId: "member-1",
          generationId: 1,
          memberAssignment: { events: [0] },
        }),
        expect.objectContaining({
          type: "consumer.rebalancing",
          groupId: "workers",
          memberId: "member-1",
        }),
        expect.objectContaining({
          type: "consumer.group_join",
          groupId: "workers",
          memberId: "member-1",
          generationId: 1,
          memberAssignment: { events: [0] },
        }),
      ]);
    } finally {
      await consumer.disconnect();
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
          writeClientResponse(
            socket,
            request,
            clientAdminBody(key, listener.port),
            key === 45 || key === 46,
          );
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
      expect(
        (await admin.createTopics([{ name: "events", numPartitions: 1, assignments: [[1]] }]))[0]
          ?.error,
      ).toBe(0);
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
      expect(
        await admin.alterPartitionReassignments([{ topic: "events", partition: 0, replicas: [1] }]),
      ).toEqual([{ topic: "events", partition: 0, error: 0, message: null }]);
      expect(await admin.listPartitionReassignments()).toEqual([
        {
          topic: "events",
          partition: 0,
          replicas: [1],
          addingReplicas: [],
          removingReplicas: [],
        },
      ]);
      expect(await admin.electLeaders("preferred", [{ topic: "events", partition: 0 }])).toEqual([
        { topic: "events", partition: 0, error: 0, message: null },
      ]);
      expect(events).toContainEqual({ type: "throttle", apiKey: 19, durationMs: 7 });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("rejects invalid batching, retry, timeout, and fetch limits", async () => {
    expect(() => new Kafka({ brokers: [UNREACHABLE_BROKER], connectTimeoutMs: 0 })).toThrow(
      RangeError,
    );
    expect(
      () =>
        new Kafka({ brokers: [UNREACHABLE_BROKER], maxResponseBytes: Number.POSITIVE_INFINITY }),
    ).toThrow(RangeError);
    expect(() => new Kafka({ brokers: [UNREACHABLE_BROKER], retry: { maxRetries: -1 } })).toThrow(
      RangeError,
    );
    const kafka = new Kafka({ brokers: [UNREACHABLE_BROKER] });
    expect(() => kafka.producer({ lingerMs: -1 })).toThrow(RangeError);
    expect(() => kafka.producer({ compression: "zstd", idempotent: true })).not.toThrow();
    const consumer = kafka.consumer();
    await expect(consumer.fetch({ maxMessages: Number.NaN })).rejects.toBeInstanceOf(RangeError);
    await kafka.disconnect();
  });
});
