import { describe, expect, test } from "bun:test";
import { Kafka, KafkaError } from "../index.ts";
import { Reader, RecordSetDecoder, Writer, crc32c, decodeRecordSet, encodeRecordBatch, murmur2 } from "../src/bun/protocol.ts";

const decode = (value: Uint8Array | null) => value === null ? null : new TextDecoder().decode(value);
const apiVersions = () => new Writer().i16(0).array(Array.from({ length: 64 }, (_, key) => key), (writer, key) => writer.i16(key).i16(0).i16(20));

describe("Bun native Kafka protocol", () => {
  test("record batches preserve values, keys, headers, offsets, and timestamps", () => {
    const batch = encodeRecordBatch([
      { key: "a", value: "one", timestamp: 1000, headers: { trace: "x", empty: null } },
      { key: new Uint8Array([2]), value: null, timestamp: 1005 },
    ]);
    const messages = decodeRecordSet(batch, "events", 3, 7);

    expect(messages).toHaveLength(2);
    expect(messages[0]!.offset).toBe(0n);
    expect(messages[1]!.offset).toBe(1n);
    expect(messages[1]!.timestamp).toBe(1005n);
    expect(decode(messages[0]!.key)).toBe("a");
    expect(decode(messages[0]!.value)).toBe("one");
    expect(decode(messages[0]!.headers.trace!)).toBe("x");
    expect(messages[0]!.headers.empty).toBeNull();
    expect(messages[1]!.value).toBeNull();
  });

  test("grows record batches beyond the initial writer buffer", () => {
    const records = Array.from({ length: 500 }, (_, i) => ({ key: String(i), value: new Uint8Array(100) }));
    expect(decodeRecordSet(encodeRecordBatch(records), "large", 0, 1)).toHaveLength(500);
  });

  test("decodes bounded zero-copy pages without losing records", () => {
    const batch = encodeRecordBatch(Array.from({ length: 25 }, (_, i) => ({ value: `v-${i}` })));
    const decoder = new RecordSetDecoder(batch, "paged", 0, 1);
    const messages = [];
    while (!decoder.done) messages.push(...decoder.read(7));
    expect(messages).toHaveLength(25);
    expect(messages.map((message) => message.offset)).toEqual(Array.from({ length: 25 }, (_, i) => BigInt(i)));
    expect(messages[0]!.value!.buffer).toBe(batch.buffer);

    const copied = new RecordSetDecoder(batch, "paged", 0, 1, { copy: true }).read(1);
    expect(copied[0]!.value!.buffer).not.toBe(batch.buffer);
  });

  test("uses fast number varints and bigint varlongs", () => {
    const writer = new Writer();
    for (const value of [-2147483648, -1, 0, 1, 2147483647]) writer.varInt(value);
    for (const value of [-9007199254740991n, -1n, 0n, 1n, 9007199254740991n]) writer.varLong(value);
    const reader = new Reader(writer.result());
    expect(Array.from({ length: 5 }, () => reader.varInt())).toEqual([-2147483648, -1, 0, 1, 2147483647]);
    expect(Array.from({ length: 5 }, () => reader.varLong())).toEqual([-9007199254740991n, -1n, 0n, 1n, 9007199254740991n]);
  });

  test("CRC32C uses the Kafka polynomial and rejects corruption", () => {
    expect(crc32c(new TextEncoder().encode("123456789"))).toBe(0xe3069283);
    const batch = encodeRecordBatch([{ value: "safe" }]);
    batch[batch.length - 1]! ^= 1;
    expect(() => decodeRecordSet(batch, "t", 0, 0)).toThrow(KafkaError);
  });

  test("compressed record batches round-trip and preserve following batches", () => {
    const gzip = encodeRecordBatch([{ key: "a", value: "one" }], Date.now(), "gzip");
    const zstd = encodeRecordBatch([{ key: "b", value: "two" }], Date.now(), "zstd");
    const batches = new Uint8Array(gzip.byteLength + zstd.byteLength);
    batches.set(gzip);
    batches.set(zstd, gzip.byteLength);
    expect(new DataView(gzip.buffer, gzip.byteOffset, gzip.byteLength).getInt16(21)).toBe(1);
    expect(new DataView(zstd.buffer, zstd.byteOffset, zstd.byteLength).getInt16(21)).toBe(4);
    expect(decodeRecordSet(batches, "compressed", 0, 1).map((message) => decode(message.value))).toEqual(["one", "two"]);
  });

  test("idempotent batches carry producer identity and sequence", () => {
    const batch = encodeRecordBatch([{ value: "one" }], 1, "none", { id: 42n, epoch: 3, sequence: 9 });
    const view = new DataView(batch.buffer, batch.byteOffset, batch.byteLength);
    expect(view.getBigInt64(43)).toBe(42n);
    expect(view.getInt16(51)).toBe(3);
    expect(view.getInt32(53)).toBe(9);
  });

  test("Kafka murmur2 partitioning is stable", () => {
    const key = new TextEncoder().encode("same-key");
    expect(murmur2(key)).toBe(murmur2(key));
    expect((murmur2(key) & 0x7fffffff) % 12).toBe(6);
  });

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
            .string(null).i32(1)
            .array([{ name: "events" }], (writer, topic) => {
              writer.i16(0).string(topic.name).bool(false).array([0], (partitionWriter, partition) => {
                partitionWriter.i16(0).i32(partition).i32(1)
                  .array([1], (itemWriter, id) => itemWriter.i32(id))
                  .array([1], (itemWriter, id) => itemWriter.i32(id));
              });
            });
          const response = new Writer().i32(0).i32(correlation).raw((key === 18 ? apiVersions() : metadata).result());
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
          if (key === 36) authPayload = new TextDecoder().decode(request.subarray(request.length - 10));
          const body = key === 18 ? apiVersions()
            : key === 17
              ? new Writer().i16(0).array(["PLAIN"], (writer, mechanism) => writer.string(mechanism))
            : key === 36
              ? new Writer().i16(0).string(null).bytes(new Uint8Array()).i64(0)
              : new Writer()
                .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) => writer.i32(broker.id).string(broker.host).i32(broker.port).string(null))
                .string(null).i32(1)
                .array([{ name: "events" }], (writer, topic) => writer.i16(0).string(topic.name).bool(false).array([0], (partitionWriter) => {
                  partitionWriter.i16(0).i32(0).i32(1).array([1], (itemWriter) => itemWriter.i32(1)).array([1], (itemWriter) => itemWriter.i32(1));
                }));
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
          const body = key === 18 ? apiVersions()
            : key === 3 ? new Writer()
              .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) => writer.i32(broker.id).string(broker.host).i32(broker.port).string(null))
              .string(null).i32(1).array(["events"], (writer, topic) => writer.i16(0).string(topic).bool(false).array([0], (partitions, partition) => partitions.i16(0).i32(partition).i32(1).array([1], (item) => item.i32(1)).array([1], (item) => item.i32(1))))
              : new Writer().array(["events"], (writer, topic) => writer.string(topic).array([0], (partitions, partition) => partitions.i32(partition).i16(0).i64(0).i64(5)));
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
      expect(events).toContainEqual(expect.objectContaining({ type: "retry", apiKey: 2, attempt: 1 }));
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
          const metadata = () => new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) => writer.i32(broker.id).string(broker.host).i32(broker.port).string(null))
            .string(null).i32(1).array(["events"], (writer, topic) => writer.i16(0).string(topic).bool(false).array([0], (partitionWriter, partition) => partitionWriter.i16(0).i32(partition).i32(1).array([1], (item) => item.i32(1)).array([1], (item) => item.i32(1))));
          const memberMetadata = new Writer().i16(0).array(["events"], (writer, topic) => writer.string(topic)).bytes(null).result();
          const assignment = new Writer().i16(0).array(["events"], (writer, topic) => writer.string(topic).array([0], (item, partition) => item.i32(partition))).bytes(null).result();
          const body = key === 18 ? apiVersions()
            : key === 10 ? new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port)
              : key === 3 ? metadata()
              : key === 11 ? new Writer().i32(0).i16(0).i32(1).string("range").string("member-1").string("member-1").array(["member-1"], (writer, member) => writer.string(member).bytes(memberMetadata))
                : key === 14 ? new Writer().i16(0).bytes(assignment)
                  : key === 9 ? new Writer().array(["events"], (writer, topic) => writer.string(topic).array([0], (item, partition) => item.i32(partition).i64(12).string(null).i16(0))).i16(0)
                    : key === 8 ? new Writer().array(["events"], (writer, topic) => writer.string(topic).array([0], (item, partition) => item.i32(partition).i16(0)))
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
          const body = key === 18 ? apiVersions()
            : key === 3 ? new Writer()
              .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) => writer.i32(broker.id).string(broker.host).i32(broker.port).string(null))
              .string(null).i32(1).array([], () => {})
              : key === 32
                ? new Writer().i32(0).array([{ resourceType: 2, name: "events" }], (writer, resource) => writer.i16(0).string(null).i8(resource.resourceType).string(resource.name).array([{ name: "cleanup.policy", value: "delete" }], (configWriter, config) => configWriter.string(config.name).string(config.value).bool(false).bool(false).bool(false)))
            : key === 33
              ? new Writer().i32(0).array(["events"], (writer, name) => writer.i16(0).string(null).i8(2).string(name))
              : new Writer().i32(key === 19 ? 7 : 0).array([{ name: "events", error: 0, message: null }], (writer, result) => {
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
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`], onEvent: (event) => events.push(event) });
    const admin = kafka.admin();
    try {
      expect((await admin.createTopics([{ name: "events", numPartitions: 1 }]))[0]?.error).toBe(0);
      expect((await admin.deleteTopics(["events"]))[0]?.name).toBe("events");
      expect((await admin.createPartitions([{ name: "events", count: 2 }]))[0]?.error).toBe(0);
      expect((await admin.describeConfigs([{ resourceType: 2, resourceName: "events" }]))[0]?.configs[0]?.value).toBe("delete");
      expect((await admin.alterConfigs([{ resourceType: 2, resourceName: "events", configs: { "cleanup.policy": "compact" } }]))[0]?.error).toBe(0);
      expect(events).toContainEqual({ type: "throttle", apiKey: 19, durationMs: 7 });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("rejects invalid batching, retry, timeout, and fetch limits", async () => {
    expect(() => new Kafka({ brokers: ["127.0.0.1:1"], connectTimeoutMs: 0 })).toThrow(RangeError);
    expect(() => new Kafka({ brokers: ["127.0.0.1:1"], maxResponseBytes: Number.POSITIVE_INFINITY })).toThrow(RangeError);
    expect(() => new Kafka({ brokers: ["127.0.0.1:1"], retry: { maxRetries: -1 } })).toThrow(RangeError);
    const kafka = new Kafka({ brokers: ["127.0.0.1:1"] });
    expect(() => kafka.producer({ lingerMs: -1 })).toThrow(RangeError);
    expect(() => kafka.producer({ compression: "zstd", idempotent: true })).not.toThrow();
    const consumer = kafka.consumer();
    await expect(consumer.fetch({ maxMessages: Number.NaN })).rejects.toBeInstanceOf(RangeError);
    await kafka.disconnect();
  });

  test("reader rejects truncated input", () => {
    expect(() => new Reader(new Uint8Array([0])).i32()).toThrow(KafkaError);
  });
});
