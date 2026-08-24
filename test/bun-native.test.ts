import { describe, expect, test } from "bun:test";
import { Kafka, KafkaError } from "../index.ts";
import { Reader, RecordSetDecoder, Writer, crc32c, decodeRecordSet, encodeRecordBatch, murmur2 } from "../src/bun/protocol.ts";

const decode = (value: Uint8Array | null) => value === null ? null : new TextDecoder().decode(value);

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
          expect(view.getInt16(4)).toBe(3);
          const correlation = view.getInt32(8);
          const metadata = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) => {
              writer.i32(broker.id).string(broker.host).i32(broker.port).string(null);
            })
            .i32(1)
            .array([{ name: "events" }], (writer, topic) => {
              writer.i16(0).string(topic.name).bool(false).array([0], (partitionWriter, partition) => {
                partitionWriter.i16(0).i32(partition).i32(1)
                  .array([1], (itemWriter, id) => itemWriter.i32(id))
                  .array([1], (itemWriter, id) => itemWriter.i32(id));
              });
            });
          const response = new Writer().i32(0).i32(correlation).raw(metadata.result());
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

  test("rejects invalid batching and fetch limits", async () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:1"] });
    expect(() => kafka.producer({ lingerMs: -1 })).toThrow(RangeError);
    const consumer = kafka.consumer();
    await expect(consumer.fetch({ maxMessages: Number.NaN })).rejects.toBeInstanceOf(RangeError);
    await kafka.disconnect();
  });

  test("reader rejects truncated input", () => {
    expect(() => new Reader(new Uint8Array([0])).i32()).toThrow(KafkaError);
  });
});
