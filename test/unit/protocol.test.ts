import { describe, expect, test } from "bun:test";
import { KafkaError } from "../../index.ts";
import { Reader, RecordSetDecoder, Writer, crc32c, decodeRecordSet, encodeRecordBatch, murmur2 } from "../../src/bun/protocol.ts";

const decode = (value: Uint8Array | null) => value === null ? null : new TextDecoder().decode(value);

describe("Kafka wire protocol primitives", () => {
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

  test("reader rejects truncated input", () => {
    expect(() => new Reader(new Uint8Array([0])).i32()).toThrow(KafkaError);
  });
});
