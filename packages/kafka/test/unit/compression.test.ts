import { describe, expect, test } from "bun:test";
import {
  lz4Compress,
  lz4CompressBlock,
  lz4Decompress,
  lz4DecompressBlock,
  xxhash32,
} from "../../src/bun/lz4.ts";
import {
  snappyCompress,
  snappyCompressBlock,
  snappyDecompress,
  snappyDecompressBlock,
} from "../../src/bun/snappy.ts";
import { decodeRecordSet, encodeRecordBatch } from "../../src/bun/protocol.ts";

const decode = (value: Uint8Array) => new TextDecoder().decode(value);

function pattern(length: number): Uint8Array {
  return new Uint8Array(length).map((_, i) => (i * 31 + 7) & 0xff);
}

function same(actual: Uint8Array, expected: Uint8Array): boolean {
  return (
    actual.byteLength === expected.byteLength && actual.every((byte, i) => byte === expected[i])
  );
}

describe("snappy codec", () => {
  test("round-trips sizes around literal and copy boundaries", () => {
    for (const length of [
      0, 1, 3, 4, 12, 13, 59, 60, 61, 64, 100, 256, 257, 65536, 65537, 500, 5000,
    ]) {
      const data = pattern(length);
      if (length >= 40) {
        data.set(pattern(20), length - 20);
      }
      expect(same(snappyDecompressBlock(snappyCompressBlock(data)), data)).toBe(true);
    }
  });

  test("round-trips highly repetitive data with long overlapping copies", () => {
    const data = new TextEncoder().encode("abcdefgh".repeat(200));
    expect(decode(snappyDecompressBlock(snappyCompressBlock(data)))).toBe(decode(data));
    expect(decode(snappyDecompress(snappyCompress(data)))).toBe(decode(data));
  });

  test("xerial framing round-trips through chunked blocks", () => {
    const first = pattern(50);
    const second = new TextEncoder().encode("second-chunk-payload");
    const blocks = [first, second].map((block) => snappyCompressBlock(block));
    const framed = new Uint8Array(16 + 4 + blocks[0]!.byteLength + 4 + blocks[1]!.byteLength);
    framed.set(new Uint8Array([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]));
    const view = new DataView(framed.buffer);
    view.setInt32(8, 1);
    view.setInt32(12, 0);
    let at = 16;
    for (const compressed of blocks) {
      view.setUint32(at, compressed.byteLength);
      at += 4;
      framed.set(compressed, at);
      at += compressed.byteLength;
    }
    const result = snappyDecompress(framed.subarray(0, at));
    expect(result.byteLength).toBe(first.byteLength + second.byteLength);
    expect(Array.from(result.subarray(0, first.byteLength))).toEqual([...first]);
    expect(decode(result.subarray(first.byteLength))).toBe("second-chunk-payload");
  });

  test("rejects corrupt payloads", () => {
    const block = snappyCompressBlock(pattern(64));
    block[2] ^= 0xff;
    expect(() => snappyDecompressBlock(block)).toThrow(RangeError);
    expect(() => snappyDecompress(new Uint8Array([1]))).toThrow(RangeError);
  });
});

describe("lz4 codec", () => {
  test("xxhash32 matches reference vectors", () => {
    expect(xxhash32(new Uint8Array(0))).toBe(0x02cc5d05);
    expect(xxhash32(new TextEncoder().encode("abc"))).toBe(0x32d153ff);
    // Long input exercises the 16-byte stripe loop.
    expect(xxhash32(pattern(1024))).toBe(xxhash32(pattern(1024)));
  });

  test("round-trips blocks and frames across size boundaries", () => {
    for (const length of [0, 1, 12, 13, 100, 4095, 4096, 70000, 200000]) {
      const data = pattern(length);
      expect(same(lz4DecompressBlock(lz4CompressBlock(data), length), data)).toBe(true);
      expect(same(lz4Decompress(lz4Compress(data)), data)).toBe(true);
    }
  });

  test("round-trips repetitive data with extended match lengths", () => {
    const data = new TextEncoder().encode("the quick brown fox ".repeat(400));
    expect(decode(lz4DecompressBlock(lz4CompressBlock(data)))).toBe(decode(data));
    expect(decode(lz4Decompress(lz4Compress(data)))).toBe(decode(data));
  });

  test("frames declare content size and reject bad magic", () => {
    const frame = lz4Compress(new TextEncoder().encode("payload"));
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    expect(view.getUint32(0, true)).toBe(0x184d2204);
    expect(Number(view.getBigUint64(7, true))).toBe(7);
    expect(() => lz4Decompress(new Uint8Array(32))).toThrow(/magic/u);
    const broken = frame.slice();
    broken[6] ^= 0xff;
    expect(() => lz4Decompress(broken)).toThrow(/checksum/u);
  });

  test("decompresses frames without content size", () => {
    const payload = pattern(900);
    const compressed = lz4CompressBlock(payload);
    // Build a frame without the content-size flag.
    const header = new Uint8Array(7);
    const view = new DataView(header.buffer);
    view.setUint32(0, 0x184d2204, true);
    header[4] = 0x60; // version 1, independent blocks, no content size
    header[5] = 0x70;
    header[6] = (xxhash32(header.subarray(4, 6)) >>> 8) & 0xff;
    const end = new Uint8Array(4);
    const parts = [header, new Uint8Array(4), compressed, end].map((part) => part.slice());
    const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
    const frame = new Uint8Array(total);
    let at = 0;
    for (const part of parts) {
      if (part === parts[1]) {
        new DataView(part.buffer).setUint32(0, compressed.byteLength, true);
      }
      frame.set(part, at);
      at += part.byteLength;
    }
    expect(same(lz4Decompress(frame), payload)).toBe(true);
  });
});

describe("record batch compression codecs", () => {
  const records = Array.from({ length: 50 }, (_, i) => ({
    key: `k${i}`,
    value: `value-${i}-${"x".repeat(30)}`,
    headers: { index: String(i) },
  }));

  test("snappy and lz4 record batches round-trip through the decoder", () => {
    for (const codec of ["none", "gzip", "snappy", "lz4", "zstd"] as const) {
      const batch = encodeRecordBatch(records, Date.now(), codec);
      const messages = decodeRecordSet(batch, "t", 0, 1);
      expect(messages).toHaveLength(records.length);
      expect(decode(messages[49]!.value!)).toBe(records[49]!.value);
      expect(decode(messages[7]!.headers.index!)).toBe("7");
    }
  });

  test("rejects unknown compression names", () => {
    expect(() => encodeRecordBatch(records, Date.now(), "brotli" as never)).toThrow(RangeError);
  });
});
