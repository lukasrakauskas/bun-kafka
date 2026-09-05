import { expect, test } from "bun:test";
import { copyMatch } from "../../src/protocol/copy-match.ts";
import { lz4DecompressBlock } from "../../src/protocol/lz4.ts";
import { snappyDecompressBlock } from "../../src/protocol/snappy.ts";
import { encodeRecordBatch, RecordSetDecoder } from "../../src/protocol/index.ts";

test("native match copies equal forward byte copies, including overlap and view boundaries", () => {
  const lengths = [...Array.from({ length: 130 }, (_, i) => i), 1024, 65536];
  for (const back of [1, 2, 3, 4, 7, 8, 16, 31, 32, 63, 64, 255, 256]) {
    for (const length of lengths) {
      for (const prefix of [0, 5]) {
        const pos = back + prefix;
        const data = new Uint8Array(pos + length + 6).fill(0xaa);
        const output = data.subarray(3, data.length - 3);
        for (let i = 0; i < pos; i++) output[i] = (i * 31 + 7) & 0xff;
        const expected = data.slice();
        for (let i = 0; i < length; i++) expected[3 + pos + i] = expected[3 + pos - back + i];
        copyMatch(output, pos, back, length);
        expect(data).toEqual(expected);
      }
    }
  }
});

test("codec boundaries still reject invalid back-references", () => {
  for (const bytes of [
    [5, 0, 97, 1, 0],
    [5, 0, 97, 1, 2],
    [4, 0, 97, 1, 1],
  ]) {
    expect(() => snappyDecompressBlock(new Uint8Array(bytes))).toThrow(RangeError);
  }
  for (const bytes of [
    [0x10, 97, 0, 0],
    [0x10, 97, 2, 0],
    [0x10, 97, 1, 0],
  ]) {
    expect(() => lz4DecompressBlock(new Uint8Array(bytes), 4)).toThrow(RangeError);
  }
});

test("compressed pages retain owned data when advancing to the next batch", () => {
  for (const codec of ["gzip", "snappy", "lz4", "zstd"] as const) {
    for (const copy of [false, true]) {
      const batch = encodeRecordBatch([{ value: "first" }, { value: "second" }], 1000, codec);
      const tail = encodeRecordBatch([{ value: "tail" }], 1000);
      const wire = new Uint8Array(batch.length + tail.length);
      wire.set(batch);
      wire.set(tail, batch.length);
      const decoder = new RecordSetDecoder(wire, "t", 0, 1, { copy });
      const [first] = decoder.read(1);
      const [second] = decoder.read(1);
      expect(first.value.buffer).not.toBe(wire.buffer);
      expect(first.value.buffer === second.value.buffer).toBe(!copy);
      expect(new TextDecoder().decode(decoder.read(1)[0].value)).toBe("tail");
      expect(decoder.done).toBe(true);
      wire.fill(0);
      expect(new TextDecoder().decode(first.value)).toBe("first");
      expect(new TextDecoder().decode(second.value)).toBe("second");
    }
  }
});
