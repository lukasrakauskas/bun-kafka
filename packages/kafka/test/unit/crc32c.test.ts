import { expect, test } from "bun:test";
import { crc32c } from "../../src/protocol/index.ts";

// Independent bit-at-a-time reference: no lookup tables or word reads.
function reference(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

test("CRC32C matches standard vectors", () => {
  expect(crc32c(new Uint8Array())).toBe(0);
  expect(crc32c(new TextEncoder().encode("123456789"))).toBe(0xe3069283);
  expect(crc32c(new Uint8Array(32))).toBe(0x8a9136aa);
  expect(crc32c(new Uint8Array(32).fill(0xff))).toBe(0x62a8ab43);
  expect(crc32c(Uint8Array.from({ length: 32 }, (_, i) => i))).toBe(0x46dd794e);
});

test("CRC32C matches the reference across word boundaries and unaligned views", () => {
  const data = new Uint8Array(65544);
  let seed = 42;
  for (let i = 0; i < data.length; i++) {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    data[i] = seed >>> 24;
  }
  for (const size of [
    ...Array.from({ length: 129 }, (_, i) => i),
    255,
    256,
    257,
    4095,
    4096,
    65536,
  ]) {
    for (let offset = 0; offset < 8; offset++) {
      const bytes = data.subarray(offset, offset + size);
      expect(crc32c(bytes)).toBe(reference(bytes));
    }
  }
});
