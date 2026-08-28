import { SIZE_I32 } from "../../bun/shared.ts";
import { requiredValue } from "../../type-guards.ts";

const MURMUR_SEED = 0x9747_b28c;
const BITS_PER_BYTE = 8;
const SHIFT_16 = 16;
const SHIFT_24 = 24;
const MURMUR_SHIFT_MIX = 13;
const MURMUR_SHIFT_FINAL = 15;
const MURMUR_TAIL_THREE = 3;
const BYTE_1 = 1;
const BYTE_2 = 2;
const BYTE_3 = 3;

export function murmur2(bytes: Uint8Array): number {
  const m = 0x5bd1e995;
  let hash = (MURMUR_SEED ^ bytes.byteLength) | 0;
  let offset = 0;
  for (; offset + SIZE_I32 <= bytes.byteLength; offset += SIZE_I32) {
    let k =
      requiredValue(bytes[offset]) |
      (requiredValue(bytes[offset + BYTE_1]) << BITS_PER_BYTE) |
      (requiredValue(bytes[offset + BYTE_2]) << SHIFT_16) |
      (requiredValue(bytes[offset + BYTE_3]) << SHIFT_24);
    k = Math.imul(k, m);
    k ^= k >>> SHIFT_24;
    hash = Math.imul(hash, m) ^ Math.imul(k, m);
  }
  const tail = bytes.byteLength - offset;
  if (tail >= MURMUR_TAIL_THREE) {
    hash ^= requiredValue(bytes[offset + BYTE_2]) << SHIFT_16;
  }
  if (tail >= 2) {
    hash ^= requiredValue(bytes[offset + BYTE_1]) << BITS_PER_BYTE;
  }
  if (tail >= 1) {
    hash ^= requiredValue(bytes[offset]);
    hash = Math.imul(hash, m);
  }
  hash ^= hash >>> MURMUR_SHIFT_MIX;
  hash = Math.imul(hash, m);
  return (hash ^ (hash >>> MURMUR_SHIFT_FINAL)) >>> 0;
}
