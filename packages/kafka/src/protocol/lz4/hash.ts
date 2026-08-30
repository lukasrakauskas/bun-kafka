/* oxlint-disable no-magic-numbers -- codec bit math */
import { requiredValue } from "../../type-guards.ts";

const rotl = (value: number, bits: number): number =>
  ((value << bits) | (value >>> (32 - bits))) >>> 0;

const XX_PRIME1 = 0x9e3779b1;
const XX_PRIME2 = 0x85ebca77;
const XX_PRIME3 = 0xc2b2ae3d;
const XX_PRIME4 = 0x27d4eb2f;
const XX_PRIME5 = 0x165667b1;

export function xxhash32(input: Uint8Array, seed = 0): number {
  const n = input.byteLength;
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  let h: number;
  let at = 0;
  if (n >= 16) {
    let v1 = (seed + XX_PRIME1 + XX_PRIME2) >>> 0;
    let v2 = (seed + XX_PRIME2) >>> 0;
    let v3 = seed >>> 0;
    let v4 = (seed - XX_PRIME1) >>> 0;
    const limit = n - 16;
    while (at <= limit) {
      v1 =
        Math.imul(
          rotl((v1 + Math.imul(view.getUint32(at, true), XX_PRIME2)) >>> 0, 13),
          XX_PRIME1,
        ) >>> 0;
      v2 =
        Math.imul(
          rotl((v2 + Math.imul(view.getUint32(at + 4, true), XX_PRIME2)) >>> 0, 13),
          XX_PRIME1,
        ) >>> 0;
      v3 =
        Math.imul(
          rotl((v3 + Math.imul(view.getUint32(at + 8, true), XX_PRIME2)) >>> 0, 13),
          XX_PRIME1,
        ) >>> 0;
      v4 =
        Math.imul(
          rotl((v4 + Math.imul(view.getUint32(at + 12, true), XX_PRIME2)) >>> 0, 13),
          XX_PRIME1,
        ) >>> 0;
      at += 16;
    }
    h = (rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)) >>> 0;
  } else {
    h = (seed + XX_PRIME5) >>> 0;
  }
  h = (h + n) >>> 0;
  let remaining = n - at;
  while (remaining >= 4) {
    h = (h + Math.imul(view.getUint32(at, true), XX_PRIME3)) >>> 0;
    h = Math.imul(rotl(h, 17), XX_PRIME4) >>> 0;
    at += 4;
    remaining -= 4;
  }
  while (remaining > 0) {
    h = (h + Math.imul(requiredValue(input[at]), XX_PRIME5)) >>> 0;
    h = Math.imul(rotl(h, 11), XX_PRIME1) >>> 0;
    at++;
    remaining--;
  }
  h ^= h >>> 15;
  h = Math.imul(h, XX_PRIME2) >>> 0;
  h ^= h >>> 13;
  h = Math.imul(h, XX_PRIME3) >>> 0;
  return (h ^ (h >>> 16)) >>> 0;
}
