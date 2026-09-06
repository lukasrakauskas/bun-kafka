import { requiredValue } from "../type-guards.ts";

/** Copy a back-reference after the codec has validated its bounds and distance. */
export function copyMatch(output: Uint8Array, pos: number, back: number, length: number): void {
  if (back === 1) {
    output.fill(requiredValue(output[pos - 1]), pos, pos + length);
    return;
  }
  // Only copy initialized bytes. Newly written bytes extend the source on each step;
  // one overlapping copyWithin would use memmove semantics, not LZ repetition.
  while (length > 0) {
    const size = Math.min(back, length);
    output.copyWithin(pos, pos - back, pos - back + size);
    pos += size;
    length -= size;
    back += size;
  }
}
