import { isString } from "../../type-guards.ts";
import type { Bytes } from "../../types.ts";

const textEncoder = new TextEncoder();

export function asBytes(value: Bytes): Uint8Array | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (isString(value)) {
    return textEncoder.encode(value);
  }
  return value instanceof Uint8Array ? value : new Uint8Array(value);
}
