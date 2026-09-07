/* oxlint-disable no-magic-numbers -- codec bit math */
import { requiredValue } from "../../type-guards.ts";
import { copyMatch } from "../copy-match.ts";

const XX_PRIME1 = 0x9e37_79b1;

type Lz4Length = { offset: number; length: number };

function growLz4Output(output: Uint8Array, needed: number): Uint8Array {
  if (needed <= output.byteLength) {
    return output;
  }
  let capacity = output.byteLength;
  while (capacity < needed) {
    capacity *= 2;
  }
  const next = new Uint8Array(capacity);
  next.set(output);
  return next;
}

function readLz4Length(input: Uint8Array, offset: number, length: number): Lz4Length {
  if (length !== 15) {
    return { offset, length };
  }
  while (true) {
    if (offset >= input.byteLength) {
      throw new RangeError("Invalid LZ4 extended length");
    }
    const byte = requiredValue(input[offset++], "Invalid LZ4 extended length");
    length += byte;
    if (byte !== 255) {
      return { offset, length };
    }
  }
}

function findLz4Match(
  input: Uint8Array,
  pos: number,
  table: Int32Array,
  bits: number,
  matchLimit: number,
): { candidate: number; length: number } | undefined {
  const value =
    requiredValue(input[pos]) |
    (requiredValue(input[pos + 1]) << 8) |
    (requiredValue(input[pos + 2]) << 16) |
    (requiredValue(input[pos + 3]) << 24);
  const hash = (Math.imul(value, XX_PRIME1) >>> (32 - bits)) & (table.length - 1);
  const candidate = requiredValue(table[hash]);
  table[hash] = pos;
  if (
    candidate === -1 ||
    pos - candidate >= 65536 ||
    input[candidate] !== input[pos] ||
    input[candidate + 1] !== input[pos + 1] ||
    input[candidate + 2] !== input[pos + 2] ||
    input[candidate + 3] !== input[pos + 3]
  ) {
    return;
  }
  let length = 4;
  while (pos + length < matchLimit && input[candidate + length] === input[pos + length]) {
    length++;
  }
  return { candidate, length };
}

function emitLz4Sequence(
  output: Uint8Array,
  input: Uint8Array,
  at: number,
  literalsFrom: number,
  literalsTo: number,
  matchFrom: number,
  matchLength: number,
): number {
  const literals = literalsTo - literalsFrom;
  const tokenAt = at++;
  let literalCode = literals;
  if (literals >= 15) {
    literalCode = 15;
    let rest = literals - 15;
    while (rest >= 255) {
      output[at++] = 255;
      rest -= 255;
    }
    output[at++] = rest;
  }
  output.set(input.subarray(literalsFrom, literalsTo), at);
  at += literals;
  let matchExtra = -1;
  let matchCode = matchLength - 4;
  if (matchCode >= 15) {
    matchExtra = matchCode - 15;
    matchCode = 15;
  }
  const back = literalsTo - matchFrom;
  output[tokenAt] = (literalCode << 4) | matchCode;
  output[at++] = back & 0xff;
  output[at++] = back >>> 8;
  if (matchExtra >= 0) {
    while (matchExtra >= 255) {
      output[at++] = 255;
      matchExtra -= 255;
    }
    output[at++] = matchExtra;
  }
  return at;
}

function emitLz4Literals(output: Uint8Array, input: Uint8Array, at: number, from: number): number {
  const count = input.byteLength - from;
  const code = Math.min(count, 15);
  output[at++] = code << 4;
  if (count >= 15) {
    let remaining = count - 15;
    while (remaining >= 255) {
      output[at++] = 255;
      remaining -= 255;
    }
    output[at++] = remaining;
  }
  output.set(input.subarray(from), at);
  return at + count;
}

type Lz4BlockState = {
  output: Uint8Array<ArrayBufferLike>;
  offset: number;
  pos: number;
  done: boolean;
};

function decodeLz4Sequence(
  input: Uint8Array,
  output: Uint8Array<ArrayBufferLike>,
  offset: number,
  pos: number,
): Lz4BlockState {
  const token = requiredValue(input[offset++], "Invalid LZ4 block");
  const literal = readLz4Length(input, offset, token >> 4);
  offset = literal.offset;
  output = growLz4Output(output, pos + literal.length);
  if (offset + literal.length > input.byteLength) {
    throw new RangeError("Invalid LZ4 literal length");
  }
  output.set(input.subarray(offset, offset + literal.length), pos);
  offset += literal.length;
  pos += literal.length;
  if (offset >= input.byteLength) {
    return { output, offset, pos, done: true };
  }
  if (offset + 2 > input.byteLength) {
    throw new RangeError("Invalid LZ4 match offset");
  }
  const back = requiredValue(input[offset]) | (requiredValue(input[offset + 1]) << 8);
  offset += 2;
  if (back === 0 || back > pos) {
    throw new RangeError("Invalid LZ4 match offset");
  }
  const match = readLz4Length(input, offset, token & 15);
  offset = match.offset;
  output = growLz4Output(output, pos + match.length + 4);
  copyMatch(output, pos, back, match.length + 4);
  return { output, offset, pos: pos + match.length + 4, done: false };
}

export function lz4DecompressBlock(input: Uint8Array, outputSize?: number): Uint8Array {
  let output: Uint8Array<ArrayBufferLike> = new Uint8Array(
    outputSize && outputSize > 0 ? outputSize : Math.max(4096, input.byteLength * 4),
  );
  let offset = 0;
  let pos = 0;
  while (offset < input.byteLength) {
    const state = decodeLz4Sequence(input, output, offset, pos);
    ({ output, offset, pos } = state);
    if (state.done) {
      break;
    }
  }
  if (outputSize !== undefined && outputSize > 0 && pos !== outputSize) {
    throw new RangeError("LZ4 payload does not match its declared size");
  }
  return output.subarray(0, pos).slice();
}
/** Decompress an LZ4 frame produced by any conformant encoder. */

export function lz4CompressBlock(input: Uint8Array): Uint8Array {
  const n = input.byteLength;
  const output = new Uint8Array(n + Math.ceil(n / 255) + 16);
  if (n < 13) {
    return output.subarray(0, emitLz4Literals(output, input, 0, 0)).slice();
  }
  const bits = Math.min(Math.max(Math.ceil(Math.log2(n)), 4), 16);
  const table = new Int32Array(1 << bits).fill(-1);
  const matchLimit = n - 5;
  let at = 0;
  let pos = 0;
  let literalsFrom = 0;
  while (pos <= n - 12) {
    const match = findLz4Match(input, pos, table, bits, matchLimit);
    if (!match) {
      pos++;
      continue;
    }
    at = emitLz4Sequence(output, input, at, literalsFrom, pos, match.candidate, match.length);
    pos += match.length;
    literalsFrom = pos;
  }
  at = emitLz4Literals(output, input, at, literalsFrom);
  return output.subarray(0, at).slice();
}

/** Compress bytes into the Kafka-compatible LZ4 frame format. */
