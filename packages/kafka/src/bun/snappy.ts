import { requiredValue } from "../type-guards.ts";

/**
 * Pure-TypeScript Snappy raw-block codec used for Kafka record batches.
 *
 * Kafka wraps one or more Snappy blocks in the "xerial" framing produced by the
 * Java client: a magic header `\x82SNAPPY\0`, a version int32, a compatibility
 * int32, then chunks of `[u32be length][snappy block]`.
 */

const XERIAL_HEADER = new Uint8Array([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);
const INVALID_COPY = "Invalid snappy copy";

function readVarint(input: Uint8Array, offset: number) {
  let value = 0;
  let shift = 0;
  while (true) {
    if (offset >= input.byteLength || shift > 35) {
      throw new RangeError("Invalid snappy varint");
    }
    const byte = requiredValue(input[offset++], "Invalid snappy varint");
    value |= (byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return { value, offset };
    }
    shift += 7;
  }
}

type SnappyCopy = { offset: number; length: number; back: number };
type SnappyLiteral = { offset: number; length: number };

function readLiteral(input: Uint8Array, offset: number, tag: number): SnappyLiteral {
  let length = tag >> 2;
  if (length >= 60) {
    const extra = length - 59;
    length = 0;
    for (let i = 0; i < extra; i++) {
      length |= requiredValue(input[offset++], "Invalid snappy literal") << (8 * i);
    }
  }
  return { offset, length: length + 1 };
}

function readCopy(input: Uint8Array, offset: number, tag: number): SnappyCopy {
  if ((tag & 3) === 1) {
    return {
      offset: offset + 1,
      length: 4 + ((tag >> 2) & 7),
      back: ((tag >> 5) << 8) | requiredValue(input[offset], INVALID_COPY),
    };
  }
  if ((tag & 3) === 2) {
    return {
      offset: offset + 2,
      length: (tag >> 2) + 1,
      back:
        requiredValue(input[offset], INVALID_COPY) |
        (requiredValue(input[offset + 1], INVALID_COPY) << 8),
    };
  }
  return {
    offset: offset + 4,
    length: (tag >> 2) + 1,
    back:
      requiredValue(input[offset], INVALID_COPY) |
      (requiredValue(input[offset + 1], INVALID_COPY) << 8) |
      (requiredValue(input[offset + 2], INVALID_COPY) << 16) |
      (requiredValue(input[offset + 3], INVALID_COPY) << 24),
  };
}

function copyBytes(output: Uint8Array, pos: number, back: number, length: number): void {
  let from = pos - back;
  for (let i = 0; i < length; i++) {
    output[pos + i] = requiredValue(output[from++], INVALID_COPY);
  }
}

function findSnappyMatch(
  input: Uint8Array,
  pos: number,
  table: Int32Array,
  shift: number,
  tableSize: number,
): { candidate: number; length: number } | undefined {
  const value =
    requiredValue(input[pos]) |
    (requiredValue(input[pos + 1]) << 8) |
    (requiredValue(input[pos + 2]) << 16) |
    (requiredValue(input[pos + 3]) << 24);
  const hash = (Math.imul(value, 0x1e35a7bd) >>> shift) & (tableSize - 1);
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
  while (pos + length < input.length && input[candidate + length] === input[pos + length]) {
    length++;
  }
  return { candidate, length };
}

type SnappyDecodeState = { offset: number; pos: number };

function decodeSnappyTag(
  input: Uint8Array,
  output: Uint8Array,
  tag: number,
  offset: number,
  pos: number,
  length: number,
): SnappyDecodeState {
  if ((tag & 3) === 0) {
    const literal = readLiteral(input, offset, tag);
    if (literal.offset + literal.length > input.byteLength || pos + literal.length > length) {
      throw new RangeError("Invalid snappy literal");
    }
    output.set(input.subarray(literal.offset, literal.offset + literal.length), pos);
    return { offset: literal.offset + literal.length, pos: pos + literal.length };
  }
  const copy = readCopy(input, offset, tag);
  if (copy.back <= 0 || copy.back > pos || pos + copy.length > length) {
    throw new RangeError(INVALID_COPY);
  }
  copyBytes(output, pos, copy.back, copy.length);
  return { offset: copy.offset, pos: pos + copy.length };
}

export function snappyDecompressBlock(input: Uint8Array): Uint8Array {
  const { value: length, offset: start } = readVarint(input, 0);
  if (length < 0 || length > 0xffffffff) {
    throw new RangeError("Invalid snappy uncompressed size");
  }
  const output = new Uint8Array(length);
  let pos = 0;
  let offset = start;
  while (offset < input.byteLength) {
    const tag = requiredValue(input[offset++], "Invalid snappy tag");
    ({ offset, pos } = decodeSnappyTag(input, output, tag, offset, pos, length));
  }
  if (pos !== length) {
    throw new RangeError("Snappy payload does not match its declared size");
  }
  return output;
}

function writeVarint(output: Uint8Array, at: number, value: number): number {
  let encoded = value >>> 0;
  while (encoded > 0x7f) {
    output[at++] = (encoded & 0x7f) | 0x80;
    encoded >>>= 7;
  }
  output[at++] = encoded;
  return at;
}

function emitLiteral(
  output: Uint8Array,
  at: number,
  input: Uint8Array,
  from: number,
  count: number,
): number {
  if (!count) {
    return at;
  }
  if (count <= 60) {
    output[at++] = (count - 1) << 2;
  } else if (count <= 0x100) {
    output[at++] = 60 << 2;
    output[at++] = count - 1;
  } else if (count <= 0x10000) {
    output[at++] = 61 << 2;
    output[at++] = (count - 1) & 0xff;
    output[at++] = (count - 1) >>> 8;
  } else if (count <= 0x1000000) {
    output[at++] = 62 << 2;
    output[at++] = (count - 1) & 0xff;
    output[at++] = ((count - 1) >>> 8) & 0xff;
    output[at++] = (count - 1) >>> 16;
  } else {
    output[at++] = 63 << 2;
    output[at++] = (count - 1) & 0xff;
    output[at++] = ((count - 1) >>> 8) & 0xff;
    output[at++] = ((count - 1) >>> 16) & 0xff;
    output[at++] = (count - 1) >>> 24;
  }
  output.set(input.subarray(from, from + count), at);
  return at + count;
}

function emitCopy(output: Uint8Array, at: number, back: number, count: number): number {
  if (back < 2048 && count <= 11) {
    output[at++] = 1 | ((back >> 8) << 5) | ((count - 4) << 2);
    output[at++] = back & 0xff;
    return at;
  }
  let remaining = count;
  while (remaining > 64) {
    output[at++] = 2 | (63 << 2);
    output[at++] = back & 0xff;
    output[at++] = back >>> 8;
    remaining -= 64;
  }
  output[at++] = 2 | ((remaining - 1) << 2);
  output[at++] = back & 0xff;
  output[at++] = back >>> 8;
  return at;
}

/** Greedy hash-table Snappy block compressor compatible with the reference decoder. */
export function snappyCompressBlock(input: Uint8Array): Uint8Array {
  const n = input.byteLength;
  const output = new Uint8Array(n + Math.ceil(n / 6) + 64);
  let at = writeVarint(output, 0, n);
  if (n < 4 || n > 0xffffffff) {
    if (n) {
      at = emitLiteral(output, at, input, 0, n);
    }
    return output.subarray(0, at).slice();
  }

  const bits = Math.min(Math.max(Math.ceil(Math.log2(n)), 4), 14);
  const tableSize = 1 << bits;
  const table = new Int32Array(tableSize).fill(-1);
  const shift = 32 - bits;
  let pos = 0;
  let literalsFrom = 0;
  while (pos + 3 < n) {
    const match = findSnappyMatch(input, pos, table, shift, tableSize);
    if (!match) {
      pos++;
      continue;
    }
    at = emitLiteral(output, at, input, literalsFrom, pos - literalsFrom);
    at = emitCopy(output, at, pos - match.candidate, match.length);
    pos += match.length;
    literalsFrom = pos;
  }
  if (literalsFrom < n) {
    at = emitLiteral(output, at, input, literalsFrom, n - literalsFrom);
  }
  return output.subarray(0, at).slice();
}

/** Compress bytes into Kafka's xerial-framed Snappy format. */
export function snappyCompress(input: Uint8Array): Uint8Array {
  const block = snappyCompressBlock(input);
  const result = new Uint8Array(XERIAL_HEADER.byteLength + 8 + 4 + block.byteLength);
  result.set(XERIAL_HEADER);
  const view = new DataView(result.buffer);
  view.setInt32(8, 1); // xerial version
  view.setInt32(12, 0); // xerial compatibility
  view.setUint32(16, block.byteLength);
  result.set(block, 20);
  return result;
}

/** Decompress Kafka's xerial-framed Snappy format (also accepts bare blocks). */
export function snappyDecompress(input: Uint8Array): Uint8Array {
  if (input.byteLength < XERIAL_HEADER.byteLength + 8) {
    throw new RangeError("Snappy payload is truncated");
  }
  for (let i = 0; i < XERIAL_HEADER.byteLength; i++) {
    if (input[i] !== XERIAL_HEADER[i]) {
      return snappyDecompressBlock(input);
    }
  }
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  const chunks: Uint8Array[] = [];
  let total = 0;
  let offset = 16;
  while (offset + 4 <= input.byteLength) {
    const chunkLength = view.getUint32(offset);
    offset += 4;
    if (chunkLength > input.byteLength - offset) {
      throw new RangeError("Invalid xerial chunk length");
    }
    chunks.push(snappyDecompressBlock(input.subarray(offset, offset + chunkLength)));
    offset += chunkLength;
  }
  for (const chunk of chunks) {
    total += chunk.byteLength;
  }
  const result = new Uint8Array(total);
  let at = 0;
  for (const chunk of chunks) {
    result.set(chunk, at);
    at += chunk.byteLength;
  }
  return result;
}
