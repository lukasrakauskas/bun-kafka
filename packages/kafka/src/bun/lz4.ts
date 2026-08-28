/**
 * Pure-TypeScript LZ4 block and frame codecs used for Kafka record batches.
 *
 * Kafka expects the LZ4 frame format (magic 0x184D2204) around LZ4 block data.
 * Compression uses a standard greedy hash-table matcher.
 */

const MAGIC = 0x184d2204;
const BLOCK_MAX = 4 * 1024 * 1024;

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
    h = (h + Math.imul(input[at]!, XX_PRIME5)) >>> 0;
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
    const byte = input[offset++]!;
    length += byte;
    if (byte !== 255) {
      return { offset, length };
    }
  }
}

function copyLz4Match(output: Uint8Array, pos: number, back: number, length: number): void {
  let from = pos - back;
  for (let i = 0; i < length; i++) {
    output[pos + i] = output[from++]!;
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
    input[pos]! | (input[pos + 1]! << 8) | (input[pos + 2]! << 16) | (input[pos + 3]! << 24);
  const hash = (Math.imul(value, XX_PRIME1) >>> (32 - bits)) & (table.length - 1);
  const candidate = table[hash]!;
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
  const token = input[offset++]!;
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
  const back = input[offset]! | (input[offset + 1]! << 8);
  offset += 2;
  if (back === 0 || back > pos) {
    throw new RangeError("Invalid LZ4 match offset");
  }
  const match = readLz4Length(input, offset, token & 15);
  offset = match.offset;
  output = growLz4Output(output, pos + match.length + 4);
  copyLz4Match(output, pos, back, match.length + 4);
  return { output, offset, pos: pos + match.length + 4, done: false };
}

type Lz4FrameHeader = { offset: number; contentSize: bigint | undefined };

function readLz4FrameHeader(input: Uint8Array, view: DataView): Lz4FrameHeader {
  if (input.byteLength < 7 || view.getUint32(0, true) !== MAGIC) {
    throw new RangeError("Invalid LZ4 frame magic");
  }
  const flg = input[4]!;
  if (flg >>> 6 !== 1) {
    throw new RangeError(`Unsupported LZ4 frame version ${flg >>> 6}`);
  }
  const expectedChecksum = (xxhash32(input.subarray(4, 6)) >>> 8) & 0xff;
  if (input[6] !== expectedChecksum) {
    throw new RangeError("Invalid LZ4 frame header checksum");
  }
  let offset = 7;
  if (flg & 0x08) {
    offset += 8;
  }
  if (flg & 0x01) {
    offset += 4;
  }
  if (flg & 0x02) {
    throw new RangeError("LZ4 skippable headers inside frames are unsupported");
  }
  return { offset, contentSize: flg & 0x08 ? view.getBigUint64(7, true) : undefined };
}

type Lz4FrameBlock = { offset: number; size: number; data: Uint8Array };
type Lz4OutputParts = { output: Uint8Array | undefined; parts: Uint8Array[] | undefined };

function readLz4FrameBlock(input: Uint8Array, view: DataView, offset: number): Lz4FrameBlock {
  if (offset + 4 > input.byteLength) {
    throw new RangeError("Truncated LZ4 frame");
  }
  const size = view.getUint32(offset, true);
  offset += 4;
  const length = size & 0x7fffffff;
  return { offset: offset + length, size, data: input.subarray(offset, offset + length) };
}

function appendLz4Block(
  output: Uint8Array | undefined,
  parts: Uint8Array[] | undefined,
  decoded: Uint8Array,
  pos: number,
): Lz4OutputParts {
  if (output && pos + decoded.byteLength <= output.byteLength) {
    output.set(decoded, pos);
  } else {
    (parts ??= []).push(decoded);
  }
  return { output, parts };
}

function assembleLz4Parts(
  output: Uint8Array | undefined,
  parts: Uint8Array[],
  pos: number,
): Uint8Array {
  const all = [...(output ? [output.subarray(0, Math.min(pos, output.byteLength))] : []), ...parts];
  const result = new Uint8Array(all.reduce((sum, part) => sum + part.byteLength, 0));
  let at = 0;
  for (const part of all) {
    result.set(part.subarray(0, Math.min(part.byteLength, result.byteLength - at)), at);
    at += part.byteLength;
  }
  return result;
}

/** Decompress one LZ4 block. The output grows dynamically when `outputSize` is unknown. */
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
export function lz4Decompress(input: Uint8Array): Uint8Array {
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  const header = readLz4FrameHeader(input, view);
  let offset = header.offset;
  let output: Uint8Array | undefined =
    header.contentSize === undefined ? undefined : new Uint8Array(Number(header.contentSize));
  let parts: Uint8Array[] | undefined;
  let pos = 0;
  while (true) {
    const block = readLz4FrameBlock(input, view, offset);
    offset = block.offset;
    if (block.size === 0) {
      break;
    }
    const decoded =
      (block.size & 0x80000000) !== 0 ? block.data.slice() : lz4DecompressBlock(block.data);
    ({ output, parts } = appendLz4Block(output, parts, decoded, pos));
    pos += decoded.byteLength;
  }
  return parts
    ? assembleLz4Parts(output, parts, pos)
    : output
      ? output.subarray(0, pos)
      : new Uint8Array();
}
/** Compress one LZ4 block with a greedy hash-table matcher. */
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
export function lz4Compress(input: Uint8Array): Uint8Array {
  type Part = { data: Uint8Array; raw: boolean };
  const parts: Part[] = [];
  for (let offset = 0; offset < input.byteLength; offset += BLOCK_MAX) {
    const chunk = input.subarray(offset, Math.min(offset + BLOCK_MAX, input.byteLength));
    const compressed = lz4CompressBlock(chunk);
    parts.push(
      compressed.byteLength < chunk.byteLength
        ? { data: compressed, raw: false }
        : { data: chunk.slice(), raw: true },
    );
  }
  let total = 20 + 4 * parts.length;
  for (const part of parts) {
    total += part.data.byteLength;
  }
  const result = new Uint8Array(total);
  const view = new DataView(result.buffer);
  let at = 0;
  view.setUint32(at, MAGIC, true);
  at += 4;
  result[at++] = 0x68; // FLG: version 1, independent blocks, content size present
  result[at++] = 0x70; // BD: 4 MB maximum block size
  result[at++] = (xxhash32(result.subarray(4, 6)) >>> 8) & 0xff; // header checksum
  view.setBigUint64(at, BigInt(input.byteLength), true);
  at += 8;
  for (const part of parts) {
    view.setUint32(at, part.raw ? 0x80000000 | part.data.byteLength : part.data.byteLength, true);
    at += 4;
    result.set(part.data, at);
    at += part.data.byteLength;
  }
  view.setUint32(at, 0, true); // end mark
  return result;
}
