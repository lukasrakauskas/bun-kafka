/* oxlint-disable no-magic-numbers -- codec bit math */
import { requiredValue } from "../../type-guards.ts";
import { lz4CompressBlock, lz4DecompressBlock } from "./block.ts";
import { xxhash32 } from "./hash.ts";

const MAGIC = 0x184d2204;
const BLOCK_MAX = 4 * 1024 * 1024;

type Lz4FrameHeader = { offset: number; contentSize: bigint | undefined };

function readLz4FrameHeader(input: Uint8Array, view: DataView): Lz4FrameHeader {
  if (input.byteLength < 7 || view.getUint32(0, true) !== MAGIC) {
    throw new RangeError("Invalid LZ4 frame magic");
  }
  const flg = requiredValue(input[4], "Invalid LZ4 frame header");
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
  if (parts) {
    return assembleLz4Parts(output, parts, pos);
  }
  if (output) {
    return output.subarray(0, pos);
  }
  return new Uint8Array();
}
/** Compress one LZ4 block with a greedy hash-table matcher. */

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
