import {
  RECORD_ATTR_CONTROL,
  RECORD_ATTR_TRANSACTIONAL,
  RECORD_BATCH_HEADER_SIZE,
} from "../../../bun/shared.ts";
import { requiredValue } from "../../../type-guards.ts";
import type { Bytes, MessageHeaders } from "../../../types.ts";
import { getCompressionStrategy, type RecordCompression } from "../../compression.ts";
import { crc32c } from "../crc32c.ts";
import { asBytes } from "../bytes.ts";
import { Writer } from "../writer.ts";

const VARINT_DATA_MASK = 0x7f;
const VARINT_SHIFT = 7;
const ZIGZAG_SHIFT32 = 31;
const RECORD_BATCH_LENGTH_OFFSET = 8;
const RECORD_BATCH_LENGTH_ADJUST = 12;
const RECORD_BATCH_CRC_OFFSET = 17;
const RECORD_BATCH_CRC_START = 21;
const textEncoder = new TextEncoder();

export type WireRecord = {
  key?: Bytes;
  value?: Bytes;
  headers?: MessageHeaders;
  timestamp?: number | bigint;
};

type PreparedHeader = { name: Uint8Array; value: Uint8Array | null };
type PreparedRecord = {
  key: Uint8Array | null;
  value: Uint8Array | null;
  headers: PreparedHeader[];
  timestamp: bigint;
  bodyLength: number;
};

function varIntSize(value: number): number {
  let encoded = ((value << 1) ^ (value >> ZIGZAG_SHIFT32)) >>> 0;
  let size = 1;
  while (encoded > VARINT_DATA_MASK) {
    encoded >>>= VARINT_SHIFT;
    size++;
  }
  return size;
}

function varLongSize(value: bigint): number {
  let encoded = (value << 1n) ^ (value >> 63n);
  let size = 1;
  while (encoded > 0x7fn) {
    encoded >>= 7n;
    size++;
  }
  return size;
}

function prepareRecord(
  record: WireRecord,
  offset: number,
  baseTimestamp: bigint,
  now: number,
): PreparedRecord {
  const timestamp = BigInt(record.timestamp ?? now);
  const key = asBytes(record.key);
  const value = asBytes(record.value);
  const headers = Object.entries(record.headers ?? {}).map(([name, raw]) => ({
    name: textEncoder.encode(name),
    value: asBytes(raw),
  }));
  const bodyLength =
    1 +
    varLongSize(timestamp - baseTimestamp) +
    varIntSize(offset) +
    varIntSize(key?.byteLength ?? -1) +
    (key?.byteLength ?? 0) +
    varIntSize(value?.byteLength ?? -1) +
    (value?.byteLength ?? 0) +
    varIntSize(headers.length) +
    headers.reduce(
      (size, header) =>
        size +
        varIntSize(header.name.byteLength) +
        header.name.byteLength +
        varIntSize(header.value?.byteLength ?? -1) +
        (header.value?.byteLength ?? 0),
      0,
    );
  return { key, value, headers, timestamp, bodyLength };
}

function writeRecord(
  writer: Writer,
  record: PreparedRecord,
  offset: number,
  baseTimestamp: bigint,
  attributes: number,
): void {
  writer
    .varInt(record.bodyLength)
    .i8(attributes)
    .varLong(record.timestamp - baseTimestamp)
    .varInt(offset)
    .varInt(record.key?.byteLength ?? -1);
  if (record.key) {
    writer.raw(record.key);
  }
  writer.varInt(record.value?.byteLength ?? -1);
  if (record.value) {
    writer.raw(record.value);
  }
  writer.varInt(record.headers.length);
  for (const header of record.headers) {
    writer
      .varInt(header.name.byteLength)
      .raw(header.name)
      .varInt(header.value?.byteLength ?? -1);
    if (header.value) {
      writer.raw(header.value);
    }
  }
}

export function encodeRecordBatch(
  records: readonly WireRecord[],
  now = Date.now(),
  compression: RecordCompression = "none",
  producer: {
    id: bigint;
    epoch: number;
    sequence: number;
    control?: boolean;
    transactional?: boolean;
  } = { id: -1n, epoch: -1, sequence: -1 },
  baseOffset = 0n,
): Uint8Array {
  if (!records.length) {
    throw new RangeError("A record batch cannot be empty");
  }
  const strategy = getCompressionStrategy(compression);
  const recordAttributes = producer.control ? RECORD_ATTR_CONTROL : 0;
  const batchAttributes =
    strategy.code | (producer.transactional && !producer.control ? RECORD_ATTR_TRANSACTIONAL : 0);
  const baseTimestamp = BigInt(requiredValue(records[0]).timestamp ?? now);
  const prepared = records.map((record, offset) =>
    prepareRecord(record, offset, baseTimestamp, now),
  );
  const maxTimestamp = prepared.reduce(
    (max, record) => (record.timestamp > max ? record.timestamp : max),
    baseTimestamp,
  );
  const size =
    RECORD_BATCH_HEADER_SIZE +
    prepared.reduce(
      (total, record) => total + varIntSize(record.bodyLength) + record.bodyLength,
      0,
    );
  const recordsWriter = new Writer(size - RECORD_BATCH_HEADER_SIZE);
  prepared.forEach((record, offset) =>
    writeRecord(recordsWriter, record, offset, baseTimestamp, recordAttributes),
  );
  const recordBytes =
    compression === "none" ? recordsWriter.view() : strategy.compress(recordsWriter.view());
  const writer = new Writer(RECORD_BATCH_HEADER_SIZE + recordBytes.byteLength);
  writer
    .i64(baseOffset)
    .i32(0)
    .i32(-1)
    .i8(2)
    .u32(0)
    .i16(batchAttributes)
    .i32(records.length - 1)
    .i64(baseTimestamp)
    .i64(maxTimestamp)
    .i64(producer.id)
    .i16(producer.epoch)
    .i32(producer.sequence)
    .i32(records.length)
    .raw(recordBytes);
  writer.patchI32(RECORD_BATCH_LENGTH_OFFSET, writer.length - RECORD_BATCH_LENGTH_ADJUST);
  writer.patchU32(RECORD_BATCH_CRC_OFFSET, crc32c(writer.view().subarray(RECORD_BATCH_CRC_START)));
  // The writer is local and never reused; transfer its exact-sized buffer to the caller.
  return writer.view();
}
