import {
  COMPRESSION_MASK,
  MAX_COMPRESSION_TYPE,
  RECORD_BATCH_LENGTH_MIN,
} from "../../../bun/shared.ts";
import { KafkaError } from "../../../errors.ts";
import { getCompressionStrategyByCode } from "../../compression.ts";
import { crc32c } from "../crc32c.ts";
import { Reader } from "../reader.ts";

const textDecoder = new TextDecoder();

export type DecodedRecord = {
  attributes: number;
  timestampDelta: bigint;
  absoluteOffset: bigint;
  key: Uint8Array | null;
  value: Uint8Array | null;
  headers: Record<string, Uint8Array | null>;
};

type BatchHeader = {
  baseOffset: bigint;
  batchEnd: number;
  attributes: number;
  baseTimestamp: bigint;
  producerId: bigint;
  recordCount: number;
  compression: number;
};

export function readBatchHeader(reader: Reader): BatchHeader {
  const baseOffset = reader.i64();
  const batchLength = reader.i32();
  if (batchLength < RECORD_BATCH_LENGTH_MIN || batchLength > reader.remaining) {
    throw new KafkaError(-1, "Invalid Kafka record batch size");
  }
  const batchEnd = reader.offset + batchLength;
  reader.i32();
  const magic = reader.i8();
  if (magic !== 2) {
    throw new KafkaError(-1, `Unsupported Kafka record magic ${magic}`);
  }
  const expectedCrc = reader.u32();
  const crcStart = reader.offset;
  const attributes = reader.i16();
  const compression = attributes & COMPRESSION_MASK;
  if (compression > MAX_COMPRESSION_TYPE) {
    throw new KafkaError(-1, `Unsupported Kafka compression codec ${compression}`);
  }
  reader.i32();
  const baseTimestamp = reader.i64();
  reader.i64();
  const producerId = reader.i64();
  reader.i16();
  reader.i32();
  const recordCount = reader.i32();
  if (recordCount < 0 || recordCount > batchLength) {
    throw new KafkaError(-1, "Invalid Kafka record count");
  }
  if (crc32c(reader.data.subarray(crcStart, batchEnd)) !== expectedCrc) {
    throw new KafkaError(-1, "Kafka record CRC mismatch");
  }
  return { baseOffset, batchEnd, attributes, baseTimestamp, producerId, recordCount, compression };
}

export function readDecodedRecord(
  reader: Reader,
  batchEnd: number,
  baseOffset: bigint,
  copy: boolean,
): DecodedRecord {
  const recordLength = reader.varInt();
  if (recordLength < 0 || recordLength > batchEnd - reader.offset) {
    throw new KafkaError(-1, "Invalid Kafka record size");
  }
  const recordEnd = reader.offset + recordLength;
  const attributes = reader.i8();
  const timestampDelta = reader.varLong();
  const absoluteOffset = baseOffset + BigInt(reader.varInt());
  const key = readRecordBytes(reader, "key", copy);
  const value = readRecordBytes(reader, "value", copy);
  const headers: Record<string, Uint8Array | null> = {};
  const headerCount = reader.varInt();
  if (headerCount < 0) {
    throw new KafkaError(-1, "Invalid Kafka header count");
  }
  for (let header = 0; header < headerCount; header++) {
    const nameLength = reader.varInt();
    if (nameLength < 0) {
      throw new KafkaError(-1, "Invalid Kafka header name");
    }
    const name = textDecoder.decode(reader.raw(nameLength));
    headers[name] = readRecordBytes(reader, "header", copy);
  }
  if (reader.offset !== recordEnd) {
    throw new KafkaError(-1, "Invalid Kafka record fields");
  }
  return { attributes, timestampDelta, absoluteOffset, key, value, headers };
}

function readRecordBytes(reader: Reader, field: string, copy: boolean): Uint8Array | null {
  const length = reader.varInt();
  if (length < -1) {
    throw new KafkaError(-1, `Invalid Kafka record ${field} size`);
  }
  const bytes = length < 0 ? null : reader.raw(length);
  return bytes && copy ? bytes.slice() : bytes;
}

export function decompressBatchRecords(
  compression: number,
  records: Uint8Array,
): Uint8Array<ArrayBuffer> {
  try {
    const strategy = getCompressionStrategyByCode(compression);
    if (!strategy) {
      throw new RangeError(`Unsupported Kafka compression codec ${compression}`);
    }
    return strategy.decompress(records);
  } catch (error) {
    throw new KafkaError(-1, `Invalid Kafka compressed record batch: ${error}`);
  }
}
