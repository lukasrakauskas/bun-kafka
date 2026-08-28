import { KafkaError } from "../errors.ts";
import { arrayBufferBytes, isString, requiredValue } from "../type-guards.ts";
import type {
  AbortedTransaction,
  Bytes,
  ClusterMetadata,
  KafkaMessage,
  MessageHeaders,
} from "../types.ts";
import {
  getCompressionStrategy,
  getCompressionStrategyByCode,
  type RecordCompression,
} from "./compression.ts";
export type { RecordCompression } from "./compression.ts";
import {
  COMPRESSION_MASK,
  INT16_MAX,
  INT32_MAX,
  INT32_MIN,
  MAX_COMPRESSION_TYPE,
  RECORD_ATTR_CONTROL,
  RECORD_ATTR_TIMESTAMP_TYPE,
  RECORD_ATTR_TRANSACTIONAL,
  RECORD_BATCH_HEADER_SIZE,
  RECORD_BATCH_LENGTH_MIN,
  SIZE_I32,
  SIZE_I64,
  UINT32_MAX,
} from "../bun/shared.ts";

const MALFORMED_RESPONSE = "Malformed Kafka response";
const VARINT_DATA_MASK = 0x7f;
const VARINT_CONTINUE_BIT = 0x80;
const VARINT_SHIFT = 7;
const VARINT_MAX_SHIFT = 35;
const ZIGZAG_SHIFT32 = 31;
const CRC32C_POLY = 0x82f6_3b78;
const CRC32C_INIT = 0xffff_ffff;
const MURMUR_SEED = 0x9747_b28c;
const BYTE_MASK = 0xff;
const BITS_PER_BYTE = 8;
const CRC_TABLE_SIZE = 256;
const RECORD_BATCH_LENGTH_OFFSET = 8;
const RECORD_BATCH_LENGTH_ADJUST = 12;
const RECORD_BATCH_CRC_OFFSET = 17;
const RECORD_BATCH_CRC_START = 21;
const RECORD_BATCH_PREAMBLE = 12;
const SHIFT_16 = 16;
const SHIFT_24 = 24;
const MURMUR_SHIFT_MIX = 13;
const MURMUR_SHIFT_FINAL = 15;
const MURMUR_TAIL_THREE = 3;
const BYTE_1 = 1;
const BYTE_2 = 2;
const BYTE_3 = 3;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const noop = () => {};

export function asBytes(value: Bytes): Uint8Array | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (isString(value)) {
    return textEncoder.encode(value);
  }
  return value instanceof Uint8Array ? value : new Uint8Array(value);
}

export class Writer {
  #buffer: ArrayBuffer;
  #view: DataView;
  length = 0;

  constructor(capacity = 256) {
    this.#buffer = new ArrayBuffer(Math.max(1, capacity));
    this.#view = new DataView(this.#buffer);
  }

  #reserve(size: number): number {
    const at = this.length;
    const needed = at + size;
    if (needed > this.#buffer.byteLength) {
      let capacity = this.#buffer.byteLength;
      while (capacity < needed) {
        capacity *= 2;
      }
      const next = new ArrayBuffer(capacity);
      new Uint8Array(next).set(new Uint8Array(this.#buffer, 0, this.length));
      this.#buffer = next;
      this.#view = new DataView(next);
    }
    this.length = needed;
    return at;
  }

  i8(value: number): this {
    const at = this.#reserve(1);
    this.#view.setInt8(at, value);
    return this;
  }
  i16(value: number): this {
    const at = this.#reserve(2);
    this.#view.setInt16(at, value);
    return this;
  }
  i32(value: number): this {
    const at = this.#reserve(SIZE_I32);
    this.#view.setInt32(at, value);
    return this;
  }
  u32(value: number): this {
    const at = this.#reserve(SIZE_I32);
    this.#view.setUint32(at, value);
    return this;
  }
  i64(value: number | bigint): this {
    const at = this.#reserve(SIZE_I64);
    this.#view.setBigInt64(at, BigInt(value));
    return this;
  }
  f64(value: number): this {
    const at = this.#reserve(SIZE_I64);
    this.#view.setFloat64(at, value);
    return this;
  }
  bool(value: boolean): this {
    return this.i8(value ? 1 : 0);
  }

  string(value: string | null): this {
    if (value === null) {
      return this.i16(-1);
    }
    const bytes = textEncoder.encode(value);
    if (bytes.byteLength > INT16_MAX) {
      throw new RangeError("Kafka string is too long");
    }
    return this.i16(bytes.byteLength).raw(bytes);
  }

  bytes(value: Uint8Array | null): this {
    return value === null ? this.i32(-1) : this.i32(value.byteLength).raw(value);
  }

  array<T>(values: readonly T[] | null, write: (writer: Writer, value: T) => void): this {
    if (values === null) {
      return this.i32(-1);
    }
    this.i32(values.length);
    for (const value of values) {
      write(this, value);
    }
    return this;
  }

  raw(value: Uint8Array): this {
    const at = this.#reserve(value.byteLength);
    new Uint8Array(this.#buffer, at, value.byteLength).set(value);
    return this;
  }

  varInt(value: number): this {
    if (!Number.isInteger(value) || value < INT32_MIN || value > INT32_MAX) {
      throw new RangeError("Kafka varint is out of range");
    }
    let encoded = ((value << 1) ^ (value >> ZIGZAG_SHIFT32)) >>> 0;
    while (encoded > VARINT_DATA_MASK) {
      this.i8((encoded & VARINT_DATA_MASK) | VARINT_CONTINUE_BIT);
      encoded >>>= VARINT_SHIFT;
    }
    return this.i8(encoded);
  }

  varLong(value: number | bigint): this {
    let encoded = (BigInt(value) << 1n) ^ (BigInt(value) >> 63n);
    while (encoded > 0x7fn) {
      this.i8(Number(encoded & 0x7fn) | VARINT_CONTINUE_BIT);
      encoded >>= 7n;
    }
    return this.i8(Number(encoded));
  }

  /** Unsigned varint used by compact strings, compact arrays, and tagged fields. */
  uvarint(value: number): this {
    if (!Number.isInteger(value) || value < 0 || value > UINT32_MAX) {
      throw new RangeError("Kafka unsigned varint is out of range");
    }
    let encoded = value;
    while (encoded > VARINT_DATA_MASK) {
      this.i8((encoded & VARINT_DATA_MASK) | VARINT_CONTINUE_BIT);
      encoded >>>= VARINT_SHIFT;
    }
    return this.i8(encoded);
  }

  /** COMPACT_STRING / COMPACT_NULLABLE_STRING: length+1 as an unsigned varint; null encodes as 0. */
  compactString(value: string | null): this {
    if (value === null) {
      return this.uvarint(0);
    }
    const bytes = textEncoder.encode(value);
    return this.uvarint(bytes.byteLength + 1).raw(bytes);
  }

  /** COMPACT_BYTES / COMPACT_NULLABLE_BYTES. */
  compactBytes(value: Uint8Array | null): this {
    return value === null ? this.uvarint(0) : this.uvarint(value.byteLength + 1).raw(value);
  }

  /** COMPACT_ARRAY: element count + 1 as an unsigned varint; null encodes as 0. */
  compactArray<T>(values: readonly T[] | null, write: (writer: Writer, value: T) => void): this {
    if (values === null) {
      return this.uvarint(0);
    }
    this.uvarint(values.length + 1);
    for (const value of values) {
      write(this, value);
    }
    return this;
  }

  /** Empty tagged-field section closing a flexible request. */
  tags(): this {
    return this.uvarint(0);
  }

  patchI32(offset: number, value: number): this {
    this.#view.setInt32(offset, value);
    return this;
  }
  patchU32(offset: number, value: number): this {
    this.#view.setUint32(offset, value);
    return this;
  }
  view(): Uint8Array {
    return new Uint8Array(this.#buffer, 0, this.length);
  }
  result(): Uint8Array {
    return this.view().slice();
  }
}

export class Reader {
  readonly data: Uint8Array;
  #view: DataView;
  offset = 0;

  constructor(data: Uint8Array) {
    this.data = data;
    this.#view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }

  get remaining(): number {
    return this.data.byteLength - this.offset;
  }

  #take(size: number): number {
    if (size < 0 || this.offset + size > this.data.byteLength) {
      throw new KafkaError(-1, MALFORMED_RESPONSE);
    }
    const at = this.offset;
    this.offset += size;
    return at;
  }

  i8(): number {
    return this.#view.getInt8(this.#take(1));
  }
  i16(): number {
    return this.#view.getInt16(this.#take(2));
  }
  i32(): number {
    return this.#view.getInt32(this.#take(SIZE_I32));
  }
  u32(): number {
    return this.#view.getUint32(this.#take(SIZE_I32));
  }
  i64(): bigint {
    return this.#view.getBigInt64(this.#take(SIZE_I64));
  }
  f64(): number {
    return this.#view.getFloat64(this.#take(SIZE_I64));
  }
  bool(): boolean {
    return this.i8() !== 0;
  }

  string(): string | null {
    const size = this.i16();
    return size < 0 ? null : textDecoder.decode(this.raw(size));
  }

  bytes(): Uint8Array | null {
    const size = this.i32();
    return size < 0 ? null : this.raw(size);
  }

  array<T>(read: (reader: Reader) => T): T[] {
    const size = this.i32();
    if (size < 0) {
      return [];
    }
    if (size > this.remaining) {
      throw new KafkaError(-1, "Malformed Kafka array");
    }
    const values = Array.from<T>({ length: size });
    for (let i = 0; i < size; i++) {
      values[i] = read(this);
    }
    return values;
  }

  raw(size: number): Uint8Array {
    const at = this.#take(size);
    return this.data.subarray(at, at + size);
  }

  varInt(): number {
    let value = 0;
    for (let shift = 0; shift < VARINT_MAX_SHIFT; shift += VARINT_SHIFT) {
      if (this.offset >= this.data.byteLength) {
        throw new KafkaError(-1, MALFORMED_RESPONSE);
      }
      const byte = requiredValue(this.data[this.offset++], MALFORMED_RESPONSE);
      value += (byte & VARINT_DATA_MASK) * 2 ** shift;
      if (!(byte & VARINT_CONTINUE_BIT)) {
        return (value >>> 1) ^ -(value & 1);
      }
    }
    throw new KafkaError(-1, "Invalid Kafka varint");
  }

  varLong(): bigint {
    let value = 0n;
    for (let shift = 0n; shift < 70n; shift += 7n) {
      if (this.offset >= this.data.byteLength) {
        throw new KafkaError(-1, MALFORMED_RESPONSE);
      }
      const byte = requiredValue(this.data[this.offset++], MALFORMED_RESPONSE);
      value |= BigInt(byte & VARINT_DATA_MASK) << shift;
      if (!(byte & VARINT_CONTINUE_BIT)) {
        return (value >> 1n) ^ -(value & 1n);
      }
    }
    throw new KafkaError(-1, "Invalid Kafka varlong");
  }

  uvarint(): number {
    let value = 0;
    for (let shift = 0; shift < VARINT_MAX_SHIFT; shift += VARINT_SHIFT) {
      if (this.offset >= this.data.byteLength) {
        throw new KafkaError(-1, MALFORMED_RESPONSE);
      }
      const byte = requiredValue(this.data[this.offset++], MALFORMED_RESPONSE);
      value += (byte & VARINT_DATA_MASK) * 2 ** shift;
      if (!(byte & VARINT_CONTINUE_BIT)) {
        return value;
      }
    }
    throw new KafkaError(-1, "Invalid Kafka unsigned varint");
  }

  /** COMPACT_STRING / COMPACT_NULLABLE_STRING. */
  compactString(): string | null {
    const size = this.uvarint();
    return size === 0 ? null : textDecoder.decode(this.raw(size - 1));
  }

  /** COMPACT_BYTES / COMPACT_NULLABLE_BYTES. */
  compactBytes(): Uint8Array | null {
    const size = this.uvarint();
    return size === 0 ? null : this.raw(size - 1);
  }

  /** COMPACT_ARRAY. */
  compactArray<T>(read: (reader: Reader) => T): T[] {
    const size = this.uvarint();
    if (size === 0) {
      return [];
    }
    const count = size - 1;
    if (count > this.remaining) {
      throw new KafkaError(-1, "Malformed Kafka array");
    }
    const values = Array.from<T>({ length: count });
    for (let i = 0; i < count; i++) {
      values[i] = read(this);
    }
    return values;
  }

  /** Consume a tagged-field section (tagged fields are not interpreted). */
  skipTags(): void {
    const count = this.uvarint();
    for (let i = 0; i < count; i++) {
      this.uvarint(); // tag id
      const size = this.uvarint();
      this.raw(size);
    }
  }
}

function createCrcTable(): Uint32Array {
  const table = new Uint32Array(CRC_TABLE_SIZE);
  for (let i = 0; i < table.length; i++) {
    let value = i;
    for (let bit = 0; bit < BITS_PER_BYTE; bit++) {
      value = value & 1 ? CRC32C_POLY ^ (value >>> 1) : value >>> 1;
    }
    table[i] = value >>> 0;
  }
  return table;
}

let crcTable: Uint32Array | undefined;

export function crc32c(bytes: Uint8Array): number {
  if (!crcTable) {
    crcTable = createCrcTable();
  }
  const table = requiredValue(crcTable);
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.byteLength; i++) {
    crc =
      requiredValue(table[(crc ^ requiredValue(bytes[i])) & BYTE_MASK]) ^ (crc >>> BITS_PER_BYTE);
  }
  return (crc ^ CRC32C_INIT) >>> 0;
}

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

type DecodedRecord = {
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

function readBatchHeader(reader: Reader): BatchHeader {
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

function readDecodedRecord(
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

function decompressBatchRecords(compression: number, records: Uint8Array): Uint8Array<ArrayBuffer> {
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
  const recordBytes = strategy.compress(new Uint8Array(arrayBufferBytes(recordsWriter.result())));
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
  return writer.result();
}

export type RecordDecoderOptions = {
  minOffset?: bigint;
  copy?: boolean;
  /** Aborted transaction ranges for read-committed consumption. */
  abortedTransactions?: readonly AbortedTransaction[];
};

export class RecordSetDecoder {
  readonly topic: string;
  readonly partition: number;
  #reader: Reader;
  #outerReader?: Reader;
  #brokerId: number;
  #minOffset: bigint;
  #copy: boolean;
  #batchEnd = 0;
  #recordsRemaining = 0;
  #baseOffset = 0n;
  #baseTimestamp = 0n;
  #attributes = 0;
  #aborted = new Map<bigint, bigint>();
  #batchProducerId = -1n;

  constructor(
    bytes: Uint8Array,
    topic: string,
    partition: number,
    brokerId: number,
    options: RecordDecoderOptions = {},
  ) {
    this.#reader = new Reader(bytes);
    this.topic = topic;
    this.partition = partition;
    this.#brokerId = brokerId;
    this.#minOffset = options.minOffset ?? -1n;
    this.#copy = options.copy ?? false;
    for (const item of options.abortedTransactions ?? []) {
      this.#aborted.set(item.producerId, item.firstOffset);
    }
  }

  get done(): boolean {
    return (
      this.#recordsRemaining === 0 &&
      this.#reader.remaining < RECORD_BATCH_PREAMBLE &&
      !this.#outerReader
    );
  }

  read(maxMessages = Number.POSITIVE_INFINITY): KafkaMessage[] {
    const messages: KafkaMessage[] = [];
    while (messages.length < maxMessages) {
      if (!this.#recordsRemaining && !this.#prepareNextBatch()) {
        break;
      }
      const record = readDecodedRecord(this.#reader, this.#batchEnd, this.#baseOffset, this.#copy);
      this.#recordsRemaining--;
      if (this.#isControlRecord(record)) {
        continue;
      }
      if (!this.#shouldInclude(record.absoluteOffset)) {
        continue;
      }
      messages.push({
        topic: this.topic,
        partition: this.partition,
        offset: record.absoluteOffset,
        key: record.key,
        value: record.value,
        timestamp: this.#baseTimestamp + record.timestampDelta,
        timestampType: this.#attributes & RECORD_ATTR_TIMESTAMP_TYPE ? 2 : 1,
        headers: record.headers,
        brokerId: this.#brokerId,
        done: noop,
      });
    }
    return messages;
  }

  #prepareNextBatch(): boolean {
    if (this.#outerReader) {
      this.#reader = this.#outerReader;
      this.#outerReader = undefined;
      this.#batchEnd = 0;
    } else if (this.#batchEnd) {
      this.#reader.offset = this.#batchEnd;
    }
    if (this.#reader.remaining < RECORD_BATCH_PREAMBLE) {
      return false;
    }
    this.#openBatch();
    return true;
  }

  #isControlRecord(record: DecodedRecord): boolean {
    if (!(record.attributes & RECORD_ATTR_CONTROL || this.#attributes & RECORD_ATTR_CONTROL)) {
      return false;
    }
    const firstControlByte = record.value?.[0];
    const abortedStart = this.#aborted.get(this.#batchProducerId);
    if (
      firstControlByte === 1 &&
      abortedStart !== undefined &&
      record.absoluteOffset >= abortedStart
    ) {
      this.#aborted.delete(this.#batchProducerId);
    }
    return true;
  }

  #shouldInclude(offset: bigint): boolean {
    const abortedStart = this.#aborted.get(this.#batchProducerId);
    return offset >= this.#minOffset && !(abortedStart !== undefined && offset >= abortedStart);
  }

  #openBatch(): void {
    const header = readBatchHeader(this.#reader);
    this.#baseOffset = header.baseOffset;
    this.#batchEnd = header.batchEnd;
    this.#attributes = header.attributes;
    this.#baseTimestamp = header.baseTimestamp;
    this.#batchProducerId = header.producerId;
    this.#recordsRemaining = header.recordCount;
    if (header.compression) {
      const records = arrayBufferBytes(this.#reader.raw(this.#batchEnd - this.#reader.offset));
      this.#outerReader = this.#reader;
      this.#reader = new Reader(
        new Uint8Array(decompressBatchRecords(header.compression, records)),
      );
      this.#batchEnd = this.#reader.data.byteLength;
    }
  }
}

export function decodeRecordSet(
  bytes: Uint8Array,
  topic: string,
  partition: number,
  brokerId: number,
): KafkaMessage[] {
  return new RecordSetDecoder(bytes, topic, partition, brokerId, { copy: true }).read();
}

export function readMetadataResponse(reader: Reader): ClusterMetadata & { controllerId: number } {
  const brokers = reader.array((r) => {
    const broker = { id: r.i32(), host: r.string() ?? "", port: r.i32() };
    r.string();
    return broker;
  });
  // Metadata v2+: the cluster id precedes the controller id.
  const clusterId = reader.string();
  const controllerId = reader.i32();
  const topics = reader.array((r) => ({
    err: r.i16(),
    name: r.string() ?? "",
    internal: r.bool(),
    partitions: r.array((partitionReader) => {
      const err = partitionReader.i16();
      const id = partitionReader.i32();
      const leader = partitionReader.i32();
      partitionReader.array((x) => x.i32());
      partitionReader.array((x) => x.i32());
      return { err, id, leader };
    }),
  }));
  return { brokers, topics, controllerId, clusterId };
}
