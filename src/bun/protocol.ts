import { KafkaError } from "../errors.ts";
import type { Bytes, ClusterMetadata, KafkaMessage, MessageHeaders } from "../types.ts";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const noop = () => {};

export function asBytes(value: Bytes): Uint8Array | null {
  if (value == null) return null;
  if (typeof value === "string") return textEncoder.encode(value);
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
      while (capacity < needed) capacity *= 2;
      const next = new ArrayBuffer(capacity);
      new Uint8Array(next).set(new Uint8Array(this.#buffer, 0, this.length));
      this.#buffer = next;
      this.#view = new DataView(next);
    }
    this.length = needed;
    return at;
  }

  i8(value: number): this { const at = this.#reserve(1); this.#view.setInt8(at, value); return this; }
  i16(value: number): this { const at = this.#reserve(2); this.#view.setInt16(at, value); return this; }
  i32(value: number): this { const at = this.#reserve(4); this.#view.setInt32(at, value); return this; }
  u32(value: number): this { const at = this.#reserve(4); this.#view.setUint32(at, value); return this; }
  i64(value: number | bigint): this { const at = this.#reserve(8); this.#view.setBigInt64(at, BigInt(value)); return this; }
  bool(value: boolean): this { return this.i8(value ? 1 : 0); }

  string(value: string | null): this {
    if (value === null) return this.i16(-1);
    const bytes = textEncoder.encode(value);
    if (bytes.byteLength > 0x7fff) throw new RangeError("Kafka string is too long");
    return this.i16(bytes.byteLength).raw(bytes);
  }

  bytes(value: Uint8Array | null): this {
    return value === null ? this.i32(-1) : this.i32(value.byteLength).raw(value);
  }

  array<T>(values: readonly T[] | null, write: (writer: Writer, value: T) => void): this {
    if (values === null) return this.i32(-1);
    this.i32(values.length);
    for (const value of values) write(this, value);
    return this;
  }

  raw(value: Uint8Array): this {
    const at = this.#reserve(value.byteLength);
    new Uint8Array(this.#buffer, at, value.byteLength).set(value);
    return this;
  }

  varInt(value: number): this {
    if (!Number.isInteger(value) || value < -0x80000000 || value > 0x7fffffff) throw new RangeError("Kafka varint is out of range");
    let encoded = ((value << 1) ^ (value >> 31)) >>> 0;
    while (encoded > 0x7f) {
      this.i8((encoded & 0x7f) | 0x80);
      encoded >>>= 7;
    }
    return this.i8(encoded);
  }

  varLong(value: number | bigint): this {
    let encoded = (BigInt(value) << 1n) ^ (BigInt(value) >> 63n);
    while (encoded > 0x7fn) {
      this.i8(Number(encoded & 0x7fn) | 0x80);
      encoded >>= 7n;
    }
    return this.i8(Number(encoded));
  }

  patchI32(offset: number, value: number): this { this.#view.setInt32(offset, value); return this; }
  patchU32(offset: number, value: number): this { this.#view.setUint32(offset, value); return this; }
  view(): Uint8Array { return new Uint8Array(this.#buffer, 0, this.length); }
  result(): Uint8Array { return this.view().slice(); }
}

export class Reader {
  readonly data: Uint8Array;
  #view: DataView;
  offset = 0;

  constructor(data: Uint8Array) {
    this.data = data;
    this.#view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }

  get remaining(): number { return this.data.byteLength - this.offset; }

  #take(size: number): number {
    if (size < 0 || this.offset + size > this.data.byteLength) throw new KafkaError(-1, "Malformed Kafka response");
    const at = this.offset;
    this.offset += size;
    return at;
  }

  i8(): number { return this.#view.getInt8(this.#take(1)); }
  i16(): number { return this.#view.getInt16(this.#take(2)); }
  i32(): number { return this.#view.getInt32(this.#take(4)); }
  u32(): number { return this.#view.getUint32(this.#take(4)); }
  i64(): bigint { return this.#view.getBigInt64(this.#take(8)); }
  bool(): boolean { return this.i8() !== 0; }

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
    if (size < 0) return [];
    if (size > this.remaining) throw new KafkaError(-1, "Malformed Kafka array");
    const values = new Array<T>(size);
    for (let i = 0; i < size; i++) values[i] = read(this);
    return values;
  }

  raw(size: number): Uint8Array {
    const at = this.#take(size);
    return this.data.subarray(at, at + size);
  }

  varInt(): number {
    let value = 0;
    for (let shift = 0; shift < 35; shift += 7) {
      if (this.offset >= this.data.byteLength) throw new KafkaError(-1, "Malformed Kafka response");
      const byte = this.data[this.offset++]!;
      value += (byte & 0x7f) * 2 ** shift;
      if (!(byte & 0x80)) return (value >>> 1) ^ -(value & 1);
    }
    throw new KafkaError(-1, "Invalid Kafka varint");
  }

  varLong(): bigint {
    let value = 0n;
    for (let shift = 0n; shift < 70n; shift += 7n) {
      if (this.offset >= this.data.byteLength) throw new KafkaError(-1, "Malformed Kafka response");
      const byte = this.data[this.offset++]!;
      value |= BigInt(byte & 0x7f) << shift;
      if (!(byte & 0x80)) return (value >> 1n) ^ -(value & 1n);
    }
    throw new KafkaError(-1, "Invalid Kafka varlong");
  }
}

let crcTable: Uint32Array | undefined;

export function crc32c(bytes: Uint8Array): number {
  if (!crcTable) {
    crcTable = new Uint32Array(256);
    for (let i = 0; i < 256; i++) {
      let value = i;
      for (let bit = 0; bit < 8; bit++) value = value & 1 ? 0x82f63b78 ^ (value >>> 1) : value >>> 1;
      crcTable[i] = value >>> 0;
    }
  }
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.byteLength; i++) crc = crcTable[(crc ^ bytes[i]!) & 0xff]! ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

export function murmur2(bytes: Uint8Array): number {
  const m = 0x5bd1e995;
  let hash = (0x9747b28c ^ bytes.byteLength) | 0;
  let offset = 0;
  for (; offset + 4 <= bytes.byteLength; offset += 4) {
    let k = bytes[offset]! | bytes[offset + 1]! << 8 | bytes[offset + 2]! << 16 | bytes[offset + 3]! << 24;
    k = Math.imul(k, m);
    k ^= k >>> 24;
    hash = Math.imul(hash, m) ^ Math.imul(k, m);
  }
  const tail = bytes.byteLength - offset;
  if (tail >= 3) hash ^= bytes[offset + 2]! << 16;
  if (tail >= 2) hash ^= bytes[offset + 1]! << 8;
  if (tail >= 1) { hash ^= bytes[offset]!; hash = Math.imul(hash, m); }
  hash ^= hash >>> 13;
  hash = Math.imul(hash, m);
  return (hash ^ hash >>> 15) >>> 0;
}

export type WireRecord = {
  key?: Bytes;
  value?: Bytes;
  headers?: MessageHeaders;
  timestamp?: number | bigint;
};

type PreparedHeader = { name: Uint8Array; value: Uint8Array | null };
type PreparedRecord = { key: Uint8Array | null; value: Uint8Array | null; headers: PreparedHeader[]; timestamp: bigint; bodyLength: number };

function varIntSize(value: number): number {
  let encoded = ((value << 1) ^ (value >> 31)) >>> 0;
  let size = 1;
  while (encoded > 0x7f) { encoded >>>= 7; size++; }
  return size;
}

function varLongSize(value: bigint): number {
  let encoded = (value << 1n) ^ (value >> 63n);
  let size = 1;
  while (encoded > 0x7fn) { encoded >>= 7n; size++; }
  return size;
}

export function encodeRecordBatch(records: readonly WireRecord[], now = Date.now()): Uint8Array {
  if (!records.length) throw new RangeError("A record batch cannot be empty");
  const baseTimestamp = BigInt(records[0]!.timestamp ?? now);
  let maxTimestamp = baseTimestamp;
  let size = 61;
  const prepared: PreparedRecord[] = new Array(records.length);

  for (let offset = 0; offset < records.length; offset++) {
    const record = records[offset]!;
    const timestamp = BigInt(record.timestamp ?? now);
    if (timestamp > maxTimestamp) maxTimestamp = timestamp;
    const key = asBytes(record.key);
    const value = asBytes(record.value);
    const headers = Object.entries(record.headers ?? {}).map(([name, raw]) => ({ name: textEncoder.encode(name), value: asBytes(raw) }));
    let bodyLength = 1 + varLongSize(timestamp - baseTimestamp) + varIntSize(offset)
      + varIntSize(key?.byteLength ?? -1) + (key?.byteLength ?? 0)
      + varIntSize(value?.byteLength ?? -1) + (value?.byteLength ?? 0)
      + varIntSize(headers.length);
    for (const header of headers) bodyLength += varIntSize(header.name.byteLength) + header.name.byteLength
      + varIntSize(header.value?.byteLength ?? -1) + (header.value?.byteLength ?? 0);
    prepared[offset] = { key, value, headers, timestamp, bodyLength };
    size += varIntSize(bodyLength) + bodyLength;
  }

  const writer = new Writer(size);
  writer.i64(0).i32(0).i32(-1).i8(2).u32(0).i16(0).i32(records.length - 1)
    .i64(baseTimestamp).i64(maxTimestamp).i64(-1).i16(-1).i32(-1).i32(records.length);
  for (let offset = 0; offset < prepared.length; offset++) {
    const record = prepared[offset]!;
    writer.varInt(record.bodyLength).i8(0).varLong(record.timestamp - baseTimestamp).varInt(offset)
      .varInt(record.key?.byteLength ?? -1);
    if (record.key) writer.raw(record.key);
    writer.varInt(record.value?.byteLength ?? -1);
    if (record.value) writer.raw(record.value);
    writer.varInt(record.headers.length);
    for (const header of record.headers) {
      writer.varInt(header.name.byteLength).raw(header.name).varInt(header.value?.byteLength ?? -1);
      if (header.value) writer.raw(header.value);
    }
  }
  writer.patchI32(8, writer.length - 12);
  writer.patchU32(17, crc32c(writer.view().subarray(21)));
  return writer.result();
}

export type RecordDecoderOptions = { minOffset?: bigint; copy?: boolean };

export class RecordSetDecoder {
  readonly topic: string;
  readonly partition: number;
  #reader: Reader;
  #brokerId: number;
  #minOffset: bigint;
  #copy: boolean;
  #batchEnd = 0;
  #recordsRemaining = 0;
  #baseOffset = 0n;
  #baseTimestamp = 0n;
  #attributes = 0;

  constructor(bytes: Uint8Array, topic: string, partition: number, brokerId: number, options: RecordDecoderOptions = {}) {
    this.#reader = new Reader(bytes);
    this.topic = topic;
    this.partition = partition;
    this.#brokerId = brokerId;
    this.#minOffset = options.minOffset ?? -1n;
    this.#copy = options.copy ?? false;
  }

  get done(): boolean {
    return this.#recordsRemaining === 0 && this.#reader.remaining < 12;
  }

  read(maxMessages = Number.POSITIVE_INFINITY): KafkaMessage[] {
    const messages: KafkaMessage[] = [];
    while (messages.length < maxMessages) {
      if (!this.#recordsRemaining) {
        if (this.#batchEnd) this.#reader.offset = this.#batchEnd;
        if (this.#reader.remaining < 12) break;
        this.#openBatch();
        if (!this.#recordsRemaining) continue;
      }
      const reader = this.#reader;
      const recordLength = reader.varInt();
      if (recordLength < 0 || recordLength > this.#batchEnd - reader.offset) throw new KafkaError(-1, "Invalid Kafka record size");
      const recordEnd = reader.offset + recordLength;
      reader.i8();
      const timestampDelta = reader.varLong();
      const offsetDelta = reader.varInt();
      const absoluteOffset = this.#baseOffset + BigInt(offsetDelta);
      const include = absoluteOffset >= this.#minOffset;
      const keyLength = reader.varInt();
      if (keyLength < -1) throw new KafkaError(-1, "Invalid Kafka record key size");
      const keyView = keyLength < 0 ? null : reader.raw(keyLength);
      const valueLength = reader.varInt();
      if (valueLength < -1) throw new KafkaError(-1, "Invalid Kafka record value size");
      const valueView = valueLength < 0 ? null : reader.raw(valueLength);
      const headers: Record<string, Uint8Array | null> = {};
      const headerCount = reader.varInt();
      if (headerCount < 0) throw new KafkaError(-1, "Invalid Kafka header count");
      for (let header = 0; header < headerCount; header++) {
        const nameLength = reader.varInt();
        if (nameLength < 0) throw new KafkaError(-1, "Invalid Kafka header name");
        const name = textDecoder.decode(reader.raw(nameLength));
        const headerLength = reader.varInt();
        if (headerLength < -1) throw new KafkaError(-1, "Invalid Kafka header size");
        const headerView = headerLength < 0 ? null : reader.raw(headerLength);
        if (include) headers[name] = headerView && this.#copy ? headerView.slice() : headerView;
      }
      if (reader.offset !== recordEnd) throw new KafkaError(-1, "Invalid Kafka record fields");
      this.#recordsRemaining--;
      if (include) messages.push({
        topic: this.topic,
        partition: this.partition,
        offset: absoluteOffset,
        key: keyView && this.#copy ? keyView.slice() : keyView,
        value: valueView && this.#copy ? valueView.slice() : valueView,
        timestamp: this.#baseTimestamp + timestampDelta,
        timestampType: this.#attributes & 8 ? 2 : 1,
        headers,
        brokerId: this.#brokerId,
        done: noop,
      });
    }
    return messages;
  }

  #openBatch(): void {
    const reader = this.#reader;
    this.#baseOffset = reader.i64();
    const batchLength = reader.i32();
    if (batchLength < 9 || batchLength > reader.remaining) throw new KafkaError(-1, "Invalid Kafka record batch size");
    this.#batchEnd = reader.offset + batchLength;
    reader.i32();
    const magic = reader.i8();
    if (magic !== 2) throw new KafkaError(-1, `Unsupported Kafka record magic ${magic}`);
    const expectedCrc = reader.u32();
    const crcStart = reader.offset;
    this.#attributes = reader.i16();
    if ((this.#attributes & 7) !== 0) throw new KafkaError(-1, "Compressed records are not supported");
    reader.i32();
    this.#baseTimestamp = reader.i64();
    reader.i64();
    reader.i64();
    reader.i16();
    reader.i32();
    this.#recordsRemaining = reader.i32();
    if (this.#recordsRemaining < 0 || this.#recordsRemaining > batchLength) throw new KafkaError(-1, "Invalid Kafka record count");
    if (crc32c(reader.data.subarray(crcStart, this.#batchEnd)) !== expectedCrc) throw new KafkaError(-1, "Kafka record CRC mismatch");
  }
}

export function decodeRecordSet(bytes: Uint8Array, topic: string, partition: number, brokerId: number): KafkaMessage[] {
  return new RecordSetDecoder(bytes, topic, partition, brokerId, { copy: true }).read();
}

export function readMetadataResponse(reader: Reader): ClusterMetadata & { controllerId: number } {
  const brokers = reader.array((r) => {
    const broker = { id: r.i32(), host: r.string() ?? "", port: r.i32() };
    r.string();
    return broker;
  });
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
  return { brokers, topics, controllerId };
}
