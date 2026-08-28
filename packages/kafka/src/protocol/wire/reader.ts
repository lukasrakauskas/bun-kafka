import { SIZE_I32, SIZE_I64 } from "../../bun/shared.ts";
import { KafkaError } from "../../errors.ts";
import { requiredValue } from "../../type-guards.ts";

const MALFORMED_RESPONSE = "Malformed Kafka response";
const VARINT_DATA_MASK = 0x7f;
const VARINT_CONTINUE_BIT = 0x80;
const VARINT_SHIFT = 7;
const VARINT_MAX_SHIFT = 35;
const textDecoder = new TextDecoder();

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
