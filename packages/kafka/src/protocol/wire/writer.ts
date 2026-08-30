import {
  INT16_MAX,
  INT32_MAX,
  INT32_MIN,
  SIZE_I32,
  SIZE_I64,
  UINT32_MAX,
} from "../../bun/shared.ts";

const VARINT_DATA_MASK = 0x7f;
const VARINT_CONTINUE_BIT = 0x80;
const VARINT_SHIFT = 7;
const ZIGZAG_SHIFT32 = 31;
const textEncoder = new TextEncoder();

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
