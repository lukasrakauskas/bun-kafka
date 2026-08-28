import type { KafkaDecoder, KafkaEncoder } from "./codec.ts";
import { Reader } from "./wire/reader.ts";
import { Writer } from "./wire/writer.ts";

/** Opaque Kafka request payload. */
export class RequestBody {
  readonly #bytes: Uint8Array;
  constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
  }
  toBytes(): Uint8Array {
    return this.#bytes;
  }
  /** @deprecated alias for transport helpers still calling result() */
  result(): Uint8Array {
    return this.#bytes;
  }

  static from(value: RequestBody | KafkaEncoder | Writer): RequestBody {
    if (value instanceof RequestBody) {
      return value;
    }
    if (value instanceof Writer) {
      return new RequestBody(value.result());
    }
    // KafkaEncoder view
    return new RequestBody(value.result());
  }
}

/** Opaque Kafka response payload with decoder methods. */
export class ResponseBody implements KafkaDecoder {
  readonly #reader: Reader;
  constructor(reader: Reader) {
    this.#reader = reader;
  }
  reader(): Reader {
    return this.#reader;
  }
  get remaining(): number {
    return this.#reader.remaining;
  }
  get data(): Uint8Array {
    return this.#reader.data;
  }
  i8(): number {
    return this.#reader.i8();
  }
  i16(): number {
    return this.#reader.i16();
  }
  i32(): number {
    return this.#reader.i32();
  }
  u32(): number {
    return this.#reader.u32();
  }
  i64(): bigint {
    return this.#reader.i64();
  }
  f64(): number {
    return this.#reader.f64();
  }
  bool(): boolean {
    return this.#reader.bool();
  }
  string(): string | null {
    return this.#reader.string();
  }
  bytes(): Uint8Array | null {
    return this.#reader.bytes();
  }
  array<T>(read: (decoder: KafkaDecoder) => T): T[] {
    return this.#reader.array((inner) => read(new ResponseBody(inner)));
  }
  raw(length: number): Uint8Array {
    return this.#reader.raw(length);
  }
  varInt(): number {
    return this.#reader.varInt();
  }
  varLong(): bigint {
    return this.#reader.varLong();
  }
  uvarint(): number {
    return this.#reader.uvarint();
  }
  compactString(): string | null {
    return this.#reader.compactString();
  }
  compactBytes(): Uint8Array | null {
    return this.#reader.compactBytes();
  }
  compactArray<T>(read: (decoder: KafkaDecoder) => T): T[] {
    return this.#reader.compactArray((inner) => read(new ResponseBody(inner)));
  }
  skipTags(): this {
    this.#reader.skipTags();
    return this;
  }
}

export function requestFromWriter(writer: Writer): RequestBody {
  return new RequestBody(writer.result());
}

export function emptyRequest(): RequestBody {
  return new RequestBody(new Uint8Array());
}

export function responseFromReader(reader: Reader): ResponseBody {
  return new ResponseBody(reader);
}

export function responseFromBytes(bytes: Uint8Array): ResponseBody {
  return new ResponseBody(new Reader(bytes));
}
