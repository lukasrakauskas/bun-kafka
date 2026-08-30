import {
  emptyRequest,
  RequestBody,
  ResponseBody,
  requestFromWriter,
  responseFromBytes,
} from "./body.ts";
import { Reader } from "./wire/reader.ts";
import { Writer } from "./wire/writer.ts";

/** Public encoder surface. Implementation is protocol-private Writer. */
export type KafkaEncoder = {
  i8(value: number): KafkaEncoder;
  i16(value: number): KafkaEncoder;
  i32(value: number): KafkaEncoder;
  u32(value: number): KafkaEncoder;
  i64(value: number | bigint): KafkaEncoder;
  f64(value: number): KafkaEncoder;
  bool(value: boolean): KafkaEncoder;
  string(value: string | null): KafkaEncoder;
  bytes(value: Uint8Array | null): KafkaEncoder;
  array<T>(
    values: readonly T[] | null,
    write: (encoder: KafkaEncoder, value: T) => void,
  ): KafkaEncoder;
  raw(value: Uint8Array): KafkaEncoder;
  varInt(value: number): KafkaEncoder;
  varLong(value: number | bigint): KafkaEncoder;
  uvarint(value: number): KafkaEncoder;
  compactString(value: string | null): KafkaEncoder;
  compactBytes(value: Uint8Array | null): KafkaEncoder;
  compactArray<T>(
    values: readonly T[] | null,
    write: (encoder: KafkaEncoder, value: T) => void,
  ): KafkaEncoder;
  tags(): KafkaEncoder;
  patchI32(offset: number, value: number): KafkaEncoder;
  patchU32(offset: number, value: number): KafkaEncoder;
  view(): Uint8Array;
  readonly length: number;
  result(): Uint8Array;
};

/** Public decoder surface. Implementation is protocol-private Reader. */
export type KafkaDecoder = {
  readonly remaining: number;
  readonly data: Uint8Array;
  i8(): number;
  i16(): number;
  i32(): number;
  u32(): number;
  i64(): bigint;
  f64(): number;
  bool(): boolean;
  string(): string | null;
  bytes(): Uint8Array | null;
  array<T>(read: (decoder: KafkaDecoder) => T): T[];
  raw(length: number): Uint8Array;
  varInt(): number;
  varLong(): bigint;
  uvarint(): number;
  compactString(): string | null;
  compactBytes(): Uint8Array | null;
  compactArray<T>(read: (decoder: KafkaDecoder) => T): T[];
  skipTags(): KafkaDecoder;
};

class EncoderView implements KafkaEncoder {
  readonly #writer: Writer;
  constructor(writer: Writer = new Writer()) {
    this.#writer = writer;
  }
  i8(value: number): KafkaEncoder {
    this.#writer.i8(value);
    return this;
  }
  i16(value: number): KafkaEncoder {
    this.#writer.i16(value);
    return this;
  }
  i32(value: number): KafkaEncoder {
    this.#writer.i32(value);
    return this;
  }
  u32(value: number): KafkaEncoder {
    this.#writer.u32(value);
    return this;
  }
  i64(value: number | bigint): KafkaEncoder {
    this.#writer.i64(value);
    return this;
  }
  f64(value: number): KafkaEncoder {
    this.#writer.f64(value);
    return this;
  }
  bool(value: boolean): KafkaEncoder {
    this.#writer.bool(value);
    return this;
  }
  string(value: string | null): KafkaEncoder {
    this.#writer.string(value);
    return this;
  }
  bytes(value: Uint8Array | null): KafkaEncoder {
    this.#writer.bytes(value);
    return this;
  }
  array<T>(
    values: readonly T[] | null,
    write: (encoder: KafkaEncoder, value: T) => void,
  ): KafkaEncoder {
    this.#writer.array(values, (writer, value) => write(new EncoderView(writer), value));
    return this;
  }
  raw(value: Uint8Array): KafkaEncoder {
    this.#writer.raw(value);
    return this;
  }
  varInt(value: number): KafkaEncoder {
    this.#writer.varInt(value);
    return this;
  }
  varLong(value: number | bigint): KafkaEncoder {
    this.#writer.varLong(value);
    return this;
  }
  uvarint(value: number): KafkaEncoder {
    this.#writer.uvarint(value);
    return this;
  }
  compactString(value: string | null): KafkaEncoder {
    this.#writer.compactString(value);
    return this;
  }
  compactBytes(value: Uint8Array | null): KafkaEncoder {
    this.#writer.compactBytes(value);
    return this;
  }
  compactArray<T>(
    values: readonly T[] | null,
    write: (encoder: KafkaEncoder, value: T) => void,
  ): KafkaEncoder {
    this.#writer.compactArray(values, (writer, value) => write(new EncoderView(writer), value));
    return this;
  }
  tags(): KafkaEncoder {
    this.#writer.tags();
    return this;
  }
  patchI32(offset: number, value: number): KafkaEncoder {
    this.#writer.patchI32(offset, value);
    return this;
  }
  patchU32(offset: number, value: number): KafkaEncoder {
    this.#writer.patchU32(offset, value);
    return this;
  }
  view(): Uint8Array {
    return this.#writer.view();
  }
  get length(): number {
    return this.#writer.length;
  }
  result(): Uint8Array {
    return this.#writer.result();
  }
  toRequest(): import("./body.ts").RequestBody {
    return requestFromWriter(this.#writer);
  }
  /** @internal */
  writer(): Writer {
    return this.#writer;
  }
}

class DecoderView implements KafkaDecoder {
  readonly #reader: Reader;
  constructor(reader: Reader) {
    this.#reader = reader;
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
    return this.#reader.array((reader) => read(new DecoderView(reader)));
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
    return this.#reader.compactArray((reader) => read(new DecoderView(reader)));
  }
  skipTags(): KafkaDecoder {
    this.#reader.skipTags();
    return this;
  }
}

/** Build a request body without exposing Writer. */
export function encodeRequest(build: (encoder: KafkaEncoder) => void): RequestBody {
  const view = new EncoderView();
  build(view);
  return requestFromWriter(view.writer());
}

export function emptyRequestBody(): RequestBody {
  return emptyRequest();
}

/** Decode a response body without exposing Reader. */
export function decodeResponse(body: ResponseBody): KafkaDecoder {
  return new DecoderView(body.reader());
}

/** Nested bytes payload as a decoder. */
export function decodeBytes(bytes: Uint8Array | null | undefined): KafkaDecoder {
  return new DecoderView(responseFromBytes(bytes ?? new Uint8Array()).reader());
}

export function encoder(capacity?: number): KafkaEncoder {
  return new EncoderView(capacity === undefined ? new Writer() : new Writer(capacity));
}

/** Decode raw bytes without exposing Reader. */
export function decoder(bytes: Uint8Array): KafkaDecoder {
  return decodeBytes(bytes);
}
