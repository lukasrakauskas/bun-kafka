import { KafkaError } from "../../errors.ts";
import { SIZE_I32 } from "../shared.ts";

export class ResponseFramer {
  #header = new Uint8Array(SIZE_I32);
  #headerOffset = 0;
  #frame?: Uint8Array;
  #frameOffset = 0;

  constructor(readonly maxResponseBytes: number) {}

  *push(chunk: Uint8Array): Generator<Uint8Array> {
    let offset = 0;
    while (offset < chunk.byteLength) {
      if (!this.#frame) {
        const nextOffset = this.#readHeader(chunk, offset);
        if (nextOffset === undefined) {
          break;
        }
        offset = nextOffset;
      }
      const frame = this.#frame;
      if (!frame) {
        break;
      }
      const count = Math.min(frame.byteLength - this.#frameOffset, chunk.byteLength - offset);
      frame.set(chunk.subarray(offset, offset + count), this.#frameOffset);
      this.#frameOffset += count;
      offset += count;
      if (this.#frameOffset === frame.byteLength) {
        this.#frame = undefined;
        this.#frameOffset = 0;
        yield frame;
      }
    }
  }

  reset(): void {
    this.#headerOffset = 0;
    this.#frame = undefined;
    this.#frameOffset = 0;
  }

  #readHeader(chunk: Uint8Array, offset: number): number | undefined {
    const count = Math.min(SIZE_I32 - this.#headerOffset, chunk.byteLength - offset);
    this.#header.set(chunk.subarray(offset, offset + count), this.#headerOffset);
    this.#headerOffset += count;
    offset += count;
    if (this.#headerOffset < SIZE_I32) {
      return;
    }
    const size = new DataView(this.#header.buffer).getInt32(0);
    this.#headerOffset = 0;
    if (size < SIZE_I32 || size > this.maxResponseBytes) {
      throw new KafkaError(-1, `Invalid Kafka response size ${size}`);
    }
    this.#frame = new Uint8Array(size);
    this.#frameOffset = 0;
    return offset;
  }
}
