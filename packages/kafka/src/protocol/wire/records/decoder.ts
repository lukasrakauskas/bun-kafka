import { RECORD_ATTR_CONTROL, RECORD_ATTR_TIMESTAMP_TYPE } from "../../../bun/shared.ts";
import { arrayBufferBytes } from "../../../type-guards.ts";
import type { AbortedTransaction, KafkaMessage } from "../../../types.ts";
import {
  decompressBatchRecords,
  readBatchHeader,
  readDecodedRecord,
  type DecodedRecord,
} from "./reader.ts";
import { Reader } from "../reader.ts";

const RECORD_BATCH_PREAMBLE = 12;
const noop = () => {};

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
