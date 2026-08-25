import { BunProducer } from "../bun/producer.ts";
import { COMPRESSION_NAMES, PRODUCER_EVENTS, CompressionTypes } from "./constants.ts";
import { wrapError, KafkaJSNonRetriableError } from "./errors.ts";
import type { ClusterGetter } from "./config.ts";
import { Emitter, Logger } from "./logger.ts";
import type { CompatOptions, LogFields } from "./types.ts";
import {
  toBunPartitioner,
  toWireMessage,
  type KafkaJsSendRecord,
  type KafkaJsSendBatchRecord,
} from "./messages.ts";

function producerOptions(options: CompatOptions) {
  const compressionCode = Number(options.compression ?? CompressionTypes.None);
  return {
    lingerMs: 5,
    compression: COMPRESSION_NAMES[compressionCode] ?? "none",
    idempotent: Boolean(options.idempotent),
// SAFETY: the surrounding protocol invariant validates this representation.
    transactionalId: options.transactionalId as string | undefined,
// SAFETY: the surrounding protocol invariant validates this representation.
    transactionTimeoutMs: options.transactionTimeout as number | undefined,
    partitioner: toBunPartitioner(options.createPartitioner ?? options.partitioner),
  };
}

function acksToWire(acks: number | undefined): 0 | 1 | "all" {
  if (acks === undefined || acks === -1) return "all";
// SAFETY: the surrounding protocol invariant validates this representation.
  return acks as 0 | 1;
}

export class CompatProducer {
  events = PRODUCER_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #options: CompatOptions;
  #emitter = new Emitter();
  #producer?: BunProducer;
  #transaction?: BunProducer;

  constructor(getter: () => ClusterGetter, logger: Logger, options: CompatOptions) {
    this.#getter = getter;
    this.#logger = logger;
    this.#options = options;
  }

  on(event: string, listener: (event: LogFields) => void): () => void {
    return this.#emitter.on(event, listener);
  }

  logger(): Logger {
    return this.#logger;
  }

  get queuedMessages(): number {
    return this.#producer?.queuedMessages ?? 0;
  }

  async connect(): Promise<void> {
    await this.#getter().ready();
    this.#emitter.emit(PRODUCER_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    if (this.#producer) {
      await this.#producer.close().catch(() => {});
      this.#producer = undefined;
    }
    if (this.#transaction) {
      await this.#transaction.close().catch(() => {});
      this.#transaction = undefined;
    }
    this.#emitter.emit(PRODUCER_EVENTS.DISCONNECT);
  }

  /** Core accepts per-send compression, so one underlying producer covers every codec. */
  #underlying(): BunProducer {
    this.#producer ??= new BunProducer(
      this.#getter().acquire(),
      producerOptions(this.#options),
      this.#getter().release,
    );
    return this.#producer!;
  }

  async send({
    topic,
    messages,
    acks,
    timeout,
    compression,
  }: KafkaJsSendRecord): Promise<
    Array<{
      topicName: string;
      partition: number;
      errorCode: number;
      baseOffset: string;
      logAppendTime: string;
    }>
  > {
    try {
      if (!messages.length) return [];
      const results = await this.#underlying().send({
        topic,
        messages: messages.map(toWireMessage),
        acks: acksToWire(acks),
        timeoutMs: timeout,
        compression: COMPRESSION_NAMES[Number(compression)] ?? undefined,
      });
      return results.map((result) => ({
        topicName: result.topic,
        partition: result.partition,
        errorCode: 0,
        baseOffset: result.baseOffset.toString(),
        logAppendTime: result.logAppendTime.toString(),
      }));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async sendBatch({
    topicMessages,
    acks,
    timeout,
    compression,
  }: KafkaJsSendBatchRecord): Promise<
    Array<{
      topicName: string;
      partition: number;
      errorCode: number;
      baseOffset: string;
      logAppendTime: string;
    }>
  > {
    try {
      const producer = this.#underlying();
      const compressionName = COMPRESSION_NAMES[Number(compression)] ?? undefined;
      const results = await Promise.all(
        topicMessages
          .filter((item) => item.messages.length)
          .map((item) =>
            producer.send({
              topic: item.topic,
              messages: item.messages.map(toWireMessage),
              acks: acksToWire(acks),
              timeoutMs: timeout,
              compression: compressionName,
            }),
          ),
      );
      await producer.flush();
      return results.flat().map((result) => ({
        topicName: result.topic,
        partition: result.partition,
        errorCode: 0,
        baseOffset: result.baseOffset.toString(),
        logAppendTime: result.logAppendTime.toString(),
      }));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async transaction(): Promise<CompatTransaction> {
    if (!this.#options.transactionalId) {
      throw new KafkaJSNonRetriableError(
        "Cannot use transactions without setting the transactionalId",
      );
    }
    if (!this.#transaction) {
      this.#transaction = new BunProducer(
        this.#getter().acquire(),
        {
          ...producerOptions(this.#options),
          idempotent: true,
          lingerMs: 0,
        },
        this.#getter().release,
      );
    }
    await this.#transaction.beginTransaction();
    return new CompatTransaction(this.#transaction);
  }

  isIdempotent(): boolean {
    return Boolean(this.#options.idempotent) || Boolean(this.#options.transactionalId);
  }
}

export class CompatTransaction {
  #producer: BunProducer;
  constructor(producer: BunProducer) {
    this.#producer = producer;
  }
  async send(record: KafkaJsSendRecord): Promise<void> {
    try {
      await this.#producer.send({
        topic: record.topic,
        messages: record.messages.map(toWireMessage),
        acks: "all",
        timeoutMs: record.timeout,
      });
    } catch (error) {
      throw wrapError(error);
    }
  }
  async sendBatch({ topicMessages }: KafkaJsSendBatchRecord): Promise<void> {
    try {
      for (const item of topicMessages) {
        await this.#producer.send({
          topic: item.topic,
          messages: item.messages.map(toWireMessage),
          acks: "all",
        });
      }
      await this.#producer.flush();
    } catch (error) {
      throw wrapError(error);
    }
  }
  async commit(): Promise<void> {
    try {
      await this.#producer.commitTransaction();
    } catch (error) {
      throw wrapError(error);
    }
  }
  async abort(): Promise<void> {
    try {
      await this.#producer.abortTransaction();
    } catch (error) {
      throw wrapError(error);
    }
  }
  async sendOffsets(
    offsets: Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: string | number | bigint }>;
    }>,
    consumerGroupId: string,
  ): Promise<void> {
    try {
      const flat = offsets.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition, offset }) => ({ topic, partition, offset: BigInt(offset) })),
      );
      await this.#producer.sendOffsetsToTransaction(flat, consumerGroupId);
    } catch (error) {
      throw wrapError(error);
    }
  }
  isActive(): boolean {
    return true;
  }
}
