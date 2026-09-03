import { Cluster } from "../bun/cluster.ts";
import { Producer } from "../bun/producer/index.ts";
import { isNumber, isString } from "../type-guards.ts";
import { COMPRESSION_NAMES, PRODUCER_EVENTS, CompressionTypes } from "./constants.ts";
import { wrapError, KafkaJSNonRetriableError } from "./errors.ts";
import type { ClusterGetter } from "./config.ts";
import { Emitter, Logger, observeRequests } from "./logger.ts";
import type { CompatOptions, LogFields } from "./types.ts";
import {
  toBunPartitioner,
  toWireMessage,
  type KafkaJsSendRecord,
  type KafkaJsSendBatchRecord,
  type KafkaJsPartitioner,
} from "./messages.ts";

function producerOptions(options: CompatOptions) {
  const compressionCode = Number(options.compression ?? CompressionTypes.None);
  return {
    lingerMs: 5,
    compression: COMPRESSION_NAMES[compressionCode] ?? "none",
    idempotent: Boolean(options.idempotent),
    transactionalId: isString(options.transactionalId) ? options.transactionalId : undefined,
    transactionTimeoutMs: isNumber(options.transactionTimeout)
      ? options.transactionTimeout
      : undefined,
    partitioner: toBunPartitioner(
      (options.createPartitioner ?? options.partitioner) as KafkaJsPartitioner | null | undefined,
    ),
  };
}

function acksToWire(acks: number | undefined): 0 | 1 | "all" {
  if (acks === 0) {
    return 0;
  }
  if (acks === 1) {
    return 1;
  }
  return "all";
}

export class CompatProducer {
  events = PRODUCER_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #options: CompatOptions;
  #emitter = new Emitter();
  #producer?: Producer;
  #transaction?: Producer;

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
  #underlying(): Producer {
    this.#producer ??= new Producer(
      this.#getter().acquire(),
      producerOptions(this.#options),
      this.#getter().release,
    );
    return this.#producer;
  }

  #observe<T>(request: () => T): T {
    return observeRequests(this.#getter().sync(), this.#emitter, PRODUCER_EVENTS, request);
  }

  #emitQueueSize(producer: Producer): void {
    this.#emitter.emit(PRODUCER_EVENTS.REQUEST_QUEUE_SIZE, {
      queueSize: producer.queuedMessages,
    });
  }

  async send({ topic, messages, acks, timeout, compression }: KafkaJsSendRecord): Promise<
    Array<{
      topicName: string;
      partition: number;
      errorCode: number;
      baseOffset: string;
      logAppendTime: string;
    }>
  > {
    try {
      if (!messages.length) {
        return [];
      }
      const producer = this.#underlying();
      const pending = this.#observe(() =>
        producer.send({
          topic,
          messages: messages.map(toWireMessage),
          acks: acksToWire(acks),
          timeoutMs: timeout,
          compression: COMPRESSION_NAMES[Number(compression)] ?? undefined,
        }),
      );
      this.#emitQueueSize(producer);
      const results = await pending;
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

  async sendBatch({ topicMessages, acks, timeout, compression }: KafkaJsSendBatchRecord): Promise<
    Array<{
      topicName: string;
      partition: number;
      errorCode: number;
      baseOffset: string;
      logAppendTime: string;
    }>
  > {
    try {
      if (!topicMessages.some((item) => item.messages.length)) {
        return [];
      }
      const producer = this.#underlying();
      const compressionName = COMPRESSION_NAMES[Number(compression)] ?? undefined;
      const pending = this.#observe(async () => {
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
        return results;
      });
      this.#emitQueueSize(producer);
      const results = await pending;
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
      this.#transaction = new Producer(
        this.#getter().acquire(),
        {
          ...producerOptions(this.#options),
          idempotent: true,
          lingerMs: 0,
        },
        this.#getter().release,
      );
    }
    const transaction = this.#transaction;
    await this.#observe(() => transaction.beginTransaction());
    return new CompatTransaction(transaction, this.#getter().sync(), this.#emitter);
  }

  isIdempotent(): boolean {
    return Boolean(this.#options.idempotent) || Boolean(this.#options.transactionalId);
  }
}

export class CompatTransaction {
  constructor(
    private producer: Producer,
    private cluster: Cluster,
    private emitter: Emitter,
  ) {}

  #observe<T>(request: () => T): T {
    return observeRequests(this.cluster, this.emitter, PRODUCER_EVENTS, request);
  }

  #emitQueueSize(): void {
    this.emitter.emit(PRODUCER_EVENTS.REQUEST_QUEUE_SIZE, {
      queueSize: this.producer.queuedMessages,
    });
  }
  async send(record: KafkaJsSendRecord): Promise<void> {
    try {
      if (!record.messages.length) {
        return;
      }
      const pending = this.#observe(() =>
        this.producer.send({
          topic: record.topic,
          messages: record.messages.map(toWireMessage),
          acks: "all",
          timeoutMs: record.timeout,
        }),
      );
      this.#emitQueueSize();
      await pending;
    } catch (error) {
      throw wrapError(error);
    }
  }
  async sendBatch({ topicMessages }: KafkaJsSendBatchRecord): Promise<void> {
    try {
      const nonEmpty = topicMessages.filter((item) => item.messages.length);
      if (!nonEmpty.length) {
        return;
      }
      const pending = this.#observe(async () => {
        for (const item of nonEmpty) {
          await this.producer.send({
            topic: item.topic,
            messages: item.messages.map(toWireMessage),
            acks: "all",
          });
        }
        await this.producer.flush();
      });
      this.#emitQueueSize();
      await pending;
    } catch (error) {
      throw wrapError(error);
    }
  }
  async commit(): Promise<void> {
    try {
      await this.#observe(() => this.producer.commitTransaction());
    } catch (error) {
      throw wrapError(error);
    }
  }
  async abort(): Promise<void> {
    try {
      await this.#observe(() => this.producer.abortTransaction());
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
      await this.#observe(() => this.producer.sendOffsetsToTransaction(flat, consumerGroupId));
    } catch (error) {
      throw wrapError(error);
    }
  }
  isActive(): boolean {
    return true;
  }
}
