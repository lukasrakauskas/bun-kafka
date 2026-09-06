import { isFunction } from "../../type-guards.ts";
import { Cluster } from "../cluster.ts";
import {
  DEFAULT_PRODUCE_BATCH_MAX_MESSAGES,
  DEFAULT_PRODUCE_LINGER_MS,
  DEFAULT_PRODUCE_TIMEOUT_MS,
  type CommittedOffset,
  type KafkaOptions,
} from "../shared.ts";
import { notifyDeliveryFailures, notifyDeliverySuccess } from "./delivery.ts";
import { ProducerSender } from "./sender.ts";
import { ProducerTransaction } from "./transaction.ts";
import type {
  Partitioner,
  PendingSend,
  ProducerBatch,
  ProducerMessage,
  ProducerOptions,
  ProduceResult,
  ProducerSend,
  ResolvedProducerOptions,
} from "./types.ts";

export type {
  Partitioner,
  PartitionerContext,
  ProducerBatch,
  ProducerMessage,
  ProducerOptions,
  ProduceResult,
  ProducerSend,
} from "./types.ts";

export class Producer {
  #cluster: Cluster;
  #roundRobin = new Map<string, number>();
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;
  #options: ResolvedProducerOptions;
  #pending: PendingSend[] = [];
  #sequences = new Map<string, number>();
  #producerPartitioner?: Partitioner;
  #transaction: ProducerTransaction;
  #queuedMessages = 0;
  #timer?: ReturnType<typeof setTimeout>;
  #flushing?: Promise<void>;
  #flushScheduled = false;
  #sender: ProducerSender;

  constructor(
    options: KafkaOptions | Cluster,
    producerOptions: ProducerOptions = {},
    onClose = () => {},
  ) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = options instanceof Cluster ? options : new Cluster(options);
    this.#options = {
      lingerMs: producerOptions.lingerMs ?? DEFAULT_PRODUCE_LINGER_MS,
      batchMaxMessages: producerOptions.batchMaxMessages ?? DEFAULT_PRODUCE_BATCH_MAX_MESSAGES,
      compression: producerOptions.compression ?? "none",
      idempotent: producerOptions.idempotent ?? false,
    };
    if (
      !Number.isFinite(this.#options.lingerMs) ||
      this.#options.lingerMs < 0 ||
      !Number.isSafeInteger(this.#options.batchMaxMessages) ||
      this.#options.batchMaxMessages < 1 ||
      !(this.#options.compression in { none: 1, gzip: 1, snappy: 1, lz4: 1, zstd: 1 }) ||
      (producerOptions.partitioner !== undefined && !isFunction(producerOptions.partitioner))
    ) {
      throw new RangeError("Invalid producer batching options");
    }
    this.#onClose = onClose;
    this.#producerPartitioner = producerOptions.partitioner;
    this.#transaction = new ProducerTransaction(this.#cluster, {
      transactionalId: producerOptions.transactionalId,
      transactionTimeoutMs: producerOptions.transactionTimeoutMs,
      onProducerId: () => this.#sequences.clear(),
    });
    this.#sender = new ProducerSender(
      this.#cluster,
      this.#options,
      this.#transaction,
      this.#roundRobin,
      this.#sequences,
      this.#producerPartitioner,
    );
  }

  send(input: ProducerSend): Promise<ProduceResult[]> {
    this.#open();
    if (!input.topic) {
      throw new TypeError("Kafka topic is required");
    }
    if (input.acks === 0 && (this.#options.idempotent || this.#transaction.isTransactional)) {
      throw new TypeError(
        "A transactional or idempotent producer requires acknowledged Produce requests",
      );
    }
    if (!input.messages.length) {
      return Promise.resolve([]);
    }
    return new Promise((resolve, reject) => {
      this.#pending.push({ input, resolve, reject });
      this.#queuedMessages += input.messages.length;
      if (this.#queuedMessages >= this.#options.batchMaxMessages || this.#options.lingerMs === 0) {
        if (!this.#flushScheduled) {
          this.#flushScheduled = true;
          queueMicrotask(() => {
            this.#flushScheduled = false;
            void this.flush().catch(() => {});
          });
        }
      } else if (!this.#timer) {
        this.#timer = setTimeout(() => void this.flush().catch(() => {}), this.#options.lingerMs);
      }
    });
  }

  async sendBatch(input: ProducerBatch): Promise<ProduceResult[]> {
    return (await Promise.all(input.topicMessages.map((batch) => this.send(batch)))).flat();
  }

  /** Messages queued in memory that have not been acknowledged yet. */
  get queuedMessages(): number {
    return this.#queuedMessages;
  }

  async flush(): Promise<void> {
    while (this.#flushing || this.#pending.length) {
      if (this.#flushing) {
        await this.#flushing;
        continue;
      }
      if (this.#timer) {
        clearTimeout(this.#timer);
      }
      this.#timer = undefined;
      const pending = this.#pending.splice(0);
      this.#queuedMessages = 0;
      this.#flushing = this.#flushPending(pending).finally(() => {
        this.#flushing = undefined;
      });
      await this.#flushing;
    }
  }

  /**
   * Initialize the producer identity and start a transaction.
   * Messages sent afterwards are not visible to consumers until commitTransaction().
   */
  async beginTransaction(): Promise<void> {
    this.#open();
    await this.#transaction.begin();
  }

  /** Flush queued messages and commit the open transaction. */
  async commitTransaction(): Promise<void> {
    this.#open();
    await this.#transaction.commit(() => this.flush());
  }

  /** Flush queued messages and abort the open transaction; aborted records are invisible to consumers. */
  async abortTransaction(): Promise<void> {
    this.#open();
    await this.#transaction.abort(() => this.flush());
  }

  async endTxnCleanup(): Promise<void> {
    await this.#transaction.endTxnCleanup();
  }

  /** Commit consumer offsets inside the open transaction. */
  async sendOffsetsToTransaction(
    offsets: readonly CommittedOffset[],
    groupId: string,
  ): Promise<void> {
    this.#open();
    await this.#transaction.sendOffsetsToTransaction(offsets, groupId);
  }

  async initProducerId(): Promise<void> {
    await this.#transaction.initProducerId();
  }

  async #flushPending(pending: PendingSend[]): Promise<void> {
    const notified = new Set<NonNullable<ProducerMessage["onDelivery"]>>();
    try {
      if (
        (this.#options.idempotent || this.#transaction.isTransactional) &&
        !this.#transaction.producer
      ) {
        await this.initProducerId();
      }
      const groups = Map.groupBy(
        pending,
        ({ input }) =>
          `${input.acks ?? 1}\0${input.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS}\0${input.compression ?? this.#options.compression}`,
      );
      for (const group of groups.values()) {
        await this.#flushPendingGroup(group, notified);
      }
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      notifyDeliveryFailures(pending, failure, notified);
      for (const item of pending) {
        item.reject(failure);
      }
      throw error;
    }
  }

  async #flushPendingGroup(
    group: PendingSend[],
    notified: Set<NonNullable<ProducerMessage["onDelivery"]>>,
  ): Promise<void> {
    const { results, routedPartitions } = await this.#sender.producePendingGroup(group);
    const byTopic = Map.groupBy(results, (result) => result.topic);
    for (const item of group) {
      item.resolve(byTopic.get(item.input.topic) ?? []);
    }
    notifyDeliverySuccess(routedPartitions, results, notified);
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    await this.flush();
    if (this.#transaction.isOpen) {
      try {
        await this.abortTransaction();
      } catch {
        /* Best-effort abort during shutdown. */
      }
    }
    this.#closed = true;
    if (this.#ownsCluster) {
      this.#cluster.close();
    }
    this.#onClose();
  }

  disconnect(): Promise<void> {
    return this.close();
  }
  #open(): void {
    if (this.#closed) {
      throw new Error("Producer is closed");
    }
  }
}
