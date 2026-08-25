import { KafkaError } from "../errors.ts";
import { isString } from "../type-guards.ts";
import { Cluster } from "./cluster.ts";
import {
  Reader,
  Writer,
  asBytes,
  encodeRecordBatch,
  murmur2,
  type WireRecord,
} from "./protocol.ts";
import {
  API_ADD_OFFSETS_TO_TXN,
  API_ADD_PARTITIONS_TO_TXN,
  API_END_TXN,
  API_INIT_PRODUCER_ID,
  API_PRODUCE,
  API_TXN_OFFSET_COMMIT,
  kafkaError,
  partitionKey,
  retryDelay,
  type CommittedOffset,
  type KafkaOptions,
  type TopicMetadata,
} from "./shared.ts";

export interface ProducerMessage extends WireRecord {
  partition?: number;
  /** Invoked exactly once when the broker acknowledges this message (or it fails for good). */
  onDelivery?: (error: KafkaError | null, result: ProduceResult | null) => void;
}

export type PartitionerContext = {
  topic: string;
  partitionCount: number;
  key: Uint8Array | null;
};

/** Chooses the destination partition when a message has no explicit partition. */
export type Partitioner = (context: PartitionerContext) => number;

export interface ProducerSend {
  topic: string;
  messages: readonly ProducerMessage[];
  acks?: 0 | 1 | "all";
  timeoutMs?: number;
  /** Overrides the producer-level compression for this request. */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
}

export interface ProducerBatch {
  topicMessages: readonly ProducerSend[];
}

export interface ProduceResult {
  topic: string;
  partition: number;
  baseOffset: bigint;
  logAppendTime: bigint;
}

type PartitionRecords = {
  topic: string;
  partition: number;
  leader: number;
  records: ProducerMessage[];
  messages: ProducerMessage[];
};

export interface ProducerOptions {
  /** Time to collect concurrent sends into one Produce request. Default 5 ms. */
  lingerMs?: number;
  /** Flush immediately at this queued message count. Default 1,000. */
  batchMaxMessages?: number;
  /** Record-batch compression through Bun primitives. */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  /** Use broker sequence numbers to make retries duplicate-safe. */
  idempotent?: boolean;
  /** Custom partition selection for messages without an explicit partition. */
  partitioner?: Partitioner;
  /** Enable transactions with this id; forces all-replica acknowledgements. */
  transactionalId?: string;
  /** Broker-side transaction timeout. Default 60,000 ms. */
  transactionTimeoutMs?: number;
}

type PendingSend = {
  input: ProducerSend;
  resolve: (results: ProduceResult[]) => void;
  reject: (error: Error) => void;
};

export class BunProducer {
  #cluster: Cluster;
  #roundRobin = new Map<string, number>();
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;
  #options: Required<
    Omit<ProducerOptions, "partitioner" | "transactionalId" | "transactionTimeoutMs">
  >;
  #pending: PendingSend[] = [];
  #producer?: { id: bigint; epoch: number };
  #sequences = new Map<string, number>();
  #producerPartitioner?: Partitioner;
  #transactionalId?: string;
  #transactionTimeoutMs: number;
  #txnOpen = false;
  #txnAddedPartitions = new Set<string>();
  #txnCoordinator?: number;
  #queuedMessages = 0;
  #timer?: ReturnType<typeof setTimeout>;
  #flushing?: Promise<void>;

  constructor(
    options: KafkaOptions | Cluster,
    producerOptions: ProducerOptions = {},
    onClose = () => {},
  ) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = options instanceof Cluster ? options : new Cluster(options);
    this.#options = {
      lingerMs: producerOptions.lingerMs ?? 5,
      batchMaxMessages: producerOptions.batchMaxMessages ?? 1_000,
      compression: producerOptions.compression ?? "none",
      idempotent: producerOptions.idempotent ?? false,
    };
    if (
      !Number.isFinite(this.#options.lingerMs) ||
      this.#options.lingerMs < 0 ||
      !Number.isSafeInteger(this.#options.batchMaxMessages) ||
      this.#options.batchMaxMessages < 1 ||
      !(this.#options.compression in { none: 1, gzip: 1, snappy: 1, lz4: 1, zstd: 1 })
    ) {
      throw new RangeError("Invalid producer batching options");
    }
    this.#onClose = onClose;
    this.#producerPartitioner = producerOptions.partitioner;
    this.#transactionalId = producerOptions.transactionalId;
    this.#transactionTimeoutMs = producerOptions.transactionTimeoutMs ?? 60_000;
  }

  send(input: ProducerSend): Promise<ProduceResult[]> {
    this.#open();
    if (!input.topic) throw new TypeError("Kafka topic is required");
    if (input.acks === 0 && (this.#options.idempotent || this.#transactionalId)) {
      throw new TypeError(
        "A transactional or idempotent producer requires acknowledged Produce requests",
      );
    }
    if (!input.messages.length) return Promise.resolve([]);
    return new Promise((resolve, reject) => {
      this.#pending.push({ input, resolve, reject });
      this.#queuedMessages += input.messages.length;
      if (this.#queuedMessages >= this.#options.batchMaxMessages || this.#options.lingerMs === 0) {
        queueMicrotask(() => void this.flush().catch(() => {}));
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
      if (this.#timer) clearTimeout(this.#timer);
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
    if (!this.#transactionalId)
      throw new Error("beginTransaction requires a transactionalId producer option");
    if (this.#txnOpen) throw new Error("A transaction is already in progress");
    if (!this.#producer) await this.initProducerId();
    this.#txnOpen = true;
    this.#txnAddedPartitions.clear();
  }

  /** Flush queued messages and commit the open transaction. */
  async commitTransaction(): Promise<void> {
    await this.#endTxn(true, `Commit transaction ${this.#transactionalId}`);
    await this.endTxnCleanup();
  }

  /** Flush queued messages and abort the open transaction; aborted records are invisible to consumers. */
  async abortTransaction(): Promise<void> {
    await this.#endTxn(false, `Abort transaction ${this.#transactionalId}`);
    await this.endTxnCleanup();
  }

  async #endTxn(committed: boolean, label: string): Promise<void> {
    this.#open();
    if (!this.#transactionalId || !this.#txnOpen) throw new Error("No transaction is in progress");
    await this.flush();
    const body = new Writer()
      .string(this.#transactionalId)
      .i64(this.#producer!.id)
      .i16(this.#producer!.epoch)
      .bool(committed);
    // EndTxn v1: same wire shape as v0 but its response carries
    // throttle_time_ms on every broker implementation.
    const response = await this.#txnCoordinatorRequest(API_END_TXN, 1, body);
    this.#cluster.throttle(API_END_TXN, response.i32());
    const error = response.i16();
    if (error) throw kafkaError(error, label);
  }

  async endTxnCleanup(): Promise<void> {
    this.#txnOpen = false;
    this.#txnAddedPartitions.clear();
    // KIP-360: bump the producer epoch after a completed transaction so
    // subsequent transactions cannot be poisoned by zombie state.
    await this.initProducerId();
  }

  /** Commit consumer offsets inside the open transaction. */
  async sendOffsetsToTransaction(
    offsets: readonly CommittedOffset[],
    groupId: string,
  ): Promise<void> {
    this.#open();
    if (!this.#transactionalId || !this.#txnOpen)
      throw new Error("sendOffsetsToTransaction requires an open transaction");
    if (!offsets.length) return;
    const topics = Map.groupBy(offsets, (o) => o.topic);
    const body = new Writer()
      .string(this.#transactionalId)
      .string(groupId)
      .i64(this.#producer!.id)
      .i16(this.#producer!.epoch)
      .array([...topics], (writer, [topicName, values]) =>
        writer
          .string(topicName)
          .array(values, (partitionWriter, value) =>
            partitionWriter.i32(value.partition).i64(value.offset).string(null),
          ),
      );
    const addOffsetsBody = new Writer()
      .string(this.#transactionalId)
      .i64(this.#producer!.id)
      .i16(this.#producer!.epoch)
      .string(groupId);
    const addOffsetsResponse = await this.#txnCoordinatorRequest(
      API_ADD_OFFSETS_TO_TXN,
      0,
      addOffsetsBody,
    );
    this.#cluster.throttle(API_ADD_OFFSETS_TO_TXN, addOffsetsResponse.i32());
    const addOffsetsError = addOffsetsResponse.i16();
    if (addOffsetsError) throw kafkaError(addOffsetsError, `AddOffsetsToTxn group ${groupId}`);
    const response = await this.#txnCoordinatorRequest(API_TXN_OFFSET_COMMIT, 0, body);
    this.#cluster.throttle(API_TXN_OFFSET_COMMIT, response.i32());
    for (const result of response.array((reader) => ({
      topic: reader.string() ?? "",
      partitions: reader.array((p) => ({ index: p.i32(), error: p.i16() })),
    }))) {
      for (const partition of result.partitions)
        if (partition.error)
          throw kafkaError(partition.error, `${result.topic}[${partition.index}]`);
    }
  }

  async #txnCoordinatorRequest(apiKey: number, apiVersion: number, body: Writer): Promise<Reader> {
    if (this.#transactionalId) {
      if (this.#txnCoordinator === undefined) {
        this.#txnCoordinator = await this.#cluster.findTxnCoordinator(this.#transactionalId);
      }
      return this.#cluster.request(this.#txnCoordinator, apiKey, apiVersion, body);
    }
    return this.#cluster.anyRequest(apiKey, apiVersion, body);
  }

  /** Register newly touched partitions of the open transaction with the coordinator. */
  async #addPartitionsToTxn(partitions: PartitionRecords[]): Promise<void> {
    if (!this.#transactionalId || !this.#txnOpen || !this.#producer) return;
    const fresh = partitions.filter(
      (group) => !this.#txnAddedPartitions.has(partitionKey(group.topic, group.partition)),
    );
    if (!fresh.length) return;
    const byTopic = Map.groupBy(fresh, (group) => group.topic);
    const response = await this.#txnCoordinatorRequest(
      API_ADD_PARTITIONS_TO_TXN,
      1,
      new Writer()
        .string(this.#transactionalId)
        .i64(this.#producer.id)
        .i16(this.#producer.epoch)
        .array([...byTopic], (writer, [name, groups]) =>
          writer
            .string(name)
            .array(groups, (partitionWriter, g) => partitionWriter.i32(g.partition)),
        ),
    );
    this.#cluster.throttle(API_ADD_PARTITIONS_TO_TXN, response.i32());
    for (const topic of response.array((reader) => ({
      name: reader.string() ?? "",
      partitions: reader.array((p) => ({ index: p.i32(), error: p.i16() })),
    }))) {
      for (const partition of topic.partitions)
        if (partition.error)
          throw kafkaError(partition.error, `AddPartitionsToTxn ${topic.name}[${partition.index}]`);
    }
    for (const group of fresh)
      this.#txnAddedPartitions.add(partitionKey(group.topic, group.partition));
  }

  async initProducerId(): Promise<void> {
    // Transactional producers must resolve their coordinator first: serving
    // FindCoordinator(key_type=transaction) is what makes brokers create the
    // internal coordinator topic on demand. The resolved broker also receives
    // every subsequent coordinator request (AddPartitionsToTxn, EndTxn, ...).
    if (this.#transactionalId) {
      this.#txnCoordinator = await this.#cluster.findTxnCoordinator(this.#transactionalId);
    }

    let lastError: unknown;
    const { maxRetries, initialBackoffMs, maxBackoffMs } = this.#cluster.retryOptions;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      const body = new Writer()
        .string(this.#transactionalId ?? null)
        .i32(this.#transactionTimeoutMs);
      const response = await this.#txnCoordinatorRequest(API_INIT_PRODUCER_ID, 1, body);
      this.#cluster.throttle(API_INIT_PRODUCER_ID, response.i32());
      const error = response.i16();
      if (!error) {
        this.#producer = { id: response.i64(), epoch: response.i16() };
        this.#sequences.clear();
        return;
      }
      lastError = kafkaError(error, "Initialize idempotent producer");
      // Fresh transactional ids can briefly answer NOT_COORDINATOR while the
      // coordinator is being elected; retry those like transport failures.
      if (!(lastError instanceof KafkaError && lastError.retriable) || attempt === maxRetries)
        break;
      const delay = Math.round(
        Math.min(maxBackoffMs, initialBackoffMs * 2 ** attempt) * (0.5 + Math.random()),
      );
      this.#cluster.log("warn", `retrying InitProducerId attempt ${attempt + 1} in ${delay}ms`);
      this.#cluster.event({
        type: "retry",
        apiKey: API_INIT_PRODUCER_ID,
        attempt: attempt + 1,
        delayMs: delay,
        error: lastError,
      });
      await Bun.sleep(delay);
    }
    throw lastError;
  }

  async #flushPending(pending: PendingSend[]): Promise<void> {
    const notified = new Set<NonNullable<ProducerMessage["onDelivery"]>>();
    const notifyFailure = (error: Error) => {
      for (const { input } of pending) {
        for (const message of input.messages) {
          if (!message.onDelivery || notified.has(message.onDelivery)) continue;
          notified.add(message.onDelivery);
          try {
            message.onDelivery(
              error instanceof KafkaError ? error : new KafkaError(-1, String(error)),
              null,
            );
          } catch {
            /* Delivery callbacks must not break flushing. */
          }
        }
      }
    };
    try {
      if ((this.#options.idempotent || this.#transactionalId) && !this.#producer) {
        await this.initProducerId();
      }

      const configs = Map.groupBy(
        pending,
        ({ input }) =>
          `${input.acks ?? 1}\0${input.timeoutMs ?? 30_000}\0${input.compression ?? this.#options.compression}`,
      );
      for (const group of configs.values()) {
        const topics = Map.groupBy(group, ({ input }) => input.topic);
        const first = group[0]!.input;
        const compression = first.compression ?? this.#options.compression;
        let results: ProduceResult[] | undefined;
        let lastError: unknown;
        let routedPartitions: PartitionRecords[] = [];
        for (let attempt = 0; attempt <= this.#cluster.retryOptions.maxRetries; attempt++) {
          try {
            routedPartitions = (
              await Promise.all(
                [...topics].map(async ([topic, sends]) => {
                  const messages = sends.flatMap(({ input }) => input.messages);
                  return this.#route(
                    topic,
                    messages,
                    sends[0]!.input.timeoutMs ?? 30_000,
                    attempt > 0,
                  );
                }),
              )
            ).flat();
            await this.#addPartitionsToTxn(routedPartitions);
            if ((first.acks ?? 1) === 0) {
              // Fire-and-forget: brokers never answer acks=0 Produce requests.
              const leaders = Map.groupBy(routedPartitions, (partition) => partition.leader);
              await Promise.all(
                [...leaders].map(([leader, leaderPartitions]) =>
                  this.#cluster.fireAndForget(
                    leader,
                    API_PRODUCE,
                    3,
                    this.#produceRequestBody(
                      leaderPartitions,
                      0,
                      first.timeoutMs ?? 30_000,
                      compression,
                    ),
                  ),
                ),
              );
              results = routedPartitions.map((group) => ({
                topic: group.topic,
                partition: group.partition,
                baseOffset: -1n,
                logAppendTime: -1n,
              }));
            } else {
              const acks =
                this.#options.idempotent || this.#transactionalId || first.acks === "all" ? -1 : 1;
              results = await this.#produce(
                routedPartitions,
                acks,
                first.timeoutMs ?? 30_000,
                compression,
              );
            }
            break;
          } catch (error) {
            lastError = error;
            if (
              !(error instanceof KafkaError && error.retriable) ||
              attempt === this.#cluster.retryOptions.maxRetries
            )
              throw error;
            this.#cluster.bumpRetries();
            const delay = retryDelay(this.#cluster.retryOptions, attempt);
            this.#cluster.log(
              "warn",
              `retrying produce attempt ${attempt + 1} in ${delay}ms: ${String(error)}`,
            );
            this.#cluster.event({
              type: "retry",
              apiKey: API_PRODUCE,
              attempt: attempt + 1,
              delayMs: delay,
              error,
            });
            if (delay) await Bun.sleep(delay);
          }
        }
        if (!results)
          throw lastError ?? new KafkaError(-1, "Kafka produce failed", { retriable: true });
        const byTopic = Map.groupBy(results, (result) => result.topic);
        for (const item of group) item.resolve(byTopic.get(item.input.topic) ?? []);
        // Per-message delivery callbacks use the authoritative partition routing.
        const byPartition = new Map(
          results.map((result) => [partitionKey(result.topic, result.partition), result]),
        );
        for (const routed of routedPartitions) {
          const result = byPartition.get(partitionKey(routed.topic, routed.partition));
          if (!result) continue;
          for (const message of routed.messages) {
            if (!message.onDelivery || notified.has(message.onDelivery)) continue;
            notified.add(message.onDelivery);
            try {
              message.onDelivery(null, result);
            } catch {
              /* Delivery callbacks must not break flushing. */
            }
          }
        }
      }
    } catch (error) {
      notifyFailure(error instanceof Error ? error : new Error(String(error)));
      for (const item of pending)
        item.reject(error instanceof Error ? error : new Error(String(error)));
      throw error;
    }
  }

  async #route(
    topic: string,
    messages: readonly ProducerMessage[],
    timeoutMs: number,
    refresh = false,
  ): Promise<PartitionRecords[]> {
    let metadata: TopicMetadata | undefined;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      metadata = await this.#cluster.topic(topic, refresh || !!metadata);
      if (!metadata.err && metadata.partitions.length) break;
      if (metadata.err !== 3 && metadata.err !== 5) throw kafkaError(metadata.err, topic);
      await Bun.sleep(10);
    }
    if (!metadata?.partitions.length || metadata.err) throw kafkaError(metadata?.err ?? 3, topic);

    const partitionMetadata = new Map(
      metadata.partitions.map((partition) => [partition.id, partition]),
    );
    const partitions = new Map<number, PartitionRecords>();
    for (const message of messages) {
      const key = asBytes(message.key);
      let partition = message.partition;
      if (partition === undefined) {
        if (this.#producerPartitioner) {
          const chosen = this.#producerPartitioner({
            topic,
            partitionCount: metadata.partitions.length,
            key,
          });
          if (!Number.isInteger(chosen) || chosen < 0 || chosen >= metadata.partitions.length) {
            throw new RangeError(
              `Custom partitioner returned invalid partition ${chosen} for ${topic}`,
            );
          }
          partition = chosen;
        } else if (key) partition = (murmur2(key) & 0x7fffffff) % metadata.partitions.length;
        else {
          partition = this.#roundRobin.get(topic) ?? 0;
          this.#roundRobin.set(topic, (partition + 1) % metadata.partitions.length);
        }
      }
      const meta = partitionMetadata.get(partition);
      if (!meta) throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
      if (meta.err) throw kafkaError(meta.err, `${topic}[${partition}]`);
      let group = partitions.get(partition);
      if (!group) {
        group = { topic, partition, leader: meta.leader, records: [], messages: [] };
        partitions.set(partition, group);
      }
      group.records.push(key && isString(message.key) ? { ...message, key } : message);
      group.messages.push(message);
    }
    return [...partitions.values()];
  }

  #produceRequestBody(
    leaderPartitions: PartitionRecords[],
    acks: number,
    timeoutMs: number,
    compression = this.#options.compression,
  ): Writer {
    const topics = Map.groupBy(leaderPartitions, (partition) => partition.topic);
    // Produce v3+: brokers reject transactional batches whose request omits the
    // matching transactional_id, so it must ride along on every produce.
    return new Writer()
      .string(this.#transactionalId ?? null)
      .i16(acks)
      .i32(timeoutMs)
      .array([...topics], (writer, [topic, topicPartitions]) => {
        writer.string(topic).array(topicPartitions, (partitionWriter, value) => {
          const key = partitionKey(value.topic, value.partition);
          const producer = this.#producer && {
            ...this.#producer,
            sequence: this.#sequences.get(key) ?? 0,
            transactional: Boolean(this.#transactionalId && this.#txnOpen),
          };
          partitionWriter
            .i32(value.partition)
            .bytes(
              encodeRecordBatch(
                value.records,
                Date.now(),
                compression ?? this.#options.compression,
                producer,
              ),
            );
        });
      });
  }

  async #produce(
    partitions: PartitionRecords[],
    acks: number,
    timeoutMs: number,
    compression?: ProducerOptions["compression"],
  ): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    const responses = await Promise.all(
      [...leaders].map(async ([leader, leaderPartitions]) => {
        const body = this.#produceRequestBody(leaderPartitions, acks, timeoutMs, compression);
        const response = await this.#cluster.request(
          leader,
          API_PRODUCE,
          3,
          body,
          timeoutMs,
          false,
        );
        const results = response
          .array((topicReader) => {
            const topic = topicReader.string() ?? "";
            return topicReader.array((partitionReader) => {
              const partition = partitionReader.i32();
              const error = partitionReader.i16();
              const baseOffset = partitionReader.i64();
              const logAppendTime = partitionReader.i64();
              if (error) throw kafkaError(error, `${topic}[${partition}]`);
              return { topic, partition, baseOffset, logAppendTime };
            });
          })
          .flat();
        this.#cluster.throttle(API_PRODUCE, response.i32());
        return results;
      }),
    );
    const results = responses.flat();
    if (this.#producer) {
      for (const partition of partitions) {
        const key = partitionKey(partition.topic, partition.partition);
        this.#sequences.set(
          key,
          ((this.#sequences.get(key) ?? 0) + partition.records.length) % 0x80000000,
        );
      }
    }
    return results;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    await this.flush();
    if (this.#txnOpen) {
      try {
        await this.abortTransaction();
      } catch {
        /* Best-effort abort during shutdown. */
      }
    }
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> {
    return this.close();
  }
  #open(): void {
    if (this.#closed) throw new Error("Producer is closed");
  }
}
