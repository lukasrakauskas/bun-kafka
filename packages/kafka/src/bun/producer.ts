import { KafkaError, KafkaErrorCode } from "../errors.ts";
import { isFunction, isString, requiredValue } from "../type-guards.ts";
import { Cluster } from "./cluster.ts";
import {
  RequestBody,
  ResponseBody,
  writeEndTransactionRequest,
  readEndTransactionResponse,
  writeAddOffsetsToTransactionRequest,
  writeTransactionOffsetCommitRequest,
  readTransactionOffsetCommitResponse,
  writeAddPartitionsToTransactionRequest,
  readTopicPartitionErrors,
  writeInitProducerIdRequest,
  readInitProducerIdResponse,
  writeProduceRequest,
  readProduceResponse,
  asBytes,
  murmur2,
  type WireRecord,
} from "../protocol/index.ts";
import {
  API_ADD_OFFSETS_TO_TXN,
  API_ADD_PARTITIONS_TO_TXN,
  API_END_TXN,
  API_INIT_PRODUCER_ID,
  API_PRODUCE,
  API_TXN_OFFSET_COMMIT,
  DEFAULT_PRODUCE_BATCH_MAX_MESSAGES,
  METADATA_REFRESH_SLEEP_MS,
  PRODUCE_API_VERSION,
  DEFAULT_PRODUCE_LINGER_MS,
  DEFAULT_PRODUCE_TIMEOUT_MS,
  DEFAULT_TRANSACTION_TIMEOUT_MS,
  INT32_MAX,
  RETRY_JITTER_BASE,
  SEQ_WRAP,
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

function notifyDeliveryCallback(
  callback: NonNullable<ProducerMessage["onDelivery"]>,
  error: KafkaError | null,
  result: ProduceResult | null,
): void {
  try {
    callback(error, result);
  } catch {
    /* Delivery callbacks must not break flushing. */
  }
}

function notifyDeliveryFailures(
  pending: PendingSend[],
  error: Error,
  notified: Set<NonNullable<ProducerMessage["onDelivery"]>>,
): void {
  for (const { input } of pending) {
    for (const message of input.messages) {
      if (!message.onDelivery || notified.has(message.onDelivery)) {
        continue;
      }
      notified.add(message.onDelivery);
      notifyDeliveryCallback(
        message.onDelivery,
        error instanceof KafkaError ? error : new KafkaError(-1, String(error)),
        null,
      );
    }
  }
}

function notifyDeliverySuccess(
  routedPartitions: PartitionRecords[],
  results: ProduceResult[],
  notified: Set<NonNullable<ProducerMessage["onDelivery"]>>,
): void {
  const byPartition = new Map(
    results.map((result) => [partitionKey(result.topic, result.partition), result]),
  );
  for (const routed of routedPartitions) {
    const result = byPartition.get(partitionKey(routed.topic, routed.partition));
    if (!result) {
      continue;
    }
    for (const message of routed.messages) {
      if (!message.onDelivery || notified.has(message.onDelivery)) {
        continue;
      }
      notified.add(message.onDelivery);
      notifyDeliveryCallback(message.onDelivery, null, result);
    }
  }
}

function appendProducerMessage(
  partitions: Map<number, PartitionRecords>,
  metadata: Map<number, TopicMetadata["partitions"][number]>,
  topic: string,
  message: ProducerMessage,
  key: Uint8Array | null,
  partition: number,
): void {
  const meta = metadata.get(partition);
  if (!meta) {
    throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
  }
  if (meta.err) {
    throw kafkaError(meta.err, `${topic}[${partition}]`);
  }
  let group = partitions.get(partition);
  if (!group) {
    group = { topic, partition, leader: meta.leader, records: [], messages: [] };
    partitions.set(partition, group);
  }
  group.records.push(key && isString(message.key) ? { ...message, key } : message);
  group.messages.push(message);
}

function chooseProducerPartition(
  topic: string,
  message: ProducerMessage,
  partitionCount: number,
  partitioner: Partitioner | undefined,
  roundRobin: Map<string, number>,
  key: Uint8Array | null,
): number {
  if (message.partition !== undefined) {
    return message.partition;
  }
  if (partitioner) {
    const partition = partitioner({ topic, partitionCount, key });
    if (!Number.isInteger(partition) || partition < 0 || partition >= partitionCount) {
      throw new RangeError(
        `Custom partitioner returned invalid partition ${partition} for ${topic}`,
      );
    }
    return partition;
  }
  if (key) {
    return (murmur2(key) & INT32_MAX) % partitionCount;
  }
  const partition = roundRobin.get(topic) ?? 0;
  roundRobin.set(topic, (partition + 1) % partitionCount);
  return partition;
}

function topicMetadataReady(metadata: TopicMetadata): boolean {
  return !metadata.err && metadata.partitions.length > 0;
}

function topicMetadataRetryable(metadata: TopicMetadata): boolean {
  return (
    metadata.err === KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION ||
    metadata.err === KafkaErrorCode.LEADER_NOT_AVAILABLE
  );
}

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
    this.#transactionalId = producerOptions.transactionalId;
    this.#transactionTimeoutMs =
      producerOptions.transactionTimeoutMs ?? DEFAULT_TRANSACTION_TIMEOUT_MS;
  }

  send(input: ProducerSend): Promise<ProduceResult[]> {
    this.#open();
    if (!input.topic) {
      throw new TypeError("Kafka topic is required");
    }
    if (input.acks === 0 && (this.#options.idempotent || this.#transactionalId)) {
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
    if (!this.#transactionalId) {
      throw new Error("beginTransaction requires a transactionalId producer option");
    }
    if (this.#txnOpen) {
      throw new Error("A transaction is already in progress");
    }
    if (!this.#producer) {
      await this.initProducerId();
    }
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
    if (!this.#transactionalId || !this.#txnOpen) {
      throw new Error("No transaction is in progress");
    }
    await this.flush();
    const producer = requiredValue(this.#producer, "Producer identity is not initialized");
    const body = writeEndTransactionRequest(
      this.#transactionalId,
      producer.id,
      producer.epoch,
      committed,
    );
    // EndTxn v1: same wire shape as v0 but its response carries
    // throttle_time_ms on every broker implementation.
    const response = await this.#txnCoordinatorRequest(API_END_TXN, 1, body);
    const endTransaction = readEndTransactionResponse(response);
    this.#cluster.throttle(API_END_TXN, endTransaction.throttleMs);
    const { error } = endTransaction;
    if (error) {
      throw kafkaError(error, label);
    }
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
    if (!this.#transactionalId || !this.#txnOpen) {
      throw new Error("sendOffsetsToTransaction requires an open transaction");
    }
    if (!offsets.length) {
      return;
    }
    const producer = requiredValue(this.#producer, "Producer identity is not initialized");
    const topics = Map.groupBy(offsets, (o) => o.topic);
    const body = writeTransactionOffsetCommitRequest(
      this.#transactionalId,
      groupId,
      producer.id,
      producer.epoch,
      topics,
    );
    const addOffsetsBody = writeAddOffsetsToTransactionRequest(
      this.#transactionalId,
      producer.id,
      producer.epoch,
      groupId,
    );
    const addOffsetsResponse = await this.#txnCoordinatorRequest(
      API_ADD_OFFSETS_TO_TXN,
      0,
      addOffsetsBody,
    );
    const addOffsets = readEndTransactionResponse(addOffsetsResponse);
    this.#cluster.throttle(API_ADD_OFFSETS_TO_TXN, addOffsets.throttleMs);
    const addOffsetsError = addOffsets.error;
    if (addOffsetsError) {
      throw kafkaError(addOffsetsError, `AddOffsetsToTxn group ${groupId}`);
    }
    const response = await this.#txnCoordinatorRequest(API_TXN_OFFSET_COMMIT, 0, body);
    const offsetCommit = readTransactionOffsetCommitResponse(response);
    this.#cluster.throttle(API_TXN_OFFSET_COMMIT, offsetCommit.throttleMs);
    for (const result of offsetCommit.topics) {
      for (const partition of result.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `${result.topic}[${partition.index}]`);
        }
      }
    }
  }

  async #txnCoordinatorRequest(
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
  ): Promise<ResponseBody> {
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
    const producer = this.#producer;
    if (!this.#transactionalId || !this.#txnOpen || !producer) {
      return;
    }
    const fresh = partitions.filter(
      (group) => !this.#txnAddedPartitions.has(partitionKey(group.topic, group.partition)),
    );
    if (!fresh.length) {
      return;
    }
    const byTopic = Map.groupBy(fresh, (group) => group.topic);
    const response = await this.#txnCoordinatorRequest(
      API_ADD_PARTITIONS_TO_TXN,
      1,
      writeAddPartitionsToTransactionRequest(
        this.#transactionalId,
        producer.id,
        producer.epoch,
        byTopic,
      ),
    );
    const addedPartitions = readTopicPartitionErrors(response);
    this.#cluster.throttle(API_ADD_PARTITIONS_TO_TXN, addedPartitions.throttleMs);
    for (const topic of addedPartitions.topics) {
      for (const partition of topic.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `AddPartitionsToTxn ${topic.name}[${partition.index}]`);
        }
      }
    }
    for (const group of fresh) {
      this.#txnAddedPartitions.add(partitionKey(group.topic, group.partition));
    }
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
      const body = writeInitProducerIdRequest(
        this.#transactionalId ?? null,
        this.#transactionTimeoutMs,
      );
      const response = await this.#txnCoordinatorRequest(API_INIT_PRODUCER_ID, 1, body);
      const initProducerId = readInitProducerIdResponse(response);
      this.#cluster.throttle(API_INIT_PRODUCER_ID, initProducerId.throttleMs);
      const { error } = initProducerId;
      if (!error) {
        this.#producer = { id: initProducerId.producerId, epoch: initProducerId.producerEpoch };
        this.#sequences.clear();
        return;
      }
      lastError = kafkaError(error, "Initialize idempotent producer");
      // Fresh transactional ids can briefly answer NOT_COORDINATOR while the
      // coordinator is being elected; retry those like transport failures.
      if (!(lastError instanceof KafkaError && lastError.retriable) || attempt === maxRetries) {
        break;
      }
      const delay = Math.round(
        Math.min(maxBackoffMs, initialBackoffMs * 2 ** attempt) *
          (RETRY_JITTER_BASE + Math.random()),
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
    try {
      if ((this.#options.idempotent || this.#transactionalId) && !this.#producer) {
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
    const { results, routedPartitions } = await this.#producePendingGroup(group);
    const byTopic = Map.groupBy(results, (result) => result.topic);
    for (const item of group) {
      item.resolve(byTopic.get(item.input.topic) ?? []);
    }
    notifyDeliverySuccess(routedPartitions, results, notified);
  }

  async #producePendingGroup(group: PendingSend[]): Promise<{
    results: ProduceResult[];
    routedPartitions: PartitionRecords[];
  }> {
    const first = requiredValue(group[0], "Cannot produce an empty group").input;
    const topics = Map.groupBy(group, ({ input }) => input.topic);
    const compression = first.compression ?? this.#options.compression;
    return this.#retryPendingGroup(topics, first, compression, 0);
  }

  async #retryPendingGroup(
    topics: Map<string, PendingSend[]>,
    first: ProducerSend,
    compression: ProducerOptions["compression"],
    attempt: number,
  ): Promise<{ results: ProduceResult[]; routedPartitions: PartitionRecords[] }> {
    try {
      return await this.#producePendingAttempt(topics, first, compression, attempt);
    } catch (error) {
      if (!(error instanceof KafkaError && error.retriable)) {
        throw error;
      }
      if (attempt === this.#cluster.retryOptions.maxRetries) {
        throw error;
      }
      await this.#retryProduce(attempt, error);
      return this.#retryPendingGroup(topics, first, compression, attempt + 1);
    }
  }

  async #producePendingAttempt(
    topics: Map<string, PendingSend[]>,
    first: ProducerSend,
    compression: ProducerOptions["compression"],
    attempt: number,
  ): Promise<{ results: ProduceResult[]; routedPartitions: PartitionRecords[] }> {
    const routedPartitions = await this.#routePendingTopics(topics, attempt);
    await this.#addPartitionsToTxn(routedPartitions);
    const results =
      (first.acks ?? 1) === 0
        ? await this.#fireAndForget(
            routedPartitions,
            first.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS,
            compression,
          )
        : await this.#produce(
            routedPartitions,
            this.#options.idempotent || this.#transactionalId || first.acks === "all" ? -1 : 1,
            first.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS,
            compression,
          );
    return { results, routedPartitions };
  }

  async #routePendingTopics(
    topics: Map<string, PendingSend[]>,
    attempt: number,
  ): Promise<PartitionRecords[]> {
    return (
      await Promise.all(
        [...topics].map(async ([topic, sends]) => {
          const firstSend = requiredValue(sends[0], `Cannot route empty topic group ${topic}`);
          return this.#route(
            topic,
            sends.flatMap(({ input }) => input.messages),
            firstSend.input.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS,
            attempt > 0,
          );
        }),
      )
    ).flat();
  }

  async #fireAndForget(
    partitions: PartitionRecords[],
    timeoutMs: number,
    compression: ProducerOptions["compression"],
  ): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    await Promise.all(
      [...leaders].map(([leader, leaderPartitions]) =>
        this.#cluster.fireAndForget(
          leader,
          API_PRODUCE,
          PRODUCE_API_VERSION,
          this.#produceRequestBody(leaderPartitions, 0, timeoutMs, compression),
        ),
      ),
    );
    return partitions.map((group) => ({
      topic: group.topic,
      partition: group.partition,
      baseOffset: -1n,
      logAppendTime: -1n,
    }));
  }

  async #retryProduce(attempt: number, error: KafkaError): Promise<void> {
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
    if (delay) {
      await Bun.sleep(delay);
    }
  }

  async #route(
    topic: string,
    messages: readonly ProducerMessage[],
    timeoutMs: number,
    refresh = false,
  ): Promise<PartitionRecords[]> {
    const metadata = await this.#topicMetadata(topic, timeoutMs, refresh);
    const partitionMetadata = new Map(
      metadata.partitions.map((partition) => [partition.id, partition]),
    );
    const partitions = new Map<number, PartitionRecords>();
    for (const message of messages) {
      const key = asBytes(message.key);
      const partition = chooseProducerPartition(
        topic,
        message,
        metadata.partitions.length,
        this.#producerPartitioner,
        this.#roundRobin,
        key,
      );
      appendProducerMessage(partitions, partitionMetadata, topic, message, key, partition);
    }
    return [...partitions.values()];
  }

  async #topicMetadata(topic: string, timeoutMs: number, refresh: boolean): Promise<TopicMetadata> {
    let metadata: TopicMetadata | undefined;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      metadata = await this.#cluster.topic(topic, refresh || Boolean(metadata));
      if (topicMetadataReady(metadata)) {
        return metadata;
      }
      if (!topicMetadataRetryable(metadata)) {
        throw kafkaError(metadata.err, topic);
      }
      await Bun.sleep(METADATA_REFRESH_SLEEP_MS);
    }
    if (!metadata || !topicMetadataReady(metadata)) {
      throw kafkaError(metadata?.err ?? KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION, topic);
    }
    return metadata;
  }

  #produceRequestBody(
    leaderPartitions: PartitionRecords[],
    acks: number,
    timeoutMs: number,
    compression = this.#options.compression,
  ): RequestBody {
    return writeProduceRequest(
      this.#transactionalId ?? null,
      acks,
      timeoutMs,
      leaderPartitions.map((value) => {
        const key = partitionKey(value.topic, value.partition);
        const producer = this.#producer && {
          ...this.#producer,
          sequence: this.#sequences.get(key) ?? 0,
          transactional: Boolean(this.#transactionalId && this.#txnOpen),
        };
        return { ...value, producer };
      }),
      compression ?? this.#options.compression ?? "none",
    );
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
          PRODUCE_API_VERSION,
          body,
          timeoutMs,
          false,
        );
        const produce = readProduceResponse(response);
        const results = produce.results.map(({ error, ...result }) => {
          if (error) {
            throw kafkaError(error, `${result.topic}[${result.partition}]`);
          }
          return result;
        });
        this.#cluster.throttle(API_PRODUCE, produce.throttleMs);
        return results;
      }),
    );
    const results = responses.flat();
    if (this.#producer) {
      for (const partition of partitions) {
        const key = partitionKey(partition.topic, partition.partition);
        this.#sequences.set(
          key,
          ((this.#sequences.get(key) ?? 0) + partition.records.length) % SEQ_WRAP,
        );
      }
    }
    return results;
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    await this.flush();
    if (this.#txnOpen) {
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
