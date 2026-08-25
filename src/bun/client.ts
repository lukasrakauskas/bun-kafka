import { KafkaError, kafkaErrorName } from "../errors.ts";
import type { Bytes, ClusterMetadata, ConsumedMessage, KafkaMessage, MessageHeaders, TopicPartition, Watermarks } from "../types.ts";
import { Connection, type BunKafkaSasl, type BunKafkaTls, type ConnectionOptions } from "./connection.ts";
import {
  Reader,
  Writer,
  asBytes,
  RecordSetDecoder,
  encodeRecordBatch,
  murmur2,
  readMetadataResponse,
  type WireRecord,
} from "./protocol.ts";

const API_PRODUCE = 0;
const API_FETCH = 1;
const API_LIST_OFFSETS = 2;
const API_METADATA = 3;
const API_CREATE_TOPICS = 19;
const API_DELETE_TOPICS = 20;
const API_CREATE_PARTITIONS = 37;
const API_DESCRIBE_CONFIGS = 32;
const API_ALTER_CONFIGS = 33;
const API_FIND_COORDINATOR = 10;
const API_JOIN_GROUP = 11;
const API_SYNC_GROUP = 14;
const API_HEARTBEAT = 12;
const API_LEAVE_GROUP = 13;
const API_OFFSET_COMMIT = 8;
const API_OFFSET_FETCH = 9;
const API_INIT_PRODUCER_ID = 22;
const API_DESCRIBE_GROUPS = 15;
const API_LIST_GROUPS = 16;
const API_DELETE_RECORDS = 21;
const API_CREATE_ACLS = 30;
const API_DESCRIBE_ACLS = 29;
const API_DELETE_ACLS = 31;
const API_DELETE_GROUPS = 42;
const API_DESCRIBE_CLIENT_QUOTAS = 48;
const API_ALTER_CLIENT_QUOTAS = 49;
const API_CREATE_DELEGATION_TOKEN = 38;
const API_RENEW_DELEGATION_TOKEN = 39;
const API_EXPIRE_DELEGATION_TOKEN = 40;
const API_DESCRIBE_DELEGATION_TOKEN = 41;
const API_ADD_PARTITIONS_TO_TXN = 24;
const API_ADD_OFFSETS_TO_TXN = 25;
// API key numbering per the Kafka protocol spec: EndTxn is 26,
// WriteTxnMarkers 27, TxnOffsetCommit 28.
const API_END_TXN = 26;
const API_TXN_OFFSET_COMMIT = 28;

const retriableErrors = new Set([1, 2, 3, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 19, 20, 25, 27, 32, 33, 38, 39, 41, 44, 45, 47, 49, 56, 70, 71, 74, 75, 78, 82, 86, 88, 89]);

function kafkaError(code: number, context: string, detail?: string | null): KafkaError {
  const label = detail ? `${context}: ${kafkaErrorName(code)} (${detail})` : `${context}: ${kafkaErrorName(code)}`;
  return new KafkaError(code, label, {
    retriable: retriableErrors.has(code),
    fatal: code === 58 || code === 34,
  });
}

function address(host: string, port: number): string {
  return `${host.includes(":") ? `[${host}]` : host}:${port}`;
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
}

function retryDelay(options: Required<RetryOptions>, attempt: number): number {
  const base = Math.min(options.maxBackoffMs, options.initialBackoffMs * 2 ** attempt);
  return Math.round(base * (0.5 + Math.random()));
}

export interface RetryOptions {
  /** Maximum retries after the first request. Default 3. */
  maxRetries?: number;
  /** Initial retry delay. Default 50 ms. */
  initialBackoffMs?: number;
  /** Maximum retry delay. Default 2,000 ms. */
  maxBackoffMs?: number;
}

export type KafkaEvent =
  | { type: "retry"; apiKey: number; attempt: number; delayMs: number; error: unknown }
  | { type: "throttle"; apiKey: number; durationMs: number }
  | { type: "stats"; stats: ClusterStats };

export interface Logger {
  debug(message: string): void;
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export interface ConnectionStats {
  requests: number;
  bytesSent: number;
  bytesReceived: number;
}

export interface BrokerHealth {
  address: string;
  brokerId?: number;
  ok: boolean;
  latencyMs: number;
  error?: unknown;
}

export interface HealthReport {
  brokers: BrokerHealth[];
}

export interface ClusterStats extends Record<string, unknown> {
  connections: number;
  requests: number;
  bytesSent: number;
  bytesReceived: number;
  retries: number;
  throttles: number;
  throttleTimeMs: number;
}

export interface KafkaOptions {
  brokers: string[];
  clientId?: string;
  tls?: BunKafkaTls;
  sasl?: BunKafkaSasl;
  requestTimeoutMs?: number;
  connectTimeoutMs?: number;
  maxResponseBytes?: number;
  retry?: RetryOptions;
  onEvent?: (event: KafkaEvent) => void;
  /** Emit a stats event on this interval (ms). */
  statsIntervalMs?: number;
  /** Optional logging hooks for operational diagnostics. */
  logger?: Partial<Logger>;
}

type TopicMetadata = ClusterMetadata["topics"][number];

export class Cluster {
  #bootstrap: string[];
  #options: ConnectionOptions;
  #retry: Required<RetryOptions>;
  #onEvent?: (event: KafkaEvent) => void;
  #logger?: Partial<Logger>;
  #retries = 0;
  #throttles = 0;
  #throttleTimeMs = 0;
  #statsTimer?: ReturnType<typeof setInterval>;
  #connections = new Map<string, Connection>();
  #brokers = new Map<number, string>();
  #controller?: number;
  #clusterId?: string | null;
  #topics = new Map<string, TopicMetadata>();

  constructor(options: KafkaOptions) {
    if (!Array.isArray(options.brokers) || !options.brokers.length) throw new TypeError("Kafka requires at least one broker");
    const requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    const connectTimeoutMs = options.connectTimeoutMs ?? 10_000;
    const maxResponseBytes = options.maxResponseBytes ?? 100 * 1024 * 1024;
    const retry = {
      maxRetries: options.retry?.maxRetries ?? 3,
      initialBackoffMs: options.retry?.initialBackoffMs ?? 50,
      maxBackoffMs: options.retry?.maxBackoffMs ?? 2_000,
    };
    if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs <= 0
      || !Number.isSafeInteger(connectTimeoutMs) || connectTimeoutMs <= 0
      || !Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 4
      || !Number.isSafeInteger(retry.maxRetries) || retry.maxRetries < 0
      || !Number.isFinite(retry.initialBackoffMs) || retry.initialBackoffMs < 0
      || !Number.isFinite(retry.maxBackoffMs) || retry.maxBackoffMs < retry.initialBackoffMs) {
      throw new RangeError("Invalid Kafka timeout, response size, or retry options");
    }
    const sasl = options.sasl;
    if (sasl && (!(new Set(["plain", "scram-sha-256", "scram-sha-512", "oauthbearer"])).has(sasl.mechanism)
      || (sasl.mechanism === "oauthbearer" ? !sasl.token : !sasl.username || !sasl.password))) {
      throw new TypeError("Invalid Kafka SASL options");
    }
    if (options.statsIntervalMs !== undefined && (!Number.isSafeInteger(options.statsIntervalMs) || options.statsIntervalMs < 1)) {
      throw new RangeError("Invalid Kafka statsIntervalMs");
    }
    this.#bootstrap = [...options.brokers];
    this.#retry = retry;
    this.#onEvent = options.onEvent;
    this.#logger = options.logger ?? {};
    this.#options = {
      clientId: options.clientId ?? "bun-kafka",
      requestTimeoutMs,
      connectTimeoutMs,
      maxResponseBytes,
      tls: options.tls,
      sasl: options.sasl,
    };
  }

  #connection(broker: string): Connection {
    let connection = this.#connections.get(broker);
    if (!connection) {
      connection = new Connection(broker, this.#options);
      this.#connections.set(broker, connection);
    }
    return connection;
  }

  async anyRequest(apiKey: number, apiVersion: number, body: Writer, flexible = false): Promise<Reader> {
    return this.#anyRequest(apiKey, apiVersion, body, flexible);
  }

  async #anyRequest(apiKey: number, apiVersion: number, body: Writer, flexible = false): Promise<Reader> {
    let lastError: unknown;
    const candidates = [...new Set([...this.#brokers.values(), ...this.#bootstrap])];
    for (const broker of candidates) {
      try {
        return await this.#connection(broker).request(apiKey, apiVersion, body, undefined, flexible);
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError ?? new KafkaError(-1, "No Kafka broker is available", { retriable: true });
  }

  async metadata(topics: string[] | null = null): Promise<ClusterMetadata> {
    const body = new Writer().array(topics, (writer, topic) => writer.string(topic));
    const response = readMetadataResponse(await this.#anyRequest(API_METADATA, 2, body));
    for (const broker of response.brokers) this.#brokers.set(broker.id, address(broker.host, broker.port));
    this.#controller = response.controllerId;
    this.#clusterId = response.clusterId;
    for (const topic of response.topics) this.#topics.set(topic.name, topic);
    return { brokers: response.brokers, topics: response.topics, clusterId: response.clusterId };
  }

  async topic(topic: string, refresh = false): Promise<TopicMetadata> {
    if (!refresh) {
      const cached = this.#topics.get(topic);
      if (cached?.partitions.length && !cached.err) return cached;
    }
    const metadata = await this.metadata([topic]);
    return metadata.topics.find((item) => item.name === topic) ?? { name: topic, err: 3, partitions: [] };
  }

  async request(brokerId: number, apiKey: number, apiVersion: number, body: Writer, timeoutMs?: number, retry = true, flexible = false): Promise<Reader> {
    let lastError: unknown;
    const maxRetries = retry ? this.#retry.maxRetries : 0;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        let broker = this.#brokers.get(brokerId);
        if (!broker) {
          await this.metadata();
          broker = this.#brokers.get(brokerId);
        }
        if (!broker) throw new KafkaError(-1, `Kafka broker ${brokerId} is not in metadata`, { retriable: true });
        return await this.#connection(broker).request(apiKey, apiVersion, body, timeoutMs, flexible);
      } catch (error) {
        lastError = error;
        if (!(error instanceof KafkaError && error.retriable) || attempt === maxRetries) throw error;
        this.#retries++;
        const delay = retryDelay(this.#retry, attempt);
        this.log("warn", `retrying ${apiKey} attempt ${attempt + 1} in ${delay}ms: ${String(error)}`);
        this.event({ type: "retry", apiKey, attempt: attempt + 1, delayMs: delay, error });
        if (delay) await Bun.sleep(delay);
      }
    }
    throw lastError;
  }

  async controllerRequest(apiKey: number, apiVersion: number, body: Writer): Promise<Reader> {
    if (this.#controller === undefined) await this.metadata();
    if (this.#controller === undefined) throw new KafkaError(-1, "Kafka metadata has no controller", { retriable: true });
    return this.request(this.#controller, apiKey, apiVersion, body);
  }

  /**
   * Resolve the transaction coordinator for a transactional id (FindCoordinator
   * v2, key_type=transaction). Brokers create their internal coordinator topic
   * on demand while serving this request, so it must precede InitProducerId.
   */
  async findTxnCoordinator(transactionalId: string): Promise<number> {
    // FindCoordinator v1/v2 wire order: coordinator_key STRING, then
    // coordinator_type INT8 (0 = group, 1 = transaction).
    const response = await this.#anyRequest(API_FIND_COORDINATOR, 2, new Writer().string(transactionalId).i8(1));
    const throttleMs = response.i32();
    if (throttleMs > 0) this.throttle(API_FIND_COORDINATOR, throttleMs);
    const error = response.i16();
    const message = response.string();
    if (error) throw kafkaError(error, `Find transaction coordinator ${transactionalId}${message ? `: ${message}` : ""}`);
    const coordinatorId = response.i32();
    response.string(); // host
    response.i32(); // port
    return coordinatorId;
  }

  /** Send a Produce request without waiting for a response (acks=0). */
  async fireAndForget(brokerId: number, apiKey: number, apiVersion: number, body: Writer): Promise<void> {
    let broker = this.#brokers.get(brokerId);
    if (!broker) {
      await this.metadata();
      broker = this.#brokers.get(brokerId);
    }
    if (!broker) throw new KafkaError(-1, `Kafka broker ${brokerId} is not in metadata`, { retriable: true });
    await this.#connection(broker).sendOnly(apiKey, apiVersion, body);
  }

  /** Cluster id reported by Metadata v2+ responses. */
  get clusterId(): string | null | undefined { return this.#clusterId; }
  get retryOptions(): Required<RetryOptions> { return this.#retry; }
  get requestTimeoutMs(): number { return this.#options.requestTimeoutMs; }

  event(event: KafkaEvent): void {
    try { this.#onEvent?.(event); } catch { /* Observability must not break requests. */ }
  }

  log(level: keyof Logger, message: string): void {
    try { this.#logger?.[level]?.(message); } catch { /* Logging must not break requests. */ }
  }

  throttle(apiKey: number, durationMs: number): void {
    if (durationMs > 0) {
      this.#throttles++;
      this.#throttleTimeMs += durationMs;
      this.log("debug", `broker throttled ${apiKey} by ${durationMs}ms`);
      this.event({ type: "throttle", apiKey, durationMs });
    }
  }

  /** Aggregate counters across all live broker connections. */
  stats(): ClusterStats {
    let requests = 0;
    let bytesSent = 0;
    let bytesReceived = 0;
    for (const connection of this.#connections.values()) {
      const one = connection.stats;
      requests += one.requests;
      bytesSent += one.bytesSent;
      bytesReceived += one.bytesReceived;
    }
    return {
      connections: this.#connections.size,
      requests: requests + this.#retries,
      bytesSent,
      bytesReceived,
      retries: this.#retries,
      throttles: this.#throttles,
      throttleTimeMs: this.#throttleTimeMs,
    };
  }

  /** Start emitting periodic stats events. */
  trackStats(intervalMs: number): void {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) throw new RangeError("Invalid stats interval");
    this.stopTrackingStats();
    this.#statsTimer = setInterval(() => this.event({ type: "stats", stats: this.stats() }), intervalMs);
    this.#statsTimer.unref?.();
  }

  stopTrackingStats(): void {
    if (this.#statsTimer) clearInterval(this.#statsTimer);
    this.#statsTimer = undefined;
  }

  /**
   * Ping every known broker with an ApiVersions request and report latency.
   */
  async healthCheck(timeoutMs = 5_000): Promise<HealthReport> {
    const targets = new Map<string, number | undefined>();
    for (const [id, addr] of this.#brokers) targets.set(addr, id);
    for (const addr of this.#bootstrap) if (!targets.has(addr)) targets.set(addr, undefined);
    const checks = await Promise.all([...targets].map(async ([addr, brokerId]) => {
      const startedAt = performance.now();
      try {
        await this.#connection(addr).request(18, 0, new Writer(), timeoutMs);
        return { address: addr, brokerId, ok: true as const, latencyMs: Math.round(performance.now() - startedAt) };
      } catch (error) {
        this.log("warn", `health check failed for ${addr}: ${String(error)}`);
        return { address: addr, brokerId, ok: false as const, latencyMs: Math.round(performance.now() - startedAt), error };
      }
    }));
    return { brokers: checks };
  }

  bumpRetries(n = 1): void { this.#retries += n; }

  close(): void {
    this.stopTrackingStats();
    for (const connection of this.#connections.values()) connection.close();
    this.#connections.clear();
  }
}

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
  reject: (error: unknown) => void;
};

export class BunProducer {
  #cluster: Cluster;
  #roundRobin = new Map<string, number>();
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;
  #options: Required<Omit<ProducerOptions, "partitioner" | "transactionalId" | "transactionTimeoutMs">>;
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

  constructor(options: KafkaOptions | Cluster, producerOptions: ProducerOptions = {}, onClose = () => {}) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = this.#ownsCluster ? new Cluster(options as KafkaOptions) : options as Cluster;
    this.#options = {
      lingerMs: producerOptions.lingerMs ?? 5,
      batchMaxMessages: producerOptions.batchMaxMessages ?? 1_000,
      compression: producerOptions.compression ?? "none",
      idempotent: producerOptions.idempotent ?? false,
    };
    if (!Number.isFinite(this.#options.lingerMs) || this.#options.lingerMs < 0
      || !Number.isSafeInteger(this.#options.batchMaxMessages) || this.#options.batchMaxMessages < 1
      || !(this.#options.compression in { none: 1, gzip: 1, snappy: 1, lz4: 1, zstd: 1 })
      || (producerOptions.partitioner !== undefined && typeof producerOptions.partitioner !== "function")) {
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
      throw new TypeError("A transactional or idempotent producer requires acknowledged Produce requests");
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
      this.#flushing = this.#flushPending(pending).finally(() => { this.#flushing = undefined; });
      await this.#flushing;
    }
  }

  /**
   * Initialize the producer identity and start a transaction.
   * Messages sent afterwards are not visible to consumers until commitTransaction().
   */
  async beginTransaction(): Promise<void> {
    this.#open();
    if (!this.#transactionalId) throw new Error("beginTransaction requires a transactionalId producer option");
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
      .i64(this.#producer!.id).i16(this.#producer!.epoch)
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
  async sendOffsetsToTransaction(offsets: readonly CommittedOffset[], groupId: string): Promise<void> {
    this.#open();
    if (!this.#transactionalId || !this.#txnOpen) throw new Error("sendOffsetsToTransaction requires an open transaction");
    if (!offsets.length) return;
    const topics = Map.groupBy(offsets as readonly CommittedOffset[], (o) => o.topic);
    const body = new Writer().string(this.#transactionalId).string(groupId)
      .i64(this.#producer!.id).i16(this.#producer!.epoch)
      .array([...topics], (writer, [topicName, values]) => writer.string(topicName).array(values, (partitionWriter, value) =>
        partitionWriter.i32(value.partition).i64(value.offset).string(null)));
    const addOffsetsBody = new Writer().string(this.#transactionalId)
      .i64(this.#producer!.id).i16(this.#producer!.epoch)
      .string(groupId);
    const addOffsetsResponse = await this.#txnCoordinatorRequest(API_ADD_OFFSETS_TO_TXN, 0, addOffsetsBody);
    this.#cluster.throttle(API_ADD_OFFSETS_TO_TXN, addOffsetsResponse.i32());
    const addOffsetsError = addOffsetsResponse.i16();
    if (addOffsetsError) throw kafkaError(addOffsetsError, `AddOffsetsToTxn group ${groupId}`);
    const response = await this.#txnCoordinatorRequest(API_TXN_OFFSET_COMMIT, 0, body);
    this.#cluster.throttle(API_TXN_OFFSET_COMMIT, response.i32());
    for (const result of response.array((reader) => ({ topic: reader.string() ?? "", partitions: reader.array((p) => ({ index: p.i32(), error: p.i16() })) }))) {
      for (const partition of result.partitions) if (partition.error) throw kafkaError(partition.error, `${result.topic}[${partition.index}]`);
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
    const fresh = partitions.filter((group) => !this.#txnAddedPartitions.has(partitionKey(group.topic, group.partition)));
    if (!fresh.length) return;
    const byTopic = Map.groupBy(fresh, (group) => group.topic);
    const response = await this.#txnCoordinatorRequest(API_ADD_PARTITIONS_TO_TXN, 1, new Writer()
      .string(this.#transactionalId)
      .i64(this.#producer.id).i16(this.#producer.epoch)
      .array([...byTopic], (writer, [name, groups]) => writer.string(name).array(groups, (partitionWriter, g) => partitionWriter.i32(g.partition))));
    this.#cluster.throttle(API_ADD_PARTITIONS_TO_TXN, response.i32());
    for (const topic of response.array((reader) => ({ name: reader.string() ?? "", partitions: reader.array((p) => ({ index: p.i32(), error: p.i16() })) }))) {
      for (const partition of topic.partitions) if (partition.error) throw kafkaError(partition.error, `AddPartitionsToTxn ${topic.name}[${partition.index}]`);
    }
    for (const group of fresh) this.#txnAddedPartitions.add(partitionKey(group.topic, group.partition));
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
      if (!(lastError instanceof KafkaError && lastError.retriable) || attempt === maxRetries) break;
      const delay = Math.round(Math.min(maxBackoffMs, initialBackoffMs * 2 ** attempt) * (0.5 + Math.random()));
      this.#cluster.log("warn", `retrying InitProducerId attempt ${attempt + 1} in ${delay}ms`);
      this.#cluster.event({ type: "retry", apiKey: API_INIT_PRODUCER_ID, attempt: attempt + 1, delayMs: delay, error: lastError });
      await Bun.sleep(delay);
    }
    throw lastError;
  }

  async #flushPending(pending: PendingSend[]): Promise<void> {
    const notified = new Set<NonNullable<ProducerMessage["onDelivery"]>>();
    const notifyFailure = (error: unknown) => {
      for (const { input } of pending) {
        for (const message of input.messages) {
          if (!message.onDelivery || notified.has(message.onDelivery)) continue;
          notified.add(message.onDelivery);
          try {
            message.onDelivery(error instanceof KafkaError ? error : new KafkaError(-1, String(error)), null);
          } catch { /* Delivery callbacks must not break flushing. */ }
        }
      }
    };
    try {
      if ((this.#options.idempotent || this.#transactionalId) && !this.#producer) {
        await this.initProducerId();
      }

      const configs = Map.groupBy(pending, ({ input }) => `${input.acks ?? 1}\0${input.timeoutMs ?? 30_000}`);
      for (const group of configs.values()) {
        const topics = Map.groupBy(group, ({ input }) => input.topic);
        const first = group[0]!.input;
        let results: ProduceResult[] | undefined;
        let lastError: unknown;
        let routedPartitions: PartitionRecords[] = [];
        for (let attempt = 0; attempt <= this.#cluster.retryOptions.maxRetries; attempt++) {
          try {
            routedPartitions = (await Promise.all([...topics].map(async ([topic, sends]) => {
              const messages = sends.flatMap(({ input }) => input.messages);
              return this.#route(topic, messages, sends[0]!.input.timeoutMs ?? 30_000, attempt > 0);
            }))).flat();
            await this.#addPartitionsToTxn(routedPartitions);
            if ((first.acks ?? 1) === 0) {
              // Fire-and-forget: brokers never answer acks=0 Produce requests.
              const leaders = Map.groupBy(routedPartitions, (partition) => partition.leader);
              await Promise.all([...leaders].map(([leader, leaderPartitions]) =>
                this.#cluster.fireAndForget(leader, API_PRODUCE, 3,
                  this.#produceRequestBody(leaderPartitions, 0, first.timeoutMs ?? 30_000))));
              results = routedPartitions.map((group) => ({ topic: group.topic, partition: group.partition, baseOffset: -1n, logAppendTime: -1n }));
            } else {
              const acks = this.#options.idempotent || this.#transactionalId || first.acks === "all" ? -1 : 1;
              results = await this.#produce(routedPartitions, acks, first.timeoutMs ?? 30_000);
            }
            break;
          } catch (error) {
            lastError = error;
            if (!(error instanceof KafkaError && error.retriable) || attempt === this.#cluster.retryOptions.maxRetries) throw error;
            this.#cluster.bumpRetries();
            const delay = retryDelay(this.#cluster.retryOptions, attempt);
            this.#cluster.log("warn", `retrying produce attempt ${attempt + 1} in ${delay}ms: ${String(error)}`);
            this.#cluster.event({ type: "retry", apiKey: API_PRODUCE, attempt: attempt + 1, delayMs: delay, error });
            if (delay) await Bun.sleep(delay);
          }
        }
        if (!results) throw lastError ?? new KafkaError(-1, "Kafka produce failed", { retriable: true });
        const byTopic = Map.groupBy(results, (result) => result.topic);
        for (const item of group) item.resolve(byTopic.get(item.input.topic) ?? []);
        // Per-message delivery callbacks use the authoritative partition routing.
        const byPartition = new Map(results.map((result) => [partitionKey(result.topic, result.partition), result]));
        for (const routed of routedPartitions) {
          const result = byPartition.get(partitionKey(routed.topic, routed.partition));
          if (!result) continue;
          for (const message of routed.messages) {
            if (!message.onDelivery || notified.has(message.onDelivery)) continue;
            notified.add(message.onDelivery);
            try { message.onDelivery(null, result); } catch { /* Delivery callbacks must not break flushing. */ }
          }
        }
      }
    } catch (error) {
      notifyFailure(error);
      for (const item of pending) item.reject(error);
      throw error;
    }
  }

  async #route(topic: string, messages: readonly ProducerMessage[], timeoutMs: number, refresh = false): Promise<PartitionRecords[]> {
    let metadata: TopicMetadata | undefined;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      metadata = await this.#cluster.topic(topic, refresh || !!metadata);
      if (!metadata.err && metadata.partitions.length) break;
      if (metadata.err !== 3 && metadata.err !== 5) throw kafkaError(metadata.err, topic);
      await Bun.sleep(10);
    }
    if (!metadata?.partitions.length || metadata.err) throw kafkaError(metadata?.err ?? 3, topic);

    const partitionMetadata = new Map(metadata.partitions.map((partition) => [partition.id, partition]));
    const partitions = new Map<number, PartitionRecords>();
    for (const message of messages) {
      const key = asBytes(message.key);
      let partition = message.partition;
      if (partition === undefined) {
        if (this.#producerPartitioner) {
          const chosen = this.#producerPartitioner({ topic, partitionCount: metadata.partitions.length, key });
          if (!Number.isInteger(chosen) || chosen < 0 || chosen >= metadata.partitions.length) {
            throw new RangeError(`Custom partitioner returned invalid partition ${chosen} for ${topic}`);
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
      group.records.push(key && typeof message.key === "string" ? { ...message, key } : message);
      group.messages.push(message);
    }
    return [...partitions.values()];
  }

  #produceRequestBody(leaderPartitions: PartitionRecords[], acks: number, timeoutMs: number): Writer {
    const topics = Map.groupBy(leaderPartitions, (partition) => partition.topic);
    // Produce v3+: brokers reject transactional batches whose request omits the
    // matching transactional_id, so it must ride along on every produce.
    return new Writer().string(this.#transactionalId ?? null).i16(acks).i32(timeoutMs).array([...topics], (writer, [topic, topicPartitions]) => {
      writer.string(topic).array(topicPartitions, (partitionWriter, value) => {
        const key = partitionKey(value.topic, value.partition);
        const producer = this.#producer && { ...this.#producer, sequence: this.#sequences.get(key) ?? 0, transactional: Boolean(this.#transactionalId && this.#txnOpen) };
        partitionWriter.i32(value.partition).bytes(encodeRecordBatch(value.records, Date.now(), this.#options.compression, producer));
      });
    });
  }

  async #produce(partitions: PartitionRecords[], acks: number, timeoutMs: number): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    const responses = await Promise.all([...leaders].map(async ([leader, leaderPartitions]) => {
      const body = this.#produceRequestBody(leaderPartitions, acks, timeoutMs);
      const response = await this.#cluster.request(leader, API_PRODUCE, 3, body, timeoutMs, false);
      const results = response.array((topicReader) => {
        const topic = topicReader.string() ?? "";
        return topicReader.array((partitionReader) => {
          const partition = partitionReader.i32();
          const error = partitionReader.i16();
          const baseOffset = partitionReader.i64();
          const logAppendTime = partitionReader.i64();
          if (error) throw kafkaError(error, `${topic}[${partition}]`);
          return { topic, partition, baseOffset, logAppendTime };
        });
      }).flat();
      this.#cluster.throttle(API_PRODUCE, response.i32());
      return results;
    }));
    const results = responses.flat();
    if (this.#producer) {
      for (const partition of partitions) {
        const key = partitionKey(partition.topic, partition.partition);
        this.#sequences.set(key, ((this.#sequences.get(key) ?? 0) + partition.records.length) % 0x80000000);
      }
    }
    return results;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    await this.flush();
    if (this.#txnOpen) {
      try { await this.abortTransaction(); } catch { /* Best-effort abort during shutdown. */ }
    }
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
  #open(): void { if (this.#closed) throw new Error("Producer is closed"); }
}

export interface ConsumerOptions<K = Uint8Array | null, V = Uint8Array | null> {
  fromBeginning?: boolean;
  fetchMaxBytes?: number;
  groupId?: string;
  /** Static group membership identity (KIP-345); requires a broker that supports JoinGroup v3+. */
  groupInstanceId?: string;
  /**
   * Consumer group partition assignor protocol. "range" (default) is eager:
   * every rebalance revokes all partitions. "cooperative-sticky" (KIP-429)
   * retains ownership across rebalances so only moved partitions change hands.
   */
  partitionAssigner?: "range" | "cooperative-sticky";
  /** Transaction visibility: read_committed filters aborted transaction records (default read_uncommitted). */
  isolationLevel?: "read_uncommitted" | "read_committed";
  sessionTimeoutMs?: number;
  rebalanceTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  autoCommit?: boolean;
  /** Replaces message keys before they are returned. */
  keyDeserializer?: (data: Uint8Array | null, context: DeserializerContext) => K;
  /** Replaces message values before they are returned. */
  valueDeserializer?: (data: Uint8Array | null, context: DeserializerContext) => V;
}

export type DeserializerContext = {
  topic: string;
  partition: number;
  offset: bigint;
  timestamp: bigint;
};

export interface ConsumerSubscribe {
  topics: string | RegExp | Array<string | RegExp>;
  fromBeginning?: boolean;
  groupId?: string;
}

export interface ConsumerAssignment {
  topic: string;
  partition: number;
  offset?: bigint | "earliest" | "latest";
}

export interface CommittedOffset {
  topic: string;
  partition: number;
  offset: bigint;
}

export interface FetchOptions {
  maxWaitMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxPartitionBytes?: number;
  maxMessages?: number;
  /** Copy payloads instead of returning stable views into the response buffer. */
  copy?: boolean;
}

type Assigned = { topic: string; partition: number; leader: number };
type GroupMember = { memberId: string; topics: string[] };
type GroupAssignment = { topic: string; partitions: number[] };

// Per-broker incremental fetch session state (KIP-227). The broker remembers
// every partition ever added to the session, so omitted partitions stay
// monitored and reappear in responses once new data arrives.
type FetchSessionState = {
  id: number; // broker-assigned session id (0 = not established yet)
  epoch: number; // INITIAL_EPOCH = 0 while opening, then increments per fetch
  sent: Map<string, bigint>; // partitionKey -> offset last requested in this session
  streaming: Map<string, boolean>; // partitionKey -> last response carried records
};

export class BunConsumer<K = Uint8Array | null, V = Uint8Array | null> implements AsyncIterable<ConsumedMessage<K, V>> {
  #cluster: Cluster;
  #options: ConsumerOptions<K, V>;
  #assigned = new Map<string, Assigned>();
  #positions = new Map<string, bigint>();
  #paused = new Set<string>();
  #decoders: RecordSetDecoder[] = [];
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;
  #groupId?: string;
  #memberId = "";
  #generationId = -1;
  #coordinator?: number;
  #heartbeat?: ReturnType<typeof setInterval>;
  // Fetch sessions (KIP-227), tracked per leader broker.
  #fetchSessions = new Map<number, FetchSessionState>();
  #groupTopics: string[] = [];
  #rejoining?: Promise<void>;

  constructor(options: KafkaOptions | Cluster, consumerOptions: ConsumerOptions<K, V> = {}, onClose = () => {}) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = this.#ownsCluster ? new Cluster(options as KafkaOptions) : options as Cluster;
    this.#options = consumerOptions;
    const session = consumerOptions.sessionTimeoutMs ?? 45_000;
    const rebalance = consumerOptions.rebalanceTimeoutMs ?? 60_000;
    const heartbeat = consumerOptions.heartbeatIntervalMs ?? 3_000;
    if (![session, rebalance, heartbeat].every((value) => Number.isSafeInteger(value) && value > 0) || heartbeat >= session) {
      throw new RangeError("Invalid consumer group timeout options");
    }
    this.#onClose = onClose;
  }

  async #findCoordinator(): Promise<number> {
    if (this.#coordinator !== undefined) return this.#coordinator;
    const response = await this.#cluster.anyRequest(API_FIND_COORDINATOR, 0, new Writer().string(this.#groupId!));
    const error = response.i16();
    const coordinator = response.i32();
    response.string();
    response.i32();
    if (error) throw kafkaError(error, `Kafka group ${this.#groupId}`);
    this.#coordinator = coordinator;
    return coordinator;
  }

  /** Encode a ConsumerProtocolSubscription. v1 adds owned partitions (KIP-429). */
  #encodeSubscription(topics: string[], owned?: Array<{ topic: string; partition: number }>): Uint8Array {
    const w = new Writer().i16(owned ? 1 : 0).array(topics, (writer, topic) => writer.string(topic));
    if (owned) {
      const byTopic = Map.groupBy(owned, (item) => item.topic);
      w.array([...byTopic], (writer, [name, partitions]) =>
        writer.string(name).array(partitions, (partitionWriter, p) => partitionWriter.i32(p.partition)));
    }
    return w.bytes(null).result();
  }

  async #joinGroup(topics: string[], fromBeginning: boolean): Promise<void> {
    const coordinator = await this.#findCoordinator();
    const instanceId = this.#options.groupInstanceId;
    const cooperative = this.#options.partitionAssigner === "cooperative-sticky";
    const protocolName = cooperative ? "cooperative-sticky" : "range";
    const memberMetadata = this.#encodeSubscription(topics, cooperative ? [...this.#assigned.values()] : undefined);
    // JoinGroup v3+ carries the static membership identity (KIP-345).
    const joinVersion = instanceId === undefined ? 2 : 3;
    const join = new Writer().string(this.#groupId!)
      .i32(this.#options.sessionTimeoutMs ?? 45_000)
      .i32(this.#options.rebalanceTimeoutMs ?? 60_000).string(this.#memberId);
    if (joinVersion >= 3) join.string(instanceId!);
    join.string("consumer")
      .array([[protocolName, memberMetadata] as const], (writer, [name, metadata]) => writer.string(name).bytes(metadata));
    const response = await this.#cluster.request(coordinator, API_JOIN_GROUP, joinVersion, join);
    this.#cluster.throttle(API_JOIN_GROUP, response.i32());
    const error = response.i16();
    this.#generationId = response.i32();
    response.string();
    const leader = response.string() ?? "";
    this.#memberId = response.string() ?? "";
    const members = response.array((reader) => {
      const memberId = reader.string() ?? "";
      const metadata = new Reader(reader.bytes() ?? new Uint8Array());
      const version = metadata.i16();
      const memberTopics = metadata.array((item) => item.string() ?? "");
      let owned: Array<{ topic: string; partition: number }> = [];
      if (version >= 1) {
        owned = metadata.array((ownedReader) => {
          const topicName = ownedReader.string() ?? "";
          return ownedReader.array((p) => p.i32()).map((partition) => ({ topic: topicName, partition }));
        }).flat();
      }
      metadata.bytes(); // user data
      return { memberId, topics: memberTopics, owned };
    });
    if (error) throw kafkaError(error, `Kafka group ${this.#groupId}`);

    const assignments = new Map<string, GroupAssignment[]>();
    if (this.#memberId === leader) {
      const allTopics = [...new Set(members.flatMap((member) => member.topics))];
      const metadata = await this.#cluster.metadata(allTopics);
      for (const member of members) assignments.set(member.memberId, []);
      for (const topicMeta of metadata.topics) {
        if (topicMeta.err || !topicMeta.partitions.length) continue;
        const eligible = members.filter((member) => member.topics.includes(topicMeta.name)).sort((a, b) => a.memberId.localeCompare(b.memberId));
        const partitions = topicMeta.partitions.map(({ id }) => id).sort((a, b) => a - b);
        if (!eligible.length) continue;
        if (cooperative) {
          // Cooperative-sticky (KIP-429): compute a fair target distribution,
          // then retain every partition its owner already holds (up to the
          // member's fair share). Only surplus partitions move, so ownership
          // changes only where the balance demands it.
          const fairShare = Math.floor(partitions.length / eligible.length);
          const extra = partitions.length % eligible.length;
          const targetSize = new Map(eligible.map((m, i) => [m.memberId, fairShare + (i < extra ? 1 : 0)]));
          const finals = new Map<string, number[]>(eligible.map((m) => [m.memberId, []]));
          const ownedBy = new Map<number, string>();
          // Pass 1: retain valid existing ownership.
          for (const m of eligible) {
            for (const o of m.owned) {
              if (o.topic !== topicMeta.name || !partitions.includes(o.partition)) continue;
              if (ownedBy.has(o.partition)) continue;
              const mine = finals.get(m.memberId)!;
              if (mine.length >= targetSize.get(m.memberId)!) continue;
              ownedBy.set(o.partition, m.memberId);
              mine.push(o.partition);
            }
          }
          // Pass 2: hand out the remaining partitions to the least-loaded subscribers.
          for (const partition of partitions) {
            if (ownedBy.has(partition)) continue;
            const candidates = eligible
              .filter((m) => finals.get(m.memberId)!.length < targetSize.get(m.memberId)!)
              .sort((a, b) => (finals.get(a.memberId)!.length - finals.get(b.memberId)!.length) || a.memberId.localeCompare(b.memberId));
            const chosen = candidates[0] ?? eligible[0]!;
            ownedBy.set(partition, chosen.memberId);
            finals.get(chosen.memberId)!.push(partition);
          }
          for (const [memberId, parts] of finals) {
            if (parts.length) assignments.get(memberId)!.push({ topic: topicMeta.name, partitions: parts.sort((a, b) => a - b) });
          }
        } else {
          let start = 0;
          eligible.forEach((member, index) => {
            const count = Math.floor(partitions.length / eligible.length) + (index < partitions.length % eligible.length ? 1 : 0);
            assignments.get(member.memberId)!.push({ topic: topicMeta.name, partitions: partitions.slice(start, start + count) });
            start += count;
          });
        }
      }
    }
    const syncInstance = this.#options.groupInstanceId;
    const syncVersion = syncInstance === undefined ? 0 : 3;
    const sync = new Writer().string(this.#groupId!).i32(this.#generationId).string(this.#memberId);
    if (syncVersion >= 3) sync.string(syncInstance!);
    sync
      .array([...assignments], (writer, [memberId, memberAssignments]) => {
        const assignment = new Writer().i16(0).array(memberAssignments, (assignmentWriter, item) => assignmentWriter.string(item.topic).array(item.partitions, (writer, partition) => writer.i32(partition))).bytes(null);
        writer.string(memberId).bytes(assignment.result());
      });
    const synced = await this.#cluster.request(coordinator, API_SYNC_GROUP, syncVersion, sync);
    if (syncVersion >= 3) synced.i32(); // throttle_time_ms added in v3
    const syncError = synced.i16();
    const syncAssignment = synced.bytes() ?? new Uint8Array();
    if (syncError) throw kafkaError(syncError, `Kafka group ${this.#groupId} sync`);
    const assignmentReader = new Reader(syncAssignment);
    assignmentReader.i16();
    const assigned: ConsumerAssignment[] = [];
    for (const item of assignmentReader.array((reader) => ({ topic: reader.string() ?? "", partitions: reader.array((reader) => reader.i32()) }))) {
      for (const partition of item.partitions) assigned.push({ topic: item.topic, partition });
    }
    const committed = new Map((await this.committed(assigned)).map((item) => [partitionKey(item.topic, item.partition), item.offset]));
    // Under cooperative-sticky, partitions this member keeps must not lose
    // their in-memory position (a rebalance is not a consumer restart).
    const retained = new Map<string, bigint>();
    if (cooperative) {
      for (const item of assigned) {
        const key = partitionKey(item.topic, item.partition);
        const position = this.#positions.get(key);
        if (position !== undefined) retained.set(key, position);
      }
    }
    await this.assign(assigned.map((item) => {
      const key = partitionKey(item.topic, item.partition);
      return {
        ...item,
        offset: retained.get(key) ?? ((committed.get(key) ?? -1n) >= 0n
          ? committed.get(key)!
          : fromBeginning ? "earliest" : "latest"),
      };
    }));
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#heartbeat = setInterval(() => void this.#heartbeatOnce(coordinator), this.#options.heartbeatIntervalMs ?? 3_000);
  }

  async #heartbeatOnce(coordinator: number): Promise<void> {
    if (!this.#groupId || this.#generationId < 0 || this.#rejoining) return;
    try {
      const heartbeatInstance = this.#options.groupInstanceId;
      const heartbeatVersion = heartbeatInstance === undefined ? 0 : 3;
      const heartbeatBody = new Writer().string(this.#groupId).i32(this.#generationId).string(this.#memberId);
      if (heartbeatVersion >= 3) heartbeatBody.string(heartbeatInstance!);
      const response = await this.#cluster.request(coordinator, API_HEARTBEAT, heartbeatVersion, heartbeatBody);
      if (heartbeatVersion >= 3) response.i32(); // throttle_time_ms added in v3
      const error = response.i16();
      if (!error) return;
      if (error === 25) this.#memberId = "";
      if (error !== 22 && error !== 25 && error !== 27) throw kafkaError(error, `Kafka group ${this.#groupId} heartbeat`);
    } catch {
      this.#coordinator = undefined;
    }
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#assigned.clear();
    this.#positions.clear();
    this.#decoders = [];
    this.#rejoining = this.#joinGroup(this.#groupTopics, this.#options.fromBeginning ?? false)
      .finally(() => { this.#rejoining = undefined; });
    try { await this.#rejoining; } catch { this.#coordinator = undefined; }
  }

  async commitOffsets(assignments: readonly ConsumerAssignment[] = this.assignment().map(({ topic, partition, offset }) => ({ topic, partition, offset: typeof offset === "bigint" ? offset : undefined }))): Promise<void> {
    this.#open();
    if (!this.#groupId) throw new Error("Consumer groupId is required for offset commits");
    const coordinator = await this.#findCoordinator();
    const topics = Map.groupBy(assignments, (assignment) => assignment.topic);
    const body = new Writer().string(this.#groupId).i32(this.#generationId).string(this.#memberId).i64(-1n)
      .array([...topics], (writer, [topic, values]) => writer.string(topic).array(values, (partitionWriter, value) => partitionWriter.i32(value.partition).i64(typeof value.offset === "bigint" ? value.offset : this.#positions.get(partitionKey(topic, value.partition)) ?? 0n).string(null)));
    const response = await this.#cluster.request(coordinator, API_OFFSET_COMMIT, 2, body);
    for (const result of response.array((reader) => ({ topic: reader.string() ?? "", partitions: reader.array((reader) => ({ partition: reader.i32(), error: reader.i16() })) }))) {
      for (const partition of result.partitions) if (partition.error) throw kafkaError(partition.error, `${result.topic}[${partition.partition}]`);
    }
  }

  async committed(assignments: readonly ConsumerAssignment[]): Promise<CommittedOffset[]> {
    this.#open();
    if (!this.#groupId) throw new Error("Consumer groupId is required for offset fetch");
    const coordinator = await this.#findCoordinator();
    const topics = Map.groupBy(assignments, (assignment) => assignment.topic);
    const body = new Writer().string(this.#groupId).array([...topics], (writer, [topic, values]) => writer.string(topic).array(values, (partitionWriter, value) => partitionWriter.i32(value.partition)));
    const response = await this.#cluster.request(coordinator, API_OFFSET_FETCH, 2, body);
    const result: CommittedOffset[] = [];
    for (const topic of response.array((reader) => ({ topic: reader.string() ?? "", partitions: reader.array((reader) => ({ partition: reader.i32(), offset: reader.i64(), metadata: reader.string(), error: reader.i16() })) }))) {
      for (const partition of topic.partitions) {
        if (partition.error) throw kafkaError(partition.error, `${topic.topic}[${partition.partition}]`);
        result.push({ topic: topic.topic, partition: partition.partition, offset: partition.offset });
      }
    }
    const error = response.i16();
    if (error) throw kafkaError(error, `Kafka group ${this.#groupId}`);
    return result;
  }

  async subscribe(input: ConsumerSubscribe | string | Array<string | RegExp>): Promise<void> {
    this.#open();
    const request = typeof input === "object" && !Array.isArray(input)
      ? input
      : { topics: input };
    let topics = await this.#resolveTopicPatterns(
      typeof request.topics === "string" || request.topics instanceof RegExp ? [request.topics] : request.topics,
    );
    const groupId = request.groupId ?? this.#options.groupId;
    if (groupId) {
      this.#groupId = groupId;
      this.#groupTopics = topics;
      await this.#joinGroup(topics, request.fromBeginning ?? this.#options.fromBeginning ?? false);
      return;
    }
    const metadata = await this.#cluster.metadata(topics);
    const assignments: ConsumerAssignment[] = [];
    for (const topic of topics) {
      const found = metadata.topics.find((item) => item.name === topic);
      if (!found || found.err) throw kafkaError(found?.err ?? 3, topic);
      for (const partition of found.partitions) assignments.push({
        topic,
        partition: partition.id,
        offset: request.fromBeginning ?? this.#options.fromBeginning ? "earliest" : "latest",
      });
    }
    await this.assign(assignments);
  }

  /** Expands RegExp topic patterns against cluster metadata into literal topic names. */
  async #resolveTopicPatterns(topics: Array<string | RegExp>): Promise<string[]> {
    const patterns = topics.filter((topic): topic is RegExp => topic instanceof RegExp);
    if (!patterns.length) return [...new Set(topics as string[])];
    const metadata = await this.#cluster.metadata(null);
    const resolved = new Set<string>();
    for (const entry of metadata.topics) {
      if (entry.err || !entry.name) continue;
      const matchesPattern = patterns.some((pattern) => pattern.test(entry.name));
      const listedLiteral = topics.some((topic) => typeof topic === "string" && topic === entry.name);
      if (matchesPattern || listedLiteral) resolved.add(entry.name);
    }
    return [...resolved];
  }

  async assign(assignments: ConsumerAssignment[]): Promise<void> {
    this.#open();
    this.#assigned.clear();
    this.#positions.clear();
    this.#paused.clear();
    this.#decoders = [];
    this.#fetchSessions.clear(); // assignment change invalidates fetch sessions
    const unresolved: Array<Assigned & { which: "earliest" | "latest" }> = [];
    for (const assignment of assignments) {
      const metadata = await this.#cluster.topic(assignment.topic);
      const partition = metadata.partitions.find((item) => item.id === assignment.partition);
      if (!partition) throw new RangeError(`Partition ${assignment.partition} does not exist on ${assignment.topic}`);
      const key = partitionKey(assignment.topic, assignment.partition);
      const assigned = { topic: assignment.topic, partition: assignment.partition, leader: partition.leader };
      this.#assigned.set(key, assigned);
      if (typeof assignment.offset === "bigint") this.#positions.set(key, assignment.offset);
      else unresolved.push({ ...assigned, which: assignment.offset ?? (this.#options.fromBeginning ? "earliest" : "latest") });
    }
    await this.#resolveOffsets(unresolved);
  }

  async #resolveOffsets(assignments: Array<Assigned & { which: "earliest" | "latest" }>): Promise<void> {
    const leaders = Map.groupBy(assignments, (assignment) => assignment.leader);
    await Promise.all([...leaders].map(async ([leader, values]) => {
      const topics = Map.groupBy(values, (assignment) => assignment.topic);
      const body = new Writer().i32(-1).array([...topics], (writer, [topic, partitions]) => {
        writer.string(topic).array(partitions, (partitionWriter, value) => {
          partitionWriter.i32(value.partition).i64(value.which === "earliest" ? -2 : -1);
        });
      });
      const response = await this.#cluster.request(leader, API_LIST_OFFSETS, 1, body);
      for (const result of response.array((topicReader) => {
        const topic = topicReader.string() ?? "";
        return topicReader.array((partitionReader) => {
          const partition = partitionReader.i32();
          const error = partitionReader.i16();
          partitionReader.i64();
          const offset = partitionReader.i64();
          return { topic, partition, error, offset };
        });
      }).flat()) {
        if (result.error) throw kafkaError(result.error, `${result.topic}[${result.partition}]`);
        this.#positions.set(partitionKey(result.topic, result.partition), result.offset);
      }
    }));
  }

  async fetch(options: FetchOptions = {}): Promise<Array<ConsumedMessage<K, V>>> {
    this.#open();
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.#cluster.retryOptions.maxRetries; attempt++) {
      try {
        const messages = await this.#fetchOnce(options);
        if (messages.length && this.#groupId && this.#options.autoCommit) await this.commitOffsets();
        return messages;
      } catch (error) {
        lastError = error;
        if (!(error instanceof KafkaError && error.retriable) || attempt === this.#cluster.retryOptions.maxRetries) throw error;
        for (const assigned of this.#assigned.values()) {
          const metadata = await this.#cluster.topic(assigned.topic, true);
          const partition = metadata.partitions.find((item) => item.id === assigned.partition);
          if (partition) assigned.leader = partition.leader;
        }
        this.#cluster.bumpRetries();
        const delay = retryDelay(this.#cluster.retryOptions, attempt);
        this.#cluster.log("warn", `retrying fetch attempt ${attempt + 1} in ${delay}ms: ${String(error)}`);
        this.#cluster.event({ type: "retry", apiKey: API_FETCH, attempt: attempt + 1, delayMs: delay, error });
        if (delay) await Bun.sleep(delay);
      }
    }
    throw lastError;
  }

  async #fetchOnce(options: FetchOptions = {}): Promise<Array<ConsumedMessage<K, V>>> {
    this.#open();
    const maxMessages = options.maxMessages ?? 500;
    if (!Number.isSafeInteger(maxMessages) || maxMessages < 1) throw new RangeError("maxMessages must be a positive integer");
    if (this.#decoders.length) return this.#drain(maxMessages);
    const active = [...this.#assigned].filter(([key]) => !this.#paused.has(key));
    if (!active.length) {
      await Bun.sleep(options.maxWaitMs ?? 500);
      return [];
    }

    const isolationLevel = this.#options.isolationLevel === "read_committed" ? 1 : 0;
    const leaders = Map.groupBy(active, ([, assignment]) => assignment.leader);
    const batches = await Promise.all([...leaders].map(async ([leader, entries]) =>
      this.#fetchBatchesFor(leader, entries, options, isolationLevel)));
    this.#decoders.push(...batches.flat());
    return this.#drain(maxMessages);
  }

  async #fetchBatchesFor(leader: number, entries: Array<[string, Assigned]>, options: FetchOptions, isolationLevel: number): Promise<RecordSetDecoder[]> {
      const session = this.#fetchSessions.get(leader) ?? { id: 0, epoch: 0, sent: new Map<string, bigint>(), streaming: new Map<string, boolean>() };
      const topics = Map.groupBy(entries, ([, assignment]) => assignment.topic);
      const requested: Array<[string, Assigned]> = [];
      for (const [topic, partitions] of topics) {
        for (const entry of partitions) {
          const key = entry[0];
          const position = this.#positions.get(key) ?? 0n;
          // Incremental fetch (KIP-227): include a partition when its position
          // changed since the last request in this session, or when the previous
          // response still carried records. Omitted partitions stay monitored by
          // the broker's session and reappear in responses once data arrives.
          if (session.epoch === 0 || session.sent.get(key) !== position || session.streaming.get(key)) {
            requested.push(entry);
          }
        }
      }
      // Partitions that left the assignment are explicitly forgotten so the
      // broker stops monitoring them in this session.
      const forgottenTopics = new Map<string, number[]>();
      for (const key of [...session.sent.keys(), ...session.streaming.keys()]) {
        if (this.#assigned.has(key) && !this.#paused.has(key)) continue;
        const [topic, partition] = key.split("\0");
        if (topic === undefined || partition === undefined) continue;
        forgottenTopics.set(topic, [...forgottenTopics.get(topic) ?? [], Number(partition)]);
        session.sent.delete(key);
        session.streaming.delete(key);
      }
      const byTopic = Map.groupBy(requested, ([, assignment]) => assignment.topic);
      const body = new Writer().i32(-1).i32(options.maxWaitMs ?? 500).i32(options.minBytes ?? 1)
        .i32(options.maxBytes ?? this.#options.fetchMaxBytes ?? 50 * 1024 * 1024).i8(isolationLevel)
        .i32(session.id).i32(session.epoch)
        .array([...byTopic], (writer, [topic, partitions]) => {
          writer.string(topic).array(partitions, (partitionWriter, [key, assignment]) => {
            partitionWriter.i32(assignment.partition).i64(this.#positions.get(key) ?? 0n)
              .i64(-1)
              .i32(options.maxPartitionBytes ?? 1024 * 1024);
          });
        })
        .array([...forgottenTopics], (writer, [topic, partitions]) => {
          writer.string(topic).array(partitions, (partitionWriter, partition) => partitionWriter.i32(partition));
        });
      // NOTE: rack_id only exists in Fetch v11+; appending it here would leak
      // stray bytes that brokers decode as the start of the next request.
      const response = await this.#cluster.request(leader, API_FETCH, 7, body, (options.maxWaitMs ?? 500) + this.#cluster.requestTimeoutMs, false);
      if (process.env.DEBUG_FETCH) console.error("fetch resp:", Array.from(response.data.slice(0, 80)).map(b=>b.toString(16).padStart(2,"0")).join(" "));
      this.#cluster.throttle(API_FETCH, response.i32());
      const topError = response.i16();
      const sessionId = response.i32();
      if (topError) {
        // Session state drifted (broker restart or expiry): start a fresh one.
        this.#fetchSessions.delete(leader);
        if (topError === 70 || topError === 71) return this.#fetchBatchesFor(leader, entries, options, isolationLevel);
        throw kafkaError(topError, `Fetch from broker ${leader}`);
      }
      if (session.id === 0) {
        session.id = sessionId;
        session.epoch = 1;
      } else {
        session.epoch += 1;
      }
      this.#fetchSessions.set(leader, session);
      return response.array((topicReader) => {
        const topic = topicReader.string() ?? "";
        return topicReader.array((partitionReader) => {
          const partition = partitionReader.i32();
          const error = partitionReader.i16();
          partitionReader.i64(); // high watermark
          partitionReader.i64(); // last stable offset
          partitionReader.i64(); // log start offset (Fetch v5+)
          const abortedTransactions = partitionReader.array((abortedReader) => ({ producerId: abortedReader.i64(), firstOffset: abortedReader.i64() }));
          const records = partitionReader.bytes();
          const key = partitionKey(topic, partition);
          // Remember the requested offset so idle partitions can be pruned
          // from subsequent incremental requests.
          session.sent.set(key, this.#positions.get(key) ?? 0n);
          session.streaming.set(key, Boolean(records));
          if (error) throw kafkaError(error, `${topic}[${partition}]`);
          return records ? new RecordSetDecoder(records, topic, partition, leader, {
            minOffset: this.#positions.get(key) ?? 0n,
            copy: options.copy,
            abortedTransactions: isolationLevel === 1 ? abortedTransactions : undefined,
          }) : null;
        }).filter((decoder): decoder is RecordSetDecoder => decoder !== null);
      }).flat();
  }

  #drain(max: number): Array<ConsumedMessage<K, V>> {
    const messages: Array<ConsumedMessage<K, V>> = [];
    while (this.#decoders.length && messages.length < max) {
      const decoder = this.#decoders[0]!;
      const next = decoder.read(max - messages.length);
      for (const message of next) {
        this.#positions.set(partitionKey(message.topic, message.partition), message.offset + 1n);
        if (!this.#options.keyDeserializer && !this.#options.valueDeserializer) {
          // Raw path only runs with no deserializers, so key/value really are bytes.
          messages.push(message as ConsumedMessage<K, V>);
          continue;
        }
        const context = { topic: message.topic, partition: message.partition, offset: message.offset, timestamp: message.timestamp };
        messages.push({
          ...message,
          key: this.#options.keyDeserializer?.(message.key, context) ?? null,
          value: this.#options.valueDeserializer?.(message.value, context) ?? null,
        } as ConsumedMessage<K, V>);
      }
      if (decoder.done) this.#decoders.shift();
    }
    return messages;
  }

  async *messages(options: FetchOptions = {}): AsyncGenerator<ConsumedMessage<K, V>, void, unknown> {
    while (!this.#closed) {
      const messages = await this.fetch(options);
      for (const message of messages) yield message;
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<ConsumedMessage<K, V>> {
    return this.messages();
  }

  seek(assignment: TopicPartition): void {
    this.#open();
    if (assignment.offset === undefined) throw new TypeError("seek requires an offset");
    const key = partitionKey(assignment.topic, assignment.partition);
    if (!this.#assigned.has(key)) throw new Error(`${assignment.topic}[${assignment.partition}] is not assigned`);
    this.#positions.set(key, BigInt(assignment.offset));
    this.#decoders = this.#decoders.filter((decoder) => partitionKey(decoder.topic, decoder.partition) !== key);
    // Force the re-positioned partition into the next fetch request.
    for (const session of this.#fetchSessions.values()) {
      session.sent.delete(key);
      session.streaming.delete(key);
    }
  }

  pause(partitions: TopicPartition[]): void {
    this.#open();
    for (const partition of partitions) this.#paused.add(partitionKey(partition.topic, partition.partition));
  }

  resume(partitions: TopicPartition[]): void {
    this.#open();
    for (const partition of partitions) {
      const key = partitionKey(partition.topic, partition.partition);
      this.#paused.delete(key);
      // Re-join the fetch session so the resumed partition is polled again.
      for (const session of this.#fetchSessions.values()) {
        session.sent.delete(key);
        session.streaming.delete(key);
      }
    }
  }

  assignment(): TopicPartition[] {
    this.#open();
    return [...this.#assigned].map(([key, value]) => ({
      topic: value.topic,
      partition: value.partition,
      offset: this.#positions.get(key),
    }));
  }

  position(topic: string, partition: number): bigint | undefined {
    this.#open();
    return this.#positions.get(partitionKey(topic, partition));
  }

  async watermarks(topic: string, partition: number): Promise<Watermarks> {
    const metadata = await this.#cluster.topic(topic);
    const leader = metadata.partitions.find((item) => item.id === partition)?.leader;
    if (leader === undefined) throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
    const query = async (timestamp: number) => {
      const body = new Writer().i32(-1).array([topic], (writer) => writer.string(topic).array([partition], (partitionWriter) => {
        partitionWriter.i32(partition).i64(timestamp);
      }));
      const response = await this.#cluster.request(leader, API_LIST_OFFSETS, 1, body);
      const result = response.array((topicReader) => {
        topicReader.string();
        return topicReader.array((partitionReader) => {
          partitionReader.i32();
          const error = partitionReader.i16();
          partitionReader.i64();
          const offset = partitionReader.i64();
          if (error) throw kafkaError(error, `${topic}[${partition}]`);
          return offset;
        });
      });
      return result[0]?.[0] ?? -1n;
    };
    const [low, high] = await Promise.all([query(-2), query(-1)]);
    return { low, high };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    if (this.#groupId && this.#coordinator !== undefined && this.#generationId >= 0) {
      try {
        const instanceId = this.#options.groupInstanceId;
        if (instanceId === undefined) {
          await this.#cluster.request(this.#coordinator, API_LEAVE_GROUP, 0, new Writer().string(this.#groupId).string(this.#memberId));
        } else {
          // LeaveGroup v3+ sends a member list that carries static identity.
          await this.#cluster.request(this.#coordinator, API_LEAVE_GROUP, 3, new Writer().string(this.#groupId).array([{ memberId: this.#memberId, instanceId }], (writer, member) => {
            writer.string(member.memberId).string(member.instanceId ?? null);
          }));
        }
      } catch {
        // The broker may already be unavailable during shutdown.
      }
    }
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
  #open(): void { if (this.#closed) throw new Error("Consumer is closed"); }
}

export interface CreateTopicInput {
  name: string;
  numPartitions: number;
  replicationFactor?: number;
  assignments?: number[][];
  configs?: Record<string, string | null>;
}

export interface TopicResult {
  name: string;
  error: number;
  message: string | null;
}

export interface CreatePartitionsInput {
  name: string;
  count: number;
  assignments?: number[][];
}

export interface ConfigResource {
  resourceType: number;
  resourceName: string;
  error: number;
  message: string | null;
  configs: Array<{ name: string; value: string | null; source: number; sensitive: boolean; readOnly: boolean }>;
}

export class BunAdmin {
  #cluster: Cluster;
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;

  constructor(options: KafkaOptions | Cluster, onClose = () => {}) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = this.#ownsCluster ? new Cluster(options as KafkaOptions) : options as Cluster;
    this.#onClose = onClose;
  }

  metadata(topics: string[] | null = null): Promise<ClusterMetadata> {
    if (this.#closed) throw new Error("Admin is closed");
    return this.#cluster.metadata(topics);
  }

  async createTopics(topics: readonly CreateTopicInput[], options: { timeoutMs?: number; validateOnly?: boolean } = {}): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) return [];
    const body = new Writer().array(topics, (writer, topic) => {
      writer.string(topic.name).i32(topic.numPartitions).i16(topic.replicationFactor ?? -1)
        .array(topic.assignments ? topic.assignments.map((brokers, partition) => ({ partition, brokers })) : [], (assignmentWriter, assignment) => assignmentWriter.i32(assignment.partition).array(assignment.brokers, (writer, broker) => writer.i32(broker)))
        .array(topic.configs ? Object.entries(topic.configs) : [], (writer, [name, value]) => writer.string(name).string(value));
    }).i32(options.timeoutMs ?? 30_000).bool(options.validateOnly ?? false);
    const response = await this.#cluster.controllerRequest(API_CREATE_TOPICS, 4, body);
    this.#cluster.throttle(API_CREATE_TOPICS, response.i32());
    return response.array((reader) => ({ name: reader.string() ?? "", error: reader.i16(), message: reader.string() }));
  }

  async deleteTopics(topics: readonly string[], options: { timeoutMs?: number } = {}): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) return [];
    const response = await this.#cluster.controllerRequest(API_DELETE_TOPICS, 3, new Writer().array(topics, (writer, topic) => writer.string(topic)).i32(options.timeoutMs ?? 30_000));
    this.#cluster.throttle(API_DELETE_TOPICS, response.i32());
    return response.array((reader) => ({ name: reader.string() ?? "", error: reader.i16(), message: null }));
  }

  async createPartitions(topics: readonly CreatePartitionsInput[], options: { timeoutMs?: number; validateOnly?: boolean } = {}): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) return [];
    const body = new Writer().array(topics, (writer, topic) => {
      writer.string(topic.name).i32(topic.count).array(topic.assignments ?? [], (assignmentWriter, assignment) => assignmentWriter.array(assignment, (writer, broker) => writer.i32(broker)));
    }).i32(options.timeoutMs ?? 30_000).bool(options.validateOnly ?? false);
    const response = await this.#cluster.controllerRequest(API_CREATE_PARTITIONS, 2, body);
    this.#cluster.throttle(API_CREATE_PARTITIONS, response.i32());
    return response.array((reader) => ({ name: reader.string() ?? "", error: reader.i16(), message: reader.string() }));
  }

  async describeConfigs(resources: readonly { resourceType: number; resourceName: string; configNames?: string[] | null }[]): Promise<ConfigResource[]> {
    this.#open();
    const body = new Writer().array(resources, (writer, resource) => writer.i8(resource.resourceType).string(resource.resourceName).array(resource.configNames ?? null, (writer, name) => writer.string(name)));
    const response = await this.#cluster.anyRequest(API_DESCRIBE_CONFIGS, 0, body);
    this.#cluster.throttle(API_DESCRIBE_CONFIGS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      const resourceType = reader.i8();
      const resourceName = reader.string() ?? "";
      const configs = reader.array((configReader) => {
        const name = configReader.string() ?? "";
        const value = configReader.string();
        const readOnly = configReader.bool();
        const isDefault = configReader.bool();
        const sensitive = configReader.bool();
        return { name, value, source: isDefault ? 5 : 0, sensitive, readOnly };
      });
      return { resourceType, resourceName, error, message, configs };
    });
  }

  async alterConfigs(resources: readonly { resourceType: number; resourceName: string; configs: Record<string, string | null> }[]): Promise<TopicResult[]> {
    this.#open();
    const body = new Writer().array(resources, (writer, resource) => writer.i8(resource.resourceType).string(resource.resourceName)
      .array(Object.entries(resource.configs), (writer, [name, value]) => writer.string(name).string(value)));
    const response = await this.#cluster.anyRequest(API_ALTER_CONFIGS, 0, body);
    this.#cluster.throttle(API_ALTER_CONFIGS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      reader.i8();
      const name = reader.string() ?? "";
      return { name, error, message };
    });
  }

  /** List consumer groups known to the cluster coordinator. */
  async listGroups(statesFilter: readonly string[] = []): Promise<Array<{ groupId: string; protocolType: string; state: string }>> {
    this.#open();
    const response = await this.#cluster.anyRequest(API_LIST_GROUPS, 1, new Writer().array(statesFilter, (writer, state) => writer.string(state)));
    this.#cluster.throttle(API_LIST_GROUPS, response.i32());
    const error = response.i16();
    if (error) throw kafkaError(error, "ListGroups");
    return response.array((reader) => ({
      groupId: reader.string() ?? "",
      protocolType: reader.string() ?? "",
      state: reader.string() ?? "",
    }));
  }

  /** Describe consumer groups: state and member details. */
  async describeGroups(groupIds: readonly string[]): Promise<GroupDescription[]> {
    this.#open();
    if (!groupIds.length) return [];
    const body = new Writer().array(groupIds, (writer, group) => writer.string(group));
    const response = await this.#cluster.anyRequest(API_DESCRIBE_GROUPS, 1, body);
    this.#cluster.throttle(API_DESCRIBE_GROUPS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      const groupId = reader.string() ?? "";
      const state = reader.string() ?? "";
      const protocolType = reader.string() ?? "";
      const protocol = reader.string();
      const members = reader.array((memberReader) => ({
        memberId: memberReader.string() ?? "",
        clientId: memberReader.string() ?? "",
        clientHost: memberReader.string() ?? "",
        memberMetadata: memberReader.bytes(),
        memberAssignment: memberReader.bytes(),
      }));
      return { error, message, groupId, state, protocolType, protocol, members };
    });
  }

  /** Delete consumer groups that no longer have active members. */
  async deleteGroups(groupIds: readonly string[]): Promise<TopicResult[]> {
    this.#open();
    if (!groupIds.length) return [];
    const body = new Writer().array(groupIds, (writer, group) => writer.string(group));
    const response = await this.#cluster.anyRequest(API_DELETE_GROUPS, 1, body);
    this.#cluster.throttle(API_DELETE_GROUPS, response.i32());
    return response.array((reader) => ({ name: reader.string() ?? "", error: reader.i16(), message: null }));
  }

  /** Truncate topic partitions below the given offsets; returns the resulting low watermark per partition. */
  async deleteRecords(topics: ReadonlyArray<{ name: string; partitions: ReadonlyArray<{ index: number; offset: bigint }> }>, options: { timeoutMs?: number } = {}): Promise<DeleteRecordsResult[]> {
    this.#open();
    if (!topics.length) return [];
    const body = new Writer().array(topics, (writer, topic) =>
      writer.string(topic.name).array(topic.partitions, (partitionWriter, partition) =>
        partitionWriter.i32(partition.index).i64(partition.offset)))
      .i32(options.timeoutMs ?? 30_000);
    const response = await this.#cluster.anyRequest(API_DELETE_RECORDS, 1, body);
    this.#cluster.throttle(API_DELETE_RECORDS, response.i32());
    return response.array((topicReader) => {
      const name = topicReader.string() ?? "";
      return topicReader.array((partitionReader) => {
        const index = partitionReader.i32();
        const lowWatermark = partitionReader.i64();
        const error = partitionReader.i16();
        return { name, index, lowWatermark, error };
      });
    }).flat();
  }

  /** Describe client quota entities matching the supplied filters (DescribeClientQuotas v0). */
  async describeClientQuotas(components: ReadonlyArray<{ entityType: string; matchType?: "exact" | "defaulted" | "any"; match: string | null }> = [], options: { strict?: boolean } = {}): Promise<Array<{ entities: Array<{ entityType: string; entityName: string | null }>; values: Array<{ name: string; value: number }> }>> {
    this.#open();
    const matchTypes = { exact: 0, defaulted: 1, any: 2 } as const;
    const body = new Writer().compactArray(components, (writer, component) =>
      writer.compactString(component.entityType).i8(matchTypes[component.matchType ?? "exact"]).compactString(component.match))
      .bool(options.strict ?? false)
      .tags();
    const response = await this.#cluster.anyRequest(API_DESCRIBE_CLIENT_QUOTAS, 1, body, true);
    this.#cluster.throttle(API_DESCRIBE_CLIENT_QUOTAS, response.i32());
    const error = response.i16();
    // The nullable error_message field must be consumed regardless of outcome.
    const errorMessage = response.compactString();
    if (error) throw kafkaError(error, "Describe client quotas", errorMessage);
    return response.compactArray((entryReader) => {
      const entry = {
        entities: entryReader.compactArray((entityReader) => {
          const entity = { entityType: entityReader.compactString() ?? "", entityName: entityReader.compactString() };
          entityReader.skipTags(); // per-struct tagged fields (KIP-482)
          return entity;
        }),
        values: entryReader.compactArray((valueReader) => {
          const value = { name: valueReader.compactString() ?? "", value: valueReader.f64() };
          valueReader.skipTags();
          return value;
        }),
      };
      entryReader.skipTags(); // entry-level tags
      return entry;
    });
  }

  /** Alter client quota entity values (AlterClientQuotas v1). */
  async alterClientQuotas(entries: ReadonlyArray<{ entity: ReadonlyArray<{ entityType: string; entityName: string | null }>; ops: ReadonlyArray<{ key: string; value?: number; remove?: boolean }>; validateOnly?: boolean }>): Promise<Array<{ error: number; message: string | null; entity: Array<{ entityType: string; entityName: string | null }> }>> {
    this.#open();
    if (!entries.length) return [];
    // Flexible versions close every struct (array elements included) with a
    // tagged-field section.
    const body = new Writer().compactArray(entries, (writer, entry) => {
      writer.compactArray(entry.entity, (entityWriter, item) => entityWriter.compactString(item.entityType).compactString(item.entityName).tags());
      writer.compactArray(entry.ops, (opsWriter, op) => opsWriter.compactString(op.key).f64(op.value ?? Number.NaN).bool(op.remove ?? false).tags());
      writer.tags(); // entry-level tags
    }).bool(entries.some((entry) => entry.validateOnly) ?? false).tags();
    const response = await this.#cluster.anyRequest(API_ALTER_CLIENT_QUOTAS, 1, body, true);
    this.#cluster.throttle(API_ALTER_CLIENT_QUOTAS, response.i32());
    return response.compactArray((entryReader) => {
      const error = entryReader.i16();
      const message = entryReader.compactString();
      const entity = entryReader.compactArray((entityReader) => {
        const item = { entityType: entityReader.compactString() ?? "", entityName: entityReader.compactString() };
        entityReader.skipTags();
        return item;
      });
      entryReader.skipTags(); // entry-level tags
      return { error, message, entity };
    });
  }

  /**
   * Create a delegation token owned by the authenticated (or delegated-to)
   * principal (CreateDelegationToken v2). Delegation token APIs require SASL
   * authentication and broker support; brokers answer UNSUPPORTED_VERSION when
   * unavailable.
   */
  async createDelegationToken(options: { ownerPrincipalType?: string | null; ownerPrincipalName?: string | null; renewalPeriodMs?: number | bigint } = {}): Promise<{ error: number; principalType: string; principalName: string; tokenRequester: string; issueTimestampMs: bigint; expiryTimestampMs: bigint; maxTimestampMs: bigint; tokenId: string; hmac: Uint8Array | null }> {
    this.#open();
    const body = new Writer().compactString(options.ownerPrincipalType ?? null)
      .compactString(options.ownerPrincipalName ?? null)
      .i64(options.renewalPeriodMs === undefined ? -1n : BigInt(options.renewalPeriodMs))
      .tags();
    const response = await this.#cluster.anyRequest(API_CREATE_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_CREATE_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Create delegation token", message);
    return {
      error,
      principalType: response.compactString() ?? "",
      principalName: response.compactString() ?? "",
      tokenRequester: response.compactString() ?? "",
      issueTimestampMs: response.i64(),
      expiryTimestampMs: response.i64(),
      maxTimestampMs: response.i64(),
      tokenId: response.compactString() ?? "",
      hmac: response.compactBytes(),
    };
  }

  /** List delegation tokens visible to the authenticated principal (DescribeDelegationToken v2). */
  async describeDelegationTokens(ownersFilters: ReadonlyArray<{ principalType: string; principalName: string }> = []): Promise<Array<{ ownerPrincipalType: string; ownerPrincipalName: string; tokenRequester: string; issueTimestampMs: bigint; expiryTimestampMs: bigint; maxTimestampMs: bigint; tokenId: string; hmac: Uint8Array | null }>> {
    this.#open();
    const body = new Writer().compactArray(ownersFilters.length ? ownersFilters : null, (writer, owner) =>
      writer.compactString(owner.principalType).compactString(owner.principalName).tags()).tags();
    const response = await this.#cluster.anyRequest(API_DESCRIBE_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_DESCRIBE_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Describe delegation tokens", message);
    return response.compactArray((tokenReader) => {
      const token = {
        ownerPrincipalType: tokenReader.compactString() ?? "",
        ownerPrincipalName: tokenReader.compactString() ?? "",
        tokenRequester: tokenReader.compactString() ?? "",
        issueTimestampMs: tokenReader.i64(),
        expiryTimestampMs: tokenReader.i64(),
        maxTimestampMs: tokenReader.i64(),
        tokenId: tokenReader.compactString() ?? "",
        hmac: tokenReader.compactBytes(),
      };
      tokenReader.skipTags();
      return token;
    });
  }

  /** Renew a delegation token before its expiry (RenewDelegationToken v2). */
  async renewDelegationToken(tokenHmac: Uint8Array, renewPeriodMs: number | bigint): Promise<{ error: number; expiryTimestampMs: bigint }> {
    this.#open();
    const body = new Writer().compactBytes(tokenHmac).i64(BigInt(renewPeriodMs)).tags();
    const response = await this.#cluster.anyRequest(API_RENEW_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_RENEW_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Renew delegation token", message);
    return { error, expiryTimestampMs: response.i64() };
  }

  /** Expire a delegation token early (ExpireDelegationToken v2). */
  async expireDelegationToken(tokenHmac: Uint8Array, expiryTimePeriodMs: number | bigint = -1): Promise<{ error: number; expiryTimestampMs: bigint }> {
    this.#open();
    const body = new Writer().compactBytes(tokenHmac).i64(BigInt(expiryTimePeriodMs)).tags();
    const response = await this.#cluster.anyRequest(API_EXPIRE_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_EXPIRE_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Expire delegation token", message);
    return { error, expiryTimestampMs: response.i64() };
  }

  /** Create ACL bindings on the broker. */
  async createAcls(bindings: readonly AclBinding[], options: { timeoutMs?: number } = {}): Promise<Array<{ error: number; message: string | null }>> {
    this.#open();
    if (!bindings.length) return [];
    const body = new Writer().array(bindings, (writer, acl) =>
      writer.i8(acl.resourceType).string(acl.resourceName)
        .string(acl.principal).string(acl.host)
        .i8(acl.operation).i8(acl.permissionType))
      .i32(options.timeoutMs ?? 30_000);
    const response = await this.#cluster.anyRequest(API_CREATE_ACLS, 0, body);
    this.#cluster.throttle(API_CREATE_ACLS, response.i32());
    return response.array((reader) => ({ error: reader.i16(), message: reader.string() }));
  }

  /** List ACLs matching the filter; null filter fields match anything. */
  async describeAcls(filter: AclFilter): Promise<{ error: number; message: string | null; acls: AclListing[] }> {
    this.#open();
    const body = new Writer()
      .i8(filter.resourceType)
      .string(filter.resourceName ?? null)
      .string(filter.principal ?? null)
      .string(filter.host ?? null)
      .i8(filter.operation)
      .i8(filter.permissionType);
    const response = await this.#cluster.anyRequest(API_DESCRIBE_ACLS, 0, body);
    this.#cluster.throttle(29, response.i32());
    const error = response.i16();
    const message = response.string();
    const acls = response.array((reader) => {
      const resourceType = reader.i8();
      const resourceName = reader.string() ?? "";
      return reader.array((aclReader) => ({
        resourceType,
        resourceName,
        principal: aclReader.string() ?? "",
        host: aclReader.string() ?? "",
        operation: aclReader.i8(),
        permissionType: aclReader.i8(),
      }));
    }).flat();
    return { error, message, acls };
  }

  /** Delete ACLs matching the filters; null filter fields match anything. */
  async deleteAcls(filters: readonly AclFilter[], options: { timeoutMs?: number } = {}): Promise<Array<{ error: number; message: string | null; acls: AclListing[] }>> {
    this.#open();
    const body = new Writer().array(filters, (writer, filter) =>
      writer.i8(filter.resourceType)
        .string(filter.resourceName ?? null)
        .string(filter.principal ?? null)
        .string(filter.host ?? null)
        .i8(filter.operation)
        .i8(filter.permissionType))
      .i32(options.timeoutMs ?? 30_000);
    const response = await this.#cluster.anyRequest(API_DELETE_ACLS, 0, body);
    this.#cluster.throttle(API_DELETE_ACLS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      // Matching ACLs are flat; each carries its own error code/message.
      const acls = reader.array((aclReader) => ({
        error: aclReader.i16(),
        message: aclReader.string(),
        resourceType: aclReader.i8(),
        resourceName: aclReader.string() ?? "",
        principal: aclReader.string() ?? "",
        host: aclReader.string() ?? "",
        operation: aclReader.i8(),
        permissionType: aclReader.i8(),
      }));
      return { error, message, acls };
    });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
  #open(): void { if (this.#closed) throw new Error("Admin is closed"); }
}

export interface GroupMemberDescription {
  memberId: string;
  clientId: string;
  clientHost: string;
  memberMetadata: Uint8Array | null;
  memberAssignment: Uint8Array | null;
}

export interface GroupDescription {
  error: number;
  message: string | null;
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string | null;
  members: GroupMemberDescription[];
}

export type DeleteRecordsResult = {
  name: string;
  index: number;
  lowWatermark: bigint;
  error: number;
};

export type AclResourceType = number;
export type AclOperation = number;
export type AclPermissionType = number;

export interface AclBinding {
  resourceType: AclResourceType;
  resourceName: string;
  principal: string;
  host: string;
  operation: AclOperation;
  permissionType: AclPermissionType;
}

export type AclFilter = {
  resourceType: AclResourceType;
  resourceName?: string;
  principal?: string;
  host?: string;
  operation: AclOperation;
  permissionType: AclPermissionType;
};

export interface AclListing {
  error?: number;
  message?: string | null;
  resourceType: number;
  resourceName: string;
  principal: string;
  host: string;
  operation: number;
  permissionType: number;
}

export class Kafka {
  #cluster: Cluster;
  #clients = new Set<{ close(): Promise<void> }>();

  constructor(options: KafkaOptions) {
    this.#cluster = new Cluster({ ...options, brokers: [...options.brokers] });
    if (options.statsIntervalMs !== undefined) this.#cluster.trackStats(options.statsIntervalMs);
  }

  /** Aggregate client counters (requests, bytes, retries, throttles). */
  stats(): ClusterStats {
    return this.#cluster.stats();
  }

  /** Ping all known brokers and report per-broker latency. */
  healthCheck(timeoutMs?: number): Promise<HealthReport> {
    return this.#cluster.healthCheck(timeoutMs ?? Math.min(this.#cluster.requestTimeoutMs, 5_000));
  }

  producer(options: ProducerOptions = {}): BunProducer {
    let producer: BunProducer;
    producer = new BunProducer(this.#cluster, options, () => this.#clients.delete(producer));
    this.#clients.add(producer);
    return producer;
  }

  consumer<K = Uint8Array | null, V = Uint8Array | null>(options: ConsumerOptions<K, V> = {}): BunConsumer<K, V> {
    let consumer: BunConsumer<K, V>;
    consumer = new BunConsumer(this.#cluster, options, () => this.#clients.delete(consumer));
    this.#clients.add(consumer);
    return consumer;
  }

  admin(): BunAdmin {
    let admin: BunAdmin;
    admin = new BunAdmin(this.#cluster, () => this.#clients.delete(admin));
    this.#clients.add(admin);
    return admin;
  }

  async disconnect(): Promise<void> {
    await Promise.all([...this.#clients].map((client) => client.close()));
    this.#cluster.close();
  }
}
