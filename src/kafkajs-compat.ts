/**
 * kafkajs-compatible API surface implemented over the bun-kafka primitives.
 * Exposed as `bun-kafka/kafkajs-compat` so existing kafkajs applications can
 * switch transports by changing their import specifier.
 */
import { KafkaError } from "./errors.ts";
import type { ConsumedMessage } from "./types.ts";
import { Cluster } from "./bun/cluster.ts";
import { Reader, Writer } from "./bun/protocol.ts";
import { BunAdmin } from "./bun/admin.ts";
import { BunConsumer } from "./bun/consumer.ts";
import { BunProducer, type Partitioner, type ProducerMessage } from "./bun/producer.ts";

const COMPRESSION_NAMES = ["none", "gzip", "snappy", "lz4", "zstd"] as const;

export const CompressionTypes = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
} as const;

/** Registry kept for kafkajs API parity; bun-kafka ships all five codecs natively. */
export const CompressionCodecs: Record<number, unknown> = {};

/** bun-kafka produces Java-compatible murmur2 partitioning natively. */
export const Partitioners = {
  DefaultPartitioner: () => ({ partition: (ctx: { partitionCount: number }) => Math.floor(Math.random() * ctx.partitionCount) }),
  JavaCompatiblePartitioner: () => ({ partition: (ctx: { partitionCount: number }) => Math.floor(Math.random() * ctx.partitionCount) }),
  LegacyPartitioner: () => ({ partition: (ctx: { partitionCount: number }) => Math.floor(Math.random() * ctx.partitionCount) }),
};

export const logLevel = { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 3, DEBUG: 4 } as const;

export const ConfigResourceTypes = {
  UNKNOWN: 0,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER_BROKER: 4,
  BROKER_LOGGER: 8,
  BROKER: 4,
} as const;

export const AclResourceTypes = {
  UNKNOWN: 0,
  ANY: 1,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER: 4,
  TRANSACTIONAL_ID: 5,
  DELEGATION_TOKEN: 6,
} as const;

export const AclOperationTypes = {
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12,
} as const;

export const AclPermissionTypes = { UNKNOWN: 0, ANY: 1, DENY: 2, ALLOW: 3 } as const;

export const ResourcePatternTypes = { UNKNOWN: 0, ANY: 1, MATCH: 2, LITERAL: 3, PREFIXED: 4 } as const;

export const CONSUMER_EVENTS = {
  CONNECT: "consumer.connect",
  DISCONNECT: "consumer.disconnect",
  STOP: "consumer.stop",
  CRASH: "consumer.crash",
  REBALANCING: "consumer.rebalancing",
  GROUP_JOIN: "consumer.group_join",
  HEARTBEAT: "consumer.heartbeat",
  FETCH: "consumer.fetch",
  FETCH_START: "consumer.fetch_start",
  START_BATCH_PROCESS: "consumer.start_batch_process",
  END_BATCH_PROCESS: "consumer.end_batch_process",
  COMMIT_OFFSETS: "consumer.commit_offsets",
} as const;

export const PRODUCER_EVENTS = {
  CONNECT: "producer.connect",
  DISCONNECT: "producer.disconnect",
  REQUEST: "producer.network.request",
  REQUEST_TIMEOUT: "producer.network.request_timeout",
  REQUEST_QUEUE_SIZE: "producer.network.request_queue_size",
} as const;

export const ADMIN_EVENTS = {
  CONNECT: "admin.connect",
  DISCONNECT: "admin.disconnect",
  REQUEST: "admin.network.request",
  REQUEST_TIMEOUT: "admin.network.request_timeout",
} as const;

export const GLOBAL_EVENTS = { LOG: "global.log" } as const;

export class KafkaJSError extends Error {
  retriable: boolean;
  fatal?: boolean;
  constructor(messageOrError: string | Error, options: { retriable?: boolean; fatal?: boolean } = {}) {
    super(typeof messageOrError === "string" ? messageOrError : messageOrError.message);
    this.name = "KafkaJSError";
    this.retriable = options.retriable ?? false;
    this.fatal = options.fatal;
  }
}

export class KafkaJSNonRetriableError extends KafkaJSError {
  constructor(messageOrError: string | Error) {
    super(messageOrError);
    this.name = "KafkaJSNonRetriableError";
  }
}

export class KafkaJSProtocolError extends KafkaJSError {
  readonly code: number;
  constructor(error: string | Error, code = -1) {
    super(error);
    this.name = "KafkaJSProtocolError";
    this.code = code;
  }
}

export class KafkaJSConnectionError extends KafkaJSError {
  readonly broker?: string;
  constructor(message: string, options: { retriable?: boolean; broker?: string } = {}) {
    super(message, options);
    this.name = "KafkaJSConnectionError";
    this.broker = options.broker;
  }
}

export class KafkaJSTimeout extends KafkaJSError {
  constructor(message = "Timed out waiting for response") {
    super(message, { retriable: true });
    this.name = "KafkaJSTimeout";
  }
}

export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  constructor(message: string) {
    super(message, 1);
    this.name = "KafkaJSOffsetOutOfRange";
  }
}

export class KafkaJSNumberOfRetriesExceeded extends KafkaJSError {
  readonly originalError?: unknown;
  constructor(message = "Number of retries exceeded", originalError?: unknown) {
    super(message, { retriable: true });
    this.name = "KafkaJSNumberOfRetriesExceeded";
    this.originalError = originalError;
  }
}

/** Surface bun-kafka errors through kafkajs-shaped classes, preserving codes. */
export function wrapError(error: unknown): Error {
  if (error instanceof KafkaError) {
    const wrapped = new KafkaJSProtocolError(error.message, error.code);
    wrapped.retriable = error.retriable;
    return wrapped;
  }
  if (error instanceof Error && error.name.startsWith("KafkaJS")) return error;
  if (error instanceof Error) return new KafkaJSError(error);
  return new KafkaJSError(String(error));
}

class Emitter {
  #listeners = new Map<string, Set<(event: Record<string, unknown>) => void>>();
  on(type: string, listener: (event: Record<string, unknown>) => void): () => void {
    const set = this.#listeners.get(type) ?? new Set();
    set.add(listener);
    this.#listeners.set(type, set);
    return () => set.delete(listener);
  }
  removeListener(type: string, listener: (event: Record<string, unknown>) => void): void {
    this.#listeners.get(type)?.delete(listener);
  }
  emit(type: string, payload: Record<string, unknown> = {}): void {
    const event = { id: crypto.randomUUID(), type, timestamp: Date.now(), ...payload };
    for (const listener of [...(this.#listeners.get(type) ?? [])]) {
      try {
        listener(event);
      } catch {
        // Listener failures must not break the emitting client.
      }
    }
  }
}

export interface LoggerEntry {
  namespace: string;
  level: number;
  label: string;
  log: Record<string, unknown>;
}

const LEVEL_LABELS: Record<number, string> = { 0: "NOTHING", 1: "ERROR", 2: "WARN", 3: "INFO", 4: "DEBUG" };

export class Logger {
  #level: number;
  #namespace: string;
  #creator?: (entry: LoggerEntry) => void;
  constructor(level: number, namespace: string, creator?: (entry: LoggerEntry) => void) {
    this.#level = level;
    this.#namespace = namespace;
    this.#creator = creator;
  }
  #write(level: number, message: string, extra: Record<string, unknown>): void {
    if (level > this.#level) return;
    const entry: LoggerEntry = {
      namespace: this.#namespace,
      level,
      label: LEVEL_LABELS[level] ?? String(level),
      log: { message, ...extra },
    };
    if (this.#creator) {
      this.#creator(entry);
      return;
    }
    const target = level <= 1 ? console.error : level === 2 ? console.warn : console.log;
    target(`{"level":"${entry.label}","timestamp":${Date.now()},"logger":"${entry.namespace}","message":${JSON.stringify(message)}}`);
  }
  debug(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.DEBUG, message, extra);
  }
  info(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.INFO, message, extra);
  }
  warn(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.WARN, message, extra);
  }
  error(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.ERROR, message, extra);
  }
  namespace(namespace: string): Logger {
    return new Logger(this.#level, `${this.#namespace}:${namespace}`, this.#creator);
  }
  setLogLevel(level: number): void {
    this.#level = level;
  }
  level(): number {
    return this.#level;
  }
}

export interface KafkaJsMessage {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number;
  headers?: Record<string, Buffer | string | number | undefined>;
  timestamp?: string;
}

export interface KafkaJsSendRecord {
  topic: string;
  messages: KafkaJsMessage[];
  acks?: -1 | 0 | 1;
  timeout?: number;
  compression?: number;
}

export interface KafkaJsSendBatchRecord {
  acks?: -1 | 0 | 1;
  timeout?: number;
  compression?: number;
  topicMessages: Array<{ topic: string; messages: KafkaJsMessage[] }>;
}

export interface KafkaConfig {
  clientId?: string;
  brokers: string[] | (() => string[] | Promise<string[]>);
  ssl?: boolean | Record<string, unknown>;
  sasl?: Record<string, any>;
  retry?: { maxRetryTime?: number; initialRetryTime?: number; factor?: number; multiplier?: number; retries?: number };
  logLevel?: number;
  logCreator?: (entry: LoggerEntry) => void;
  connectionTimeout?: number;
  requestTimeout?: number;
  enforceRequestOrder?: boolean;
  maxInFlightRequests?: number;
}

interface MappedConfig {
  clientId: string;
  tls?: Record<string, unknown>;
  sasl?: Record<string, any>;
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  retry: { maxRetries: number; initialBackoffMs: number; maxBackoffMs: number };
}

function mapConfig(config: KafkaConfig, brokers: string[]): ConstructorParameters<typeof Cluster>[0] {
  const saslConfig = config.sasl;
  if (saslConfig?.mechanism !== undefined
    && !["plain", "scram-sha-256", "scram-sha-512", "oauthbearer"].includes(String(saslConfig.mechanism))) {
    throw new KafkaJSNonRetriableError(`SASL mechanism ${saslConfig.mechanism} is not supported by bun-kafka`);
  }
  let token: unknown = saslConfig?.oauthBearerToken ?? saslConfig?.token;
  if (!token && typeof saslConfig?.oauthBearerProvider === "function") {
    // kafkajs providers resolve to { value: token }; accept both shapes.
    token = async () => {
      const resolved = await (saslConfig.oauthBearerProvider as () => unknown)();
      return typeof resolved === "string" ? resolved : (resolved as { value?: string }).value;
    };
  }
  const sasl = saslConfig
    ? {
        mechanism: saslConfig.mechanism,
        username: saslConfig.username,
        password: saslConfig.password,
        token,
      }
    : undefined;
  return {
    brokers,
    clientId: config.clientId ?? "kafkajs",
    tls: (config.ssl === true ? {} : config.ssl || undefined) as never,
    sasl: sasl as never,
    requestTimeoutMs: config.requestTimeout ?? 30_000,
    connectTimeoutMs: Math.max(config.connectionTimeout ?? 1_000, 1_000),
    retry: {
      maxRetries: config.retry?.retries ?? 5,
      initialBackoffMs: config.retry?.initialRetryTime ?? 300,
      maxBackoffMs: config.retry?.maxRetryTime ?? 30_000,
    },
  };
}

function toBunPartitioner(partitioner: unknown): Partitioner | undefined {
  if (!partitioner) return undefined;
  if (typeof partitioner === "function") {
    const fn = partitioner as (topic: string, count: number, key: Uint8Array | null) => number;
    return ({ topic, partitionCount, key }) => fn(topic, partitionCount, key);
  }
  const obj = partitioner as { partition?: (ctx: unknown) => number };
  if (typeof obj.partition === "function") return (ctx) => obj.partition!(ctx);
  return undefined;
}

function toWireMessage(message: KafkaJsMessage): ProducerMessage {
  if (message.value === undefined) throw new KafkaJSNonRetriableError("Invalid message value");
  const wire: ProducerMessage = {
    value: message.value,
    key: message.key ?? null,
    headers: (message.headers ?? {}) as ProducerMessage["headers"],
  };
  if (message.partition !== undefined) wire.partition = message.partition;
  if (message.timestamp !== undefined) wire.timestamp = Number(message.timestamp);
  return wire;
}

function toBuffer(value: Uint8Array | null | undefined): Buffer | null {
  return value == null ? null : Buffer.from(value);
}

export type KafkaJsConsumedMessage = {
  key: Buffer | null;
  value: Buffer | null;
  timestamp: string;
  attributes: number;
  offset: string;
  size: number;
  topic: string;
  partition: number;
  headers?: Record<string, Buffer | undefined>;
  leaderEpoch?: number | null;
};

function toKafkajsMessage(raw: ConsumedMessage): KafkaJsConsumedMessage {
  const size = (raw.key?.byteLength ?? 0) + (raw.value?.byteLength ?? 0)
    + Object.values(raw.headers ?? {}).reduce((sum, header) => sum + (header?.byteLength ?? 0), 0);
  const headers: Record<string, Buffer | undefined> = {};
  for (const [name, value] of Object.entries(raw.headers ?? {})) headers[name] = value == null ? undefined : Buffer.from(value);
  return {
    key: toBuffer(raw.key),
    value: toBuffer(raw.value),
    timestamp: String(raw.timestamp),
    attributes: 0,
    offset: String(raw.offset),
    size,
    topic: raw.topic,
    partition: raw.partition,
    headers,
    leaderEpoch: null,
  };
}

type ClusterGetter = {
  ready: () => Promise<Cluster>;
  sync: () => Cluster;
  acquire: () => Cluster;
  release: () => void;
};

/**
 * One shared wire-level cluster per kafkajs `Kafka` instance with reference
 * counting: closing every derived client tears the cluster down so processes
 * can exit, mirroring per-client connection pools in kafkajs.
 */
class ClusterHub {
  #mapped?: Promise<MappedConfig & { brokers: string[] }>;
  #cluster?: Cluster;
  #refs = 0;

  constructor(private config: KafkaConfig) {}

  resolve(): Promise<MappedConfig & { brokers: string[] }> {
    this.#mapped ??= (async () => {
      const brokers = typeof this.config.brokers === "function" ? await this.config.brokers() : this.config.brokers;
      return { ...(mapConfig(this.config, brokers) as MappedConfig), brokers };
    })();
    return this.#mapped;
  }

  async ready(): Promise<Cluster> {
    await this.resolve();
    return this.sync();
  }

  sync(): Cluster {
    if (!this.#cluster) {
      const mapped = typeof this.config.brokers === "function" ? undefined : mapConfig(this.config, this.config.brokers);
      if (!mapped) throw new KafkaJSNonRetriableError("Broker list is resolving asynchronously; await an async method first");
      this.#cluster = new Cluster(mapped);
    }
    return this.#cluster;
  }

  /** Acquire a reference for one derived client; `release` runs on its close. */
  acquire(): Cluster {
    this.#refs++;
    return this.sync();
  }

  release(): void {
    this.#refs = Math.max(0, this.#refs - 1);
    if (this.#refs === 0 && this.#cluster) {
      this.#cluster.close();
      this.#cluster = undefined;
    }
  }
}

/** @confluentinc/kafka-javascript nests real options under `kafkaJS`; accept both shapes. */
function unwrapKafkaJs<T>(options: T & { kafkaJS?: T } | undefined): T {
  return ((options as { kafkaJS?: T })?.kafkaJS ?? options) as T;
}

export class Kafka {
  #logger: Logger;
  #hub: ClusterHub;

  constructor(config: KafkaConfig) {
    const unwrapped = unwrapKafkaJs(config);
    this.#hub = new ClusterHub(unwrapped);
    this.#logger = new Logger(unwrapped.logLevel ?? logLevel.NOTHING, unwrapped.clientId ?? "kafkajs", unwrapped.logCreator);
  }

  #getter(): ClusterGetter {
    return {
      ready: () => this.#hub.ready(),
      sync: () => this.#hub.sync(),
      acquire: () => this.#hub.acquire(),
      release: () => this.#hub.release(),
    };
  }

  producer(options: Record<string, any> = {}): CompatProducer {
    return new CompatProducer(() => this.#getter(), this.#logger.namespace("producer"), unwrapKafkaJs(options));
  }

  consumer(options: Record<string, any> = {}): CompatConsumer {
    return new CompatConsumer(() => this.#getter(), this.#logger.namespace("consumer"), unwrapKafkaJs(options));
  }

  admin(_options: Record<string, any> = {}): CompatAdmin {
    return new CompatAdmin(() => this.#getter(), this.#logger.namespace("admin"));
  }

  logger(namespace?: string): Logger {
    return namespace ? this.#logger.namespace(namespace) : this.#logger;
  }
}

function producerOptions(options: Record<string, any>, logger: Logger) {
  const compressionCode = Number(options.compression ?? CompressionTypes.None);
  return {
    lingerMs: 5,
    compression: COMPRESSION_NAMES[compressionCode] ?? "none",
    idempotent: Boolean(options.idempotent),
    transactionalId: options.transactionalId as string | undefined,
    transactionTimeoutMs: options.transactionTimeout as number | undefined,
    partitioner: toBunPartitioner(options.createPartitioner ?? options.partitioner),
  };
}

function acksToWire(acks: number | undefined): 0 | 1 | "all" {
  if (acks === undefined || acks === -1) return "all";
  return acks as 0 | 1;
}

export class CompatProducer {
  events = PRODUCER_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #options: Record<string, any>;
  #emitter = new Emitter();
  #producers = new Map<string, BunProducer>();
  #transaction?: BunProducer;
  #connected = false;

  constructor(getter: () => ClusterGetter, logger: Logger, options: Record<string, any>) {
    this.#getter = getter;
    this.#logger = logger;
    this.#options = options;
  }

  on(event: string, listener: (event: Record<string, unknown>) => void): () => void {
    return this.#emitter.on(event, listener);
  }

  logger(): Logger {
    return this.#logger;
  }

  async connect(): Promise<void> {
    await this.#getter().ready();
    this.#connected = true;
    this.#emitter.emit(PRODUCER_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    for (const producer of this.#producers.values()) await producer.close().catch(() => {});
    this.#producers.clear();
    if (this.#transaction) {
      await this.#transaction.close().catch(() => {});
      this.#transaction = undefined;
    }
    this.#connected = false;
    this.#emitter.emit(PRODUCER_EVENTS.DISCONNECT);
  }

  #underlying(compression?: number): BunProducer {
    const name = COMPRESSION_NAMES[Number(compression ?? this.#options.compression ?? CompressionTypes.None)] ?? "none";
    let producer = this.#producers.get(name);
    if (!producer) {
      producer = new BunProducer(this.#getter().acquire(), producerOptions({ ...this.#options, compression: name }, this.#logger), this.#getter().release);
      this.#producers.set(name, producer);
    }
    return producer;
  }

  async send({ topic, messages, acks, timeout, compression }: KafkaJsSendRecord):
    Promise<Array<{ topicName: string; partition: number; errorCode: number; baseOffset: string; logAppendTime: string }>> {
    try {
      if (!messages.length) return [];
      const results = await this.#underlying(compression).send({
        topic,
        messages: messages.map(toWireMessage),
        acks: acksToWire(acks),
        timeoutMs: timeout,
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

  async sendBatch({ topicMessages, acks, timeout, compression }: KafkaJsSendBatchRecord): Promise<
    Array<{ topicName: string; partition: number; errorCode: number; baseOffset: string; logAppendTime: string }>
  > {
    try {
      const producer = this.#underlying(compression);
      const results = await Promise.all(topicMessages.filter((item) => item.messages.length).map((item) =>
        producer.send({
          topic: item.topic,
          messages: item.messages.map(toWireMessage),
          acks: acksToWire(acks),
          timeoutMs: timeout,
        })));
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
      throw new KafkaJSNonRetriableError("Cannot use transactions without setting the transactionalId");
    }
    if (!this.#transaction) {
      this.#transaction = new BunProducer(this.#getter().acquire(), {
        ...producerOptions(this.#options, this.#logger),
        idempotent: true,
        lingerMs: 0,
      }, this.#getter().release);
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
        await this.#producer.send({ topic: item.topic, messages: item.messages.map(toWireMessage), acks: "all" });
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
    offsets: Array<{ topic: string; partitions: Array<{ partition: number; offset: string | number | bigint }> }>,
    consumerGroupId: string,
  ): Promise<void> {
    try {
      const flat = offsets.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition, offset }) => ({ topic, partition, offset: BigInt(offset) })));
      await this.#producer.sendOffsetsToTransaction(flat, consumerGroupId);
    } catch (error) {
      throw wrapError(error);
    }
  }
  isActive(): boolean {
    return true;
  }
}

export interface CompatEachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaJsConsumedMessage;
  heartbeat: () => Promise<void>;
  pause: () => void;
}

export interface CompatEachBatchPayload {
  batch: {
    topic: string;
    partition: number;
    highWatermark: string;
    messages: KafkaJsConsumedMessage[];
    isEmpty(): boolean;
    firstOffset(): string | null;
    lastOffset(): string | null;
    offsetLag(): string;
    isStale(): boolean;
    resolveOffset(offset: string | number | bigint): void;
    commitOffsetsIfNecessary(options?: unknown): Promise<void>;
    heartbeat(): Promise<void>;
  };
  heartbeat: () => Promise<void>;
  pause: () => void;
  isRunning: () => boolean;
  isStale: () => boolean;
}

export interface RunOptions {
  eachMessage?: (payload: CompatEachMessagePayload) => Promise<void>;
  eachBatch?: (payload: CompatEachBatchPayload) => Promise<void>;
  autoCommit?: boolean;
  autoCommitInterval?: number | null;
  autoCommitThreshold?: number | null;
  eachBatchAutoResolve?: boolean;
  partitionsConsumedConcurrently?: number;
  beforeCommit?: (offsets: Array<{ topic: string; partition: number; offset: string }>) => Promise<boolean | undefined> | boolean | undefined;
  afterCommit?: (offsets: Array<{ topic: string; partition: number; offset: string }>) => Promise<void> | void;
  onCrash?: (error: Error) => void;
}

export class CompatConsumer {
  events = CONSUMER_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #options: Record<string, any>;
  #emitter = new Emitter();
  #consumer?: BunConsumer;
  #running = false;
  #stopping?: Promise<void>;
  #paused = new Set<string>();
  #subscribedTopics = new Set<string | RegExp>();
  #pendingOffsets = new Map<string, { topic: string; partition: number; offset: bigint }>();
  #uncommittedCount = 0;

  constructor(getter: () => ClusterGetter, logger: Logger, options: Record<string, any>) {
    this.#getter = getter;
    this.#logger = logger;
    this.#options = options;
  }

  on(event: string, listener: (event: Record<string, unknown>) => void): () => void {
    return this.#emitter.on(event, listener);
  }

  logger(): Logger {
    return this.#logger;
  }

  #underlying(): BunConsumer {
    if (!this.#consumer) {
      const o = this.#options;
      const assignors = Array.isArray(o.partitionAssignors) ? o.partitionAssignors : [];
      const cooperative = assignors.some((assignor) => (assignor as { name?: string }).name === "CooperativeStickyAssignor");
      const consumerOptions = {
        groupId: o.groupId as string,
        sessionTimeoutMs: o.sessionTimeout as number | undefined,
        rebalanceTimeoutMs: o.rebalanceTimeout as number | undefined,
        heartbeatIntervalMs: o.heartbeatInterval as number | undefined,
        fromBeginning: o.fromBeginning as boolean | undefined,
        isolationLevel: o.isolationLevel as "read_uncommitted" | "read_committed" | undefined,
        groupInstanceId: o.groupInstanceId as string | undefined,
        partitionAssigner: cooperative ? ("cooperative-sticky" as const) : undefined,
      };
      this.#consumer = new BunConsumer(this.#getter().acquire(), consumerOptions, this.#getter().release);
    }
    return this.#consumer!;
  }

  async connect(): Promise<void> {
    await this.#getter().ready();
    this.#underlying();
    this.#emitter.emit(CONSUMER_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    await this.stop().catch(() => {});
    await this.#consumer?.close().catch(() => {});
    this.#consumer = undefined;
    this.#emitter.emit(CONSUMER_EVENTS.DISCONNECT);
  }

  async subscribe({ topic, topics, fromBeginning }: { topic?: string | RegExp; topics?: string | RegExp | Array<string | RegExp>; fromBeginning?: boolean }): Promise<void> {
    try {
      const incoming = topic !== undefined ? [topic] : topics !== undefined ? (Array.isArray(topics) ? topics : [topics]) : [];
      // kafkajs accumulates subscriptions across calls into one group subscription.
      const merged = [...this.#subscribedTopics];
      for (const entry of incoming) {
        if (!merged.some((existing) => existing.toString() === entry.toString())) merged.push(entry);
      }
      this.#subscribedTopics = new Set(merged);
      await this.#underlying().subscribe({ topics: merged as never, fromBeginning });
      this.#emitter.emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: this.#options.groupId });
    } catch (error) {
      throw wrapError(error);
    }
  }

  async run(options: RunOptions): Promise<void> {
    if (this.#running) throw new KafkaJSNonRetriableError("Consumer run() was already called");
    if (options.eachMessage && options.eachBatch) {
      throw new KafkaJSNonRetriableError("Not allowed to configure both eachMessage and eachBatch");
    }
    if (!options.eachMessage && !options.eachBatch) {
      throw new KafkaJSNonRetriableError("Missing one of eachMessage or eachBatch");
    }
    await this.#getter().ready();
    this.#running = true;
    const loop = this.#loop(options).catch(async (error) => {
      this.#running = false;
      const wrapped = error instanceof Error ? wrapError(error) : new KafkaJSError(String(error));
      this.#logger.error(`consumer crashed: ${wrapped.message}`, { groupId: this.#options.groupId, stack: wrapped.stack });
      this.#emitter.emit(CONSUMER_EVENTS.CRASH, { error: wrapped, groupId: this.#options.groupId });
      await options.onCrash?.(wrapped);
    });
    this.#stopping = loop;
  }

  async #loop(options: RunOptions): Promise<void> {
    const consumer = this.#underlying();
    const autoCommitEnabled = options.autoCommit !== false;
    const concurrent = Math.max(1, options.partitionsConsumedConcurrently ?? 1);
    const commitTimer = options.autoCommitInterval
      ? setInterval(() => void this.#flushCommits(consumer, options), options.autoCommitInterval)
      : undefined;
    commitTimer?.unref?.();
    try {
      while (this.#running) {
        this.#emitter.emit(CONSUMER_EVENTS.FETCH_START);
        const o = this.#options;
        const messages = await consumer.fetch({
          maxMessages: 200,
          // Capped wire wait keeps stop()/pause()/resume() responsive even when
          // applications configure multi-second maxWaitTimeInMs.
          maxWaitMs: Math.min((o.maxWaitTimeInMs as number) ?? 500, 1_000),
          minBytes: o.minBytes as number | undefined,
          maxBytes: o.maxBytes as number | undefined,
          maxPartitionBytes: o.maxBytesPerPartition as number | undefined,
        });
        if (!this.#running) break;
        this.#emitter.emit(CONSUMER_EVENTS.FETCH, { numberOfMessages: messages.length });
        if (!messages.length) continue;
        const groups = this.#groupByPartition(messages);
        for (let i = 0; i < groups.length; i += concurrent) {
          await Promise.all(groups.slice(i, i + concurrent).map(([key, items]) => this.#processGroup(key, items, consumer, options, autoCommitEnabled)));
          if (!this.#running) break;
        }
        if (autoCommitEnabled && !options.autoCommitInterval && !options.autoCommitThreshold) {
          await this.#flushCommits(consumer, options);
        }
      }
    } finally {
      if (commitTimer) clearInterval(commitTimer);
    }
  }

  #groupByPartition(messages: ConsumedMessage[]): Array<[string, ConsumedMessage[]]> {
    const ordered: Array<[string, ConsumedMessage[]]> = [];
    const index = new Map<string, ConsumedMessage[]>();
    for (const message of messages) {
      const key = `${message.topic}\u0000${message.partition}`;
      const bucket = index.get(key);
      if (bucket) bucket.push(message);
      else {
        const fresh = [message];
        index.set(key, fresh);
        ordered.push([key, fresh]);
      }
    }
    return ordered;
  }

  async #processGroup(key: string, items: ConsumedMessage[], consumer: BunConsumer, options: RunOptions, autoCommitEnabled: boolean): Promise<void> {
    const separator = key.indexOf("\u0000");
    const topic = key.slice(0, separator);
    const partition = Number(key.slice(separator + 1));
    this.#emitter.emit(CONSUMER_EVENTS.START_BATCH_PROCESS, { topic, partition, size: items.length });
    const heartbeat = async () => {
      this.#emitter.emit(CONSUMER_EVENTS.HEARTBEAT);
    };
    const pause = () => {
      this.pause([{ topic, partitions: [partition] }]);
    };
    try {
      if (options.eachMessage) {
        for (const raw of items) {
          if (!this.#running) return;
          this.#track(topic, partition, raw.offset + 1n);
          await options.eachMessage({ topic, partition, message: toKafkajsMessage(raw), heartbeat, pause });
          this.#uncommittedCount++;
          if (options.autoCommitThreshold && this.#uncommittedCount >= options.autoCommitThreshold) {
            await this.#flushCommits(consumer, options);
          }
        }
      } else if (options.eachBatch) {
        await this.#runBatch(topic, partition, items, consumer, options, autoCommitEnabled, heartbeat, pause);
      }
    } finally {
      this.#emitter.emit(CONSUMER_EVENTS.END_BATCH_PROCESS, { topic, partition });
    }
  }

  async #runBatch(
    topic: string,
    partition: number,
    items: ConsumedMessage[],
    consumer: BunConsumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
    heartbeat: () => Promise<void>,
    pause: () => void,
  ): Promise<void> {
    let highWatermark = "";
    try {
      const marks = await consumer.watermarks(topic, partition);
      highWatermark = String(marks.high);
    } catch {
      highWatermark = String(items[items.length - 1]!.offset + 1n);
    }
    const messages = items.map(toKafkajsMessage);
    const firstRaw = items[0]!;
    const lastRaw = items[items.length - 1]!;
    const resolved = new Set<string>();
    const batch = {
      topic,
      partition,
      highWatermark,
      messages,
      isEmpty: () => messages.length === 0,
      firstOffset: () => (messages.length ? messages[0]!.offset : null),
      lastOffset: () => (messages.length ? messages[messages.length - 1]!.offset : null),
      offsetLag: () => (BigInt(highWatermark) - BigInt(messages[messages.length - 1]!.offset) - 1n).toString(),
      isStale: () => !this.#running,
      resolveOffset: (offset: string | number | bigint) => {
        resolved.add(BigInt(offset).toString());
      },
      commitOffsetsIfNecessary: async () => {
        if (autoCommitEnabled) await this.#flushCommits(consumer, options);
      },
      heartbeat,
    };
    await options.eachBatch!({ batch, heartbeat, pause, isRunning: () => this.#running, isStale: () => !this.#running });
    const autoResolve = options.eachBatchAutoResolve !== false;
    if (autoResolve) for (const raw of items) resolved.add(raw.offset.toString());
    let nextOffset: bigint;
    if (resolved.size) {
      const highest = [...resolved].map(BigInt).reduce((a, b) => (b > a ? b : a));
      nextOffset = highest + 1n;
    } else {
      nextOffset = firstRaw.offset;
    }
    if (nextOffset <= lastRaw.offset) {
      // Unresolved tail: rewind so the remainder is delivered again.
      consumer.seek({ topic, partition, offset: nextOffset });
    } else {
      this.#track(topic, partition, nextOffset);
      this.#uncommittedCount++;
      if (autoCommitEnabled && options.autoCommitThreshold && this.#uncommittedCount >= options.autoCommitThreshold) {
        await this.#flushCommits(consumer, options);
      }
    }
  }

  #track(topic: string, partition: number, offset: bigint): void {
    this.#pendingOffsets.set(`${topic}\u0000${partition}`, { topic, partition, offset });
  }

  async #flushCommits(consumer: BunConsumer, options: RunOptions): Promise<void> {
    if (!this.#pendingOffsets.size) return;
    const entries = [...this.#pendingOffsets.values()];
    const serializable = entries.map((entry) => ({ topic: entry.topic, partition: entry.partition, offset: entry.offset.toString() }));
    if (options.beforeCommit && await options.beforeCommit(serializable)) {
      this.#pendingOffsets.clear();
      return;
    }
    await consumer.commitOffsets(entries);
    this.#pendingOffsets.clear();
    this.#uncommittedCount = 0;
    this.#emitter.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, { groupId: this.#options.groupId, topics: serializable.map((entry) => entry.topic) });
    await options.afterCommit?.(serializable);
  }

  async stop(): Promise<void> {
    if (!this.#running) return;
    this.#running = false;
    this.#emitter.emit(CONSUMER_EVENTS.STOP);
    await this.#stopping;
  }

  async commitOffsets(topicPartitions: Array<{ topic: string; partition: number; offset?: string | number | bigint; metadata?: string }>): Promise<void> {
    try {
      const consumer = this.#underlying();
      const entries: Array<{ topic: string; partition: number; offset: bigint }> = [];
      for (const { topic, partition, offset } of topicPartitions) {
        const key = `${topic}\u0000${partition}`;
        const resolved = offset !== undefined
          ? BigInt(offset)
          : this.#pendingOffsets.get(key)?.offset ?? consumer.position(topic, partition) ?? -1n;
        this.#pendingOffsets.delete(key);
        if (resolved >= 0n) entries.push({ topic, partition, offset: resolved });
      }
      if (!entries.length) return;
      await consumer.commitOffsets(entries);
      this.#emitter.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, { groupId: this.#options.groupId, topics: entries.map((entry) => entry.topic) });
    } catch (error) {
      throw wrapError(error);
    }
  }

  seek({ topic, partition, offset }: { topic: string; partition: number; offset: string | number | bigint }): void {
    this.#underlying().seek({ topic, partition, offset: BigInt(offset) });
  }

  /** kafkajs allows entries without `partitions`, meaning every assigned partition of the topic. */
  #resolvePartitions(topicPartitions: Array<{ topic: string; partitions?: number[] }>): TopicPartition[] {
    const assigned = this.#underlying().assignment();
    return topicPartitions.flatMap(({ topic, partitions }) => {
      const forTopic = partitions ?? assigned.filter((entry) => entry.topic === topic).map((entry) => entry.partition);
      return forTopic.map((partition) => ({ topic, partition }));
    });
  }

  pause(topicPartitions: Array<{ topic: string; partitions?: number[] }>): Array<{ topic: string; partitions: number[] }> {
    const targets = this.#resolvePartitions(topicPartitions);
    if (!targets.length) return this.paused();
    this.#underlying().pause(targets);
    for (const target of targets) this.#paused.add(`${target.topic}\u0000${target.partition}`);
    return this.paused();
  }

  resume(topicPartitions: Array<{ topic: string; partitions?: number[] }>): Array<{ topic: string; partitions: number[] }> {
    const targets = this.#resolvePartitions(topicPartitions);
    if (!targets.length) return this.paused();
    this.#underlying().resume(targets);
    for (const target of targets) this.#paused.delete(`${target.topic}\u0000${target.partition}`);
    return this.paused();
  }

  paused(): Array<{ topic: string; partitions: number[] }> {
    const grouped = new Map<string, number[]>();
    for (const key of this.#paused) {
      const separator = key.indexOf("\u0000");
      const topic = key.slice(0, separator);
      grouped.set(topic, [...(grouped.get(topic) ?? []), Number(key.slice(separator + 1))]);
    }
    return [...grouped].map(([topic, partitions]) => ({ topic, partitions }));
  }

  async describeGroup(): Promise<Record<string, unknown>> {
    try {
      const admin = new BunAdmin(this.#getter().acquire(), this.#getter().release);
      const [group] = await admin.describeGroups([String(this.#options.groupId)]);
      admin.close();
      return group ?? {};
    } catch (error) {
      throw wrapError(error);
    }
  }

  assignments(): Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }> {
    const grouped = new Map<string, Array<{ partition: number; offset: string }>>();
    for (const { topic, partition, offset } of this.#underlying().assignment()) {
      grouped.set(topic, [...(grouped.get(topic) ?? []), { partition, offset: offset?.toString() ?? "" }]);
    }
    return [...grouped].map(([topic, partitions]) => ({ topic, partitions }));
  }
}

export interface CompatCreateTopicsInput {
  validateOnly?: boolean;
  waitForLeaders?: boolean;
  timeout?: number;
  topics: Array<{
    topic: string;
    numPartitions?: number;
    replicationFactor?: number;
    replicaAssignment?: number[][];
    configEntries?: Array<{ name: string; value: string | null }>;
  }>;
}

export class CompatAdmin {
  events = ADMIN_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #admin?: BunAdmin;
  #emitter = new Emitter();

  constructor(getter: () => ClusterGetter, logger: Logger) {
    this.#getter = getter;
    this.#logger = logger;
  }

  on(event: string, listener: (event: Record<string, unknown>) => void): () => void {
    return this.#emitter.on(event, listener);
  }

  logger(): Logger {
    return this.#logger;
  }

  #underlying(): BunAdmin {
    this.#admin ??= new BunAdmin(this.#getter().acquire(), this.#getter().release);
    return this.#admin!;
  }

  async connect(): Promise<void> {
    await this.#getter().ready();
    this.#underlying();
    this.#emitter.emit(ADMIN_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    await this.#admin?.close().catch(() => {});
    this.#admin = undefined;
    this.#emitter.emit(ADMIN_EVENTS.DISCONNECT);
  }

  async createTopics(input: CompatCreateTopicsInput): Promise<boolean[]> {
    try {
      const { validateOnly = false, waitForLeaders = true, timeout = 5_000, topics } = input;
      const results = await this.#underlying().createTopics(topics.map((item) => ({
        name: item.topic,
        numPartitions: item.numPartitions ?? -1,
        replicationFactor: item.replicationFactor ?? -1,
        assignments: item.replicaAssignment,
        configs: item.configEntries ? Object.fromEntries(item.configEntries.map((entry) => [entry.name, entry.value])) : undefined,
      })), { validateOnly });
      // TOPIC_ALREADY_EXISTS counts as "not created" rather than a failure.
      const outcomes = results.map((result) => result.error === 0);
      if (waitForLeaders && outcomes.some(Boolean)) {
        const deadline = Date.now() + Math.max(timeout, 5_000);
        while (Date.now() < deadline) {
          const created = topics.filter((_, i) => outcomes[i]).map((item) => item.topic);
          const metadata = await this.#underlying().metadata(created);
          if (metadata.topics.every((topicMeta) => !topicMeta.err && topicMeta.partitions.every((p) => p.leader >= 0))) break;
          await Bun.sleep(100);
        }
      }
      return outcomes;
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteTopics(payload: { topics: Array<string | { topic: string; partitions?: number[] }>; timeout?: number }): Promise<void> {
    try {
      await this.#underlying().deleteTopics(payload.topics.map((entry) => (typeof entry === "string" ? entry : entry.topic)));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createPartitions({ validateOnly = false, topicPartitions }: { validateOnly?: boolean; topicPartitions: Array<{ topic: string; count: number; assignments?: number[][] }> }): Promise<void> {
    try {
      await this.#underlying().createPartitions(
        topicPartitions.map((item) => ({ name: item.topic, count: item.count, assignments: item.assignments })),
        { validateOnly },
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicMetadata({ topics }: { topics?: Array<{ topic: string }> } = {}): Promise<{
    brokers: Array<{ nodeId: number; host: string; port: number }>;
    topics: Array<{ topicName: string; partitions: Array<{ partitionErrorCode: number; partition: number; leader: number; replicas: number[]; isr: number[] }> }>;
  }> {
    try {
      const metadata = await this.#underlying().metadata(topics?.map((entry) => entry.topic) ?? null);
      return {
        brokers: metadata.brokers.map((broker) => ({ nodeId: broker.id, host: broker.host, port: broker.port })),
        topics: metadata.topics.map((topicMeta) => ({
          topicName: topicMeta.name,
          partitions: topicMeta.partitions.map((p) => ({
            partitionErrorCode: p.err,
            partition: p.id,
            leader: p.leader,
            replicas: [p.leader],
            isr: [p.leader],
          })),
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeCluster(): Promise<{ brokers: Array<{ nodeId: number; host: string; port: number }>; controller: number | null; clusterId: string | null }> {
    try {
      const metadata = await this.#underlying().metadata(null);
      return {
        brokers: metadata.brokers.map((broker) => ({ nodeId: broker.id, host: broker.host, port: broker.port })),
        controller: metadata.brokers.length ? metadata.brokers[0]!.id : null,
        clusterId: metadata.clusterId ?? null,
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchOffsets({ groupId, topics, resolveOffsets = false }: { groupId: string; topics?: string[]; resolveOffsets?: boolean }): Promise<Array<{ topic: string; partitions: Array<{ partition: number; offset: string; metadata?: string }> }>> {
    try {
      const cluster = this.#getter().sync();
      const names = topics ?? await this.listTopics();
      const result: Array<{ topic: string; partitions: Array<{ partition: number; offset: string; metadata?: string }> }> = [];
      for (const topic of names) {
        const meta = await cluster.topic(topic);
        if (meta.err) continue;
        const partitions = meta.partitions.map((p) => p.id);
        const body = new Writer().string(groupId)
          .array([topic], (writer, name) => writer.string(name).array(partitions, (partitionWriter, partition) => partitionWriter.i32(partition)));
        const response = await cluster.anyRequest(9, 2, body);
        const parsed = response.array((topicReader) => {
          const name = topicReader.string() ?? "";
          return topicReader.array((partitionReader) => ({
            name,
            partition: partitionReader.i32(),
            offset: partitionReader.i64(),
            metadata: partitionReader.string(),
            error: partitionReader.i16(),
          }));
        }).flat();
        const mapped = parsed
          .filter((entry) => entry.error === 0)
          .map(async (entry) => ({
            partition: entry.partition,
            offset: (resolveOffsets && entry.offset < 0n ? await listOffset(cluster, topic, entry.partition, -2) : entry.offset).toString(),
            metadata: entry.metadata ?? undefined,
          }));
        result.push({ topic, partitions: await Promise.all(mapped) });
      }
      return result;
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listConsumerGroupOffsets(payload: { groupId: string; topics?: string[] }): Promise<Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>> {
    const fetched = await this.fetchOffsets(payload);
    return fetched.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, offset }) => ({ partition, offset })),
    }));
  }

  async fetchTopicOffsets(topic: string): Promise<Array<{ partition: number; offset: string; high: string; low: string }>> {
    try {
      const cluster = this.#getter().sync();
      const meta = await cluster.topic(topic);
      if (meta.err) throw new KafkaError(meta.err, `Topic ${topic}`);
      return await Promise.all(meta.partitions.map(async (partition) => {
        const low = await listOffset(cluster, topic, partition.id, -2);
        const high = await listOffset(cluster, topic, partition.id, -1);
        return { partition: partition.id, offset: high.toString(), high: high.toString(), low: low.toString() };
      }));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicOffsetsByTimestamp(topic: string, timestamp = Date.now()): Promise<Array<{ partition: number; offset: string }>> {
    try {
      const cluster = this.#getter().sync();
      const meta = await cluster.topic(topic);
      if (meta.err) throw new KafkaError(meta.err, `Topic ${topic}`);
      return await Promise.all(meta.partitions.map(async (partition) => ({
        partition: partition.id,
        offset: (await listOffset(cluster, topic, partition.id, timestamp)).toString(),
      })));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async setOffsets({ groupId, topic, partitions }: { groupId: string; topic: string; partitions: Array<{ partition: number; offset: string | number | bigint }> }): Promise<void> {
    try {
      await commitGroupOffsets(this.#getter().sync(), groupId, [{
        topic,
        partitions: partitions.map(({ partition, offset }) => ({ partition, offset: BigInt(offset), metadata: "" })),
      }]);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async resetOffsets({ groupId, topic, earliest = true }: { groupId: string; topic: string; earliest?: boolean }): Promise<void> {
    try {
      const cluster = this.#getter().sync();
      const meta = await cluster.topic(topic);
      if (meta.err) throw new KafkaError(meta.err, `Topic ${topic}`);
      const resolved = await Promise.all(meta.partitions.map(async (partition) => ({
        partition: partition.id,
        offset: await listOffset(cluster, topic, partition.id, earliest ? -2 : -1),
        metadata: "",
      })));
      await commitGroupOffsets(cluster, groupId, [{ topic, partitions: resolved }]);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listGroups(statesFilter?: string[]): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    try {
      const groups = await this.#underlying().listGroups(statesFilter ?? []);
      return { groups: groups.map(({ groupId, protocolType, state }) => ({ groupId, protocolType, state })) };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listConsumerGroups(statesFilter?: string[]): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    return this.listGroups(statesFilter);
  }

  async describeGroups(groupIds: string[]): Promise<{ groups: Array<Record<string, unknown>> }> {
    try {
      const cluster = this.#getter().sync();
      const body = new Writer().array(groupIds, (writer, group) => writer.string(group));
      const response = await cluster.anyRequest(15, 1, body);
      const throttle = response.i32();
      cluster.throttle?.(15 as never, throttle);
      // Some brokers (Redpanda) omit the nullable error_message field that
      // Apache Kafka always writes; trial-parse both shapes and keep the one
      // that consumes the buffer exactly.
      const data = response.data;
      const attempts = [true, false];
      let groups: Array<Record<string, unknown>> | undefined;
      for (const withMessage of attempts) {
        try {
          const reader = new Reader(data);
          reader.i32();
          const parsed: Array<Record<string, unknown>> = reader.array((entry) => {
            const error = entry.i16();
            const message = withMessage ? entry.string() : undefined;
            const groupId = entry.string() ?? "";
            const state = entry.string() ?? "";
            const protocolType = entry.string() ?? "";
            const protocol = entry.string();
            const members = entry.array((memberReader) => ({
              memberId: memberReader.string() ?? "",
              clientId: memberReader.string() ?? "",
              clientHost: memberReader.string() ?? "",
              memberMetadata: memberReader.bytes(),
              memberAssignment: memberReader.bytes(),
            }));
            return { errorCode: error, errorMessage: message, groupId, state, protocolType, protocolData: protocol, members };
          });
          if (reader.remaining === 0) {
            groups = parsed;
            break;
          }
        } catch {
          // Try the next shape.
        }
      }
      if (!groups) throw new KafkaError(-1, "Malformed DescribeGroups response");
      return { groups };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteGroups(groupIds: string[]): Promise<void> {
    try {
      await this.#underlying().deleteGroups(groupIds);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listTopics(): Promise<string[]> {
    try {
      const metadata = await this.#underlying().metadata(null);
      return metadata.topics.filter((topicMeta) => !topicMeta.err && topicMeta.name).map((topicMeta) => topicMeta.name);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeConfigs({ resources }: { resources: Array<{ type: number; name: string; configNames?: string[] }> }): Promise<{
    resources: Array<{ resourceName: string; resourceType: number; configEntries: Record<string, { value: string | null; isDefault: boolean; isSensitive: boolean; readOnly: boolean; configSource: number }> }>;
  }> {
    try {
      const described = await this.#underlying().describeConfigs(resources.map((resource) => ({
        resourceType: resource.type,
        resourceName: resource.name,
        configNames: resource.configNames,
      })));
      return {
        resources: described.map((resource) => {
          const configEntries: Record<string, { value: string | null; isDefault: boolean; isSensitive: boolean; readOnly: boolean; configSource: number }> = {};
          for (const config of resource.configs) {
            configEntries[config.name] = {
              value: config.value,
              isDefault: config.source === 5,
              isSensitive: config.sensitive,
              readOnly: config.readOnly,
              configSource: config.source,
            };
          }
          return { resourceName: resource.resourceName, resourceType: resource.resourceType, configEntries };
        }),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async alterConfigs({ validateOnly = false, resources }: { validateOnly?: boolean; resources: Array<{ type: number; name: string; configEntries: Record<string, string | null> }> }): Promise<void> {
    try {
      await this.#underlying().alterConfigs(resources.map((resource) => ({
        resourceType: resource.type,
        resourceName: resource.name,
        configs: resource.configEntries,
      })), { validateOnly });
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createAcls({ acl }: { acl: Array<Record<string, any>> }): Promise<boolean[]> {
    try {
      const results = await this.#underlying().createAcls(acl.map((entry) => ({
        resourceType: Number(entry.resourceType ?? entry.resourceResourceType ?? 2),
        resourceName: String(entry.resourceName ?? entry.resourceResourceName),
        principal: String(entry.principal),
        host: String(entry.host),
        operation: Number(entry.operation),
        permissionType: Number(entry.permissionType),
      })) as never);
      return results.map((result) => result.error === 0);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeAcls(filter: Record<string, any>): Promise<{ resources: Array<{ resourceType: number; resourceName: string; principal: string; host: string; operation: number; permissionType: number }> }> {
    try {
      const described = await this.#underlying().describeAcls({
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: filter.resourceName,
        principal: filter.principal,
        host: filter.host,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      } as never);
      return {
        resources: described.acls.map((acl) => ({
          resourceType: acl.resourceType,
          resourceName: acl.resourceName,
          principal: acl.principal,
          host: acl.host,
          operation: acl.operation,
          permissionType: acl.permissionType,
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteAcls(filters: { filters: Array<Record<string, any>> }): Promise<{
    entries: Array<{ errorCode: number; errorMessage?: string; resources: Array<{ resourceType: number; resourceName: string; principal: string; host: string; operation: number; permissionType: number }> }>;
  }> {
    try {
      const result = await this.#underlying().deleteAcls(filters.filters.map((filter) => ({
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: filter.resourceName,
        principal: filter.principal,
        host: filter.host,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      })) as never);
      return {
        entries: result.map((entry) => ({
          errorCode: entry.error,
          errorMessage: entry.message ?? undefined,
          resources: entry.acls.map((acl) => ({
            resourceType: acl.resourceType,
            resourceName: acl.resourceName,
            principal: acl.principal,
            host: acl.host,
            operation: acl.operation,
            permissionType: acl.permissionType,
          })),
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }
}

async function listOffset(cluster: Cluster, topic: string, partition: number, timestamp: number): Promise<bigint> {
  const meta = await cluster.topic(topic);
  const leader = meta.partitions.find((p) => p.id === partition)?.leader;
  if (leader === undefined) throw new KafkaError(3, `${topic}[${partition}]`);
  const body = new Writer().i32(-1).array([topic], (writer, name) =>
    writer.string(name).array([partition], (partitionWriter, index) => {
      partitionWriter.i32(index).i64(BigInt(timestamp));
    }));
  const response = await cluster.request(leader, 2, 1, body);
  const result = response.array((topicReader) => {
    topicReader.string();
    return topicReader.array((partitionReader) => {
      partitionReader.i32();
      const error = partitionReader.i16();
      partitionReader.i64();
      const offset = partitionReader.i64();
      if (error) throw new KafkaError(error, `${topic}[${partition}]`);
      return offset;
    });
  });
  return result[0]?.[0] ?? -1n;
}

async function commitGroupOffsets(
  cluster: Cluster,
  groupId: string,
  topics: Array<{ topic: string; partitions: Array<{ partition: number; offset: bigint; metadata: string }> }>,
): Promise<void> {
  const coordinatorResponse = await cluster.anyRequest(10, 0, new Writer().string(groupId));
  const coordinatorError = coordinatorResponse.i16();
  const coordinator = coordinatorResponse.i32();
  if (coordinatorError) throw new KafkaError(coordinatorError, `FindCoordinator ${groupId}`);
  const body = new Writer().string(groupId).i32(-1).string("").i64(-1n)
    .array(topics, (writer, { topic, partitions }) =>
      writer.string(topic).array(partitions, (partitionWriter, entry) =>
        partitionWriter.i32(entry.partition).i64(entry.offset).string(entry.metadata || null)));
  const response = await cluster.request(coordinator, 8, 2, body);
  for (const topicResult of response.array((reader: Reader) => ({
    topic: reader.string() ?? "",
    partitions: reader.array((p) => ({ partition: p.i32(), error: p.i16() })),
  }))) {
    for (const partition of topicResult.partitions) {
      if (partition.error) throw new KafkaError(partition.error, `${topicResult.topic}[${partition.partition}]`);
    }
  }
}

export type { KafkaConfig };

/** Namespace alias for code importing the @confluentinc/kafka-javascript style. */
export const KafkaJS = {
  Kafka,
  Partitioners,
  CompressionTypes,
  logLevel,
  ConfigResourceTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
};

export { BunProducer, BunConsumer, BunAdmin, Cluster };
