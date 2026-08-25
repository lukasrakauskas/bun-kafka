import { KafkaError } from "../errors.ts";
import type { Bytes, ClusterMetadata, KafkaMessage, MessageHeaders, TopicPartition, Watermarks } from "../types.ts";
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

const errorNames: Record<number, string> = {
  1: "Offset out of range",
  2: "Corrupt message",
  3: "Unknown topic or partition",
  5: "Leader not available",
  6: "Not leader for partition",
  7: "Request timed out",
  10: "Message too large",
  29: "Topic authorization failed",
  30: "Group authorization failed",
  35: "Unsupported version",
};
const retriableErrors = new Set([3, 5, 6, 7, 13, 14, 15, 19, 20, 41, 56]);

function kafkaError(code: number, context: string): KafkaError {
  return new KafkaError(code, `${context}: ${errorNames[code] ?? `Kafka error ${code}`}`, {
    retriable: retriableErrors.has(code),
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
  | { type: "throttle"; apiKey: number; durationMs: number };

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
}

type TopicMetadata = ClusterMetadata["topics"][number];

export class Cluster {
  #bootstrap: string[];
  #options: ConnectionOptions;
  #retry: Required<RetryOptions>;
  #onEvent?: (event: KafkaEvent) => void;
  #connections = new Map<string, Connection>();
  #brokers = new Map<number, string>();
  #controller?: number;
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
    this.#bootstrap = [...options.brokers];
    this.#retry = retry;
    this.#onEvent = options.onEvent;
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

  async anyRequest(apiKey: number, apiVersion: number, body: Writer): Promise<Reader> {
    return this.#anyRequest(apiKey, apiVersion, body);
  }

  async #anyRequest(apiKey: number, apiVersion: number, body: Writer): Promise<Reader> {
    let lastError: unknown;
    const candidates = [...new Set([...this.#brokers.values(), ...this.#bootstrap])];
    for (const broker of candidates) {
      try {
        return await this.#connection(broker).request(apiKey, apiVersion, body);
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError ?? new KafkaError(-1, "No Kafka broker is available", { retriable: true });
  }

  async metadata(topics: string[] | null = null): Promise<ClusterMetadata> {
    const body = new Writer().array(topics, (writer, topic) => writer.string(topic));
    const response = readMetadataResponse(await this.#anyRequest(API_METADATA, 1, body));
    for (const broker of response.brokers) this.#brokers.set(broker.id, address(broker.host, broker.port));
    this.#controller = response.controllerId;
    for (const topic of response.topics) this.#topics.set(topic.name, topic);
    return { brokers: response.brokers, topics: response.topics };
  }

  async topic(topic: string, refresh = false): Promise<TopicMetadata> {
    if (!refresh) {
      const cached = this.#topics.get(topic);
      if (cached?.partitions.length && !cached.err) return cached;
    }
    const metadata = await this.metadata([topic]);
    return metadata.topics.find((item) => item.name === topic) ?? { name: topic, err: 3, partitions: [] };
  }

  async request(brokerId: number, apiKey: number, apiVersion: number, body: Writer, timeoutMs?: number, retry = true): Promise<Reader> {
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
        return await this.#connection(broker).request(apiKey, apiVersion, body, timeoutMs);
      } catch (error) {
        lastError = error;
        if (!(error instanceof KafkaError && error.retriable) || attempt === maxRetries) throw error;
        const delay = retryDelay(this.#retry, attempt);
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

  get retryOptions(): Required<RetryOptions> { return this.#retry; }
  get requestTimeoutMs(): number { return this.#options.requestTimeoutMs; }

  event(event: KafkaEvent): void {
    try { this.#onEvent?.(event); } catch { /* Observability must not break requests. */ }
  }

  throttle(apiKey: number, durationMs: number): void {
    if (durationMs > 0) this.event({ type: "throttle", apiKey, durationMs });
  }

  close(): void {
    for (const connection of this.#connections.values()) connection.close();
    this.#connections.clear();
  }
}

export interface ProducerMessage extends WireRecord {
  partition?: number;
}

export interface ProducerSend {
  topic: string;
  messages: readonly ProducerMessage[];
  acks?: 1 | "all";
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
};

export interface ProducerOptions {
  /** Time to collect concurrent sends into one Produce request. Default 5 ms. */
  lingerMs?: number;
  /** Flush immediately at this queued message count. Default 1,000. */
  batchMaxMessages?: number;
  /** Record-batch compression through Bun primitives. */
  compression?: "none" | "gzip" | "zstd";
  /** Use broker sequence numbers to make retries duplicate-safe. */
  idempotent?: boolean;
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
  #options: Required<ProducerOptions>;
  #pending: PendingSend[] = [];
  #producer?: { id: bigint; epoch: number };
  #sequences = new Map<string, number>();
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
      || !("none gzip snappy lz4 zstd".split(" ").includes(this.#options.compression))) {
      throw new RangeError("Invalid producer batching options");
    }
    this.#onClose = onClose;
  }

  send(input: ProducerSend): Promise<ProduceResult[]> {
    this.#open();
    if (!input.topic) throw new TypeError("Kafka topic is required");
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

  async #flushPending(pending: PendingSend[]): Promise<void> {
    try {
      if (this.#options.idempotent && !this.#producer) {
        const response = await this.#cluster.anyRequest(API_INIT_PRODUCER_ID, 0, new Writer().string(null).i32(30_000));
        this.#cluster.throttle(API_INIT_PRODUCER_ID, response.i32());
        const error = response.i16();
        const id = response.i64();
        const epoch = response.i16();
        if (error) throw kafkaError(error, "Initialize idempotent producer");
        this.#producer = { id, epoch };
      }
      const configs = Map.groupBy(pending, ({ input }) => `${input.acks ?? 1}\0${input.timeoutMs ?? 30_000}`);
      for (const group of configs.values()) {
        const topics = Map.groupBy(group, ({ input }) => input.topic);
        const first = group[0]!.input;
        let results: ProduceResult[] | undefined;
        let lastError: unknown;
        for (let attempt = 0; attempt <= this.#cluster.retryOptions.maxRetries; attempt++) {
          try {
            const partitions = (await Promise.all([...topics].map(async ([topic, sends]) => {
              const messages = sends.flatMap(({ input }) => input.messages);
              return this.#route(topic, messages, sends[0]!.input.timeoutMs ?? 30_000, attempt > 0);
            }))).flat();
            results = await this.#produce(partitions, this.#options.idempotent || first.acks === "all" ? -1 : 1, first.timeoutMs ?? 30_000, this.#options.compression);
            break;
          } catch (error) {
            lastError = error;
            if (!(error instanceof KafkaError && error.retriable) || attempt === this.#cluster.retryOptions.maxRetries) throw error;
            const delay = retryDelay(this.#cluster.retryOptions, attempt);
            this.#cluster.event({ type: "retry", apiKey: API_PRODUCE, attempt: attempt + 1, delayMs: delay, error });
            if (delay) await Bun.sleep(delay);
          }
        }
        if (!results) throw lastError ?? new KafkaError(-1, "Kafka produce failed", { retriable: true });
        const byTopic = Map.groupBy(results, (result) => result.topic);
        for (const item of group) item.resolve(byTopic.get(item.input.topic) ?? []);
      }
    } catch (error) {
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
        if (key) partition = (murmur2(key) & 0x7fffffff) % metadata.partitions.length;
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
        group = { topic, partition, leader: meta.leader, records: [] };
        partitions.set(partition, group);
      }
      group.records.push(key && typeof message.key === "string" ? { ...message, key } : message);
    }
    return [...partitions.values()];
  }

  async #produce(partitions: PartitionRecords[], acks: number, timeoutMs: number, compression: "none" | "gzip" | "zstd"): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    const responses = await Promise.all([...leaders].map(async ([leader, leaderPartitions]) => {
      const topics = Map.groupBy(leaderPartitions, (partition) => partition.topic);
      const body = new Writer().string(null).i16(acks).i32(timeoutMs).array([...topics], (writer, [topic, topicPartitions]) => {
        writer.string(topic).array(topicPartitions, (partitionWriter, value) => {
          const key = partitionKey(value.topic, value.partition);
          const producer = this.#producer && { ...this.#producer, sequence: this.#sequences.get(key) ?? 0 };
          partitionWriter.i32(value.partition).bytes(encodeRecordBatch(value.records, Date.now(), compression, producer));
        });
      });
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
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
  #open(): void { if (this.#closed) throw new Error("Producer is closed"); }
}

export interface ConsumerOptions {
  fromBeginning?: boolean;
  fetchMaxBytes?: number;
  groupId?: string;
  sessionTimeoutMs?: number;
  rebalanceTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  autoCommit?: boolean;
}

export interface ConsumerSubscribe {
  topics: string | string[];
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

export class BunConsumer implements AsyncIterable<KafkaMessage> {
  #cluster: Cluster;
  #options: ConsumerOptions;
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
  #groupTopics: string[] = [];
  #rejoining?: Promise<void>;

  constructor(options: KafkaOptions | Cluster, consumerOptions: ConsumerOptions = {}, onClose = () => {}) {
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

  async #joinGroup(topics: string[], fromBeginning: boolean): Promise<void> {
    const coordinator = await this.#findCoordinator();
    const memberMetadata = new Writer().i16(0).array(topics, (writer, topic) => writer.string(topic)).bytes(null).result();
    const join = new Writer().string(this.#groupId!).i32(this.#options.sessionTimeoutMs ?? 45_000)
      .i32(this.#options.rebalanceTimeoutMs ?? 60_000).string(this.#memberId).string("consumer")
      .array([["range", memberMetadata] as const], (writer, [name, metadata]) => writer.string(name).bytes(metadata));
    const response = await this.#cluster.request(coordinator, API_JOIN_GROUP, 2, join);
    this.#cluster.throttle(API_JOIN_GROUP, response.i32());
    const error = response.i16();
    this.#generationId = response.i32();
    response.string();
    const leader = response.string() ?? "";
    this.#memberId = response.string() ?? "";
    const members = response.array((reader) => {
      const memberId = reader.string() ?? "";
      const metadata = new Reader(reader.bytes() ?? new Uint8Array());
      metadata.i16();
      const memberTopics = metadata.array((item) => item.string() ?? "");
      metadata.bytes();
      return { memberId, topics: memberTopics };
    });
    if (error) throw kafkaError(error, `Kafka group ${this.#groupId}`);

    const assignments = new Map<string, GroupAssignment[]>();
    if (this.#memberId === leader) {
      const metadata = await this.#cluster.metadata(topics);
      for (const member of members) assignments.set(member.memberId, []);
      for (const topic of metadata.topics) {
        const eligible = members.filter((member) => member.topics.includes(topic.name)).sort((a, b) => a.memberId.localeCompare(b.memberId));
        const partitions = topic.partitions.map(({ id }) => id).sort((a, b) => a - b);
        let start = 0;
        eligible.forEach((member, index) => {
          const count = Math.floor(partitions.length / eligible.length) + (index < partitions.length % eligible.length ? 1 : 0);
          assignments.get(member.memberId)!.push({ topic: topic.name, partitions: partitions.slice(start, start + count) });
          start += count;
        });
      }
    }
    const sync = new Writer().string(this.#groupId!).i32(this.#generationId).string(this.#memberId)
      .array([...assignments], (writer, [memberId, memberAssignments]) => {
        const assignment = new Writer().i16(0).array(memberAssignments, (assignmentWriter, item) => assignmentWriter.string(item.topic).array(item.partitions, (writer, partition) => writer.i32(partition))).bytes(null);
        writer.string(memberId).bytes(assignment.result());
      });
    const synced = await this.#cluster.request(coordinator, API_SYNC_GROUP, 0, sync);
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
    await this.assign(assigned.map((item) => ({
      ...item,
      offset: (committed.get(partitionKey(item.topic, item.partition)) ?? -1n) >= 0n
        ? committed.get(partitionKey(item.topic, item.partition))!
        : fromBeginning ? "earliest" : "latest",
    })));
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#heartbeat = setInterval(() => void this.#heartbeatOnce(coordinator), this.#options.heartbeatIntervalMs ?? 3_000);
  }

  async #heartbeatOnce(coordinator: number): Promise<void> {
    if (!this.#groupId || this.#generationId < 0 || this.#rejoining) return;
    try {
      const response = await this.#cluster.request(coordinator, API_HEARTBEAT, 0, new Writer().string(this.#groupId).i32(this.#generationId).string(this.#memberId));
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
    const body = new Writer().string(this.#groupId).i32(this.#generationId).string(this.#memberId).i32(-1)
      .array([...topics], (writer, [topic, values]) => writer.string(topic).array(values, (partitionWriter, value) => partitionWriter.i32(value.partition).i64(typeof value.offset === "bigint" ? value.offset : this.#positions.get(partitionKey(topic, value.partition)) ?? 0n).string(null)));
    const response = await this.#cluster.request(coordinator, API_OFFSET_COMMIT, 2, body);
    this.#cluster.throttle(API_OFFSET_COMMIT, response.i32());
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

  async subscribe(input: ConsumerSubscribe | string | string[]): Promise<void> {
    this.#open();
    const request = typeof input === "object" && !Array.isArray(input)
      ? input
      : { topics: input };
    const topics = Array.isArray(request.topics) ? request.topics : [request.topics];
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

  async assign(assignments: ConsumerAssignment[]): Promise<void> {
    this.#open();
    this.#assigned.clear();
    this.#positions.clear();
    this.#paused.clear();
    this.#decoders = [];
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

  async fetch(options: FetchOptions = {}): Promise<KafkaMessage[]> {
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
        const delay = retryDelay(this.#cluster.retryOptions, attempt);
        this.#cluster.event({ type: "retry", apiKey: API_FETCH, attempt: attempt + 1, delayMs: delay, error });
        if (delay) await Bun.sleep(delay);
      }
    }
    throw lastError;
  }

  async #fetchOnce(options: FetchOptions = {}): Promise<KafkaMessage[]> {
    this.#open();
    const maxMessages = options.maxMessages ?? 500;
    if (!Number.isSafeInteger(maxMessages) || maxMessages < 1) throw new RangeError("maxMessages must be a positive integer");
    if (this.#decoders.length) return this.#drain(maxMessages);
    const active = [...this.#assigned].filter(([key]) => !this.#paused.has(key));
    if (!active.length) {
      await Bun.sleep(options.maxWaitMs ?? 500);
      return [];
    }

    const leaders = Map.groupBy(active, ([, assignment]) => assignment.leader);
    const batches = await Promise.all([...leaders].map(async ([leader, entries]) => {
      const topics = Map.groupBy(entries, ([, assignment]) => assignment.topic);
      const body = new Writer().i32(-1).i32(options.maxWaitMs ?? 500).i32(options.minBytes ?? 1)
        .i32(options.maxBytes ?? this.#options.fetchMaxBytes ?? 50 * 1024 * 1024).i8(0)
        .array([...topics], (writer, [topic, partitions]) => {
          writer.string(topic).array(partitions, (partitionWriter, [key, assignment]) => {
            partitionWriter.i32(assignment.partition).i64(this.#positions.get(key) ?? 0n)
              .i32(options.maxPartitionBytes ?? 1024 * 1024);
          });
        });
      const response = await this.#cluster.request(leader, API_FETCH, 4, body, (options.maxWaitMs ?? 500) + this.#cluster.requestTimeoutMs, false);
      this.#cluster.throttle(API_FETCH, response.i32());
      return response.array((topicReader) => {
        const topic = topicReader.string() ?? "";
        return topicReader.array((partitionReader) => {
          const partition = partitionReader.i32();
          const error = partitionReader.i16();
          partitionReader.i64();
          partitionReader.i64();
          partitionReader.array((abortedReader) => ({ producerId: abortedReader.i64(), firstOffset: abortedReader.i64() }));
          const records = partitionReader.bytes();
          if (error) throw kafkaError(error, `${topic}[${partition}]`);
          return records ? new RecordSetDecoder(records, topic, partition, leader, {
            minOffset: this.#positions.get(partitionKey(topic, partition)) ?? 0n,
            copy: options.copy,
          }) : null;
        }).filter((decoder): decoder is RecordSetDecoder => decoder !== null);
      }).flat();
    }));
    this.#decoders.push(...batches.flat());
    return this.#drain(maxMessages);
  }

  #drain(max: number): KafkaMessage[] {
    const messages: KafkaMessage[] = [];
    while (this.#decoders.length && messages.length < max) {
      const decoder = this.#decoders[0]!;
      const next = decoder.read(max - messages.length);
      messages.push(...next);
      if (next.length) {
        const last = next[next.length - 1]!;
        this.#positions.set(partitionKey(last.topic, last.partition), last.offset + 1n);
      }
      if (decoder.done) this.#decoders.shift();
    }
    return messages;
  }

  async *messages(options: FetchOptions = {}): AsyncGenerator<KafkaMessage, void, unknown> {
    while (!this.#closed) {
      const messages = await this.fetch(options);
      for (const message of messages) yield message;
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    return this.messages();
  }

  seek(assignment: TopicPartition): void {
    this.#open();
    if (assignment.offset === undefined) throw new TypeError("seek requires an offset");
    const key = partitionKey(assignment.topic, assignment.partition);
    if (!this.#assigned.has(key)) throw new Error(`${assignment.topic}[${assignment.partition}] is not assigned`);
    this.#positions.set(key, BigInt(assignment.offset));
    this.#decoders = this.#decoders.filter((decoder) => partitionKey(decoder.topic, decoder.partition) !== key);
  }

  pause(partitions: TopicPartition[]): void {
    this.#open();
    for (const partition of partitions) this.#paused.add(partitionKey(partition.topic, partition.partition));
  }

  resume(partitions: TopicPartition[]): void {
    this.#open();
    for (const partition of partitions) this.#paused.delete(partitionKey(partition.topic, partition.partition));
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
        await this.#cluster.request(this.#coordinator, API_LEAVE_GROUP, 0, new Writer().string(this.#groupId).string(this.#memberId));
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

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
  #open(): void { if (this.#closed) throw new Error("Admin is closed"); }
}

export class Kafka {
  #cluster: Cluster;
  #clients = new Set<{ close(): Promise<void> }>();

  constructor(options: KafkaOptions) {
    this.#cluster = new Cluster({ ...options, brokers: [...options.brokers] });
  }

  producer(options: ProducerOptions = {}): BunProducer {
    let producer: BunProducer;
    producer = new BunProducer(this.#cluster, options, () => this.#clients.delete(producer));
    this.#clients.add(producer);
    return producer;
  }

  consumer(options: ConsumerOptions = {}): BunConsumer {
    let consumer: BunConsumer;
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
