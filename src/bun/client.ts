import { KafkaError } from "../errors.ts";
import type { Bytes, ClusterMetadata, KafkaMessage, MessageHeaders, TopicPartition, Watermarks } from "../types.ts";
import { Connection, type BunKafkaTls, type ConnectionOptions } from "./connection.ts";
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
const retriableErrors = new Set([3, 5, 6, 7, 13, 14, 15, 19, 20, 56]);

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

export interface KafkaOptions {
  brokers: string[];
  clientId?: string;
  tls?: BunKafkaTls;
  requestTimeoutMs?: number;
  maxResponseBytes?: number;
}

type TopicMetadata = ClusterMetadata["topics"][number];

export class Cluster {
  #bootstrap: string[];
  #options: ConnectionOptions;
  #connections = new Map<string, Connection>();
  #brokers = new Map<number, string>();
  #topics = new Map<string, TopicMetadata>();

  constructor(options: KafkaOptions) {
    if (!Array.isArray(options.brokers) || !options.brokers.length) throw new TypeError("Kafka requires at least one broker");
    const requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    const maxResponseBytes = options.maxResponseBytes ?? 100 * 1024 * 1024;
    if (requestTimeoutMs <= 0 || maxResponseBytes < 4) throw new RangeError("Invalid Kafka timeout or response size");
    this.#bootstrap = [...options.brokers];
    this.#options = {
      clientId: options.clientId ?? "bun-kafka",
      requestTimeoutMs,
      maxResponseBytes,
      tls: options.tls,
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

  async request(brokerId: number, apiKey: number, apiVersion: number, body: Writer, timeoutMs?: number): Promise<Reader> {
    let broker = this.#brokers.get(brokerId);
    if (!broker) {
      await this.metadata();
      broker = this.#brokers.get(brokerId);
    }
    if (!broker) throw new KafkaError(-1, `Kafka broker ${brokerId} is not in metadata`, { retriable: true });
    return this.#connection(broker).request(apiKey, apiVersion, body, timeoutMs);
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
  #queuedMessages = 0;
  #timer?: ReturnType<typeof setTimeout>;
  #flushing?: Promise<void>;

  constructor(options: KafkaOptions | Cluster, producerOptions: ProducerOptions = {}, onClose = () => {}) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = this.#ownsCluster ? new Cluster(options as KafkaOptions) : options as Cluster;
    this.#options = { lingerMs: producerOptions.lingerMs ?? 5, batchMaxMessages: producerOptions.batchMaxMessages ?? 1_000 };
    if (!Number.isFinite(this.#options.lingerMs) || this.#options.lingerMs < 0
      || !Number.isSafeInteger(this.#options.batchMaxMessages) || this.#options.batchMaxMessages < 1) {
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
      const configs = Map.groupBy(pending, ({ input }) => `${input.acks ?? 1}\0${input.timeoutMs ?? 30_000}`);
      for (const group of configs.values()) {
        const topics = Map.groupBy(group, ({ input }) => input.topic);
        const partitions = (await Promise.all([...topics].map(async ([topic, sends]) => {
          const messages = sends.flatMap(({ input }) => input.messages);
          return this.#route(topic, messages, sends[0]!.input.timeoutMs ?? 30_000);
        }))).flat();
        const first = group[0]!.input;
        const results = await this.#produce(partitions, first.acks === "all" ? -1 : 1, first.timeoutMs ?? 30_000);
        const byTopic = Map.groupBy(results, (result) => result.topic);
        for (const item of group) item.resolve(byTopic.get(item.input.topic) ?? []);
      }
    } catch (error) {
      for (const item of pending) item.reject(error);
      throw error;
    }
  }

  async #route(topic: string, messages: readonly ProducerMessage[], timeoutMs: number): Promise<PartitionRecords[]> {
    let metadata: TopicMetadata | undefined;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      metadata = await this.#cluster.topic(topic, !!metadata);
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

  async #produce(partitions: PartitionRecords[], acks: number, timeoutMs: number): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    const responses = await Promise.all([...leaders].map(async ([leader, leaderPartitions]) => {
      const topics = Map.groupBy(leaderPartitions, (partition) => partition.topic);
      const body = new Writer().string(null).i16(acks).i32(timeoutMs).array([...topics], (writer, [topic, topicPartitions]) => {
        writer.string(topic).array(topicPartitions, (partitionWriter, value) => {
          partitionWriter.i32(value.partition).bytes(encodeRecordBatch(value.records));
        });
      });
      const response = await this.#cluster.request(leader, API_PRODUCE, 3, body, timeoutMs);
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
      response.i32();
      return results;
    }));
    return responses.flat();
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
}

export interface ConsumerSubscribe {
  topics: string | string[];
  fromBeginning?: boolean;
}

export interface ConsumerAssignment {
  topic: string;
  partition: number;
  offset?: bigint | "earliest" | "latest";
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

  constructor(options: KafkaOptions | Cluster, consumerOptions: ConsumerOptions = {}, onClose = () => {}) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = this.#ownsCluster ? new Cluster(options as KafkaOptions) : options as Cluster;
    this.#options = consumerOptions;
    this.#onClose = onClose;
  }

  async subscribe(input: ConsumerSubscribe | string | string[]): Promise<void> {
    this.#open();
    const request = typeof input === "object" && !Array.isArray(input)
      ? input
      : { topics: input };
    const topics = Array.isArray(request.topics) ? request.topics : [request.topics];
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
      const response = await this.#cluster.request(leader, API_FETCH, 4, body, (options.maxWaitMs ?? 500) + 30_000);
      response.i32();
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
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
  #open(): void { if (this.#closed) throw new Error("Consumer is closed"); }
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

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> { return this.close(); }
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
