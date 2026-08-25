import { KafkaError } from "../errors.ts";
import type { ConsumedMessage, TopicPartition, Watermarks } from "../types.ts";
import { Cluster } from "./cluster.ts";
import { RecordSetDecoder, Reader, Writer } from "./protocol.ts";
import {
  API_FETCH,
  API_FIND_COORDINATOR,
  API_HEARTBEAT,
  API_JOIN_GROUP,
  API_LEAVE_GROUP,
  API_LIST_OFFSETS,
  API_OFFSET_COMMIT,
  API_OFFSET_FETCH,
  API_SYNC_GROUP,
  kafkaError,
  partitionKey,
  retryDelay,
  type CommittedOffset,
  type KafkaOptions,
} from "./shared.ts";

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
    // Accept both `topics` and the singular `topic` spelling used by kafkajs.
    const requested = request.topics ?? (request as { topic?: string | RegExp }).topic;
    let topics = await this.#resolveTopicPatterns(
      typeof requested === "string" || requested instanceof RegExp ? [requested] : requested,
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
      // Bounded idle nap so pause()/resume()/seek() take effect promptly.
      await Bun.sleep(Math.min(options.maxWaitMs ?? 500, 250));
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

  /**
   * Pause fetching. Entries may omit `partition` to pause every currently
   * assigned partition of the topic.
   */
  pause(partitions: TopicPartition[]): void {
    this.#open();
    for (const target of this.#expandPartitions(partitions)) this.#paused.add(partitionKey(target.topic, target.partition));
  }

  resume(partitions: TopicPartition[]): void {
    this.#open();
    for (const target of this.#expandPartitions(partitions)) {
      const key = partitionKey(target.topic, target.partition);
      this.#paused.delete(key);
      // Re-join the fetch session so the resumed partition is polled again.
      for (const session of this.#fetchSessions.values()) {
        session.sent.delete(key);
        session.streaming.delete(key);
      }
    }
  }

  #expandPartitions(partitions: TopicPartition[]): Array<{ topic: string; partition: number }> {
    return partitions.flatMap(({ topic, partition }) => {
      if (partition !== undefined) return [{ topic, partition }];
      return [...this.#assigned.values()].filter((assigned) => assigned.topic === topic).map(({ partition }) => ({ topic, partition }));
    });
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
