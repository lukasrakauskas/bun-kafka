import { KafkaError } from "../errors.ts";
import { isBigInt, isString, requiredValue } from "../type-guards.ts";
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
  topics?: string | RegExp | Array<string | RegExp>;
  topic?: string | RegExp;
  fromBeginning?: boolean;
  groupId?: string;
}

function isConsumerSubscribe(
  input: ConsumerSubscribe | string | Array<string | RegExp>,
): input is ConsumerSubscribe {
  return typeof input === "object" && input !== null && !Array.isArray(input);
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
type GroupAssignment = { topic: string; partitions: number[] };

type GroupMember = {
  memberId: string;
  topics: string[];
  owned: Array<{ topic: string; partition: number }>;
};

type GroupMetadata = Awaited<ReturnType<Cluster["metadata"]>>;

function readGroupMember(reader: Reader): GroupMember {
  const memberId = reader.string() ?? "";
  const metadata = new Reader(reader.bytes() ?? new Uint8Array());
  const version = metadata.i16();
  const topics = metadata.array((item) => item.string() ?? "");
  const owned =
    version < 1
      ? []
      : metadata
          .array((ownedReader) => {
            const topic = ownedReader.string() ?? "";
            return ownedReader
              .array((item) => item.i32())
              .map((partition) => ({ topic, partition }));
          })
          .flat();
  metadata.bytes();
  return { memberId, topics, owned };
}

function assignGroupPartitions(
  members: GroupMember[],
  metadata: GroupMetadata,
  cooperative: boolean,
): Map<string, GroupAssignment[]> {
  const assignments = new Map<string, GroupAssignment[]>(
    members.map((member) => [member.memberId, []]),
  );
  for (const topic of metadata.topics) {
    if (!topic.err && topic.partitions.length) {
      assignTopicPartitions(
        assignments,
        members,
        topic.name,
        topic.partitions.map(({ id }) => id),
        cooperative,
      );
    }
  }
  return assignments;
}

function assignTopicPartitions(
  assignments: Map<string, GroupAssignment[]>,
  members: GroupMember[],
  topic: string,
  partitions: number[],
  cooperative: boolean,
): void {
  const eligible = members
    .filter((member) => member.topics.includes(topic))
    .sort((a, b) => a.memberId.localeCompare(b.memberId));
  if (!eligible.length) {
    return;
  }
  if (cooperative) {
    assignCooperatively(assignments, eligible, topic, partitions);
  } else {
    assignByRange(assignments, eligible, topic, partitions);
  }
}

function assignByRange(
  assignments: Map<string, GroupAssignment[]>,
  members: GroupMember[],
  topic: string,
  partitions: number[],
): void {
  const base = Math.floor(partitions.length / members.length);
  const extra = partitions.length % members.length;
  members.forEach((member, index) => {
    const start = index * base + Math.min(index, extra);
    const count = base + (index < extra ? 1 : 0);
    requiredValue(
      assignments.get(member.memberId),
      `Missing assignment for ${member.memberId}`,
    ).push({
      topic,
      partitions: partitions.slice(start, start + count),
    });
  });
}

function assignCooperatively(
  assignments: Map<string, GroupAssignment[]>,
  members: GroupMember[],
  topic: string,
  partitions: number[],
): void {
  const base = Math.floor(partitions.length / members.length);
  const extra = partitions.length % members.length;
  const targetSize = new Map(
    members.map((member, index) => [member.memberId, base + (index < extra ? 1 : 0)]),
  );
  const finals = new Map<string, number[]>(members.map((member) => [member.memberId, []]));
  const ownedBy = new Map<number, string>();
  retainOwnedPartitions(members, topic, partitions, targetSize, finals, ownedBy);
  for (const partition of partitions) {
    if (ownedBy.has(partition)) {
      continue;
    }
    const chosen =
      members
        .filter(
          (member) =>
            requiredValue(finals.get(member.memberId)).length <
            requiredValue(targetSize.get(member.memberId)),
        )
        .sort(
          (a, b) =>
            requiredValue(finals.get(a.memberId)).length -
              requiredValue(finals.get(b.memberId)).length || a.memberId.localeCompare(b.memberId),
        )[0] ?? requiredValue(members[0], "No group members available");
    ownedBy.set(partition, chosen.memberId);
    requiredValue(finals.get(chosen.memberId), `Missing assignment for ${chosen.memberId}`).push(
      partition,
    );
  }
  for (const [memberId, owned] of finals) {
    if (owned.length) {
      requiredValue(assignments.get(memberId), `Missing assignment for ${memberId}`).push({
        topic,
        partitions: owned.sort((a, b) => a - b),
      });
    }
  }
}

function retainOwnedPartitions(
  members: GroupMember[],
  topic: string,
  partitions: number[],
  targetSize: Map<string, number>,
  finals: Map<string, number[]>,
  ownedBy: Map<number, string>,
): void {
  for (const member of members) {
    for (const owned of member.owned) {
      if (
        owned.topic !== topic ||
        !partitions.includes(owned.partition) ||
        ownedBy.has(owned.partition)
      ) {
        continue;
      }
      const mine = requiredValue(
        finals.get(member.memberId),
        `Missing assignment for ${member.memberId}`,
      );
      if (mine.length >= requiredValue(targetSize.get(member.memberId))) {
        continue;
      }
      ownedBy.set(owned.partition, member.memberId);
      mine.push(owned.partition);
    }
  }
}

// Per-broker incremental fetch session state (KIP-227). The broker remembers
// every partition ever added to the session, so omitted partitions stay
// monitored and reappear in responses once new data arrives.
type FetchSessionState = {
  id: number; // broker-assigned session id (0 = not established yet)
  epoch: number; // INITIAL_EPOCH = 0 while opening, then increments per fetch
  sent: Map<string, bigint>; // partitionKey -> offset last requested in this session
  streaming: Map<string, boolean>; // partitionKey -> last response carried records
};

function withGroupOffsets(
  assigned: ConsumerAssignment[],
  committed: Map<string, bigint>,
  retained: Map<string, bigint>,
  fromBeginning: boolean,
): ConsumerAssignment[] {
  return assigned.map((item) => {
    const key = partitionKey(item.topic, item.partition);
    const committedOffset = committed.get(key);
    const retainedOffset = retained.get(key);
    let offset: bigint | "earliest" | "latest";
    if (retainedOffset !== undefined) {
      offset = retainedOffset;
    } else if (committedOffset !== undefined && committedOffset >= 0n) {
      offset = committedOffset;
    } else if (fromBeginning) {
      offset = "earliest";
    } else {
      offset = "latest";
    }
    return { ...item, offset };
  });
}

export class BunConsumer<K = Uint8Array | null, V = Uint8Array | null> implements AsyncIterable<
  ConsumedMessage<K, V>
> {
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

  constructor(
    options: KafkaOptions | Cluster,
    consumerOptions: ConsumerOptions<K, V> = {},
    onClose = () => {},
  ) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = options instanceof Cluster ? options : new Cluster(options);
    this.#options = consumerOptions;
    const session = consumerOptions.sessionTimeoutMs ?? 45_000;
    const rebalance = consumerOptions.rebalanceTimeoutMs ?? 60_000;
    const heartbeat = consumerOptions.heartbeatIntervalMs ?? 3_000;
    if (
      ![session, rebalance, heartbeat].every((value) => Number.isSafeInteger(value) && value > 0) ||
      heartbeat >= session
    ) {
      throw new RangeError("Invalid consumer group timeout options");
    }
    this.#onClose = onClose;
  }

  async #findCoordinator(): Promise<number> {
    if (this.#coordinator !== undefined) {
      return this.#coordinator;
    }
    const response = await this.#cluster.anyRequest(
      API_FIND_COORDINATOR,
      0,
      new Writer().string(this.#requiredGroupId()),
    );
    const error = response.i16();
    const coordinator = response.i32();
    response.string();
    response.i32();
    if (error) {
      throw kafkaError(error, `Kafka group ${this.#groupId}`);
    }
    this.#coordinator = coordinator;
    return coordinator;
  }

  /** Encode a ConsumerProtocolSubscription. v1 adds owned partitions (KIP-429). */
  #encodeSubscription(
    topics: string[],
    owned?: Array<{ topic: string; partition: number }>,
  ): Uint8Array {
    const w = new Writer()
      .i16(owned ? 1 : 0)
      .array(topics, (writer, topic) => writer.string(topic));
    if (owned) {
      const byTopic = Map.groupBy(owned, (item) => item.topic);
      w.array([...byTopic], (writer, [name, partitions]) =>
        writer
          .string(name)
          .array(partitions, (partitionWriter, p) => partitionWriter.i32(p.partition)),
      );
    }
    return w.bytes(null).result();
  }

  async #joinGroup(topics: string[], fromBeginning: boolean): Promise<void> {
    const joined = await this.#joinRequest(topics);
    const assignments = await this.#buildAssignments(
      joined.leader,
      joined.members,
      joined.cooperative,
    );
    const assigned = await this.#syncGroup(joined.coordinator, assignments);
    await this.#applyGroupAssignment(assigned, joined.cooperative, fromBeginning);
    this.#startHeartbeat(joined.coordinator);
  }

  async #joinRequest(topics: string[]): Promise<{
    coordinator: number;
    leader: string;
    members: GroupMember[];
    cooperative: boolean;
  }> {
    const coordinator = await this.#findCoordinator();
    const instanceId = this.#options.groupInstanceId;
    const cooperative = this.#options.partitionAssigner === "cooperative-sticky";
    const protocolName = cooperative ? "cooperative-sticky" : "range";
    const memberMetadata = this.#encodeSubscription(
      topics,
      cooperative ? [...this.#assigned.values()] : undefined,
    );
    const joinVersion = instanceId === undefined ? 2 : 3;
    const join = new Writer()
      .string(this.#requiredGroupId())
      .i32(this.#options.sessionTimeoutMs ?? 45_000)
      .i32(this.#options.rebalanceTimeoutMs ?? 60_000)
      .string(this.#memberId);
    if (instanceId !== undefined) {
      join.string(instanceId);
    }
    join
      .string("consumer")
      .array([[protocolName, memberMetadata] as const], (writer, [name, metadata]) =>
        writer.string(name).bytes(metadata),
      );
    const response = await this.#cluster.request(coordinator, API_JOIN_GROUP, joinVersion, join);
    this.#cluster.throttle(API_JOIN_GROUP, response.i32());
    const error = response.i16();
    this.#generationId = response.i32();
    response.string();
    const leader = response.string() ?? "";
    this.#memberId = response.string() ?? "";
    const members = response.array(readGroupMember);
    if (error) {
      throw kafkaError(error, `Kafka group ${this.#groupId}`);
    }
    return { coordinator, leader, members, cooperative };
  }

  async #buildAssignments(
    leader: string,
    members: GroupMember[],
    cooperative: boolean,
  ): Promise<Map<string, GroupAssignment[]>> {
    if (this.#memberId !== leader) {
      return new Map();
    }
    const metadata = await this.#cluster.metadata([
      ...new Set(members.flatMap((member) => member.topics)),
    ]);
    return assignGroupPartitions(members, metadata, cooperative);
  }

  async #syncGroup(
    coordinator: number,
    assignments: Map<string, GroupAssignment[]>,
  ): Promise<ConsumerAssignment[]> {
    const instanceId = this.#options.groupInstanceId;
    const version = instanceId === undefined ? 0 : 3;
    const sync = new Writer()
      .string(this.#requiredGroupId())
      .i32(this.#generationId)
      .string(this.#memberId);
    if (instanceId !== undefined) {
      sync.string(instanceId);
    }
    sync.array([...assignments], (writer, [memberId, memberAssignments]) => {
      const assignment = new Writer()
        .i16(0)
        .array(memberAssignments, (assignmentWriter, item) =>
          assignmentWriter
            .string(item.topic)
            .array(item.partitions, (partitionWriter, partition) => partitionWriter.i32(partition)),
        )
        .bytes(null);
      writer.string(memberId).bytes(assignment.result());
    });
    const response = await this.#cluster.request(coordinator, API_SYNC_GROUP, version, sync);
    if (version === 3) {
      response.i32();
    }
    const error = response.i16();
    if (error) {
      throw kafkaError(error, `Kafka group ${this.#groupId} sync`);
    }
    const reader = new Reader(response.bytes() ?? new Uint8Array());
    reader.i16();
    return reader
      .array((item) => ({
        topic: item.string() ?? "",
        partitions: item.array((partition) => partition.i32()),
      }))
      .flatMap((item) => item.partitions.map((partition) => ({ topic: item.topic, partition })));
  }

  async #applyGroupAssignment(
    assigned: ConsumerAssignment[],
    cooperative: boolean,
    fromBeginning: boolean,
  ): Promise<void> {
    const committed = new Map(
      (await this.committed(assigned)).map((item) => [
        partitionKey(item.topic, item.partition),
        item.offset,
      ]),
    );
    const retained = cooperative ? this.#retainedPositions(assigned) : new Map<string, bigint>();
    await this.assign(withGroupOffsets(assigned, committed, retained, fromBeginning));
  }

  #retainedPositions(assigned: ConsumerAssignment[]): Map<string, bigint> {
    const retained = new Map<string, bigint>();
    for (const item of assigned) {
      const key = partitionKey(item.topic, item.partition);
      const position = this.#positions.get(key);
      if (position !== undefined) {
        retained.set(key, position);
      }
    }
    return retained;
  }

  #startHeartbeat(coordinator: number): void {
    if (this.#heartbeat) {
      clearInterval(this.#heartbeat);
    }
    this.#heartbeat = setInterval(
      () => void this.#heartbeatOnce(coordinator),
      this.#options.heartbeatIntervalMs ?? 3_000,
    );
  }

  async #heartbeatOnce(coordinator: number): Promise<void> {
    if (!this.#groupId || this.#generationId < 0 || this.#rejoining) {
      return;
    }
    try {
      await this.#sendHeartbeat(coordinator);
    } catch {
      this.#coordinator = undefined;
    }
    await this.#restartGroup();
  }

  async #sendHeartbeat(coordinator: number): Promise<void> {
    const instanceId = this.#options.groupInstanceId;
    const version = instanceId === undefined ? 0 : 3;
    const body = new Writer()
      .string(this.#requiredGroupId())
      .i32(this.#generationId)
      .string(this.#memberId);
    if (instanceId !== undefined) {
      body.string(instanceId);
    }
    const response = await this.#cluster.request(coordinator, API_HEARTBEAT, version, body);
    if (version === 3) {
      response.i32();
    }
    const error = response.i16();
    if (!error) {
      return;
    }
    if (error === 25) {
      this.#memberId = "";
    }
    if (error !== 22 && error !== 25 && error !== 27) {
      throw kafkaError(error, `Kafka group ${this.#groupId} heartbeat`);
    }
  }

  async #restartGroup(): Promise<void> {
    if (this.#heartbeat) {
      clearInterval(this.#heartbeat);
    }
    this.#assigned.clear();
    this.#positions.clear();
    this.#decoders = [];
    this.#rejoining = this.#joinGroup(
      this.#groupTopics,
      this.#options.fromBeginning ?? false,
    ).finally(() => {
      this.#rejoining = undefined;
    });
    try {
      await this.#rejoining;
    } catch {
      this.#coordinator = undefined;
    }
  }

  async commitOffsets(
    assignments: readonly ConsumerAssignment[] = this.assignment().map(
      ({ topic, partition, offset }) => ({
        topic,
        partition,
        offset: isBigInt(offset) ? offset : undefined,
      }),
    ),
  ): Promise<void> {
    this.#open();
    if (!this.#groupId) {
      throw new Error("Consumer groupId is required for offset commits");
    }
    const coordinator = await this.#findCoordinator();
    const topics = Map.groupBy(assignments, (assignment) => assignment.topic);
    const body = new Writer()
      .string(this.#groupId)
      .i32(this.#generationId)
      .string(this.#memberId)
      .i64(-1n)
      .array([...topics], (writer, [topic, values]) =>
        writer.string(topic).array(values, (partitionWriter, value) =>
          partitionWriter
            .i32(value.partition)
            .i64(
              isBigInt(value.offset)
                ? value.offset
                : (this.#positions.get(partitionKey(topic, value.partition)) ?? 0n),
            )
            .string(null),
        ),
      );
    const response = await this.#cluster.request(coordinator, API_OFFSET_COMMIT, 2, body);
    for (const result of response.array((reader) => ({
      topic: reader.string() ?? "",
      partitions: reader.array((partitionReader) => ({
        partition: partitionReader.i32(),
        error: partitionReader.i16(),
      })),
    }))) {
      for (const partition of result.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `${result.topic}[${partition.partition}]`);
        }
      }
    }
  }

  async committed(assignments: readonly ConsumerAssignment[]): Promise<CommittedOffset[]> {
    this.#open();
    if (!this.#groupId) {
      throw new Error("Consumer groupId is required for offset fetch");
    }
    const coordinator = await this.#findCoordinator();
    const topics = Map.groupBy(assignments, (assignment) => assignment.topic);
    const body = new Writer()
      .string(this.#groupId)
      .array([...topics], (writer, [topic, values]) =>
        writer
          .string(topic)
          .array(values, (partitionWriter, value) => partitionWriter.i32(value.partition)),
      );
    const response = await this.#cluster.request(coordinator, API_OFFSET_FETCH, 2, body);
    const result: CommittedOffset[] = [];
    for (const topic of response.array((reader) => ({
      topic: reader.string() ?? "",
      partitions: reader.array((partitionReader) => ({
        partition: partitionReader.i32(),
        offset: partitionReader.i64(),
        metadata: partitionReader.string(),
        error: partitionReader.i16(),
      })),
    }))) {
      for (const partition of topic.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `${topic.topic}[${partition.partition}]`);
        }
        result.push({
          topic: topic.topic,
          partition: partition.partition,
          offset: partition.offset,
        });
      }
    }
    const error = response.i16();
    if (error) {
      throw kafkaError(error, `Kafka group ${this.#groupId}`);
    }
    return result;
  }

  async subscribe(input: ConsumerSubscribe | string | Array<string | RegExp>): Promise<void> {
    this.#open();
    const request = isConsumerSubscribe(input) ? input : { topics: input };
    const requested = request.topics ?? request.topic;
    if (requested === undefined) {
      throw new TypeError("subscribe requires a topic");
    }
    const topics = await this.#resolveTopicPatterns(
      isString(requested) || requested instanceof RegExp ? [requested] : requested,
    );
    const groupId = request.groupId ?? this.#options.groupId;
    if (groupId) {
      this.#groupId = groupId;
      this.#groupTopics = topics;
      await this.#joinGroup(topics, request.fromBeginning ?? this.#options.fromBeginning ?? false);
      return;
    }
    await this.#subscribeWithoutGroup(topics, request.fromBeginning ?? this.#options.fromBeginning);
  }

  async #subscribeWithoutGroup(
    topics: string[],
    fromBeginning: boolean | undefined,
  ): Promise<void> {
    const metadata = await this.#cluster.metadata(topics);
    const assignments = topics.flatMap((topic) => {
      const found = metadata.topics.find((item) => item.name === topic);
      if (!found || found.err) {
        throw kafkaError(found?.err ?? 3, topic);
      }
      return found.partitions.map((partition) => ({
        topic,
        partition: partition.id,
        offset: fromBeginning ? ("earliest" as const) : ("latest" as const),
      }));
    });
    await this.assign(assignments);
  }

  /** Expands RegExp topic patterns against cluster metadata into literal topic names. */
  async #resolveTopicPatterns(topics: Array<string | RegExp>): Promise<string[]> {
    const patterns = topics.filter((topic): topic is RegExp => topic instanceof RegExp);
    if (!patterns.length) {
      return [...new Set(topics.filter(isString))];
    }
    const metadata = await this.#cluster.metadata(null);
    const resolved = new Set<string>();
    for (const entry of metadata.topics) {
      if (entry.err || !entry.name) {
        continue;
      }
      const matchesPattern = patterns.some((pattern) => pattern.test(entry.name));
      const listedLiteral = topics.some((topic) => isString(topic) && topic === entry.name);
      if (matchesPattern || listedLiteral) {
        resolved.add(entry.name);
      }
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
      if (!partition) {
        throw new RangeError(
          `Partition ${assignment.partition} does not exist on ${assignment.topic}`,
        );
      }
      const key = partitionKey(assignment.topic, assignment.partition);
      const assigned = {
        topic: assignment.topic,
        partition: assignment.partition,
        leader: partition.leader,
      };
      this.#assigned.set(key, assigned);
      if (isBigInt(assignment.offset)) {
        this.#positions.set(key, assignment.offset);
      } else {
        unresolved.push({
          ...assigned,
          which: assignment.offset ?? (this.#options.fromBeginning ? "earliest" : "latest"),
        });
      }
    }
    await this.#resolveOffsets(unresolved);
  }

  async #resolveOffsets(
    assignments: Array<Assigned & { which: "earliest" | "latest" }>,
  ): Promise<void> {
    const leaders = Map.groupBy(assignments, (assignment) => assignment.leader);
    await Promise.all(
      [...leaders].map(async ([leader, values]) => {
        const topics = Map.groupBy(values, (assignment) => assignment.topic);
        const body = new Writer().i32(-1).array([...topics], (writer, [topic, partitions]) => {
          writer.string(topic).array(partitions, (partitionWriter, value) => {
            partitionWriter.i32(value.partition).i64(value.which === "earliest" ? -2 : -1);
          });
        });
        const response = await this.#cluster.request(leader, API_LIST_OFFSETS, 1, body);
        for (const result of response
          .array((topicReader) => {
            const topic = topicReader.string() ?? "";
            return topicReader.array((partitionReader) => {
              const partition = partitionReader.i32();
              const error = partitionReader.i16();
              partitionReader.i64();
              const offset = partitionReader.i64();
              return { topic, partition, error, offset };
            });
          })
          .flat()) {
          if (result.error) {
            throw kafkaError(result.error, `${result.topic}[${result.partition}]`);
          }
          this.#positions.set(partitionKey(result.topic, result.partition), result.offset);
        }
      }),
    );
  }

  async fetch(options: FetchOptions = {}): Promise<Array<ConsumedMessage<K, V>>> {
    this.#open();
    return this.#fetchWithRetry(options);
  }

  async #fetchWithRetry(options: FetchOptions): Promise<Array<ConsumedMessage<K, V>>> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.#cluster.retryOptions.maxRetries; attempt++) {
      try {
        const messages = await this.#fetchOnce(options);
        if (messages.length && this.#groupId && this.#options.autoCommit) {
          await this.commitOffsets();
        }
        return messages;
      } catch (error) {
        lastError = error;
        if (!(error instanceof KafkaError)) {
          throw error;
        }
        await this.#handleFetchError(error, attempt);
      }
    }
    throw lastError;
  }

  async #handleFetchError(error: KafkaError, attempt: number): Promise<void> {
    const maxRetries = this.#cluster.retryOptions.maxRetries;
    if (!error.retriable || attempt === maxRetries) {
      throw error;
    }
    await this.#retryFetch(attempt, error);
  }

  async #retryFetch(attempt: number, error: KafkaError): Promise<void> {
    await this.#refreshFetchLeaders();
    this.#cluster.bumpRetries();
    const delay = retryDelay(this.#cluster.retryOptions, attempt);
    this.#cluster.log(
      "warn",
      `retrying fetch attempt ${attempt + 1} in ${delay}ms: ${String(error)}`,
    );
    this.#cluster.event({
      type: "retry",
      apiKey: API_FETCH,
      attempt: attempt + 1,
      delayMs: delay,
      error,
    });
    if (delay) {
      await Bun.sleep(delay);
    }
  }

  async #refreshFetchLeaders(): Promise<void> {
    for (const assigned of this.#assigned.values()) {
      const metadata = await this.#cluster.topic(assigned.topic, true);
      const partition = metadata.partitions.find((item) => item.id === assigned.partition);
      if (partition) {
        assigned.leader = partition.leader;
      }
    }
  }

  async #fetchOnce(options: FetchOptions = {}): Promise<Array<ConsumedMessage<K, V>>> {
    this.#open();
    const maxMessages = options.maxMessages ?? 500;
    if (!Number.isSafeInteger(maxMessages) || maxMessages < 1) {
      throw new RangeError("maxMessages must be a positive integer");
    }
    if (this.#decoders.length) {
      return this.#drain(maxMessages);
    }
    const active = [...this.#assigned].filter(([key]) => !this.#paused.has(key));
    if (!active.length) {
      // Bounded idle nap so pause()/resume()/seek() take effect promptly.
      await Bun.sleep(Math.min(options.maxWaitMs ?? 500, 250));
      return [];
    }

    const isolationLevel = this.#options.isolationLevel === "read_committed" ? 1 : 0;
    const leaders = Map.groupBy(active, ([, assignment]) => assignment.leader);
    const batches = await Promise.all(
      [...leaders].map(async ([leader, entries]) =>
        this.#fetchBatchesFor(leader, entries, options, isolationLevel),
      ),
    );
    this.#decoders.push(...batches.flat());
    return this.#drain(maxMessages);
  }

  async #fetchBatchesFor(
    leader: number,
    entries: Array<[string, Assigned]>,
    options: FetchOptions,
    isolationLevel: number,
  ): Promise<RecordSetDecoder[]> {
    const session = this.#fetchSessions.get(leader) ?? this.#newFetchSession();
    const body = this.#buildFetchBody(session, entries, options, isolationLevel);
    const response = await this.#cluster.request(
      leader,
      API_FETCH,
      7,
      body,
      (options.maxWaitMs ?? 500) + this.#cluster.requestTimeoutMs,
      false,
    );
    return this.#decodeFetchResponse(leader, entries, options, isolationLevel, session, response);
  }

  #newFetchSession(): FetchSessionState {
    return { id: 0, epoch: 0, sent: new Map(), streaming: new Map() };
  }

  #buildFetchBody(
    session: FetchSessionState,
    entries: Array<[string, Assigned]>,
    options: FetchOptions,
    isolationLevel: number,
  ): Writer {
    const requested = this.#requestedPartitions(session, entries);
    const forgotten = this.#forgottenPartitions(session);
    const byTopic = Map.groupBy(requested, ([, assignment]) => assignment.topic);
    return new Writer()
      .i32(-1)
      .i32(options.maxWaitMs ?? 500)
      .i32(options.minBytes ?? 1)
      .i32(options.maxBytes ?? this.#options.fetchMaxBytes ?? 50 * 1024 * 1024)
      .i8(isolationLevel)
      .i32(session.id)
      .i32(session.epoch)
      .array([...byTopic], (writer, [topic, partitions]) => {
        writer.string(topic).array(partitions, (partitionWriter, [key, assignment]) => {
          partitionWriter
            .i32(assignment.partition)
            .i64(this.#positions.get(key) ?? 0n)
            .i64(-1)
            .i32(options.maxPartitionBytes ?? 1024 * 1024);
        });
      })
      .array([...forgotten], (writer, [topic, partitions]) =>
        writer
          .string(topic)
          .array(partitions, (partitionWriter, partition) => partitionWriter.i32(partition)),
      );
  }

  #requestedPartitions(
    session: FetchSessionState,
    entries: Array<[string, Assigned]>,
  ): Array<[string, Assigned]> {
    return entries.filter(([key]) => {
      const position = this.#positions.get(key) ?? 0n;
      return (
        session.epoch === 0 || session.sent.get(key) !== position || session.streaming.get(key)
      );
    });
  }

  #forgottenPartitions(session: FetchSessionState): Map<string, number[]> {
    const forgotten = new Map<string, number[]>();
    for (const key of [...session.sent.keys(), ...session.streaming.keys()]) {
      if (this.#assigned.has(key) && !this.#paused.has(key)) {
        continue;
      }
      const [topic, partition] = key.split("\0");
      if (topic === undefined || partition === undefined) {
        continue;
      }
      forgotten.set(topic, [...(forgotten.get(topic) ?? []), Number(partition)]);
      session.sent.delete(key);
      session.streaming.delete(key);
    }
    return forgotten;
  }

  async #decodeFetchResponse(
    leader: number,
    entries: Array<[string, Assigned]>,
    options: FetchOptions,
    isolationLevel: number,
    session: FetchSessionState,
    response: Reader,
  ): Promise<RecordSetDecoder[]> {
    if (process.env.DEBUG_FETCH) {
      console.error(
        "fetch resp:",
        Array.from(response.data.slice(0, 80))
          .map((b) => b.toString(16).padStart(2, "0"))
          .join(" "),
      );
    }
    this.#cluster.throttle(API_FETCH, response.i32());
    const topError = response.i16();
    const sessionId = response.i32();
    if (topError) {
      this.#fetchSessions.delete(leader);
      if (topError === 70 || topError === 71) {
        return this.#fetchBatchesFor(leader, entries, options, isolationLevel);
      }
      throw kafkaError(topError, `Fetch from broker ${leader}`);
    }
    session.id = session.id === 0 ? sessionId : session.id;
    session.epoch = session.id === sessionId && session.epoch === 0 ? 1 : session.epoch + 1;
    this.#fetchSessions.set(leader, session);
    return response
      .array((topicReader) =>
        this.#decodeFetchTopic(topicReader, leader, options, isolationLevel, session),
      )
      .flat();
  }

  #decodeFetchTopic(
    reader: Reader,
    leader: number,
    options: FetchOptions,
    isolationLevel: number,
    session: FetchSessionState,
  ): RecordSetDecoder[] {
    const topic = reader.string() ?? "";
    return reader
      .array((partitionReader) => {
        const partition = partitionReader.i32();
        return this.#decodeFetchPartition(
          partitionReader,
          topic,
          partition,
          leader,
          options,
          isolationLevel,
          session,
        );
      })
      .filter((decoder): decoder is RecordSetDecoder => decoder !== null);
  }

  #decodeFetchPartition(
    reader: Reader,
    topic: string,
    partition: number,
    leader: number,
    options: FetchOptions,
    isolationLevel: number,
    session: FetchSessionState,
  ): RecordSetDecoder | null {
    const error = reader.i16();
    reader.i64();
    reader.i64();
    reader.i64();
    const abortedTransactions = reader.array((item) => ({
      producerId: item.i64(),
      firstOffset: item.i64(),
    }));
    const records = reader.bytes();
    const key = partitionKey(topic, partition);
    session.sent.set(key, this.#positions.get(key) ?? 0n);
    session.streaming.set(key, Boolean(records));
    if (error) {
      throw kafkaError(error, `${topic}[${partition}]`);
    }
    return records
      ? new RecordSetDecoder(records, topic, partition, leader, {
          minOffset: this.#positions.get(key) ?? 0n,
          copy: options.copy,
          abortedTransactions: isolationLevel === 1 ? abortedTransactions : undefined,
        })
      : null;
  }

  #drain(max: number): Array<ConsumedMessage<K, V>> {
    const messages: Array<ConsumedMessage<K, V>> = [];
    while (this.#decoders.length && messages.length < max) {
      const decoder = this.#decoders[0];
      if (!decoder) {
        break;
      }
      for (const message of decoder.read(max - messages.length)) {
        messages.push(this.#convertMessage(message));
      }
      if (decoder.done) {
        this.#decoders.shift();
      }
    }
    return messages;
  }

  #convertMessage(message: ConsumedMessage): ConsumedMessage<K, V> {
    this.#positions.set(partitionKey(message.topic, message.partition), message.offset + 1n);
    if (!this.#options.keyDeserializer && !this.#options.valueDeserializer) {
      return message as ConsumedMessage<K, V>;
    }
    const context = {
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
      timestamp: message.timestamp,
    };
    return {
      ...message,
      key: this.#options.keyDeserializer?.(message.key, context) ?? null,
      value: this.#options.valueDeserializer?.(message.value, context) ?? null,
    } as ConsumedMessage<K, V>;
  }

  async *messages(
    options: FetchOptions = {},
  ): AsyncGenerator<ConsumedMessage<K, V>, void, unknown> {
    while (!this.#closed) {
      const messages = await this.fetch(options);
      for (const message of messages) {
        yield message;
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<ConsumedMessage<K, V>> {
    return this.messages();
  }

  seek(assignment: TopicPartition): void {
    this.#open();
    if (assignment.offset === undefined) {
      throw new TypeError("seek requires an offset");
    }
    const key = partitionKey(assignment.topic, assignment.partition);
    if (!this.#assigned.has(key)) {
      throw new Error(`${assignment.topic}[${assignment.partition}] is not assigned`);
    }
    this.#positions.set(key, BigInt(assignment.offset));
    this.#decoders = this.#decoders.filter(
      (decoder) => partitionKey(decoder.topic, decoder.partition) !== key,
    );
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
    for (const target of this.#expandPartitions(partitions)) {
      this.#paused.add(partitionKey(target.topic, target.partition));
    }
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
      if (partition !== undefined) {
        return [{ topic, partition }];
      }
      return [...this.#assigned.values()]
        .filter((assigned) => assigned.topic === topic)
        .map(({ partition: assignedPartition }) => ({
          topic,
          partition: assignedPartition,
        }));
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
    if (leader === undefined) {
      throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
    }
    const query = async (timestamp: number) => {
      const body = new Writer().i32(-1).array([topic], (writer) =>
        writer.string(topic).array([partition], (partitionWriter) => {
          partitionWriter.i32(partition).i64(timestamp);
        }),
      );
      const response = await this.#cluster.request(leader, API_LIST_OFFSETS, 1, body);
      const result = response.array((topicReader) => {
        topicReader.string();
        return topicReader.array((partitionReader) => {
          partitionReader.i32();
          const error = partitionReader.i16();
          partitionReader.i64();
          const offset = partitionReader.i64();
          if (error) {
            throw kafkaError(error, `${topic}[${partition}]`);
          }
          return offset;
        });
      });
      return result[0]?.[0] ?? -1n;
    };
    const [low, high] = await Promise.all([query(-2), query(-1)]);
    return { low, high };
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    if (this.#heartbeat) {
      clearInterval(this.#heartbeat);
    }
    if (this.#groupId && this.#coordinator !== undefined && this.#generationId >= 0) {
      try {
        const instanceId = this.#options.groupInstanceId;
        if (instanceId === undefined) {
          await this.#cluster.request(
            this.#coordinator,
            API_LEAVE_GROUP,
            0,
            new Writer().string(this.#groupId).string(this.#memberId),
          );
        } else {
          // LeaveGroup v3+ sends a member list that carries static identity.
          await this.#cluster.request(
            this.#coordinator,
            API_LEAVE_GROUP,
            3,
            new Writer()
              .string(this.#groupId)
              .array([{ memberId: this.#memberId, instanceId }], (writer, member) => {
                writer.string(member.memberId).string(member.instanceId ?? null);
              }),
          );
        }
      } catch {
        // The broker may already be unavailable during shutdown.
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
  #requiredGroupId(): string {
    if (!this.#groupId) {
      throw new Error("Consumer groupId is required");
    }
    return this.#groupId;
  }

  #open(): void {
    if (this.#closed) {
      throw new Error("Consumer is closed");
    }
  }
}
