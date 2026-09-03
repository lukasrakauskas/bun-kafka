import { isBigInt, isString } from "../type-guards.ts";
import type { ConsumedMessage, TopicPartition, Watermarks } from "../types.ts";
import { Cluster } from "../bun/cluster.ts";
import { GroupCoordinator } from "./group.ts";
import { OffsetStore } from "./offsets.ts";
import { Heartbeat } from "./heartbeat.ts";
import { Fetcher } from "./fetch.ts";
import { MessageDecoder } from "./message-decoder.ts";
import {
  expandPartitions,
  fetchWatermarks,
  resolveTopicPatterns,
  topicAssignments,
} from "./subscription.ts";
import type {
  Assigned,
  ConsumerAssignment,
  ConsumerOptions,
  ConsumerState,
  ConsumerSubscribe,
  FetchOptions,
} from "./types.ts";
import {
  API_LEAVE_GROUP,
  GROUP_INSTANCE_API_VERSION,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_REBALANCE_TIMEOUT_MS,
  DEFAULT_SESSION_TIMEOUT_MS,
  partitionKey,
  type KafkaOptions,
} from "../bun/shared.ts";
import { writeLeaveGroupRequest } from "../protocol/index.ts";

export type {
  ConsumerOptions,
  ConsumerSubscribe,
  ConsumerAssignment,
  FetchOptions,
} from "./types.ts";
export type { ConsumerGroupEvent, DeserializerContext } from "./types.ts";

function isConsumerSubscribe(
  input: ConsumerSubscribe | string | Array<string | RegExp>,
): input is ConsumerSubscribe {
  return typeof input === "object" && input !== null && !Array.isArray(input);
}

export class Consumer<K = Uint8Array | null, V = Uint8Array | null> implements AsyncIterable<
  ConsumedMessage<K, V>
> {
  #cluster: Cluster;
  #options: ConsumerOptions<K, V>;
  #assigned = new Map<string, Assigned>();
  #positions = new Map<string, bigint>();
  #paused = new Set<string>();
  #state: ConsumerState = { memberId: "", generationId: -1, groupTopics: [] };
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;
  #group: GroupCoordinator;
  #offsets: OffsetStore;
  #heartbeat: Heartbeat;
  #decoder: MessageDecoder<K, V>;
  #fetcher: Fetcher<K, V>;

  constructor(
    options: KafkaOptions | Cluster,
    consumerOptions: ConsumerOptions<K, V> = {},
    onClose = () => {},
  ) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = options instanceof Cluster ? options : new Cluster(options);
    this.#options = consumerOptions;
    const session = consumerOptions.sessionTimeoutMs ?? DEFAULT_SESSION_TIMEOUT_MS;
    const rebalance = consumerOptions.rebalanceTimeoutMs ?? DEFAULT_REBALANCE_TIMEOUT_MS;
    const heartbeat = consumerOptions.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
    if (
      ![session, rebalance, heartbeat].every((value) => Number.isSafeInteger(value) && value > 0) ||
      heartbeat >= session
    ) {
      throw new RangeError("Invalid consumer group timeout options");
    }
    this.#onClose = onClose;
    this.#decoder = new MessageDecoder(consumerOptions, this.#positions);
    this.#offsets = new OffsetStore(this.#cluster, consumerOptions, this.#positions, this.#state);
    this.#group = new GroupCoordinator({
      cluster: this.#cluster,
      options: consumerOptions,
      state: this.#state,
      assigned: this.#assigned,
      positions: this.#positions,
      committed: (assignments) => this.#offsets.committed(assignments),
      assign: (assignments) => this.assign(assignments),
      onEvent: consumerOptions.onGroupEvent ?? (() => {}),
    });
    this.#heartbeat = new Heartbeat(this.#cluster, consumerOptions, this.#state, () =>
      this.#restartGroup(),
    );
    this.#fetcher = new Fetcher(
      this.#cluster,
      consumerOptions,
      this.#state,
      this.#assigned,
      this.#positions,
      this.#paused,
      this.#decoder,
      () => this.commitOffsets(),
    );
  }

  async #joinGroup(topics: string[], fromBeginning: boolean): Promise<void> {
    const coordinator = await this.#group.join(topics, fromBeginning);
    this.#heartbeat.start(coordinator);
  }

  async #restartGroup(): Promise<void> {
    this.#heartbeat.stop();
    this.#assigned.clear();
    this.#positions.clear();
    this.#decoder.clear();
    this.#fetcher.reset();
    this.#state.rejoining = this.#joinGroup(
      this.#state.groupTopics,
      this.#options.fromBeginning ?? false,
    ).finally(() => {
      this.#state.rejoining = undefined;
    });
    try {
      await this.#state.rejoining;
    } catch {
      this.#state.coordinator = undefined;
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
    await this.#offsets.commit(assignments);
  }

  async committed(
    assignments: readonly ConsumerAssignment[],
  ): Promise<import("../bun/shared.ts").CommittedOffset[]> {
    this.#open();
    return this.#offsets.committed(assignments);
  }

  async subscribe(input: ConsumerSubscribe | string | Array<string | RegExp>): Promise<void> {
    this.#open();
    const request = isConsumerSubscribe(input) ? input : { topics: input };
    const requested = request.topics ?? request.topic;
    if (requested === undefined) {
      throw new TypeError("subscribe requires a topic");
    }
    const topics = await resolveTopicPatterns(
      this.#cluster,
      isString(requested) || requested instanceof RegExp ? [requested] : requested,
    );
    const groupId = request.groupId ?? this.#options.groupId;
    if (groupId) {
      this.#state.groupId = groupId;
      this.#state.groupTopics = topics;
      await this.#joinGroup(topics, request.fromBeginning ?? this.#options.fromBeginning ?? false);
      return;
    }
    await this.assign(
      await topicAssignments(
        this.#cluster,
        topics,
        request.fromBeginning ?? this.#options.fromBeginning,
      ),
    );
  }

  async assign(assignments: ConsumerAssignment[]): Promise<void> {
    this.#open();
    this.#assigned.clear();
    this.#positions.clear();
    this.#paused.clear();
    this.#decoder.clear();
    this.#fetcher.reset();
    const unresolved: Array<Assigned & { which: "earliest" | "latest" }> = [];
    for (const assignment of assignments) {
      const metadata = await this.#cluster.topic(assignment.topic);
      const partition = metadata.partitions.find((item) => item.id === assignment.partition);
      if (!partition) {
        throw new RangeError(
          `Partition ${assignment.partition} does not exist on ${assignment.topic}`,
        );
      }
      const assigned = {
        topic: assignment.topic,
        partition: assignment.partition,
        leader: partition.leader,
      };
      const key = partitionKey(assignment.topic, assignment.partition);
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
    await this.#offsets.resolve(unresolved);
  }

  async fetch(options: FetchOptions = {}): Promise<Array<ConsumedMessage<K, V>>> {
    this.#open();
    return this.#fetcher.fetch(options);
  }

  async *messages(
    options: FetchOptions = {},
  ): AsyncGenerator<ConsumedMessage<K, V>, void, unknown> {
    while (!this.#closed) {
      for (const message of await this.fetch(options)) {
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
    this.#decoder.clearPartition(key);
    this.#fetcher.resetPartition(key);
  }

  pause(partitions: TopicPartition[]): void {
    this.#open();
    for (const target of expandPartitions(partitions, this.#assigned)) {
      this.#paused.add(partitionKey(target.topic, target.partition));
    }
  }

  resume(partitions: TopicPartition[]): void {
    this.#open();
    for (const target of expandPartitions(partitions, this.#assigned)) {
      const key = partitionKey(target.topic, target.partition);
      this.#paused.delete(key);
      this.#fetcher.resetPartition(key);
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

  watermarks(topic: string, partition: number): Promise<Watermarks> {
    return fetchWatermarks(this.#cluster, topic, partition);
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#heartbeat.stop();
    if (
      this.#state.groupId &&
      this.#state.coordinator !== undefined &&
      this.#state.generationId >= 0
    ) {
      try {
        const instanceId = this.#options.groupInstanceId;
        await this.#cluster.request(
          this.#state.coordinator,
          API_LEAVE_GROUP,
          instanceId === undefined ? 0 : GROUP_INSTANCE_API_VERSION,
          writeLeaveGroupRequest(this.#state.groupId, this.#state.memberId, instanceId),
        );
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

  #open(): void {
    if (this.#closed) {
      throw new Error("Consumer is closed");
    }
  }
}
