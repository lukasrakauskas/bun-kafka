import { KafkaErrorCode } from "../../errors.ts";
import type { Cluster } from "../../bun/cluster.ts";
import {
  API_CONSUMER_GROUP_HEARTBEAT,
  API_METADATA,
  CONSUMER_GROUP_HEARTBEAT_API_VERSION,
  DEFAULT_REBALANCE_TIMEOUT_MS,
  TOPIC_ID_METADATA_API_VERSION,
  kafkaError,
  partitionKey,
} from "../../bun/shared.ts";
import {
  readConsumerGroupHeartbeatResponse,
  readTopicIdsMetadataResponse,
  writeConsumerGroupHeartbeatRequest,
  writeTopicIdsMetadataRequest,
  type ConsumerGroupTopicPartitions,
} from "../../protocol/index.ts";
import type {
  Assigned,
  ConsumerAssignment,
  ConsumerGroupEvent,
  ConsumerSettings,
  ConsumerState,
} from "../types.ts";
import { withGroupOffsets } from "./assignor.ts";

const topicIdKey = (id: Uint8Array): string => Array.from(id).join(",");

export type GroupDependencies = {
  cluster: Cluster;
  options: ConsumerSettings;
  state: ConsumerState;
  assigned: Map<string, Assigned>;
  positions: Map<string, bigint>;
  committed(
    assignments: readonly ConsumerAssignment[],
  ): Promise<Array<{ offset: bigint; topic: string; partition: number }>>;
  assign(assignments: ConsumerAssignment[]): Promise<void>;
  onEvent(event: ConsumerGroupEvent): void;
};

type HeartbeatResponse = ReturnType<typeof readConsumerGroupHeartbeatResponse>;
type HeartbeatRequest = Parameters<typeof writeConsumerGroupHeartbeatRequest>[0];

export class ConsumerGroupProtocol {
  #topicIds = new Map<string, Uint8Array>();
  #topicNames = new Map<string, string>();
  #ackAssignment = false;
  #fromBeginning = false;

  constructor(
    private readonly deps: GroupDependencies,
    private readonly onRebalance: () => void,
    private readonly onJoined: (assigned: ConsumerAssignment[], startedAt: number) => void,
  ) {}

  async join(
    coordinator: number,
    topics: string[],
    fromBeginning: boolean,
  ): Promise<ConsumerAssignment[]> {
    const { state } = this.deps;
    this.#fromBeginning = fromBeginning;
    state.groupTopics = topics;
    if (state.generationId < 0) {
      state.generationId = 0;
    }
    const response = await this.#request(coordinator, true);
    if (response.assignment !== undefined) {
      const assigned = await this.#resolveAssignment(response.assignment);
      this.#ackAssignment = true;
      await this.#applyAssignment(assigned);
    }
    return this.#currentAssignment();
  }

  async heartbeat(coordinator: number): Promise<void> {
    const startedAt = performance.now();
    const response = await this.#request(coordinator);
    if (response.assignment === undefined) {
      return;
    }
    const assigned = await this.#resolveAssignment(response.assignment);
    this.#ackAssignment = true;
    if (this.#sameAssignment(assigned)) {
      return;
    }
    this.onRebalance();
    await this.#applyAssignment(assigned);
    this.onJoined(assigned, startedAt);
  }

  async leave(coordinator: number): Promise<void> {
    await this.#request(coordinator, false, -1);
  }

  async #request(
    coordinator: number,
    full = false,
    memberEpoch = this.deps.state.generationId,
  ): Promise<HeartbeatResponse> {
    const { state } = this.deps;
    const acknowledge = this.#ackAssignment;
    const response = readConsumerGroupHeartbeatResponse(
      await this.deps.cluster.request(
        coordinator,
        API_CONSUMER_GROUP_HEARTBEAT,
        CONSUMER_GROUP_HEARTBEAT_API_VERSION,
        writeConsumerGroupHeartbeatRequest(this.#requestInput(full, memberEpoch)),
        undefined,
        true,
        true,
      ),
    );
    this.deps.cluster.throttle(API_CONSUMER_GROUP_HEARTBEAT, response.throttleMs);
    this.#throwIfError(response);
    if (acknowledge) {
      this.#ackAssignment = false;
    }
    state.memberId = response.memberId ?? state.memberId;
    state.generationId = response.memberEpoch;
    state.heartbeatIntervalMs = response.heartbeatIntervalMs;
    return response;
  }

  #requestInput(full: boolean, memberEpoch: number): HeartbeatRequest {
    const { options, state } = this.deps;
    const input: HeartbeatRequest = {
      groupId: this.#requiredGroupId(),
      memberId: state.memberId,
      memberEpoch,
    };
    if (full) {
      input.instanceId = options.groupInstanceId;
      input.rackId = this.deps.cluster.rackId;
      input.rebalanceTimeoutMs = options.rebalanceTimeoutMs ?? DEFAULT_REBALANCE_TIMEOUT_MS;
      input.subscribedTopics = state.groupTopics;
      input.serverAssignor = options.groupRemoteAssignor;
    }
    if (memberEpoch >= 0 && (this.#ackAssignment || (full && this.deps.assigned.size))) {
      input.topicPartitions = this.#ownedTopicPartitions();
    }
    return input;
  }

  #throwIfError(response: HeartbeatResponse): void {
    if (!response.error) {
      return;
    }
    if (response.error === KafkaErrorCode.UNKNOWN_MEMBER_ID) {
      this.deps.state.memberId = "";
      this.deps.state.generationId = 0;
    } else if (response.error === KafkaErrorCode.FENCED_MEMBER_EPOCH) {
      this.deps.state.generationId = 0;
    }
    throw kafkaError(
      response.error,
      `Kafka group ${this.deps.state.groupId} heartbeat`,
      response.message,
    );
  }

  #ownedTopicPartitions(): ConsumerGroupTopicPartitions[] {
    return [...Map.groupBy([...this.deps.assigned.values()], ({ topic }) => topic)].flatMap(
      ([topic, assignments]) => {
        const topicId = this.#topicIds.get(topic);
        return topicId
          ? [{ topicId, partitions: assignments.map(({ partition }) => partition) }]
          : [];
      },
    );
  }

  async #resolveAssignment(
    assignment: ConsumerGroupTopicPartitions[],
  ): Promise<ConsumerAssignment[]> {
    if (assignment.some(({ topicId }) => !this.#topicNames.has(topicIdKey(topicId)))) {
      await this.#refreshTopicIds();
    }
    return assignment.flatMap(({ topicId, partitions }) => {
      const topic = this.#topicNames.get(topicIdKey(topicId));
      if (!topic) {
        throw new Error("Kafka consumer group assignment contains an unknown topic id");
      }
      return partitions.map((partition) => ({ topic, partition }));
    });
  }

  async #refreshTopicIds(): Promise<void> {
    const response = readTopicIdsMetadataResponse(
      await this.deps.cluster.anyRequest(
        API_METADATA,
        TOPIC_ID_METADATA_API_VERSION,
        writeTopicIdsMetadataRequest(this.deps.state.groupTopics),
        true,
      ),
    );
    this.deps.cluster.throttle(API_METADATA, response.throttleMs);
    for (const topic of response.topics) {
      if (topic.error) {
        throw kafkaError(topic.error, `Kafka topic ${topic.name}`);
      }
      this.#topicIds.set(topic.name, topic.id);
      this.#topicNames.set(topicIdKey(topic.id), topic.name);
    }
  }

  async #applyAssignment(assigned: ConsumerAssignment[]): Promise<void> {
    const committed = new Map(
      (await this.deps.committed(assigned)).map((item) => [
        partitionKey(item.topic, item.partition),
        item.offset,
      ]),
    );
    const retained = new Map(
      assigned.flatMap((item) => {
        const key = partitionKey(item.topic, item.partition);
        const position = this.deps.positions.get(key);
        return position === undefined ? [] : [[key, position] as const];
      }),
    );
    await this.deps.assign(withGroupOffsets(assigned, committed, retained, this.#fromBeginning));
  }

  #sameAssignment(assigned: ConsumerAssignment[]): boolean {
    const current = new Set(this.deps.assigned.keys());
    return (
      current.size === assigned.length &&
      assigned.every((item) => current.has(partitionKey(item.topic, item.partition)))
    );
  }

  #currentAssignment(): ConsumerAssignment[] {
    return [...this.deps.assigned.values()].map(({ topic, partition }) => ({ topic, partition }));
  }

  #requiredGroupId(): string {
    if (!this.deps.state.groupId) {
      throw new Error("Consumer groupId is required");
    }
    return this.deps.state.groupId;
  }
}
