import type { CommittedOffset } from "../bun/shared.ts";
import type { Cluster } from "../bun/cluster.ts";
import {
  API_FIND_COORDINATOR,
  API_LIST_OFFSETS,
  API_OFFSET_COMMIT,
  API_OFFSET_FETCH,
  CONSUMER_GROUP_OFFSET_API_VERSION,
  EARLIEST_OFFSET,
  kafkaError,
  partitionKey,
} from "../bun/shared.ts";
import {
  readConsumerGroupOffsetCommitResponse,
  readConsumerGroupOffsetFetchResponse,
  readConsumerOffsetFetchResponse,
  readConsumerOffsetCommitResponse,
  readListOffsetsResponse,
  readGroupCoordinatorResponse,
  writeFindCoordinatorRequest,
  writeConsumerGroupOffsetCommitRequest,
  writeConsumerGroupOffsetFetchRequest,
  writeConsumerGroupRequest,
  writeConsumerOffsetFetchRequest,
  writeListOffsetsRequest,
} from "../protocol/index.ts";
import type { ConsumerAssignment, ConsumerSettings, ConsumerState, Assigned } from "./types.ts";
import { isBigInt } from "../type-guards.ts";

export class OffsetStore {
  constructor(
    private readonly cluster: Cluster,
    private readonly options: ConsumerSettings,
    private readonly positions: Map<string, bigint>,
    private readonly state: ConsumerState,
  ) {}

  async commit(assignments: readonly ConsumerAssignment[]): Promise<void> {
    const groupId = this.requireGroupId("offset commits");
    const coordinator = await this.findCoordinator();
    const topics = new Map(
      [...Map.groupBy(assignments, (assignment) => assignment.topic)].map(([topic, values]) => [
        topic,
        values.map((value) => ({
          partition: value.partition,
          offset: isBigInt(value.offset)
            ? value.offset
            : (this.positions.get(partitionKey(topic, value.partition)) ?? 0n),
        })),
      ]),
    );
    const consumerProtocol = this.options.groupProtocol === "consumer";
    const body = consumerProtocol
      ? writeConsumerGroupOffsetCommitRequest(
          groupId,
          this.state.generationId,
          this.state.memberId,
          this.options.groupInstanceId,
          topics,
        )
      : writeConsumerGroupRequest(groupId, this.state.generationId, this.state.memberId, topics);
    const response = await this.cluster.request(
      coordinator,
      API_OFFSET_COMMIT,
      consumerProtocol ? CONSUMER_GROUP_OFFSET_API_VERSION : 2,
      body,
      undefined,
      true,
      consumerProtocol,
    );
    const results = consumerProtocol
      ? readConsumerGroupOffsetCommitResponse(response)
      : readConsumerOffsetCommitResponse(response);
    for (const result of results) {
      for (const partition of result.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `${result.topic}[${partition.partition}]`);
        }
      }
    }
  }

  async committed(assignments: readonly ConsumerAssignment[]): Promise<CommittedOffset[]> {
    const groupId = this.requireGroupId("offset fetch");
    const coordinator = await this.findCoordinator();
    const topics = new Map(
      [...Map.groupBy(assignments, (assignment) => assignment.topic)].map(([topic, values]) => [
        topic,
        values.map((value) => ({ partition: value.partition })),
      ]),
    );
    const consumerProtocol = this.options.groupProtocol === "consumer";
    const response = await this.cluster.request(
      coordinator,
      API_OFFSET_FETCH,
      consumerProtocol ? CONSUMER_GROUP_OFFSET_API_VERSION : 2,
      consumerProtocol
        ? writeConsumerGroupOffsetFetchRequest(
            groupId,
            this.state.generationId < 0 ? null : this.state.memberId,
            this.state.generationId,
            topics,
          )
        : writeConsumerOffsetFetchRequest(groupId, topics),
      undefined,
      true,
      consumerProtocol,
    );
    const decoded = consumerProtocol
      ? readConsumerGroupOffsetFetchResponse(response)
      : readConsumerOffsetFetchResponse(response);
    if (decoded.error) {
      throw kafkaError(decoded.error, `Kafka group ${groupId}`);
    }
    return decoded.topics.flatMap((topic) =>
      topic.partitions.map((partition) => {
        if (partition.error) {
          throw kafkaError(partition.error, `${topic.topic}[${partition.partition}]`);
        }
        return { topic: topic.topic, partition: partition.partition, offset: partition.offset };
      }),
    );
  }

  async resolve(assignments: Array<Assigned & { which: "earliest" | "latest" }>): Promise<void> {
    const leaders = Map.groupBy(assignments, (assignment) => assignment.leader);
    await Promise.all(
      [...leaders].map(async ([leader, values]) => {
        const topics = Map.groupBy(values, (assignment) => assignment.topic);
        const response = await this.cluster.request(
          leader,
          API_LIST_OFFSETS,
          1,
          writeListOffsetsRequest(
            new Map(
              [...topics].map(([topic, partitions]) => [
                topic,
                partitions.map((value) => ({
                  partition: value.partition,
                  timestamp: BigInt(value.which === "earliest" ? EARLIEST_OFFSET : -1),
                })),
              ]),
            ),
          ),
        );
        for (const result of readListOffsetsResponse(response)) {
          if (result.error) {
            throw kafkaError(result.error, `${result.topic}[${result.partition}]`);
          }
          this.positions.set(partitionKey(result.topic, result.partition), result.offset);
        }
      }),
    );
  }

  private requireGroupId(action: string): string {
    if (!this.state.groupId) {
      throw new Error(`Consumer groupId is required for ${action}`);
    }
    return this.state.groupId;
  }

  private async findCoordinator(): Promise<number> {
    if (this.state.coordinator !== undefined) {
      return this.state.coordinator;
    }
    const response = await this.cluster.anyRequest(
      API_FIND_COORDINATOR,
      0,
      writeFindCoordinatorRequest(this.requireGroupId("coordinator")),
    );
    const { error, coordinatorId } = readGroupCoordinatorResponse(response);
    if (error) {
      throw kafkaError(error, `Kafka group ${this.state.groupId}`);
    }
    this.state.coordinator = coordinatorId;
    return coordinatorId;
  }
}
