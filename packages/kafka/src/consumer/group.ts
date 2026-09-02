import type { Cluster } from "../bun/cluster.ts";
import {
  DEFAULT_REBALANCE_TIMEOUT_MS,
  DEFAULT_SESSION_TIMEOUT_MS,
  GROUP_INSTANCE_API_VERSION,
  JOIN_GROUP_BASE_VERSION,
  API_FIND_COORDINATOR,
  API_JOIN_GROUP,
  API_SYNC_GROUP,
  kafkaError,
  partitionKey,
} from "../bun/shared.ts";
import {
  readConsumerJoinResponse,
  readConsumerSyncResponse,
  readGroupCoordinatorResponse,
  writeConsumerJoinRequest,
  writeFindCoordinatorRequest,
  writeConsumerSubscription,
  writeConsumerSyncRequest,
} from "../protocol/index.ts";
import type {
  Assigned,
  ConsumerAssignment,
  ConsumerSettings,
  ConsumerGroupEvent,
  ConsumerState,
  GroupAssignment,
  GroupMember,
} from "./types.ts";

type GroupDependencies = {
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

export class GroupCoordinator {
  readonly #deps: GroupDependencies;

  constructor(deps: GroupDependencies) {
    this.#deps = deps;
  }

  async join(topics: string[], fromBeginning: boolean): Promise<number> {
    const startedAt = performance.now();
    this.#event({
      type: "rebalancing",
      groupId: this.#deps.state.groupId ?? "",
      memberId: this.#deps.state.memberId,
    });
    const joined = await this.#joinRequest(topics);
    const assignments = await this.#buildAssignments(
      joined.leader,
      joined.members,
      joined.cooperative,
    );
    const assigned = await this.#syncGroup(joined.coordinator, assignments);
    const committed = new Map(
      (await this.#deps.committed(assigned)).map((item) => [
        partitionKey(item.topic, item.partition),
        item.offset,
      ]),
    );
    const retained = joined.cooperative
      ? this.#retainedPositions(assigned)
      : new Map<string, bigint>();
    await this.#deps.assign(withGroupOffsets(assigned, committed, retained, fromBeginning));
    this.#event({
      type: "group_join",
      groupId: this.#deps.state.groupId ?? "",
      memberId: this.#deps.state.memberId,
      generationId: this.#deps.state.generationId,
      memberAssignment: Object.fromEntries(
        [...Map.groupBy(assigned, ({ topic }) => topic)].map(([topic, topicAssignments]) => [
          topic,
          topicAssignments.map(({ partition }) => partition),
        ]),
      ),
      duration: performance.now() - startedAt,
    });
    return joined.coordinator;
  }

  #event(event: ConsumerGroupEvent): void {
    try {
      this.#deps.onEvent(event);
    } catch {
      // Lifecycle listeners must not change consumer behavior.
    }
  }

  async #findCoordinator(): Promise<number> {
    if (this.#deps.state.coordinator !== undefined) {
      return this.#deps.state.coordinator;
    }
    const response = await this.#deps.cluster.anyRequest(
      API_FIND_COORDINATOR,
      0,
      writeFindCoordinatorRequest(this.#requiredGroupId()),
    );
    const { error, coordinatorId } = readGroupCoordinatorResponse(response);
    if (error) {
      throw kafkaError(error, `Kafka group ${this.#deps.state.groupId}`);
    }
    this.#deps.state.coordinator = coordinatorId;
    return coordinatorId;
  }

  async #joinRequest(topics: string[]): Promise<{
    coordinator: number;
    leader: string;
    members: GroupMember[];
    cooperative: boolean;
  }> {
    const coordinator = await this.#findCoordinator();
    const { options, state } = this.#deps;
    const cooperative = options.partitionAssigner === "cooperative-sticky";
    const instanceId = options.groupInstanceId;
    const version = instanceId === undefined ? JOIN_GROUP_BASE_VERSION : GROUP_INSTANCE_API_VERSION;
    const response = await this.#deps.cluster.request(
      coordinator,
      API_JOIN_GROUP,
      version,
      writeConsumerJoinRequest(
        this.#requiredGroupId(),
        options.sessionTimeoutMs ?? DEFAULT_SESSION_TIMEOUT_MS,
        options.rebalanceTimeoutMs ?? DEFAULT_REBALANCE_TIMEOUT_MS,
        state.memberId,
        instanceId,
        cooperative ? "cooperative-sticky" : "range",
        writeConsumerSubscription(
          topics,
          cooperative ? [...this.#deps.assigned.values()] : undefined,
        ),
      ),
    );
    const joined = readConsumerJoinResponse(response);
    this.#deps.cluster.throttle(API_JOIN_GROUP, joined.throttleMs);
    state.generationId = joined.generationId;
    state.memberId = joined.memberId;
    if (joined.error) {
      throw kafkaError(joined.error, `Kafka group ${state.groupId}`);
    }
    return { coordinator, leader: joined.leader, members: joined.members, cooperative };
  }

  async #buildAssignments(
    leader: string,
    members: GroupMember[],
    cooperative: boolean,
  ): Promise<Map<string, GroupAssignment[]>> {
    if (this.#deps.state.memberId !== leader) {
      return new Map();
    }
    const metadata = await this.#deps.cluster.metadata([
      ...new Set(members.flatMap((member) => member.topics)),
    ]);
    return assignGroupPartitions(members, metadata, cooperative);
  }

  async #syncGroup(
    coordinator: number,
    assignments: Map<string, GroupAssignment[]>,
  ): Promise<ConsumerAssignment[]> {
    const instanceId = this.#deps.options.groupInstanceId;
    const version = instanceId === undefined ? 0 : GROUP_INSTANCE_API_VERSION;
    const response = await this.#deps.cluster.request(
      coordinator,
      API_SYNC_GROUP,
      version,
      writeConsumerSyncRequest(
        this.#requiredGroupId(),
        this.#deps.state.generationId,
        this.#deps.state.memberId,
        instanceId,
        assignments,
      ),
    );
    const synced = readConsumerSyncResponse(response, version === GROUP_INSTANCE_API_VERSION);
    if (synced.error) {
      throw kafkaError(synced.error, `Kafka group ${this.#deps.state.groupId} sync`);
    }
    return synced.assignment;
  }

  #retainedPositions(assigned: ConsumerAssignment[]): Map<string, bigint> {
    return new Map(
      assigned.flatMap((item) => {
        const key = partitionKey(item.topic, item.partition);
        const position = this.#deps.positions.get(key);
        return position === undefined ? [] : [[key, position] as const];
      }),
    );
  }

  #requiredGroupId(): string {
    if (!this.#deps.state.groupId) {
      throw new Error("Consumer groupId is required");
    }
    return this.#deps.state.groupId;
  }
}

export { assignGroupPartitions, withGroupOffsets } from "./group/assignor.ts";
import { assignGroupPartitions, withGroupOffsets } from "./group/assignor.ts";
