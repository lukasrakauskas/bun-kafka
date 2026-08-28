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
  ConsumerState,
  GroupAssignment,
  GroupMember,
} from "./types.ts";
import { requiredValue } from "../type-guards.ts";

type GroupMetadata = Awaited<ReturnType<Cluster["metadata"]>>;

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
};

export class GroupCoordinator {
  readonly #deps: GroupDependencies;

  constructor(deps: GroupDependencies) {
    this.#deps = deps;
  }

  async join(topics: string[], fromBeginning: boolean): Promise<number> {
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
    return joined.coordinator;
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

export function assignGroupPartitions(
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
  if (eligible.length) {
    (cooperative ? assignCooperatively : assignByRange)(assignments, eligible, topic, partitions);
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
    requiredValue(
      assignments.get(member.memberId),
      `Missing assignment for ${member.memberId}`,
    ).push({ topic, partitions: partitions.slice(start, start + base + (index < extra ? 1 : 0)) });
  });
}

// eslint-disable-next-line sonarjs/cognitive-complexity
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
      if (mine.length < requiredValue(targetSize.get(member.memberId))) {
        ownedBy.set(owned.partition, member.memberId);
        mine.push(owned.partition);
      }
    }
  }
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

export function withGroupOffsets(
  assigned: ConsumerAssignment[],
  committed: Map<string, bigint>,
  retained: Map<string, bigint>,
  fromBeginning: boolean,
): ConsumerAssignment[] {
  return assigned.map((item) => {
    const key = partitionKey(item.topic, item.partition);
    const retainedOffset = retained.get(key);
    if (retainedOffset !== undefined) {
      return { ...item, offset: retainedOffset };
    }
    const committedOffset = committed.get(key);
    if (committedOffset !== undefined && committedOffset >= 0n) {
      return { ...item, offset: committedOffset };
    }
    return { ...item, offset: fromBeginning ? "earliest" : "latest" };
  });
}
