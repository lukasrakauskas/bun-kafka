import type { Cluster } from "../../bun/cluster.ts";
import { partitionKey } from "../../bun/shared.ts";
import { requiredValue } from "../../type-guards.ts";
import type { ConsumerAssignment, GroupAssignment, GroupMember } from "../types.ts";

type GroupMetadata = Awaited<ReturnType<Cluster["metadata"]>>;

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
