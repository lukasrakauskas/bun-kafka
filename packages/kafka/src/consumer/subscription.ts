import type { Cluster } from "../bun/cluster.ts";
import { API_LIST_OFFSETS, EARLIEST_OFFSET, kafkaError } from "../bun/shared.ts";
import { KafkaErrorCode } from "../errors.ts";
import { isString } from "../type-guards.ts";
import { readListOffsetsResponse, writeListOffsetsRequest } from "../protocol/index.ts";
import type { TopicPartition, Watermarks } from "../types.ts";
import type { Assigned, ConsumerAssignment } from "./types.ts";

export async function resolveTopicPatterns(
  cluster: Cluster,
  topics: Array<string | RegExp>,
): Promise<string[]> {
  const patterns = topics.filter((topic): topic is RegExp => topic instanceof RegExp);
  if (!patterns.length) {
    return [...new Set(topics.filter(isString))];
  }
  const metadata = await cluster.metadata(null);
  const resolved = new Set<string>();
  for (const entry of metadata.topics) {
    if (entry.err || !entry.name) {
      continue;
    }
    if (
      patterns.some((pattern) => {
        pattern.lastIndex = 0;
        return pattern.test(entry.name);
      }) ||
      topics.some((topic) => isString(topic) && topic === entry.name)
    ) {
      resolved.add(entry.name);
    }
  }
  return [...resolved];
}

export async function topicAssignments(
  cluster: Cluster,
  topics: string[],
  fromBeginning: boolean | undefined,
): Promise<ConsumerAssignment[]> {
  const metadata = await cluster.metadata(topics);
  return topics.flatMap((topic) => {
    const found = metadata.topics.find((item) => item.name === topic);
    if (!found || found.err) {
      throw kafkaError(found?.err ?? KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION, topic);
    }
    return found.partitions.map((partition) => ({
      topic,
      partition: partition.id,
      offset: fromBeginning ? ("earliest" as const) : ("latest" as const),
    }));
  });
}

export async function fetchWatermarks(
  cluster: Cluster,
  topic: string,
  partition: number,
): Promise<Watermarks> {
  const metadata = await cluster.topic(topic);
  const leader = metadata.partitions.find((item) => item.id === partition)?.leader;
  if (leader === undefined) {
    throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
  }
  const query = async (timestamp: number) => {
    const response = await cluster.request(
      leader,
      API_LIST_OFFSETS,
      1,
      writeListOffsetsRequest(new Map([[topic, [{ partition, timestamp: BigInt(timestamp) }]]])),
    );
    const item = readListOffsetsResponse(response)[0];
    if (item?.error) {
      throw kafkaError(item.error, `${topic}[${partition}]`);
    }
    return item?.offset ?? -1n;
  };
  const [low, high] = await Promise.all([query(EARLIEST_OFFSET), query(-1)]);
  return { low, high };
}

export function expandPartitions(
  partitions: TopicPartition[],
  assigned: Map<string, Assigned>,
): Array<{ topic: string; partition: number }> {
  return partitions.flatMap(({ topic, partition }) =>
    partition !== undefined
      ? [{ topic, partition }]
      : [...assigned.values()]
          .filter((item) => item.topic === topic)
          .map(({ partition: assignedPartition }) => ({ topic, partition: assignedPartition })),
  );
}
