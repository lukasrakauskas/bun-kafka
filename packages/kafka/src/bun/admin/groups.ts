import {
  readAdminListOffsetsResponse,
  readDeleteGroupsResponse,
  readDeleteRecordsResponse,
  readDescribeGroupsResponse,
  readGroupCoordinatorResponse,
  readListGroupsResponse,
  readOffsetCommitResponse,
  readOffsetFetchAdminResponse,
  writeAdminListOffsetsRequest,
  writeDeleteRecordsRequest,
  writeFindCoordinatorRequest,
  writeGroupIdsRequest,
  writeListGroupsRequest,
  writeOffsetCommitRequest,
  writeOffsetFetchRequest,
} from "../../protocol/index.ts";
import {
  API_DELETE_GROUPS,
  API_DELETE_RECORDS,
  API_DESCRIBE_GROUPS,
  API_FIND_COORDINATOR,
  API_LIST_GROUPS,
  API_LIST_OFFSETS,
  API_OFFSET_COMMIT,
  API_OFFSET_FETCH,
  DEFAULT_ADMIN_TIMEOUT_MS,
  EARLIEST_OFFSET,
  kafkaError,
} from "../shared.ts";
import { AdminTopics } from "./topics.ts";
import type { DeleteRecordsResult, GroupDescription, TopicResult } from "./types.ts";

export class AdminGroups extends AdminTopics {
  async listGroups(
    statesFilter: readonly string[] = [],
  ): Promise<Array<{ groupId: string; protocolType: string; state: string }>> {
    this.open();
    if (statesFilter.length) {
      // States filtering arrived in ListGroups v4; filter client-side instead.
      const all = await this.listGroups();
      return statesFilter.length ? all.filter((group) => statesFilter.includes(group.state)) : all;
    }
    const response = await this.cluster.anyRequest(API_LIST_GROUPS, 1, writeListGroupsRequest());
    const decoded = readListGroupsResponse(response);
    this.cluster.throttle(API_LIST_GROUPS, decoded.throttleMs);
    const { error } = decoded;
    if (error) {
      throw kafkaError(error, "ListGroups");
    }
    return decoded.groups;
  }

  /** Describe consumer groups: state and member details. */
  async describeGroups(groupIds: readonly string[]): Promise<GroupDescription[]> {
    this.open();
    if (!groupIds.length) {
      return [];
    }
    const body = writeGroupIdsRequest(groupIds);
    const response = await this.cluster.anyRequest(API_DESCRIBE_GROUPS, 1, body);
    const decoded = readDescribeGroupsResponse(response);
    this.cluster.throttle(API_DESCRIBE_GROUPS, decoded.throttleMs);
    return decoded.groups;
  }

  /** Delete consumer groups that no longer have active members. */
  async deleteGroups(groupIds: readonly string[]): Promise<TopicResult[]> {
    this.open();
    if (!groupIds.length) {
      return [];
    }
    const body = writeGroupIdsRequest(groupIds);
    const response = await this.cluster.anyRequest(API_DELETE_GROUPS, 1, body);
    const decoded = readDeleteGroupsResponse(response);
    this.cluster.throttle(API_DELETE_GROUPS, decoded.throttleMs);
    return decoded.results;
  }

  /** Truncate topic partitions below the given offsets; returns the resulting low watermark per partition. */
  async deleteRecords(
    topics: ReadonlyArray<{
      name: string;
      partitions: ReadonlyArray<{ index: number; offset: bigint }>;
    }>,
    options: { timeoutMs?: number } = {},
  ): Promise<DeleteRecordsResult[]> {
    this.open();
    if (!topics.length) {
      return [];
    }
    const body = writeDeleteRecordsRequest(topics, options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS);
    const response = await this.cluster.anyRequest(API_DELETE_RECORDS, 1, body);
    const decoded = readDeleteRecordsResponse(response);
    this.cluster.throttle(API_DELETE_RECORDS, decoded.throttleMs);
    return decoded.results;
  }

  /** Describe client quota entities matching the supplied filters (DescribeClientQuotas v0). */

  async groupOffsets(
    groupId: string,
    topics?: readonly string[],
  ): Promise<
    Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: bigint; metadata: string | null }>;
    }>
  > {
    this.open();
    if (!groupId) {
      throw new Error("groupId is required");
    }
    const names =
      topics ??
      (await this.metadata(null)).topics
        .filter((topic) => !topic.err && topic.name && !topic.name.startsWith("__"))
        .map((topic) => topic.name);
    const coordinator = await this.findGroupCoordinator(groupId);
    const result: Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: bigint; metadata: string | null }>;
    }> = [];
    for (const topic of names) {
      const meta = await this.cluster.topic(topic);
      if (meta.err || !meta.partitions.length) {
        continue;
      }
      const partitions = meta.partitions.map((p) => p.id);
      const body = writeOffsetFetchRequest(groupId, topic, partitions);
      const response = await this.cluster.request(coordinator, API_OFFSET_FETCH, 2, body);
      const offsetResponse = readOffsetFetchAdminResponse(response);
      const parsed = offsetResponse.topics;
      result.push({
        topic,
        partitions: parsed
          .filter((entry) => entry.error === 0)
          .map(({ partition, offset, metadata }) => ({ partition, offset, metadata })),
      });
    }
    return result;
  }

  /** Commit offsets for a group with no active members (simple-consumer path: generation -1). */
  async setGroupOffsets(
    groupId: string,
    topics: ReadonlyArray<{
      topic: string;
      partitions: ReadonlyArray<{ partition: number; offset: bigint; metadata?: string }>;
    }>,
  ): Promise<void> {
    this.open();
    if (!topics.length) {
      return;
    }
    const coordinator = await this.findGroupCoordinator(groupId);
    const body = writeOffsetCommitRequest(groupId, topics);
    const response = await this.cluster.request(coordinator, API_OFFSET_COMMIT, 2, body);
    for (const topicResult of readOffsetCommitResponse(response)) {
      for (const partition of topicResult.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `${topicResult.topic}[${partition.partition}]`);
        }
      }
    }
  }

  /** Move a group's offsets to the earliest or latest watermarks of a topic. */
  async resetGroupOffsets(groupId: string, topic: string, earliest = true): Promise<void> {
    this.open();
    const resolved = await Promise.all(
      (await this.topicOffsets(topic)).map(async ({ partition, low, high }) => ({
        partition,
        offset: earliest ? low : high,
      })),
    );
    await this.setGroupOffsets(groupId, [{ topic, partitions: resolved }]);
  }

  /** Low and high watermarks for every partition of a topic. */
  async topicOffsets(
    topic: string,
  ): Promise<Array<{ partition: number; low: bigint; high: bigint }>> {
    this.open();
    const meta = await this.cluster.topic(topic);
    if (meta.err) {
      throw kafkaError(meta.err, topic);
    }
    return Promise.all(
      meta.partitions.map(async (partition) => {
        const [low, high] = await Promise.all([
          this.offsetByTimestamp(topic, partition.id, EARLIEST_OFFSET),
          this.offsetByTimestamp(topic, partition.id, -1),
        ]);
        return { partition: partition.id, low, high };
      }),
    );
  }

  /** First offset at or after a timestamp; -2/-1 resolve to earliest/latest. Returns -1 when none match. */
  async offsetByTimestamp(topic: string, partition: number, timestamp: number): Promise<bigint> {
    this.open();
    const meta = await this.cluster.topic(topic);
    const leader = meta.partitions.find((p) => p.id === partition)?.leader;
    if (leader === undefined) {
      throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
    }
    const body = writeAdminListOffsetsRequest(topic, partition, BigInt(timestamp));
    const response = await this.cluster.request(leader, API_LIST_OFFSETS, 1, body);
    const result = readAdminListOffsetsResponse(response);
    const item = result[0];
    if (item?.error) {
      throw kafkaError(item.error, `${topic}[${partition}]`);
    }
    return item?.offset ?? -1n;
  }

  protected async findGroupCoordinator(groupId: string): Promise<number> {
    const response = await this.cluster.anyRequest(
      API_FIND_COORDINATOR,
      0,
      writeFindCoordinatorRequest(groupId),
    );
    const coordinatorResponse = readGroupCoordinatorResponse(response);
    const { error, coordinatorId: coordinator } = coordinatorResponse;
    if (error) {
      throw kafkaError(error, `Kafka group ${groupId}`);
    }
    return coordinator;
  }
}
