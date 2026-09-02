import { EARLIEST_OFFSET } from "../../bun/shared.ts";
import { wrapError } from "../errors.ts";
import type { CompatOptions } from "../types.ts";
import { CompatAdminTopics } from "./topics.ts";

export class CompatAdminGroups extends CompatAdminTopics {
  async fetchOffsets({
    groupId,
    topics,
    resolveOffsets = false,
  }: {
    groupId: string;
    topics?: string[];
    resolveOffsets?: boolean;
  }): Promise<
    Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: string; metadata?: string }>;
    }>
  > {
    try {
      const listed = await this.observe(() => this.underlying().groupOffsets(groupId, topics));
      const result: Array<{
        topic: string;
        partitions: Array<{ partition: number; offset: string; metadata?: string }>;
      }> = [];
      for (const { topic, partitions } of listed) {
        const mapped = await Promise.all(
          partitions.map(async ({ partition, offset, metadata }) => ({
            partition,
            offset: (resolveOffsets && offset < 0n
              ? await this.observe(() =>
                  this.underlying().offsetByTimestamp(topic, partition, EARLIEST_OFFSET),
                )
              : offset
            ).toString(),
            metadata: metadata ?? undefined,
          })),
        );
        result.push({ topic, partitions: mapped });
      }
      return result;
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listConsumerGroupOffsets(payload: {
    groupId: string;
    topics?: string[];
  }): Promise<Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>> {
    const fetched = await this.fetchOffsets(payload);
    return fetched.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, offset }) => ({ partition, offset })),
    }));
  }

  async fetchTopicOffsets(
    topic: string,
  ): Promise<Array<{ partition: number; offset: string; high: string; low: string }>> {
    try {
      return (await this.observe(() => this.underlying().topicOffsets(topic))).map(
        ({ partition, low, high }) => ({
          partition,
          offset: high.toString(),
          high: high.toString(),
          low: low.toString(),
        }),
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp = Date.now(),
  ): Promise<Array<{ partition: number; offset: string }>> {
    try {
      const marks = await this.observe(() => this.underlying().topicOffsets(topic));
      return await Promise.all(
        marks.map(async ({ partition }) => ({
          partition,
          offset: (
            await this.observe(() =>
              this.underlying().offsetByTimestamp(topic, partition, timestamp),
            )
          ).toString(),
        })),
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async setOffsets({
    groupId,
    topic,
    partitions,
  }: {
    groupId: string;
    topic: string;
    partitions: Array<{ partition: number; offset: string | number | bigint }>;
  }): Promise<void> {
    try {
      await this.observe(() =>
        this.underlying().setGroupOffsets(groupId, [
          {
            topic,
            partitions: partitions.map(({ partition, offset }) => ({
              partition,
              offset: BigInt(offset),
              metadata: "",
            })),
          },
        ]),
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async resetOffsets({
    groupId,
    topic,
    earliest = true,
  }: {
    groupId: string;
    topic: string;
    earliest?: boolean;
  }): Promise<void> {
    try {
      await this.observe(() => this.underlying().resetGroupOffsets(groupId, topic, earliest));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listGroups(
    statesFilter?: string[],
  ): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    try {
      const groups = await this.observe(() => this.underlying().listGroups(statesFilter ?? []));
      return {
        groups: groups.map(({ groupId, protocolType, state }) => ({
          groupId,
          protocolType,
          state,
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listConsumerGroups(
    statesFilter?: string[],
  ): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    return this.listGroups(statesFilter);
  }

  async describeGroups(groupIds: string[]): Promise<{ groups: Array<CompatOptions> }> {
    try {
      const described = await this.observe(() => this.underlying().describeGroups(groupIds));
      return {
        groups: described.map((group) => ({
          errorCode: group.error,
          errorMessage: group.message,
          groupId: group.groupId,
          state: group.state,
          protocolType: group.protocolType,
          protocolData: group.protocol,
          members: group.members.map((member) => ({
            memberId: member.memberId,
            clientId: member.clientId,
            clientHost: member.clientHost,
            memberMetadata: member.memberMetadata,
            memberAssignment: member.memberAssignment,
          })),
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteGroups(groupIds: string[]): Promise<void> {
    try {
      await this.observe(() => this.underlying().deleteGroups(groupIds));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listTopics(): Promise<string[]> {
    try {
      const metadata = await this.observe(() => this.underlying().metadata(null));
      return metadata.topics
        .filter((topicMeta) => !topicMeta.err && topicMeta.name)
        .map((topicMeta) => topicMeta.name);
    } catch (error) {
      throw wrapError(error);
    }
  }
}
