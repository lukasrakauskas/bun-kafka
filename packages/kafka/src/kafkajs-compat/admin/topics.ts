import { isString } from "../../type-guards.ts";
import { wrapError } from "../errors.ts";
import { CompatAdminBase } from "./base.ts";
import type { CompatCreateTopicsInput } from "./types.ts";

export class CompatAdminTopics extends CompatAdminBase {
  async createTopics(input: CompatCreateTopicsInput): Promise<boolean[]> {
    try {
      const { validateOnly = false, waitForLeaders = true, timeout = 5_000, topics } = input;
      const results = await this.underlying().createTopics(
        topics.map((item) => ({
          name: item.topic,
          numPartitions: item.numPartitions ?? -1,
          replicationFactor: item.replicationFactor ?? -1,
          assignments: item.replicaAssignment,
          configs: item.configEntries
            ? Object.fromEntries(item.configEntries.map((entry) => [entry.name, entry.value]))
            : undefined,
        })),
        { validateOnly, waitForLeaders, timeoutMs: timeout },
      );
      // TOPIC_ALREADY_EXISTS counts as "not created" rather than a failure.
      return results.map((result) => result.error === 0);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteTopics(payload: {
    topics: Array<string | { topic: string; partitions?: number[] }>;
    timeout?: number;
  }): Promise<void> {
    try {
      await this.underlying().deleteTopics(
        payload.topics.map((entry) => (isString(entry) ? entry : entry.topic)),
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createPartitions({
    validateOnly = false,
    topicPartitions,
  }: {
    validateOnly?: boolean;
    topicPartitions: Array<{ topic: string; count: number; assignments?: number[][] }>;
  }): Promise<void> {
    try {
      await this.underlying().createPartitions(
        topicPartitions.map((item) => ({
          name: item.topic,
          count: item.count,
          assignments: item.assignments,
        })),
        { validateOnly },
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicMetadata({ topics }: { topics?: Array<{ topic: string }> } = {}): Promise<{
    brokers: Array<{ nodeId: number; host: string; port: number }>;
    topics: Array<{
      topicName: string;
      partitions: Array<{
        partitionErrorCode: number;
        partition: number;
        leader: number;
        replicas: number[];
        isr: number[];
      }>;
    }>;
  }> {
    try {
      const metadata = await this.underlying().metadata(
        topics?.map((entry) => entry.topic) ?? null,
      );
      return {
        brokers: metadata.brokers.map((broker) => ({
          nodeId: broker.id,
          host: broker.host,
          port: broker.port,
        })),
        topics: metadata.topics.map((topicMeta) => ({
          topicName: topicMeta.name,
          partitions: topicMeta.partitions.map((p) => ({
            partitionErrorCode: p.err,
            partition: p.id,
            leader: p.leader,
            replicas: [p.leader],
            isr: [p.leader],
          })),
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeCluster(): Promise<{
    brokers: Array<{ nodeId: number; host: string; port: number }>;
    controller: number | null;
    clusterId: string | null;
  }> {
    try {
      const metadata = await this.underlying().metadata(null);
      return {
        brokers: metadata.brokers.map((broker) => ({
          nodeId: broker.id,
          host: broker.host,
          port: broker.port,
        })),
        controller: metadata.brokers.length ? metadata.brokers[0].id : null,
        clusterId: metadata.clusterId ?? null,
      };
    } catch (error) {
      throw wrapError(error);
    }
  }
}
