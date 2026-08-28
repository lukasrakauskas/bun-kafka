import {
  readAlterConfigsResponse,
  readCreatePartitionsResponse,
  readCreateTopicsResponse,
  readDeleteTopicsResponse,
  readDescribeConfigsResponse,
  readIncrementalAlterConfigsResponse,
  writeAlterConfigsRequest,
  writeCreatePartitionsRequest,
  writeCreateTopicsRequest,
  writeDeleteTopicsRequest,
  writeDescribeConfigsRequest,
  writeIncrementalAlterConfigsRequest,
} from "../../protocol/index.ts";
import {
  API_ALTER_CONFIGS,
  API_CREATE_PARTITIONS,
  API_CREATE_TOPICS,
  API_DELETE_TOPICS,
  API_DESCRIBE_CONFIGS,
  API_INCREMENTAL_ALTER_CONFIGS,
  CREATE_TOPICS_API_VERSION,
  DEFAULT_ADMIN_POLL_MIN_MS,
  DEFAULT_ADMIN_POLL_SLEEP_MS,
  DEFAULT_ADMIN_TIMEOUT_MS,
  DELETE_TOPICS_API_VERSION,
} from "../shared.ts";
import { AdminBase } from "./base.ts";
import type {
  ConfigResource,
  CreatePartitionsInput,
  CreateTopicInput,
  TopicResult,
} from "./types.ts";

export class AdminTopics extends AdminBase {
  async createTopics(
    topics: readonly CreateTopicInput[],
    options: { timeoutMs?: number; validateOnly?: boolean; waitForLeaders?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.open();
    if (!topics.length) {
      return [];
    }
    const body = writeCreateTopicsRequest(
      topics.map((topic) => ({
        ...topic,
        assignments: topic.assignments?.map((brokers, partition) => ({ partition, brokers })) ?? [],
      })),
      options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS,
      options.validateOnly ?? false,
    );
    const response = await this.cluster.controllerRequest(
      API_CREATE_TOPICS,
      CREATE_TOPICS_API_VERSION,
      body,
    );
    const decoded = readCreateTopicsResponse(response);
    this.cluster.throttle(API_CREATE_TOPICS, decoded.throttleMs);
    const results = decoded.results;
    if (!options.waitForLeaders || !results.some((result) => result.error === 0)) {
      return results;
    }
    // Wait until every created partition reports a leader so immediate
    // produce/fetch does not race leader election.
    const created = results.filter((result) => result.error === 0).map((result) => result.name);
    const deadline =
      Date.now() +
      Math.max(options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS, DEFAULT_ADMIN_POLL_MIN_MS);
    while (Date.now() < deadline) {
      const metadata = await this.metadata(created);
      if (
        metadata.topics.every(
          (topicMeta) => !topicMeta.err && topicMeta.partitions.every((p) => p.leader >= 0),
        )
      ) {
        break;
      }
      await Bun.sleep(DEFAULT_ADMIN_POLL_SLEEP_MS);
    }
    return results;
  }

  async deleteTopics(
    topics: readonly string[],
    options: { timeoutMs?: number } = {},
  ): Promise<TopicResult[]> {
    this.open();
    if (!topics.length) {
      return [];
    }
    const response = await this.cluster.controllerRequest(
      API_DELETE_TOPICS,
      DELETE_TOPICS_API_VERSION,
      writeDeleteTopicsRequest(topics, options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS),
    );
    const decoded = readDeleteTopicsResponse(response);
    this.cluster.throttle(API_DELETE_TOPICS, decoded.throttleMs);
    return decoded.results.map(({ message: _message, ...result }) => ({
      ...result,
      message: null,
    }));
  }

  async createPartitions(
    topics: readonly CreatePartitionsInput[],
    options: { timeoutMs?: number; validateOnly?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.open();
    if (!topics.length) {
      return [];
    }
    const body = writeCreatePartitionsRequest(
      topics,
      options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS,
      options.validateOnly ?? false,
    );
    const response = await this.cluster.controllerRequest(API_CREATE_PARTITIONS, 2, body);
    const decoded = readCreatePartitionsResponse(response);
    this.cluster.throttle(API_CREATE_PARTITIONS, decoded.throttleMs);
    return decoded.results;
  }

  async describeConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      configNames?: string[] | null;
    }[],
  ): Promise<ConfigResource[]> {
    this.open();
    const body = writeDescribeConfigsRequest(resources);
    const response = await this.cluster.anyRequest(API_DESCRIBE_CONFIGS, 0, body);
    const decoded = readDescribeConfigsResponse(response);
    this.cluster.throttle(API_DESCRIBE_CONFIGS, decoded.throttleMs);
    return decoded.results;
  }

  async alterConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      configs: Record<string, string | null>;
    }[],
  ): Promise<TopicResult[]> {
    this.open();
    const body = writeAlterConfigsRequest(resources);
    const response = await this.cluster.anyRequest(API_ALTER_CONFIGS, 0, body);
    const decoded = readAlterConfigsResponse(response);
    this.cluster.throttle(API_ALTER_CONFIGS, decoded.throttleMs);
    return decoded.results;
  }

  /**
   * Incremental config updates (IncrementalAlterConfigs v1): set/delete/append/subtract
   * individual entries without replacing the resource's remaining configuration.
   * Requires Apache Kafka 2.3+ / current Redpanda; older brokers answer UNSUPPORTED_VERSION.
   */
  async incrementalAlterConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      ops: ReadonlyArray<{
        name: string;
        operation: "set" | "delete" | "append" | "subtract";
        value?: string | null;
      }>;
    }[],
    options: { validateOnly?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.open();
    if (!resources.length) {
      return [];
    }
    const body = writeIncrementalAlterConfigsRequest(resources, options.validateOnly ?? false);
    const response = await this.cluster.anyRequest(API_INCREMENTAL_ALTER_CONFIGS, 1, body, true);
    const decoded = readIncrementalAlterConfigsResponse(response);
    this.cluster.throttle(API_INCREMENTAL_ALTER_CONFIGS, decoded.throttleMs);
    return decoded.results;
  }

  /** List consumer groups (ListGroups v1 wire shape: groupId + protocolType per entry). */
}
