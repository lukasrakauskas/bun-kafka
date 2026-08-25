import { wrapError } from "./errors.ts";
import { ADMIN_EVENTS } from "./constants.ts";
import type { ClusterGetter } from "./config.ts";
import { Emitter, Logger } from "./logger.ts";
import { BunAdmin, type AclBinding, type AclFilter } from "../bun/admin.ts";
import { isString } from "../type-guards.ts";
import type { CompatOptions, LogFields } from "./types.ts";

export interface CompatCreateTopicsInput {
  validateOnly?: boolean;
  waitForLeaders?: boolean;
  timeout?: number;
  topics: Array<{
    topic: string;
    numPartitions?: number;
    replicationFactor?: number;
    replicaAssignment?: number[][];
    configEntries?: Array<{ name: string; value: string | null }>;
  }>;
}

export class CompatAdmin {
  events = ADMIN_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #admin?: BunAdmin;
  #emitter = new Emitter();

  constructor(getter: () => ClusterGetter, logger: Logger) {
    this.#getter = getter;
    this.#logger = logger;
  }

  on(event: string, listener: (event: LogFields) => void): () => void {
    return this.#emitter.on(event, listener);
  }

  logger(): Logger {
    return this.#logger;
  }

  #underlying(): BunAdmin {
    this.#admin ??= new BunAdmin(this.#getter().acquire(), this.#getter().release);
    return this.#admin!;
  }

  async connect(): Promise<void> {
    await this.#getter().ready();
    this.#underlying();
    this.#emitter.emit(ADMIN_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    await this.#admin?.close().catch(() => {});
    this.#admin = undefined;
    this.#emitter.emit(ADMIN_EVENTS.DISCONNECT);
  }

  async createTopics(input: CompatCreateTopicsInput): Promise<boolean[]> {
    try {
      const { validateOnly = false, waitForLeaders = true, timeout = 5_000, topics } = input;
      const results = await this.#underlying().createTopics(
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
      await this.#underlying().deleteTopics(
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
      await this.#underlying().createPartitions(
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
      const metadata = await this.#underlying().metadata(
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
      const metadata = await this.#underlying().metadata(null);
      return {
        brokers: metadata.brokers.map((broker) => ({
          nodeId: broker.id,
          host: broker.host,
          port: broker.port,
        })),
        controller: metadata.brokers.length ? metadata.brokers[0]!.id : null,
        clusterId: metadata.clusterId ?? null,
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

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
      const listed = await this.#underlying().groupOffsets(groupId, topics);
      const result: Array<{
        topic: string;
        partitions: Array<{ partition: number; offset: string; metadata?: string }>;
      }> = [];
      for (const { topic, partitions } of listed) {
        const mapped = await Promise.all(
          partitions.map(async ({ partition, offset, metadata }) => ({
            partition,
            offset: (resolveOffsets && offset < 0n
              ? await this.#underlying().offsetByTimestamp(topic, partition, -2)
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
      return (await this.#underlying().topicOffsets(topic)).map(({ partition, low, high }) => ({
        partition,
        offset: high.toString(),
        high: high.toString(),
        low: low.toString(),
      }));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp = Date.now(),
  ): Promise<Array<{ partition: number; offset: string }>> {
    try {
      const marks = await this.#underlying().topicOffsets(topic);
      return await Promise.all(
        marks.map(async ({ partition }) => ({
          partition,
          offset: (
            await this.#underlying().offsetByTimestamp(topic, partition, timestamp)
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
      await this.#underlying().setGroupOffsets(groupId, [
        {
          topic,
          partitions: partitions.map(({ partition, offset }) => ({
            partition,
            offset: BigInt(offset),
            metadata: "",
          })),
        },
      ]);
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
      await this.#underlying().resetGroupOffsets(groupId, topic, earliest);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listGroups(
    statesFilter?: string[],
  ): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    try {
      const groups = await this.#underlying().listGroups(statesFilter ?? []);
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
      const described = await this.#underlying().describeGroups(groupIds);
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
      await this.#underlying().deleteGroups(groupIds);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listTopics(): Promise<string[]> {
    try {
      const metadata = await this.#underlying().metadata(null);
      return metadata.topics
        .filter((topicMeta) => !topicMeta.err && topicMeta.name)
        .map((topicMeta) => topicMeta.name);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeConfigs({
    resources,
  }: {
    resources: Array<{ type: number; name: string; configNames?: string[] }>;
  }): Promise<{
    resources: Array<{
      resourceName: string;
      resourceType: number;
      configEntries: Record<
        string,
        {
          value: string | null;
          isDefault: boolean;
          isSensitive: boolean;
          readOnly: boolean;
          configSource: number;
        }
      >;
    }>;
  }> {
    try {
      const described = await this.#underlying().describeConfigs(
        resources.map((resource) => ({
          resourceType: resource.type,
          resourceName: resource.name,
          configNames: resource.configNames,
        })),
      );
      return {
        resources: described.map((resource) => {
          const configEntries: Record<
            string,
            {
              value: string | null;
              isDefault: boolean;
              isSensitive: boolean;
              readOnly: boolean;
              configSource: number;
            }
          > = {};
          for (const config of resource.configs) {
            configEntries[config.name] = {
              value: config.value,
              isDefault: config.source === 5,
              isSensitive: config.sensitive,
              readOnly: config.readOnly,
              configSource: config.source,
            };
          }
          return {
            resourceName: resource.resourceName,
            resourceType: resource.resourceType,
            configEntries,
          };
        }),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async alterConfigs({
    validateOnly = false,
    resources,
  }: {
    validateOnly?: boolean;
    resources: Array<{ type: number; name: string; configEntries: Record<string, string | null> }>;
  }): Promise<void> {
    try {
      await this.#underlying().alterConfigs(
        resources.map((resource) => ({
          resourceType: resource.type,
          resourceName: resource.name,
          configs: resource.configEntries,
        })),
        { validateOnly },
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createAcls({ acl }: { acl: Array<CompatOptions> }): Promise<boolean[]> {
    try {
      const bindings: AclBinding[] = acl.map((entry) => ({
        resourceType: Number(entry.resourceType ?? entry.resourceResourceType ?? 2),
        resourceName: String(entry.resourceName ?? entry.resourceResourceName),
        principal: String(entry.principal),
        host: String(entry.host),
        operation: Number(entry.operation),
        permissionType: Number(entry.permissionType),
      }));
      const results = await this.#underlying().createAcls(bindings);
      return results.map((result) => result.error === 0);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeAcls(filter: CompatOptions): Promise<{
    resources: Array<{
      resourceType: number;
      resourceName: string;
      principal: string;
      host: string;
      operation: number;
      permissionType: number;
    }>;
  }> {
    try {
      const aclFilter: AclFilter = {
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: isString(filter.resourceName) ? filter.resourceName : undefined,
        principal: isString(filter.principal) ? filter.principal : undefined,
        host: isString(filter.host) ? filter.host : undefined,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      };
      const described = await this.#underlying().describeAcls(aclFilter);
      return {
        resources: described.acls.map((acl) => ({
          resourceType: acl.resourceType,
          resourceName: acl.resourceName,
          principal: acl.principal,
          host: acl.host,
          operation: acl.operation,
          permissionType: acl.permissionType,
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteAcls(filters: { filters: Array<CompatOptions> }): Promise<{
    entries: Array<{
      errorCode: number;
      errorMessage?: string;
      resources: Array<{
        resourceType: number;
        resourceName: string;
        principal: string;
        host: string;
        operation: number;
        permissionType: number;
      }>;
    }>;
  }> {
    try {
      const aclFilters: AclFilter[] = filters.filters.map((filter) => ({
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: isString(filter.resourceName) ? filter.resourceName : undefined,
        principal: isString(filter.principal) ? filter.principal : undefined,
        host: isString(filter.host) ? filter.host : undefined,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      }));
      const result = await this.#underlying().deleteAcls(aclFilters);
      return {
        entries: result.map((entry) => ({
          errorCode: entry.error,
          errorMessage: entry.message ?? undefined,
          resources: entry.acls.map((acl) => ({
            resourceType: acl.resourceType,
            resourceName: acl.resourceName,
            principal: acl.principal,
            host: acl.host,
            operation: acl.operation,
            permissionType: acl.permissionType,
          })),
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }
}
