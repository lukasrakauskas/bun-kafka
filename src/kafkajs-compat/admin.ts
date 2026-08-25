import { KafkaError } from "../errors.ts";
import { Cluster } from "../bun/cluster.ts";
import { Reader, Writer } from "../bun/protocol.ts";
import { BunAdmin } from "../bun/admin.ts";
import { ADMIN_EVENTS } from "./constants.ts";
import { wrapError } from "./errors.ts";
import type { ClusterGetter } from "./config.ts";
import { Emitter, Logger } from "./logger.ts";

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

  on(event: string, listener: (event: Record<string, unknown>) => void): () => void {
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
      const results = await this.#underlying().createTopics(topics.map((item) => ({
        name: item.topic,
        numPartitions: item.numPartitions ?? -1,
        replicationFactor: item.replicationFactor ?? -1,
        assignments: item.replicaAssignment,
        configs: item.configEntries ? Object.fromEntries(item.configEntries.map((entry) => [entry.name, entry.value])) : undefined,
      })), { validateOnly });
      // TOPIC_ALREADY_EXISTS counts as "not created" rather than a failure.
      const outcomes = results.map((result) => result.error === 0);
      if (waitForLeaders && outcomes.some(Boolean)) {
        const deadline = Date.now() + Math.max(timeout, 5_000);
        while (Date.now() < deadline) {
          const created = topics.filter((_, i) => outcomes[i]).map((item) => item.topic);
          const metadata = await this.#underlying().metadata(created);
          if (metadata.topics.every((topicMeta) => !topicMeta.err && topicMeta.partitions.every((p) => p.leader >= 0))) break;
          await Bun.sleep(100);
        }
      }
      return outcomes;
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteTopics(payload: { topics: Array<string | { topic: string; partitions?: number[] }>; timeout?: number }): Promise<void> {
    try {
      await this.#underlying().deleteTopics(payload.topics.map((entry) => (typeof entry === "string" ? entry : entry.topic)));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createPartitions({ validateOnly = false, topicPartitions }: { validateOnly?: boolean; topicPartitions: Array<{ topic: string; count: number; assignments?: number[][] }> }): Promise<void> {
    try {
      await this.#underlying().createPartitions(
        topicPartitions.map((item) => ({ name: item.topic, count: item.count, assignments: item.assignments })),
        { validateOnly },
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicMetadata({ topics }: { topics?: Array<{ topic: string }> } = {}): Promise<{
    brokers: Array<{ nodeId: number; host: string; port: number }>;
    topics: Array<{ topicName: string; partitions: Array<{ partitionErrorCode: number; partition: number; leader: number; replicas: number[]; isr: number[] }> }>;
  }> {
    try {
      const metadata = await this.#underlying().metadata(topics?.map((entry) => entry.topic) ?? null);
      return {
        brokers: metadata.brokers.map((broker) => ({ nodeId: broker.id, host: broker.host, port: broker.port })),
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

  async describeCluster(): Promise<{ brokers: Array<{ nodeId: number; host: string; port: number }>; controller: number | null; clusterId: string | null }> {
    try {
      const metadata = await this.#underlying().metadata(null);
      return {
        brokers: metadata.brokers.map((broker) => ({ nodeId: broker.id, host: broker.host, port: broker.port })),
        controller: metadata.brokers.length ? metadata.brokers[0]!.id : null,
        clusterId: metadata.clusterId ?? null,
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchOffsets({ groupId, topics, resolveOffsets = false }: { groupId: string; topics?: string[]; resolveOffsets?: boolean }): Promise<Array<{ topic: string; partitions: Array<{ partition: number; offset: string; metadata?: string }> }>> {
    try {
      const cluster = this.#getter().sync();
      const names = topics ?? await this.listTopics();
      const result: Array<{ topic: string; partitions: Array<{ partition: number; offset: string; metadata?: string }> }> = [];
      for (const topic of names) {
        const meta = await cluster.topic(topic);
        if (meta.err) continue;
        const partitions = meta.partitions.map((p) => p.id);
        const body = new Writer().string(groupId)
          .array([topic], (writer, name) => writer.string(name).array(partitions, (partitionWriter, partition) => partitionWriter.i32(partition)));
        const response = await cluster.anyRequest(9, 2, body);
        const parsed = response.array((topicReader) => {
          const name = topicReader.string() ?? "";
          return topicReader.array((partitionReader) => ({
            name,
            partition: partitionReader.i32(),
            offset: partitionReader.i64(),
            metadata: partitionReader.string(),
            error: partitionReader.i16(),
          }));
        }).flat();
        const mapped = parsed
          .filter((entry) => entry.error === 0)
          .map(async (entry) => ({
            partition: entry.partition,
            offset: (resolveOffsets && entry.offset < 0n ? await listOffset(cluster, topic, entry.partition, -2) : entry.offset).toString(),
            metadata: entry.metadata ?? undefined,
          }));
        result.push({ topic, partitions: await Promise.all(mapped) });
      }
      return result;
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listConsumerGroupOffsets(payload: { groupId: string; topics?: string[] }): Promise<Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>> {
    const fetched = await this.fetchOffsets(payload);
    return fetched.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, offset }) => ({ partition, offset })),
    }));
  }

  async fetchTopicOffsets(topic: string): Promise<Array<{ partition: number; offset: string; high: string; low: string }>> {
    try {
      const cluster = this.#getter().sync();
      const meta = await cluster.topic(topic);
      if (meta.err) throw new KafkaError(meta.err, `Topic ${topic}`);
      return await Promise.all(meta.partitions.map(async (partition) => {
        const low = await listOffset(cluster, topic, partition.id, -2);
        const high = await listOffset(cluster, topic, partition.id, -1);
        return { partition: partition.id, offset: high.toString(), high: high.toString(), low: low.toString() };
      }));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async fetchTopicOffsetsByTimestamp(topic: string, timestamp = Date.now()): Promise<Array<{ partition: number; offset: string }>> {
    try {
      const cluster = this.#getter().sync();
      const meta = await cluster.topic(topic);
      if (meta.err) throw new KafkaError(meta.err, `Topic ${topic}`);
      return await Promise.all(meta.partitions.map(async (partition) => ({
        partition: partition.id,
        offset: (await listOffset(cluster, topic, partition.id, timestamp)).toString(),
      })));
    } catch (error) {
      throw wrapError(error);
    }
  }

  async setOffsets({ groupId, topic, partitions }: { groupId: string; topic: string; partitions: Array<{ partition: number; offset: string | number | bigint }> }): Promise<void> {
    try {
      await commitGroupOffsets(this.#getter().sync(), groupId, [{
        topic,
        partitions: partitions.map(({ partition, offset }) => ({ partition, offset: BigInt(offset), metadata: "" })),
      }]);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async resetOffsets({ groupId, topic, earliest = true }: { groupId: string; topic: string; earliest?: boolean }): Promise<void> {
    try {
      const cluster = this.#getter().sync();
      const meta = await cluster.topic(topic);
      if (meta.err) throw new KafkaError(meta.err, `Topic ${topic}`);
      const resolved = await Promise.all(meta.partitions.map(async (partition) => ({
        partition: partition.id,
        offset: await listOffset(cluster, topic, partition.id, earliest ? -2 : -1),
        metadata: "",
      })));
      await commitGroupOffsets(cluster, groupId, [{ topic, partitions: resolved }]);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listGroups(statesFilter?: string[]): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    try {
      const groups = await this.#underlying().listGroups(statesFilter ?? []);
      return { groups: groups.map(({ groupId, protocolType, state }) => ({ groupId, protocolType, state })) };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async listConsumerGroups(statesFilter?: string[]): Promise<{ groups: Array<{ groupId: string; protocolType: string; state: string }> }> {
    return this.listGroups(statesFilter);
  }

  async describeGroups(groupIds: string[]): Promise<{ groups: Array<Record<string, unknown>> }> {
    try {
      const cluster = this.#getter().sync();
      const body = new Writer().array(groupIds, (writer, group) => writer.string(group));
      const response = await cluster.anyRequest(15, 1, body);
      const throttleTime = response.i32();
      cluster.throttle?.(15 as never, throttleTime);
      // Some brokers (Redpanda) omit the nullable error_message field that
      // Apache Kafka always writes; trial-parse both shapes and keep the one
      // that consumes the buffer exactly.
      const data = response.data;
      let groups: Array<Record<string, unknown>> | undefined;
      for (const withMessage of [true, false]) {
        try {
          const reader = new Reader(data);
          reader.i32();
          const parsed: Array<Record<string, unknown>> = reader.array((entry) => {
            const error = entry.i16();
            const message = withMessage ? entry.string() : undefined;
            const groupId = entry.string() ?? "";
            const state = entry.string() ?? "";
            const protocolType = entry.string() ?? "";
            const protocol = entry.string();
            const members = entry.array((memberReader) => ({
              memberId: memberReader.string() ?? "",
              clientId: memberReader.string() ?? "",
              clientHost: memberReader.string() ?? "",
              memberMetadata: memberReader.bytes(),
              memberAssignment: memberReader.bytes(),
            }));
            return { errorCode: error, errorMessage: message, groupId, state, protocolType, protocolData: protocol, members };
          });
          if (reader.remaining === 0) {
            groups = parsed;
            break;
          }
        } catch {
          // Try the next shape.
        }
      }
      if (!groups) throw new KafkaError(-1, "Malformed DescribeGroups response");
      return { groups };
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
      return metadata.topics.filter((topicMeta) => !topicMeta.err && topicMeta.name).map((topicMeta) => topicMeta.name);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeConfigs({ resources }: { resources: Array<{ type: number; name: string; configNames?: string[] }> }): Promise<{
    resources: Array<{ resourceName: string; resourceType: number; configEntries: Record<string, { value: string | null; isDefault: boolean; isSensitive: boolean; readOnly: boolean; configSource: number }> }>;
  }> {
    try {
      const described = await this.#underlying().describeConfigs(resources.map((resource) => ({
        resourceType: resource.type,
        resourceName: resource.name,
        configNames: resource.configNames,
      })));
      return {
        resources: described.map((resource) => {
          const configEntries: Record<string, { value: string | null; isDefault: boolean; isSensitive: boolean; readOnly: boolean; configSource: number }> = {};
          for (const config of resource.configs) {
            configEntries[config.name] = {
              value: config.value,
              isDefault: config.source === 5,
              isSensitive: config.sensitive,
              readOnly: config.readOnly,
              configSource: config.source,
            };
          }
          return { resourceName: resource.resourceName, resourceType: resource.resourceType, configEntries };
        }),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async alterConfigs({ validateOnly = false, resources }: { validateOnly?: boolean; resources: Array<{ type: number; name: string; configEntries: Record<string, string | null> }> }): Promise<void> {
    try {
      await this.#underlying().alterConfigs(resources.map((resource) => ({
        resourceType: resource.type,
        resourceName: resource.name,
        configs: resource.configEntries,
      })), { validateOnly });
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createAcls({ acl }: { acl: Array<Record<string, any>> }): Promise<boolean[]> {
    try {
      const results = await this.#underlying().createAcls(acl.map((entry) => ({
        resourceType: Number(entry.resourceType ?? entry.resourceResourceType ?? 2),
        resourceName: String(entry.resourceName ?? entry.resourceResourceName),
        principal: String(entry.principal),
        host: String(entry.host),
        operation: Number(entry.operation),
        permissionType: Number(entry.permissionType),
      })) as never);
      return results.map((result) => result.error === 0);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeAcls(filter: Record<string, any>): Promise<{ resources: Array<{ resourceType: number; resourceName: string; principal: string; host: string; operation: number; permissionType: number }> }> {
    try {
      const described = await this.#underlying().describeAcls({
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: filter.resourceName,
        principal: filter.principal,
        host: filter.host,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      } as never);
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

  async deleteAcls(filters: { filters: Array<Record<string, any>> }): Promise<{
    entries: Array<{ errorCode: number; errorMessage?: string; resources: Array<{ resourceType: number; resourceName: string; principal: string; host: string; operation: number; permissionType: number }> }>;
  }> {
    try {
      const result = await this.#underlying().deleteAcls(filters.filters.map((filter) => ({
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: filter.resourceName,
        principal: filter.principal,
        host: filter.host,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      })) as never);
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

async function listOffset(cluster: Cluster, topic: string, partition: number, timestamp: number): Promise<bigint> {
  const meta = await cluster.topic(topic);
  const leader = meta.partitions.find((p) => p.id === partition)?.leader;
  if (leader === undefined) throw new KafkaError(3, `${topic}[${partition}]`);
  const body = new Writer().i32(-1).array([topic], (writer, name) =>
    writer.string(name).array([partition], (partitionWriter, index) => {
      partitionWriter.i32(index).i64(BigInt(timestamp));
    }));
  const response = await cluster.request(leader, 2, 1, body);
  const result = response.array((topicReader) => {
    topicReader.string();
    return topicReader.array((partitionReader) => {
      partitionReader.i32();
      const error = partitionReader.i16();
      partitionReader.i64();
      const offset = partitionReader.i64();
      if (error) throw new KafkaError(error, `${topic}[${partition}]`);
      return offset;
    });
  });
  return result[0]?.[0] ?? -1n;
}

async function commitGroupOffsets(
  cluster: Cluster,
  groupId: string,
  topics: Array<{ topic: string; partitions: Array<{ partition: number; offset: bigint; metadata: string }> }>,
): Promise<void> {
  const coordinatorResponse = await cluster.anyRequest(10, 0, new Writer().string(groupId));
  const coordinatorError = coordinatorResponse.i16();
  const coordinator = coordinatorResponse.i32();
  if (coordinatorError) throw new KafkaError(coordinatorError, `FindCoordinator ${groupId}`);
  const body = new Writer().string(groupId).i32(-1).string("").i64(-1n)
    .array(topics, (writer, { topic, partitions }) =>
      writer.string(topic).array(partitions, (partitionWriter, entry) =>
        partitionWriter.i32(entry.partition).i64(entry.offset).string(entry.metadata || null)));
  const response = await cluster.request(coordinator, 8, 2, body);
  for (const topicResult of response.array((reader: Reader) => ({
    topic: reader.string() ?? "",
    partitions: reader.array((p) => ({ partition: p.i32(), error: p.i16() })),
  }))) {
    for (const partition of topicResult.partitions) {
      if (partition.error) throw new KafkaError(partition.error, `${topicResult.topic}[${partition.partition}]`);
    }
  }
}
