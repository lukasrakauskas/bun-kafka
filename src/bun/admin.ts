import { KafkaError } from "../errors.ts";
import type { ClusterMetadata } from "../types.ts";
import { Cluster } from "./cluster.ts";
import { Reader, Writer } from "./protocol.ts";
import {
  API_ALTER_CLIENT_QUOTAS,
  API_ALTER_CONFIGS,
  API_CREATE_ACLS,
  API_CREATE_DELEGATION_TOKEN,
  API_CREATE_PARTITIONS,
  API_CREATE_TOPICS,
  API_DELETE_ACLS,
  API_DELETE_GROUPS,
  API_DELETE_RECORDS,
  API_DELETE_TOPICS,
  API_DESCRIBE_ACLS,
  API_DESCRIBE_CLIENT_QUOTAS,
  API_DESCRIBE_CONFIGS,
  API_DESCRIBE_DELEGATION_TOKEN,
  API_DESCRIBE_GROUPS,
  API_EXPIRE_DELEGATION_TOKEN,
  API_FIND_COORDINATOR,
  API_LIST_GROUPS,
  API_LIST_OFFSETS,
  API_OFFSET_COMMIT,
  API_OFFSET_FETCH,
  API_RENEW_DELEGATION_TOKEN,
  kafkaError,
  type KafkaOptions,
} from "./shared.ts";

export interface CreateTopicInput {
  name: string;
  numPartitions: number;
  replicationFactor?: number;
  assignments?: number[][];
  configs?: Record<string, string | null>;
}

export interface TopicResult {
  name: string;
  error: number;
  message: string | null;
}

export interface CreatePartitionsInput {
  name: string;
  count: number;
  assignments?: number[][];
}

export interface ConfigResource {
  resourceType: number;
  resourceName: string;
  error: number;
  message: string | null;
  configs: Array<{
    name: string;
    value: string | null;
    source: number;
    sensitive: boolean;
    readOnly: boolean;
  }>;
}

export class BunAdmin {
  #cluster: Cluster;
  #closed = false;
  #ownsCluster: boolean;
  #onClose: () => void;

  constructor(options: KafkaOptions | Cluster, onClose = () => {}) {
    this.#ownsCluster = !(options instanceof Cluster);
    this.#cluster = options instanceof Cluster ? options : new Cluster(options);
    this.#onClose = onClose;
  }

  metadata(topics: string[] | null = null): Promise<ClusterMetadata> {
    if (this.#closed) throw new Error("Admin is closed");
    return this.#cluster.metadata(topics);
  }

  async createTopics(
    topics: readonly CreateTopicInput[],
    options: { timeoutMs?: number; validateOnly?: boolean; waitForLeaders?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) return [];
    const body = new Writer()
      .array(topics, (writer, topic) => {
        writer
          .string(topic.name)
          .i32(topic.numPartitions)
          .i16(topic.replicationFactor ?? -1)
          .array(
            topic.assignments
              ? topic.assignments.map((brokers, partition) => ({ partition, brokers }))
              : [],
            (assignmentWriter, assignment) =>
              assignmentWriter
                .i32(assignment.partition)
                .array(assignment.brokers, (writer, broker) => writer.i32(broker)),
          )
          .array(topic.configs ? Object.entries(topic.configs) : [], (writer, [name, value]) =>
            writer.string(name).string(value),
          );
      })
      .i32(options.timeoutMs ?? 30_000)
      .bool(options.validateOnly ?? false);
    const response = await this.#cluster.controllerRequest(API_CREATE_TOPICS, 4, body);
    this.#cluster.throttle(API_CREATE_TOPICS, response.i32());
    const results = response.array((reader) => ({
      name: reader.string() ?? "",
      error: reader.i16(),
      message: reader.string(),
    }));
    if (!options.waitForLeaders || !results.some((result) => result.error === 0)) return results;
    // Wait until every created partition reports a leader so immediate
    // produce/fetch does not race leader election.
    const created = results.filter((result) => result.error === 0).map((result) => result.name);
    const deadline = Date.now() + Math.max(options.timeoutMs ?? 30_000, 5_000);
    while (Date.now() < deadline) {
      const metadata = await this.metadata(created);
      if (
        metadata.topics.every(
          (topicMeta) => !topicMeta.err && topicMeta.partitions.every((p) => p.leader >= 0),
        )
      )
        break;
      await Bun.sleep(100);
    }
    return results;
  }

  async deleteTopics(
    topics: readonly string[],
    options: { timeoutMs?: number } = {},
  ): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) return [];
    const response = await this.#cluster.controllerRequest(
      API_DELETE_TOPICS,
      3,
      new Writer()
        .array(topics, (writer, topic) => writer.string(topic))
        .i32(options.timeoutMs ?? 30_000),
    );
    this.#cluster.throttle(API_DELETE_TOPICS, response.i32());
    return response.array((reader) => ({
      name: reader.string() ?? "",
      error: reader.i16(),
      message: null,
    }));
  }

  async createPartitions(
    topics: readonly CreatePartitionsInput[],
    options: { timeoutMs?: number; validateOnly?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) return [];
    const body = new Writer()
      .array(topics, (writer, topic) => {
        writer
          .string(topic.name)
          .i32(topic.count)
          .array(topic.assignments ?? [], (assignmentWriter, assignment) =>
            assignmentWriter.array(assignment, (writer, broker) => writer.i32(broker)),
          );
      })
      .i32(options.timeoutMs ?? 30_000)
      .bool(options.validateOnly ?? false);
    const response = await this.#cluster.controllerRequest(API_CREATE_PARTITIONS, 2, body);
    this.#cluster.throttle(API_CREATE_PARTITIONS, response.i32());
    return response.array((reader) => ({
      name: reader.string() ?? "",
      error: reader.i16(),
      message: reader.string(),
    }));
  }

  async describeConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      configNames?: string[] | null;
    }[],
  ): Promise<ConfigResource[]> {
    this.#open();
    const body = new Writer().array(resources, (writer, resource) =>
      writer
        .i8(resource.resourceType)
        .string(resource.resourceName)
        .array(resource.configNames ?? null, (writer, name) => writer.string(name)),
    );
    const response = await this.#cluster.anyRequest(API_DESCRIBE_CONFIGS, 0, body);
    this.#cluster.throttle(API_DESCRIBE_CONFIGS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      const resourceType = reader.i8();
      const resourceName = reader.string() ?? "";
      const configs = reader.array((configReader) => {
        const name = configReader.string() ?? "";
        const value = configReader.string();
        const readOnly = configReader.bool();
        const isDefault = configReader.bool();
        const sensitive = configReader.bool();
        return { name, value, source: isDefault ? 5 : 0, sensitive, readOnly };
      });
      return { resourceType, resourceName, error, message, configs };
    });
  }

  async alterConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      configs: Record<string, string | null>;
    }[],
  ): Promise<TopicResult[]> {
    this.#open();
    const body = new Writer().array(resources, (writer, resource) =>
      writer
        .i8(resource.resourceType)
        .string(resource.resourceName)
        .array(Object.entries(resource.configs), (writer, [name, value]) =>
          writer.string(name).string(value),
        ),
    );
    const response = await this.#cluster.anyRequest(API_ALTER_CONFIGS, 0, body);
    this.#cluster.throttle(API_ALTER_CONFIGS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      reader.i8();
      const name = reader.string() ?? "";
      return { name, error, message };
    });
  }

  /** List consumer groups (ListGroups v1 wire shape: groupId + protocolType per entry). */
  async listGroups(
    statesFilter: readonly string[] = [],
  ): Promise<Array<{ groupId: string; protocolType: string; state: string }>> {
    this.#open();
    if (statesFilter.length) {
      // States filtering arrived in ListGroups v4; filter client-side instead.
      const all = await this.listGroups();
      return statesFilter.length ? all.filter((group) => statesFilter.includes(group.state)) : all;
    }
    const response = await this.#cluster.anyRequest(
      API_LIST_GROUPS,
      1,
      new Writer().array([], () => {}),
    );
    this.#cluster.throttle(API_LIST_GROUPS, response.i32());
    const error = response.i16();
    if (error) throw kafkaError(error, "ListGroups");
    return response.array((reader) => ({
      groupId: reader.string() ?? "",
      protocolType: reader.string() ?? "",
      state: "",
    }));
  }

  /** Describe consumer groups: state and member details. */
  async describeGroups(groupIds: readonly string[]): Promise<GroupDescription[]> {
    this.#open();
    if (!groupIds.length) return [];
    const body = new Writer().array(groupIds, (writer, group) => writer.string(group));
    const response = await this.#cluster.anyRequest(API_DESCRIBE_GROUPS, 1, body);
    this.#cluster.throttle(API_DESCRIBE_GROUPS, response.i32());
    // Apache Kafka always writes the nullable error_message; some brokers
    // (Redpanda) omit it entirely. Trial-parse both shapes and keep the one
    // that consumes the buffer exactly.
    const data = response.data;
    for (const withMessage of [true, false]) {
      try {
        const reader = new Reader(data);
        reader.i32();
        const parsed: GroupDescription[] = reader.array((entryReader) => {
          const error = entryReader.i16();
          const message = withMessage ? entryReader.string() : null;
          const groupId = entryReader.string() ?? "";
          const state = entryReader.string() ?? "";
          const protocolType = entryReader.string() ?? "";
          const protocol = entryReader.string();
          const members = entryReader.array((memberReader) => ({
            memberId: memberReader.string() ?? "",
            clientId: memberReader.string() ?? "",
            clientHost: memberReader.string() ?? "",
            memberMetadata: memberReader.bytes(),
            memberAssignment: memberReader.bytes(),
          }));
          return { error, message, groupId, state, protocolType, protocol, members };
        });
        if (reader.remaining === 0) return parsed;
      } catch {
        // Try the next shape.
      }
    }
    throw new KafkaError(-1, "Malformed DescribeGroups response");
  }

  /** Delete consumer groups that no longer have active members. */
  async deleteGroups(groupIds: readonly string[]): Promise<TopicResult[]> {
    this.#open();
    if (!groupIds.length) return [];
    const body = new Writer().array(groupIds, (writer, group) => writer.string(group));
    const response = await this.#cluster.anyRequest(API_DELETE_GROUPS, 1, body);
    this.#cluster.throttle(API_DELETE_GROUPS, response.i32());
    return response.array((reader) => ({
      name: reader.string() ?? "",
      error: reader.i16(),
      message: null,
    }));
  }

  /** Truncate topic partitions below the given offsets; returns the resulting low watermark per partition. */
  async deleteRecords(
    topics: ReadonlyArray<{
      name: string;
      partitions: ReadonlyArray<{ index: number; offset: bigint }>;
    }>,
    options: { timeoutMs?: number } = {},
  ): Promise<DeleteRecordsResult[]> {
    this.#open();
    if (!topics.length) return [];
    const body = new Writer()
      .array(topics, (writer, topic) =>
        writer
          .string(topic.name)
          .array(topic.partitions, (partitionWriter, partition) =>
            partitionWriter.i32(partition.index).i64(partition.offset),
          ),
      )
      .i32(options.timeoutMs ?? 30_000);
    const response = await this.#cluster.anyRequest(API_DELETE_RECORDS, 1, body);
    this.#cluster.throttle(API_DELETE_RECORDS, response.i32());
    return response
      .array((topicReader) => {
        const name = topicReader.string() ?? "";
        return topicReader.array((partitionReader) => {
          const index = partitionReader.i32();
          const lowWatermark = partitionReader.i64();
          const error = partitionReader.i16();
          return { name, index, lowWatermark, error };
        });
      })
      .flat();
  }

  /** Describe client quota entities matching the supplied filters (DescribeClientQuotas v0). */
  async describeClientQuotas(
    components: ReadonlyArray<{
      entityType: string;
      matchType?: "exact" | "defaulted" | "any";
      match: string | null;
    }> = [],
    options: { strict?: boolean } = {},
  ): Promise<
    Array<{
      entities: Array<{ entityType: string; entityName: string | null }>;
      values: Array<{ name: string; value: number }>;
    }>
  > {
    this.#open();
    const matchTypes = { exact: 0, defaulted: 1, any: 2 } as const;
    const body = new Writer()
      .compactArray(components, (writer, component) =>
        writer
          .compactString(component.entityType)
          .i8(matchTypes[component.matchType ?? "exact"])
          .compactString(component.match),
      )
      .bool(options.strict ?? false)
      .tags();
    const response = await this.#cluster.anyRequest(API_DESCRIBE_CLIENT_QUOTAS, 1, body, true);
    this.#cluster.throttle(API_DESCRIBE_CLIENT_QUOTAS, response.i32());
    const error = response.i16();
    // The nullable error_message field must be consumed regardless of outcome.
    const errorMessage = response.compactString();
    if (error) throw kafkaError(error, "Describe client quotas", errorMessage);
    return response.compactArray((entryReader) => {
      const entry = {
        entities: entryReader.compactArray((entityReader) => {
          const entity = {
            entityType: entityReader.compactString() ?? "",
            entityName: entityReader.compactString(),
          };
          entityReader.skipTags(); // per-struct tagged fields (KIP-482)
          return entity;
        }),
        values: entryReader.compactArray((valueReader) => {
          const value = { name: valueReader.compactString() ?? "", value: valueReader.f64() };
          valueReader.skipTags();
          return value;
        }),
      };
      entryReader.skipTags(); // entry-level tags
      return entry;
    });
  }

  /** Alter client quota entity values (AlterClientQuotas v1). */
  async alterClientQuotas(
    entries: ReadonlyArray<{
      entity: ReadonlyArray<{ entityType: string; entityName: string | null }>;
      ops: ReadonlyArray<{ key: string; value?: number; remove?: boolean }>;
      validateOnly?: boolean;
    }>,
  ): Promise<
    Array<{
      error: number;
      message: string | null;
      entity: Array<{ entityType: string; entityName: string | null }>;
    }>
  > {
    this.#open();
    if (!entries.length) return [];
    // Flexible versions close every struct (array elements included) with a
    // tagged-field section.
    const body = new Writer()
      .compactArray(entries, (writer, entry) => {
        writer.compactArray(entry.entity, (entityWriter, item) =>
          entityWriter.compactString(item.entityType).compactString(item.entityName).tags(),
        );
        writer.compactArray(entry.ops, (opsWriter, op) =>
          opsWriter
            .compactString(op.key)
            .f64(op.value ?? Number.NaN)
            .bool(op.remove ?? false)
            .tags(),
        );
        writer.tags(); // entry-level tags
      })
      .bool(entries.some((entry) => entry.validateOnly) ?? false)
      .tags();
    const response = await this.#cluster.anyRequest(API_ALTER_CLIENT_QUOTAS, 1, body, true);
    this.#cluster.throttle(API_ALTER_CLIENT_QUOTAS, response.i32());
    return response.compactArray((entryReader) => {
      const error = entryReader.i16();
      const message = entryReader.compactString();
      const entity = entryReader.compactArray((entityReader) => {
        const item = {
          entityType: entityReader.compactString() ?? "",
          entityName: entityReader.compactString(),
        };
        entityReader.skipTags();
        return item;
      });
      entryReader.skipTags(); // entry-level tags
      return { error, message, entity };
    });
  }

  /**
   * Create a delegation token owned by the authenticated (or delegated-to)
   * principal (CreateDelegationToken v2). Delegation token APIs require SASL
   * authentication and broker support; brokers answer UNSUPPORTED_VERSION when
   * unavailable.
   */
  async createDelegationToken(
    options: {
      ownerPrincipalType?: string | null;
      ownerPrincipalName?: string | null;
      renewalPeriodMs?: number | bigint;
    } = {},
  ): Promise<{
    error: number;
    principalType: string;
    principalName: string;
    tokenRequester: string;
    issueTimestampMs: bigint;
    expiryTimestampMs: bigint;
    maxTimestampMs: bigint;
    tokenId: string;
    hmac: Uint8Array | null;
  }> {
    this.#open();
    const body = new Writer()
      .compactString(options.ownerPrincipalType ?? null)
      .compactString(options.ownerPrincipalName ?? null)
      .i64(options.renewalPeriodMs === undefined ? -1n : BigInt(options.renewalPeriodMs))
      .tags();
    const response = await this.#cluster.anyRequest(API_CREATE_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_CREATE_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Create delegation token", message);
    return {
      error,
      principalType: response.compactString() ?? "",
      principalName: response.compactString() ?? "",
      tokenRequester: response.compactString() ?? "",
      issueTimestampMs: response.i64(),
      expiryTimestampMs: response.i64(),
      maxTimestampMs: response.i64(),
      tokenId: response.compactString() ?? "",
      hmac: response.compactBytes(),
    };
  }

  /** List delegation tokens visible to the authenticated principal (DescribeDelegationToken v2). */
  async describeDelegationTokens(
    ownersFilters: ReadonlyArray<{ principalType: string; principalName: string }> = [],
  ): Promise<
    Array<{
      ownerPrincipalType: string;
      ownerPrincipalName: string;
      tokenRequester: string;
      issueTimestampMs: bigint;
      expiryTimestampMs: bigint;
      maxTimestampMs: bigint;
      tokenId: string;
      hmac: Uint8Array | null;
    }>
  > {
    this.#open();
    const body = new Writer()
      .compactArray(ownersFilters.length ? ownersFilters : null, (writer, owner) =>
        writer.compactString(owner.principalType).compactString(owner.principalName).tags(),
      )
      .tags();
    const response = await this.#cluster.anyRequest(API_DESCRIBE_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_DESCRIBE_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Describe delegation tokens", message);
    return response.compactArray((tokenReader) => {
      const token = {
        ownerPrincipalType: tokenReader.compactString() ?? "",
        ownerPrincipalName: tokenReader.compactString() ?? "",
        tokenRequester: tokenReader.compactString() ?? "",
        issueTimestampMs: tokenReader.i64(),
        expiryTimestampMs: tokenReader.i64(),
        maxTimestampMs: tokenReader.i64(),
        tokenId: tokenReader.compactString() ?? "",
        hmac: tokenReader.compactBytes(),
      };
      tokenReader.skipTags();
      return token;
    });
  }

  /** Renew a delegation token before its expiry (RenewDelegationToken v2). */
  async renewDelegationToken(
    tokenHmac: Uint8Array,
    renewPeriodMs: number | bigint,
  ): Promise<{ error: number; expiryTimestampMs: bigint }> {
    this.#open();
    const body = new Writer().compactBytes(tokenHmac).i64(BigInt(renewPeriodMs)).tags();
    const response = await this.#cluster.anyRequest(API_RENEW_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_RENEW_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Renew delegation token", message);
    return { error, expiryTimestampMs: response.i64() };
  }

  /** Expire a delegation token early (ExpireDelegationToken v2). */
  async expireDelegationToken(
    tokenHmac: Uint8Array,
    expiryTimePeriodMs: number | bigint = -1,
  ): Promise<{ error: number; expiryTimestampMs: bigint }> {
    this.#open();
    const body = new Writer().compactBytes(tokenHmac).i64(BigInt(expiryTimePeriodMs)).tags();
    const response = await this.#cluster.anyRequest(API_EXPIRE_DELEGATION_TOKEN, 2, body, true);
    this.#cluster.throttle(API_EXPIRE_DELEGATION_TOKEN, response.i32());
    const error = response.i16();
    const message = response.compactString();
    if (error) throw kafkaError(error, "Expire delegation token", message);
    return { error, expiryTimestampMs: response.i64() };
  }

  /** Create ACL bindings on the broker. */
  async createAcls(
    bindings: readonly AclBinding[],
    options: { timeoutMs?: number } = {},
  ): Promise<Array<{ error: number; message: string | null }>> {
    this.#open();
    if (!bindings.length) return [];
    const body = new Writer()
      .array(bindings, (writer, acl) =>
        writer
          .i8(acl.resourceType)
          .string(acl.resourceName)
          .string(acl.principal)
          .string(acl.host)
          .i8(acl.operation)
          .i8(acl.permissionType),
      )
      .i32(options.timeoutMs ?? 30_000);
    const response = await this.#cluster.anyRequest(API_CREATE_ACLS, 0, body);
    this.#cluster.throttle(API_CREATE_ACLS, response.i32());
    return response.array((reader) => ({ error: reader.i16(), message: reader.string() }));
  }

  /** List ACLs matching the filter; null filter fields match anything. */
  async describeAcls(
    filter: AclFilter,
  ): Promise<{ error: number; message: string | null; acls: AclListing[] }> {
    this.#open();
    const body = new Writer()
      .i8(filter.resourceType)
      .string(filter.resourceName ?? null)
      .string(filter.principal ?? null)
      .string(filter.host ?? null)
      .i8(filter.operation)
      .i8(filter.permissionType);
    const response = await this.#cluster.anyRequest(API_DESCRIBE_ACLS, 0, body);
    this.#cluster.throttle(29, response.i32());
    const error = response.i16();
    const message = response.string();
    const acls = response
      .array((reader) => {
        const resourceType = reader.i8();
        const resourceName = reader.string() ?? "";
        return reader.array((aclReader) => ({
          resourceType,
          resourceName,
          principal: aclReader.string() ?? "",
          host: aclReader.string() ?? "",
          operation: aclReader.i8(),
          permissionType: aclReader.i8(),
        }));
      })
      .flat();
    return { error, message, acls };
  }

  /** Delete ACLs matching the filters; null filter fields match anything. */
  async deleteAcls(
    filters: readonly AclFilter[],
    options: { timeoutMs?: number } = {},
  ): Promise<Array<{ error: number; message: string | null; acls: AclListing[] }>> {
    this.#open();
    const body = new Writer()
      .array(filters, (writer, filter) =>
        writer
          .i8(filter.resourceType)
          .string(filter.resourceName ?? null)
          .string(filter.principal ?? null)
          .string(filter.host ?? null)
          .i8(filter.operation)
          .i8(filter.permissionType),
      )
      .i32(options.timeoutMs ?? 30_000);
    const response = await this.#cluster.anyRequest(API_DELETE_ACLS, 0, body);
    this.#cluster.throttle(API_DELETE_ACLS, response.i32());
    return response.array((reader) => {
      const error = reader.i16();
      const message = reader.string();
      // Matching ACLs are flat; each carries its own error code/message.
      const acls = reader.array((aclReader) => ({
        error: aclReader.i16(),
        message: aclReader.string(),
        resourceType: aclReader.i8(),
        resourceName: aclReader.string() ?? "",
        principal: aclReader.string() ?? "",
        host: aclReader.string() ?? "",
        operation: aclReader.i8(),
        permissionType: aclReader.i8(),
      }));
      return { error, message, acls };
    });
  }

  /**
   * Committed offsets for a consumer group. Topics omitted scans every
   * cluster topic (OffsetFetch v2 per topic; no v5 nullable-topics needed).
   */
  async groupOffsets(
    groupId: string,
    topics?: readonly string[],
  ): Promise<
    Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: bigint; metadata: string | null }>;
    }>
  > {
    this.#open();
    if (!groupId) throw new Error("groupId is required");
    const names =
      topics ??
      (await this.metadata(null)).topics
        .filter((topic) => !topic.err && topic.name && !topic.name.startsWith("__"))
        .map((topic) => topic.name);
    const coordinator = await this.#findGroupCoordinator(groupId);
    const result: Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: bigint; metadata: string | null }>;
    }> = [];
    for (const topic of names) {
      const meta = await this.#cluster.topic(topic);
      if (meta.err || !meta.partitions.length) continue;
      const partitions = meta.partitions.map((p) => p.id);
      const body = new Writer()
        .string(groupId)
        .array([topic], (writer, name) =>
          writer
            .string(name)
            .array(partitions, (partitionWriter, partition) => partitionWriter.i32(partition)),
        );
      const response = await this.#cluster.request(coordinator, API_OFFSET_FETCH, 2, body);
      const parsed = response
        .array((topicReader) => {
          const name = topicReader.string() ?? "";
          return topicReader.array((partitionReader) => ({
            name,
            partition: partitionReader.i32(),
            offset: partitionReader.i64(),
            metadata: partitionReader.string(),
            error: partitionReader.i16(),
          }));
        })
        .flat();
      response.i16(); // top-level error code
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
    this.#open();
    if (!topics.length) return;
    const coordinator = await this.#findGroupCoordinator(groupId);
    const body = new Writer()
      .string(groupId)
      .i32(-1)
      .string("")
      .i64(-1n)
      .array(topics, (writer, { topic, partitions }) =>
        writer.string(topic).array(partitions, (partitionWriter, entry) =>
          partitionWriter
            .i32(entry.partition)
            .i64(entry.offset)
            .string(entry.metadata ?? null),
        ),
      );
    const response = await this.#cluster.request(coordinator, API_OFFSET_COMMIT, 2, body);
    for (const topicResult of response.array((reader) => ({
      topic: reader.string() ?? "",
      partitions: reader.array((p) => ({ partition: p.i32(), error: p.i16() })),
    }))) {
      for (const partition of topicResult.partitions) {
        if (partition.error)
          throw kafkaError(partition.error, `${topicResult.topic}[${partition.partition}]`);
      }
    }
  }

  /** Move a group's offsets to the earliest or latest watermarks of a topic. */
  async resetGroupOffsets(groupId: string, topic: string, earliest = true): Promise<void> {
    this.#open();
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
    this.#open();
    const meta = await this.#cluster.topic(topic);
    if (meta.err) throw kafkaError(meta.err, topic);
    return Promise.all(
      meta.partitions.map(async (partition) => {
        const [low, high] = await Promise.all([
          this.offsetByTimestamp(topic, partition.id, -2),
          this.offsetByTimestamp(topic, partition.id, -1),
        ]);
        return { partition: partition.id, low, high };
      }),
    );
  }

  /** First offset at or after a timestamp; -2/-1 resolve to earliest/latest. Returns -1 when none match. */
  async offsetByTimestamp(topic: string, partition: number, timestamp: number): Promise<bigint> {
    this.#open();
    const meta = await this.#cluster.topic(topic);
    const leader = meta.partitions.find((p) => p.id === partition)?.leader;
    if (leader === undefined)
      throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
    const body = new Writer().i32(-1).array([topic], (writer, name) =>
      writer.string(name).array([partition], (partitionWriter, index) => {
        partitionWriter.i32(index).i64(BigInt(timestamp));
      }),
    );
    const response = await this.#cluster.request(leader, API_LIST_OFFSETS, 1, body);
    const result = response.array((topicReader) => {
      topicReader.string();
      return topicReader.array((partitionReader) => {
        partitionReader.i32();
        const error = partitionReader.i16();
        partitionReader.i64();
        const offset = partitionReader.i64();
        if (error) throw kafkaError(error, `${topic}[${partition}]`);
        return offset;
      });
    });
    return result[0]?.[0] ?? -1n;
  }

  async #findGroupCoordinator(groupId: string): Promise<number> {
    const response = await this.#cluster.anyRequest(
      API_FIND_COORDINATOR,
      0,
      new Writer().string(groupId),
    );
    const error = response.i16();
    const coordinator = response.i32();
    if (error) throw kafkaError(error, `Kafka group ${groupId}`);
    return coordinator;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#ownsCluster) this.#cluster.close();
    this.#onClose();
  }

  disconnect(): Promise<void> {
    return this.close();
  }
  #open(): void {
    if (this.#closed) throw new Error("Admin is closed");
  }
}

export interface GroupMemberDescription {
  memberId: string;
  clientId: string;
  clientHost: string;
  memberMetadata: Uint8Array | null;
  memberAssignment: Uint8Array | null;
}

export interface GroupDescription {
  error: number;
  message: string | null;
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string | null;
  members: GroupMemberDescription[];
}

export type DeleteRecordsResult = {
  name: string;
  index: number;
  lowWatermark: bigint;
  error: number;
};

export type AclResourceType = number;
export type AclOperation = number;
export type AclPermissionType = number;

export interface AclBinding {
  resourceType: AclResourceType;
  resourceName: string;
  principal: string;
  host: string;
  operation: AclOperation;
  permissionType: AclPermissionType;
}

export type AclFilter = {
  resourceType: AclResourceType;
  resourceName?: string;
  principal?: string;
  host?: string;
  operation: AclOperation;
  permissionType: AclPermissionType;
};

export interface AclListing {
  error?: number;
  message?: string | null;
  resourceType: number;
  resourceName: string;
  principal: string;
  host: string;
  operation: number;
  permissionType: number;
}
