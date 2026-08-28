import type { ClusterMetadata } from "../types.ts";
import { Cluster } from "./cluster.ts";
import {
  writeCreateTopicsRequest,
  readCreateTopicsResponse,
  writeDeleteTopicsRequest,
  readDeleteTopicsResponse,
  writeCreatePartitionsRequest,
  readCreatePartitionsResponse,
  writeDescribeConfigsRequest,
  readDescribeConfigsResponse,
  writeAlterConfigsRequest,
  readAlterConfigsResponse,
  writeIncrementalAlterConfigsRequest,
  readIncrementalAlterConfigsResponse,
  writeListGroupsRequest,
  readListGroupsResponse,
  writeGroupIdsRequest,
  readDeleteGroupsResponse,
  readDescribeGroupsResponse,
  writeDeleteRecordsRequest,
  readDeleteRecordsResponse,
  writeDescribeClientQuotasRequest,
  readDescribeClientQuotasResponse,
  writeAlterClientQuotasRequest,
  readAlterClientQuotasResponse,
  writeCreateDelegationTokenRequest,
  readCreateDelegationTokenResponse,
  writeDescribeDelegationTokenRequest,
  readDescribeDelegationTokenResponse,
  writeTokenPeriodRequest,
  readTokenPeriodResponse,
  writeCreateAclsRequest,
  readCreateAclsResponse,
  writeDescribeAclsRequest,
  readDescribeAclsResponse,
  writeDeleteAclsRequest,
  readDeleteAclsResponse,
  writeOffsetFetchRequest,
  readOffsetFetchAdminResponse,
  writeOffsetCommitRequest,
  readOffsetCommitResponse,
  writeAdminListOffsetsRequest,
  readAdminListOffsetsResponse,
  writeFindCoordinatorRequest,
  readGroupCoordinatorResponse,
} from "../protocol/index.ts";
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
  API_INCREMENTAL_ALTER_CONFIGS,
  API_LIST_GROUPS,
  API_LIST_OFFSETS,
  API_OFFSET_COMMIT,
  API_OFFSET_FETCH,
  API_RENEW_DELEGATION_TOKEN,
  CREATE_TOPICS_API_VERSION,
  DELETE_TOPICS_API_VERSION,
  DESCRIBE_ACLS_API_KEY,
  DEFAULT_ADMIN_POLL_MIN_MS,
  DEFAULT_ADMIN_POLL_SLEEP_MS,
  DEFAULT_ADMIN_TIMEOUT_MS,
  EARLIEST_OFFSET,
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
    if (this.#closed) {
      throw new Error("Admin is closed");
    }
    return this.#cluster.metadata(topics);
  }

  async createTopics(
    topics: readonly CreateTopicInput[],
    options: { timeoutMs?: number; validateOnly?: boolean; waitForLeaders?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.#open();
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
    const response = await this.#cluster.controllerRequest(
      API_CREATE_TOPICS,
      CREATE_TOPICS_API_VERSION,
      body,
    );
    const decoded = readCreateTopicsResponse(response);
    this.#cluster.throttle(API_CREATE_TOPICS, decoded.throttleMs);
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
    this.#open();
    if (!topics.length) {
      return [];
    }
    const response = await this.#cluster.controllerRequest(
      API_DELETE_TOPICS,
      DELETE_TOPICS_API_VERSION,
      writeDeleteTopicsRequest(topics, options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS),
    );
    const decoded = readDeleteTopicsResponse(response);
    this.#cluster.throttle(API_DELETE_TOPICS, decoded.throttleMs);
    return decoded.results.map(({ message: _message, ...result }) => ({
      ...result,
      message: null,
    }));
  }

  async createPartitions(
    topics: readonly CreatePartitionsInput[],
    options: { timeoutMs?: number; validateOnly?: boolean } = {},
  ): Promise<TopicResult[]> {
    this.#open();
    if (!topics.length) {
      return [];
    }
    const body = writeCreatePartitionsRequest(
      topics,
      options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS,
      options.validateOnly ?? false,
    );
    const response = await this.#cluster.controllerRequest(API_CREATE_PARTITIONS, 2, body);
    const decoded = readCreatePartitionsResponse(response);
    this.#cluster.throttle(API_CREATE_PARTITIONS, decoded.throttleMs);
    return decoded.results;
  }

  async describeConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      configNames?: string[] | null;
    }[],
  ): Promise<ConfigResource[]> {
    this.#open();
    const body = writeDescribeConfigsRequest(resources);
    const response = await this.#cluster.anyRequest(API_DESCRIBE_CONFIGS, 0, body);
    const decoded = readDescribeConfigsResponse(response);
    this.#cluster.throttle(API_DESCRIBE_CONFIGS, decoded.throttleMs);
    return decoded.results;
  }

  async alterConfigs(
    resources: readonly {
      resourceType: number;
      resourceName: string;
      configs: Record<string, string | null>;
    }[],
  ): Promise<TopicResult[]> {
    this.#open();
    const body = writeAlterConfigsRequest(resources);
    const response = await this.#cluster.anyRequest(API_ALTER_CONFIGS, 0, body);
    const decoded = readAlterConfigsResponse(response);
    this.#cluster.throttle(API_ALTER_CONFIGS, decoded.throttleMs);
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
    this.#open();
    if (!resources.length) {
      return [];
    }
    const body = writeIncrementalAlterConfigsRequest(resources, options.validateOnly ?? false);
    const response = await this.#cluster.anyRequest(API_INCREMENTAL_ALTER_CONFIGS, 1, body, true);
    const decoded = readIncrementalAlterConfigsResponse(response);
    this.#cluster.throttle(API_INCREMENTAL_ALTER_CONFIGS, decoded.throttleMs);
    return decoded.results;
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
    const response = await this.#cluster.anyRequest(API_LIST_GROUPS, 1, writeListGroupsRequest());
    const decoded = readListGroupsResponse(response);
    this.#cluster.throttle(API_LIST_GROUPS, decoded.throttleMs);
    const { error } = decoded;
    if (error) {
      throw kafkaError(error, "ListGroups");
    }
    return decoded.groups;
  }

  /** Describe consumer groups: state and member details. */
  async describeGroups(groupIds: readonly string[]): Promise<GroupDescription[]> {
    this.#open();
    if (!groupIds.length) {
      return [];
    }
    const body = writeGroupIdsRequest(groupIds);
    const response = await this.#cluster.anyRequest(API_DESCRIBE_GROUPS, 1, body);
    const decoded = readDescribeGroupsResponse(response);
    this.#cluster.throttle(API_DESCRIBE_GROUPS, decoded.throttleMs);
    return decoded.groups;
  }

  /** Delete consumer groups that no longer have active members. */
  async deleteGroups(groupIds: readonly string[]): Promise<TopicResult[]> {
    this.#open();
    if (!groupIds.length) {
      return [];
    }
    const body = writeGroupIdsRequest(groupIds);
    const response = await this.#cluster.anyRequest(API_DELETE_GROUPS, 1, body);
    const decoded = readDeleteGroupsResponse(response);
    this.#cluster.throttle(API_DELETE_GROUPS, decoded.throttleMs);
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
    this.#open();
    if (!topics.length) {
      return [];
    }
    const body = writeDeleteRecordsRequest(topics, options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS);
    const response = await this.#cluster.anyRequest(API_DELETE_RECORDS, 1, body);
    const decoded = readDeleteRecordsResponse(response);
    this.#cluster.throttle(API_DELETE_RECORDS, decoded.throttleMs);
    return decoded.results;
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
    const body = writeDescribeClientQuotasRequest(components, options.strict ?? false);
    const response = await this.#cluster.anyRequest(API_DESCRIBE_CLIENT_QUOTAS, 1, body, true);
    const decoded = readDescribeClientQuotasResponse(response);
    this.#cluster.throttle(API_DESCRIBE_CLIENT_QUOTAS, decoded.throttleMs);
    if (decoded.error) {
      throw kafkaError(decoded.error, "Describe client quotas", decoded.message);
    }
    return decoded.entries;
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
    if (!entries.length) {
      return [];
    }
    // Flexible versions close every struct (array elements included) with a
    // tagged-field section.
    const body = writeAlterClientQuotasRequest(entries);
    const response = await this.#cluster.anyRequest(API_ALTER_CLIENT_QUOTAS, 1, body, true);
    const decoded = readAlterClientQuotasResponse(response);
    this.#cluster.throttle(API_ALTER_CLIENT_QUOTAS, decoded.throttleMs);
    return decoded.entries;
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
    const body = writeCreateDelegationTokenRequest(
      options.ownerPrincipalType ?? null,
      options.ownerPrincipalName ?? null,
      options.renewalPeriodMs ?? -1,
    );
    const response = await this.#cluster.anyRequest(API_CREATE_DELEGATION_TOKEN, 2, body, true);
    const decoded = readCreateDelegationTokenResponse(response);
    this.#cluster.throttle(API_CREATE_DELEGATION_TOKEN, decoded.throttleMs);
    if (decoded.error) {
      throw kafkaError(decoded.error, "Create delegation token", decoded.message);
    }
    return { error: decoded.error, ...decoded.token };
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
    const body = writeDescribeDelegationTokenRequest(ownersFilters);
    const response = await this.#cluster.anyRequest(API_DESCRIBE_DELEGATION_TOKEN, 2, body, true);
    const decoded = readDescribeDelegationTokenResponse(response);
    this.#cluster.throttle(API_DESCRIBE_DELEGATION_TOKEN, decoded.throttleMs);
    if (decoded.error) {
      throw kafkaError(decoded.error, "Describe delegation tokens", decoded.message);
    }
    return decoded.tokens.map(
      ({
        principalType,
        principalName,
        tokenRequester,
        issueTimestampMs,
        expiryTimestampMs,
        maxTimestampMs,
        tokenId,
        hmac,
      }) => ({
        ownerPrincipalType: principalType,
        ownerPrincipalName: principalName,
        tokenRequester,
        issueTimestampMs,
        expiryTimestampMs,
        maxTimestampMs,
        tokenId,
        hmac,
      }),
    );
  }

  /** Renew a delegation token before its expiry (RenewDelegationToken v2). */
  async renewDelegationToken(
    tokenHmac: Uint8Array,
    renewPeriodMs: number | bigint,
  ): Promise<{ error: number; expiryTimestampMs: bigint }> {
    this.#open();
    const body = writeTokenPeriodRequest(tokenHmac, renewPeriodMs);
    const response = await this.#cluster.anyRequest(API_RENEW_DELEGATION_TOKEN, 2, body, true);
    const decoded = readTokenPeriodResponse(response);
    this.#cluster.throttle(API_RENEW_DELEGATION_TOKEN, decoded.throttleMs);
    if (decoded.error) {
      throw kafkaError(decoded.error, "Renew delegation token", decoded.message);
    }
    return { error: decoded.error, expiryTimestampMs: decoded.expiryTimestampMs };
  }

  /** Expire a delegation token early (ExpireDelegationToken v2). */
  async expireDelegationToken(
    tokenHmac: Uint8Array,
    expiryTimePeriodMs: number | bigint = -1,
  ): Promise<{ error: number; expiryTimestampMs: bigint }> {
    this.#open();
    const body = writeTokenPeriodRequest(tokenHmac, expiryTimePeriodMs);
    const response = await this.#cluster.anyRequest(API_EXPIRE_DELEGATION_TOKEN, 2, body, true);
    const decoded = readTokenPeriodResponse(response);
    this.#cluster.throttle(API_EXPIRE_DELEGATION_TOKEN, decoded.throttleMs);
    if (decoded.error) {
      throw kafkaError(decoded.error, "Expire delegation token", decoded.message);
    }
    return { error: decoded.error, expiryTimestampMs: decoded.expiryTimestampMs };
  }

  /** Create ACL bindings on the broker. */
  async createAcls(
    bindings: readonly AclBinding[],
    options: { timeoutMs?: number } = {},
  ): Promise<Array<{ error: number; message: string | null }>> {
    this.#open();
    if (!bindings.length) {
      return [];
    }
    const body = writeCreateAclsRequest(bindings, options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS);
    const response = await this.#cluster.anyRequest(API_CREATE_ACLS, 0, body);
    const decoded = readCreateAclsResponse(response);
    this.#cluster.throttle(API_CREATE_ACLS, decoded.throttleMs);
    return decoded.results;
  }

  /** List ACLs matching the filter; null filter fields match anything. */
  async describeAcls(
    filter: AclFilter,
  ): Promise<{ error: number; message: string | null; acls: AclListing[] }> {
    this.#open();
    const body = writeDescribeAclsRequest({
      ...filter,
      resourceName: filter.resourceName ?? null,
      principal: filter.principal ?? null,
      host: filter.host ?? null,
    });
    const response = await this.#cluster.anyRequest(API_DESCRIBE_ACLS, 0, body);
    const decoded = readDescribeAclsResponse(response);
    this.#cluster.throttle(DESCRIBE_ACLS_API_KEY, decoded.throttleMs);
    return decoded;
  }

  /** Delete ACLs matching the filters; null filter fields match anything. */
  async deleteAcls(
    filters: readonly AclFilter[],
    options: { timeoutMs?: number } = {},
  ): Promise<Array<{ error: number; message: string | null; acls: AclListing[] }>> {
    this.#open();
    const body = writeDeleteAclsRequest(
      filters.map((filter) => ({
        ...filter,
        resourceName: filter.resourceName ?? null,
        principal: filter.principal ?? null,
        host: filter.host ?? null,
      })),
      options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS,
    );
    const response = await this.#cluster.anyRequest(API_DELETE_ACLS, 0, body);
    const decoded = readDeleteAclsResponse(response);
    this.#cluster.throttle(API_DELETE_ACLS, decoded.throttleMs);
    return decoded.results;
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
    if (!groupId) {
      throw new Error("groupId is required");
    }
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
      if (meta.err || !meta.partitions.length) {
        continue;
      }
      const partitions = meta.partitions.map((p) => p.id);
      const body = writeOffsetFetchRequest(groupId, topic, partitions);
      const response = await this.#cluster.request(coordinator, API_OFFSET_FETCH, 2, body);
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
    this.#open();
    if (!topics.length) {
      return;
    }
    const coordinator = await this.#findGroupCoordinator(groupId);
    const body = writeOffsetCommitRequest(groupId, topics);
    const response = await this.#cluster.request(coordinator, API_OFFSET_COMMIT, 2, body);
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
    this.#open();
    const meta = await this.#cluster.topic(topic);
    const leader = meta.partitions.find((p) => p.id === partition)?.leader;
    if (leader === undefined) {
      throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
    }
    const body = writeAdminListOffsetsRequest(topic, partition, BigInt(timestamp));
    const response = await this.#cluster.request(leader, API_LIST_OFFSETS, 1, body);
    const result = readAdminListOffsetsResponse(response);
    const item = result[0];
    if (item?.error) {
      throw kafkaError(item.error, `${topic}[${partition}]`);
    }
    return item?.offset ?? -1n;
  }

  async #findGroupCoordinator(groupId: string): Promise<number> {
    const response = await this.#cluster.anyRequest(
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

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    if (this.#ownsCluster) {
      this.#cluster.close();
    }
    this.#onClose();
  }

  disconnect(): Promise<void> {
    return this.close();
  }
  #open(): void {
    if (this.#closed) {
      throw new Error("Admin is closed");
    }
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
