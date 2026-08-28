import {
  readAlterClientQuotasResponse,
  readCreateAclsResponse,
  readCreateDelegationTokenResponse,
  readDeleteAclsResponse,
  readDescribeAclsResponse,
  readDescribeClientQuotasResponse,
  readDescribeDelegationTokenResponse,
  readTokenPeriodResponse,
  writeAlterClientQuotasRequest,
  writeCreateAclsRequest,
  writeCreateDelegationTokenRequest,
  writeDeleteAclsRequest,
  writeDescribeAclsRequest,
  writeDescribeClientQuotasRequest,
  writeDescribeDelegationTokenRequest,
  writeTokenPeriodRequest,
} from "../../protocol/index.ts";
import {
  API_ALTER_CLIENT_QUOTAS,
  API_CREATE_ACLS,
  API_CREATE_DELEGATION_TOKEN,
  API_DELETE_ACLS,
  API_DESCRIBE_ACLS,
  API_DESCRIBE_CLIENT_QUOTAS,
  API_DESCRIBE_DELEGATION_TOKEN,
  API_EXPIRE_DELEGATION_TOKEN,
  API_RENEW_DELEGATION_TOKEN,
  DEFAULT_ADMIN_TIMEOUT_MS,
  DESCRIBE_ACLS_API_KEY,
  kafkaError,
} from "../shared.ts";
import { AdminGroups } from "./groups.ts";
import type { AclBinding, AclFilter, AclListing } from "./types.ts";

export class AdminSecurity extends AdminGroups {
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
    this.open();
    const body = writeDescribeClientQuotasRequest(components, options.strict ?? false);
    const response = await this.cluster.anyRequest(API_DESCRIBE_CLIENT_QUOTAS, 1, body, true);
    const decoded = readDescribeClientQuotasResponse(response);
    this.cluster.throttle(API_DESCRIBE_CLIENT_QUOTAS, decoded.throttleMs);
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
    this.open();
    if (!entries.length) {
      return [];
    }
    // Flexible versions close every struct (array elements included) with a
    // tagged-field section.
    const body = writeAlterClientQuotasRequest(entries);
    const response = await this.cluster.anyRequest(API_ALTER_CLIENT_QUOTAS, 1, body, true);
    const decoded = readAlterClientQuotasResponse(response);
    this.cluster.throttle(API_ALTER_CLIENT_QUOTAS, decoded.throttleMs);
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
    this.open();
    const body = writeCreateDelegationTokenRequest(
      options.ownerPrincipalType ?? null,
      options.ownerPrincipalName ?? null,
      options.renewalPeriodMs ?? -1,
    );
    const response = await this.cluster.anyRequest(API_CREATE_DELEGATION_TOKEN, 2, body, true);
    const decoded = readCreateDelegationTokenResponse(response);
    this.cluster.throttle(API_CREATE_DELEGATION_TOKEN, decoded.throttleMs);
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
    this.open();
    const body = writeDescribeDelegationTokenRequest(ownersFilters);
    const response = await this.cluster.anyRequest(API_DESCRIBE_DELEGATION_TOKEN, 2, body, true);
    const decoded = readDescribeDelegationTokenResponse(response);
    this.cluster.throttle(API_DESCRIBE_DELEGATION_TOKEN, decoded.throttleMs);
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
    this.open();
    const body = writeTokenPeriodRequest(tokenHmac, renewPeriodMs);
    const response = await this.cluster.anyRequest(API_RENEW_DELEGATION_TOKEN, 2, body, true);
    const decoded = readTokenPeriodResponse(response);
    this.cluster.throttle(API_RENEW_DELEGATION_TOKEN, decoded.throttleMs);
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
    this.open();
    const body = writeTokenPeriodRequest(tokenHmac, expiryTimePeriodMs);
    const response = await this.cluster.anyRequest(API_EXPIRE_DELEGATION_TOKEN, 2, body, true);
    const decoded = readTokenPeriodResponse(response);
    this.cluster.throttle(API_EXPIRE_DELEGATION_TOKEN, decoded.throttleMs);
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
    this.open();
    if (!bindings.length) {
      return [];
    }
    const body = writeCreateAclsRequest(bindings, options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS);
    const response = await this.cluster.anyRequest(API_CREATE_ACLS, 0, body);
    const decoded = readCreateAclsResponse(response);
    this.cluster.throttle(API_CREATE_ACLS, decoded.throttleMs);
    return decoded.results;
  }

  /** List ACLs matching the filter; null filter fields match anything. */
  async describeAcls(
    filter: AclFilter,
  ): Promise<{ error: number; message: string | null; acls: AclListing[] }> {
    this.open();
    const body = writeDescribeAclsRequest({
      ...filter,
      resourceName: filter.resourceName ?? null,
      principal: filter.principal ?? null,
      host: filter.host ?? null,
    });
    const response = await this.cluster.anyRequest(API_DESCRIBE_ACLS, 0, body);
    const decoded = readDescribeAclsResponse(response);
    this.cluster.throttle(DESCRIBE_ACLS_API_KEY, decoded.throttleMs);
    return decoded;
  }

  /** Delete ACLs matching the filters; null filter fields match anything. */
  async deleteAcls(
    filters: readonly AclFilter[],
    options: { timeoutMs?: number } = {},
  ): Promise<Array<{ error: number; message: string | null; acls: AclListing[] }>> {
    this.open();
    const body = writeDeleteAclsRequest(
      filters.map((filter) => ({
        ...filter,
        resourceName: filter.resourceName ?? null,
        principal: filter.principal ?? null,
        host: filter.host ?? null,
      })),
      options.timeoutMs ?? DEFAULT_ADMIN_TIMEOUT_MS,
    );
    const response = await this.cluster.anyRequest(API_DELETE_ACLS, 0, body);
    const decoded = readDeleteAclsResponse(response);
    this.cluster.throttle(API_DELETE_ACLS, decoded.throttleMs);
    return decoded.results;
  }

  /**
   * Committed offsets for a consumer group. Topics omitted scans every
   * cluster topic (OffsetFetch v2 per topic; no v5 nullable-topics needed).
   */
}
