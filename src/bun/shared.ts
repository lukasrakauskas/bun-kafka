import { KafkaError, kafkaErrorName } from "../errors.ts";
import type { ClusterMetadata } from "../types.ts";
import type { BunKafkaSasl, BunKafkaTls } from "./connection.ts";

export {
  API_ADD_OFFSETS_TO_TXN,
  API_ADD_PARTITIONS_TO_TXN,
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
  API_END_TXN,
  API_EXPIRE_DELEGATION_TOKEN,
  API_FETCH,
  API_FIND_COORDINATOR,
  API_HEARTBEAT,
  API_INIT_PRODUCER_ID,
  API_JOIN_GROUP,
  API_LEAVE_GROUP,
  API_LIST_GROUPS,
  API_LIST_OFFSETS,
  API_METADATA,
  API_OFFSET_COMMIT,
  API_OFFSET_FETCH,
  API_PRODUCE,
  API_RENEW_DELEGATION_TOKEN,
  API_SYNC_GROUP,
  API_TXN_OFFSET_COMMIT,
  address,
  kafkaError,
  partitionKey,
  retryDelay,
};
export type { TopicMetadata };

const API_PRODUCE = 0;
const API_FETCH = 1;
const API_LIST_OFFSETS = 2;
const API_METADATA = 3;
const API_CREATE_TOPICS = 19;
const API_DELETE_TOPICS = 20;
const API_CREATE_PARTITIONS = 37;
const API_DESCRIBE_CONFIGS = 32;
const API_ALTER_CONFIGS = 33;
const API_FIND_COORDINATOR = 10;
const API_JOIN_GROUP = 11;
const API_SYNC_GROUP = 14;
const API_HEARTBEAT = 12;
const API_LEAVE_GROUP = 13;
const API_OFFSET_COMMIT = 8;
const API_OFFSET_FETCH = 9;
const API_INIT_PRODUCER_ID = 22;
const API_DESCRIBE_GROUPS = 15;
const API_LIST_GROUPS = 16;
const API_DELETE_RECORDS = 21;
const API_CREATE_ACLS = 30;
const API_DESCRIBE_ACLS = 29;
const API_DELETE_ACLS = 31;
const API_DELETE_GROUPS = 42;
const API_DESCRIBE_CLIENT_QUOTAS = 48;
const API_ALTER_CLIENT_QUOTAS = 49;
const API_CREATE_DELEGATION_TOKEN = 38;
const API_RENEW_DELEGATION_TOKEN = 39;
const API_EXPIRE_DELEGATION_TOKEN = 40;
const API_DESCRIBE_DELEGATION_TOKEN = 41;
const API_ADD_PARTITIONS_TO_TXN = 24;
const API_ADD_OFFSETS_TO_TXN = 25;
// API key numbering per the Kafka protocol spec: EndTxn is 26,
// WriteTxnMarkers 27, TxnOffsetCommit 28.
const API_END_TXN = 26;
const API_TXN_OFFSET_COMMIT = 28;

const retriableErrors = new Set([
  1, 2, 3, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 19, 20, 25, 27, 32, 33, 38, 39, 41, 44, 45, 47, 49,
  56, 70, 71, 74, 75, 78, 82, 86, 88, 89,
]);

function kafkaError(code: number, context: string, detail?: string | null): KafkaError {
  const label = detail
    ? `${context}: ${kafkaErrorName(code)} (${detail})`
    : `${context}: ${kafkaErrorName(code)}`;
  return new KafkaError(code, label, {
    retriable: retriableErrors.has(code),
    fatal: code === 58 || code === 34,
  });
}

function address(host: string, port: number): string {
  return `${host.includes(":") ? `[${host}]` : host}:${port}`;
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
}

function retryDelay(options: Required<RetryOptions>, attempt: number): number {
  const base = Math.min(options.maxBackoffMs, options.initialBackoffMs * 2 ** attempt);
  return Math.round(base * (0.5 + Math.random()));
}

export interface RetryOptions {
  /** Maximum retries after the first request. Default 3. */
  maxRetries?: number;
  /** Initial retry delay. Default 50 ms. */
  initialBackoffMs?: number;
  /** Maximum retry delay. Default 2,000 ms. */
  maxBackoffMs?: number;
}

export type KafkaEvent =
  | { type: "retry"; apiKey: number; attempt: number; delayMs: number; error: unknown }
  | { type: "throttle"; apiKey: number; durationMs: number }
  | { type: "stats"; stats: ClusterStats };

export interface Logger {
  debug(message: string): void;
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export interface ConnectionStats {
  requests: number;
  bytesSent: number;
  bytesReceived: number;
}

export interface BrokerHealth {
  address: string;
  brokerId?: number;
  ok: boolean;
  latencyMs: number;
  error?: unknown;
}

export interface HealthReport {
  brokers: BrokerHealth[];
}

export interface ClusterStats extends Record<string, unknown> {
  connections: number;
  requests: number;
  bytesSent: number;
  bytesReceived: number;
  retries: number;
  throttles: number;
  throttleTimeMs: number;
}

export interface KafkaOptions {
  brokers: string[];
  clientId?: string;
  tls?: BunKafkaTls;
  sasl?: BunKafkaSasl;
  requestTimeoutMs?: number;
  connectTimeoutMs?: number;
  maxResponseBytes?: number;
  retry?: RetryOptions;
  onEvent?: (event: KafkaEvent) => void;
  /** Emit a stats event on this interval (ms). */
  statsIntervalMs?: number;
  /** Optional logging hooks for operational diagnostics. */
  logger?: Partial<Logger>;
}

type TopicMetadata = ClusterMetadata["topics"][number];

export interface CommittedOffset {
  topic: string;
  partition: number;
  offset: bigint;
}
