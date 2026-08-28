import { KafkaError, KafkaErrorCode, kafkaErrorName } from "../errors.ts";
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
  API_INCREMENTAL_ALTER_CONFIGS,
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
const API_INCREMENTAL_ALTER_CONFIGS = 44;
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

/** Shared client defaults (ms / bytes). */
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
export const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
export const DEFAULT_MAX_RESPONSE_BYTES = 104_857_600; // 100 MiB
export const DEFAULT_MAX_RETRIES = 3;
export const DEFAULT_INITIAL_BACKOFF_MS = 50;
export const DEFAULT_MAX_BACKOFF_MS = 2_000;
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;
export const DEFAULT_HEALTH_CHECK_TIMEOUT_MS = 5_000;
export const DEFAULT_SESSION_TIMEOUT_MS = 45_000;
export const DEFAULT_REBALANCE_TIMEOUT_MS = 60_000;
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 3_000;
export const DEFAULT_FETCH_MAX_WAIT_MS = 500;
export const DEFAULT_FETCH_IDLE_SLEEP_MS = 250;
export const DEFAULT_FETCH_MAX_MESSAGES = 500;
export const DEFAULT_FETCH_MAX_BYTES = 52_428_800; // 50 MiB
export const DEFAULT_FETCH_MAX_PARTITION_BYTES = 1_048_576; // 1 MiB
export const DEFAULT_PRODUCE_TIMEOUT_MS = 30_000;
export const DEFAULT_PRODUCE_LINGER_MS = 5;
export const DEFAULT_PRODUCE_BATCH_MAX_MESSAGES = 1_000;
export const DEFAULT_TRANSACTION_TIMEOUT_MS = 60_000;
export const DEFAULT_ADMIN_TIMEOUT_MS = 30_000;
export const DEFAULT_ADMIN_POLL_MIN_MS = 5_000;
export const DEFAULT_ADMIN_POLL_SLEEP_MS = 100;
export const DEFAULT_KAFKAJS_CONNECT_TIMEOUT_MS = 1_000;
export const DEFAULT_KAFKAJS_MAX_RETRIES = 5;
export const DEFAULT_KAFKAJS_INITIAL_BACKOFF_MS = 300;
export const DEFAULT_KAFKAJS_MAX_BACKOFF_MS = 30_000;
export const DEFAULT_KAFKAJS_MAX_WAIT_MS_CAP = 1_000;
export const DEFAULT_BROKER_PORT = 9092;
export const SIZE_I32 = 4;
export const SIZE_I64 = 8;
export const INT16_MAX = 0x7fff;
export const INT32_MAX = 0x7fff_ffff;
export const INT32_MIN = -0x8000_0000;
export const UINT32_MAX = 0xffff_ffff;
export const SEQ_WRAP = 0x8000_0000;
export const MAX_TCP_PORT = 65_535;
export const HEX_DUMP_BYTES = 80;
export const RETRY_JITTER_BASE = 0.5;
export const EARLIEST_OFFSET = -2;
export const LATEST_OFFSET = -1;
export const CONFIG_SOURCE_DEFAULT = 5;
export const RECORD_ATTR_CONTROL = 0x20;
export const RECORD_ATTR_TRANSACTIONAL = 0x10;
export const RECORD_ATTR_TIMESTAMP_TYPE = 8;
export const RECORD_BATCH_HEADER_SIZE = 61;
export const RECORD_BATCH_LENGTH_MIN = 9;
export const FETCH_API_VERSION = 7;
export const CREATE_TOPICS_API_VERSION = 4;
export const DELETE_TOPICS_API_VERSION = 3;
export const PRODUCE_API_VERSION = 3;
/** Join/Sync/Heartbeat/Leave versions that carry group.instance.id. */
export const GROUP_INSTANCE_API_VERSION = 3;
export const JOIN_GROUP_BASE_VERSION = 2;
export const DESCRIBE_ACLS_API_KEY = 29;
export const METADATA_REFRESH_SLEEP_MS = 10;
export const RADIX_HEX = 16;

export const API_API_VERSIONS = 18;
export const API_SASL_HANDSHAKE = 17;
export const API_SASL_AUTHENTICATE = 36;
export const COMPRESSION_MASK = 7;
export const MAX_COMPRESSION_TYPE = 4;

const retriableErrors = new Set<number>([
  KafkaErrorCode.OFFSET_OUT_OF_RANGE,
  KafkaErrorCode.CORRUPT_MESSAGE,
  KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
  KafkaErrorCode.LEADER_NOT_AVAILABLE,
  KafkaErrorCode.NOT_LEADER_OR_FOLLOWER,
  KafkaErrorCode.REQUEST_TIMED_OUT,
  KafkaErrorCode.BROKER_NOT_AVAILABLE,
  KafkaErrorCode.REPLICA_NOT_AVAILABLE,
  KafkaErrorCode.MESSAGE_TOO_LARGE,
  KafkaErrorCode.NETWORK_EXCEPTION,
  KafkaErrorCode.COORDINATOR_LOAD_IN_PROGRESS,
  KafkaErrorCode.COORDINATOR_NOT_AVAILABLE,
  KafkaErrorCode.NOT_COORDINATOR,
  KafkaErrorCode.NOT_ENOUGH_REPLICAS,
  KafkaErrorCode.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
  KafkaErrorCode.UNKNOWN_MEMBER_ID,
  KafkaErrorCode.REBALANCE_IN_PROGRESS,
  KafkaErrorCode.INVALID_TIMESTAMP,
  KafkaErrorCode.UNSUPPORTED_SASL_MECHANISM,
  KafkaErrorCode.INVALID_REPLICATION_FACTOR,
  KafkaErrorCode.INVALID_REPLICA_ASSIGNMENT,
  KafkaErrorCode.NOT_CONTROLLER,
  KafkaErrorCode.POLICY_VIOLATION,
  KafkaErrorCode.OUT_OF_ORDER_SEQUENCE_NUMBER,
  KafkaErrorCode.INVALID_PRODUCER_EPOCH,
  KafkaErrorCode.INVALID_PRODUCER_ID_MAPPING,
  KafkaErrorCode.KAFKA_STORAGE_ERROR,
  KafkaErrorCode.FETCH_SESSION_ID_NOT_FOUND,
  KafkaErrorCode.INVALID_FETCH_SESSION_EPOCH,
  KafkaErrorCode.FENCED_LEADER_EPOCH,
  KafkaErrorCode.UNKNOWN_LEADER_EPOCH,
  KafkaErrorCode.OFFSET_NOT_AVAILABLE,
  KafkaErrorCode.FENCED_INSTANCE_ID,
  KafkaErrorCode.GROUP_SUBSCRIBED_TO_TOPIC,
  KafkaErrorCode.UNSTABLE_OFFSET_COMMIT,
  KafkaErrorCode.THROTTLING_QUOTA_EXCEEDED,
]);

function kafkaError(code: number, context: string, detail?: string | null): KafkaError {
  const label = detail
    ? `${context}: ${kafkaErrorName(code)} (${detail})`
    : `${context}: ${kafkaErrorName(code)}`;
  return new KafkaError(code, label, {
    retriable: retriableErrors.has(code),
    fatal:
      code === KafkaErrorCode.SASL_AUTHENTICATION_FAILED ||
      code === KafkaErrorCode.ILLEGAL_SASL_STATE,
  });
}

function address(host: string, port: number): string {
  const formattedHost = host.includes(":") ? `[${host}]` : host;
  return `${formattedHost}:${port}`;
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
}

function retryDelay(options: Required<RetryOptions>, attempt: number): number {
  const base = Math.min(options.maxBackoffMs, options.initialBackoffMs * 2 ** attempt);
  return Math.round(base * (RETRY_JITTER_BASE + Math.random()));
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
