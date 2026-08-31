import { INT32_MAX } from "../bun/shared.ts";
import { asBytes, murmur2 } from "../protocol/index.ts";
import type { KafkaJsPartitioner } from "./messages.ts";

export const COMPRESSION_NAMES = ["none", "gzip", "snappy", "lz4", "zstd"] as const;

export const CompressionTypes = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
} as const;

/** Registry kept for kafkajs API parity; bun-kafka ships all five codecs natively. */
export const CompressionCodecs: Record<number, never> = {};

/** KafkaJS's default partitioner uses Java-compatible Murmur2 key affinity. */
const javaCompatiblePartitioner: KafkaJsPartitioner = () => {
  const counters = new Map<string, number>();
  return ({ topic, partitionMetadata, message }) => {
    if (message.partition !== undefined) {
      return message.partition;
    }
    const key = asBytes(message.key);
    if (key) {
      return (murmur2(key) & INT32_MAX) % partitionMetadata.length;
    }
    const counter = counters.get(topic) ?? 0;
    counters.set(topic, counter + 1);
    const partition = partitionMetadata[counter % partitionMetadata.length];
    if (!partition) {
      throw new RangeError(`No partitions available for ${topic}`);
    }
    return partition.partitionId;
  };
};

export const Partitioners = {
  DefaultPartitioner: javaCompatiblePartitioner,
  JavaCompatiblePartitioner: javaCompatiblePartitioner,
  LegacyPartitioner: javaCompatiblePartitioner,
};

export const logLevel = { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 3, DEBUG: 4 } as const;

export const ConfigResourceTypes = {
  UNKNOWN: 0,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER_BROKER: 4,
  BROKER_LOGGER: 8,
  BROKER: 4,
} as const;

export const AclResourceTypes = {
  UNKNOWN: 0,
  ANY: 1,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER: 4,
  TRANSACTIONAL_ID: 5,
  DELEGATION_TOKEN: 6,
} as const;

export const AclOperationTypes = {
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12,
} as const;

export const AclPermissionTypes = { UNKNOWN: 0, ANY: 1, DENY: 2, ALLOW: 3 } as const;

export const ResourcePatternTypes = {
  UNKNOWN: 0,
  ANY: 1,
  MATCH: 2,
  LITERAL: 3,
  PREFIXED: 4,
} as const;

export const CONSUMER_EVENTS = {
  CONNECT: "consumer.connect",
  DISCONNECT: "consumer.disconnect",
  STOP: "consumer.stop",
  CRASH: "consumer.crash",
  REBALANCING: "consumer.rebalancing",
  GROUP_JOIN: "consumer.group_join",
  HEARTBEAT: "consumer.heartbeat",
  FETCH: "consumer.fetch",
  FETCH_START: "consumer.fetch_start",
  START_BATCH_PROCESS: "consumer.start_batch_process",
  END_BATCH_PROCESS: "consumer.end_batch_process",
  COMMIT_OFFSETS: "consumer.commit_offsets",
} as const;

export const PRODUCER_EVENTS = {
  CONNECT: "producer.connect",
  DISCONNECT: "producer.disconnect",
  REQUEST: "producer.network.request",
  REQUEST_TIMEOUT: "producer.network.request_timeout",
  REQUEST_QUEUE_SIZE: "producer.network.request_queue_size",
} as const;

export const ADMIN_EVENTS = {
  CONNECT: "admin.connect",
  DISCONNECT: "admin.disconnect",
  REQUEST: "admin.network.request",
  REQUEST_TIMEOUT: "admin.network.request_timeout",
} as const;

export const GLOBAL_EVENTS = { LOG: "global.log" } as const;
