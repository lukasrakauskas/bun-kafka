export const COMPRESSION_NAMES = ["none", "gzip", "snappy", "lz4", "zstd"] as const;

export const CompressionTypes = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
} as const;

/** Registry kept for kafkajs API parity; bun-kafka ships all five codecs natively. */
export const CompressionCodecs: Record<number, unknown> = {};

/** bun-kafka produces Java-compatible murmur2 partitioning natively. */
export const Partitioners = {
  DefaultPartitioner: () => ({ partition: (ctx: { partitionCount: number }) => Math.floor(Math.random() * ctx.partitionCount) }),
  JavaCompatiblePartitioner: () => ({ partition: (ctx: { partitionCount: number }) => Math.floor(Math.random() * ctx.partitionCount) }),
  LegacyPartitioner: () => ({ partition: (ctx: { partitionCount: number }) => Math.floor(Math.random() * ctx.partitionCount) }),
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

export const ResourcePatternTypes = { UNKNOWN: 0, ANY: 1, MATCH: 2, LITERAL: 3, PREFIXED: 4 } as const;

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
