/**
 * kafkajs-compatible API surface implemented over the bun-kafka primitives.
 * Exposed as `bun-kafka/kafkajs-compat` so existing kafkajs applications can
 * switch transports by changing their import specifier.
 */
export * from "./constants.ts";
export * from "./errors.ts";
export { Logger, type LoggerEntry } from "./logger.ts";
export {
  ClusterHub,
  unwrapKafkaJs,
  mapConfig,
  type KafkaConfig,
  type MappedConfig,
  type ClusterGetter,
} from "./config.ts";
export {
  toBunPartitioner,
  toWireMessage,
  toKafkajsMessage,
  type KafkaJsMessage,
  type KafkaJsSendRecord,
  type KafkaJsSendBatchRecord,
  type KafkaJsConsumedMessage,
} from "./messages.ts";
export { Kafka } from "./kafka.ts";
export { CompatProducer, CompatTransaction } from "./producer.ts";
export {
  CompatConsumer,
  type RunOptions,
  type CompatEachMessagePayload,
  type CompatEachBatchPayload,
} from "./consumer.ts";
export { CompatAdmin, type CompatCreateTopicsInput } from "./admin.ts";

/** Namespace alias for code importing the @confluentinc/kafka-javascript style. */
import { Kafka } from "./kafka.ts";
import {
  Partitioners,
  CompressionTypes,
  logLevel,
  ConfigResourceTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
} from "./constants.ts";

export const KafkaJS = {
  Kafka,
  Partitioners,
  CompressionTypes,
  logLevel,
  ConfigResourceTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
};
