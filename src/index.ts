export { Producer } from "./producer.ts";
export { Consumer } from "./consumer.ts";
export { Admin } from "./admin.ts";
export { KafkaError } from "./errors.ts";
export { getDriver, setDriver, useNative, ffiDriver } from "./native/index.ts";
export type { NativeDriver, NativeProducer, NativeConsumer, NativeAdmin } from "./native/types.ts";
export type {
  KafkaConfig,
  KafkaMessage,
  ProduceInput,
  TopicPartition,
  ConsumerRunOptions,
  MessageHeaders,
  ClusterMetadata,
  Watermarks,
  NativeKind,
} from "./types.ts";

export const RD_KAFKA_PARTITION_UA = -1;
export const RD_KAFKA_OFFSET_BEGINNING = -2;
export const RD_KAFKA_OFFSET_END = -1;
export const RD_KAFKA_OFFSET_STORED = -1000;
export const RD_KAFKA_OFFSET_INVALID = -1001;
export const RD_KAFKA_RESP_ERR_NO_ERROR = 0;
export const RD_KAFKA_RESP_ERR__PARTITION_EOF = -191;
export const RD_KAFKA_RESP_ERR__TIMED_OUT = -185;

import { getDriver } from "./native/index.ts";

export function version(): { number: number; string: string } {
  return getDriver().version();
}
