export type KafkaConfig = Record<string, string | number | boolean>;

export type Bytes = string | Uint8Array | ArrayBuffer | null | undefined;

export interface TopicPartition {
  topic: string;
  partition: number;
  offset?: number | bigint;
  err?: number;
}

export type MessageHeaders = Record<string, Bytes>;

export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: bigint;
  /** Valid until done() — zero-copy view into librdkafka memory on FFI. */
  key: Uint8Array | null;
  /** Valid until done() — zero-copy view into librdkafka memory on FFI. */
  value: Uint8Array | null;
  timestamp: bigint;
  timestampType: number;
  headers: Record<string, Uint8Array | null>;
  brokerId: number;
  /** Release native message resources (idempotent). */
  done(): void;
}

export interface ProduceInput {
  topic: string;
  value?: Bytes;
  key?: Bytes;
  partition?: number;
  timestamp?: number | bigint;
  headers?: MessageHeaders;
}

export interface ConsumerRunOptions {
  timeoutMs?: number;
  /** Max messages to drain per native wait (default 64). */
  batchSize?: number;
  eachMessageCommit?: boolean;
  stopOnEof?: boolean;
  throwOnError?: boolean;
}

export interface Watermarks {
  low: bigint;
  high: bigint;
}

export interface MetadataBroker {
  id: number;
  host: string;
  port: number;
}

export interface MetadataPartition {
  id: number;
  err: number;
  leader: number;
}

export interface MetadataTopic {
  name: string;
  err: number;
  partitions: MetadataPartition[];
}

export interface ClusterMetadata {
  brokers: MetadataBroker[];
  topics: MetadataTopic[];
}

export type NativeKind = "ffi" | "napi";
