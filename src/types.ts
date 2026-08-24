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
  /**
   * Key bytes. Bun-native messages own this copy. The legacy FFI backend can use
   * a zero-copy view that is valid only until `done()`.
   */
  key: Uint8Array | null;
  /**
   * Value bytes. Bun-native messages own this copy. The legacy FFI backend can use
   * a zero-copy view that is valid only until `done()`.
   */
  value: Uint8Array | null;
  timestamp: bigint;
  timestampType: number;
  headers: Record<string, Uint8Array | null>;
  brokerId: number;
  /** Release backend message resources (idempotent; a no-op for Bun-native messages). */
  done(): void;
}

export interface ProduceInput {
  topic: string;
  value?: Bytes;
  key?: Bytes;
  partition?: number;
  timestamp?: number | bigint;
  headers?: MessageHeaders;
  /** Opaque token returned in delivery reports (string/number only). */
  opaque?: string | number;
}

export interface DeliveryReport {
  topic: string;
  partition: number;
  offset: bigint;
  errorCode: number;
  errorMessage: string | null;
  opaque?: string | number;
}

export interface ConsumerRunOptions {
  timeoutMs?: number;
  /** Max messages to drain per native wait (default 64; batches() default 512). */
  batchSize?: number;
  eachMessageCommit?: boolean;
  /** After each yielded batch, commit the last message per partition (batches() only). */
  eachBatchCommit?: boolean;
  stopOnEof?: boolean;
  throwOnError?: boolean;
  /**
   * Copy key/value out of librdkafka memory (default true).
   * Set false on FFI for zero-copy; then buffers are invalid after done().
   */
  copy?: boolean;
}

export interface ClientOptions {
  onDelivery?: (report: DeliveryReport) => void;
  onError?: (err: import("./errors.ts").KafkaError) => void;
  onLog?: (level: number, facility: string, message: string) => void;
  onRebalance?: (event: {
    kind: "assign" | "revoke" | "error";
    partitions: TopicPartition[];
    error?: import("./errors.ts").KafkaError;
  }) => void;
  /**
   * Consumer only. If true (default), install librdkafka rebalance_cb that
   * assign/unassigns automatically (eager + cooperative). Set false only if you
   * fully handle assignment yourself.
   */
  autoRebalance?: boolean;
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
