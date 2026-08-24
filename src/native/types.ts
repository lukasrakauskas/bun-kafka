import type {
  ClusterMetadata,
  KafkaConfig,
  KafkaMessage,
  ProduceInput,
  TopicPartition,
  Watermarks,
} from "../types.ts";

/** Low-level librdkafka backend. Core talks only to this. */
export interface NativeDriver {
  readonly kind: "ffi" | "napi";
  version(): { number: number; string: string };
  err2str(code: number): string;
  producer(config: KafkaConfig): NativeProducer;
  consumer(config: KafkaConfig): NativeConsumer;
  admin(config: KafkaConfig): NativeAdmin;
}

export interface NativeProducer {
  send(msg: ProduceInput): void;
  poll(timeoutMs: number): number;
  flush(timeoutMs: number): void;
  outQueueLength(): number;
  close(): void;
}

export interface NativeConsumer {
  subscribe(topics: string[]): void;
  unsubscribe(): void;
  subscription(): TopicPartition[];
  assign(partitions: TopicPartition[]): void;
  assignment(): TopicPartition[];
  assignmentLost(): boolean;
  rebalanceProtocol(): string;
  /** One message, null on idle/timeout/eof. */
  poll(timeoutMs: number): KafkaMessage | null;
  /** Drain up to max messages with a single blocking wait, then non-blocking. */
  pollBatch(timeoutMs: number, max: number): KafkaMessage[];
  commit(offsets: TopicPartition[] | null, async: boolean): void;
  committed(partitions: TopicPartition[], timeoutMs: number): TopicPartition[];
  position(partitions: TopicPartition[]): TopicPartition[];
  seek(partitions: TopicPartition[], timeoutMs: number): void;
  pause(partitions: TopicPartition[]): void;
  resume(partitions: TopicPartition[]): void;
  storeOffsets(offsets: TopicPartition[]): void;
  queryWatermarkOffsets(topic: string, partition: number, timeoutMs: number): Watermarks;
  getWatermarkOffsets(topic: string, partition: number): Watermarks;
  offsetsForTimes(queries: TopicPartition[], timeoutMs: number): TopicPartition[];
  memberId(): string | null;
  close(): void;
}

export interface NativeAdmin {
  metadata(allTopics: boolean, timeoutMs: number): ClusterMetadata;
  clusterId(timeoutMs: number): string | null;
  close(): void;
}
