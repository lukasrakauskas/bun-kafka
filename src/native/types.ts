import type {
  ClusterMetadata,
  DeliveryReport,
  KafkaConfig,
  KafkaMessage,
  ProduceInput,
  TopicPartition,
  Watermarks,
} from "../types.ts";
import type { KafkaError } from "../errors.ts";

export type ClientEventHandlers = {
  onDelivery?: (report: DeliveryReport) => void;
  onError?: (err: KafkaError) => void;
  onLog?: (level: number, facility: string, message: string) => void;
  onRebalance?: (event: {
    kind: "assign" | "revoke" | "error";
    partitions: TopicPartition[];
    error?: KafkaError;
  }) => void;
  /** Default true: auto assign/revoke in rebalance_cb. */
  autoRebalance?: boolean;
};

/** Low-level librdkafka backend. Core talks only to this. */
export interface NativeDriver {
  readonly kind: "ffi" | "napi";
  version(): { number: number; string: string };
  err2str(code: number): string;
  producer(config: KafkaConfig, handlers?: ClientEventHandlers): NativeProducer;
  consumer(config: KafkaConfig, handlers?: ClientEventHandlers): NativeConsumer;
  admin(config: KafkaConfig, handlers?: ClientEventHandlers): NativeAdmin;
}

export interface NativeProducer {
  send(msg: ProduceInput): void;
  poll(timeoutMs: number): number;
  flush(timeoutMs: number): void;
  outQueueLength(): number;
  fatalError(): KafkaError | null;
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
  poll(timeoutMs: number): KafkaMessage | null;
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
  fatalError(): KafkaError | null;
  close(): void;
}

export interface NativeAdmin {
  metadata(allTopics: boolean, timeoutMs: number): ClusterMetadata;
  clusterId(timeoutMs: number): string | null;
  fatalError(): KafkaError | null;
  close(): void;
}
