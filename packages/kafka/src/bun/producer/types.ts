import type { KafkaError } from "../../errors.ts";
import type { WireRecord } from "../../protocol/index.ts";

export interface ProducerMessage extends WireRecord {
  partition?: number;
  /** Invoked exactly once when the broker acknowledges this message (or it fails for good). */
  onDelivery?: (error: KafkaError | null, result: ProduceResult | null) => void;
}

export type PartitionerContext = {
  topic: string;
  partitionCount: number;
  key: Uint8Array | null;
};

/** Chooses the destination partition when a message has no explicit partition. */
export type Partitioner = (context: PartitionerContext) => number;

export interface ProducerSend {
  topic: string;
  messages: readonly ProducerMessage[];
  acks?: 0 | 1 | "all";
  timeoutMs?: number;
  /** Overrides the producer-level compression for this request. */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
}

export interface ProducerBatch {
  topicMessages: readonly ProducerSend[];
}

export interface ProduceResult {
  topic: string;
  partition: number;
  baseOffset: bigint;
  logAppendTime: bigint;
}

export type PartitionRecords = {
  topic: string;
  partition: number;
  leader: number;
  records: ProducerMessage[];
  messages: ProducerMessage[];
};

export interface ProducerOptions {
  /** Time to collect concurrent sends into one Produce request. Default 5 ms. */
  lingerMs?: number;
  /** Flush immediately at this queued message count. Default 1,000. */
  batchMaxMessages?: number;
  /** Record-batch compression through Bun primitives. */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  /** Use broker sequence numbers to make retries duplicate-safe. */
  idempotent?: boolean;
  /** Custom partition selection for messages without an explicit partition. */
  partitioner?: Partitioner;
  /** Enable transactions with this id; forces all-replica acknowledgements. */
  transactionalId?: string;
  /** Broker-side transaction timeout. Default 60,000 ms. */
  transactionTimeoutMs?: number;
}

export type PendingSend = {
  input: ProducerSend;
  resolve: (results: ProduceResult[]) => void;
  reject: (error: Error) => void;
};

export type ResolvedProducerOptions = Required<
  Omit<ProducerOptions, "partitioner" | "transactionalId" | "transactionTimeoutMs">
>;
