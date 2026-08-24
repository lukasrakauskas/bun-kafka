export type Bytes = string | Uint8Array | ArrayBuffer | null | undefined;

export interface TopicPartition {
  topic: string;
  partition: number;
  offset?: number | bigint;
}

export type MessageHeaders = Record<string, Bytes>;

export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: bigint;
  key: Uint8Array | null;
  value: Uint8Array | null;
  timestamp: bigint;
  timestampType: number;
  headers: Record<string, Uint8Array | null>;
  brokerId: number;
  done(): void;
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
