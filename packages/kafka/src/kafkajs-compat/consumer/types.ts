import type { KafkaJsConsumedMessage } from "../messages.ts";
import type { CompatOptions } from "../types.ts";

export interface CompatEachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaJsConsumedMessage;
  heartbeat: () => Promise<void>;
  pause: () => void;
}

export interface CompatEachBatchPayload {
  batch: {
    topic: string;
    partition: number;
    highWatermark: string;
    messages: KafkaJsConsumedMessage[];
    isEmpty(): boolean;
    firstOffset(): string | null;
    lastOffset(): string | null;
    offsetLag(): string;
    isStale(): boolean;
    resolveOffset(offset: string | number | bigint): void;
    commitOffsetsIfNecessary(options?: CompatOptions): Promise<void>;
    heartbeat(): Promise<void>;
  };
  heartbeat: () => Promise<void>;
  pause: () => void;
  isRunning: () => boolean;
  isStale: () => boolean;
}

export interface RunOptions {
  eachMessage?: (payload: CompatEachMessagePayload) => Promise<void>;
  eachBatch?: (payload: CompatEachBatchPayload) => Promise<void>;
  autoCommit?: boolean;
  autoCommitInterval?: number | null;
  autoCommitThreshold?: number | null;
  eachBatchAutoResolve?: boolean;
  partitionsConsumedConcurrently?: number;
  beforeCommit?: (
    offsets: Array<{ topic: string; partition: number; offset: string }>,
  ) => Promise<boolean | undefined> | boolean | undefined;
  afterCommit?: (
    offsets: Array<{ topic: string; partition: number; offset: string }>,
  ) => Promise<void> | void;
  onCrash?: (error: Error) => void;
}
