import type { ConsumedMessage } from "../types.ts";
import { KafkaJSNonRetriableError } from "./errors.ts";
import type { Partitioner, PartitionerContext, ProducerMessage } from "../bun/producer/index.ts";
import { isFunction, isNumber } from "../type-guards.ts";

export interface KafkaJsMessage {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number;
  headers?: Record<string, Buffer | string | number | undefined>;
  timestamp?: string;
}

export interface KafkaJsSendRecord {
  topic: string;
  messages: KafkaJsMessage[];
  acks?: -1 | 0 | 1;
  timeout?: number;
  compression?: number;
}

export interface KafkaJsSendBatchRecord {
  acks?: -1 | 0 | 1;
  timeout?: number;
  compression?: number;
  topicMessages: Array<{ topic: string; messages: KafkaJsMessage[] }>;
}

export type KafkaJsConsumedMessage = {
  key: Buffer | null;
  value: Buffer | null;
  timestamp: string;
  attributes: number;
  offset: string;
  size: number;
  topic: string;
  partition: number;
  headers?: Record<string, Buffer | undefined>;
  leaderEpoch?: number | null;
};

export type KafkaJsPartitioner =
  | ((topic: string, count: number, key: Uint8Array | null) => number)
  | { partition: (context: PartitionerContext) => number };

export function toBunPartitioner(
  partitioner: KafkaJsPartitioner | null | undefined,
): Partitioner | undefined {
  if (!partitioner) {
    return undefined;
  }
  if (isFunction(partitioner)) {
    return ({ topic, partitionCount, key }) => partitioner(topic, partitionCount, key);
  }
  if ("partition" in partitioner) {
    return (context) => partitioner.partition(context);
  }
  return undefined;
}

export function toWireMessage(message: KafkaJsMessage): ProducerMessage {
  if (message.value === undefined) {
    throw new KafkaJSNonRetriableError("Invalid message value");
  }
  const wire: ProducerMessage = {
    value: message.value,
    key: message.key ?? null,
    headers: Object.fromEntries(
      Object.entries(message.headers ?? {}).map(([key, value]) => [
        key,
        isNumber(value) ? String(value) : (value ?? null),
      ]),
    ),
  };
  if (message.partition !== undefined) {
    wire.partition = message.partition;
  }
  if (message.timestamp !== undefined) {
    wire.timestamp = Number(message.timestamp);
  }
  return wire;
}

function toBuffer(value: Uint8Array | null | undefined): Buffer | null {
  return value === null || value === undefined ? null : Buffer.from(value);
}

export function toKafkajsMessage(raw: ConsumedMessage): KafkaJsConsumedMessage {
  const size =
    (raw.key?.byteLength ?? 0) +
    (raw.value?.byteLength ?? 0) +
    Object.values(raw.headers ?? {}).reduce((sum, header) => sum + (header?.byteLength ?? 0), 0);
  const headers: Record<string, Buffer | undefined> = {};
  for (const [name, value] of Object.entries(raw.headers ?? {})) {
    headers[name] = value === null || value === undefined ? undefined : Buffer.from(value);
  }
  return {
    key: toBuffer(raw.key),
    value: toBuffer(raw.value),
    timestamp: String(raw.timestamp),
    attributes: 0,
    offset: String(raw.offset),
    size,
    topic: raw.topic,
    partition: raw.partition,
    headers,
    leaderEpoch: null,
  };
}
