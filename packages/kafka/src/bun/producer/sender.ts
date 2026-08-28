import { KafkaErrorCode } from "../../errors.ts";
import {
  asBytes,
  murmur2,
  readProduceResponse,
  writeProduceRequest,
  type RequestBody,
} from "../../protocol/index.ts";
import { isString, requiredValue } from "../../type-guards.ts";
import type { Cluster } from "../cluster.ts";
import {
  API_PRODUCE,
  DEFAULT_PRODUCE_TIMEOUT_MS,
  INT32_MAX,
  METADATA_REFRESH_SLEEP_MS,
  PRODUCE_API_VERSION,
  SEQ_WRAP,
  kafkaError,
  partitionKey,
  type TopicMetadata,
} from "../shared.ts";
import { topicMetadataReady, topicMetadataRetryable } from "./metadata.ts";
import { withRetry } from "./retry.ts";
import type { ProducerTransaction } from "./transaction.ts";
import type {
  Partitioner,
  PartitionRecords,
  PendingSend,
  ProduceResult,
  ProducerMessage,
  ProducerOptions,
  ProducerSend,
  ResolvedProducerOptions,
} from "./types.ts";

function appendProducerMessage(
  partitions: Map<number, PartitionRecords>,
  metadata: Map<number, TopicMetadata["partitions"][number]>,
  topic: string,
  message: ProducerMessage,
  key: Uint8Array | null,
  partition: number,
): void {
  const meta = metadata.get(partition);
  if (!meta) {
    throw new RangeError(`Partition ${partition} does not exist on ${topic}`);
  }
  if (meta.err) {
    throw kafkaError(meta.err, `${topic}[${partition}]`);
  }
  let group = partitions.get(partition);
  if (!group) {
    group = { topic, partition, leader: meta.leader, records: [], messages: [] };
    partitions.set(partition, group);
  }
  group.records.push(key && isString(message.key) ? { ...message, key } : message);
  group.messages.push(message);
}

function chooseProducerPartition(
  topic: string,
  message: ProducerMessage,
  partitionCount: number,
  partitioner: Partitioner | undefined,
  roundRobin: Map<string, number>,
  key: Uint8Array | null,
): number {
  if (message.partition !== undefined) {
    return message.partition;
  }
  if (partitioner) {
    const partition = partitioner({ topic, partitionCount, key });
    if (!Number.isInteger(partition) || partition < 0 || partition >= partitionCount) {
      throw new RangeError(
        `Custom partitioner returned invalid partition ${partition} for ${topic}`,
      );
    }
    return partition;
  }
  if (key) {
    return (murmur2(key) & INT32_MAX) % partitionCount;
  }
  const partition = roundRobin.get(topic) ?? 0;
  roundRobin.set(topic, (partition + 1) % partitionCount);
  return partition;
}

export class ProducerSender {
  constructor(
    private readonly cluster: Cluster,
    private readonly options: ResolvedProducerOptions,
    private readonly transaction: ProducerTransaction,
    private readonly roundRobin: Map<string, number>,
    private readonly sequences: Map<string, number>,
    private readonly partitioner?: Partitioner,
  ) {}

  async producePendingGroup(group: PendingSend[]): Promise<{
    results: ProduceResult[];
    routedPartitions: PartitionRecords[];
  }> {
    const first = requiredValue(group[0], "Cannot produce an empty group").input;
    const topics = Map.groupBy(group, ({ input }) => input.topic);
    const compression = first.compression ?? this.options.compression;
    return withRetry(
      this.cluster.retryOptions,
      (attempt) => this.#producePendingAttempt(topics, first, compression, attempt),
      (attempt, error, delay) => {
        this.cluster.bumpRetries();
        this.cluster.log(
          "warn",
          `retrying produce attempt ${attempt + 1} in ${delay}ms: ${String(error)}`,
        );
        this.cluster.event({
          type: "retry",
          apiKey: API_PRODUCE,
          attempt: attempt + 1,
          delayMs: delay,
          error,
        });
      },
    );
  }

  async #producePendingAttempt(
    topics: Map<string, PendingSend[]>,
    first: ProducerSend,
    compression: ProducerOptions["compression"],
    attempt: number,
  ): Promise<{ results: ProduceResult[]; routedPartitions: PartitionRecords[] }> {
    const routedPartitions = await this.#routePendingTopics(topics, attempt);
    await this.transaction.addPartitions(routedPartitions);
    const results =
      (first.acks ?? 1) === 0
        ? await this.#fireAndForget(
            routedPartitions,
            first.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS,
            compression,
          )
        : await this.#produce(
            routedPartitions,
            this.options.idempotent || this.transaction.isTransactional || first.acks === "all"
              ? -1
              : 1,
            first.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS,
            compression,
          );
    return { results, routedPartitions };
  }

  async #routePendingTopics(
    topics: Map<string, PendingSend[]>,
    attempt: number,
  ): Promise<PartitionRecords[]> {
    return (
      await Promise.all(
        [...topics].map(async ([topic, sends]) => {
          const firstSend = requiredValue(sends[0], `Cannot route empty topic group ${topic}`);
          return this.#route(
            topic,
            sends.flatMap(({ input }) => input.messages),
            firstSend.input.timeoutMs ?? DEFAULT_PRODUCE_TIMEOUT_MS,
            attempt > 0,
          );
        }),
      )
    ).flat();
  }

  async #fireAndForget(
    partitions: PartitionRecords[],
    timeoutMs: number,
    compression: ProducerOptions["compression"],
  ): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    await Promise.all(
      [...leaders].map(([leader, leaderPartitions]) =>
        this.cluster.fireAndForget(
          leader,
          API_PRODUCE,
          PRODUCE_API_VERSION,
          this.#produceRequestBody(leaderPartitions, 0, timeoutMs, compression),
        ),
      ),
    );
    return partitions.map((group) => ({
      topic: group.topic,
      partition: group.partition,
      baseOffset: -1n,
      logAppendTime: -1n,
    }));
  }

  async #route(
    topic: string,
    messages: readonly ProducerMessage[],
    timeoutMs: number,
    refresh = false,
  ): Promise<PartitionRecords[]> {
    const metadata = await this.#topicMetadata(topic, timeoutMs, refresh);
    const partitionMetadata = new Map(
      metadata.partitions.map((partition) => [partition.id, partition]),
    );
    const partitions = new Map<number, PartitionRecords>();
    for (const message of messages) {
      const key = asBytes(message.key);
      const partition = chooseProducerPartition(
        topic,
        message,
        metadata.partitions.length,
        this.partitioner,
        this.roundRobin,
        key,
      );
      appendProducerMessage(partitions, partitionMetadata, topic, message, key, partition);
    }
    return [...partitions.values()];
  }

  async #topicMetadata(topic: string, timeoutMs: number, refresh: boolean): Promise<TopicMetadata> {
    let metadata: TopicMetadata | undefined;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      metadata = await this.cluster.topic(topic, refresh || Boolean(metadata));
      if (topicMetadataReady(metadata)) {
        return metadata;
      }
      if (!topicMetadataRetryable(metadata)) {
        throw kafkaError(metadata.err, topic);
      }
      await Bun.sleep(METADATA_REFRESH_SLEEP_MS);
    }
    if (!metadata || !topicMetadataReady(metadata)) {
      throw kafkaError(metadata?.err ?? KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION, topic);
    }
    return metadata;
  }

  #produceRequestBody(
    leaderPartitions: PartitionRecords[],
    acks: number,
    timeoutMs: number,
    compression = this.options.compression,
  ): RequestBody {
    return writeProduceRequest(
      this.transaction.transactionalId ?? null,
      acks,
      timeoutMs,
      leaderPartitions.map((value) => {
        const key = partitionKey(value.topic, value.partition);
        const producerIdentity = this.transaction.producer;
        const producer = producerIdentity && {
          ...producerIdentity,
          sequence: this.sequences.get(key) ?? 0,
          transactional: this.transaction.isOpen,
        };
        return { ...value, producer };
      }),
      compression ?? this.options.compression ?? "none",
    );
  }

  async #produce(
    partitions: PartitionRecords[],
    acks: number,
    timeoutMs: number,
    compression?: ProducerOptions["compression"],
  ): Promise<ProduceResult[]> {
    const leaders = Map.groupBy(partitions, (partition) => partition.leader);
    const responses = await Promise.all(
      [...leaders].map(async ([leader, leaderPartitions]) => {
        const body = this.#produceRequestBody(leaderPartitions, acks, timeoutMs, compression);
        const response = await this.cluster.request(
          leader,
          API_PRODUCE,
          PRODUCE_API_VERSION,
          body,
          timeoutMs,
          false,
        );
        const produce = readProduceResponse(response);
        const results = produce.results.map(({ error, ...result }) => {
          if (error) {
            throw kafkaError(error, `${result.topic}[${result.partition}]`);
          }
          return result;
        });
        this.cluster.throttle(API_PRODUCE, produce.throttleMs);
        return results;
      }),
    );
    const results = responses.flat();
    if (this.transaction.producer) {
      for (const partition of partitions) {
        const key = partitionKey(partition.topic, partition.partition);
        this.sequences.set(
          key,
          ((this.sequences.get(key) ?? 0) + partition.records.length) % SEQ_WRAP,
        );
      }
    }
    return results;
  }
}
