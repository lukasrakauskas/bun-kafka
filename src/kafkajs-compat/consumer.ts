import { BunAdmin } from "../bun/admin.ts";
import { BunConsumer } from "../bun/consumer.ts";
import type { ConsumedMessage } from "../types.ts";
import { CONSUMER_EVENTS } from "./constants.ts";
import { wrapError, KafkaJSError, KafkaJSNonRetriableError } from "./errors.ts";
import type { ClusterGetter } from "./config.ts";
import { Emitter, Logger } from "./logger.ts";
import { toKafkajsMessage, type KafkaJsConsumedMessage } from "./messages.ts";

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
    commitOffsetsIfNecessary(options?: unknown): Promise<void>;
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

export class CompatConsumer {
  events = CONSUMER_EVENTS;
  #getter: () => ClusterGetter;
  #logger: Logger;
  #options: Record<string, any>;
  #emitter = new Emitter();
  #consumer?: BunConsumer;
  #running = false;
  #stopping?: Promise<void>;
  #paused = new Set<string>();
  #subscribedTopics = new Set<string | RegExp>();
  #pendingOffsets = new Map<string, { topic: string; partition: number; offset: bigint }>();
  #uncommittedCount = 0;

  constructor(getter: () => ClusterGetter, logger: Logger, options: Record<string, any>) {
    this.#getter = getter;
    this.#logger = logger;
    this.#options = options;
  }

  on(event: string, listener: (event: Record<string, unknown>) => void): () => void {
    return this.#emitter.on(event, listener);
  }

  logger(): Logger {
    return this.#logger;
  }

  #underlying(): BunConsumer {
    if (!this.#consumer) {
      const o = this.#options;
      const assignors = Array.isArray(o.partitionAssignors) ? o.partitionAssignors : [];
      const cooperative = assignors.some(
        (assignor) => (assignor as { name?: string }).name === "CooperativeStickyAssignor",
      );
      const consumerOptions = {
        groupId: o.groupId as string,
        sessionTimeoutMs: o.sessionTimeout as number | undefined,
        rebalanceTimeoutMs: o.rebalanceTimeout as number | undefined,
        heartbeatIntervalMs: o.heartbeatInterval as number | undefined,
        fromBeginning: o.fromBeginning as boolean | undefined,
        isolationLevel: o.isolationLevel as "read_uncommitted" | "read_committed" | undefined,
        groupInstanceId: o.groupInstanceId as string | undefined,
        partitionAssigner: cooperative ? ("cooperative-sticky" as const) : undefined,
      };
      this.#consumer = new BunConsumer(
        this.#getter().acquire(),
        consumerOptions,
        this.#getter().release,
      );
    }
    return this.#consumer!;
  }

  async connect(): Promise<void> {
    await this.#getter().ready();
    this.#underlying();
    this.#emitter.emit(CONSUMER_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    await this.stop().catch(() => {});
    await this.#consumer?.close().catch(() => {});
    this.#consumer = undefined;
    this.#emitter.emit(CONSUMER_EVENTS.DISCONNECT);
  }

  async subscribe({
    topic,
    topics,
    fromBeginning,
  }: {
    topic?: string | RegExp;
    topics?: string | RegExp | Array<string | RegExp>;
    fromBeginning?: boolean;
  }): Promise<void> {
    try {
      const incoming =
        topic !== undefined
          ? [topic]
          : topics !== undefined
            ? Array.isArray(topics)
              ? topics
              : [topics]
            : [];
      // kafkajs accumulates subscriptions across calls into one group subscription.
      const merged = [...this.#subscribedTopics];
      for (const entry of incoming) {
        if (!merged.some((existing) => existing.toString() === entry.toString()))
          merged.push(entry);
      }
      this.#subscribedTopics = new Set(merged);
      await this.#underlying().subscribe({ topics: merged as never, fromBeginning });
      this.#emitter.emit(CONSUMER_EVENTS.GROUP_JOIN, { groupId: this.#options.groupId });
    } catch (error) {
      throw wrapError(error);
    }
  }

  async run(options: RunOptions): Promise<void> {
    if (this.#running) throw new KafkaJSNonRetriableError("Consumer run() was already called");
    if (options.eachMessage && options.eachBatch) {
      throw new KafkaJSNonRetriableError("Not allowed to configure both eachMessage and eachBatch");
    }
    if (!options.eachMessage && !options.eachBatch) {
      throw new KafkaJSNonRetriableError("Missing one of eachMessage or eachBatch");
    }
    await this.#getter().ready();
    this.#running = true;
    const loop = this.#loop(options).catch(async (error) => {
      this.#running = false;
      const wrapped = error instanceof Error ? wrapError(error) : new KafkaJSError(String(error));
      this.#logger.error(`consumer crashed: ${wrapped.message}`, {
        groupId: this.#options.groupId,
        stack: wrapped.stack,
      });
      this.#emitter.emit(CONSUMER_EVENTS.CRASH, { error: wrapped, groupId: this.#options.groupId });
      await options.onCrash?.(wrapped);
    });
    this.#stopping = loop;
  }

  async #loop(options: RunOptions): Promise<void> {
    const consumer = this.#underlying();
    const autoCommitEnabled = options.autoCommit !== false;
    const concurrent = Math.max(1, options.partitionsConsumedConcurrently ?? 1);
    const commitTimer = options.autoCommitInterval
      ? setInterval(() => void this.#flushCommits(consumer, options), options.autoCommitInterval)
      : undefined;
    commitTimer?.unref?.();
    try {
      while (this.#running) {
        this.#emitter.emit(CONSUMER_EVENTS.FETCH_START);
        const o = this.#options;
        const messages = await consumer.fetch({
          maxMessages: 200,
          // Capped wire wait keeps stop()/pause()/resume() responsive even when
          // applications configure multi-second maxWaitTimeInMs.
          maxWaitMs: Math.min((o.maxWaitTimeInMs as number) ?? 500, 1_000),
          minBytes: o.minBytes as number | undefined,
          maxBytes: o.maxBytes as number | undefined,
          maxPartitionBytes: o.maxBytesPerPartition as number | undefined,
        });
        if (!this.#running) break;
        this.#emitter.emit(CONSUMER_EVENTS.FETCH, { numberOfMessages: messages.length });
        if (!messages.length) continue;
        const groups = this.#groupByPartition(messages);
        for (let i = 0; i < groups.length; i += concurrent) {
          await Promise.all(
            groups
              .slice(i, i + concurrent)
              .map(([key, items]) =>
                this.#processGroup(key, items, consumer, options, autoCommitEnabled),
              ),
          );
          if (!this.#running) break;
        }
        if (autoCommitEnabled && !options.autoCommitInterval && !options.autoCommitThreshold) {
          await this.#flushCommits(consumer, options);
        }
      }
    } finally {
      if (commitTimer) clearInterval(commitTimer);
    }
  }

  #groupByPartition(messages: ConsumedMessage[]): Array<[string, ConsumedMessage[]]> {
    const ordered: Array<[string, ConsumedMessage[]]> = [];
    const index = new Map<string, ConsumedMessage[]>();
    for (const message of messages) {
      const key = `${message.topic}\u0000${message.partition}`;
      const bucket = index.get(key);
      if (bucket) bucket.push(message);
      else {
        const fresh = [message];
        index.set(key, fresh);
        ordered.push([key, fresh]);
      }
    }
    return ordered;
  }

  async #processGroup(
    key: string,
    items: ConsumedMessage[],
    consumer: BunConsumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
  ): Promise<void> {
    const separator = key.indexOf("\u0000");
    const topic = key.slice(0, separator);
    const partition = Number(key.slice(separator + 1));
    this.#emitter.emit(CONSUMER_EVENTS.START_BATCH_PROCESS, {
      topic,
      partition,
      size: items.length,
    });
    const heartbeat = async () => {
      this.#emitter.emit(CONSUMER_EVENTS.HEARTBEAT);
    };
    const pause = () => {
      this.pause([{ topic, partitions: [partition] }]);
    };
    try {
      if (options.eachMessage) {
        for (const raw of items) {
          if (!this.#running) return;
          this.#track(topic, partition, raw.offset + 1n);
          await options.eachMessage({
            topic,
            partition,
            message: toKafkajsMessage(raw),
            heartbeat,
            pause,
          });
          this.#uncommittedCount++;
          if (
            options.autoCommitThreshold &&
            this.#uncommittedCount >= options.autoCommitThreshold
          ) {
            await this.#flushCommits(consumer, options);
          }
        }
      } else if (options.eachBatch) {
        await this.#runBatch(
          topic,
          partition,
          items,
          consumer,
          options,
          autoCommitEnabled,
          heartbeat,
          pause,
        );
      }
    } finally {
      this.#emitter.emit(CONSUMER_EVENTS.END_BATCH_PROCESS, { topic, partition });
    }
  }

  async #runBatch(
    topic: string,
    partition: number,
    items: ConsumedMessage[],
    consumer: BunConsumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
    heartbeat: () => Promise<void>,
    pause: () => void,
  ): Promise<void> {
    let highWatermark = "";
    try {
      const marks = await consumer.watermarks(topic, partition);
      highWatermark = String(marks.high);
    } catch {
      highWatermark = String(items[items.length - 1]!.offset + 1n);
    }
    const messages = items.map(toKafkajsMessage);
    const firstRaw = items[0]!;
    const lastRaw = items[items.length - 1]!;
    const resolved = new Set<string>();
    const batch = {
      topic,
      partition,
      highWatermark,
      messages,
      isEmpty: () => messages.length === 0,
      firstOffset: () => (messages.length ? messages[0]!.offset : null),
      lastOffset: () => (messages.length ? messages[messages.length - 1]!.offset : null),
      offsetLag: () =>
        (BigInt(highWatermark) - BigInt(messages[messages.length - 1]!.offset) - 1n).toString(),
      isStale: () => !this.#running,
      resolveOffset: (offset: string | number | bigint) => {
        resolved.add(BigInt(offset).toString());
      },
      commitOffsetsIfNecessary: async () => {
        if (autoCommitEnabled) await this.#flushCommits(consumer, options);
      },
      heartbeat,
    };
    await options.eachBatch!({
      batch,
      heartbeat,
      pause,
      isRunning: () => this.#running,
      isStale: () => !this.#running,
    });
    const autoResolve = options.eachBatchAutoResolve !== false;
    if (autoResolve) for (const raw of items) resolved.add(raw.offset.toString());
    let nextOffset: bigint;
    if (resolved.size) {
      const highest = [...resolved].map(BigInt).reduce((a, b) => (b > a ? b : a));
      nextOffset = highest + 1n;
    } else {
      nextOffset = firstRaw.offset;
    }
    if (nextOffset <= lastRaw.offset) {
      // Unresolved tail: rewind so the remainder is delivered again.
      consumer.seek({ topic, partition, offset: nextOffset });
    } else {
      this.#track(topic, partition, nextOffset);
      this.#uncommittedCount++;
      if (
        autoCommitEnabled &&
        options.autoCommitThreshold &&
        this.#uncommittedCount >= options.autoCommitThreshold
      ) {
        await this.#flushCommits(consumer, options);
      }
    }
  }

  #track(topic: string, partition: number, offset: bigint): void {
    this.#pendingOffsets.set(`${topic}\u0000${partition}`, { topic, partition, offset });
  }

  async #flushCommits(consumer: BunConsumer, options: RunOptions): Promise<void> {
    if (!this.#pendingOffsets.size) return;
    const entries = [...this.#pendingOffsets.values()];
    const serializable = entries.map((entry) => ({
      topic: entry.topic,
      partition: entry.partition,
      offset: entry.offset.toString(),
    }));
    if (options.beforeCommit && (await options.beforeCommit(serializable))) {
      this.#pendingOffsets.clear();
      return;
    }
    await consumer.commitOffsets(entries);
    this.#pendingOffsets.clear();
    this.#uncommittedCount = 0;
    this.#emitter.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, {
      groupId: this.#options.groupId,
      topics: serializable.map((entry) => entry.topic),
    });
    await options.afterCommit?.(serializable);
  }

  async stop(): Promise<void> {
    if (!this.#running) return;
    this.#running = false;
    this.#emitter.emit(CONSUMER_EVENTS.STOP);
    await this.#stopping;
  }

  async commitOffsets(
    topicPartitions: Array<{
      topic: string;
      partition: number;
      offset?: string | number | bigint;
      metadata?: string;
    }>,
  ): Promise<void> {
    try {
      const consumer = this.#underlying();
      const entries: Array<{ topic: string; partition: number; offset: bigint }> = [];
      for (const { topic, partition, offset } of topicPartitions) {
        const key = `${topic}\u0000${partition}`;
        const resolved =
          offset !== undefined
            ? BigInt(offset)
            : (this.#pendingOffsets.get(key)?.offset ?? consumer.position(topic, partition) ?? -1n);
        this.#pendingOffsets.delete(key);
        if (resolved >= 0n) entries.push({ topic, partition, offset: resolved });
      }
      if (!entries.length) return;
      await consumer.commitOffsets(entries);
      this.#emitter.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, {
        groupId: this.#options.groupId,
        topics: entries.map((entry) => entry.topic),
      });
    } catch (error) {
      throw wrapError(error);
    }
  }

  seek({
    topic,
    partition,
    offset,
  }: {
    topic: string;
    partition: number;
    offset: string | number | bigint;
  }): void {
    this.#underlying().seek({ topic, partition, offset: BigInt(offset) });
  }

  /** Core expands topic-only entries; compat mirrors the result for paused(). */
  pause(
    topicPartitions: Array<{ topic: string; partitions?: number[] }>,
  ): Array<{ topic: string; partitions: number[] }> {
    for (const target of this.#resolvePartitions(topicPartitions))
      this.#paused.add(`${target.topic}\u0000${target.partition}`);
    this.#underlying().pause(topicPartitions);
    return this.paused();
  }

  resume(
    topicPartitions: Array<{ topic: string; partitions?: number[] }>,
  ): Array<{ topic: string; partitions: number[] }> {
    for (const target of this.#resolvePartitions(topicPartitions))
      this.#paused.delete(`${target.topic}\u0000${target.partition}`);
    this.#underlying().resume(topicPartitions);
    return this.paused();
  }

  #resolvePartitions(
    topicPartitions: Array<{ topic: string; partitions?: number[] }>,
  ): Array<{ topic: string; partition: number }> {
    const assigned = this.#underlying().assignment();
    return topicPartitions.flatMap(({ topic, partitions }) => {
      const forTopic =
        partitions ??
        assigned.filter((entry) => entry.topic === topic).map((entry) => entry.partition);
      return forTopic.map((partition) => ({ topic, partition }));
    });
  }

  paused(): Array<{ topic: string; partitions: number[] }> {
    const grouped = new Map<string, number[]>();
    for (const key of this.#paused) {
      const separator = key.indexOf("\u0000");
      const topic = key.slice(0, separator);
      grouped.set(topic, [...(grouped.get(topic) ?? []), Number(key.slice(separator + 1))]);
    }
    return [...grouped].map(([topic, partitions]) => ({ topic, partitions }));
  }

  async describeGroup(): Promise<Record<string, unknown>> {
    try {
      const admin = new BunAdmin(this.#getter().acquire(), this.#getter().release);
      const [group] = await admin.describeGroups([String(this.#options.groupId)]);
      admin.close();
      return group ?? {};
    } catch (error) {
      throw wrapError(error);
    }
  }

  assignments(): Array<{
    topic: string;
    partitions: Array<{ partition: number; offset: string }>;
  }> {
    const grouped = new Map<string, Array<{ partition: number; offset: string }>>();
    for (const { topic, partition, offset } of this.#underlying().assignment()) {
      grouped.set(topic, [
        ...(grouped.get(topic) ?? []),
        { partition, offset: offset?.toString() ?? "" },
      ]);
    }
    return [...grouped].map(([topic, partitions]) => ({ topic, partitions }));
  }
}
