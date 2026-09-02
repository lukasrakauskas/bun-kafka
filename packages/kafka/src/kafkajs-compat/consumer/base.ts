import { Admin } from "../../bun/admin.ts";
import { Consumer } from "../../consumer/index.ts";
import { CONSUMER_EVENTS } from "../constants.ts";
import type { ClusterGetter } from "../config.ts";
import { wrapError } from "../errors.ts";
import { Emitter, Logger } from "../logger.ts";
import type { CompatOptions, LogFields } from "../types.ts";
import { createCompatConsumerOptions } from "./helpers.ts";

export class CompatConsumerBase {
  events = CONSUMER_EVENTS;
  protected emitter = new Emitter();
  protected consumer?: Consumer;
  protected running = false;
  protected stopping?: Promise<void>;
  protected pausedPartitions = new Set<string>();
  protected subscribedTopics = new Set<string | RegExp>();
  protected pendingOffsets = new Map<
    string,
    { topic: string; partition: number; offset: bigint }
  >();
  protected uncommittedCount = 0;

  constructor(
    protected getter: () => ClusterGetter,
    protected log: Logger,
    protected options: CompatOptions,
  ) {}

  on(event: string, listener: (event: LogFields) => void): () => void {
    return this.emitter.on(event, listener);
  }

  logger(): Logger {
    return this.log;
  }

  protected underlying(): Consumer {
    return (this.consumer ??= new Consumer(
      this.getter().acquire(),
      createCompatConsumerOptions(this.options, ({ type, ...event }) =>
        this.emitter.emit(
          type === "rebalancing" ? CONSUMER_EVENTS.REBALANCING : CONSUMER_EVENTS.GROUP_JOIN,
          event,
        ),
      ),
      this.getter().release,
    ));
  }

  async connect(): Promise<void> {
    await this.getter().ready();
    this.underlying();
    this.emitter.emit(CONSUMER_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    await this.stop().catch(() => {});
    await this.consumer?.close().catch(() => {});
    this.consumer = undefined;
    this.emitter.emit(CONSUMER_EVENTS.DISCONNECT);
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
      let incoming: Array<string | RegExp>;
      if (topic !== undefined) {
        incoming = [topic];
      } else if (topics === undefined) {
        incoming = [];
      } else if (Array.isArray(topics)) {
        incoming = topics;
      } else {
        incoming = [topics];
      }
      // kafkajs accumulates subscriptions across calls into one group subscription.
      const merged = [...this.subscribedTopics];
      for (const entry of incoming) {
        if (!merged.some((existing) => existing.toString() === entry.toString())) {
          merged.push(entry);
        }
      }
      this.subscribedTopics = new Set(merged);
      await this.underlying().subscribe({ topics: merged, fromBeginning });
    } catch (error) {
      throw wrapError(error);
    }
  }

  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }
    this.running = false;
    this.emitter.emit(CONSUMER_EVENTS.STOP);
    await this.stopping;
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
      const consumer = this.underlying();
      const entries: Array<{ topic: string; partition: number; offset: bigint }> = [];
      for (const { topic, partition, offset } of topicPartitions) {
        const key = `${topic}\u0000${partition}`;
        const resolved =
          offset !== undefined
            ? BigInt(offset)
            : (this.pendingOffsets.get(key)?.offset ?? consumer.position(topic, partition) ?? -1n);
        this.pendingOffsets.delete(key);
        if (resolved >= 0n) {
          entries.push({ topic, partition, offset: resolved });
        }
      }
      if (!entries.length) {
        return;
      }
      await consumer.commitOffsets(entries);
      this.emitter.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, {
        groupId: String(this.options.groupId ?? ""),
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
    this.underlying().seek({ topic, partition, offset: BigInt(offset) });
  }

  /** Core expands topic-only entries; compat mirrors the result for paused(). */
  pause(
    topicPartitions: Array<{ topic: string; partitions?: number[] }>,
  ): Array<{ topic: string; partitions: number[] }> {
    const resolved = this.resolvePartitions(topicPartitions);
    for (const target of resolved) {
      this.pausedPartitions.add(`${target.topic}\u0000${target.partition}`);
    }
    this.underlying().pause(resolved);
    return this.paused();
  }

  resume(
    topicPartitions: Array<{ topic: string; partitions?: number[] }>,
  ): Array<{ topic: string; partitions: number[] }> {
    const resolved = this.resolvePartitions(topicPartitions);
    for (const target of resolved) {
      this.pausedPartitions.delete(`${target.topic}\u0000${target.partition}`);
    }
    this.underlying().resume(resolved);
    return this.paused();
  }

  protected resolvePartitions(
    topicPartitions: Array<{ topic: string; partitions?: number[] }>,
  ): Array<{ topic: string; partition: number }> {
    const assigned = this.underlying().assignment();
    return topicPartitions.flatMap(({ topic, partitions }) => {
      const forTopic =
        partitions ??
        assigned.filter((entry) => entry.topic === topic).map((entry) => entry.partition);
      return forTopic.map((partition) => ({ topic, partition }));
    });
  }

  paused(): Array<{ topic: string; partitions: number[] }> {
    const grouped = new Map<string, number[]>();
    for (const key of this.pausedPartitions) {
      const separator = key.indexOf("\u0000");
      const topic = key.slice(0, separator);
      grouped.set(topic, [...(grouped.get(topic) ?? []), Number(key.slice(separator + 1))]);
    }
    return [...grouped].map(([topic, partitions]) => ({ topic, partitions }));
  }

  async describeGroup(): Promise<CompatOptions> {
    try {
      const admin = new Admin(this.getter().acquire(), this.getter().release);
      try {
        const [group] = await admin.describeGroups([String(this.options.groupId)]);
        return group
          ? {
              errorCode: group.error,
              errorMessage: group.message,
              groupId: group.groupId,
              state: group.state,
              protocolType: group.protocolType,
              protocolData: group.protocol,
              members: group.members.map((member) => ({
                memberId: member.memberId,
                clientId: member.clientId,
                clientHost: member.clientHost,
                memberMetadata: member.memberMetadata,
                memberAssignment: member.memberAssignment,
              })),
            }
          : {};
      } finally {
        await admin.close();
      }
    } catch (error) {
      throw wrapError(error);
    }
  }

  assignments(): Array<{
    topic: string;
    partitions: Array<{ partition: number; offset: string }>;
  }> {
    const grouped = new Map<string, Array<{ partition: number; offset: string }>>();
    for (const { topic, partition, offset } of this.underlying().assignment()) {
      grouped.set(topic, [
        ...(grouped.get(topic) ?? []),
        { partition, offset: offset?.toString() ?? "" },
      ]);
    }
    return [...grouped].map(([topic, partitions]) => ({ topic, partitions }));
  }
}
