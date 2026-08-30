import { Consumer } from "../../consumer/index.ts";
import type { ConsumedMessage } from "../../types.ts";
import { CONSUMER_EVENTS } from "../constants.ts";
import { toKafkajsMessage, type KafkaJsConsumedMessage } from "../messages.ts";
import { CompatConsumerBase } from "./base.ts";
import { highestOffset } from "./helpers.ts";
import type { CompatEachBatchPayload, RunOptions } from "./types.ts";

export class CompatConsumerBatch extends CompatConsumerBase {
  protected async processMessages(
    topic: string,
    partition: number,
    items: ConsumedMessage[],
    consumer: Consumer,
    options: RunOptions,
    heartbeat: () => Promise<void>,
    pause: () => void,
  ): Promise<void> {
    for (const raw of items) {
      if (!this.running) {
        return;
      }
      this.track(topic, partition, raw.offset + 1n);
      await options.eachMessage({
        topic,
        partition,
        message: toKafkajsMessage(raw),
        heartbeat,
        pause,
      });
      this.uncommittedCount++;
      if (options.autoCommitThreshold && this.uncommittedCount >= options.autoCommitThreshold) {
        await this.flushCommits(consumer, options);
      }
    }
  }

  protected async runBatch(
    topic: string,
    partition: number,
    items: ConsumedMessage[],
    consumer: Consumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
    heartbeat: () => Promise<void>,
    pause: () => void,
  ): Promise<void> {
    const highWatermark = await this.highWatermark(topic, partition, items, consumer);
    const messages = items.map(toKafkajsMessage);
    const resolved = new Set<string>();
    const batch = this.createBatch(
      topic,
      partition,
      highWatermark,
      messages,
      resolved,
      consumer,
      options,
      autoCommitEnabled,
      heartbeat,
    );
    await options.eachBatch({
      batch,
      heartbeat,
      pause,
      isRunning: () => this.running,
      isStale: () => !this.running,
    });
    await this.finishBatch(topic, partition, items, resolved, consumer, options, autoCommitEnabled);
  }

  protected async highWatermark(
    topic: string,
    partition: number,
    items: ConsumedMessage[],
    consumer: Consumer,
  ): Promise<string> {
    try {
      return String((await consumer.watermarks(topic, partition)).high);
    } catch {
      return String(items[items.length - 1].offset + 1n);
    }
  }

  protected createBatch(
    topic: string,
    partition: number,
    highWatermark: string,
    messages: KafkaJsConsumedMessage[],
    resolved: Set<string>,
    consumer: Consumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
    heartbeat: () => Promise<void>,
  ): CompatEachBatchPayload["batch"] {
    return {
      topic,
      partition,
      highWatermark,
      messages,
      isEmpty: () => messages.length === 0,
      firstOffset: () => (messages.length ? messages[0].offset : null),
      lastOffset: () => (messages.length ? messages[messages.length - 1].offset : null),
      offsetLag: () =>
        (BigInt(highWatermark) - BigInt(messages[messages.length - 1].offset) - 1n).toString(),
      isStale: () => !this.running,
      resolveOffset: (offset) => resolved.add(BigInt(offset).toString()),
      commitOffsetsIfNecessary: async () => {
        if (autoCommitEnabled) {
          await this.flushCommits(consumer, options);
        }
      },
      heartbeat,
    };
  }

  protected async finishBatch(
    topic: string,
    partition: number,
    items: ConsumedMessage[],
    resolved: Set<string>,
    consumer: Consumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
  ): Promise<void> {
    if (options.eachBatchAutoResolve !== false) {
      for (const raw of items) {
        resolved.add(raw.offset.toString());
      }
    }
    const first = items[0];
    const last = items[items.length - 1];
    const nextOffset = resolved.size ? highestOffset(resolved) + 1n : first.offset;
    if (nextOffset <= last.offset) {
      consumer.seek({ topic, partition, offset: nextOffset });
      return;
    }
    this.track(topic, partition, nextOffset);
    this.uncommittedCount++;
    if (
      autoCommitEnabled &&
      options.autoCommitThreshold &&
      this.uncommittedCount >= options.autoCommitThreshold
    ) {
      await this.flushCommits(consumer, options);
    }
  }

  protected track(topic: string, partition: number, offset: bigint): void {
    this.pendingOffsets.set(`${topic}\u0000${partition}`, { topic, partition, offset });
  }

  protected async flushCommits(consumer: Consumer, options: RunOptions): Promise<void> {
    if (!this.pendingOffsets.size) {
      return;
    }
    const entries = [...this.pendingOffsets.values()];
    const serializable = entries.map((entry) => ({
      topic: entry.topic,
      partition: entry.partition,
      offset: entry.offset.toString(),
    }));
    if (options.beforeCommit && (await options.beforeCommit(serializable))) {
      this.pendingOffsets.clear();
      return;
    }
    await consumer.commitOffsets(entries);
    this.pendingOffsets.clear();
    this.uncommittedCount = 0;
    this.emitter.emit(CONSUMER_EVENTS.COMMIT_OFFSETS, {
      groupId: this.options.groupId,
      topics: serializable.map((entry) => entry.topic),
    });
    await options.afterCommit?.(serializable);
  }
}
