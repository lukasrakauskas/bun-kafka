import { DEFAULT_FETCH_MAX_WAIT_MS, DEFAULT_KAFKAJS_MAX_WAIT_MS_CAP } from "../../bun/shared.ts";
import { Consumer } from "../../consumer/index.ts";
import type { ConsumedMessage } from "../../types.ts";
import { CONSUMER_EVENTS } from "../constants.ts";
import { KafkaJSError, KafkaJSNonRetriableError, wrapError } from "../errors.ts";
import { CompatConsumerBatch } from "./batch.ts";
import { numberOption } from "./helpers.ts";
import type { RunOptions } from "./types.ts";

export class CompatConsumerRunner extends CompatConsumerBatch {
  async run(options: RunOptions): Promise<void> {
    if (this.running) {
      throw new KafkaJSNonRetriableError("Consumer run() was already called");
    }
    if (options.eachMessage && options.eachBatch) {
      throw new KafkaJSNonRetriableError("Not allowed to configure both eachMessage and eachBatch");
    }
    if (!options.eachMessage && !options.eachBatch) {
      throw new KafkaJSNonRetriableError("Missing one of eachMessage or eachBatch");
    }
    await this.getter().ready();
    this.running = true;
    const loop = this.loop(options).catch(async (error) => {
      this.running = false;
      const wrapped = error instanceof Error ? wrapError(error) : new KafkaJSError(String(error));
      this.logger.error(`consumer crashed: ${wrapped.message}`, {
        groupId: this.options.groupId,
        stack: wrapped.stack,
      });
      this.emitter.emit(CONSUMER_EVENTS.CRASH, { error: wrapped, groupId: this.options.groupId });
      await options.onCrash?.(wrapped);
    });
    this.stopping = loop;
  }

  protected async loop(options: RunOptions): Promise<void> {
    const consumer = this.underlying();
    const autoCommitEnabled = options.autoCommit !== false;
    const concurrent = Math.max(1, options.partitionsConsumedConcurrently ?? 1);
    const commitTimer = options.autoCommitInterval
      ? setInterval(() => void this.flushCommits(consumer, options), options.autoCommitInterval)
      : undefined;
    commitTimer?.unref?.();
    try {
      while (this.running) {
        await this.loopIteration(consumer, options, autoCommitEnabled, concurrent);
      }
    } finally {
      if (commitTimer) {
        clearInterval(commitTimer);
      }
    }
  }

  protected async loopIteration(
    consumer: Consumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
    concurrent: number,
  ): Promise<void> {
    this.emitter.emit(CONSUMER_EVENTS.FETCH_START);
    const messages = await consumer.fetch({
      maxMessages: 200,
      maxWaitMs: Math.min(
        numberOption(this.options.maxWaitTimeInMs) ?? DEFAULT_FETCH_MAX_WAIT_MS,
        DEFAULT_KAFKAJS_MAX_WAIT_MS_CAP,
      ),
      minBytes: numberOption(this.options.minBytes),
      maxBytes: numberOption(this.options.maxBytes),
      maxPartitionBytes: numberOption(this.options.maxBytesPerPartition),
    });
    if (!this.running) {
      return;
    }
    this.emitter.emit(CONSUMER_EVENTS.FETCH, { numberOfMessages: messages.length });
    if (messages.length) {
      await this.processGroups(messages, consumer, options, autoCommitEnabled, concurrent);
    }
    if (autoCommitEnabled && !options.autoCommitInterval && !options.autoCommitThreshold) {
      await this.flushCommits(consumer, options);
    }
  }

  protected async processGroups(
    messages: ConsumedMessage[],
    consumer: Consumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
    concurrent: number,
  ): Promise<void> {
    const groups = this.groupByPartition(messages);
    for (let i = 0; i < groups.length; i += concurrent) {
      await Promise.all(
        groups
          .slice(i, i + concurrent)
          .map(([key, items]) =>
            this.processGroup(key, items, consumer, options, autoCommitEnabled),
          ),
      );
      if (!this.running) {
        break;
      }
    }
  }

  protected groupByPartition(messages: ConsumedMessage[]): Array<[string, ConsumedMessage[]]> {
    const ordered: Array<[string, ConsumedMessage[]]> = [];
    const index = new Map<string, ConsumedMessage[]>();
    for (const message of messages) {
      const key = `${message.topic}\u0000${message.partition}`;
      const bucket = index.get(key);
      if (bucket) {
        bucket.push(message);
      } else {
        const fresh = [message];
        index.set(key, fresh);
        ordered.push([key, fresh]);
      }
    }
    return ordered;
  }

  protected async processGroup(
    key: string,
    items: ConsumedMessage[],
    consumer: Consumer,
    options: RunOptions,
    autoCommitEnabled: boolean,
  ): Promise<void> {
    const separator = key.indexOf("\u0000");
    const topic = key.slice(0, separator);
    const partition = Number(key.slice(separator + 1));
    this.emitter.emit(CONSUMER_EVENTS.START_BATCH_PROCESS, {
      topic,
      partition,
      size: items.length,
    });
    const heartbeat = async () => this.emitter.emit(CONSUMER_EVENTS.HEARTBEAT);
    const pause = () => this.pause([{ topic, partitions: [partition] }]);
    try {
      if (options.eachMessage) {
        await this.processMessages(topic, partition, items, consumer, options, heartbeat, pause);
      } else if (options.eachBatch) {
        await this.runBatch(
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
      this.emitter.emit(CONSUMER_EVENTS.END_BATCH_PROCESS, { topic, partition });
    }
  }
}
