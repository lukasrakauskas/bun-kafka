import { getDriver } from "./native/index.ts";
import type { NativeConsumer } from "./native/types.ts";
import type {
  ConsumerRunOptions, KafkaConfig, KafkaMessage, TopicPartition, Watermarks,
} from "./types.ts";

export class Consumer implements AsyncIterable<KafkaMessage> {
  #n: NativeConsumer;
  #closed = false;
  #defaults: ConsumerRunOptions;

  constructor(config: KafkaConfig = {}, defaults: ConsumerRunOptions = {}) {
    this.#n = getDriver().consumer(config);
    this.#defaults = defaults;
  }

  subscribe(topics: string | string[]): void {
    this.#e();
    this.#n.subscribe(Array.isArray(topics) ? topics : [topics]);
  }

  unsubscribe(): void {
    this.#e();
    this.#n.unsubscribe();
  }

  subscription(): TopicPartition[] {
    this.#e();
    return this.#n.subscription();
  }

  assign(partitions: TopicPartition[]): void {
    this.#e();
    this.#n.assign(partitions);
  }

  assignment(): TopicPartition[] {
    this.#e();
    return this.#n.assignment();
  }

  assignmentLost(): boolean {
    return this.#n.assignmentLost();
  }

  rebalanceProtocol(): string {
    return this.#n.rebalanceProtocol();
  }

  poll(timeoutMs = 1000): KafkaMessage | null {
    this.#e();
    return this.#n.poll(timeoutMs);
  }

  /** Drain a batch of ready messages (one blocking wait, then non-blocking). */
  pollBatch(timeoutMs = 1000, max = 64): KafkaMessage[] {
    this.#e();
    return this.#n.pollBatch(timeoutMs, max);
  }

  messages(opts: ConsumerRunOptions = {}): AsyncGenerator<KafkaMessage, void, unknown> {
    const o = { ...this.#defaults, ...opts };
    const timeoutMs = o.timeoutMs ?? 100;
    const batchSize = o.batchSize ?? 64;
    const self = this;
    return (async function* () {
      let pending: KafkaMessage[] = [];
      let i = 0;
      while (!self.#closed) {
        if (i >= pending.length) {
          i = 0;
          pending = [];
          try {
            pending = self.#n.pollBatch(timeoutMs, batchSize);
            if (!pending.length) {
              await Promise.resolve();
              continue;
            }
          } catch (e) {
            if (o.throwOnError === false) {
              await Promise.resolve();
              continue;
            }
            throw e;
          }
        }
        const msg = pending[i++]!;
        try {
          if (o.eachMessageCommit) self.commitMessage(msg);
          yield msg;
        } finally {
          msg.done();
        }
      }
    })();
  }

  [Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    return this.messages();
  }

  commit(offsets?: TopicPartition[] | null, async = false): void {
    this.#e();
    this.#n.commit(offsets ?? null, async);
  }

  commitMessage(msg: KafkaMessage, async = false): void {
    this.commit([{ topic: msg.topic, partition: msg.partition, offset: msg.offset + 1n }], async);
  }

  committed(partitions: TopicPartition[], timeoutMs = 5000): TopicPartition[] {
    this.#e();
    return this.#n.committed(partitions, timeoutMs);
  }

  position(partitions: TopicPartition[]): TopicPartition[] {
    this.#e();
    return this.#n.position(partitions);
  }

  seek(partitions: TopicPartition[], timeoutMs = 1000): void {
    this.#e();
    this.#n.seek(partitions, timeoutMs);
  }

  pause(partitions: TopicPartition[]): void {
    this.#e();
    this.#n.pause(partitions);
  }

  resume(partitions: TopicPartition[]): void {
    this.#e();
    this.#n.resume(partitions);
  }

  storeOffsets(offsets: TopicPartition[]): void {
    this.#e();
    this.#n.storeOffsets(offsets);
  }

  queryWatermarkOffsets(topic: string, partition: number, timeoutMs = 5000): Watermarks {
    this.#e();
    return this.#n.queryWatermarkOffsets(topic, partition, timeoutMs);
  }

  getWatermarkOffsets(topic: string, partition: number): Watermarks {
    this.#e();
    return this.#n.getWatermarkOffsets(topic, partition);
  }

  offsetsForTimes(queries: TopicPartition[], timeoutMs = 5000): TopicPartition[] {
    this.#e();
    return this.#n.offsetsForTimes(queries, timeoutMs);
  }

  memberId(): string | null {
    this.#e();
    return this.#n.memberId();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#n.close();
  }

  #e() {
    if (this.#closed) throw new Error("Consumer is closed");
  }
}
