import { getDriver } from "./native/index.ts";
import type { NativeProducer } from "./native/types.ts";
import type { KafkaConfig, ProduceInput } from "./types.ts";

export class Producer {
  #n: NativeProducer;
  #closed = false;

  constructor(config: KafkaConfig = {}) {
    this.#n = getDriver().producer(config);
  }

  send(msg: ProduceInput): void {
    this.#e();
    this.#n.send(msg);
  }

  sendBatch(msgs: ProduceInput[]): void {
    for (const m of msgs) this.send(m);
  }

  poll(timeoutMs = 0): number {
    this.#e();
    return this.#n.poll(timeoutMs);
  }

  async flush(timeoutMs = 10_000): Promise<void> {
    this.#e();
    await Promise.resolve();
    this.#n.flush(timeoutMs);
  }

  outQueueLength(): number {
    this.#e();
    return this.#n.outQueueLength();
  }

  async close(timeoutMs = 10_000): Promise<void> {
    if (this.#closed) return;
    try {
      await this.flush(timeoutMs);
    } finally {
      this.#closed = true;
      this.#n.close();
    }
  }

  #e() {
    if (this.#closed) throw new Error("Producer is closed");
  }
}
