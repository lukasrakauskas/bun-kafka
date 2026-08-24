import { getDriver } from "./native/index.ts";
import type { NativeAdmin } from "./native/types.ts";
import type { ClientOptions, ClusterMetadata, KafkaConfig } from "./types.ts";
import type { KafkaError } from "./errors.ts";

export class Admin {
  #n: NativeAdmin;
  #closed = false;

  constructor(config: KafkaConfig = {}, options: ClientOptions = {}) {
    this.#n = getDriver().admin(config, options);
  }

  metadata(opts: { allTopics?: boolean; timeoutMs?: number } = {}): ClusterMetadata {
    this.#e();
    return this.#n.metadata(opts.allTopics !== false, opts.timeoutMs ?? 10_000);
  }

  clusterId(timeoutMs = 5000): string | null {
    this.#e();
    return this.#n.clusterId(timeoutMs);
  }

  fatalError(): KafkaError | null {
    if (this.#closed) return null;
    return this.#n.fatalError();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#n.close();
  }

  #e() {
    if (this.#closed) throw new Error("Admin is closed");
    const f = this.#n.fatalError();
    if (f) throw f;
  }
}
