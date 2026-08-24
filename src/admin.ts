import { getDriver } from "./native/index.ts";
import type { NativeAdmin } from "./native/types.ts";
import type { ClusterMetadata, KafkaConfig } from "./types.ts";

export class Admin {
  #n: NativeAdmin;
  #closed = false;

  constructor(config: KafkaConfig = {}) {
    this.#n = getDriver().admin(config);
  }

  metadata(opts: { allTopics?: boolean; timeoutMs?: number } = {}): ClusterMetadata {
    this.#e();
    return this.#n.metadata(opts.allTopics !== false, opts.timeoutMs ?? 10_000);
  }

  clusterId(timeoutMs = 5000): string | null {
    this.#e();
    return this.#n.clusterId(timeoutMs);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#n.close();
  }

  #e() {
    if (this.#closed) throw new Error("Admin is closed");
  }
}
