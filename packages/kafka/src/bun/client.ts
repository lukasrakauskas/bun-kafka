import { Cluster } from "./cluster.ts";
import { Admin } from "./admin.ts";
import { Consumer, type ConsumerOptions } from "../consumer/index.ts";
import { Producer, type ProducerOptions } from "./producer/index.ts";
import {
  DEFAULT_HEALTH_CHECK_TIMEOUT_MS,
  type ClusterStats,
  type HealthReport,
  type KafkaOptions,
} from "./shared.ts";

export class Kafka {
  #cluster: Cluster;
  #clients = new Set<{ close(): Promise<void> }>();

  constructor(options: KafkaOptions) {
    this.#cluster = new Cluster({ ...options, brokers: [...options.brokers] });
    if (options.statsIntervalMs !== undefined) {
      this.#cluster.trackStats(options.statsIntervalMs);
    }
  }

  /** Aggregate client counters (requests, bytes, retries, throttles). */
  stats(): ClusterStats {
    return this.#cluster.stats();
  }

  /** Ping all known brokers and report per-broker latency. */
  healthCheck(timeoutMs?: number): Promise<HealthReport> {
    return this.#cluster.healthCheck(
      timeoutMs ?? Math.min(this.#cluster.requestTimeoutMs, DEFAULT_HEALTH_CHECK_TIMEOUT_MS),
    );
  }

  producer(options: ProducerOptions = {}): Producer {
    const producer = new Producer(this.#cluster, options, () => this.#clients.delete(producer));
    this.#clients.add(producer);
    return producer;
  }

  consumer<K = Uint8Array | null, V = Uint8Array | null>(
    options: ConsumerOptions<K, V> = {},
  ): Consumer<K, V> {
    const consumer = new Consumer<K, V>(this.#cluster, options, () =>
      this.#clients.delete(consumer),
    );
    this.#clients.add(consumer);
    return consumer;
  }

  admin(): Admin {
    const admin = new Admin(this.#cluster, () => this.#clients.delete(admin));
    this.#clients.add(admin);
    return admin;
  }

  async disconnect(): Promise<void> {
    await Promise.all([...this.#clients].map((client) => client.close()));
    this.#cluster.close();
  }
}
