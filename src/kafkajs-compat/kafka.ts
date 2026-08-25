import { Cluster } from "../bun/cluster.ts";
import { BunAdmin } from "../bun/admin.ts";
import { BunConsumer } from "../bun/consumer.ts";
import { BunProducer } from "../bun/producer.ts";
import { ClusterHub, type ClusterGetter } from "./config.ts";
import { Logger } from "./logger.ts";
import { logLevel } from "./constants.ts";
import { unwrapKafkaJs, type KafkaConfig } from "./config.ts";
import { CompatProducer } from "./producer.ts";
import { CompatConsumer } from "./consumer.ts";
import { CompatAdmin } from "./admin.ts";

export class Kafka {
  #logger: Logger;
  #hub: ClusterHub;

  constructor(config: KafkaConfig) {
    const unwrapped = unwrapKafkaJs(config);
    this.#hub = new ClusterHub(unwrapped);
    this.#logger = new Logger(
      unwrapped.logLevel ?? logLevel.NOTHING,
      unwrapped.clientId ?? "kafkajs",
      unwrapped.logCreator,
    );
  }

  #getter(): ClusterGetter {
    return {
      ready: () => this.#hub.ready(),
      sync: () => this.#hub.sync(),
      acquire: () => this.#hub.acquire(),
      release: () => this.#hub.release(),
    };
  }

  producer(options: Record<string, any> = {}): CompatProducer {
    return new CompatProducer(
      () => this.#getter(),
      this.#logger.namespace("producer"),
      unwrapKafkaJs(options),
    );
  }

  consumer(options: Record<string, any> = {}): CompatConsumer {
    return new CompatConsumer(
      () => this.#getter(),
      this.#logger.namespace("consumer"),
      unwrapKafkaJs(options),
    );
  }

  admin(_options: Record<string, any> = {}): CompatAdmin {
    return new CompatAdmin(() => this.#getter(), this.#logger.namespace("admin"));
  }

  logger(namespace?: string): Logger {
    return namespace ? this.#logger.namespace(namespace) : this.#logger;
  }
}

export { BunProducer, BunConsumer, BunAdmin, Cluster };
