import { Cluster } from "../bun/cluster.ts";
import { Admin } from "../bun/admin.ts";
import { Consumer } from "../consumer/index.ts";
import { Producer } from "../bun/producer/index.ts";
import { ClusterHub, type ClusterGetter, unwrapKafkaJs, type KafkaConfig } from "./config.ts";
import { Logger } from "./logger.ts";
import { logLevel } from "./constants.ts";
import { CompatProducer } from "./producer.ts";
import { CompatConsumer } from "./consumer.ts";
import { CompatAdmin } from "./admin.ts";
import type { CompatOptions } from "./types.ts";

type KafkaJsOptions = CompatOptions;

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

  producer(options: KafkaJsOptions = {}): CompatProducer {
    return new CompatProducer(
      () => this.#getter(),
      this.#logger.namespace("producer"),
      unwrapKafkaJs(options),
    );
  }

  consumer(options: KafkaJsOptions = {}): CompatConsumer {
    return new CompatConsumer(
      () => this.#getter(),
      this.#logger.namespace("consumer"),
      unwrapKafkaJs(options),
    );
  }

  admin(_options: KafkaJsOptions = {}): CompatAdmin {
    return new CompatAdmin(() => this.#getter(), this.#logger.namespace("admin"));
  }

  logger(namespace?: string): Logger {
    return namespace ? this.#logger.namespace(namespace) : this.#logger;
  }
}

export { Producer, Consumer, Admin, Cluster };
