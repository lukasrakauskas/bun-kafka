import { Cluster } from "../bun/cluster.ts";
import { KafkaJSNonRetriableError } from "./errors.ts";

export interface KafkaConfig {
  clientId?: string;
  brokers: string[] | (() => string[] | Promise<string[]>);
  ssl?: boolean | Record<string, unknown>;
  sasl?: Record<string, any>;
  retry?: {
    maxRetryTime?: number;
    initialRetryTime?: number;
    factor?: number;
    multiplier?: number;
    retries?: number;
  };
  logLevel?: number;
  logCreator?: (entry: import("./logger.ts").LoggerEntry) => void;
  connectionTimeout?: number;
  requestTimeout?: number;
  enforceRequestOrder?: boolean;
  maxInFlightRequests?: number;
}

export interface MappedConfig {
  clientId: string;
  tls?: Record<string, unknown>;
  sasl?: Record<string, any>;
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  retry: { maxRetries: number; initialBackoffMs: number; maxBackoffMs: number };
}

export function mapConfig(
  config: KafkaConfig,
  brokers: string[],
): ConstructorParameters<typeof Cluster>[0] {
  const saslConfig = config.sasl;
  if (
    saslConfig?.mechanism !== undefined &&
    !["plain", "scram-sha-256", "scram-sha-512", "oauthbearer"].includes(
      String(saslConfig.mechanism),
    )
  ) {
    throw new KafkaJSNonRetriableError(
      `SASL mechanism ${saslConfig.mechanism} is not supported by bun-kafka`,
    );
  }
  let token: unknown = saslConfig?.oauthBearerToken ?? saslConfig?.token;
  if (!token && typeof saslConfig?.oauthBearerProvider === "function") {
    // kafkajs providers resolve to { value: token }; accept both shapes.
    token = async () => {
      const resolved = await (saslConfig.oauthBearerProvider as () => unknown)();
      return typeof resolved === "string" ? resolved : (resolved as { value?: string }).value;
    };
  }
  const sasl = saslConfig
    ? {
        mechanism: saslConfig.mechanism,
        username: saslConfig.username,
        password: saslConfig.password,
        token,
      }
    : undefined;
  return {
    brokers,
    clientId: config.clientId ?? "kafkajs",
    tls: (config.ssl === true ? {} : config.ssl || undefined) as never,
    sasl: sasl as never,
    requestTimeoutMs: config.requestTimeout ?? 30_000,
    connectTimeoutMs: Math.max(config.connectionTimeout ?? 1_000, 1_000),
    retry: {
      maxRetries: config.retry?.retries ?? 5,
      initialBackoffMs: config.retry?.initialRetryTime ?? 300,
      maxBackoffMs: config.retry?.maxRetryTime ?? 30_000,
    },
  };
}

/** @confluentinc/kafka-javascript nests real options under `kafkaJS`; accept both shapes. */
export function unwrapKafkaJs<T>(options: (T & { kafkaJS?: T }) | undefined): T {
  return ((options as { kafkaJS?: T })?.kafkaJS ?? options) as T;
}

export type ClusterGetter = {
  ready: () => Promise<Cluster>;
  sync: () => Cluster;
  acquire: () => Cluster;
  release: () => void;
};

/**
 * One shared wire-level cluster per kafkajs `Kafka` instance with reference
 * counting: closing every derived client tears the cluster down so processes
 * can exit, mirroring per-client connection pools in kafkajs.
 */
export class ClusterHub {
  #mapped?: Promise<MappedConfig & { brokers: string[] }>;
  #cluster?: Cluster;
  #refs = 0;

  constructor(private config: KafkaConfig) {}

  resolve(): Promise<MappedConfig & { brokers: string[] }> {
    this.#mapped ??= (async () => {
      const brokers =
        typeof this.config.brokers === "function"
          ? await this.config.brokers()
          : this.config.brokers;
      return { ...(mapConfig(this.config, brokers) as MappedConfig), brokers };
    })();
    return this.#mapped;
  }

  async ready(): Promise<Cluster> {
    await this.resolve();
    return this.sync();
  }

  sync(): Cluster {
    if (!this.#cluster) {
      const mapped =
        typeof this.config.brokers === "function"
          ? undefined
          : mapConfig(this.config, this.config.brokers);
      if (!mapped)
        throw new KafkaJSNonRetriableError(
          "Broker list is resolving asynchronously; await an async method first",
        );
      this.#cluster = new Cluster(mapped);
    }
    return this.#cluster;
  }

  /** Acquire a reference for one derived client; `release` runs on its close. */
  acquire(): Cluster {
    this.#refs++;
    return this.sync();
  }

  release(): void {
    this.#refs = Math.max(0, this.#refs - 1);
    if (this.#refs === 0 && this.#cluster) {
      this.#cluster.close();
      this.#cluster = undefined;
    }
  }
}
