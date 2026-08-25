import { Cluster } from "../bun/cluster.ts";
import { KafkaJSNonRetriableError } from "./errors.ts";
import { hasStringValue, isFunction, isString } from "../type-guards.ts";
import type { CompatOptions } from "./types.ts";
export interface KafkaConfig {
  clientId?: string;
  brokers: string[] | (() => string[] | Promise<string[]>);
  ssl?: boolean | Bun.TLSOptions;
  sasl?: CompatOptions;
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
  tls?: Bun.TLSOptions;
  sasl?: CompatOptions;
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
  const configuredToken = saslConfig?.oauthBearerToken ?? saslConfig?.token;
  let token = isString(configuredToken) ? configuredToken : undefined;
  const provider = saslConfig?.oauthBearerProvider;
  if (!token && isFunction(provider)) {
    // kafkajs providers resolve to { value: token }; accept both shapes.
    token = async () => {
      const resolved = await provider();
      return isString(resolved) ? resolved : hasStringValue(resolved) ? (resolved.value ?? "") : "";
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
// SAFETY: the surrounding protocol invariant validates this representation.
    tls: (config.ssl === true ? {} : config.ssl || undefined) as never,
// SAFETY: the surrounding protocol invariant validates this representation.
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
// SAFETY: the surrounding protocol invariant validates this representation.
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
        isFunction(this.config.brokers)
          ? await this.config.brokers()
          : this.config.brokers;
// SAFETY: the surrounding protocol invariant validates this representation.
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
        isFunction(this.config.brokers)
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
