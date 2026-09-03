import { Cluster } from "../bun/cluster.ts";
import {
  DEFAULT_KAFKAJS_CONNECT_TIMEOUT_MS,
  DEFAULT_KAFKAJS_INITIAL_BACKOFF_MS,
  DEFAULT_KAFKAJS_MAX_BACKOFF_MS,
  DEFAULT_KAFKAJS_MAX_RETRIES,
  DEFAULT_REQUEST_TIMEOUT_MS,
} from "../bun/shared.ts";
import type { BunKafkaSasl } from "../bun/connection/index.ts";
import { KafkaJSNonRetriableError } from "./errors.ts";
import { hasStringValue, isFunction, isString } from "../type-guards.ts";
import type { CompatOptions } from "./types.ts";
export interface KafkaConfig {
  clientId?: string;
  rackId?: string;
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
  rackId?: string;
  tls?: Bun.TLSOptions;
  sasl?: BunKafkaSasl;
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  retry: { maxRetries: number; initialBackoffMs: number; maxBackoffMs: number };
}

function validateSaslMechanism(mechanism: CompatOptions["mechanism"]): void {
  if (
    mechanism !== undefined &&
    (!isString(mechanism) ||
      !["plain", "scram-sha-256", "scram-sha-512", "oauthbearer"].includes(mechanism))
  ) {
    throw new KafkaJSNonRetriableError(
      `SASL mechanism ${String(mechanism)} is not supported by bun-kafka`,
    );
  }
}

function resolveSaslToken(
  saslConfig: CompatOptions | undefined,
): string | (() => Promise<string>) | undefined {
  const configured = saslConfig?.oauthBearerToken ?? saslConfig?.token;
  if (isString(configured)) {
    return configured;
  }
  if (!isFunction(saslConfig?.oauthBearerProvider)) {
    return undefined;
  }
  const provider = saslConfig.oauthBearerProvider;
  return async () => {
    const resolved = await provider();
    if (isString(resolved)) {
      return resolved;
    }
    return hasStringValue(resolved) ? (resolved.value ?? "") : "";
  };
}

function createBasicSasl(
  mechanism: "plain" | "scram-sha-256" | "scram-sha-512",
  config: CompatOptions,
): BunKafkaSasl {
  const username = isString(config.username) ? config.username : undefined;
  const password = isString(config.password) ? config.password : undefined;
  if (username === undefined || password === undefined) {
    throw new KafkaJSNonRetriableError(`${mechanism} SASL requires username and password`);
  }
  return { mechanism, username, password };
}

function createSaslCredentials(
  mechanism: CompatOptions["mechanism"],
  config: CompatOptions,
  token: string | (() => Promise<string>) | undefined,
): BunKafkaSasl | undefined {
  if (mechanism === "plain" || mechanism === "scram-sha-256" || mechanism === "scram-sha-512") {
    return createBasicSasl(mechanism, config);
  }
  return mechanism === "oauthbearer" && token ? { mechanism, token } : undefined;
}

function mapSaslConfig(saslConfig: CompatOptions | undefined): BunKafkaSasl | undefined {
  if (saslConfig === undefined) {
    return undefined;
  }
  const mechanism = saslConfig.mechanism;
  validateSaslMechanism(mechanism);
  return createSaslCredentials(mechanism, saslConfig, resolveSaslToken(saslConfig));
}

export function mapConfig(
  config: KafkaConfig,
  brokers: string[],
): MappedConfig & { brokers: string[] } {
  return {
    brokers,
    clientId: config.clientId ?? "kafkajs",
    rackId: config.rackId,
    tls: config.ssl === true ? {} : config.ssl || undefined,
    sasl: mapSaslConfig(config.sasl),
    requestTimeoutMs: config.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT_MS,
    connectTimeoutMs: Math.max(
      config.connectionTimeout ?? DEFAULT_KAFKAJS_CONNECT_TIMEOUT_MS,
      DEFAULT_KAFKAJS_CONNECT_TIMEOUT_MS,
    ),
    retry: {
      maxRetries: config.retry?.retries ?? DEFAULT_KAFKAJS_MAX_RETRIES,
      initialBackoffMs: config.retry?.initialRetryTime ?? DEFAULT_KAFKAJS_INITIAL_BACKOFF_MS,
      maxBackoffMs: config.retry?.maxRetryTime ?? DEFAULT_KAFKAJS_MAX_BACKOFF_MS,
    },
  };
}

/** @confluentinc/kafka-javascript nests real options under `kafkaJS`; accept both shapes. */
export function unwrapKafkaJs<T>(options: (T & { kafkaJS?: T }) | undefined): T {
  if (options === undefined) {
    throw new TypeError("KafkaJS options are required");
  }
  return options.kafkaJS ?? options;
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
      const brokers = isFunction(this.config.brokers)
        ? await this.config.brokers()
        : this.config.brokers;
      return mapConfig(this.config, brokers);
    })();
    return this.#mapped;
  }

  async ready(): Promise<Cluster> {
    await this.resolve();
    return this.sync();
  }

  sync(): Cluster {
    if (!this.#cluster) {
      const mapped = isFunction(this.config.brokers)
        ? undefined
        : mapConfig(this.config, this.config.brokers);
      if (!mapped) {
        throw new KafkaJSNonRetriableError(
          "Broker list is resolving asynchronously; await an async method first",
        );
      }
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
