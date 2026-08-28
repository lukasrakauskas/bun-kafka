import { KafkaError } from "../errors.ts";
import type { ClusterMetadata } from "../types.ts";
import { Connection, type ConnectionOptions } from "./connection.ts";
import { Reader, Writer, readMetadataResponse } from "./protocol.ts";
import {
  API_FIND_COORDINATOR,
  API_METADATA,
  address,
  kafkaError,
  retryDelay,
  type HealthReport,
  type KafkaEvent,
  type KafkaOptions,
  type Logger,
  type RetryOptions,
  type ClusterStats,
  type TopicMetadata,
} from "./shared.ts";

function validateClusterTimeouts(
  requestTimeoutMs: number,
  connectTimeoutMs: number,
  maxResponseBytes: number,
  retry: Required<RetryOptions>,
): void {
  if (
    !Number.isSafeInteger(requestTimeoutMs) ||
    requestTimeoutMs <= 0 ||
    !Number.isSafeInteger(connectTimeoutMs) ||
    connectTimeoutMs <= 0 ||
    !Number.isSafeInteger(maxResponseBytes) ||
    maxResponseBytes < 4 ||
    !Number.isSafeInteger(retry.maxRetries) ||
    retry.maxRetries < 0 ||
    !Number.isFinite(retry.initialBackoffMs) ||
    retry.initialBackoffMs < 0 ||
    !Number.isFinite(retry.maxBackoffMs) ||
    retry.maxBackoffMs < retry.initialBackoffMs
  ) {
    throw new RangeError("Invalid Kafka timeout, response size, or retry options");
  }
}

function validateSaslOptions(options: KafkaOptions): void {
  const sasl = options.sasl;
  if (
    sasl &&
    (!new Set(["plain", "scram-sha-256", "scram-sha-512", "oauthbearer"]).has(sasl.mechanism) ||
      (sasl.mechanism === "oauthbearer" ? !sasl.token : !sasl.username || !sasl.password))
  ) {
    throw new TypeError("Invalid Kafka SASL options");
  }
}

function validateStatsInterval(options: KafkaOptions): void {
  if (
    options.statsIntervalMs !== undefined &&
    (!Number.isSafeInteger(options.statsIntervalMs) || options.statsIntervalMs < 1)
  ) {
    throw new RangeError("Invalid Kafka statsIntervalMs");
  }
}

export class Cluster {
  #bootstrap: string[];
  #options: ConnectionOptions;
  #retry: Required<RetryOptions>;
  #onEvent?: (event: KafkaEvent) => void;
  #logger?: Partial<Logger>;
  #retries = 0;
  #throttles = 0;
  #throttleTimeMs = 0;
  #statsTimer?: ReturnType<typeof setInterval>;
  #connections = new Map<string, Connection>();
  #brokers = new Map<number, string>();
  #controller?: number;
  #clusterId?: string | null;
  #topics = new Map<string, TopicMetadata>();

  constructor(options: KafkaOptions) {
    if (!Array.isArray(options.brokers) || !options.brokers.length) {
      throw new TypeError("Kafka requires at least one broker");
    }
    const requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    const connectTimeoutMs = options.connectTimeoutMs ?? 10_000;
    const maxResponseBytes = options.maxResponseBytes ?? 100 * 1024 * 1024;
    const retry = {
      maxRetries: options.retry?.maxRetries ?? 3,
      initialBackoffMs: options.retry?.initialBackoffMs ?? 50,
      maxBackoffMs: options.retry?.maxBackoffMs ?? 2_000,
    };
    validateClusterTimeouts(requestTimeoutMs, connectTimeoutMs, maxResponseBytes, retry);
    validateSaslOptions(options);
    validateStatsInterval(options);
    this.#bootstrap = [...options.brokers];
    this.#retry = retry;
    this.#onEvent = options.onEvent;
    this.#logger = options.logger ?? {};
    this.#options = {
      clientId: options.clientId ?? "bun-kafka",
      requestTimeoutMs,
      connectTimeoutMs,
      maxResponseBytes,
      tls: options.tls,
      sasl: options.sasl,
    };
  }

  #connection(broker: string): Connection {
    let connection = this.#connections.get(broker);
    if (!connection) {
      connection = new Connection(broker, this.#options);
      this.#connections.set(broker, connection);
    }
    return connection;
  }

  async anyRequest(
    apiKey: number,
    apiVersion: number,
    body: Writer,
    flexible = false,
  ): Promise<Reader> {
    return this.#anyRequest(apiKey, apiVersion, body, flexible);
  }

  async #anyRequest(
    apiKey: number,
    apiVersion: number,
    body: Writer,
    flexible = false,
  ): Promise<Reader> {
    let lastError: unknown;
    const candidates = [...new Set([...this.#brokers.values(), ...this.#bootstrap])];
    for (const broker of candidates) {
      try {
        return await this.#connection(broker).request(
          apiKey,
          apiVersion,
          body,
          undefined,
          flexible,
        );
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError ?? new KafkaError(-1, "No Kafka broker is available", { retriable: true });
  }

  async metadata(topics: string[] | null = null): Promise<ClusterMetadata> {
    const body = new Writer().array(topics, (writer, topic) => writer.string(topic));
    const response = readMetadataResponse(await this.#anyRequest(API_METADATA, 2, body));
    for (const broker of response.brokers) {
      this.#brokers.set(broker.id, address(broker.host, broker.port));
    }
    this.#controller = response.controllerId;
    this.#clusterId = response.clusterId;
    for (const topic of response.topics) {
      this.#topics.set(topic.name, topic);
    }
    return { brokers: response.brokers, topics: response.topics, clusterId: response.clusterId };
  }

  async topic(topic: string, refresh = false): Promise<TopicMetadata> {
    if (!refresh) {
      const cached = this.#topics.get(topic);
      if (cached?.partitions.length && !cached.err) {
        return cached;
      }
    }
    const metadata = await this.metadata([topic]);
    return (
      metadata.topics.find((item) => item.name === topic) ?? { name: topic, err: 3, partitions: [] }
    );
  }

  async request(
    brokerId: number,
    apiKey: number,
    apiVersion: number,
    body: Writer,
    timeoutMs?: number,
    retry = true,
    flexible = false,
  ): Promise<Reader> {
    return this.#requestWithRetry(brokerId, apiKey, apiVersion, body, timeoutMs, retry, flexible);
  }

  async #requestWithRetry(
    brokerId: number,
    apiKey: number,
    apiVersion: number,
    body: Writer,
    timeoutMs: number | undefined,
    retry: boolean,
    flexible: boolean,
  ): Promise<Reader> {
    const maxRetries = retry ? this.#retry.maxRetries : 0;
    let lastError: unknown;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await this.#requestOnce(brokerId, apiKey, apiVersion, body, timeoutMs, flexible);
      } catch (error) {
        lastError = error;
        if (!(error instanceof KafkaError && error.retriable) || attempt === maxRetries) {
          throw error;
        }
        await this.#retryRequest(apiKey, attempt, error);
      }
    }
    throw lastError;
  }

  async #requestOnce(
    brokerId: number,
    apiKey: number,
    apiVersion: number,
    body: Writer,
    timeoutMs: number | undefined,
    flexible: boolean,
  ): Promise<Reader> {
    let broker = this.#brokers.get(brokerId);
    if (!broker) {
      await this.metadata();
      broker = this.#brokers.get(brokerId);
    }
    if (!broker) {
      throw new KafkaError(-1, `Kafka broker ${brokerId} is not in metadata`, { retriable: true });
    }
    return this.#connection(broker).request(apiKey, apiVersion, body, timeoutMs, flexible);
  }

  async #retryRequest(apiKey: number, attempt: number, error: KafkaError): Promise<void> {
    this.#retries++;
    const delay = retryDelay(this.#retry, attempt);
    this.log("warn", `retrying ${apiKey} attempt ${attempt + 1} in ${delay}ms: ${String(error)}`);
    this.event({ type: "retry", apiKey, attempt: attempt + 1, delayMs: delay, error });
    if (delay) {
      await Bun.sleep(delay);
    }
  }

  async controllerRequest(apiKey: number, apiVersion: number, body: Writer): Promise<Reader> {
    if (this.#controller === undefined) {
      await this.metadata();
    }
    if (this.#controller === undefined) {
      throw new KafkaError(-1, "Kafka metadata has no controller", { retriable: true });
    }
    return this.request(this.#controller, apiKey, apiVersion, body);
  }

  /**
   * Resolve the transaction coordinator for a transactional id (FindCoordinator
   * v2, key_type=transaction). Brokers create their internal coordinator topic
   * on demand while serving this request, so it must precede InitProducerId.
   */
  async findTxnCoordinator(transactionalId: string): Promise<number> {
    // FindCoordinator v1/v2 wire order: coordinator_key STRING, then
    // coordinator_type INT8 (0 = group, 1 = transaction).
    const response = await this.#anyRequest(
      API_FIND_COORDINATOR,
      2,
      new Writer().string(transactionalId).i8(1),
    );
    const throttleMs = response.i32();
    if (throttleMs > 0) {
      this.throttle(API_FIND_COORDINATOR, throttleMs);
    }
    const error = response.i16();
    const message = response.string();
    if (error) {
      throw kafkaError(
        error,
        message
          ? `Find transaction coordinator ${transactionalId}: ${message}`
          : `Find transaction coordinator ${transactionalId}`,
      );
    }
    const coordinatorId = response.i32();
    response.string(); // host
    response.i32(); // port
    return coordinatorId;
  }

  /** Send a Produce request without waiting for a response (acks=0). */
  async fireAndForget(
    brokerId: number,
    apiKey: number,
    apiVersion: number,
    body: Writer,
  ): Promise<void> {
    let broker = this.#brokers.get(brokerId);
    if (!broker) {
      await this.metadata();
      broker = this.#brokers.get(brokerId);
    }
    if (!broker) {
      throw new KafkaError(-1, `Kafka broker ${brokerId} is not in metadata`, { retriable: true });
    }
    await this.#connection(broker).sendOnly(apiKey, apiVersion, body);
  }

  /** Cluster id reported by Metadata v2+ responses. */
  get clusterId(): string | null | undefined {
    return this.#clusterId;
  }
  get retryOptions(): Required<RetryOptions> {
    return this.#retry;
  }
  get requestTimeoutMs(): number {
    return this.#options.requestTimeoutMs;
  }

  event(event: KafkaEvent): void {
    try {
      this.#onEvent?.(event);
    } catch {
      /* Observability must not break requests. */
    }
  }

  log(level: keyof Logger, message: string): void {
    try {
      this.#logger?.[level]?.(message);
    } catch {
      /* Logging must not break requests. */
    }
  }

  throttle(apiKey: number, durationMs: number): void {
    if (durationMs > 0) {
      this.#throttles++;
      this.#throttleTimeMs += durationMs;
      this.log("debug", `broker throttled ${apiKey} by ${durationMs}ms`);
      this.event({ type: "throttle", apiKey, durationMs });
    }
  }

  /** Aggregate counters across all live broker connections. */
  stats(): ClusterStats {
    let requests = 0;
    let bytesSent = 0;
    let bytesReceived = 0;
    for (const connection of this.#connections.values()) {
      const one = connection.stats;
      requests += one.requests;
      bytesSent += one.bytesSent;
      bytesReceived += one.bytesReceived;
    }
    return {
      connections: this.#connections.size,
      requests: requests + this.#retries,
      bytesSent,
      bytesReceived,
      retries: this.#retries,
      throttles: this.#throttles,
      throttleTimeMs: this.#throttleTimeMs,
    };
  }

  /** Start emitting periodic stats events. */
  trackStats(intervalMs: number): void {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) {
      throw new RangeError("Invalid stats interval");
    }
    this.stopTrackingStats();
    this.#statsTimer = setInterval(
      () => this.event({ type: "stats", stats: this.stats() }),
      intervalMs,
    );
    this.#statsTimer.unref?.();
  }

  stopTrackingStats(): void {
    if (this.#statsTimer) {
      clearInterval(this.#statsTimer);
    }
    this.#statsTimer = undefined;
  }

  /**
   * Ping every known broker with an ApiVersions request and report latency.
   */
  async healthCheck(timeoutMs = 5_000): Promise<HealthReport> {
    const targets = new Map<string, number | undefined>();
    for (const [id, addr] of this.#brokers) {
      targets.set(addr, id);
    }
    for (const addr of this.#bootstrap) {
      if (!targets.has(addr)) {
        targets.set(addr, undefined);
      }
    }
    const checks = await Promise.all(
      [...targets].map(async ([addr, brokerId]) => {
        const startedAt = performance.now();
        try {
          await this.#connection(addr).request(18, 0, new Writer(), timeoutMs);
          return {
            address: addr,
            brokerId,
            ok: true as const,
            latencyMs: Math.round(performance.now() - startedAt),
          };
        } catch (error) {
          this.log("warn", `health check failed for ${addr}: ${String(error)}`);
          return {
            address: addr,
            brokerId,
            ok: false as const,
            latencyMs: Math.round(performance.now() - startedAt),
            error,
          };
        }
      }),
    );
    return { brokers: checks };
  }

  bumpRetries(n = 1): void {
    this.#retries += n;
  }

  close(): void {
    this.stopTrackingStats();
    for (const connection of this.#connections.values()) {
      connection.close();
    }
    this.#connections.clear();
  }
}
