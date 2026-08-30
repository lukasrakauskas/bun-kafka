import { KafkaError } from "../errors.ts";
import type { ClusterMetadata } from "../types.ts";
import { Connection, type ConnectionOptions } from "./connection/index.ts";
import { checkClusterHealth } from "./cluster/health.ts";
import { resolveClusterOptions } from "./cluster/options.ts";
import { ClusterTelemetry } from "./cluster/telemetry.ts";
import {
  RequestBody,
  ResponseBody,
  readFindCoordinatorResponse,
  readMetadataResponse,
  writeEmptyRequest,
  writeFindCoordinatorRequestV1,
  writeMetadataRequest,
} from "../protocol/index.ts";
import {
  API_API_VERSIONS,
  API_FIND_COORDINATOR,
  API_METADATA,
  address,
  kafkaError,
  retryDelay,
  type HealthReport,
  type KafkaEvent,
  type KafkaOptions,
  type RetryOptions,
  type ClusterStats,
  type TopicMetadata,
} from "./shared.ts";

export class Cluster {
  #bootstrap: string[];
  #options: ConnectionOptions;
  #retry: Required<RetryOptions>;
  #telemetry: ClusterTelemetry;
  #connections = new Map<string, Connection>();
  #brokers = new Map<number, string>();
  #controller?: number;
  #clusterId?: string | null;
  #topics = new Map<string, TopicMetadata>();

  constructor(options: KafkaOptions) {
    const resolved = resolveClusterOptions(options);
    this.#bootstrap = resolved.bootstrap;
    this.#retry = resolved.retry;
    this.#options = resolved.connection;
    this.#telemetry = new ClusterTelemetry(
      () => this.#connections.values(),
      options.onEvent,
      options.logger,
    );
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
    body: RequestBody,
    flexible = false,
  ): Promise<ResponseBody> {
    return this.#anyRequest(apiKey, apiVersion, body, flexible);
  }

  async #anyRequest(
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
    flexible = false,
  ): Promise<ResponseBody> {
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
    const body = writeMetadataRequest(topics);
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
    body: RequestBody,
    timeoutMs?: number,
    retry = true,
    flexible = false,
  ): Promise<ResponseBody> {
    return this.#requestWithRetry(brokerId, apiKey, apiVersion, body, timeoutMs, retry, flexible);
  }

  async #requestWithRetry(
    brokerId: number,
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
    timeoutMs: number | undefined,
    retry: boolean,
    flexible: boolean,
  ): Promise<ResponseBody> {
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
    body: RequestBody,
    timeoutMs: number | undefined,
    flexible: boolean,
  ): Promise<ResponseBody> {
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
    this.#telemetry.bumpRetries(1);
    const delay = retryDelay(this.#retry, attempt);
    this.log("warn", `retrying ${apiKey} attempt ${attempt + 1} in ${delay}ms: ${String(error)}`);
    this.event({ type: "retry", apiKey, attempt: attempt + 1, delayMs: delay, error });
    if (delay) {
      await Bun.sleep(delay);
    }
  }

  async controllerRequest(
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
  ): Promise<ResponseBody> {
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
      writeFindCoordinatorRequestV1(transactionalId, 1),
    );
    const coordinator = readFindCoordinatorResponse(response);
    if (coordinator.throttleMs > 0) {
      this.throttle(API_FIND_COORDINATOR, coordinator.throttleMs);
    }
    const { error, message } = coordinator;
    if (error) {
      throw kafkaError(
        error,
        message
          ? `Find transaction coordinator ${transactionalId}: ${message}`
          : `Find transaction coordinator ${transactionalId}`,
      );
    }
    return coordinator.coordinatorId;
  }

  /** Send a Produce request without waiting for a response (acks=0). */
  async fireAndForget(
    brokerId: number,
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
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
    this.#telemetry.event(event);
  }

  log(level: keyof import("./shared.ts").Logger, message: string): void {
    this.#telemetry.log(level, message);
  }

  throttle(apiKey: number, durationMs: number): void {
    this.#telemetry.throttle(apiKey, durationMs);
  }

  stats(): ClusterStats {
    return this.#telemetry.stats();
  }

  trackStats(intervalMs: number): void {
    this.#telemetry.track(intervalMs);
  }

  stopTrackingStats(): void {
    this.#telemetry.stop();
  }

  /** Ping every known broker with an ApiVersions request and report latency. */
  healthCheck(timeoutMs = 5_000): Promise<HealthReport> {
    return checkClusterHealth(
      {
        brokers: this.#brokers,
        bootstrap: this.#bootstrap,
        request: (broker, timeout) =>
          this.#connection(broker).request(API_API_VERSIONS, 0, writeEmptyRequest(), timeout),
        log: (message) => this.log("warn", message),
      },
      timeoutMs,
    );
  }

  bumpRetries(n = 1): void {
    this.#telemetry.bumpRetries(n);
  }

  close(): void {
    this.#telemetry.stop();
    for (const connection of this.#connections.values()) {
      connection.close();
    }
    this.#connections.clear();
  }
}
