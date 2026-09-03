import type { ConnectionOptions } from "../connection/index.ts";
import {
  DEFAULT_CONNECT_TIMEOUT_MS,
  DEFAULT_INITIAL_BACKOFF_MS,
  DEFAULT_MAX_BACKOFF_MS,
  DEFAULT_MAX_RESPONSE_BYTES,
  DEFAULT_MAX_RETRIES,
  DEFAULT_REQUEST_TIMEOUT_MS,
  SIZE_I32,
  type KafkaOptions,
  type RetryOptions,
} from "../shared.ts";

type ResolvedClusterOptions = {
  bootstrap: string[];
  rackId?: string;
  connection: ConnectionOptions;
  retry: Required<RetryOptions>;
};

function validateTimeouts(
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
    maxResponseBytes < SIZE_I32 ||
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

function validateSasl(options: KafkaOptions): void {
  const sasl = options.sasl;
  if (
    sasl &&
    (!new Set(["plain", "scram-sha-256", "scram-sha-512", "oauthbearer"]).has(sasl.mechanism) ||
      (sasl.mechanism === "oauthbearer" ? !sasl.token : !sasl.username || !sasl.password))
  ) {
    throw new TypeError("Invalid Kafka SASL options");
  }
}

function validateRack(options: KafkaOptions): void {
  if (
    options.rackId !== undefined &&
    (!options.rackId.length || options.rackId.trim() !== options.rackId)
  ) {
    throw new TypeError("Kafka rackId must be a non-empty, trimmed string");
  }
}

function validateStats(options: KafkaOptions): void {
  if (
    options.statsIntervalMs !== undefined &&
    (!Number.isSafeInteger(options.statsIntervalMs) || options.statsIntervalMs < 1)
  ) {
    throw new RangeError("Invalid Kafka statsIntervalMs");
  }
}

export function resolveClusterOptions(options: KafkaOptions): ResolvedClusterOptions {
  if (!Array.isArray(options.brokers) || !options.brokers.length) {
    throw new TypeError("Kafka requires at least one broker");
  }
  const requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const connectTimeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
  const maxResponseBytes = options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES;
  const retry = {
    maxRetries: options.retry?.maxRetries ?? DEFAULT_MAX_RETRIES,
    initialBackoffMs: options.retry?.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
    maxBackoffMs: options.retry?.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS,
  };
  validateTimeouts(requestTimeoutMs, connectTimeoutMs, maxResponseBytes, retry);
  validateSasl(options);
  validateRack(options);
  validateStats(options);
  return {
    bootstrap: [...options.brokers],
    rackId: options.rackId,
    retry,
    connection: {
      clientId: options.clientId ?? "bun-kafka",
      requestTimeoutMs,
      connectTimeoutMs,
      maxResponseBytes,
      tls: options.tls,
      sasl: options.sasl,
    },
  };
}
