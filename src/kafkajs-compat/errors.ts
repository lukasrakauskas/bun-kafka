import { KafkaError } from "../errors.ts";

export class KafkaJSError extends Error {
  retriable: boolean;
  fatal?: boolean;
  constructor(messageOrError: string | Error, options: { retriable?: boolean; fatal?: boolean } = {}) {
    super(typeof messageOrError === "string" ? messageOrError : messageOrError.message);
    this.name = "KafkaJSError";
    this.retriable = options.retriable ?? false;
    this.fatal = options.fatal;
  }
}

export class KafkaJSNonRetriableError extends KafkaJSError {
  constructor(messageOrError: string | Error) {
    super(messageOrError);
    this.name = "KafkaJSNonRetriableError";
  }
}

export class KafkaJSProtocolError extends KafkaJSError {
  readonly code: number;
  constructor(error: string | Error, code = -1) {
    super(error);
    this.name = "KafkaJSProtocolError";
    this.code = code;
  }
}

export class KafkaJSConnectionError extends KafkaJSError {
  readonly broker?: string;
  constructor(message: string, options: { retriable?: boolean; broker?: string } = {}) {
    super(message, options);
    this.name = "KafkaJSConnectionError";
    this.broker = options.broker;
  }
}

export class KafkaJSTimeout extends KafkaJSError {
  constructor(message = "Timed out waiting for response") {
    super(message, { retriable: true });
    this.name = "KafkaJSTimeout";
  }
}

export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  constructor(message: string) {
    super(message, 1);
    this.name = "KafkaJSOffsetOutOfRange";
  }
}

export class KafkaJSNumberOfRetriesExceeded extends KafkaJSError {
  readonly originalError?: unknown;
  constructor(message = "Number of retries exceeded", originalError?: unknown) {
    super(message, { retriable: true });
    this.name = "KafkaJSNumberOfRetriesExceeded";
    this.originalError = originalError;
  }
}

/** Surface bun-kafka errors through kafkajs-shaped classes, preserving codes. */
export function wrapError(error: unknown): Error {
  if (error instanceof KafkaError) {
    const wrapped = new KafkaJSProtocolError(error.message, error.code);
    wrapped.retriable = error.retriable;
    return wrapped;
  }
  if (error instanceof Error && error.name.startsWith("KafkaJS")) return error;
  if (error instanceof Error) return new KafkaJSError(error);
  return new KafkaJSError(String(error));
}
