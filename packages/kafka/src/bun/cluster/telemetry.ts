import type { Connection } from "../connection/index.ts";
import type { ClusterStats, KafkaEvent, Logger } from "../shared.ts";

export class ClusterTelemetry {
  #retries = 0;
  #throttles = 0;
  #throttleTimeMs = 0;
  #timer?: ReturnType<typeof setInterval>;

  constructor(
    private readonly connections: () => Iterable<Connection>,
    private readonly onEvent?: (event: KafkaEvent) => void,
    private readonly logger: Partial<Logger> = {},
  ) {}

  event(event: KafkaEvent): void {
    try {
      this.onEvent?.(event);
    } catch {
      /* Observability must not break requests. */
    }
  }

  log(level: keyof Logger, message: string): void {
    try {
      this.logger[level]?.(message);
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

  stats(): ClusterStats {
    let requests = 0;
    let bytesSent = 0;
    let bytesReceived = 0;
    let connections = 0;
    for (const connection of this.connections()) {
      connections++;
      requests += connection.stats.requests;
      bytesSent += connection.stats.bytesSent;
      bytesReceived += connection.stats.bytesReceived;
    }
    return {
      connections,
      requests: requests + this.#retries,
      bytesSent,
      bytesReceived,
      retries: this.#retries,
      throttles: this.#throttles,
      throttleTimeMs: this.#throttleTimeMs,
    };
  }

  track(intervalMs: number): void {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) {
      throw new RangeError("Invalid stats interval");
    }
    this.stop();
    this.#timer = setInterval(() => this.event({ type: "stats", stats: this.stats() }), intervalMs);
    this.#timer.unref?.();
  }

  stop(): void {
    if (this.#timer) {
      clearInterval(this.#timer);
    }
    this.#timer = undefined;
  }

  bumpRetries(count: number): void {
    this.#retries += count;
  }
}
