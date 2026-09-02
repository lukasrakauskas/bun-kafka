import { AsyncLocalStorage } from "node:async_hooks";

export type KafkaRequestEvent = {
  apiKey: number;
  apiVersion: number;
  broker: string;
  duration: number;
  timeout: boolean;
  error?: Error;
};

type KafkaRequestListener = (event: KafkaRequestEvent) => void;

export class RequestObserver {
  #scope = new AsyncLocalStorage<KafkaRequestListener>();

  run<T>(listener: KafkaRequestListener, request: () => T): T {
    return this.#scope.run(listener, request);
  }

  async observe<T>(
    broker: string,
    apiKey: number,
    apiVersion: number,
    request: () => Promise<T>,
  ): Promise<T> {
    const startedAt = performance.now();
    try {
      const response = await request();
      this.#emit({ apiKey, apiVersion, broker, duration: performance.now() - startedAt });
      return response;
    } catch (error) {
      this.#emit({
        apiKey,
        apiVersion,
        broker,
        duration: performance.now() - startedAt,
        error: error instanceof Error ? error : new Error(String(error)),
      });
      throw error;
    }
  }

  #emit(event: Omit<KafkaRequestEvent, "timeout">): void {
    try {
      this.#scope.getStore()?.({
        ...event,
        timeout: /timed out|timeout/i.test(event.error?.message ?? ""),
      });
    } catch {
      // Instrumentation must not break requests.
    }
  }
}
