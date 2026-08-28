import { KafkaError } from "../../errors.ts";
import { retryDelay, type RetryOptions } from "../shared.ts";

type RetryCallback = (attempt: number, error: KafkaError, delayMs: number) => void | Promise<void>;

function canRetry(error: KafkaError, attempt: number, maxRetries: number): boolean {
  return error.retriable && attempt < maxRetries;
}

export async function withRetry<T>(
  options: Required<RetryOptions>,
  operation: (attempt: number) => Promise<T>,
  onRetry?: RetryCallback,
): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await operation(attempt);
    } catch (error) {
      if (!(error instanceof KafkaError) || !canRetry(error, attempt, options.maxRetries)) {
        throw error;
      }
      const delay = retryDelay(options, attempt);
      await onRetry?.(attempt, error, delay);
      if (delay) {
        await Bun.sleep(delay);
      }
    }
  }
}
