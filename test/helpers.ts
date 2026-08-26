import { BunAdmin, BunConsumer, BunProducer, type KafkaOptions } from "../index.ts";

export const BROKERS = process.env.KAFKA_BROKERS ?? "127.0.0.1:9092";

const options = (): KafkaOptions => ({ brokers: BROKERS.split(",") });

export function topic(prefix = "bun-kafka") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function producer() {
  return new BunProducer(options());
}

export function consumer(extra: { fromBeginning?: boolean } = {}) {
  return new BunConsumer(options(), extra);
}

export function admin() {
  return new BunAdmin(options());
}

export async function waitFor<T>(
  fn: () => T | Promise<T | null | undefined> | null | undefined,
  { timeoutMs = 20_000, intervalMs = 50 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> {
  const start = Date.now();
  let lastErr: unknown;
  while (Date.now() - start < timeoutMs) {
    try {
      const value = await fn();
      if (value !== undefined) return value;
    } catch (error) {
      lastErr = error;
    }
    await Bun.sleep(intervalMs);
  }
  const detail = lastErr ? `: ${lastErr}` : "";
  throw new Error(`waitFor timed out after ${timeoutMs}ms${detail}`);
}

export async function waitTopic(name: string, timeoutMs = 15_000) {
  const client = admin();
  try {
    return await waitFor(
      async () => {
        const metadata = await client.metadata([name]);
        return metadata.topics.find((item) => item.name === name && item.partitions.length) ?? null;
      },
      { timeoutMs, intervalMs: 150 },
    );
  } finally {
    await client.close();
  }
}

export async function produceN(p: BunProducer, name: string, count: number, prefix = "m") {
  await p.send({
    topic: name,
    messages: Array.from({ length: count }, (_, i) => ({
      key: `k-${i}`,
      value: `${prefix}-${i}`,
      headers: { i: String(i) },
    })),
  });
}

export function dec(value: Uint8Array | null | undefined) {
  return value ? new TextDecoder().decode(value) : null;
}
