import { Producer, Consumer, Admin, useNative, type NativeKind } from "../src/index.ts";

export const BROKERS = process.env.KAFKA_BROKERS ?? "127.0.0.1:9092";

export async function initNative(kind?: NativeKind) {
  const k = kind ?? ((process.env.BUN_KAFKA_NATIVE as NativeKind) || "ffi");
  await useNative(k);
  return k;
}

export function topic(prefix = "bun-kafka") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function producer(extra: Record<string, string | number | boolean> = {}) {
  return new Producer({
    "bootstrap.servers": BROKERS,
    "socket.timeout.ms": 10000,
    "message.timeout.ms": 10000,
    acks: "all",
    ...extra,
  });
}

export function consumer(extra: Record<string, string | number | boolean> = {}) {
  return new Consumer({
    "bootstrap.servers": BROKERS,
    "group.id": `g-${crypto.randomUUID()}`,
    "enable.auto.commit": false,
    "auto.offset.reset": "earliest",
    "socket.timeout.ms": 10000,
    "session.timeout.ms": 10000,
    ...extra,
  });
}

export function admin() {
  return new Admin({ "bootstrap.servers": BROKERS, "socket.timeout.ms": 10000 });
}

export async function waitFor<T>(
  fn: () => T | Promise<T | null | undefined> | null | undefined,
  { timeoutMs = 20_000, intervalMs = 50 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const v = await fn();
    if (v) return v as T;
    await Bun.sleep(intervalMs);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

export async function produceN(p: Producer, t: string, n: number, prefix = "m") {
  for (let i = 0; i < n; i++) {
    p.send({ topic: t, key: `k-${i}`, value: `${prefix}-${i}`, headers: { i: String(i) } });
  }
  await p.flush(15_000);
}

export function dec(u: Uint8Array | null | undefined) {
  return u ? new TextDecoder().decode(u) : null;
}
